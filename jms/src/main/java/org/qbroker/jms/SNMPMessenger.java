package org.qbroker.jms;

/* SNMPMessenger.java - an SNMP messenger for JMS messages */

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.jms.Topic;
import javax.jms.Queue;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.net.SNMPConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * SNMPMessenger handles JMS Messages IO on SNMP resources.  The SNMP env
 * has to be established before using this object.  Due to SNMP4J, it
 * requires Java 1.4 or above.
 *<br><br>
 * There are four methods, listen(), talk(), inquire() and reply() on packets.
 * The method of listen() is NOT MT-Safe.  The method of talk() is for
 * asynchronous output whereas inquire() is for synchronous requests.
 *<br><br>
 * For charSet and encoding, you can overwrite the default encoding by
 * starting JVM with "-Dfile.encoding=UTF8 your_class".
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SNMPMessenger extends SNMPConnector {
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int sleepTime = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int capacity, maxNumberMsg = 0;
    private Template template = null;
    private int[] partition;
    private SimpleDateFormat dateFormat = null;
    private java.lang.reflect.Method parse;
    private Object parser;

    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private String communityField, versionField;
    private String uriField, oidField, rcField, operation = "listen";

    public SNMPMessenger(Map props) {
        super(props);
        Object o;

        if ((o = props.get("URIField")) != null)
            uriField = (String) o;
        else
            uriField = "UDP";

        if ((o = props.get("OIDField")) != null)
            oidField = (String) o;
        else
            oidField = "OID";

        if ((o = props.get("VersionField")) != null)
            versionField = (String) o;
        else
            versionField = "VERTION";

        if ((o = props.get("CommunityField")) != null)
            communityField = (String) o;
        else
            communityField = "COMMUNITY";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value, cellID;
            Template temp = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            cellID = (String) props.get("CellID");
            if (cellID == null || cellID.length() <= 0)
                cellID = "0";
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if ((propertyName[n] = MessageUtils.getPropertyID(key)) == null)
                    propertyName[n] = key;
                if (value != null && value.length() > 0) {
                    propertyValue[n] = temp.substitute("CellID", cellID, value);
                }
                n ++;
            }
        }

        if (props.get("Capacity") != null)
            capacity = Integer.parseInt((String) props.get("Capacity"));
        else
            capacity = 1;

        if (props.get("Operation") != null) {
            operation = (String) props.get("Operation");
        }

        if ("inform".equals(operation) || "inquire".equals(operation)) {
            Map ph = (Map) props.get("Parser");
            if (ph != null && (o = ph.get("ClassName")) != null) {
                String className;
                Object parserArg;
                Class<?> cls, cc;
                className = (String) o;
                if (className.length() == 0)
                    throw(new IllegalArgumentException(uri +
                        ": ClassName is empty for "+operation));

                try {
                    cls = Class.forName(className);
                    parse = cls.getMethod("parse", new Class[]{String.class});
                    if (parse == null)
                       throw(new IllegalArgumentException(uri+": no parse()"+
                           " method in " + className + " for " + operation));

                    if ((o = ph.get("ParserArgument")) != null) {
                        parserArg = o;
                        if (parserArg instanceof String)
                            cc = String.class;
                        else if (parserArg instanceof Map)
                            cc = Map.class;
                        else
                            cc = List.class;
                    }
                    else {
                        parserArg = (String) null;
                        cc = String.class;
                    }
                }
                catch (Exception e) {
                  throw(new IllegalArgumentException(uri+": no parse() method"+
                        " in "+ className + " for " + operation + ": " +
                        Event.traceStack(e)));
                }

                try {
                    java.lang.reflect.Constructor con =
                        cls.getConstructor(new Class[]{cc});
                    parser = con.newInstance(new Object[] {parserArg});
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(uri+": failed to " +
                        "instantiate "+ className+" for "+operation+": "+
                        Event.traceStack(e)));
                }
            }
            else {
                parser = null;
                parse = null;
            }
        }

        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else if ((o = props.get("CellID")) != null) {
            partition = new int[2];
            partition[0] = Integer.parseInt((String) o);
            partition[1] = 1;
        }
        else {
            partition = new int[2];
            partition[0] = partition[1] = 0;
        }

        if (props.get("TextMode") != null) {
            textMode = Integer.parseInt((String) props.get("TextMode"));
        }
        if (props.get("XAMode") != null) {
            xaMode = Integer.parseInt((String) props.get("XAMode"));
        }
        if (props.get("DisplayMask") != null) {
            displayMask = Integer.parseInt((String) props.get("DisplayMask"));
        }
        if ((o = props.get("MaxNumberMessage")) != null) {
            maxNumberMsg = Integer.parseInt((String) o);
        }
        if (props.get("WaitTime") != null) {
            waitTime= Long.parseLong((String) props.get("WaitTime"));
            if (waitTime <= 0)
                waitTime = 500L;
        }
        if (props.get("ReceiveTime") != null) {
            receiveTime= Integer.parseInt((String) props.get("ReceiveTime"));
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        if (props.get("SleepTime") != null) {
            sleepTime= Integer.parseInt((String) props.get("SleepTime"));
        }
        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
        }
        if (props.get("MaxMsgLength") != null) {
            maxMsgLength = Long.parseLong((String) props.get("MaxMsgLength"));
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS zz");
    }

    /**
     * It listens to the DatagramSocket for incoming SNMP packets.  Once
     * receiving an SNMP packet, it loads the payload of the packet to a
     * JMSMessage and puts it to the XQueue as output.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like receive,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br><br>
     * This method is NOT MT-Safe.
     */
    public void listen(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        String[] content = null;
        String msgStr = null;
        StringBuffer strBuf;
        int sid = -1, cid, length, mask = 0;
        long count = 0, stm = 10;
        DatagramPacket packet;
        byte[] buffer = new byte[bufferSize];
        boolean isText;
        boolean isSleepy = (sleepTime > 0);
        int shift = partition[0];
        int len = partition[1];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        switch (len) {
          case 0:
            do { // reserve an empty cell
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            do { // reserve the empty cell
                sid = xq.reserve(waitTime, shift);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            do { // reserve a partitioned empty cell
                sid = xq.reserve(waitTime, shift, len);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
        }

        if (sid >= 0) {
            if (textMode == 0)
                outMessage = new BytesEvent();
            else
                outMessage = new TextEvent();
            isText = (outMessage instanceof TextMessage);
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return;
        }
        else {
            return;
        }

        packet = new DatagramPacket(buffer, 0, bufferSize);
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            try {
                content = receive(packet);
            }
            catch (InterruptedIOException e) {
                if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                    break;
                else
                    continue;
            }
            catch (IOException e) {
                xq.cancel(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.cancel(sid);
                Event.flush(e);
            }
            if (content == null || content.length <= 0) {
                if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                    break;
                else
                    continue;
            }

            try {
                if (propertyName != null && propertyValue != null) {
                    outMessage.clearProperties();
                    for (int i=0; i<propertyName.length; i++)
                        MessageUtils.setProperty(propertyName[i],
                            propertyValue[i], outMessage);
                }
                MessageUtils.setProperty(uriField, content[0], outMessage);
                MessageUtils.setProperty("type", content[1], outMessage);
                strBuf = new StringBuffer();
                for (int i=2; i<content.length; i++)
                    strBuf.append(content[i] + "\n");
                length = strBuf.length();
                msgStr = strBuf.toString();

                if (isText)
                    ((TextMessage) outMessage).setText(msgStr);
                else
                    ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());

                outMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg with content " +
                    "listened from " + content[0] + ": " +
                    Event.traceStack(e)).send();
                return;
            }

            cid = xq.add(outMessage, sid);
            if (cid >= 0) {
                if (displayMask > 0) try {
                    String line = MessageUtils.display(outMessage,
                        msgStr, displayMask, null);
                    new Event(Event.INFO, "listened " + length + " bytes from "+
                        content[0] + " into a msg (" + line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.INFO, "listened " + length + " bytes from "+
                        content[0] + " into a msg").send();
                }
                count ++;
                if (isSleepy) { // slow down a while
                    long tm = System.currentTimeMillis() + sleepTime;
                    do {
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) > 0) // temporarily disabled
                            break;
                        else try {
                            Thread.sleep(stm);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (tm > System.currentTimeMillis());
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) > 0) // temporarily disabled
                        break;
                }
            }
            else {
                xq.cancel(sid);
                new Event(Event.WARNING, xq.getName() +
                     "XQ is full and lost SNMP content: " + msgStr).send();
            }

            sid = -1;
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.PAUSE) == 0) switch (len) {
              case 0:
                do { // reserve an empty cell
                    sid = xq.reserve(waitTime);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
              case 1:
                do { // reserve the empty cell
                    sid = xq.reserve(waitTime, shift);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
              default:
                do { // reserve a partitioned empty cell
                    sid = xq.reserve(waitTime, shift, len);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
            }

            if (sid >= 0) {
                if (textMode == 0)
                    outMessage = new BytesEvent();
                else
                    outMessage = new TextEvent();
            }
            else {
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0)
                    new Event(Event.ERR, "failed to reserve a cell on " +
                        xq.getName() + " for " + uri).send();
                else if (displayMask != 0 && maxNumberMsg != 1)
                    new Event(Event.INFO, "listened " + count + " msgs from " +
                        uri).send();
                return;
            }
        }
        if (sid >= 0)
            xq.cancel(sid);

        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "listened "+count+" msgs from "+ uri).send();
    }

    /**
     * It continually gets a JMS Message with an SNMP request from the XQueue.
     * Then it sends the request to the destination and waits for the response.
     * Once response is back, it puts the content back to the message and
     * removes the message from the XQueue.
     *<br><br>
     * Most of the IOExceptions from the packet IO operations, like send,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br><br>
     * Since the inquire operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the inquirey is successful.  This method will
     * not acknowledge messages.  It is up to the requester to do that.
     *<br><br>
     * This method is NOT MT-Safe.
     */
    public void inquire(XQueue xq) throws IOException, JMSException {
        Message inMessage;
        int begin, len, length, m = 0, version;
        int sid = -1, mask;
        long count = 0;
        String oid = null, toURI = null, msgStr = null, community;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        byte[] buffer = new byte[bufferSize];

        begin = partition[0];
        len = partition[1];
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            switch (len) {
              case 0:
                sid = xq.getNextCell(waitTime);
                break;
              case 1:
                sid = xq.getNextCell(waitTime, begin);
                break;
              default:
                sid = xq.getNextCell(waitTime, begin, len);
                break;
            }
            if (sid < 0) {
                continue;
            }

            inMessage = (Message) xq.browse(sid);
            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, "-1", inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, "-1", inMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        inMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    inMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                inMessage = null;
                continue;
            }

            toURI = null;
            oid = null;
            community = null;
            version = -1;
            try {
                toURI = MessageUtils.getProperty(uriField, inMessage);
                oid = MessageUtils.getProperty(oidField, inMessage);
                msgStr = MessageUtils.getProperty(versionField, inMessage);
                community = MessageUtils.getProperty(communityField, inMessage);
                if (msgStr != null)
                    version = Integer.parseInt(msgStr);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get toURI for " +
                    uri).send();
                inMessage = null;
                continue;
            }

            if (oid == null) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri +
                    ": failed to get oid from msg").send();
                inMessage = null;
                continue;
            }

            msgStr = null;
            try {
                if (toURI == null) { // use the default target
                    msgStr = snmpGet(null, oid);
                    toURI = agentAddress.getHostAddress() + "/" + port;
                }
                else {
                    if (version < 0 || version > 2)
                        version = getDefaultVersion();
                    if (community == null)
                        community = this.community;
                    msgStr = snmpInquire(version, community, toURI, oid);
                }
            }
            catch (IOException e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(e);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                Event.flush(e);
            }

            try {
                if (inMessage instanceof TextMessage)
                    ((TextMessage) inMessage).setText(msgStr);
                else
                    ((BytesMessage) inMessage).writeBytes(msgStr.getBytes());
                MessageUtils.setProperty(rcField, String.valueOf(sid),
                    inMessage);
                inMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set content to msg: " +
                     msgStr).send();
            }

            if (ack) try { // try to ack the msg
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after talk to " +
                    toURI + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after talk to " +
                    toURI + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after takl to " +
                    toURI + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "inquired of " + count + " msgs for " +
                uri).send();
    }

    /**
     * It continually gets a JMS Message from the XQueue, sends its content
     * and its OID to an SNMP manager.
     *<br><br>
     * Most of the IOExceptions from the packet IO operations, like send,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br><br>
     * It will not modify the original messages.  If the XQueue has enabled
     * the EXTERNAL_XA, it will also acknowledge every messages.  This method
     * is MT-Safe.
     */
    public void send(XQueue xq) throws IOException, JMSException {
        Message inMessage;
        int length, version;
        int sid = -1, mask;
        int dmask = MessageUtils.SHOW_DATE;
        long count = 0, stm = 10;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        String oid = null, toURI = null, msgStr = null, dt = null;
        String community;
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            toURI = null;
            oid = null;
            community = null;
            version = -1;
            try {
                toURI = MessageUtils.getProperty(uriField, inMessage);
                oid = MessageUtils.getProperty(oidField, inMessage);
                msgStr = MessageUtils.getProperty(versionField, inMessage);
                community = MessageUtils.getProperty(communityField, inMessage);
                if (msgStr != null)
                    version = Integer.parseInt(msgStr);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to get toURI for " +
                    uri + ": " + e.toString()).send();
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(inMessage,buffer,template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null || oid == null) {
                if (msgStr == null)
                    new Event(Event.WARNING, uri +
                        ": unknown msg type to "+ toURI +
                        " and dropped the msg: " + msgStr).send();
                else
                    new Event(Event.WARNING, uri +
                        ": failed to get oid on "+ toURI +
                        " and dropped the msg: " + msgStr).send();
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            length = msgStr.length();
            try {
                if (toURI == null) { // use the default target
                    if (getDefaultVersion() == 2)
                        snmpNotify(null, new String[][]{{oid, "s", msgStr}});
                    else
                       sendTrap(null,null,6,1,new String[][]{{oid,"s",msgStr}});
                    toURI = agentAddress.getHostAddress() + "/" + port;
                }
                else {
                    if (version < 0 || version > 2)
                        version = getDefaultVersion();
                    if (community == null)
                        community = this.community;
                    if (version == 2)
                        snmpNotify(version, community, toURI,
                            new String[][]{{oid, "s", msgStr}});
                    else
                        snmpTrap(version, community, toURI, null,
                            6, 1, new String[][]{{oid, "s", msgStr}});
                }
            }
            catch (IOException e) {
                xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            if (ack) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after talk to "+
                    toURI + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after talk to "+
                    toURI + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after talk to "+
                    toURI + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt= Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "talked a packet of " + length +
                        " bytes to " + toURI + " with a msg ( Date: " +
                        dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "talked a packet of " + length +
                        " bytes to " + toURI + " with a msg (" +
                        line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "talked a packet of " + length +
                    " bytes to " + toURI + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "talked " + count + " msgs to " + uri).send();
    }

    public String getOperation() {
        return operation;
    }
}
