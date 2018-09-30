package org.qbroker.jms;

/* MessagePacket.java - a Datagram Pacekt for JMS messages */

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
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
import org.qbroker.common.QuickCache;
import org.qbroker.common.QList;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Browser;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * MessagePacket handles JMS Messages IO on DatagramSocket.  The DatagramSocket
 * has to be established before using this object.
 *<br/><br/>
 * There are four methods, listen(), talk(), inquire() and reply() on packets.
 * The method of listen() is NOT MT-Safe.  The method of talk() is for
 * asynchronous output whereas inquire() is for synchronous requests.
 *<br/><br/>
 * For charSet and encoding, you can overwrite the default encoding by
 * starting JVM with "-Dfile.encoding=UTF8 your_class".
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MessagePacket {
    private String uri;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int sleepTime = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int capacity, maxNumberMsg = 0;
    private int[] partition;
    private SimpleDateFormat dateFormat = null;
    private Template template = null;
    private QuickCache cache = null;
    private QList pktList = null;
    private CollectibleCells cells = null;
    private java.lang.reflect.Method parse = null, ackMethod = null;
    private Object parser = null;

    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private String uriField, rcField, operation = "listen";

    public MessagePacket(Map props) {
        Object o;

        if ((o = props.get("URI")) != null)
            uri = (String) o;
        else
            throw(new IllegalArgumentException("URI is not defined"));

        if ((o = props.get("URIField")) != null)
            uriField = (String) o;
        else
            uriField = "UDP";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

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

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if (props.get("Capacity") != null)
            capacity = Integer.parseInt((String) props.get("Capacity"));
        else
            capacity = 1;

        if (props.get("Operation") != null) {
            operation = (String) props.get("Operation");
        }

        if ("reply".equals(operation) || "inquire".equals(operation)) {
            Class<?> cls, cc;
            Map ph = (Map) props.get("Parser");
            if (ph != null && (o = ph.get("ClassName")) != null) {
                String className;
                Object parserArg;
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
                        cc = List.class;
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
            if ("reply".equals(operation)) try {
                cls = Class.forName("org.qbroker.common.CollectibleCells");
                ackMethod = cls.getMethod("acknowledge",
                        new Class[] {long[].class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(uri+": failed to " +
                    "find ack method: " + e.toString()));
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

        pktList = new QList(uri, capacity);
        cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 1024);
        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It listens to the DatagramSocket for incoming DatagramPackets.  Once
     * receiving a DatagramPacket, it loads the payload of the packet to a
     * JMSMessage and puts it to the XQueue as output.
     *<br/><br/>
     * Most of the IOExceptions from the stream IO operations, like receive,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br/><br/>
     * This method is NOT MT-Safe.
     */
    public long listen(DatagramSocket in, XQueue xq) throws IOException,
        JMSException {
        Message outMessage;
        DatagramPacket packet;
        String fromURI;
        int sid = -1, cid, length, mask = 0;
        long count = 0;
        byte[] buffer = new byte[bufferSize];
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

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
            return count;
        }
        else {
            return count;
        }

        packet = new DatagramPacket(buffer, 0, bufferSize);
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // paused temporarily
                xq.cancel(sid);
                break;
            }
            try {
                in.receive(packet);
            }
            catch (InterruptedIOException e) {
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

//            buffer = packet.getData();
            length = packet.getLength();
            fromURI = packet.getAddress().getHostAddress()+":"+packet.getPort();
            packet.setLength(bufferSize);

            try {
                if (propertyName != null && propertyValue != null) {
                    outMessage.clearProperties();
                    for (int i=0; i<propertyName.length; i++)
                        MessageUtils.setProperty(propertyName[i],
                            propertyValue[i], outMessage);
                }
                MessageUtils.setProperty(uriField, fromURI, outMessage);

                if (isText)
                    ((TextMessage) outMessage).setText(
                        new String(buffer,0,length));
                else
                    ((BytesMessage) outMessage).writeBytes(buffer, 0, length);

                outMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg with packet " +
                    "listened from " + fromURI + ": " +
                    Event.traceStack(e)).send();
                return count;
            }

            cid = xq.add(outMessage, sid);
            if (cid >= 0) {
                if (displayMask > 0) try {
                    String line = MessageUtils.display(outMessage,
                        new String(buffer, 0, length), displayMask, null);
                    new Event(Event.INFO, "listened " + length + " bytes from "+
                        fromURI + " into a msg (" + line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.INFO, "listened " + length + " bytes from "+
                        fromURI + " into a msg").send();
                }
                count ++;
            }
            else {
                xq.cancel(sid);
                new Event(Event.WARNING, xq.getName() +
                     "XQ is full and lost packet: " +
                     new String(buffer, 0, length)).send();
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
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0)
                    new Event(Event.ERR, "failed to reserve a cell on " +
                        xq.getName() + " for " + uri).send();
                else if (displayMask != 0 && maxNumberMsg != 1)
                    new Event(Event.INFO, "listened " + count +
                        " msgs from " + uri).send();
                return count;
            }
        }
        if (sid >= 0)
            xq.cancel(sid);

        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "listened "+count+" msgs from "+ uri).send();

        return count;
    }

    /**
     * returns number of replied messages
     */
    private int feedback(XQueue xq, DatagramSocket sock, QList pktList,
        CollectibleCells cells) {
        int n = 0, k = 0, id;
        DatagramPacket packet;
        Message msg;
        Object o;
        String msgStr;

        while ((id = cells.collect(waitTime)) >= 0) {
            o = pktList.takeback(id);
            if (o == null || !(o instanceof Object[]) || ((Object[])o).length<2)
                continue;
            msg = (Message) ((Object[]) o)[0];
            packet = (DatagramPacket) ((Object[]) o)[1];
            if (displayMask > 0)
                n = packet.getLength();
            msgStr = send(sock, msg, packet);
            k ++;
            if (displayMask > 0) try {
                String line = MessageUtils.display(msg, msgStr,
                    displayMask, null);
                String fromURI = MessageUtils.getProperty(uriField, msg);
                new Event(Event.INFO, "replied on an inquiry of " + n +
                    " bytes from " + fromURI + " with a msg (" + line +
                    " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "replied on an inquiry of " +
                    n + " bytes with a msg").send();
            }
        }
        return k;
    }

    /**
     * returns the string sent to the UDP destination
     */
    private String send(DatagramSocket sock, Message message,
        DatagramPacket packet) {
        int length;
        String msgStr = null;
        byte[] buffer = null;
        if (sock == null || message == null || packet == null)
            return null;
        buffer = packet.getData();
        if (buffer == null)
            return null;
        try {
            if (template != null)
                msgStr = MessageUtils.format(message, buffer, template);
            else
                msgStr = MessageUtils.processBody(message, buffer);
        }
        catch (JMSException e) {
            return null;
        }

        if (msgStr == null)
            return null;
        length = msgStr.length();
        packet.setData(msgStr.getBytes(), 0, length);
        try {
            sock.send(packet);
        }
        catch (IOException e) {
        }
        return msgStr;
    }

    /**
     * It listens to a DatagramSocket for incoming DatagramPackets and converts
     * the packet into a JMS message as a request.  Once
     * receiving a DatagramPacket, it loads the payload of the packet to a
     * JMSMessage and puts it to the XQueue as output.  Meanwhile, it checks
     * the XQueue for the feedback messages.  Once it finds one, the content
     * of the message will be sent out as the reply.
     *<br/><br/>
     * Most of the IOExceptions from the packet IO operations, like receive,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br/><br/>
     * This method is NOT MT-Safe.
     */
    public long reply(DatagramSocket sock, XQueue xq) throws IOException,
        JMSException {
        Message outMessage;
        DatagramPacket packet;
        String fromURI;
        int sid = -1, cid, length;
        int begin, len, mask = 0;
        long count = 0;
        boolean isText = (textMode != 0);
        byte[] buffer;

        begin = partition[0];
        len = partition[1];

        switch (len) {
          case 0:
            do {
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            do {
                sid = xq.reserve(waitTime, begin);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            do {
                sid = xq.reserve(waitTime, begin, len);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
        }

        if (sid >= 0) {
            if (isText)
                outMessage = new TextEvent();
            else
                outMessage = new BytesEvent();
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return count;
        }
        else {
            return count;
        }

        packet = new DatagramPacket(new byte[bufferSize], 0, bufferSize);
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // paused temporarily
                xq.cancel(sid);
                break;
            }
            try {
                sock.receive(packet);
            }
            catch (InterruptedIOException e) {
                if (pktList.depth() > 0)
                    count += feedback(xq, sock, pktList, cells);
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

            buffer = packet.getData();
            length = packet.getLength();
            fromURI = packet.getAddress().getHostAddress()+":"+packet.getPort();

            try {
                if (propertyName != null && propertyValue != null) {
                    outMessage.clearProperties();
                    for (int i=0; i<propertyName.length; i++)
                        MessageUtils.setProperty(propertyName[i],
                            propertyValue[i], outMessage);
                }
                MessageUtils.setProperty(uriField, fromURI, outMessage);
                if (parse != null) {
                    Throwable t = MessageUtils.parse(parse, parser,
                        new String(buffer, 0, length), outMessage);
                    if (t != null) {
                        if (t instanceof Exception)
                            throw((Exception) t);
                        else
                            throw((Error) t);
                    }
                }
                else if (isText) {
                    ((TextMessage) outMessage).setText(new String(buffer,
                        0, length));
                }
                else {
                    ((BytesMessage) outMessage).writeBytes(buffer, 0, length);
                }
                outMessage.setJMSTimestamp(System.currentTimeMillis());
                ((JMSEvent) outMessage).setAckObject(cells, ackMethod,
                    new long[]{sid});
            }
            catch (JMSException e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg with packet " +
                    "received from " + fromURI + ": " +
                    Event.traceStack(e)).send();
                throw(e);
            }
            catch (Exception e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg with packet " +
                    "received from " + fromURI + ": " +
                    Event.traceStack(e)).send();
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg with packet " +
                    "received from " + fromURI + ": " + e.toString()).send();
                Event.flush(e);
            }

            pktList.reserve(sid);
            pktList.add(new Object[]{outMessage, packet}, sid);
            packet = new DatagramPacket(new byte[bufferSize], 0, bufferSize);
            cid = xq.add(outMessage, sid);
            if (cid <= 0) {
                pktList.takeback(sid);
                xq.cancel(sid);
                new Event(Event.WARNING, xq.getName() +
                     "XQ is full and lost packet: " +
                     new String(buffer, 0, length)).send();
            }

            count += feedback(xq, sock, pktList, cells);

            sid = -1;
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.PAUSE) == 0) switch (len) {
              case 0:
                do {
                    sid = xq.reserve(waitTime);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
              case 1:
                do {
                    sid = xq.reserve(waitTime, begin);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
              default:
                do {
                    sid = xq.reserve(waitTime, begin, len);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0);
                break;
            }

            if (pktList.depth() > 0)
                count += feedback(xq, sock, pktList, cells);

            if (sid >= 0) { // reserved a cell
                // check if anything failed to be collected
                if (pktList.getNextID(sid) >= 0) { // need to reply
                    cells.collect(-1L, sid);
                    Object o = pktList.browse(sid);
                    pktList.remove(sid);
                    if (o != null && o instanceof Object[] &&
                        ((Object[]) o).length == 2) {
                        TextMessage msg = (TextMessage) ((Object[]) o)[0];
                        DatagramPacket p = (DatagramPacket) ((Object[]) o)[1];
                        if (displayMask > 0)
                            length = p.getLength();
                        send(sock, msg, p);
                        if (displayMask > 0) try {
                            String line = MessageUtils.display(msg,
                                msg.getText(), displayMask, null);
                            fromURI = MessageUtils.getProperty(uriField, msg);
                            new Event(Event.INFO, "replied on an inquiry of " +
                                length + " bytes from " + fromURI +
                                " with a msg (" + line + " )").send();
                       }
                       catch (Exception e) {
                           new Event(Event.INFO, "replied on an inquiry of " +
                               length + " bytes with a msg").send();
                       }
                       count ++;
                    }
                }
                if (isText)
                    outMessage = new TextEvent();
                else
                    outMessage = new BytesEvent();
            }
            else {
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0)
                    new Event(Event.ERR, "failed to reserve a cell on " +
                        xq.getName() + " for " + uri).send();
                else if (displayMask != 0 && maxNumberMsg != 1)
                    new Event(Event.INFO, "replied " + count +
                        " msgs for "+ uri).send();
                return count;
            }
        }

        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "replied "+count+" msgs for "+ uri).send();

        return count;
    }

    /**
     * It scans the each occupied msg via ids in msgList to check if it is
     * expired or not.  If it is expired, the msg will be removed from the
     * xqueue and the id will be removed from the list. It returns the number
     * of msgs expired and removed from the list. The expiration is set by
     * the requester. If it is not set, the default is to never get expired.
     */
    private int expire(QList msgList, XQueue xq) {
        Message msg;
        Browser browser;
        int k, sid;
        long t, currentTime;
        boolean ack;

        ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        browser = msgList.browser();
        currentTime = System.currentTimeMillis();
        k = 0;
        while ((sid = browser.next()) >= 0) {
            msg = (Message) xq.browse(sid);
            if (msg == null)
                continue;
            else try {
                t = msg.getJMSExpiration();
            }
            catch (Exception e) {
                t = 0L;
            }
            if (t > 0L && t <= currentTime) { // msg expired
                k ++;
                if (ack) try { // try to ack the msg
                    msg.acknowledge();
                }
                catch (Exception ee) {
                }
                msgList.remove(sid);
                xq.remove(sid);
                if (displayMask > 0) try {
                    new Event(Event.INFO, "expired a request at " + sid +
                        " to " + uri + " (" + MessageUtils.display(msg, null,
                        displayMask, null) + " )").send();
                }
                catch (Exception e) {
                }
            }
        }

        return k;
    }

    /**
     *  returns number of received messages
     */
    private int receive(DatagramSocket sock, XQueue xq, QList msgList,
        DatagramPacket packet) throws IOException {
        int i, n, sid, k = 0, length;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String fromURI, line = null, msgStr = null;
        long tm;
        byte[] buffer;
        Message message;

        if (packet == null) {
            new Event(Event.WARNING, xq.getName() + ": null packet for "+
                uri).send();
            return 0;
        }

        try {
            sock.receive(packet);
        }
        catch (InterruptedIOException e) {
            return 0;
        }
        catch (IOException e) {
            throw(e);
        }
        catch (Exception e) {
            throw(new RuntimeException(e.getMessage()));
        }
        catch (Error e) {
            Event.flush(e);
        }

        buffer = packet.getData();
        length = packet.getLength();
        msgStr = new String(buffer, 0, length);
        fromURI = packet.getAddress().getHostAddress()+":"+packet.getPort();
        packet.setLength(bufferSize);

        sid =  msgList.getNextID();
        if (sid < 0) {
            new Event(Event.WARNING, uri + ": dropped a packet from "+
                fromURI + ": " + msgStr).send();
            return 0;
        }
        message = (Message) msgList.browse(sid);
        if (message == null) {
            new Event(Event.WARNING, uri + ": dropped a packet from "+
                fromURI + ": " + msgStr).send();
            msgList.remove(sid);
            xq.remove(sid);
            return 0;
        }

        try {
            if (parse != null) { // parser defined
                tm = message.getJMSTimestamp();
                Throwable t = MessageUtils.parse(parse, parser, msgStr,
                    message);
                if (t != null) {
                    if (t instanceof Exception)
                        throw((Exception) t);
                    else
                        throw((Error) t);
                }
                // get sid from the received message
                line = MessageUtils.getProperty(rcField, message);
                if (line != null && !line.equals(String.valueOf(sid))) {
                    i = Integer.parseInt(line);
                    try {
                        message.clearProperties();
                        message.clearBody();
                        message.setJMSTimestamp(tm);
                        MessageUtils.setProperty(rcField, String.valueOf(sid),
                            message);
                    }
                    catch (Exception ex) {
                    }
                    msgList.putback(sid);
                    sid = msgList.getNextID(i);
                    message = (Message) msgList.browse(sid); 
                    t = MessageUtils.parse(parse, parser, msgStr, message);
                    if (t != null) {
                        if (t instanceof Exception)
                            throw((Exception) t);
                        else
                            throw((Error) t);
                    }
                }
            }
            else if (message instanceof TextMessage)
                ((TextMessage) message).setText(msgStr);
            else
                ((BytesMessage) message).writeBytes(buffer, 0, length);

            MessageUtils.setProperty(uriField, fromURI, message);
            if (displayMask > 0)
                line = MessageUtils.display(message, msgStr, displayMask,
                    null);
            message.setJMSTimestamp(System.currentTimeMillis());
        }
        catch (JMSException e) {
            msgList.putback(sid);
            new Event(Event.WARNING, "failed to load msg with packet " +
                "received from " + fromURI + ": "+ Event.traceStack(e)).send();
            return 0;
        }
        catch (Exception e) {
            msgList.putback(sid);
            new Event(Event.WARNING, "failed to load msg with packet " +
                "received from " + fromURI + ": "+ Event.traceStack(e)).send();
            return 0;
        }
        catch (Error e) {
            msgList.putback(sid);
            new Event(Event.WARNING, "failed to load msg with packet " +
                "received from " + fromURI + ": "+ e.toString()).send();
            Event.flush(e);
        }
        if (ack) try {
            message.acknowledge();
        }
        catch (JMSException e) {
            String str = "";
            Exception ee = e.getLinkedException();
            if (ee != null)
                str += "Linked exception: " + ee.getMessage()+ "\n";
            new Event(Event.ERR, "failed to ack msg after talk to "+
                fromURI + ": " + str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to ack msg after talk to "+
                fromURI + ": " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            msgList.remove(sid);
            xq.remove(sid);
            new Event(Event.ERR, "failed to ack msg after talk to "+
                fromURI + ": " + e.toString()).send();
            Event.flush(e);
        }
        msgList.remove(sid);
        xq.remove(sid);
        if (displayMask > 0)
            new Event(Event.INFO, "inquired a packet of " + length +
                " bytes from " + fromURI + " (" + line + " )").send();
        return 1;
    }

    /**
     * It listens to an XQueue for JMS Messages and converts the content of
     * the message into a packet as a request.  Once receiving a message,
     * it loads its content to a DatagramPacket according to the format
     * rules and sends the packet to the DatagramSocket.  Meanwhile, it
     * listens to the DatagramSocket for reply packet and sends the reply
     * back to the XQueue if there is one.
     *<br/><br/>
     * Most of the IOExceptions from the packet IO operations, like send,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br/><br/>
     * Since the inquire operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the inquirey is successful.  This method will
     * not acknowledge messages.  It is up to the requester to do that.
     *<br/><br/>
     * This method is NOT MT-Safe.
     */
    public long inquire(XQueue xq, DatagramSocket sock) throws IOException,
        JMSException {
        Message inMessage;
        DatagramPacket p, packet = null;
        int aPort, length, m = 0, mask;
        int sid = -1;
        long count = 0;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String pURI = "", toURI = null, msgStr = null;
        byte[] buffer = new byte[bufferSize];

        if (pktList.size() > 0) { // reentry due to exception
            while ((sid = pktList.getNextID()) >= 0) {
                pktList.remove(sid);
                inMessage = (Message) xq.browse(sid);
                if (inMessage != null && ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
            }
        }

        packet = new DatagramPacket(new byte[bufferSize], 0, bufferSize);
        p = new DatagramPacket(buffer, 0, bufferSize);
        aPort = -1;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                count += receive(sock, xq, pktList, packet);
                expire(pktList, xq);
                break;
            }
            sid = xq.getNextCell(waitTime);
            if (sid < 0) {
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
                continue;
            }

            inMessage = (Message) xq.browse(sid);
            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
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
                    count += receive(sock, xq, pktList, packet);
                    if (m++ > 5)
                        m = expire(pktList, xq);
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                inMessage = null;
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
                continue;
            }

            toURI = null;
            try {
                toURI = MessageUtils.getProperty(uriField, inMessage);
                MessageUtils.setProperty(rcField, String.valueOf(sid),
                    inMessage);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get toURI for " +
                    uri).send();
                inMessage = null;
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
                continue;
            }
            if (toURI == null) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri +
                    ": failed to get toURI from the msg").send();
                inMessage = null;
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
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

            aPort = resetPacket(p, toURI);

            if (msgStr == null || aPort <= 0) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                if (msgStr == null)
                    new Event(Event.WARNING, uri +
                        ": unknown msg type to "+ toURI +
                        " and dropped the msg: " + msgStr).send();
                else
                    new Event(Event.WARNING, uri +
                        ": failed to reset packet on "+ toURI +
                        " and dropped the msg: " + msgStr).send();
                inMessage = null;
                count += receive(sock, xq, pktList, packet);
                if (m++ > 5)
                    m = expire(pktList, xq);
                continue;
            }

            length = msgStr.length();
            p.setData(msgStr.getBytes(), 0, length);
            try {
                sock.send(p);
            }
            catch (IOException e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                throw(e);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ee) {
                }
                xq.remove(sid);
                Event.flush(e);
            }

            pktList.reserve(sid);
            pktList.add(inMessage, sid);
            try {
                inMessage.clearProperties();
                inMessage.clearBody();
                MessageUtils.setProperty(rcField, String.valueOf(sid),
                    inMessage);
                inMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "inquire to " + toURI).send();
            }
            count += receive(sock, xq, pktList, packet);
            if (m++ > 5)
                m = expire(pktList, xq);

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "inquired of " + count + " msgs for " +
                uri).send();

        return count;
    }

    /**
     * It continually gets a JMS Message from the XQueue, loads its content
     * to a DatagramPacket according to the format rules and sends the packet
     * to the DatagramSocket.
     *<br/><br/>
     * Most of the IOExceptions from the packet IO operations, like send,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the socket or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the socket at all.
     *<br/><br/>
     * It will not modify the original messages.  If the XQueue has enabled
     * the EXTERNAL_XA, it will also acknowledge every messages.  This method
     * is MT-Safe.
     */
    public long talk(XQueue xq, DatagramSocket out) throws IOException,
        JMSException {
        Message inMessage;
        DatagramPacket packet = null;
        long count = 0;
        int aPort, length, mask;
        int sid = -1;
        int dmask = MessageUtils.SHOW_DATE;
        String pURI = "", toURI = null, msgStr = null, dt = null;
        boolean isSleepy = (sleepTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        byte[] buffer = new byte[bufferSize];

        dmask ^= displayMask;
        dmask &= displayMask;
        packet = new DatagramPacket(buffer, 0, bufferSize);
        aPort = -1;
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
            try {
                toURI = MessageUtils.getProperty(uriField, inMessage);
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
            if (toURI == null)
                toURI = uri.substring(6);

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(inMessage,buffer,template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (!toURI.equals(pURI)) {
                aPort = resetPacket(packet, toURI);
                pURI = toURI;
            }

            if (msgStr == null || aPort <= 0) {
                if (msgStr == null)
                    new Event(Event.WARNING, uri +
                        ": unknown msg type to "+ toURI +
                        " and dropped the msg: " + msgStr).send();
                else
                    new Event(Event.WARNING, uri +
                        ": failed to reset packet on "+ toURI +
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
            packet.setData(msgStr.getBytes(), 0, length);
            try {
                out.send(packet);
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
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "talked " + count + " msgs to " + uri).send();

        return count;
    }

    /**
     * It resets IP and port on the packet based on the cache info.  Key is
     * the URI in the format of IP:port. It returns the port or -1 on failure.
     */
    private synchronized int resetPacket(DatagramPacket packet, String key) {
        int i, port = -1;
        InetAddress address = null;
        if (cache.containsKey(key)) {
            address = (InetAddress) cache.get(key);
            port = cache.getMetaData(key)[1];
        }
        else if ((i = key.indexOf(":")) > 0) {
            try {
                address = InetAddress.getByName(key.substring(0, i));
                port = Integer.parseInt(key.substring(i+1));
            }
            catch (Exception e) {
                return -1;
            }
            cache.insert(key, System.currentTimeMillis(), 0,
               new int[] {cache.size(), port}, address);
        }
        if (port > 0 && address != null) {
            packet.setAddress(address);
            packet.setPort(port);
        }
        else
            port = -1;
        return port;
    }

    public String getOperation() {
        return operation;
    }
}
