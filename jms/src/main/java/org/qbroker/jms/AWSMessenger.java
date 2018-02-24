package org.qbroker.jms;

/* AWSMessenger.java - an AWS messenger for JMS messages */

import java.io.StringReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Connector;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.aws.AWSClient;
import org.qbroker.aws.STSClient;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.Event;

/**
 * AWSMessenger connects to an AWS service and converts the JMS messages into
 * AWS requests. Currently, it only supports the action of request.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class AWSMessenger {
    private AWSClient client;
    private int bufferSize = 4096;

    private String uri, saxParser;
    private Template template = null;

    private boolean isConnected = false;
    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private int resultType = Utils.RESULT_TEXT;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private Map<String, String> reqProperty = null;
    private String operation = "display";
    private String awsField, rcField, resultField;
    private long waitTime = 500L;
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private int sleepTime = 0;
    private int receiveTime = 1000;
    private int textMode = 1;
    private int xaMode = 0;
    private int[] partition;
    private int maxMsgLength = 4194304;
    private QuickCache cache = null;
    private Msg2Text msg2Text = null;

    /** Creates new AWSRequester */
    public AWSMessenger(Map props) {
        Object o;
        String scheme;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            URI u = new URI(uri);
            scheme = u.getScheme().toLowerCase();
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ("sts".equals(scheme))
            client = new STSClient(props);
        else try { // dynamic instantiation
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName("org.qbroker.aws." +
                scheme.toUpperCase() + "Client");
            con = cls.getConstructor(new Class[]{Map.class});
            client = (AWSClient) con.newInstance(new Object[]{props});
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            if (t == null)
                throw(new IllegalArgumentException(uri +
                    " failed to init AWSMessenger: " + e.toString()));
            else if (t instanceof IllegalArgumentException)
                throw((IllegalArgumentException) t);
            else
                throw(new IllegalArgumentException(uri +
                    " failed to init AWSMessenger: " + t.toString()));
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
            partition[0] = 0;
            partition[1] = 0;
        }

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

        if ((o = props.get("RequestProperty")) != null && o instanceof Map) {
            reqProperty = new HashMap<String, String>();
            for (Object obj : ((Map) o).keySet())
                if (obj != null)
                    reqProperty.put((String) obj, (String) ((Map) o).get(obj));
            if (reqProperty.size() <= 0)
                reqProperty = null;
        }

        if ((o = props.get("Operation")) != null)
            operation = (String) props.get("Operation");
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        if ((o = props.get("ResultType")) != null)
            resultType = Integer.parseInt((String) o);
        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberMessage")) != null)
            maxNumberMsg = Integer.parseInt((String) o);
        if ((o = props.get("MaxIdleTime")) != null) {
            maxIdleTime = 1000 * Integer.parseInt((String) o);
            if (maxIdleTime < 0)
                maxIdleTime = 0;
        }
        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        if ((o = props.get("WaitTime")) != null) {
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 500L;
        }
        if ((o = props.get("SleepTime")) != null)
            sleepTime= Integer.parseInt((String) o);

        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);

        cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 0);

        if ("display".equals(operation)) {
            if ((o = props.get("AWSField")) != null)
                awsField = (String) o;
            else
                awsField = "AWS";

            if ((o = props.get("RCField")) != null && o instanceof String)
                rcField = (String) o;
            else
                rcField = "ReturnCode";

            if ((o = props.get("ResultField")) != null && o instanceof String)
                resultField = (String) o;
            else
                resultField = "MsgCount";

            if ((o = props.get("Template")) != null &&
                ((String) o).length()>0)
                template = new Template((String) o);
        }
        else
            throw(new IllegalArgumentException(uri +
                ": operation is not supported for " + operation));

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the AWS
     * request from each message.  After the creating a list of AWS requests,
     * it sends each request to the AWS server as a query and waits
     * for the response back. The response will be loaded into body of the
     * request message according to the result type, similar to a DB qurey.
     */
    public void display(XQueue xq) throws IOException, JMSException,
        TimeoutException {
        Message outMessage, msg;
        Map<String, String> req = new HashMap<String, String>();
        String msgStr = null, line = null;
        int k, n, mask;
        int cid, sid = -1, heartbeat = 600000, ttl = 7200000;
        long currentTime, idleTime, tm, count = 0;
        StringBuffer strBuf;
        Map map;
        Msg2Text msg2Text = null;
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        byte[] buffer = new byte[bufferSize];

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        n = 0;
                        currentTime = System.currentTimeMillis();
                        if (currentTime - cache.getMTime() >= heartbeat) {
                            cache.disfragment(currentTime);
                            cache.setStatus(cache.getStatus(), currentTime);
                        }
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            if (!(outMessage instanceof TextMessage) &&
                !(outMessage instanceof BytesMessage)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "unsupported msg type from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            msgStr = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                msgStr = MessageUtils.getProperty(awsField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    if (template != null)
                        msgStr = MessageUtils.format(outMessage, buffer,
                            template);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }

            k = 0;
            strBuf = new StringBuffer();
            try {
                n = copyProperties(req, outMessage, buffer);
                Map resp = client.getResponse(req);
                strBuf.append(JSON2Map.toJSON(resp));
                k = 1;
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to copy properties for " +
                    uri + ": " + Event.traceStack(e)).send();
                continue;
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to get response from " +
                    uri + ": " + Event.traceStack(e)).send();
                continue;
            }

            if ((resultType & Utils.RESULT_XML) > 0) {
                strBuf.insert(0, "<Result>" + Utils.RS);
                strBuf.append("</Result>");
            }
            else if ((resultType & Utils.RESULT_JSON) > 0) {
                strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
                strBuf.append("]}");
            }

            try {
                outMessage.clearBody();
                if (outMessage instanceof TextMessage)
                    ((TextMessage) outMessage).setText(strBuf.toString());
                else {
                    line = strBuf.toString();
                    ((BytesMessage) outMessage).writeBytes(line.getBytes());
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(k),
                    outMessage);
                if (displayMask > 0)
                    line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
            }
            catch (JMSException e) {
                String str = msgStr;
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                new Event(Event.ERR, str + " failed to set property " +
                    Event.traceStack(e)).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) // display the message
                new Event(Event.INFO, (count+1) +":" + line).send();
            count ++;
            outMessage = null;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries").send();
    }

    private int copyProperties(Map<String, String> map, Message msg,
        byte[] buffer)
        throws JMSException {
        int n = 0;
        String str;
        map.clear();
        if (reqProperty != null) {
            for (String key : reqProperty.keySet()) {
                str = reqProperty.get(key);
                if ("body".equals(str))
                    str = MessageUtils.processBody(msg, buffer);
                else
                    str = MessageUtils.getProperty(reqProperty.get(key), msg);
                if (str == null || str.length() <= 0)
                    continue;
                map.put(key, str);
                n ++;
            }
        }
        else {
            String key;
            Enumeration propNames = msg.getPropertyNames();
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.length() <= 0)
                    continue;
                if (key.startsWith("JMS"))
                    continue;
                str = MessageUtils.getProperty(key, msg);
                if (str == null || str.length() <= 0)
                    continue;
                map.put(key, str);
                n ++;
            }
        }
        return n;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return true;
    }

    public void close() {
        isConnected = false;
        if (client != null)
            client.close();
        client = null;
    }
}
