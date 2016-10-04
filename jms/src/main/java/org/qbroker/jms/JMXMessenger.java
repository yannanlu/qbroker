package org.qbroker.jms;

/* JMXMessenger.java - a JMX messenger for JMS messages */

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.management.JMException;
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
import org.qbroker.net.JMXRequester;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.Event;

/**
 * JMXMessenger connects to a JMX server and converts the JMS messages into
 * JMX requests. The request retrieved from a message is supposed to be as
 * follows:
 *<br/>
 * ACTION Target attr0:attr1:attr2
 *<br/>
 * where the colon delimitered attributes are optional. If there is no
 * attribute defined, it will display all attributes if the target is not a
 * pattern. In case the target contains at least one wildcard chars, * or ?, it
 * will only list names of the MBeans.  Currently, it only supports the action
 * of display.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMXMessenger implements Connector {
    private int bufferSize = 4096;

    private String uri, saxParser;
    private Template template = null;

    private boolean isConnected = false;
    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private JMXRequester jmxReq = null;
    private int resultType = Utils.RESULT_TEXT;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String operation = "display";
    private String jmxField, rcField, resultField;
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

    /** Creates new JMXMessenger */
    public JMXMessenger(Map props) {
        Object o;
        Map<String, Object> ph;
        String scheme;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            URI u = new URI(uri);
            scheme = u.getScheme();
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        ph = new HashMap<String, Object>();
        ph.put("URI", uri);
        if ((o = props.get("Username")) != null) {
            ph.put("Username", o);
            ph.put("Password", props.get("Password"));
        }
        if ("service".equals(scheme))
            jmxReq = new JMXRequester(ph);
        else if (!"imq".equals(scheme))
            throw(new IllegalArgumentException("unsupported uri: " + uri));
        else try { // dynamic instantiation
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName("org.qbroker.jms.IMQRequester");
            con = cls.getConstructor(new Class[]{Map.class});
            jmxReq = (JMXRequester) con.newInstance(new Object[]{ph});
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            if (t == null)
                throw(new IllegalArgumentException(uri +
                    " failed to init IMQRequester: " + e.toString()));
            else if (t instanceof IllegalArgumentException)
                throw((IllegalArgumentException) t);
            else
                throw(new IllegalArgumentException(uri +
                    " failed to init IMQRequester: " + t.toString()));
        }
        ph.clear();

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
            if ((o = props.get("JMXField")) != null)
                jmxField = (String) o;
            else
                jmxField = "JMX";

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
     * It listens to an XQueue for incoming JMS messages and extracts the JMX
     * request from each message.  After the creating a list of JMX requests,
     * it sends each request to the JMX server as a query and waits
     * for the response back. The response will be loaded into body of the
     * request message according to the result type, similar to a DB qurey.
     *<br/><br/>
     * It also supports the dynamic content filter and the formatter on queried
     * messages. The filter and the formatter are defined via a JSON text with
     * Filters as the base name. The JSON text is stored in the body of the
     * request message. If the filter is defined, it will be used to select
     * certain messages based on the content and the header. If the formatter
     * is defined, it will be used to format the messages.
     */
    public void display(XQueue xq) throws JMException, JMSException,
        TimeoutException {
        Message outMessage, msg;
        String[] keys = null;
        String msgStr = null;
        String target, attrs, line = null;
        int k, n, mask;
        int cid, sid = -1, heartbeat = 600000, ttl = 7200000;
        long currentTime, idleTime, tm, count = 0;
        StringBuffer strBuf;
        Map map;
        Msg2Text msg2Text = null;
        MessageFilter[] filters = null;
        boolean withPattern = false;
        boolean withFilter = false;
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
                msgStr = MessageUtils.getProperty(jmxField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    if (template != null)
                        msgStr = MessageUtils.format(outMessage, buffer,
                            template);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            // get target and the attributes
            target = JMXRequester.getTarget(msgStr);
            if (target == null || target.length() <= 0) {
                // no target defined
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "bad JMX query from " + xq.getName()+
                    ": " + msgStr).send();
                outMessage = null;
                continue;
            }
            attrs = JMXRequester.getAttributes(msgStr);

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

            // get dynamic content filters
            msgStr = null;
            try { // retrieve dynamic content filters from body
                msgStr = MessageUtils.processBody(outMessage, buffer);
                if (msgStr == null || msgStr.length() <= 0)
                    filters = null;
                else if (msgStr.indexOf("<Filters>") < 0)
                    filters = null;
                else if (cache.containsKey(msgStr) &&
                    !cache.isExpired(msgStr, currentTime))
                    filters = (MessageFilter[]) cache.get(msgStr, currentTime);
                else { // new or expired
                    Map ph;
                    StringReader ins = new StringReader(msgStr);
                    ph = (Map) JSON2Map.parse(ins);
                    ins.close();
                    if (currentTime - cache.getMTime() >= heartbeat) {
                        cache.disfragment(currentTime);
                        cache.setStatus(cache.getStatus(), currentTime);
                    }
                    filters = MessageFilter.initFilters(ph);
                    if (filters != null && filters.length > 0)
                        cache.insert(msgStr, currentTime, ttl, null, filters);
                    else
                        filters = null;
                }
            }
            catch (Exception e) {
                filters = null;
                new Event(Event.WARNING, xq.getName() +
                    ": failed to retrieve content filters for "+target + ": " +
                    msgStr + ": " + Event.traceStack(e)).send();
            }

            // set checkers for dynamic content filters
            if (filters != null && filters.length > 0) {
                withFilter = true;
            }
            else {
                withFilter = false;
            }

            if (!isConnected) {
                String str;
                if ((str = reconnect()) != null) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new JMException("JMX connection failed on " + uri +
                        ": " + str));
                }
            }

            withPattern = false;
            keys = null;
            if (target.indexOf('*') < 0 && target.indexOf('?') < 0)
                keys = new String[]{target};
            else try { // for a pattern
                keys = jmxReq.list(target);
                if (keys == null)
                    keys = new String[0];
                withPattern = true;
            }
            catch (JMException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new JMException("failed to list on " + target + ": " +
                    Event.traceStack(e)));
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new JMSException("failed to process list request: "
                    + Event.traceStack(e)));
            }

            strBuf = new StringBuffer();
            if((attrs == null || attrs.length() <= 0) && withPattern){//for list
                k = 0;
                msg2Text = getFormatter("body", currentTime);
                for (String key : keys) {
                    if (withFilter) { // apply filters
                        int j;
                        for (j=0; j<filters.length; j++) {
                            if (filters[j].evaluate(key))
                                break;
                        }

                        if (j >= filters.length) // no hit
                            continue;
                        msg = new TextEvent(key);
                        if (filters[j].hasFormatter())
                            filters[j].format(msg, buffer);
                    }
                    else
                        msg = new TextEvent(key);
                    line = msg2Text.format(resultType, msg);
                    if (line != null && line.length() > 0) {
                        if ((resultType & Utils.RESULT_XML) > 0)
                            strBuf.append(line + Utils.RS);
                        else if ((resultType & Utils.RESULT_JSON) > 0) {
                            if (k > 0)
                                strBuf.append(",");
                            strBuf.append(line + Utils.RS);
                        }
                        else
                            strBuf.append(line + Utils.RS);
                        k ++;
                        if (maxNumberMsg > 0 && k >= maxNumberMsg)
                            break;
                    }
                }
            }
            else { // for a list or a specific MBean
                String[] a;
                if (attrs != null && attrs.length() > 0) { // for a list
                    msg2Text = getFormatter(attrs, currentTime);
                    a = (attrs.indexOf(':') < 0) ? new String[]{attrs} :
                        Utils.split(":", attrs);
                }
                else try { // for a specific MBean
                    a = jmxReq.getAllAttributes(target);
                    msg2Text = getFormatter(a, target, currentTime);
                }
                catch (JMException e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new JMException("failed to get info for " + target +
                        ": " + Event.traceStack(e)));
                }

                k = 0;
                try {
                    for (String key : keys) {
                        if (withFilter) { // apply filters on keys first
                            int j;
                            for (j=0; j<filters.length; j++) {
                                if (filters[j].evaluate(key))
                                    break;
                            }

                            if (j >= filters.length) // no hit
                                continue;
                        }
                        map = jmxReq.getValues(key, a);
                        msg = MessageUtils.convert(map, key);
                        if (withFilter) { // apply filters
                            int j;
                            for (j=0; j<filters.length; j++) {
                                if (filters[j].evaluate(msg, key)) {
                                    if (filters[j].hasFormatter())
                                        filters[j].format(msg, buffer);
                                    break;
                                }
                            }

                            if (j >= filters.length) // no hit
                                continue;
                        }
                        line = msg2Text.format(resultType, msg);
                        if (line != null && line.length() > 0) {
                            if ((resultType & Utils.RESULT_XML) > 0)
                                strBuf.append(line + Utils.RS);
                            else if ((resultType & Utils.RESULT_JSON) > 0) {
                                if (k > 0)
                                    strBuf.append(",");
                                strBuf.append(line + Utils.RS);
                            }
                            else
                                strBuf.append(line + Utils.RS);
                            k ++;
                            if (maxNumberMsg > 0 && k >= maxNumberMsg)
                                break;
                        }
                    }
                }
                catch (JMException e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new JMException("failed to get attributes for " +
                        keys[k] + ": " + Event.traceStack(e)));
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to format msg " + k + " for " +
                        keys[k] + ": " + Event.traceStack(e)).send();
                }
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
                String str = target;
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

    /**
     * returns the initialized formatter for a given target and its attributes
     */
    private Msg2Text getFormatter(String[] a, String target, long currentTime) {
        int ttl = 7200000;
        if (target == null || target.length() <= 0)
            return null;
        if (cache.containsKey(target) && !cache.isExpired(target, currentTime))
            return (Msg2Text) cache.get(target, currentTime);
        if (a == null || a.length <= 0 || a[0] == null || a[0].length() <= 0)
            return null;
        cache.insert(target, currentTime, ttl, null, initFormatter(a));
        return (Msg2Text) cache.get(target, currentTime);
    }

    /** returns the initialized formatter for a given attribute string  */
    private Msg2Text getFormatter(String attrs, long currentTime) {
        int ttl = 7200000;
        if (attrs == null || attrs.length() <= 0)
            return null;
        if (cache.containsKey(attrs) && !cache.isExpired(attrs, currentTime))
            return (Msg2Text) cache.get(attrs, currentTime);
        if (attrs.indexOf(':') < 0)
            cache.insert(attrs, currentTime, ttl, null,
                initFormatter(new String[]{attrs}));
        else
            cache.insert(attrs, currentTime, ttl, null,
                initFormatter(Utils.split(":", attrs)));
        return (Msg2Text) cache.get(attrs, currentTime);
    }

    /** returns the initialized formatter for a given attribute list  */
    private Msg2Text initFormatter(String[] attrs) {
        int i, n;
        String key;
        StringBuffer strBuf;
        Map<String, Object> ph;

        if (attrs == null || attrs.length <= 0)
            return null;

        strBuf = new StringBuffer();
        n = attrs.length;
        for (i=0; i<n; i++) {
            key = attrs[i];
            if (key == null || key.length() <= 0)
                continue;
            strBuf.append("##" + key + "## ");
        }

        ph = new HashMap<String, Object>();
        ph.put("Name", uri);
        ph.put("Template", strBuf.toString());
        ph.put("BaseTag", "Record");

        return new Msg2Text(ph);
    }

    /** reconnects and returns null upon sucess or error msg otherwise */
    public String reconnect() {
        isConnected = false;
        if (jmxReq != null) {
            String str = jmxReq.reconnect();
            if (str == null)
                isConnected = true;
            return str;
        }
        else
            return "it is closed";
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void close() {
        if (cache != null)
            cache.clear();
        if (jmxReq != null)
            jmxReq.close();
        jmxReq = null;
        isConnected = false;
    }
}
