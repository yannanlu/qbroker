package org.qbroker.sonicmq;

/* SonicMQMessenger.java - a SonicMQ messenger for JMS/JMX messages */

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
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
import javax.management.Notification;
import javax.management.NotificationListener;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Utils;
import org.qbroker.common.XML2Map;
import org.qbroker.net.JMXRequester;
import org.qbroker.sonicmq.SonicMQRequester;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.Event;

/**
 * SonicMQMessenger connects to a SonicMQ broker and converts the JMS messages
 * into JMX requests. The request retrieved from a message is supposed to be
 * as follows:
 *<br/>
 * ACTION Target attr0:attr1:attr2
 *<br/>
 * where the colon delimited attributes are optional. If there is no
 * attribute defined, it will display all attributes.
 * Currently, it only supports the action of display and listen.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SonicMQMessenger extends SonicMQRequester {
    private int bufferSize = 4096;

    private Template template = null;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private int resultType = Utils.RESULT_TEXT;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String operation = "display";
    private String jmxField, rcField, resultField;
    private String lsnrTarget = null;
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
    private XML2Map xmlReader = null;
    private Msg2Text msg2Text = null;
    private XQueue notice = null;

    /** Creates new SonicMQFMessenger */
    public SonicMQMessenger(Map props) {
        super(props);
        Object o;
        Map<String, Object> ph;

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

        if ("display".equals(operation)) { // for JMX metrics or data
            String saxParser = null;
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

            if ((o = props.get("SAXParser")) != null)
                saxParser = (String) o;
            if (saxParser == null)
                saxParser=(String)System.getProperty("org.xml.sax.driver",null);
            if (saxParser == null)
                saxParser = "org.apache.xerces.parsers.SAXParser";

            try {
                xmlReader = new XML2Map(saxParser);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(uri +
                    " failed to init xmlReader: " + e.toString()));
            }
        }
        else if ("listen".equals(operation)) { // for notifications
            if ((o = props.get("Target")) != null)
                lsnrTarget = (String) o;
            else
                throw(new IllegalArgumentException(
                    "target for listener is not defined for "+ uri));
        }
        else
            throw(new IllegalArgumentException(uri +
                ": operation is not supported for " + operation));

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It adds a notification listener to the SonicMQ broker and saves the
     * XQueue instance as notice for the downstream. The plugin method of
     * onNotification converts the notifications into JMS TextMessages and
     * puts them into notice. Since the method is called by the broker, it
     * is passive or asynchronous in the nature. Therefore, it will ignore
     * the actions of flow controls. In case the XQueue is on PAUSE state,
     * the messages may still flow into it. Once the XQueue is full, the
     * extra notifications will be dropped. So please set fairly large
     * capacity for the XQueue of notice.
     */
    public void listen(XQueue xq) throws JMException {
        NotificationListener listener = null;
        if (notice == null)
            notice = xq;
        else if (notice != xq)
            notice = xq;

        listener = addListener(lsnrTarget, this.getClass().getName(),
            "onNotification", this);
        if (listener != null)
            new Event(Event.INFO, "listener has been added on " +
                lsnrTarget + " for " + uri).send();
        else
            throw(new JMException("failed to add listener on " + lsnrTarget +
                " for " + uri));

        int mask;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            try { // wait for interruption
                Thread.sleep(1000L);
            }
            catch (InterruptedException e) {
            }
        }
        try {
            removeListener(listener);
        }
        catch (Exception e) {
        }
        new Event(Event.INFO, "listener has been removed on " +
            lsnrTarget + " for " + uri).send();
    }

    @SuppressWarnings("unchecked")
    public void onNotification(Notification notification) {
        XQueue xq = notice;
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0) {
            int i, sid = -1;
            int shift = partition[0];
            int len = partition[1];
            long tm = notification.getTimeStamp();
            Map attrs = getAttributes(notification); 
            attrs.put("Type", notification.getType().toString());
            attrs.put("Source", notification.getSource().toString());
            Message msg = MessageUtils.convert(attrs, lsnrTarget);
            switch (len) {
              case 0:
                for (i=0; i<1000; i++) { // reserve an empty cell
                    sid = xq.reserve(waitTime);
                    if (sid >= 0)
                        break;
                }
                break;
              case 1:
                for (i=0; i<1000; i++) { // reserve the empty cell
                    sid = xq.reserve(waitTime, shift);
                    if (sid >= 0)
                        break;
                }
                break;
              default:
                for (i=0; i<1000; i++) { // reserve a partitioned empty cell
                    sid = xq.reserve(waitTime, shift, len);
                    if (sid >= 0)
                        break;
                }
                break;
            }
            if (sid < 0) {
                new Event(Event.ERR, "failed to reserve a cell on " +
                    xq.getName() + " for dropped event from " + uri).send();
            }
            else {
                i = xq.add(msg, sid);
            }
        }
        else { // xq stopped
            new Event(Event.WARNING, xq.getName() + " is stopped. " +
                "Please remove the listener for " + uri).send();
        }
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the JMX
     * request from each message.  After the creating a list of JMX requests,
     * it sends each request to the SonicMQ broker as a query and waits
     * for the response back. The response will be loaded into body of the
     * request message according to the result type, similar to a DB qurey.
     *<br/><br/>
     * It also supports the dynamic content filter and the formatter on queried
     * messages. The filter and the formatter are defined via an XML text with
     * Filters as the base tag. The XML text is stored in the body of the
     * request message. If the filter is defined, it will be used to select
     * certain messages based on the content and the header. If the formatter
     * is defined, it will be used to format the messages.
     */
    public void display(XQueue xq) throws JMException, JMSException,
        TimeoutException {
        Message outMessage, msg;
        String msgStr = null;
        String target, attrs, line = null;
        int k, n, mask;
        int cid, sid = -1, heartbeat = 600000, ttl = 7200000;
        long currentTime, idleTime, tm, count = 0;
        StringBuffer strBuf;
        Msg2Text msg2Text = null;
        MessageFilter[] filters = null;
        boolean isBrowse = false;
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
                    ph = (Map) xmlReader.getMap(ins).get("Filters");
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

            if (!isConnected()) {
                String str;
                if ((str = reconnect()) != null) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new JMException("SonicMQ connection failed on " +uri+
                        ": " + str));
                }
            }

            String[] keys = null;
            if (target.indexOf(",category=") < 0) { // not supported yet
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, xq.getName() +
                    " illegal target: "+ target + " on " + uri).send();
                continue;
            }
            else if (target.indexOf(",category=topic") > 0) { // for a browse
                keys = new String[]{target};
                withPattern = false;
                isBrowse = true;
            }
            else if (target.indexOf('*') < 0) { // for a query
                keys = new String[]{target};
                withPattern = false;
                isBrowse = false;
            }
            else try { // for a list with pattern
                keys = list(target, attrs);
                if (keys == null)
                    keys = new String[0];
                withPattern = true;
                isBrowse = false;
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
            if (withPattern) { // for a list
                k = 0;
                msg2Text = getFormatter("body", currentTime);
                try {
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
                catch (Exception e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to format msg " + k + " for " +
                        target + ": " + Event.traceStack(e)).send();
                    continue;
                }
            }
            else { // for a query
                List list;
                int limit = (maxNumberMsg > 0) ? maxNumberMsg : 1000;
                msg2Text = getFormatter("", currentTime);
                try {
                    if (isBrowse) // for browse on a durable subscription
                        list = browse(target, limit, true);
                    else if (attrs == null || attrs.length() <= 0) // select all
                        list = query(target, null);
                    else if (attrs.indexOf(':') > 0) { // select many
                        attrs = attrs.replace(':', '|');
                        list = query(target, ".*(" + attrs + ")$");
                    }
                    else { // select one
                        list = query(target, ".*" + attrs + "$");
                    }
                }
                catch (JMException e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new JMException("failed to query on " + target +
                        " from " + uri + ": " + Event.traceStack(e)));
                }
                catch (Exception e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to query on " + target +
                        " from " + uri + ": " + Event.traceStack(e)).send();
                    continue;
                }

                k = 0;
                try {
                    for (Object o : list) {
                        if (isBrowse)
                            msg = (Message) o;
                        else
                            msg = MessageUtils.convert((Map) o, target);
                        if (msg == null)
                            continue;
                        if (withFilter) { // apply filters
                            int j;
                            for (j=0; j<filters.length; j++) {
                                if (filters[j].evaluate(msg, target)) {
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
                catch (Exception e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to format msg " + k + " for " +
                        target + ": " + Event.traceStack(e)).send();
                    continue;
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

    /** returns the initialized formatter for a given attribute string  */
    private Msg2Text getFormatter(String attrs, long currentTime) {
        int ttl = 7200000;
        if (attrs == null)
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
        StringBuffer strBuf = new StringBuffer();
        Map<String, Object> ph = new HashMap<String, Object>();

        if (attrs != null && attrs.length > 0) {
            int i, n;
            String key;
            n = attrs.length;
            for (i=0; i<n; i++) {
                key = attrs[i];
                if (key == null || key.length() <= 0)
                    continue;
                strBuf.append("##" + key + "## ");
            }
        }

        ph.put("Name", uri);
        ph.put("BaseTag", "Record");
        ph.put("Template", strBuf.toString());

        return new Msg2Text(ph);
    }

    public void close() {
        if (cache != null)
            cache.clear();
        super.close();
    }
}
