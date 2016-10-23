package org.qbroker.wmq;

/* PCFRequester.java - a requester to send PCF Messages to MQSeries */

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQSecurityExit;
import com.ibm.mq.MQException;
import com.ibm.mq.pcf.CMQC;
import com.ibm.mq.pcf.CMQCFC;
import com.ibm.mq.pcf.PCFParameter;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.management.JMException;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.WrapperException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Requester;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Filter;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.Event;

/**
 * PCFRequester connects to the admin service of a WebSphere MQ queue manager
 * for PCF requests. Currently, it supports the method of display only.
 * This is NOT MT-Safe due to IBM's implementation on sessions and conn.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PCFRequester implements Requester {
    private int bufferSize = 4096;

    private int port = 1414;
    private String hostName;
    private String channelName;
    private String qmgrName;
    private String uri, saxParser;
    private Template template = null;
    private MQQueueManager qmgr = null;
    private PCFMessageAgent pcfMsgAgent = null;

    private boolean isConnected = false;
    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private int resultType = Utils.RESULT_JSON;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String operation = "display";
    private String securityExit = null;
    private String securityData = null;
    private String username = null;
    private String password = null;
    private String pcfField, rcField, resultField;
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

    private Map<String, String> qlMap = null;
    private int[] qlAttrs = null;
    private Msg2Text qlMsg2Text = null;
    private final static String[][] qlTable = {
        {"CMQC.MQCA_Q_NAME", "QUEUE"},
        {"CMQC.MQIA_Q_TYPE", "TYPE"},
        {"CMQC.MQIA_OPEN_INPUT_COUNT", "IPPROCS"},
        {"CMQC.MQIA_OPEN_OUTPUT_COUNT", "OPPROCS"},
        {"CMQC.MQIA_CURRENT_Q_DEPTH", "CURDEPTH"},
        {"CMQC.MQIA_MAX_Q_DEPTH", "MAXDEPTH"},
        {"CMQC.MQIA_USAGE", "USAGE"},
        {"CMQC.MQCA_INITIATION_Q_NAME", "INITQ"},
        {"CMQC.MQCA_ALTERATION_DATE", "ALTDATE"},
        {"CMQC.MQCA_ALTERATION_TIME", "ALTTIME"},
        {"CMQC.MQCA_Q_DESC", "DESCR"}
    };

    private Map<String, String> qaMap = null;
    private int[] qaAttrs = null;
    private Msg2Text qaMsg2Text = null;
    private final static String[][] qaTable = {
        {"CMQC.MQCA_Q_NAME", "QUEUE"},
        {"CMQC.MQIA_Q_TYPE", "TYPE"},
        {"CMQC.MQCA_BASE_Q_NAME", "TARGQ"},
        {"CMQC.MQCA_ALTERATION_DATE", "ALTDATE"},
        {"CMQC.MQCA_ALTERATION_TIME", "ALTTIME"},
        {"CMQC.MQCA_Q_DESC", "DESCR"}
    };

    private Map<String, String> qrMap = null;
    private int[] qrAttrs = null;
    private Msg2Text qrMsg2Text = null;
    private final static String[][] qrTable = {
        {"CMQC.MQCA_Q_NAME", "QUEUE"},
        {"CMQC.MQIA_Q_TYPE", "TYPE"},
        {"CMQC.MQCA_XMIT_Q_NAME", "XMITQ"},
        {"CMQC.MQCA_REMOTE_Q_NAME", "RNAME"},
        {"CMQC.MQCA_REMOTE_Q_MGR_NAME", "RQMNAME"},
        {"CMQC.MQCA_ALTERATION_DATE", "ALTDATE"},
        {"CMQC.MQCA_ALTERATION_TIME", "ALTTIME"},
        {"CMQC.MQCA_Q_DESC", "DESCR"}
    };

    private Map<String, String> qsMap = null;
    private int[] qsAttrs = null;
    private Msg2Text qsMsg2Text = null;
    private final static String[][] qsTable = {
        {"CMQC.MQCA_Q_NAME", "QUEUE"},
        {"CMQCFC.MQIACF_Q_STATUS_TYPE", "TYPE"},
        {"CMQC.MQIA_OPEN_INPUT_COUNT", "IPPROCS"},
        {"CMQC.MQIA_OPEN_OUTPUT_COUNT", "OPPROCS"},
        {"CMQC.MQIA_CURRENT_Q_DEPTH", "CURDEPTH"}
    };

    private Map<String, String> qhMap = null;
    private int[] qhAttrs = null;
    private Msg2Text qhMsg2Text = null;
    private final static String[][] qhTable = {
        {"CMQC.MQCA_Q_NAME", "QUEUE"},
        {"CMQCFC.MQIACF_Q_STATUS_TYPE", "TYPE"},
        {"CMQCFC.MQCACH_CONNECTION_NAME", "CONNAME"},
        {"CMQCFC.MQCACH_CHANNEL_NAME", "CHANNEL"},
        {"CMQCFC.MQIACF_PROCESS_ID", "PID"},
        {"CMQCFC.MQIACF_THREAD_ID", "TID"},
        {"CMQCFC.MQIACF_OPEN_TYPE", "URTYPE"},
        {"CMQCFC.MQIACF_OPEN_INPUT_TYPE", "INPUT"},
        {"CMQCFC.MQIACF_OPEN_OUTPUT", "OUTPUT"},
        {"CMQCFC.MQCACF_APPL_TAG", "APPTAG"},
        {"CMQCFC.MQCACF_USER_IDENTIFIER", "USERID"}
    };

    private Map<String, String> chlMap = null;
    private int[] chlAttrs = null;
    private Msg2Text chlMsg2Text = null;
    private final static String[][] chlTable = {
        {"CMQCFC.MQCACH_CHANNEL_NAME", "CHANNEL"},
        {"CMQCFC.MQIACH_CHANNEL_TYPE", "TYPE"},
        {"CMQCFC.MQCACH_CONNECTION_NAME", "CONNAME"},
        {"CMQCFC.MQCACH_XMIT_Q_NAME", "XMITQ"},
        {"CMQCFC.MQCACH_DESC", "DESCR"}
    };

    private Map<String, String> chsMap = null;
    private int[] chsAttrs = null;
    private Msg2Text chsMsg2Text = null;
    private final static String[][] chsTable = {
        {"CMQCFC.MQCACH_CHANNEL_NAME", "CHANNEL"},
        {"CMQCFC.MQIACH_CHANNEL_TYPE", "TYPE"},
        {"CMQCFC.MQIACH_CHANNEL_STATUS", "STATUS"},
        {"CMQCFC.MQCACH_CONNECTION_NAME", "CONNAME"},
        {"CMQCFC.MQIACH_MSGS", "MSGS"},
        {"CMQCFC.MQCACH_LAST_MSG_DATE", "LSTMSGDA"},
        {"CMQCFC.MQCACH_LAST_MSG_TIME", "LSTMSGTI"},
        {"CMQCFC.MQCACH_CHANNEL_START_DATE", "CHSTADA"},
        {"CMQCFC.MQCACH_CHANNEL_START_TIME", "CHSTATI"}
    };

    /** Creates new PCFRequester */
    public PCFRequester(Map props) {
        boolean ignoreInitFailure = false;
        Object o;

        hostName = (String) props.get("HostName");
        qmgrName = (String) props.get("QueueManager");
        uri = (String) props.get("URI");
        if (hostName != null && hostName.length() > 0) {
            uri = "pcf://" + hostName;
            if ((o = props.get("Port")) != null)
                port = Integer.parseInt((String) o);
            uri += ":" + port;
        }
        else if (qmgrName != null && qmgrName.length() > 0)
            uri = "pcf:///" + qmgrName;
        else if (uri == null || uri.length() <= 5)
            throw(new IllegalArgumentException(
                "HostName or QueueManager not defined"));
        else try {
            URI u = new URI(uri);
            hostName = u.getHost();
            if (hostName == null || hostName.length() <= 0) {
                qmgrName = u.getPath();
                if (qmgrName != null && qmgrName.length() > 1)
                    qmgrName = qmgrName.substring(1);
                else
                    throw(new IllegalArgumentException(
                        "HostName or QueueManager not defined"));
            }
            else if (u.getPort() > 0)
                port = u.getPort();
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        channelName = (String) props.get("ChannelName");
        if (channelName == null)
            channelName = "SYSTEM.DEF.SVRCONN";

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
            Template temp = new Template("##CellID##");
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
                    value = temp.substitute("CellID", cellID, value);
                    propertyValue[n] = MonitorUtils.substitute(value, null);
                }
                n ++;
            }
        }

        if ((o = props.get("SecurityExit")) != null)
            securityExit = (String) o;
        if ((o = props.get("SecurityData")) != null)
            securityData = (String) o;
        if ((o = props.get("Username")) != null)
            username = (String) o;
        if ((o = props.get("Password")) != null)
            password = (String) o;
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
        if ((o = props.get("IgnoreInitFailure")) != null &&
            "true".equalsIgnoreCase((String) o))
            ignoreInitFailure = true;

        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);

        cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 0);
        if ("display".equals(operation)) {
            if ((o = props.get("PCFField")) != null)
                pcfField = (String) o;
            else
                pcfField = "PCF";

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

        // init maps, attrs and formatter for supported targets
        qlMap = initMap(qlTable);
        qlAttrs = initAttrs(qlTable);
        qlMsg2Text = initFormatter(qlAttrs, qlMap);
        qaMap = initMap(qaTable);
        qaAttrs = initAttrs(qaTable);
        qaMsg2Text = initFormatter(qaAttrs, qaMap);
        qrMap = initMap(qrTable);
        qrAttrs = initAttrs(qrTable);
        qrMsg2Text = initFormatter(qrAttrs, qrMap);
        qsMap = initMap(qsTable);
        qsAttrs = initAttrs(qsTable);
        qsMsg2Text = initFormatter(qsAttrs, qsMap);
        qhMap = initMap(qhTable);
        qhAttrs = initAttrs(qhTable);
        qhMsg2Text = initFormatter(qhAttrs, qhMap);
        chlMap = initMap(chlTable);
        chlAttrs = initAttrs(chlTable);
        chlMsg2Text = initFormatter(chlAttrs, chlMap);
        chsMap = initMap(chsTable);
        chsAttrs = initAttrs(chsTable);
        chsMsg2Text = initFormatter(chsAttrs, chsMap);

        try {
            pcfMsgAgent = connect();
        }
        catch (Exception e) {
            if (ignoreInitFailure)
                new Event(Event.ERR, "failed to connect to " + uri +
                    ": " + Event.traceStack(e)).send();
            else
                throw(new IllegalArgumentException("failed to connect to " +
                    uri + ": " + Event.traceStack(e)));
        }
        if (pcfMsgAgent != null)
            isConnected = true;
    }

    private PCFMessageAgent connect() throws MQException {
        PCFMessageAgent pcf = null;

        if (hostName == null) { // bind mode
            qmgr = null;
            pcf = new PCFMessageAgent(qmgrName);
        }
        else if (securityExit == null) { // client mode without securityExit
            qmgr = null;
            pcf = new PCFMessageAgent(hostName, port, channelName);
        }
        else { // client mode with securityExit
            try {
                Class<?> cls = Class.forName(securityExit);
                java.lang.reflect.Constructor con =
             cls.getConstructor(new Class[]{Class.forName("java.lang.String")});
                MQEnvironment.securityExit =
                    (MQSecurityExit)con.newInstance(new Object[]{securityData});
            }
            catch (Exception ex) {
                throw(new IllegalArgumentException(" failed to " +
                    "instantiate "+securityExit+": "+Event.traceStack(ex)));
            }
            MQEnvironment.hostname = hostName;
            MQEnvironment.port = port;
            MQEnvironment.channel = channelName;
            if (password != null)
                MQEnvironment.password = password;
            if (username != null)
                MQEnvironment.userID = username;
            qmgr = new MQQueueManager("");
            pcf = new PCFMessageAgent(qmgr);
        }

        return pcf;
    }

    private void disconnect() {
        if (pcfMsgAgent != null) try {
            pcfMsgAgent.disconnect();
        }
        catch (Exception e) {
        }
        pcfMsgAgent = null;
        if (qmgr != null) try {
            qmgr.disconnect();
        }
        catch (Exception e) {
        }
        qmgr = null;
        isConnected = false;
    }

    /** reconnects and returns null upon success or error msg otherwise */
    public String reconnect() {
        disconnect();
        try {
            pcfMsgAgent = connect();
            if (pcfMsgAgent != null)
               isConnected = true;
        }
        catch (Exception e) {
            String str = "failed to connect to " + uri + ": ";
            return str + Event.traceStack(e);
        }
        return null;
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the PCF
     * request from each message.  After the creating a PCF Message for the
     * request, it sends the message to the queue manager as a query and waits
     * for the response back. The response will be loaded into body of the
     * request message according to the result type, similar to a DB qurey.
     *<br/><br/>
     * It also supports the dynamic content filter and the formatter on queried
     * messages. The filter and the formatter are defined via a JSON text with
     * Ruleset defined as a list. The JSON text is stored in the body of the
     * request message. If the filter is defined, it will be used to select
     * certain messages based on the content and the header. If the formatter
     * is defined, it will be used to format the messages.
     */
    public void display(XQueue xq) throws JMException, TimeoutException {
        Message outMessage;
        String msgStr = null, line = null, key, category, type;
        int k = -1, n, mask, cid, tid, rc, reason;
        int sid = -1, heartbeat = 600000, ttl = 7200000;
        long currentTime, idleTime, tm, count = 0;
        StringBuffer strBuf;
        PCFMessage request = null;
        PCFMessage[] responses = null;
        MessageFilter[] filters = null;
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
                    new Event(Event.ERR, "failed to set RC on a msg from "+
                        xq.getName() + " for " + uri).send();
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
                new Event(Event.ERR, "failed to set RC on a msg from " +
                    xq.getName() + " for " + uri).send();
                outMessage = null;
                continue;
            }

            if (!(outMessage instanceof TextMessage) &&
                !(outMessage instanceof BytesMessage)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "unsupported msg type from " +
                    xq.getName() + " for " + uri).send();
                outMessage = null;
                continue;
            }

            line = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                line = MessageUtils.getProperty(pcfField, outMessage);
                if (line == null || line.length() <= 0) {
                    if (template != null)
                        line = MessageUtils.format(outMessage, buffer,
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
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "message expired from " + xq.getName() +
                    " for " + uri + ": "+Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }

            // get target and the type
            key = getTarget(msgStr);
            category = getCategory(msgStr);
            cid = getCategoryID(category);
            if (cid < 0 || key == null || key.length() <= 0) {
                // no target defined
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "bad PCF command from " + xq.getName()+
                    " for " + uri + ": " + msgStr).send();
                outMessage = null;
                continue;
            }
            type = getType(msgStr);
            tid = getTypeID(cid, type);

            // get dynamic content filters
            msgStr = null;
            try { // retrieve dynamic content filter from body
                msgStr = MessageUtils.processBody(outMessage, buffer);
                if (msgStr == null || msgStr.length() <= 0)
                    filters = null;
                else if (msgStr.indexOf("Ruleset") < 0)
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
                new Event(Event.WARNING,
                    "failed to retrieve content filters for request '" + line +
                    "' from "+ xq.getName() + " for " + uri + " out of " +
                    msgStr + ": " + Event.traceStack(e)).send();
            }

            // set checkers for dynamic content filters
            if (filters != null && filters.length > 0) {
                withFilter = true;
            }
            else {
                withFilter = false;
            }

            if (!isConnected) try {
                pcfMsgAgent = connect();
                if (pcfMsgAgent != null)
                    isConnected = true;
            }
            catch (MQException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (e.completionCode == MQException.MQCC_FAILED &&
                    e.reasonCode == CMQC.MQRC_OBJECT_ALREADY_EXISTS) {
                    // object already exists
                    disconnect();
                    pcfMsgAgent = null;
                }
                else
                    disconnect();
                throw(new JMException("PCF connection failed: " +
                    Event.traceStack(e)));
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                disconnect();
                throw(new JMException("MQ connection failed: " +
                    Event.traceStack(e)));
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                disconnect();
                new Event(Event.ERR, "failed to connect MQ server on " + uri +
                    " for a msg from " + xq.getName() + " with " + key + ": " +
                    e.toString()).send();
                Event.flush(e);
            }

            request = getPCFMessage(cid, tid, key);

            try { // send the request
                responses = pcfMsgAgent.send(request);
            }
            catch (MQException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                reason = e.reasonCode;
                rc = e.completionCode;
                if (rc == MQException.MQCC_FAILED && (
                    reason == CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND ||
                    reason == CMQCFC.MQRCCF_CHANNEL_NOT_FOUND ||
                    reason == CMQCFC.MQRCCF_Q_STATUS_NOT_FOUND ||
                    reason == MQException.MQRC_SELECTOR_ERROR ||
                    reason == MQException.MQRC_UNKNOWN_OBJECT_NAME)) {
                    new Event(Event.ERR, "failed to query on " + key + " for "+
                        category + " with " + rc + "/" + reason +
                        " from " + xq.getName() + " to " + uri).send();
                    continue;
                }
                else { // fatal error
                    disconnect();
                    throw(new JMException("failed to get PCF responses: " +
                        Event.traceStack(e)));
                }
            }
            catch (IOException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                disconnect();
                throw(new JMException("MQ PCF send failed: " +
                    Event.traceStack(e)));
            }

            rc = responses[0].getCompCode();
            reason = responses[0].getReason();
            // Check the PCF header (MQCFH) in the first response message
            if (reason != 0 || rc != 0) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to get response on " + key +
                    " for " + category + " with " + rc + "/" + reason +
                    " from " + xq.getName() + " to " + uri).send();
                outMessage = null;
                continue;
            }

            Map map = getMap(cid, tid);
            Msg2Text msg2Text = getFormatter(cid, tid);
            strBuf = new StringBuffer();
            k = 0;
            try {
                for (PCFMessage resp : responses) {
                    Message msg = convert(resp, map);
                    if (msg == null)
                        continue;

                    if (withFilter) { // apply filters
                        int j;
                        for (j=0; j<filters.length; j++) {
                            if (filters[j].evaluate(msg, null)) {
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
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to format msg " + k + " for " +
                    key + " from " + xq.getName() + " to " + uri + ": "+
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to format msg " + k + " for " +
                    key + " from " + xq.getName() + " to " + uri + ": "+
                    Event.traceStack(e)).send();
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
                String str = "";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                new Event(Event.WARNING, "failed to set properties on a msg " +
                    "from " + xq.getName() + " to " + uri + ": " + str +
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
     * This sends a request to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection.
     *<br/></br/>
     * In case of a fatal failure, it disconnects first and then throws a
     * WrapperException with cause set to the vendor specific exception or IO
     * related exception. Therefore please make sure to get the cause exception
     * from it and handles the cause exception with care. For other failures,
     * it throws an IllegalArgumentException. If the disconnect flag is set to
     * true, it disconnects right before it throws the exception.
     */
    public int getResponse(String pcfCmd, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        PCFMessage[] responses = null;
        PCFMessage request = null;
        String key, type, category, target, line = null;
        int k = -1, cid, tid, rc, reason;

        if (pcfCmd == null || pcfCmd.length() <= 0)
            throw(new IllegalArgumentException("empty request for " + uri));

        // get target and the type
        key = getTarget(pcfCmd);
        category = getCategory(pcfCmd);
        cid = getCategoryID(category);
        if (cid < 0 || key == null || key.length() <= 0)
            throw(new IllegalArgumentException("bad request of " + pcfCmd +
                " for " + uri));
        type = getType(pcfCmd);
        tid = getTypeID(cid, type);

        if (!isConnected) try {
            pcfMsgAgent = connect();
            if (pcfMsgAgent != null)
                isConnected = true;
        }
        catch (MQException e) {
            if (e.completionCode == MQException.MQCC_FAILED &&
                e.reasonCode == CMQC.MQRC_OBJECT_ALREADY_EXISTS) {
                // object already exists
                disconnect();
                pcfMsgAgent = null;
            }
            else
                disconnect();
            throw(new WrapperException("PCF connection failed", e));
        }
        catch (Exception e) {
            disconnect();
            throw(new WrapperException("MQ connection failed", e));
        }
        catch (Error e) {
            disconnect();
            throw(e);
        }

        request = getPCFMessage(cid, tid, key);

        try { // send the request
            responses = pcfMsgAgent.send(request);
        }
        catch (MQException e) {
            reason = e.reasonCode;
            rc = e.completionCode;
            if (rc == MQException.MQCC_FAILED && (
                reason == CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND ||
                reason == CMQCFC.MQRCCF_CHANNEL_NOT_FOUND ||
                reason == CMQCFC.MQRCCF_Q_STATUS_NOT_FOUND ||
                reason == MQException.MQRC_SELECTOR_ERROR ||
                reason == MQException.MQRC_UNKNOWN_OBJECT_NAME)) {
                if (autoDisconn)
                    disconnect();
                throw(new IllegalArgumentException("failed to query on " + key +
                    " for " + category + " with " + rc + "/"+reason));
            }
            else { // fatal error
                disconnect();
                throw(new WrapperException("failed to get PCF responses", e));
            }
        }
        catch (IOException e) {
            disconnect();
            throw(new WrapperException("PCF send failed", e));
        }

        rc = responses[0].getCompCode();
        reason = responses[0].getReason();
        // Check the PCF header (MQCFH) in the first response message
        if (reason != 0 || rc != 0) {
            if (autoDisconn)
                disconnect();
            throw(new IllegalArgumentException("failed to get response on " +
                key + " for " + category + " with " + rc + "/" +reason));
        }

        Map map = getMap(cid, tid);
        Msg2Text msg2Text = getFormatter(cid, tid);
        k = 0;
        try {
            for (PCFMessage resp : responses) {
                Message msg = convert(resp, map);
                if (msg == null)
                    continue;
                line = msg2Text.format(Utils.RESULT_JSON, msg);
                if (line != null && line.length() > 0) {
                    if (k++ > 0)
                        strBuf.append(",");
                    strBuf.append(line + Utils.RS);
                }
            }
        }
        catch (Exception e) {
            if (autoDisconn)
                disconnect();
            throw(new IllegalArgumentException("failed to format msg " + k +
                " for " + key + ": "+ TraceStackThread.traceStack(e)));
        }

        strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
        strBuf.append("]}");
        if (autoDisconn)
            disconnect();
        return k;
    }

    /** returns the type id of the target in the PCF command */
    public static int getTypeID(int cid, String type) {
        int id = -1;
        if (cid <= 0 || type == null || type.length() <= 0)
            return id;

        switch (cid) {
          case CMQCFC.MQCMD_INQUIRE_Q:
            if ("QLOCAL".equals(type))
                id = CMQC.MQQT_LOCAL;
            else if ("QREMOTE".equals(type))
                id = CMQC.MQQT_REMOTE;
            else if ("QALIAS".equals(type))
                id = CMQC.MQQT_ALIAS;
            else if ("QCLUSTER".equals(type))
                id = CMQC.MQQT_CLUSTER;
            else if ("QMODEL".equals(type))
                id = CMQC.MQQT_MODEL;
            else if ("ALL".equals(type))
                id = CMQC.MQQT_ALL;
            break;
          case CMQCFC.MQCMD_INQUIRE_Q_STATUS:
            if ("QUEUE".equals(type))
                id = CMQCFC.MQIACF_Q_STATUS;
            else if ("HANDLE".equals(type))
                id = CMQCFC.MQIACF_Q_HANDLE;
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL:
            if ("SDR".equals(type))
                id = ChannelMonitor.CH_SENDER;
            else if ("SVR".equals(type))
                id = ChannelMonitor.CH_SERVER;
            else if ("RCVR".equals(type))
                id = ChannelMonitor.CH_RECEIVER;
            else if ("RQSTR".equals(type))
                id = ChannelMonitor.CH_REQUESTER;
            else if ("ALL".equals(type))
                id = ChannelMonitor.CH_ALL;
            else if ("CLNTCONN".equals(type))
                id = ChannelMonitor.CH_CLNTCONN;
            else if ("SVRCONN".equals(type))
                id = ChannelMonitor.CH_SVRCONN;
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS:
                id = 0;
            break;
          default:
        }

        return id;
    }

    /** returns the type of the target in the PCF command */
    public static String getType(String cmd) {
        int i, j, n;
        char c;
        String str;
        if (cmd == null || cmd.length() <= 7)
            return null;
        i = 0;
        n = cmd.length();
        while ((c = cmd.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = cmd.substring(i, i+8).toUpperCase();
        if ("DISPLAY ".equals(str)) { // for display
            i += 8;
            str = cmd.substring(i);
            j = i + str.indexOf(") ");
            if (j > i) {
                str = cmd.substring(j+2).toUpperCase();
                str = str.trim();
                i = str.indexOf("TYPE");
                if (i >= 0) {
                    str = str.substring(i+4);
                    str = str.trim();
                    i = str.indexOf('(');
                    j = str.indexOf(")", i);
                    if (i == 0 && j > i)
                        return str.substring(i+1, j).trim();
                }
            }
        }
        return null;
    }

    /** returns the id of the category in the PCF command */
    public static int getCategoryID(String category) {
        int id;
        if (category == null || category.length() <= 0)
            id = -1;
        else if ("QUEUE".equals(category))
            id = CMQCFC.MQCMD_INQUIRE_Q;
        else if ("QSTATUS".equals(category))
            id = CMQCFC.MQCMD_INQUIRE_Q_STATUS;
        else if ("CHANNEL".equals(category))
            id = CMQCFC.MQCMD_INQUIRE_CHANNEL;
        else if ("CHSTATUS".equals(category))
            id = CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS;
        else
            id = -1;
        return id;
    }

    /** returns the category of the target in the PCF command */
    public static String getCategory(String cmd) {
        int i, j, n;
        char c;
        String str;
        if (cmd == null || cmd.length() <= 7)
            return null;
        i = 0;
        n = cmd.length();
        while ((c = cmd.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = cmd.substring(i, i+8).toUpperCase();
        if ("DISPLAY ".equals(str)) { // for display
            i += 8;
            str = cmd.substring(i).toUpperCase();
            j = str.indexOf('(');
            if (j > 0) {
                str = str.substring(0, j);
                return str.trim();
            }
        }
        return null;
    }

    /** returns the name of the target in the PCF command */
    public static String getTarget(String cmd) {
        int i, j, n;
        char c;
        String str;
        if (cmd == null || cmd.length() <= 7)
            return null;
        i = 0;
        n = cmd.length();
        while ((c = cmd.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = cmd.substring(i, i+8).toUpperCase();
        if ("DISPLAY ".equals(str)) { // for display
            i += 8;
            str = cmd.substring(i);
            j = i + str.indexOf('(');
            if (j > i) {
                str = cmd.substring(j+1);
                str = str.trim();
                i = str.indexOf(')');
                if (i > 0) {
                    str = str.substring(0, i);
                    str = str.trim();
                    i = str.indexOf("'");
                    if (i < 0) // upper case
                        return str.toUpperCase();
                    j = str.indexOf("'", i);
                    if (j > i) // original case
                        return str.substring(i, j).trim();
                }
            }
        }
        return null;
    }

    /** returns a PCFMessage for the category and the type */
    private PCFMessage getPCFMessage(int cid, int tid, String name) {
        PCFMessage msg = null;
        if (name == null)
            return null;

        switch (cid) {
          case CMQCFC.MQCMD_INQUIRE_Q:
            msg = new PCFMessage(cid);
            msg.addParameter(CMQC.MQCA_Q_NAME, name);
            switch (tid) {
              case CMQC.MQQT_REMOTE:
                msg.addParameter(CMQC.MQIA_Q_TYPE, tid);
                msg.addParameter(CMQCFC.MQIACF_Q_ATTRS, qrAttrs);
                break;
              case CMQC.MQQT_ALIAS:
                msg.addParameter(CMQC.MQIA_Q_TYPE, tid);
                msg.addParameter(CMQCFC.MQIACF_Q_ATTRS, qaAttrs);
                break;
              case CMQC.MQQT_LOCAL:
              case CMQC.MQQT_ALL:
              case CMQC.MQQT_CLUSTER:
              case CMQC.MQQT_MODEL:
                msg.addParameter(CMQC.MQIA_Q_TYPE, tid);
              default:
                msg.addParameter(CMQCFC.MQIACF_Q_ATTRS, qlAttrs);
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_Q_STATUS:
            msg = new PCFMessage(cid);
            msg.addParameter(CMQC.MQCA_Q_NAME, name);
            switch (tid) {
              case CMQCFC.MQIACF_Q_HANDLE:
                msg.addParameter(CMQCFC.MQIACF_Q_STATUS_TYPE,
                    CMQCFC.MQIACF_Q_HANDLE);
                msg.addParameter(CMQCFC.MQIACF_Q_ATTRS, qhAttrs);
                break;
              case CMQCFC.MQIACF_Q_STATUS:
              default:
                msg.addParameter(CMQCFC.MQIACF_Q_STATUS_TYPE,
                    CMQCFC.MQIACF_Q_STATUS);
                msg.addParameter(CMQCFC.MQIACF_Q_ATTRS, qsAttrs);
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL:
              msg = new PCFMessage(cid);
              msg.addParameter(CMQCFC.MQCACH_CHANNEL_NAME, name);
              if (tid > 0)
                  msg.addParameter(CMQCFC.MQIACH_CHANNEL_TYPE, tid);
              msg.addParameter(CMQCFC.MQIACF_CHANNEL_ATTRS, chlAttrs);
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS:
              msg = new PCFMessage(cid);
              msg.addParameter(CMQCFC.MQCACH_CHANNEL_NAME, name);
              msg.addParameter(CMQCFC.MQIACH_CHANNEL_INSTANCE_ATTRS, chsAttrs);
            break;
          default:
        }
        return msg;
    }

    /** returns the formatter for the cid and tid   */
    private Msg2Text getFormatter(int cid, int tid) {
        Msg2Text msg2Text = null;
        switch (cid) {
          case CMQCFC.MQCMD_INQUIRE_Q:
            switch (tid) {
              case CMQC.MQQT_REMOTE:
                msg2Text = qrMsg2Text;
                break;
              case CMQC.MQQT_ALIAS:
                msg2Text = qaMsg2Text;
                break;
              case CMQC.MQQT_LOCAL:
              case CMQC.MQQT_ALL:
              case CMQC.MQQT_CLUSTER:
              case CMQC.MQQT_MODEL:
              default:
                msg2Text = qlMsg2Text;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_Q_STATUS:
            switch (tid) {
              case CMQCFC.MQIACF_Q_HANDLE:
                msg2Text = qhMsg2Text;
                break;
              case CMQCFC.MQIACF_Q_STATUS:
              default:
                msg2Text = qsMsg2Text;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL:
            msg2Text = chlMsg2Text;
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS:
            msg2Text = chsMsg2Text;
            break;
          default:
        }
        return msg2Text;
    }

    /** returns the initialized formatter for a given attribute list  */
    private Msg2Text initFormatter(int[] attrs, Map map) {
        int i, k, n;
        String key;
        StringBuffer strBuf;
        Map<String, Object> ph;

        if(attrs == null || attrs.length <= 0 || map == null || map.size() <= 0)
            return null;

        strBuf = new StringBuffer();
        n = attrs.length;
        for (i=0; i<n; i++) {
            k = attrs[i];
            key = (String) map.get(String.valueOf(k));
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

    /** returns the array of the attributes for the cid and tid */
    private int[] getAttrs(int cid, int tid) {
        int[] attrs = null;
        switch (cid) {
          case CMQCFC.MQCMD_INQUIRE_Q:
            switch (tid) {
              case CMQC.MQQT_REMOTE:
                attrs = qrAttrs;
                break;
              case CMQC.MQQT_ALIAS:
                attrs = qaAttrs;
                break;
              case CMQC.MQQT_LOCAL:
              case CMQC.MQQT_ALL:
              case CMQC.MQQT_CLUSTER:
              case CMQC.MQQT_MODEL:
              default:
                attrs = qlAttrs;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_Q_STATUS:
            switch (tid) {
              case CMQCFC.MQIACF_Q_HANDLE:
                attrs = qhAttrs;
                break;
              case CMQCFC.MQIACF_Q_STATUS:
              default:
                attrs = qsAttrs;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL:
            attrs = chlAttrs;
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS:
            attrs = chsAttrs;
            break;
          default:
        }
        return attrs;
    }

    /** converts the table into an array of attributes for queries */
    private int[] initAttrs(String[][] table) {
        int i, j, k, n;
        String key, value;
        Object o;
        int[] attrs = null, a;
        if (table == null)
            return null;
        j = 0;
        n = table.length;
        a = new int[n];
        for (i=0; i<n; i++) {
            if (table[i] == null || table[i].length < 2)
                continue;
            key = table[i][0];
            if (key == null || key.length() <= 0)
                continue;
            value = table[i][1];
            if (value == null || value.length() <= 0)
                continue;
            k = key.indexOf('.');
            if (k <= 0)
                continue;
            o = Utils.getDeclaredConstant("com.ibm.mq.pcf." +
                key.substring(0, k), key.substring(k+1), uri);
            if (o != null && o instanceof Integer)
                a[j++] = ((Integer) o).intValue();
        }
        if (j == n)
            attrs = a;
        else {
            n = j;
            attrs = new int[n];
            for (i=0; i<n; i++)
                attrs[i] = a[i];
        }

        return attrs;
    }

    /** returns the lookup map for the cid and the tid */
    private Map<String, String> getMap(int cid, int tid) {
        Map<String, String> map = null;
        switch (cid) {
          case CMQCFC.MQCMD_INQUIRE_Q:
            switch (tid) {
              case CMQC.MQQT_REMOTE:
                map = qrMap;
                break;
              case CMQC.MQQT_ALIAS:
                map = qaMap;
                break;
              case CMQC.MQQT_LOCAL:
              case CMQC.MQQT_ALL:
              case CMQC.MQQT_CLUSTER:
              case CMQC.MQQT_MODEL:
              default:
                map = qlMap;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_Q_STATUS:
            switch (tid) {
              case CMQCFC.MQIACF_Q_HANDLE:
                map = qhMap;
                break;
              case CMQCFC.MQIACF_Q_STATUS:
              default:
                map = qsMap;
            }
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL:
            map = chlMap;
            break;
          case CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS:
            map = chsMap;
            break;
          default:
            map = new HashMap<String, String>();
        }
        return map;
    }

    /** converts the table into a Map for lookups */
    private Map<String, String> initMap(String[][] table) {
        int i, k, n;
        String key, value;
        Map<String, String> map;
        Object o;
        if (table == null)
            return null;
        map = new HashMap<String, String>();
        n = table.length;
        for (i=0; i<n; i++) {
            if (table[i] == null || table[i].length < 2)
                continue;
            key = table[i][0];
            if (key == null || key.length() <= 0)
                continue;
            value = table[i][1];
            if (value == null || value.length() <= 0)
                continue;
            k = key.indexOf('.');
            if (k <= 0)
                continue;
            o = Utils.getDeclaredConstant("com.ibm.mq.pcf." +
                key.substring(0, k), key.substring(k+1), uri);
            if (o != null && o instanceof Integer)
                map.put(o.toString(), value);
        }
        return map;
    }

    /** converts a PCF Message into a JMS TextMessage */
    private Message convert(PCFMessage msg, Map map) {
        int count, rc, reason, id;
        PCFParameter param;
        TextEvent event;
        Enumeration enumeration;
        Object o;
        String key;

        if (msg == null)
            map = new HashMap();

        count = msg.getParameterCount();
        reason = msg.getReason();
        rc = msg.getCompCode();
        enumeration = msg.getParameters();

        event = new TextEvent();

        while (enumeration.hasMoreElements()) {
            param  = (PCFParameter) enumeration.nextElement();
            if (param == null)
                continue;
            id = param.getParameter();
            key = String.valueOf(id);
            if (map.containsKey(key))
                key = (String) map.get(key);
            if (key == null || key.length() <= 0)
                continue;
            o = param.getValue();
            if (o instanceof String)
                event.setAttribute(key, ((String) o).trim());
            else if (o instanceof String[]) {
                StringBuffer strBuf = new StringBuffer();
                String[] keys = (String[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].trim());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Integer)
                event.setAttribute(key, ((Integer) o).toString());
            else if (o instanceof Integer[]) {
                StringBuffer strBuf = new StringBuffer();
                Integer[] keys = (Integer[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Long)
                event.setAttribute(key, ((Long) o).toString());
            else if (o instanceof Long[]) {
                StringBuffer strBuf = new StringBuffer();
                Long[] keys = (Long[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Double)
                event.setAttribute(key, ((Double) o).toString());
            else if (o instanceof Double[]) {
                StringBuffer strBuf = new StringBuffer();
                Double[] keys = (Double[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Float)
                event.setAttribute(key, ((Float) o).toString());
            else if (o instanceof Float[]) {
                StringBuffer strBuf = new StringBuffer();
                Float[] keys = (Float[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Object[]) {
                StringBuffer strBuf = new StringBuffer();
                Object[] keys = (Object[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else
                event.setAttribute(key, o.toString());
        }
        return event;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public String getURI() {
        return uri;
    }

    public void close() {
        if (cache != null)
            cache.clear();
        if (isConnected)
            disconnect();
    }

    public static void main(String args[]) {
        PCFRequester pcf;
        String line, type = null, hostname = "localhost", name = null;
        StringBuffer strBuf;
        int port = 1414;
        int id, n;
        Map<String, Object> ph;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 't':
                if (i+1 < args.length)
                    type = args[++i].toUpperCase();
                break;
              case 'h':
                if (i+1 < args.length)
                    hostname = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }
        name = args[args.length-1];

        if (type == null || name == null || name.length() <= 0) {
            System.out.println("bad type or name");
            System.exit(0);
        }

        if ("QL".equals(type))
            line = "DISPLAY QUEUE(" + name + ") TYPE(QLOCAL)";
        else if ("QA".equals(type))
            line = "DISPLAY QUEUE(" + name + ") TYPE(QALIAS)";
        else if ("QR".equals(type))
            line = "DISPLAY QUEUE(" + name + ") TYPE(QREMOTE)";
        else if ("Q".equals(type))
            line = "DISPLAY QUEUE(" + name + ") TYPE(ALL)";
        else if ("QS".equals(type))
            line = "DISPLAY QSTATUS(" + name + ") TYPE(QUEUE)";
        else if ("QH".equals(type))
            line = "DISPLAY QSTATUS(" + name + ") TYPE(HANDLE)";
        else if ("CHL".equals(type))
            line = "DISPLAY CHANNEL(" + name + ")";
        else if ("CHS".equals(type))
            line = "DISPLAY CHSTATUS(" + name + ")";
        else if ("SDR".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(SDR)";
        else if ("SVR".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(SVR)";
        else if ("RCVR".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(RCVR)";
        else if ("REQSTR".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(REQSTR)";
        else if ("ALL".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(ALL)";
        else if ("SVRCONN".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(SVRCONN)";
        else if ("CLNTCONN".equals(type))
            line = "DISPLAY CHANNEL(" + name + ") CHLTYPE(CLNTCONN)";
        else {
            line = null;
            System.out.println("type not supported: " + type);
            System.exit(0);
        }

        ph = new HashMap<String, Object>();
        ph.put("HostName", hostname);
        ph.put("Port", String.valueOf(port));
        ph.put("MaxIdleTime", "1");
        ph.put("ResultType", String.valueOf(Utils.RESULT_JSON));
        strBuf = new StringBuffer();
        try {
            pcf = new PCFRequester(ph);
            n = pcf.getResponse(line, strBuf, true);
            System.out.println(line + ": " + n);
            if (n >= 0)
                System.out.println(strBuf.toString());
            pcf.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("PCFRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("PCFRequester: a PCF requester to query queues or channels");
        System.out.println("Usage: java org.qbroker.wmq.PCFRequester -t type -h host -p port name");
        System.out.println("  -?: print this message");
        System.out.println("Following types supported:");
        System.out.println("  q: query queues");
        System.out.println("  ql: query local queues");
        System.out.println("  qa: query alias queues");
        System.out.println("  qr: query remote queues");
        System.out.println("  qs: query queue status on queues");
        System.out.println("  qh: query queue status on handles");
        System.out.println("  chl: query channels");
        System.out.println("  chs: query channel status");
        System.out.println("  sdr: query sender channels");
        System.out.println("  rcvr: query receiver channels");
        System.out.println("  svrconn: query serconn channels");
        System.out.println("  svr: query server channels");
        System.out.println("  reqstr: query requester channels");
    }
}
