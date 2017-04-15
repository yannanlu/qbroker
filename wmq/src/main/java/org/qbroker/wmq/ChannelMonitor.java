package org.qbroker.wmq;

/* ChannelMonitor.java - a monitor on an MQSeries channel */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQSecurityExit;
import com.ibm.mq.MQException;
import com.ibm.mq.pcf.CMQC;
import com.ibm.mq.pcf.CMQCFC;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.GenericLogger;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.event.Event;

/**
 * ChannelMonitor monitors an MQSeries channel object for its stats and status
 * Since it monitors the flow rate of the channel, it will disable itself
 * at the start up for only once.
 *<br/><br/>
 * It supports SecurityExit in case it is required on the server side.
 * You can specify the username and password if SecurityExit is defined.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ChannelMonitor extends Monitor {
    private final static String HOSTNAME = Event.getHostName().toUpperCase();
    private Map shortHostName = null;
    private String channelName, hostName = null;
    private String mqCommand, qmgrName, chName, qName, uri;
    private String securityExit = null, securityData = null;
    private PCFMessageAgent pcfMsgAgent = null;
    private MQQueueManager qmgr = null;
    private boolean isConnected = false;
    private int mqTimeout, port;
    private int chType, chStatus, previousMsgs, previousDepth, previousBytes;
    private int previousChannelStatus, channelStatusOffset, openMode;
    private String username = null, password = null;
    public final static String chStatusText[] = {"STUCK",
        "NOAPPS", "UNKNOWN", "NOTFOUND", "INACTIVE",
        "BINDING", "STARTING", "RUNNING", "STOPPING",
        "RETRYING", "STOPPED", "REQUESTING", "PAUSED",
        "UNDEFINED", "UNDEFINED", "UNDEFINED", "UNDEFINED",
        "INITIALIZING"
    };
    public final static int CH_STUCK = -4;
    public final static int CH_NOAPPS = -3;
    public final static int CH_UNKNOWN = -2;
    public final static int CH_NOTFOUND = -1;
    public final static int CH_INACTIVE = 0;
    public final static int CH_BINDING = 1;
    public final static int CH_STARTING = 2;
    public final static int CH_RUNNING = 3;
    public final static int CH_STOPPING = 4;
    public final static int CH_RETRYING = 5;
    public final static int CH_STOPPED = 6;
    public final static int CH_REQUESTING = 7;
    public final static int CH_PAUSED = 8;
    public final static int CH_INITIALIZING = 13;
    public static int [] qAttrs = {
        CMQC.MQCA_Q_NAME,
        CMQC.MQIA_OPEN_INPUT_COUNT,
        CMQC.MQIA_OPEN_OUTPUT_COUNT,
        CMQC.MQIA_CURRENT_Q_DEPTH,
        CMQC.MQIA_MAX_Q_DEPTH
    };
    public static int [] chAttrs = {
        CMQCFC.MQIACH_CHANNEL_STATUS,
        CMQCFC.MQIACH_CHANNEL_TYPE,
        CMQCFC.MQIACH_MSGS,
        CMQCFC.MQIACH_BYTES_SENT,
        CMQCFC.MQIACH_BYTES_RCVD,
        CMQCFC.MQIACH_CURRENT_SEQ_NUMBER,
        CMQCFC.MQIACH_LAST_SEQ_NUMBER,
        CMQCFC.MQCACH_LAST_MSG_DATE,
        CMQCFC.MQCACH_LAST_MSG_TIME,
        CMQCFC.MQCACH_CHANNEL_START_DATE,
        CMQCFC.MQCACH_CHANNEL_START_TIME
    };
    private PCFMessage qQuery = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q);
    private PCFMessage chQuery =
        new PCFMessage(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS);
    public final static int Q_KEEPOPEN = 1;
    public final static int CH_SENDER = 1;
    public final static int CH_SERVER = 2;
    public final static int CH_RECEIVER = 3;
    public final static int CH_REQUESTER = 4;
    public final static int CH_ALL = 5;
    public final static int CH_CLNTCONN = 6;
    public final static int CH_SVRCONN = 7;
    public final static String chTypeText[] = {"UNDEFINED", "SENDER",
        "SERVER", "RECEIVER", "REQUESTER", "ALL", "CLITCONN", "SVRCONN"};

    public ChannelMonitor(Map props) {
        super(props);
        Object o;
        int n;

        if (type == null)
            type = "ChannelMonitor";

        template=new Template("##hostname##, ##HOSTNAME##, ##HOST##, ##host##");

        if ((o = props.get("ShortHostName")) != null && o instanceof Map)
            shortHostName = (Map) o;

        if (description == null)
            description = "monitor an MQ channel";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            URI u = null;
            String path;
            uri = substitute((String) o);

            try {
                u = new URI(uri);
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException(e.toString()));
            }

            if (!"wmq".equals(u.getScheme()))
                throw(new IllegalArgumentException("wrong scheme: " +
                    u.getScheme()));
            if ((path = u.getPath()) != null && path.length() > 1)
                qmgrName = path.substring(1);
            else
                qmgrName = "";
            if ((port = u.getPort()) <= 0)
                port = 1414;
            hostName = u.getHost();
            if (hostName != null) {
                if ((o = props.get("SecurityExit")) != null)
                    securityExit = (String) o;
                if ((o = props.get("SecurityData")) != null)
                    securityData = (String) o;
                if ((o = props.get("Username")) != null)
                    username = (String) o;
                if ((o = props.get("Password")) != null)
                    password = (String) o;
                if (securityData == null)
                    securityData = "";
            }

            if ((o = props.get("ConnectionChannel")) != null)
                channelName = (String) o;
            else
                channelName = "SYSTEM.DEF.SVRCONN";
        }
        else if ((o = MonitorUtils.select(props.get("QueueManager")))!=null){
            qmgrName = substitute((String) o);
            uri = "wmq:///" + qmgrName;
        }
        else {
      throw(new IllegalArgumentException("URI or QueueManager is not defined"));
        }

        if ((o = MonitorUtils.select(props.get("ChannelName"))) == null)
            throw(new IllegalArgumentException("ChannelName is not defined"));
        chName = substitute((String) o);

        if ((o = props.get("ChannelType")) == null || !(o instanceof String))
            throw(new IllegalArgumentException("ChannelType is not defined"));
        if ("Sender".equals((String) o) || "sender". equals((String) o))
            chType = 1;
        else
            chType = 3;

        if ((o = MonitorUtils.select(props.get("QueueName"))) != null) {
            qName = substitute((String) o);
            qQuery.addParameter(CMQC.MQCA_Q_NAME, qName);
            qQuery.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
            qQuery.addParameter(CMQCFC.MQIACF_Q_ATTRS, qAttrs);
        }
        else
            qName = "";

        if ((o = props.get("QueueOpenMode")) != null) {
            if ("KeepOpen".equals((String) o) || "keepopen".equals((String) o))
                openMode = 1;
            else
                openMode = 0;
        }
        else
            openMode = 0;

        // override the statsLogger from parent
        if ((o = MonitorUtils.select(props.get("StatsLog"))) != null) {
            Map h = (Map) props.get("LoggerProperties");
            if (h != null) {
                String loggerName = substitute((String) o);
                Properties p = new Properties();
                if ((o = h.get("LoggerName")) != null && o instanceof String)
                    loggerName = substitute((String) o);
                Iterator iter = (h.keySet()).iterator();
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    if ((o = h.get(key)) != null && o instanceof String)
                        p.setProperty(key, (String) o);
                }
                statsLogger = new GenericLogger(loggerName, p);
            }
            else { // use the default settings
                statsLogger = new GenericLogger(substitute((String) o));
            }
        }

        chQuery.addParameter(CMQCFC.MQCACH_CHANNEL_NAME, chName);
        chQuery.addParameter(CMQCFC.MQIACH_CHANNEL_INSTANCE_ATTRS, chAttrs);


        try {
            pcfMsgAgent = MQConnect();
        }
        catch (MQException e) {
            new Event(Event.ERR, name + " failed on initial connection to " +
                ((hostName != null) ? hostName : qmgrName) + ": " +
                 e.getMessage()).send();
        }
        MQDisconnect();
        pcfMsgAgent = null;

        if ((o = props.get("MQCommand")) != null)
            mqCommand = (String) o;
        else
            mqCommand = "/opt/mqm/bin/dspmqcsv";

        if ((o = props.get("MQTimeout")) == null ||
            (mqTimeout = 1000*Integer.parseInt((String) o)) < 0)
            mqTimeout = 60000;

        previousBytes = 0;
        previousMsgs = 0;
        previousDepth = 0;
        previousChannelStatus = CH_UNKNOWN;
        channelStatusOffset = 0 - CH_STUCK;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        report.clear();
        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                return report;
            }
            else {
                skip = NOSKIP;
                serialNumber ++;
            }
        }
        else {
            skip = NOSKIP;
            serialNumber ++;
        }

        if (dependencyGroup != null) { // check dependency
            skip = MonitorUtils.checkDependencies(currentTime, dependencyGroup,
                name);
            if (skip != NOSKIP) {
                if (skip == EXCEPTION) {
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                    return report;
                }
                else if (skip == SKIPPED)
                    return report;
                else if (!disabledWithReport)
                    return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        if (!isConnected) try {
            pcfMsgAgent = MQConnect();
        }
        catch (MQException e) {
            if (e.completionCode == MQException.MQCC_FAILED &&
                e.reasonCode == CMQC.MQRC_OBJECT_ALREADY_EXISTS) {
                // object already exists
                MQDisconnect();
                pcfMsgAgent = null;
            }
            else {
                MQDisconnect();
                pcfMsgAgent = null;
            }
            throw(new IOException("MQ connection failed: " +
                Event.traceStack(e)));
        }
        isConnected = true;

        PCFMessage[] responses = null, q_responses = null;
        PCFMessage msg = null;
        Object o;
        int lastSeq = 0, curSeq = 0, bytesSent = 0, bytesRcvd = 0, msgs = 0;
        int totalMsgs = 0, totalBytes = 0, outMsgs = 0, inMsgs = 0, curDepth=0;
        int ipps = 0, opps = 0, n, count = 0, reason = -1, q_reason = -1;
        String msgDate = null, msgTime = null, startDate = null, startTime=null;

        responses = null;
        try {
            responses = pcfMsgAgent.send(chQuery);
        }
        catch (MQException e) {
            if (e.completionCode != MQException.MQCC_FAILED ||
                e.reasonCode != CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND) {
                MQDisconnect();
                pcfMsgAgent = null;
                throw(new IOException("MQ PCF send failed: " +
                    Event.traceStack(e)));
            }
            reason = CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND;
            responses = null;
        }
        if (qName.length() > 0) try {
            q_responses = pcfMsgAgent.send(qQuery);
        }
        catch (MQException e) {
            MQDisconnect();
            pcfMsgAgent = null;
            throw(new IOException("MQ PCF send failed: " +
                Event.traceStack(e)));
        }
        MQDisconnect();
        pcfMsgAgent = null;

        report.put("ChannelName", chName);
        report.put("QueueName", qName);

        if (qName.length() > 0) { // query the queue depth
            if (q_responses != null && q_responses.length > 0) {
                msg = q_responses[0];
                count = msg.getParameterCount();
            }
            else {
              throw(new IOException("failed to get response from queue query"));
            }

            // Check the PCF header (MQCFH) in the response message
            if (msg == null || msg.getReason() != 0 || count <= 0) {
                throw(new IOException("failed to query " + qmgrName +
                    " on " + qName + " with reason: " + reason));
            }

            // get what we want from the returned parameters
            o = msg.getParameterValue(CMQC.MQIA_CURRENT_Q_DEPTH);
            if (o != null && o instanceof Integer) {
                curDepth = ((Integer) o).intValue();
                report.put("CurrentDepth", String.valueOf(curDepth));
            }
            o = msg.getParameterValue(CMQC.MQIA_MAX_Q_DEPTH);
            if (o != null && o instanceof Integer) {
                ipps = ((Integer) o).intValue();
                report.put("MaxDepth", String.valueOf(ipps));
            }
            o = msg.getParameterValue(CMQC.MQIA_OPEN_INPUT_COUNT);
            if (o != null && o instanceof Integer) {
                ipps = ((Integer) o).intValue();
                report.put("IppsCount", String.valueOf(ipps));
            }
            o = msg.getParameterValue(CMQC.MQIA_OPEN_OUTPUT_COUNT);
            if (o != null && o instanceof Integer) {
                opps = ((Integer) o).intValue();
                report.put("OppsCount", String.valueOf(opps));
            }
        }

        if (responses != null && responses.length > 0) { // query is OK
            msg = responses[0];
            count = msg.getParameterCount();
            reason = msg.getReason();

            // Check the PCF header (MQCFH) in the response message
            if (reason == 0) { // get values from the returned paramenters
                o = msg.getParameterValue(CMQCFC.MQIACH_BYTES_SENT);
                if (o != null && o instanceof Integer) {
                    bytesSent = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_BYTES_RCVD);
                if (o != null && o instanceof Integer) {
                    bytesRcvd = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_CURRENT_SEQ_NUMBER);
                if (o != null && o instanceof Integer) {
                    curSeq = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_LAST_SEQ_NUMBER);
                if (o != null && o instanceof Integer) {
                    lastSeq = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_CHANNEL_STATUS);
                if (o != null && o instanceof Integer) {
                    chStatus = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_CHANNEL_TYPE);
                if (o != null && o instanceof Integer) {
                    chType = ((Integer) o).intValue();
                }
                o = msg.getParameterValue(CMQCFC.MQIACH_MSGS);
                if (o != null && o instanceof Integer) {
                    msgs = ((Integer) o).intValue();
                }
                if (chType == CH_SENDER || chType == CH_SERVER) {
                    o = msg.getParameterValue(CMQCFC.MQCACH_XMIT_Q_NAME);
                    if (o != null && o instanceof String) {
                        String xmitQName = (String) o;
                        xmitQName = xmitQName.trim();
                        if (xmitQName != null && !xmitQName.equals(qName)) {
                            qName = xmitQName;
                            qQuery.initialize(CMQCFC.MQCMD_INQUIRE_Q);
                            qQuery.addParameter(CMQC.MQCA_Q_NAME, qName);
                         qQuery.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
                            qQuery.addParameter(CMQCFC.MQIACF_Q_ATTRS, qAttrs);
                        }
                    }
                }
                o = msg.getParameterValue(CMQCFC.MQCACH_LAST_MSG_DATE);
                if (o != null && o instanceof String) {
                    msgDate = (String) o;
                    msgDate = msgDate.trim();
                }
                o = msg.getParameterValue(CMQCFC.MQCACH_LAST_MSG_TIME);
                if (o != null && o instanceof String) {
                    msgTime = (String) o;
                    msgTime = msgTime.trim();
                }
                o = msg.getParameterValue(CMQCFC.MQCACH_CHANNEL_START_DATE);
                if (o != null && o instanceof String) {
                    startDate = (String) o;
                    startDate = startDate.trim();
                }
                o = msg.getParameterValue(CMQCFC.MQCACH_CHANNEL_START_TIME);
                if (o != null && o instanceof String) {
                    startTime = (String) o;
                    startTime = startTime.trim();
                }
            }
            else if (reason == CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND) {
                chStatus = CH_NOTFOUND;
            }
            else {
                throw(new IOException("failed to query " + qmgrName +
                    " on " + chName + " with reason: " + reason));
            }
        }
        else if (responses == null &&
            reason == CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND) {
            chStatus = CH_NOTFOUND;
        }
        else {
            throw(new IOException("failed to query " + qmgrName +
                " on " + chName + " with reason: " + reason));
        }

        if (chStatus > CH_NOTFOUND && chStatus < CH_INITIALIZING) {
            // calculate the in/out rate with cur = pre + ins - outs
            if (previousStatus < TimeWindows.EXCEPTION) { // OM just got bounced
                previousMsgs = msgs;
                previousDepth = curDepth;
                previousBytes = (chType == CH_SENDER) ? bytesSent : bytesRcvd;
            }
            totalMsgs = (msgs >= previousMsgs) ? msgs - previousMsgs : msgs;
            previousMsgs = msgs;
            if (chType == CH_SENDER) {
                totalBytes = (bytesSent >= previousBytes) ?
                    bytesSent - previousBytes : bytesSent;
                previousBytes = bytesSent;
                outMsgs = totalMsgs;
                inMsgs = totalMsgs + curDepth - previousDepth;
            }
            else {
                totalBytes = (bytesRcvd >= previousBytes) ?
                    bytesRcvd - previousBytes : bytesRcvd;
                previousBytes = bytesRcvd;
                outMsgs = totalMsgs + previousDepth - curDepth;
                inMsgs = totalMsgs;
            }
        }
        else if (chType == CH_SENDER || chType == CH_SERVER) { // assume no outs
            outMsgs = 0;
            inMsgs = curDepth - previousDepth;
            previousMsgs = 0;
            previousBytes = 0;
        }
        else if (chType == CH_RECEIVER || chType == CH_REQUESTER ||
            chType == CH_SVRCONN) { // assume no ins
            outMsgs = previousDepth - curDepth;
            inMsgs = 0;
            previousMsgs = 0;
            previousBytes = 0;
        }
        else {
            previousMsgs = 0;
            previousBytes = 0;
        }
        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(qmgrName + ":");
            strBuf.append(chName + " ");
            strBuf.append(chStatusText[chStatus + channelStatusOffset] + " ");
            if (chStatus > CH_NOTFOUND && chStatus < CH_INITIALIZING) {
                strBuf.append(totalMsgs + " ");
                strBuf.append(totalBytes + " ");
                strBuf.append(msgs + " ");
                strBuf.append(((chType == CH_SENDER) ? bytesSent : bytesRcvd));
                strBuf.append(" " + lastSeq + " ");
                n = curSeq - lastSeq;
                strBuf.append(n + " ");
                if (msgDate != null && msgDate.length() > 0)
                    strBuf.append(msgDate + " ");
                else
                    strBuf.append(startDate + " ");
                if (msgTime != null && msgTime.length() > 0)
                    strBuf.append(msgTime);
                else
                    strBuf.append(startTime);
            }
            if (qName != null && qName.length() > 0) {
                strBuf.append(" " + qmgrName + ":");
                strBuf.append(qName + " ");
                strBuf.append(inMsgs + " ");
                strBuf.append(outMsgs + " ");
                strBuf.append(curDepth + " ");
                strBuf.append(ipps + " ");
                strBuf.append(opps);
            }
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        report.put("ChannelName", chName);
        report.put("OutMessages", String.valueOf(outMsgs));
        report.put("InMessages", String.valueOf(inMsgs));
        report.put("ChannelStatus", String.valueOf(chStatus));
        report.put("ChannelType", String.valueOf(chType));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        previousDepth = curDepth;

        if (serialNumber == 1 && curDepth > 0) // disabled at the 1st time
            skip = DISABLED;
        else if ((disableMode > 0 && chStatus == CH_STOPPED) ||
            (disableMode < 0 && chStatus != CH_STOPPED))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int ippsCount = 0, oppsCount = 0, curDepth = 0, level = 0;
        int reasonCode = 0, channelStatus = CH_UNKNOWN, channelType = 1;
        int inMsgs = 0, outMsgs = 0, preDepth = 0;
        StringBuffer strBuf = new StringBuffer();
        String chStatusString;
        Object o;
        if ((o = latest.get("IppsCount")) != null && o instanceof String)
            ippsCount = Integer.parseInt((String) o);
        if ((o = latest.get("OppsCount")) != null && o instanceof String)
            oppsCount = Integer.parseInt((String) o);
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Integer.parseInt((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Integer.parseInt((String) o);
        if ((o = latest.get("ChannelStatus")) != null && o instanceof String)
            channelStatus = Integer.parseInt((String) o);
        if ((o = latest.get("ChannelType")) != null && o instanceof String)
            channelType = Integer.parseInt((String) o);
        if ((o = latest.get("InMessages")) != null && o instanceof String)
            inMsgs = Integer.parseInt((String) o);
        if ((o = latest.get("OutMessages")) != null && o instanceof String)
            outMsgs = Integer.parseInt((String) o);

        chStatusString = chStatusText[channelStatus + channelStatusOffset];

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                exceptionCount = 0;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            level = Event.INFO;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
            }
          case TimeWindows.EXCEPTION: // exception
            actionCount = 0;
            if (status == TimeWindows.EXCEPTION) {
                level = Event.WARNING;
                if (previousStatus != status) { // reset count and adjust step
                    exceptionCount = 0;
                    if (step > 0)
                        step = 0;
                }
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            channelStatus = CH_UNKNOWN;
            break;
          case TimeWindows.BLACKOUT: // blackout
            level = Event.INFO;
            if (previousStatus != status) {
                if (normalStep > 0)
                    step = normalStep;
                actionCount = 0;
                exceptionCount = 0;
            }
          default: // normal cases
            level = Event.INFO;
            exceptionCount = 0;
            if (status != TimeWindows.BLACKOUT &&
                previousStatus == TimeWindows.BLACKOUT)
                actionCount = 0;
            actionCount ++;
            if (channelType == CH_SENDER && oppsCount == 0 &&
                (openMode == Q_KEEPOPEN || (curDepth > 0 && preDepth > 0))) {
                if (status != TimeWindows.BLACKOUT) { // for normal cases
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: no apps write to ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName);
                channelStatus = CH_NOAPPS;
                if (previousChannelStatus != channelStatus)
                    actionCount = 1;
            }
            else if (channelType == CH_RECEIVER && ippsCount == 0 &&
                (openMode == Q_KEEPOPEN || (curDepth > 0 && preDepth > 0))) {
                if (status != TimeWindows.BLACKOUT) { // for normal cases
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: no apps read from ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName);
                channelStatus = CH_NOAPPS;
                if (previousChannelStatus != channelStatus)
                    actionCount = 1;
            }
            else if (curDepth > 0 && preDepth > 0 && outMsgs <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal cases
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Channel: ");
                strBuf.append(qmgrName + ":");
                strBuf.append(chName + " is ");
                strBuf.append(chStatusString);
                if (channelType == CH_RECEIVER) {
                    strBuf.append(", but apps not reading");
                }
                else {
                    strBuf.append(", but msgs not moving");
                }
                channelStatus = CH_STUCK;
                if (previousChannelStatus != channelStatus)
                    actionCount = 1;
            }
            else if (previousChannelStatus != channelStatus) {
                strBuf.append("Channel: ");
                strBuf.append(qmgrName + ":");
                strBuf.append(chName + " is ");
                strBuf.append(chStatusString);
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousStatus = status;
        previousChannelStatus = channelStatus;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            count = actionCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // found errors
            count = actionCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                if (count == 1 || count == tolerance) // react only 
                    break;
                return null;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // exceptions
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
          default:
            if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
        }

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
            event.setAttribute("currentDepth", "N/A");
            event.setAttribute("ippsCount", "N/A");
            event.setAttribute("oppsCount", "N/A");
        }
        else {
            count = actionCount;
            event.setAttribute("currentDepth", String.valueOf(curDepth));
            event.setAttribute("ippsCount", String.valueOf(ippsCount));
            event.setAttribute("oppsCount", String.valueOf(oppsCount));
        }
        chStatusString = chStatusText[channelStatus + channelStatusOffset];

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("channel", chName);
        event.setAttribute("queue", qName);
        event.setAttribute("channelType", chTypeText[channelType]);
        event.setAttribute("channelStatus", chStatusString);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        event.setAttribute("actionScript", actionStatus);
        event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    private String substitute(String input) {
        if (template == null)
            return input;
        if (input.indexOf("##hostname##") >=0)
            input = template.substitute("hostname", HOSTNAME.toLowerCase(),
                input);
        if (input.indexOf("##HOSTNAME##") >=0)
            input = template.substitute("HOSTNAME", HOSTNAME, input);
        if (input.indexOf("##host##") >=0)
            input = template.substitute("host", lookup(HOSTNAME).toLowerCase(),
                input);
        if (input.indexOf("##HOST##") >=0)
            input = template.substitute("HOST", lookup(HOSTNAME), input);
        return input;
    }

    private String lookup(String h) {
        if (shortHostName == null)
            return h;
        else if (shortHostName.get(h) == null)
            return h;
        else
            return (String) shortHostName.get(h);
    }

    private PCFMessageAgent MQConnect() throws MQException {
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
                throw(new IllegalArgumentException(name + " failed to " +
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

    private void MQDisconnect() {
        isConnected = false;
        if (pcfMsgAgent != null) try {
            pcfMsgAgent.disconnect();
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to disconnect from " +
                hostName + ": " + Event.traceStack(e)).send();
        }
        if (qmgr != null) try {
            qmgr.disconnect();
            qmgr = null;
        }
        catch (Exception e) {
            qmgr = null;
        }
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
       chkpt.put("PreviousChannelStatus",String.valueOf(previousChannelStatus));
        chkpt.put("PreviousDepth", String.valueOf(previousDepth));
        chkpt.put("PreviousBytes", String.valueOf(previousBytes));
        chkpt.put("PreviousMsgs", String.valueOf(previousMsgs));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pDepth, pStatus, sNumber, pBytes, pMsgs,
            pChannelStatus;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            ct = Long.parseLong((String) o);
            if (ct <= System.currentTimeMillis() - checkpointTimeout)
                return;
        }
        else
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ActionCount")) != null)
            aCount = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ExceptionCount")) != null)
            eCount = Integer.parseInt((String) o);
        else
            return;

        if ((o = chkpt.get("PreviousBytes")) != null)
            pBytes = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousMsgs")) != null)
            pMsgs = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousDepth")) != null)
            pDepth = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousChannelStatus")) != null)
            pChannelStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDepth = pDepth;
        previousBytes = pBytes;
        previousMsgs = pMsgs;
        previousChannelStatus = pChannelStatus;
    }

    public void destroy() {
        super.destroy();
        MQDisconnect();
        pcfMsgAgent = null;
    }

    public static void main(String args[]) {
        String filename = null;
        Monitor report = null;

        if (args.length == 0) {
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
              case 'I':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            report = new ChannelMonitor(ph);
            Map r = report.generateReport(0L);
            String str = (String) r.get("ChannelStatus");
            if (str != null)
                System.out.println(str + ": " + r.get("CurrentDepth") + " " +
                    r.get("InMessages") + " " + r.get("OutMessages"));
            else
                System.out.println("failed to get channel stats");
            if (report != null)
                report.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (report != null)
                report.destroy();
        }
    }

    private static void printUsage() {
        System.out.println("ChannelMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("ChannelMonitor: monitor a WebSphere channel via PCF");
        System.out.println("Usage: java org.qbroker.wmq.ChannelMonitor -I cfg.json");
    }
}
