package org.qbroker.wmq;

/* QueueMonitor.java - a monitor on a MQSeries queue */

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
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.event.Event;

/**
 * QueueMonitor monitors an MQSeries queue object for queue depth and status
 *<br><br>
 * If ResetQStats is set true and StatsLog is set, it will reset Queue stats
 * on the queue and log the enq/deq counts and hiDepth.  The reset will have
 * qmgr reset counters on the queue.  So there should be only one monitor to
 * reset the stats on the same queue.  Otherwise, the stats returned by the
 * call will not be correct.
 *<br><br>
 * It supports SecurityExit in case it is required on the server side.
 * You can specify the username and password if SecurityExit is defined.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class QueueMonitor extends Monitor {
    private String channelName;
    private String mqCommand, qmgrName, qName, uri, hostName = null;
    private double waterMark, previousPct;
    private MQQueueManager qmgr = null;
    private PCFMessageAgent pcfMsgAgent = null;
    private boolean isConnected = false;
    private boolean resetQStats = false;
    private int port, threshold;
    private int previousIncrement, previousDepth, mqTimeout;
    private String username = null, password = null;
    private String securityExit = null, securityData = null;
    public static int [] qAttrs = {
        CMQC.MQCA_Q_NAME,
        CMQC.MQIA_OPEN_INPUT_COUNT,
        CMQC.MQIA_OPEN_OUTPUT_COUNT,
        CMQC.MQIA_CURRENT_Q_DEPTH,
        CMQC.MQIA_MAX_Q_DEPTH
    };
    private PCFMessage qQuery = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q);
    private PCFMessage qStats = new PCFMessage(CMQCFC.MQCMD_RESET_Q_STATS);

    public QueueMonitor(Map props) {
        super(props);
        Object o;
        int n;

        if (type == null)
            type = "QueueMonitor";

        if (description == null)
            description = "monitor an MQ queue";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            URI u = null;
            String path;
            uri = MonitorUtils.substitute((String) o, template);

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
                if ((o = props.get("SecurityExit")) != null) {
                    securityExit = (String) o;
                    if ((o = props.get("SecurityData")) != null)
                        securityData = (String) o;
                    else if((o=props.get("EncryptedSecurityData")) != null) try{
                        securityData = Utils.decrypt((String) o);
                    }
                    catch (Exception e) {
                        throw(new IllegalArgumentException("failed to decrypt "+
                            "EncryptedSecurityData: " + e.toString()));
                    }
                }
                if ((o = props.get("Username")) != null) {
                    username = (String) o;
                    if ((o = props.get("Password")) != null)
                        password = (String) o;
                    else if ((o = props.get("EncryptedPassword")) != null) try {
                        password = Utils.decrypt((String) o);
                    }
                    catch (Exception e) {
                        throw(new IllegalArgumentException("failed to decrypt "+
                            "EncryptedPassword: " + e.toString()));
                    }
                }
                if (securityData == null)
                    securityData = "";
            }

            if ((o = MonitorUtils.select(props.get("ChannelName"))) != null)
                channelName = (String) o;
            else
                channelName = "SYSTEM.DEF.SVRCONN";
        }
        else if ((o = MonitorUtils.select(props.get("QueueManager")))!=null){
            qmgrName = MonitorUtils.substitute((String) o, template);
            uri = "wmq:///" + qmgrName;
        }
        else {
      throw(new IllegalArgumentException("URI or QueueManager is not defined"));
        }

        if ((o = MonitorUtils.select(props.get("QueueName"))) == null)
            throw(new IllegalArgumentException("QueueName is not defined"));
        qName = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("WaterMark")) == null || !(o instanceof String))
            throw(new IllegalArgumentException("WaterMark is not defined"));
        waterMark = Double.parseDouble((String) o);
        threshold = (int) waterMark;

        qQuery.addParameter(CMQC.MQCA_Q_NAME, qName);
        qQuery.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
        qQuery.addParameter(CMQCFC.MQIACF_Q_ATTRS, qAttrs);
        qStats.addParameter(CMQC.MQCA_Q_NAME, qName);

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

        if ((o = props.get("ResetQStats")) != null && "true".equals((String) o))
            resetQStats = true;

        if ((o = props.get("MQCommand")) != null)
            mqCommand = (String) o;
        else
            mqCommand = "/opt/mqm/bin/dspmqcsv";

        if ((o = props.get("MQTimeout")) == null ||
            (mqTimeout = 1000*Integer.parseInt((String) o)) < 0)
            mqTimeout = 60000;

        previousPct = (waterMark > 0) ? waterMark : 0;
        previousIncrement = 1 - (int) waterMark;
        previousDepth = 0;
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
            isConnected = true;
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

        PCFMessage[] responses = null, stats_responses = null;
        PCFMessage msg = null;
        Object o;
        int curDepth = 0, ipps = 0, opps = 0, reason = -1;
        int hiDepth = 0, deqCount = 0, enqCount = 0, count = 0;
        try {
            responses = pcfMsgAgent.send(qQuery);
            if (resetQStats)
                stats_responses = pcfMsgAgent.send(qStats);
        }
        catch (MQException e) {
            MQDisconnect();
            pcfMsgAgent = null;
            throw(new IOException("MQ PCF send failed: " +
                Event.traceStack(e)));
        }
        MQDisconnect();
        pcfMsgAgent = null;

        if (responses != null && responses.length > 0) {
            msg = responses[0];
            count = msg.getParameterCount();
            reason = msg.getReason();
        }
        else {
            throw(new IOException("failed to get response from queue query"));
        }

        report.put("QueueName", qName);

        // Check the PCF header (MQCFH) in the response message
        if (msg == null || reason != 0 || count <= 0) {
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

        if (resetQStats) {
            if (stats_responses != null && stats_responses.length > 0) {
                msg = stats_responses[0];
                count = msg.getParameterCount();
                reason = msg.getReason();
            }
            else {
                throw(new IOException("failed to get response for stats"));
            }

            // Check the PCF header (MQCFH) in the response message
            if (msg == null || reason != 0 || count <= 0) {
                throw(new IOException("failed to reset " + qName +
                    " on " + qmgrName + " with reason: " + reason));
            }

            // get what we want from the returned parameters
            o = msg.getParameterValue(CMQC.MQIA_HIGH_Q_DEPTH);
            if (o != null && o instanceof Integer) {
                hiDepth = ((Integer) o).intValue();
                report.put("HiDepth", String.valueOf(hiDepth));
            }
            o = msg.getParameterValue(CMQC.MQIA_MSG_ENQ_COUNT);
            if (o != null && o instanceof Integer) {
                enqCount = ((Integer) o).intValue(); 
                report.put("EnqCount", String.valueOf(enqCount));
            }
            o = msg.getParameterValue(CMQC.MQIA_MSG_DEQ_COUNT);
            if (o != null && o instanceof Integer) {
                deqCount = ((Integer) o).intValue(); 
                report.put("DeqCount", String.valueOf(deqCount));
            }
        }

        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            if (resetQStats) {
                strBuf.append(enqCount + " ");
                strBuf.append(deqCount + " ");
                strBuf.append(hiDepth + " ");
            }
            if (qmgrName != null && qmgrName.length() > 0)
                strBuf.append(qmgrName + ":");
            strBuf.append(qName + " ");
            strBuf.append(enqCount + " ");
            strBuf.append(deqCount + " ");
            strBuf.append(curDepth + " ");
            strBuf.append(ipps + " ");
            strBuf.append(opps + " 0");
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        if ((disableMode > 0 && curDepth <= 0 && previousDepth <= 0) ||
            (disableMode < 0 && (curDepth > 0 || previousDepth > 0)))
            skip = DISABLED;
        previousDepth = curDepth;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int ipps = 0, opps = 0, curDepth = 0, maxDepth = 0, level = 0;
        double pct = 0.0;
        int increment = 0;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("IppsCount")) != null && o instanceof String)
            ipps = Integer.parseInt((String) o);
        if ((o = latest.get("OppsCount")) != null && o instanceof String)
            opps = Integer.parseInt((String) o);
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Integer.parseInt((String) o);
        if ((o = latest.get("MaxDepth")) != null && o instanceof String)
            maxDepth = Integer.parseInt((String) o);

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
            o = latest.get("Exception");
            if (status == TimeWindows.EXCEPTION) {
                level = Event.WARNING;
                if (previousStatus != status) { // reset count and adjust step
                    exceptionCount = 0;
                    if (step > 0)
                        step = 0;
                    new Event(Event.WARNING, name +
                        " failed to generate report on " + qName + ": " +
                        Event.traceStack((Exception) o)).send();
                }
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) o).toString());
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
            if (threshold < 1 && waterMark > 0.0) { // percentage
                pct = ((double) curDepth) / maxDepth;
                strBuf.append("Queue: ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName + " is ");
                strBuf.append(String.valueOf((int) (100*pct)) + "% full");
            }
            else if (threshold <= 0) { // increment
                pct = curDepth;
                increment = curDepth - (int) (previousPct + 0.001);
                strBuf.append("Queue: ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName + " has ");
                strBuf.append(String.valueOf(increment));
                strBuf.append(" messages more than that of the previous test");
            }
            else { // curdepth
                pct = curDepth;
                strBuf.append("Queue: ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName + " has ");
                strBuf.append(String.valueOf(curDepth) + " messages");
            }

            if (waterMark > 0.0) { // static depth
                if ((threshold == 0 && pct >= waterMark) ||
                    (threshold > 0 && curDepth >= threshold)) {
                    if (status != TimeWindows.BLACKOUT) { // for normal case
                        level = Event.ERR;
                        if (step > 0)
                            step = 0;
                    }
                    if (previousPct < waterMark)
                        actionCount = 1;
                }
                else if (previousPct >= waterMark) {
                    actionCount = 1;
                    if (normalStep > 0)
                        step = normalStep;
                }
            }
            else { // dynamic increment
                increment = curDepth - (int) (previousPct + 0.001);
                if (increment > (-threshold)) {
                    if (status != TimeWindows.BLACKOUT) { // for normal case
                        level = Event.ERR;
                        if (step > 0)
                            step = 0;
                    }
                    if (previousIncrement <= (-threshold))
                        actionCount = 1;
                }
                else if (previousIncrement > (-threshold)) {
                    actionCount = 1;
                    if (normalStep > 0)
                        step = normalStep;
                }
                previousIncrement = increment;
            }
            previousPct = pct;
            break;
        }
        previousStatus = status;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            count = actionCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // errors
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
            event.setAttribute("maxDepth", "N/A");
            event.setAttribute("ippsCount", "N/A");
            event.setAttribute("oppsCount", "N/A");
        }
        else {
            count = actionCount;
            event.setAttribute("currentDepth", String.valueOf(curDepth));
            event.setAttribute("maxDepth", String.valueOf(maxDepth));
            event.setAttribute("ippsCount", String.valueOf(ipps));
            event.setAttribute("oppsCount", String.valueOf(opps));
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("queue", qName);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (waterMark < 0.0)
            event.setAttribute("currentIncrement", String.valueOf(increment));

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
        chkpt.put("PreviousPct", String.valueOf(previousPct));
        chkpt.put("PreviousIncrement", String.valueOf(previousIncrement));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pIncrement, pStatus, sNumber;
        double pPct;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
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

        if ((o = chkpt.get("PreviousPct")) != null)
            pPct = Double.parseDouble((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousIncrement")) != null)
            pIncrement = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousPct = pPct;
        previousIncrement = pIncrement;
    }

    public void destroy() {
        super.destroy();
        MQDisconnect();
        pcfMsgAgent = null;
    }

    public static void main(String args[]) {
        String filename = null;
        Monitor monitor = null;

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
            long tm = System.currentTimeMillis();
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            monitor = new QueueMonitor(ph);
            Map r = monitor.generateReport(tm);
            String str = (String) r.get("CurrentDepth");
            if (str != null) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else 
                    System.out.println(str + " " + r.get("IppsCount") + " " +
                        r.get("OppsCount") + " " + r.get("MaxDepth"));
            }
            else
                System.out.println("failed to get queue stats");
            if (monitor != null)
                monitor.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (monitor != null)
                monitor.destroy();
        }
    }

    private static void printUsage() {
        System.out.println("QueueMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("QueueMonitor: monitor a WebSphere queue via PCF");
        System.out.println("Usage: java org.qbroker.wmq.QueueMonitor -I cfg.json");
    }
}
