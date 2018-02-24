package org.qbroker.monitor;

/* QMFQMonitor.java - a monitor checking message flow rate on a queue via QMF */

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.io.IOException;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.TimeWindows;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.net.QMFRequester;

/**
 * QMFQMonitor monitors the message flow on a queue via QMF. It supports
 * generic QMF for Apache QPid for now.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QMFQMonitor extends Monitor {
    private String qName = null, uri;
    private QMFRequester qmfReq = null;
    private int vendorId = 0, watermark = 0;
    private long previousDepth, previousIn, previousOut;
    private String previousQStatus;
    private String[] attrs = null;
    public final static String[] qpidAttrs = { // for Apache Qpid
        "name",
        "msgTotalEnqueues",
        "msgTotalDequeues",
        "msgDepth",
        "consumerCount",
        "bindingCount"
    };

    public QMFQMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        int n;

        if (type == null)
            type = "QMFQMonitor";

        if (description == null)
            description = "monitor a QPid queue";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            String path, scheme = null;
            uri = MonitorUtils.substitute((String) o, template);

            try {
                URI u = new URI(uri);
                scheme = u.getScheme();
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException("Bad URI: " + uri +
                    ": " +e.toString()));
            }

            h.put("URI", uri);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }

            h.put("ConnectOnInit", "false");
            if ("amqp".equals(scheme)) { // for QMF
                qmfReq = new QMFRequester(h);
            }
            else { // not supported
                throw(new IllegalArgumentException("URI is not supported"));
            }
            h.clear();
        }
        else {
            throw(new IllegalArgumentException("URI is not defined"));
        }

        if ((o = MonitorUtils.select(props.get("ObjectName"))) == null)
            throw(new IllegalArgumentException("ObjectName is not defined"));
        qName = MonitorUtils.substitute((String) o, template);
        attrs = qpidAttrs;

        if (qName == null || attrs == null)
            throw(new IllegalArgumentException("illegal ObjectName: " + qName));

        if ((o = props.get("WaterMark")) == null ||
            (watermark = Integer.parseInt((String) o)) < 0)
            watermark = 0;

        previousIn = 0;
        previousOut = 0;
        previousDepth = 0;
        previousQStatus = "UNKNOWN";
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long curDepth = 0, totalMsgs = 0, inMsgs = 0, outMsgs = 0;
        long oppsCount = 0, ippsCount = 0;
        String qStatus;
        Object o;
        Map map;

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

        if ((qStatus = qmfReq.reconnect()) != null)
            throw(new IOException(name + " failed to connect to " + uri +
                ": "+ qStatus));
        try {
            map = qmfReq.getValues(qName, qpidAttrs);
        }
        catch (Exception e) {
            try {
                Thread.sleep(500L);
            }
            catch (Exception ex) {
            }
            qmfReq.reconnect();
            try {
                map = qmfReq.getValues(qName, qpidAttrs);
            }
            catch (Exception ex) {
                qmfReq.close();
                throw(new IOException(name + " failed to query queue metrics: "+
                    Event.traceStack(e)));
            }
        }
        qmfReq.close();

        if (map == null || map.size() <= 0)
            throw(new IOException(name + " failed to get queue metrics for " +
                qName));
        else try {
            o = map.get("msgTotalEnqueues");
            totalMsgs = ((Long) o).longValue();
            inMsgs = (totalMsgs >= previousIn) ? totalMsgs-previousIn:totalMsgs;
            previousIn = totalMsgs;
            o = map.get("msgDepth");
            curDepth = ((Long) o).longValue();
            outMsgs = inMsgs - curDepth + previousDepth;
            if (outMsgs < 0)
                outMsgs = 0;
            o = map.get("consumerCount");
            ippsCount = ((Long) o).longValue();
            o = map.get("bindingCount");
            oppsCount = ((Long) o).longValue();
            if (serialNumber == 1) { // initial reset
                inMsgs = 0;
                outMsgs = 0;
                qStatus = "OK";
            }
            else if (curDepth > 0 && previousDepth > 0 && outMsgs <= 0)
                qStatus = (ippsCount == 0) ? "NOAPPS" : "STUCK";
            else
                qStatus = "OK";
            report.put("StateLabel", qStatus);
        }
        catch (Exception e) {
            throw(new IOException(name + " failed to parse metrics for "+
                qName + ": " + Event.traceStack(e)));
        }

        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(qName + " ");
            strBuf.append(inMsgs + " ");
            strBuf.append(outMsgs + " ");
            strBuf.append(curDepth + " ");
            strBuf.append(ippsCount + " ");
            strBuf.append(oppsCount);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        report.put("OutMessages", String.valueOf(outMsgs));
        report.put("InMessages", String.valueOf(inMsgs));
        report.put("CurrentDepth", String.valueOf(curDepth));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        if ((disableMode > 0 && curDepth <= 0 && previousDepth <= 0) ||
            (disableMode < 0 && (curDepth > 0 || previousDepth > 0)))
            skip = DISABLED;
        previousDepth = curDepth;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long inMsgs = 0, outMsgs = 0, preDepth = 0, curDepth = 0;
        String qStatus = "UNKNOWN";
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Long.parseLong((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Long.parseLong((String) o);
        if ((o = latest.get("InMessages")) != null && o instanceof String)
            inMsgs = Long.parseLong((String) o);
        if ((o = latest.get("OutMessages")) != null && o instanceof String)
            outMsgs = Long.parseLong((String) o);
        if ((o = latest.get("StateLabel")) != null && o instanceof String)
            qStatus = (String) o;

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
            if (curDepth > 0 && preDepth > 0 && outMsgs <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    if (watermark > 0) {
                        if (curDepth >= watermark && preDepth >= watermark)
                            level = Event.ERR;
                        else
                            level = Event.WARNING;
                    }
                    else
                        level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: application seems not reading from ");
                strBuf.append(qName);
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (!previousQStatus.equals(qStatus)) {
                strBuf.append("Queue: application is OK");
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousStatus = status;
        previousQStatus = qStatus;

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
            event.setAttribute("inMessages", "N/A");
            event.setAttribute("outMessages", "N/A");
        }
        else {
            count = actionCount;
            event.setAttribute("currentDepth", String.valueOf(curDepth));
            event.setAttribute("inMessages", String.valueOf(inMsgs));
            event.setAttribute("outMessages", String.valueOf(outMsgs));
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
        event.setAttribute("qStatus", qStatus);
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

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousQStatus", String.valueOf(previousQStatus));
        chkpt.put("PreviousDepth", String.valueOf(previousDepth));
        chkpt.put("PreviousIn", String.valueOf(previousIn));
        chkpt.put("PreviousOut", String.valueOf(previousOut));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct, pDepth, pIn, pOut;
        int aCount, eCount, pStatus, sNumber;
        String pQStatus;

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

        if ((o = chkpt.get("PreviousIn")) != null)
            pIn = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousOut")) != null)
            pOut = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousDepth")) != null)
            pDepth = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousQStatus")) != null)
            pQStatus = (String) o;
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDepth = pDepth;
        previousQStatus = pQStatus;
        previousIn = pIn;
        previousOut = pOut;
    }

    public void destroy() {
        super.destroy();
        if (qmfReq != null) {
            qmfReq.close();
            qmfReq = null;
        }
    }

    protected void finalize() {
        destroy();
    }

    public static void main(String args[]) {
        String filename = null;
        MonitorReport report = null;

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

            report = (MonitorReport) new QMFQMonitor(ph);
            Map r = report.generateReport(0L);
            String str = (String) r.get("StateLabel");
            if (str != null)
                System.out.println(str + ": " + r.get("CurrentDepth") + " " +
                    r.get("InMessages") + " " + r.get("OutMessages"));
            else
                System.out.println("failed to get queue stats");
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
        System.out.println("QMFQMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("QMFQMonitor: monitor a QPid queue via QMF");
        System.out.println("Usage: java org.qbroker.monitor.QMFQMonitor -I cfg.json");
    }
}
