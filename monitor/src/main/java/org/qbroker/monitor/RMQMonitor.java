package org.qbroker.monitor;

/* RMQMonitor.java - a monitor checking message flow rate on a queue of RMQ */

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.io.StringReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.TimeWindows;
import org.qbroker.json.JSON2Map;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;

/**
 * RMQMonitor monitors the message flow rate on a queue of RabbitMQ via
 * REST calls. Since the stats are collected with active consumers, they
 * may get reset frequently. The stats data may not be accurate. So we
 * try to collect the averate rates. In the event, the number of inMessages
 * is actually enqRate, while the outMessages for deqRate.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RMQMonitor extends Monitor {
    private String qName = null, uri;
    private WebTester webTester = null;
    private int vendorId = 0, watermark = 0;
    private long previousDepth, previousIn, previousOut, previousCount;
    private String previousQStatus;
    private String[] attrs = null;

    public RMQMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        int n;

        if (type == null)
            type = "RMQMonitor";

        if (description == null)
            description = "monitor a RabbitMQ queue";

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

            h.put("Name", name);
            h.put("URI", uri);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                h.put("Password", props.get("Password"));
            }

            if ("http".equals(scheme)) { // for RMQ management plugin
                webTester = new WebTester(h);
            }
            else { // not supported
                throw(new IllegalArgumentException("URI is not supported"));
            }
            h.clear();
        }
        else {
            throw(new IllegalArgumentException("URI is not defined"));
        }

        qName = name;
        if (qName == null)
            throw(new IllegalArgumentException("illegal QueueName: " + qName));

        if ((o = props.get("WaterMark")) == null ||
            (watermark = Integer.parseInt((String) o)) < 0)
            watermark = 0;

        previousCount = 0;
        previousIn = 0;
        previousOut = 0;
        previousDepth = 0;
        previousQStatus = "OK";
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int returnCode;
        long curDepth = 0, dlvCount = 0, pubCount = 0, inMsgs = 0, outMsgs = 0;
        long oppsCount = 0, ippsCount = 0;
        double enqRate = 0.0, deqRate = 0.0;
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

        Map<String, Object> r = webTester.generateReport(currentTime);
        if (r.get("ReturnCode") != null) {
            returnCode = Integer.parseInt((String) r.get("ReturnCode"));
        }
        else {
            throw(new IOException(name + " failed to rest call on " + uri));
        }

        if (returnCode != 0)
            throw(new IOException(name + " failed to rest call on " + uri +
                ": " + returnCode));
        else try {
            String json = webTester.getContent();
            map = (Map) JSON2Map.parse(new StringReader(json));
        }
        catch (Exception e) {
            throw(new IOException(name + " failed to parse json for " + qName +
                ": " + Event.traceStack(e)));
        }

        if (map == null || map.size() <= 0)
            throw(new IOException(name + " failed to get metrics for "+qName));
        else try {
            o = map.get("consumers");
            ippsCount = Long.parseLong((String) o);
            o = map.get("messages");
            curDepth = Long.parseLong((String) o);
            o = JSON2Map.get(map, "backing_queue_status.avg_ingress_rate");
            enqRate = Double.parseDouble((String) o);
            o = JSON2Map.get(map, "backing_queue_status.avg_egress_rate");
            deqRate = Double.parseDouble((String) o);
/** commented out since the data only available on an active session
            o = JSON2Map.get(map, "message_stats.publish");
            pubCount = Long.parseLong((String) o);
            o = JSON2Map.get(map, "message_stats.deliver_get");
            dlvCount = Long.parseLong((String) o);
            if (ippsCount > 0 && previousCount > 0) {//consumer has been running
                if (dlvCount > 0 && previousOut > 0 && dlvCount >= previousOut){
                    outMsgs = dlvCount - previousOut;
                    inMsgs = outMsgs + curDepth - previousDepth;
                    if (inMsgs < 0)
                        inMsgs = 0;
                }
                else if(pubCount > 0 && previousIn > 0 && pubCount>=previousIn){
                    inMsgs = pubCount - previousIn;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (curDepth == 0) {
                        if (outMsgs < dlvCount)
                            outMsgs = dlvCount;
                    }
                    else if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (dlvCount > 0) { // consumer bounced
                    outMsgs = dlvCount;
                    inMsgs = outMsgs + curDepth - previousDepth;
                    if (inMsgs < 0)
                        inMsgs = 0;
                }
                else if (pubCount > 0) {
                    inMsgs = pubCount;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (curDepth == 0) {
                        if (outMsgs < dlvCount)
                            outMsgs = dlvCount;
                    }
                    else if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (curDepth >= previousDepth) {
                    inMsgs = curDepth - previousDepth;
                    outMsgs = 0;
                }
                else {
                    inMsgs = 0;
                    outMsgs = previousDepth - curDepth;
                }
            }
            else if (ippsCount > 0) { // consumer just started
                if (pubCount > 0 && previousIn > 0 && pubCount >= previousIn) {
                    inMsgs = pubCount - previousIn;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (curDepth == 0) {
                        if (outMsgs < dlvCount)
                            outMsgs = dlvCount;
                    }
                    else if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (dlvCount > 0) {
                    outMsgs = dlvCount;
                    inMsgs = outMsgs + curDepth - previousDepth;
                    if (inMsgs < 0)
                        inMsgs = 0;
                }
                else if (pubCount > 0) {
                    inMsgs = pubCount;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (curDepth >= previousDepth) {
                    inMsgs = curDepth - previousDepth;
                    outMsgs = 0;
                }
                else {
                    inMsgs = 0;
                    outMsgs = previousDepth - curDepth;
                }
            }
            else { // consumer just stopped or is not running
                if (pubCount > 0 && previousIn > 0 && pubCount >= previousIn) {
                    inMsgs = pubCount - previousIn;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (pubCount > 0) {
                    inMsgs = pubCount;
                    outMsgs = inMsgs + previousDepth - curDepth;
                    if (outMsgs < 0)
                        outMsgs = 0;
                }
                else if (curDepth >= previousDepth) {
                    inMsgs = curDepth - previousDepth;
                    outMsgs = 0;
                }
                else {
                    inMsgs = 0;
                    outMsgs = previousDepth - curDepth;
                }
            }
            previousIn = pubCount;
            previousOut = dlvCount;
            previousCount = ippsCount;

            if (serialNumber == 1) { // initial reset
                inMsgs = 0;
                outMsgs = 0;
                qStatus = "OK";
            }
            else if (curDepth > 0 && previousDepth > 0 && outMsgs <= 0)
                qStatus = (ippsCount == 0) ? "NOAPPS" : "STUCK";
            else
                qStatus = "OK";
*/
            if (curDepth > 0 && previousDepth > 0 && deqRate <= 0.0)
                qStatus = (ippsCount == 0) ? "NOAPPS" : "STUCK";
            else
                qStatus = "OK";
            report.put("StateLabel", qStatus);
        }
        catch (Exception e) {
            throw(new IOException(name +" failed to parse metrics for "+ qName +
                ": " + Event.traceStack(e)));
        }

        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(qName + " ");
            strBuf.append(enqRate + " ");
            strBuf.append(deqRate + " ");
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
        report.put("OutMessages", String.valueOf(deqRate));
        report.put("InMessages", String.valueOf(enqRate));
        report.put("CurrentDepth", String.valueOf(curDepth));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        previousDepth = curDepth;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long inMsgs = 0, outMsgs = 0, preDepth = 0, curDepth = 0;
        double enqRate = 0, deqRate = 0;
        String qStatus = "UNKNOWN";
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Long.parseLong((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Long.parseLong((String) o);
        if ((o = latest.get("InMessages")) != null && o instanceof String)
            enqRate = Double.parseDouble((String) o);
//            inMsgs = Long.parseLong((String) o);
        if ((o = latest.get("OutMessages")) != null && o instanceof String)
            deqRate = Double.parseDouble((String) o);
//            outMsgs = Long.parseLong((String) o);
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
            event.setAttribute("inMessages", String.valueOf(enqRate));
            event.setAttribute("outMessages", String.valueOf(deqRate));
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
        chkpt.put("PreviousCount", String.valueOf(previousCount));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct, pDepth, pIn, pOut, pCount;
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
        if ((o = chkpt.get("PreviousCount")) != null)
            pCount = Long.parseLong((String) o);
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
        previousCount = pCount;
    }

    public void destroy() {
        super.destroy();
        if (webTester != null) {
            webTester.destroy();
            webTester = null;
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

            report = (MonitorReport) new RMQMonitor(ph);
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
        System.out.println("RMQMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("RMQMonitor: monitor a RabbitMQ queue");
        System.out.println("Usage: java org.qbroker.monitor.RMQMonitor -I cfg.json");
    }
}
