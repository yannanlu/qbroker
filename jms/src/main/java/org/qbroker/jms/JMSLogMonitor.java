package org.qbroker.jms;

/* JMSLogMonitor.java - a monitor on the JMS implementation by MessageLogger */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.NewlogFetcher;
import org.qbroker.common.Utils;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.event.Event;

/**
 * JMSLogMonitor monitors on a JMS queue implemented by a plain logfile via
 * MessageLogger.  It reports the deqCount, enqCount and curDepth, etc.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMSLogMonitor extends Monitor {
    private String uri;
    private File referenceFile;
    private int errorIgnored, maxNumberLogs;
    private int previousDepth, logSize;
    private int maxScannedLogs, debug;
    private long position, timestamp, offset;
    private long previousPosition, previousTimestamp, previousOffset;
    private long savedPosition, savedTimestamp, savedOffset;
    private NewlogFetcher newlog;
    private boolean oldLogFile, needReset;
    public final static int MAXNUMBERLOGS = 40960;
    public final static int MAXSCANNEDLOGS = 40960;

    public JMSLogMonitor(Map props) {
        super(props);
        Object o;
        URI u;
        String s, logfile;
        int n;

        if (type == null)
            type = "UnixlogMonitor";

        if (description == null)
            description = "monitor a logfile";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        logfile = u.getPath();
        if (logfile == null || logfile.length() == 0)
            throw(new IllegalArgumentException("URI has no path: " + uri));
        if ((s = u.getScheme()) != null && !"log".equals(s))
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        else try {
            logfile = Utils.decode(logfile);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to decode: " + logfile));
        }

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = 0;

        if ((o = props.get("ErrorIgnored")) != null)
            errorIgnored = Integer.parseInt((String) o);
        else
            errorIgnored = 0;

        if ((o = props.get("MaxNumberLogs")) == null ||
            (maxNumberLogs = Integer.parseInt((String) o)) <= 0)
            maxNumberLogs = MAXNUMBERLOGS;

        if ((o = props.get("MaxScannedLogs")) == null ||
            (maxScannedLogs = Integer.parseInt((String) o)) <= 0)
            maxScannedLogs = MAXSCANNEDLOGS;

        if ((o = props.get("LogSize")) == null ||
            (logSize = Integer.parseInt((String) o)) < 0)
            logSize = 1;
        if (logSize > 20)
            logSize = 20;

        Map<String, Object> h = new HashMap<String, Object>();
        h.put("LogFile", logfile);
        if ((o = props.get("OldLogFile")) != null) {
            s = MonitorUtils.substitute(MonitorUtils.select(o), template);
            if (s != null && s.length() > 0) {
                h.put("OldLogFile", s);
                oldLogFile = true;
            }
            else
                oldLogFile = false;
        }
        else
            oldLogFile = false;
        h.put("ReferenceFile", props.get("ReferenceFile"));
        h.put("TimePattern", props.get("TimePattern"));
        h.put("OrTimePattern", props.get("OrTimePattern"));
        h.put("PerlPattern", props.get("PerlPattern"));
        h.put("MaxNumberLogs", props.get("MaxNumberLogs"));
        h.put("MaxScannedLogs", props.get("MaxScannedLogs"));
        h.put("MaxLogLength", props.get("MaxLogLength"));
        h.put("SaveReference", "false");
        h.put("Debug", props.get("Debug"));

        newlog = new NewlogFetcher(h);
        savedTimestamp = newlog.getTimestamp();
        savedPosition = newlog.getPosition();
        savedOffset = newlog.getOffset();
        timestamp = savedTimestamp;
        position =savedPosition;
        offset = savedOffset;
        referenceFile = new File((String) props.get("ReferenceFile"));

        previousDepth = 0;
        needReset = false;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int maxLogs = 100 * MAXSCANNEDLOGS;

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
                if (skip == EXCEPTION)
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        int i, number = 0, n = 0, enqCount, deqCount, curDepth;
        long pos, l, t, tm = 0L, tt = 0L, ll = 0L;
        StringBuffer strBuf = new StringBuffer();
        String entry = null, firstEntry = null, line = null;

        if (needReset)
            newlog.setReference(savedPosition, savedTimestamp, savedOffset);
        needReset = true;

        previousPosition = position;
        previousTimestamp = timestamp;
        previousOffset = offset;
        getReference();
        i=compareReference(previousPosition, previousTimestamp, previousOffset);
        if (i == 0) { // reference not updated so deqCount is zero
            deqCount = 0;
            tm = -1L;
        }
        else { // deqCount is not zero so compare with current reference
            i = compareReference(savedPosition, savedTimestamp, savedOffset);
            if (i > 0) { // reference file updated 
                tt = timestamp;
                ll = offset;
                deqCount = previousDepth;
                tm = 0L;
            }
            else if (i < 0) { // reference file late
                newlog.setReference(position, timestamp, offset);
                tt = savedTimestamp;
                ll = savedOffset;
                deqCount = previousDepth;
                tm = 0L;
            }
            else { // reference file ontime
                deqCount = previousDepth;
                tm = -1L;
            }
        }

        try {
            pos = newlog.locateNewlog();
        }
        catch (IOException e) {
            throw(new IOException("failed to locate new log: " +
                Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IOException("failed to locate new entries: " +
                Event.traceStack(e)));
        }

        if (oldLogFile && newlog.logBuffer.size() > 0) { // process old log
            n = newlog.logBuffer.size();
            firstEntry = (String) newlog.logBuffer.get(0);
            entry = (String) newlog.logBuffer.get(n-1);
            number += n;
        }

        if (serialNumber > 1) // reset limit on number logs to scan
            maxLogs = maxScannedLogs;

        n = 0;
        while ((line = newlog.getLine()) != null) {
            l = line.length();
            if ((t = newlog.getTimestamp(line)) >= 0) { // valid log
                if (n > 0) {
                    number ++;
                    entry = strBuf.toString();
                    if (number == 1)
                        firstEntry = entry;
                    strBuf = new StringBuffer();
                    if (tm >  0) { // check objective and the offset
                        if (ll <= newlog.getOffset()) { // found it
                            deqCount += (i > 0) ? number : - number;
                            tm = -1L;
                        }
                    }
                }
                if (tm == 0L && t > tt) // search is on
                    tm = t;
                else if (tm > 0L && t > tm) // search is off
                    tm = -1L;
                strBuf.append(line);
                newlog.updateReference(pos, t, l);
                n = 1;
            }
            else if (n > 0 && n < logSize) {
                strBuf.append(line);
                newlog.updateReference(l);
                n ++;
            }
            else {
                newlog.updateReference(l);
            }
            pos += l;
        }
        newlog.close();
        if (n > 0) {
            entry = strBuf.toString();
            number ++;
            if (number == 1)
                firstEntry = entry;
            if (tm >  0) { // check objective and the offset
                if (ll <= newlog.getOffset()) { // found it
                    deqCount += (i > 0) ? number : - number;
                    tm = -1L;
                }
            }
        }
        savedTimestamp = newlog.getTimestamp();
        savedPosition = newlog.getPosition();
        savedOffset = newlog.getOffset();
        needReset = false;

        if (i >= 0) {
            enqCount = number;
            curDepth = previousDepth + enqCount - deqCount;
        }
        else {
            curDepth = number;
            enqCount = deqCount + curDepth - previousDepth;
        }

        report.put("EnqCount", String.valueOf(enqCount));
        report.put("DeqCount", String.valueOf(deqCount));
        report.put("CurrentDepth", String.valueOf(curDepth));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        report.put("FirstEntry", firstEntry);
        report.put("LastEntry", entry);
        previousDepth = curDepth;

        if (statsLogger != null) {
            strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(referenceFile.getPath() + " ");
            strBuf.append(position + " ");
            strBuf.append(timestamp + " ");
            strBuf.append(offset + " ");
            strBuf.append(uri + " ");
            strBuf.append(enqCount + " ");
            strBuf.append(deqCount + " ");
            strBuf.append(curDepth + " ");
            strBuf.append("0 0");
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        if ((disableMode > 0 && number == 0) || (disableMode < 0 && number > 0))
            skip = DISABLED;
        else if (serialNumber == 1 && curDepth > 0) // disabled for the 1st time
            skip = DISABLED;

        return report;
    }

    private int compareReference(long position, long timestamp, long offset) {
        if (this.timestamp > timestamp) {
            return 1;
        }
        else if (this.timestamp < timestamp) {
            return -1;
        }
        else if (this.position == position) {
            if (this.offset > offset)
                return 1;
            else if (this.offset < offset)
                return -1;
            else
                return 0;
        }
        else { // log may be rotated
            return -1;
        }
    }

    private int getReference() {
        String str = null;
        int n, len = 0, bufferSize = 256;
        byte[] buffer = new byte[256];
        if (referenceFile.exists()) {
            try {
                FileInputStream in = new FileInputStream(referenceFile);
                while ((n = in.read(buffer, len, bufferSize - len)) >= 0) {
                    len += n;
                    if (len >= 128)
                        break;
                }
                in.close();
                str = new String(buffer, 0, len);
            }
            catch (IOException e) {
                return -1;
            }
            if (str.length() > 0) {
                n = str.indexOf(" ");
                position = Long.parseLong(str.substring(0, n));
                str = str.substring(n+1, str.length()-1);
                n = str.indexOf(" ");
                timestamp = Long.parseLong(str.substring(0, n));
                offset = Long.parseLong(str.substring(n+1));
                return 1;
            }
        }
        return 0;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int i;
        int curDepth = 0, preDepth = 0, enqCount = 0, deqCount = 0, level = 0;
        String firstEntry = null, lastEntry = null;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("FirstEntry")) != null)
            firstEntry = (String) o;
        if ((o = latest.get("LastEntry")) != null)
            lastEntry = (String) o;
        if ((o = latest.get("EnqCount")) != null && o instanceof String)
            enqCount = Integer.parseInt((String) o);
        if ((o = latest.get("DeqCount")) != null && o instanceof String)
            deqCount = Integer.parseInt((String) o);
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Integer.parseInt((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Integer.parseInt((String) o);

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
            if (curDepth > 0 && preDepth > 0 && deqCount <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal cases
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append(curDepth + " messages in ");
                strBuf.append(uri + " not moving");
            }
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
          case Event.ERR: // found matching logs that may be ignored
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
          case Event.WARNING: // exceptions or downgrades
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

        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
            if (strBuf.length() == 0)
                strBuf.append("'" + uri + "' has got " + enqCount +" messages");
        }

        Event event = new Event(level, strBuf.toString());
        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("uri", uri);
        event.setAttribute("curDepth", String.valueOf(curDepth));
        event.setAttribute("enqCount", String.valueOf(enqCount));
        event.setAttribute("deqCount", String.valueOf(deqCount));
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (enqCount > 0 && firstEntry != null) {
            if ((i = firstEntry.indexOf(" Content(")) > 0)
                event.setAttribute("firstEntry", firstEntry.substring(0, i));
            else
                event.setAttribute("firstEntry", firstEntry);
        }
        if (enqCount > 1 && lastEntry != null) {
            if ((i = lastEntry.indexOf(" Content(")) > 0)
                event.setAttribute("lastEntry", lastEntry.substring(0, i));
            else
                event.setAttribute("lastEntry", lastEntry);
        }

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
        chkpt.put("PreviousDepth", String.valueOf(previousDepth));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pDepth, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousDepth")) != null)
            pDepth = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDepth = pDepth;
    }

    public void destroy() {
        super.destroy();
        if (newlog != null) {
            newlog.close();
            newlog = null;
        }
    }
}
