package org.qbroker.monitor;

/* UnixlogMonitor.java - a log monitor checking patterns */

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.NewlogFetcher;
import org.qbroker.common.Utils;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.event.Event;

/**
 * UnixlogMonitor monitors a general Unix log file and detects the new
 * occurrences of log entries matching at least one of the given patterns
 *<br><br>
 * Use UnixlogMonitor to detect the new occurrence of log entries matching
 * the given patterns.  UnixlogMonitor may be repetitively used to check the log
 * file.  It will pick up the new log entries only.  It also guarantees the
 * order of the occurrences.  If a certain log entry appears, the new entry will
 * be stored in the buffer for analysis.  The buffer will be updated every time.
 *<br><br>
 * UnixlogMonitor can also sum up numbers in the log entries, as long as the
 * pattern contains something like (\d+).  Currently, it will process the
 * only one number or less, determined by the attribute of numberDataFields.
 * In default, it is -1, ie, no number to sum.  It will use the log buffer and
 * number of logs.  If it is set to 0, the sum will be the number of logs.
 *<br><br>
 * UnixlogMonitor also has excluding patterns to exclude the special log
 * entries that are not critical.  The monitor is fully configurable.
 * The matching events are categoried as INFO, WARNING, ERR and CRIT.
 * WARNING is for small of number matches that is no more than the number
 * of errorIgnored.  If the number of match is more than that, the event is
 * CRIT that triggers page alerts.  If WARNING persists for more than Tolerence
 * in a row, it will be upgraded to ERR.  In case of ERR, the monitor will
 * try to invoke the action scripts if they are defined.  If ERR continues and
 * stays the same for more than maxRetry in a row, it will be upgraded to the
 * next level, CRIT.
 *<br><br>
 * Sometimes, certain log entries are fatal to the application and require
 * special attentions. For example, OutOfMemoryError requires a restart since
 * the working thread is dead. UnixlogMonitor supports escalation patterns to
 * single out the fatal log entries from the matched log buffer. It allows the
 * end user to set up certain actions in ActionGroup to react on it. In case
 * there is a catch, the event priority will be escalated to ALERT temporarily
 * so that it can trigger the actions on ALERT only. In this case, the value for
 * the attribute of firstEntry will be the first fatal log entry.
 *<br><br>
 * UnixlogMonitor supports ExpirationTime.  If it is defined and is larger
 * than zero in second, UnixlogMonitor will ignore any new log entry that
 * has a timestamp older than the difference of currentTime - expirationTime.
 * It can be used to skip the old log entries.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class UnixlogMonitor extends Monitor {
    private String uri;
    private int errorIgnored, maxNumberLogs, maxSessionCount;
    private int previousNumber, logSize;
    private int numberDataFields, maxScannedLogs, debug;
    private long expirationTime = 0;
    private NewlogFetcher newlog;
    private boolean advanceLog, verifyLastlog, isLastlog, oldLogFile;
    private Pattern pattern;
    private Pattern[][] ePatternGroup;
    public final static int MAXNUMBERLOGS = 40960;
    public final static int MAXSCANNEDLOGS = 40960;

    @SuppressWarnings("unchecked")
    public UnixlogMonitor(Map props) {
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

        if((o = props.get("PatternGroup")) == null || !(o instanceof List))
            throw(new IllegalArgumentException("PatternGroup is not defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
            ePatternGroup = MonitorUtils.getPatterns("EPatternGroup",props,pc);
        }
        catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed" + e.getMessage()));
        }
        if (aPatternGroup.length <= 0)
            throw(new IllegalArgumentException("PatternGroup is empty"));

        if ((o = props.get("NumberDataFields")) == null ||
            (numberDataFields = Integer.parseInt((String) o)) < 0)
            numberDataFields = 0;

        if ((o = props.get("MaxSessionCount")) != null)
            maxSessionCount = Integer.parseInt((String) o);
        else
            maxSessionCount = 0;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = 0;

        advanceLog = false;
        verifyLastlog = false;
        if ((o = props.get("ActionGroup")) != null &&
            actionGroup.getNumberScripts() > 0) {
            // If AdvanceLog is set to true, it will try to skip log entries
            // generated after ActionGroup so that next check will not catch
            // those logs.  The action may add a sleep to wait for logs.
            o = props.get("AdvanceLog");
            if (o != null && Integer.parseInt((String) o) != 0)
                advanceLog = true;
            o = props.get("VerifyLastLog");
            if (o != null && Integer.parseInt((String) o) != 0)
                verifyLastlog = true;
        }

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

        if (maxPage == 0 && errorIgnored > 0)
            errorIgnored = 0;

        if ((o = props.get("LogSize")) == null ||
            (logSize = Integer.parseInt((String) o)) < 0)
            logSize = 1;
        if (logSize > 50)
            logSize = 50;

        if ((o = props.get("ExpirationTime")) == null ||
            (expirationTime = 1000*Integer.parseInt((String) o)) < 0)
            expirationTime = 0;

        if ((o = props.get("OldLogFile")) != null) {
            s = MonitorUtils.substitute(MonitorUtils.select(o), template);
            if (s != null && s.length() > 0) {
                props.put("OldLogFile", s);
                oldLogFile = true;
            }
            else
                oldLogFile = false;
        }
        else
            oldLogFile = false;

        props.put("LogFile", logfile);
        newlog = new NewlogFetcher(props);
        props.remove("LogFile");
        if (oldLogFile)
            props.put("OldLogFile", o);

        previousNumber = -10;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int maxLogs = 100 * MAXSCANNEDLOGS;

        report.clear();
        isLastlog = false;
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

        int i, k, number = 0, totalNumber = 0, n = 0, m = 0;
        long pos, l, t = 0L;
        List<String> logBuffer = new ArrayList<String>();
        StringBuffer strBuf = new StringBuffer();
        String line = null;
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
            String entry;
            n = newlog.logBuffer.size();
            for (i=0; i<n; i++) {
                entry = (String) newlog.logBuffer.get(i);
                m = 0;
                if (MonitorUtils.filter(entry, aPatternGroup, pm, true)) {
                    if (expirationTime > 0) { // check expiration
                        if ((k = entry.indexOf("\n")) < 0)
                            k = entry.indexOf("\r");
                        line = (k > 0) ? entry.substring(0, k) : entry;
                        if ((t = newlog.getTimestamp(line)) >= 0 &&
                            currentTime > t + expirationTime) // expired
                            continue;
                    }
                    if (numberDataFields > 0) { // number in the last pattern
                        MatchResult mr = pm.getMatch();
                        if (mr.groups() > 1)
                            m = Integer.parseInt(mr.group(1));
                    }
                    if (!MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                        if (numberDataFields > 0)
                            totalNumber += m;
                        else
                            logBuffer.add(entry);
                        number ++;
                    }
                }
            }
        }

        if (serialNumber > 1) // reset limit on number logs to scan
            maxLogs = maxScannedLogs;

        n = 0;
        k = 0;
        while ((line = newlog.getLine()) != null) {
            l = line.length();
            if ((t = newlog.getTimestamp(line)) >= 0) { // valid log
                if ((k++ % 100) == 0) {
                    if ((debug & NewlogFetcher.DEBUG_NEWLOG) > 0)
                        System.out.println((k-1) + ": " +
                            line.substring(0,((l > 48L)? 48 : (int)l))+"!"+ t);
                    if (k >= maxLogs) // stop scanning
                        break;
                }
                if (n > 0) { // matching the patterns
                    String entry = strBuf.toString();
                    strBuf = new StringBuffer();
                    m = 0;
                    if (MonitorUtils.filter(entry, aPatternGroup, pm, true)) {
                        if (numberDataFields > 0) { // number in the last p
                            MatchResult mr = pm.getMatch();
                            if (mr.groups() > 1)
                                m = Integer.parseInt(mr.group(1));
                        }
                        if (!MonitorUtils.filter(entry,xPatternGroup,pm,false)){
                            if (numberDataFields > 0)
                                totalNumber += m;
                            else
                                logBuffer.add(entry);
                            number ++;
                        }
                    }
                }
                if (number >= maxNumberLogs) {
                    n = 0;
                    newlog.seek(pos);
                    break;
                }
                newlog.updateReference(pos, t, l);
                if (expirationTime > 0 && currentTime > t + expirationTime) {
                    n = 0;
                }
                else { // not expire yet
                    strBuf.append(line);
                    n = 1;
                }
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
        if (n > 0) { // leftover
            String entry;
            if (n < logSize) { // there may be lines leftover
                t = 2000 / (logSize - n);
                do {
                    try {
                        Thread.sleep(t);
                    }
                    catch (InterruptedException e) {
                    }
                    n ++;
                    if ((line = newlog.getLine()) != null) {
                        l = line.length();
                        if (newlog.getTimestamp(line) >= 0) // valid log
                            break;
                        strBuf.append(line);
                        newlog.updateReference(l);
                    }
                } while (n < logSize);
            }
            entry = strBuf.toString();
            if ((debug & NewlogFetcher.DEBUG_NEWLOG) > 0 && (--k % 100) > 0) {
                l = strBuf.length();
                if (l > 48L)
                    l = 48L; 
                System.out.println(k + ": " + entry.substring(0, (int)l)+ "!"+
                    newlog.getTimestamp(entry.substring(0, (int)l)));
            }
            m = 0;
            if (MonitorUtils.filter(entry, aPatternGroup, pm, true)) {
                if (numberDataFields > 0) { // number in the last pattern
                    MatchResult mr = pm.getMatch();
                    if (mr.groups() > 1)
                        m = Integer.parseInt(mr.group(1));
                }
                if (!MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                    if (numberDataFields > 0)
                        totalNumber += m;
                    else
                        logBuffer.add(entry);
                    number ++;
                }
            }
        }
        newlog.close();
        newlog.saveReference();
        if (number > 0 && number < maxNumberLogs)
            isLastlog = true;

        if (numberDataFields <= 0)
            totalNumber = number;

        report.put("TotalNumber", String.valueOf(totalNumber));
        report.put("LogBuffer", logBuffer);

        if ((disableMode > 0 && number == 0) || (disableMode < 0 && number > 0))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int numberLogs, level = 0;
        List logBuffer;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.remove("LogBuffer")) != null && o instanceof List) {
            logBuffer = (List) o;
        }
        else
            logBuffer = new ArrayList();
        numberLogs = logBuffer.size();

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                exceptionCount = 0;
                logBuffer.clear();
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                previousNumber = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            level = Event.INFO;
            if (previousStatus != status) { // reset count and adjust step
               exceptionCount = 0;
               if (normalStep > 0)
                   step = normalStep;
            }
          case TimeWindows.EXCEPTION: // exception
            actionCount = 0;
            previousNumber = 0;
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
                previousNumber = 0;
                exceptionCount = 0;
            }
          default: // normal cases
            level = Event.INFO;
            exceptionCount = 0;
            if (status != TimeWindows.BLACKOUT &&
                previousStatus == TimeWindows.BLACKOUT) { // reset actionCount
                actionCount = 0;
                previousNumber = 0;
            }
            actionCount ++;
            if (numberLogs > 0) {
                if (previousNumber == 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                    if (errorIgnored > 0 && numberLogs > errorIgnored &&
                        previousNumber <= errorIgnored) // upgrade at once
                            level = Event.CRIT;
                    else if (errorIgnored < 0 && numberLogs <= -errorIgnored)
                        level = Event.WARNING;
                }
                previousNumber = numberLogs;
            }
            else if (previousNumber > 0) {
                if ((maxSessionCount <= 0 || actionCount > maxSessionCount)) {
                    actionCount = 0;
                    previousNumber = 0;
                }
                if (normalStep > 0)
                    step = normalStep;
            }
            else {
                previousNumber = 0;
            }
            break;
        }
        previousStatus = status;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            break;
          case Event.ERR: // found matching logs that may be ignored
            count = actionCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                if (count == 1 || count == tolerance) // react only 
                    break;
                logBuffer.clear();
                return null;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                if (count > maxRetry + maxPage) {
                    logBuffer.clear();
                    return null;
                }
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // exceptions or downgrades
            if (exceptionTolerance >= 0 && exceptionCount > exceptionTolerance){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    if (count > maxRetry + maxPage) {
                        logBuffer.clear();
                        return null;
                    }
                    level = Event.CRIT;
                }
            }
            else if (actionCount > 1 || exceptionCount > 1) {
                logBuffer.clear();
                return null;
            }
            break;
          default:
            if (actionCount > 1 || exceptionCount > 1) {
                logBuffer.clear();
                return null;
            }
            break;
        }

        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
            strBuf.append("'" + uri);
            strBuf.append("' has ");
            strBuf.append(numberLogs + " new matching entries");
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
        event.setAttribute("numberLogs", String.valueOf(numberLogs));
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (numberLogs > 0)
            event.setAttribute("lastEntry",(String)logBuffer.get(numberLogs-1));
        else
            event.setAttribute("lastEntry", "");
        if (numberLogs > 1)
            event.setAttribute("firstEntry", (String) logBuffer.get(0));
        else
            event.setAttribute("firstEntry", "");

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if ((! verifyLastlog) || (verifyLastlog && isLastlog)) {
                    if (actionGroup.isActive(currentTime, event))
                        actionStatus = "executed";
                    else
                        actionStatus = "skipped";
                }
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        if (numberLogs > 0 && ePatternGroup.length > 0) { // check escalations
            for (Object entry : logBuffer) {
                if (MonitorUtils.filter((String) entry, ePatternGroup,pm,true)){
                    event.setAttribute("firstEntry", (String) entry);
                    if ("executed".equals(actionStatus))
                        actionStatus = "escalated";
                    else if ("skipped".equals(actionStatus))
                        actionStatus = "matched";
                    else
                        actionStatus = "selected";
                    break;
                }
            }
            if ("escalated".equals(actionStatus)) {
                event.setPriority(Event.ALERT);
                if (!actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                event.setPriority(level);
            }
            else if ("matched".equals(actionStatus)) {
                event.setPriority(Event.ALERT);
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "escalated";
                event.setPriority(level);
            }
        }

        event.setAttribute("actionScript", actionStatus);
        event.send();

        logBuffer.clear();

        if ("skipped".equals(actionStatus) || "matched".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) { // execution
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);

            if (advanceLog) try {
                generateReport(currentTime);
            }
            catch (IOException ex) {
            }
        }
        else if ("secalated".equals(actionStatus)) { // escalation
            actionGroup.enableActionScript();
            event.setPriority(Event.ALERT);
            actionGroup.invokeAction(currentTime, event);
            event.setPriority(level);

            if (advanceLog) try {
                generateReport(currentTime);
            }
            catch (IOException ex) {
            }
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousNumber", String.valueOf(previousNumber));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pNumber, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousNumber")) != null)
            pNumber = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousNumber = pNumber;
    }

    public void destroy() {
        super.destroy();
        if (newlog != null) {
            newlog.close();
            newlog = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
