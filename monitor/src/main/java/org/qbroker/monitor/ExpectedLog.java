package org.qbroker.monitor;

/* ExpectedLog.java - a monitor watching timestamp of the new entries */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.NewlogFetcher;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.ScriptLauncher;

/**
 * ExpectedLog monitors a generic Unix log file and expects new log entries
 * with certain patterns showing up frequently. In case it fails to find those
 * log entries within a certain period, ExpectedLog treats the log late and
 * raises alerts.
 *<br/><br/>
 * Use ExpectedLog to detect the new occurrence of log entries matching one of
 * the given patterns.  ExpectedLog may be repetitively used to check the log
 * file.  It will pick up the new log entries only.  It also guarantees the
 * order of the occurrences.  If a certain log entry appears, the new entry will
 * be stored in the buffer for analysis.  The buffer will be updated every time.
 *<br/><br/>
 * ExpectedLog supports excluding patterns to exclude the certain log entries
 * that are not interesting.  The monitor is fully configurable.  In case it
 * fails to find any log to match the predefined patterns, it will use the
 * previous timestamp for evaluations. If ExpectedLog finds no match
 * at the start up, the monitor will disable itself for only once to delay the
 * evaluations.  If there is still no log found next time, the default
 * timestamp of the log file will be used for evaluations.
 *<br/><br/>
 * ExpectedLog also supports time correlations and size correlations between
 * the reference file and the log entries that match to the patterns.
 * The reference file controls the correlation process.  Whenever the reference
 * file is updated or modified, ExpectedLog adjusts its timer and correlates
 * this change with the target log entries.  If the log has any new target log
 * entries as expected, the ExpectedLog treats it OK.  Otherwise, it will send
 * alerts according to the predefined tolerance on the lateness of the target
 * logs.
 *<br/><br/>
 * In order to configure ExpectedLog to do time correlations, you have to
 * specify a map named reference in its property hash. The reference map
 * contains most of the properties required by a FileMonitor object, such as
 * Filename, Name, etc.  The tolerance of the lateness will be controlled
 * by the threshold parameters.  In fact, ExpectedLog will create a separate
 * instance for the reference file.  The method of performAction() will actually
 * do the time correlations between two objects.
 *<br/><br/>
 * In case of the size correlations, you must specify the triggerSize in the
 * property map.  The triggerSize is zero or any positive number that
 * defines two different states, on and off.  In on state, the size of the
 * reference file is no less than the triggerSize.  In off state, the size of
 * the reference file is less than the triggerSize and the monitor is in
 * Blackout state.  Therefore, the size of the reference file acts like a
 * switch.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ExpectedLog extends Monitor {
    private String uri, referenceName, lastEntry;
    private int maxNumberLogs, updateCount, previousLevel, logSize;
    private int sleepTime, previousTrigger, triggerSize, timeOffset;
    private int triggerStatus, maxScannedLogs, debug;
    private long previousLeadTime,timeDifference,previousTime,previousRefTime;
    private NewlogFetcher newlog;
    private MonitorReport reference;
    private ScriptLauncher script;
    private TimeWindows tw = null;
    private boolean verifyLastlog, isLastlog, oldLogFile;
    public final static int MAXNUMBERLOGS = 40960;
    public final static int MAXSCANNEDLOGS = 40960;

    @SuppressWarnings("unchecked")
    public ExpectedLog(Map props) {
        super(props);
        Object o;
        URI u;
        String s, logfile, className;
        int n;

        if (type == null)
            type = "ExpectedLog";

        if (description == null)
            description = "monitor timestamp of certain log entries";

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
            pm = new Perl5Matcher();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed" + e.getMessage()));
        }
        if (aPatternGroup.length <= 0)
            throw(new IllegalArgumentException("PatternGroup is empty"));

        if ((o = props.get("TestScript")) != null) {
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("Name", name);
            h.put("Script", o);
            if ((o = props.get("ScriptTimeout")) != null)
                h.put("ScriptTimeout", o);
            if ((o = props.get("ScriptXPatternGroup")) != null)
                h.put("XPatternGroup", o);
            script = new ScriptLauncher(h);

            if ((o = props.get("SleepTime")) == null ||
                (sleepTime = 1000 * Integer.parseInt((String) o)) < 0)
                sleepTime = 0;
        }
        else
            script = null;

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        timeOffset = 0;
        tolerance = 0;
        triggerSize = -1;
        if ((o = props.get("Reference")) != null) {
            if (!(o instanceof Map))
              throw(new IllegalArgumentException("Reference is not a Map"));
            Map<String, Object> ref = Utils.cloneProperties((Map) o);
            if (ref.get("Reference") != null)
                ref.remove("Reference");
            if (ref.get("ActionProgram") != null)
                ref.remove("ActionProgram");
            if (ref.get("MaxRetry") != null)
                ref.remove("MaxRetry");
            ref.put("Step", "1");
            referenceName = (String) ref.get("Name");

            if ((o = ref.get("ClassName")) != null)
                className = (String) o;
            else if ((o = ref.get("Type")) != null)
                className = "org.qbroker.monitor." + (String) o;
            else
                className = "org.qbroker.monitor.AgeMonitor";
            try {
                java.lang.reflect.Constructor con;
                Class<?> cls = Class.forName(className);
                con = cls.getConstructor(new Class[]{Map.class});
                reference = (MonitorReport) con.newInstance(new Object[]{ref});
            }
            catch (InvocationTargetException e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e.getTargetException())).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e)).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }

            if ((o = props.get("TimeOffset")) == null ||
                (timeOffset = 1000 * Integer.parseInt((String) o)) < 0)
                timeOffset = 30000;
            if ((o = props.get("TriggerSize")) == null ||
                (triggerSize = Integer.parseInt((String) o)) < -1)
                triggerSize = -1;
            if ((o = props.get("Tolerance")) != null)
                tolerance = Integer.parseInt((String) o);
        }
        else
            reference = null;

        if (referenceName == null)
            referenceName = "";

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = 0;

        verifyLastlog = false;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            o = props.get("VerifyLastLog");
            if (o != null && Integer.parseInt((String) o) != 0)
                verifyLastlog = true;
        }

        if ((o = props.get("MaxNumberLogs")) == null ||
            (maxNumberLogs = Integer.parseInt((String) o)) <= 0)
            maxNumberLogs = MAXNUMBERLOGS;

        if ((o = props.get("MaxScannedLogs")) == null ||
            (maxScannedLogs = Integer.parseInt((String) o)) <= 0)
            maxScannedLogs = MAXSCANNEDLOGS;

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

        previousTime = newlog.getTimestamp();
        previousLevel = -1;
        previousTrigger = -1;
        previousLeadTime = previousTime;
        previousRefTime = -1L;
        updateCount = 0;
        lastEntry = "";
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

        int i, k, number = 0, n = 0;
        if (script != null) {
            try {
                report = script.generateReport(currentTime);
            }
            catch (TimeoutException e) {
                throw(new IOException("script timedout: " + e.toString()));
            }
            catch (Exception e) {
                throw(new IOException("script exceptioned: " + e.toString()));
            }
            n = Integer.parseInt((String) report.get("ReturnCode"));
            if (n != 0)
                throw(new IOException("script failed(" + n + "): " +
                    (String) report.get("Output")));
            if (sleepTime > 0) {
                try {
                    Thread.sleep((long) sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }

        if (reference != null) try {
            report = reference.generateReport(currentTime);
            if (report.get("LastEntry") != null)
                report.remove("LastEntry");
        }
        catch (Exception e) {
            throw(new IOException("reference failed: " + Event.traceStack(e)));
        }

        long pos, t, tt = 0L, l, mtime = previousTime;
        StringBuffer strBuf = new StringBuffer();
        String line;
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
                if (MonitorUtils.filter(entry, aPatternGroup, pm, true) &&
                    !MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                    tt = newlog.getTimestamp(entry);
                    lastEntry = entry;
                    number ++;
                }
            }
        }

        if (serialNumber > 1) // reset limit on number logs to scan
            maxLogs = maxScannedLogs;

        n = 0;
        k = 0;
        while ((line = newlog.getLine()) != null) {
            l = (long) line.length();
            if ((t = newlog.getTimestamp(line)) >= 0) { // valid log
                if ((k++ % 100) == 0) {
                    if ((debug & NewlogFetcher.DEBUG_NEWLOG) > 0)
                        System.out.println((k-1) + ": " +
                            line.substring(0, 48) + "!" + t);
                    if (k >= maxLogs) // stop scanning
                        break;
                }
                if (n > 0) { // matching the patterns
                    String entry = strBuf.toString();
                    strBuf = new StringBuffer();
                    if (MonitorUtils.filter(entry, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                        tt = t;
                        lastEntry = entry;
                        number ++;
                    }
                }
                if (number >= maxNumberLogs) {
                    n = 0;
                    newlog.seek(pos);
                    break;
                }
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
        if (n > 0) {
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
            if ((debug & NewlogFetcher.DEBUG_NEWLOG) > 0 && (--k % 100) > 0)
                System.out.println(k + ": " + entry.substring(0, 48) + "!" +
                    newlog.getTimestamp(entry.substring(0, 48)));
            if (MonitorUtils.filter(entry, aPatternGroup, pm, true) &&
                !MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                tt = newlog.getTimestamp(entry);
                lastEntry = entry;
                number ++;
            }
        }
        newlog.close();
        newlog.saveReference();
        if (number > 0) { // found match
            if (number < maxNumberLogs)
                isLastlog = true;
            mtime = tt - timeDifference;
        }
        // else to use the previousTime

        if (reference != null) {
            report.put("LatestTime", new long[] {mtime});
            report.put("NumberLogs", new long[] {number});
        }
        else {
            report.put("SampleTime", new long[] {mtime});
            report.put("SampleSize", new long[] {number});
        }
        if (number > 0)
            report.put("LastEntry", lastEntry);
        else if (serialNumber == 1) { // disabled at startup if no log found
            skip = DISABLED;
            return report;
        }

        if (disableMode != 0) { // for dependencies
            if (reference != null) { // compare against reference
                Object o;
                if ((o = report.get("SampleTime")) != null &&
                    o instanceof long[] && ((long[]) o).length > 0) {
                    tt = ((long[]) o)[0];
                    if (mtime - tt > - timeOffset) // caught up
                        skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    else // not caught up yet
                        skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
                else {
                    previousTime = mtime;
                    throw(new IOException("failed to get reference time"));
                }
            }
            else if (tw != null) { // time window defined for no ref
                i = tw.getThresholdLength();
                if (i >= 2) switch (tw.check(currentTime, mtime)) {
                  case TimeWindows.NORMAL:
                  case TimeWindows.SLATE:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.ALATE:
                  case TimeWindows.ELATE:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
                else switch (tw.check(currentTime, mtime)) {
                  case TimeWindows.NORMAL:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.OCCURRED:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
            }
            else if (mtime > previousTime) // updated
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else // not updated yet
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
        }
        previousTime = mtime;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, numberLogs;
        long sampleTime;
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if (reference != null) {
            return correlate(status, currentTime, latest);
        }

        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        else
            sampleTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            numberLogs = (int) (((long[]) o)[0]);
        }
        else
            numberLogs = 0;

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
          case TimeWindows.NORMAL:
            if (previousStatus == status) { // always normal
                exceptionCount = 0;
                return null;
            }
            else { // just back to normal
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' has " + numberLogs);
                strBuf.append(" new matching log entries");
            }
            break;
          case TimeWindows.ELATE:
          case TimeWindows.ALATE: // very late
            level = Event.ERR;
            exceptionCount = 0;
            if (previousStatus < TimeWindows.ALATE) {//reset count & adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' has no matching log entries in the last ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes");
            break;
          case TimeWindows.SLATE: // somewhat late
            level = Event.WARNING;
            exceptionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' had matching log entries ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes ago");
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not being checked due to blackout");
            break;
          default: // should never reach here
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level) {
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level) {
                return null;
            }
            break;
        }
        previousStatus = status;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
            event.setAttribute("latestTime", "N/A");
            event.setAttribute("numberLogs", "N/A");
        }
        else {
            count = actionCount;
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(sampleTime)));
            event.setAttribute("numberLogs", String.valueOf(numberLogs));
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (numberLogs > 0) {
            event.setAttribute("lastEntry", (String) latest.get("LastEntry"));
        }
        else
            event.setAttribute("lastEntry", "");

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if ((! verifyLastlog) ||
                    (verifyLastlog && (isLastlog || numberLogs == 0))) {
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

    private Event correlate(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, numberLogs;
        long sampleTime, sampleSize, latestTime;
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        else
            sampleTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleSize = ((long[]) o)[0];
        }
        else
            sampleSize = 0L;

        if ((o = latest.get("LatestTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            latestTime = ((long[]) o)[0];
        }
        else
            latestTime = -1L;

        if ((o = latest.get("NumberLogs")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            numberLogs = (int) ((long[]) o)[0];
        }
        else
            numberLogs = 0;

        if (status >= TimeWindows.NORMAL) { // good test
            if (latestTime - sampleTime > - timeOffset) {
                if (triggerSize >= 0) { // trigger enabled
                    triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
                }
                status = TimeWindows.OCCURRED;
            }
            else if (triggerSize >= 0) { // trigger enabled but no update
                triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
            }
            if (triggerSize >= 0 && triggerStatus == 0) // turn it off
                status = TimeWindows.BLACKOUT;
        }

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.OCCURRED:
            if (previousStatus == status &&
                previousTrigger == triggerStatus) { // always OK
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just updated
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' has new matching log entries");
            }
            break;
          case TimeWindows.NORMAL:// just update
          case TimeWindows.SLATE: // somewhat late
          case TimeWindows.ALATE: // very late
            level = Event.INFO;
            exceptionCount = 0;
            if (previousStatus < TimeWindows.NORMAL ||
                previousStatus == TimeWindows.OCCURRED ||
                previousStatus >= TimeWindows.ELATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            if(latestTime > previousLeadTime || numberLogs > 0){//target updated
                actionCount = 0;
                updateCount = 0;
            }
            if (sampleTime > previousRefTime) // reference updated
                updateCount ++;

            if (status == TimeWindows.NORMAL)
                level = Event.INFO;
            else if (status == TimeWindows.SLATE)
                level = Event.WARNING;
            else
                level = Event.ERR;

            if (updateCount <= 1) { // only one update on reference
                if (previousLevel != level)
                    actionCount = 0;
            }
            else if (updateCount > tolerance) { // upgrade to ERR
                level = Event.ERR;
                if (previousLevel > level)
                    actionCount = 0;
            }
            else {
                level = Event.WARNING;
                if (previousLevel != level)
                    actionCount = 0;
            }
            actionCount ++;
            if (level == Event.INFO) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' has been updated recently");
                if (previousTrigger != triggerStatus)
                    strBuf.append(" with the state change");
            }
            else if (level == Event.WARNING) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' was updated ");
                if (previousTrigger != triggerStatus)
                    strBuf.append("with the state change ");
                strBuf.append((int) ((currentTime - sampleTime)/60000));
                strBuf.append(" minutes ago");
            }
            else {
                strBuf.append("'" + uri);
                strBuf.append("' has no matching log entries in the last ");
                strBuf.append((int) ((currentTime - latestTime)/60000));
                strBuf.append(" minutes");
            }
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not checked due to blackout");
            break;
          case TimeWindows.ELATE: // extremely late and do not care
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            exceptionCount = 0;
            actionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Reference: '");
            strBuf.append(referenceName);
            strBuf.append("' has not been updated since ");
            strBuf.append(Event.dateFormat(new Date(sampleTime)));
            break;
          default: // should never reach here
            break;
        }
        if (latestTime > 0L)
            previousLeadTime = latestTime;
        if (sampleTime > 0L)
            previousRefTime = sampleTime;

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousTrigger = triggerStatus;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousTrigger = triggerStatus;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus) { // slate or normal
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus) {
                return null;
            }
            break;
        }
        previousStatus = status;
        previousTrigger = triggerStatus;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        event.setAttribute("updateCount", String.valueOf(updateCount));
        event.setAttribute("reference", referenceName);
        if (sampleTime >= 0)
            event.setAttribute("referenceTime",
                Event.dateFormat(new Date(sampleTime)));
        else
            event.setAttribute("referenceTime", "N/A");
        if (sampleSize >= 0)
            event.setAttribute("referenceSize", String.valueOf(sampleSize));
        else
            event.setAttribute("referenceSize", "N/A");
        if (latestTime >= 0)
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(latestTime)));
        else
            event.setAttribute("latestTime", "N/A");
        if (numberLogs >= 0)
            event.setAttribute("numberLogs", String.valueOf(numberLogs));
        else
            event.setAttribute("numberLogs", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (numberLogs > 0)
            event.setAttribute("lastEntry", (String) latest.get("LastEntry"));
        else
            event.setAttribute("lastEntry", "");

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if ((! verifyLastlog) ||
                    (verifyLastlog && (isLastlog || numberLogs == 0))) {
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
        chkpt.put("PreviousLevel", String.valueOf(previousLevel));
        chkpt.put("PreviousTrigger", String.valueOf(previousTrigger));
        chkpt.put("PreviousTime", String.valueOf(previousTime));
        chkpt.put("PreviousLeadTime", String.valueOf(previousLeadTime));
        chkpt.put("PreviousRefTime", String.valueOf(previousRefTime));
        chkpt.put("UpdateCount", String.valueOf(updateCount));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, uCount, pLevel, pStatus, sNumber, pTrigger;
        long pTime, pLeadTime, pRefTime;
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

        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTrigger")) != null)
            pTrigger = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTime")) != null)
            pTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousLeadTime")) != null)
            pLeadTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousRefTime")) != null)
            pRefTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("UpdateCount")) != null)
            uCount = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousTime = pTime;
        previousLeadTime = pLeadTime;
        previousRefTime = pRefTime;
        previousLevel = pLevel;
        previousTrigger = pTrigger;
        updateCount = uCount;
    }

    public void destroy() {
        super.destroy();
        if (reference != null) {
            reference.destroy();
            reference = null;
        }
        if (newlog != null) {
            newlog.close();
            newlog = null;
        }
        if (script != null) {
            script.destroy();
            script = null;
        }
    }
}
