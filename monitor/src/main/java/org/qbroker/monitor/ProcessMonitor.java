package org.qbroker.monitor;

/* ProcessMonitor.java - a monitor watching an OS process */

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.RunCommand;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;

/**
 * ProcessMonitor monitors an OS process with specific patterns
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ProcessMonitor extends Monitor {
    private String psCommand;
    private File psFile;
    private int previousNumber, psTimeout;
    private Pattern pattern, pidPattern;

    public ProcessMonitor(Map props) {
        super(props);
        Object o;
        List list;
        String str;
        int n;

        if (type == null)
            type = "ProcessMonitor";

        if (description == null)
            description = "monitor an OS process";

        if ((o = MonitorUtils.select(props.get("PSCommand"))) != null)
            psCommand = (String) o;
        else
            psCommand = "/usr/ucb/ps -auxwww";

        if ((o = props.get("PSTimeout")) == null ||
            (psTimeout = 1000*Integer.parseInt((String) o)) < 0)
            psTimeout = 60000;

        if ((o = MonitorUtils.select(props.get("PSFile"))) != null)
            psFile = new File((String) o);
        else
            psFile = null;

        if ((o = MonitorUtils.select(props.get("PidPattern"))) != null)
            str = (String) o;
        else
            str = "\\w+\\s+(\\d+)";

        if ((o = props.get("PatternGroup")) == null||!(o instanceof List))
            throw(new IllegalArgumentException("PatternGroup is not defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pattern = pc.compile("\\n");
            pidPattern = pc.compile(str);
            pm = new Perl5Matcher();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        if (aPatternGroup.length <= 0)
            throw(new IllegalArgumentException("PatternGroup is empty"));

        previousNumber = -1;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws TimeoutException {
        List<String> lineBuffer = new ArrayList<String>();

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

        String output = RunCommand.exec(psCommand, psTimeout);

        if (psFile != null) {
            StringBuffer strBuf = new StringBuffer();
            BufferedReader in = null;
            for (int i=0; i<psTimeout; i+=2000) {
                if (psFile.length() > 0L)
                    break;
                try {
                    Thread.sleep(2000);
                }
                catch (Exception e) {
                }
            }
            try {
                String line;
                in = new BufferedReader(new InputStreamReader(
                    new FileInputStream(psFile)));
                while ((line = in.readLine()) != null) {
                    strBuf.append(line + "\n");
                }
            }
            catch (IOException e) {
                try {
                    if (in != null)
                        in.close();
                    if (psFile.exists())
                        psFile.delete();
                }
                catch (IOException ex) {
                }
             throw(new TimeoutException("psFile failed: "+Event.traceStack(e)));
            }
            output = strBuf.toString();
            try {
                in.close();
                if (psFile.exists())
                    psFile.delete();
            }
            catch (IOException e) {
            }
        }

        if (output != null && output.length() > 0) {
            int i, k, n;
            StringBuffer strBuf = new StringBuffer();
            String line;
            List list = new ArrayList();
            Util.split(list, pm, pattern, output);
            n = list.size();
            k = 0;
            for (i=0; i<n; i++) {
                line = (String) list.get(i);
                if (line == null || line.length() == 0)
                    continue;
                if (MonitorUtils.filter(line, aPatternGroup, pm, true) &&
                    !MonitorUtils.filter(line, xPatternGroup, pm, false) &&
                    pm.contains(line, pidPattern)) {
                    if (k++ > 0)
                        strBuf.append(" ");
                    strBuf.append(pm.getMatch().group(1));
                    lineBuffer.add(line);
                }
            }
            report.put("NumberPids", String.valueOf(k));
            report.put("Pids", strBuf.toString());
            report.put("PSLines", lineBuffer);
            if (statsLogger != null) {
                strBuf = new StringBuffer();
                strBuf.append(Event.dateFormat(new Date(currentTime)) +" ");
                strBuf.append(name + " ");
                strBuf.append(k + " " + getPid(lineBuffer.toString()));
                report.put("Stats", strBuf.toString());
                try {
                    statsLogger.log(strBuf.toString());
                }
                catch (Exception e) {
                }
            }
            if (disableMode < 0 && k > 0 || disableMode > 0 && k <= 0)
                skip = DISABLED;
            return report;
        }
        else {
            throw(new TimeoutException("empty output"));
        }
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int number = 0, level = 0;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("NumberPids")) != null &&
            o instanceof String) {
            number = Integer.parseInt((String) o);
        }

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
            if (number == 0) {
                if (previousNumber > 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousNumber == 0) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousNumber = number;
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
          case Event.ERR: // very late
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

        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
            strBuf.append("Process: '");
            strBuf.append(name);
            strBuf.append("' ");
            strBuf.append(((number>0) ? "is OK" : "is down"));
        }
        Event event = new Event(level, strBuf.toString());

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        event.setAttribute("numberPids", String.valueOf(number));
        if (number > 0)
            event.setAttribute("pids", (String) latest.get("Pids"));

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

    public int getPid(String line) {
        if (line == null || line.length() <= 0)
            return 0;

        if (pm.contains(line, pidPattern))
            return Integer.parseInt(pm.getMatch().group(1));
        else
            return 0;
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
}
