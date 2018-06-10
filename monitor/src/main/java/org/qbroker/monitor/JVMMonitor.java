package org.qbroker.monitor;

/* JVMMonitor.java - a monitor checking JVM resources */

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DataSet;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.ProcessMonitor;

/**
 * JVMMonitor monitors the resources of the JVM.  It watches two user specified
 * parameters of JVM, such as resident memory and/or CPU usage.  One of them
 * is the leading number with range defined.  The other is just for reference.
 * Via PSCommand, user can define which numbers to monitor.  The leading
 * number is always the second one defind in the Pattern.  JVMMonitor also
 * reports its free memory of JVM.  It can be used to trigger the garbage
 * collection on the JVM, etc.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JVMMonitor extends Monitor {
    private DataSet warningRange = null;
    private DataSet errorRange = null;
    private DataSet criticalRange = null;
    private MonitorReport reporter;
    private Pattern pattern;
    private TextSubstitution tSub = null;
    private Runtime runtime = null;
    private int previousLevel;
    private boolean isDouble = false;

    public JVMMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        int n;

        if (type == null)
            type = "JVMMonitor";

        if (description == null)
            description = "monitor JVM resources";

        if ((o = props.get("Pattern")) == null || !(o instanceof String))
            throw(new IllegalArgumentException("Pattern is not well defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            String ps = MonitorUtils.substitute((String) o, template);
            pattern = pc.compile(ps);
            if ((o = props.get("Substitution")) != null)
                tSub = new TextSubstitution((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if((o = props.get("CriticalRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            criticalRange = new DataSet((List) o);
            if (criticalRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if ((o = props.get("ErrorRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            errorRange = new DataSet((List) o);
            if (errorRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if ((o = props.get("WarningRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            warningRange = new DataSet((List) o);
            if (warningRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if (criticalRange == null && errorRange == null && warningRange == null)
            throw(new IllegalArgumentException("no number range defined"));

        h.put("Name", name);
        h.put("Timeout", (String) props.get("Timeout"));
        h.put("Step", "1");
        if ((o = props.get("PidPattern")) != null)
            h.put("PidPattern", o);
        if ((o = props.get("PSCommand")) != null)
            h.put("PSCommand", o);
        else
            h.put("PSCommand", "/bin/ps -o user,rss,pcpu,args -p " +
                Event.getPID());
        if ((o = props.get("PSFile")) != null)
            h.put("PSFile", o);
        if ((o = props.get("PatternGroup")) != null)
            h.put("PatternGroup", o);
        h.put("PSTimeout", (String) props.get("Timeout"));
        reporter = new ProcessMonitor(h);

        runtime = Runtime.getRuntime();

        previousLevel = -10;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int returnCode = -1;
        long leadingNumber = -1L, freeMemory, totalMemory;
        double doubleNumber = -1;
        Map<String, Object> r = null;
        List dataBlock = new ArrayList();

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

        freeMemory = runtime.freeMemory() / 1024;
        totalMemory = runtime.totalMemory() / 1024;

        try {
            r = reporter.generateReport(currentTime);
        }
        catch (Exception e) {
            throw(new IOException("failed to get report: " +
                Event.traceStack(e)));
        }

        if (r.get("NumberPids") != null) {
            returnCode = Integer.parseInt((String) r.get("NumberPids"));
            if (returnCode > 0)
                dataBlock = (List) r.get("PSLines");
        }
        else {
            throw(new IOException("ps failed on " + name));
        }

        String str;
        int i, n = dataBlock.size();
        if (n <= 0) {
            throw(new IOException("test failed on " + name +
                ": " + returnCode));
        }
        for (i=0; i<n; i++) {
            long extraNumber = -1L;
            long number = -1L;
            double f = 0;
            if (pm.contains((String) dataBlock.get(i), pattern)) {
                MatchResult mr = pm.getMatch();
                if (mr.groups() > 2) try {
                    str = (tSub == null) ? mr.group(2) :
                        tSub.substitute(mr.group(2));
                    extraNumber = Long.parseLong(mr.group(1));
                    if (isDouble)
                        f = Double.parseDouble(str);
                    else
                        number = Long.parseLong(str);
                }
                catch (Exception e) {
                    throw(new IOException("bad number: " + e.getMessage()));
                }
                else
                    continue;
            }
            else {
                throw(new IOException("failed to match number: " +
                    (String) dataBlock.get(i)));
            }
            if (isDouble && f > doubleNumber) {
                doubleNumber = f;
                report.put("LeadingBlock", (String) dataBlock.get(i));
                report.put("ExtraNumber", String.valueOf(extraNumber));
                report.put("LeadingNumber", String.valueOf(doubleNumber));
            }
            else if (!isDouble && number > leadingNumber) {
                leadingNumber = number;
                report.put("LeadingBlock", (String) dataBlock.get(i));
                report.put("ExtraNumber", String.valueOf(extraNumber));
                report.put("LeadingNumber", String.valueOf(leadingNumber));
            }
        }

        report.put("FreeMemory", String.valueOf(freeMemory));
        report.put("TotalMemory", String.valueOf(totalMemory));
        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append((String) report.get("LeadingBlock") + " ");
            strBuf.append(name + " ");
            strBuf.append(Event.getPID() + " ");
            strBuf.append((String) report.get("ExtraNumber") + " ");
            strBuf.append((totalMemory - freeMemory) + " ");
            strBuf.append(freeMemory + " ");
            if (isDouble)
                strBuf.append(doubleNumber);
            else
                strBuf.append(leadingNumber);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long leadingNumber = -1L, freeMemory, totalMemory;
        double doubleNumber = -1;
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if ((o = latest.get("LeadingNumber")) != null) {
            if (isDouble)
                doubleNumber = Double.parseDouble((String) o);
            else
                leadingNumber = Long.parseLong((String) o);
        }

        if ((o = latest.get("FreeMemory")) != null)
            freeMemory = Long.parseLong((String) o);
        else
            freeMemory = -1L;

        if ((o = latest.get("TotalMemory")) != null)
            totalMemory = Long.parseLong((String) o);
        else
            totalMemory = -1L;

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
            strBuf.append("'" + name);
            strBuf.append("' is not being checked due to blackout");
            break;
          default: // normal cases
            exceptionCount = 0;
            if (criticalRange != null && ((isDouble &&
                criticalRange.contains(doubleNumber)) || (!isDouble &&
                criticalRange.contains(leadingNumber)))) {
                level = Event.CRIT;
                if (previousStatus != status || step > 0) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (step > 0)
                        step = 0;
                }
                actionCount ++;
                strBuf.append("the number for JVM is out of range: ");
                if (isDouble)
                    strBuf.append(doubleNumber);
                else
                    strBuf.append(leadingNumber);
            }
            else if (errorRange != null && ((isDouble &&
                errorRange.contains(doubleNumber)) || (!isDouble &&
                errorRange.contains(leadingNumber)))) {
                level = Event.ERR;
                if (previousStatus != status || step > 0) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (step > 0)
                        step = 0;
                }
                actionCount ++;
                strBuf.append("the number for JVM is out of range: ");
                if (isDouble)
                    strBuf.append(doubleNumber);
                else
                    strBuf.append(leadingNumber);
            }
            else if (warningRange != null && ((isDouble &&
                warningRange.contains(doubleNumber)) || (!isDouble &&
                warningRange.contains(leadingNumber)))) {
                level = Event.WARNING;
                if (previousStatus != status || previousLevel != level) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (normalStep > 0)
                        step = normalStep;
                }
                actionCount ++;
                strBuf.append("the number is: ");
                if (isDouble)
                    strBuf.append(doubleNumber);
                else
                    strBuf.append(leadingNumber);
            }
            else if (previousStatus == status && previousLevel == Event.INFO) {
                // always normal
                return null;
            }
            else { // just back to normal
                level = Event.INFO;
                actionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("the number is in the normal range: ");
                if (isDouble)
                    strBuf.append(doubleNumber);
                else
                    strBuf.append(leadingNumber);
            }
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very large
            count = actionCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                break;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either large or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
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
        previousStatus = status;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        event.setAttribute("leadingNumber", ((isDouble) ?
            String.valueOf(doubleNumber) : String.valueOf(leadingNumber)));
        event.setAttribute("freeMemory", String.valueOf(freeMemory));
        event.setAttribute("usedMemory",
            String.valueOf(totalMemory-freeMemory));
        event.setAttribute("extraNumber", (String) latest.get("ExtraNumber"));
        event.setAttribute("percentUsed", String.valueOf((100L -
           100L*freeMemory/totalMemory)));

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));

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
        chkpt.put("PreviousLevel", String.valueOf(previousLevel));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pStatus, sNumber, pLevel;
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

        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousLevel = pLevel;
    }

    public void destroy() {
        super.destroy();
        if (reporter != null) {
            reporter.destroy();
            reporter = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
