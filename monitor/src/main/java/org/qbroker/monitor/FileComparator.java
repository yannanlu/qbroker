package org.qbroker.monitor;

/* FileComparator.java - a monitor comparing two directories */

import java.util.Map;
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
import org.qbroker.common.TimeoutException;
import org.qbroker.common.RunCommand;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;

/**
 * FileComparator uses rsync to compare checksums between two sets of files
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FileComparator extends Monitor {
    private String script, uri, baseDir;
    private int maxDiffs, scriptTimeout;
    private int previousNumber;
    private Pattern bPattern = null, tPattern = null;

    public FileComparator(Map props) {
        super(props);
        Object o;
        int n;

        if (type == null)
            type = "FileComparator";

        if (description == null)
            description = "compare checksums between two sets of files";

        if ((o = MonitorUtils.select(props.get("DiffScript"))) == null)
            throw(new IllegalArgumentException("DiffScript is not defined"));
        script = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("ScriptTimeout")) == null ||
            (scriptTimeout = 1000*Integer.parseInt((String) o)) < 0)
            scriptTimeout = 60000;

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        if ((o = MonitorUtils.select(props.get("BaseDir"))) == null)
            throw(new IllegalArgumentException("BaseDir is not defined"));
        baseDir = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("MaxDiffs")) == null ||
            (maxDiffs = Integer.parseInt((String) o)) < 0)
            maxDiffs = 0;

        try {
            Perl5Compiler pc = null;
            if ((o = props.get("XPatternGroup")) != null && o instanceof List) {
                pc = new Perl5Compiler();
                pm = new Perl5Matcher();
                xPatternGroup=MonitorUtils.getPatterns("XPatternGroup",
                    props, pc);
                if ((o = props.get("StartBoundaryPattern")) != null)
                tPattern = pc.compile(MonitorUtils.substitute((String) o,
                    template));
                if ((o = props.get("EndBoundaryPattern")) != null)
                bPattern = pc.compile(MonitorUtils.substitute((String) o,
                    template));
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        previousNumber = -1;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws TimeoutException {

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

        String output = RunCommand.exec(script, scriptTimeout);

        if (output != null && output.length() > 0) {
            String line;
            int i=0, j=0, n=0, k;
            List<String> list = new ArrayList<String>();

            if (tPattern != null)
                n = 0;
            else
                n = 1;
            if ((i = output.indexOf('\n')) < 0) {
                j = i + 1;
            }
            while (i >= j) {
                line = output.substring(j, i);
                j = i + 1;
                i += 1 + (output.substring(j)).indexOf('\n');
                if (n == 0 && tPattern != null && pm.contains(line, tPattern)) {
                    n = 1;
                    continue;
                }
                else if (n > 0 && bPattern != null &&
                    pm.contains(line, bPattern)) {
                    break;
                }
                if (n == 0 || line == null || line.length() == 0)
                    continue;
                if (!MonitorUtils.filter(line, xPatternGroup, pm, false))
                    list.add(line);
            }

            report.put("FileList", list);
            if ((disableMode > 0 && list.size() > 0) ||
                (disableMode < 0 && list.size() == 0))
                skip = DISABLED;
            return report;
        }
        if (disableMode < 0)
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, numberFiles;
        List fileList;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("FileList")) != null && o instanceof List) {
            fileList = (List) o;
            numberFiles = fileList.size();
        }
        else {
            fileList = null;
            numberFiles = -1;
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
            if (numberFiles > 0) {
                if (previousNumber == 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.WARNING;
                    if (step > 0)
                        step = 0;
                    if (numberFiles > maxDiffs) // upgrade
                        level = Event.ERR;
                }
            }
            else if (previousNumber > 0) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousNumber = numberFiles;
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
            strBuf.append(numberFiles + " files out of sync on ");
            strBuf.append("'" + uri + "'");
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
        event.setAttribute("numberFiles", String.valueOf(numberFiles));
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (numberFiles > 0) {
            event.setAttribute("fileList", getDetails(currentTime, fileList));
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

    private String getDetails(long currentTime, List list) {
        int i, j, n;
        StringBuffer strBuf = new StringBuffer();
        n = list.size();
        for (i=0; i<n; i++) {
            if (i > 0)
                strBuf.append("\n\t ");
            strBuf.append(baseDir + "/");
            strBuf.append((String) list.get(i));
        }
        return strBuf.toString();
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
