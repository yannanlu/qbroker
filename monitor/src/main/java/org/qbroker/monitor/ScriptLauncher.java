package org.qbroker.monitor;

/* ScriptLauncher.java - a script launcher */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;

/**
 * ScriptLauncher launches a script outside JVM and parses its stdout for
 * patterns. It supports time dependent command with __MM____dd__ and a single
 * secret in the form of __secret__.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ScriptLauncher extends Monitor {
    private String script, secret = null;
    private int previousError;
    private int scriptTimeout;
    private Pattern pattern;

    public ScriptLauncher(Map props) {
        super(props);
        Object o;
        int n;

        if (type == null)
            type = "ScriptLauncher";

        if (description == null)
            description = "launch a script";

        if ((o = MonitorUtils.select(props.get("Script"))) == null)
            throw(new IllegalArgumentException("Script is not defined"));
        script = MonitorUtils.substitute((String) o, template);

        if ((o = MonitorUtils.select(props.get("Secret"))) != null)
            secret = (String) o;
        else if((o=MonitorUtils.select(props.get("EncryptedSecret")))!=null)try{
            secret = Utils.decrypt((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("ScriptTimeout")) == null ||
            (scriptTimeout = 1000*Integer.parseInt((String) o)) < 0)
            scriptTimeout = 60000;

        aPatternGroup = null;
        xPatternGroup = null;
        if ((o = props.get("XPatternGroup")) != null && o instanceof List) try {
            Perl5Compiler pc = new Perl5Compiler();
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup", props,pc);
            if (xPatternGroup.length > 0)
                pattern = pc.compile("\\n");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        // check time dependency on cmd
        n = initTimeTemplate(script);
        if (n > 0)
            script = updateContent(System.currentTimeMillis());

        if (reportMode != REPORT_CACHED)
            previousError = -1;
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

        if (timeFormat.length > 0) { // time-depenendent command
            script = updateContent(currentTime);
        }

        String output;
        try {
            if (secret != null && script.indexOf("__secret__") > 0 &&
                timeTemplate != null) { // with secret defined in script
                String s = timeTemplate.substitute("secret", secret, script);
                output = RunCommand.exec(s, scriptTimeout);
            }
            else
                output = RunCommand.exec(script, scriptTimeout);
        }
        catch (Exception e) {
            throw(new TimeoutException("script failed"+Event.traceStack(e)));
        }

        if (output != null && output.length() > 0) {
            int i, n;
            if (xPatternGroup != null && xPatternGroup.length > 0) {
                String line;
                List list = new ArrayList();
                Util.split(list, pm, pattern, output);
                n = list.size();
                line = (String) list.get(n-1);
                if ((line == null || line.length() == 0) && n > 1)
                    n --;
                for (i=0; i<n; i++) {
                    line = (String) list.get(i);
                    if (!MonitorUtils.filter(line, xPatternGroup, pm, false))
                        break;
                }
                if (i >= n) { // no hit
                    report.put("ReturnCode", "0");
                    if (disableMode < 0)
                        skip = DISABLED;
                    return report;
                }
            }
            report.put("Output", output);
            report.put("ReturnCode", "1");
            if (disableMode > 0)
                skip = DISABLED;
            return report;
        }
        report.put("ReturnCode", "0");
        if (disableMode < 0)
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int errorCode = -1, level = 0;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("ReturnCode")) != null &&
            o instanceof String) {
            errorCode = Integer.parseInt((String) o);
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
            if (errorCode != 0) {
                if (previousError == 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousError != 0) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousError = errorCode;
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
            if(exceptionCount > exceptionTolerance && exceptionTolerance >=0){
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
            strBuf.append("Script: '");
            strBuf.append(name);
            if (errorCode != 0) {
                strBuf.append("': " + (String) latest.get("Output"));
                int l = strBuf.length();
                if (l > 0 && strBuf.charAt(l-1) == '\n')
                    strBuf.deleteCharAt(l-1);
            }
            else
                strBuf.append("' completed");
        }

        Event event = new Event(level, strBuf.toString());
        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("script", script);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("errorCode", String.valueOf(errorCode));
       event.setAttribute("testTime",Event.dateFormat(new Date(currentTime)));

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
        chkpt.put("PreviousError", String.valueOf(previousError));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pError, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousError")) != null)
            pError = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousError = pError;
    }
}
