package org.qbroker.monitor;

/* WebOperator.java - a monitor checking the content of a web page */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;

/**
 * WebOperator responses to operation problems of a web server, such as
 * it hangs or it is down.  If SessionInitializer is defined, it will get
 * the session cookie and maintains the session while testing.
 *<br/><br/>
 * WebOperator may get READFAILED error sometimes. ReadErrorIgnored is used to
 * ignore the error if it is larger than zero.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class WebOperator extends Monitor {
    private String uri;
    private WebTester httpTester, sessionInitializer = null;
    private int previousWebStatus, webStatusOffset, readErrorIgnored;
    private long sessionTimeout = 0, previousTime = 0;
    private String sessionCookie = null;
    private Pattern cookiePattern = null;
    private final static String webStatusText[] = {"Test failed",
        "Server is OK", "Protocol error", "Pattern not matched",
        "Not Multipart", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Server is down"};

    public WebOperator(Map props) {
        super(props);
        Object o;
        URI u;
        int n;
        Map<String, Object> h;

        if (type == null)
            type = "WebOperator";

        if (description == null)
            description = "monitor a web server";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        // convert the fields of ##MM## into __MM__ to avoid URI Exception
        Template pathTemplate = new Template(uri, "##[a-zA-Z]+##");
        if (pathTemplate.numberOfFields() > 0) {
            String [] allFields = pathTemplate.getAllFields();
            for (int i=0; i<allFields.length; i++) {
                uri = pathTemplate.substitute(allFields[i],
                    "__" + allFields[i] + "__", uri);
            }
        }

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"http".equals(u.getScheme()) && !"tcp".equals(u.getScheme()) &&
            !"https".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((o = props.get("ReadErrorIgnored")) != null)
            readErrorIgnored = Integer.parseInt((String) o);
        else
            readErrorIgnored = 0;

        // for session initializer
        if((o = props.get("SessionInitializer")) != null &&
            o instanceof Map) try {
            sessionInitializer = new WebTester((Map) o);
            if ((o = props.get("SessionTimeout")) != null)
                sessionTimeout = 1000*Long.parseLong((String) o);
            else
                sessionTimeout = 7200000;
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            cookiePattern = pc.compile("Set-cookie:\\s+([^;]+);");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed init session: " +
                e.toString()));
        }

        h = Utils.cloneProperties(props);
        h.put("URI", uri);
        h.put("Step", "1");
        h.remove("DependencyGroup");
        h.remove("DisabeMode");
        h.remove("ActiveTime");
        h.remove("SessionInitializer");
        h.remove("SessionTimeout");
        httpTester = new WebTester(h);

        previousWebStatus = -10;
        webStatusOffset = 0 - WebTester.TESTFAILED;
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

        if (sessionInitializer != null) try { // session maintainence
            if (sessionCookie == null ||
                currentTime - previousTime >= sessionTimeout) {
                Map<String, Object> rpt;
                rpt = sessionInitializer.generateReport(currentTime);
                int i = Integer.parseInt((String) rpt.get("ReturnCode"));
                sessionCookie = null;
                if (i == WebTester.TESTOK) {
                    int j;
                    String str = (String) rpt.get("Response"); 
                    if (str != null && (j = str.indexOf("\r\n\r\n")) > 0)
                        str = str.substring(0, j);
                    while ((j = str.indexOf("Set-cookie:")) > 0) {
                        if (pm.contains(str, cookiePattern)) { // got match
                            MatchResult mr = pm.getMatch();
                            if (sessionCookie == null)
                                sessionCookie = mr.group(1);
                            else
                                sessionCookie += "; " + mr.group(1);
                        }
                        str = str.substring(j+11);
                    }
                }
                if (sessionCookie != null) { // reset the cookie
                    httpTester.setCookie(sessionCookie);
                    previousTime = currentTime;
                }
                else // failed to init session
                    throw(new Exception("failed to get session cookie: " +
                        webStatusText[i + webStatusOffset]));
            }
        }
        catch (Exception e) {
            skip = EXCEPTION;
            report.put("Exception", e);
            return report;
        }

        report = httpTester.generateReport(currentTime);

        if (disableMode != 0) {
            int returnCode =
                Integer.parseInt((String) report.get("ReturnCode"));

            if (returnCode == WebTester.TESTOK)
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else if (returnCode == WebTester.PATTERNNOHIT)
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
            else
                skip = EXCEPTION;
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int webStatus, level = 0;
        long size;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("ReturnCode")) != null &&
            o instanceof String) {
            webStatus = Integer.parseInt((String) o);
        }
        else
            webStatus = WebTester.TESTFAILED;

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
            if (sessionInitializer != null) // invalid session
                sessionCookie = null;
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
            if (webStatus != WebTester.TESTOK) {
                if (previousWebStatus == WebTester.TESTOK)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                    if (webStatus == WebTester.READFAILED) { // check downgrade
                        if (readErrorIgnored>0 && actionCount<=readErrorIgnored)
                            level = Event.WARNING;
                    }
                }
            }
            else if (previousWebStatus != WebTester.TESTOK) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousWebStatus = webStatus;
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

        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
            strBuf.append(name + ": ");
            strBuf.append(webStatusText[webStatus + webStatusOffset]);
        }

        Event event = new Event(level, strBuf.toString());
        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
       event.setAttribute("webStatus",webStatusText[webStatus+webStatusOffset]);
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
        chkpt.put("PreviousWebStatus", String.valueOf(previousWebStatus));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, wStatus, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousWebStatus")) != null)
            wStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousWebStatus = wStatus;
    }

    public void destroy() {
        super.destroy();
        if (httpTester != null) {
            httpTester.destroy();
            httpTester = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
