package org.qbroker.monitor;

/* URLMonitor.java - a monitor checking the age of an URL */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;

/**
 * URLMonitor monitors a given web page on its last modified time and/or size
 *<br/><br/>
 * Use URLMonitor to get the update time and/or size of the given page.  It is
 * assumed that the page exists and is accessible all times.  The page should
 * contain update time stamp that will be parsed by URLMonitor.
 *<br/><br/>
 * You can use URLMonitor to monitor when the page has been updated.  If not,
 * how late it is.  In case of very late, URLMonitor sends alerts.
 *<br/><br/>
 * URLMonitor also supports mtime correlations and size correlations between
 * the page and other mtime objects.  In this case, there are two objects
 * involved.  One is the reference.  The other is the target page to be
 * monitored.  The reference controls the correlation process.  Whenever the
 * reference is updated or modified, URLMonitor adjusts its timer and
 * correlates this change with the target page.  If the target page has been
 * updated accordingly, the URLMonitor treats it OK.  Otherwise, it will
 * send alerts according to the predefined tolerance on the lateness of the
 * target page being updated.
 *<br/><br/>
 * In order to configure URLMonitor to do time correlations, you have to
 * specify a map named reference in its property map.  The reference
 * map contains most of the properties required by an mtime object, such as
 * uri, name, type,  etc.  The tolerance of the lateness will be controlled
 * by the threshold parameters.  In fact, URLMonitor will create a separate
 * instance for the reference.  The method of performAction() will actually
 * do the time correlations between two objects.
 *<br/><br/>
 * In case of the size correlations, you must specify the triggerSize in the
 * property map.  The triggerSize is zero or any positive number that
 * defines two different states.  One is the state that the page size is less
 * than the trigger_size.  The other is the opposite.  In case state of the
 * reference changes, URLMonitor will check the state of the target page.
 * If both are in the same states, URLMonitor thinks it OK.  Otherwise,
 * URLMonitor will send alerts according to the predefined tolerance on
 * the lateness of the target page keeping its state in sync.
 *<br/><br/>
 * In case that URLMonitor is used as a dependency with the disableMode enabled,
 * it checks mtime of the page to determine the skipping status. If CheckETag
 * is set to be true and the request is HEAD, it will only checks the etag to
 * determine the skipping status.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class URLMonitor extends Monitor {
    private String uri, referenceName;
    private WebTester webTester;
    private Pattern pattern;
    private long timeDifference, previousTime, previousRefTime;
    private long previousLeadTime;
    private int triggerSize, timeOffset, previousTrigger, updateCount;
    private int webStatusOffset, previousLevel;
    private DateFormat dateFormat;
    private MonitorReport reference;
    private TimeWindows tw = null;
    private String previousETag = "";
    private boolean checkETag = false;
    private final static String webStatusText[] = {"Exception",
        "Test OK", "Protocol error", "Pattern not matched",
        "Not Multipart", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Server is down"};

    public URLMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h;
        URI u;
        String className;
        int n;

        if (type == null)
            type = "URLMonitor";

        if (description == null)
            description = "monitor update time on a web page";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"http".equals(u.getScheme()) && !"https".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((o = props.get("DateFormat")) == null || !(o instanceof String))
          throw(new IllegalArgumentException("DateFormat is not well defined"));
        dateFormat = new SimpleDateFormat((String) o);

        if ((o = props.get("TimeZone")) != null)
            dateFormat.setTimeZone(TimeZone.getTimeZone((String) o));

        if ((o = props.get("Pattern")) == null || !(o instanceof String))
            throw(new IllegalArgumentException("Pattern is not well defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            String ps = MonitorUtils.substitute((String) o, template);
            pattern = pc.compile(ps);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        h = Utils.cloneProperties(props);
        h.put("URI", uri);
        h.put("Step", "1");
        h.remove("DependencyGroup");
        h.remove("DisabeMode");
        h.remove("ActiveTime");
        h.remove("DateFormat");
        h.remove("TimeZone");
        h.remove("TimeDifference");
        h.remove("Pattern");
        webTester = new WebTester(h);

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        if (disableMode != 0) {
            if ((o = props.get("ActiveTime")) != null) // init TimeWindows
                tw = new TimeWindows((Map) o);
        }

        previousTime = -1;
        previousLeadTime = -1;
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

        updateCount = 0;
        previousTrigger = -1;
        previousRefTime = -1L;
        previousLevel = -1;
        webStatusOffset = 0 - webTester.TESTFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long size = -1L;
        long mtime = -10L;
        int returnCode = -1;
        String etag = null;

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

        if (reference != null) {
            try {
                report = reference.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("reference failed: " +
                    Event.traceStack(e)));
            }
        }
        report.put("URI", uri);

        Map<String, Object> r = webTester.generateReport(currentTime);
        if (r.get("ReturnCode") != null) {
            returnCode = Integer.parseInt((String) r.get("ReturnCode"));
        }
        else {
            throw(new IOException("web test failed on " + uri));
        }

        if (returnCode == 0) {
            String response = (String) r.get("Response");
            size = (long) response.length();
            if (checkETag) { // check etag only
                etag = Utils.getHttpHeader("ETag", response);
                if (etag == null || etag.length() <= 0)
                    throw(new IOException("failed to get ETag: " + response));
            }
            else if (pm.contains(response, pattern)) {
                StringBuffer strBuf = new StringBuffer();
                MatchResult mr = pm.getMatch();
                char c;
                int n = mr.groups() - 1;
                for (int i=1; i<=n; i++) {
                    if (i > 1)
                        strBuf.append(" ");
                    if ((mr.group(i)).length() == 1) // hack on a.m./p.m.
                        c = (mr.group(i)).charAt(0);
                    else
                        c = 'F';
                    if (c == 'a' || c == 'A' || c == 'p' || c == 'P') {
                        strBuf.append(c);
                        strBuf.append('M');
                    }
                    else
                        strBuf.append(mr.group(i));
                }
                Date date = dateFormat.parse(strBuf.toString(),
                    new ParsePosition(0));
                if (date != null)
                    mtime = date.getTime() - timeDifference;
                if (mtime < 0)
                    throw(new IOException("failed to parse mtime: " +
                        strBuf.toString()));
            }
            else {
                throw(new IOException("failed to match mtime with " +
                    pattern.getPattern() + ": " + response));
            }
            if (size < 0) {
                throw(new IOException("failed to get size: " + size));
            }
            if (reference != null) {
                report.put("LatestTime", new long[] {mtime});
                report.put("PageSize", new long[] {size});
            }
            else {
                report.put("SampleTime", new long[] {mtime});
                report.put("SampleSize", new long[] {size});
            }
        }
        else {
            throw(new IOException("web test failed on " + uri +
                ": " + returnCode));
        }

        if (checkETag) {
            if (etag.equals(previousETag))
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
            else
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            previousETag = etag;
        }
        else if (disableMode != 0 && mtime > 0) {
            if (reference != null) { // compare against reference
                Object o;
                if ((o = report.get("SampleTime")) != null &&
                    o instanceof long[] && ((long[]) o).length > 0) {
                    long tt = ((long[]) o)[0];
                    if (mtime - tt > -timeOffset) // caught up
                        skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    else // not caught up yet
                        skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
                else
                    throw(new IOException("failed to get reference time"));
            }
            else if (tw != null) { // time window defined
                int i = tw.getThresholdLength();
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

            previousTime = mtime;
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long sampleTime, pageSize;
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
            pageSize = ((long[]) o)[0];
        }
        else
            pageSize = -1L;

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
                strBuf.append("' has been updated recently");
            }
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
            strBuf.append("' was updated ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes ago");
            break;
          case TimeWindows.ELATE: // extremely late
          case TimeWindows.ALATE: // very late
            level = Event.ERR;
            exceptionCount = 0;
            if (previousStatus < TimeWindows.ALATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' has not been updated");
            strBuf.append(" in the last ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes");
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
            else if (previousStatus == status && previousLevel == level) {//late
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level)
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

        if (sampleTime >= 0)
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(sampleTime)));
        else
            event.setAttribute("latestTime", "N/A");
        if (pageSize >= 0)
            event.setAttribute("size", String.valueOf(pageSize));
        else
            event.setAttribute("size", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
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

    private Event correlate(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, triggerStatus = -1;
        long sampleTime, sampleSize, latestTime = -1L, pageSize = -1L;
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
            sampleSize = -1L;

        if ((o = latest.get("LatestTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            latestTime = ((long[]) o)[0];
        }
        else
            latestTime = -1L;

        if ((o = latest.get("PageSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            pageSize = ((long[]) o)[0];
        }
        else
            pageSize = -1L;

        if (status >= TimeWindows.NORMAL) { // good test
            if (latestTime - sampleTime > - timeOffset) {
                if (triggerSize >= 0) { // trigger enabled
                    triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
                    if ((pageSize >= triggerSize && triggerStatus == 1) ||
                        (pageSize < triggerSize && triggerStatus == 0)) {
                        status = TimeWindows.OCCURRED;
                    }
                }
                else { // trigger disabled
                    status = TimeWindows.OCCURRED;
                }
            }
            else if (triggerSize >= 0) { // trigger enabled but no update
                triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
            }
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
                strBuf.append("' has been updated accordingly");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
            }
            break;
          case TimeWindows.NORMAL:// just updated
          case TimeWindows.SLATE: // somewhat late
          case TimeWindows.ALATE: // very late
            exceptionCount = 0;
            if (previousStatus < TimeWindows.NORMAL ||
                previousStatus == TimeWindows.OCCURRED ||
                previousStatus >= TimeWindows.ELATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            if (latestTime > previousLeadTime) { // target just updated
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
                    strBuf.append("with the state changed ");
                strBuf.append((int) ((currentTime - sampleTime)/60000));
                strBuf.append(" minutes ago");
            }
            else {
                strBuf.append("'" + uri);
                strBuf.append("' has not been updated");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
                strBuf.append(" in the last ");
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
                previousTrigger == triggerStatus) {
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus)
                return null;
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

        event.setAttribute("reference", referenceName);
        event.setAttribute("updateCount", String.valueOf(updateCount));
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
        if (pageSize >= 0)
            event.setAttribute("size", String.valueOf(pageSize));
        else
            event.setAttribute("size", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
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
        int aCount, eCount, uCount, pTrigger, pStatus, pLevel, sNumber;
        long pTime, pLeadTime, pRefTime;
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
        previousLevel = pLevel;
        previousTrigger = pTrigger;
        previousTime = pTime;
        previousLeadTime = pLeadTime;
        previousRefTime = pRefTime;
        updateCount = uCount;
    }

    public void destroy() {
        super.destroy();
        if (reference != null) {
            reference.destroy();
            reference = null;
        }
        if (webTester != null) {
            webTester.destroy();
            webTester = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
