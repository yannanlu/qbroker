package org.qbroker.monitor;

/* LatestRecord.java - a monitor checking the timestamp of a record */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.DBQuery;

/**
 * LatestRecord queries a DB for a record and monitor its latest update time
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class LatestRecord extends Monitor {
    private DBQuery dbQuery;
    private String uri, baseURI, referenceName, sqlQuery;
    private long timeDifference, previousTime, previousRefTime,previousLeadTime;
    private int previousLevel, sqlStatusOffset, updateCount;
    private int triggerSize, timeOffset, previousTrigger;
    private SimpleDateFormat dateFormat, timeFormat[];
    private MonitorReport reference;
    private TimeWindows tw = null;
    private boolean isEmptyOK = false;
    private final static String sqlStatusText[] = {"Exception",
        "Query OK", "Protocol error", "Query not compile",
        "Query failed", "Login timedout", "Login refused"};
    private final static String formatText[] = {"yyyy",
        "yy", "MMM", "MM", "dd", "HH", "mm", "ss"};

    public LatestRecord(Map props) {
        super(props);
        Object o;
        URI u;
        String s, className;
        int n, index[];
        if (type == null)
            type = "LatestRecord";

        if (description == null)
            description = "monitor update time on a record of DB";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((s = u.getScheme()) != null && "jdbc".equals(s)) {
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("Name", name);
            h.put("DBDriver", (String) props.get("DBDriver"));
            h.put("Username", (String) props.get("Username"));
            h.put("Password", (String) props.get("Password"));
            h.put("URI", uri);
            sqlQuery = MonitorUtils.substitute((String) props.get("SQLQuery"),
                template);
            h.put("SQLQuery", sqlQuery);
            h.put("Timeout", (String) props.get("Timeout"));
            h.put("Step", "1");
            dbQuery = new DBQuery(h);
        }
        else {
            dbQuery = null;
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        }

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        if ((o = props.get("TimePattern")) != null)
            dateFormat = new SimpleDateFormat((String) o);
        else
            dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

        if ((o = props.get("IgnoreEmptyResult")) != null &&
            "true".equals((String) o))
            isEmptyOK = true;

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
            if (ref.get("Dependency") != null)
                ref.remove("Dependency");
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
        sqlStatusOffset = 0 - DBQuery.CONNFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long size = -1L;
        long mtime = -10L;
        int returnCode = -1;

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

        if (reference != null) { // check reference
            try {
                report = reference.generateReport(currentTime);
                if (report.get("LastEntry") != null)
                    report.remove("LastEntry");
            }
            catch (Exception e) {
                throw(new IOException("reference failed: " +
                    Event.traceStack(e)));
            }
        }

        report.put("URI", uri);
        int i, n;
        Map<String, Object> r = dbQuery.generateReport(currentTime);
        if (r.get("ReturnCode") != null && (returnCode =
            Integer.parseInt((String) r.get("ReturnCode"))) == DBQuery.QUERYOK){
            StringBuffer strBuf = new StringBuffer();
            try {
                java.sql.ResultSet rs = (java.sql.ResultSet) r.get("ResultSet");
                if (rs != null) {
                    java.sql.ResultSetMetaData rsmd = rs.getMetaData();
                    n = rsmd.getColumnCount();
                    if (rs.next()) {
                        for (i=1; i<=n; i++) {
                            if (i > 1)
                                strBuf.append(" | ");
                            strBuf.append(rs.getString(i));
                        }
                    }
                    rs.last();
                    size = (long) rs.getRow();
                    rs.close();
                }
                dbQuery.dbClose();
            }
            catch (java.sql.SQLException e) {
                throw(new IOException(Event.traceStack(e)));
            }
            catch (Exception e) {
                throw(new IOException(Event.traceStack(e)));
            }

            String entry = strBuf.toString();
            report.put("LastEntry", entry);

            Date date = dateFormat.parse(entry, new ParsePosition(0));
            if (date != null) {
                mtime = date.getTime() - timeDifference;
            }
            else if (strBuf.length() == 0 && isEmptyOK) { // empty report
                skip = DISABLED;
                return report;
            }
            else {
                throw(new IOException("failed to get timestamp from: "+entry));
            }
        }
        else if (r.get("ReturnCode") == null) {
            throw(new IOException("DBQuery failed: returnCode is null"));
        }
        else {
            throw(new IOException("DBQuery failed: " +
                sqlStatusText[returnCode + sqlStatusOffset]));
        }

        if (reference != null) {
            report.put("LatestTime", new long[] {mtime});
            report.put("NumberRecords", new long[] {size});
        }
        else {
            report.put("SampleTime", new long[] {mtime});
            report.put("SampleSize", new long[] {size});
        }

        if (disableMode != 0 && mtime > 0) {
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

            previousTime = mtime;
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long sampleTime, numberRecords;
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
            numberRecords = ((long[]) o)[0];
        }
        else
            numberRecords = -1L;

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
            else if (previousStatus == status && previousLevel == level) //late
                return null;
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
        if (numberRecords >= 0)
            event.setAttribute("numberRecords", String.valueOf(numberRecords));
        else
            event.setAttribute("numberRecords", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (numberRecords > 0)
            event.setAttribute("lastEntry", (String) latest.get("LastEntry"));
        else
            event.setAttribute("lastEntry", "");

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
        long sampleTime, sampleSize, latestTime = -1L, numberRecords = -1L;
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

        if ((o = latest.get("NumberRecords")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            numberRecords = ((long[]) o)[0];
        }
        else
            numberRecords = -1L;

        if (status >= TimeWindows.NORMAL) { // good test
            if (latestTime - sampleTime > - timeOffset) {
                if (triggerSize >= 0) { // trigger enabled
                    triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
                    if ((numberRecords >= triggerSize && triggerStatus == 1) ||
                        (numberRecords < triggerSize && triggerStatus == 0)) {
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
                    strBuf.append("with the state change ");
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
        if (numberRecords >= 0)
            event.setAttribute("numberRecords", String.valueOf(numberRecords));
        else
            event.setAttribute("numberRecords", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (numberRecords > 0)
            event.setAttribute("lastEntry", (String) latest.get("LastEntry"));
        else
            event.setAttribute("lastEntry", "");

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

        if ((o = chkpt.get("PreviousTrigger")) != null)
            pTrigger = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
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
        if (dbQuery != null) {
            dbQuery.destroy();
            dbQuery = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
