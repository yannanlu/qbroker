package org.qbroker.monitor;

/* DBRecord.java - a monitor checking DB records */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.DBQuery;

/**
 * DBRecord queries a DB for records and scan all the records for
 * certain patterns
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DBRecord extends Monitor {
    private DBQuery dbQuery;
    private String uri, statsURI, fs, tmpFilename;
    private int sqlStatusOffset, previousNumber;
    private File statsFile = null;
    private final static String sqlStatusText[] = {"Exception",
        "Query OK", "Protocol error", "Query not compile",
        "Query failed", "Login timedout", "Login refused"};

    public DBRecord(Map props) {
        super(props);
        Object o;
        URI u;
        String s;
        int n, index[];

        if (type == null)
            type = "DBRecord";

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
            h.put("DBDriver", props.get("DBDriver"));
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("URI", uri);
            h.put("SQLQuery", props.get("SQLQuery"));
            h.put("Timeout", props.get("Timeout"));
            h.put("SQLExecTimeout", props.get("SQLExecTimeout"));
            h.put("Step", "1");
            dbQuery = new DBQuery(h);
        }
        else {
            dbQuery = null;
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        }

        if ((o = props.get("StatsURI")) != null && o instanceof String) {
            s = MonitorUtils.substitute((String) o, template);

            try {
                u = new URI(s);
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException(e.toString()));
            }
            tmpFilename = u.getPath();
            if (tmpFilename == null || tmpFilename.length() == 0)
                throw(new IllegalArgumentException("StatsURI has no path: "+s));
            try {
                tmpFilename = Utils.decode(tmpFilename);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decode: "+
                    tmpFilename));
            }
            if ((s = u.getScheme()) == null || "log".equals(s)) {
                statsFile = new File(tmpFilename);
                statsLogger = new GenericLogger(tmpFilename);
                tmpFilename = null;
            }
            else if ("file".equals(s)) {
                statsFile = new File(tmpFilename);
                tmpFilename += ".tmp";
                statsLogger = new GenericLogger(tmpFilename);
            }
            else {
                throw(new IllegalArgumentException("unsupported scheme: " + s));
            }
        }
        else {
            statsLogger = null;
            statsFile = null;
            tmpFilename = null;
        }

        try {
            Perl5Compiler pc = new Perl5Compiler();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("Pattern failed"+e.toString()));
        }
        if (aPatternGroup.length <= 0 && xPatternGroup.length <= 0)
            throw(new IllegalArgumentException("PatternGroup is empty"));

        if ((o = props.get("FieldSeparator")) != null)
            fs = (String) o;
        else
            fs = " | ";

        previousNumber = -10;
        sqlStatusOffset = 0 - DBQuery.CONNFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
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

        report.put("URI", uri);
        int i, n;
        Map<String, Object> r = dbQuery.generateReport(currentTime);
        if (r.get("ReturnCode") != null && (returnCode =
            Integer.parseInt((String) r.get("ReturnCode"))) == DBQuery.QUERYOK){
            StringBuffer strBuf = new StringBuffer();
            List<String> recordBuffer = new ArrayList<String>();
            try {
                java.sql.ResultSet rs = (java.sql.ResultSet) r.get("ResultSet");
                if (rs != null) {
                    java.sql.ResultSetMetaData rsmd = rs.getMetaData();
                    n = rsmd.getColumnCount();
                    while (rs.next()) {
                        for (i=1; i<=n; i++) {
                            if (i > 1)
                                strBuf.append(fs);
                            strBuf.append(rs.getString(i));
                        }
                        String entry = strBuf.toString();
                        strBuf = new StringBuffer();
                        if (MonitorUtils.filter(entry,aPatternGroup,pm,true) &&
                            !MonitorUtils.filter(entry,xPatternGroup,pm,false))
                            recordBuffer.add(entry);
                    }
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

            report.put("RecordBuffer", recordBuffer);
            n = recordBuffer.size();

            if (disableMode != 0) {
                if (n > 0) {
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                }
                else {
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
            }

            if (statsLogger != null && tmpFilename == null) {
                strBuf = new StringBuffer();
                for (i=0; i<n; i++) {
                    if (i > 0)
                        strBuf.append("\n");
                    strBuf.append((String) recordBuffer.get(i));
                }
                report.put("Stats", strBuf.toString());
                try {
                    statsLogger.log(Event.dateFormat(new Date(currentTime)) +
                        " got " + n + " records\n" + strBuf.toString());
                }
                catch (Exception e) {
                }
            }
            else if (statsLogger != null && tmpFilename.length() > 0) {
                File f = new File(tmpFilename);
                n = recordBuffer.size();
                strBuf = new StringBuffer();
                for (i=0; i<n; i++) {
                    if (i > 0)
                        strBuf.append("\n");
                    strBuf.append((String) recordBuffer.get(i));
                }
                try {
                    statsLogger.overwrite(strBuf.toString());
                    f.renameTo(statsFile);
                }
                catch (Exception e) {
                }
            }
        }
        else if (r.get("ReturnCode") == null) {
            throw(new IOException("DBQuery failed: returnCode is null"));
        }
        else {
            throw(new IOException("DBQuery failed: " + r.get("ErrorMessage") +
                " with " + sqlStatusText[returnCode + sqlStatusOffset]));
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        int numberRecords;
        List recordBuffer;
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if ((o = latest.get("RecordBuffer")) != null && o instanceof List) {
            recordBuffer = (List) o;
        }
        else
            recordBuffer = new ArrayList();
        numberRecords = recordBuffer.size();

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
            if (numberRecords > 0) {
                if (previousNumber == 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousNumber > 0) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousNumber = numberRecords;
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
            strBuf.append("query on '" + uri);
            strBuf.append("' returns ");
            strBuf.append(numberRecords + " matching entries");
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
        event.setAttribute("numberRecords", String.valueOf(numberRecords));
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (numberRecords > 0)
      event.setAttribute("lastEntry",(String)recordBuffer.get(numberRecords-1));
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
        if (dbQuery != null) {
            dbQuery.destroy();
            dbQuery = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
