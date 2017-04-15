package org.qbroker.monitor;

/* DBQuery.java - a monitor querying a give database */

import java.util.Map;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.qbroker.common.TimeWindows;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;

/**
 * DBQuery queres a Database to see if it hangs or goes down
 *<br/><br/>
 * It queries the given DBDriver, URI and an SQL Query statement.  It
 * throws exceptions if the internal error occurs.   Otherwise it returns an
 * integer indicating the status of the db server.
 *<br/><br/>
 * Status     Code         Description<br/>
 *   -2     NOTAVAILABLE   not available<br/>
 *   -1     CONNFAILED     connection failed<br/>
 *   0      QUERYOK        query is successful<br/>
 *   1      PROTOCOLERROR  protocol error, probably wrong port<br/>
 *   2      QUERYERROR     SQL Query not compile<br/>
 *   3      QUERYFAILED    SQL Query failed<br/>
 *   4      LOGINTIMEOUT   login timeout, probably server is busy<br/>
 *   5      LOGINREFUSED   login refused, probably password is wrong<br/>
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DBQuery extends Monitor {
    private int sqlExecTimeout, timeout;
    private String username, password, uri, sqlQuery;
    private Connection con = null;
    private PreparedStatement ps = null;
    private String errorMsg;
    private int actionTimeout, previousDBStatus, dbStatusOffset;
    public final static int NOTAVAILABLE = -2;
    public final static int CONNFAILED = -1;
    public final static int QUERYOK = 0;
    public final static int PROTOCOLERROR = 1;
    public final static int QUERYERROR = 2;
    public final static int QUERYFAILED = 3;
    public final static int LOGINTIMEOUT = 4;
    public final static int LOGINREFUSED = 5;
    private final static String dbStatusText[] = {"Not available",
        "Connection failed", "Query OK", "Protocol error", "Query error",
        "Query failed", "Login timeout", "Login refused"};

    public DBQuery(Map props) {
        super(props);
        Object o;
        String dbDriver;
        int n;

        if (type == null)
            type = "DBQuery";

        if (description == null)
            description = "Query Database Server";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("DBDriver")) != null)
            dbDriver = (String) o;
        else try {
            String ssp;
            URI u = new URI(uri);
            ssp = u.getSchemeSpecificPart();
            if (ssp == null)
                throw(new IllegalArgumentException("uri is not well defined"));
            else if (ssp.startsWith("oracle"))
                dbDriver = "oracle.jdbc.driver.OracleDriver";
            else if (ssp.startsWith("mysql"))
                dbDriver = "com.mysql.jdbc.Driver";
            else if (ssp.startsWith("postgresql"))
                dbDriver = "org.postgresql.Driver";
            else if (ssp.startsWith("microsoft"))
                dbDriver = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
            else if (ssp.startsWith("db2"))
                dbDriver = "com.ibm.db2.jcc.DB2Driver";
            else
                throw(new IllegalArgumentException("DBDriver is not defined"));
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = MonitorUtils.select(props.get("Username"))) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        username = (String) o;

        if ((o = MonitorUtils.select(props.get("Password"))) == null)
            throw(new IllegalArgumentException("Password is not defined"));
        password = (String) o;

        if ((o = MonitorUtils.select(props.get("SQLQuery"))) == null)
            throw(new IllegalArgumentException("SQLQuery is not defined"));
        sqlQuery = MonitorUtils.substitute((String) o, template);

        // check time dependency on sqlQuery
        n = initTimeTemplate(sqlQuery);
        if (n > 0)
            sqlQuery = updateContent(System.currentTimeMillis());

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 30000;

        /* This is the timeout for how long the sql query takes to execute.
         * It is in seconds, not milliseconds like the one above.
         */
        if ((o = props.get("SQLExecTimeout")) != null)
            sqlExecTimeout = Integer.parseInt((String) o);
        else
            sqlExecTimeout = -1;

        try {
            Class.forName(dbDriver);
            DriverManager.setLoginTimeout(timeout);
            con = DriverManager.getConnection(uri, username, password);
            con.close();
            con = null;
        }
        catch (ClassNotFoundException e) {
            throw(new IllegalArgumentException("class not found: " + e));
        }
        catch (SQLException e) {
            throw(new IllegalArgumentException("SQLException: " + e));
        }

        previousDBStatus = -10;
        dbStatusOffset = 0 - NOTAVAILABLE;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int returnCode = 0, num = -1;
        java.sql.ResultSet rs = null;
        report.clear();
        skip = NOSKIP;
        errorMsg = null;
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

        if (con == null)
            returnCode = dbConnect();

        if (returnCode != 0) {
            dbClose();
            report.put("ErrorMessage", errorMsg);
            report.put("ReturnCode", String.valueOf(returnCode));
            if (disableMode > 0)
                skip = DISABLED;
            else if (disableMode < 0)
                skip = NOSKIP;
            else
                skip = EXCEPTION;
            return report;
        }

        if (timeFormat.length > 0) // time-dependent query
            sqlQuery = updateContent(currentTime);

        try {
            ps = con.prepareStatement(sqlQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
            if (sqlExecTimeout >= 0)
                ps.setQueryTimeout(sqlExecTimeout);
        }
        catch (SQLException e) {
            dbClose();
            errorMsg = e.getMessage();
            report.put("ErrorMessage", errorMsg);
            returnCode = QUERYERROR;
        }
        if (ps != null) try {
            rs = ps.executeQuery();
            report.put("ResultSet", rs);
            returnCode = QUERYOK;
        }
        catch (SQLException e) {
            dbClose();
            errorMsg = e.getMessage();
            report.put("ErrorMessage", errorMsg);
            returnCode = QUERYFAILED;
        }
        report.put("ReturnCode", String.valueOf(returnCode));

        if (disableMode != 0) {
            if ((returnCode != QUERYOK && disableMode > 0) ||
                (returnCode == QUERYOK && disableMode < 0))
                skip = DISABLED;
            try {
                rs.close();
            }
            catch (SQLException e) {
            }
            dbClose();
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int dbStatus, level = 0;
        long size;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("ReturnCode")) != null &&
            o instanceof String) {
            dbStatus = Integer.parseInt((String) o);
        }
        else
            dbStatus = NOTAVAILABLE;

        if ((o = latest.get("ResultSet")) != null &&
            o instanceof java.sql.ResultSet) {
            try {
                ((java.sql.ResultSet) o).close();
            }
            catch (SQLException e) {
            }
            dbClose();
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
            if (dbStatus != QUERYOK) {
                if (previousDBStatus == QUERYOK)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousDBStatus != QUERYOK) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousDBStatus = dbStatus;
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
                break;
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
            strBuf.append(dbStatusText[dbStatus + dbStatusOffset]);
            if (dbStatus != QUERYOK)
                strBuf.append(": " + (String) latest.get("ErrorMessage"));
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
        event.setAttribute("dbStatus", dbStatusText[dbStatus+dbStatusOffset]);
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

    private int dbConnect() {
        dbClose();
        try {
            con = DriverManager.getConnection(uri, username, password);
        }
        catch (SQLException e) {
            errorMsg = e.getMessage();
            return CONNFAILED;
        }
        return 0;
    }

    public void dbClose() {
        if (ps != null) {
            try {
                ps.close();
            }
            catch (SQLException e) {
            }
            ps = null;
        }
        if (con != null) {
            try {
                con.close();
            }
            catch (SQLException e) {
            }
            con = null;
        }
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousDBStatus", String.valueOf(previousDBStatus));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pDBStatus, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousDBStatus")) != null)
            pDBStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDBStatus = pDBStatus;
    }

    public void destroy() {
        super.destroy();
        dbClose();
    }

    protected void finalize() {
        destroy();
    }
}
