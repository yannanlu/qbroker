 package org.qbroker.monitor;
/* QReportQuery.java - a generic monitor watching queue reports */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;
import org.qbroker.json.JSON2Map;

/**
 * QReportQuery queries an internal report for queue status and queue depth.
 * It is used as a dependency to test the skipping status.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QReportQuery extends Report {
    private int debug = 0;
    private int reportExpiration = 0;
    private java.lang.reflect.Method getReport = null;

    public QReportQuery(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "ReportQuery";

        if ((o = props.get("ReportClass")) != null && o instanceof String) {
            try {
                Class<?> cls = Class.forName((String) o);
                getReport= cls.getMethod("getReport",new Class[]{String.class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to get 'getReport'" +
                    " method in: " + (String) o + ": " + Event.traceStack(e)));
            }
            if (!java.lang.reflect.Modifier.isStatic(getReport.getModifiers()))
                throw(new IllegalArgumentException("getReport: not a static " +
                    "method of: " + (String) o));
        }

        if ((o = props.get("ReportExpiration")) != null &&
            (reportExpiration = Integer.parseInt((String) o)) < 0)
            reportExpiration = 0;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
    }

    public Map<String,Object>generateReport(long currentTime) throws Exception {
        Object o;
        Map r;
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
                    report.put("Exception", new Exception(reportName +
                        " failed to check dependencies"));
                return report;
            }
        }

        if (getReport == null)
            r = MonitorUtils.getReport(reportName);
        else
            r = (Map) getReport.invoke(null, new Object[] {reportName});

        if (r == null)
            throw(new IllegalArgumentException("no such report: " +reportName));
        else { // copy the content over
            for (Object ky : r.keySet())
                report.put((String) ky, r.get(ky));
            r.clear();
        }

        if ((o = report.get("TestTime")) == null) {
            if (debug > 0)
                new Event(Event.DEBUG, reportName + " failed to get TestTime: "+
                    JSON2Map.toJSON(report)).send();
            skip = EXCEPTION;
            report.put("Exception", new Exception(reportName +
                " failed to get testTime"));
            return report;
        }
        else if (reportExpiration > 0) { // check if testTime expired or not
            long tm = Long.parseLong((String) o);
            if (currentTime - reportExpiration >= tm) { // expired
                if (debug > 0)
                    new Event(Event.DEBUG, reportName + " expired at " +
                        new Date(tm)).send();
                skip = EXCEPTION;
                report.put("Exception", new Exception(reportName +
                    " expired at " + new Date(tm)));
                return report;
            }
        }

        try {
            long curDepth, preDepth;
            curDepth = Long.parseLong((String) report.get("CurrentDepth"));
            preDepth = Long.parseLong((String) report.get("PreviousDepth"));

            if ((disableMode > 0 && curDepth <= 0 && preDepth <= 0) ||
                (disableMode < 0 && (curDepth > 0 || preDepth > 0)))
                skip = DISABLED;
        }
        catch (Exception e) {
            if (debug > 0)
                new Event(Event.DEBUG, reportName +" failed to get data: " +
                    Event.traceStack(e)).send();
            skip = EXCEPTION;
            report.put("Exception", new Exception(reportName +
                " failed to get data: " + e.toString()));
        }

        return report;
    }
}
