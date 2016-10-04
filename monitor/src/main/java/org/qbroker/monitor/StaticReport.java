package org.qbroker.monitor;

/* StaticReport.java - a shared report with static content */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import org.qbroker.event.Event;
import org.qbroker.common.DisabledException;
import org.qbroker.common.Utils;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * StaticReport defines a set of key-value pairs as a shared report
 * in the name space specified by the ReportClass.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class StaticReport extends Report {

    public StaticReport(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "StaticReport";

        reportMode = REPORT_FINAL;

        if ((o = props.get("InitialReport")) != null && o instanceof Map)
            report = Utils.cloneProperties((Map) o);

        if ((o = props.get("EnvironmentVariable")) != null && o instanceof Map){
            String str, key, prefix;
            Map ph = (Map) o;
            Map<String, String> map = System.getenv();
            Iterator iter = ph.keySet().iterator();
            prefix = (String) props.get("Prefix");
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (prefix != null)
                    str = System.getProperty(prefix + "/" + key);
                else
                    str = System.getProperty(key);
                if (str != null)
                    report.put(key, str);
                else if ((str = map.get(key)) != null)
                    report.put(key, str);
                else if ((o = ph.get(key)) != null) { // default value
                    str = MonitorUtils.select(o);
                    report.put(key, MonitorUtils.substitute(str, template));
                }
            }
        }
        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String str, key;
            Iterator iter = ((Map) o).keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (report.containsKey(key)) // no override
                    continue;
                str = MonitorUtils.select(((Map) o).get(key));
                report.put(key, MonitorUtils.substitute(str, template));
            }
        }
        if (!report.containsKey("Status"))
            report.put("Status", "0");
        if (!report.containsKey("TestTime"))
            report.put("TestTime", String.valueOf(System.currentTimeMillis()));

        serialNumber = 0;
        // initialize the report in the name space of the ReportClass
        if ((o = props.get("ReportClass")) != null && o instanceof String) {
            java.lang.reflect.Method initReport = null;
            try {
                Class<?> cls = Class.forName((String) o);
                initReport = cls.getMethod("initReport",
                    new Class[]{String.class, String[].class, Map.class,
                    int.class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to get 'initReport'"+
                  " method in: " + (String) o + ": " + Event.traceStack(e)));
            }
            if (!java.lang.reflect.Modifier.isStatic(initReport.getModifiers()))
                throw(new IllegalArgumentException("getReport: not a static " +
                  "method of: " + (String) o));

            try {
                initReport.invoke(null, new Object[] {reportName, new String[0],
                    report, new Integer(reportMode)});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to invoke " +
                    "'initReport' method in: " + (String) o + ": " +
                    Event.traceStack(e)));
            }
        }
        else { // default name space
            MonitorUtils.initReport(reportName,new String[0],report,reportMode);
        }
        throw(new DisabledException("final report initialized"));
    }

    public Map<String, Object> generateReport(long currentTime) {
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

        return report;
    }
}
