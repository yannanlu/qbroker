package org.qbroker.monitor;

/* StaticReport.java - a shared report with static content */

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.List;
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
 * in the name space specified by the ReportClass. It can pick up properties
 * from either a given property file or a list of environment variables.
 * The loading starts from property file, and then environment variables and
 * inline properties at last.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class StaticReport extends Report {

    public StaticReport(Map props) {
        super(props);
        Object o;
        String filename = null;

        if (type == null)
            type = "StaticReport";

        reportMode = REPORT_FINAL;

        if ((o = props.get("InitialReport")) != null && o instanceof Map)
            report = Utils.cloneProperties((Map) o);

        if ((o = props.get("PropertyFile")) != null) try {
            String str;
            Properties ph = new Properties();
            filename = MonitorUtils.select(o);
            FileInputStream fs = new FileInputStream(filename);
            ph.load(fs);
            fs.close();
            for (String key : ph.stringPropertyNames()) {
                str = ph.getProperty(key);
                if (str != null)
                    report.put(key, str);
            }
            ph.clear();
        }
        catch (IOException e) {
            throw(new IllegalArgumentException("failed to load properties from "
                + filename + ": " + e.toString()));
        }

        if ((o = props.get("EnvPrefix")) != null && o instanceof List) {
            String str, prefix;
            List pl = (List) o;
            Map<String, String> map = System.getenv();
            for (String key : map.keySet()) {
                for (Object obj : pl) {
                    if (obj == null || !(obj instanceof String))
                        continue;
                    prefix = (String) o;
                    if (prefix.length() <= 0)
                        continue;
                    if (!key.startsWith(prefix))
                        continue;
                    if ((str = map.get(key)) != null)
                        report.put(key, str);
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
