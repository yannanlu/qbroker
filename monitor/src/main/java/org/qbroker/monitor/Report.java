package org.qbroker.monitor;

/* Report.java - an abstract class for MonitorReport */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DisabledException;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Template;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;

/**
 * @author yannanlu@yahoo.com
 */

public abstract class Report implements MonitorReport {
    protected String name, type = null;
    protected int step, skip, disableMode, serialNumber;
    protected int reportMode, cachedSkip = NOSKIP, resolution = 1000;
    protected boolean disabledWithReport = false;
    protected Template template;
    protected Map<String, Object> report = new HashMap<String, Object>();
    protected List[] dependencyGroup = null;
    protected GenericLogger statsLogger = null;
    protected String reportName = null;
    protected String[] keyList = null;
    protected Perl5Matcher pm = null;
    protected SimpleDateFormat timeFormat[] = new SimpleDateFormat[0];
    protected Template timeTemplate = null;
    protected final static String formatText[] = {"yyyy",
        "yy", "MMM", "MM", "dd", "HH", "mm", "ss", "zz"};
    protected final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    public Report(Map props) {
        Object o;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("Type")) != null)
            type = (String) o;

        template = new Template("##hostname## ##HOSTNAME## ##host## ##HOST##");

        if ((o = props.get("StaticDependencyGroup")) != null &&
            o instanceof List) {
            List[] depGroup = MonitorUtils.getDependencies((List) o);
            long tt = System.currentTimeMillis();
            int skip = MonitorUtils.checkDependencies(tt, depGroup, name);
            MonitorUtils.clearDependencies(depGroup);
            if (skip == SKIPPED || skip == DISABLED)
                throw(new DisabledException("disabled by static dependency"));
            else if (skip == EXCEPTION)
                throw(new IllegalArgumentException("static dep exceptioned"));
        }

        if ((o = props.get("Step")) != null)
            step = Integer.parseInt((String) o);
        else
            step = 0;

        if ((o = props.get("ReportName")) != null) {
            reportName = MonitorUtils.select(o);
            reportName = MonitorUtils.substitute(reportName, template);
        }
        else
            reportName = name;

        if ((o = props.get("ReportKey")) != null && o instanceof List) {
            keyList = new String[((List) o).size()];
            for (int i=0; i<keyList.length; i++)
                keyList[i] = (String) ((List) o).get(i);
        }

        o = props.get("ReportMode");
        reportMode = MonitorUtils.getReportMode((String) o);

        if ((o = props.get("DependencyGroup")) != null && o instanceof List) {
            dependencyGroup = MonitorUtils.getDependencies((List) o);
            disableMode = 0;
            if ((o = props.get("DisabledWithReport")) != null &&
                "true".equalsIgnoreCase((String) o)) // run report if disabled
                disabledWithReport = true;
        }
        else if ((o = props.get("DisableMode")) != null &&
            Integer.parseInt((String) o) != 0) { // initialize disable report
            disableMode = Integer.parseInt((String) o);
            dependencyGroup = null;
            if (reportMode == REPORT_SHARED)
                keyList = null;
        }
        else { // no dependency
            dependencyGroup = null;
            disableMode = 0;
        }

        // initialize the dep report with a new report instance
        if (reportMode == REPORT_FINAL || reportMode == REPORT_CACHED) {
            long tt = System.currentTimeMillis();
            MonitorReport dep = MonitorUtils.getStaticDependency(props);
            try {
                report =  dep.generateReport(tt);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed in static report " +
                    e.toString()));
            }
            int skip = dep.getSkippingStatus();

            if (skip == EXCEPTION)
                throw(new IllegalArgumentException("dep report exceptioned"));

            cachedSkip = skip;
            if (reportMode == REPORT_FINAL) {
                MonitorUtils.initReport(name, tt, TimeWindows.NORMAL, skip);
                throw(new DisabledException("final report initialized"));
            }
        }

        if ((o = props.get("Resolution")) != null)
            resolution = 1000 * Integer.parseInt((String) o);
        if (resolution <= 0)
            resolution = 1000;

        if ((o = MonitorUtils.select(props.get("StatsLog"))) != null) {
            int j;
            String loggerName = MonitorUtils.substitute((String) o, template);
            Map h = (Map) props.get("LoggerProperties");
            if (h != null) {
                Properties p = new Properties();
                if ((o = h.get("LoggerName")) != null && o instanceof String)
                    loggerName = MonitorUtils.substitute((String) o, template);
                Iterator iter = (h.keySet()).iterator();
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    if ((o = h.get(key)) != null && o instanceof String)
                        p.setProperty(key, (String) o);
                }
                statsLogger = new GenericLogger(loggerName, p);
            }
            else if (loggerName.matches("^.*" + FILE_SEPARATOR +
                "\\.[_0-9a-zA-Z]+$") && (j = loggerName.lastIndexOf(".")) > 0)
                // insert the default name
                statsLogger = new GenericLogger(loggerName.substring(0, j) +
                    name + loggerName.substring(j));
            else // use the default properties
                statsLogger = new GenericLogger(loggerName);
        }

        pm = new Perl5Matcher();
        report = new HashMap<String, Object>();

        serialNumber = 0;
    }

    public abstract Map<String, Object> generateReport(long currentTime)
        throws Exception;

    public int getSkippingStatus() {
        return skip;
    }

    public int getReportMode() {
        return reportMode;
    }

    public String getReportName() {
        return reportName;
    }

    public String[] getReportKeys() {
        if (reportMode < REPORT_SHARED || keyList == null)
            return null;
        else {
            String keys[] = new String[keyList.length];
            for (int i=0; i<keyList.length; i++)
                keys[i] = keyList[i];
            return keys;
        }
    }

    /** initializes the time-dependent template for dynamic support */
    protected int initTimeTemplate(String text) {
        // check time dependency on text
        timeTemplate = new Template(text, "__[a-zA-Z]+__");
        int n = 0;
        int[] index = new int[formatText.length];
        for (int i=0; i<formatText.length; i++) {
            if (timeTemplate.containsField(formatText[i])) {
                index[n] = i;
                n ++;
            }
        }
        if (n > 0) {
            timeFormat = new SimpleDateFormat[n];
            for (int i=0; i<n; i++) {
                timeFormat[i] = new SimpleDateFormat(formatText[index[i]]);
            }
        }
        return n;
    }

    /** returns the updated time-dependent content or null if it is static */
    protected String updateContent(long currentTime) {
        String key, value, text;
        if (timeTemplate == null)
            return null;
        text = timeTemplate.copyText();
        Date now = new Date(currentTime - (currentTime % resolution));
        for (int i=0; i<timeFormat.length; i++) {
            key = timeFormat[i].toPattern();
            value = timeFormat[i].format(now);
            text = timeTemplate.substitute(key, value, text);
        }
        return text;
    }

    public void destroy() {
        if (report != null)
            report.clear();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (template != null) {
            template.clear();
            template = null;
        }
        if (timeTemplate != null) {
            timeTemplate.clear();
            timeTemplate = null;
        }
        statsLogger = null;
        pm = null;
        timeFormat = null;
    }

    protected void finalize() {
        destroy();
    }
}
