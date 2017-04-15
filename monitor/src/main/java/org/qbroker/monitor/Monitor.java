package org.qbroker.monitor;

/* Monitor.java - an abstract class for MonitorReport and MonitorAction */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DisabledException;
import org.qbroker.common.Template;
import org.qbroker.common.GenericLogger;
import org.qbroker.event.Event;
import org.qbroker.event.EventActionGroup;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorAction;

/**
 * Monitor is an abstract class with partial implementation of both
 * MonitorReport and MonitorAction.  It has two abstract methods,
 * generateReport() and performAction().  Other methods can be overridden
 * if it is necessary.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public abstract class Monitor implements MonitorReport, MonitorAction {
    protected String name;
    protected String site;
    protected String category;
    protected String type = null;
    protected String description = null;
    protected Template template;
    protected Map<String, Object> report;
    protected EventActionGroup actionGroup;
    protected List[] dependencyGroup = null;
    protected int maxRetry, maxPage, tolerance, exceptionTolerance;
    protected int repeatPeriod, step, normalStep, serialNumber;
    protected int previousStatus, skip, actionCount, exceptionCount;
    protected int disableMode, reportMode, checkpointTimeout, statusOffset;
    protected int cachedSkip = NOSKIP, resolution = 1000;
    protected boolean disabledWithReport = false;
    protected GenericLogger statsLogger = null;
    protected Pattern[][] aPatternGroup = null;
    protected Pattern[][] xPatternGroup = null;
    protected Perl5Matcher pm = null;
    protected String reportName = null;
    protected String[] keyList = null;
    protected SimpleDateFormat timeFormat[] = new SimpleDateFormat[0];
    protected Template timeTemplate = null;
    protected final static String statusText[] = {"Exception",
        "Exception in blackout", "Disabled", "Blackout", "Normal",
        "Occurred", "Late", "Very late", "Extremely late", "Expired"};
    protected final static String formatText[] = {"yyyy",
        "yy", "MMM", "MM", "dd", "HH", "mm", "ss"};
    protected final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    public Monitor(Map props) {
        Object o;
        List list;
        String str;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = MonitorUtils.select(props.get("Site"));
        category = MonitorUtils.select(props.get("Category"));
        if ((o = props.get("Type")) != null)
            type = (String) o;

        template = new Template("##hostname##, ##HOSTNAME##");

        if ((o = props.get("Description")) != null)
            description = MonitorUtils.substitute((String) o, template);

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

        pm = new Perl5Matcher();
        try {
            Perl5Compiler pc = new Perl5Compiler();
            aPatternGroup= MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup=MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

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
            int skip;
            long tt = System.currentTimeMillis();
            MonitorReport dep = MonitorUtils.getStaticDependency(props);
            try {
                report =  dep.generateReport(tt);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed in static report " +
                    e.toString()));
            }
            skip = dep.getSkippingStatus();

            if (skip == EXCEPTION)
                throw(new IllegalArgumentException("dep report exceptioned"));

            cachedSkip = skip;
            if (reportMode == REPORT_FINAL) {
                MonitorUtils.initReport(name, tt, TimeWindows.NORMAL, skip);
                throw(new DisabledException("final report initialized"));
            }
        }

        if ((o = props.get("ActionGroup")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            actionGroup = new EventActionGroup(props);
        }
        else
            actionGroup = null;

        if ((o = props.get("Tolerance")) == null ||
            (tolerance = Integer.parseInt((String) o)) < 0)
            tolerance = 0;

        if ((o = props.get("ExceptionTolerance")) == null)
            exceptionTolerance = 0;
        else
            exceptionTolerance = Integer.parseInt((String) o);

        if ((o = props.get("MaxRetry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) < 0)
            maxRetry = 2;

        if ((o = props.get("MaxPage")) == null ||
            (maxPage = Integer.parseInt((String) o)) < 0)
            maxPage = 2;

        if ((o = props.get("QuietPeriod")) == null ||
            (n = Integer.parseInt((String) o)) < 0)
            n = 0;

        repeatPeriod = maxRetry + maxPage + n;

        if ((o = props.get("Step")) == null ||
            (step = Integer.parseInt((String) o)) < 0)
            step = 0;

        if (step == 1)
            step = 0;
        normalStep = step;

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

        if ((o = props.get("CheckpointTimeout")) == null ||
            (checkpointTimeout = 1000*Integer.parseInt((String) o)) < 0)
            checkpointTimeout = 480000;

        report = new HashMap<String, Object>();

        serialNumber = 0;
        actionCount = 0;
        exceptionCount = 0;
        previousStatus = -10;
        statusOffset = 0 - TimeWindows.EXCEPTION;
    }

    public abstract Map<String, Object> generateReport(long currentTime)
        throws Exception;

    public abstract Event performAction(int status, long currentTime,
        Map<String, Object> latest);

    public String getName() {
        return name;
    }

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

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        chkpt.put("Name", name);
        chkpt.put("CheckpointTime", String.valueOf(System.currentTimeMillis()));
        chkpt.put("SerialNumber", String.valueOf(serialNumber));
        chkpt.put("ActionCount", String.valueOf(actionCount));
        chkpt.put("ExceptionCount", String.valueOf(exceptionCount));
        chkpt.put("PreviousStatus", String.valueOf(previousStatus));
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

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
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
        if (statsLogger != null)
            statsLogger = null;
    }

    protected void finalize() {
        destroy();
    }
}
