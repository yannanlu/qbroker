package org.qbroker.monitor;

/* ReportQuery.java - a generic reporter watching internal reports */

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Date;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * ReportQuery queries an internal report for skipping status or other data.
 * It is used as a dependency to test the skipping status or to match the data
 * content.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ReportQuery extends Report {
    private int reportExpiration = 0;
    private boolean withPrivateReport = false;
    private Map<String, String> keyMap;
    private Pattern[][] aPatternGroup = null;
    private Pattern[][] xPatternGroup = null;
    private java.lang.reflect.Method getReport = null;

    public ReportQuery(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "ReportQuery";

        if ((o = props.get("ReportClass")) != null && o instanceof String) {
            try {
                Class<?> cls = Class.forName((String) o);
                getReport = cls.getMethod("getReport",
                    new Class[]{String.class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to get 'getReport'" +
                  " method in: " + (String) o + ": " + Event.traceStack(e)));
            }
            if (!java.lang.reflect.Modifier.isStatic(getReport.getModifiers()))
                throw(new IllegalArgumentException("getReport: not a static " +
                  "method of: " + (String) o));
        }
        else if ((o = props.get("WithPrivateReport")) != null &&
            "true".equals((String) o)) // for private report
            withPrivateReport = true;

        if ((o = props.get("ReportExpiration")) != null &&
            (reportExpiration = Integer.parseInt((String) o)) < 0)
            reportExpiration = 0;

        if (keyList != null && keyList.length > 0) try {
            Perl5Compiler pc = new Perl5Compiler();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed" + e.getMessage()));
        }

        keyMap = new HashMap<String, String>();
        if ((o = props.get("KeyMap")) != null && o instanceof Map) {
            Map map = (Map) o;
            for (Object ky : map.keySet()) {
                if ((o = map.get(ky)) != null && o instanceof String)
                    keyMap.put((String) ky, (String) o);
            }
        }
    }

    public Map<String, Object> generateReport(long currentTime)
        throws Exception {
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

        if (withPrivateReport) // for private report
            r = MonitorUtils.getPrivateReport();
        else if (getReport == null)
            r = MonitorUtils.getReport(reportName);
        else
            r = (Map) getReport.invoke(null, new Object[] {reportName});

        if (r == null)
            throw(new IllegalArgumentException("no such report: " +reportName));
        else if (keyMap.size() > 0) { // copy content over with keyMap
            String str;
            for (String key : keyMap.keySet()) {
                str = keyMap.get(key);
                if (str == null || str.length() <= 0 || !r.containsKey(str))
                    continue;
                report.put(key, r.get(str));
            }
            if (!keyMap.containsKey("TestTime") && !r.containsKey("TestTime"))
                report.put("TestTime", String.valueOf(currentTime));
            r.clear();
        }
        else  { // copy the content over
            for (Object ky : r.keySet())
                report.put((String) ky, r.get(ky));
            r.clear();
        }

        if (keyList == null) { // only checks the SkippingStatus
            o = report.get("SkippingStatus");
            if (o != null && o instanceof String && ((String) o).length() > 0)
                skip = Integer.parseInt((String) o);
            else
                skip = EXCEPTION;
        }
        else if (keyList.length > 0) {
            o = report.get(keyList[0]);
            if (o == null || o instanceof String) { // normal case
                String text = (String) o;
                if (text == null)
                    text = "";

                if (reportExpiration > 0) {
                    if ((o = report.get("TestTime")) != null && currentTime -
                        reportExpiration >= Long.parseLong((String) o))
                        skip = DISABLED;
                    else
                        skip = NOSKIP;
                }
                else if (MonitorUtils.filter(text, aPatternGroup, pm, true) &&
                    !MonitorUtils.filter(text, xPatternGroup, pm, false))
                    skip = NOSKIP;
                else if ((o = report.get("Status")) != null &&
                    TimeWindows.NORMAL != Integer.parseInt((String) o))
                    skip = DISABLED;
                else
                    skip = DISABLED;
            }
            else
                skip = NOSKIP;
        }
        else if (reportExpiration > 0) { // check if testTime expired or not
            if ((o = report.get("TestTime")) != null && currentTime -
                reportExpiration >= Long.parseLong((String) o)) // expired
                skip = DISABLED;
            else
                skip = NOSKIP;
        }
        else { // check the status
            if ((o = report.get("Status")) != null &&
                TimeWindows.NORMAL != Integer.parseInt((String) o))
                skip = DISABLED;
            else
                skip = NOSKIP;
        }

        if (statsLogger != null) {
            String line;
            StringBuffer strBuf =
                new StringBuffer(Event.dateFormat(new Date(currentTime)));

            if (template != null) {
                line = template.substitute(template.copyText(), report);
                if (line != null)
                    strBuf.append(" " + line);
            }
            else if (keyList != null) {
                for (int i=0; i<keyList.length; i++) {
                    line = keyList[i];
                    if ((o = report.get(line)) != null && o instanceof String) {
                        strBuf.append(" " + line + " " + (String) o);
                    }
                }
            }
            else {
                int i = 0;
                Iterator iter;
                String[] keys = new String[report.size()];
                for (iter=report.keySet().iterator(); iter.hasNext();)
                    keys[i++] = (String) iter.next();
                Arrays.sort(keys);
                for (i=0; i<keys.length; i++) {
                    if((o = report.get(keys[i])) != null && o instanceof String)
                        strBuf.append(" " + keys[i] + " " + (String) o);
                }
            }
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        if (disableMode < 0) { // reverse the skipping status
            if (skip == DISABLED)
                skip = NOSKIP;
            else if (skip == NOSKIP)
                skip = DISABLED;
        }

        return report;
    }

    public void destroy() {
        super.destroy();
        getReport = null;
        aPatternGroup = null;
        xPatternGroup = null;
        if (keyMap != null) {
            keyMap.clear();
            keyMap = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
