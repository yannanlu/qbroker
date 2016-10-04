 package org.qbroker.monitor;
/* ReportAggregator.java - an aggregator on internal reports */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.qbroker.common.Aggregator;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;
import org.qbroker.event.Event;

/**
 * ReportAggregator aggregates multiple internal reports for certain data.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ReportAggregator extends Report {
    private int reportExpiration = 0;
    private Aggregator aggr = null;
    private String[] rptList = null;
    private java.lang.reflect.Method getReport = null;

    public ReportAggregator(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "ReportAggregator";

        if ((o = props.get("ReportClass")) != null && o instanceof String) {
            try {
                Class<?> cls = Class.forName((String) o);
                getReport=cls.getMethod("getReport", new Class[]{String.class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to get 'getReport'" +
                  " method in: " + (String) o + ": " + Event.traceStack(e)));
            }
            if (!java.lang.reflect.Modifier.isStatic(getReport.getModifiers()))
                throw(new IllegalArgumentException("getReport: not a static " +
                  "method of: " + (String) o));
        }

        if ((o = props.get("ReportList")) != null && o instanceof List) {
            String str;
            List<String> list = new ArrayList<String>();
            for (Object obj : (List) o) {
                str = MonitorUtils.select(obj);
                list.add(MonitorUtils.substitute(str, template));
            }
            rptList = list.toArray(new String[list.size()]);
        }
        else
            throw(new IllegalArgumentException("ReportList not well defined"));

        if ((o = props.get("Aggregation")) != null && o instanceof List)
            aggr = new Aggregator(name, (List) o);
        else
            throw(new IllegalArgumentException("Aggregation not well defined"));

        if ((o = props.get("ReportExpiration")) != null &&
            (reportExpiration = Integer.parseInt((String) o)) < 0)
            reportExpiration = 0;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws Exception {
        int i = 0;
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
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                return report;
            }
        }

        for (String rptName : rptList) {
            if (getReport == null)
                r = MonitorUtils.getReport(rptName);
            else
                r = (Map) getReport.invoke(null, new Object[] {rptName});

            if (r == null)
                throw(new IllegalArgumentException("no such report: "+rptName));
            else if (i <= 0) { // init the report
                aggr.initialize(currentTime, r, report);
                i ++;
            }
            else {
                aggr.aggregate(currentTime, r, report);
                i ++;
            }
        }

        return report;
    }

    public void destroy() {
        super.destroy();
        if (aggr != null)
            aggr.clear();
    }

    protected void finalize() {
        destroy();
    }
}
