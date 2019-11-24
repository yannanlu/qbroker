package org.qbroker.monitor;

/* ActionSkipper.java - an MonitorReport to control skipping status */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * ActionSkipper controls whether to skip the actions according to step info
 *<br><br>
 * Use ActionSkiper to bypass actions.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ActionSkipper extends Report {
    private TimeWindows tw = null;

    public ActionSkipper(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "ActionSkipper";

        if (disableMode != 0) { // initialize disabled report
            Map h = Utils.cloneProperties((Map) props.get("ActiveTime"));
            List list = (List) h.get("TimeWindow");
            int n = 0;
            if (list != null) {
                n = list.size();
                for (int i=0; i<n; i++)
                    if (((Map) list.get(i)).get("Threshold") != null)
                        ((Map) list.get(i)).remove("Threshold");
            }
            tw = new TimeWindows(h);
        }
    }

    public Map<String, Object> generateReport(long currentTime) {
        report.clear();
        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                report.put("SampleTime", new long[] {currentTime});
                report.put("SerialNumber", String.valueOf(serialNumber));
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

        if (disableMode != 0 && tw != null) {
            int status = tw.check(currentTime, 0L);
            if ((status != TimeWindows.NORMAL && disableMode > 0) ||
                (status == TimeWindows.NORMAL && disableMode < 0)) {
                skip = DISABLED;
                return report;
            }
        }

        report.put("SampleTime", new long[] {currentTime});
        report.put("SerialNumber", String.valueOf(serialNumber));
        return report;
    }
}
