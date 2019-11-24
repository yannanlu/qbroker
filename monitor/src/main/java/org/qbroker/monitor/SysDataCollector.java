package org.qbroker.monitor;

/* SysDataCollector.java - a report collecting stats of OS */

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.qbroker.common.RunCommand;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * SysDataMonitor collects the basic stats of OS and monitors their ranges.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SysDataCollector extends Report {
    private String topCommand;
    private TextSubstitution tsub;
    private int timeout;

    public SysDataCollector(Map props) {
        super(props);
        Object o;

        if (type == null)
            type = "SysDataMonitor";

        if ((o = MonitorUtils.select(props.get("TopCommand"))) != null)
            topCommand = (String) o;
        else
            topCommand = "/usr/bin/top -b -n 2 -d 2 -u nobody";

        if ((o = props.get("Timeout")) == null ||
            (timeout = 1000*Integer.parseInt((String) o)) < 0)
            timeout = 60000;

        tsub = new TextSubstitution("s/\n/ /g");
    }

    public Map<String, Object> generateReport(long currentTime)
        throws TimeoutException {
        List<String> lineBuffer = new ArrayList<String>();

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

        String output = RunCommand.exec(topCommand, timeout);

        if (output != null && output.length() > 0) {
            int i, k, n;
            StringBuffer strBuf = new StringBuffer();
            i = output.indexOf("\n\n\n");
            if (i > 0) {
                int j = output.indexOf("\n\n", i+3);
                if (j > 0)
                    output = output.substring(i+3, j);
            }
            else {
                i = output.indexOf("\n\n");
                if (i > 0)
                    output = output.substring(0, i);
            }
            output = tsub.substitute(output);
        }
        else {
            throw(new TimeoutException("empty output"));
        }

        report.put("NumberPids", "0");
        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(name + " " + output);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        if (disableMode > 0)
            skip = DISABLED;

        return report;
    }
}
