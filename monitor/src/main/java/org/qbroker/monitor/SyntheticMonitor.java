package org.qbroker.monitor;

/* SyntheticMonitor.java - a monitor with scripts on a web site */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;
import org.qbroker.net.ScriptedBrowser;

/**
 * SyntheticMonitor runs a script as a headless browser on a web site. The
 * first step is always to get the page of the given URI. If NextTask is
 * defined in the config, each of the tasks will be executed one after another
 * in the listed order. If any task fails, the entire test will be fail.
 *<br><br>
 * Each member of NextTask will contain Operation, LocatorType, LocatorValue,
 * SleepTime in ms and WaitTime in sec.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SyntheticMonitor extends Monitor {
    private String uri;
    private Map<String, Object> ph = null;
    private int previousWebStatus, webStatusOffset;
    private long sessionTimeout = 0, previousTime = 0;
    private int[][] taskInfo;
    private String[][] taskData;
    private final static int TASK_ID = 0;
    private final static int TASK_TYPE = 1;
    private final static int TASK_PAUSE = 2;
    private final static int TASK_WAIT = 3;
    private final static int TASK_EXTRA = 4;
    private final static String webStatusText[] = {"Test failed",
        "Service is OK", "Protocol error", "Pattern not matched",
        "Not Multipart", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Service is down"};

    public SyntheticMonitor(Map props) {
        super(props);
        Object o;
        int k, n;
        String str;
        List list;
        Map map;

        if (type == null)
            type = "SyntheticMonitor";

        if (description == null)
            description = "monitor a web site";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        // convert the fields of ##MM## into __MM__ to avoid URI Exception
        Template pathTemplate = new Template(uri, "##[a-zA-Z]+##");
        if (pathTemplate.numberOfFields() > 0) {
            String [] allFields = pathTemplate.getAllFields();
            for (int i=0; i<allFields.length; i++) {
                uri = pathTemplate.substitute(allFields[i],
                    "__" + allFields[i] + "__", uri);
            }
        }

        k = 0;
        if ((o = props.get("NextTask")) != null && o instanceof List){
            list = (List) o;
            k = list.size();
            taskInfo = new int[k][];
            taskData = new String[k][];
        }
        else {
            list = new ArrayList();
            taskInfo = new int[0][];
            taskData = new String[0][];
        }

        for (int i=0; i<k; i++) {
            if ((o = list.get(i)) == null || !(o instanceof Map))
                continue;
            taskInfo[i] = new int[]{0, 0, 0, 0, 0};
            taskData[i] = new String[]{null, null, null, null, null};
            map = (Map) o;
            str = (String) map.get("Operation");
            taskInfo[i][TASK_ID] = ScriptedBrowser.parseOperation(str);
            str = (String) map.get("LocatorType");
            taskInfo[i][TASK_TYPE] = ScriptedBrowser.parseType(str);
            str = (String) map.get("LocatorType2");
            taskInfo[i][TASK_EXTRA] = ScriptedBrowser.parseType(str);
            str = (String) map.get("PauseTime");
            taskInfo[i][TASK_PAUSE] = (str != null) ? Integer.parseInt(str) :-1;
            str = (String) map.get("WaitTime");
            taskInfo[i][TASK_WAIT] = (str != null) ? Integer.parseInt(str) : -1;
            taskData[i][TASK_ID] = (String) map.get("KeyData");
            taskData[i][TASK_TYPE] = (String) map.get("LocatorText");
            taskData[i][TASK_EXTRA] = (String) map.get("LocatorText2");
        }

        ph = Utils.cloneProperties(props);
        ph.put("URI", uri);
        ph.remove("DependencyGroup");
        ph.remove("DisabeMode");
        ph.remove("NextTask");

        // disable default logging
        Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.FATAL);
        Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.FATAL);

        previousWebStatus = -10;
        webStatusOffset = 0 - WebTester.TESTFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int i, rc = 0;
        long tm;
        String str;
        ScriptedBrowser browser = null;

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

        browser = new ScriptedBrowser(ph);
        tm = System.currentTimeMillis();
        str = browser.get(uri);
        for (i=0; i<taskInfo.length; i++) {
            rc = 0;
            str = null;
            try {
                switch (taskInfo[i][TASK_ID]) {
                  case ScriptedBrowser.GET:
                  case ScriptedBrowser.GETTITLE:
                  case ScriptedBrowser.GETSOURCE:
                  case ScriptedBrowser.PAUSE:
                    if (taskInfo[i][TASK_PAUSE] > 0) try {
                        Thread.sleep((long) taskInfo[i][TASK_PAUSE]);
                    }
                    catch (Exception e) {
                    }
                    if (taskInfo[i][TASK_ID] == ScriptedBrowser.GET) {
                        str = browser.get(uri);
                    }
                    else if (taskInfo[i][TASK_ID] == ScriptedBrowser.GETTITLE) {
                        str = browser.getTitle();
                        report.put("Title", str);
                    }
                    else if (taskInfo[i][TASK_ID] == ScriptedBrowser.GETSOURCE){
                        str = browser.getPageSource();
                        report.put("PageSource", str);
                    }
                    break;
                  case ScriptedBrowser.FIND_CLICK:
                    str = browser.findAndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case ScriptedBrowser.FIND2_CLICK:
                    str = browser.find2AndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_EXTRA],
                        taskData[i][TASK_EXTRA], (long)taskInfo[i][TASK_PAUSE]);
                    break;
                  case ScriptedBrowser.WAIT_CLICK:
                    str = browser.waitAndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case ScriptedBrowser.WAIT2_CLICK:
                    str = browser.waitAndFindClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        taskInfo[i][TASK_EXTRA], taskData[i][TASK_EXTRA],
                        (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case ScriptedBrowser.FIND_SEND:
                    str = browser.findAndSend(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskData[i][TASK_ID]);
                    break;
                  case ScriptedBrowser.WAIT_SEND:
                    str = browser.waitAndSend(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        taskData[i][TASK_ID]);
                    break;
                  case ScriptedBrowser.WAIT_FIND:
                    str = browser.waitAndFind(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  default:
                    rc = -1;
                    str = "Wrong Type: " + taskInfo[i][TASK_ID];
                }
            }
            catch (Exception e) {
                rc = -1;
                str = e.toString();
                report.put("Task_ID", String.valueOf(i+1));
                report.put("Error", str);
                break;
            }
            if (rc < 0) {
                report.put("Task_ID", String.valueOf(i+1));
                report.put("Error", str);
                break;
            }
        }
        browser.close();
        tm = System.currentTimeMillis() - tm;
        report.put("ReturnCode", String.valueOf(rc));

        if (statsLogger != null) {
            String stats = Event.dateFormat(new Date(currentTime)) +
                " " + name + " " + rc + " " + tm;
            report.put("Stats", stats);
            try {
                statsLogger.log(stats);
            }
            catch (Exception e) {
            }
        }

        if (disableMode != 0) {
            if (rc == WebTester.TESTOK)
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else if (rc == WebTester.PATTERNNOHIT)
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
            else
                skip = EXCEPTION;
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int webStatus, level = 0;
        long size;
        StringBuffer strBuf = new StringBuffer();
        String id = null;
        Object o;
        if ((o = latest.get("ReturnCode")) != null &&
            o instanceof String) {
            webStatus = Integer.parseInt((String) o);
        }
        else
            webStatus = WebTester.TESTFAILED;
        if (webStatus != 0)
            id = (String) latest.get("Task_ID");

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
            if (webStatus != WebTester.TESTOK) {
                if (previousWebStatus == WebTester.TESTOK)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousWebStatus != WebTester.TESTOK) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousWebStatus = webStatus;
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
                if (count == 1 || count == tolerance) // react only 
                    break;
                return null;
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
            strBuf.append(webStatusText[webStatus + webStatusOffset]);
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
       event.setAttribute("webStatus",webStatusText[webStatus+webStatusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (id != null) {
            event.setAttribute("task_id", id);
            event.setAttribute("error_msg", (String) latest.get("Error"));
        }

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

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousWebStatus", String.valueOf(previousWebStatus));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, wStatus, pStatus, sNumber;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
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

        if ((o = chkpt.get("PreviousWebStatus")) != null)
            wStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousWebStatus = wStatus;
    }

    public void destroy() {
        super.destroy();
        if (ph != null)
            ph.clear();
    }

    protected void finalize() {
        destroy();
    }

    public static void main(String args[]) {
        String filename = null;
        Monitor monitor = null;

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'I':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
            long tm = System.currentTimeMillis();
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            monitor = new SyntheticMonitor(ph);
            Map r = monitor.generateReport(0L);
            String str = (String) r.get("ReturnCode");
            if (str != null) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else if ("0".equals(str))
                    System.out.println("returnCode: " + str);
                else
                    System.out.println("error: " + r.get("Error"));
                if (r.containsKey("Title"))
                    System.out.println("title: " + r.get("Title"));
                if (r.containsKey("PageSource"))
                    System.out.println("source: " + r.get("PageSource"));
            }
            else
                System.out.println("failed to test");
            if (monitor != null)
                monitor.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (monitor != null)
                monitor.destroy();
        }
    }

    private static void printUsage() {
        System.out.println("SyntheticMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("SyntheticMonitor: run a Selenium script on a website");
        System.out.println("Usage: java org.qbroker.monitor.SyntheticMonitor -I cfg.json");
    }
}
