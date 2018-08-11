package org.qbroker.monitor;

/* ServiceMonitor.java - a monitor checking a service on Monit status page */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.Evaluation;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.MonitorReport;

/**
 * ServiceMonitor queries the status page of Monit and extrats data for a
 * specific service. Then it applies the EvalTemplate on the data and evaluates
 * the expression. If the evaluation returns false, the service is OK.
 * Otherwise, the service is in an error condition. ServiceMonitor escalates
 * the status based on the error conditions and the occurrance.
 *<br/><br/>
 * Currently, it supports 3 types of data sources, such as http/https, script
 * and report for private reports.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ServiceMonitor extends Monitor {
    private String uri, serviceName, serviceType;
    private int previousState;
    private MonitorReport reporter = null;
    private Perl5Matcher pm = null;
    private List<Map> mapList = null;
    private Map<String, TextSubstitution> tSub;
    private boolean isScript = false;
    private String[] attrs = null;
    public final static String[] systemAttrs = { // for system
        "status",
        "monitor",
        "cpu_user",
        "cpu_system",
        "cpu_wait",
        "load_avg01",
        "load_avg05",
        "load_avg15",
        "memory_percent",
        "memory_kilobyte",
        "swap_percent",
        "swap_kilobyte"
    };
    public final static String[] remotehostAttrs = { // for a host
        "status",
        "monitor",
        "port_responsetime"
    };
    public final static String[] processAttrs = { // for a process
        "status",
        "monitor",
        "pid",
        "cpu_percent",
        "cpu_percenttotal",
        "memory_percent",
        "memory_percenttotal",
        "memory_kilobyte",
        "memory_kilobytetotal",
        "children",
        "threads",
        "uid",
        "gid",
        "ppid",
        "uptime"
    };
    public final static String[] fileAttrs = { // for a file
        "status",
        "monitor",
        "mode",
        "uid",
        "gid",
        "size",
        "timestamp"
    };
    public final static String[] filesystemAttrs = { // for a filesystem
        "status",
        "monitor",
        "mode",
        "uid",
        "gid",
        "flags",
        "block_percent",
        "block_usage",
        "block_total",
        "inode_percent",
        "inode_usage",
        "inode_total"
    };
    public final static String[] serviceTypes = { // for types
        "filesystem",
        "directory",
        "file",
        "process",
        "remotehost",
        "system",
        "fifo",
        "program"
    };
    public final static String[] monitStatus = { // for monit status
        "checksum",
        "resource",
        "timeout",
        "timestamp",
        "size",
        "connection",
        "permission",
        "uid",
        "gid",
        "nonexist",
        "invlid"
    };

    public ServiceMonitor(Map props) {
        super(props);
        Object o;
        int n;
        String str = null;

        if (type == null)
            type = "ServiceMonitor";

        if (description == null)
            description = "monitor a service via Monit";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            Map<String, Object> h = new HashMap<String, Object>();
            String path, scheme = null;
            uri = MonitorUtils.substitute((String) o, template);

            try {
                URI u = new URI(uri);
                scheme = u.getScheme();
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException("Bad URI: " + uri +
                    ": " +e.toString()));
            }

            h.put("Name", name);
            h.put("URI", uri);
            if ((o = props.get("Timeout")) != null)
                h.put("Timeout", o);
            if ((o = props.get("Debug")) != null)
                h.put("Debug", o);

            if ("http".equals(scheme) || "https".equals(scheme)) { // for web
                if ((o = props.get("MaxBytes")) != null)
                    h.put("MaxBytes", o);
                else
                    h.put("MaxBytes", "0");
                if ((o = props.get("TrustAllCertificates")) != null)
                    h.put("TrustAllCertificates", o);
                if ((o = props.get("BasicAuthorization")) != null)
                    h.put("BasicAuthorization", o);
                else if ((o = props.get("AuthString")) != null)
                    h.put("AuthString", o);
                else if ((o = props.get("Username")) != null) {
                    h.put("Username", o);
                    if ((o = props.get("Password")) != null)
                        h.put("Password", o);
                    else if ((o = props.get("EncryptedPassword")) != null)
                        h.put("EncryptedPassword", o);
                }

                reporter = new WebTester(h);
            }
            else if ("script".equals(scheme)) { // for script
                isScript = true;
                if ((o = props.get("Script")) != null)
                    h.put("Script", o);
                if ((o = props.get("Timeout")) != null)
                    h.put("ScriptTimeout", o);
                reporter = new ScriptLauncher(h);
            }
            else if ("report".equals(scheme)) { // for private report
                reporter = null;
            }
            else { // not supported
                h.clear();
                throw(new IllegalArgumentException(name +
                    " URI is not supported"));
            }
            h.clear();
        }
        else
            throw(new IllegalArgumentException("URI is not defined"));

        if ((o = MonitorUtils.select(props.get("ServiceName"))) != null)
            serviceName = MonitorUtils.substitute((String) o, template);
        else
            serviceName = name;

        if ((o = props.get("ServiceType")) == null) {
            serviceType = "system";
            attrs = systemAttrs;
        }
        else if ("process".equalsIgnoreCase((String) o)) {
            serviceType = "process";
            attrs = processAttrs;
        }
        else if ("remotehost".equalsIgnoreCase((String) o)) {
            serviceType = "remotehost";
            attrs = remotehostAttrs;
        }
        else if ("filesystem".equalsIgnoreCase((String) o)) {
            serviceType = "filesystem";
            attrs = filesystemAttrs;
        }
        else if ("file".equalsIgnoreCase((String) o)) {
            serviceType = "file";
            attrs = fileAttrs;
        }
        else {
            serviceType = "system";
            attrs = systemAttrs;
        }

        Perl5Matcher pm = new Perl5Matcher();
        if ((o = MonitorUtils.select(props.get("EvalTemplate"))) != null) {
            Map<String, Object> map = new HashMap<String, Object>();
            mapList = new ArrayList<Map>();
            try {
                Perl5Compiler pc = new Perl5Compiler();
                map.put("Pattern", pc.compile(".")); 
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(
                    "failed to compile patterns: " + e.toString()));
            }
            map.put("Template",
                new Template(MonitorUtils.substitute((String) o, template)));
            mapList.add(map); 
        }
        else if ((o = props.get("EvalMappingRule")) == null)
            throw(new IllegalArgumentException(
                "Neither EvalTemplate nor EvalMappingRule is defined"));
        else if (!(o instanceof List))
            throw(new IllegalArgumentException("EvalMappingRule is not List"));
        else try {
            Perl5Compiler pc = new Perl5Compiler();
            mapList = MonitorUtils.getGenericMapList((List) o, pc, template);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to compile patterns: " +
                e.toString()));
        }

        if (mapList == null || mapList.size() <= 0)
            throw(new IllegalArgumentException("EvalMappingRule is empty"));
        else { // check each rule
            HashSet<String> hset = new HashSet<String>();
            Template temp;
            int i = 0;
            for (String key : attrs)
                hset.add(key);
            for (Map map : mapList) { // check each template
                i ++;
                temp = (Template) map.get("Template");
                if (temp == null || temp.numberOfFields() <= 0)
                    throw(new IllegalArgumentException("EvalTemplate, " + i +
                        ", is either null or empty"));
                for (String key : temp.getAllFields()) { // verify each field
                    if ("state".equals(key) || "monit".equals(key))  
                        continue;
                    if (!hset.contains(key)) {
                        hset.clear();
                        throw(new IllegalArgumentException(
                            "found illegal field, "+ key +
                            ", in EvalTemplate " + i));
                    }
                }
            }
            hset.clear();
        }

        tSub = new HashMap<String, TextSubstitution>();
        for (String key : attrs) {
            int i = key.indexOf("_");
            if (i > 0)
                str = key.substring(i+1);
            else
                str = key;
            if (!tSub.containsKey(str))
                tSub.put(str,
                    new TextSubstitution("s/^.+<" + str + ">([^<]+)<.+$/$1/"));
        }

        previousState = -3;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        String text = null, mStatus = null;
        int state = -1;

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

        report.put("URI", uri);

        if (reporter != null) {
            Map r;
            Object o;
            int returnCode;

            try {
                r = reporter.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException(name + " failed to get report: " +
                    Event.traceStack(e)));
            }

            if ((o = r.get("ReturnCode")) != null)
                returnCode = Integer.parseInt((String) o);
            else {
                r.clear();
                throw(new IOException(name + ": request failed on " + uri));
            }
            if (isScript) {
                if (returnCode != 0)
                    text = getData((String) r.remove("Output"));
            }
            else if (returnCode == 0) {
                int n = 0;
                text = (String) r.remove("Response");
                if (text != null && (n = text.indexOf("\r\n\r\n")) > 0)
                    text = getData(text.substring(n+4));
                else
                    text = null;
            }
            r.clear();
        }
        else { // for private report
            Map mp = MonitorUtils.getPrivateReport();
            if (mp != null && mp.size() == 1) { // get wrapped content
                Object o = mp.get("body");
                if (o != null && o instanceof String)
                    text = (String) o;
                mp.clear();
            }
            else if (mp != null) // not expected so just ignore it
                mp.clear();
        }

        if (text != null && text.length() > 0) { // parse dataset for stats
            int i, j, k;
            String str = tSub.get("status").substitute(text);
            state = Integer.parseInt(str);
            report.put("state", str);
            mStatus = tSub.get("monitor").substitute(text);
            report.put("monit", mStatus);

            if ("1".equals(mStatus) && state != 512) for (String key : attrs) {
                if ("status".equals(key) || "monitor".equals(key))
                    continue;
                if ((i = key.indexOf("_")) > 0) {
                    String value;
                    str = key.substring(0, i);
                    j = text.indexOf("<"+str+">");
                    k = text.indexOf("</"+str+">");
                    if (j > 0 && k > j)
                        str = text.substring(j, k+3+str.length());
                    else
                        throw(new IllegalArgumentException(name +
                            " failed to parse data for "+str+ ": " +j+ "/" +k));
                    value = tSub.get(key.substring(i+1)).substitute(str);
                    if (str.equals(value)) // no such attribute
                        report.put(key, "-1");
                    else
                        report.put(key, value);
                }
                else {
                    str = tSub.get(key).substitute(text);
                    if (text.equals(str)) // no such attribute
                        report.put(key, "-1");
                    else
                        report.put(key, str); 
                }
            }
        }
        else
            throw(new IllegalArgumentException(name+" failed to process data"));

        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(serviceName);
            strBuf.append(":" + mStatus + " " + String.valueOf(state));
            if ("1".equals(mStatus) && state != 512) for (String key : attrs) {
                if ("status".equals(key) || "monitor".equals(key))
                    continue;
                strBuf.append(" " + (String) report.get(key));
            }
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        if (disableMode > 0 && !"1".equals(mStatus))
            skip = DISABLED;
        else if (disableMode < 0 && "1".equals(mStatus))
            skip = DISABLED;
        else if (disableMode == 0 && !"1".equals(mStatus))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, state = -1;
        StringBuffer strBuf = new StringBuffer();
        String expr = null, mStatus = "-1";
        Object o;

        if ((o = latest.get("monit")) != null)
            mStatus = (String) o;
        if ((o = latest.get("state")) != null)
            state = Integer.parseInt((String) o);
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
            if (state == 512) { // service does not exist
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append(serviceType + " of " + serviceName +
                    " does not exist");
                if (previousState != state)
                    actionCount = 1;
            }
            else { // with stats
                Template temp = MonitorUtils.getMappedTemplate(serviceName,
                    mapList, pm);
                if (temp != null) {
                    expr = temp.substitute(temp.copyText(), latest);
                    o = Evaluation.evaluate(expr);
                }
                if (temp == null) { // no template found
                    level = Event.NOTICE;
                    strBuf.append("Exception! failed to find EvalTemplate");
                    if (previousState != state)
                        actionCount = 1;
                }
                else if (o == null || !(o instanceof Integer))  { // exception
                    level = Event.NOTICE;
                    strBuf.append("Exception! failed to evaluate data: "+expr);
                    if (previousState != state)
                        actionCount = 1;
                }
                else if (((Integer) o).intValue() == 0) { // ok
                    if (previousState != state) {
                        strBuf.append(serviceType + " of " + serviceName +
                            " is OK");
                        actionCount = 1;
                        if (normalStep > 0)
                            step = normalStep;
                    }
                }
                else { // error
                    if (status != TimeWindows.BLACKOUT) { // for normal case
                        level = Event.ERR;
                        if (step > 0)
                            step = 0;
                    }
                    strBuf.append(serviceType + " of " + serviceName +
                        " is in error condition");
                    if (previousState != state)
                        actionCount = 1;
                }
            }
            break;
        }
        previousStatus = status;
        previousState = state;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            count = actionCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // found errors
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

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
            for (String key : attrs) {
                if ("status".equals(key) || "monitor".equals(key))
                    continue;
                event.setAttribute(key, "N/A");
            }
        }
        else {
            count = actionCount;
            for (String key : attrs) {
                if ("status".equals(key) || "monitor".equals(key))
                    continue;
                o = latest.get(key);
                if (o != null)
                    event.setAttribute(key, (String) o);
                else
                    event.setAttribute(key, "N/A");
            }
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("serviceName", serviceName);
        event.setAttribute("serviceType", serviceType);
        event.setAttribute("monit", mStatus);
        event.setAttribute("state", String.valueOf(state));
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));

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


    private String getData(String text) {
        int i, j, k;
        if (text == null || text.length() <= 0)
            return text;
        if ((k = text.indexOf("<name>"+serviceName+"</name>")) < 0)
            return "";
        i = text.lastIndexOf("<service type=", k);
        j = text.indexOf("</service>", k);
        if (i > 0 && j > i)
            return text.substring(i, j+10);
        else
            return "";
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousState", String.valueOf(previousState));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pStatus, sNumber, pState;

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

        if ((o = chkpt.get("PreviousState")) != null)
            pState = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousState = pState;
    }

    public void destroy() {
        super.destroy();
        if (mapList != null) {
            for (Map map : mapList)
                map.clear();
            mapList.clear();
            mapList = null;
        }
        if (reporter != null)
            reporter.destroy();
        tSub.clear();
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

            monitor = new ServiceMonitor(ph);
            Map r = monitor.generateReport(tm);
            String str = (String) r.get("monit");
            if (str != null) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else {
                    Object o;
                    String[] attrs;
                    if ((o = ph.get("ServiceType")) == null)
                        attrs = systemAttrs;
                    else if ("process".equalsIgnoreCase((String) o))
                        attrs = processAttrs;
                    else if ("remotehost".equalsIgnoreCase((String) o))
                        attrs = remotehostAttrs;
                    else if ("filesystem".equalsIgnoreCase((String) o))
                        attrs = filesystemAttrs;
                    else if ("file".equalsIgnoreCase((String) o))
                        attrs = fileAttrs;
                    else
                        attrs = systemAttrs;

                    System.out.print(str + ": " + (String) r.get("state"));
                    for (String key : attrs) {
                        if ("status".equals(key) || "monitor".equals(key))
                            continue;
                        System.out.print(" " + (String) r.get(key));
                    }
                    System.out.println();
                }
            }
            else
                System.out.println("failed to get service stats");
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
        System.out.println("ServiceMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("ServiceMonitor: monitor a service via Monit");
        System.out.println("Usage: java org.qbroker.monitor.ServiceMonitor -I cfg.json");
    }
}
