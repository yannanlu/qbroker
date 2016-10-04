package org.qbroker.monitor;

/* AgeMonitor.java - a monitor watching an object's age */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.IOException;
import javax.management.JMException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.Requester;
import org.qbroker.net.SNMPConnector;
import org.qbroker.net.JMXRequester;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;
import org.qbroker.monitor.FileMonitor;
import org.qbroker.monitor.ProcessMonitor;
import org.qbroker.monitor.UnixlogMonitor;
import org.qbroker.monitor.ScriptLauncher;
import org.qbroker.monitor.DBRecord;
import org.qbroker.monitor.ReportQuery;
import org.qbroker.monitor.GenericList;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * AgeMonitor monitors the age of an object via its timestamp in the datablock;
 * It supports various schemes to get the datablock containing the timestamp.
 * If it gets an empty datablock or there is no match on the pattern,
 * AgeMonitor will use EmptyDataIgnored to decide how to react.  If it is set
 * to true, AgeMonitor will disable itself without Exceptions.  Otherwise,
 * AgeMonitor will throw an IOException.  Operation determines how to select
 * the data object.
 *<br/><br/>
 * In case the data does not contain the timestamp, or it is empty, AgeMonitor
 * will either disable itself or just throw exception on the bad data.
 * If EmptyDataIgnored is set true, AgeMonitor will ignore the bad data and
 * disable itself.  Otherwise, it will throw exception on the bad data.
 * By default, EmptyDataIgnored is set true.
 *<br/><br/>
 * There is a special treatment for the age of a UNIX process.
 * First, you should use /bin/ps -o user,pid,etime,args -u USER to
 * get the info of the process since etime is more accurate than the
 * stime.  On DateFormat, you should use D-HH:mm:ss for the etime.
 * On a Windows process, you can use tasklist.exe /V to get its CPUTime.  In
 * this case, D-HH:mm:ss should be used as the time pattern.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class AgeMonitor extends Monitor {
    private String uri, referenceName;
    private MonitorReport reporter;
    private Requester requester;
    private SNMPConnector snmp;
    protected JMXRequester jmxReq = null;
    private String queryStr, dataField, snmpOID = null, jsonPath = null;
    private TimeWindows tw = null;
    private Pattern pattern, patternLF;
    protected TextSubstitution tSub = null;
    private long timeDifference;
    private long previousTime, previousRefTime, previousLeadTime, t0 = 0L;
    private int triggerSize, timeOffset, previousTrigger, oid, op = NUM_MIN;
    private int updateCount, previousLevel;
    private boolean isNumber= false, isETime= false, emptyDataIgnored;
    private SimpleDateFormat dateFormat;
    private MonitorReport reference;
    private final static int OBJ_HTTP = 1;
    private final static int OBJ_SCRIPT = 2;
    private final static int OBJ_REPORT = 3;
    private final static int OBJ_PROC = 4;
    private final static int OBJ_FILE = 5;
    private final static int OBJ_FTP = 6;
    private final static int OBJ_TCP = 7;
    private final static int OBJ_UDP = 8;
    private final static int OBJ_LOG = 9;
    private final static int OBJ_JMS = 10;
    private final static int OBJ_JDBC = 11;
    private final static int OBJ_SNMP = 12;
    private final static int OBJ_JMX = 13;
    private final static int OBJ_PCF = 14;
    private final static int OBJ_SONIC = 15;
    private final static int OBJ_REQ = 16;
    private final static int NUM_MIN = 0;
    private final static int NUM_MAX = 1;
    private final static int NUM_FIRST = 2;
    private final static int NUM_LAST = 3;

    public AgeMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        URI u = null;

        if (type == null)
            type = "AgeMonitor";

        if (description != null)
            description = "monitor age of an object";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;
        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ("http".equals(u.getScheme()) || "https".equals(u.getScheme()))
            oid = OBJ_HTTP;
        else if ("script".equals(u.getScheme()))
            oid = OBJ_SCRIPT;
        else if ("report".equals(u.getScheme()))
            oid = OBJ_REPORT;
        else if ("proc".equals(u.getScheme()))
            oid = OBJ_PROC;
        else if ("file".equals(u.getScheme()))
            oid = OBJ_FILE;
        else if ("ftp".equals(u.getScheme()) || "sftp".equals(u.getScheme()))
            oid = OBJ_FTP;
/*
        else if ("wmq".equals(u.getScheme()))
            oid = OBJ_JMS;
*/
        else if ("tcp".equals(u.getScheme())) {
//            if ((o = props.get("ConnectionFactoryName")) != null)
//                oid = OBJ_JMS;
//            else
                oid = OBJ_TCP;
        }
        else if ("udp".equals(u.getScheme()))
            oid = OBJ_UDP;
        else if ("log".equals(u.getScheme()))
            oid = OBJ_LOG;
        else if ("jdbc".equals(u.getScheme()))
            oid = OBJ_JDBC;
        else if ("snmp".equals(u.getScheme()))
            oid = OBJ_SNMP;
        else if ("service".equals(u.getScheme()))
            oid = OBJ_JMX;
//        else if ((o = props.get("ConnectionFactoryName")) != null)
//            oid = OBJ_JMS;
        else
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((o = props.get("DateFormat")) == null || !(o instanceof String))
          throw(new IllegalArgumentException("DateFormat is not well defined"));
        dateFormat = new SimpleDateFormat((String) o);
        if ("SSS".equals((String) o))
            isNumber = true;
        if ((o = props.get("TimeZone")) != null)
            dateFormat.setTimeZone(TimeZone.getTimeZone((String) o));

        if ((o = props.get("Pattern")) == null || !(o instanceof String))
            throw(new IllegalArgumentException("Pattern is not well defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            String ps = MonitorUtils.substitute((String) o, template);
            pattern = pc.compile(ps);
            patternLF = pc.compile("\\n");
            if ((o = props.get("Substitution")) != null)
                tSub = new TextSubstitution((String) o);
            if (oid == OBJ_SCRIPT) { // for script only
                aPatternGroup = MonitorUtils.getPatterns("PatternGroup",
                    props, pc);
                xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",
                    props, pc);
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.getMessage()));
        }

        reporter = null;
        requester = null;
        h.put("Name", name);
        h.put("URI", uri);
        h.put("Timeout", (String) props.get("Timeout"));
        h.put("Step", "1");
        Map<String, Object> map = new HashMap<String, Object>();
        switch (oid) {
          case OBJ_FILE:
          case OBJ_FTP:
            reporter = new FileMonitor(h);
            isNumber = true;
            break;
          case OBJ_TCP:
            jsonPath = (String) MonitorUtils.select(props.get("JSONPath"));
            jsonPath = MonitorUtils.substitute(jsonPath, template);
            queryStr = (String)MonitorUtils.select(props.get("RequestCommand"));
            queryStr = MonitorUtils.substitute(queryStr, template);
            h.put("Operation", "request");
            h.put("ClassName", "org.qbroker.persister.StreamPersister");
            h.put("Capacity", "2");
            h.put("Partition", "0,0");
            h.put("DisplayMask", "0");
            h.put("SOTimeout", "5");
            h.put("TextMode", "1");
            h.put("EOTBytes", "0x0a");
            h.put("Template", "##body## ~ ReturnCode ~ ##ReturnCode##\n");
            map.put("ClassName", "org.qbroker.event.EventParser");
            h.put("Parser", map);
            requester = GenericList.initRequester(h,
                "org.qbroker.flow.GenericRequester", name);
            break;
          case OBJ_UDP:
            jsonPath = (String) MonitorUtils.select(props.get("JSONPath"));
            jsonPath = MonitorUtils.substitute(jsonPath, template);
            queryStr = (String)MonitorUtils.select(props.get("RequestCommand"));
            queryStr = MonitorUtils.substitute(queryStr, template);
            h.put("Operation", "inquire");
            h.put("ClassName", "org.qbroker.persister.PacketPersister");
            h.put("URIField", "UDP");
            h.put("Capacity", "2");
            h.put("Partition", "0,0");
            h.put("DisplayMask", "0");
            h.put("SOTimeout", "5");
            h.put("TextMode", "1");
            h.put("Template", "##body## ~ ReturnCode ~ ##ReturnCode##\n");
            map.put("ClassName", "org.qbroker.event.EventParser");
            h.put("Parser", map);
            requester = GenericList.initRequester(h,
                "org.qbroker.flow.GenericRequester", name);
            break;
          case OBJ_HTTP:
            if ((o = props.get("MaxBytes")) != null)
                h.put("MaxBytes", o);
            else
                h.put("MaxBytes", "0");
            if ((o = props.get("EncryptedAuthorization")) != null)
                h.put("EncryptedAuthorization", o);
            else if ((o = props.get("AuthString")) != null)
                h.put("AuthString", o);
            else if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                h.put("Password", props.get("Password"));
            }
            reporter = new WebTester(h);
            break;
          case OBJ_SCRIPT:
            if ((o = props.get("Script")) != null)
                h.put("Script", o);
            h.put("ScriptTimeout", (String) props.get("Timeout"));
            reporter = new ScriptLauncher(h);
            break;
          case OBJ_REPORT:
            if ((o = props.get("ReportName")) != null)
                h.put("ReportName", o);
            if ((o = props.get("ReportExpiration")) != null)
                h.put("ReportExpiration", o);
            if ((o = props.get("ReportClass")) != null)
                h.put("ReportClass", o);
            if ((o = props.get("ReportKey")) != null)
                h.put("ReportKey", o);
            reporter = new ReportQuery(h);
            break;
          case OBJ_PROC:
            if ((o = props.get("PSCommand")) != null)
                h.put("PSCommand", o);
            if ((o = props.get("PidPattern")) != null)
                h.put("PidPattern", o);
            if ((o = props.get("PSFile")) != null)
                h.put("PSFile", o);
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
            h.put("PSTimeout", (String) props.get("Timeout"));
            reporter = new ProcessMonitor(h);
            if ("D-HH:mm:ss".equals(dateFormat.toPattern())) { // for etime only
                t0 = dateFormat.parse("0-00:00:00",
                    new ParsePosition(0)).getTime();
                isETime = true;
            }
            break;
          case OBJ_LOG:
            if ((o = props.get("TimePattern")) != null)
                h.put("TimePattern", o);
            if ((o = props.get("OrTimePattern")) != null)
                h.put("OrTimePattern", o);
            if ((o = props.get("ReferenceFile")) != null)
                h.put("ReferenceFile", o);
            if ((o = props.get("OldLogFile")) != null)
                h.put("OldLogFile", o);
            if ((o = props.get("LogSize")) != null)
                h.put("LogSize", o);
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
            if ((o = props.get("MaxNumberLogs")) != null)
                h.put("MaxNumberLogs", o);
            if ((o = props.get("MaxScannedLogs")) != null)
                h.put("MaxScannedLogs", o);
            if ((o = props.get("NumberDataFields")) != null)
                h.put("NumberDataFields", o);
            reporter = new UnixlogMonitor(h);
            break;
          case OBJ_JDBC:
            if ((o = props.get("DBDriver")) != null)
                h.put("DBDriver", o);
            if ((o = props.get("Username")) != null)
                h.put("Username", o);
            if ((o = props.get("Password")) != null)
                h.put("Password", o);
            if ((o = props.get("SQLQuery")) != null)
                h.put("SQLQuery", o);
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
            if ((o = props.get("FieldSeparator")) != null)
                h.put("FieldSeparator", o);
            reporter = new DBRecord(h);
            break;
          case OBJ_SNMP:
            if ((o = props.get("Version")) != null)
                h.put("Version", o);
            if ((o = props.get("Community")) != null)
                h.put("Community", o);
            h.put("Operation", "request");
            if ((o = props.get("OID")) != null)
                snmpOID = (String) o;
            if (snmpOID == null || snmpOID.length() <= 0)
                snmpOID = "1.3.6.1.4.1.4301.3.0.0";
            snmp = new SNMPConnector(h);
            reporter = null;
            break;
/*
          case OBJ_JMS:
            h.put("ContextFactory",
                MonitorUtils.select(props.get("ContextFactory")));
            if ((o = props.get("ContextFactory")) != null)
                h.put("ContextFactory", o);
            if ((o = props.get("ConnectionFactoryName")) != null)
                h.put("ConnectionFactoryName", o);
            else if ((o = props.get("ChannelName")) != null)
                h.put("ChannelName", o);
            if ((o = props.get("SecurityExit")) != null)
                h.put("SecurityExit", o);
            if ((o = props.get("SecurityData")) != null)
                h.put("SecurityData", o);
            if ((o = props.get("QueueName")) != null)
                h.put("QueueName", o);
            if ((o = props.get("Operation")) != null)
                h.put("Operation", o);
            if ((o = props.get("MessageSelector")) != null)
                h.put("MessageSelector", o);
            if ((o = props.get("IsPhysical")) != null)
                h.put("IsPhysical", o);
            if ((o = props.get("Username")) != null)
                h.put("Username", o);
            if ((o = props.get("Password")) != null)
                h.put("Password", o);
            reporter = (MonitorReport) new JMSMonitor(h);
            break;
*/
          case OBJ_JMX:
            if ((o = props.get("Username")) != null)
                h.put("Username", o);
            if ((o = props.get("Password")) != null)
                h.put("Password", o);
            dataField = (String)MonitorUtils.select(props.get("AttributeName"));
            dataField = MonitorUtils.substitute(dataField, template);
            queryStr = (String) MonitorUtils.select(props.get("MBeanName"));
            queryStr = MonitorUtils.substitute(queryStr, template);
            if (dataField == null || dataField.length() <= 0 ||
                queryStr == null || queryStr.length() <= 0)
                throw(new IllegalArgumentException("MBeanName or AttributeName"+
                    " is not defined for " + uri));
            jmxReq = new JMXRequester(h);
            reporter = null;
            break;
          default:
            break;
        }

        if (requester != null) { // for requester
            Event ev = new Event(Event.INFO);
            ev.setAttribute("type", "query");
            o = props.get("AssetName");
            ev.setAttribute("category", (String) o);
            o = props.get("TargetName");
            ev.setAttribute("name", (String) o);
            ev.setAttribute("status", "Normal");
            queryStr = Event.getIPAddress() + " " + EventUtils.collectible(ev);
        }

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        if (oid == OBJ_REPORT)
            reportMode = REPORT_NONE;

        if (disableMode != 0) {
            if ((o = props.get("ActiveTime")) != null)
                tw = new TimeWindows((Map) o);
        }

        previousTime = -1;
        previousLeadTime = -1;
        timeOffset = 0;
        tolerance = 0;
        triggerSize = -1;
        if ((o = props.get("Reference")) != null) {
            String className;
            if (!(o instanceof Map))
              throw(new IllegalArgumentException("Reference is not a Map"));
            Map<String, Object> ref = Utils.cloneProperties((Map) o);
            if (ref.get("Reference") != null)
                ref.remove("Reference");
            if (ref.get("ActionProgram") != null)
                ref.remove("ActionProgram");
            if (ref.get("MaxRetry") != null)
                ref.remove("MaxRetry");
            ref.put("Step", "1");
            referenceName = (String) ref.get("Name");
            if ((o = ref.get("ClassName")) != null)
                className = (String) o;
            else if ((o = ref.get("Type")) != null)
                className = "org.qbroker.monitor." + (String) o;
            else
                className = "org.qbroker.monitor.AgeMonitor";
            try {
                java.lang.reflect.Constructor con;
                Class<?> cls = Class.forName(className);
                con = cls.getConstructor(new Class[]{Map.class});
                reference = (MonitorReport) con.newInstance(new Object[]{ref});
            }
            catch (java.lang.reflect.InvocationTargetException e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e.getTargetException())).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e)).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }

            if ((o = props.get("TimeOffset")) == null ||
                (timeOffset = 1000 * Integer.parseInt((String) o)) < 0)
                timeOffset = 30000;
            if ((o = props.get("TriggerSize")) == null ||
                (triggerSize = Integer.parseInt((String) o)) < -1)
                triggerSize = -1;
            if ((o = props.get("Tolerance")) != null)
                tolerance = Integer.parseInt((String) o);
        }
        else
            reference = null;

        if (referenceName == null)
            referenceName = "";

        if ((o = props.get("Operation")) != null) { // for aggregation
            if ("latest".equalsIgnoreCase((String) o))
                op = NUM_MAX;
            else if ("first".equalsIgnoreCase((String) o))
                op = NUM_FIRST;
            else if ("last".equalsIgnoreCase((String) o))
                op = NUM_LAST;
            else
                op = NUM_MIN;
        }

        if ((o = props.get("EmptyDataIgnored")) != null &&
            "false".equalsIgnoreCase((String) o))
            emptyDataIgnored = false;
        else if (disableMode == 0)
            emptyDataIgnored = true;
        else
            emptyDataIgnored = false;

        updateCount = 0;
        previousLevel = Event.DEBUG;
        previousTrigger = -1;
        previousRefTime = -1L;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long leadingTime = -1L;
        int init = -1, returnCode = -1, n;
        Map<String, Object> r = null;
        List<Object> dataBlock = new ArrayList<Object>();
        Object o;
        String str;
        StringBuffer strBuf;

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
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        if (reference != null) {
            try {
                report = reference.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("reference failed: " +
                    Event.traceStack(e)));
            }
        }
        report.put("URI", uri);

        if (reporter != null) try {
            r = reporter.generateReport(currentTime);
        }
        catch (Exception e) {
            throw(new IOException("failed to get report: " +
                Event.traceStack(e)));
        }

        switch (oid) {
          case OBJ_FILE:
          case OBJ_FTP:
            dataBlock.add(String.valueOf(((long[]) r.get("SampleTime"))[0]));
            break;
          case OBJ_TCP:
          case OBJ_UDP:
            strBuf = new StringBuffer();
            try {
                n = requester.getResponse(queryStr, strBuf, true);
            }
            catch (Exception e) {
                throw(new IOException("failed on request of '" + queryStr +
                    "' for " + uri + ": " + Event.traceStack(e)));
            }
            if (n < 0)
                throw(new IllegalArgumentException(name +
                    ": failed to get response with " + n));
            else if (n > 0)
                n =GenericList.pickupList(strBuf.toString(),jsonPath,dataBlock);
            if (n < 0)
                throw(new IOException(name + ": unexpected json response '"+
                    strBuf.toString() + "'"));
            break;
          case OBJ_HTTP:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                throw(new IOException("web test failed on " + uri));
            }

            if (returnCode == 0)
                Util.split(dataBlock, pm, patternLF, (String)r.get("Response"));
            break;
          case OBJ_SCRIPT:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                throw(new IOException("script failed on " + uri));
            }

            if (returnCode != 0) {
                Util.split(dataBlock, pm, patternLF, (String) r.get("Output"));
                if (aPatternGroup.length > 0 || xPatternGroup.length > 0) {
                    n = dataBlock.size();
                    for (int i=n-1; i>=0; i--) { // select lines on patterns
                        str = (String) dataBlock.get(i);
                        if (str == null) {
                            dataBlock.remove(i);
                            continue;
                        }
                        if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                            !MonitorUtils.filter(str, xPatternGroup, pm, false))
                            continue;
                        dataBlock.remove(i);
                    }
                }
            }
            break;
          case OBJ_REPORT:
            o = r.get(keyList[0]);
            if (o == null)
                throw(new IOException("got null value on " + keyList[0] +
                    " from " + uri));
            else if (o instanceof String)
                Util.split(dataBlock, pm, patternLF, (String) o);
            else if (o instanceof List)
                dataBlock = Utils.cloneProperties((List) o);
            else if (o instanceof long[])
                dataBlock.add(String.valueOf(((long[]) o)[0]));
            else if (o instanceof int[])
                dataBlock.add(String.valueOf(((int[]) o)[0]));
            else
                throw(new IOException("unexpected data type on " +
                    keyList[0] + " from " + uri));
            break;
          case OBJ_PROC:
            if (r.get("NumberPids") != null) {
                returnCode = Integer.parseInt((String) r.get("NumberPids"));
            }
            else {
                throw(new IOException("ps failed on " + uri));
            }

            if (returnCode > 0)
                dataBlock = Utils.cloneProperties((List) r.get("PSLines"));
            break;
          case OBJ_LOG:
            dataBlock = Utils.cloneProperties((List) r.get("LogBuffer"));
            break;
          case OBJ_JDBC:
            dataBlock = Utils.cloneProperties((List) r.get("RecordBuffer"));
            break;
          case OBJ_SNMP:
            if (!snmp.isListening())
                snmp.reconnect();
            str = snmp.snmpGet(null, snmpOID);
            if (str == null) // failed
                str = "";
            dataBlock.add(str);
            snmp.close();
            break;
          case OBJ_JMS:
            if ((o = r.get("MsgString")) != null) {
                int i;
                str = (String) o;
                i = str.indexOf(" ");
                if (i > 0)
                    dataBlock.add(str.substring(0, i));
            }
            break;
          case OBJ_JMX:
            jmxReq.reconnect();
            try {
                o = jmxReq.getValue(queryStr, dataField);
            }
            catch (JMException e) {
                throw(new IOException(Event.traceStack(e)));
            }
            if (o == null) // failed
                str = "";
            else
                str = o.toString();
            dataBlock.add(str);
            jmxReq.close();
            break;
          default:
            break;
        }

        long mtime = -10L;
        int k = dataBlock.size();
        if (k <= 0 && init < 0) { // empty datablock so reset to previousTime
            leadingTime = previousTime;
        }
        for (int i=0; i<k; i++) {
            if (pm.contains((String) dataBlock.get(i), pattern)) {
                strBuf = new StringBuffer();
                MatchResult mr = pm.getMatch();
                Date date;
                char c;
                n = mr.groups() - 1;
                for (int j=1; j<=n; j++) {
                    if (j > 1)
                        strBuf.append(" ");
                    if ((mr.group(j)).length() == 1) // hack on a.m./p.m.
                        c = (mr.group(j)).charAt(0);
                    else
                        c = 'F';
                    if (c == 'a' || c == 'A' || c == 'p' || c == 'P') {
                        strBuf.append(c);
                        strBuf.append('M');
                    }
                    else
                        strBuf.append(mr.group(j));
                }
                if (isETime) { // elapse time of a process
                    if (strBuf.length() < 6)
                        strBuf.insert(0, new char[] {'0', '-', '0', '0', ':'});
                    else if (strBuf.length() < 8)
                        strBuf.insert(0, new char[] {'0', '-', '0'});
                    else if (strBuf.length() < 9)
                        strBuf.insert(0, new char[] {'0', '-'});
                }
                str = (tSub == null) ? strBuf.toString() :
                    tSub.substitute(pm, strBuf.toString());
                if (isNumber)
                    date = new Date(Long.parseLong(str));
                else
                    date = dateFormat.parse(str, new ParsePosition(0));
                if (date != null) {
                    if (isETime) // for etime
                        mtime = currentTime - (date.getTime() - t0);
                    else
                        mtime = date.getTime();
                    mtime -= timeDifference;
                    if (init < 0)
                        init = 0;
                }
                else
                    throw(new IOException("failed to parse mtime: " +
                        strBuf.toString()));
                if (init < 0)
                    continue;
                else if (init == 0) {
                    leadingTime = mtime;
                    report.put("LeadingBlock", (String) dataBlock.get(i));
                    init = 1;
                }
                else switch (op) { // aggregation
                  case NUM_MAX:
                    if (mtime > leadingTime) {
                        leadingTime = mtime;
                        report.put("LeadingBlock", (String) dataBlock.get(i));
                    }
                    break;
                  case NUM_MIN:
                    if (mtime < leadingTime) {
                        leadingTime = mtime;
                        report.put("LeadingBlock", (String) dataBlock.get(i));
                    }
                    break;
                  case NUM_LAST:
                    leadingTime = mtime;
                    report.put("LeadingBlock", (String) dataBlock.get(i));
                    break;
                  case NUM_FIRST:
                  default:
                }
            }
            else { // no match
                continue;
            }
        }
        if (init >= 0) {
            if (reference != null)
                report.put("LatestTime", new long[] {leadingTime});
            else
                report.put("SampleTime", new long[] {leadingTime});
        }
        else if (emptyDataIgnored) // ignoring empty data
            skip = DISABLED;
        else
            throw(new IOException(name + " failed to get mtime from " + uri +
                ": " + (String) dataBlock.get(0)));

        if (disableMode != 0) {
            if (reference != null) { // compare against reference
                if ((o = report.get("SampleTime")) != null &&
                    o instanceof long[] && ((long[]) o).length > 0) {
                    long tt = ((long[]) o)[0];
                    if (leadingTime - tt > -timeOffset) // caught up
                        skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    else // not caught up yet
                        skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
                else {
                    previousTime = leadingTime;
                    throw(new IOException("failed to get reference time"));
                }
            }
            else if (tw != null) { // time window defined
                int i = tw.getThresholdLength();
                if (i >= 2) switch (tw.check(currentTime, leadingTime)) {
                  case TimeWindows.NORMAL:
                  case TimeWindows.SLATE:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.ALATE:
                  case TimeWindows.ELATE:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
                else switch (tw.check(currentTime, leadingTime)) {
                  case TimeWindows.NORMAL:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.OCCURRED:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
            }
            else if (leadingTime > previousTime) // updated
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else // not updated yet
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
        }
        previousTime = leadingTime;

        if (statsLogger != null && skip == NOSKIP) {
            strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(name + " ");
            strBuf.append(previousTime);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long sampleTime = -1L;
        StringBuffer strBuf = new StringBuffer();
        String block = null;
        Object o;

        if (reference != null) {
            return correlate(status, currentTime, latest);
        }

        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        if ((o = latest.get("LeadingBlock")) != null)
            block = (String) o;

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
          case TimeWindows.NORMAL:
            if (previousStatus == status) { // always normal
                exceptionCount = 0;
                return null;
            }
            else { // just back to normal
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' is pretty new");
            }
            break;
          case TimeWindows.OCCURRED:
            level = Event.NOTICE;
            exceptionCount = 0;
            if (previousStatus == status) { // still occurred
                actionCount ++;
                strBuf.append("'" + uri);
                strBuf.append("' has occurred recently");
            }
            else { // first time to notice it
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri + "' occurred ");
                strBuf.append((int) ((currentTime - sampleTime)/60000));
                strBuf.append(" minutes ago");
            }
            break;
          case TimeWindows.SLATE: // somewhat late
            level = Event.WARNING;
            exceptionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' is only ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes old");
            break;
          case TimeWindows.ELATE: // extremely late
          case TimeWindows.ALATE: // very late
            level = Event.ERR;
            exceptionCount = 0;
            if (previousStatus < TimeWindows.ALATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' is ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes old");
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not being checked due to blackout");
            break;
          default: // should never reach here
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level) //slate
                return null;
            break;
          case Event.NOTICE: // occurred
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousLevel = level;
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level)
                return null;
            break;
        }
        previousStatus = status;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        if (sampleTime >= 0) {
            event.setAttribute("leadingTime",
                Event.dateFormat(new Date(sampleTime)));
            event.setAttribute("age",
                (int) ((currentTime - sampleTime)/60000) + " minutes");
        }
        else
            event.setAttribute("leadingTime", "N/A");
        event.setAttribute("leadingBlock", (block != null) ? block : "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (oid == OBJ_PROC && block != null &&
            reporter instanceof ProcessMonitor) {
            int pid = ((ProcessMonitor) reporter).getPid(block);
            if (pid > 0)
                event.setAttribute("pids", String.valueOf(pid));
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

    private Event correlate(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, triggerStatus = -1;
        long sampleTime, sampleSize, latestTime = -1L;
        StringBuffer strBuf = new StringBuffer();
        String block = null;
        Object o;
        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        else
            sampleTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleSize = ((long[]) o)[0];
        }
        else
            sampleSize = -1L;

        if ((o = latest.get("LatestTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            latestTime = ((long[]) o)[0];
        }
        else
            latestTime = -1L;

        if ((o = latest.get("LeadingBlock")) != null)
            block = (String) o;

        if (status >= TimeWindows.NORMAL) { // good test
            if (latestTime - sampleTime > - timeOffset) {
                if (triggerSize >= 0) { // trigger enabled
                    triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
                }
                status = TimeWindows.OCCURRED;
            }
            else if (triggerSize >= 0) { // trigger enabled but no update
                triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
            }
        }

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.OCCURRED:
            if (previousStatus == status &&
                previousTrigger == triggerStatus) { // always OK
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just updated
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' has been updated accordingly");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
            }
            break;
          case TimeWindows.NORMAL:// just updated
          case TimeWindows.SLATE: // somewhat late
          case TimeWindows.ALATE: // very late
            exceptionCount = 0;
            if (previousStatus < TimeWindows.NORMAL ||
                previousStatus == TimeWindows.OCCURRED ||
                previousStatus >= TimeWindows.ELATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            if (latestTime > previousLeadTime) { // target just updated
                actionCount = 0;
                updateCount = 0;
            }
            if (sampleTime > previousRefTime) // reference updated
                updateCount ++;

            if (status == TimeWindows.NORMAL)
                level = Event.INFO;
            else if (status == TimeWindows.SLATE)
                level = Event.WARNING;
            else
                level = Event.ERR;

            if (updateCount <= 1) { // only one update on reference
                if (previousLevel != level)
                    actionCount = 0;
            }
            else if (updateCount > tolerance) { // upgrade to ERR
                level = Event.ERR;
                if (previousLevel > level)
                    actionCount = 0;
            }
            else {
                level = Event.WARNING;
                if (previousLevel != level)
                    actionCount = 0;
            }
            actionCount ++;
            if (level == Event.INFO) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' has been updated recently");
                if (previousTrigger != triggerStatus)
                    strBuf.append(" with the state change");
            }
            else if (level == Event.WARNING) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' was updated ");
                if (previousTrigger != triggerStatus)
                    strBuf.append("with the state change ");
                strBuf.append((int) ((currentTime - sampleTime)/60000));
                strBuf.append(" minutes ago");
            }
            else {
                strBuf.append("'" + uri);
                strBuf.append("' has not been updated");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
                strBuf.append(" in the last ");
                strBuf.append((int) ((currentTime - latestTime)/60000));
                strBuf.append(" minutes");
            }
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not being checked due to blackout");
            break;
          case TimeWindows.ELATE: // extremely late and do not care
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            exceptionCount = 0;
            actionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Reference: '");
            strBuf.append(referenceName);
            strBuf.append("' has not been updated since ");
            strBuf.append(Event.dateFormat(new Date(sampleTime)));
            break;
          default: // should never reach here
            break;
        }
        if (latestTime > 0L)
            previousLeadTime = latestTime;
        if (sampleTime > 0L)
            previousRefTime = sampleTime;

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousTrigger = triggerStatus;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousTrigger = triggerStatus;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus) {
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus)
                return null;
            break;
        }
        previousStatus = status;
        previousTrigger = triggerStatus;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        event.setAttribute("reference", referenceName);
        event.setAttribute("updateCount", String.valueOf(updateCount));
        if (sampleTime >= 0)
            event.setAttribute("referenceTime",
                Event.dateFormat(new Date(sampleTime)));
        else
            event.setAttribute("referenceTime", "N/A");
        if (latestTime >= 0)
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(latestTime)));
        else
            event.setAttribute("latestTime", "N/A");
        event.setAttribute("leadingBlock", (block != null) ? block : "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));
        if (oid == OBJ_PROC && block != null &&
            reporter instanceof ProcessMonitor) {
            int pid = ((ProcessMonitor) reporter).getPid(block);
            if (pid > 0)
                event.setAttribute("pids", String.valueOf(pid));
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
        chkpt.put("PreviousLevel", String.valueOf(previousLevel));
        chkpt.put("PreviousTime", String.valueOf(previousTime));
        chkpt.put("PreviousLeadTime", String.valueOf(previousLeadTime));
        chkpt.put("PreviousRefTime", String.valueOf(previousRefTime));
        chkpt.put("PreviousTrigger", String.valueOf(previousTrigger));
        chkpt.put("UpdateCount", String.valueOf(updateCount));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pStatus, sNumber;
        int uCount, pLevel, pTrigger;
        long pTime, pLeadTime, pRefTime;
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

        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTime")) != null)
            pTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousLeadTime")) != null)
            pLeadTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousRefTime")) != null)
            pRefTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTrigger")) != null)
            pTrigger = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("UpdateCount")) != null)
            uCount = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousLevel = pLevel;
        previousTime = pTime;
        previousLeadTime = pLeadTime;
        previousRefTime = pRefTime;
        previousTrigger = pTrigger;
        updateCount = uCount;
    }

    public void destroy() {
        super.destroy();
        if (reference != null) {
            reference.destroy();
            reference = null;
        }
        if (reporter != null) {
            reporter.destroy();
            reporter = null;
        }
        if (requester != null) {
            requester.close();
            requester = null;
        }
        if (snmp != null)
            snmp.close();
        if (jmxReq != null)
            jmxReq.close();
    }

    protected void finalize() {
        destroy();
    }
}
