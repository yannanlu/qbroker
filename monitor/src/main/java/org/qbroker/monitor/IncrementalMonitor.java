package org.qbroker.monitor;

/* IncrementalMonitor.java - a monitor checking the incremental of a number */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;
import java.util.Date;
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
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Requester;
import org.qbroker.common.DataSet;
import org.qbroker.common.Utils;
import org.qbroker.net.SNMPConnector;
import org.qbroker.net.JMXRequester;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.FileTester;
import org.qbroker.monitor.WebTester;
import org.qbroker.monitor.ProcessMonitor;
import org.qbroker.monitor.UnixlogMonitor;
import org.qbroker.monitor.ScriptLauncher;
import org.qbroker.monitor.DBRecord;
import org.qbroker.monitor.ReportQuery;
import org.qbroker.monitor.GenericList;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * IncrementalMonitor monitors the incremental of a number to see if it is
 * out of the ranges.  It supports various schemes to get the text containing
 * the number.  The number is supposed to be increasing except for a reset.
 * The output may contains multiple lines.  IncrementalMonitor parses the text
 * line by line to get the numbers.  Then it aggregates them according to
 * the specified operation.
 *<br><br>
 * It supports v1 and v2c SNMP query.  But it requires Java 1.4 or above due
 * SNMP4J.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class IncrementalMonitor extends Monitor {
    private DataSet warningRange = null;
    private DataSet errorRange = null;
    private DataSet criticalRange = null;
    private String uri;
    private MonitorReport reporter;
    private Requester requester;
    private SNMPConnector snmp = null;
    private JMXRequester jmxReq = null;
    private String queryStr, dataField, snmpOID = null, jsonPath = null;
    private Pattern pattern, patternLF;
    private TextSubstitution tSub = null;
    private int oid, op = NUM_COUNT, previousLevel;
    private double scale, shift, previousIncremental;
    private boolean isDouble = false, emptyDataIgnored, logDetail = false;
    private String previousNumber;
    protected final static int OBJ_HTTP = 1;
    protected final static int OBJ_SCRIPT = 2;
    protected final static int OBJ_REPORT = 3;
    protected final static int OBJ_PROC = 4;
    protected final static int OBJ_FILE = 5;
    protected final static int OBJ_FTP = 6;
    protected final static int OBJ_TCP = 7;
    protected final static int OBJ_UDP = 8;
    protected final static int OBJ_LOG = 9;
    protected final static int OBJ_JMS = 10;
    protected final static int OBJ_JDBC = 11;
    protected final static int OBJ_SNMP = 12;
    protected final static int OBJ_JMX = 13;
    protected final static int OBJ_PCF = 14;
    protected final static int OBJ_SONIC = 15;
    protected final static int OBJ_REQ = 16;
    public final static int NUM_COUNT = 0;
    public final static int NUM_SUM = 1;
    public final static int NUM_MIN = 2;
    public final static int NUM_MAX = 3;
    public final static int NUM_AVG = 4;

    public IncrementalMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        URI u = null;
        String str = null;
        int n;
        if (type == null)
            type = "IncrementalMonitor";

        if (description == null)
            description = "monitor a number incremental";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

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
        else if ("tcp".equals(u.getScheme())) {
            if ((o = props.get("ConnectionFactoryName")) != null)
                oid = OBJ_JMS;
            else if ((o = props.get("RequestCommand")) != null)
                oid = OBJ_SONIC;
            else
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
        else if ("service".equals(u.getScheme()) || "imq".equals(u.getScheme()))
            oid = OBJ_JMX;
        else if ("pcf".equals(u.getScheme()))
            oid = OBJ_PCF;
        else if ("wmq".equals(u.getScheme()))
            oid = OBJ_JMS;
        else if ((o = props.get("RequesterClass")) != null)
            oid = OBJ_REQ;
        else if ((o = props.get("ConnectionFactoryName")) != null)
            oid = OBJ_JMS;
        else
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((o = MonitorUtils.select(props.get("Pattern"))) == null)
            throw(new IllegalArgumentException("Pattern is not well defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
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
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("Operation")) != null) {
            if ("count".equalsIgnoreCase((String) o))
                op = NUM_COUNT;
            else if ("sum".equalsIgnoreCase((String) o))
                op = NUM_SUM;
            else if ("max".equalsIgnoreCase((String) o))
                op = NUM_MAX;
            else if ("min".equalsIgnoreCase((String) o))
                op = NUM_MIN;
            else if ("average".equalsIgnoreCase((String) o))
                op = NUM_AVG;
            else
                op = NUM_COUNT;
        }

        if ((o = props.get("LogDetail")) != null && "true".equals((String) o))
            logDetail = true;

        if((o = props.get("CriticalRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            criticalRange = new DataSet((List) o);
            if (criticalRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if ((o = props.get("ErrorRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            errorRange = new DataSet((List) o);
            if (errorRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if ((o = props.get("WarningRange")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            warningRange = new DataSet((List) o);
            if (warningRange.getDataType() == DataSet.DATA_DOUBLE)
                isDouble = true;
        }
        if (criticalRange == null && errorRange == null && warningRange == null)
            throw(new IllegalArgumentException("no number range defined"));

        reporter = null;
        requester = null;
        h.put("Name", name);
        h.put("URI", uri);
        if ((o = props.get("Timeout")) != null)
            h.put("Timeout", o);
        if ((o = props.get("Debug")) != null)
            h.put("Debug", o);
        h.put("Step", "1");
        Map<String, Object> map = new HashMap<String, Object>();
        switch (oid) {
          case OBJ_FILE:
          case OBJ_FTP:
            reporter = new FileTester(h);
            break;
          case OBJ_TCP:
            if ((o = props.get("JSONPath")) != null) {
                jsonPath = (String) MonitorUtils.select(o);
                jsonPath = MonitorUtils.substitute(jsonPath, template);
            }
            else
                throw(new IllegalArgumentException("JSONPath not defined"));
            if ((o = props.get("RequestCommand")) != null) {
                queryStr = (String) MonitorUtils.select(o);
                queryStr = MonitorUtils.substitute(queryStr, template);
            }   
            else
                throw(new IllegalArgumentException("RequestCommand is null"));
            h.put("RequestCommand", queryStr);
            if ((o = props.get("XPath")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("XPath", MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("ResultSubstitution")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("ResultSubstitution",
                    MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
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
                "org.qbroker.persister.GenericRequester", name);
            break;
          case OBJ_UDP:
            if ((o = props.get("JSONPath")) != null) {
                jsonPath = (String) MonitorUtils.select(o);
                jsonPath = MonitorUtils.substitute(jsonPath, template);
            }
            else
                throw(new IllegalArgumentException("JSONPath not defined"));
            if ((o = props.get("RequestCommand")) != null) {
                queryStr = (String) MonitorUtils.select(o);
                queryStr = MonitorUtils.substitute(queryStr, template);
            }   
            else
                throw(new IllegalArgumentException("RequestCommand is null"));
            h.put("RequestCommand", queryStr);
            if ((o = props.get("XPath")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("XPath", MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("ResultSubstitution")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("ResultSubstitution",
                    MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
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
                "org.qbroker.persister.GenericRequester", name);
            break;
          case OBJ_HTTP:
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
            h.put("KeyList", new ArrayList());
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
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            if ((o = props.get("SQLQuery")) != null)
                h.put("SQLQuery", o);
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
            if ((o = props.get("FieldSeparator")) != null)
                h.put("FieldSeparator", o);
            if ((o = props.get("StatsURI")) != null)
                h.put("StatsURI", o);
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
          case OBJ_JMX:
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
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
          case OBJ_JMS:
            if ((o = props.get("JSONPath")) != null) {
                jsonPath = (String) MonitorUtils.select(o);
                jsonPath = MonitorUtils.substitute(jsonPath, template);
            }
            else
                throw(new IllegalArgumentException("JSONPath not defined"));
            if ((o = props.get("RequestBody")) != null) {
                queryStr = (String) MonitorUtils.select(o);
                queryStr = MonitorUtils.substitute(queryStr, template);
            }   
            else
                throw(new IllegalArgumentException("RequestBody not defined"));
            if ((o = props.get("ContextFactory")) != null)
                h.put("ContextFactory", o);
            if ((o = props.get("ConnectionFactoryName")) != null)
                h.put("ConnectionFactoryName", o);
            if ((o = props.get("QueueName")) != null)
                h.put("QueueName", o);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            if ((o = props.get("Principal")) != null) {
                h.put("Principal", o);
                if ((o = props.get("Credentials")) != null)
                    h.put("Credentials", o);
                if ((o = props.get("EncryptedCredentials")) != null)
                    h.put("EncryptedCredentials", o);
            }
            if ((o = props.get("StringProperty")) != null)
                h.put("StringProperty", o);
            if ((o = props.get("ReponseProperty")) != null)
                h.put("ReponseProperty", o);
            if ((o = props.get("RequestTimeout")) != null)
                h.put("RequestTimeout", o);
            if ((o = props.get("ReceiveTime")) != null)
                h.put("ReceiveTime", o);
            if ((o = props.get("ResultType")) != null)
                h.put("ResultType", o);
            if ((o = props.get("DataField")) != null)
                h.put("DataField", o);
            if ((o = props.get("RCField")) != null)
                h.put("RCField", o);
            if ((o = props.get("XPath")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("XPath", MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("ResultSubstitution")) != null) {
                str = (String) MonitorUtils.select(o);
                h.put("ResultSubstitution",
                    MonitorUtils.substitute(str, template));
            }
            if ((o = props.get("PatternGroup")) != null)
                h.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                h.put("XPatternGroup", o);
            h.put("Operation", "request");
            h.put("ClassName", "org.qbroker.persister.JMSPersister");
            h.put("Capacity", "2");
            h.put("Partition", "0,0");
            h.put("DisplayMask", "0");
            h.put("TextMode", "1");
            requester = GenericList.initRequester(h,
                "org.qbroker.persister.GenericRequester", name);
            break;
          default:
            break;
        }

        previousNumber = (isDouble) ? String.valueOf(Double.MAX_VALUE) :
            String.valueOf(Long.MAX_VALUE);

        if ((o = props.get("EmptyDataIgnored")) != null &&
            "false".equalsIgnoreCase((String) o))
            emptyDataIgnored = false;
        else if (disableMode == 0)
            emptyDataIgnored = true;
        else
            emptyDataIgnored = false;

        previousIncremental = 0.0;
        previousLevel = -10;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int init = -1, returnCode = -1, n;
        long leadingNumber = -1L;
        double doubleNumber = 0.0;
        Map<String, Object> r = null;
        List dataBlock = new ArrayList();
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
            dataBlock.add((String) r.get("Size"));
            r.clear();
            break;
          case OBJ_TCP:
          case OBJ_UDP:
          case OBJ_JMS:
            strBuf = new StringBuffer();
            try {
                n = requester.getResponse(queryStr, strBuf, true);
            }
            catch (Exception e) {
                throw(new IOException("failed on request of '" + queryStr +
                    "' for "  + uri + ": " + Event.traceStack(e)));
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
                r.clear();
                throw(new IOException("web test failed on " + uri));
            }

            if (returnCode == 0) { // get content of http response
                String text = (String) r.remove("Response");
                if (text != null && (n = text.indexOf("\r\n\r\n")) > 0)
                    text = text.substring(n+4);
                else
                    text = "";

                Util.split(dataBlock, pm, patternLF, text);
            }
            r.clear();
            break;
          case OBJ_SCRIPT:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                r.clear();
                throw(new IOException("script failed on " + uri));
            }

            if (returnCode != 0) {
                Util.split(dataBlock, pm, patternLF,(String)r.remove("Output"));
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
            r.clear();
            break;
          case OBJ_REPORT:
            o = r.remove(keyList[0]);
            if (o == null) {
                r.clear();
                throw(new IOException("got null value on " + keyList[0] +
                    " from " + uri));
            }
            else if (o instanceof String)
                Util.split(dataBlock, pm, patternLF, (String) o);
            else if (o instanceof List)
                dataBlock = (List) o;
            else if (o instanceof long[])
                dataBlock.add(String.valueOf(((long[]) o)[0]));
            else {
                r.clear();
                throw(new IOException("unexpected data type on " +
                    keyList[0] + " from " + uri));
            }
            r.clear();
            break;
          case OBJ_PROC:
            if (r.get("NumberPids") != null) {
                returnCode = Integer.parseInt((String) r.get("NumberPids"));
            }
            else {
                r.clear();
                throw(new IOException("ps failed on " + uri));
            }

            if (returnCode > 0)
                dataBlock = (List) r.remove("PSLines");
            r.clear();
            break;
          case OBJ_LOG:
            dataBlock = (List) r.remove("LogBuffer");
            r.clear();
            break;
          case OBJ_JDBC:
            dataBlock = (List) r.remove("RecordBuffer");
            r.clear();
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

        long number = 0;
        n = dataBlock.size();
        if (n <= 0 && init < 0) { // empty dataBlock so reset to previousNumber
            if (isDouble)
                doubleNumber = Double.parseDouble(previousNumber);
            else
                leadingNumber = Long.parseLong(previousNumber);
        }
        for (int i=0; i<n; i++) {
            if (pm.contains((String) dataBlock.get(i), pattern)) {
                double f = 0;
                MatchResult mr;
                switch (op) {
                  case NUM_MAX:
                  case NUM_MIN:
                  case NUM_SUM:
                  case NUM_AVG:
                    mr = pm.getMatch();
                    if (mr.groups() > 1) { // got the number
                        str = (tSub == null) ? mr.group(1) :
                            tSub.substitute(mr.group(1));
                        if (isDouble)
                            f = Double.parseDouble(str);
                        else
                            number = Long.parseLong(str);
                        if (init <= 0) {
                            init = 1;
                            if (isDouble)
                                doubleNumber = f;
                            else
                                leadingNumber = number;
                            if (logDetail)
                                report.put("LeadingBlock",
                                    (String) dataBlock.get(i));
                            else
                                report.put("LeadingBlock", str);
                        }
                        else if (isDouble && ((op == NUM_MAX &&
                            f > doubleNumber) || (op == NUM_MIN &&
                            f < doubleNumber))) {
                            doubleNumber = f;
                            if (logDetail)
                                report.put("LeadingBlock",
                                    (String) dataBlock.get(i));
                            else
                                report.put("LeadingBlock", str);
                        }
                        else if (!isDouble && ((op == NUM_MAX &&
                            number > leadingNumber) || (op == NUM_MIN &&
                            number < leadingNumber))) {
                            leadingNumber = number;
                            if (logDetail)
                                report.put("LeadingBlock",
                                    (String) dataBlock.get(i));
                            else
                                report.put("LeadingBlock", str);
                        }
                        else if (op == NUM_SUM || op == NUM_AVG) {
                            if (isDouble)
                                doubleNumber += f;
                            else
                                leadingNumber += number;
                            if (logDetail)
                                report.put("LeadingBlock",
                                    (String) dataBlock.get(i));
                            else
                                report.put("LeadingBlock", str);
                            init ++;
                        }
                    }
                    break;
                  case NUM_COUNT:
                  default:
                    str = (String) dataBlock.get(i);
                    if (init < 0) {
                        init = 1;
                        number = 1;
                        if (logDetail)
                            report.put("LeadingBlock", str);
                        else if (str != null && str.length() > 128)
                            report.put("LeadingBlock", str.substring(0, 128));
                        else
                            report.put("LeadingBlock", str);
                    }
                    else {
                        init ++;
                        number ++;
                        if (logDetail)
                            report.put("LeadingBlock", str);
                        else if (str != null && str.length() > 128)
                            report.put("LeadingBlock", str.substring(0, 128));
                        else
                            report.put("LeadingBlock", str);
                    }
                    break;
                }
            }
            else { // no match
                continue;
            }
        }
        if (op == NUM_COUNT) {
            leadingNumber = number;
            report.put("LeadingNumber", String.valueOf(number));
        }
        else if (op == NUM_AVG && init > 0) {
            if (isDouble) {
                doubleNumber /= init;
                report.put("LeadingNumber", String.valueOf(doubleNumber));
            }
            else {
                leadingNumber /= init;
                report.put("LeadingNumber", String.valueOf(leadingNumber));
            }
        }
        else if (init > 0) { // for other operations
            if (isDouble)
                report.put("LeadingNumber", String.valueOf(doubleNumber));
            else
                report.put("LeadingNumber", String.valueOf(leadingNumber));
        }
        else if (emptyDataIgnored) // ignore the empty data
            skip = DISABLED;
        else {
            str = (String) dataBlock.get(0);
            dataBlock.clear();
            throw(new IOException(name + " failed to get number from " + uri +
                ": " + str));
        }
        dataBlock.clear();

        if (statsLogger != null && skip == NOSKIP) {
            strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(name + " ");
            strBuf.append(previousNumber + " " + previousIncremental);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }

        if (isDouble) {
            previousIncremental=doubleNumber-Double.parseDouble(previousNumber);
            previousNumber = String.valueOf(doubleNumber);
        }
        else {
            previousIncremental = leadingNumber-Long.parseLong(previousNumber);
            previousNumber = String.valueOf(leadingNumber);
        }
        if (previousIncremental < 0.0) // assuming it is positive so reset it
            previousIncremental = 0.0;
        report.put("Incremental", String.valueOf(previousIncremental));

        if (disableMode != 0) {
            if (criticalRange != null &&
                criticalRange.contains(previousIncremental)) { // out of range
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            }
            else if (errorRange != null &&
                errorRange.contains(previousIncremental)) { // out of range
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            }
            else { // in the normal range
                skip = (disableMode < 0) ? NOSKIP : DISABLED;
            }
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long leadingNumber = -1L;
        double doubleNumber = 0.0, incremental = 0.0;
        StringBuffer strBuf = new StringBuffer();
        String block = null;
        Object o;

        if ((o = latest.get("LeadingNumber")) != null) {
            if (isDouble)
                doubleNumber = Double.parseDouble((String) o);
            else
                leadingNumber = Long.parseLong((String) o);
        }

        if ((o = latest.get("Incremental")) != null) {
            long num = (long) (100.0 * Double.parseDouble((String) o));
            incremental = num / 100.0;
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
          default: // normal cases
            exceptionCount = 0;
            if (criticalRange != null && criticalRange.contains(incremental)) {
                level = Event.CRIT;
                if (previousStatus != status || step > 0) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (step > 0)
                        step = 0;
                }
                actionCount ++;
                strBuf.append("the incremental for '" + uri);
                strBuf.append("' is out of range: ");
                strBuf.append(incremental);
            }
            else if (errorRange != null && errorRange.contains(incremental)) {
                level = Event.ERR;
                if (previousStatus != status || step > 0) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (step > 0)
                        step = 0;
                }
                actionCount ++;
                strBuf.append("the incremental for '" + uri);
                strBuf.append("' is out of range: ");
                strBuf.append(incremental);
            }
            else if (warningRange != null &&
                warningRange.contains(incremental)) {
                level = Event.WARNING;
                if (previousStatus != status || previousLevel != level) {
                    // reset count and adjust step
                    actionCount = 0;
                    if (normalStep > 0)
                        step = normalStep;
                }
                actionCount ++;
                strBuf.append("the number for '" + uri);
                strBuf.append("' is ");
                strBuf.append(incremental);
            }
            else if (previousStatus == status && previousLevel == Event.INFO) {
                // always normal
                return null;
            }
            else { // just back to normal
                level = Event.INFO;
                actionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' is in the normal range: ");
                strBuf.append(incremental);
            }
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very large
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
                previousStatus = status;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either out of range or exception
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
            else if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
          case Event.CRIT: // very large
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

        event.setAttribute("incremental", String.valueOf(incremental));
        event.setAttribute("leadingNumber", ((isDouble) ?
            String.valueOf(doubleNumber) : String.valueOf(leadingNumber)));
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
        chkpt.put("PreviousNumber", previousNumber);
        chkpt.put("PreviousLevel", String.valueOf(previousLevel));
        chkpt.put("PreviousIncremental", String.valueOf(previousIncremental));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        String pNumber;
        int aCount, eCount, pStatus, sNumber, pLevel;
        double pIncremental;
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
        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;

        if ((o = chkpt.get("PreviousNumber")) != null)
            pNumber = (String) o;
        else
            return;
        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousIncremental")) != null)
            pIncremental = Double.parseDouble((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousNumber = pNumber;
        previousLevel = pLevel;
        previousIncremental = pIncremental;
    }

    public void destroy() {
        super.destroy();
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

    public static void main(String[] args) {
        String filename = null;
        Monitor monitor = null;

        if (args.length <= 1) {
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
            Object o;
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            monitor = new IncrementalMonitor(ph);
            Map r = monitor.generateReport(tm);
            if ((o = r.get("LeadingNumber")) != null && o instanceof String) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else {
                    String block = (String) r.get("LeadingBlock");
                    System.out.println((String) o);
                    if (block != null)
                        System.out.println(block);
                }
            }
            else
                System.out.println("failed to get the number");
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
        System.out.println("IncrementalMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("IncrementalMonitor: monitor an increment");
        System.out.println("Usage: java org.qbroker.monitor.IncrementalMonitor -I cfg.json");
    }
}
