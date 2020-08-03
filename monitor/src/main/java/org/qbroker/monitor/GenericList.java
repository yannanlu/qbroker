package org.qbroker.monitor;

/* GenericList.java - a reporter checking on a list of names or data */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.io.StringReader;
import java.io.IOException;
import javax.management.JMException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Util;
import org.qbroker.common.Evaluation;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.Requester;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.JMXRequester;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;
import org.qbroker.monitor.WebTester;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * GenericList queries a data source for a list of items. The list of the items
 * will be stored as a List under the key of "List" in the report. The data
 * queried from the data source is expected to be in a format of TEXT, JSON
 * or XML, etc, which is specified by ResultType. Based on the ResultType,
 * GenericList will convert queried data into the items of the speicified
 * DataType. By default, DataType is TEXT which means that the data type of all
 * the items in the list is String. In this case, items in the list are also
 * called as keys. If DataType is not TEXT, the data type of items will be Map.
 *<br><br>
 * If KeyTemplate is defined, it will be used to transform the queried data to
 * the keys. These keys will be used by filters to select queried data. In case
 * that KeySubstitution is also defined, all selected keys will be formatted by
 * the TextSubstition. Meanwhile, GenericList will save the original data into
 * the report at the field of "Data" as the private report. If DataType is TEXT,
 * KeySubstitution will be used to format the output even if KeyTemplate is not
 * defined. If the value of KeyTemplate is set to "##body##", GenericList will
 * save the list of String to the field of "Data". In this case, DataType will
 * be reset to TEXT. If KeyTemplate is not defined, there will be no private
 * report. In this case, EvalTemplate can be defined to select queried map
 * data when DataType is not TEXT.
 *<br><br>
 * This needs more work to support other data sources.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class GenericList extends Report {
    private String uri;
    private JMXRequester jmxReq = null;
    private MonitorReport reporter = null;
    private Requester requester= null;
    private String target;
    private String jsonPath = null;
    private DocumentBuilder builder = null;
    private XPathExpression xpe = null;
    private Template kTemp = null;
    private Template eTemp = null;
    private TextSubstitution tSub = null;
    private int oid;
    private int resultType = Utils.RESULT_JSON;
    private int itemType = Utils.RESULT_TEXT;
    private boolean isString = false;
    private Pattern patternLF = null;
    private Pattern[][] aPatternGroup = null;
    private Pattern[][] xPatternGroup = null;
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

    public GenericList(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        URI u = null;
        String str = null;
        if (type == null)
            type = "GenericList";

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

        if ((o = props.get("DataType")) != null && o instanceof String)
            itemType = Integer.parseInt((String) o);
        if ((o = props.get("KeyTemplate")) != null) { // for private report
            str = MonitorUtils.select(o);
            str = MonitorUtils.substitute(str, template);
            kTemp = new Template(str);
            if ("##body##".equals(str)) // list of Strings for private report
                isString = true;
        }
        else if ((o = props.get("EvalTemplate")) != null) { //for map selections
            str = MonitorUtils.select(o);
            str = MonitorUtils.substitute(str, template);
            eTemp = new Template(str);
        }
        if ((o = props.get("KeySubstitution")) != null) {
            str = MonitorUtils.select(o);
            str = MonitorUtils.substitute(str, template);
            tSub = new TextSubstitution(str);
        }
        if ((o = props.get("ResultType")) != null && o instanceof String)
            resultType = Integer.parseInt((String) o);
        if (resultType == Utils.RESULT_JSON) {
            if ((o = props.get("JSONPath")) != null) {
                str = MonitorUtils.select(o);
                str = MonitorUtils.substitute(str, template);
                jsonPath = str;
            }
            else
                jsonPath = ".Record";
            if (jsonPath == null || jsonPath.length() <= 0)
                throw(new IllegalArgumentException("jsonPath is not defined"));
        }
        else if (resultType == Utils.RESULT_XML) {
            if ((o = props.get("XPath")) != null) {
                str = MonitorUtils.select(o);
                str = MonitorUtils.substitute(str, template);
            }
            if (str == null || str.length() <= 0)
                throw(new IllegalArgumentException("XPath is not defined"));

            try {
                XPath xpath = XPathFactory.newInstance().newXPath();
                xpe = xpath.compile(str);
                builder = Utils.getDocBuilder();
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to get XPath: " + e.toString()));
            }
        }
        else if (itemType != Utils.RESULT_TEXT) // non-structural data
            itemType = Utils.RESULT_TEXT;

        try {
            Perl5Compiler pc = new Perl5Compiler();
            patternLF = pc.compile("\\n");
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",
                props, pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",
                props, pc);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        h.put("Name", name);
        h.put("URI", uri);
        if ((o = props.get("Timeout")) != null)
            h.put("Timeout", o);
        if ((o = props.get("Debug")) != null)
            h.put("Debug", o);
        switch (oid) {
          case OBJ_JMX:
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            target = (String) MonitorUtils.select(props.get("MBeanName"));
            if (target == null || target.length() <= 0)
                throw(new IllegalArgumentException("MBeanName"+
                    " is not defined for " + uri));
            target = MonitorUtils.substitute(target, template);
            jmxReq = new JMXRequester(h);
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
            h.put("ScriptTimeout", props.get("Timeout"));
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
            reportMode = MonitorReport.REPORT_NONE;
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
            if ((o = props.get("Timeout")) != null)
                h.put("Timeout", o);
            if ((o = props.get("SQLExecTimeout")) != null)
                h.put("SQLExecTimeout", o);
            reporter = new DBRecord(h);
            break;
          case OBJ_PCF:
            h.put("ClassName", "org.qbroker.wmq.PCFRequester");
            h.put("Operation", "display");
            if ((o = props.get("SecurityExit")) != null)
                h.put("SecurityExit", o);
            if ((o = props.get("SecurityData")) != null)
                h.put("SecurityData", o);
            if ((o = props.get("ChannelName")) != null)
                h.put("ChannelName", o);
            if ((o = props.get("ResultType")) != null)
                h.put("ResultType", o);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            target = (String) MonitorUtils.select(props.get("PCFCommand"));
            if (target == null || target.length() <= 0)
                throw(new IllegalArgumentException("PCFCommand" +
                    " is not defined for " + uri));
            target = MonitorUtils.substitute(target, template);
            requester = initRequester(h, null, name);
            break;
          case OBJ_SONIC:
            str = "org.qbroker.sonicmq.SonicMQRequester";
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("ConnectOnInit", "false");
            target = (String) MonitorUtils.select(props.get("RequestCommand"));
            if (target == null || target.length() <= 0)
                throw(new IllegalArgumentException("RequestCommand" +
                    " is not defined for " + uri));
            target = MonitorUtils.substitute(target, template);
            requester = initRequester(h, str, name);
            break;
          case OBJ_REQ:
            str = (String) props.get("RequesterClass");
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("ConnectOnInit", "false");
            target = (String) MonitorUtils.select(props.get("RequestCommand"));
            if (target == null || target.length() <= 0)
                throw(new IllegalArgumentException("RequestCommand" +
                    " is not defined for " + uri));
            target = MonitorUtils.substitute(target, template);
            requester = initRequester(h, str, name);
            break;
          default:
            break;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int n, returnCode = -1;
        List<Object> dataBlock = new ArrayList<Object>();
        Map<String, Object> r = null;
        StringBuffer strBuf;
        String str;
        String[] keys = null;
        Object o;

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
            throw(new IOException(name + " failed to get report: " +
                Event.traceStack(e)));
        }

        switch (oid) {
          case OBJ_JMX:
            jmxReq.reconnect();
            try {
                keys = jmxReq.list(target); 
            }
            catch (JMException e) {
                throw(new IOException(Event.traceStack(e)));
            }
            jmxReq.close();
            if (keys != null) {
                n = keys.length;
                for (int i=0; i<n; i++) {
                    str = keys[i];
                    if (str != null && str.length() > 0)
                        dataBlock.add(str);
                }
            }
            break;
          case OBJ_HTTP:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                r.clear();
                throw(new IOException("web request failed on " + uri));
            }

            if (returnCode == 0) { // get content of http response
                String text = (String) r.remove("Response");
                if (text != null && (n = text.indexOf("\r\n\r\n")) > 0)
                    text = text.substring(n+4);
                else
                    text = "";
                n = getResult(dataBlock, resultType, text);
            }
            r.clear();
            break;
          case OBJ_SCRIPT:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                r.clear();
                throw(new IOException(name + ": script failed on " + uri));
            }

            if (returnCode != 0)
                n = getResult(dataBlock, resultType, (String) r.get("Output"));
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
          case OBJ_REPORT:
            o = r.get(keyList[0]);
            if (o == null) {
                r.clear();
                throw(new IOException(name + " got null value on " +
                    keyList[0] + " from " + reporter.getReportName()));
            }
            else if (o instanceof List) {
                for (Object obj : (List) o)
                    dataBlock.add(obj);
            }
            else {
                r.clear();
                throw(new IOException(name + " got unexpected data type on " +
                    keyList[0] + " from " + reporter.getReportName()));
            }
            r.clear();
            break;
          case OBJ_JDBC:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                r.clear();
                throw(new IOException(name + ": db query failed on " + uri));
            }

            if (returnCode == 0)
                dataBlock = Utils.cloneProperties((List) r.get("RecordBuffer"));
            r.clear();
            break;
          case OBJ_PCF:
          case OBJ_REQ:
          case OBJ_SONIC:
            strBuf = new StringBuffer();
            try {
                n = requester.getResponse(target, strBuf, true);
            }
            catch (Exception e) {
                try {
                    Thread.sleep(500L);
                }
                catch (Exception ex) {
                }
                try { // retry
                    n = requester.getResponse(target, strBuf, true);
                }
                catch (Exception ex) {
                    throw(new IOException(name + " failed to request: " +
                        Event.traceStack(e)));
                }
            }
            if (n < 0)
                throw(new IOException(name + ": request failed with " + n));
            else if (n > 0)
                n = getResult(dataBlock, Utils.RESULT_JSON, strBuf.toString());
            if (n < 0)
                throw(new IOException(name + ": unexpected json response '" +
                    strBuf.toString() + "'"));
            break;
          default:
            break;
        }

        if (kTemp != null) // save original result to Data for private report
            report.put("Data", dataBlock);

        if ((n = dataBlock.size()) > 0) { // select items on patterns
            if (kTemp == null && itemType == Utils.RESULT_TEXT) { // for strings
                for (int i=n-1; i>=0; i--) {
                    str = (String) dataBlock.get(i);
                    if (str == null || str.length() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(str, xPatternGroup, pm, false)) {
                        if (tSub != null)
                            dataBlock.set(i, tSub.substitute(str));
                    }
                    else
                        dataBlock.remove(i);
                }
                report.put("List", dataBlock);
            }
            else if (kTemp == null) { // list of maps without keys
                int k = 0;
                for (int i=n-1; i>=0; i--) {
                    Map map = (Map) dataBlock.get(i);
                    if (map == null || map.size() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    if (eTemp != null) { // applying filter
                        str = eTemp.substitute(map, eTemp.copyText());
                        if (str == null || str.length() <= 0) {
                            dataBlock.remove(i);
                            continue;
                        }
                        o = Evaluation.evaluate(str);
                        if (o == null || !(o instanceof Integer) ||
                            ((Integer) o).intValue() == 0)
                            dataBlock.remove(i);
                    }
                }
                report.put("List", dataBlock);
            }
            else if (isString) { // dataBlock is a list of Strings
                List<String> list = new ArrayList<String>();
                report.put("List", list);
                keys = new String[n];
                int k = 0;
                for (int i=n-1; i>=0; i--) {
                    str = (String) dataBlock.get(i);
                    if (str == null || str.length() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(str, xPatternGroup, pm, false)) {
                        if (tSub != null)
                            keys[k++] = tSub.substitute(str);
                        else
                            keys[k++] = str;
                    }
                    else
                        dataBlock.remove(i);
                }
                for (int i=k-1; i>=0; i--) // reverse the order
                    list.add(keys[i]);
            }
            else { // convert maps into keys or not
                keys = new String[n];
                int k = 0;
                for (int i=n-1; i>=0; i--) {
                    Map map = (Map) dataBlock.get(i);
                    if (map == null || map.size() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    str = kTemp.substitute(map, kTemp.copyText());
                    if (str == null || str.length() <= 0)
                        continue;
                    if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(str, xPatternGroup, pm, false)) {
                        if (tSub != null)
                            keys[k++] = tSub.substitute(str);
                        else
                            keys[k++] = str;
                    }
                    else
                        dataBlock.remove(i);
                }
                if (itemType == Utils.RESULT_TEXT) { // list of Strings
                    List<String> list = new ArrayList<String>();
                    report.put("List", list);
                    for (int i=k-1; i>=0; i--) // reverse the order
                        list.add(keys[i]);
                }
                else // list of Maps
                    report.put("List", dataBlock);
            }
        }
        else if (kTemp == null || isString || itemType == Utils.RESULT_TEXT)
            report.put("List", new ArrayList<String>());
        else
            report.put("List", new ArrayList<Map>());

        if (disableMode != 0) {
            if ((dataBlock.size() <= 0 && disableMode > 0) ||
                (dataBlock.size() > 0 && disableMode < 0))
                skip = DISABLED;
        }

        return report;
    }

    @SuppressWarnings("unchecked")
    private int getResult(List list, int dataType, String text) {
        Object o;
        String str;
        int n = -1;
        if (text == null || list == null)
            return -1;

        if (dataType == Utils.RESULT_JSON) {
            if (isString || (kTemp == null && itemType == Utils.RESULT_TEXT))
                n = pickupList(text, jsonPath, list);
            else { // with map data
                StringReader sr = null;
                try {
                    sr = new StringReader(text);
                    o = JSON2Map.parse(sr);
                    sr.close();
                }
                catch (Exception e) {
                    o = null;
                    if (sr != null)
                        sr.close();
                    throw(new IllegalArgumentException(
                        "failed to parse JSON payload: "+ Event.traceStack(e)));
                }

                if (o == null || !(o instanceof Map))
                    return -1;
                o = ((Map) o).remove("Record");
                if (o == null || !(o instanceof List))
                    return -1;
                List pl = (List) o;
                for (Object obj : pl) {
                    if (obj instanceof Map) { // data map
                        list.add((Map) obj);
                        n ++;
                    }
                }
                pl.clear();
            }
        }
        else if (dataType == Utils.RESULT_XML) {
            StringReader sr = null;
            Document doc;
            try {
                sr = new StringReader(text);
                doc = builder.parse(new InputSource(sr));
                sr.close();
                o = xpe.evaluate(doc, XPathConstants.NODESET);
            }
            catch (Exception e) {
                if (sr != null)
                    sr.close();
                throw(new IllegalArgumentException(
                    "failed to parse XML payload: " + Event.traceStack(e)));
            }
            if (o != null && o instanceof NodeList) {
                NodeList nl = (NodeList) o;
                int k = nl.getLength();
                n = 0;
                if(isString ||(kTemp == null && itemType == Utils.RESULT_TEXT)){
                    for (int i=0; i<k; i++) {
                        str = Utils.nodeToText(nl.item(i));
                        if (str == null || str.length() <= 0)
                            continue;
                        list.add(str);
                        n ++;
                    }
                }
                else { // with map data
                    Map<String, String> map;
                    for (int i=0; i<k; i++) {
                        map = Utils.nodeToMap(nl.item(i));
                        if (map == null || map.size() <= 0)
                            continue;
                        list.add(map);
                        n ++;
                    }
                }
            }
        }
        else { // assume for text
            Util.split(list, pm, patternLF, text);
            n = list.size();
        }

        return n;
    }

    @SuppressWarnings("unchecked")
    public static int pickupList(String text, String jsonPath, List list) {
        Object o;
        if(text == null || text.length()<=0 || list == null || jsonPath == null)
            return -1;

        StringReader sr = null;
        Object json = null;
        try {
            sr = new StringReader(text);
            json = JSON2Map.parse(sr);
            sr.close();
        }
        catch (Exception e) {
            json = null;
            if (sr != null)
                sr.close();
            throw(new IllegalArgumentException("failed to parse JSON payload: "+
                Event.traceStack(e)));
        }

        if (json == null)
            return -1;
        else if (json instanceof List)
            o = JSON2Map.get((List) json, jsonPath);
        else if (json instanceof Map)
            o = JSON2Map.get((Map) json, jsonPath);
        else
            return -1;
        int n = 0;
        if (o == null)
            return -1;
        else if (o instanceof List) { // a list
            List pl = (List) o;
            for (Object obj : pl) {
                if (obj instanceof Map)
                    list.add(JSON2Map.toJSON((Map) obj, null, null));
                else if (obj instanceof List)
                    list.add(JSON2Map.toJSON((List) obj, null, null));
                else
                    list.add((String) obj);
                n ++;
            }
        }
        else if (o instanceof Map) { // for a map
            list.add(JSON2Map.toJSON((Map) o, null, null));
            n ++;
        }
        else { // for a string
            list.add((String) o);
            n ++;
        }

        return n;
    }

    public void destroy() {
        super.destroy();
        if (kTemp != null)
            kTemp.clear();
        if (eTemp != null)
            eTemp.clear();
        if (tSub != null)
            tSub.clear();
        if (jmxReq != null)
            jmxReq.close();
        if (reporter != null)
            reporter.destroy();
        if (requester != null)
            requester.close();
    }

    protected void finalize() {
        destroy();
    }

    /**
     * It instantiates a Requester from the given props and returns it
     * upon success.  In case of failure, it throws IllegalArgumentException.
     */
    public static Requester initRequester(Map ph, String className,
        String prefix) {
        Object o;
        String key;

        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException(prefix +
                ": null or empty property Map"));

        if ((o = ph.get("URI")) == null || !(o instanceof String))
            throw(new IllegalArgumentException(prefix + ": URI not defined"));
        key = (String) o;

        if (className == null)
            className = (String) ph.get("ClassName");
        if (className == null || className.length() <= 0)
            throw(new IllegalArgumentException(prefix +
                ": ClassName not defined for " + key));

        try { // instantiate the Requester
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            return (Requester) con.newInstance(new Object[]{ph});
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            if (ex == null)
                throw(new RuntimeException("failed to instantiate "+
                    className + ": " + Event.traceStack(e)));
            else
                throw(new RuntimeException("failed to instantiate "+
                    className + ": " + Event.traceStack(ex)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(prefix +
                ": failed to instantiate " + className + ": " +
                Event.traceStack(e)));
        }
    }

    public static void main(String[] args) {
        String filename = null;
        MonitorReport report = null;

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
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) JSON2Map.parse(fr);
            fr.close();

            report = new GenericList(ph);
            Map r = report.generateReport(0L);
            List list = (List) r.get("List");
            if (list != null) {
                int i = 0;
                for (Object o : list) {
                    System.out.println(i++ + ": " + (String) o);
                }
            }
            else
                System.out.println("failed to get the list");
            if (report != null)
                report.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (report != null)
                report.destroy();
        }
    }

    private static void printUsage() {
        System.out.println("GenericList Version 1.0 (written by Yannan Lu)");
        System.out.println("GenericList: for a list of generic items");
        System.out.println("Usage: java org.qbroker.monitor.GenericList -I cfg.json");
    }
}
