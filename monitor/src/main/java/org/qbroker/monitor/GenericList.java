package org.qbroker.monitor;

/* GenericList.java - a monitor checking on a list of names or data */

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
import javax.xml.parsers.DocumentBuilderFactory;
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
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Util;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.Requester;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSONFormatter;
import org.qbroker.net.JMXRequester;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;
import org.qbroker.monitor.WebTester;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * GenericList queries a data source for a list of keys. The list of the keys
 * will be stored in a List under the key of "List" in the report. 
 *<br/><br/>
 * This needs more work on the support of other data sources.
 *<br/>
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
    private Template temp = null;
    private TextSubstitution tsub = null;
    private int oid;
    private int resultType = Utils.RESULT_JSON;
    Pattern patternLF = null;
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
        else if ("service".equals(u.getScheme()) || "imq".equals(u.getScheme()))
            oid = OBJ_JMX;
        else if ("jdbc".equals(u.getScheme()))
            oid = OBJ_JDBC;
        else if ("pcf".equals(u.getScheme()))
            oid = OBJ_PCF;
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
        else if ("wmq".equals(u.getScheme()))
            oid = OBJ_JMS;
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
        else if ("snmp".equals(u.getScheme()))
            oid = OBJ_SNMP;
        else if ((o = props.get("RequesterClass")) != null)
            oid = OBJ_REQ;
        else if ((o = props.get("ConnectionFactoryName")) != null)
            oid = OBJ_JMS;
        else
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((o = props.get("KeyTemplate")) != null) {
            if (oid == OBJ_PCF || oid == OBJ_SONIC || oid == OBJ_REQ) {
                str = MonitorUtils.select(o);
                str = MonitorUtils.substitute(str, template);
                temp = new Template(str);
            }
            else
                throw(new IllegalArgumentException("KeyTemplate is not " +
                    "supported for " + uri));
        }
        if ((o = props.get("KeySubstitution")) != null) {
            str = MonitorUtils.select(o);
            str = MonitorUtils.substitute(str, template);
            tsub = new TextSubstitution(str);
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

            DocumentBuilderFactory factory;
            XPath xpath;
            try {
                factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                builder = factory.newDocumentBuilder();
                xpath = XPathFactory.newInstance().newXPath();
                xpe = xpath.compile(str);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to get XPath: " + e.toString()));
            }
        }

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
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
        h.put("Timeout", (String) props.get("Timeout"));
        switch (oid) {
          case OBJ_JMX:
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                h.put("Password", props.get("Password"));
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
            if ((o = props.get("EncryptedAuthorization")) != null)
                h.put("EncryptedAuthorization", o);
            else if ((o = props.get("AuthString")) != null)
                h.put("AuthString", o);
            else if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                h.put("Password", props.get("Password"));
            }
            if ((o = props.get("Resolution")) != null)
                h.put("Resolution", o);
            reporter = new WebTester(h);
            break;
          case OBJ_SCRIPT:
            if ((o = props.get("Script")) != null)
                h.put("Script", o);
            h.put("ScriptTimeout", props.get("Timeout"));
            if ((o = props.get("Resolution")) != null)
                h.put("Resolution", o);
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
            h.put("PSTimeout", (String) props.get("Timeout"));
            reporter = new ProcessMonitor(h);
            break;
          case OBJ_JDBC:
            if ((o = props.get("DBDriver")) != null)
                h.put("DBDriver", o);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                h.put("Password", props.get("Password"));
            }
            if ((o = props.get("SQLQuery")) != null)
                h.put("SQLQuery", o);
            if ((o = props.get("Timeout")) != null)
                h.put("Timeout", o);
            if ((o = props.get("SQLExecTimeout")) != null)
                h.put("SQLExecTimeout", o);
            if ((o = props.get("Resolution")) != null)
                h.put("Resolution", o);
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
                h.put("Password", props.get("Password"));
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
                h.put("Password", props.get("Password"));
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
                h.put("Password", props.get("Password"));
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
                throw(new IOException("web test failed on " + uri));
            }

            if (returnCode == 0)
                n = getResult(dataBlock, resultType, (String) r.get("Output"));
            break;
          case OBJ_SCRIPT:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                throw(new IOException(name + ": script failed on " + uri));
            }

            if (returnCode != 0)
                n = getResult(dataBlock, resultType, (String) r.get("Output"));
            break;
          case OBJ_REPORT:
            o = r.get(keyList[0]);
            if (o == null)
                throw(new IOException(name + " got null value on " +
                    keyList[0] + " from " + reporter.getReportName()));
            else if (o instanceof List) {
                for (Object obj : (List) o)
                    dataBlock.add(obj);
            }
            else
                throw(new IOException(name + " got unexpected data type on " +
                    keyList[0] + " from " + reporter.getReportName()));
            break;
          case OBJ_JDBC:
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
            }
            else {
                throw(new IOException(name + ": db query failed on " + uri));
            }

            if (returnCode == 0)
                dataBlock = Utils.cloneProperties((List) r.get("RecordBuffer"));
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

        if (temp != null)
            report.put("Data", dataBlock);
        else
            report.put("List", dataBlock);
        if ((n = dataBlock.size()) > 0) { // select lines on patterns
            if (temp == null) { // list of strings
                for (int i=n-1; i>=0; i--) {
                    str = (String) dataBlock.get(i);
                    if (str == null || str.length() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(str, xPatternGroup, pm, false)) {
                        if (tsub != null)
                            dataBlock.set(i, tsub.substitute(pm, str));
                    }
                    else
                        dataBlock.remove(i);
                }
            }
            else { // list of maps
                List<String> list = new ArrayList<String>();
                report.put("List", list);
                keys = new String[n];
                int k = 0;
                for (int i=n-1; i>=0; i--) {
                    Map map = (Map) dataBlock.get(i);
                    if (map == null || map.size() <= 0) {
                        dataBlock.remove(i);
                        continue;
                    }
                    str = temp.substitute(pm, temp.copyText(), map);
                    if (str == null || str.length() <= 0)
                        continue;
                    if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                        !MonitorUtils.filter(str, xPatternGroup, pm, false)) {
                        if (tsub != null)
                            keys[k++] = tsub.substitute(pm, str);
                        else
                            keys[k++] = str;
                    }
                    else
                        dataBlock.remove(i);
                }
                for (int i=k-1; i>=0; i--) // reverse the order
                    list.add(keys[i]);
            }
        }
        else if (temp != null)
            report.put("List", new ArrayList<String>());

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
            if (temp == null)
                n = pickupList(text, jsonPath, list);
            else { // with data
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
                for (int i=0; i<k; i++) {
                    str = Utils.nodeToText(nl.item(i));
                    if (str == null || str.length() <= 0)
                        continue;
                    list.add(str);
                    n ++;
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
        String url = null;
        String username = null;
        String password = null;
        String request = null;
        String classname = null;
        String type = "json";
        String pattern = null;
        String xpattern = null;
        String substitution = null;
        String template = null;
        MonitorReport report = null;
        Map<String, Object> ph;
        int i, n = 0;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'u':
                if (i+1 < args.length)
                    url = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    username = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    password = args[++i];
                break;
              case 'd':
                if (i+1 < args.length)
                    type = args[++i];
                break;
              case 'r':
                if (i+1 < args.length)
                    request = args[++i];
                break;
              case 'c':
                if (i+1 < args.length)
                    classname = args[++i];
                break;
              case 'x':
                if (i+1 < args.length)
                    xpattern = args[++i];
                break;
              case 'y':
                if (i+1 < args.length)
                    pattern = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    template = args[++i];
                break;
              case 's':
                if (i+1 < args.length)
                    substitution = args[++i];
                break;
              default:
            }
        }

        ph = new HashMap<String, Object>();
        ph.put("Name", "Test");
        ph.put("URI", url);
        if (username != null) {
            ph.put("Username", username);
            ph.put("Password", password);
        }
        if ("json".equalsIgnoreCase(type))
            ph.put("ResultType", String.valueOf(Utils.RESULT_JSON));
        else if ("xml".equalsIgnoreCase(type))
            ph.put("ResultType", String.valueOf(Utils.RESULT_XML));
        else
            ph.put("ResultType", String.valueOf(Utils.RESULT_TEXT));
        ph.put("RequestCommand", request);
        if (classname != null)
            ph.put("RequestClass", classname);
        if (pattern != null) { // primary pattern
            List<String> pl = new ArrayList<String>();
            pl.add(pattern);
            Map<String, List> map = new HashMap<String, List>();
            map.put("Pattern", pl);
            List<Map> list = new ArrayList<Map>();
            list.add(map);
            ph.put("PatternGroup", list);
        }
        if (xpattern != null) { // primary xpattern
            List<String> pl = new ArrayList<String>();
            pl.add(xpattern);
            Map<String, List> map = new HashMap<String, List>();
            map.put("Pattern", pl);
            List<Map> list = new ArrayList<Map>();
            list.add(map);
            ph.put("XPatternGroup", list);
        }
        if (template != null)
            ph.put("KeyTemplate", template);
        if (substitution != null)
            ph.put("KeySubstitution", substitution);

        try {
            report = new GenericList(ph);
            Map r = report.generateReport(0L);
            List list = (List) r.get("List");
            i = 0;
            if (list != null) {
                for (Object o : list) {
                    System.out.println(i++ + ": " + (String) o);
                }
            }
            else
                System.out.println("request failed: " + r.get("ReturnCode"));
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
        System.out.println("Usage: java org.qbroker.monitor.GenericList -u url -n username -p password -t target -a attributes");
        System.out.println(" -?: print this message");
        System.out.println("  u: service url");
        System.out.println("  n: username");
        System.out.println("  p: password");
        System.out.println("  r: request command");
        System.out.println("  c: request classname");
        System.out.println("  d: data type");
        System.out.println("  y: matching pattern");
        System.out.println("  x: excluding pattern");
        System.out.println("  t: key template");
        System.out.println("  s: key substitution");
        System.out.println("\nExample: java org.qbroker.monitor.SonicMQRequester -u tcp://intsonicqa1:2506 -n Administrator -p xxxx -r 'DISPLAY Domain1.brQA01:ID=brQA01,category=queue,queue=*'");
    }
}
