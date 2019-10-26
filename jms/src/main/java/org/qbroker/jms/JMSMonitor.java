package org.qbroker.jms;

/* JMSMonitor.java - a monitor checking flow rate on a JMS applicaiton */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Date;
import java.io.StringReader;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import javax.management.JMException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Requester;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.GenericList;
import org.qbroker.monitor.QReportQuery;
import org.qbroker.monitor.UnixlogMonitor;
import org.qbroker.jms.QueueConnector;
import org.qbroker.jms.QConnector;
import org.qbroker.event.Event;

/**
 * JMSMonitor monitors a JMS application by watching its log and its queue
 * depth. It supports 4 modes: Q_RESET, Q_BROWSE, Q_REMOVE and Q_GET or Q_PUT.
 * With Q_RESET, it queries the EnqCount and DeqCount on a WMQ queue with reset
 * option. With Q_BROWSE, it tries to browse the first message and checks its
 * JMSTimestamp and JMSMessageID for the change. With Q_REMOVE, it tries to get
 * a message from the queue. With Q_GET or Q_PUT, it counts the messages logged
 * into a log file while checking on the queue depth.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMSMonitor extends Monitor {
    private String hostName=null, connFactoryName;
    private String qmgrName, qName, logFile, uri, previousMsgString;
    private String query = null;
    private MonitorReport queue = null;
    private Requester requester = null;
    private QueueConnector jmsQ = null;
    private UnixlogMonitor log = null;
    private int previousQStatus, previousDepth, operation, qStatusOffset;
    private int disableMode, port, waterMark;
    private boolean isTopic = false;
    public final static int Q_SLOW = -4;
    public final static int Q_STUCK = -3;
    public final static int Q_NOAPPS = -2;
    public final static int Q_UNKNOWN = -1;
    public final static int Q_OK = 0;
    public final static int Q_GET = 1;
    public final static int Q_PUT = 2;
    public final static int Q_BROWSE = 3;
    public final static int Q_RESET = 4;
    public final static int Q_REMOVE = 5;
    public final static String qStatusText[] = {"SLOW", "STUCK",
        "NOAPPS", "UNKNOW", "OK", "GET", "PUT", "BROWSE", "RESET", "REMOVE"};

    public JMSMonitor(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();

        if (type == null)
            type = "JMSMonitor";

        if (description == null)
            description = "monitor a JMS application";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            URI u = null;
            String path;
            uri = MonitorUtils.substitute((String) o, template);

            try {
                u = new URI(uri);
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException(e.toString()));
            }

            if ("wmq".equals(u.getScheme())) {
                if ((path = u.getPath()) != null && path.length() > 1)
                    qmgrName = path.substring(1);
                else
                    qmgrName = "";
                if ((port = u.getPort()) <= 0)
                    port = 1414;
                hostName = u.getHost();
                if ((o = MonitorUtils.select(
                     props.get("ConnectionFactoryName"))) != null)
                     connFactoryName =
                         MonitorUtils.substitute((String)o, template);
                else
                     connFactoryName = "SYSTEM.DEF.SVRCONN";
            }
            else if ((o = props.get("ConnectionFactoryName")) != null) {
                connFactoryName = MonitorUtils.substitute((String) o, template);
            }
            else if ("report".equals(u.getScheme())) {
                if ((path = u.getPath()) != null && path.length() > 1)
                    qmgrName = path.substring(1);
                else
                    qmgrName = "TBD";
            }
        }
        else if ((o = MonitorUtils.select(props.get("QueueManager"))) != null) {
            qmgrName = MonitorUtils.substitute((String) o, template);
            uri = "wmq:///" + qmgrName;
            connFactoryName = "";
        }
        else {
            throw(new IllegalArgumentException(
                "URI or QueueManager is not defined"));
        }

        if ((o = MonitorUtils.select(props.get("QueueName"))) == null)
            throw(new IllegalArgumentException("QueueName is not defined"));
        qName = MonitorUtils.substitute((String) o, template);

        if ((o = MonitorUtils.select(props.get("LogFile"))) != null)
            logFile = MonitorUtils.substitute((String) o, template);
        else
            logFile = null;

        if ((o = props.get("WaterMark")) != null)
            waterMark = Integer.parseInt((String) o);
        else
            waterMark = 0;

        if ((o = props.get("Operation")) != null) {
            if ("get".equals(((String) o).toLowerCase()))
                operation = Q_GET;
            else if ("put".equals(((String) o).toLowerCase()))
                operation = Q_PUT;
            else if ("reset".equals(((String) o).toLowerCase()))
                operation = Q_RESET;
            else if ("remove".equals(((String) o).toLowerCase()))
                operation = Q_REMOVE;
            else
                operation = Q_BROWSE;
        }
        else
            operation = Q_BROWSE;

        // for QueueMonitor
        h.put("Name", name);
        h.put("URI", uri);
        h.put("QueueName", qName);
        h.put("WaterMark", "1000");
        h.put("Step", "1");
        if ((o = props.get("Username")) != null) {
            h.put("Username", MonitorUtils.select(o));
            if ((o = props.get("Password")) != null)
                h.put("Password", MonitorUtils.select(o));
            else if ((o = props.get("EncryptedPassword")) != null)
                h.put("EncryptedPassword", MonitorUtils.select(o));
        }
        if (uri.startsWith("wmq://") && operation != Q_BROWSE &&
            operation != Q_REMOVE) { // for wmq
            if (operation == Q_RESET && (o = props.get("StatsLog")) != null) {
                h.put("ResetQStats", "true");
                h.put("StatsLog", MonitorUtils.select(o));
                // disable the statsLogger
                statsLogger = null;
            }
            if (connFactoryName.length() > 0)
                h.put("ChannelName", connFactoryName);
            if ((o = props.get("SecurityExit")) != null) {
                h.put("SecurityExit", o);
                if ((o = props.get("SecurityData")) != null)
                    h.put("SecurityData", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedSecurityData")) != null)
                    h.put("EncryptedSecurityData", MonitorUtils.select(o));
            }

            try {
                Class<?> cls = Class.forName("org.qbroker.wmq.QueueMonitor");
                java.lang.reflect.Constructor con =
                    cls.getConstructor(new Class[]{Map.class});
                queue = (MonitorReport) con.newInstance(new Object[]{h});
            }
            catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                if (ex == null)
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "QueueMonitor: " + Event.traceStack(e)));
                else
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "QueueMonitor: " + Event.traceStack(ex)));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    "QueueMonitor: " + Event.traceStack(e)));
            }
        }
        else if (uri.startsWith("tcp://") && operation != Q_BROWSE &&
            operation != Q_REMOVE) { // sonicmq
            if ((o = props.get("Timeout")) != null)
                h.put("Timeout", o);
            h.put("ObjectName", qName);

            ObjectName objName;
            try {
                objName = new ObjectName(qName);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(
                    "failed to parse ObjectName '" + qName + "': " +
                    Event.traceStack(e)));
            }
            qmgrName = objName.getKeyProperty("ID");
            qName = objName.getKeyProperty("topic");
            if (qName == null || qName.length() <= 0)
                qName = objName.getKeyProperty("name");
            else
                isTopic = true;
            if (qName == null || qName.length() <= 0)
                throw(new IllegalArgumentException((isTopic ? "topic" : "name")+
                    " is not defined in " + objName.toString()));

            try {
                Class<?>cls=Class.forName("org.qbroker.sonicmq.SonicMQMonitor");
                java.lang.reflect.Constructor con =
                    cls.getConstructor(new Class[]{Map.class});
                queue = (MonitorReport) con.newInstance(new Object[]{h});
            }
            catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                if (ex == null)
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "MonitorReport: " + Event.traceStack(e)));
                else
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "MonitorReport: " + Event.traceStack(ex)));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    "MonitorReport: " + Event.traceStack(e)));
            }
        }
        else if (uri.startsWith("report://") && operation != Q_BROWSE &&
            operation != Q_REMOVE) { //report
            if ((o = props.get("ReportName")) != null)
                h.put("ReportName", o);
            else
                h.put("ReportName", qName);
            h.put("ReportExpiration", props.get("ReportExpiration"));
            h.put("ReportClass", props.get("ReportClass"));
            queue = new QReportQuery(h);
        }
        else if ((o = props.get("ClassNameForQueue")) != null) {
            h.put("ContextFactory",
                MonitorUtils.select(props.get("ContextFactory")));
            h.put("ConnectionFactoryName", connFactoryName);

            String className = (String) o;
            if (className == null || className.length() == 0)
              throw(new IllegalArgumentException("ClassNameForQueue is empty"));

            try {
                Class<?> cls = Class.forName(className);
                java.lang.reflect.Constructor con =
                    cls.getConstructor(new Class[]{Map.class});
                queue = (MonitorReport) con.newInstance(new Object[] {h});
            }
            catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                if (ex == null)
                    throw(new IllegalArgumentException("failed to instantiate "+
                        className + ": " + Event.traceStack(e)));
                else
                    throw(new IllegalArgumentException("failed to instantiate "+
                        className + ": " + Event.traceStack(ex)));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    className + ": " + Event.traceStack(e)));
            }
        }
        else { // MonitorReport is not required
            queue = null;
        }

        h.clear();
        if (logFile != null && operation != Q_RESET) { // log
            h.put("Name", name);
            h.put("URI", "log://" + logFile);
            h.put("ReferenceFile", props.get("ReferenceFile"));
            h.put("PatternGroup", props.get("PatternGroup"));
            h.put("XPatternGroup", props.get("XPatternGroup"));
            h.put("LogSize", props.get("LogSize"));
            h.put("MaxNumberLogs", props.get("MaxNumberLogs"));
            h.put("MaxScannedLogs", props.get("MaxScannedLogs"));
            h.put("TimePattern", props.get("TimePattern"));
            h.put("PerlPattern", props.get("PerlPattern"));
            h.put("NumberDataFields", props.get("NumberDataFields"));
            h.put("Step", "1");
            log = new UnixlogMonitor(h);
            jmsQ = null;
            requester = null;
        }
        else if (uri.startsWith("wmq://") && (operation == Q_BROWSE ||
            operation == Q_REMOVE)) { // wmq
            if (hostName != null) {
                h.put("HostName", hostName);
                h.put("Port", String.valueOf(port));
                h.put("ChannelName", connFactoryName);
            }
            else if (qmgrName.length() > 0) {
                h.put("QueueManager", qmgrName);
            }
            h.put("QueueName", qName);
            if (operation == Q_REMOVE)
                h.put("Operation", "get");
            else
                h.put("Operation", "browse");
            if ((o = props.get("SecurityExit")) != null) {
                h.put("SecurityExit", MonitorUtils.select(o));
                if ((o = props.get("SecurityData")) != null)
                    h.put("SecurityData", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedSecurityData")) != null)
                    h.put("EncryptedSecurityData", MonitorUtils.select(o));
            }
            h.put("DisplayMask", "0");
            h.put("MaxNumberMessage", "1");
            h.put("MessageSelector",
                MonitorUtils.select(props.get("MessageSelector")));

            try {
                Class<?> cls = Class.forName("org.qbroker.wmq.QConnector");
                java.lang.reflect.Constructor con =
                    cls.getConstructor(new Class[]{Map.class});
                jmsQ = (QueueConnector) con.newInstance(new Object[]{h});
                jmsQ.close();
            }
            catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                if (ex == null)
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "QueueConnector: " + Event.traceStack(e)));
                else
                    throw(new IllegalArgumentException("failed to instantiate "+
                        "QueueConnector: " + Event.traceStack(ex)));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    "QueueConnector: " + Event.traceStack(e)));
            }
            log = null;
            requester = null;
        }
        else if (uri.startsWith("tcp://") && operation == Q_BROWSE &&
            !props.containsKey("ContextFactory")) { //browse sonicmq durable sub
            query = "DISPLAY " + qName + " 1";
            h.put("URI", uri);
            if ((o = props.get("Timeout")) != null)
                h.put("Timeout", o);

            ObjectName objName;
            try {
                objName = new ObjectName(qName);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(
                    "failed to parse ObjectName '" + qName + "': " +
                    Event.traceStack(e)));
            }
            qmgrName = objName.getKeyProperty("ID");
            qName = objName.getKeyProperty("topic");
            isTopic = true;
            if (qName == null || qName.length() <= 0)
                throw(new IllegalArgumentException("Topic is not defined in "+
                    objName.toString()));

            try {
                requester = GenericList.initRequester(h,
                    "org.qbroker.sonicmq.SonicMQRequester", name);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    "Requester: " + Event.traceStack(e)));
            }
            log = null;
            jmsQ = null;
        }
        else if (operation == Q_BROWSE || // generic JMS browser
            operation == Q_REMOVE) { // generic JMS receiver
            h.put("URI", uri);
            h.put("ContextFactory",
                MonitorUtils.select(props.get("ContextFactory")));
            if ((o = props.get("IsPhysical")) != null)
                h.put("IsPhysical",  MonitorUtils.select(o));
            if ((o = props.get("Principal")) != null) {
                h.put("Principal", MonitorUtils.select(o));
                if ((o = props.get("Credentials")) != null)
                    h.put("Credentials", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedCredentials")) != null)
                    h.put("EncryptedCredentials", MonitorUtils.select(o));
            }
            h.put("ConnectionFactoryName", connFactoryName);
            h.put("QueueName", qName);
            if (operation == Q_REMOVE)
                h.put("Operation", "get");
            else
                h.put("Operation", "browse");
            h.put("MaxNumberMessage", "1");
            h.put("MessageSelector",
                MonitorUtils.select(props.get("MessageSelector")));

            try {
                jmsQ = (QueueConnector) new QConnector(h);
                jmsQ.close();
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to instantiate " +
                    "QueueConnector: " + Event.traceStack(e)));
            }
            log = null;
            requester = null;
        }

        previousMsgString = "";
        previousDepth = 0;
        previousQStatus = Q_SLOW - 1;
        qStatusOffset = 0 - Q_SLOW;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int curDepth = 0, totalMsgs = 0, inMsgs = 0, outMsgs = 0;
        int oppsCount = 0, ippsCount = 0;
        String msgString = "";
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

        if (operation == Q_BROWSE && jmsQ != null) {
            Message msg = null;
            String str = jmsQ.reconnect();
            if (str != null)
                throw(new IOException("failed to connect to " + qName +
                    " on " + uri + ": " + str));
            try {
                msg = (Message) browseFirstMsg(
                    jmsQ.getQueueBrowser().getEnumeration());
            }
            catch (Exception e) {
                try {
                    Thread.sleep(500L);
                }
                catch (Exception ex) {
                }
                jmsQ.reconnect();
                try { // retry
                    msg = (Message) browseFirstMsg(
                        jmsQ.getQueueBrowser().getEnumeration());
                }
                catch (Exception ex) {
                    jmsQ.close();
                    throw(new IOException("failed to browse msg from " + qName +
                        ": " + Event.traceStack(e)));
                }
            }
            jmsQ.close();
            if (msg != null) try {
                msgString = msg.getJMSTimestamp() + " " + msg.getJMSMessageID();
            }
            catch (JMSException e) {
                throw(new IOException("failed to get info from the msg for " +
                    qName + ": " + Event.traceStack(e)));
            }
            totalMsgs = 0;
        }
        else if (operation == Q_REMOVE && jmsQ != null) {
            Message msg = null;
            String str = jmsQ.reconnect();
            if (str != null)
                throw(new IOException("failed to connect to " + qName +
                    " on " + uri + ": " + str));
            try {
                msg = (Message) jmsQ.getQueueReceiver().receive(5000);
            }
            catch (Exception e) {
                try {
                    Thread.sleep(500L);
                }
                catch (Exception ex) {
                }
                jmsQ.reconnect();
                try { // retry
                    msg = (Message) jmsQ.getQueueReceiver().receive(5000);
                }
                catch (Exception ex) {
                    jmsQ.close();
                    throw(new IOException("failed to get msg from " + qName +
                        ": " + Event.traceStack(e)));
                }
            }
            jmsQ.close();
            if (msg != null) try {
                msgString = msg.getJMSTimestamp() + " " + msg.getJMSMessageID();
            }
            catch (JMSException e) {
                throw(new IOException("failed to get info from the msg for " +
                    qName + ": " + Event.traceStack(e)));
            }
            totalMsgs = 0;
        }
        else if (operation == Q_BROWSE) { // browse for SonicMQ durable subs
            StringBuffer strBuf = new StringBuffer();
            int n = -1;
            try {
                n = requester.getResponse(query, strBuf, true);
            }
            catch (Exception e) {
                try {
                    Thread.sleep(500L);
                }
                catch (Exception ex) {
                }
                try { // retry
                    n = requester.getResponse(query, strBuf, true);
                }
                catch (Exception ex) {
                    throw(new IOException("failed to browse on " + qName +
                        " from " + uri + ": " + Event.traceStack(e)));
                }
            }

            if (n < 0)
                throw(new IOException(name + ": browse failed with " + n));
            else if (n > 0) { // got response
                Object o;
                StringReader sr = null;
                try {
                    sr = new StringReader(strBuf.toString());
                    o = JSON2Map.parse(sr);
                    sr.close();
                }
                catch (Exception e) {
                    o = null;
                    if (sr != null)
                        sr.close();
                    throw(new IllegalArgumentException(name +
                        " failed to parse JSON payload: "+Event.traceStack(e)));
                }

                if (o == null || !(o instanceof Map))
                    throw(new IOException(name+": unexpected json response '" +
                        strBuf.toString() + "'"));
                o = ((Map) o).remove("Record");
                if (o == null || !(o instanceof List))
                    throw(new IOException(name+": unexpected JSON response '" +
                        strBuf.toString() + "'"));
                List list = (List) o;
                if (list.size() > 0) try {
                    Map map = (Map) list.get(0);
                    msgString = (String) map.get("JMSTimestamp") + " " +
                        (String) map.get("JMSMessageID");
                }
                catch (Exception e) {
                    throw(new IOException("unexpected response for  " + qName +
                        " from " + uri + ": " + Event.traceStack(e)));
                }
            }
            totalMsgs = 0;
        }
        else if (operation != Q_RESET) { // for log
            Map r = null;
            try {
                r = log.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("failed to get log count: " +
                    Event.traceStack(e)));
            }
            if (r != null && r.containsKey("TotalNumber"))
                totalMsgs = Integer.parseInt((String) r.get("TotalNumber"));
            else
                throw(new IOException("failed to get log count"));
        }

        if (queue != null) {
            Map r = null;
            try {
                r = queue.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("failed to get queue depth: " +
                    Event.traceStack(e)));
            }

            if (r != null) {
                curDepth = Integer.parseInt((String) r.get("CurrentDepth"));
                oppsCount = Integer.parseInt((String) r.get("OppsCount"));
                ippsCount = Integer.parseInt((String) r.get("IppsCount"));
                if (operation == Q_RESET) {
                    report.put("EnqCount", r.get("EnqCount"));
                    report.put("DeqCount", r.get("DeqCount"));
                }
            }
            else
                throw(new IOException("failed to get queue depth"));
        }
        else {
            curDepth = 0;
            oppsCount = 0;
            ippsCount = 0;
        }

        // calculate the in/out rate with cur = pre + ins - outs
        switch (operation) {
          case Q_GET:
            outMsgs = totalMsgs;
            inMsgs = totalMsgs + curDepth - previousDepth;
            break;
          case Q_PUT:
            inMsgs = totalMsgs;
            outMsgs = totalMsgs + previousDepth - curDepth;
            break;
          case Q_RESET:
            Object o;
            if ((o = report.get("EnqCount")) != null)
                inMsgs = Integer.parseInt((String) o);
            else
                inMsgs = 0;
            if ((o = report.get("DeqCount")) != null)
                outMsgs = Integer.parseInt((String) o);
            else
                outMsgs = 0;
            break;
          case Q_BROWSE:
          case Q_REMOVE:
          default:
            inMsgs = 0;
            outMsgs = 0;
            break;
        }

        if (statsLogger != null && queue != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            if (qmgrName != null && qmgrName.length() > 0)
                strBuf.append(qmgrName + ":");
            strBuf.append(qName + " ");
            strBuf.append(inMsgs + " ");
            strBuf.append(outMsgs + " ");
            strBuf.append(curDepth + " ");
            strBuf.append(ippsCount + " ");
            strBuf.append(oppsCount + " 0");
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        report.put("OutMessages", String.valueOf(outMsgs));
        report.put("InMessages", String.valueOf(inMsgs));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        report.put("Operation", String.valueOf(operation));
        if (operation == Q_BROWSE || operation == Q_REMOVE) {
            report.put("PreviousMsgString", previousMsgString);
            report.put("MsgString", msgString);
        }
        else if (logFile != null) {
            report.put("LogFile", logFile);
        }
        previousDepth = curDepth;

        if ((disableMode > 0 && ((operation == Q_GET && ippsCount <= 0) ||
            (operation == Q_PUT && oppsCount <= 0))) ||
            (disableMode < 0 && ((operation == Q_GET && ippsCount > 0) ||
            (operation == Q_PUT && oppsCount > 0))))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int curDepth = 0, level = 0, qStatus = Q_UNKNOWN;
        int inMsgs = 0, outMsgs = 0, preDepth = 0;
        String msgString = "";
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Integer.parseInt((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Integer.parseInt((String) o);
        if ((o = latest.get("InMessages")) != null && o instanceof String)
            inMsgs = Integer.parseInt((String) o);
        if ((o = latest.get("OutMessages")) != null && o instanceof String)
            outMsgs = Integer.parseInt((String) o);
        if (operation == Q_BROWSE || operation == Q_REMOVE)
            msgString = (String) latest.get("MsgString");

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
            o = latest.get("Exception");
            if (status == TimeWindows.EXCEPTION) {
                level = Event.WARNING;
                if (previousStatus != status) { // reset count and adjust step
                    exceptionCount = 0;
                    if (step > 0)
                        step = 0;
                    new Event(Event.WARNING, name +
                        " failed to generate report on " + qName + ": " +
                        Event.traceStack((Exception) o)).send();
                }
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) o).toString());
            qStatus = Q_UNKNOWN;
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
            qStatus = Q_OK;
            exceptionCount = 0;
            if (status != TimeWindows.BLACKOUT &&
                previousStatus == TimeWindows.BLACKOUT)
                actionCount = 0;
            actionCount ++;
            if (operation == Q_BROWSE) {
                if(msgString.length()>0 && msgString.equals(previousMsgString)){
                    if (status != TimeWindows.BLACKOUT) { // for normal case
                        level = Event.ERR;
                        if (step > 0)
                            step = 0;
                    }
                    if (isTopic)
                        strBuf.append("Topic: ");
                    else
                        strBuf.append("Queue: ");
                    strBuf.append("application seems not reading from "+qName);
                    qStatus = Q_STUCK;
                    if (previousQStatus != qStatus)
                        actionCount = 1;
                }
                else if (previousQStatus != qStatus) {
                    if (isTopic)
                        strBuf.append("Topic: application is OK");
                    else
                        strBuf.append("Queue: application is OK");
                    actionCount = 1;
                    if (normalStep > 0)
                        step = normalStep;
                }
                previousMsgString = msgString;
            }
            else if (operation == Q_REMOVE) {
                if (msgString.length() <= 0) { // no message got received
                    if (status != TimeWindows.BLACKOUT) { // for normal case
                        level = Event.ERR;
                        if (step > 0)
                            step = 0;
                    }
                    strBuf.append("no message received from " + qName);
                    qStatus = Q_STUCK;
                    if (previousQStatus != qStatus)
                        actionCount = 1;
                }
                else if (previousQStatus != qStatus) {
                    strBuf.append("Queue is OK");
                    actionCount = 1;
                    if (normalStep > 0)
                        step = normalStep;
                }
                previousMsgString = msgString;
            }
            else if (curDepth > 0 && preDepth > 0 && outMsgs <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: application seems not reading from ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName);
                qStatus = Q_STUCK;
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (waterMark > 0 && curDepth > waterMark &&
                preDepth > waterMark) {
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: application is slow to read from ");
                strBuf.append(qmgrName + ":");
                strBuf.append(qName);
                qStatus = Q_SLOW;
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (previousQStatus != qStatus) {
                if (isTopic)
                    strBuf.append("Topic: application is OK");
                else
                    strBuf.append("Queue: application is OK");
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousStatus = status;
        previousQStatus = qStatus;

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
                if (count == 1 || count == tolerance) // react on 1st or last
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
            if (queue != null)
                event.setAttribute("currentDepth", "N/A");
            if (operation != Q_BROWSE && operation != Q_REMOVE) {
                event.setAttribute("inMessages", "N/A");
                event.setAttribute("outMessages", "N/A");
                event.setAttribute("logFile", logFile);
            }
            else {
                event.setAttribute("firstMsg", "N/A");
                event.setAttribute("MessageID", "N/A");
            }
        }
        else {
            count = actionCount;
            if (queue != null)
                event.setAttribute("currentDepth", String.valueOf(curDepth));
            if (operation != Q_BROWSE && operation != Q_REMOVE) {
                event.setAttribute("inMessages", String.valueOf(inMsgs));
                event.setAttribute("outMessages", String.valueOf(outMsgs));
                event.setAttribute("logFile", logFile);
            }
            else if (msgString.length() > 0) {
                int i = msgString.indexOf(" ");
                event.setAttribute("MessageID", msgString.substring(i+1));
                try {
                    long tm = Long.parseLong(msgString.substring(0, i));
                    event.setAttribute("firstMsg",
                        Event.dateFormat(new Date(tm)) +msgString.substring(i));
                }
                catch (Exception e) {
                    event.setAttribute("firstMsg", msgString);
                }
            }
            else {
                event.setAttribute("firstMsg", "");
                event.setAttribute("MessageID", "N/A");
            }
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        if (requester != null) {
            event.setAttribute("broker", qmgrName);
            if (isTopic)
                event.setAttribute("topic", qName);
            else
                event.setAttribute("queue", qName);
        }
        else {
            event.setAttribute("connFactory", connFactoryName);
            event.setAttribute("queue", qName);
        }
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

    @SuppressWarnings("unchecked")
    private int getResult(List list, String text) {
        Object o;
        String str;
        int n = -1;
        if (text == null || list == null)
            return -1;

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
        return n;
    }
    private Message browseFirstMsg(Enumeration msgQ) {
        if (msgQ != null && msgQ.hasMoreElements())
            return (Message) msgQ.nextElement();
        else
            return null;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousQStatus", String.valueOf(previousQStatus));
        chkpt.put("PreviousDepth", String.valueOf(previousDepth));
        chkpt.put("PreviousMsgString", String.valueOf(previousMsgString));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pDepth, pStatus, sNumber, pQStatus;
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

        if ((o = chkpt.get("PreviousDepth")) != null)
            pDepth = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousQStatus")) != null)
            pQStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDepth = pDepth;
        previousQStatus = pQStatus;
        previousMsgString = (String) chkpt.get("PreviousMsgString");
    }

    public void destroy() {
         super.destroy();
        if (queue != null) {
            queue.destroy();
            queue = null;
        }
        if (log != null) {
            log.destroy();
            log = null;
        }
        if (jmsQ != null) {
            jmsQ.close();
            jmsQ = null;
        }
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

            monitor = new JMSMonitor(ph);
            Map r = monitor.generateReport(tm);
            String str = (String) r.get("CurrentDepth");
            if (str != null) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else {
                    System.out.println(str + " " + r.get("InMessages") + " " +
                        r.get("OutMessages"));
                    str = (String) r.get("MsgString");
                    if (str != null)
                        System.out.println(str);
                }
            }
            else
                System.out.println("failed to get queue stats");
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
        System.out.println("JMSMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("JMSMonitor: monitor a JMS queue");
        System.out.println("Usage: java org.qbroker.jms.JMSMonitor -I cfg.json");
    }
}
