package org.qbroker.sonicmq;

/* SonicMQMonitor.java - a monitor checking message flow rate on a queue */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Date;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import javax.management.JMException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.sonicmq.SonicMQRequester;
import org.qbroker.event.Event;

/**
 * SonicMQMonitor monitors the message storage on a SonicMQ broker via JMS/JMX. 
 * Currently, it only supports broker, queues and durable subscriptions. Since
 * there is no metric for deq and enq on destinations, it queries the metrics
 * of connections associated with the specific destination. Unfortunately,
 * SonicMQ management API does not provide a easy way to query all connections
 * on a queue. So it is assumed that the consumer application of the queue is
 * is running as a dedicated user only on certain well known hosts. Therefore,
 * SonicMQMonitor is able to use the combinations of the user and the hostnames
 * to associate non-topic connections with a specific queue.
 *<br/></br>
 * HostPattern is a Perl5Pattern expression to select hostnames for the
 * consumer applications. UserMappingRule is a list of maps with two items of
 * Pattern and Substitution. On a given name of the destination, SonicMQMonitor 
 * loops through the list to apply patterns matching the name one by one until
 * it finds a match. Then its substitution part is applied on the name to map
 * the destination name to the username. One example is to cut off the ending
 * "Queue" to convert a queue name to a user name.
 *<br/></br>
 * In order for SonicMQMonitor to work, the metrics on connections, queues and
 * brokers must be enabled. Please check the source code to figure out the list
 * of the metrics.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SonicMQMonitor extends Monitor {
    private String qName = null, uri, target = null;
    private SonicMQRequester jmxc = null;
    private Map<String, String> cache = null;
    private List<Map> mapList = null;
    private int watermark = 0;
    private long previousDepth, previousMsgs;
    private String previousQStatus, brokerName, subID = null;
    private String targetConn = null, connUser = null, hostPattern = null;
    private boolean isTopic = false;
    private boolean isQueue = false;
    private boolean withPrivateReport = false;

    public SonicMQMonitor(Map props) {
        super(props);
        Object o;
        ObjectName objName;
        Map<String, Object> h = new HashMap<String, Object>();

        if (type == null)
            type = "SonicMQMonitor";

        if (description == null)
            description = "monitor a SonicMQ queue";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
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

            h.put("URI", uri);
            if ((o = props.get("Username")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }

            h.put("ConnectOnInit", "false");
            if ("tcp".equals(scheme)) { // for SonicMQ
                jmxc = new SonicMQRequester(h);
            }
            else { // not supported
                throw(new IllegalArgumentException("URI is not supported"));
            }
            h.clear();
        }
        else {
            throw(new IllegalArgumentException("URI is not defined"));
        }

        if ((o = props.get("HostPattern")) != null)
            hostPattern = (String) o;

        if((o = props.get("UserMappingRule")) != null && o instanceof List) try{
            Perl5Compiler pc = new Perl5Compiler();
            mapList = MonitorUtils.getGenericMapList((List) o, pc, template);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to compile patterns: " +
                e.toString()));
        }

        if ((o = props.get("WithPrivateReport")) != null &&
            "true".equals((String) o)) // for private report
            withPrivateReport = true;

        if ((o = MonitorUtils.select(props.get("ObjectName"))) == null)
            throw(new IllegalArgumentException("ObjectName is not defined"));
        
        target = MonitorUtils.substitute((String) o, template);
        try {
            objName = new ObjectName(target);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to parse ObjectName '" +
                target + "': " + Event.traceStack(e)));
        }

        brokerName = objName.getKeyProperty("ID");
        qName = objName.getKeyProperty("name");
        if (qName != null && qName.length() > 0) { // for queue
            Perl5Matcher pm = new Perl5Matcher();
            isQueue = true;
            targetConn = target;
            connUser = MonitorUtils.getMappedName(qName, mapList, pm);
            targetConn = target.replaceFirst(",category=.*$", "") +
                ",category=metric,type=connection,user=" + connUser +
                ",queue=" + qName;
            cache = new HashMap<String, String>();
        }
        else { // for topic or broker
            qName = objName.getKeyProperty("topic");
            if (qName == null || qName.length() <= 0) { // default is broker
                qName = brokerName;
            }
            else { // for topic and replace '.' with ':' due to the hack
                subID = objName.getKeyProperty("subscription_id");
                connUser = objName.getKeyProperty("user");
                if (connUser == null || connUser.length() <= 0) {
                    Perl5Matcher pm = new Perl5Matcher();
                    connUser = MonitorUtils.getMappedName(qName, mapList, pm);
                }
                targetConn = target.replaceFirst(",category=.*$", "") +
                    ",category=metric,type=connection,user=" + connUser +
                    ",topic=" + qName + ",subscription_id=" + subID;
                subID = subID.replace('.', ':');
                isTopic = true;
            }
        }

        if (qName == null || qName.length() <= 0)
            throw(new IllegalArgumentException("no queue or topic defined in " +
                target));

        if ((o = props.get("WaterMark")) == null ||
            (watermark = Integer.parseInt((String) o)) < 0)
            watermark = 0;

        previousMsgs = 0;
        previousDepth = 0;
        previousQStatus = "UNKNOWN";
    }

    public Map<String, Object> generateReport(long currentTime)
        throws JMException {
        long curDepth = 0, totalMsgs = 0, inMsgs = 0, outMsgs = 0;
        long oppsCount = 0, ippsCount = 0, previousIppsCount = 0;
        String qStatus;
        Object o;
        List<Map> list = null;

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

        qStatus = jmxc.reconnect();
        if (qStatus != null)
            throw(new JMException("failed to connect to " + uri + " for " +
                qName + ": " + qStatus));

        if (!withPrivateReport) {
            try {
                list = jmxc.query(target, null);
            }
            catch (Exception e) {
                try {
                    Thread.sleep(500L);
                }
                catch (Exception ex) {
                }
                jmxc.reconnect();
                try {
                    list = jmxc.query(target, null);
                }
                catch (Exception ex) {
                    jmxc.close();
                    throw(new JMException("failed to get SonicMQ metrics on " +
                        qName + " from " + target + ": " +
                        Event.traceStack(e)));
                }
            }
            if (!isTopic && !isQueue)
                jmxc.close();
        }
        else try { // for private report
            list = new ArrayList<Map>();
            o = MonitorUtils.getPrivateReport();
            if (o != null && o instanceof Map)
                list.add((Map) o);
        }
        catch (Exception e) {
            throw(new JMException("failed to get the private report for " +
                "SonicMQ metrics of " + qName + ": " + Event.traceStack(e)));
        }

        if (list == null || list.size() <= 0) {
            throw(new JMException("failed to get metrics on " + qName +
               " from " + ((withPrivateReport)? "private report" : target)));
        }

        String str;
        Map map = list.get(0);
        list.clear();
        if (isTopic) { // for topic metrics
            if (serialNumber == 1) { // make sure the data is right
                str = (String) map.get("topic");
                if (!qName.equals(str))
                    new Event(Event.ERR, name + " got a different topic: " +
                        str).send();
            }
            str = (String) map.get("lastconnectedtime");
            if ("-1".equals(str)) try { // subscription is active
                list = jmxc.query(targetConn, hostPattern);
                ippsCount = list.size();
            }
            catch (Exception e) {
                ippsCount = -1L;
                new Event(Event.ERR, "failed to get SonicMQ metrics on " +
                    "connections for " + qName + " from " + targetConn + ": " +
                    Event.traceStack(e)).send();
            }
            jmxc.close();
            str = (String) map.get("messagecount");
            map.clear();
            try {
                curDepth = Long.parseLong(str);
                if (ippsCount > 0) { // got connection metrics
                    map = list.get(0);
                    str = (String) map.get("messages_Delivered");
                    totalMsgs = Long.parseLong(str);
                    map.clear();
                    list.clear();
                }
                else
                    totalMsgs = -1;
            }
            catch (Exception e) {
                map.clear();
                list.clear();
                throw(new JMException("failed to parse metrics for " + qName +
                    " with " + JSON2Map.toJSON(map, null, null) + ": " +
                    Event.traceStack(e)));
            }

            oppsCount = 0;
            if (totalMsgs >= 0) { //calculate in/out with cur = pre + ins - outs
                if (previousStatus < TimeWindows.EXCEPTION) { // initial reset
                    previousMsgs = totalMsgs;
                    previousDepth = curDepth;
                }
                outMsgs = (totalMsgs >= previousMsgs) ?
                    totalMsgs - previousMsgs : totalMsgs;
                inMsgs = outMsgs - previousDepth + curDepth;
                previousMsgs = totalMsgs;
            }
            else {
                inMsgs = 0;
                outMsgs = 0;
            }

            if (serialNumber == 1) { // initial reset
                qStatus = "OK";
                previousIppsCount = 1;
            }
            else if (curDepth > 0 && previousIppsCount <= 0 && ippsCount <= 0)
                qStatus = "STUCK";
            else
                qStatus = "OK";
        }
        else if (isQueue) { // for queue metrics
            if (serialNumber == 1) { // make sure the data is right
                str = (String) map.get("name");
                if (!qName.equals(str))
                    new Event(Event.ERR, name + " got a different queue: " +
                        str).send();
            }
            try {
                list = jmxc.query(targetConn, hostPattern);
                ippsCount = list.size();
            }
            catch (Exception e) {
                ippsCount = -1L;
                new Event(Event.ERR, "failed to get SonicMQ metrics on " +
                    "connections for " + qName + " from " + targetConn + ": " +
                    Event.traceStack(e)).send();
            }
            jmxc.close();
            if (ippsCount > 0) {
                if (previousStatus < TimeWindows.EXCEPTION) { // initial reset
                    for (Map mp : list) {
                        str = (String) mp.get("host");
                        str += "/" + (String) mp.get("name");
                        cache.put(str, (String) mp.get("messages_Delivered"));
                    }
                }
                else { // clean up cache
                    HashSet<String> hset = new HashSet<String>();
                    for (Map mp : list) {
                        str = (String) mp.get("host");
                        str += "/" + (String) mp.get("name");
                        hset.add(str);
                    }
                    for (String key : cache.keySet()) {
                        if (!hset.contains(key))
                            cache.remove(key);
                    }
                    hset.clear();
                }
            }

            outMsgs = 0;
            str = (String) map.get("messages_Count");
            map.clear();
            try {
                long prev;
                curDepth = Long.parseLong(str);
                previousMsgs = 0;
                if (ippsCount > 0) for (Map mp : list) {
                    map = mp;
                    String key = (String) mp.get("host");
                    key += "/" + (String) mp.get("name");
                    if (cache.containsKey(key))
                        prev = Long.parseLong(cache.get(key)); 
                    else
                        prev = 0;
                    str = (String) mp.get("messages_Delivered");
                    cache.put(key, str);
                    totalMsgs = Long.parseLong(str);
                    outMsgs += (totalMsgs >= prev) ? totalMsgs-prev : totalMsgs;
                    previousMsgs += totalMsgs;
                    mp.clear();
                }
                list.clear();
            }
            catch (Exception e) {
                list.clear();
                throw(new JMException(name + " failed to parse metrics for " +
                    qName + " with " + JSON2Map.toJSON(map, null, null) + ": "+
                    Event.traceStack(e)));
            }

            oppsCount = 0;
            if (previousStatus < TimeWindows.EXCEPTION) // initial reset
                previousDepth = curDepth;
            inMsgs = outMsgs - previousDepth + curDepth;

            if (serialNumber == 1) { // initial reset
                qStatus = "OK";
                previousIppsCount = 1;
            }
            else if (curDepth > 0 && previousDepth > 0 && outMsgs <= 0)
                qStatus = "STUCK";
            else
                qStatus = "OK";
        }
        else try { // for broker metrics
            str = (String) map.get("bytes_FlowToDiskMemorySize");
            curDepth = Long.parseLong(str);
            str = (String) map.get("connections_Count");
            ippsCount = Long.parseLong(str);
            str = (String) map.get("messages_Received");
            inMsgs = Long.parseLong(str);
            str = (String) map.get("messages_Delivered");
            outMsgs = Long.parseLong(str);
            str = (String) map.get("bytes_TopicDBSize");
            oppsCount = Long.parseLong(str);
            str = (String) map.get("connections_RejectedPerMinute");
            totalMsgs = Long.parseLong(str);
            previousMsgs = outMsgs;
            if (watermark > 0 && curDepth > watermark)
                qStatus = "BUSY";
            else
                qStatus = "OK";
        }
        catch (Exception e) {
            throw(new JMException(name + " failed to parse metric for " +
                qName + ": " + Event.traceStack(e)));
        }
        report.put("StateLabel", qStatus);

        if (statsLogger != null) {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(Event.dateFormat(new Date(currentTime)) + " ");
            strBuf.append(brokerName +":" + qName + " ");
            strBuf.append(inMsgs + " ");
            strBuf.append(outMsgs + " ");
            strBuf.append(curDepth + " ");
            strBuf.append(ippsCount + " ");
            if (isQueue || isTopic)
                strBuf.append(oppsCount);
            else
                strBuf.append(oppsCount + "." + totalMsgs);
            report.put("Stats", strBuf.toString());
            try {
                statsLogger.log(strBuf.toString());
            }
            catch (Exception e) {
            }
        }
        report.put("OutMessages", String.valueOf(outMsgs));
        report.put("InMessages", String.valueOf(inMsgs));
        report.put("CurrentDepth", String.valueOf(curDepth));
        report.put("PreviousDepth", String.valueOf(previousDepth));
        if ((disableMode > 0 && curDepth <= 0 && previousDepth <= 0) ||
            (disableMode < 0 && (curDepth > 0 || previousDepth > 0)))
            skip = DISABLED;
        previousDepth = curDepth;
        if (isTopic) {
            report.put("IppsCount", String.valueOf(ippsCount));
            report.put("PreviousIpps", String.valueOf(previousIppsCount));
            previousIppsCount = ippsCount;
        }
        else
            report.put("IppsCount", String.valueOf(ippsCount));
        report.put("OppsCount", String.valueOf(oppsCount));

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long inMsgs = 0, outMsgs = 0, preDepth = 0, curDepth = 0;
        long ipps = 0, preIpps = 0;
        String qStatus = "UNKNOWN";
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("CurrentDepth")) != null && o instanceof String)
            curDepth = Long.parseLong((String) o);
        if ((o = latest.get("PreviousDepth")) != null && o instanceof String)
            preDepth = Long.parseLong((String) o);
        if ((o = latest.get("InMessages")) != null && o instanceof String)
            inMsgs = Long.parseLong((String) o);
        if ((o = latest.get("OutMessages")) != null && o instanceof String)
            outMsgs = Long.parseLong((String) o);
        if ((o = latest.get("StateLabel")) != null && o instanceof String)
            qStatus = (String) o;
        if (isTopic) {
            if ((o = latest.get("PreviousIpps")) != null && o instanceof String)
                preIpps = Long.parseLong((String) o);
            if ((o = latest.get("IppsCount")) != null && o instanceof String)
                ipps = Long.parseLong((String) o);
        }
        else if (!isQueue)
            if ((o = latest.get("IppsCount")) != null && o instanceof String)
                ipps = Long.parseLong((String) o);

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
            if (isTopic && curDepth > 0 && preIpps <= 0 && ipps <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    if (watermark > 0) {
                        if (curDepth >= watermark && preDepth >= watermark)
                            level = Event.ERR;
                        else
                            level = Event.WARNING;
                    }
                    else
                        level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Topic: application seems not reading from ");
                strBuf.append(qName);
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (isQueue && curDepth > 0 && preDepth > 0 && outMsgs <= 0) {
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    if (watermark > 0) {
                        if (curDepth >= watermark && preDepth >= watermark)
                            level = Event.ERR;
                        else
                            level = Event.WARNING;
                    }
                    else
                        level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Queue: application seems not reading from ");
                strBuf.append(qName);
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (!isQueue && !isTopic && watermark>0 && curDepth>watermark){
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
                strBuf.append("Broker: TopicDBSize is too high for " + qName);
                if (previousQStatus != qStatus)
                    actionCount = 1;
            }
            else if (!previousQStatus.equals(qStatus)) {
                if (isTopic)
                    strBuf.append("Topic: application is OK");
                else if (isQueue)
                    strBuf.append("Queue: application is OK");
                else
                    strBuf.append("Broker: " + qName + " is OK");
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
            if (isQueue || isTopic)
                event.setAttribute("currentDepth", "N/A");
            event.setAttribute("inMessages", "N/A");
            event.setAttribute("outMessages", "N/A");
        }
        else {
            count = actionCount;
            if (isQueue || isTopic)
                event.setAttribute("currentDepth", String.valueOf(curDepth));
            event.setAttribute("inMessages", String.valueOf(inMsgs));
            event.setAttribute("outMessages", String.valueOf(outMsgs));
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("broker", brokerName);
        if (isTopic) {
            event.setAttribute("topic", qName);
            event.setAttribute("subscription_id", subID);
            event.setAttribute("ipps", String.valueOf(ipps));
            event.setAttribute("tStatus", qStatus);
            event.setAttribute("user", connUser);
        }
        else if (isQueue) {
            event.setAttribute("queue", qName);
            event.setAttribute("qStatus", qStatus);
            event.setAttribute("user", connUser);
        }
        else {
            if (status < TimeWindows.BLACKOUT) {
                event.setAttribute("connectionCount", "N/A");
                event.setAttribute("flow2DiskSize", "N/A");
            }
            else {
                event.setAttribute("flow2DiskSize", String.valueOf(curDepth));
                event.setAttribute("connectionCount", String.valueOf(ipps));
            }
            event.setAttribute("bStatus", qStatus);
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

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousQStatus", String.valueOf(previousQStatus));
        chkpt.put("PreviousDepth", String.valueOf(previousDepth));
        chkpt.put("PreviousMsgs", String.valueOf(previousMsgs));
        if (isQueue && cache != null)
            chkpt.put("Cache", Utils.cloneProperties(cache));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct, pDepth, pMsgs;
        int aCount, eCount, pStatus, sNumber;
        String pQStatus;

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

        if ((o = chkpt.get("PreviousMsgs")) != null)
            pMsgs = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousDepth")) != null)
            pDepth = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousQStatus")) != null)
            pQStatus = (String) o;
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDepth = pDepth;
        previousQStatus = pQStatus;
        previousMsgs = pMsgs;

        if (isQueue && (o = chkpt.get("Cache")) != null && o instanceof Map) {
            Map map = (Map) o;
            for (Object obj : map.keySet()) {
                String key = (String) obj;
                if (key != null && key.length() > 0)
                    cache.put(key, (String) map.get(key));
            }
        }
    }

    public void destroy() {
        super.destroy();
        if (cache != null) {
            cache.clear();
            cache = null;
        }
        if (mapList != null) {
            for (Map map : mapList) 
                map.clear();
            mapList.clear();
            mapList = null;
        }
        if (jmxc != null) {
            jmxc.close();
            jmxc = null;
        }
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

            monitor = new SonicMQMonitor(ph);
            Map r = monitor.generateReport(tm);
            String str = (String) r.get("StateLabel");
            if (str != null) {
                Event event = monitor.performAction(0, tm, r);
                if (event != null)
                    event.print(System.out);
                else
                    System.out.println(str + ": " + r.get("CurrentDepth") + " "+
                        r.get("InMessages") + " " + r.get("OutMessages") + " " +
                        r.get("IppsCount") + " " + r.get("OppsCount"));
            }
            else
                System.out.println("failed to get sonicmq stats");
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
        System.out.println("SonicMQMonitor Version 1.0 (written by Yannan Lu)");
        System.out.println("SonicMQMonitor: monitor a SonicMQ queue or topic");
        System.out.println("Usage: java org.qbroker.sonicmq.SonicMQMonitor -I cfg.json");
    }
}
