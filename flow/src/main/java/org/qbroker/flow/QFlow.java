package org.qbroker.flow;

/* QFlow.java - a JMS container hosting message flows */

import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.ReservableCells;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Service;
import org.qbroker.common.RunCommand;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.HTTPServer;
import org.qbroker.net.MessageMailer;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorGroup;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.PropertyMonitor;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.JMSEvent;
import org.qbroker.receiver.MessageReceiver;
import org.qbroker.persister.MessagePersister;
import org.qbroker.persister.StreamPersister;
import org.qbroker.persister.PacketPersister;
import org.qbroker.persister.FilePersister;
import org.qbroker.node.NodeUtils;
import org.qbroker.flow.MessageFlow;
import org.qbroker.flow.MonitorAgent;
import org.qbroker.cluster.ClusterNode;
import org.qbroker.cluster.PacketNode;
import org.qbroker.event.EventAction;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventLogger;
import org.qbroker.event.EventActionGroup;
import org.qbroker.event.Event;

/**
 * QFlow is a light-weight JMS message flow container for various purposes.
 * A message flow consists of a number of message nodes.  There are
 * three types of message nodes, MessageRecever, MessageNode and
 * MessagePersister.  The nodes of MessageReceiver and MessagePersister
 * are for JMSMessage I/O.  The nodes of MessageNode are for JMSMessage
 * processing.  With these three basic types of nodes, you can build various
 * message flows, just like you can build objects with Lego components.
 * Besides, QFlow also provides certain services to all the flows, like
 * clutser, load balance and monitor supports.
 *<br/><br/>
 * QFlow has a special internal queue called escalation.  It is the internal
 * escalation channel shared by all components.  Its name, escalation, is one
 * of the reserved name for XQueues.  In fact, QFlow listens to this queue.
 * It allows all components to escalate events or messages to the container.
 * For a MessageFlow, it can only be used as an outLink by a MessageNode with
 * the proper partition specified explicitly.  Please make sure the container's
 * EscalationCapacity is large enough for it.  By default, it is 64.
 *<br/><br/>
 * QFlow supports the remote control and query synchronously via AdminServer.
 * To enable this feature, you need to define the AdminServer in the master
 * configuration file.  AdminServer is a plugin to QFlow for direct queries
 * or controls.  In fact, its output XQueue is escalation.  If it is defined,
 * the user will be able to query or modify the status of the instance of
 * QFlow directly.
 *<br/><br/>
 * If ClusterNode is defined, QFlow will run in cluster mode.  The
 * ClusterNode will monitor the status of the cluster and will update its
 * own status frequently.  It will also escalate information to the container
 * via the escalation channel.  In the cluster mode, you have to specify the
 * DefauleStatus for the broker so that QFlow will be able to switch its
 * status between SERVICE_RUNNING and the DefaultStatus.  It also supports
 * the failover actions to manage the resources at the failover.
 *<br/><br/>
 * If ConfigRepository is defined, QFlow will support on-demand reload of
 * configuration changes from the repository.  By design, QFlow supports
 * multiple message flows via MessageFlow list.  However, only the legacy
 * configuration with a single flow has been tested.  To make sure it is
 * the legacy configuration, please always set MaxNumberFlow to 1.
 *<br/><br/>
 * It is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QFlow implements Service, Runnable {
    private String name;
    private int heartbeat;
    private int timeout;
    private int maxRetry;
    private int pauseTime;
    private long mtime;
    private Map<String, Object> configRepository = null, includeMap;
    private Map cachedProps = null;
    private XQueue pool = null;       // xq for all reporters
    private XQueue escalation = null; // xq for all escalations
    private XQueue defaultXQ = null;  // root xq for the default flow
    private CollectibleCells reqList; // for tracking remote requests on root
    private ReservableCells idList;   // for protecting all escalations
    private MessageFlow defaultFlow = null;
    private ClusterNode clusterNode = null;
    private MessageReceiver adminServer = null;
    private HTTPServer httpServer = null;
    private java.lang.reflect.Method ackMethod = null;
    private MonitorGroup group;
    private AssetList flowList;
    private long[] groupInfo;
    private EventActionGroup actionGroup = null;
    private int brokerStatus, previousStatus, brokerRole, previousRole;
    private int debug = 0, retry, statusOffset, debugForReload = 0;
    private int[] aPartition = new int[] {0, 0}; // for admin server
    private int[] cPartition = new int[] {0, 0}; // for cluster node
    private int[] rPartition = new int[] {0, 0}; // for remote request by doReq
    private String rcField = "ReturnCode", myURI = null, restartScript = null;
    private String[] displayPropertyName = null;
    private static Map<String, Object> reports = new HashMap<String, Object>();
    private Thread monitor = null, cluster = null, admin = null;
    private File completedFile;
    private DateFormat zonedDateFormat;

    private final static int GROUP_ID = 0;
    private final static int GROUP_TID = 1;
    private final static int GROUP_TYPE = 2;
    private final static int GROUP_SIZE = 3;
    private final static int GROUP_HBEAT = 4;
    private final static int GROUP_TIMEOUT = 5;
    private final static int GROUP_STATUS = 6;
    private final static int GROUP_COUNT = 7;
    private final static int GROUP_SPAN = 8;
    private final static int GROUP_RETRY = 9;
    private final static int GROUP_TIME = 10;
    protected final static String reportStatusText[] = {"Exception",
        "ExceptionInBlackout", "Disabled", "Blackout", "Normal",
        "Occurred", "Late", "VeryLate", "ExtremelyLate"};

    /** Creates new QFlow */
    public QFlow(Map props) {
        this(props, 0, null);
    }

    /** Creates new QFlow */
    @SuppressWarnings("unchecked")
    public QFlow(Map props, int debugForReload, String configFile) {
        Object o;
        String className, key;
        int i, j, k, n, escalationCapacity = 0, flowSize = 32;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("name not defined"));
        name = (String) o;

        if (debugForReload > 0) // in case configRepository get reloaded
            this.debugForReload = debugForReload;

        includeMap = new HashMap<String, Object>();
        mtime = System.currentTimeMillis();
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        // HACK: init GlobalProperties for container
        if ((o = props.get("global_var")) != null && o instanceof Map) {
            Map map = (Map) o;
            if ((o = map.get("ReportName")) != null && o instanceof String &&
                "GlobalProperties".equals((String) o) &&
                (o = map.get("ReportClass")) != null && o instanceof String &&
                this.getClass().getName().equals((String) o)) try {
                new org.qbroker.monitor.StaticReport(map);
            }
            catch (org.qbroker.common.DisabledException e) { // success
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to init global_var: " +
                  Event.traceStack(e)).send();
            }
        }

        // check repository monitor
        if ((o = props.get("ConfigRepository")) != null && o instanceof String){
            Map ph; 
            ph = MessageUtils.substituteProperties((Map) props.get((String) o));
            if (debugForReload <= 0 || configFile == null) // normal case
                configRepository = MonitorGroup.initMonitor(ph, name);
            else { //reset PropertyFile for reload test only
                o = ph.put("PropertyFile", configFile);
                configRepository = MonitorGroup.initMonitor(ph, name);
                ph.put("PropertyFile", o);
            }
            if (configRepository != null) { // reload config from repository
                configRepository.put("Properties", ph);
                if (debugForReload > 0) { // reset debug mode
                    PropertyMonitor pm;
                    pm = (PropertyMonitor) configRepository.get("Report");
                    pm.setDebugMode(debugForReload);
                    if (configFile != null) // save the path for reload
                        configRepository.put("ConfigFile", configFile);
                }
                retry = 0;
                o = getNewProperties(mtime, props);
                retry = 0;
                if (o != null && o instanceof Map) { // with changes
                    props = (Map) o;
                    if ((debug & DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, "Init diff reloaded").send();
                }
            }
            else
                new Event(Event.ERR, name +
                    ": failed to load ConfigRepository").send();
            if (debugForReload > 0) // testing is done
                return;
        }
        else if (debugForReload > 0) {
            new Event(Event.ERR, name +
                ": ConfigRepository is not defined").send();
            return;
        }

        try {
            Class<?> cls = this.getClass();
            ackMethod = cls.getMethod("acknowledge", new Class[]{long[].class});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("found no ack method"));
        }

        if ((o = props.get("MaxNumberFlow")) != null)
            flowSize = Integer.parseInt((String) o);
        if (flowSize <= 0)
            flowSize = 32;

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);

        if (heartbeat <= 0)
            heartbeat = 300000;

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = heartbeat + heartbeat;

        if (timeout < 0)
            timeout = heartbeat + heartbeat;

        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);
        else
            maxRetry = 3;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = props.get("CompletedFile")) != null)
            completedFile = new File((String) o);
        else
            completedFile = null;

        if ((o = props.get("PauseTime")) == null ||
            (pauseTime = Integer.parseInt((String) o)) <= 0)
            pauseTime = 2000;

        // property name for displaying internal messages only
        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            Iterator iter = ((Map) o).keySet().iterator();
            n = ((Map) o).size();
            displayPropertyName = new String[n];
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if ((displayPropertyName[n] =
                    MessageUtils.getPropertyID(key)) == null)
                    displayPropertyName[n] = key;
                n ++;
            }
        }

        Map<String, Object> h = new HashMap<String, Object>();
        h.put("OS_NAME", MonitorUtils.OS_NAME);
        h.put("OS_ARCH", MonitorUtils.OS_ARCH);
        h.put("OS_VERSION", MonitorUtils.OS_VERSION);
        h.put("JAVA_VERSION", MonitorUtils.JAVA_VERSION);
        h.put("USER_NAME", MonitorUtils.USER_NAME);
        h.put("HOSTNAME", Event.getHostName());
        h.put("IP", Event.getIPAddress());
        h.put("PID", String.valueOf(Event.getPID()));
        h.put("HOSTID", Integer.toHexString(Event.getHostID()));
        h.put("PROGRAM", Event.getProgramName());
        h.put("MaxNumberFlow", String.valueOf(flowSize));
        h.put("TestTime", String.valueOf(mtime));
        h.put("TZ", TimeZone.getDefault().getID());
        h.put("Status", String.valueOf(TimeWindows.NORMAL));
        h.put("ReleaseTag", ReleaseTag.getReleaseTag());
        h.put("Name", name);
        reports.put("SYSTEM", h);

        brokerRole = -1;
        previousRole = -1;
        brokerStatus = SERVICE_READY;
        previousStatus = SERVICE_CLOSED;
        statusOffset = 0 - TimeWindows.EXCEPTION;

        if ((o = props.get("EscalationCapacity")) != null)
            escalationCapacity = Integer.parseInt((String) o);
        if (escalationCapacity <= 0)
            escalationCapacity = 64;

        escalation = new IndexedXQueue("escalation", escalationCapacity);

        idList = null;
        if ((o = props.get("AdminServer")) != null) { // for admin server
            Map<String, Object> ph = MessageUtils.substituteProperties((Map) o);
            if ((o = ph.get("Partition")) != null) { // for admin requests
                aPartition = TimeWindows.parseThreshold((String) o);
                aPartition[0] /= 1000;
                aPartition[1] /= 1000;
            }
            else {
                aPartition[0] = 0;
                aPartition[1] = escalationCapacity / 2;
                ph.put("Partition", "0," + aPartition[1]);
            }
            if ((o = ph.get("ClassName")) != null) {
                className = (String) o;
                if (className.length() == 0)
                    throw(new IllegalArgumentException(name +
                        ": ClassName is empty"));

                String str = "failed to instantiate " + className + ":";
                try {
                    java.lang.reflect.Constructor con;
                    Class<?> cls = Class.forName(className);
                    con = cls.getConstructor(new Class[]{Map.class});
                    o = con.newInstance(new Object[]{ph});
                    if (o instanceof MessageReceiver)
                        adminServer = (MessageReceiver) o;
                    else if (o instanceof HTTPServer)
                        httpServer = (HTTPServer) o;
                }
                catch (InvocationTargetException e) {
                    Throwable ex = e.getTargetException();
                    str += e.toString() + " with Target exception: " + "\n";
                    throw(new IllegalArgumentException(str +
                        Event.traceStack(ex)));
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(str +
                        Event.traceStack(e)));
                }

                if (adminServer != null) {
                    admin = new Thread(this, "AdminServer");
                    admin.setPriority(Thread.NORM_PRIORITY);
                    admin.start();
                    if ((o = ph.get("RCField")) != null)
                        rcField = (String) o;
                }
                else if (httpServer == null)
                    throw(new IllegalArgumentException(name +
                        ": ClassName is not supported: " + className));
                else try { // http server
                    httpServer.start(this);
                    idList = new ReservableCells("idList", aPartition[0] +
                        aPartition[1]);
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(
                        "failed to start httpServer: " + Event.traceStack(e)));
                }
            }
            if ((o = ph.get("RestartScript")) != null)
                restartScript = (String) o;
            else
                restartScript = null;
        }
        else { // for external requests
            aPartition[0] = 0;
            aPartition[1] = escalationCapacity / 2;
            restartScript = null;
            idList = new ReservableCells("idList", escalationCapacity/2);
        }

        if ((o = props.get("ClusterNode")) != null) { // for cluster node
            int cid, sessionTimeout;
            long currentTime, previousTime;
            Map ph = MessageUtils.substituteProperties((Map) o);
            if ((o = ph.get("ClassName")) != null) {
                className = (String) o;
                if (className.length() == 0)
                    throw(new IllegalArgumentException("ClassName is empty"));

                String str = "failed to instantiate " + className + ":";
                try {
                    java.lang.reflect.Constructor con;
                    Class<?> cls = Class.forName(className);
                    con = cls.getConstructor(new Class[]{Map.class});
                    clusterNode =(ClusterNode)con.newInstance(new Object[]{ph});
                }
                catch (InvocationTargetException e) {
                    Throwable ex = e.getTargetException();
                    if (ex != null && ex instanceof JMSException) {
                        Exception ee = ((JMSException) ex).getLinkedException();
                        if (ee != null)
                            str += "Linked exception: " + ee.toString() + "\n";
                    }
                    else if (ex != null) {
                        str += e.toString() + " with Target exception: " + "\n";
                    }
                    throw(new IllegalArgumentException(str +
                        Event.traceStack(ex)));
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(str +
                        Event.traceStack(e)));
                }
            }
            else {
                clusterNode = new PacketNode(ph);
            }
            if ((o = ph.get("Partition")) != null) { // for cluster escalations
                cPartition = TimeWindows.parseThreshold((String) o);
                cPartition[0] /= 1000;
                cPartition[1] /= 1000;
            }
            else {
                cPartition[0] = aPartition[0] + aPartition[1];
                cPartition[1] = (escalationCapacity - cPartition[0]) / 2;
            }

            if ((o = ph.get("Resource")) != null && o instanceof List &&
                ((List) o).size() > 0) {
                h = new HashMap<String, Object>();
                h.put("Name", name);
                h.put("Site", props.get("Site"));
                h.put("Category", props.get("Category"));
                h.put("ActionGroup", o);
                actionGroup = new EventActionGroup(h);
                h.clear();
            }
            cluster = new Thread(this, "Cluster");
            cluster.setPriority(Thread.NORM_PRIORITY);
            myURI = clusterNode.getURI();
            cluster.start();

            // join in the cluster first
            sessionTimeout = 2 * clusterNode.getTimeout();
            previousTime = System.currentTimeMillis();
            while ((escalation.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
                currentTime = System.currentTimeMillis();
                if (currentTime - previousTime >= sessionTimeout)
                    break;

                if ((cid = escalation.getNextCell(1000)) >= 0) {
                    Event event;
                    if ((event = (Event) escalation.browse(cid)) == null) {
                        escalation.remove(cid);
                        new Event(Event.WARNING, name +
                            ": null event from escalation channel").send();
                        continue;
                    }
                    escalation.remove(cid);
                    if (!event.attributeExists("role"))
                        continue;
                    brokerRole = Integer.parseInt(event.getAttribute("role"));
                    if (brokerRole >= 0) // sucessfully joined in the cluster
                        break;
                }
            }
            previousRole = brokerRole;
            if (brokerRole < 0) { // timeout
                new Event(Event.ERR, name + ": failed to join cluster").send();
                brokerRole = 0;
            }
        }
        else { // overlap with adminServer for internal reports
            cPartition[0] = aPartition[0];
            cPartition[1] = aPartition[1];
        }

        // HACK: clean up GlobalProperties
        if (reports.containsKey("GlobalProperties"))
            reports.remove("GlobalProperties");

        groupInfo = new long[GROUP_TIME+1];
        for (i=0; i<=GROUP_TIME; i++)
            groupInfo[i] = 0;
        pool = new IndexedXQueue("reporterPool",MonitorGroup.GROUP_CAPACITY);
        h = new HashMap<String, Object>();
        h.put("Name", props.get("Name"));
        if ((o = props.get("Type")) != null)
            h.put("Type", o);
        if ((o = props.get("Heartbeat")) != null)
            h.put("Heartbeat", o);
        if ((o = props.get("Timeout")) != null)
            h.put("Timeout", o);
        if ((o = props.get("MaxRetry")) != null)
            h.put("MaxRetry", o);
        if (debug > 0) {
            k = (DEBUG_INIT & debug) | (DEBUG_DIFF & debug) |
                (DEBUG_LOAD & debug);
            h.put("Debug", String.valueOf(k));
        }
        if ((o = props.get("Reporter")) != null) {
            h.put("Reporter", o);
            k = Utils.copyListProps("Reporter", h, props);
            group = new MonitorGroup(h);
            groupInfo[GROUP_ID] = 0;
            groupInfo[GROUP_TID] = -1;
            groupInfo[GROUP_TYPE] = (group.isDynamic()) ? 1 : 0;
            groupInfo[GROUP_SIZE] = group.getSize();
            groupInfo[GROUP_HBEAT] = group.getHeartbeat();
            groupInfo[GROUP_TIMEOUT] = group.getTimeout();
            groupInfo[GROUP_RETRY] = group.getMaxRetry();
            groupInfo[GROUP_COUNT] = 0;
            groupInfo[GROUP_SPAN] = 0;
            groupInfo[GROUP_STATUS] = SERVICE_READY;
            groupInfo[GROUP_TIME] = mtime;
            if (groupInfo[GROUP_HBEAT] <= 0)
                groupInfo[GROUP_HBEAT] = heartbeat;
            if (groupInfo[GROUP_TIMEOUT] <= 0)
                groupInfo[GROUP_TIMEOUT] = timeout;
            if (groupInfo[GROUP_RETRY] < 0)
                groupInfo[GROUP_RETRY] = maxRetry;
            if (group.getSize() > 0 || group.isDynamic()) {
                initReports(group);
                monitor = new Thread(this, "Reporters");
                monitor.setDaemon(true);
                monitor.setPriority(Thread.NORM_PRIORITY);
            }
            else {
                groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
                group.close();
                group = null;
                monitor = null;
            }
        }
        else {
            groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
            groupInfo[GROUP_TIME] = mtime;
            group = null;
            monitor = null;
        }

        n = 0;
        flowList = new AssetList(name, flowSize);
        if (flowSize == 1) { // for the legacy flow
            Map<String, Object> ph;
            n = 1;
            ph = new HashMap<String, Object>();
            ph.put("Name", "default");
            if ((o = props.get("Type")) != null)
                ph.put("Type", o);
            if ((o = props.get("Capacity")) != null)
                ph.put("Capacity", o);
            if ((o = props.get("Partition")) != null)
                ph.put("Partition", o);
            if ((o = props.get("XAMode")) != null)
                ph.put("XAMode", o);
            if ((o = props.get("Mode")) != null)
                ph.put("Mode", o);
            if ((o = props.get("TakebackEnabled")) != null)
                ph.put("TakebackEnabled", o);
            if ((o = props.get("DefaultStatus")) != null)
                ph.put("DefaultStatus", o);
            if ((o = props.get("ActiveNode")) != null)
                ph.put("ActiveNode", o);
            if ((o = props.get("PassiveNode")) != null)
                ph.put("PassiveNode", o);
            if ((o = props.get("CheckpointDir")) != null)
                ph.put("CheckpointDir", o);
            if ((o = props.get("SaxParser")) != null)
                ph.put("SaxParser", o);
            if ((o = props.get("MaxNumberNode")) != null)
                ph.put("MaxNumberNode", o);
            if ((o = props.get("PauseTime")) != null)
                ph.put("PauseTime", o);
            if ((o = props.get("StandbyTime")) != null)
                ph.put("StandbyTime", o);
            if ((o = props.get("Debug")) != null)
                ph.put("Debug", o);
            if ((o = props.get("Receiver")) != null)
                ph.put("Receiver", o);
            if ((o = props.get("Node")) != null)
                ph.put("Node", o);
            if ((o = props.get("Persister")) != null)
                ph.put("Persister", o);
            key = (String) ph.get("Name");
            k = Utils.copyListProps("Receiver", ph, props);
            k = Utils.copyListProps("Node", ph, props);
            k = Utils.copyListProps("Persister", ph, props);
            k = addFlow(mtime, ph, -1);
            if (k >= 0)
                new Event(Event.INFO, name + " flow " + key +
                    " has been created on " + k).send();
            else
                new Event(Event.ERR, name + " failed to create flow " +
                    key + ": " + k).send();
            k = flowList.size();
            new Event(Event.INFO, name + ": " + k + " out of " + n +
                " flows have been initialized").send();
        }
        else if ((o = props.get("MessageFlow")) != null &&
            o instanceof List) { // for multiple message flows
            Map<String, Object> ph;
            MessageFlow flow = null;
            List list = (List) o;
            n = list.size();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                ph = Utils.cloneProperties((Map) o);
                o = ph.get("Name");
                if (o == null || !(o instanceof String))
                    continue;
                key = (String) o;
                if (key.length() <= 0 || flowList.containsKey(key)) {
                    new Event(Event.ERR, name + ": duplicate flow for " +
                        key).send();
                    ph.clear();
                    continue;
                }
                k = Utils.copyListProps("Receiver", ph, props);
                k = Utils.copyListProps("Node", ph, props);
                k = Utils.copyListProps("Persister", ph, props);
                k = addFlow(mtime, ph, -1);
                if (k >= 0)
                    new Event(Event.INFO, name + " flow " + key +
                        " has been created on " + k).send();
                else
                    new Event(Event.ERR, name + " failed to create flow " +
                        key + ": " + k).send();
            }

            if (n > 0) {
                k = flowList.size();
                new Event(Event.INFO, name + ": " + k + " out of " + n +
                    " flows have been initialized").send();
            }
        }

        reqList = null;
        if (defaultXQ != null) // for tracking requests to the message flow
            reqList = new CollectibleCells("reqCells", defaultXQ.getCapacity());

        previousStatus = SERVICE_READY;
        brokerStatus = SERVICE_RUNNING;

        cachedProps = props;
    }

    /** It initializes a new flow, adds it to the list and returns its id */
    private int addFlow(long tm, Map ph, int id) {
        MessageFlow flow;
        long[] flowInfo;
        String key;
        if (ph == null || ph.size() <= 0)
            return -1;

        key = (String) ph.get("Name");
        try { // message flow defined, instantiate it here
            if ("default".equals(key) && defaultFlow == null) {
                flow = new MessageFlow(ph, escalation);
                defaultFlow = flow;
                defaultXQ = flow.getRootQueue();
                rPartition = flow.getPartition();
            }
            else
                flow = new MessageFlow(ph);
        }
        catch(Exception e) {
            flow = null;
            new Event(Event.ERR, name + " failed to instantiate " +
                "message flow for " + key + ": " + Event.traceStack(e)).send();
            ph.clear();
            return -1;
        }
        flowInfo = new long[GROUP_TIME + 1];
        for (int i=0; i<=GROUP_TIME; i++)
            flowInfo[i] = 0;
        if (id >= 0)
            id = flowList.add(key, flowInfo, flow, id);
        else
            id = flowList.add(key, flowInfo, flow);
        flowInfo[GROUP_ID] = id;
        flowInfo[GROUP_SIZE] = flow.getNumberOfNodes(MessageFlow.MF_RCVR);
        flowInfo[GROUP_COUNT] = flow.getNumberOfNodes(MessageFlow.MF_NODE);
        flowInfo[GROUP_SPAN] = flow.getNumberOfNodes(MessageFlow.MF_PSTR);
        flowInfo[GROUP_STATUS] = flow.getStatus();
        flowInfo[GROUP_TIME] = mtime;

        return id;
    }

    /**
     * It returns a Map of the full properties from the PropertyMonitor.
     * First, it launches the test part of the monitor to check if there is
     * any change.  Then it performs the action to diff the properties and to
     * update the local copies if there is a change.  If nothing changed, it
     * returns null.  Otherwise, the new property Map is returned.
     */
    @SuppressWarnings("unchecked")
    private Map getNewProperties(long currentTime, Map orig) {
        Object o;
        PropertyMonitor pm;
        TimeWindows tw;
        Map props = null;
        Map<String, Object> latest = null;
        Exception ex;
        String configFile = null;
        int status, skippingStatus, skip;

        if (configRepository == null || configRepository.size() <= 0)
            return null;
        pm = (PropertyMonitor) configRepository.get("Report");
        tw = (TimeWindows) configRepository.get("TimeWindow");
        skip = MonitorReport.NOSKIP;

        if (pm == null) { // bad monitor
            new Event(Event.ERR, name +
                ": no configuration repository initialized").send();
            configRepository.clear();
            configRepository = null;
            return null;
        }
        else if (orig != null) { // set the initial properties
            pm.setOriginalProperty(orig);
            includeMap = pm.getIncludeMap();
        }

        ex = null;
        try { // load configuration
            latest = pm.generateReport(currentTime);
        }
        catch (Exception e) {
            latest = new HashMap<String, Object>();
            skip = MonitorReport.EXCEPTION;
            ex = e;
        }

        // skip both check and the action if the skip flag is set
        skippingStatus = pm.getSkippingStatus();
        if (skippingStatus == MonitorReport.SKIPPED ||
            skip == MonitorReport.SKIPPED) {
            return null;
        }

        // exception handling, put exception to latest
        if (skippingStatus != MonitorReport.DISABLED &&
            skip != MonitorReport.DISABLED) {
            status = tw.check(currentTime, 0L);

            if (skippingStatus <= MonitorReport.EXCEPTION ||
                skip <= MonitorReport.EXCEPTION) {
                status = (status != tw.BLACKOUT) ? tw.EXCEPTION :
                    tw.BLACKEXCEPTION;
                if (ex != null)
                    latest.put("Exception", ex);
            }
        }
        else { //disabled
            status = tw.DISABLED;
        }

        // get the new properties
        props = (Map) latest.get("Properties");

        // check and update configuration repository first
        if (props != null) { // got properties
            String operation = null;
            String key = pm.getName();

            if (orig != null) { // HACK: reinit GlobalProperties for container
                Map ph = null;
                if (reports.containsKey("GlobalProperties"))
                    ph = (Map) reports.remove("GlobalProperties");
                if ((o = props.get("global_var")) != null && o instanceof Map) {
                    Map map = (Map) o;
                    if((o=map.get("ReportName"))!=null && o instanceof String &&
                        "GlobalProperties".equals((String) o) &&
                        (o = map.get("ReportClass")) != null &&
                        o instanceof String &&
                        this.getClass().getName().equals((String) o)) try {
                        new org.qbroker.monitor.StaticReport(map);
                    }
                    catch (org.qbroker.common.DisabledException e) { // success
                    }
                    catch (Exception e) {
                        if (ph != null) // rollback
                            reports.put("GlobalProperties", ph);
                        new Event(Event.ERR, name +
                            " failed to init global_var: " +
                            Event.traceStack(e)).send();
                    }
                }
            }

            Map ph = (Map) configRepository.get("Properties");
            o = props.get("ConfigRepository");
            if (o == null) { // repository removed
                pm.destroy();
                configRepository.clear();
                configRepository = null;
                ph.clear();
                operation = "removed";
            }
            else if (!key.equals((String) o)) { // name has changed
                Map<String, Object> c;
                String str = (String) o;
                ph = MessageUtils.substituteProperties((Map) props.get(str));
                configFile = (String) configRepository.get("ConfigFile");
                if (debugForReload <= 0 || configFile == null) // normal case
                    c = MonitorGroup.initMonitor(ph, name);
                else { // reset PropertyFile for test only
                    o = ph.put("PropertyFile", configFile);
                    c = MonitorGroup.initMonitor(ph, name);
                    ph.put("PropertyFile", o);
                }
                if (c != null && (o = c.get("Report")) != null) {
                    pm.destroy();
                    ((Map) configRepository.get("Properties")).clear();
                    configRepository.clear();
                    configRepository = c;
                    pm = (PropertyMonitor) o;
                    configRepository.put("Properties", ph);
                    if (debugForReload > 0) { // reset debug mode
                        pm.setDebugMode(debugForReload);
                        if (configFile != null) // save the path for reload
                            configRepository.put("ConfigFile", configFile);
                    }
                    retry ++;
                    new Event(Event.INFO, name + " Config: " + key +
                        " has been replaced by "+str+" "+retry+" times").send();
                    if (retry < 3)
                        return getNewProperties(currentTime,
                            (orig != null) ? orig : cachedProps);
                }
                else { // failed to init so roll it back
                    props.remove(str);
                    ph.clear();
                    props.put("ConfigRepository", key);
                    props.put(key, orig.get(key));
                    operation = "rolled back";
                }
            }
            else { // name is same
                o = props.get(key);
                if (o == null) {// repository removed
                    pm.destroy();
                    configRepository.clear();
                    configRepository = null;
                    ph.clear();
                    operation = "deleted";
                }
                else if (!Utils.compareProperties(ph, (Map)
                    (o = MessageUtils.substituteProperties((Map) o)))){ //reload
                    Map<String, Object> c;
                    ph = (Map) o;
                    configFile = (String) configRepository.get("ConfigFile");
                    if (debugForReload <= 0 || configFile == null) //normal case
                        c = MonitorGroup.initMonitor(ph, name);
                    else { // reset PropertyFile for test only
                        o = ph.put("PropertyFile", configFile);
                        c = MonitorGroup.initMonitor(ph, name);
                        ph.put("PropertyFile", o);
                    }
                    if (c != null && (o = c.get("Report")) != null) {
                        pm.destroy();
                        ((Map) configRepository.get("Properties")).clear();
                        configRepository.clear();
                        configRepository = c;
                        pm = (PropertyMonitor) o;
                        configRepository.put("Properties", ph);
                        if (debugForReload > 0) { // reset debug mode
                            pm.setDebugMode(debugForReload);
                            if (configFile != null) // save the path for reload
                                configRepository.put("ConfigFile", configFile);
                        }
                        retry ++;
                        new Event(Event.INFO, name + " Config: " + key +
                            " has been reloaded " + retry + " times").send();
                        if (retry < 3)
                            return getNewProperties(currentTime,
                                (orig != null) ? orig : cachedProps);
                    }
                    else { // failed to init so roll it back
                        ph.clear();
                        props.put(key, orig.get(key));
                        operation = "rolled back";
                    }
                }
            }
            if (operation != null) {
                new Event(Event.INFO, name + " Config: " + key +
                    " has been " + operation).send();
            }
        }

        try { // get change and update the local files
            pm.performAction(status, currentTime, latest);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " Exception in: " +
                pm.getName() + ": " + Event.traceStack(e)).send();
            return null;
        }

        // check the difference
        if ((o = latest.get("Properties")) == null || ((Map) o).size() <= 0)
            return null;
        else { // with changes, so return the new properties
            if ((debug & DEBUG_DIFF) > 0) {
                Map change = (Map) o;
                if (orig != null)
                    showChange("Init", change, orig, false);
                else
                    showChange("Reload", change, cachedProps,
                        ((debug & DEBUG_TRAN) > 0));
                change.clear();
            }
            return props;
        }
    }

    /** It loggs the details of the change returned by diff()  */
    private void showChange(String prefix, Map change, Map props,
        boolean detail) {
        Object o;
        Map h;
        String key;
        StringBuffer strBuf = new StringBuffer();
        int n = 0;
        if (change == null)
            return;
        for (Iterator iter=change.keySet().iterator(); iter.hasNext();) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = change.get(key);
            n ++;
            if (o == null)
                strBuf.append("\n\t"+n+": "+ key + " has been removed");
            else if (name.equals(key)) {
                strBuf.append("\n\t"+n+": "+ key + " has changes");
                if(!detail || props == null)
                    continue;
                h = Utils.getMasterProperties(name, includeMap, props);
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
            else if (props == null)
                strBuf.append("\n\t"+n+": "+ key + " seems new");
            else if (!props.containsKey(key))
                strBuf.append("\n\t"+n+": "+ key + " is new");
            else if ((h = (Map) props.get(key)) == null || h.size() <= 0)
                strBuf.append("\n\t"+n+": "+ key + " was empty");
            else if (h.equals((Map) o))
                strBuf.append("\n\t"+n+": "+ key + " has no diff");
            else {
                strBuf.append("\n\t"+n+": "+ key + " has been changed");
                if (!detail)
                    continue;
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
        }
        if (n > 0)
            new Event(Event.DEBUG, prefix +" diff:"+ strBuf.toString()).send();
    }

    /**
     * reloads the new properties
     */
    private int reload(Map props) {
        Object o;
        Map change = null;
        List list;
        Browser browser;
        String key, basename = name;
        long tm;
        int i, k, n, id, size;

        size = props.size();

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);

        if (heartbeat <= 0)
            heartbeat = 300000;

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = heartbeat + heartbeat;

        if (timeout < 0)
            timeout = heartbeat + heartbeat;

        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);
        else
            maxRetry = 3;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = props.get("CompletedFile")) != null)
            completedFile = new File((String) o);
        else
            completedFile = null;

        n = 0;
        tm = System.currentTimeMillis();
        // reload the group
        if ((o = props.get("Reporter")) != null) { // for reporters
            Map<String, Object>ph = new HashMap<String, Object>();
            ph.put("Reporter", o);
            ph.put("Name", props.get("Name"));
            if ((o = props.get("Type")) != null)
                ph.put("Type", o);
            if ((o = props.get("Heartbeat")) != null)
                ph.put("Heartbeat", o);
            if ((o = props.get("Timeout")) != null)
                ph.put("Timeout", o);
            if ((o = props.get("MaxRetry")) != null)
                ph.put("MaxRetry", o);
            if (debug > 0) {
                k = (DEBUG_INIT & debug) | (DEBUG_DIFF & debug) |
                    (DEBUG_LOAD & debug);
                ph.put("Debug", String.valueOf(k));
            }
            k = Utils.copyListProps("Reporter", ph, props);
            if (group == null) { // new group
                group = new MonitorGroup(ph);
                groupInfo = new long[GROUP_TIME+1];
                groupInfo[GROUP_ID] = 0;
                groupInfo[GROUP_TID] = -1;
                groupInfo[GROUP_TYPE] = (group.isDynamic())? 1 : 0;
                groupInfo[GROUP_SIZE] = group.getSize();
                groupInfo[GROUP_HBEAT] = group.getHeartbeat();
                groupInfo[GROUP_TIMEOUT] = group.getTimeout();
                groupInfo[GROUP_RETRY] = group.getMaxRetry();
                groupInfo[GROUP_COUNT] = 0;
                groupInfo[GROUP_SPAN] = 0;
                groupInfo[GROUP_STATUS] = SERVICE_READY;
                groupInfo[GROUP_TIME] = tm;
                if (groupInfo[GROUP_HBEAT] <= 0)
                    groupInfo[GROUP_HBEAT] = heartbeat;
                if (groupInfo[GROUP_TIMEOUT] <= 0)
                    groupInfo[GROUP_TIMEOUT] = timeout;
                if (groupInfo[GROUP_RETRY] < 0)
                    groupInfo[GROUP_RETRY] = maxRetry;

                if (group.getSize() > 0 || group.isDynamic()) {
                    initReports(group);
                    monitor = new Thread(this, "Reporters");
                    monitor.setDaemon(true);
                    monitor.setPriority(Thread.NORM_PRIORITY);
                    monitor.start();
                }
                else { // empty group
                    groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
                    group.close();
                    group = null;
                    monitor = null;
                }
            }
            else { // existing group with changes
                change = group.diff(ph);
                if (change != null && change.size() > 0) {
                    if ((debug & DEBUG_DIFF) > 0)
                        group.showChange("reporter", "Reporter",
                            change, ((debug & DEBUG_TRAN) > 0));
                    stopRunning(pool);
                    pool.clear();
                    if (monitor != null && monitor.isAlive()) {
                        monitor.interrupt();
                        try {
                            monitor.join(2000);
                        }
                        catch (Exception e) {
                            monitor.interrupt();
                        }
                    }
                    resumeRunning(pool);
                    key = group.getName();
                    try {
                        group.reload(change);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name+" failed to reload group "+
                            key + ": " + Event.traceStack(e)).send();
                    }
                    catch (Error e) {
                        new Event(Event.ERR, name+" failed to reload group "+
                            key + ": " + Event.traceStack(e)).send();
                        Event.flush(e);
                    }
                    groupInfo[GROUP_TYPE] = (group.isDynamic()) ? 1 : 0;
                    groupInfo[GROUP_SIZE] = group.getSize();
                    groupInfo[GROUP_HBEAT] = group.getHeartbeat();
                    groupInfo[GROUP_TIMEOUT] = group.getTimeout();
                    groupInfo[GROUP_RETRY] = group.getMaxRetry();
                    groupInfo[GROUP_COUNT] = 0;
                    groupInfo[GROUP_SPAN] = 0;
                    groupInfo[GROUP_STATUS] = SERVICE_READY;
                    groupInfo[GROUP_TIME] = tm;
                    if (groupInfo[GROUP_HBEAT] <= 0)
                        groupInfo[GROUP_HBEAT] = heartbeat;
                    if (groupInfo[GROUP_TIMEOUT] <= 0)
                        groupInfo[GROUP_TIMEOUT] = timeout;
                    if (groupInfo[GROUP_RETRY] < 0)
                        groupInfo[GROUP_RETRY] = maxRetry;

                    if (group.getSize() > 0 || group.isDynamic()) {
                        initReports(group);
                        monitor = new Thread(this, "Reporters");
                        monitor.setDaemon(true);
                        monitor.setPriority(Thread.NORM_PRIORITY);
                        monitor.start();
                    }
                    else { // empty group
                        groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
                        group.close();
                        group = null;
                        monitor = null;
                    }
                }
            }
        }
        else { // no group defined, so close the group
            groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
            if (group != null)
                group.close();
            group = null;
            stopRunning(pool);
            pool.clear();
            if (monitor != null && monitor.isAlive()) {
                monitor.interrupt();
                try {
                    monitor.join(2000);
                }
                catch (Exception e) {
                    monitor.interrupt();
                }
            }
            monitor = null;
            resumeRunning(pool);
        }

        tm = System.currentTimeMillis();
        // reload flows
        if (flowList.getCapacity() == 1) { // for legacy
            MessageFlow flow = null;
            long[] flowInfo;
            Map<String, Object>ph = new HashMap<String, Object>();
            ph.put("Name", "default");
            if ((o = props.get("Type")) != null)
                ph.put("Type", o);
            if ((o = props.get("Capacity")) != null)
                ph.put("Capacity", o);
            if ((o = props.get("Partition")) != null)
                ph.put("Partition", o);
            if ((o = props.get("XAMode")) != null)
                ph.put("XAMode", o);
            if ((o = props.get("Mode")) != null)
                ph.put("Mode", o);
            if ((o = props.get("TakebackEnabled")) != null)
                ph.put("TakebackEnabled", o);
            if ((o = props.get("DefaultStatus")) != null)
                ph.put("DefaultStatus", o);
            if ((o = props.get("ActiveNode")) != null)
                ph.put("ActiveNode", o);
            if ((o = props.get("PassiveNode")) != null)
                ph.put("PassiveNode", o);
            if ((o = props.get("MaxNumberNode")) != null)
                ph.put("MaxNumberNode", o);
            if ((o = props.get("PauseTime")) != null)
                ph.put("PauseTime", o);
            if ((o = props.get("StandbyTime")) != null)
                ph.put("StandbyTime", o);
            if ((o = props.get("Debug")) != null)
                ph.put("Debug", o);
            if ((o = props.get("Receiver")) != null)
                ph.put("Receiver", o);
            if ((o = props.get("Node")) != null)
                ph.put("Node", o);
            if ((o = props.get("Persister")) != null)
                ph.put("Persister", o);
            k = Utils.copyListProps("Receiver", ph, props);
            k = Utils.copyListProps("Node", ph, props);
            k = Utils.copyListProps("Persister", ph, props);
            key = (String) ph.get("Name");
            id = flowList.getID(key);
            flow = (MessageFlow) flowList.get(id);
            flowInfo = flowList.getMetaData(id);
            change = flow.diff(ph);
            if (change == null || change.size() <= 0) // with no change
                flowList.rotate(id);
            else if (flow.hasMajorChange(change)) { // replace the flow
                if ((debug & DEBUG_DIFF) > 0)
                    flow.showChange("flow "+ key, "FLOW", change,
                        ((debug & DEBUG_TRAN) > 0));
                flow.stop();
                flow.close();
                flowList.remove(id);
                if ("default".equals(key)) { // clean up
                    defaultFlow = null;
                    if (defaultXQ != null && (k = defaultXQ.size()) > 0) {
                        new Event(Event.WARNING, name + ": " + k +
                            " msgs has been discarded for replacing " +
                            key).send();
                        defaultXQ.clear();
                    }
                }
                k = addFlow(tm, ph, id);
                if (k >= 0 && (o = flowList.get(k)) != null) {
                    new Event(Event.INFO, name + " flow " + key +
                        " has been replaced on " + id).send();
                    flow = (MessageFlow) o;
                    flowInfo = flowList.getMetaData(k);
                    switch (flow.getDefaultStatus()) {
                      case SERVICE_RUNNING:
                        if (brokerRole > 0) // start worker
                            flow.demote();
                        else // start master or standalone
                            flow.start();
                        break;
                      case SERVICE_PAUSE:
                      case SERVICE_STANDBY:
                      case SERVICE_DISABLED:
                        if (brokerRole == 0) // start master
                            flow.start();
                        else // disable worker or standalone
                            flow.disable();
                        break;
                      case SERVICE_STOPPED:
                        if (brokerRole == 0) // start master
                            flow.promote();
                        else // stop worker or standalone
                            flow.stop();
                        break;
                      default:
                        flow.stop();
                        break;
                    }
                    flowInfo[GROUP_STATUS] = flow.getStatus();
                }
                else
                    new Event(Event.ERR, name + " failed to recreate flow "+
                        key + ": " + k + "/" + id).send();
            }
            else { // reload the flow without node changed
                if ((debug & DEBUG_DIFF) > 0)
                    flow.showChange("flow "+ key, "FLOW", change,
                        ((debug & DEBUG_TRAN) > 0));
                flowList.rotate(id);
                try {
                    flow.reload(change);
                    flowInfo = flowList.getMetaData(id);
                    flowInfo[GROUP_STATUS] = flow.getStatus();
                    flowInfo[GROUP_TIME] = tm;
                    new Event(Event.INFO, name + " flow " + key +
                        " has been reloaded on " + id).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name+" failed to reload flow "+
                        key + ": " + Event.traceStack(e)).send();
                }
                catch (Error e) {
                    new Event(Event.ERR, name+" failed to reload flow "+
                        key + ": " + Event.traceStack(e)).send();
                    Event.flush(e);
                }
            }
        }
        else if ((o = props.get("MessageFlow")) != null &&
            o instanceof List) { // for MessageFlow
            MessageFlow flow = null;
            long[] flowInfo;
            list = (List) o;
            n = list.size();
            Map<String, Object>ph = new HashMap<String, Object>();
            for (i=0; i<n; i++) { // init map for flow names
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                o = ((Map) o).get("Name");
                if (o == null || !(o instanceof String))
                    ph.put((String) o, null);
            }
            browser = flowList.browser();
            while ((id = browser.next()) >= 0) { // delete flows
                key = flowList.getKey(id);
                if (!ph.containsKey(key)) { // remove the flow
                    flow = (MessageFlow) flowList.remove(id);
                    flow.stop();
                    flow.close();
                    if ("default".equals(key)) {
                        defaultFlow = null;
                        if (defaultXQ != null && defaultXQ.size() > 0)
                            defaultXQ.clear();
                    }
                }
            }
            ph.clear();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                ph = Utils.cloneProperties((Map) o);
                k = Utils.copyListProps("Receiver", ph, props);
                k = Utils.copyListProps("Node", ph, props);
                k = Utils.copyListProps("Persister", ph, props);
                key = (String) ph.get("Name");
                if (!flowList.containsKey(key)) { // new flow
                    id = addFlow(tm, ph, -1);
                    if (id >= 0 && (o = flowList.get(id)) != null) {
                        new Event(Event.INFO, name + " flow " + key +
                            " has been created on " + id).send();
                        flow = (MessageFlow) o;
                        flowInfo = flowList.getMetaData(id);
                        switch (flow.getDefaultStatus()) {
                          case SERVICE_RUNNING:
                            if (brokerRole > 0) // start worker
                                flow.demote();
                            else // start master or standalone
                                flow.start();
                            break;
                          case SERVICE_PAUSE:
                          case SERVICE_STANDBY:
                          case SERVICE_DISABLED:
                            if (brokerRole == 0) // start master
                                flow.start();
                            else // disable worker or standalone
                                flow.disable();
                            break;
                          case SERVICE_STOPPED:
                            if (brokerRole == 0) // start master
                                flow.promote();
                            else // stop worker or standalone
                                flow.stop();
                            break;
                          default:
                            flow.stop();
                            break;
                        }
                        flowInfo[GROUP_STATUS] = flow.getStatus();
                    }
                    else
                        new Event(Event.ERR, name + " failed to create flow " +
                            key + ": " + id).send();
                }
                else { // update
                    id = flowList.getID(key);
                    flow = (MessageFlow) flowList.get(id);
                    flowInfo = flowList.getMetaData(id);
                    change = flow.diff(ph);
                    if (change == null || change.size() <= 0) // with no change
                        flowList.rotate(id);
                    else if (flow.hasMajorChange(change)) { // replace the flow
                        if ((debug & DEBUG_DIFF) > 0)
                            flow.showChange("flow "+ key, "FLOW", change,
                                ((debug & DEBUG_TRAN) > 0));
                        flow.stop();
                        flow.close();
                        flowList.remove(id);
                        if ("default".equals(key)) { // clean up
                            defaultFlow = null;
                            if(defaultXQ != null && (k = defaultXQ.size()) > 0){
                                new Event(Event.WARNING, name + ": " + k +
                                    " msgs has been discarded for replacing " +
                                    key).send();
                                defaultXQ.clear();
                            }
                        }
                        k = addFlow(tm, ph, id);
                        if (k >= 0 && (o = flowList.get(k)) != null) {
                            new Event(Event.INFO, name + " flow " + key +
                                " has been replaced on " + id).send();
                            flow = (MessageFlow) o;
                            flowInfo = flowList.getMetaData(k);
                            switch (flow.getDefaultStatus()) {
                              case SERVICE_RUNNING:
                                if (brokerRole > 0) // start worker
                                    flow.demote();
                                else // start master or standalone
                                    flow.start();
                                break;
                              case SERVICE_PAUSE:
                              case SERVICE_STANDBY:
                              case SERVICE_DISABLED:
                                if (brokerRole == 0) // start master
                                    flow.start();
                                else // disable worker or standalone
                                    flow.disable();
                                break;
                              case SERVICE_STOPPED:
                                if (brokerRole == 0) // start master
                                    flow.promote();
                                else // stop worker or standalone
                                    flow.stop();
                                break;
                              default:
                                flow.stop();
                                break;
                            }
                            flowInfo[GROUP_STATUS] = flow.getStatus();
                        }
                        else
                            new Event(Event.ERR, name + " failed to recreate "+
                                "flow " + key + ": " + k + "/" + id).send();
                    }
                    else { // reload the flow without node changed
                        if ((debug & DEBUG_DIFF) > 0)
                            flow.showChange("flow "+ key, "FLOW", change,
                                ((debug & DEBUG_TRAN) > 0));
                        flowList.rotate(id);
                        try {
                            flow.reload(change);
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, name+" failed to reload flow "+
                                key + ": " + Event.traceStack(e)).send();
                            continue;
                        }
                        catch (Error e) {
                            new Event(Event.ERR, name+" failed to reload flow "+
                                key + ": " + Event.traceStack(e)).send();
                            Event.flush(e);
                        }
                        flowInfo = flowList.getMetaData(id);
                        flowInfo[GROUP_STATUS] = flow.getStatus();
                        flowInfo[GROUP_TIME] = tm;
                        new Event(Event.INFO, name + " flow " + key +
                            " has been reloaded on " + id).send();
                    }
                }
            }
        }
        else if (flowList.getCapacity() > 1) { // no flow defined
            MessageFlow flow = null;
            browser = flowList.browser();
            while ((id = browser.next()) >= 0) { // delete flows
                key = flowList.getKey(id);
                flow = (MessageFlow) flowList.remove(id);
                flow.stop();
                flow.close();
                if ("default".equals(key)) {
                    defaultFlow = null;
                    if (defaultXQ != null && defaultXQ.size() > 0)
                        defaultXQ.clear();
                }
            }
        }

        cachedProps.clear();
        cachedProps = props;

        return n;
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return "process";
    }

    public int getStatus() {
        return brokerStatus;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    /**
     * It processes requests according to the preconfigured rulesets.
     * If timeout < 0, there is no wait.  If timeout > 0, it will wait for
     * the processed event until timeout.  If timeout = 0, it blocks until
     * the processed event is done.  It returns 1 for success, 0 for timed
     * out and -1 for failure.  The method is MT-Safe.
     *<br/><br/>
     * A request is either a JMSEvent for remote queries and escalations or a
     * non-JMS Event for local queries and escalations.  In case of remote, the
     * root XQueue of the default flow is used as the message gateway to request
     * the remote objects.  For local queries, the outlink of escalation is used
     * to route the messages.
     *<br/><br/>
     * N.B., The input request will be modified for the response.
     */
    public int doRequest(org.qbroker.common.Event req, int timeout) {
        XQueue xq;
        int i, n, sid = -1, begin, len;
        int m = 1;

        if (req == null)
            return -1;

        Event request = (Event) req;
        if (timeout > 0)
            m = timeout / 500 + 1; 
        else if (timeout == 0)
            m = 100;

        if (request instanceof JMSEvent && httpServer == null) { // for requests
            if (reqList == null)
                return -1;
            xq = defaultXQ;
            begin = rPartition[0];
            len = rPartition[1];

            switch (len) {
              case 0:
                if (timeout < 0) {
                    sid = xq.reserve(500L);
                }
                else for (i=0; i<m; i++) {
                    sid = xq.reserve(500L);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
              case 1:
                if (timeout < 0) {
                    sid = xq.reserve(500L, begin);
                }
                else for (i=0; i<m; i++) {
                    sid = xq.reserve(500L, begin);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
              default:
                if (timeout < 0) {
                    sid = xq.reserve(500L, begin, len);
                }
                else for (i=0; i<m; i++) {
                    sid = xq.reserve(500L, begin, len);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
            }

            if (sid < 0)
                return 0;
            // make sure the cell of sid is empty
            i = reqList.collect(-1L, sid);
            if (i >= 0) // a previous request timed out
                new Event(Event.NOTICE,name+" collected a time-out request at "+
                    sid + "/" + i).send();

            request.setAttribute("reqID", String.valueOf(sid));
            ((JMSEvent) request).setAckObject(this, ackMethod, new long[]{sid});
            i = xq.add(request, sid);

            for (i=0; i<m; i++) { // wait for ack
                if (reqList.collect(500L, sid) >= 0) { // collected it
                    return 1;
                }
            }
            // timed out on collection
            request.setExpiration(System.currentTimeMillis() - 60000L);
        }
        else { // for escalations
            if (idList == null)
                return -1;
            xq = escalation;
            begin = aPartition[0];
            len = aPartition[1];

            switch (len) {
              case 0:
                if (timeout < 0) {
                    sid = idList.reserve(500L);
                }
                else for (i=0; i<m; i++) {
                    sid = idList.reserve(500L);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
              case 1:
                if (timeout < 0) {
                    sid = idList.reserve(500L, begin);
                }
                else for (i=0; i<m; i++) {
                    sid = idList.reserve(500L, begin);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
              default:
                if (timeout < 0) {
                    sid = idList.reserve(500L, begin, len);
                }
                else for (i=0; i<m; i++) {
                    sid = idList.reserve(500L, begin, len);
                    if (sid >= 0 || !keepRunning(xq))
                        break;
                }
                break;
            }

            if (sid < 0)
                return 0;
            i = xq.reserve(50L, sid);
            if (i != sid) {
                idList.cancel(sid);
                return -1;
            }
            request.setAttribute("reqID", String.valueOf(sid));
            i = xq.add(request, sid);

            for (i=0; i<m; i++) {
                if (xq.collect(500L, sid) >= 0) { // try to collect the response
                    idList.cancel(sid);
                    return 1;
                }
            }
            request.setExpiration(System.currentTimeMillis() - 60000L);
            xq.takeback(sid);
            idList.cancel(sid);
        }
        return 0;
    }

    /** acknowledges the request msgs only */
    public void acknowledge(long[] state) throws JMSException {
        if (state != null && state.length > 0 && reqList != null) {
            int i = reqList.take((int) state[0]);
            if (i < 0)
                new Event(Event.ERR, name + " failed to ack a request at "+
                    state[0]).send();
            else if ((debug & (DEBUG_TRAN + DEBUG_TRAN)) > 0)
                new Event(Event.DEBUG, name + " acked a request at " +
                    state[0] + " with " + i + " pending").send();
        }
    }

    public void close() {
        int id;
        MessageFlow flow;
        Browser browser;

        if (pool != null) {
            groupInfo[GROUP_STATUS] = SERVICE_CLOSED;
            stopRunning(pool);
            if (monitor != null && monitor.isAlive())
                monitor.interrupt();
        }

        browser = flowList.browser();
        while ((id = browser.next()) >= 0) {
            flow = (MessageFlow) flowList.get(id);
            flow.stop();
            flow.close();
        }

        previousStatus = brokerStatus;
        brokerStatus = SERVICE_CLOSED;
        if (actionGroup != null) try {
            Event event = new Event(Event.INFO, name + ": resources stopped");
            event.setAttribute("status", "stop");
            actionGroup.invokeAction(System.currentTimeMillis(), event);
            event.send();
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": resources failed to stop: " +
                e.toString()).send();
        }

        stopRunning(escalation);
        if (clusterNode != null) { // stop clusterNode
            clusterNode.close();
        }
        if (adminServer != null) { // stop adminServer
            adminServer.close();
        }
        else if (httpServer != null) try { // stop httpServer
            httpServer.stop();
        }
        catch (Exception e) {
        }

        if (defaultXQ != null && !isStopped(defaultXQ))
            stopRunning(defaultXQ);
    }

    private boolean isStopped(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        if ((xq.getGlobalMask() & mask) > 0)
            return false;
        else
            return true;
    }

    private boolean keepRunning(XQueue xq) {
        if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0)
            return true;
        else
            return false;
    }

    private void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    private void resumeRunning(XQueue xq) {
        int mask = XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.KEEP_RUNNING);
    }

    public void run() {
        String threadName = null;
        String operation;
        String linkName;
        int i;

        threadName = Thread.currentThread().getName();

        if ("shutdown".equals(threadName)) { // for shutdown thread
            shutdown();
        }
        else if ("Manager".equals(threadName)) { // for manager thread
             start();
        }
        else if ("AdminServer".equals(threadName)) { // for admin server
            if (adminServer != null) {
                new Event(Event.INFO, name + ": adminServer started").send();
                adminServer.receive(escalation, 0);
                adminServer.setStatus(MessageReceiver.RCVR_STOPPED);
                new Event(Event.INFO, name + ": adminServer stopped").send();
            }
        }
        else if ("Cluster".equals(threadName)) { // for cluster node
            if (clusterNode != null) {
                new Event(Event.INFO, name + ": cluster node started").send();
                clusterNode.start(escalation, cPartition[0], cPartition[1]);
                new Event(Event.INFO, name + ": cluster node stopped").send();
            }
        }
        else if ("Reporters".equals(threadName) && pool != null) {
            new Event(Event.INFO, name + ": reporter pool started").send();
            for (;;) {
                while (groupInfo[GROUP_STATUS] >= SERVICE_READY &&
                    groupInfo[GROUP_STATUS] < SERVICE_PAUSE) {
                    runJob(pool);
                }

                while (groupInfo[GROUP_STATUS] >= SERVICE_PAUSE &&
                    groupInfo[GROUP_STATUS] < SERVICE_STOPPED) {
                    if (isStopped(pool))
                        break;
                    try {
                        Thread.sleep(pauseTime);
                    }
                    catch (InterruptedException e) {
                    }
                }
                if(isStopped(pool)|| groupInfo[GROUP_STATUS] >= SERVICE_STOPPED)
                    break;
            }
            if (groupInfo[GROUP_STATUS] < SERVICE_STOPPED)
                groupInfo[GROUP_STATUS] = SERVICE_STOPPED;
            new Event(Event.INFO, name + ": reporter pool stopped").send();
        }

        return;
    }

    /**
     * gets a reporter job from XQ and runs it, then updates report
     */
    private void runJob(XQueue xq) {
        Object o;
        int status, sid, skip, skippingStatus;
        long currentTime = 0L, sampleTime;
        Map<String, Object> latest = null;
        Map task;
        String[] keyList;
        MonitorReport report;
        TimeWindows tw;
        long[] taskInfo;
        String name, reportName;
        int reportMode;

        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((sid = xq.getNextCell(2000L)) < 0)
                    continue;

            currentTime = System.currentTimeMillis();
            task = (Map) xq.browse(sid);
            if (task == null) { // task is not supposed to be null
                new Event(Event.ERR, "null task from " + xq.getName()).send();
                xq.remove(sid);
                continue;
            }

            skip = MonitorReport.NOSKIP;
            name = (String)  task.get("Name");
            report = (MonitorReport) task.get("Report");
            reportName = report.getReportName();
            if (reportName == null)
                reportName = name;
            tw = (TimeWindows) task.get("TimeWindow");
            taskInfo = (long[]) task.get("TaskInfo");
            keyList = (String[]) task.get("KeyList");

            reportMode = report.getReportMode();
            if (reportMode == MonitorReport.REPORT_NODE ||
                reportMode == MonitorReport.REPORT_CLUSTER) {
                if (clusterNode != null && (brokerRole != 0 ||
                    brokerStatus != SERVICE_RUNNING)) {
                    // disabled since it is not master or not running
                    xq.remove(sid);
                    continue;
                }
            }

            try {
                latest = report.generateReport(currentTime);
            }
            catch (Exception e) {
                latest = new HashMap<String, Object>();
                skip = MonitorReport.EXCEPTION;
                new Event(Event.ERR, name + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                new Event(Event.ERR, name + " crashed: " + e.toString()).send();
                Event.flush(e);
            }

            if (taskInfo != null) {
                taskInfo[MonitorGroup.TASK_COUNT] ++;
                taskInfo[MonitorGroup.TASK_TIME] = currentTime;
            }

            // check if we need to update shared report
            if(reportMode == MonitorReport.REPORT_SHARED && keyList == null){
                updateReport(reportName, currentTime, tw.check(currentTime, 0L),
                    report.getSkippingStatus());
                xq.remove(sid);
                continue;
            }
            else if (reportMode == MonitorReport.REPORT_LOCAL ||
                reportMode == MonitorReport.REPORT_NONE) {
                xq.remove(sid);
                continue;
            }

            // skip both check and the action if the skip flag is set
            skippingStatus = report.getSkippingStatus();
            if (skippingStatus == MonitorReport.SKIPPED ||
                skip == MonitorReport.SKIPPED) {
                xq.remove(sid);
                continue;
            }

            // exception handling
            if (skippingStatus != MonitorReport.DISABLED &&
                skip != MonitorReport.DISABLED) {
                if (latest != null && (o = latest.get("SampleTime")) != null
                    && o instanceof long[] && ((long[]) o).length > 0)
                    sampleTime = ((long[]) o)[0];
                else
                    sampleTime = 0L;

                status = tw.check(currentTime, sampleTime);
                if (skippingStatus <= MonitorReport.EXCEPTION ||
                    skip <= MonitorReport.EXCEPTION) {
                    status = (status != tw.BLACKOUT) ? tw.EXCEPTION :
                        tw.BLACKEXCEPTION;
                }
            }
            else { //disabled
                status = tw.DISABLED;
            }

            // update  the report even it is DISABLED or EXCEPTION
            latest.put("Status", String.valueOf(status));
            latest.put("SkippingStatus", String.valueOf(skippingStatus));
            latest.put("TestTime", String.valueOf(currentTime));
            updateReport(reportName, keyList, latest, reportMode);

            xq.remove(sid);
        }
    }

    /** updates the existing shared report only */
    @SuppressWarnings("unchecked")
    private void updateReport(String rptName, long currentTime,
        int status, int skippingStatus) {
        Map r;
        if (rptName == null)
            return;

        r = (Map) reports.get(rptName);
        if (r == null)
            return;

        synchronized (r) {
            r.put("Status", String.valueOf(status));
            r.put("SkippingStatus", String.valueOf(skippingStatus));
            r.put("TestTime", String.valueOf(currentTime));
        }
    }

    /** updates the existing shared report and escalates the report */
    @SuppressWarnings("unchecked")
    private void updateReport(String rptName, String[] keyList,
        Map latest, int reportMode) {
        String key;
        Object o;
        if (rptName == null || latest == null)
            return;

        Map r = (Map) reports.get(rptName);
        if (r == null)
            return;

        synchronized (r) { // update shared report
            if (keyList == null || keyList.length == 0) {
                Iterator iter = latest.keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key != null && !"Name".equals(key))
                        r.put(key, latest.get(key));
                }
            }
            else { // has the keyList
                for (int i=0; i<keyList.length; i++) {
                    key = keyList[i];
                    if (key != null && !"Name".equals(key) &&
                        (o = latest.get(key)) != null)
                        r.put(key, o);
                }
                if (latest.containsKey("Status"))
                    r.put("Status", latest.get("Status"));
                if (latest.containsKey("TestTime"))
                    r.put("TestTime", latest.get("TestTime"));
            }
        }

        if (reportMode < MonitorReport.REPORT_NODE) // not an escalation
            return;
        else if (clusterNode == null)
            return;
        if (reportMode == MonitorReport.REPORT_CLUSTER &&
            (clusterNode == null || brokerRole != 0)) // not the master
            return;

        Event event;
        StringBuffer strBuf = new StringBuffer();
        int status = Integer.parseInt((String) latest.get("Status"));
        if (reportMode == MonitorReport.REPORT_NODE &&
            status == TimeWindows.NORMAL) { // for node
            event = new Event(Event.WARNING);
            event.setAttribute("operation", "failover");
            event.setAttribute("master", (String) latest.get("master"));
            event.setAttribute("role", "-1");
            event.setAttribute("size", String.valueOf(clusterNode.getSize()));
            event.setAttribute("status",
                String.valueOf(ClusterNode.NODE_RUNNING));
        }
        else if (reportMode == MonitorReport.REPORT_CLUSTER) { // for cluster
            event = new Event(Event.INFO);
            event.setAttribute("operation", "report");
        }
        else {
            return;
        }
        event.setAttribute("uri", myURI);
        event.setAttribute("name", rptName);
        strBuf.append("Report( ~ ): name ~ " + rptName);

        if (keyList == null || keyList.length == 0) { // default content
            Iterator iter = latest.keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                o = latest.get(key);
                if ("SampleTime".equals(key)) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    key = Event.dateFormat(new Date(((long[]) o)[0]));
                    strBuf.append(key);
                }
                else if ("SampleSize".equals(key)) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(((long[]) o)[0]);
                }
                else if (key != null && o != null && o instanceof String) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    key = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                        Event.ESCAPED_ITEM_SEPARATOR, (String) o);
                    strBuf.append(key);
                }
            }
        }
        else { // customized content
            for (int i=0; i<keyList.length; i++) {
                key = keyList[i];
                o = latest.get(key);
                if ("SampleTime".equals(key)) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    key = Event.dateFormat(new Date(((long[]) o)[0]));
                    strBuf.append(key);
                }
                else if ("SampleSize".equals(key)) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(((long[]) o)[0]);
                }
                else if (key != null && o != null && o instanceof String) {
                    strBuf.append(Event.ITEM_SEPARATOR);
                    strBuf.append(key);
                    strBuf.append(Event.ITEM_SEPARATOR);
                    key = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                        Event.ESCAPED_ITEM_SEPARATOR, (String) o);
                    strBuf.append(key);
                }
            }
        }
        event.setAttribute("text", strBuf.toString());

        if (reportMode == MonitorReport.REPORT_NODE) {
            int cid = -1, e0 = cPartition[0] + cPartition[1];
            if (e0 > 0)
                cid = escalation.reserve(1000, e0, escalation.getCapacity()-e0);
            else
                cid = escalation.reserve(1000);
            if (cid >= 0)
                escalation.add(event, cid);
        }
        else
            clusterNode.relay(event, 500);

        if ((debug & DEBUG_REPT) > 0)
            new Event(Event.DEBUG, "report: " + strBuf.toString()).send();
    }

    /** initializes reports for all occurrences in a group */
    private int initReports(MonitorGroup group) {
        XQueue xq;
        int k, n;

        if (group == null || ((n = group.getSize()) <= 0 && !group.isDynamic()))
            return 0;

        xq = new IndexedXQueue(name, n);
        n = 0;
        group.dispatch(xq);
        if (xq != null && xq.size() > 0) {
            Object o;
            String rptName;
            MonitorReport report;
            Browser browser = xq.browser();
            while ((k = browser.next()) >= 0) { // init report
                o = xq.browse(k);
                if (o == null || !(o instanceof Map))
                    continue;
                report = (MonitorReport) ((Map) o).get("Report");
                if (report == null)
                    continue;
                if (report.getReportMode() < MonitorReport.REPORT_SHARED)
                    continue;
                rptName = report.getReportName();
                if (rptName == null)
                    rptName = (String) ((Map) o).get("Name");
                if (rptName != null && !reports.containsKey(rptName)) {
                    Map<String, Object> r = new HashMap<String, Object>();
                    r.put("Name", rptName);
                    reports.put(rptName, r);
                    n ++;
                }
            }
            xq.clear();
        }
        return n;
    }

    /**
     * It is used for reporter to initialize the static report
     */
    public synchronized static void initReport(String rptName, String[] keyList,
        Map latest, int reportMode) {
        String key;
        Object o;
        if (rptName == null || latest == null || reports.containsKey(rptName))
            return;

        Map<String, Object> r = Utils.cloneProperties(latest);
        r.put("Name", rptName);
        if (keyList == null || keyList.length == 0) {
            Iterator iter = latest.keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key != null && !"Name".equals(key))
                    r.put(key, latest.get(key));
            }
        }
        else { // has the keyList
            for (int i=0; i<keyList.length; i++) {
                key = keyList[i];
                if (key != null && !"Name".equals(key) &&
                    (o = latest.get(key)) != null)
                    r.put(key, o);
            }
            if (latest.containsKey("Status"))
                r.put("Status", latest.get("Status"));
            if (latest.containsKey("TestTime"))
                r.put("TestTime", latest.get("TestTime"));
        }
        reports.put(rptName, r);
    }

    /**
     * returns a copy of the report
     */
    public static Map<String, Object> getReport(String rptName) {
        Map r = (Map) reports.get(rptName);
        if (r == null)
            return null;
        else synchronized (r) {
            return Utils.cloneProperties(r);
        }
    }

    /**
     * returns all the names of the reports
     */
    public static String[] getReportNames() {
        return reports.keySet().toArray(new String[reports.size()]);
    }

    private void touch(File file) {
        if (file.exists()) {
            file.setLastModified(System.currentTimeMillis());
        }
        else {
            try {
                file.createNewFile();
            }
            catch (IOException e) {
            }
        }
    }

    /**
     * disables the id-th flow and returns its previous status
     */
    private int disable(long currentTime, int id) {
        MessageFlow flow = (MessageFlow) flowList.get(id);
        long[] flowInfo = flowList.getMetaData(id);

        if (flow == null)
            return -1;

        int i = flow.getStatus();
        flow.disable();
        flowInfo[GROUP_STATUS] = flow.getStatus();
        flowInfo[GROUP_TIME] = currentTime;

        return i;
    }

    /**
     * enables the id-th flow and returns the previous status
     */
    private int enable(long currentTime, int id) {
        MessageFlow flow = (MessageFlow) flowList.get(id);
        long[] flowInfo = flowList.getMetaData(id);

        if (flow == null)
            return -1;

        int i = flow.getStatus();
        flow.enable();
        flowInfo[GROUP_STATUS] = flow.getStatus();
        flowInfo[GROUP_TIME] = currentTime;

        return i;
    }

    /**
     * promotes the id-th flow to the running status and returns the previous
     * status
     */
    private int promote(long currentTime, int id) {
        MessageFlow flow = (MessageFlow) flowList.get(id);
        long[] flowInfo = flowList.getMetaData(id);

        if (flow == null)
            return -1;

        int i = flow.getStatus();
        flow.promote();
        flowInfo[GROUP_STATUS] = flow.getStatus();
        flowInfo[GROUP_TIME] = currentTime;

        return i;
    }

    /**
     * demotes the id-th flow to its default status and returns the previous
     * status
     */
    private int demote(long currentTime, int id) {
        MessageFlow flow = (MessageFlow) flowList.get(id);
        long[] flowInfo = flowList.getMetaData(id);

        if (flow == null)
            return -1;

        int i = flow.getStatus();
        flow.demote();
        flowInfo[GROUP_STATUS] = flow.getStatus();
        flowInfo[GROUP_TIME] = currentTime;

        return i;
    }

    private Map<String, String> loadGroupInfo(String key, long[] info) {
        Map<String, String> h;
        String str;
        if (info == null || info.length != GROUP_TIME + 1)
            return null;

        h = new HashMap<String, String>();
        h.put("Name", key);
        h.put("Size", String.valueOf(info[GROUP_SIZE]));
        h.put("Heartbeat", String.valueOf(info[GROUP_HBEAT]/1000));
        h.put("Timeout", String.valueOf(info[GROUP_TIMEOUT]/1000));
        h.put("Count", String.valueOf(info[GROUP_COUNT]));
        h.put("Retry", String.valueOf(info[GROUP_RETRY]));
        str = statusText[((int) info[GROUP_STATUS] + statusText.length) % 
            statusText.length];
        h.put("Status", str);
        h.put("Span", String.valueOf(info[GROUP_SPAN]));
        str = Event.dateFormat(new Date(info[GROUP_TIME]));
        h.put("Time", str);

        return h;
    }

    /**
     * It invokes the predefined actions upon the incoming event and returns
     * the current role of the container which may get changed as the result
     * of the actions.
     *<br/><br/>
     * There are two types of incoming events, TextEvent and non-JMS Event.
     * TextEvent is an external JMS message either delivered by AdminServer or
     * escalated from the default message flow.  Non-JMS Event is escalated
     * from either certain monitors or the cluster node.  The incoming events
     * are categorized according to their priorities.  WARNING is for alerts
     * from the container or flows, and operational actions such as restart,
     * reload and failover, etc.  NOTICE is for notifications such as state
     * change and status update, etc.  INFO is for queries of state and
     * escalations of event, report and data, etc.
     *<br/><br/>
     * The following attributes are required for each incoming event:
     *<br/>
     * priority     INFO for query, NOTICE for update and WARNING for admin<br/>
     * operation    action of the request: query,reload,stop,disable,enable<br/>
     * category     category of the target<br/>
     * name         name the target<br/>
     * type         type the response content: json, xml or text<br/>
     * status       status
     */
    private int invokeAction(long currentTime, long sessionTime, Event event) {
        String uri, data, target, key, str, text = null;
        Event ev = null;
        long tm;
        Object o;
        MessageFlow flow = null;
        int i, n, role = -1, size = -1, sid, id = -2, status = -1, action, type;
        boolean isExternal, isRemote, isFlow;
        if (event == null)
            return brokerRole;

        isRemote = (event instanceof TextEvent);
        isExternal = (isRemote) ? true : false;
        if ((str = event.getAttribute("reqID")) != null) {
            try {
                sid = Integer.parseInt(str);
                if (sid >= 0)
                    isExternal = true;
            }
            catch (Exception e) {
            }
        }

        tm = event.getTimestamp();
        uri = event.getAttribute("uri");
        if ((o = event.getAttribute("role")) != null)
            role = Integer.parseInt((String) o);
        if ((o = event.getAttribute("size")) != null)
            size = Integer.parseInt((String) o);

        key = event.getAttribute("name");
        target = event.getAttribute("category");
        isFlow = ("FLOW".equals(target));

        if ((o = event.getAttribute("status")) != null)
            status = (isExternal) ? -1 : Integer.parseInt((String) o);

        if ((str = event.getAttribute("operation")) != null && str.length()>0) {
            action = -2;
            switch (str.charAt(0)) {
              case 's':
              case 'S':
                if ("stop".equalsIgnoreCase(str))
                    action = EventAction.ACTION_STOP;
                else if ("standby".equalsIgnoreCase(str))
                    action = EventAction.ACTION_STANDBY;
                break;
              case 'q':
              case 'Q':
                if ("query".equalsIgnoreCase(str))
                    action = EventAction.ACTION_QUERY;
                break;
              case 'l':
              case 'L':
                if ("list".equalsIgnoreCase(str))
                    action = EventAction.ACTION_LIST;
                break;
              case 'd':
              case 'D':
                if ("disable".equalsIgnoreCase(str))
                    action = EventAction.ACTION_DISABLE;
                else if ("deploy".equalsIgnoreCase(str))
                    action = EventAction.ACTION_DEPLOY;
                break;
              case 'e':
              case 'E':
                if ("enable".equalsIgnoreCase(str))
                    action = EventAction.ACTION_ENABLE;
                break;
              case 'u':
              case 'U':
                if ("update".equalsIgnoreCase(str))
                    action = EventAction.ACTION_UPDATE;
                else if ("undeploy".equalsIgnoreCase(str))
                    action = EventAction.ACTION_UNDEPLOY;
                break;
              case 'c':
              case 'C':
                if ("create".equalsIgnoreCase(str))
                    action = EventAction.ACTION_CREATE;
                break;
              case 'r':
              case 'R':
                if ("rename".equalsIgnoreCase(str))
                    action = EventAction.ACTION_RENAME;
                else if ("remove".equalsIgnoreCase(str))
                    action = EventAction.ACTION_REMOVE;
                else if ("restart".equalsIgnoreCase(str))
                    action = EventAction.ACTION_RESTART;
                else if ("reload".equalsIgnoreCase(str))
                    action = EventAction.ACTION_RELOAD;
                break;
              case 'f':
              case 'F':
                if ("failover".equalsIgnoreCase(str))
                    action = EventAction.ACTION_FAILOVER;
                break;
              case 'b':
              case 'B':
                if ("balance".equalsIgnoreCase(str))
                    action = EventAction.ACTION_BALANCE;
                break;
              default:
            }
        }
        else
            action = -1;

        type = Utils.RESULT_TEXT;
        if ((str = event.getAttribute("type")) != null) {
            if ("xml".equalsIgnoreCase(str)) // expecting XML as response
                type = Utils.RESULT_XML;
            else if ("json".equalsIgnoreCase(str)) // expecting JSON as response
                type = Utils.RESULT_JSON;
        }

        switch (event.getPriority()) {
          case Event.WARNING: // application event or node alert
            if ((debug & DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " got a message: " +
                    EventUtils.compact(event)).send();

            if (isFlow) // for FLOW
                id = flowList.getID(key);
            else if (name.equals(key)) // for QB
                id = -1;

            if (isExternal) switch (action) {
              case EventAction.ACTION_RESTART:
                if (isFlow) { // for flow
                    if (id >= 0) {
                        flow = (MessageFlow) flowList.get(id);
                        status = flow.getStatus();
                        if (status <= SERVICE_STOPPED) {
                            text = "manually restarted";
                            groupInfo = flowList.getMetaData(id);
                            flow.stop();
                            flow.start();
                            groupInfo[GROUP_STATUS] = flow.getStatus();
                            new Event(Event.INFO, name + ": flow '" + key +
                                "' " + text + " from " +
                                statusText[status]).send();
                            text = key + " " + text;
                        }
                        else {
                            text = key + " already closed";
                        }
                    }
                    else { // no such flow
                        text = "no such flow for " + key;
                    }
                    if (isRemote) {
                        ev = new Event(Event.INFO, text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", text);
                        event.setAttribute("rc", "0");
                    }
                    role = brokerRole;
                    break;
                }
                // for container
                if (restartScript == null) {
                    text = "restart script not defined";
                    if (isRemote) {
                        ev = new Event(Event.WARNING, target + ": " + text);
                        ev.setAttribute("rc", "-1");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.WARNING);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "-1");
                    }
                    break;
                }
                status = ClusterNode.NODE_DOWN;
                try {
                    text = RunCommand.exec(restartScript, 120000);
                }
                catch (Exception e) {
                    text = "restart script failed: " + e.toString();
                    if (isRemote) {
                        ev = new Event(Event.WARNING, target + ": " + text);
                        ev.setAttribute("rc", "-1");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.WARNING);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "-1");
                    }
                    break;
                }
                // should not reach here if restart script works
                if (isRemote) {
                    ev = new Event(Event.WARNING, target+" restarting: "+text);
                    ev.setAttribute("rc", "-1");
                }
                else if (isExternal) { // external event
                    event.setPriority(Event.WARNING);
                    event.setAttribute("text", target + " restarting: " + text);
                    event.setAttribute("rc", "-1");
                }
                break;
              case EventAction.ACTION_STOP:
                if (isFlow) { // for flow
                    if (id >= 0) {
                        flow = (MessageFlow) flowList.get(id);
                        status = flow.getStatus();
                        if (status < SERVICE_STOPPED) {
                            text = "manually stopped";
                            groupInfo = flowList.getMetaData(id);
                            flow.stop();
                            groupInfo[GROUP_STATUS] = flow.getStatus();
                            new Event(Event.INFO, name + ": flow '" + key +
                                "' " + text + " from " +
                                statusText[status]).send();
                            text = key + " " + text;
                        }
                        else {
                            text = key + " already stopped";
                        }
                    }
                    else { // no such flow
                        text = "no such flow for " + key;
                    }
                    if (isRemote) {
                        ev = new Event(Event.INFO, text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", text);
                        event.setAttribute("rc", "0");
                    }
                    role = brokerRole;
                    break;
                }
                // for container
                text = "stopping";
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else {
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "0");
                }
                status = ClusterNode.NODE_DOWN;
                break;
              case EventAction.ACTION_FAILOVER:
                if (isFlow) { // for flow
                    text = "failover on " + key + " is not supported";
                    if (isRemote) {
                        ev = new Event(Event.INFO, text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", text);
                        event.setAttribute("rc", "0");
                    }
                    role = brokerRole;
                }
                else if (clusterNode != null) { // for container
                    size = clusterNode.getSize();
                    if (brokerRole == 0) { // this is master
                        if (size > 1)
                            text = "failing over";
                        else { // abort failover
                            text = "nowhere to failover";
                            role = brokerRole;
                        }
                    }
                    else { // not the master so forward over
                        text = "Report( ~ ): name ~ master";
                        text += " ~ masterURI ~ " + myURI;
                        text += " ~ operation ~ failover";
                        event.setAttribute("text", text);
                        text = "failover request forwarded";
                    }
                    if (isRemote) {
                        ev = new Event(Event.INFO, target + ": " + text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (brokerRole == 0) {
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "0");
                    }
                    status = ClusterNode.NODE_RUNNING;
                }
                else {
                    text = "not a cluster";
                    if (isRemote) {
                        ev = new Event(Event.WARNING, target + ": " + text);
                        ev.setAttribute("rc", "-1");
                    }
                    else {
                        event.setPriority(Event.WARNING);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "-1");
                    }
                }
                break;
              case EventAction.ACTION_ENABLE:
                status = -1;
                if (isFlow) { // for flow
                    if (id >= 0) {
                        flow = (MessageFlow) flowList.get(id);
                        status = flow.getStatus();
                        if (status >= SERVICE_READY && status<=SERVICE_RETRYING)
                            text = key + " already enabled";
                        else if (status <= SERVICE_DISABLED) {
                            text = "manually enabled";
                            groupInfo = flowList.getMetaData(id);
                            groupInfo[GROUP_STATUS] = flow.enable();
                            new Event(Event.INFO, name + ": flow '" + key +
                                "' " + text + " from " +
                                statusText[status]).send();
                            text = key + " " + text;
                        }
                        else {
                            text = key + " stopped or closed";
                        }
                    }
                    else { // no such flow
                        text = "no such flow for " + key;
                    } 
                    role = brokerRole;
                } 
                else if (defaultFlow != null) { // for default flow
                    i = defaultFlow.getStatus();
                    if (i >= SERVICE_READY && i <= SERVICE_RETRYING) {
                        text = "already enabled";
                    }
                    else if (i <= SERVICE_DISABLED) {
                        text = "manually enabled";
                        status = ClusterNode.NODE_STANDBY;
                    }
                    else {
                        text = "failed since it is stopped or closed";
                    }
                }
                else
                    text = "no default flow";
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text +
                        " for " + key);
                    ev.setAttribute("rc", "0");
                }
                else {
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text +
                        " for " + key);
                    event.setAttribute("rc", "0");
                }
                break;
              case EventAction.ACTION_STANDBY:
              case EventAction.ACTION_DISABLE:
                status = -1;
                if (isFlow) { // for flow
                    if (id >= 0) {
                        flow = (MessageFlow) flowList.get(id);
                        status = flow.getStatus();
                        if (status >= SERVICE_READY && status<SERVICE_DISABLED){
                            text = "manually disabled";
                            groupInfo = flowList.getMetaData(id);
                            groupInfo[GROUP_STATUS] = flow.disable();
                            new Event(Event.INFO, name + ": flow '" + key +
                                "' " + text + " from " +
                                statusText[status]).send();
                            text = key + " " + text;
                        }
                        else if (status == SERVICE_DISABLED) {
                            text = key + " already enabled";
                        }
                        else {
                            text = key + " stopped or closed";
                        }
                    }
                    else if (isFlow) { // no such flow
                        text = "no such flow for " + key;
                    }
                    role = brokerRole;
                }
                else if (defaultFlow != null) {
                    i = defaultFlow.getStatus();
                    if (i >= SERVICE_READY && i < SERVICE_DISABLED) {
                        text = "manually disabled";
                        status = ClusterNode.NODE_STANDBY;
                    }
                    else if (i == SERVICE_DISABLED) {
                        text = "already disabled";
                    }
                    else {
                        text = "not necessary since it is stopped or closed";
                    }
                }
                else
                    text = "no default flow";
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else {
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "0");
                }
                break;
              case EventAction.ACTION_RELOAD:
                if ("RULE".equals(target)) { // refresh the rule
                    if (defaultFlow != null) {
                        i = defaultFlow.refreshRules(key);
                        if (i == 0)
                            text = "no changes to refresh for " + key;
                        else if (i == -1)
                            text = "no rule found for " + key;
                        else if (i == -2)
                            text = "failed to refresh the rule " + str;
                        else
                            text = i + " external rules for "+key+" refreshed";
                    }
                    else
                        text = "no default flow";
                }
                else if (configRepository != null) { // repository defined
                    Map props;
                    retry = 0;
                    props = getNewProperties(tm, null);
                    retry = 0;
                    if (props != null && props.size() > 0) {
                        reload(props);
                        props = null;
                        text = "changes have been reloaded";
                    }
                    else {
                        text = "no changes to reload";
                    }
                }
                else
                    text = "repository is not defined";
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else {
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "0");
                }
                break;
              default:
                text = "bad request";
                if (isRemote) {
                    ev = new Event(Event.WARNING, target + ": " + text);
                    ev.setAttribute("rc", "-1");
                }
                else {
                    event.setPriority(Event.WARNING);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "-1");
                }
                break;
            }
            if (!isFlow) switch (status) { // take actions
              case ClusterNode.NODE_RUNNING:
                if (role != 0 && brokerRole == 0 && size > 1) { // demoted
                    i = demote(currentTime, 0);
                    mtime = currentTime;
                    clusterNode.relay(event, 500);
                    new Event(Event.INFO, name + " demoted to " + role +
                        " from " + previousRole).send();
                }
                else if (role == 0 && brokerRole == 0) { // no role change
                    role = brokerRole;
                }
                else if (isExternal && brokerRole > 0) { // forward to master
                    clusterNode.relay(event, 500);
                    role = brokerRole;
                    if (isRemote) {
                        event.setAttribute("text", " ");
                    }
                    else { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "0");
                    }
                }
                break;
              case ClusterNode.NODE_STANDBY:
                if (action == EventAction.ACTION_DISABLE) { // disable flow
                    i = disable(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name+": flow default "+text+" from "+
                        ((i>=0) ? statusText[i] : String.valueOf(i))).send();
                }
                else if (action == EventAction.ACTION_STANDBY) { // disable flow
                    i = disable(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name+": flow default "+text+" from "+
                        ((i>=0) ? statusText[i] : String.valueOf(i))).send();
                }
                else if (action == EventAction.ACTION_ENABLE) { // enable flow
                    i = enable(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name+": flow default "+text+" from "+
                        ((i>=0) ? statusText[i] : String.valueOf(i))).send();
                }
                break;
              case ClusterNode.NODE_NEW:
              case ClusterNode.NODE_DOWN:
                close();
                brokerStatus = SERVICE_CLOSED;
                break;
              default:
                role = brokerRole;
                break;
            }
            break;
          case Event.NOTICE: // node events for state changes
            if (isFlow && (str = event.getAttribute("status")) != null) {
                if ("disabled".equalsIgnoreCase(str))
                    status = SERVICE_DISABLED;
                else if ("normal".equalsIgnoreCase(str))
                    status = SERVICE_RUNNING;
            }

            if ((debug & DEBUG_CTRL) > 0 && (debug & DEBUG_UPDT) > 0)
                new Event(Event.DEBUG, name + " got a message: " +
                    EventUtils.compact(event)).send();

            if (isFlow) switch (status) { // for flow
              case SERVICE_RUNNING:
                role = brokerRole;
                if ((id = flowList.getID(key)) >= 0) {
                    flow = (MessageFlow) flowList.get(id);
                    status = flow.getStatus();
                    if(status > SERVICE_RETRYING && status <= SERVICE_DISABLED){
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.enable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' enabled from " + statusText[status]).send();
                    }
                }
                break;
              case SERVICE_STANDBY:
              case SERVICE_DISABLED:
                role = brokerRole;
                if ((id = flowList.getID(key)) >= 0) {
                    flow = (MessageFlow) flowList.get(id);
                    status = flow.getStatus();
                    if (status >= SERVICE_READY && status < SERVICE_DISABLED) {
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.disable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' disabled from " + statusText[status]).send();
                    }
                }
                break;
              default:
                role = brokerRole;
                break;
            }
            else if (uri.equals(myURI)) switch (status) { // for container
              case ClusterNode.NODE_RUNNING:
                if (role == 0 && brokerRole != 0) { // promoted to master
                    i = promote(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name + " promoted to " + role +
                        " from " + brokerRole).send();
                }
                else if (role != 0 && brokerRole == 0) { // demoted to monitor
                    i = demote(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name + " demoted to " + role +
                        " from " + brokerRole).send();
                }
                else if (role == 0 && brokerRole == 0) { // no role change
                    role = brokerRole;
                }
                break;
              case ClusterNode.NODE_STANDBY:
              case ClusterNode.NODE_TIMEOUT:
                if (brokerStatus == SERVICE_RUNNING &&
                    brokerRole == 0 && role > 0) { // master resigned
                    i = demote(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name + " demoted to " + role +
                        " from " + brokerRole).send();
                }
                break;
              case ClusterNode.NODE_NEW:
                if (role < 0 && brokerRole == 0) { // reinitialize
                    i = demote(currentTime, 0);
                    mtime = currentTime;
                    new Event(Event.INFO, name + " demoted to " + role +
                        " from " + brokerRole).send();
                }
                break;
              case ClusterNode.NODE_DOWN:
                close();
                brokerStatus = SERVICE_CLOSED;
                break;
              default:
                break;
            }
            else { // update for other node
                role = brokerRole;
            }
            break;
          case Event.INFO: // report data from node or external query 
            role = brokerRole;
            if (isExternal) { // external query or flow escalation
                if ("CLUSTER".equals(target) && clusterNode != null) {
                    // for relaying event to cluster
                    if ((debug & DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name + " got an escalation: " +
                            EventUtils.compact(event)).send();
                    StringBuffer strBuf = new StringBuffer();
                    Iterator iter = event.getAttributeNames();
                    while (iter.hasNext()) { // format into a text
                        key = (String) iter.next();
                        if (key == null || key.length() <= 0)
                            continue;
                        if ("name".equals(key))
                            continue;
                        strBuf.append(Event.ITEM_SEPARATOR);
                        strBuf.append(key);
                        strBuf.append(Event.ITEM_SEPARATOR);
                        key = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                            Event.ESCAPED_ITEM_SEPARATOR,
                            event.getAttribute(key));
                        strBuf.append(key);
                    }
                    key = "Event(" + Event.ITEM_SEPARATOR + "): name" +
                        Event.ITEM_SEPARATOR + event.getAttribute("name");
                    event.setAttribute("text", key + strBuf.toString());
                    clusterNode.relay(event, 500);
                    return role;
                }

                // for queries
                if ((debug & DEBUG_UPDT) > 0 && (debug & DEBUG_TRAN) > 0)
                    new Event(Event.DEBUG, name + " got a request: " +
                        EventUtils.compact(event)).send();
                if("HELP".equals(target) || "USAGE".equals(target) || key==null)
                    key = target;
                Map h = queryInfo(currentTime, sessionTime, target, key, type);

                if (h != null) { // got response
                    if ((type & Utils.RESULT_JSON) > 0) { // json
                        if (h.size() == 1) // for list of records
                            text = "{\"Record\": " +(String)h.get(target)+ "}";
                        else // for a map
                            text = JSON2Map.toJSON(h);
                        ev = new Event(Event.INFO, text);
                        h.clear();
                    }
                    else if ((type & Utils.RESULT_XML) > 0) { // xml
                        if (h.size() == 1) // for list of records
                            text = (String) h.get(target);
                        else // for a map
                            text = JSON2Map.toXML(h);
                        ev = new Event(Event.INFO, "<Result>" + text +
                            "</Result>");
                        h.clear();
                    }
                    else if (isRemote) {
                        ev = new Event(Event.INFO, target + ": query OK");
                        ev.setAttribute("rc", "0");
                        Iterator iter = h.keySet().iterator();
                        while (iter.hasNext()) {
                            str = (String) iter.next();
                            o = h.get(str);
                            if (o == null || !(o instanceof String))
                                continue;
                            ev.setAttribute(str, (String) o);
                        }
                        h.clear();
                    }
                    else { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", target + ": query OK");
                        event.setAttribute("rc", "0");
                        event.setBody(h);
                    }
                }
                else if ((type & Utils.RESULT_JSON) > 0) { // failed to get json
                    ev = new Event(Event.ERR, "{\"Error\":\"" + target +
                        ": not found " + key + "\", \"RC\": \"-1\"}");
                }
                else if ((type & Utils.RESULT_XML) > 0) { // failed to get xml
                    ev = new Event(Event.ERR, "<Result><Error>" + target +
                        ": not found " + key + "</Error><RC>-1</RC></Result>");
                }
                else if (isRemote) {
                    ev = new Event(Event.WARNING, key +" not found in "+target);
                    ev.setAttribute("rc", "-1");
                }
                else { // external event
                    event.setPriority(Event.ERR);
                    event.setAttribute("text", target+": not found "+key);
                    event.setAttribute("rc", "-1");
                    event.setBody(null);
                }
            }
            else { // for relayed report and event
                Map<String, String> report = new HashMap<String, String>();
                data = event.getAttribute("text");
                if (data == null || data.length() <= 0)
                    break;
                if (data.charAt(0) != 'R') { // for relayed event
                    Iterator iter;
                    TextEvent msg;
                    n = Utils.split(Event.ITEM_SEPARATOR,
                        Event.ESCAPED_ITEM_SEPARATOR,data.substring(12),report);
                    msg = new TextEvent();
                    for (String ky : report.keySet()) {
                        msg.setAttribute(ky, report.get(ky));
                    }
                    key = report.get("name");
                    flow = defaultFlow;
                    if (flow != null) { // found the flow, ingest msg
                        i = flow.ingest(key, msg);
                        if (i < 0)
                            new Event(Event.ERR, name +
                                ": failed to ingest an event into "+key).send();
                        else if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name +
                                " ingested an event at " + i + " into " + key+
                                ": " + EventUtils.compact(msg)).send();
                    }
                    else
                        new Event(Event.ERR, name + ": no flow with " +
                            key + " to ingest an event").send();
                    return role;
                }

                // for relayed report
                if ((debug & DEBUG_REPT) > 0)
                    new Event(Event.DEBUG, name + " got an escalation: " +
                        EventUtils.compact(event)).send();
                n = Utils.split(Event.ITEM_SEPARATOR,
                    Event.ESCAPED_ITEM_SEPARATOR, data.substring(13), report);
                uri = (String) report.get("name");
                report.remove("name");
                if (uri != null && brokerRole > 0) // assuming reporter disabled
                    updateReport(uri, null, report,
                        MonitorReport.REPORT_SHARED);
                else if ("master".equals(uri) && brokerRole == 0) { // takeover
                    event.setPriority(Event.WARNING);
                    event.setAttribute("operation",
                        (String) report.get("operation"));
                    event.setAttribute("masterURI",
                        (String) report.get("masterURI"));
                    role = -1;
                    i = demote(currentTime, 0);
                    mtime = currentTime;
                    clusterNode.relay(event, 500);
                    new Event(Event.INFO, name + " demoted to " + role +
                        " from " + brokerRole).send();
                }
            }
            break;
          default:
            role = brokerRole;
            break;
        }

        if (isRemote) {
            str = event.getAttribute("operation");
            if (ev == null)
                ev = new Event(Event.WARNING, "bad request on " + target);
            ev.setAttribute("category", target);
            ev.setAttribute("name", key);
            ev.setAttribute("operation", str);
            ev.setAttribute("status", String.valueOf(brokerStatus));
            ev.setAttribute("role", String.valueOf(brokerRole));
            str = event.getAttribute("type");
            if ((str = event.getAttribute("type")) != null)
                ev.setAttribute("type", str);
            str = event.getAttribute(rcField);
            if (str != null && str.length() > 0)
                ev.setAttribute(rcField, str);
            if (adminServer != null) { // for collectible via adminServer
                str = zonedDateFormat.format(new Date()) + " " +
                    Event.getIPAddress() + " " + EventUtils.collectible(ev);
            }
            else { // no adminServer so just use original text
                str = ev.getAttribute("text");
            }

            try {
                ((TextEvent) event).setText(str);
            }
            catch (JMSException e) {
                new Event(Event.WARNING,name+" failed to set response: "
                    + Event.traceStack(e)).send();
            }
            ev.clearAttributes();
            if ((debug & DEBUG_CTRL) > 0 && (debug & DEBUG_TRAN) > 0)
                new Event(Event.DEBUG, name + " responded with: " + str).send();
        }

        return role;
    }

    /**
     * returns a Map containing queried information
     */
    private Map<String, String> queryInfo(long currentTime, long sessionTime,
        String target, String key, int type) {
        int i, j, k, n, id, size;
        long tm;
        String str, text, NAME = name.toUpperCase();
        Map<String, String> h = null;
        Map<String, Object> map;
        Object o;
        boolean isXML = (type & Utils.RESULT_XML) > 0;
        boolean isJSON = (type & Utils.RESULT_JSON) > 0;

        if ("FLOW".equals(NAME)) // reset name for container due to collision
            NAME = "QFLOW";
        if (key == null)
            key = target;
        if ("PROPERTIES".equals(target)) { // for properties
            o = cachedProps.get(key);
            if (o != null && o instanceof Map) // for a specific obj
                map = Utils.cloneProperties((Map) o);
            else if (key.equals(name)) { // for master config file
                List list = null;
                map =  Utils.cloneProperties(cachedProps);
                if (configRepository != null &&
                    (o = configRepository.get("Name")) != null)
                    map.remove((String) o);

                o = map.get("Reporter");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map)
                        str = (String) ((Map) o).get("Name"); 
                    else
                        str = (String) o;
                    map.remove(str);
                }
                o = map.get("Receiver");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    map.remove((String) o);
                }
                o = map.get("Node");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    map.remove((String) o);
                }
                o = map.get("Persister");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    map.remove((String) o);
                }
                JSON2Map.flatten(map);
                h = new HashMap<String, String>();
                for (String ky : map.keySet()) {
                    h.put(ky, (String) map.get(ky));
                }
            }
            else if ("PROPERTIES".equals(key)) { // list all properties
                StringBuffer strBuf = new StringBuffer();
                List list = null;
                Map g = null;
                h = new HashMap<String, String>();
                if (isXML) {
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Name>" + name + "</Name>");
                    o = cachedProps.get("Type");
                    strBuf.append("<Type>" + Utils.escapeXML((String) o) +
                        "</Type>");
                    o = cachedProps.get("Heartbeat");
                    strBuf.append("<Heartbeat>" + Utils.escapeXML((String) o) +
                        "</Heartbeat>");
                    strBuf.append("<LinkName>N/A</LinkName>");
                    strBuf.append("<Description>master configuration" +
                        "</Description>");
                    strBuf.append("</Record>");

                    if (configRepository != null)
                        g = (Map) configRepository.get("Properties");
                    if (g != null && g.size() > 0) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>"+(String)g.get("Name")+"</Name>");
                        strBuf.append("<Type>" + (String) g.get("Type") +
                            "</Type>");
                        strBuf.append("<Heartbeat>" +
                            (String) cachedProps.get("Heartbeat") +
                            "</Heartbeat>");
                        strBuf.append("<LinkName>N/A</LinkName>");
                        strBuf.append("<Description>" +
                            (String) g.get("Description") + "</Description>");
                        strBuf.append("</Record>");
                    }
                }
                else if (isJSON) {
                    strBuf.append("{");
                    strBuf.append("\"Name\":\"" + name + "\",");
                    o = cachedProps.get("Type");
                    strBuf.append("\"Type\":\"" + Utils.escapeJSON((String) o) +
                        "\",");
                    o = cachedProps.get("Heartbeat");
                    strBuf.append("\"Heartbeat\":\"" +
                        Utils.escapeJSON((String) o) + "\",");
                    strBuf.append("\"LinkName\":\"N/A\",");
                    strBuf.append("\"Description\":\"master configuration\"");
                    strBuf.append("}");
                }
                else {
                    o = cachedProps.get("Type");
                    strBuf.append((String) o + " ");
                    o = cachedProps.get("Heartbeat");
                    strBuf.append((String) o);
                    strBuf.append(" N/A master configuration");
                    h.put(name, strBuf.toString());
                    if (configRepository != null)
                        g = (Map) configRepository.get("Properties");
                    if (g != null && g.size() > 0) {
                        text = (String) g.get("Type") + " " +
                            cachedProps.get("Heartbeat") + " N/A " +
                            (String) g.get("Description");
                        h.put((String) g.get("Name"), text);
                    }

                }
                o = cachedProps.get("Reporter");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Map)
                        o = ((Map) o).get("Name");
                    else if (!(o instanceof String))
                        continue;
                    g = (Map) cachedProps.get(o);
                    if (g == null)
                        continue;
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        str = Utils.escapeXML((String) o);
                        strBuf.append("<Name>" + str + "</Name>");
                        strBuf.append("<Type>Reporter</Type>");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("<Heartbeat>" +
                            Utils.escapeXML((String) o) + "</Heartbeat>");
                        strBuf.append("<LinkName>N/A</LinkName>");
                        o = g.get("Description");
                        strBuf.append("<Description>" +
                            Utils.escapeXML((String) o) + "</Description>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        str = Utils.escapeJSON((String) o);
                        strBuf.append("\"Name\":\"" + str + "\",");
                        strBuf.append("\"Type\":\"Reporter\",");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("\"Heartbeat\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        strBuf.append("\"LinkName\":\"N/A\",");
                        o = g.get("Description");
                        strBuf.append("\"Description\":\"" +
                            Utils.escapeJSON((String) o) + "\"");
                        strBuf.append("}");
                    }
                    else {
                        strBuf.setLength(0);
                        str = (String) o;
                        strBuf.append("Reporter ");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append((String) o + " N/A ");
                        o = g.get("Description");
                        strBuf.append((String) o);
                        h.put(str, strBuf.toString());
                    }
                }
                o = cachedProps.get("Receiver");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Map)
                        o = ((Map) o).get("Name");
                    else if (!(o instanceof String))
                        continue;
                    g = (Map) cachedProps.get(o);
                    if (g == null)
                        continue;
                    if (isXML) {
                        str = Utils.escapeXML((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + str + "</Name>");
                        strBuf.append("<Type>Receiver</Type>");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("<Heartbeat>" +
                            Utils.escapeXML((String) o) + "</Heartbeat>");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("<LinkName>" +
                            Utils.escapeXML((String) o) + "</LinkName>");
                        o = g.get("Description");
                        strBuf.append("<Description>" +
                            Utils.escapeXML((String) o) + "</Description>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        str = Utils.escapeJSON((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + str + "\",");
                        strBuf.append("\"Type\":\"Receiver\",");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("\"Heartbeat\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("\"LinkName\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("Description");
                        strBuf.append("\"Description\":\"" +
                            Utils.escapeJSON((String) o) + "\"");
                        strBuf.append("}");
                    }
                    else {
                        strBuf.setLength(0);
                        str = (String) o;
                        strBuf.append("Receiver ");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append((String) o + " ");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append((String) o + " ");
                        o = g.get("Description");
                        strBuf.append((String) o);
                        h.put(str, strBuf.toString());
                    }
                }
                o = cachedProps.get("Node");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    g = (Map) cachedProps.get(o);
                    if (g == null)
                        continue;
                    if (isXML) {
                        str = Utils.escapeXML((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + str + "</Name>");
                        strBuf.append("<Type>Node</Type>");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("<Heartbeat>" +
                            Utils.escapeXML((String) o) + "</Heartbeat>");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("<LinkName>" +
                            Utils.escapeXML((String) o) + "</LinkName>");
                        o = g.get("Description");
                        strBuf.append("<Description>" +
                            Utils.escapeXML((String) o) + "</Description>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        str = Utils.escapeJSON((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + str + "\",");
                        strBuf.append("\"Type\":\"Node\",");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("\"Heartbeat\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("\"LinkName\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("Description");
                        strBuf.append("\"Description\":\"" +
                            Utils.escapeJSON((String) o) + "\"");
                        strBuf.append("}");
                    }
                    else {
                        strBuf.setLength(0);
                        str = (String) o;
                        strBuf.append("Node ");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append((String) o + " ");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append((String) o + " ");
                        o = g.get("Description");
                        strBuf.append((String) o);
                        h.put(str, strBuf.toString());
                    }
                }
                o = cachedProps.get("Persister");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Map)
                        o = ((Map) o).get("Name");
                    else if (!(o instanceof String))
                        continue;
                    g = (Map) cachedProps.get(o);
                    if (g == null)
                        continue;
                    if (isXML) {
                        str = Utils.escapeXML((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + str + "</Name>");
                        strBuf.append("<Type>Persister</Type>");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("<Heartbeat>" +
                            Utils.escapeXML((String) o) + "</Heartbeat>");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("<LinkName>" +
                            Utils.escapeXML((String) o) + "</LinkName>");
                        o = g.get("Description");
                        strBuf.append("<Description>" +
                            Utils.escapeXML((String) o) + "</Description>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        str = Utils.escapeJSON((String) o);
                        if (h.containsKey(str)) // skip repeated item
                            continue;
                        h.put(str, null);
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + str + "\",");
                        strBuf.append("\"Type\":\"Persister\",");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append("\"Heartbeat\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append("\"LinkName\":\"" +
                            Utils.escapeJSON((String) o) + "\",");
                        o = g.get("Description");
                        strBuf.append("\"Description\":\"" +
                            Utils.escapeJSON((String) o) + "\"");
                        strBuf.append("}");
                    }
                    else {
                        strBuf.setLength(0);
                        str = (String) o;
                        strBuf.append("Persister ");
                        o = cachedProps.get("Heartbeat");
                        strBuf.append((String) o + " ");
                        o = g.get("LinkName");
                        if (o == null)
                            o = "unknown";
                        strBuf.append((String) o + " ");
                        o = g.get("Description");
                        strBuf.append((String) o);
                        h.put(str, strBuf.toString());
                    }
                }
                if (isXML) {
                    h.clear();
                    h.put(target, strBuf.toString());
                }
                else if (isJSON) {
                    h.clear();
                    h.put(target, "[" + strBuf.toString() + "]");
                }
            }
        }
        else if ("REPORT".equals(target)) { // for report
            if ((map = getReport(key)) != null) { // for a specific report
                o = map.get("TestTime");
                if (o != null && o instanceof String) try {
                    tm = Long.parseLong((String) o);
                    map.put("TestTime", Event.dateFormat(new Date(tm)));
                }
                catch (Exception e) {
                }
                o = map.get("Status");
                if (o != null && o instanceof String) try {
                    id = Integer.parseInt((String) o);
                    map.put("Status", reportStatusText[id+statusOffset]);
                }
                catch (Exception e) {
                }
                JSON2Map.flatten(map);
                h = new HashMap<String, String>();
                for (String ky : map.keySet()) {
                    h.put(ky, (String) map.get(ky));
                }
            }
            else if ("REPORT".equals(key)) { // list all reports
                StringBuffer strBuf = new StringBuffer();
                String[] keys = getReportNames();
                h = new HashMap<String, String>();
                for (i=0; i<keys.length; i++) {
                    str = keys[i];
                    if (str == null || str.length() <= 0)
                        continue;
                    o = reports.get(str);
                    if (o == null || !(o instanceof Map))
                        continue;
                    size = ((Map) o).size();
                    try {
                        keys[i] = (String) ((Map) o).get("Status");
                        if (keys[i] == null || keys[i].length() <= 0)
                            id = TimeWindows.EXCEPTION;
                        else
                            id = Integer.parseInt(keys[i]);
                    }
                    catch (Exception e) {
                        id = TimeWindows.EXCEPTION;
                    }
                    text = reportStatusText[id + statusOffset];
                    o = ((Map) o).get("TestTime");
                    if (o == null || !(o instanceof String))
                        tm = 0L;
                    else try {
                        tm = Long.parseLong((String) o);
                    }
                    catch (Exception e) {
                        tm = -1L;
                    }
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" +
                            Utils.escapeXML(Event.dateFormat(new Date(tm))) +
                            "</Time>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm))) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                keys = MonitorAgent.getReportNames();
                for (i=0; i<keys.length; i++) { // for MonitorAgent reports
                    str = keys[i];
                    o = MonitorAgent.getReport(str);
                    if (o == null || !(o instanceof Map))
                        continue;
                    size = ((Map) o).size();
                    try {
                        keys[i] = (String) ((Map) o).get("Status");
                        if (keys[i] == null || keys[i].length() <= 0)
                            id = TimeWindows.EXCEPTION;
                        else
                            id = Integer.parseInt(keys[i]);
                    }
                    catch (Exception e) {
                        id = TimeWindows.EXCEPTION;
                    }
                    text = reportStatusText[id + statusOffset];
                    o = ((Map) o).get("TestTime");
                    if (o == null || !(o instanceof String))
                        tm = 0L;
                    else try {
                        tm = Long.parseLong((String) o);
                    }
                    catch (Exception e) {
                        tm = -1L;
                    }
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" +
                            Utils.escapeXML(Event.dateFormat(new Date(tm))) +
                            "</Time>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm))) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ((map = MonitorAgent.getReport(key)) != null) {
                // for MintorAgent report
                o = map.get("TestTime");
                if (o != null && o instanceof String) try {
                    tm = Long.parseLong((String) o);
                    map.put("TestTime", Event.dateFormat(new Date(tm)));
                }
                catch (Exception e) {
                }
                o = map.get("Status");
                if (o != null && o instanceof String) try {
                    id = Integer.parseInt((String) o);
                    map.put("Status", reportStatusText[id+statusOffset]);
                }
                catch (Exception e) {
                }
                JSON2Map.flatten(map);
                h = new HashMap<String, String>();
                for (String ky : map.keySet()) {
                    h.put(ky, (String) map.get(ky));
                }
            }
        }
        else if (NAME.equals(target) || "STATUS".equals(target)) { // for status
            h = new HashMap<String, String>();
            h.put("Name", name);
            h.put("ReleaseTag", ReleaseTag.getReleaseTag());
            if (previousRole == 0)
                str = "master";
            else if (previousRole > 0)
                str = "worker_" + previousRole;
            else
                str = "standalone";
            h.put("PreviousRole", str);
            id = statusText.length;
            h.put("BrokerStatus", statusText[(brokerStatus+id)%id]);
            h.put("PreviousStatus", statusText[(previousStatus+id)%id]);
            h.put("GroupSize", "1");
            h.put("FlowSize", String.valueOf(flowList.size()));
            h.put("Debug", String.valueOf(debug));
            h.put("Time", Event.dateFormat(new Date(mtime)));
            h.put("SessionTime", Event.dateFormat(new Date(sessionTime)));
            if (clusterNode != null) {
                h.put("ClusterURI", myURI);
                h.put("ClusterSize",
                    String.valueOf(clusterNode.getSize()));
                h.put("ClusterStatus",
                    statusText[(clusterNode.getStatus()+id)%id]);
                id = clusterNode.getRole();
                if (id == 0)
                    str = "master";
                else if (id > 0)
                    str = "worker_" + id;
                else
                    str = "unknown";
                h.put("BrokerRole", str);
            }
            else {
                if (brokerRole == 0)
                    str = "master";
                else if (brokerRole > 0)
                    str = "worker_" + brokerRole;
                else
                    str = "standalone";
                h.put("BrokerRole", str);
            }
        }
        else if ("XQ".equals(target)) { // for XQueue
            XQueue xq;
            int flowSize = flowList.getCapacity();
            if ("XQ".equals(key)) { // list all xqs
                StringBuffer strBuf = new StringBuffer();
                String tmStr;
                long[] list = new long[6];
                h = new HashMap<String, String>();
                if (escalation != null) { // for escalation
                    xq = escalation;
                    str = xq.getName();
                    list[0] = xq.depth();
                    list[1] = xq.size();
                    list[2] = xq.getCount();
                    list[3] = xq.getMTime();
                    list[4] = xq.getCapacity();
                    list[5] = xq.getGlobalMask();
                    size = (int) list[1];
                    tmStr = Event.dateFormat(new Date(list[3]));
                    id = (int) list[5];
                    if ((id & XQueue.KEEP_RUNNING) > 0)
                        text = statusText[SERVICE_RUNNING];
                    else if ((id & XQueue.PAUSE) > 0)
                        text = statusText[SERVICE_PAUSE];
                    else if ((id & XQueue.STANDBY) > 0)
                        text = statusText[SERVICE_STANDBY];
                    else
                        text = statusText[SERVICE_STOPPED];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Depth>" + list[0] + "</Depth>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + list[2] + "</Count>");
                        strBuf.append("<Time>" + Utils.escapeXML(tmStr)+
                            "</Time>");
                        strBuf.append("<Capacity>" + list[4] + "</Capacity>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Depth\":\"" + list[0] + "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Count\":\"" + list[2] + "\",");
                        strBuf.append("\"Time\":\"" + Utils.escapeJSON(tmStr)+
                            "\",");
                        strBuf.append("\"Capacity\":\"" + list[4] + "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, list[0] + " " + size + " " + list[2] + " " +
                            tmStr + " " + list[4] + " " + text);
                }
                if (pool != null) { // for reporterPool
                    xq = pool;
                    str = xq.getName();
                    list[0] = xq.depth();
                    list[1] = xq.size();
                    list[2] = xq.getCount();
                    list[3] = xq.getMTime();
                    list[4] = xq.getCapacity();
                    list[5] = xq.getGlobalMask();
                    size = (int) list[1];
                    tmStr = Event.dateFormat(new Date(list[3]));
                    id = (int) list[5];
                    if ((id & XQueue.KEEP_RUNNING) > 0)
                        text = statusText[SERVICE_RUNNING];
                    else if ((id & XQueue.PAUSE) > 0)
                        text = statusText[SERVICE_PAUSE];
                    else if ((id & XQueue.STANDBY) > 0)
                        text = statusText[SERVICE_STANDBY];
                    else
                        text = statusText[SERVICE_STOPPED];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Depth>" + list[0] + "</Depth>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + list[2] + "</Count>");
                        strBuf.append("<Time>" + Utils.escapeXML(tmStr)+
                            "</Time>");
                        strBuf.append("<Capacity>" + list[4] + "</Capacity>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Depth\":\"" + list[0] + "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Count\":\"" + list[2] + "\",");
                        strBuf.append("\"Time\":\"" + Utils.escapeJSON(tmStr)+
                            "\",");
                        strBuf.append("\"Capacity\":\"" + list[4] + "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, list[0] + " " + size + " " + list[2] + " " +
                            tmStr + " " + list[4] + " " + text);
                }
                if (flowSize == 1 && defaultFlow != null) { // for legacy broker
                    Map<String, String> ph = defaultFlow.queryInfo(currentTime,
                        sessionTime, "XQ", "XQ", type);
                    if (isXML)
                        strBuf.append(ph.get("XQ"));
                    else if (isJSON) {
                        str = ph.get("XQ");
                        if (str != null && (size = str.length()) > 2) { // merge
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append(str.substring(1, size-1));
                        }
                    }
                    else { // copy xqs over
                        for (String ky : ph.keySet()) {
                            if (!h.containsKey(ky))
                                h.put(ky, ph.get(ky));
                        }
                    }
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ("escalation".equals(key) && escalation != null) {
                // for a specific XQ
                h = MessageUtils.listXQ(escalation, type, displayPropertyName);
            }
            else if ("reporterPool".equals(key) && pool != null) {
                // for a specific XQ
                h = MessageUtils.listXQ(pool, type, displayPropertyName);
            }
            else if (flowSize == 1 && defaultFlow != null) {
                // for a specific XQ
                h = defaultFlow.queryInfo(currentTime, sessionTime, "XQ",
                    key, type);
            }
        }
        else if ("FLOW".equals(target)) { // for flow
            MessageFlow flow;
            long[] flowInfo;
            if (flowList.containsKey(key)) { // for a specific flow
                String ts;
                StringBuffer strBuf = new StringBuffer();
                i = flowList.getID(key);
                h = new HashMap<String, String>();
                flowInfo = flowList.getMetaData(i);
                flow = (MessageFlow) flowList.get(i);
                flowInfo[GROUP_STATUS] = flow.getStatus();
                tm = flowInfo[GROUP_TIME];
                size = (int) flowInfo[GROUP_SIZE];
                text = statusText[((int) flowInfo[GROUP_STATUS] +
                    statusText.length) % statusText.length];
                ts = Event.dateFormat(new Date(tm));
                if (isXML) {
                    ts = Utils.escapeXML(ts);
                    text = Utils.escapeXML(text);
                    if (size > 0) { // for receiver
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>Receiver</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" + ts + "</Time>");
                        strBuf.append("<Status>" + text + "</Status>");
                        strBuf.append("</Record>");
                    }
                    size = (int) flowInfo[GROUP_COUNT];
                    if (size > 0) { // for node
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>Node</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" + ts + "</Time>");
                        strBuf.append("<Status>" + text + "</Status>");
                        strBuf.append("</Record>");
                    }
                    size = (int) flowInfo[GROUP_SPAN];
                    if (size > 0) { // for persister
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>Persister</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" + ts + "</Time>");
                        strBuf.append("<Status>" + text + "</Status>");
                        strBuf.append("</Record>");
                    }
                    size = flow.getNumberOfXQueues();
                    if (size > 0) { // for xqueue
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>XQueue</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" + ts + "</Time>");
                        strBuf.append("<Status>" + text + "</Status>");
                        strBuf.append("</Record>");
                    }
                }
                else if (isJSON) {
                    ts = Utils.escapeJSON(ts);
                    text = Utils.escapeJSON(text);
                    if (size > 0) { // for receiver
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"Receiver\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" + ts + "\",");
                        strBuf.append("\"Status\":\"" + text + "\"");
                        strBuf.append("}");
                    }
                    size = (int) flowInfo[GROUP_COUNT];
                    if (size > 0) { // for node
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"Node\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" + ts + "\",");
                        strBuf.append("\"Status\":\"" + text + "\"");
                        strBuf.append("}");
                    }
                    size = (int) flowInfo[GROUP_SPAN];
                    if (size > 0) { // for persister
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"Persister\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" + ts + "\",");
                        strBuf.append("\"Status\":\"" + text + "\"");
                        strBuf.append("}");
                    }
                    size = flow.getNumberOfXQueues();
                    if (size > 0) { // for xqueue
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"XQueue\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" + ts + "\",");
                        strBuf.append("\"Status\":\"" + text + "\"");
                        strBuf.append("}");
                    }
                }
                else {
                    if (size > 0) // for receiver
                        h.put("Receiver", size + " " + ts + " " + text);
                    size = (int) flowInfo[GROUP_COUNT];
                    if (size > 0) // for node
                        h.put("Node", size + " " + ts + " " + text);
                    size = (int) flowInfo[GROUP_SPAN];
                    if (size > 0) // for persister
                        h.put("Persister", size + " " + ts + " " + text);
                    size = flow.getNumberOfXQueues();
                    if (size > 0) // for xqueue
                        h.put("XQueue", size + " " + ts + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ("FLOW".equals(key)) { // list all flows
                StringBuffer strBuf = new StringBuffer();
                Browser browser = flowList.browser();
                h = new HashMap<String, String>();
                while ((i = browser.next()) >= 0) {
                    str = flowList.getKey(i);
                    flowInfo = flowList.getMetaData(i);
                    flow = (MessageFlow) flowList.get(i);
                    flowInfo[GROUP_STATUS] = flow.getStatus();
                    size = (int) flowInfo[GROUP_SIZE];
                    tm = flowInfo[GROUP_TIME];
                    text = statusText[((int) flowInfo[GROUP_STATUS] +
                        statusText.length) % statusText.length];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Receiver>" + size + "</Receiver>");
                        strBuf.append("<Node>" + flowInfo[GROUP_COUNT] +
                            "</Node>");
                        strBuf.append("<Persister>" + flowInfo[GROUP_SPAN] +
                            "</Persister>");
                        strBuf.append("<Time>" +
                            Utils.escapeXML(Event.dateFormat(new Date(tm)))+
                            "</Time>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Receiver\":\"" + size + "\",");
                        strBuf.append("\"Node\":\"" + flowInfo[GROUP_COUNT] +
                            "\",");
                        strBuf.append("\"Persister\":\"" + flowInfo[GROUP_SPAN]+
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm)))+
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " + flowInfo[GROUP_COUNT] + " " +
                            flowInfo[GROUP_SPAN] + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
        }
        else if ("RECEIVER".equals(target) || "PERSISTER".equals(target) ||
            "NODE".equals(target)) { // for an object on a specific flow
            MessageFlow flow = (MessageFlow) flowList.get(0);
            h = null;
            if (flow != null) { // list all nodes on the flow
                h = flow.queryInfo(currentTime, sessionTime, target,
                    key, type);
            }
        }
        else if ("OUT".equals(target) || "RULE".equals(target) ||
            "MSG".equals(target)) { // for a specific flow
            MessageFlow flow = (MessageFlow) flowList.get(0);
            h = null;
            if (flow != null) {
                h = flow.queryInfo(currentTime, sessionTime, target,
                    key, type);
            }
        }
        else if ("MONITOR".equals(target)) { // for monitor group
            h = loadGroupInfo(key, groupInfo);
        }
        else if ("HELP".equals(target) || "USAGE".equals(target)) {
            StringBuffer strBuf = new StringBuffer();
            h = new HashMap<String, String>();
            if (isXML) {
                strBuf.append("<Record>\n");
                strBuf.append("  <USAGE>query CATEGORY [name]</USAGE>\n");
                strBuf.append("  <PROPERTIES>List of configuration " +
                    "properties</PROPERTIES>\n");
                strBuf.append("  <" + NAME + ">Status of " + name +
                    "</" + NAME + ">\n");
                strBuf.append("  <FLOW>List of all message flows"+"</FLOW>\n");
                strBuf.append("  <REPORT>List of all shared reports" +
                    "</REPORT>\n");
                strBuf.append("  <NODE>List of all message nodes</NODE>\n");
                strBuf.append("  <RECEIVER>List of all receivers</RECEIVER>\n");
                strBuf.append("  <PERSISTER>List of all persisters" +
                    "</PERSISTER>\n");
                strBuf.append("  <MONITOR>List of all monitors</MONITOR>\n");
                strBuf.append("  <XQ>List of all internal XQueues</XQ>\n");
                strBuf.append("  <RULE>List of rulesets of the message " +
                    "node</RULE>\n");
                strBuf.append("  <OUT>List of outLinks of the message " +
                    "node</OUT>\n");
                strBuf.append("  <MSG>State info on outstanding messages of " +
                    "the message node</MSG>\n");
                strBuf.append("</Record>");
                h.put(target, strBuf.toString());
            }
            else if (isJSON) {
                strBuf.append("{\n");
                strBuf.append("  \"USAGE\":\"query CATEGORY [name]\",\n");
                strBuf.append("  \"PROPERTIES\":\"List of configuration " +
                    "properties\",\n");
                strBuf.append("  \"" + NAME + "\":\"Status of " +name+ "\",\n");
                strBuf.append("  \"FLOW\":\"List of all message flows\",\n");
                strBuf.append("  \"REPORT\":\"List of all shared reports\",\n");
                strBuf.append("  \"NODE\":\"List of all message nodes\",\n");
                strBuf.append("  \"RECEIVER\":\"List of all receivers\",\n");
                strBuf.append("  \"PERSISTER\":\"List of all persisters\",\n");
                strBuf.append("  \"MONITOR\":\"List of all monitors\",\n");
                strBuf.append("  \"XQ\":\"List of all internal XQueues\",\n");
                strBuf.append("  \"RULE\":\"List of rulesets of the message " +
                    "node\",\n");
                strBuf.append("  \"OUT\":\"List of outLinks of the message " +
                    "node\",\n");
                strBuf.append("  \"MSG\":\"State info on outstanding messages "+
                    "of the message node\"\n");
                strBuf.append("}");
                h.put(target, strBuf.toString());
            }
            else {
                strBuf.append("query CATEGORY [name]\n");
                strBuf.append("where the following CATEGORY supported:\n");
                strBuf.append(NAME + ": for status of " + name +"\n");
                strBuf.append("PROPERTIES: for configuration properties\n");
                strBuf.append("FLOW: for message flows\n");
                strBuf.append("REPORT: for shared reports\n");
                strBuf.append("NODE: for state info of message nodes\n");
                strBuf.append("RECEIVER: for state info of message " +
                    "receivers\n");
                strBuf.append("PERSISTER: for state info of message " +
                    "persisters\n");
                strBuf.append("MONITOR: for state info of monitor\n");
                strBuf.append("XQ: for state info of XQueues\n");
                strBuf.append("RULE: for stats of rulesets of the node\n");
                strBuf.append("OUT: for state info on outlinks of the node\n");
                strBuf.append("MSG: for info on outstanding msgs of the node");
                h.put("Usage", strBuf.toString());
            }
        }
        return h;
    }

    private void display(int flag) {
        StringBuffer strBuf = null;
        MessageFlow flow;
        Browser browser;
        XQueue xq;
        long[] flowInfo;
        String key;
        int id, mask;

        if ((debug & flag) > 0)
            strBuf = new StringBuffer();
        else
            return;

        browser = flowList.browser();
        while ((id = browser.next()) >= 0) {
            flow = (MessageFlow) flowList.get(id);
            flowInfo = flowList.getMetaData(id);
            xq = (XQueue) flow.getRootQueue();
            if (xq != null) {
                key = xq.getName();
                mask = xq.getGlobalMask();
            }
            else {
                key = "unknown";
                mask = -1;
            }
            flowInfo[GROUP_STATUS] = flow.getStatus();
            strBuf.append("\n\t" + flowList.getKey(id) + ": " + id + " " +
                flowInfo[GROUP_SIZE] + " " + flowInfo[GROUP_COUNT] + " " +
                flowInfo[GROUP_SPAN] + " " + flowInfo[GROUP_STATUS] + " " +
                flow.getDefaultStatus() + " - "  + mask + " " + key);
        }

        if (strBuf.length() > 0)
            new Event(Event.DEBUG, name + " FlowName: ID RCVR NODE PSTR " +
                "Status DefStatus - Mask LinkName role=" + brokerRole +
                " status=" +statusText[brokerStatus]+ strBuf.toString()).send();
    }

    /**
     * starts all threads and manages their states
     */
    public void start() {
        XQueue xq;
        MessageFlow flow;
        Browser browser;
        String key;
        long[] flowInfo;
        long sessionTime, currentTime, tm;
        int count = 0;
        int i, j, id, cid, size;

        currentTime = System.currentTimeMillis();
        if (brokerRole == 0 && actionGroup != null) try { // enable resources
            Event event = new Event(Event.INFO, name+": resources started");
            event.setAttribute("status", "start");
            actionGroup.invokeAction(currentTime, event);
            event.send();
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": resources failed to start: " +
                e.toString()).send();
        }

        currentTime = System.currentTimeMillis();
        if (group != null && monitor != null) { // start all reporters
            monitor.start();

            if (groupInfo[GROUP_STATUS] == SERVICE_READY) { // dispatch monitors
                if (pool.size() == 0) { // no jobs running in queue
                    currentTime = System.currentTimeMillis();
                    group.dispatch(pool);
                    groupInfo[GROUP_SIZE] = group.getSize();
                    groupInfo[GROUP_SPAN] = 0;
                    groupInfo[GROUP_TIME] = currentTime;
                    groupInfo[GROUP_RETRY] = 0;
                    groupInfo[GROUP_STATUS] = SERVICE_RUNNING;
                }

                while (pool.size() > 0) { // wait until all jobs complete 
                    try {
                        Thread.sleep(1000L);
                    }
                    catch (InterruptedException e) {
                    }
                }
            }

            if (completedFile != null)
                touch(completedFile);
        }

        browser = flowList.browser();
        while ((id = browser.next()) >= 0) { // start all flows
            flow = (MessageFlow) flowList.get(id);
            flowInfo = flowList.getMetaData(id);
            switch (flow.getDefaultStatus()) { // start flow
              case SERVICE_RUNNING:
                if (brokerRole > 0) // start worker
                   flow.demote();
                else // start master or standalone
                   flow.start();
                count ++;
                break;
              case SERVICE_PAUSE:
              case SERVICE_STANDBY:
              case SERVICE_DISABLED:
                if (brokerRole == 0) { // start master
                   flow.start();
                   count ++;
                }
                else // disable worker or standalone
                    flow.disable();
                break;
              case SERVICE_STOPPED:
                if (brokerRole == 0) // start master
                    flow.promote();
                else // stop worker or standalone
                    flow.stop();
                break;
              default:
                flow.stop();
                break;
            }
            flowInfo[GROUP_STATUS] = flow.getStatus();
            flowInfo[GROUP_TIME] = currentTime;
        }
        display(DEBUG_INIT);
        new Event(Event.INFO, name + ": " + count +
            " flows started with the build of " +
            ReleaseTag.getReleaseTag()).send();

        count = 0;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime + heartbeat;

        // main loop to manage flows and the group
        while ((escalation.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((cid = escalation.getNextCell(1000)) >= 0) { //got an escalation
                Event event;
                if ((event = (Event) escalation.browse(cid)) == null) {
                    escalation.remove(cid);
                    new Event(Event.WARNING, name +
                        ": null event from escalation channel").send();
                    continue;
                }
                try {
                    i = invokeAction(currentTime, sessionTime, event);
                    if (i != brokerRole) { // update the role
                        if (i >= 0)
                            previousRole = brokerRole;
                        brokerRole = i;
                    }
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": failed to process " +
                        "incoming event: " + Event.traceStack(e)).send();
                }
                if (event instanceof JMSEvent) try { // for remote request
                    ((JMSEvent) event).acknowledge();
                }
                catch (Exception e) {
                }
                escalation.remove(cid);
                if (event.getPriority() != Event.INFO) { // status update
                    if (clusterNode != null && brokerRole < 0 &&
                        (debug & DEBUG_LOAD) > 0)
                        new Event(Event.DEBUG, name + ": in transient state"+
                            " with status=" + statusText[brokerStatus]).send();
                    else
                        display(DEBUG_LOAD);
                }
            }
            currentTime = System.currentTimeMillis();
            if (count++ > 5 && group != null && pool.size() == 0) {
                count = 0;
                if (groupInfo[GROUP_STATUS] == SERVICE_RUNNING ||
                    groupInfo[GROUP_STATUS] == SERVICE_RETRYING) {
                    groupInfo[GROUP_COUNT] ++;
                    groupInfo[GROUP_SPAN] = currentTime - groupInfo[GROUP_TIME];
                    groupInfo[GROUP_STATUS] = SERVICE_READY;
                }
            }
            if (currentTime < sessionTime)
                continue;

            sessionTime = currentTime + heartbeat;
            if (brokerRole < 0 && clusterNode != null) { // transient state
                if (currentTime - mtime >= heartbeat) { // timeout in transient
                    brokerRole = previousRole;
                    mtime = currentTime;
                    new Event(Event.WARNING, name + ": timed out in transient "+
                        "state, roll back now to " + previousRole).send();
                }
            }

            if (group != null && pool.size() > 0 && groupInfo[GROUP_TIMEOUT] > 0
                && currentTime>=groupInfo[GROUP_TIME]+groupInfo[GROUP_TIMEOUT]){
                groupInfo[GROUP_RETRY] ++;
                if (groupInfo[GROUP_STATUS] == SERVICE_RUNNING ||
                    groupInfo[GROUP_STATUS] == SERVICE_READY)
                    groupInfo[GROUP_STATUS] = SERVICE_RETRYING;
                if (groupInfo[GROUP_RETRY] <= maxRetry) {
                    new Event(Event.WARNING, "reporter: timedout with " +
                        pool.size() + " jobs left").send();
                }
                else {
                    new Event(Event.ERR, "reporter: timedout with " +
                        pool.size() + " jobs left").send();
                    stopRunning(pool);
                    pool.clear();
                    monitor.interrupt();
                    try {
                        monitor.join(2000);
                    }
                    catch (Exception e) {
                        monitor.interrupt();
                    }
                    resumeRunning(pool);
                    monitor = new Thread(this, "Reporters");
                    monitor.setDaemon(true);
                    monitor.setPriority(Thread.NORM_PRIORITY);
                    monitor.start();
                    groupInfo[GROUP_RETRY] = 0;
                    if (groupInfo[GROUP_STATUS] == SERVICE_RETRYING)
                        groupInfo[GROUP_STATUS] = SERVICE_READY;
                }
            }

            if (group != null && pool.size() == 0) { // no jobs running in queue
                if (groupInfo[GROUP_STATUS] >= SERVICE_READY &&
                    groupInfo[GROUP_STATUS] <= SERVICE_RETRYING) {
                    group.dispatch(pool);
                    groupInfo[GROUP_SIZE] = group.getSize();
                    groupInfo[GROUP_SPAN] = 0;
                    groupInfo[GROUP_TIME] = System.currentTimeMillis();
                    groupInfo[GROUP_RETRY] = 0;
                    groupInfo[GROUP_STATUS] = SERVICE_RUNNING;
                    if (groupInfo[GROUP_STATUS] != SERVICE_READY)
                        groupInfo[GROUP_COUNT] ++;
                }
            }

            if (completedFile != null)
                touch(completedFile);

            if (adminServer != null && keepRunning(escalation) &&
                admin != null && !admin.isAlive() &&
                adminServer.getStatus() == MessageReceiver.RCVR_STOPPED) {
                adminServer.setStatus(MessageReceiver.RCVR_READY);
                admin = new Thread(this, "AdminServer");
                admin.setPriority(Thread.NORM_PRIORITY);
                admin.start();
            }

            display(DEBUG_LOOP);
            currentTime = System.currentTimeMillis();
        }

        if (pool != null)
            stopRunning(pool);

        if (brokerStatus != SERVICE_CLOSED)
            shutdown();
    }

    private void shutdown() {
        int i;
        close();
        if (monitor != null && monitor.isAlive()) {
            monitor.interrupt();
            try {
                monitor.join(2000);
            }
            catch(Exception e) {
                monitor.interrupt();
            }
        }
        if (cluster != null && cluster.isAlive()) {
            cluster.interrupt();
            try {
                cluster.join(2000);
            }
            catch(Exception e) {
                cluster.interrupt();
            }
        }
        if (admin != null && admin.isAlive()) {
            admin.interrupt();
            try {
                admin.join(2000);
            }
            catch(Exception e) {
                admin.interrupt();
            }
        }
        else if (httpServer != null) try {
            httpServer.join();
        }
        catch (Exception e) {
        }

        new Event(Event.INFO, name + ": instance has been shutdown").send();
    }

    /**
     * @param args the command line arguments
     */
    @SuppressWarnings("unchecked")
    public static void main(String args[]) {
        int i, n, size;
        int show = 0, debugForReload = 0;
        Object o;
        Thread c = null;
        String name = null, group = "default", category = null, action = null;
        QFlow qf = null;
        List list = null;
        File file;
        Map props = new HashMap();
        String operation, cfgDir, configFile = null;
        String FILE_SEPARATOR = System.getProperty("file.separator");

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2)
                continue;
            switch (args[i].charAt(1)) {
              case 'W':
                System.exit(0);
                break;
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'l':
                show = 1;
                break;
              case 'd':
                if (i+1 < args.length) {
                    debugForReload = Integer.parseInt(args[i+1]);
                }
                break;
              case 'I':
                if (i+1 < args.length) {
                    configFile = args[i+1];
                }
                break;
              case 'A':
                if (i+1 < args.length) {
                    action = args[i+1];
                }
                break;
              case 'C':
                if (i+1 < args.length) {
                    category = args[i+1];
                }
                break;
              case 'G':
                if (i+1 < args.length) {
                    group = args[i+1];
                }
                break;
              case 'N':
                if (i+1 < args.length) {
                    name = args[i+1];
                }
                break;
              default:
                break;
            }
        }

        if (configFile == null) {
            System.out.println("config file not specified");
            System.exit(0);
        }

        try {
            FileReader fr = new FileReader(configFile);
            props = (Map) JSON2Map.parse(fr);
            fr.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        if (action != null) {
            int priority = Event.INFO;
            DateFormat zonedDateFormat;
            MessagePersister pstr = null;
            String uri = null;

            o = props.get("AdminServer");
            if (o == null || !(o instanceof Map)) {
                System.err.println("AdminServer not defined");
                System.exit(1);
            }

            if ("stop".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "stop";
                category = "CLUSTER";
                name = "default";
            }
            else if ("reload".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "reload";
                if (category == null || category.charAt(0) == '-') {
                    category = "CLUSTER";
                    name = "default";
                }
                else if (name == null)
                    name = "default";
            }
            else if ("failover".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "failover";
                category = "CLUSTER";
                name = "default";
            }
            else if ("disable".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "disable";
                category = "FLOW";
                if (name == null)
                    name = "default";
            }
            else if ("enable".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "enable";
                category = "FLOW";
                if (name == null)
                    name = "default";
            }
            else if ("query".equals(action.toLowerCase())) {
                priority = Event.INFO;
                action = "query";
                if (category == null || category.charAt(0) == '-')
                    category = "USAGE";
                if (name == null)
                    name = category;
            }
            else {
                System.err.println("Action not supported: " + action);
                System.exit(1);
            }

            Map<String, Object> ph = Utils.cloneProperties((Map) o);
            String key = (String) ph.get("Operation");
            if ("react".equals(key)) {
                uri = null;
                ph.put("Operation", "download");
                ph.put("ClassName", "org.qbroker.persister.FilePersister");
                ph.put("Capacity", "2");
                ph.put("Partition", "0,0");
                ph.put("IsPost", "true");
                ph.put("EventPostable", "true");
                ph.put("DisplayMask", "0");
                ph.put("SOTimeout", "5");
                pstr = new FilePersister(ph);
            }
            else if ("respond".equals(key)) {
                uri = null;
                ph.put("Operation", "request");
                ph.put("ClassName", "org.qbroker.persister.StreamPersister");
                ph.put("Capacity", "2");
                ph.put("Partition", "0,0");
                ph.put("DisplayMask", "0");
                ph.put("SOTimeout", "5");
                pstr = new StreamPersister(ph);
            }
            else if ("reply".equals(key)) {
                uri = (String) ph.get("URI");
                ph.put("Operation", "inquire");
                ph.put("ClassName", "org.qbroker.persister.PacketPersister");
                ph.put("URI", "udp://localhost");
                ph.put("URIField", "UDP");
                ph.put("Capacity", "2");
                ph.put("Partition", "0,0");
                ph.put("DisplayMask", "0");
                ph.put("SOTimeout", "5");
                pstr = new PacketPersister(ph);
            }
            else {
                System.err.println("Operation not supported: " + key);
                System.exit(1);
            }

            TextEvent event = new TextEvent();
            if ("react".equals(key)) {
                event.setAttribute("operation", action);
                event.setAttribute("name", name);
                event.setAttribute("type", "json");
                event.setAttribute("group", group);
                event.setAttribute("category", category);
                event.setAttribute("status", "Normal");
                event.setAttribute("jsp", "/getJSON.jsp");
                key = null;
            }
            else {
                zonedDateFormat =
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");
                Event ev = new Event(priority);
                ev.setAttribute("operation", action);
                ev.setAttribute("name", name);
                ev.setAttribute("type", "json");
                event.setAttribute("group", group);
                ev.setAttribute("category", category);
                ev.setAttribute("status", "Normal");
                key = zonedDateFormat.format(new Date()) + " " +
                    Event.getIPAddress() + " " + EventUtils.collectible(ev);
            }

            try {
                event.setJMSPriority(9-priority);
                if (key != null)
                    event.setText(key);
                if (uri != null) // udp stuff
                    event.setStringProperty("UDP", uri.substring(6));
            }
            catch (Exception e) {
                e.printStackTrace();
                pstr.close();
                System.exit(1);
            }

            uri = key;
            key = MonitorAgent.query(pstr, event);
            pstr.close();
            if (key == null) { // got response back
                try {
                    System.err.println(event.getText());
                }
                catch (Exception e) {
                    System.err.println(e.toString());
                }
            }
            else {
                System.err.println("failed to get response for " + uri);
                System.err.println(key);
            }

            System.exit(1);
        }

        if (show == 0 && debugForReload == 0) {
            if ((o = props.get("LogDir")) != null) {
                String logDir = (String) o;
                if ((o = props.get("LogDatePattern")) != null)
                    Event.setLogDir(logDir, (String) o);
                else if ((o = props.get("LogMaxFileSize")) != null) {
                    int maxBackups = 1;
                    String maxSize = (String) o;
                    if ((o = props.get("LogMaxBackupIndex")) != null)
                        maxBackups = Integer.parseInt((String) o);
                    Event.setLogDir(logDir, maxSize, maxBackups);
                }
                else
                    Event.setLogDir(logDir, null);
            }

            if ((o = props.get("Site")) != null)
                Event.setDefaultSite((String) o);

            if ((o = props.get("Category")) != null)
                Event.setDefaultCategory((String) o);

            if ((o = props.get("Name")) != null)
                Event.setDefaultName((String) o);

            if ((o = props.get("Type")) != null)
                Event.setDefaultType((String) o);

            if ((o = props.get("MailHost")) != null)
                MessageMailer.setSMTPHost((String) o);
        }

        if (debugForReload > 0) // reset cfgDir
            o = new File(configFile).getParent();
        else if ((o = props.get("ConfigDir")) == null)
            throw(new IllegalArgumentException("ConfigDir is not defined"));
        cfgDir = (String) o;

        if (cfgDir.length() <= 0)
            cfgDir = ".";

        File f = new File(cfgDir);
        if (!f.exists() || !f.isDirectory() || !f.canRead())
            throw(new IllegalArgumentException("failed to read " + cfgDir));

        if (show > 0) {
            String text = JSON2Map.toJSON(props, "", "\n");
            i = text.lastIndexOf("}");
            System.out.println(text.substring(0, i));
        }

        // ConfigRepository
        size = 0;
        if ((o = props.get("ConfigRepository")) != null && o instanceof String){
            String key = (String) o;
            file = new File(cfgDir + FILE_SEPARATOR + key + ".json");
            if (file.exists() && file.isFile() && file.canRead()) try {
                FileReader fr = new FileReader(file);
                o = JSON2Map.parse(fr);
                fr.close();
                if (o == null || !(o instanceof Map)) {
                    new Event(Event.ERR, "empty property file for "+key).send();
                }
                else if (show > 0) {
                    System.out.println("\n,\"" + key + "\":" +
                        JSON2Map.toJSON((Map) o, "", "\n") +
                        " // end of "+ file.getName());
                }
                else {
                    props.put(key, o);
                    size ++;
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to load the config file for "+
                    key + ": " + Event.traceStack(e)).send();
            }
        }

        list = (List) props.get("Reporter");
        if (list == null)
            list = new ArrayList();
        size += MonitorAgent.loadListProperties(list, cfgDir, show, props);

        list = (List) props.get("Receiver");
        if (list == null)
            list = new ArrayList();
        size += MonitorAgent.loadListProperties(list, cfgDir, show, props);

        list = (List) props.get("Node");
        if (list == null)
            list = new ArrayList();
        size += MonitorAgent.loadListProperties(list, cfgDir, show, props);

        // merge 2nd includes only on MessageNodes
        if ((o = props.get("IncludePolicy")) != null && o instanceof Map)
            MonitorAgent.loadIncludedProperties(list, cfgDir, show, props,
                (Map) o);

        list = (List) props.get("Persister");
        if (list == null)
            list = new ArrayList();
        size += MonitorAgent.loadListProperties(list, cfgDir, show, props);

        if (show > 0) {
            System.out.println("} // end of " + configFile);
            System.exit(0);
        }
        else if (debugForReload > 0) try {
            qf = new QFlow(props, debugForReload, configFile);
            System.exit(0);
        }
        catch (Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }

        try {
            qf = new QFlow(props);
            c = new Thread(qf, "shutdown");
            Runtime.getRuntime().addShutdownHook(c);
        }
        catch (Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }

        if (qf != null) try { // start the daemon
            qf.start();
            new Event(Event.INFO, "QFlow " + qf.getName() + " exits").send();
        }
        catch (Throwable t) {
            new Event(Event.ERR, "QFlow " + qf.getName() + " aborts: " +
                Event.traceStack(t)).send();
            t.printStackTrace();
            qf.close();
        }

        if (c != null && c.isAlive()) try {
            c.join();
        }
        catch (Exception e) {
        }

        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("QFlow Version 3.0 (written by Yannan Lu)");
        System.out.println("QFlow: a Message Flow for processing JMS messages");
        System.out.println("Usage: java org.qbroker.flow.QFlow [-?|-l|-I ConfigFile]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -l: list all properties");
        System.out.println("  -d: debug mode for reload testing only");
        System.out.println("  -I: ConfigFile (default: ./Flow.json)");
        System.out.println("  -A: Action for query (default: none)");
        System.out.println("  -C: Category for query (default: USAGE)");
        System.out.println("  -G: Group for query (default: default)");
        System.out.println("  -N: Name for query (default: value of category)");
        System.out.println("  -W: Doing nothing (for stopping service on Win2K)");
/*
        System.out.println("  -S: Capacity of XQueue (default: 1)");
        System.out.println("  -L: LogDir (default: no logging)");
        System.out.println("  -X: XAMode (1: Client; 2: Commit; default: 1)");
*/
    }
}
