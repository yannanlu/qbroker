package org.qbroker.flow;

/* MonitorAgent.java - a container hosting various montiors */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.TimeZone;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.QList;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.ReservableCells;
import org.qbroker.common.Service;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.HTTPServer;
import org.qbroker.net.HTTPConnector;
import org.qbroker.net.MessageMailer;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.receiver.MessageReceiver;
import org.qbroker.persister.MessagePersister;
import org.qbroker.persister.StreamPersister;
import org.qbroker.persister.PacketPersister;
import org.qbroker.persister.FilePersister;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorAction;
import org.qbroker.monitor.MonitorGroup;
import org.qbroker.monitor.PropertyMonitor;
import org.qbroker.flow.MessageFlow;
import org.qbroker.event.EventAction;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventLogger;
import org.qbroker.event.Event;

/**
 * MonitorAgent is a Java container hosting multiple MonitorGroups
 * and MessageFlows.  The MonitorGroup contains various monitors that
 * periodically check if something have happened.  Whereas the MessageFlow
 * is to process messages for on-demand services.
 *<br/><br/>
 * An MonitorAgent periodically runs the registered monitors.
 * Registered monitors are grouped according to their dependencies.
 * The monitors in a same group are guaranteed to be checked in the
 * same order as they are in the group.  However, the order of different
 * groups is not honored by MonitorAgent due to MT implementation.
 *<br/><br/>
 * Each registered monitore contains three components, a MonitorReport
 * to generate the latest report on the monitored object, a MonitorAction
 * to handle the monitor report, and a set of time windows in which the
 * monitor is expected to be active.  It first calls
 * MonitorReport.generateReport(currentTime) that returns a report in
 * a Map object.  If there is no any exceptions, it checks the latest
 * monitor time with the predefined configuration of the time windows,
 * defined in ActiveTime, to see if the monitor is late or in time.
 * The result is passed through in status.  No matter what status is,
 * the MonitorAction.performAction(status, currentTime, report) will always
 * be called.
 *<br/><br/>
 * If an exception is thrown by MonitorReport.generateReport(currentTime),
 * the content of the report is invalid.  It does not make sense to check
 * on the invalid report.  Therefore MonitorAgent always catches the
 * exception and skips both the checking process and the action if any
 * exception is caught.
 *<br/><br/>
 * It is up to developers to implement both interfaces of MonitorReport
 * and MonitorAction.  The return object of
 * MonitorReport.generateReport(currentTime) is a Map.  It is
 * an opaque container for developers to put anything about the monitor report
 * in it.  It may be the status code indicating the latest status or anything,
 * or the timestamp of a file, or a ResultSet of a database query, etc, as long
 * as the associated action knows how to retrieve them.
 *<br/><br/>
 * MonitorAgent also supports shared information.  Some of the reports
 * can have their test results shared with others as the dependencies or
 * merged into a new report.  The shared information is called the internal
 * report that is a Map, too.  The other objects can access them via
 * ReportQuery report.
 *<br/><br/>
 * You can disable all the monitor groups via some monitor jobs.  To do that,
 * you have to define the report and specify the correct ReportMode.
 *<br/><br/>
 * MonitorAgent also can be configured to pick up its configuration
 * changes automatically from the configuration repository.  The object
 * needs to be defined in the default group. Its name must be specified within
 * ConfigRepository tag.
 *<br/><br/>
 * MonitorAgent can also host multiple message flows.  A message flow
 * is a message driven application for message broker service.  With the
 * message flow, you can have MonitorAgent to pick up and deliver
 * JMS messages via various transports.  In order to configure a message flow,
 * you need to define Receivers, Nodes and Persisters under MessageFlow list.
 * For detailed information, please reference the docs for QFlow.
 *<br/><br/>
 * The default MessageFlow is the flow with the name of default.  It is the
 * dedicated MessageFlow for event escalations.  It shoule be always the first
 * message flow if it is defined.  In this case, URL should not be defined in
 * the master configuration file since all the events will be escalated by the
 * default MessageFlow.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MonitorAgent implements Service, Runnable {
    private ThreadGroup[] pg = null;
    private Thread[] pool = null;
    private Thread admin = null;
    private String name;
    private String site;
    private String category;
    private String description, restartScript = null, statsURL = null;
    private String homeDir, rcField = "ReturnCode";
    private File completedFile;
    private int currentStatus, previousStatus, statusOffset, debug = 0;
    private int maxNumberThread, defaultHeartbeat, defaultTimeout, retry;
    private int maxRetry, capacity = MonitorGroup.GROUP_CAPACITY;
    private int debugForReload = 0;
    private long mtime;
    private int[] aPartition = new int[] {0, 0}; // for admin requests
    private int[] cPartition = new int[] {0, 0}; // for cluster escalations
    private int[] rPartition = new int[] {0, 0}; // for escalations to defaultXQ
    private int[] heartbeat = null;
    private AssetList groupList, flowList, jobList;
    private QList control = null;     // list for active job queues
    private XQueue[] jobPool = null;  // job queues
    private XQueue escalation = null; // xq for all escalations
    private XQueue defaultXQ = null;  // root xq for the default flow
    private ReservableCells idList;   // for protecting all escalations
    private MessageFlow defaultFlow = null;
    private MessageReceiver adminServer = null;
    private HTTPServer httpServer = null;
    private String[] displayPropertyName = null;
    private Map cachedProps;
    private Map<String, Object> includeMap, configRepository = null;
    private DateFormat zonedDateFormat;
    private static Map<String, Object> reports = new HashMap<String, Object>();
    private static ThreadLocal<Map> privateReport = new ThreadLocal<Map>();
    private boolean checkRunawayThread = true;

    private final static int JOB_GID = 0;
    private final static int JOB_TID = 1;
    private final static int JOB_SIZE = 2;
    private final static int JOB_RETRY = 3;
    private final static int JOB_COUNT = 4;
    private final static int JOB_TIME = 5;
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
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");
    protected final static String reportStatusText[] = {"Exception",
        "ExceptionInBlackout", "Disabled", "Blackout", "Normal",
        "Occurred", "Late", "VeryLate", "ExtremelyLate"};

    public MonitorAgent(Map props) {
        this(props, 0, null);
    }

    public MonitorAgent(Map props, int debugForReload, String configFile) {
        Object o;
        String key;
        int n, i, j, k, hmax, escalationCapacity = 0, groupSize = 32;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if (debugForReload > 0) // in case configRepository get reloaded
            this.debugForReload = debugForReload;

        if ((o = props.get("MaxNumberGroup")) != null)
            groupSize = Integer.parseInt((String) o);
        if (groupSize <= 0)
            groupSize = 32;

        if ((o = props.get("HomeDir")) == null)
            throw(new IllegalArgumentException("HomeDir is not defined"));
        homeDir = (String) o;

        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Description")) != null)
            description = (String) o;
        else
            description = "Monitor Agent";

        includeMap = new HashMap<String, Object>();
        mtime = System.currentTimeMillis();
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        // check repository monitor
        if ((o = props.get("ConfigRepository")) != null && o instanceof String){
            Map<String, Object> ph =
                Utils.cloneProperties((Map) props.get((String) o));
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

        if ((o = props.get("StatsURL")) != null && o instanceof String)
            statsURL = (String) o;

        if ((o = props.get("MaxNumberThread")) != null)
            maxNumberThread = Integer.parseInt((String) o);
        else
            maxNumberThread = 2;
        if (maxNumberThread <= 0)
            maxNumberThread = 2;

        if ((o = props.get("CheckRunawayThread")) != null &&
            "false".equalsIgnoreCase((String) o))
            checkRunawayThread = false;

        if ((o = props.get("Heartbeat")) != null)
            defaultHeartbeat = 1000 * Integer.parseInt((String) o);

        if (defaultHeartbeat <= 0)
            defaultHeartbeat = 300000;

        if ((o = props.get("Timeout")) != null)
            defaultTimeout = 1000 * Integer.parseInt((String) o);
        else
            defaultTimeout = defaultHeartbeat + defaultHeartbeat;

        if (defaultTimeout < 0)
            defaultTimeout = defaultHeartbeat + defaultHeartbeat;

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

        Map<String, String> h = new HashMap<String, String>();
        h.put("OS_NAME", MonitorUtils.OS_NAME);
        h.put("OS_ARCH", MonitorUtils.OS_ARCH);
        h.put("OS_VERSION", MonitorUtils.OS_VERSION);
        h.put("JAVA_VERSION", MonitorUtils.JAVA_VERSION);
        h.put("RELEASE_TAG", ReleaseTag.getReleaseTag());
        h.put("USER_NAME", MonitorUtils.USER_NAME);
        h.put("HOSTNAME", Event.getHostName());
        h.put("IP", Event.getIPAddress());
        h.put("PID", String.valueOf(Event.getPID()));
        h.put("HOSTID", Integer.toHexString(Event.getHostID()));
        h.put("PROGRAM", Event.getProgramName());
        h.put("MaxNumberGroup", String.valueOf(groupSize));
        h.put("TestTime", String.valueOf(mtime));
        h.put("TZ", TimeZone.getDefault().getID());
        h.put("Status", String.valueOf(TimeWindows.NORMAL));
        reports.put("SYSTEM", h);

        if ((o = props.get("EscalationCapacity")) != null)
            escalationCapacity = Integer.parseInt((String) o);
        if (escalationCapacity <= 0)
            escalationCapacity = 64;

        escalation = new IndexedXQueue("escalation", escalationCapacity);

        idList = null;
        if ((o = props.get("AdminServer")) != null) { // adminServer
            String className;
            Map<String, Object> ph = Utils.cloneProperties((Map) o);
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

                if ((o = ph.get("RestartScript")) != null)
                    restartScript = (String) o;
                else
                    restartScript = null;
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
                    throw(new IllegalArgumentException(name + 
                        " failed to start httpServer: " + Event.traceStack(e)));
                }
            }
        }
        else { // for external requests
            aPartition[0] = 0;
            aPartition[1] = escalationCapacity / 2;
            restartScript = null;
            idList = new ReservableCells("idList", escalationCapacity/2);
        }

        // for the default cluster escalations
        cPartition[0] = aPartition[0];
        cPartition[1] = aPartition[1];

        groupList = new AssetList(name, groupSize);
        if ((o = props.get("MonitorGroup")) != null && o instanceof List) {
            Map<String, Object> ph;
            List pl = null, list = (List) o;
            MonitorGroup group = null;
            long[] groupInfo;
            StringBuffer strBuf = ((debug & DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
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
                if (key.length() <= 0 || groupList.containsKey(key)) {
                    new Event(Event.ERR, name + ": duplicate group for " +
                        key).send();
                    ph.clear();
                    continue;
                }
                k = Utils.copyListProps("Monitor", ph, props);
                k = addGroup(mtime, ph);
                group = (MonitorGroup) groupList.get(k);
                if (group != null) {
                    new Event(Event.INFO, name +
                        ((group.isDynamic()) ? " dynamic group " : " group ") +
                        key + " has been created on " + k + " with " +
                        group.getSize() + " monitors").send();
                }
            }

            k = groupList.size();
            heartbeat = new int[k];
            Browser browser = groupList.browser();
            k = 0;
            while ((i = browser.next()) >= 0) {
                groupInfo = groupList.getMetaData(i);
                heartbeat[k++] = (int) groupInfo[GROUP_HBEAT];
                if ((debug & DEBUG_INIT) > 0)
                    strBuf.append("\n\t" + groupList.getKey(i) + ": " + i +" "+
                        groupInfo[GROUP_HBEAT]/1000 + " " +
                        groupInfo[GROUP_TIMEOUT]/1000 + " " +
                        groupInfo[GROUP_RETRY] + " " +
                        groupInfo[GROUP_SIZE] + " " + groupInfo[GROUP_STATUS]);
            }
            if (k <= 0)
                heartbeat = new int[] {defaultHeartbeat};
            else if ((debug & DEBUG_INIT) > 0)
                new Event(Event.DEBUG, name + " GroupName: ID HBEAT TIMEOUT " +
                    "RETRY SIZE STATUS - " +k+"/"+n + strBuf.toString()).send();
        }

        if (heartbeat != null)
            heartbeat = MonitorUtils.getHeartbeat(heartbeat);
        else
            heartbeat = new int[] {defaultHeartbeat};

        k = groupList.size();
        if (maxNumberThread < k)
            k = maxNumberThread;

        jobList = new AssetList(name, k);
        control = new QList(name, k);
        jobPool = new XQueue[k];
        long[] jobInfo;
        for (i=0; i<k; i++) {
            control.reserve(i);
            jobInfo = new long[JOB_TIME + 1];
            jobInfo[JOB_GID] = -1;
            jobInfo[JOB_TID] = i;
            jobInfo[JOB_SIZE] = 0;
            jobInfo[JOB_RETRY] = 0;
            jobInfo[JOB_COUNT] = 0;
            jobInfo[JOB_TIME] = mtime;
            control.add(jobInfo, i);
            jobPool[i] = new IndexedXQueue("pool_" + i, capacity);
            jobList.add(jobPool[i].getName(), jobInfo, jobPool[i], i);
            disableAck(jobPool[i]);
        }

        pg = new ThreadGroup[jobPool.length];
        pool = new Thread[jobPool.length];
        for (i=0; i<pool.length; i++) { // initialize jobPool thread groups
            pg[i] = new ThreadGroup("agentPool_" + i);
            pool[i] = new Thread(pg[i], this, "AgentPool_" + i);
            pool[i].setDaemon(true);
            pool[i].setPriority(Thread.NORM_PRIORITY);
        }

        // for message flows
        flowList = new AssetList(name, groupSize / 4 + 2);
        if ((o = props.get("MessageFlow")) != null && o instanceof List) {
            Map<String, Object> ph;
            List list = (List) o;
            StringBuffer strBuf = ((debug & DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
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

        cachedProps = props;
        currentStatus = SERVICE_READY;
        previousStatus = SERVICE_CLOSED;
        statusOffset = 0 - TimeWindows.EXCEPTION;
    }

    public void run() {
        int tid = -1, k = 0;
        String threadName = Thread.currentThread().getName();
        ThreadGroup thg = Thread.currentThread().getThreadGroup();

        if ("close".equals(threadName)) {
            close();

            new Event(Event.INFO, name + " terminated").send();
            return;
        }
        else if ("AdminServer".equals(threadName)) {
            if (adminServer != null) {
                new Event(Event.INFO, name + ": adminServer started").send();
                adminServer.receive(escalation, 0);
                adminServer.setStatus(MessageReceiver.RCVR_STOPPED);
                new Event(Event.INFO, name + ": adminServer stopped").send();
            }
            return;
        }
        else if ("manager".equals(threadName)) {
            try {
                start();
            }
            catch (Exception e) {
                new Event(Event.INFO, name + " aborted").send();
                close();
                return;
            }
            new Event(Event.INFO, name + " stopped").send();
            return;
        }

        if (threadName.indexOf("AgentPool_") == 0) {
            tid = Integer.parseInt(threadName.substring(10));
            tid %= jobPool.length;
        }
        else {
            new Event(Event.INFO, threadName + " stopped").send();
            return;
        }

        new Event(Event.INFO, threadName + ": started").send();

        XQueue xq = jobPool[tid];
        try {
            for (;;) {
                do {
                    String key = runJob(xq);
                    if (!checkRunawayThread)
                        continue;
                    if (keepRunning(xq)) {
                        int n = Thread.activeCount();
                        int m = thg.activeGroupCount();
                        if (n > 1 || m > 0) { // runaway threads?
                            new Event(Event.WARNING, name + " found " + n + "/"+
                                m + " active threads/groups in the group of " +
                                thg.getName() + " on " + key + ": " + k).send();
                            if (++k >= 3) {
                                int l = 0;
                                StringBuffer strBuf = new StringBuffer();
                                if (m > 0) {
                                    ThreadGroup[] group = new ThreadGroup[m+2];
                                    l = thg.enumerate(group);
                                    if (l > 0)
                                        strBuf.append("\n\tThreadGroup:");
                                    for (int i=0; i<l; i++)
                                        strBuf.append(" " + group[i].getName());
                                }
                                if (n > 0) {
                                    Thread[] list = new Thread[n+2];
                                    l = thg.enumerate(list);
                                    if (l > 0)
                                        strBuf.append("\n\tThread:");
                                    for (int i=0; i<l; i++)
                                        strBuf.append(" " + list[i].getName());
                                }
                                new Event(Event.NOTICE, threadName +": stopped"+
                                    " due to running away threads" +
                                    strBuf.toString()).send();
                                return;
                            }
                        }
                        else // reset counter
                            k = 0;
                    }
                } while (keepRunning(xq));
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    try {
                        Thread.sleep(6000);
                    }
                    catch (InterruptedException e) {
                    }
                }
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    try {
                        Thread.sleep(60000);
                    }
                    catch (InterruptedException e) {
                    }
                }
                if (keepRunning(xq)) continue;
                break;
            }
        }
        catch (Exception e) {
        }
        stopRunning(xq);
        new Event(Event.INFO, threadName + ": stopped").send();
    }

    private String runJob(XQueue xq) {
        Object o;
        int status, sid, skip, skippingStatus;
        long sampleTime, currentTime = 0L, st;
        Map<String, Object> latest = null;
        Map task;
        HTTPConnector httpConn = null;
        StringBuffer strBuf = new StringBuffer();
        MonitorReport report;
        MonitorAction action;
        Event event;
        TimeWindows tw;
        String[] keyList;
        long[] taskInfo;
        String rptName, gName = null;
        int reportMode = MonitorReport.REPORT_NONE;
        Exception ex;
        boolean withPrivateReport = false;

        if (statsURL != null) try {
            URI u = new URI(statsURL);
            if ("http".equals(u.getScheme()) || "https".equals(u.getScheme())) {
                Map<String, Object> h = new HashMap<String, Object>();
                h.put("URI", statsURL);
                h.put("IsPost", "true");
                h.put("SOTimeout", "30");
                httpConn = new HTTPConnector(h);
            }
        }
        catch (Exception e) {
            httpConn = null;
            new Event(Event.ERR, "failed to instantiate statsLogger: " +
                Event.traceStack(e)).send();
        }

        st = System.currentTimeMillis() + defaultHeartbeat;
        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((sid = xq.getNextCell(1000L)) < 0) {
                if (currentTime > st) // jump out of loop
                    break;
                currentTime = System.currentTimeMillis();
                continue;
            }

            currentTime = System.currentTimeMillis();
            task = (Map) xq.browse(sid);
            if (task == null) { // task is not supposed to be null
                new Event(Event.ERR, "null task from " + xq.getName()).send();
                xq.remove(sid);
                continue;
            }

            skip = MonitorReport.NOSKIP;
            report = (MonitorReport) task.get("Report");
            action = (MonitorAction) task.get("Action");
            tw = (TimeWindows) task.get("TimeWindow");
            taskInfo = (long[]) task.get("TaskInfo");
            keyList = (String[]) task.get("KeyList");
            if ((rptName = report.getReportName()) == null)
                rptName = (String) task.get("Name");
            gName = (String) task.get("GroupName");
            o = task.remove("PrivateReport");
            if (o != null && o instanceof Map) { // for private report
                privateReport.set((Map) o);
                withPrivateReport = true;
                if ((debug & DEBUG_LOOP) > 0)
                    new Event(Event.DEBUG, name + " added priavte report for " +
                        gName + " with " + ((Map) o).size()).send();
            }
            else
                withPrivateReport = false;

            ex = null;
            try {
                latest = report.generateReport(currentTime);
            }
            catch (Exception e) {
                if (withPrivateReport)
                    privateReport.remove();
                latest = new HashMap<String, Object>();
                skip = MonitorReport.EXCEPTION;
                ex = e;
            }
            if (withPrivateReport)
                privateReport.remove();

            // check if there is any stats data to send
            if (httpConn != null && latest.containsKey("Stats") &&
                (o = latest.get("Stats")) != null) {
                Event ev = new Event(Event.INFO, (String) o);
                ev.setAttribute("name", (String) task.get("Name"));
                ev.setAttribute("site", (String) task.get("Site"));
                ev.setAttribute("type", (String) task.get("Type"));
                ev.setAttribute("category", (String) task.get("Category"));
                httpConn.reconnect();
                try {
                    int rc =
                        httpConn.doPost(null, EventUtils.postable(ev), strBuf);
                    if (rc != HttpURLConnection.HTTP_OK)
                        new Event(Event.ERR, "failed to send stats for " +
                            (String) task.get("Name") + ": " + rc).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to send stats for " +
                        (String) task.get("Name") + ": " +
                        Event.traceStack(e)).send();
                }
                httpConn.close();
            }

            // check if we need to update shared report
            reportMode = report.getReportMode();
            if (reportMode == MonitorReport.REPORT_SHARED && keyList == null) {
                updateReport(rptName, currentTime, tw.check(currentTime, 0L),
                    report.getSkippingStatus());
                xq.remove(sid);
                if (taskInfo != null) {
                    taskInfo[MonitorGroup.TASK_COUNT] ++;
                    taskInfo[MonitorGroup.TASK_TIME] = currentTime;
                }
                continue;
            }

            if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                skip = MonitorReport.SKIPPED;

            // skip both check and the action if the skip flag is set
            skippingStatus = report.getSkippingStatus();
            if (skippingStatus == MonitorReport.SKIPPED ||
                skip == MonitorReport.SKIPPED) {
                xq.remove(sid);
                continue;
            }

            // exception handling, put exception to latest
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
                    if (ex != null)
                        latest.put("Exception", ex);
                }
            }
            else { //disabled
                status = tw.DISABLED;
            }

            if ((reportMode == MonitorReport.REPORT_SHARED &&
                keyList != null && keyList.length > 0) ||
                reportMode > MonitorReport.REPORT_SHARED) {
                latest.put("Status", String.valueOf(status));
                latest.put("SkippingStatus", String.valueOf(skippingStatus));
                latest.put("TestTime", String.valueOf(currentTime));
                updateReport(rptName, keyList, latest, reportMode);
                if (reportMode != MonitorReport.REPORT_SHARED) { // report only
                    xq.remove(sid);
                    if (taskInfo != null) {
                        taskInfo[MonitorGroup.TASK_COUNT] ++;
                        taskInfo[MonitorGroup.TASK_TIME] = currentTime;
                    }
                    continue;
                }
            }

            try {
                event = action.performAction(status, currentTime, latest);
            }
            catch (Exception e) {
                new Event(Event.ERR, "Exception in: " + action.getName() +
                    ": " + Event.traceStack(e)).send();
                xq.remove(sid);
                if (taskInfo != null) {
                    taskInfo[MonitorGroup.TASK_COUNT] ++;
                    taskInfo[MonitorGroup.TASK_TIME] = currentTime;
                }
                continue;
            }

            xq.remove(sid);
            if (taskInfo != null) {
                taskInfo[MonitorGroup.TASK_COUNT] ++;
                taskInfo[MonitorGroup.TASK_TIME] = currentTime;
            }

            if (event != null) { // escalation
                if (defaultXQ != null) try { // escalate to the default flow
                    int cid;
                    if (rPartition[1] > 0)
                        cid = defaultXQ.reserve(2000, rPartition[0],
                            rPartition[1]);
                    else
                        cid = defaultXQ.reserve(2000);
                    if (cid < 0) {
                        new Event(Event.ERR, name+" failed to reserve on "+
                            defaultXQ.getName() + " for event from " +
                            action.getName() + ": " + defaultXQ.depth() +
                            "/" + defaultXQ.size()).send();
                    }
                    else {
                        cid = defaultXQ.add(TextEvent.toTextEvent(event), cid);
                        if (cid < 0)
                            new Event(Event.ERR, name + " failed to escalate " +
                                 "event to " + defaultXQ.getName() + " for " +
                                 action.getName() + ": " + defaultXQ.depth() +
                                 "/" + defaultXQ.size()).send();
                    }
                }
                catch (Exception e) {
                    new Event(Event.ERR, name+" failed to escalate event to "+
                        defaultXQ.getName() + " for " + action.getName() +
                        ": " + Event.traceStack(e)).send();
                }
                event = null;
            }
        }

        return gName;
    }

    @SuppressWarnings("unchecked")
    private void updateReport(String rptName, long currentTime, int status,
        int skippingStatus) {
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

        synchronized (r) {
            if (keyList == null || keyList.length == 0) {
                Iterator iter = latest.keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0 || "Name".equals(key))
                        continue;
                    if ("SampleTime".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key,
                                Event.dateFormat(new Date(((long[]) o)[0])));
                        else
                            r.put(key, o);
                    }
                    else if ("SampleSize".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key, String.valueOf(((long[]) o)[0]));
                        else
                            r.put(key, o);
                    }
                    else if ("LatestTime".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key,
                                Event.dateFormat(new Date(((long[]) o)[0])));
                        else
                            r.put(key, o);
                    }
                    else if ("NumberLogs".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key, String.valueOf(((long[]) o)[0]));
                        else
                            r.put(key, o);
                    }
                    else
                        r.put(key, latest.get(key));
                }
            }
            else { // has keyList
                for (int i=0; i<keyList.length; i++) {
                    key = keyList[i];
                    if (key == null || key.length() <= 0 || "Name".equals(key))
                        continue;
                    if ("SampleTime".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key,
                                Event.dateFormat(new Date(((long[]) o)[0])));
                        else
                            r.put(key, o);
                    }
                    else if ("SampleSize".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key, String.valueOf(((long[]) o)[0]));
                        else
                            r.put(key, o);
                    }
                    else if ("LatestTime".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key,
                                Event.dateFormat(new Date(((long[]) o)[0])));
                        else
                            r.put(key, o);
                    }
                    else if ("NumberLogs".equals(key)) {
                        o = latest.get(key);
                        if (o != null && o instanceof long[])
                            r.put(key, String.valueOf(((long[]) o)[0]));
                        else
                            r.put(key, o);
                    }
                    else
                        r.put(key, latest.get(key));
                }
                if (latest.containsKey("Status"))
                    r.put("Status", latest.get("Status"));
                if (latest.containsKey("TestTime"))
                    r.put("TestTime", latest.get("TestTime"));
            }
        }

        if (reportMode < MonitorReport.REPORT_NODE)
            return;

        Event event;
        int status = Integer.parseInt((String) latest.get("Status"));
        if (reportMode >= MonitorReport.REPORT_NODE &&
            status >= TimeWindows.DISABLED) {
            event = new Event(Event.NOTICE);
            event.setAttribute("status",
                reportStatusText[status-TimeWindows.EXCEPTION]);
            if (reportMode == MonitorReport.REPORT_FLOW) // for flow
                event.setAttribute("category", "FLOW");
        }
        else {
            return;
        }

        if (reportMode <= MonitorReport.REPORT_FLOW) { // for node or flow
            if (keyList == null || keyList.length == 0) {
                Iterator iter = latest.keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    o = latest.get(key);
                    if ("SampleTime".equals(key)) {
                        event.setAttribute(key,
                            Event.dateFormat(new Date(((long[]) o)[0])));
                    }
                    else if ("SampleSize".equals(key)) {
                        event.setAttribute(key, String.valueOf(((long[])o)[0]));
                    }
                    else if ("TestTime".equals(key)) {
                        event.setAttribute(key, Event.dateFormat(
                            new Date(Long.parseLong((String) o))));
                    }
                    else if (key != null && o != null && o instanceof String) {
                        event.setAttribute(key, (String) o);
                    }
                }
            }
            else {
                for (int i=0; i<keyList.length; i++) {
                    key = keyList[i];
                    o = latest.get(key);
                    if ("SampleTime".equals(key)) {
                        event.setAttribute(key,
                            Event.dateFormat(new Date(((long[]) o)[0])));
                    }
                    else if ("SampleSize".equals(key)) {
                        event.setAttribute(key, String.valueOf(((long[])o)[0]));
                    }
                    else if ("TestTime".equals(key)) {
                        event.setAttribute(key, Event.dateFormat(
                            new Date(Long.parseLong((String) o))));
                    }
                    else if (key != null && o != null && o instanceof String) {
                        event.setAttribute(key, (String) o);
                    }
                }
            }
            event.setAttribute("text", rptName + " is " +
                reportStatusText[status - TimeWindows.EXCEPTION]);
        }
        else { // for clusterNode
            StringBuffer strBuf = new StringBuffer();
            strBuf.append("Report( ~ ): name ~ " + rptName);
            if (keyList == null || keyList.length == 0) {
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
                    else if ("TestTime".equals(key)) {
                        strBuf.append(Event.ITEM_SEPARATOR);
                        strBuf.append(key);
                        strBuf.append(Event.ITEM_SEPARATOR);
                        key = Event.dateFormat(
                             new Date(Long.parseLong((String) o)));
                        strBuf.append(key);
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
            else {
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
                    else if ("TestTime".equals(key)) {
                        strBuf.append(Event.ITEM_SEPARATOR);
                        strBuf.append(key);
                        strBuf.append(Event.ITEM_SEPARATOR);
                        key = Event.dateFormat(
                             new Date(Long.parseLong((String) o)));
                        strBuf.append(key);
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
        }
        event.setAttribute("name", rptName);

        if (reportMode >= MonitorReport.REPORT_NODE) {
            int cid = -1, e0 = cPartition[0] + cPartition[1];
            if (e0 > 0)
                cid = escalation.reserve(1000, e0, escalation.getCapacity()-e0);
            else
                cid = escalation.reserve(1000);
            if (cid >= 0)
                escalation.add(event, cid);
        }

        if ((debug & DEBUG_REPT) > 0)
            new Event(Event.DEBUG,"report: "+event.getAttribute("text")).send();
    }

    /** initializes internal shared reports for all monitors in a group */
    private int initReports(MonitorGroup group) {
        XQueue xq;
        int k, n;

        if (group == null || (group.getSize() <= 0 && !group.isDynamic()))
            return 0;

        n = group.getCapacity();
        xq = new IndexedXQueue(name, n);
        n = group.dispatch(xq);
        n = 0;
        if (xq != null && xq.size() > 0) {
            Object o;
            String rptName;
            MonitorReport report;
            Browser browser = xq.browser();
            while ((k = browser.next()) >= 0) { // init shared report
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
     * initializes the static shared report without keyList
     */
    public synchronized static void initReport(String rptName, long currentTime,
        int status, int skippingStatus) {
        if (rptName == null || reports.containsKey(rptName))
            return;

        Map<String, Object> r = new HashMap<String, Object>();
        r.put("Status", String.valueOf(status));
        r.put("SkippingStatus", String.valueOf(skippingStatus));
        r.put("TestTime", String.valueOf(currentTime));
        r.put("Name", rptName);
        reports.put(rptName, r);
    }

    /**
     * initializes the static shared report with keyList
     */
    public synchronized static void initReport(String rptName, String[] keyList,
        Map latest, int reportMode) {
        if (rptName == null || latest == null || reports.containsKey(rptName))
            return;

        Map<String, Object> r = Utils.cloneProperties(latest);
        r.put("Status", String.valueOf(TimeWindows.NORMAL));
        r.put("TestTime", String.valueOf(System.currentTimeMillis()));
        r.put("Name", rptName);
        reports.put(rptName, r);
    }

    /**
     * returns a copy of the shared report
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
     * returns all the names of the shared reports
     */
    public static String[] getReportNames() {
        return reports.keySet().toArray(new String[reports.size()]);
    }

    /**
     * returns the report map stored in the privateReport
     */
    public static Map getPrivateReport() {
        return privateReport.get();
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
     * The following attributes are required in the incoming event:
     *<br/>
     * priority     INFO for query, NOTICE for update and WARNING for admin<br/>
     * operation    action of the request: query,update,stop,disable,enable<br/>
     * category     category of the target<br/>
     * group        group of the target<br/>
     * name         name the target<br/>
     * type         type of response: json, xml or text<br/>
     * status       status
     */
    private int invokeAction(long currentTime, long sessionTime, Event event) {
        String key, target, str, text;
        MessageFlow flow = null;
        Event ev = null;
        long[] groupInfo;
        long tm;
        Object o;
        Browser browser;
        boolean isExternal, isRemote, isFlow = false;
        int i, k, n, action, status, size, id, type = 0;
        if (event == null)
            return currentStatus;

        isRemote = (event instanceof TextEvent);
        isExternal = (isRemote) ? true : false;
        if ((str = event.getAttribute("reqID")) != null) {
            try {
                id = Integer.parseInt(str);
                if (id >= 0)
                    isExternal = true;
            }
            catch (Exception e) {
            }
        }

        key = event.getAttribute("name");
        target = event.getAttribute("category");
        isFlow = ("FLOW".equals(target));

        if ((str = event.getAttribute("operation")) != null) {
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

        if ((str = event.getAttribute("type")) != null) {
            if ("xml".equalsIgnoreCase(str)) // expecting XML as response
                type = Utils.RESULT_XML;
            else if ("json".equalsIgnoreCase(str)) // expecting JSON as response
                type = Utils.RESULT_JSON;
        }

        tm = event.getTimestamp();
        switch (event.getPriority()) {
          case Event.WARNING: // state change event
            if ((debug & DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " got a message: " +
                    EventUtils.compact(event)).send();
            if (isFlow) // for FLOW
                id = flowList.getID(key);
            else if (name.equals(key)) // for Agent
                id = -1;
            else // for GROUP
                id = groupList.getID(key);
            switch (action) {
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
                    status = currentStatus;
                    if (isRemote) {
                        ev = new Event(Event.INFO, target + ": " + text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "0");
                    }
                    break;
                }
                else if (id >= 0) { // for group
                    status = currentStatus;
                    text = "restart on group is not supported";
                    if (isRemote) {
                        ev = new Event(Event.INFO, target + ": " + text);
                        ev.setAttribute("rc", "0");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.INFO);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "0");
                    }
                    break;
                }
                // for container
                if (restartScript == null) {
                    text = "restart script is not defined";
                    if (isRemote) {
                        ev = new Event(Event.WARNING, target + ": " + text);
                        ev.setAttribute("rc", "-1");
                    }
                    else if (isExternal) { // external event
                        event.setPriority(Event.WARNING);
                        event.setAttribute("text", target + ": " + text);
                        event.setAttribute("rc", "-1");
                    }
                    status = currentStatus;
                    break;
                }
                status = SERVICE_STOPPED;
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
                    ev = new Event(Event.WARNING, target+" restarted: "+text);
                    ev.setAttribute("rc", "-1");
                }
                else if (isExternal) { // external event
                    event.setPriority(Event.WARNING);
                    event.setAttribute("text", target + " restarted: " + text);
                    event.setAttribute("rc", "-1");
                }
                break;
              case EventAction.ACTION_STOP:
                if (isFlow && id >= 0) { // for flow
                    flow = (MessageFlow) flowList.get(id); 
                    status = flow.getStatus();
                    if (status >= SERVICE_READY && status < SERVICE_STOPPED) {
                        text = "manually stopped";
                        groupInfo = flowList.getMetaData(id);
                        flow.stop();
                        groupInfo[GROUP_STATUS] = flow.getStatus();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' " + text + " from " + statusText[status]).send();
                        text = key + " " + text;
                    }
                    else {
                        text = key + " already stopped";
                    }
                    status = currentStatus;
                }
                else if (isFlow) { // no such flow
                    text = "no such flow for " + key;
                    status = currentStatus;
                }
                else if (id >= 0) { // for group
                    text = "stop on group is not supported";
                    status = currentStatus;
                }
                else { // for container
                    text = "stopping";
                    new Event(Event.INFO, name + ": " + text +
                        " from " + statusText[currentStatus]).send();
                    status = SERVICE_STOPPED;
                    previousStatus = currentStatus;
                    mtime = currentTime;
                }
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else if (isExternal) { // external event
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "0");
                }
                break;
              case EventAction.ACTION_ENABLE:
                if (isFlow && id >= 0) { // for flow
                    flow = (MessageFlow) flowList.get(id); 
                    status = flow.getStatus();
                    if (status >= SERVICE_READY && status <= SERVICE_RETRYING)
                        text = key + " already enabled";
                    else if (status <= SERVICE_DISABLED) {
                        text = "manually enabled";
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.enable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' " + text + " from " + statusText[status]).send();
                        text = key + " " + text;
                    }
                    else {
                        text = key + " stopped or closed";
                    }
                    status = currentStatus;
                }
                else if (isFlow) { // no such flow
                    text = "no such flow for " + key;
                    status = currentStatus;
                }
                else if (id >= 0) { // for group
                    groupInfo = groupList.getMetaData(id);
                    status = (int) groupInfo[GROUP_STATUS];
                    if (status >= SERVICE_READY && status <= SERVICE_RETRYING)
                        text = key + " already enabled";
                    else if (status <= SERVICE_DISABLED) {
                        text = "manually enabled";
                        groupInfo[GROUP_STATUS] = SERVICE_READY;
                        groupInfo[GROUP_TIME] = currentTime;
                        new Event(Event.INFO, name + ": group '" + key +
                            "' " + text + " from " + statusText[status]).send();
                        text = key + " " + text;
                    }
                    else {
                        text = key + " stopped or closed";
                    }
                    status = currentStatus;
                }
                else if (currentStatus >= SERVICE_READY &&
                    currentStatus <= SERVICE_RETRYING) { // for container
                    text = "already enabled";
                    status = currentStatus;
                }
                else if (currentStatus <= SERVICE_DISABLED) { // for container
                    text = "manually enabled";
                    status = SERVICE_RUNNING;
                    previousStatus = currentStatus;
                    mtime = currentTime;
                    browser = groupList.browser();
                    while ((id = browser.next()) >= 0) { // reset group status
                        if ("default".equals(groupList.getKey(id)))
                            continue;
                        groupInfo = groupList.getMetaData(id);
                        if (groupInfo[GROUP_STATUS] == SERVICE_DISABLED) {
                            groupInfo[GROUP_STATUS] = SERVICE_READY;
                            groupInfo[GROUP_TIME] = currentTime;
                        }
                    }
                    new Event(Event.INFO, name + ": all groups " + text +
                        " from " + statusText[currentStatus]).send();
                }
                else { // for container
                    text = "stopped or closed";
                    status = currentStatus;
                }
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else if (isExternal) { // external event
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "0");
                }
                break;
              case EventAction.ACTION_DISABLE:
                if (isFlow && id >= 0) { // for flow
                    flow = (MessageFlow) flowList.get(id); 
                    status = flow.getStatus();
                    if (status >= SERVICE_READY && status < SERVICE_DISABLED) {
                        text = "manually disabled";
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.disable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' " + text + " from " + statusText[status]).send();
                        text = key + " " + text;
                    }
                    else if (status <= SERVICE_DISABLED) {
                        text = key + " already disabled";
                    }
                    else {
                        text = key + " stopped or closed";
                    }
                    status = currentStatus;
                }
                else if (isFlow) { // no such flow
                    text = "no such flow for " + key;
                    status = currentStatus;
                }
                else if (id >= 0) { // for group
                    groupInfo = groupList.getMetaData(id);
                    status = (int) groupInfo[GROUP_STATUS];
                    if (status >= SERVICE_READY && status < SERVICE_DISABLED) {
                        text = "manually disabled";
                        groupInfo[GROUP_STATUS] = SERVICE_DISABLED;
                        groupInfo[GROUP_TIME] = currentTime;
                        new Event(Event.INFO, name + ": group '" + key +
                            "' " + text + " from " + statusText[status]).send();
                        text = key + " " + text;
                    }
                    else if (status <= SERVICE_DISABLED) {
                        text = key + " already disabled";
                    }
                    else {
                        text = key + " stopped or closed";
                    }
                    status = currentStatus;
                }
                else if (currentStatus >= SERVICE_READY &&
                    currentStatus < SERVICE_DISABLED) {
                    text = "manually disabled";
                    status = SERVICE_DISABLED;
                    previousStatus = currentStatus;
                    mtime = currentTime;
                    browser = groupList.browser();
                    while ((id = browser.next()) >= 0) { // reset group status
                        if ("default".equals(groupList.getKey(id)))
                            continue;
                        groupInfo = groupList.getMetaData(id);
                        if (groupInfo[GROUP_STATUS] == SERVICE_READY) {
                            groupInfo[GROUP_STATUS] = SERVICE_DISABLED;
                            groupInfo[GROUP_TIME] = currentTime;
                        }
                    }
                    new Event(Event.INFO, name + ": all groups " + text +
                        " from " + statusText[currentStatus]).send();
                }
                else if (currentStatus <= SERVICE_DISABLED) { // for  container
                    text = "already disabled";
                    status = currentStatus;
                }
                else { // for container 
                    text = "not necessary since it is stopped or closed"; 
                    status = currentStatus;
                }
                if (isRemote) {
                    ev = new Event(Event.INFO, target + ": " + text);
                    ev.setAttribute("rc", "0");
                }
                else if (isExternal) { // external event
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
                else if (isExternal) { // external event
                    event.setPriority(Event.WARNING);
                    event.setAttribute("text", target + ": " + text);
                    event.setAttribute("rc", "-1");
                }
                status = currentStatus;
                break;
            }
            break;
          case Event.NOTICE: // internal event to update state
            if ((str = event.getAttribute("status")) != null) {
                if ("disabled".equalsIgnoreCase(str))
                    status = SERVICE_DISABLED;
                else if ("normal".equalsIgnoreCase(str))
                    status = SERVICE_RUNNING;
                else
                    return currentStatus;
            }
            else
                return currentStatus;

            if ((debug & DEBUG_CTRL) > 0 && (debug & DEBUG_UPDT) > 0)
                new Event(Event.DEBUG, name + " got an escalation: " +
                    EventUtils.compact(event)).send();

            if (isFlow) // for FLOW
                id = flowList.getID(key);
            else if (name.equals(key)) // for Agent
                id = -1;
            else if ("default".equals(key)) { // no action on the default group
                status = currentStatus;
                break;
            }
            else
                id = groupList.getID(key);

            switch (status) {
              case SERVICE_RUNNING:
                if (isFlow && id >= 0) { // for flow
                    flow = (MessageFlow) flowList.get(id); 
                    status = flow.getStatus();
                    if(status > SERVICE_RETRYING && status <= SERVICE_DISABLED){
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.enable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' enabled from " + statusText[status]).send();
                    }
                    status = currentStatus;
                }
                else if (isFlow) { // no such flow
                    status = currentStatus;
                }
                else if (id >= 0) { // for groups
                    groupInfo = groupList.getMetaData(id);
                    status =  (int) groupInfo[GROUP_STATUS];
                    if (status > SERVICE_RETRYING && status <= SERVICE_STANDBY){
                        new Event(Event.INFO, name + ": group '" + key +
                            "' enabled from " + statusText[status]).send();
                        groupInfo[GROUP_STATUS] = SERVICE_READY;
                        groupInfo[GROUP_TIME] = currentTime;
                    }
                    status = currentStatus;
                }
                else if (currentStatus > SERVICE_RETRYING &&
                    currentStatus <= SERVICE_STANDBY) { // for container
                    browser = groupList.browser();
                    while ((id = browser.next()) >= 0) { // reset group status
                        if ("default".equals(groupList.getKey(id)))
                            continue;
                        groupInfo = groupList.getMetaData(id);
                        if (groupInfo[GROUP_STATUS] == SERVICE_STANDBY) {
                            groupInfo[GROUP_STATUS] = SERVICE_READY;
                            groupInfo[GROUP_TIME] = currentTime;
                        }
                    }
                    new Event(Event.INFO, name + ": all groups enabled" +
                        " from " + statusText[currentStatus]).send();
                    status = SERVICE_RUNNING;
                    previousStatus = currentStatus;
                    mtime = currentTime;
                }
                else
                    status = currentStatus;
                break;
              case SERVICE_STANDBY:
              case SERVICE_DISABLED:
                if (isFlow && id >= 0) { // for flow
                    flow = (MessageFlow) flowList.get(id); 
                    status = flow.getStatus();
                    if (status >= SERVICE_READY && status < SERVICE_DISABLED) {
                        groupInfo = flowList.getMetaData(id);
                        groupInfo[GROUP_STATUS] = flow.disable();
                        new Event(Event.INFO, name + ": flow '" + key +
                            "' disabled from " + statusText[status]).send();
                    }
                    status = currentStatus;
                }
                else if (isFlow) { // no such flow
                    status = currentStatus;
                }
                else if (id >= 0) { // for groups
                    groupInfo = groupList.getMetaData(id);
                    status =  (int) groupInfo[GROUP_STATUS];
                    if (status >= SERVICE_READY && status < SERVICE_STANDBY) {
                        new Event(Event.INFO, name + ": group '" + key +
                            "' disabled from " + statusText[status]).send();
                        groupInfo[GROUP_STATUS] = SERVICE_STANDBY;
                        groupInfo[GROUP_TIME] = currentTime;
                    }
                    status = currentStatus;
                }
                else if (currentStatus >= SERVICE_READY &&
                    currentStatus < SERVICE_STANDBY) { // for container
                    browser = groupList.browser();
                    while ((id = browser.next()) >= 0) { // reset group status
                        if ("default".equals(groupList.getKey(id)))
                            continue;
                        groupInfo = groupList.getMetaData(id);
                        if (groupInfo[GROUP_STATUS] == SERVICE_READY) {
                            groupInfo[GROUP_STATUS] = SERVICE_STANDBY;
                            groupInfo[GROUP_TIME] = currentTime;
                        }
                    }
                    new Event(Event.INFO, name + ": all groups disabled" +
                        " from " + statusText[currentStatus]).send();
                    status = SERVICE_STANDBY;
                    previousStatus = currentStatus;
                    mtime = currentTime;
                }
                else
                    status = currentStatus;
                break;
              default:
                status = currentStatus;
                break;
            }
            break;
          case Event.INFO: // query event
            if ((debug & DEBUG_UPDT) > 0 && (debug & DEBUG_TRAN) > 0)
                new Event(Event.DEBUG, name + " got an escalation: " +
                    EventUtils.compact(event)).send();
            Map h = null;
            status = currentStatus;
            if ("HELP".equals(target) || "USAGE".equals(target) || key == null)
                key = target;
            text = null;
            if (isFlow && action == EventAction.ACTION_LIST) {
                flow = (MessageFlow) flowList.get(key);
                if (flow != null)
                    text = flow.list();
            }
            else if ("PROPERTIES".equals(target) && // for a specific property
                (o = cachedProps.get(key)) != null && o instanceof Map) {
                h = Utils.cloneProperties((Map) o);
            }
            else {
                h = queryInfo(currentTime, sessionTime, target, key, type);
            }
            if (h != null) {
                if ((type & Utils.RESULT_JSON) > 0) { // for json
                    if ("XQUEUE".equals(target)) // for XQ of a flow
                        text = (h.size() != 1) ? JSON2Map.toJSON(h) :
                            "{\"Record\": " + (String) h.get("XQ") + "}";
                    else if (h.size() == 1) // for list of records
                        text = "{\"Record\": " + (String) h.get(target) + "}";
                    else // for hash
                        text = JSON2Map.toJSON(h);
                    ev = new Event(Event.INFO, text);
                    h.clear();
                }
                else if ((type & Utils.RESULT_XML) > 0) { // for xml
                    if ("XQUEUE".equals(target)) // for XQ of a flow
                        text = (h.size() == 1) ? (String) h.get("XQ") :
                            JSON2Map.toXML(h);
                    else if (h.size() == 1) // for list of records
                        text = (String) h.get(target);
                    else // for hash
                        text = JSON2Map.toXML(h);
                    ev = new Event(Event.INFO, "<Result>" + text + "</Result>");
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
                else if (isExternal) { // external event
                    event.setPriority(Event.INFO);
                    event.setAttribute("text", target + ": query OK");
                    event.setAttribute("rc", "0");
                    event.setBody(h);
                }
            }
            else if (text != null) { // for list
                ev = new Event(Event.INFO, text);
            }
            else {
                if (isRemote && (type & Utils.RESULT_JSON) > 0) {
                    ev = new Event(Event.ERR, "{\"Error\":\"" + target +
                        ": not found " + key + "\",\"RC\":-1}");
                }
                else if (isRemote && (type & Utils.RESULT_XML) > 0) {
                    ev = new Event(Event.ERR, "<Result><Error>" + target +
                        ": not found " + key + "</Error><RC>-1</RC></Result>");
                }
                else if (isRemote) {
                    ev = new Event(Event.ERR, target+": not found "+key);
                    ev.setAttribute("rc", "-1");
                }
                else if (isExternal) { // external event
                    event.setPriority(Event.ERR);
                    event.setAttribute("text",target+": not found "+key);
                    event.setAttribute("rc", "-1");
                    event.setBody(null);
                }
            }
            break;
          default:
            status = currentStatus;
            break;
        }

        if (isRemote) {
            if (ev == null) {
                ev = new Event(Event.WARNING, "bad request on " + target);
                ev.setAttribute("rc", "-1");
            }
            str = event.getAttribute("operation");
            ev.setAttribute("category", target);
            ev.setAttribute("name", key);
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

        return status;
    }

    private void defaultGroup(long sessionTime) {
        Object o;
        MonitorGroup group;
        long[] groupInfo, jobInfo;
        long currentTime;
        int i, j, n;
        int gid = 0; // the id of the monitor group
        int tid = -1;// the id of the thread or job queue
        int cid = -1;// the id of the cell

        gid = groupList.getID("default");
        if (gid < 0) // return if no default group
            return;
        group = (MonitorGroup) groupList.get(gid);
        groupInfo = groupList.getMetaData(gid);

        if (groupInfo[GROUP_STATUS] != SERVICE_READY) // not ready
            return;

        if (group.getSize() > 0) { // default group not empty
            for (j=0; j<10; j++) {
                if ((tid = control.reserve()) >= 0)
                    break;
                try {
                    Thread.sleep(500L);
                }
                catch (Exception e) {
                }
                update(control, jobPool);
            }
        }
        else
            tid = -1;

        n = 0;
        currentTime = System.currentTimeMillis();
        if (tid >= 0 && jobPool[tid].size() == 0) { //special case for the group
            jobInfo = jobList.getMetaData(tid);
            control.add(jobInfo, tid);
            n = group.dispatch(jobPool[tid]);
            groupInfo[GROUP_TID] = tid;
            groupInfo[GROUP_SPAN] = 0;
            groupInfo[GROUP_STATUS] = SERVICE_RUNNING;
            groupInfo[GROUP_TIME] = currentTime;
            jobInfo[JOB_GID] = gid;
            jobInfo[JOB_SIZE] = n;
            jobInfo[JOB_TIME] = currentTime;

            do { // wait until all jobs completed
                if ((cid = escalation.getNextCell(1000L)) >= 0) {//process event
                    Event event;
                    if ((event = (Event) escalation.browse(cid)) == null) {
                        escalation.remove(cid);
                        new Event(Event.WARNING, name +
                            ": null event from escalation channel").send();
                    }
                    else { // got an escalation
                        try {
                            currentStatus = invokeAction(currentTime,
                                sessionTime, event);
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name+": failed to respond"+
                                " to an event: "+Event.traceStack(e)).send();
                        }
                        if (event instanceof TextEvent) try {
                            ((TextEvent) event).acknowledge();
                        }
                        catch (Exception e) {
                        }
                        escalation.remove(cid);
                        if (event.getPriority() != Event.INFO)
                            display(DEBUG_LOAD);
                        if (currentStatus == SERVICE_STOPPED) {
                            try {
                                Thread.sleep(1000L);
                            }
                            catch (Exception e) {
                            }
                            break;
                        }
                    }
                }
            } while (jobPool[tid].size() > 0 && keepRunning(escalation));
        }
        else if (tid >= 0) {
            control.cancel(tid);
        }
    }

    /**
     * starts the main workflow and controls all groups of jobs and msg flows.
     * It dispatches the job groups according to their schedules while
     * listening on the escalation queue for escalations.
     */
    public void start() {
        Object o;
        Browser browser;
        MonitorGroup group;
        MessageFlow flow;
        String gName;
        long[] groupInfo, jobInfo, flowInfo;
        long sessionTime, currentTime;
        int count = 0;
        int i, j, k, cid, min = 0, size, leftover;
        int[] activeGroup;
        int gid = 0; // the id of the monitor group
        int tid = 0; // the id of the thread or job queue
        int hid = 0; // the id of the heartbeat array
        int n, hbeat, defaultHBeat;
        StringBuffer strBuf = null;
        boolean inDetail = ((debug & DEBUG_LOOP) > 0);

        for (i=0; i<pool.length; i++) { // start jobPool threads
            pool[i].start();
        }

        previousStatus = currentStatus;
        currentStatus = SERVICE_RUNNING;
        mtime = System.currentTimeMillis();
        sessionTime = mtime;
        if (defaultFlow != null) {
            defaultFlow.start();
            i = flowList.getID("default");
            if (i >= 0) {
                flowInfo = flowList.getMetaData(i);
                flowInfo[GROUP_STATUS] = defaultFlow.getStatus();
                flowInfo[GROUP_TIME] = sessionTime;
            }
        }

        defaultGroup(sessionTime);
        if (currentStatus == SERVICE_STOPPED) {
            close();
            return;
        }
        groupInfo = (long[]) groupList.getMetaData("default");
        defaultHBeat = (int) groupInfo[GROUP_HBEAT];

        activeGroup = new int[groupList.size()];
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        browser = flowList.browser();
        while ((i = browser.next()) >= 0) { // start all flows
            flow = (MessageFlow) flowList.get(i);
            if ("default".equals(flow.getName()))
                continue;
            flowInfo = flowList.getMetaData(i);
            switch (flow.getDefaultStatus()) {
              case SERVICE_RUNNING:
                flow.start();
                break;
              case SERVICE_PAUSE:
              case SERVICE_STANDBY:
              case SERVICE_DISABLED:
                flow.disable();
                break;
              case SERVICE_STOPPED:
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
        new Event(Event.INFO, name + ": " + activeGroup.length +
            " groups started with the build of " +
            ReleaseTag.getReleaseTag()).send();

        strBuf = new StringBuffer();
        browser = groupList.browser();
        // set all groups but the first active at startup
        hid = heartbeat.length - 1;
        hbeat = heartbeat[hid];
        leftover = 0;
        n = 0;
        while (keepRunning(escalation)) {
            if ((cid = escalation.getNextCell(1000L)) >= 0) { // process event
                Event event;
                if ((event = (Event) escalation.browse(cid)) == null) {
                    escalation.remove(cid);
                    new Event(Event.WARNING, name +
                        ": null event from escalation channel").send();
                }
                else { // got an escalation
                    try {
                        currentStatus = invokeAction(currentTime,
                            sessionTime, event);
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING,name + ": failed to respond "+
                            "to an event: "+Event.traceStack(e)).send();
                    }
                    if (event instanceof TextEvent) try {
                        ((TextEvent) event).acknowledge();
                    }
                    catch (Exception e) {
                    }
                    escalation.remove(cid);
                    if (currentStatus == SERVICE_STOPPED) { // stopped
                        try {
                            Thread.sleep(1000);
                        }
                        catch (Exception e) {
                        }
                        break;
                    }
                }
            }
            currentTime = System.currentTimeMillis();
            if (currentTime < sessionTime) { // session not due yet
                if (++count > 5) {
                    count = 0;
                    if (n > 0 && control.size() > 0)
                        update(control, jobPool);
                }
                continue;
            }

            inDetail = ((debug & DEBUG_LOOP) > 0);
            if (n > 0)
                update(control, jobPool);

            if (leftover <= 0) { // session starting
                n = 0;
                browser.reset();
                while ((gid = browser.next()) >= 0) { // update active groups
                    groupInfo = groupList.getMetaData(gid);
                    if ((hbeat % (int) groupInfo[GROUP_HBEAT]) != 0)
                        continue;
                    if ((tid = (int) groupInfo[GROUP_TID]) >= 0) { // active
                        gName = groupList.getKey(gid);
                        jobInfo = (long[]) jobList.getMetaData(tid);
                        if (groupInfo[GROUP_TIMEOUT] > 0 && currentTime >=
                            groupInfo[GROUP_TIME] + groupInfo[GROUP_TIMEOUT]) {
                            jobInfo[JOB_RETRY] ++;
                            if ((int)groupInfo[GROUP_STATUS]==SERVICE_RUNNING ||
                                (int)groupInfo[GROUP_STATUS]==SERVICE_READY)
                                groupInfo[GROUP_STATUS] = SERVICE_RETRYING;
                            j = (int) jobInfo[JOB_SIZE] - jobPool[tid].size();
                            o = jobPool[tid].browse(j);
                            if (o == null || !(o instanceof Map))
                                continue;
                            String key = (String) ((Map) o).get("Name");
                            if (jobInfo[JOB_RETRY] <= maxRetry) {
                                new Event(Event.WARNING,"group: "+ gName +
                                    " timedout with the job '" + key +
                                    "' stuck on AgentPool_" + tid).send();
                            }
                            else {
                                new Event(Event.ERR, "group: "+ gName +
                                    " timedout with the job '" + key +
                                    "' stuck on AgentPool_" + tid).send();
                                stopRunning(jobPool[tid]);
                                jobPool[tid].clear();
                                if (pool[tid].isAlive()) {
                                    pool[tid].interrupt();
                                    try {
                                       pool[tid].join(5000);
                                    }
                                    catch (Exception e) {
                                    }
                                }
                                jobInfo[JOB_TID] += jobPool.length;
                                jobPool[tid] = new IndexedXQueue("pool_" +
                                    jobInfo[JOB_TID], capacity);
                                jobList.set(tid, jobPool[tid]);
                                pool[tid] =new Thread(pg[tid],this,"AgentPool_"+
                                    jobInfo[JOB_TID]);
                                pool[tid].setDaemon(true);
                                pool[tid].setPriority(Thread.NORM_PRIORITY);
                                resumeRunning(jobPool[tid]);
                                pool[tid].start();
                                jobInfo[JOB_RETRY] = 0;
                                if ((int)groupInfo[GROUP_STATUS] ==
                                    SERVICE_RETRYING)
                                    groupInfo[GROUP_STATUS] = SERVICE_READY;
                            }
                        }
                        continue;
                    }
                    // add to active group
                    activeGroup[n++] = gid;
                }
            }

            if (currentStatus == SERVICE_RUNNING) { // not disabled
                if (leftover > 0) // jumped back
                    j = n - leftover;
                else
                    j = 0;
                leftover = 0;
                for (i=j; i<n; i++) { // assign active groups to empty xq
                    gid = activeGroup[i];
                    groupInfo = groupList.getMetaData(gid);
                    gName = groupList.getKey(gid);
                    if ((int) groupInfo[GROUP_STATUS] != SERVICE_READY ||
                        (groupInfo[GROUP_SIZE] <= 0L &&
                        groupInfo[GROUP_TYPE] != 1L))
                        continue;
                    for (j=0; j<10; j++) {
                        if ((tid = control.reserve()) >= 0)
                            break;
                        try {
                            Thread.sleep(500L);
                        }
                        catch (Exception e) {
                        }
                        update(control, jobPool);
                    }
                    if (tid < 0) { // all threads are busy
                        leftover = n-i;
                        break;
                    }
                    if (jobPool[tid].size() > 0) {// jobs still running in xq?
                        i --;
                        control.cancel(tid);
                        new Event(Event.WARNING, "redispatch the group " +
                            gName + " on the non-empty pool " + tid + " - " +
                            jobPool[tid].getName() + ": " +jobPool[tid].size()+
                            ":" + jobPool[tid].depth()).send();
                        update(control, jobPool);
                        continue;
                    }
                    if (inDetail)
                        strBuf.append("\n\t" + gName + ": " + gid + " " + tid);
                    jobInfo = jobList.getMetaData(tid);
                    j = control.add(jobInfo, tid);
                    if (j <= 0) { // failed to add the group 
                        control.cancel(tid);
                        new Event(Event.ERR, "failed to dispatch the group " +
                            gName + " on the reserved cell " + tid + " of XQ " +
                            control.getName() + ": " + control.size() + ":" +
                            control.depth()).send();
                        update(control, jobPool);
                        continue;
                    }
                    currentTime = System.currentTimeMillis();
                    group = (MonitorGroup) groupList.get(gid);
                    j = group.dispatch(jobPool[tid]);
                    if (group.isDynamic())
                        groupInfo[GROUP_SIZE] = group.getSize();
                    if (j > 0) { // jobs dispatched
                        groupInfo[GROUP_TID] = tid;
                        groupInfo[GROUP_SPAN] = 0;
                        groupInfo[GROUP_STATUS] = SERVICE_RUNNING;
                        groupInfo[GROUP_TIME] = currentTime;
                        jobInfo[JOB_GID] = gid;
                        jobInfo[JOB_SIZE] = j;
                        jobInfo[JOB_TIME] = currentTime;
                        jobInfo[JOB_RETRY] = 0;
                    }
                    else { // empty group
                        control.getNextID(tid);
                        control.remove(tid);
                    }
                }
                if (leftover > 0) // go back to check incoming events
                    continue;
            }
            else { // for default group in disabled mode
                if (leftover > 0) // jumped back
                    j = n - leftover;
                else
                    j = 0;
                leftover = 0;
                if (j == 0 && n > 0 && hbeat % defaultHBeat == 0) {
                    defaultGroup(sessionTime);
                    if (currentStatus == SERVICE_STANDBY ||
                        currentStatus == SERVICE_DISABLED) {
                        // make sure group status in sync with currentStatus
                        browser.reset();
                        while ((gid = browser.next()) >= 0){//reset group status
                            if ("default".equals(groupList.getKey(gid)))
                                continue;
                            groupInfo = groupList.getMetaData(gid);
                            if (groupInfo[GROUP_STATUS] == SERVICE_READY) {
                                groupInfo[GROUP_STATUS] = currentStatus;
                                groupInfo[GROUP_TIME] = currentTime;
                            }
                        }
                    }
                    else if (currentStatus == SERVICE_STOPPED) {
                        break;
                    }
                }
                n = 0;
            }
            leftover = 0;

            update(control, jobPool);
            if (inDetail && strBuf.length() > 0) {
                new Event(Event.DEBUG, "ActiveGroup: GID HID - " + n + " " +
                    hid + " " + control.size() + ":" + control.depth() +
                    strBuf.toString()).send();
                strBuf = new StringBuffer();
            }

            if (hbeat % defaultHBeat == 0) { // default session ends here
                if (completedFile != null)
                    touch(completedFile);
                if (configRepository != null) {
                    retry = 0;
                    Map props = getNewProperties(currentTime, null);
                    retry = 0;
                    if (props != null && props.size() > 0) { //with changes
                        reload(props);
                        props = null;
                        activeGroup = new int[groupList.size()];
                        defaultGroup(sessionTime);
                        if (currentStatus == SERVICE_STOPPED) {
                            break;
                        }
                        groupInfo = groupList.getMetaData("default");
                        defaultHBeat = (int) groupInfo[GROUP_HBEAT];
                        hid = heartbeat.length - 1;
                        hbeat = heartbeat[hid];
                        currentTime = System.currentTimeMillis();
                        sessionTime = currentTime;
                        n = 0;
                        continue;
                    }
                }
                if (currentStatus < SERVICE_STOPPED) {
                    for (i=0; i<pool.length; i++) { // check jobPool threads
                        if (pool[i] != null && pool[i].isAlive())
                            continue;
                        pool[i] = new Thread(pg[i], this, pool[i].getName());
                        pool[i].setDaemon(true);
                        pool[i].setPriority(Thread.NORM_PRIORITY);
                        pool[i].start();
                        new Event(Event.NOTICE, name + " thread " +
                            pool[i].getName() + " is restarted").send();
                    }
                    if (adminServer != null && keepRunning(escalation) &&
                        admin != null && !admin.isAlive() &&
                        adminServer.getStatus()==MessageReceiver.RCVR_STOPPED){
                        adminServer.setStatus(MessageReceiver.RCVR_READY);
                        admin = new Thread(this, "AdminServer");
                        admin.setPriority(Thread.NORM_PRIORITY);
                        admin.start();
                    }
                }
            }

            if (currentStatus == SERVICE_STOPPED)
                break;

            hid ++;
            if (hid >= heartbeat.length) { // reset session
                hid = 0;
                sessionTime += heartbeat[hid];
            }
            else {
                sessionTime += heartbeat[hid] - hbeat;
            }
            hbeat = heartbeat[hid];
            currentTime = System.currentTimeMillis();
            if (currentTime > sessionTime) // reset sessionTime
                sessionTime = currentTime;
        }

        close();
    }

    /**
     * updates control and job pools as well as the group info and returns the
     * number of the done queues.  It scans all active groups to see if its
     * job queue is done.  If one is done, it refreshes the status of control
     * so that it is available for other groups.
     */
    private int update(QList list, XQueue[] out) { 
        Object o;
        Browser browser;
        long[] jobInfo, groupInfo = null;
        int i, k, n, gid, tid;
        long t;

        if ((n = list.size()) <= 0)
            return 0;

        k = 0;
        t = System.currentTimeMillis();
        browser = list.browser();
        while ((tid = browser.next()) >= 0) { // tid is active
            o = list.browse(tid);
            if (o == null || !(o instanceof long[])) {
                list.getNextID(tid);
                list.remove(tid);
                continue;
            }
            if (out[tid].size() > 0) { // jobs not done yet
                k++;
                continue;
            }

            list.getNextID(tid);
            list.remove(tid);
            jobInfo = (long[]) o;
            gid = (int) jobInfo[JOB_GID];
            jobInfo[JOB_GID] = -1;
            jobInfo[JOB_SIZE] = 0;
            jobInfo[JOB_COUNT] ++;
            jobInfo[JOB_TIME] = t;
            if (gid < 0) { // look for the gid for tid
                Browser b = groupList.browser();
                while ((i = b.next()) >= 0) {
                    groupInfo = groupList.getMetaData(i);
                    if (groupInfo != null && groupInfo.length > GROUP_TIME &&
                        tid == (int) groupInfo[GROUP_TID]) {
                        gid = i;
                        break;
                    }
                }
                if (gid < 0) // nobody using tid
                    continue;
            }
            else {
                groupInfo = groupList.getMetaData(gid);
                if (groupInfo == null || groupInfo.length <= GROUP_TIME)
                    continue;
            }
            groupInfo[GROUP_TID] = -1;
            groupInfo[GROUP_COUNT] ++;
            groupInfo[GROUP_SPAN] = t - groupInfo[GROUP_TIME];
            if ((int) groupInfo[GROUP_STATUS] == SERVICE_RUNNING ||
                (int) groupInfo[GROUP_STATUS] == SERVICE_RETRYING)
                groupInfo[GROUP_STATUS] = SERVICE_READY;
        }

        return n - k;
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
        if (props != null) { // with changes
            String operation = null;
            String key = pm.getName();
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
                ph = (Map) props.get(str);
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
                    configRepository.put("Properties",
                        Utils.cloneProperties(ph));
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
                    props.put("ConfigRepository", key);
                    props.put(key, configRepository.get("Properties"));
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
                else if (!Utils.compareProperties(ph, (Map) o)) { // reload
                    Map<String, Object> c;
                    ph = (Map) o;
                    configFile = (String) configRepository.get("ConfigFile");
                    if (debugForReload <= 0 || configFile == null) //normal case
                        c = MonitorGroup.initMonitor(ph, name);
                    else { // reset PropertyFile for reload test only
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
                        configRepository.put("Properties",
                            Utils.cloneProperties(ph));
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
                        props.put(key, configRepository.get("Properties"));
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
        else {
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
            else if ("Agent".equals(key)) {
                strBuf.append("\n\t"+n+": "+ key + " has changes");
                if(!detail || props == null)
                    continue;
                h = Utils.getMasterProperties("Agent", includeMap, props);
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
     * reloads the properties
     */
    private int reload(Map props) {
        Object o;
        MonitorGroup group;
        long[] groupInfo;
        Map change = null;
        List list;
        Browser browser;
        String key, basename = "Agent";
        int i, k, n, id;

        n = props.size();
        if ((o = props.get("StatsURL")) != null && o instanceof String)
            statsURL = (String) o;

        if ((o = props.get("MaxNumberThread")) != null)
            maxNumberThread = Integer.parseInt((String) o);
        if (maxNumberThread <= 0)
            maxNumberThread = 2;

        if ((o = props.get("Heartbeat")) != null)
            defaultHeartbeat = 1000 * Integer.parseInt((String) o);

        if (defaultHeartbeat <= 0)
            defaultHeartbeat = 300000;

        if ((o = props.get("Timeout")) != null)
            defaultTimeout = 1000 * Integer.parseInt((String) o);
        else
            defaultTimeout = defaultHeartbeat + defaultHeartbeat;

        if (defaultTimeout < 0)
            defaultTimeout = defaultHeartbeat + defaultHeartbeat;

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

        if (control.size() > 0)
            update(control, jobPool);

        mtime = System.currentTimeMillis();
        // reload groups
        if ((o = props.get("MonitorGroup")) != null && o instanceof List) {
            // for MonitorGroup
            StringBuffer strBuf = ((debug & DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
            list = (List) o;
            n = list.size();
            Map<String, Object>ph = new HashMap<String, Object>();
            for (i=0; i<n; i++) { // init map for group names
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                o = ((Map) o).get("Name");
                if (o != null && o instanceof String)
                    ph.put((String) o, null);
            }
            browser = groupList.browser();
            while ((id = browser.next()) >= 0) { // delete groups
                key = groupList.getKey(id);
                if (!ph.containsKey(key)) { // remove the group
                    groupInfo =  groupList.getMetaData(id);
                    k = (int) groupInfo[GROUP_TID];
                    group = (MonitorGroup) groupList.remove(id);
                    if (k >= 0) // restart thread if group is still running
                        restartThread(k);
                    group.close();
                    new Event(Event.INFO, name + " group " + key +
                        " has been removed on " + id).send();
                }
            }

            for (i=0; i<n; i++) { // add or update groups
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                ph = Utils.cloneProperties((HashMap) o);
                k = Utils.copyListProps("Monitor", ph, props);
                key = (String) ph.get("Name");
                if (groupList.containsKey(key)) { // update
                    id = groupList.getID(key);
                    group = (MonitorGroup) groupList.get(id);
                    change = group.diff(ph);
                    groupList.rotate(id);
                    if (change != null && change.size() > 0) { // with changes
                        if ((debug & DEBUG_DIFF) > 0)
                            group.showChange("group "+key, "GROUP", change,
                                ((debug & DEBUG_TRAN) > 0));
                        groupInfo =  groupList.getMetaData(id);
                        k = (int) groupInfo[GROUP_TID];
                        if (k >= 0) {// restart thread if group is still running
                            restartThread(k);
                            groupInfo[GROUP_TID] = -1;
                            groupInfo[GROUP_STATUS] = SERVICE_READY;
                        }
                        try {
                            group.reload(change);
                            groupInfo[GROUP_SIZE] = group.getSize();
                            groupInfo[GROUP_HBEAT] = group.getHeartbeat();
                            groupInfo[GROUP_TIMEOUT] = group.getTimeout();
                            groupInfo[GROUP_RETRY] = group.getMaxRetry();
                            groupInfo[GROUP_TYPE] = (group.isDynamic()) ? 1 : 0;
                            groupInfo[GROUP_TIME] = mtime;
                            if (groupInfo[GROUP_HBEAT] <= 0)
                                groupInfo[GROUP_HBEAT] = defaultHeartbeat;
                            if (groupInfo[GROUP_TIMEOUT] <= 0)
                                groupInfo[GROUP_TIMEOUT] = defaultTimeout;
                            if (groupInfo[GROUP_RETRY] < 0)
                                groupInfo[GROUP_RETRY] = maxRetry;
                            k = initReports(group);
                            new Event(Event.INFO, name + " group " + key +
                                " has been reloaded on " + id + " with " +
                                k + " shared reports initialized").send();
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, name + " failed to reload " +
                                "group "+key+": "+ Event.traceStack(e)).send();
                        }
                        catch (Error e) {
                            new Event(Event.ERR, name + " failed to reload " +
                                "group "+key+": "+ Event.traceStack(e)).send();
                            Event.flush(e);
                        }
                    }
                }
                else { // add a new group
                    id = addGroup(mtime, ph);
                    new Event(Event.INFO, name + " group " + key +
                        " has been created on " + id).send();
                }
            }

            k = groupList.size();
            heartbeat = new int[k];
            browser = groupList.browser();
            k = 0;
            while ((i = browser.next()) >= 0) {
                groupInfo = groupList.getMetaData(i);
                heartbeat[k++] = (int) groupInfo[GROUP_HBEAT];
                if ((debug & DEBUG_INIT) > 0)
                    strBuf.append("\n\t" + groupList.getKey(i) + ": " + i +" "+
                        groupInfo[GROUP_HBEAT]/1000 + " " +
                        groupInfo[GROUP_TIMEOUT]/1000 + " " +
                        groupInfo[GROUP_RETRY] + " " +
                        groupInfo[GROUP_SIZE] + " " + groupInfo[GROUP_STATUS]);
            }
            if (k <= 0)
                heartbeat = new int[]{defaultHeartbeat};
            else if ((debug & DEBUG_INIT) > 0)
                new Event(Event.DEBUG, name + " GroupName: ID HBEAT TIMEOUT " +
                    "RETRY SIZE STATUS - " +k+"/"+n + strBuf.toString()).send();

            heartbeat = MonitorUtils.getHeartbeat(heartbeat);
        }
        else { // no group defined, so delete all groups
            browser = groupList.browser();
            while ((id = browser.next()) >= 0) { // delete groups
                key = groupList.getKey(id);
                groupInfo =  groupList.getMetaData(id);
                k = (int) groupInfo[GROUP_TID];
                group = (MonitorGroup) groupList.remove(id);
                if (k >= 0) // restart thread if group is still running
                    restartThread(k);
                group.close();
                new Event(Event.INFO, name + " group " + key +
                    " has been deleted on " + id).send();
            }
            heartbeat = new int[]{defaultHeartbeat};
        }

        k = groupList.size();
        if (maxNumberThread < k)
            k = maxNumberThread;

        // reload flows
        if ((o = props.get("MessageFlow")) != null && o instanceof List) {
            // for MessageFlow
            MessageFlow flow = null;
            long[] flowInfo;
            StringBuffer strBuf = ((debug & DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
            list = (List) o;
            n = list.size();
            Map<String, Object> ph = new HashMap<String, Object>();
            for (i=0; i<n; i++) { // init map for flow names
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                o = ((Map) o).get("Name");
                if (o != null && o instanceof String)
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
                    new Event(Event.INFO, name + " flow " + key +
                        " has been removed on " + id).send();
                }
            }
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                ph = Utils.cloneProperties((HashMap) o);
                k = Utils.copyListProps("Receiver", ph, props);
                k = Utils.copyListProps("Node", ph, props);
                k = Utils.copyListProps("Persister", ph, props);
                key = (String) ph.get("Name");
                if (!flowList.containsKey(key)) { // new flow
                    id = addFlow(mtime, ph, -1);
                    if (id >= 0 && (o = flowList.get(id)) != null) {
                        new Event(Event.INFO, name + " flow " + key +
                            " has been created on " + id).send();
                        flow = (MessageFlow) o;
                        flowInfo = flowList.getMetaData(id);
                        if ("default".equals(key))
                            flow.start();
                        else switch (flow.getDefaultStatus()) {
                          case SERVICE_RUNNING:
                            flow.start();
                            break;
                          case SERVICE_PAUSE:
                          case SERVICE_STANDBY:
                          case SERVICE_DISABLED:
                            flow.disable();
                            break;
                          case SERVICE_STOPPED:
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
                    change = flow.diff(ph);
                    if (change == null || change.size() <= 0) // with no change
                        flowList.rotate(id);
                    else if (flow.hasMajorChange(change)) { // replace the flow
                        if ((debug & DEBUG_DIFF) > 0) // with changes
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
                        k = addFlow(mtime, ph, id);
                        if (k >= 0 && (o = flowList.get(k)) != null) {
                            new Event(Event.INFO, name + " flow " + key +
                                " has been replaced on " + id).send();
                            flow = (MessageFlow) o;
                            flowInfo = flowList.getMetaData(id);
                            if ("default".equals(key))
                                flow.start();
                            else switch (flow.getDefaultStatus()) {
                              case SERVICE_RUNNING:
                                flow.start();
                                break;
                              case SERVICE_PAUSE:
                              case SERVICE_STANDBY:
                              case SERVICE_DISABLED:
                                flow.disable();
                                break;
                              case SERVICE_STOPPED:
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
                        if ((debug & DEBUG_DIFF) > 0) // with changes
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
                        flowInfo[GROUP_TIME] = mtime;
                        new Event(Event.INFO, name + " flow " + key +
                            " has been reloaded on " + id).send();
                    }
                }
            }
        }
        else { // no flow defined, so delete all flows
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
                new Event(Event.INFO, name + " flow " + key +
                    " has been deleted on " + id).send();
            }
        }

        cachedProps.clear();
        cachedProps = props;

        return n;
    }

    /** It initializes a new group, adds it to the list and returns its id */
    private int addGroup(long tm, Map ph) {
        MonitorGroup group;
        long[] groupInfo;
        String key;
        int id;
        if (ph == null || ph.size() <= 0)
            return -1;

        group = new MonitorGroup(ph);
        key = group.getName();
        groupInfo = new long[GROUP_TIME + 1];
        id = groupList.add(key, groupInfo, group);
        groupInfo[GROUP_ID] = id;
        groupInfo[GROUP_TID] = -1;
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
            groupInfo[GROUP_HBEAT] = defaultHeartbeat;
        if (groupInfo[GROUP_TIMEOUT] <= 0)
            groupInfo[GROUP_TIMEOUT] = defaultTimeout;
        if (groupInfo[GROUP_RETRY] < 0)
            groupInfo[GROUP_RETRY] = maxRetry;
        if (!ph.containsKey("Debug")) // set the default debug mode
            group.setDebugMode(debug);

        int k = initReports(group);
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG, name + " initialized " + k +
                " shared reprots for group " + key).send();

        return id;
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

    /** It restarts the thread of tid */
    private int restartThread(int tid) {
        int k, gid = -1;

        if ((k = control.getNextID(tid)) >= 0) {
            long[] jobInfo = (long[]) control.browse(tid);
            if (jobInfo != null) {
                gid = (int) jobInfo[JOB_GID];
                jobInfo[JOB_GID] = -1;
            }
            control.remove(tid);
            if (jobPool[tid].size() > 0) { // group is still running
                stopRunning(jobPool[tid]);
                jobPool[tid].clear();
                if (pool[tid].isAlive()) {
                    pool[tid].interrupt();
                    try {
                        pool[tid].join(2000);
                    }
                    catch (Exception e) {
                    }
                }
                if (!pg[tid].isDestroyed()) try {
                    pg[tid].destroy();
                }
                catch (Exception e) {
                }
                pool[tid] = null;
                pg[tid] = null;
                resumeRunning(jobPool[tid]);
                pg[tid] = new ThreadGroup("agentPool_" + tid);
                pool[tid] = new Thread(pg[tid], this, "AgentPool_" + tid);
                pool[tid].setDaemon(true);
                pool[tid].setPriority(Thread.NORM_PRIORITY);
                pool[tid].start();
            }
            if (gid >= 0) {
                long[] info = groupList.getMetaData(gid);
                if (info != null) {
                    info[GROUP_TID] = -1;
                    info[GROUP_STATUS] = SERVICE_READY;
                }
            }
        }

        return gid;
    }

    /**
     * returns a Map containing queried information according to the result type
     */
    private Map<String, String> queryInfo(long currentTime, long sessionTime,
        String target, String key, int type) {
        int i, j, k, n, id, size;
        long tm;
        String str, text, value;
        Browser browser;
        Map ph;
        Map<String, String> h = null;
        Map<String, Object> map;
        Object o;
        boolean isXML = (type & Utils.RESULT_XML) > 0;
        boolean isJSON = (type & Utils.RESULT_JSON) > 0;

        if (key == null)
            key = target;
        if ("PROPERTIES".equals(target)) { // for properties
            o = cachedProps.get(key);
            if (o != null && o instanceof Map) { // for a specific property
                // since it is a Map<String, Object>, it should be handled
                // else where already, so return null here
                return null;
            }
            else if ("Agent".equals(key)) { // for master config file
                Map g = null;
                List pl, list = null;
                map = Utils.cloneProperties(cachedProps);
                if (configRepository != null &&
                    (o = configRepository.get("Name")) != null)
                    map.remove((String) o);

                o = map.get("MonitorGroup");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) { // clean up monitors
                    g = (Map) list.get(i);
                    o = g.get("Monitor");
                    if (o != null && o instanceof List) {
                        pl = (List) o;
                        n = pl.size(); 
                        for (j=0; j<n; j++) {
                            o = pl.get(j);
                            if (o == null)
                                continue;
                            if (o instanceof Map)
                                str = (String) ((Map) o).get("Name");
                            else
                                str = (String) o;
                            map.remove(str);
                        }
                    }
                }
                o = map.get("MessageFlow");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) { // clean up flow components
                    g = (Map) list.get(i);
                    o = g.get("Receiver");
                    if (o != null && o instanceof List) {
                        n = ((List) o).size(); 
                        for (j=0; j<n; j++)
                            map.remove(((List) o).get(j));
                    }
                    o = g.get("Node");
                    if (o != null && o instanceof List) {
                        n = ((List) o).size(); 
                        for (j=0; j<n; j++)
                            map.remove(((List) o).get(j));
                    }
                    o = g.get("Persister");
                    if (o != null && o instanceof List) {
                        n = ((List) o).size(); 
                        for (j=0; j<n; j++)
                            map.remove(((List) o).get(j));
                    }
                }
                JSON2Map.flatten(map);
                h = new HashMap<String, String>();
                for (String ky : map.keySet()) {
                    h.put(ky, (String) map.get(ky));
                }
            }
            else if ("PROPERTIES".equals(key)) { // for list 
                StringBuffer strBuf = new StringBuffer();
                h = new HashMap<String, String>();
                Map g = null;
                List list = null, gl = null;
                if (isXML) { // for master config
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Name>Agent</Name>");
                    strBuf.append("<Type>" +
                        Utils.escapeXML((String) cachedProps.get("Type")) +
                        "</Type>");
                    strBuf.append("<Heartbeat>" +
                        (String) cachedProps.get("Heartbeat") + "</Heartbeat>");
                    strBuf.append("<MonitorGroup>N/A</MonitorGroup>");
                    strBuf.append("<Description>master configuration" +
                        "</Description>");
                    strBuf.append("</Record>");

                    if (configRepository != null)
                        g = (HashMap) configRepository.get("Properties");
                    if (g != null && g.size() > 0) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>"+(String)g.get("Name")+"</Name>");
                        strBuf.append("<Type>" +
                            Utils.escapeXML((String) g.get("Type")) +"</Type>");
                        strBuf.append("<Heartbeat>" +
                            (String) cachedProps.get("Heartbeat") +
                            "</Heartbeat>");
                        strBuf.append("<MonitorGroup>N/A</MonitorGroup>");
                        strBuf.append("<Description>" +
                            Utils.escapeXML((String) g.get("Description")) +
                            "</Description>");
                        strBuf.append("</Record>");
                    }
                }
                else if (isJSON) { // json
                    strBuf.append("{");
                    strBuf.append("\"Name\":\"Agent\",");
                    strBuf.append("\"Type\":\"" +
                        Utils.escapeJSON((String) cachedProps.get("Type")) +
                        "\",");
                    strBuf.append("\"Heartbeat\":\"" +
                        (String)cachedProps.get("Heartbeat") + "\",");
                    strBuf.append("\"MonitorGroup\":\"N/A\",");
                    strBuf.append("\"Description\":\"master configuration\"");
                    strBuf.append("}");

                    if (configRepository != null)
                        g = (HashMap) configRepository.get("Properties");
                    if (g != null && g.size() > 0) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + (String) g.get("Name") +
                            "\",");
                        strBuf.append("\"Type\":\"" +
                            Utils.escapeJSON((String) g.get("Type")) + "\",");
                        strBuf.append("\"Heartbeat\":\"" +
                            (String) cachedProps.get("Heartbeat") + "\",");
                        strBuf.append("\"MonitorGroup\":\"N/A\",");
                        strBuf.append("\"Description\":\"" +
                            Utils.escapeJSON((String) g.get("Description")) +
                            "\"");
                        strBuf.append("}");
                    }
                }
                else {
                    text = (String) cachedProps.get("Type") + " " +
                        cachedProps.get("Heartbeat") + " N/A " +
                        "master configuration";
                    h.put("Agent", text);
                    if (configRepository != null)
                        g = (Map) configRepository.get("Properties");
                    if (g != null && g.size() > 0) {
                        text = (String) g.get("Type") + " " +
                            cachedProps.get("Heartbeat") + " N/A " +
                            (String) g.get("Description");
                        h.put((String) g.get("Name"), text);
                    }
                }
                o = cachedProps.get("MonitorGroup");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) { // loop thru all groups
                    g = (Map) list.get(i);
                    o = g.get("Monitor");
                    if (o == null || !(o instanceof List))
                        continue;
                    text = (String) g.get("Name");
                    value = (g.containsKey("Heartbeat")) ?
                        (String) g.get("Heartbeat") :
                        (String) cachedProps.get("Heartbeat");
                    gl = (List) o;
                    n = gl.size();
                    for (j=0; j<n; j++) { // for all monitors
                        o = gl.get(j);
                        if (o != null && o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            str = (String) o;
                        if (str == null || h.containsKey(str))
                            continue;
                        g = (Map) cachedProps.get(str);
                        if (g == null)
                            continue;
                        if (isXML) { // xml
                            strBuf.append("<Record type=\"ARRAY\">");
                            strBuf.append("<Name>" + Utils.escapeXML(str) +
                                "</Name>");
                            strBuf.append("<Type>" +
                                Utils.escapeXML((String) g.get("Type")) +
                                "</Type>");
                            strBuf.append("<Heartbeat>" +
                                Utils.escapeXML(value) + "</Heartbeat>");
                            strBuf.append("<MonitorGroup>" +
                                Utils.escapeXML(text)+"</MonitorGroup>");
                            strBuf.append("<Description>" +
                                Utils.escapeXML((String)g.get("Description"))+
                                "</Description>");
                            strBuf.append("</Record>");
                        }
                        else if (isJSON) { // json
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append("{");
                            strBuf.append("\"Name\":\""+Utils.escapeJSON(str)+
                                "\",");
                            strBuf.append("\"Type\":\"" +
                                Utils.escapeJSON((String) g.get("Type")) +
                                "\",");
                            strBuf.append("\"Heartbeat\":\"" +
                                Utils.escapeJSON(value) + "\",");
                            strBuf.append("\"MonitorGroup\":\"" +
                                Utils.escapeJSON(text) + "\",");
                            strBuf.append("\"Description\":\"" +
                                Utils.escapeJSON((String)g.get("Description"))+
                                "\"");
                            strBuf.append("}");
                        }
                        else {
                            strBuf.append((String) g.get("Type"));
                            strBuf.append(" " + value);
                            strBuf.append(" " + text);
                            strBuf.append(" "+(String)g.get("Description"));
                            h.put(str, strBuf.toString());
                        }
                    }
                }
                o = cachedProps.get("MessageFlow");
                if (o != null && o instanceof List) {
                    list = (List) o;
                    size = list.size();
                }
                else
                    size = 0;
                for (i=0; i<size; i++) { // loop thru all flows
                    g = (Map) list.get(i);
                    text = (String) g.get("Name");
                    value = (g.containsKey("Heartbeat")) ?
                        (String) g.get("Heartbeat") :
                        (String) cachedProps.get("Heartbeat");
                    o = g.get("Receiver");
                    if (o != null && o instanceof List) {
                        gl = (List) o;
                        n = gl.size();
                    }
                    else
                        n = 0;
                    for (j=0; j<n; j++) { // for all receivers
                        o = gl.get(j);
                        if (o != null && o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            str = (String) o;
                        if (str == null || h.containsKey(str))
                            continue;
                        o = cachedProps.get(str);
                        if (o == null || !(o instanceof Map))
                            continue;
                        if (isXML) { // xml
                            strBuf.append("<Record type=\"ARRAY\">");
                            strBuf.append("<Name>" + Utils.escapeXML(str) +
                                "</Name>");
                            strBuf.append("<Type>Receiver</Type>");
                            strBuf.append("<Heartbeat>" +
                                Utils.escapeXML(value) + "</Heartbeat>");
                            strBuf.append("<MonitorGroup>" +
                                Utils.escapeXML(text)+"</MonitorGroup>");
                            strBuf.append("<Description>" + Utils.escapeXML(
                                (String)((Map) o).get("Description"))+
                                "</Description>");
                            strBuf.append("</Record>");
                        }
                        else if (isJSON) { // json
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append("{");
                            strBuf.append("\"Name\":\"" +Utils.escapeJSON(str)+
                                "\",");
                            strBuf.append("\"Type\":\"Receiver\",");
                            strBuf.append("\"Heartbeat\":\"" +
                                Utils.escapeJSON(value) + "\",");
                            strBuf.append("\"MonitorGroup\":\"" +
                                Utils.escapeJSON(text) + "\",");
                            strBuf.append("\"Description\":\"" +
                                Utils.escapeJSON(
                                (String)((Map) o).get("Description"))+ "\"");
                            strBuf.append("}");
                        }
                        else {
                            strBuf.append("Receiver");
                            strBuf.append(" " + value);
                            strBuf.append(" " + text);
                            strBuf.append(" " +
                                (String) ((Map) o).get("Description"));
                            h.put(str, strBuf.toString());
                        }
                    }
                    o = g.get("Node");
                    if (o != null && o instanceof List) {
                        gl = (List) o;
                        n = gl.size();
                    }
                    else
                        n = 0;
                    for (j=0; j<n; j++) { // for all nodes
                        o = gl.get(j);
                        if (o != null && o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            str = (String) o;
                        if (str == null || h.containsKey(str))
                            continue;
                        o = cachedProps.get(str);
                        if (o == null || !(o instanceof Map))
                            continue;
                        if (isXML) { // xml
                            strBuf.append("<Record type=\"ARRAY\">");
                            strBuf.append("<Name>" + Utils.escapeXML(str) +
                                "</Name>");
                            strBuf.append("<Type>Node</Type>");
                            strBuf.append("<Heartbeat>" +
                                Utils.escapeXML(value) + "</Heartbeat>");
                            strBuf.append("<MonitorGroup>" +
                                Utils.escapeXML(text)+"</MonitorGroup>");
                            strBuf.append("<Description>" + Utils.escapeXML(
                                (String) ((Map) o).get("Description"))+
                                "</Description>");
                            strBuf.append("</Record>");
                        }
                        else if (isJSON) { // json
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append("{");
                            strBuf.append("\"Name\":\"" +Utils.escapeJSON(str)+
                                "\",");
                            strBuf.append("\"Type\":\"Node\",");
                            strBuf.append("\"Heartbeat\":\"" +
                                Utils.escapeJSON(value) + "\",");
                            strBuf.append("\"MonitorGroup\":\"" +
                                Utils.escapeJSON(text) + "\",");
                            strBuf.append("\"Description\":\"" +
                                Utils.escapeJSON(
                                (String)((Map) o).get("Description"))+ "\"");
                            strBuf.append("}");
                        }
                        else {
                            strBuf.append("Node");
                            strBuf.append(" " + value);
                            strBuf.append(" " + text);
                            strBuf.append(" " +
                                (String) ((Map) o).get("Description"));
                            h.put(str, strBuf.toString());
                        }
                    }
                    o = g.get("Persister");
                    if (o != null && o instanceof List) {
                        gl = (List) o;
                        n = gl.size();
                    }
                    else
                        n = 0;
                    for (j=0; j<n; j++) { // for all persisters
                        o = gl.get(j);
                        if (o != null && o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            str = (String) o;
                        if (str == null || h.containsKey(str))
                            continue;
                        o = cachedProps.get(str);
                        if (o == null || !(o instanceof Map))
                            continue;
                        if (isXML) { // xml
                            strBuf.append("<Record type=\"ARRAY\">");
                            strBuf.append("<Name>" + Utils.escapeXML(str) +
                                "</Name>");
                            strBuf.append("<Type>Persister</Type>");
                            strBuf.append("<Heartbeat>" +
                                Utils.escapeXML(value) + "</Heartbeat>");
                            strBuf.append("<MonitorGroup>" +
                                Utils.escapeXML(text)+"</MonitorGroup>");
                            strBuf.append("<Description>" + Utils.escapeXML(
                                (String) ((Map) o).get("Description"))+
                                "</Description>");
                            strBuf.append("</Record>");
                        }
                        else if (isJSON) { // json
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append("{");
                            strBuf.append("\"Name\":\"" +Utils.escapeJSON(str)+
                                "\",");
                            strBuf.append("\"Type\":\"Persister\",");
                            strBuf.append("\"Heartbeat\":\"" +
                                Utils.escapeJSON(value) + "\",");
                            strBuf.append("\"MonitorGroup\":\"" +
                                Utils.escapeJSON(text) + "\",");
                            strBuf.append("\"Description\":\"" +
                                Utils.escapeJSON(
                                (String)((Map) o).get("Description"))+ "\"");
                            strBuf.append("}");
                        }
                        else {
                            strBuf.append("Persister");
                            strBuf.append(" " + value);
                            strBuf.append(" " + text);
                            strBuf.append(" " +
                                (String) ((Map) o).get("Description"));
                            h.put(str, strBuf.toString());
                        }
                    }
                }
                if (isXML) // xml
                    h.put(target, strBuf.toString());
                else if (isJSON) // json
                    h.put(target, "[" + strBuf.toString() + "]");
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
                    map.put("Status", reportStatusText[id + statusOffset]);
                }
                catch (Exception e) {
                }
                o = map.get("SampleTime");
                if (o != null && o instanceof long[]) try {
                    tm = ((long[]) o)[0];
                    map.put("SampleTime", Event.dateFormat(new Date(tm)));
                }
                catch (Exception e) {
                }
                o = map.get("LatestTime");
                if (o != null && o instanceof long[]) try {
                    tm = ((long[]) o)[0];
                    map.put("LatestTime", Event.dateFormat(new Date(tm)));
                }
                catch (Exception e) {
                }
                o = map.get("SampleSize");
                if (o != null && o instanceof long[]) try {
                    tm = ((long[]) o)[0];
                    map.put("SampleSize", String.valueOf(tm));
                }
                catch (Exception e) {
                }
                o = map.get("FileSize");
                if (o != null && o instanceof long[]) try {
                    tm = ((long[]) o)[0];
                    map.put("FileSize", String.valueOf(tm));
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
                            id = Integer.parseInt((String) keys[i]);
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
                    if (isXML) { // xml
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" +
                            Utils.escapeXML(Event.dateFormat(new Date(tm)))+
                            "</Time>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) { // json
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm)))+
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                keys = QFlow.getReportNames(); // reports from QFlow
                for (i=0; i<keys.length; i++) {
                    str = keys[i];
                    if (str == null || str.length() <= 0)
                        continue;
                    o = QFlow.getReport(str);
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
                    if (isXML) { // xml
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Time>" +
                            Utils.escapeXML(Event.dateFormat(new Date(tm)))+
                            "</Time>");
                        strBuf.append("<Status>" + Utils.escapeXML(text) +
                            "</Status>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) { // json
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm)))+
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML) // xml
                    h.put(target, strBuf.toString());
                else if (isJSON) // json
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ((map = QFlow.getReport(key)) != null) {
                // for QFlow report
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
        else if ("AGENT".equals(target) || "STATUS".equals(target)){//for status
            h = new HashMap<String, String>();
            h.put("Name", name);
            h.put("ReleaseTag", ReleaseTag.getReleaseTag());
            h.put("CurrentStatus", statusText[currentStatus]);
            h.put("PreviousStatus", statusText[previousStatus]);
            h.put("GroupSize", String.valueOf(groupList.size()));
            h.put("FlowSize", String.valueOf(flowList.size()));
            h.put("Debug", String.valueOf(debug));
            h.put("Time", Event.dateFormat(new Date(mtime)));
            h.put("SessionTime", Event.dateFormat(new Date(sessionTime)));
        }
        else if ("GROUP".equals(target)) { // for group
            MonitorGroup group;
            long[] groupInfo;
            if (groupList.containsKey(key)) { // for a specific group
                group = (MonitorGroup) groupList.get(key);
                h = group.queryInfo(currentTime, sessionTime, target,
                    target, type);
            }
            else if ("GROUP".equals(key)) { // list all groups
                StringBuffer strBuf = new StringBuffer();
                browser = groupList.browser();
                h = new HashMap<String, String>();
                while ((i = browser.next()) >= 0) {
                    str = groupList.getKey(i);
                    groupInfo = groupList.getMetaData(i);
                    size = (int) groupInfo[GROUP_SIZE];
                    tm = groupInfo[GROUP_TIME];
                    text = statusText[((int) groupInfo[GROUP_STATUS] +
                        statusText.length) % statusText.length];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + groupInfo[GROUP_COUNT] +
                            "</Count>");
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
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Count\":\"" + groupInfo[GROUP_COUNT] +
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm)))+
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " + groupInfo[GROUP_COUNT] +
                            " "+ Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
        }
        else if ("POOL".equals(target)) { // for jobPool
            long[] jobInfo;
            if ("POOL".equals(key)) {
                StringBuffer strBuf = new StringBuffer();
                h = new HashMap<String, String>();
                for (i=0; i<jobPool.length; i++) {
                    str = jobPool[i].getName();
                    jobInfo = jobList.getMetaData(i);
                    size = (int) jobInfo[JOB_SIZE];
                    tm = jobInfo[JOB_TIME];
                    id = jobPool[i].getGlobalMask();
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
                        strBuf.append("<Name>POOL_" + i + "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + jobInfo[JOB_COUNT] +
                            "</Count>");
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
                        strBuf.append("\"Name\":\"POOL_" + i + "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Count\":\"" + jobInfo[JOB_COUNT] +
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm)))+
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("POOL_" + i, size + " " + jobInfo[JOB_COUNT] +
                            " " + Event.dateFormat(new Date(tm))+ " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if (key.startsWith("POOL_")) {
                str = key.substring(5);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
                if (id >= 0) {
                    id %= jobPool.length;
                    XQueue xq = jobPool[id];
                    jobInfo = (long[]) control.browse(id);
                    h = new HashMap<String, String>();
                    h.put("Name", jobPool[id].getName());
                    h.put("Capacity", String.valueOf(xq.getCapacity()));
                    h.put("Depth", String.valueOf(xq.depth()));
                    h.put("Size", String.valueOf(xq.size()));
                    h.put("Mask", String.valueOf(xq.getGlobalMask()));
                    h.put("Indexed", "1");
                    h.put("Gid", String.valueOf(jobInfo[JOB_GID]));
                    h.put("Tid", String.valueOf(id));
                    h.put("Retry", String.valueOf(jobInfo[JOB_RETRY]));
                    h.put("Count", String.valueOf(jobInfo[JOB_COUNT]));
                    h.put("Time",
                        Event.dateFormat(new Date(jobInfo[JOB_TIME])));
                }
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
                browser = flowList.browser();
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
            MessageFlow flow = (MessageFlow) flowList.get(key);
            h = null;
            if (flow != null) { // list all nodes on the flow
                h = flow.queryInfo(currentTime, sessionTime, target,
                    target, type);
            }
            else if ((i = key.indexOf("/")) > 0) { // find id and flow
                str = key.substring(0, i);
                flow = (MessageFlow) flowList.get(str);
                str = key.substring(i+1);
                try {
                    i = Integer.parseInt(str);
                }
                catch (Exception e) {
                    i = -1;
                }
                if (flow != null) { // list info for the object
                    key = (i >= 0) ? target + "_" + i : str;
                    h = flow.queryInfo(currentTime, sessionTime, target,
                        key, type);
                }
            }
        }
        else if ("XQUEUE".equals(target)) { // for a specific flow
            MessageFlow flow = (MessageFlow) flowList.get(key);
            h = null;
            if (flow != null) { // for all XQs of the flow
                h = flow.queryInfo(currentTime, sessionTime, "XQ", "XQ", type);
            }
            else if ((i = key.indexOf("/")) > 0) { // for a specific XQ
                str = key.substring(0, i);
                flow = (MessageFlow) flowList.get(str);
                if (flow != null) { // list info for an XQ 
                    key = key.substring(i+1);
                    h = flow.queryInfo(currentTime, sessionTime, "XQ",
                        key, type);
                }
            }
        }
        else if ("OUT".equals(target) || "RULE".equals(target) ||
            "MSG".equals(target)) { // for a specific flow
            MessageFlow flow = null;
            h = null;
            if ((i = key.indexOf("/")) > 0) { // find node id and flow
                str = key.substring(0, i);
                flow = (MessageFlow) flowList.get(str);
                str = key.substring(i+1);
                try {
                    i = Integer.parseInt(str);
                }
                catch (Exception e) {
                    i = -1;
                }
                if (flow != null) { // list info for the node
                    key = (i >= 0) ? "NODE_" + i : str;
                    h = flow.queryInfo(currentTime, sessionTime, target,
                        key, type);
                }
            }
        }
        else if ("XQ".equals(target)) { // for XQueue of container
            XQueue xq;
            if ("XQ".equals(key)) { // list all xqs
                StringBuffer strBuf = new StringBuffer();
                String tmStr;
                long[] list = new long[6];
                h = new HashMap<String, String>();
                if (escalation != null) {
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
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ("escalation".equals(key) && escalation != null) {
                h = MessageUtils.listXQ(escalation, type, displayPropertyName);
            }
        }
        else if ("HELP".equals(target) || "USAGE".equals(target)) { // usage
            StringBuffer strBuf = new StringBuffer();
            h = new HashMap<String, String>();
            if (isXML) {
                strBuf.append("<Record>\n");
                strBuf.append("  <USAGE>query CATEGORY [name]</USAGE>\n");
                strBuf.append("  <PROPERTIES>List of configuration " +
                    "properties</PROPERTIES>\n");
                strBuf.append("  <REPORT>List of shared reports</REPORT>\n");
                strBuf.append("  <AGENT>status of monitor agent"+"</AGENT>\n");
                strBuf.append("  <POOL>List of XQueues for job pool</POOL>\n");
                strBuf.append("  <XQ>List of internal XQueues</XQ>\n");
                strBuf.append("  <GROUP>List of monitor groups</GROUP>\n");
                strBuf.append("  <FLOW>List of message flows</FLOW>\n");
                strBuf.append("  <XQUEUE>List of all XQueues " +
                    "on a specific flow (name=: flowName)</XQUEUE>\n");
                strBuf.append("  <RECEIVER>List of all receivers " +
                    "on a specifc flow (name=: flowName)</RECEIVER>\n");
                strBuf.append("  <NODE>List of all nodes " +
                    "on a specific flow (name=: flowName)</NODE>\n");
                strBuf.append("  <PERSISTER>List of all persisters " +
                    "on a specifc flow (name=: flowName)</PERSISTER>\n");
                strBuf.append("  <OUT>List of outlinks for the id-th node " +
                    "on a specific flow (name=: flowName/nodeID)</OUT>\n");
                strBuf.append("  <RULE>List of rulesets for the id-th node " +
                    "on a specific flow (name=: flowName/nodeID)</RULE>\n");
                strBuf.append("  <MSG>List of stuck messages for the id-th " +
                    "node on a specific flow (name=: flowName/nodeID)</MSG>\n");
                strBuf.append("</Record>");
                h.put(target, strBuf.toString());
            }
            else if (isJSON) {
                strBuf.append("{\n");
                strBuf.append("  \"USAGE\":\"query CATEGORY [name]\",\n");
                strBuf.append("  \"PROPERTIES\":\"List of configuration " +
                    "properties\",\n");
                strBuf.append("  \"REPORT\":\"List of shared reports\",\n");
                strBuf.append("  \"AGENT\":\"status of monitor agent\"," +
                    "\n");
                strBuf.append("  \"POOL\":\"List of XQueues for job pool\",\n");
                strBuf.append("  \"XQ\":\"List of internal XQueues\",\n");
                strBuf.append("  \"GROUP\":\"List of monitor groups\",\n");
                strBuf.append("  \"FLOW\":\"List of message flows\",\n");
                strBuf.append("  \"XQUEUE\":\"List of all XQueues " +
                    "on a specific flow (name=: flowName)\",\n");
                strBuf.append("  \"RECEIVER\":\"List of all receivers " +
                    "on a specifc flow (name=: flowName)\",\n");
                strBuf.append("  \"NODE\":\"List of all nodes " +
                    "on a specific flow (name=: flowName)\",\n");
                strBuf.append("  \"PERSISTER\":\"List of all persisters " +
                    "on a specifc flow (name=: flowName)\",\n");
                strBuf.append("  \"OUT\":\"List of outlinks for the id-th " +
                    "node on a specific flow (name=: flowName/nodeID)\",\n");
                strBuf.append("  \"RULE\":\"List of rulesets for the id-th " +
                    "node on a specific flow (name=: flowName/nodeID)\",\n");
               strBuf.append("  \"MSG\":\"List of stuck messages for the id-th"+
                    " node on a specific flow (name=: flowName/nodeID)\"\n");
                strBuf.append("}");
                h.put(target, strBuf.toString());
            }
            else {
                strBuf.append("query CATEGORY [name]\n");
                strBuf.append("where following CATEGORY supported:\n");
                strBuf.append("PROPERTIES: list of configuration properties\n");
                strBuf.append("REPORT: lis of shared reports\n");
                strBuf.append("AGENT: status of monitor agent\n");
                strBuf.append("POOL: list of XQueues with job pool\n");
                strBuf.append("XQ: list of internal XQueues\n");
                strBuf.append("GROUP: list of monitore groups\n");
                strBuf.append("FLOW: list of message flows\n");
                strBuf.append("XQUEUE: list of all XQueues " +
                    "on a specific flow (name=: flowName)\n");
                strBuf.append("RECEIVER: list of all receivers " +
                    "on a specific flow (name=: flowName)\n");
                strBuf.append("NODE: list of all nodes " +
                    "on a specific flow (name=: flowName\n");
                strBuf.append("PERSISTER: list of all persisters " +
                    "on a specific flow (name=: flowName)\n");
                strBuf.append("OUT: list of outlinks for the id-th node " +
                    "on a specific flow (name=: flowName/nodeID)\n");
                strBuf.append("RULE: list of rulesets for the id-th node " +
                    "on a specific flow (name=: flowName/nodeID)\n");
                strBuf.append("MSG: list of stuck messages for the id-th node "+
                    "on a specific flow (name=: flowName/nodeID)");
                h.put("Usage", strBuf.toString());
            }
        }
        return h;
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

    public void close() {
        Browser browser;
        MessageFlow flow;
        long[] flowInfo;
        int i;
        previousStatus = currentStatus;
        currentStatus = SERVICE_CLOSED;
        if (escalation != null)
            stopRunning(escalation);

        for (i=0; i<jobPool.length; i++)
            stopRunning(jobPool[i]);

        for (i=0; i<pool.length; i++) {
            if (pool[i].isAlive())
                pool[i].interrupt();
        }
        if (adminServer != null) {
            adminServer.close();
            if (admin.isAlive())
                admin.interrupt();
        }
        else if (httpServer != null) try {
            httpServer.stop();
        }
        catch (Exception e) {
        }

        browser = flowList.browser();
        while ((i = browser.next()) >= 0) {
            flow = (MessageFlow) flowList.get(i);
            flow.stop();
            flowInfo = flowList.getMetaData(i);
            flowInfo[GROUP_STATUS] = flow.getStatus();
        }

        if (adminServer != null) {
            if (admin.isAlive()) try {
                admin.join(2000);
            }
            catch (Exception e) {
            }
        }
        else if (httpServer != null) try {
            httpServer.join();
        }
        catch (Exception e) {
        }

        for (i=0; i<pool.length; i++) {
            if (pool[i].isAlive()) try {
                pool[i].join(2000);
            }
            catch (Exception e) {
            }
        }
    }

    /**
     * It processes the request according to the preconfigured rulesets.
     * If timeout < 0, there is no wait.  If timeout > 0, it will wait for
     * the processed event until timeout.  If timeout = 0, it blocks until
     * the processed event is done.  It returns 1 for success, 0 for timed
     * out and -1 for failure.  The method is MT-Safe.
     *<br/><br/>
     * N.B., The input request will be modified for the response.  It is
     * mutual-exclusive with the adminServer.
     */
    public int doRequest(org.qbroker.common.Event req, int timeout) {
        int i, n, sid = -1, m = 1;

        if (req == null || idList == null)
            return -1;

        Event request = (Event) req;
        if (timeout > 0)
            m = timeout / 500 + 1;
        else if (timeout == 0)
            m = 100;

        switch (aPartition[1]) {
          case 0:
            if (timeout < 0) {
                sid = idList.reserve(500L);
            }
            else for (i=0; i<m; i++) {
                sid = idList.reserve(500L);
                if (sid >= 0 || !keepRunning(escalation))
                    break;
            }
            break;
          case 1:
            if (timeout < 0) {
                sid = idList.reserve(500L, aPartition[0]);
            }
            else for (i=0; i<m; i++) {
                sid = idList.reserve(500L, aPartition[0]);
                if (sid >= 0 || !keepRunning(escalation))
                    break;
            }
            break;
          default:
            if (timeout < 0) {
                sid = idList.reserve(500L, aPartition[0], aPartition[1]);
            }
            else for (i=0; i<m; i++) {
                sid = idList.reserve(500L, aPartition[0], aPartition[1]);
                if (sid >= 0 || !keepRunning(escalation))
                    break;
            }
            break;
        }

        if (sid >= 0) {
            i = escalation.reserve(50L, sid); 
            if (i != sid) {
                idList.cancel(sid);
                return -1;
            }
            request.setAttribute("reqID", String.valueOf(sid));
            i = escalation.add(request, sid);
            for (i=0; i<m; i++) {
                if (escalation.collect(500L, sid) >= 0) { // collect response
                    idList.cancel(sid);
                    return 1;
                }
                if (!keepRunning(escalation))
                    break;
            }
            request.setExpiration(System.currentTimeMillis() - 60000L);
            escalation.takeback(sid);
            idList.cancel(sid);
        }
        return 0;
    }

    public boolean keepRunning(XQueue xq) {
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

    private void pause(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.PAUSE);
    }

    private void standby(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.STANDBY);
    }

    private void disableAck(XQueue xq) {
        int mask = xq.getGlobalMask();
        xq.setGlobalMask(mask & (~XQueue.EXTERNAL_XA));
    }

    private void enableAck(XQueue xq) {
        int mask = xq.getGlobalMask();
        xq.setGlobalMask(mask | XQueue.EXTERNAL_XA);
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return "monitor";
    }

    public int getStatus() {
        return currentStatus;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    /**
     * It loads the property map from configDir for each string item of the
     * given list and adds the property map to the given props. It only supports
     * the config files with the extension of ".json". It returns the number
     * of included configs upon success or -1 otherwise.
     */
    @SuppressWarnings("unchecked")
    public static int loadListProperties(List list, String configDir,
        int show, Map props) {
        int i, n, size;
        File file;
        Object o;
        String name;

        if (list == null || configDir == null || props == null)
            return -1;

        n = 0;
        size = list.size();
        for (i=0; i<size; ++i) {
            o = list.get(i);
            if (o == null)
                continue;
            if (o instanceof String)
                name = (String) o;
            else if (o instanceof Map) // for generated monitor
                name = (String) ((Map) o).get("Name");
            else // not supported
                continue;
            if ((o = props.get(name)) != null && o instanceof Map) // loaded
                continue;

            file = new File(configDir + FILE_SEPARATOR + name + ".json");
            if (file.exists() && file.isFile() && file.canRead()) {
                try {
                    FileReader fr = new FileReader(file);
                    o = JSON2Map.parse(fr);
                    fr.close();
                    if (o == null || !(o instanceof Map)) {
                        new Event(Event.ERR, "bad content for " + name +
                            " or base tag not found in "+file.getPath()).send();
                        continue;
                    }
                    else if (show > 0) {
                        System.out.println("\n,\"" + name + "\":" +
                            JSON2Map.toJSON((Map) o, "", "\n") + " // " +
                            "end of "+ file.getName());
                        continue;
                    }
                    props.put(name, o);
                    n ++;
                }
                catch (Exception e) {
                    new Event(Event.ERR,"failed to load the property file for "+
                        name + ": " + Event.traceStack(e)).send();
                }
            }
            else { // file not readable
                new Event(Event.WARNING, "failed to open the property file " +
                    file.getPath()).send();
            }
        }
        return n;
    }

    /**
     * It checks the loaded primay include on its ClassName for each string
     * item of the given group. If its classname is defined as a key in the
     * given policy map for 2nd includes, the value will be the name of the
     * list that may contain items to be included secondarily. For each item
     * to be included, the method will load its property map from cfgDir and
     * merges it to its primary include loaded already to props. It only
     * supports the config files with the extension of ".json". It returns the
     * number of merged configs upon success or -1 otherwise.
     */
    @SuppressWarnings("unchecked")
    public static int loadIncludedProperties(List group, String cfgDir,
        int show, Map props, Map policy) {
        int i, k, n, size;
        Object o;
        List list;
        Map ph;
        File file;
        String name, key;

        if (group == null || cfgDir == null || props == null || policy == null)
            return -1;

        k = 0;
        n = group.size();
        for (i=0; i<n; ++i) {
            // merge individual 2nd includes into its primary include
            name = (String) group.get(i);
            if (name == null || !props.containsKey(name))
                continue;
            o = props.get(name);
            if (o == null || !(o instanceof Map))
                continue;
            ph = (Map) o;
            if (!ph.containsKey("ClassName"))
                continue;
            key = (String) ph.get("ClassName");
            if (!policy.containsKey(key))
                continue;
            o = policy.get(key);
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = ph.get(key);
            if (o == null || !(o instanceof List))
                continue;
            list = (List) o;
            size = list.size();
            for (int j=0; j<size; ++j) { // look for item for 2nd include
                o = list.get(j);
                if (o == null || !(o instanceof String))
                    continue;
                key = (String) o;
                if ((o = ph.get(key)) != null && o instanceof Map) // included
                    continue;
                file = new File(cfgDir + FILE_SEPARATOR + key + ".json");
                if (file.exists() && file.isFile() && file.canRead()) try {
                    FileReader fr = new FileReader(file);
                    o = JSON2Map.parse(fr);
                    fr.close();
                    if (o == null ||!(o instanceof Map))
                        continue;
                    else if (show > 0) {
                        System.out.println("\n,\"" + key + "\":" +
                            JSON2Map.toJSON((Map) o, "", "\n") +
                            " // end of "+ file.getName());
                    }
                    ph.put(key, o);
                    k ++;
                    continue;
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to merge the property " +
                        "file of " + key +" into "+ name + ": " +
                        Event.traceStack(e)).send();
                    continue;
                }
                // file not readable
                new Event(Event.WARNING,"failed to open the property file " +
                    file.getPath() + " for merge into " + name).send();
            }
        }
        return k;
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
                "Status DefStatus - Mask LinkName" + strBuf.toString()).send();
    }

    /**
     * sends a query message via the given persister and waits for response.
     * It returns null if sucessful.  Otherwise, error message is returned.
     * This method is not efficient at all.  It is supposed to be used
     * for ad hoc request.  The persister has to be fully initialized.
     * The most of the persisters only support TextMessage or BytesMessage.
     */
    public static String query(MessagePersister pstr, Message msg) {
        int i, id, cid;
        ThreadPool pool = null;
        IndexedXQueue xq = null;
        Thread client = null;

        if (msg == null || pstr == null)
            return "null pstr or msg";

        xq = new IndexedXQueue("client", 2);
        id = xq.reserve(50L);
        cid = xq.add(msg, id);
        if (id < 0 || cid <= 0)
            return "failed to add the query msg: " + id + ":" + cid;

        pool = new ThreadPool("client", 1, 1, pstr, "persist",
            new Class[]{XQueue.class, int.class});
        client = pool.checkout(50L);
        if (client == null)
            return "failed to checkout a connection";

        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        cid = -1;
        for (i=0; i<10; i++) {
            cid = xq.reserve(500L, id);
            if (cid >= 0)
                break;
        }
        MessageUtils.stopRunning(xq);
        pool.close();
        if (cid >= 0)
            return null;
        else
            return "failed to get the response";
    }

    /**
     * The configuration parameters are stored in the property file,
     * Agent.json, that lists all monitorGroups, etc.
     *<br/><br/>
     * Usage: java org.qbroker.flow.MonitorAgent [-?|-l|-I ConfigFile|...]
     *<br/>
     * @param args the command line arguments
     */
    @SuppressWarnings("unchecked")
    public static void main(String args[]) {
        int i, n, show = 0, size = 0, debugForReload = 0;
        Map<String, Object> props = new HashMap<String, Object>();
        String cfgDir, homeDir, configFile = null;
        String action = null, category = null, name = null, groupName="default";
        List list = null, group = null;
        MonitorAgent agent = null;
        Object o;
        Thread c = null;

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
                    groupName = args[i+1];
                }
                break;
              case 'N':
                if (i+1 < args.length) {
                    name = args[i+1];
                }
                break;
              default:
            }
        }

        if (configFile == null)
            configFile = "/opt/qbroker/agent/Agent.json";

        try {
            FileReader fr = new FileReader(configFile);
            props = (Map) JSON2Map.parse(fr);
            fr.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
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
                category = "AGENT";
                name = "Agent";
            }
            else if ("disable".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "disable";
                if (category == null || category.charAt(0) == '-') {
                    category = "AGENT";
                    name = "Agent";
                }
                else if (name == null)
                    name = "Agent";
            }
            else if ("enable".equals(action.toLowerCase())) {
                priority = Event.WARNING;
                action = "enable";
                if (category == null || category.charAt(0) == '-') {
                    category = "AGENT";
                    name = "Agent";
                }
                else if (name == null)
                    name = "Agent";
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
                event.setAttribute("group", groupName);
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
                event.setAttribute("group", groupName);
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
            key = query(pstr, event);
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

        if ((o = props.get("HomeDir")) != null)
            homeDir = (String) o;
        else
            homeDir = "/opt/qbroker";

        File f = new File(homeDir);
        if (!f.exists() || !f.isDirectory() || !f.canRead())
            throw(new IllegalArgumentException("failed to read " + homeDir));

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
        }

        if ((o = props.get("URL")) != null) {
            try {
                EventLogger.enableWeblog((String) o);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to enable Weblog: " +
                    Event.traceStack(e)).send();
            }
        }

        if (debugForReload > 0) // reset cfgDir
            cfgDir = new File(configFile).getParent();
        else
            cfgDir = homeDir + FILE_SEPARATOR + "agent";

        if ((o = props.get("MailHost")) != null)
            MessageMailer.setSMTPHost((String) o);

        if (show > 0) {
            String text = JSON2Map.toJSON(props, "", "\n");
            i = text.lastIndexOf("}");
            System.out.println(text.substring(0, i));
        }

        // ConfigRepository
        if ((o = props.get("ConfigRepository")) != null && o instanceof String){
            String key = (String) o;
            File file = new File(cfgDir + FILE_SEPARATOR + key + ".json");
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
                new Event(Event.ERR, "failed to load the property file for "+
                    key + ": " + Event.traceStack(e)).send();
            }
        }

        // MonitorGroup
        if ((o = props.get("MonitorGroup")) != null && o instanceof List)
            group = (List) o;
        else
            group = new ArrayList();

        if (group == null || group.size() == 0) {
            new Event(Event.WARNING, "no MonitorGroup defined").send();
        }

        n = group.size();
        for (i=0; i<n; i++) { // merge individual configs into master config
            o = group.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            Map h = (Map) o;
            String configDir = (String) h.get("ConfigDir");
            if (configDir == null)
                configDir = cfgDir;
            list = (List) h.get("Monitor");
            if (list == null)
                list = new ArrayList();
            size += loadListProperties(list, configDir, show, props);
        }

        // MessageFlow
        if ((o = props.get("MessageFlow")) != null && o instanceof List)
            group = (List) o;
        else
            group = new ArrayList();

        n = group.size();
        for (i=0; i<n; i++) { // merge individual configs into master config
            o = group.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            Map h = (Map) o;
            String configDir = (String) h.get("ConfigDir");
            if (configDir == null)
                configDir = cfgDir;
            list = (List) h.get("Receiver");
            if (list == null)
                list = new ArrayList();
            size += loadListProperties(list, configDir, show, props);

            list = (List) h.get("Node");
            if (list == null)
                list = new ArrayList();
            size += loadListProperties(list, configDir, show, props);

            // implement include policy only on MessageNodes
            if ((o = h.get("IncludePolicy")) != null && o instanceof Map)
                loadIncludedProperties(list, configDir, show, props, (Map) o);

            list = (List) h.get("Persister");
            if (list == null)
                list = new ArrayList();
            size += loadListProperties(list, configDir, show, props);
        }

        if (show > 0) {
            System.out.println("} // end of " + configFile);
            System.exit(0);
        }
        else if (debugForReload > 0) try {
            agent = new MonitorAgent(props, debugForReload, configFile);
            System.exit(0);
        }
        catch (Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }

        try {
            agent = new MonitorAgent(props);
            c = new Thread(agent, "close");
            Runtime.getRuntime().addShutdownHook(c);
        }
        catch (Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }

        if (agent != null) try { // start the daemon
            agent.start();
            new Event(Event.INFO, "MonitorAgent " + agent.getName() +
                " exits").send();
        }
        catch (Throwable t) {
            new Event(Event.ERR, "MonitorMonitor " + agent.getName() +
                " aborts: " + Event.traceStack(t)).send();
            t.printStackTrace();
            agent.close();
        }

        if (c != null && c.isAlive()) try {
            c.join();
        }
        catch (Exception e) {
        }

        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("MonitorAgent Version 1.0 (written by Yannan Lu)");
        System.out.println("MonitorAgent: a Monitor Agent");
        System.out.println("Usage: java org.qbroker.flow.MonitorAgent [-?|-l|-I ConfigFile]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -l: list all properties");
        System.out.println("  -d: debug mode for reload testing only");
        System.out.println("  -I: ConfigFile (default: /opt/qbroker/agent/Agent.json)");
        System.out.println("  -A: Action for query (default: none)");
        System.out.println("  -C: Category for query (default: AGENT)");
        System.out.println("  -G: Group for query (default: default)");
        System.out.println("  -N: Name for query (default: value of categort)");
        System.out.println("  -W: Doing nothing (for stopping service on Win2K)");
/*
        System.out.println("  -L: LogDir (default: no logging)");
        System.out.println("  -H: HomeDir (default: /opt/qbroker)");
        System.out.println("  -N: MaxNumberThread");
*/
    }
}
