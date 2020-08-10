package org.qbroker.flow;

/* MessageFlow.java - a JMS message flow */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Service;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.jms.MessageUtils;
import org.qbroker.receiver.MessageReceiver;
import org.qbroker.persister.MessagePersister;
import org.qbroker.node.MessageNode;
import org.qbroker.event.Event;

/**
 * MessageFlow is a message-driven application with a group of inter-linked
 * message nodes.  Each message node is well designed for certain purpose.
 * It is running in a separate thread and communicates with the linked nodes
 * via the internal queues.  There are three types of message nodes,
 * MessageReceiver, MessageNode and MessagePersister.  The nodes of
 * MessageReceiver and MessagePersister are for JMSMessage I/O.  The nodes of
 * MessageNode are for JMSMessage processing.  With these three basic types of
 * nodes, you can build a message flow for various purposes, just like you can
 * build objects with Lego components.  For the I/O nodes, MessageFlow uses
 * them to pick up or deliver messages via various transports, like JMS, TCP
 * Socket, FTP, HTTP, JDBC, Serial Port, UDP Packet, SYSLOG, etc.  It is also
 * expandable and extensible by implementing new message nodes.
 *<br><br>
 * Between two linked message nodes, there is a communication channel for them
 * to transport JMSMessages internally.  MessageFlow uses XQueue for the
 * inter-node communications.  As an instance of XQueue, also called a link,
 * it supports both FIFO and transactions.  A MessageFlow can have at least
 * one link for the minimum of two nodes.  The properties of a link, like
 * Capacity and XAMode, are always determined by a linked node of MessageNode
 * type.  If there is no MessageNode on the link, the properties of the link
 * will be determined by the default values of MessageFlow.  If the nodes at
 * both ends of the link are type of MessageNode, the node at the sending
 * side wins.  Among all the links, root is a special link.  For example,
 * the root link controls the XAMode of all other links.  XAMode and Capacity
 * of MessageFlow are used only as the default values.  For a node of either
 * MessageReceiver or MessagePersister, its XAMode only determines its JMS
 * Transaction type.
 *<br><br>
 * MessageFlow supports the escalation channel for message nodes to escalate
 * messages to the container or the caller.  Its name of "escalation" is a
 * reserved name and it can only be used in MessageNodes as an outLink with
 * its partition specified explicitly.
 *<br><br>
 * MessageFlow supports the static ConfigTemplate for both Receiver and
 * Persister. It means a list of persisters or receivers can share the same
 * configuration template. But it does not allow duplicated names for either
 * generated persisters or generated recievers.
 *<br><br>
 * FLOW_READY    initialized and ready to work<br>
 * FLOW_RUNNING  running normally<br>
 * FLOW_RETRYING trying to get back to running state<br>
 * FLOW_PAUSE    pausing for a bit while still running<br>
 * FLOW_STANDBY  standby for a bit longer while still running<br>
 * FLOW_DISABLED flow is disabled<br>
 * FLOW_STOPPED  all threads stopped, can be restart later<br>
 * FLOW_CLOSED   end of life time, is not runable any more<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageFlow implements Runnable {
    private String name;
    private int capacity = 1;
    private int pauseTime;
    private int standbyTime;
    private long mtime, waitTime;
    private Map<String, String> upLink, outLink;
    private XQueue root = null;
    private XQueue control = null;
    private int activeNodeID = -1, passiveNodeID = -1;
    private int xaMode = MessageUtils.XA_CLIENT;
    private int flowStatus, statusOffset, debug = 0;
    private int defaultStatus, previousStatus;
    private boolean isDisabled, isDaemon, takebackEnabled = false;
    private String checkpointDir = null;
    private int[] partition = new int[] {0, 0};
    private XQueue escalation = null;
    private AssetList nodeList;
    private AssetList receiverList;
    private AssetList persisterList;
    private AssetList threadList;
    private AssetList linkList;
    private AssetList templateList;
    private ThreadGroup thg;
    private Map cachedProps = null;
    private Map<String, Object> rcvrMap, pstrMap;
    private static Map<String, Object> componentMap, includeMap, linkMap;

    public final static int MF_RCVR = 0;
    public final static int MF_NODE = 1;
    public final static int MF_PSTR = 2;
    public final static int GROUP_ID = 0;
    public final static int GROUP_TID = 1;
    public final static int GROUP_TYPE = 2;
    public final static int GROUP_SIZE = 3;
    public final static int GROUP_HBEAT = 4;
    public final static int GROUP_TIMEOUT = 5;
    public final static int GROUP_STATUS = 6;
    public final static int GROUP_COUNT = 7;
    public final static int GROUP_SPAN = 8;
    public final static int GROUP_RETRY = 9;
    public final static int GROUP_TIME = 10;
    public final static int FLOW_READY = Service.SERVICE_READY;
    public final static int FLOW_RUNNING = Service.SERVICE_RUNNING;
    public final static int FLOW_RETRYING = Service.SERVICE_RETRYING;
    public final static int FLOW_PAUSE = Service.SERVICE_PAUSE;
    public final static int FLOW_STANDBY = Service.SERVICE_STANDBY;
    public final static int FLOW_DISABLED = Service.SERVICE_DISABLED;
    public final static int FLOW_STOPPED = Service.SERVICE_STOPPED;
    public final static int FLOW_CLOSED = Service.SERVICE_CLOSED;
    private final static String[] statusText = Service.statusText;
    private final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    /** Creates new MessageFlow */
    public MessageFlow(Map props, XQueue escalation) {
        Object o;
        Browser browser;
        long[] info;
        String className;
        List list;
        int n, i, nodeSize = 32;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("name not defined"));
        name = (String) o;

        if ((o = props.get("MaxNumberNode")) != null)
            nodeSize = Integer.parseInt((String) o);
        if (nodeSize <= 0)
            nodeSize = 32;

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;

        if ((o = props.get("XAMode")) != null) // XQ type determined by QB
            xaMode = Integer.parseInt((String) o);

        if ((o = props.get("Mode")) == null)
            isDaemon = true;
        else if ("daemon".equals(((String) o).toLowerCase()))
            isDaemon = true;
        else
            isDaemon = false;

        if ((o = props.get("TakebackEnabled")) != null &&
            "true".equalsIgnoreCase((String) o))
            takebackEnabled = true;

        // partition for escalations on root, sharing with other receivers
        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else { // assume no receiver on root, so use all
            partition[0] = 0;
            partition[1] = 0;
        }

        if ((o = props.get("CheckpointDir")) != null)
            checkpointDir = (String) o;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        this.escalation = escalation;
        thg = new ThreadGroup(name);

        mtime = System.currentTimeMillis();
        flowStatus = FLOW_READY;
        previousStatus = FLOW_CLOSED;
        statusOffset = 0 - TimeWindows.EXCEPTION;

        rcvrMap = new HashMap<String, Object>();
        pstrMap = new HashMap<String, Object>();
        receiverList = new AssetList("Receiver", nodeSize + nodeSize);
        nodeList = new AssetList("Node", nodeSize);
        persisterList = new AssetList("Persister", nodeSize + nodeSize);
        threadList = new AssetList("Thread", 4 * nodeSize);
        linkList = new AssetList("Link", 3 * nodeSize);
        templateList = new AssetList("Temp", 3 * nodeSize);
        initialize(props);
        initXQueues();

        setNodeStatus(flowStatus, MF_PSTR, persisterList);
        setNodeStatus(flowStatus, MF_NODE, nodeList);
        setNodeStatus(flowStatus, MF_RCVR, receiverList);

        if (defaultStatus == FLOW_RUNNING) { // init control link
            String key;
            MessageNode node;
            if ((o = props.get("ActiveNode")) != null) {
                key = (String) o;
                node = (MessageNode) nodeList.get(key);
                if (node != null) {
                    activeNodeID = nodeList.getID(key);
                    key = node.getLinkName();
                    control = (XQueue) linkList.get(key);
                }
            }
            if ((o = props.get("PassiveNode")) != null) {
                key = (String) o;
                node = (MessageNode) nodeList.get(key);
                if (node != null) {
                    passiveNodeID = nodeList.getID(key);
                    if (passiveNodeID != activeNodeID) // two nodes
                        node.setStatus(FLOW_DISABLED);
                }
            }
            if (activeNodeID < 0 || passiveNodeID < 0)
                control = null;
            else if ((debug & Service.DEBUG_INIT) > 0)
                new Event(Event.DEBUG, name + ": control link of " +
                    control.getName() + " is set for " + activeNodeID +
                    " / " + passiveNodeID).send();
        }

        cachedProps = props;
    }

    public MessageFlow(Map props) {
        this(props, null);
    }

    /**
     * It initializes all objects and threads including the ones for
     * the monitor
     */
    private void initialize(Map props) {
        int i, j, k, n, hmax, size;
        Map h, c, node;
        List list = null;
        Browser browser;
        long[] info;
        Object o;
        long tm;
        String key, str;

        if ((o = props.get("DefaultStatus")) != null) {
            key = (String) o;
            if (key.indexOf("${") >= 0)
                key = MessageUtils.substitute(key);
            defaultStatus = Integer.parseInt(key);
        }
        else
            defaultStatus = FLOW_RUNNING;

        if (defaultStatus <= FLOW_RETRYING)
            defaultStatus = FLOW_RUNNING;
        else if (defaultStatus >= FLOW_STOPPED)
            defaultStatus = FLOW_STOPPED;

        mtime = System.currentTimeMillis();

        if ((o = props.get("WaitTime")) == null ||
            (waitTime = Integer.parseInt((String) o)) <= 0)
            waitTime = 500L;

        if ((o = props.get("PauseTime")) == null ||
            (pauseTime = Integer.parseInt((String) o)) <= 0)
            pauseTime = 5000;

        if ((o = props.get("StandbyTime")) == null ||
            (standbyTime = Integer.parseInt((String) o)) <= 0)
            standbyTime = 15000;

        upLink = new HashMap<String, String>();
        outLink = new HashMap<String, String>();

        initNodeList(props, MF_PSTR);
        tm = System.currentTimeMillis();
        browser = persisterList.browser();
        while ((i = browser.next()) >= 0) {
            info = persisterList.getMetaData(i);
            info[GROUP_STATUS] = MessagePersister.PSTR_READY;
            info[GROUP_TIME] = tm;
            o = persisterList.get(i);
            key = ((MessagePersister) o).getLinkName();
            if (key == null)
                key = "unknown";
            if (!outLink.containsKey(key))
                outLink.put(key, "0");
            if (upLink.containsKey(key))
                k = Integer.parseInt((String) upLink.get(key));
            else
                k = 0;
            upLink.put(key, String.valueOf(++k));
        }

        initNodeList(props, MF_NODE);
        tm = System.currentTimeMillis();
        browser = nodeList.browser();
        while ((i = browser.next()) >= 0) {
            info = nodeList.getMetaData(i);
            info[GROUP_STATUS] = MessageNode.NODE_READY;
            info[GROUP_TIME] = tm;
            o = nodeList.get(i);
            key = ((MessageNode) o).getLinkName();
            if (key == null)
                key = "unknown";
            if (!outLink.containsKey(key))
                outLink.put(key, "0");
            if (upLink.containsKey(key))
                k = Integer.parseInt((String) upLink.get(key));
            else
                k = 0;
            upLink.put(key, String.valueOf(++k));
        }

        initNodeList(props, MF_RCVR);
        tm = System.currentTimeMillis();
        browser = receiverList.browser();
        while ((i = browser.next()) >= 0) {
            info = receiverList.getMetaData(i);
            info[GROUP_STATUS] = MessageReceiver.RCVR_READY;
            info[GROUP_TIME] = tm;
            o = receiverList.get(i);
            str = ((MessageReceiver) o).getName();
            key = ((MessageReceiver) o).getLinkName();
            if (outLink.containsKey(key)) {
                k = Integer.parseInt((String) outLink.get(key));
                outLink.put(key, String.valueOf(++k));
            }
            else {
                new Event(Event.ERR, MF_RCVR + ": " + key + " for " +
                    str + " has no consumers at the output end").send();
            }
        }

        browser = nodeList.browser();
        while ((i = browser.next()) >= 0) {
            o = nodeList.get(i);
            str = ((MessageNode) o).getName();
            size = ((MessageNode) o).getNumberOfOutLinks();
            for (j=0; j<size; j++) {
                key = ((MessageNode) o).getLinkName(j);
                if (outLink.containsKey(key)) {
                    k = Integer.parseInt((String) outLink.get(key));
                    outLink.put(key, String.valueOf(++k));
                }
                else if ("escalation".equals(key)) { // link to container
                    k = 0;
                    outLink.put(key, String.valueOf(++k));
                }
                else {
                    new Event(Event.ERR, MF_NODE + ": " + key + " for " +
                        str + " has no consumers at the output end").send();
                }
            }
        }
    }

    private void initNodeList(Map props, int type) {
        Object o;
        Map<String, Object> ph;
        AssetList al = null;
        List list = null;
        String key, str;
        int i, k, n, id;

        switch (type) {
          case MF_RCVR:
            o = props.get("Receiver");
            al = receiverList;
            break;
          case MF_NODE:
            o = props.get("Node");
            al = nodeList;
            break;
          case MF_PSTR:
            o = props.get("Persister");
            al = persisterList;
            break;
          default:
            return;
        }

        if (o != null && o instanceof List)
            list = (List) o;

        if (list == null)
            list = new ArrayList();

        n = list.size();
        for (i=0; i<n; i++) { // for nodes
            o = list.get(i);
            if (o == null || !(o instanceof String || o instanceof Map)) {
                new Event(Event.WARNING, name + " props: " +
                    " an illegal object at " + i).send();
                continue;
            }
            key = null;
            if (o instanceof Map) { // for configTemplate
                ConfigTemplate temp = null;
                Map<String, Object> c;
                int m;
                if (type == MF_NODE) {
                    new Event(Event.ERR, name +
                        " props: config template is not supported for Node " +
                        key + " at " + i).send();
                    continue;
                }
                try {
                    c = Utils.cloneProperties((Map) o);
                    key = (String) c.get("Name");
                    c.put("Property", props.get(key));
                    temp = new ConfigTemplate(c);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name +
                        " props: failed to init config template for " + key +
                        " at " + i + ": " + Event.traceStack(e)).send();
                    continue;
                }
                m = temp.getSize();
                if (!templateList.containsKey(key))
                    templateList.add(key, new long[]{m, type}, temp);
                else {
                    new Event(Event.ERR, name +
                        " props: duplicated key for config template: " +
                        key).send();
                    temp.close();
                    continue;
                }
                for (int j=0; j<m; j++) { // init all generated nodes
                    str = temp.getItem(j);
                    o = temp.getProps(str);
                    if (o == null || !(o instanceof Map)) {
                        new Event(Event.ERR, name + " failed to get props for "+
                            key + ":" + str + " at " + j + "/" + i).send();
                        continue;
                    }
                    ph = Utils.cloneProperties((Map) o);
                    if (type == MF_RCVR) { // for sharing the same linkName
                        str = (String) ph.get("LinkName");
                        if (str == null)
                            str = "unknown";
                        else if (str.indexOf("${") >= 0)
                            str = MessageUtils.substitute(str);
                        if (outLink.containsKey(str)) {//downstream is on the xq
                            k = Integer.parseInt((String) outLink.get(str));
                        }
                        else { // no persisters or nodes listens on the xq
                            k = 0;
                            new Event(Event.ERR, name + ": XQ " + str + " for "+
                                key + " at " + j + "/" + i +
                                " has no consumers at the output end").send();
                        }
                        ph.put("CellID", String.valueOf(k));
                        outLink.put(str, String.valueOf(++k));
                    }
                    str = (String) ph.get("Name");
                    if (str == null || str.length() <= 0) {
                        new Event(Event.ERR, name + " name is not defined for "+
                            key + " at " + j + "/" + i).send();
                        continue;
                    }
                    id = addNode(mtime, ph);
                    if (id < 0)
                        new Event(Event.ERR, name + " failed to add " + key +
                        ":" + str + " to list at " + j + "/" + i + ": " +
                        al.size() + "/" + al.getCapacity()).send();
                }
            }
            else { // normal case
                key = (String) o;
                if ((o = props.get(key)) == null || !(o instanceof Map)) {
                    new Event(Event.ERR, "props: " + key +
                        " is an empty object or not a Map for "+
                        name).send();
                    continue;
                }

                ph = Utils.cloneProperties((Map) o);
                if (type == MF_RCVR) { // for sharing the same linkName
                    str = (String) ph.get("LinkName");
                    if (str == null)
                        str = "unknown";
                    else if (str.indexOf("${") >= 0)
                        str = MessageUtils.substitute(str);
                    if (outLink.containsKey(str)) { // downstream is on the xq
                        k = Integer.parseInt((String) outLink.get(str));
                    }
                    else { // no persisters or nodes listens on the xq
                        k = 0;
                        new Event(Event.ERR, name + ": XQ " + str + " for " +
                            key + " has no consumers at the output end").send();
                    }
                    ph.put("CellID", String.valueOf(k));
                    outLink.put(str, String.valueOf(++k));
                }

                id = addNode(mtime, ph);
                if (id < 0)
                    new Event(Event.ERR, name + " failed to add " + key +
                        " to list at " + i + ": " + al.size() + "/" +
                        al.getCapacity()).send();
            }
        }
    }

    /** It initializes a new node, adds it to the list and returns its id */
    private int addNode(long tm, Map ph) {
        Object o;
        long[] info;
        String key, str;
        int i, k, id = -1;

        if (ph == null || ph.size() <= 0)
            return -1;

        ph = MessageUtils.substituteProperties(ph);
        key = (String) ph.get("Name");
        if ((o = ph.get("LinkName")) == null) { // check linkName first
            new Event(Event.ERR,name+": linkName not defined for "+key).send();
            ph.clear();
            return -1;
        }
        o = MessageUtils.initNode(ph, name);

        if (o == null) {
            ph.clear();
            return -1;
        }
        else if (o instanceof MessageReceiver) {
            k = -1;
            if (!receiverList.containsKey(key)) { // first
                info = new long[GROUP_TIME + 1];
                id = receiverList.add(key, info, o);
                info[GROUP_ID] = -1;
            }
            else { // chain up duplicated keys
                k = receiverList.getID(key);
                info = receiverList.getMetaData(k);
                while (info[GROUP_ID] >= 0) {
                    k = (int) info[GROUP_ID];
                    info = receiverList.getMetaData(k);
                }
                id = receiverList.add(key + "_" + k, new long[GROUP_TIME+1], o);
                if (id >= 0) { // new duplicate added
                    info[GROUP_ID] = id;
                    info = receiverList.getMetaData(id);
                    info[GROUP_ID] = -1;
                    rcvrMap.put(key + "_" + k, key);
                }
            }
            if (id >= 0) {
                info = receiverList.getMetaData(id);
                if (defaultStatus < FLOW_STOPPED) {
                    Thread th;
                    th = new Thread(thg, this, name + "/Receiver_" + id);
                    th.setDaemon(true);
                    th.setPriority(Thread.NORM_PRIORITY);
                    k = threadList.add(name + "/Receiver_" + id, null, th);
                    info[GROUP_TID] = k;
                }
                else
                    info[GROUP_TID] = -1;
            }
            else { // list is full?
                new Event(Event.ERR, name + " failed to add an instance of " +
                    key + ((k<0) ? "" : "_" + k) + " at " + receiverList.size()+
                    " out of " + receiverList.getCapacity()).send();
                ((MessageReceiver) o).close();
            }
        }
        else if (o instanceof MessageNode) {
            info = new long[GROUP_TIME + 1];
            if (!nodeList.containsKey(key)) {
                id = nodeList.add(key, info, o);
                if (id >= 0) {
                    if (defaultStatus < FLOW_STOPPED) {
                        Thread th;
                        th = new Thread(thg, this, name + "/Node_" + id);
                        th.setDaemon(true);
                        th.setPriority(Thread.NORM_PRIORITY);
                        k = threadList.add(name + "/Node_" + id, null, th);
                        info[GROUP_TID] = k;
                    }
                    else
                        info[GROUP_TID] = -1;
                    // restore from the checkpoint
                    restore((MessageNode) o);
                }
                else { // list is full?
                    new Event(Event.ERR, name +" failed to add an instance of "+
                        key + " at " + nodeList.size() + " out of " +
                        nodeList.getCapacity()).send();
                    ((MessageNode) o).close();
                }
            }
            else {
                new Event(Event.ERR, name + ": duplicated node for " +
                    key).send();
                id = -1;
                ((MessageNode) o).close();
            }
        }
        else if (o instanceof MessagePersister) {
            k = -1;
            if (!persisterList.containsKey(key)) { // first
                info = new long[GROUP_TIME + 1];
                id = persisterList.add(key, info, o);
                info[GROUP_ID] = -1;
            }
            else { // chain up duplicated keys
                k = persisterList.getID(key);
                info = persisterList.getMetaData(k);
                while (info[GROUP_ID] >= 0) {
                    k = (int) info[GROUP_ID];
                    info = persisterList.getMetaData(k);
                }
                id = persisterList.add(key + "_" + k, new long[GROUP_TIME+1],o);
                if (id >= 0) { // new duplicate added
                    info[GROUP_ID] = id;
                    info = persisterList.getMetaData(id);
                    info[GROUP_ID] = -1;
                    pstrMap.put(key + "_" + k, key);
                }
            }
            if (id >= 0) {
                info = persisterList.getMetaData(id);
                if (defaultStatus < FLOW_STOPPED) {
                    Thread th;
                    th = new Thread(thg, this, name + "/Persister_" + id);
                    th.setDaemon(true);
                    th.setPriority(Thread.NORM_PRIORITY);
                    k = threadList.add(name + "/Persister_" + id, null, th);
                    info[GROUP_TID] = k;
                }
                else
                    info[GROUP_TID] = -1;
            }
            else { // list is full?
                new Event(Event.ERR, name + " failed to add an instance of "+
                    key + ((k<0) ? "" : "_" + k) + " at " +persisterList.size()+
                    " out of " + persisterList.getCapacity()).send();
                ((MessagePersister) o).close();
            }
        }
        else if (o instanceof InvocationTargetException) {
            InvocationTargetException e = (InvocationTargetException) o;
            Throwable ex = e.getTargetException();
            if (ex == null) {
                new Event(Event.ERR, name + " failed to instantiate " +key+
                    ": " + Event.traceStack(e)).send();
            }
            else if (ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    new Event(Event.ERR, name + " failed to instantiate " +key+
                        ": " + ee.toString() +" "+ Event.traceStack(ex)).send();
                else
                    new Event(Event.ERR, name + " failed to instantiate " +key+
                        ": " + Event.traceStack(ex)).send();
            }
            else {
                new Event(Event.ERR, name + " failed to instantiate " + key +
                    ": " + Event.traceStack(ex)).send();
            }
            ph.clear();
            return -2;
        }
        else if (o instanceof Exception) {
            Exception e = (Exception) o;
            new Event(Event.ERR, name + " failed to instantiate " + key +
                ": " + Event.traceStack(e)).send();
            ph.clear();
            return -2;
        }
        else if (o instanceof Error) {
            Error e = (Error) o;
            new Event(Event.ERR, name + " failed to instantiate " + key +
                ": " + e.toString()).send();
            ph.clear();
            Event.flush(e);
        }
        else {
            new Event(Event.ERR, name + " failed to instantiate " + key +
                ": " + o.toString()).send();
            ph.clear();
            return -2;
        }

        str = (String) ph.get("ClassName");
        if (str == null)
            str = "unknown";
        k = str.lastIndexOf('.');
        if (k < 0)
            k = 0;
        else
            k ++;
        new Event(Event.INFO, name + " " + str.substring(k) + " for " +
            key + " is instantiated").send();
        ph.clear();

        return id;
    }

    /**
     * With the assumption that the asset with the id, the key and the metadata
     * of info has already been removed from the list, it cleans up the map
     * and chains up the broken chain for the duplicates.  Upon failure, it
     * returns -1.
     */
    private static int chainupDuplicates(int id, String key, long[] info,
        Map map, AssetList list) {
        int k = -1;
        if (list == null || info == null || info.length <= GROUP_TIME ||
            key == null || key.length() <= 0)
            return -1;
        if (id < 0 || id >= list.getCapacity() || list.getKey(id) != null ||
            list.containsKey(key))
            return -1;

        if (map.containsKey(key)) { // duplicate has been removed
            long[] state;
            String base = (String) map.get(key);
            k = list.getID(base);
            do { // look for the id on lower chain
                state = list.getMetaData(k);
                if (state[GROUP_ID] == id) { // found it
                    state[GROUP_ID] = info[GROUP_ID];
                    break;
                }
            } while ((k = (int) state[GROUP_ID]) >= 0);
            if (info[GROUP_ID] >= 0) { // middle of chain
                key = list.replaceKey(k, key);
                map.remove(key);
            }
            else { // end of chain
                map.remove(key);
            }
        }
        else { // base has been removed
            if (info[GROUP_ID] >= 0) { // base of chain
                k = (int) info[GROUP_ID];
                key = list.replaceKey(k, key);
                map.remove(key);
            }
            else { // end of chain
                k = id;
            }
        }
        return k;
    }

    /**
     * It disables the id-th node of the type and returns the disabled status
     * upon success.  If no node for the given id, it returns -1. In case of
     * timeout, it returns the previous status right before the final reset of
     * status.
     */
    private int disableNode(int id, int type) {
        MessageReceiver rcvr = null;
        MessagePersister pstr = null;
        MessageNode node = null;
        String key = null, str = null;
        int status = 0;
        long[] info = null;

        switch (type) {
          case MF_RCVR:
            info = receiverList.getMetaData(id);
            rcvr = (MessageReceiver) receiverList.get(id);
            if (rcvr == null || info == null)
                return -1;
            status = rcvr.getStatus();
            key = rcvr.getLinkName();
            str = rcvr.getName();
            break;
          case MF_NODE:
            info = nodeList.getMetaData(id);
            node = (MessageNode) nodeList.get(id);
            if (node == null || info == null)
                return -1;
            status = node.getStatus();
            key = node.getLinkName();
            str = node.getName();
            break;
          case MF_PSTR:
            info = persisterList.getMetaData(id);
            pstr = (MessagePersister) persisterList.get(id);
            if (pstr == null || info == null)
                return -1;
            status = pstr.getStatus();
            key = pstr.getLinkName();
            str = pstr.getName();
            break;
          default:
            return -1;
        }

        info[GROUP_STATUS] = status;
        if (status >= FLOW_READY && status <= FLOW_RETRYING) {
            int mask = 0;
            int pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
            XQueue xq = (XQueue) linkList.get(key);
            if (xq != null) { // with xq
                mask = xq.getGlobalMask();
                switch (type) {
                  case MF_RCVR:
                    rcvr.setStatus(FLOW_READY);
                    xq.setGlobalMask(mask | XQueue.PAUSE);
                    for (int j=0; j<10; j++) { // wait for disabled
                        if ((status = rcvr.getStatus()) == FLOW_DISABLED)
                            break;
                        else try {
                            Thread.sleep(pt);
                        }
                        catch (Exception e) {
                        }
                    }
                    status = rcvr.getStatus();
                    mask = xq.getGlobalMask();
                    break;
                  case MF_NODE:
                    node.setStatus(FLOW_READY);
                    xq.setGlobalMask(mask | XQueue.STANDBY);
                    for (int j=0; j<10; j++) { // wait for disabled
                        if ((status = node.getStatus()) == FLOW_DISABLED)
                            break;
                        else try {
                            Thread.sleep(pt);
                        }
                        catch (Exception e) {
                        }
                    }
                    status = node.getStatus();
                    mask = xq.getGlobalMask();
                    break;
                  case MF_PSTR:
                    pstr.setStatus(FLOW_READY);
                    xq.setGlobalMask(mask | XQueue.STANDBY);
                    for (int j=0; j<10; j++) { // wait for disabled
                        if ((status = pstr.getStatus()) == FLOW_DISABLED)
                            break;
                        else try {
                            Thread.sleep(pt);
                        }
                        catch (Exception e) {
                        }
                    }
                    status = pstr.getStatus();
                    mask = xq.getGlobalMask();
                    break;
                  default:
                    break;
                }

                if (status != FLOW_DISABLED) { // timed out
                    new Event(Event.ERR, name + ": timed out to disable " +
                        str + ": " + statusText[status]).send();
                }
            }
            else { // no xq
                new Event(Event.ERR, name + ": null xq of " + key +
                    " for " + str + ": " + statusText[status]).send();
            }
            if (status != FLOW_DISABLED) switch (type) {
              case MF_RCVR:
                rcvr.setStatus(FLOW_DISABLED);
                break;
              case MF_NODE:
                node.setStatus(FLOW_DISABLED);
                break;
              case MF_PSTR:
                pstr.setStatus(FLOW_DISABLED);
                break;
              default:
                break;
            }
            info[GROUP_STATUS] = FLOW_DISABLED;
            if (xq != null) { // remove the disable bit
                if (type == MF_RCVR && (mask & XQueue.PAUSE) > 0)
                    xq.setGlobalMask(mask ^ XQueue.PAUSE);
                else if ((mask & XQueue.STANDBY) > 0)
                    xq.setGlobalMask(mask ^ XQueue.STANDBY);
            }
        }
        else if (status >= FLOW_PAUSE && status < FLOW_DISABLED) {
            switch (type) {
              case MF_RCVR:
                rcvr.setStatus(FLOW_DISABLED);
                break;
              case MF_NODE:
                node.setStatus(FLOW_DISABLED);
                break;
              case MF_PSTR:
                pstr.setStatus(FLOW_DISABLED);
                break;
              default:
                break;
            }
            info[GROUP_STATUS] = FLOW_DISABLED;
            status = FLOW_DISABLED;
        }

        return status;
    }

    /** stops the id-th node of the type and removes the thread */
    private void stopNode(int id, int type) {
        MessageReceiver rcvr = null;
        MessagePersister pstr = null;
        MessageNode node = null;
        long[] info = null;
        String key;
        int k, status;

        // disable the node first
        switch (type) {
          case MF_RCVR:
            info = receiverList.getMetaData(id);
            rcvr = (MessageReceiver) receiverList.get(id);
            if (rcvr == null || info == null)
                return;
            key = rcvr.getName();
            status = disableNode(id, type);
            if (status < FLOW_PAUSE)
                new Event(Event.ERR, name + " failed to disable receiver " +
                    key + ": " + ((status >= 0) ? statusText[status] :
                    String.valueOf(status))).send();
            rcvr.setStatus(FLOW_STOPPED);
            break;
          case MF_NODE:
            info = nodeList.getMetaData(id);
            node = (MessageNode) nodeList.get(id);
            if (node == null || info == null)
                return;
            key = node.getName();
            status = disableNode(id, type);
            if (status < FLOW_PAUSE)
                new Event(Event.ERR, name + " failed to disable node " + key +
                    ": " + ((status >= 0) ? statusText[status] :
                    String.valueOf(status))).send();
            node.setStatus(FLOW_STOPPED);
            break;
          case MF_PSTR:
            info = persisterList.getMetaData(id);
            pstr = (MessagePersister) persisterList.get(id);
            if (pstr == null || info == null)
                return;
            key = pstr.getName();
            status = disableNode(id, type);
            if (status < FLOW_PAUSE)
                new Event(Event.ERR, name + " failed to disable persister " +
                    key + ": " + ((status >= 0) ? statusText[status] :
                    String.valueOf(status))).send();
            pstr.setStatus(FLOW_STOPPED);
            break;
          default:
            return;
        }

        info[GROUP_STATUS] = FLOW_STOPPED;
        if ((k = (int) info[GROUP_TID]) >= 0) { // remove the thread
            Thread th = (Thread) threadList.remove(k);
            int pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
            info[GROUP_TID] = -1;
            if (th != null) for (int i=0; i<10; i++) { // join the thread
                if (!th.isAlive())
                    break;
                th.interrupt();
                try {
                    th.join(pt);
                }
                catch (Exception e) {
                }
            }
            if (th != null && th.isAlive())
                new Event(Event.ERR, name + " failed to stop the thread: " +
                    th.getName() + " for " + key).send();
        }
    }

    /** starts the id-th node of the type and the thread */
    private void startNode(int id, int type) {
        MessageReceiver rcvr = null;
        MessagePersister pstr = null;
        MessageNode node = null;
        Thread th;
        String str = null;
        long[] info = null;
        int k;

        switch (type) {
          case MF_RCVR:
            info = receiverList.getMetaData(id);
            rcvr = (MessageReceiver) receiverList.get(id);
            if (rcvr == null || info == null)
                return;
            rcvr.setStatus(FLOW_RUNNING);
            str = "Receiver";
            break;
          case MF_NODE:
            info = nodeList.getMetaData(id);
            node = (MessageNode) nodeList.get(id);
            if (node == null || info == null)
                return;
            node.setStatus(FLOW_RUNNING);
            str = "Node";
            break;
          case MF_PSTR:
            info = persisterList.getMetaData(id);
            pstr = (MessagePersister) persisterList.get(id);
            if (pstr == null || info == null)
                return;
            pstr.setStatus(FLOW_RUNNING);
            str = "Persister";
            break;
          default:
            return;
        }

        if ((k = (int) info[GROUP_TID]) < 0) { // init the thread
            str = name + "/" + str + "_" + id;
            th = new Thread(thg, this, str);
            th.setDaemon(true);
            th.setPriority(Thread.NORM_PRIORITY);
            k = threadList.add(str, null, th);
            info[GROUP_TID] = k;
        }

        th = (Thread) threadList.get(k);
        if (th != null) // start the thread
            th.start();
    }

    /**
     * sets the status on all the nodes in the list
     */
    private int setNodeStatus(int status, int type, AssetList list) {
        MessageReceiver rcvr;
        MessagePersister pstr;
        MessageNode node;
        Browser browser;
        long[] info;
        long tm = System.currentTimeMillis();
        int i;

        if (list == null)
            return -1;
        browser = list.browser();
        switch (type) {
          case MF_RCVR:
            while ((i = browser.next()) >= 0) {
                info = list.getMetaData(i);
                rcvr = (MessageReceiver) list.get(i);
                info[GROUP_STATUS] = rcvr.getStatus();
                info[GROUP_TIME] = tm;
                rcvr.setStatus(status);
            }
            break;
          case MF_PSTR:
            while ((i = browser.next()) >= 0) {
                info = list.getMetaData(i);
                pstr = (MessagePersister) list.get(i);
                info[GROUP_STATUS] = pstr.getStatus();
                info[GROUP_TIME] = tm;
                pstr.setStatus(status);
            }
            break;
          case MF_NODE:
            while ((i = browser.next()) >= 0) {
                info = list.getMetaData(i);
                node = (MessageNode) list.get(i);
                info[GROUP_STATUS] = node.getStatus();
                info[GROUP_TIME] = tm;
                node.setStatus(status);
            }
            break;
          default:
            break;
        }
        return list.size();
    }

    /**
     * initializes all XQueues used by all the nodes and propagates acks
     */
    private void initXQueues() {
        String linkName, key;
        MessageNode node;
        MessageReceiver rcvr;
        Browser browser, b;
        Map<String, String> map, doneMap;
        XQueue xq;
        boolean ack = false;
        int[] list, nids;
        int i, ii, j, jj, cc, cn, k, kk, m, n, ipps, opps;

        m = 0;

        n = nodeList.size();
        nids = new int[n];  // store node ids packed in generation order
        jj = n;
        n = 0;
        cc = capacity;
        browser = nodeList.browser();
        while((i = browser.next()) >= 0) { // xa, capacity of root set by nodes
            node = (MessageNode) nodeList.get(i);
            linkName = node.getLinkName();
            if (linkName == null || !"root".equals(linkName)) { // only for root
                nids[--jj] = i;
                continue;
            }
            k = node.getXAMode();
            if (k > 0 && (k & MessageUtils.XA_CLIENT) > 0) // update the default
                ack = true;
            cn = node.getCapacity();
            if (cn > cc)
                cc = cn;
            nids[n++] = i;
        }

        root = new IndexedXQueue("root", cc);
        disableAck(root);

        browser = receiverList.browser();
        while((i = browser.next()) >= 0) { // XA_CLIENT determined by receivers
            rcvr = (MessageReceiver) receiverList.get(i);
            linkName = rcvr.getLinkName();
            if (linkName == null || !"root".equals(linkName))
                continue;
            k = rcvr.getXAMode();
            if (k > 0 && (k & MessageUtils.XA_CLIENT) > 0) { // enable root
                enableAck(root);
                break;
            }
        }
        k = root.getGlobalMask();
        if (ack && (k & XQueue.EXTERNAL_XA) == 0) // overwrite XA bit on root
            enableAck(root);
        // update the default ack
        ack = ((root.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        if (upLink.containsKey("root"))
            ipps = Integer.parseInt((String) upLink.get("root"));
        else
            ipps = 0;
        if (outLink.containsKey("root"))
            opps = Integer.parseInt((String) outLink.get("root"));
        else
            opps = 0;
        linkList.add("root", new long[]{cc, ipps, opps}, root);
        m ++;

        if (escalation != null) {
            cn = escalation.getCapacity();
            linkList.add("escalation", new long[]{cn, 0, 0}, escalation);
            m ++;
        }

        map = new HashMap<String, String>();
        k = receiverList.getCapacity();
        list = new int[k];
        k = 0;
        list[k++] = 0;
        map.put("root", null);

        // init xqs linked to other non-root receivers
        browser = receiverList.browser();
        while((i = browser.next()) >= 0) { // for receivers
            rcvr = (MessageReceiver) receiverList.get(i);
            linkName = rcvr.getLinkName();
            if (linkName == null || "root".equals(linkName))
                continue;
            if (linkList.containsKey(linkName))
                continue;

            cc = 0;
            for (j=nids.length-1; j>=n; j--) { // loop thru non-root nodes
                node = (MessageNode) nodeList.get(nids[j]);
                key = node.getLinkName();
                if (!linkName.equals(key))
                    continue;
                cn = node.getCapacity();
                if (cn > cc)
                    cc = cn;
                // rotate to the left end and increase n and j by 1
                kk = nids[j];
                for (ii=j; ii>n; ii--) // shift to right
                    nids[ii] = nids[ii-1];
                nids[n++] = kk;
                j ++;
            }
            if (cc <= 0)
                cc = rcvr.getCapacity();
            if (cc <= 0)
                cc = capacity;

            xq = new IndexedXQueue(linkName, cc);
            if (upLink.containsKey(linkName))
                ipps = Integer.parseInt((String) upLink.get(linkName));
            else
                ipps = 0;
            if (outLink.containsKey(linkName))
                opps = Integer.parseInt((String) outLink.get(linkName));
            else
                opps = 0;
            list[k++] = linkList.add(linkName, new long[]{cc, ipps, opps}, xq);
            map.put(linkName, null);
            m ++;
        }

        // reset ack on non-root xqs linked to any receiver
        browser = receiverList.browser();
        for (j=0; j<k; j++) {
            xq = (XQueue) linkList.get(list[j]);
            key = xq.getName();
            if ("root".equals(key))
                continue;
            browser.reset();
            while((i = browser.next()) >= 0) { // for receivers
                rcvr = (MessageReceiver) receiverList.get(i);
                linkName = rcvr.getLinkName();
                if (linkName == null || "root".equals(linkName))
                    continue;
                if (!linkName.equals(key))
                    continue;
                cc = rcvr.getXAMode();
                if (cc > 0 && (cc & MessageUtils.XA_CLIENT) > 0) {
                    enableAck(xq);
                    break;
                }
            }
        }

        // first generation has jj root nodes and n-jj non-root nodes packed
        j = 0;
        while (n < nids.length) { // pack the descendants in generation order
            k = n;
            for (ii=nids.length-1; ii>=n; ii--) { // loop thru leftover
                linkName = ((MessageNode) nodeList.get(nids[ii])).getLinkName();
                for (i=j; i<jj; i++) { // look for parent in root branch first
                    node = (MessageNode) nodeList.get(nids[i]);
                    if (node.containsXQ(linkName))
                        break;
                }
                if (i < jj) { // found its parent and rotate it to left end
                    kk = nids[ii];
                    for (int ji=ii; ji>n; ji--) // shift to right
                        nids[ji] = nids[ji-1];
                    nids[n++] = kk;
                    ii ++;
                }
            }
            j = n;
            for (ii=nids.length-1; ii>=n; ii--) { //loop thru leftover once more
                linkName = ((MessageNode) nodeList.get(nids[ii])).getLinkName();
                for (i=jj; i<k; i++) { // look for parent in non-root branch
                    node = (MessageNode) nodeList.get(nids[i]);
                    if (node.containsXQ(linkName))
                        break;
                }
                if (i < k) { // found its parent and rotate it to left end
                    kk = nids[ii];
                    for (int ji=ii; ji>n; ji--) // shift to right
                        nids[ji] = nids[ji-1];
                    nids[n++] = kk;
                    ii ++;
                }
            }
            if (k >= n) // end of generation tree
                break;
            jj = j;
            j = k;
        }
        // ids of all the nodes have been packed to nids in generation order

        // init all xqs linked to any node
        browser = nodeList.browser();
        while((i = browser.next()) >= 0) { // for MessageNode
            node = (MessageNode) nodeList.get(i);
            if (node.getCapacity() > 0)
                cn = node.getCapacity();
            else
                cn = capacity;
            linkName = node.getLinkName();
            if (linkName != null && !linkList.containsKey(linkName)){//for Input
                xq = new IndexedXQueue(linkName, cn);

                if (upLink.containsKey(linkName))
                    ipps = Integer.parseInt((String) upLink.get(linkName));
                else
                    ipps = 0;
                if (outLink.containsKey(linkName))
                    opps = Integer.parseInt((String) outLink.get(linkName));
                else
                    opps = 0;

                linkList.add(linkName, new long[]{cn, ipps, opps}, xq);
                m ++;
            }

            k = node.getNumberOfOutLinks();
            if (k <= 0)
                continue;
            for (j=0; j<k; j++) {
                linkName = node.getLinkName(j);
                if (node.getCapacity(j) > 0)
                    cc = node.getCapacity(j);
                else
                    cc = cn;
                if (!linkList.containsKey(linkName)) { // not init yet
                    xq = new IndexedXQueue(linkName, cc);

                    if (upLink.containsKey(linkName))
                        ipps = Integer.parseInt((String) upLink.get(linkName));
                    else
                        ipps = 0;
                    if (outLink.containsKey(linkName))
                        opps = Integer.parseInt((String) outLink.get(linkName));
                    else
                        opps = 0;
                    linkList.add(linkName, new long[]{cc, ipps, opps}, xq);
                    m ++;
                }
            }
        }

        doneMap = new HashMap<String, String>();
        doneMap.put("root", null);
        doneMap.put("escalation", null);

        // propagate ack to outlinks of all nodes in the generation order
        k = linkList.getCapacity();
        list = new int[k];
        k = 0;
        for (i=0; i<n; i++) {
            node = (MessageNode) nodeList.get(nids[i]);
            key = node.getLinkName();
            xq = (XQueue) linkList.get(key);
            if (!doneMap.containsKey(key))
                doneMap.put(key, null);
            if ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) == 0) {
                kk = node.getOutLinkBoundary();
                if (kk < -1) { // enable ack staring from BOUNDARY regardlessly
                    jj = node.getNumberOfOutLinks();
                    for (j=-(kk+1); j<jj; j++) {
                        key = node.getLinkName(j);
                        if (doneMap.containsKey(key)) // bypass feedbacks
                            continue;
                        xq = (XQueue) linkList.get(key);
                        enableAck(xq);
                        if (!map.containsKey(key)) {
                            map.put(key, null);
                            list[k++] = linkList.getID(key);
                        }
                    }
                }
                continue;
            }
            kk = node.getOutLinkBoundary();
            if (kk == 0) { // skipping the first outlink
                jj = node.getNumberOfOutLinks();
                for (j=1; j<jj; j++) {
                    key = node.getLinkName(j);
                    if (doneMap.containsKey(key)) // bypass feedbacks
                        continue;
                    xq = (XQueue) linkList.get(key);
                    enableAck(xq);
                    if (!map.containsKey(key)) {
                        map.put(key, null);
                        list[k++] = linkList.getID(key);
                    }
                }
            }
            else if (kk > 0) { // propagate to outlinks lower than BOUNDARY
                for (j=0; j<kk; j++) {
                    key = node.getLinkName(j);
                    if (doneMap.containsKey(key)) // bypass feedbacks
                        continue;
                    xq = (XQueue) linkList.get(key);
                    enableAck(xq);
                    if (!map.containsKey(key)) {
                        map.put(key, null);
                        list[k++] = linkList.getID(key);
                    }
                }
            }
            else { // propagate to all outlinks
                jj = node.getNumberOfOutLinks();
                for (j=0; j<jj; j++) {
                    key = node.getLinkName(j);
                    if (doneMap.containsKey(key)) // bypass feedbacks
                        continue;
                    xq = (XQueue) linkList.get(key);
                    enableAck(xq);
                    if (!map.containsKey(key)) {
                        map.put(key, null);
                        list[k++] = linkList.getID(key);
                    }
                }
            }
        }

        if (escalation != null) {
            if (!outLink.containsKey("escalation")) { // no one has escalation
                linkList.remove("escalation");
                m --;
            }
        }
        else if (outLink.containsKey("escalation")) // someone has escalation
            new Event(Event.ERR, name + ": escalation is not defined, but " +
                "it is referenced by some nodes").send();

        if (!outLink.containsKey("root")) { // no one has root
            linkList.remove("root");
            m --;
        }

        if ((debug & Service.DEBUG_INIT) > 0) {
            StringBuffer strBuf = new StringBuffer();
            long[] info;
            browser = linkList.browser();
            while ((i = browser.next()) >= 0) {
                xq = (XQueue) linkList.get(i);
                info = linkList.getMetaData(i);
                ipps = (int) info[1];
                opps = (int) info[2];
                strBuf.append("\n\t" + xq.getName() + ": " + xq.getCapacity() +
                    " " + 1 + " " + xq.getGlobalMask() + " " + ipps +
                    ((ipps == opps)?" = ":" ? ") + opps);
            }
            new Event(Event.DEBUG, name + " LinkName: Capacity IsIndexed Mask"+
                " InCount ? OutCount - totalLinks="+m+strBuf.toString()).send();
        }
    }

    private void resetXQueues(int status, int type) {
        MessagePersister pstr;
        MessageNode node;
        MessageReceiver rcvr;
        Browser browser;
        String linkName;
        XQueue xq;
        long[] info;
        int i, k, mask;

        if (status < FLOW_READY || status > FLOW_STOPPED)
            return;

        switch (type) {
          case MF_PSTR:
            browser = persisterList.browser();
            while ((i = browser.next()) >= 0) {
                pstr = (MessagePersister) persisterList.get(i);
                linkName = pstr.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                switch (status) {
                  case FLOW_RUNNING:
                    resumeRunning(xq);
                    break;
                  case FLOW_PAUSE:
                    pause(xq);
                    break;
                  case FLOW_STANDBY:
                    standby(xq);
                    break;
                  case FLOW_STOPPED:
                    stopRunning(xq);
                    break;
                  case FLOW_DISABLED:
                    mask = xq.getGlobalMask();
                    xq.setGlobalMask(mask | XQueue.STANDBY);
                    break;
                  default:
                    break;
                }
                info = persisterList.getMetaData(i);
                k = (int) info[GROUP_TID];
                if (k >= 0) { // notify the thread
                    Thread th = (Thread) threadList.get(k);
                    if (th != null && th.isAlive())
                        th.interrupt();
                }
            }
            break;
          case MF_NODE:
            browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                node = (MessageNode) nodeList.get(i);
                linkName = node.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                switch (status) {
                  case FLOW_RUNNING:
                    resumeRunning(xq);
                    break;
                  case FLOW_PAUSE:
                    pause(xq);
                    break;
                  case FLOW_STANDBY:
                    standby(xq);
                    break;
                  case FLOW_STOPPED:
                    stopRunning(xq);
                    break;
                  case FLOW_DISABLED:
                    mask = xq.getGlobalMask();
                    xq.setGlobalMask(mask | XQueue.STANDBY);
                    break;
                  default:
                    break;
                }
                info = nodeList.getMetaData(i);
                k = (int) info[GROUP_TID];
                if (k >= 0) { // notify the thread
                    Thread th = (Thread) threadList.get(k);
                    if (th != null && th.isAlive())
                        th.interrupt();
                }
            }
            break;
          case MF_RCVR:
            browser = receiverList.browser();
            while((i = browser.next()) >= 0) {
                rcvr = (MessageReceiver) receiverList.get(i);
                linkName = rcvr.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                switch (status) {
                  case FLOW_READY:
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.PAUSE) > 0)
                        xq.setGlobalMask(mask ^ XQueue.PAUSE);
                    break;
                  case FLOW_RUNNING:
                    resumeRunning(xq);
                    break;
                  case FLOW_PAUSE:
                    pause(xq);
                    break;
                  case FLOW_STANDBY:
                    standby(xq);
                    break;
                  case FLOW_STOPPED:
                    stopRunning(xq);
                    break;
                  case FLOW_DISABLED:
                    mask = xq.getGlobalMask();
                    xq.setGlobalMask(mask | XQueue.PAUSE);
                    break;
                  default:
                    break;
                }
                info = receiverList.getMetaData(i);
                k = (int) info[GROUP_TID];
                if (k >= 0) { // notify the thread
                    Thread th = (Thread) threadList.get(k);
                    if (th != null && th.isAlive())
                        th.interrupt();
                }
            }
            break;
          default:
            break;
        }
    }

    private void cleanupXQueues(int type) {
        MessagePersister pstr;
        MessageNode node;
        MessageReceiver rcvr;
        Browser browser;
        String linkName;
        XQueue xq;
        int i;

        switch (type) {
          case MF_PSTR:
            browser = persisterList.browser();
            while ((i = browser.next()) >= 0) {
                pstr = (MessagePersister) persisterList.get(i);
                linkName = pstr.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                xq.clear();
            }
            break;
          case MF_NODE:
            browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                node = (MessageNode) nodeList.get(i);
                linkName = node.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                xq.clear();
            }
            break;
          case MF_RCVR:
            browser = receiverList.browser();
            while((i = browser.next()) >= 0) {
                rcvr = (MessageReceiver) receiverList.get(i);
                linkName = rcvr.getLinkName();
                xq = (XQueue) linkList.get(linkName);
                if (xq == null)
                    continue;
                xq.clear();
            }
            break;
          default:
            break;
        }
    }

    /** returns number of outstaning messages in all xqs of receivers */
    private int getLeftover() {
        MessageReceiver rcvr;
        String linkName;
        XQueue xq;
        HashSet<String> hSet = new HashSet<String>();
        Browser browser = receiverList.browser();
        int i, n = 0;
        while((i = browser.next()) >= 0) {
            rcvr = (MessageReceiver) receiverList.get(i);
            linkName = rcvr.getLinkName();
            xq = (XQueue) linkList.get(linkName);
            if (xq == null || hSet.contains(linkName))
                continue;
            n += xq.size();
            hSet.add(linkName);
        }
        hSet.clear();
        return n;
    }

    public void run() {
        String threadName = null;
        String linkName, str;
        XQueue xq = null;
        int i, j, k;

        threadName = Thread.currentThread().getName();

        if ("shutdown".equals(threadName)) { // for asynchronous shutdown
            close();
            new Event(Event.INFO, name + ": message flow is shutdown").send();
            return;
        }

        k = threadName.lastIndexOf('_');
        j = threadName.lastIndexOf('/');
        if (j <= 0 || k <= 0 || k <= j) {
            new Event(Event.WARNING, name + ": threadName not well defined: "+
                threadName).send();
            return;
        }
        str = threadName.substring(j+1, k);

        if ("Node".equals(str)) { // for MessageNode
            int mask, status, previous_status;
            MessageNode node = null;
            XQueue [] outLinks;
            i = Integer.parseInt(threadName.substring(k+1));
            node = (MessageNode) nodeList.get(i);
            linkName = node.getLinkName();

            if(linkName==null || (xq = (XQueue)linkList.get(linkName)) == null){
                node.setStatus(MessageNode.NODE_STOPPED);
                new Event(Event.ERR, name + ": " + node.getName() +
                    " stopped with no linkName defined").send();
                return;
            }
            k = node.getNumberOfOutLinks();
            outLinks = new XQueue[k];
            for (i=0; i<k; i++) {
                str = node.getLinkName(i);
                outLinks[i] = (XQueue) linkList.get(str);
            }

            status = node.getStatus();
            if (status == MessageNode.NODE_READY ||
                status == MessageNode.NODE_RETRYING)
                node.setStatus(MessageNode.NODE_RUNNING);
            new Event(Event.INFO, name + ": " + node.getName() +
                " started on " + xq.getName()).send();
            previous_status = node.getStatus();
            for (;;) {
                if (status == MessageNode.NODE_RUNNING) try {
                    node.propagate(xq, outLinks);

                    if (((mask=xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // standby temporarily
                        status = node.getStatus();
                        if (status == MessageNode.NODE_READY) //for confirmation
                            node.setStatus(MessageNode.NODE_DISABLED);
                        else if (status == MessageNode.NODE_RUNNING) try {
                            // no state change so just yield
                            Thread.sleep(500);
                        }
                        catch (Exception ex) {
                        }
                    }

                    status = node.getStatus();
                    if (status > MessageNode.NODE_RETRYING &&
                        status < MessageNode.NODE_STOPPED) // state changed
                        new Event(Event.INFO, name + ": " + node.getName() +
                            " is " + statusText[status] + " on " +
                            xq.getName()).send();
                }
                catch (JMSException e) {
                    str = name + ": ";
                    Exception ee = e.getLinkedException();
                    if (ee != null)
                        str += "Linked exception: " + ee.toString() + "\n";
                    if (node.getStatus() == MessageNode.NODE_RUNNING)
                        node.setStatus(MessageNode.NODE_RETRYING);
                    new Event(Event.ERR, str + Event.traceStack(e)).send();
                    try {
                        Thread.sleep(pauseTime);
                    }
                    catch (Exception ex) {
                    }
                }
                catch (Exception e) {
                    if (node.getStatus() == MessageNode.NODE_RUNNING)
                        node.setStatus(MessageNode.NODE_RETRYING);
                    new Event(Event.ERR, name + ": " + node.getName()+
                        " is retrying: " + Event.traceStack(e)).send();
                    try {
                        Thread.sleep(pauseTime);
                    }
                    catch (Exception ex) {
                    }
                }
                catch (Error e) {
                    node.setStatus(MessageNode.NODE_STOPPED);
                    new Event(Event.ERR, name + ": " + node.getName()+
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                    Event.flush(e);
                }

                status = node.getStatus();
                while (status == MessageNode.NODE_DISABLED) { // disabled
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    previous_status = status;
                    status = node.getStatus();
                }

                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                    status == MessageNode.NODE_PAUSE) {
                    if (status >= MessageNode.NODE_STOPPED || keepRunning(xq))
                        break;
                    try {
                        Thread.sleep(pauseTime);
                    }
                    catch (Exception e) {
                    }
                    previous_status = status;
                    status = node.getStatus();
                }

                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                    status == MessageNode.NODE_STANDBY) {
                    if (status >= MessageNode.NODE_STOPPED || keepRunning(xq))
                        break;
                    try {
                        Thread.sleep(standbyTime);
                    }
                    catch (Exception e) {
                    }
                    previous_status = status;
                    status = node.getStatus();
                }

                if (isStopped(xq) || status >= MessageNode.NODE_STOPPED)
                    break;
                if (status == MessageNode.NODE_READY) {
                    node.setStatus(MessageNode.NODE_RUNNING);
                    new Event(Event.INFO, name + ": " + node.getName() +
                        " restarted on " + xq.getName()).send();
                }
                else if (status == MessageNode.NODE_RETRYING)
                    node.setStatus(MessageNode.NODE_RUNNING);

                status = node.getStatus();
                if (previous_status > MessageNode.NODE_RETRYING &&
                    previous_status < MessageNode.NODE_STOPPED &&
                    previous_status != status) { // state changed
                    new Event(Event.INFO, name + ": " + node.getName() +
                        " is " + statusText[status] + " on " +
                        xq.getName()).send();
                    previous_status = status;
                }
            }

            if (node.getStatus() < MessageNode.NODE_STOPPED)
                node.setStatus(MessageNode.NODE_STOPPED);
               
            if (node.getStatus() >= MessageNode.NODE_STOPPED) { // reset Node
                // checkpoint first
                checkpoint(node);
                node.resetMetaData(xq, outLinks);
            }

            if (node.getStatus() == MessageNode.NODE_CLOSED) { // close XQs
                for (i=0; i<outLinks.length; i++)
                    pause(outLinks[i]);
                try {
                    Thread.sleep(2000);
                }
                catch (Exception e) {
                }
                for (i=0; i<outLinks.length; i++)
                    stopRunning(outLinks[i]);
            }
            new Event(Event.INFO, name + ": " + node.getName() +
                " stopped on " + xq.getName()).send();
            if (node != null &&
                node.getStatus() == MessageNode.NODE_CLOSED) {
                if (!isStopped(xq))
                    stopRunning(xq);
                node.close();
            }
        }
        else if ("Receiver".equals(str)) { // for MessageReceiver
            MessageReceiver rcvr = null;
            i = Integer.parseInt(threadName.substring(k+1));
            rcvr = (MessageReceiver) receiverList.get(i);
            linkName = rcvr.getLinkName();

            if(linkName==null || (xq = (XQueue)linkList.get(linkName)) == null){
                rcvr.setStatus(MessageReceiver.RCVR_STOPPED);
                new Event(Event.ERR, name + ": " + rcvr.getName() +
                    " stopped with no linkName defined").send();
                return;
            }

            new Event(Event.INFO, name + ": " + rcvr.getName() +
                " started on " + xq.getName()).send();
            try {
                rcvr.receive(xq, 0);
                if (!isDaemon) { // wait for all the messages consumed
                    while (!isStopped(xq) && xq.size() > 0) {
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception ex) {
                        }
                    }
                    stopRunning(xq);
                    if ("root".equals(linkName)) { // job is done
                        previousStatus = flowStatus;
                        flowStatus = FLOW_CLOSED;
                        mtime = System.currentTimeMillis();
                    }
                }
                new Event(Event.INFO, name + ": " + rcvr.getName() +
                    " stopped on " + xq.getName()).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + rcvr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                rcvr.setStatus(MessageReceiver.RCVR_STOPPED);
            }
            catch (Error e) {
                new Event(Event.ERR, name + ": " + rcvr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                rcvr.setStatus(MessageReceiver.RCVR_STOPPED);
                Event.flush(e);
            }
            if (rcvr != null &&
                rcvr.getStatus() == MessageReceiver.RCVR_CLOSED) {
                if (!isStopped(xq))
                    stopRunning(xq);
                rcvr.close();
            }
        }
        else if ("Persister".equals(str)) { // for MessagePersister
            MessagePersister pstr = null;
            i = Integer.parseInt(threadName.substring(k+1));
            pstr = (MessagePersister) persisterList.get(i);
            linkName = pstr.getLinkName();

            if(linkName==null || (xq = (XQueue)linkList.get(linkName)) == null){
                pstr.setStatus(MessagePersister.PSTR_STOPPED);
                new Event(Event.ERR, name + ": " + pstr.getName() +
                    " stopped with no linkName defined").send();
                return;
            }

            new Event(Event.INFO, name + ": " + pstr.getName() +
                " started on " + xq.getName()).send();
            try {
                pstr.persist(xq, 0);
                new Event(Event.INFO, name + ": " + pstr.getName() +
                    " stopped on " + xq.getName()).send();
            }
            catch(Exception e) {
                new Event(Event.ERR, name + ": " + pstr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                pstr.setStatus(MessagePersister.PSTR_STOPPED);
            }
            catch(Error e) {
                new Event(Event.ERR, name + ": " + pstr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                pstr.setStatus(MessagePersister.PSTR_STOPPED);
                Event.flush(e);
            }
            if (pstr != null &&
                pstr.getStatus() == MessagePersister.PSTR_CLOSED) {
                if (!isStopped(xq))
                    stopRunning(xq);
                pstr.close();
            }
        }
        else {
            new Event(Event.WARNING, name + ": failed to parse threadName " +
                "for the supported key: " + str).send();
        }
    }

    private void startThreads(AssetList list) {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = list.browser();
        while ((id = browser.next()) >= 0) { // start all threads
            info = list.getMetaData(id);
            if ((k = (int) info[GROUP_TID]) < 0)
                continue;
            th = (Thread) threadList.get(k);
            if (th != null)
                th.start();
        }
    }

    private void interruptThreads(AssetList list) {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = list.browser();
        while ((id = browser.next()) >= 0) { // interrupt all threads
            info = list.getMetaData(id);
            if ((k = (int) info[GROUP_TID]) < 0)
                continue;
            th = (Thread) threadList.get(k);
            if (th != null && th.isAlive())
                th.interrupt();
        }
    }

    private void joinThreads(AssetList list) {
        int id, k, pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
        long[] info;
        Thread th;
        Browser browser = list.browser();
        while ((id = browser.next()) >= 0) { // join all threads
            info = list.getMetaData(id);
            if ((k = (int) info[GROUP_TID]) < 0)
                continue;
            info[GROUP_TID] = -1;
            th = (Thread) threadList.remove(k);
            if (th != null && th.isAlive()) {
                for (int i=0; i<10; i++) { 
                    th.interrupt();
                    try {
                        th.join(pt);
                    }
                    catch (Exception e) {
                        th.interrupt();
                    }
                    if (!th.isAlive())
                        break;
                }
            }
        }
    }

    private void initThreads(AssetList list, String key) {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = list.browser();
        while ((id = browser.next()) >= 0) {
            info = list.getMetaData(id);
            th = new Thread(thg, this, key + "_" + id);
            th.setDaemon(true);
            th.setPriority(Thread.NORM_PRIORITY);
            k = threadList.add(key + "_" + id, null, th);
            info[GROUP_TID] = k;
        }
    }

    /**
     * starts all threads and/or nodes
     */
    public synchronized void start() {
        if (flowStatus == FLOW_READY && previousStatus == FLOW_CLOSED) { //init
            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            startThreads(persisterList);
            startThreads(nodeList);
            startThreads(receiverList);
            try { // wait for threads start up
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow started").send();
            display(Service.DEBUG_INIT);
        }
        else if (flowStatus == FLOW_CLOSED) { // flow closed
            new Event(Event.WARNING, name +
                ": can not start a closed message flow").send();
        }
        else if (flowStatus != FLOW_RUNNING) { // flow not running either
            initThreads(receiverList, name + "/Receiver");
            setNodeStatus(MessageReceiver.RCVR_RUNNING, MF_RCVR, receiverList);

            initThreads(nodeList, name + "/Node");
            setNodeStatus(MessageNode.NODE_RUNNING, MF_NODE, nodeList);

            initThreads(persisterList, name + "/Persister");
            setNodeStatus(MessagePersister.PSTR_RUNNING,MF_PSTR,persisterList);
            resetXQueues(MessagePersister.PSTR_RUNNING, MF_PSTR);
            resetXQueues(MessageNode.NODE_RUNNING, MF_NODE);

            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            startThreads(persisterList);
            startThreads(nodeList);
            startThreads(receiverList);
            try { // wait for threads start up
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow started").send();
            display(Service.DEBUG_LOAD);
        }
        else { // flow already running
            new Event(Event.INFO, name + ": flow already running").send();
        }
    }

    /**
     * stops all nodes and the threads
     */
    public void stop() {
        if (flowStatus == FLOW_READY && previousStatus == FLOW_CLOSED) { //init
            // flow has not started yet, so abort it
            XQueue xq;
            Browser browser;
            MessageReceiver rcvr = null;
            MessagePersister pstr = null;
            String key;
            int i;

            setNodeStatus(MessagePersister.PSTR_STOPPED,MF_PSTR,persisterList);
            setNodeStatus(MessageNode.NODE_STOPPED, MF_NODE, nodeList);
            setNodeStatus(MessageReceiver.RCVR_STOPPED, MF_RCVR, receiverList);

            browser = persisterList.browser();
            while ((i = browser.next()) >= 0) {
                pstr = (MessagePersister) persisterList.get(i);
                key = pstr.getLinkName();
                xq = (XQueue) linkList.get(key);
                pstr.persist(xq, 1);
            }

            browser = receiverList.browser();
            while ((i = browser.next()) >= 0) {
                rcvr = (MessageReceiver) receiverList.get(i);
                key = rcvr.getLinkName();
                xq = (XQueue) linkList.get(key);
                rcvr.receive(xq, 1);
            }
            try { // wait for threads stopped
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            previousStatus = flowStatus;
            flowStatus = FLOW_STOPPED;
            new Event(Event.INFO, name + ": message flow stopped").send();
            display(Service.DEBUG_INIT);
        }
        else if (flowStatus < FLOW_STOPPED) { // stop the flow
            int n;
            if ((root.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) { // with XA
                setNodeStatus(MessagePersister.PSTR_STOPPED, MF_PSTR,
                    persisterList);
                resetXQueues(MessagePersister.PSTR_PAUSE, MF_PSTR);
                interruptThreads(persisterList);

                setNodeStatus(MessageNode.NODE_STOPPED, MF_NODE, nodeList);
                setNodeStatus(MessageReceiver.RCVR_STOPPED, MF_RCVR,
                    receiverList);
                resetXQueues(MessageNode.NODE_PAUSE, MF_NODE);
                interruptThreads(nodeList);
                interruptThreads(receiverList);
            }
            else { // without XA
                setNodeStatus(MessageReceiver.RCVR_STOPPED, MF_RCVR,
                    receiverList);
                resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
                interruptThreads(receiverList);
                try { // wait for receiver threads disabled 
                    Thread.sleep(1000);
                }
                catch (Exception e) {
                }

                setNodeStatus(MessageNode.NODE_STOPPED, MF_NODE, nodeList);
                setNodeStatus(MessagePersister.PSTR_STOPPED, MF_PSTR,
                    persisterList);
                resetXQueues(MessageNode.NODE_PAUSE, MF_NODE);
                resetXQueues(MessagePersister.PSTR_PAUSE, MF_PSTR);
                interruptThreads(receiverList);
                interruptThreads(nodeList);
                interruptThreads(persisterList);
            }
            joinThreads(persisterList);
            joinThreads(nodeList);
            joinThreads(receiverList);
            if ((n = getLeftover()) > 0)
                new Event(Event.WARNING, name + ": there are " + n +
                    " outstanding msgs lost in " + root.getName()).send();
            cleanupXQueues(MF_PSTR);
            cleanupXQueues(MF_NODE);
            previousStatus = flowStatus;
            flowStatus = FLOW_STOPPED;
            new Event(Event.INFO, name + ": message flow stopped").send();
            display(Service.DEBUG_LOAD);
        }
        else { // flow already stopped or closed
            new Event(Event.INFO, name + ": flow already stopped").send();
        }
    }

    /**
     * enables the disabled flow to its previous status
     */
    public int enable() {
        if (flowStatus != FLOW_DISABLED) // only act on disabled flow
            return flowStatus;

        switch (previousStatus) {
          case FLOW_CLOSED:
          case FLOW_STOPPED:
            new Event(Event.WARNING, name +
                ": can not enable a stopped or closed message flow").send();
            break;
          case FLOW_RUNNING:
          case FLOW_DISABLED:
            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            setNodeStatus(MessageReceiver.RCVR_RUNNING, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_READY, MF_RCVR);
            try { // wait for receiver threads enabled 
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow enabled from " +
                statusText[previousStatus] + " to " +
                statusText[flowStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          default:
            flowStatus = previousStatus;
            previousStatus = FLOW_DISABLED;
            setNodeStatus(previousStatus, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_READY, MF_RCVR);
            try { // wait for receiver threads enabled 
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow enabled from " +
                statusText[previousStatus] + " to " +
                statusText[flowStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
        }
        return flowStatus;
    }

    /**
     * disables the flow from the current status to the disabled status
     */
    public int disable() {
        switch (flowStatus) {
          case FLOW_CLOSED:
          case FLOW_STOPPED:
          case FLOW_DISABLED:
            // no need to disable
            previousStatus = flowStatus;
            break;
          case FLOW_READY:
            if (previousStatus == FLOW_CLOSED) { // initial start
                previousStatus = flowStatus;
                flowStatus = FLOW_DISABLED;
                setNodeStatus(MessageReceiver.RCVR_DISABLED, MF_RCVR,
                    receiverList);
                startThreads(persisterList);
                startThreads(nodeList);
                startThreads(receiverList);
                try { // wait for threads start up
                    Thread.sleep(1000);
                }
                catch (Exception e) {
                }
                new Event(Event.INFO, name + ": message flow disabled from " +
                    statusText[previousStatus]).send();
                display(Service.DEBUG_INIT);
                break;
            }
          default:
            previousStatus = flowStatus;
            flowStatus = FLOW_DISABLED;
            setNodeStatus(MessageReceiver.RCVR_DISABLED, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
            try { // wait for receiver threads disabled 
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow disabled from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
        }
        return flowStatus;
    }

    /**
     * promotes the flow from the current status to running status
     */
    public int promote() {
        switch (flowStatus) {
          case FLOW_READY:
            if (previousStatus != FLOW_CLOSED) // not for init
                break;
          case FLOW_STOPPED:
            initThreads(receiverList, name + "/Receiver");
            setNodeStatus(MessageReceiver.RCVR_RUNNING, MF_RCVR, receiverList);

            initThreads(nodeList, name + "/Node");
            setNodeStatus(MessageNode.NODE_RUNNING, MF_NODE, nodeList);

            initThreads(persisterList, name + "/Persister");
            setNodeStatus(MessagePersister.PSTR_RUNNING,MF_PSTR,persisterList);
            resetXQueues(MessagePersister.PSTR_RUNNING, MF_PSTR);
            resetXQueues(MessageNode.NODE_RUNNING, MF_NODE);
            resetXQueues(MessageReceiver.RCVR_READY, MF_RCVR);

            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            startThreads(persisterList);
            startThreads(nodeList);
            startThreads(receiverList);

            try { // wait for receiver threads start up
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow promoted from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_PAUSE:
          case FLOW_STANDBY:
          case FLOW_DISABLED:
            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            setNodeStatus(MessageReceiver.RCVR_RUNNING, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_READY, MF_RCVR);
            try { // wait for receiver threads enabled
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message flow promoted from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_RUNNING:
            if (defaultStatus == flowStatus && control != null &&
                passiveNodeID != activeNodeID) { // active-passive
                MessageNode node;
                XQueue [] outLinks;
                String key;
                int i, k;
                node = (MessageNode) nodeList.get(passiveNodeID);
                k = node.getNumberOfOutLinks();
                outLinks = new XQueue[k];
                for (i=0; i<k; i++) {
                    key = node.getLinkName(i);
                    outLinks[i] = (XQueue) linkList.get(key);
                }
                // disable passiveNode
                disableNode(passiveNodeID, MF_NODE);
                node.resetMetaData(control, outLinks);

                // enable activeNode
                node = (MessageNode) nodeList.get(activeNodeID);
                node.setStatus(FLOW_RUNNING);
                new Event(Event.INFO, name + ": message flow promoted with " +
                    node.getName() + " activated").send();
                if ((debug & Service.DEBUG_LOAD) > 0) try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
                display(Service.DEBUG_LOAD);
            }
            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            break;
          default:
            break;
        }

        return flowStatus;
    }

    /**
     * demotes the flow from the current status to its default status
     */
    public int demote() {
        switch (defaultStatus) {
          case FLOW_STOPPED:
            if (flowStatus == FLOW_STOPPED)
                break;
            setNodeStatus(MessageReceiver.RCVR_DISABLED, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
            try {
                Thread.sleep(pauseTime);
            }
            catch (Exception e) {
            }
            resetXQueues(MessageReceiver.RCVR_READY, MF_RCVR);
            setNodeStatus(MessagePersister.PSTR_STOPPED, MF_PSTR,persisterList);
            resetXQueues(MessagePersister.PSTR_PAUSE, MF_PSTR);
            interruptThreads(persisterList);

            setNodeStatus(MessageNode.NODE_STOPPED, MF_NODE, nodeList);
            setNodeStatus(MessageReceiver.RCVR_STOPPED, MF_RCVR, receiverList);
            resetXQueues(MessageNode.NODE_PAUSE, MF_NODE);
            interruptThreads(nodeList);
            interruptThreads(receiverList);

            joinThreads(persisterList);
            joinThreads(nodeList);
            joinThreads(receiverList);
            cleanupXQueues(MF_PSTR);
            cleanupXQueues(MF_NODE);
            previousStatus = flowStatus;
            flowStatus = FLOW_STOPPED;
            new Event(Event.INFO, name+": message flow stopped from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_PAUSE:
            if (flowStatus == FLOW_PAUSE)
                break;
            setNodeStatus(MessageReceiver.RCVR_PAUSE, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
            previousStatus = flowStatus;
            flowStatus = FLOW_PAUSE;
            new Event(Event.INFO, name+": message flow paused from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_STANDBY:
            if (flowStatus == FLOW_STANDBY)
                break;
            setNodeStatus(MessageReceiver.RCVR_STANDBY, MF_RCVR, receiverList);
            resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
            previousStatus = flowStatus;
            flowStatus = FLOW_STANDBY;
            new Event(Event.INFO, name+": message flow in standby from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_DISABLED:
            if (flowStatus == FLOW_DISABLED)
                break;
            setNodeStatus(MessageReceiver.RCVR_DISABLED, MF_RCVR,receiverList);
            resetXQueues(MessageReceiver.RCVR_DISABLED, MF_RCVR);
            previousStatus = flowStatus;
            flowStatus = FLOW_DISABLED;
            new Event(Event.INFO, name+": message flow disabled from " +
                statusText[previousStatus]).send();
            display(Service.DEBUG_LOAD);
            break;
          case FLOW_RUNNING:
            if (flowStatus == FLOW_RUNNING && control != null &&
                passiveNodeID != activeNodeID) { // active-passive
                MessageNode node;
                XQueue[] outLinks;
                String key;
                int i, k;
                node = (MessageNode) nodeList.get(activeNodeID);
                k = node.getNumberOfOutLinks();
                outLinks = new XQueue[k];
                for (i=0; i<k; i++) {
                    key = node.getLinkName(i);
                    outLinks[i] = (XQueue) linkList.get(key);
                }

                // disable activeNode
                disableNode(activeNodeID, MF_NODE);
                node.resetMetaData(control, outLinks);

                // enable passiveNode
                node = (MessageNode) nodeList.get(passiveNodeID);
                node.setStatus(FLOW_RUNNING);
                new Event(Event.INFO, name + ": message flow demoted with " +
                    node.getName() + " activated").send();
                if ((debug & Service.DEBUG_LOAD) > 0) try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
                display(Service.DEBUG_LOAD);
            }
            else if (flowStatus == FLOW_READY && previousStatus == FLOW_CLOSED){
                if (control == null || passiveNodeID == activeNodeID) {
                    startThreads(persisterList);
                    startThreads(nodeList);
                    startThreads(receiverList);
                    new Event(Event.INFO, name+": message flow started").send();
                }
                else { // active-passive nodes
                    MessageNode node;
                    node = (MessageNode) nodeList.get(activeNodeID);
                    node.setStatus(FLOW_DISABLED);
                    node = (MessageNode) nodeList.get(passiveNodeID);
                    node.setStatus(FLOW_RUNNING);
                    startThreads(persisterList);
                    startThreads(nodeList);
                    startThreads(receiverList);
                    new Event(Event.INFO, name + ": message flow started with "+
                        node.getName() + " activated").send();
                }
                display(Service.DEBUG_INIT);
            }
            previousStatus = flowStatus;
            flowStatus = FLOW_RUNNING;
            break;
          default:
            // not demoteable
            break;
        }

        return flowStatus;
    }

    /**
     * It ingests the message into the XQueue with the name of key and
     * returns the id upon success.  Otherwise, it returns -1. It assumes
     * that the 2nd half of cells for the XQueue are not used.
     */
    public int ingest(String key, Message msg) {
        int id, c, k;
        XQueue xq;
        xq = (XQueue) linkList.get(key);
        if (xq != null) { // found destination, add msg
            c = xq.getCapacity();
            k = c / 2;
            id = xq.reserve(500, c - k, k);
            if (id >= 0)
                xq.add(msg, id);
            return id;
        }
        return -1;
    }

    /** returns the display text on the message at the cell id of the queue */
    public String show(String key, int id, int type) {
        if (linkList.containsKey(key)) {
            Message msg;
            XQueue xq = (XQueue) linkList.get(key);
            if (xq == null) // no such xq found
                return null;
            if (xq.getCellStatus(id) <= XQueue.CELL_RESERVED) // no msg yet
                return "";
            msg = (Message) xq.browse(id);
            if (msg == null)
                return "";
            return MessageUtils.show(msg, type);
        }
        else { // look it up in outlinks
            int i;
            MessageNode node;
            Browser browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                node = (MessageNode) nodeList.get(i);
                if (node.containsXQ(key))
                    return node.show(key, id, type);
            }
        }

        return null;
    }

    /** takes back the message at the cell of id from the queue */
    public Message takeback(String key, int id) {
        if (!takebackEnabled)
            return null;

        if (linkList.containsKey(key)) {
            XQueue xq = (XQueue) linkList.get(key);
            if (xq == null) // no such xq found
                return null;
            if (xq.getCellStatus(id) == XQueue.CELL_OCCUPIED) // no msg yet
                return null;
            return (Message) xq.takeback(id);
        }
        else { // look it up in dynamic XQs
            int i;
            MessageNode node;
            Browser browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                node = (MessageNode) nodeList.get(i);
                if (!node.internalXQSupported())
                    continue;
                if (node.containsXQ(key))
                    return node.takeback(key, id);
            }
        }

        return null;
    }

    private Map<String, String> loadNodeInfo(String key, long[] info) {
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
        h.put("Type", String.valueOf(info[GROUP_TYPE]));
        str = statusText[((int) info[GROUP_STATUS] +
            statusText.length) % statusText.length];
        h.put("Status", str);
        h.put("TID", String.valueOf(info[GROUP_TID]));
        str = Event.dateFormat(new Date(info[GROUP_TIME]));
        h.put("Time", str);

        return h;
    }

    /**
     * returns a Map containing queried information according to the result type
     */
    public Map<String, String> queryInfo(long currentTime, long sessionTime,
        String target, String key, int type) {
        int i, j, k, n, id, size;
        long tm;
        String str, text;
        Map<String, String> h = null;
        Browser browser;
        long[] info;
        Object o;
        boolean isXML = (type & Utils.RESULT_XML) > 0;
        boolean isJSON = (type & Utils.RESULT_JSON) > 0;
        if (key == null)
            key = target;
        if ("FLOW".equals(target)) { // for flow instance
            h = new HashMap<String, String>();
            id = statusText.length;
            h.put("FlowStatus", statusText[(flowStatus+id)%id]);
            h.put("PreviousStatus", statusText[(previousStatus+id)%id]);
            h.put("DefaultStatus", statusText[(defaultStatus+id)%id]);
            h.put("Debug", String.valueOf(debug));
            h.put("Time", Event.dateFormat(new Date(mtime)));
            h.put("SessionTime", Event.dateFormat(new Date(sessionTime)));
        }
        else if ("NODE".equals(target)) { // for Nodes
            MessageNode node;
            id = -1;
            if ("NODE".equals(key)) { // list all Nodes
                StringBuffer strBuf = new StringBuffer();
                h = new HashMap<String, String>();
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) {
                    str = nodeList.getKey(id);
                    info = nodeList.getMetaData(id);
                    node = (MessageNode) nodeList.get(id);
                    info[GROUP_STATUS] = node.getStatus();
                    tm = info[GROUP_TIME];
                    text = statusText[(int) info[GROUP_STATUS]];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>NODE_" + id + "</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
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
                        strBuf.append("\"Node\":\"NODE_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm))) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("NODE_" + id, str + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if (key.startsWith("NODE_")) {
                str = key.substring(5);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
                if (id >= 0 && (str = nodeList.getKey(id)) != null) {
                    info = nodeList.getMetaData(id);
                    node = (MessageNode) nodeList.get(id);
                    info[GROUP_STATUS] = node.getStatus();
                    h = loadNodeInfo(str, info);
                }
            }
            else if ((id = nodeList.getID(key)) >= 0) {
                info = nodeList.getMetaData(id);
                node = (MessageNode) nodeList.get(id);
                info[GROUP_STATUS] = node.getStatus();
                h = loadNodeInfo(key, info);
            }
        }
        else if ("RECEIVER".equals(target)) { // for Receivers
            MessageReceiver rcvr;
            id = -1;
            if ("RECEIVER".equals(key)) { // list all Receivers
                StringBuffer strBuf = new StringBuffer();
                h = new HashMap<String, String>();
                browser = receiverList.browser();
                while ((id = browser.next()) >= 0) {
                    str = receiverList.getKey(id);
                    info = receiverList.getMetaData(id);
                    rcvr = (MessageReceiver) receiverList.get(id);
                    info[GROUP_STATUS] = rcvr.getStatus();
                    tm = info[GROUP_TIME];
                    text = statusText[(int) info[GROUP_STATUS]];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>RECEIVER_"+id+"</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
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
                        strBuf.append("\"Node\":\"RECEIVER_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm))) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("RECEIVER_" + id, str + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if (key.startsWith("RECEIVER_")) {
                str = key.substring(9);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
                if (id >= 0 && (str = receiverList.getKey(id)) != null) {
                    info = receiverList.getMetaData(id);
                    rcvr = (MessageReceiver) receiverList.get(id);
                    info[GROUP_STATUS] = rcvr.getStatus();
                    h = loadNodeInfo(str, info);
                }
            }
            else if ((id = receiverList.getID(key)) >= 0) {
                info = receiverList.getMetaData(id);
                rcvr = (MessageReceiver) receiverList.get(id);
                info[GROUP_STATUS] = rcvr.getStatus();
                h = loadNodeInfo(key, info);
            }
        }
        else if ("PERSISTER".equals(target)) { // for Persisters
            MessagePersister pstr;
            id = -1;
            if ("PERSISTER".equals(key)) { // list all Persisters
                StringBuffer strBuf = new StringBuffer();
                h = new HashMap<String, String>();
                browser = persisterList.browser();
                while ((id = browser.next()) >= 0) {
                    str = persisterList.getKey(id);
                    info = persisterList.getMetaData(id);
                    pstr = (MessagePersister) persisterList.get(id);
                    info[GROUP_STATUS] = pstr.getStatus();
                    tm = info[GROUP_TIME];
                    text = statusText[(int) info[GROUP_STATUS]];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>PERSISTER_"+id+"</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
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
                        strBuf.append("\"Node\":\"PERSISTER_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Time\":\"" +
                            Utils.escapeJSON(Event.dateFormat(new Date(tm))) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("PERSISTER_" + id, str + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if (key.startsWith("PERSISTER_")) {
                str = key.substring(10);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
                if (id >= 0 && (str = persisterList.getKey(id)) != null) {
                    info = persisterList.getMetaData(id);
                    pstr = (MessagePersister) persisterList.get(id);
                    info[GROUP_STATUS] = pstr.getStatus();
                    h = loadNodeInfo(str, info);
                }
            }
            else if ((id = persisterList.getID(key)) >= 0) {
                info = persisterList.getMetaData(id);
                pstr = (MessagePersister) persisterList.get(id);
                info[GROUP_STATUS] = pstr.getStatus();
                h = loadNodeInfo(key, info);
            }
        }
        else if ("XQ".equals(target)) { // for XQueue
            XQueue xq;
            if ("XQ".equals(key)) { // list all xqs
                StringBuffer strBuf = new StringBuffer();
                MessageNode node;
                String tmStr;
                long[] list;
                h = new HashMap<String, String>();
                list = new long[6];
                browser = linkList.browser();
                while ((j = browser.next()) >= 0) {
                    str = linkList.getKey(j);
                    if ("escalation".equals(str)) // skip escalation
                        continue;
                    xq = (XQueue) linkList.get(j);
                    list[0] = xq.depth();
                    list[1] = xq.size();
                    list[2] = xq.getCount();
                    list[3] = xq.getMTime();
                    list[4] = xq.getCapacity();
                    list[5] = xq.getGlobalMask();
                    size = (int) list[1];
                    tmStr = Event.dateFormat(new Date(list[3]));
                    i = (int) list[5];
                    if ((i & XQueue.KEEP_RUNNING) > 0)
                        text = statusText[FLOW_RUNNING];
                    else if ((i & XQueue.PAUSE) > 0)
                        text = statusText[FLOW_PAUSE];
                    else if ((i & XQueue.STANDBY) > 0)
                        text = statusText[FLOW_STANDBY];
                    else
                        text = statusText[FLOW_STOPPED];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Depth>" + list[0] + "</Depth>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + list[2] + "</Count>");
                        strBuf.append("<Time>" + Utils.escapeXML(tmStr) + 
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
                        strBuf.append("\"Time\":\"" + Utils.escapeJSON(tmStr) + 
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
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) { // dynamic xqs
                    node = (MessageNode) nodeList.get(id);
                    if (!node.internalXQSupported())
                        continue;
                    n = node.getNumberOfOutLinks();
                    if (n <= 0)
                        continue;
                    if (node.getMetaData(MessageNode.META_XQ, 0, list) < 0)
                        continue;
                    str = node.getLinkName(2);
                    if (linkList.containsKey(str)) // nohit at 2
                        k = 3;
                    else  // nohit at 1
                        k = 2;
                    for (j=k; j<n; j++) { // all dynamic XQs
                        str = node.getLinkName(j);
                        if (linkList.containsKey(str))
                            continue;
                        if (node.getMetaData(MessageNode.META_XQ, j, list) < 0)
                            continue;
                        size = (int) list[1];
                        tmStr = Event.dateFormat(new Date(list[3]));
                        i = (int) list[5];
                        if ((i & XQueue.KEEP_RUNNING) > 0)
                            text = statusText[FLOW_RUNNING];
                        else if ((i & XQueue.PAUSE) > 0)
                            text = statusText[FLOW_PAUSE];
                        else if ((i & XQueue.STANDBY) > 0)
                            text = statusText[FLOW_STANDBY];
                        else
                            text = statusText[FLOW_STOPPED];
                        if (isXML) {
                            strBuf.append("<Record type=\"ARRAY\">");
                            strBuf.append("<Name>" + Utils.escapeXML(str) +
                                "</Name>");
                            strBuf.append("<Depth>" + list[0] + "</Depth>");
                            strBuf.append("<Size>" + size + "</Size>");
                            strBuf.append("<Count>" + list[2] + "</Count>");
                            strBuf.append("<Time>" + Utils.escapeXML(tmStr) + 
                                "</Time>");
                            strBuf.append("<Capacity>" +list[4]+ "</Capacity>");
                            strBuf.append("<Status>" + Utils.escapeXML(text) +
                                "</Status>");
                            strBuf.append("</Record>");
                        }
                        else if (isJSON) {
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append("{");
                            strBuf.append("\"Name\":\"" + Utils.escapeJSON(str)+
                                "\",");
                            strBuf.append("\"Depth\":\"" + list[0] + "\",");
                            strBuf.append("\"Size\":\"" + size + "\",");
                            strBuf.append("\"Count\":\"" + list[2] + "\",");
                            strBuf.append("\"Time\":\""+Utils.escapeJSON(tmStr)+
                                "\",");
                            strBuf.append("\"Capacity\":\"" + list[4] + "\",");
                            strBuf.append("\"Status\":\"" +
                                Utils.escapeJSON(text) + "\"");
                            strBuf.append("}");
                        }
                        else
                            h.put(str, list[0] + " " + size + " " + list[2] +
                                " " + tmStr + " " + list[4] + " " + text);
                    }
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ((o = linkList.get(key)) != null && o instanceof XQueue) {
                // for a static XQ
                MessageNode node = null;
                String[] pn;
                h = null;
                xq = (XQueue) o;
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) { // check uplink first
                    node = (MessageNode) nodeList.get(id);
                    i = node.getStatus();
                    if (i <= FLOW_READY || i > FLOW_DISABLED)
                        continue;
                    if (key.equals(node.getLinkName()) &&
                        (pn = node.getDisplayProperties()) != null) { //found it
                        h = MessageUtils.listXQ(xq, type, pn);
                        break;
                    }
                }
                if (id < 0) { // not found yet
                    browser.reset();
                    while ((id = browser.next()) >= 0) { // check outlinks
                        node = (MessageNode) nodeList.get(id);
                        i = node.getStatus();
                        if (i <= FLOW_READY || i > FLOW_DISABLED)
                            continue;
                        if (node.containsXQ(key)) { // found it
                            h = node.list(key, type);
                            break;
                        }
                    }
                }
            }
            else if (key.lastIndexOf("_") > 0) { // check dynamic XQs
                MessageNode node;
                h = null;
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) {
                    node = (MessageNode) nodeList.get(id);
                    if (!node.internalXQSupported())
                        continue;
                    i = node.getStatus();
                    if (i <= FLOW_READY || i > FLOW_DISABLED)
                        continue;
                    if (node.containsXQ(key)) { // found it
                        h = node.list(key, type);
                        break;
                    }
                }
            }
        }
        else if ("MSG".equals(target)) { // for pendings of node in the uplink
            StringBuffer strBuf = new StringBuffer();
            id = -1;
            if (key.startsWith("NODE_")) {
                str = key.substring(5);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
            }
            else {
                id = nodeList.getID(key);
            }
            if (id >= 0 && (o = nodeList.get(id)) != null) {
                MessageNode node = (MessageNode) o;
                str = node.getLinkName();
                o = linkList.get(str);
                if (o != null && o instanceof XQueue) {
                    h = node.listPendings((XQueue) o, type);
                }
            }
            else if ("MSG".equals(key)) { // list all Nodes
                MessageNode node;
                h = new HashMap<String, String>();
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) {
                    node = (MessageNode) nodeList.get(id);
                    str = nodeList.getKey(id);
                    k = node.getCapacity();
                    text = node.getLinkName();
                    o = linkList.get(text);
                    size = 0;
                    if (o != null && o instanceof XQueue)
                        size = ((XQueue) o).size();
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>NODE_" + id + "</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<LinkName>" +
                            Utils.escapeXML(text) + "</LinkName>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Capacity>" + k +"</Capacity>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Node\":\"NODE_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"LinkName\":\"" +
                            Utils.escapeXML(text) + "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Capacity\":\"" + k +"\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("NODE_" + id, str + " " + text + " " +
                            size + " " + k);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
        }
        else if ("OUT".equals(target)) { // for node's outlinks
            StringBuffer strBuf = new StringBuffer();
            id = -1;
            if (key.startsWith("NODE_")) {
                str = key.substring(5);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
            }
            else {
                id = nodeList.getID(key);
            }
            if (id >= 0 && (o = nodeList.get(id)) != null) {
                long[] list = new long[(int) MessageNode.OUT_QTIME+1];
                MessageNode node = (MessageNode) o;
                h = new HashMap<String, String>();
                n = node.getNumberOfOutLinks();
                for (i=0; i<n; i++) {
                    str = node.getLinkName(i);
                    if (node.getMetaData(MessageNode.META_OUT, i, list) < 0)
                        continue;
                    tm = list[MessageNode.OUT_TIME];
                    size = (int) list[MessageNode.OUT_SIZE];
                    id = (int) list[MessageNode.OUT_STATUS];
                    text = statusText[id];
                    id = (int) list[MessageNode.OUT_COUNT];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + id + "</Count>");
                        str = Event.dateFormat(new Date(tm));
                        strBuf.append("<Time>" + Utils.escapeXML(str) +
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
                        strBuf.append("\"Count\":\"" + id + "\",");
                        str = Event.dateFormat(new Date(tm));
                        strBuf.append("\"Time\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Status\":\"" + Utils.escapeJSON(text) +
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, size + " " + id + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ("OUT".equals(key)) { // list all Nodes
                MessageNode node;
                h = new HashMap<String, String>();
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) {
                    node = (MessageNode) nodeList.get(id);
                    str = nodeList.getKey(id);
                    size = node.getNumberOfOutLinks();
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>NODE_" + id + "</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<OutSize>" + size + "</OutSize>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Node\":\"NODE_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"OutSize\":\"" + size + "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("NODE_" + id, str + " " + size);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
        }
        else if ("RULE".equals(target)) { // for node's rulesets
            StringBuffer strBuf = new StringBuffer();
            int pendings = 0;
            id = -1;
            if (key.startsWith("NODE_")) {
                str = key.substring(5);
                if (str.length() <= 0)
                    id = -1;
                else try {
                    id = Integer.parseInt(str);
                }
                catch (Exception e) {
                    id = -1;
                }
            }
            else {
                id = nodeList.getID(key);
            }
            if (id >= 0 && (o = nodeList.get(id)) != null) {
                long[] list = new long[(int) MessageNode.RULE_TIME+1];
                MessageNode node = (MessageNode) o;
                h = new HashMap<String, String>();
                n = node.getNumberOfRules();
                for (i=0; i<n; i++) {
                    str = node.getRuleName(i);
                    if(node.getMetaData(MessageNode.META_RULE, i, list) < 0)
                        continue;
                    tm = list[MessageNode.RULE_TIME];
                    pendings = (int) list[MessageNode.RULE_PEND];
                    size = (int) list[MessageNode.RULE_SIZE];
                    id = (int)  list[MessageNode.RULE_OID];
                    text = (id >= 0) ?  node.getLinkName(id) : "" + id;
                    if (text == null)
                        text = "-";
                    id = (int) list[MessageNode.RULE_COUNT];
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<Pending>" + pendings + "</Pending>");
                        strBuf.append("<Size>" + size + "</Size>");
                        strBuf.append("<Count>" + id + "</Count>");
                        str = Event.dateFormat(new Date(tm));
                        strBuf.append("<Time>" + Utils.escapeXML(str) +
                            "</Time>");
                        strBuf.append("<OutLink>" + Utils.escapeXML(text) +
                            "</OutLink>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"Pending\":\"" + pendings + "\",");
                        strBuf.append("\"Size\":\"" + size + "\",");
                        strBuf.append("\"Count\":\"" + id + "\",");
                        str = Event.dateFormat(new Date(tm));
                        strBuf.append("\"Time\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"OutLink\":\"" + Utils.escapeJSON(text)+
                            "\"");
                        strBuf.append("}");
                    }
                    else
                        h.put(str, pendings + " " + size + " " + id + " " +
                            Event.dateFormat(new Date(tm)) + " " + text);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
            else if ("RULE".equals(key)) { // list all Nodes
                MessageNode node;
                h = new HashMap<String, String>();
                browser = nodeList.browser();
                while ((id = browser.next()) >= 0) {
                    node = (MessageNode) nodeList.get(id);
                    str = nodeList.getKey(id);
                    size = node.getNumberOfRules();
                    if (isXML) {
                        strBuf.append("<Record type=\"ARRAY\">");
                        strBuf.append("<Node>NODE_" + id + "</Node>");
                        strBuf.append("<Name>" + Utils.escapeXML(str) +
                            "</Name>");
                        strBuf.append("<RuleSize>"+ size +"</RuleSize>");
                        strBuf.append("</Record>");
                    }
                    else if (isJSON) {
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        strBuf.append("{");
                        strBuf.append("\"Node\":\"NODE_" + id + "\",");
                        strBuf.append("\"Name\":\"" + Utils.escapeJSON(str) +
                            "\",");
                        strBuf.append("\"RuleSize\":\""+ size +"\"");
                        strBuf.append("}");
                    }
                    else
                        h.put("NODE_" + id, str + " " + size);
                }
                if (isXML)
                    h.put(target, strBuf.toString());
                else if (isJSON)
                    h.put(target, "[" + strBuf.toString() + "]");
            }
        }
        return h;
    }

    /** returns a text of list with all nodes in either JSON or XML */
    public String list(int type) {
        if ((type & Utils.RESULT_JSON) > 0)
            return listJSON();
        else if ((type & Utils.RESULT_XML) > 0)
            return listXML();
        else
            return null;
    }

    /** returns a JSON text of list with all nodes */
    private String listJSON() {
        StringBuffer strBuf;
        Browser browser;
        int i, k, n;
        strBuf = new StringBuffer();
        n = receiverList.size();
        k = 0;
        if (n > 0) {
            browser = receiverList.browser();
            strBuf.append("\"File\":[{");
            strBuf.append("\"DirName\":\"Receiver\",");
            strBuf.append("\"File\":[");
            k = 0;
            while ((i = browser.next()) >= 0) {
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append("\"" + receiverList.getKey(i) + " (" + i + ")\"");
            }
            strBuf.append("]}");
            k = 1;
        }
        n = nodeList.size();
        if (n > 0) {
            browser = nodeList.browser();
            if (k > 0)
                strBuf.append(",{");
            else
                strBuf.append("\"File\":[{");
            strBuf.append("\"DirName\":\"Node\",");
            strBuf.append("\"File\":[");
            k = 0;
            while ((i = browser.next()) >= 0) {
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append("\"" + nodeList.getKey(i) + " (" + i + ")\"");
            }
            strBuf.append("]}");
            k = 1;
        }
        n = persisterList.size();
        if (n > 0) {
            browser = persisterList.browser();
            if (k > 0)
                strBuf.append(",{");
            else
                strBuf.append("\"File\":[{");
            strBuf.append("\"DirName\":\"Persister\",");
            strBuf.append("\"File\":[");
            k = 0;
            while ((i = browser.next()) >= 0) {
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append("\""+ persisterList.getKey(i) + " (" + i + ")\"");
            }
            strBuf.append("]}");
            k = 1;
        }
        Map<String, String> h = queryInfo(0L, 0L, "XQ", "XQ",Utils.RESULT_TEXT);
        if (h != null && h.size() > 0) {
            if (k > 0)
                strBuf.append(",{");
            else
                strBuf.append("\"File\":[{");
            strBuf.append("\"DirName\":\"XQueue\",");
            strBuf.append("\"File\":[");
            k = 0;
            for (String key : h.keySet()) {
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append("\"" + key + "\"");
            }
            strBuf.append("]}");
        }
        if (strBuf.length() > 0)
            return "{\"Name\":\""+ name + "\",\"DirName\":\"FLOW\"," +
                strBuf.toString() + "]}";
        else
            return null;
    }

    /** returns an XML text of list with all nodes */
    private String listXML() {
        StringBuffer strBuf;
        Browser browser;
        int i, n;
        strBuf = new StringBuffer();
        n = receiverList.size();
        if (n > 0) {
            browser = receiverList.browser();
            strBuf.append("<File type=\"ARRAY\">");
            strBuf.append("<DirName>Receiver</DirName>");
            while ((i = browser.next()) >= 0) {
                strBuf.append("<File type=\"ARRAY\">" + receiverList.getKey(i)+
                    " (" + i + ")</File>");
            }
            strBuf.append("</File>");
        }
        n = nodeList.size();
        if (n > 0) {
            browser = nodeList.browser();
            strBuf.append("<File type=\"ARRAY\">");
            strBuf.append("<DirName>Node</DirName>");
            while ((i = browser.next()) >= 0) {
                strBuf.append("<File type=\"ARRAY\">" + nodeList.getKey(i) +
                    " (" + i + ")</File>");
            }
            strBuf.append("</File>");
        }
        n = persisterList.size();
        if (n > 0) {
            browser = persisterList.browser();
            strBuf.append("<File type=\"ARRAY\">");
            strBuf.append("<DirName>Persister</DirName>");
            while ((i = browser.next()) >= 0) {
                strBuf.append("<File type=\"ARRAY\">"+ persisterList.getKey(i)+
                    " (" + i + ")</File>");
            }
            strBuf.append("</File>");
        }
        Map<String, String> h = queryInfo(0L, 0L, "XQ", "XQ",Utils.RESULT_TEXT);
        if (h != null && h.size() > 0) {
            Object[] o = h.keySet().toArray();
            n = h.size();
            strBuf.append("<File type=\"ARRAY\">");
            strBuf.append("<DirName>XQueue</DirName>");
            for (i=0; i<n; i++) {
                if (o[i] == null)
                    continue;
                strBuf.append("<File type=\"ARRAY\">" + o[i].toString() +
                    "</File>");
            }
            strBuf.append("</File>");
        }
        if (strBuf.length() > 0) {
            strBuf.append("</" + name + ">");
            return "<" + name + "><Name>" + name +
                "</Name><DirName>FLOW</DirName>" + strBuf.toString();
        }
        return null;
    }

    private void display(int flag) {
        StringBuffer strBuf = null;
        MessageReceiver rcvr;
        MessageNode node;
        MessagePersister pstr;
        Browser browser;
        long[] info;
        XQueue xq;
        String key, value;
        int i, mask;

        if ((debug & flag) > 0)
            strBuf = new StringBuffer();
        else
            return;

        browser = receiverList.browser();
        while ((i = browser.next()) >= 0) {
            rcvr = (MessageReceiver) receiverList.get(i);
            info = receiverList.getMetaData(i);
            key = rcvr.getLinkName();
            value = rcvr.getName();
            xq = (XQueue) linkList.get(key);
            if (xq != null)
                mask = xq.getGlobalMask();
            else
                mask = -1;
            info[GROUP_STATUS] = rcvr.getStatus();
            strBuf.append("\n\tRcvr: " + i + " " + info[GROUP_STATUS] + " " +
                mask + " - " + key + " " + value + " 1");
        }

        browser = nodeList.browser();
        while ((i = browser.next()) >= 0) {
            node = (MessageNode) nodeList.get(i);
            info = nodeList.getMetaData(i);
            key = node.getLinkName();
            value = node.getName();
            xq = (XQueue) linkList.get(key);
            if (xq != null)
                mask = xq.getGlobalMask();
            else
                mask = -1;
            info[GROUP_STATUS] = node.getStatus();
            strBuf.append("\n\tNode: " + i + " " + info[GROUP_STATUS] + " " +
                mask + " - " + key + " " + value + " " +
                node.getNumberOfOutLinks());
        }

        browser = persisterList.browser();
        while ((i = browser.next()) >= 0) {
            pstr = (MessagePersister) persisterList.get(i);
            info = persisterList.getMetaData(i);
            key = pstr.getLinkName();
            value = pstr.getName();
            xq = (XQueue) linkList.get(key);
            if (xq != null)
                mask = xq.getGlobalMask();
            else
                mask = -1;
            info[GROUP_STATUS] = pstr.getStatus();
            strBuf.append("\n\tPstr: " + i + " " + info[GROUP_STATUS] + " " +
                mask + " - " + key + " " + value + " 0");
        }

        new Event(Event.DEBUG, name + " Type: ID Status Mask - LinkName " +
            "NodeName OutLinks status=" +flowStatus + strBuf.toString()).send();
    }

    private static boolean keepRunning(XQueue xq) {
        if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0)
            return true;
        else
            return false;
    }

    private static boolean isStopped(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        if ((xq.getGlobalMask() & mask) > 0)
            return false;
        else
            return true;
    }

    private static void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    private static void resumeRunning(XQueue xq) {
        int mask = XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.KEEP_RUNNING);
    }

    private static void pause(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.PAUSE);
    }

    private static void standby(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.STANDBY);
    }

    private static void disableAck(XQueue xq) {
        int mask = xq.getGlobalMask();
        xq.setGlobalMask(mask & (~XQueue.EXTERNAL_XA));
    }

    private static void enableAck(XQueue xq) {
        int mask = xq.getGlobalMask();
        xq.setGlobalMask(mask | XQueue.EXTERNAL_XA);
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return "flow";
    }

    public int getStatus() {
        return flowStatus;
    }

    public int getDefaultStatus() {
        return defaultStatus;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public XQueue getRootQueue() {
        return root;
    }

    /**
     * returns partition of root queue for external requests
     */
    public int[] getPartition() {
        return new int[] {partition[0], partition[1]};
    }

    /**
     * returns number of nodes of the specific type
     */
    public int getNumberOfNodes(int type) {
        if (type == MF_RCVR)
            return receiverList.size();
        else if (type == MF_NODE)
            return nodeList.size();
        else if (type == MF_PSTR)
            return persisterList.size();
        else
            return 0;
    }

    /**
     * returns number of XQueues of the flow
     */
    public int getNumberOfXQueues() {
        return linkList.size();
    }

    /**
     * With the new property Map, props, of the node and the Map
     * of change for the node, it creates or updates all the rules according
     * to the order specified in the master. The Map of change may contain
     * the key of basename if there is any changes on the container.  Other
     * keys are for rules.  A key with null value is for removal.  Otherwise,
     * it is for either a new rule or an existing rule.  It returns the number
     * of failures.
     */
    private int updateNode(MessageNode node, Map props,
        Map<String, Object> change) {
        Object o;
        XQueue in = null;
        List list = null;
        String key = null;
        int i, k, n, id, m = 0;

        if (node == null || change == null || change.size() <= 0) // no change
            return 0;

        props = MessageUtils.substituteProperties(props);
        change = MessageUtils.substituteProperties(change);
        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = MessageUtils.substituteProperties((List) o);
        else
            list = new ArrayList();

        if (change.containsKey("NODE")) { // find keys with null value to remove
            Map master = (Map) change.remove("NODE"); 
            if ((o = master.get("Debug")) == null) {
                if (node.getDebugMode() != 0) {
                    m += 1;
                    node.setDebugMode(0);
                }
            }
            else try {
                i = Integer.parseInt((String) o);
                if (i != node.getDebugMode()) {
                    m += 1;
                    node.setDebugMode(i);
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to reset debug on " +
                    node.getName() + ": " + e.toString()).send();
            }

            if ((o = master.get("DisplayMask")) == null) {
                if (node.getDisplayMask() != 0) {
                    m += 2;
                    node.setDisplayMask(0);
                }
            }
            else try {
                i = Integer.parseInt((String) o);
                if (i != node.getDisplayMask()) {
                    m += 2;
                    node.setDisplayMask(i);
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to reset displayMask on "+
                    node.getName() + ": " + e.toString()).send();
            }

            if ((o = master.get("StringProperty")) == null ||
                !(o instanceof Map)) {
                if ((o = node.getDisplayProperties()) != null) {
                    m += 4;
                    node.setDisplayProperties(null);
                }
            }
            else try {
                Iterator iter = ((Map) o).keySet().iterator();
                i = ((Map) o).size();
                String[] pn = new String[i];
                i = 0;
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if ((pn[i] = MessageUtils.getPropertyID(key)) == null)
                        pn[i] = key;
                    i ++;
                }
                if (i > 0) {
                    if ((o = node.getDisplayProperties()) != null) {
                        HashSet<String> hSet = new HashSet<String>();
                        for (String ky : (String[]) o)
                            hSet.add(ky);

                        for (String ky : pn) {
                            if (hSet.contains(ky))
                                continue;
                            m += 4;
                            node.setDisplayProperties(pn);
                            break;
                        }
                        hSet.clear();
                    }
                    else {
                        m += 4;
                        node.setDisplayProperties(pn);
                    }
                }
                else if ((o = node.getDisplayProperties()) != null) {
                    pn = (String[]) o; 
                    if (pn.length > 0) {
                        m += 4;
                        node.setDisplayProperties(new String[0]);
                    }
                }
                else {
                    m += 4;
                    node.setDisplayProperties(new String[0]);
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to update " +
                    "displayPropertyName on " + key + " for " +
                    node.getName() + ": " + e.toString()).send();
            }

            try { // update the other parameters for the node
                i = node.updateParameters(master);
                if (i > 0)
                    m += 8;
            }
            catch (Exception e) {
                new Event(Event.ERR, name +
                    ": failed to update base properties for " +
                    node.getName() + ": " + e.toString()).send();
            }

            if (m > 0 && (debug & Service.DEBUG_DIFF) > 0)
                new Event(Event.DEBUG, name + " node " + node.getName() +
                    " has base properties updated with mask of " + m).send();

            n = change.size();
            String[] keys = change.keySet().toArray(new String[n]);
            for (i=0; i<n; i++) {
                key = keys[i];
                if (key == null || key.length() <= 0)
                    continue;
                if ((o = change.get(key)) == null) try { // null for deletion
                    in = (XQueue) linkList.get(node.getLinkName());
                    change.remove(key);
                    id = node.removeRule(key, in);
                    new Event(Event.INFO, name + " rule " + key +
                        " has been removed on " + id + " from " +
                        node.getName()).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " failed to remove rule "+
                        key + " from " + node.getName() + ": " +
                        Event.traceStack(e)).send();
                }
            }
        }

        k = 0;
        n = list.size();
        for (i=0; i<n; i++) { // loop thru all rules
            o = list.get(i);
            if (o == null || !(o instanceof String || o instanceof Map)) {
                new Event(Event.WARNING, name + ": an illegal rule at " + i +
                    " for " + node.getName()).send();
                continue;
            }
            if (o instanceof Map) // for inline rules
                key = (String) ((Map) o).get("Name");
            else  // for included rules
                key = (String) o;

            if (key.length() <= 0) {
                new Event(Event.WARNING, name + ": " + " empty rule name at " +
                    i + " for " + node.getName()).send();
                continue;
            }

            if (!change.containsKey(key)) { // existing rule with no change
                if (node.containsRule(key)) { // existing rule
                    id = -1;
                    try { // rotate it first
                        id = node.rotateRule(key);
                    }
                    catch (Exception e) {
                        k ++;
                        new Event(Event.ERR, name + " failed to reotaye rule "+
                            key + ": " + id + " for " + node.getName()).send();
                        continue;
                    }
                    if ((debug & Service.DEBUG_DIFF) > 0 &&
                        (debug & Service.DEBUG_TRAN) > 0)
                        new Event(Event.DEBUG, name + " rule " + key +
                            " has been shuffled on " + id + " for " +
                            node.getName()).send();
                }
                else try { // failed rule
                    o = props.get(key);
                    id = node.addRule((Map) o);
                    if (id < 0) { // failed
                        k ++;
                        new Event(Event.ERR, name + " failed to recreate rule "+
                            key + ": " + id + " for " + node.getName()).send();
                    }
                    else // recreated
                        new Event(Event.INFO, name + " rule " + key +
                            " has been recreated on " + id + " for " +
                            node.getName()).send();
                }
                catch (Exception e) {
                    k ++;
                    new Event(Event.ERR, name + " failed to recreate rule "+
                        key + " for " + node.getName() + ": " +
                        Event.traceStack(e)).send();
                }
            }
            else if (node.containsRule(key)) { // replace existing rule
                in = (XQueue) linkList.get(node.getLinkName());
                id = -1;
                o = change.remove(key); // assume the key is used only once
                try { // rotate it first, then replace it
                    id = node.rotateRule(key);
                    id = node.replaceRule(key, (Map) o, in);
                }
                catch (Exception e) {
                    k ++;
                    new Event(Event.ERR, name + " failed to replace rule "+
                        key + " for " + node.getName() + ": " +
                        Event.traceStack(e)).send();
                    continue;
                }
                catch (Error e) {
                    new Event(Event.ERR, name + " failed to replace rule "+
                        key + " for " + node.getName() + ": " +
                        Event.traceStack(e)).send();
                    Event.flush(e);
                }
                if (id < 0) { // failed
                    k ++;
                    new Event(Event.ERR, name + " failed to replace rule "+
                        key + ": " + id + " for " + node.getName()).send();
                }
                else // replaced
                    new Event(Event.INFO, name + " rule " + key +
                        " has been replaced on " + id + " for " +
                        node.getName()).send();
            }
            else { // add a new rule
                id = -1;
                o = change.remove(key); // assume the key is used only once
                try {
                    id = node.addRule((Map) o);
                }
                catch (Exception e) {
                    k ++;
                    new Event(Event.ERR, name + " failed to create rule "+
                        key + " for " + node.getName() + ": " +
                        Event.traceStack(e)).send();
                    continue;
                }
                if (id < 0) { // failed
                    k ++;
                    new Event(Event.ERR, name + " failed to create rule "+
                        key + ": " + id + " for " + node.getName()).send();
                }
                else // created
                    new Event(Event.INFO, name + " rule " + key +
                        " has been created on " + id + " for " +
                        node.getName()).send();
            }
        }

        props.clear();
        change.clear();
        return k;
    }

    /**
     * It refreshes the external rules on all the nodes contains the rule
     * with the given name and returns the number of external rulesets updated.
     */
    public int refreshRules(String key) {
        int k, n, id, ps, status = -1;
        String str;
        MessageNode node = null;
        XQueue in;
        Browser browser = nodeList.browser();
        n = -1;
        while ((id = browser.next()) >= 0) {
            node = (MessageNode) nodeList.get(id);
            if (node == null || !node.containsRule(key))
                continue;
            in = (XQueue) linkList.get(node.getLinkName());
            // refresh external rule
            str = node.getName();
            status = node.getStatus();
            ps = disableNode(id, MF_NODE);
            try {
                k = node.refreshRule(key, in);
            }
            catch (Exception e) {
                k = -2;
                if (n < 0)
                    n = -2;
                new Event(Event.ERR, name + " failed to refresh node " + str +
                    " on rule " + key + ": " + Event.traceStack(e)).send();
            }
            node.setStatus(status);
            if (k >= 0) {
                if (n < 0) // reset count
                    n = 0;
                n += k;
                new Event(Event.INFO, name + ": " + k + " external rules in " +
                    key + " have been refreshed on the node " + id + " and " +
                    str +" is back to " + statusText[status] + " from " +
                    ((ps >= 0) ? statusText[ps] : String.valueOf(ps))).send();
            }
        }
        return n;
    }

    /**
     * It looks for missing generated nodes to delete and returns the number of
     * nodes deleted.
     */
    private int deleteMissingNodes(Map props, Map<String, Object> change,
        int type) {
        Object o;
        Map ph, c;
        List list;
        AssetList myList = null;
        MessageNode node;
        MessagePersister pstr;
        MessageReceiver rcvr;
        ConfigTemplate temp;
        String key, str, group;
        int i, j, k, m, n, id;

        if (props == null || props.size() <= 0)
            return -1;

        if (type == MF_PSTR) {
            group = "Persister";
            myList = persisterList;
        }
        else if (type == MF_RCVR) {
            group = "Receiver";
            myList = receiverList;
        }
        else if (type == MF_NODE) {
            group = "Node";
            myList = nodeList;
        }
        else
            return -1;

        if (change == null)
            change = new HashMap<String, Object>();

        k = 0;
        // remove missing generated nodes
        if ((o = props.get(group)) != null && o instanceof List) {
            list = (List) o;
            n = list.size();
            for (i=0; i<n; i++) { // check template only
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                ph = (Map) o;
                if ((o = ph.get("Name")) == null || !(o instanceof String))
                    continue;
                key = (String) o;
                if (key.length() <= 0 || !templateList.containsKey(key))
                    continue;
                temp = (ConfigTemplate) templateList.get(key);
                if (temp == null) {
                    templateList.remove(key);
                    continue;
                }
                try {
                    c = temp.diff(ph);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " reload: diff failed for "+
                        key + ": " + e.toString()).send();
                    c = null;
                }
                if (c == null) // no change on names and list
                    continue;
                else if (c.containsKey("Item")) { // some items removed
                    o = c.get("Item");
                    if (o == null)
                        o = new ArrayList();
                    else if (!(o instanceof List)) { // not supported
                        new Event(Event.WARNING, name + ": Item for " +key+
                            " is not a list").send();
                        o = new ArrayList();
                    }
                }
                m = ((List) o).size();
                for (j=0; j<m; j++) { // removing items
                    str = (String) ((List) o).get(j);
                    if (str == null || str.length() <= 0)
                        continue;
                    if (temp.containsItem(str)) { // a missing node
                        str = temp.remove(str);
                        if ((id = myList.getID(str)) >= 0) {
                            stopNode(id, type);
                            if (type == MF_PSTR) {
                                pstr = (MessagePersister) myList.remove(id);
                                if (pstr != null) // clean up
                                    pstr.close();
                            }
                            else if (type == MF_RCVR) {
                                rcvr = (MessageReceiver) myList.remove(id);
                                if (rcvr != null) // clean up
                                    rcvr.close();
                            }
                            else { // for node
                                node = (MessageNode) myList.remove(id);
                                if (node != null) // clean up
                                    node.close();
                            }
                            new Event(Event.INFO, name + " " + group + " " +
                                key + ":" + str + " has been removed on " +
                                id).send();
                            k ++;
                        }
                    }
                }
                if (temp.getSize() <= 0) { // empty
                    templateList.remove(key);
                    temp.close();
                    new Event(Event.INFO, name + ": template " + key +
                        " has been deleted").send();
                }
                else if (!change.containsKey(key)) { // no change on key
                    if ((o = c.get("Template")) != null) { // renamed
                        // since rename is not supported yet, remove them now
                        m = temp.getSize();
                        for (j=0; j<m; j++) { // remove existing nodes
                            str = temp.getKey(j);
                            if ((id = myList.getID(str)) >= 0) {
                                stopNode(id, type);
                                if (type == MF_PSTR) {
                                    pstr = (MessagePersister) myList.remove(id);
                                    if (pstr != null) // clean up
                                        pstr.close();
                                }
                                else if (type == MF_RCVR) {
                                    rcvr = (MessageReceiver) myList.remove(id);
                                    if (rcvr != null) // clean up
                                        rcvr.close();
                                }
                                else {
                                    node = (MessageNode) myList.remove(id);
                                    if (node != null) // clean up
                                        node.close();
                                }
                                new Event(Event.INFO, name + " " + group +" "+
                                    key+ ":"+str+" has been removed on "+id +
                                    " due to rename").send();
                            }
                        }
                    }
                    else {
                        // in case new items added
                        o = ph.get("Item");
                        if (o != null && o instanceof List)
                            temp.updateItems((List) o);
                    }
                }
            }
        }

        return k;
    }

    @SuppressWarnings("unchecked")
    private int reloadExistingNodes(long currentTime, Map props,
        Map<String, Object> change, int type, boolean isNewMaster) {
        Object o;
        Map<String, String> ph;
        Map h;
        List list;
        AssetList myList;
        Browser browser;
        MessageNode node;
        MessagePersister pstr;
        MessageReceiver rcvr;
        ConfigTemplate temp;
        String key, str, group;
        long[] info;
        int i, j, k, n, m, id;

        if (props == null || props.size() <= 0)
            return -1;

        if (type == MF_PSTR) {
            group = "Persister";
            myList = persisterList;
        }
        else if (type == MF_RCVR) {
            group = "Receiver";
            myList = receiverList;
        }
        else if (type == MF_NODE) {
            group = "Node";
            myList = nodeList;
        }
        else
            return -1;

        if (change == null)
            change = new HashMap();

        k = 0;
        // for existing or new nodes
        if ((o = props.get(group)) != null && o instanceof List){
            StringBuffer strBuf = ((debug & Service.DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
            int count = 0;
            list = (List) o;

            ph = new HashMap<String, String>(); // for count of duplicates
            n = list.size();
            for (i=0; i<n; i++) { // add or update nodes
                o = list.get(i);
                if (o == null || !(o instanceof String || o instanceof Map)) {
                    new Event(Event.WARNING, name + " reload: " +
                        " an illegal " + group + " at " + i).send();
                    continue;
                }
                if (o instanceof Map) // for generated nodes
                    key = (String) ((Map) o).get("Name");
                else  // normal case
                    key = (String) o;

                if (key.length() <= 0) {
                    new Event(Event.ERR, name + " reload: " +
                        " empty name at " + i + " for " + group).send();
                    continue;
                }

                if (!change.containsKey(key)) { // existing obj with no change
                    if (isNewMaster && !props.containsKey(key)) // copy it over
                        props.put(key, cachedProps.get(key));

                    if (myList.containsKey(key)) { // existing one
                        if (!ph.containsKey(key)) { // first appearance
                            id = myList.getID(key);
                            ph.put(key, "1");
                            myList.rotate(id);
                        }
                        else if (type != MF_NODE) { // duplicates
                            count = Integer.parseInt((String) ph.get(key));
                            id = myList.getID(key);
                            for (j=0; j<count; j++) {
                                info = myList.getMetaData(id);
                                id = (int) info[GROUP_ID];
                                if (id < 0)
                                    break;
                            }
                            if (id < 0) { // end of chain so add a new one
                                o = props.get(key);
                                id = addNode(mtime, (Map) o);
                                if (id >= 0) { // created
                                    ph.put(key, String.valueOf(++count));
                                    new Event(Event.INFO, name + " " + group +
                                        " " + key + " has been recreated on " +
                                        id).send();
                                    info = myList.getMetaData(id);
                                    info[GROUP_TIME] = currentTime;
                                    startNode(id, type);
                                }
                                else { // failed
                                    k ++;
                                    new Event(Event.ERR, name +
                                        " failed to recreate " + group + " " +
                                        key + ": " + id + "/" + count).send();
                                }
                            }
                            else { // rotate the existing one
                                ph.put(key, String.valueOf(++count));
                                myList.rotate(id);
                            }
                        }
                        else { // no duplicates for node
                            k ++;
                            new Event(Event.ERR, name + " duplicated key for " +
                                group + ": " + key).send();
                            continue;
                        }
                    }
                    else if (!(o instanceof Map)) { // failed before
                        o = props.get(key);
                        id = addNode(mtime, (Map) o);
                        if (id >= 0) { // recreated
                            ph.put(key, "1");
                            new Event(Event.INFO, name + " " + group + " " +
                                key + " has been recreated on " + id).send();
                            info = myList.getMetaData(id);
                            info[GROUP_TIME] = currentTime;
                            startNode(id, type);
                        }
                        else { // failed again
                            k ++;
                            new Event(Event.ERR, name + " failed to recreate "+
                                group + " " + key + ": " + id).send();
                        }
                    }
                    else if (!templateList.containsKey(key)) { // recreate list
                        try {
                            Map<String, Object> c;
                            c = Utils.cloneProperties((Map) o);
                            c.put("Property", props.get(key));
                            temp = new ConfigTemplate(c);
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, name + " failed to init " +
                                group + " template for " + key + ": " +
                                Event.traceStack(e)).send();
                            continue;
                        }
                        m = temp.getSize();
                        if (!templateList.containsKey(key))
                            templateList.add(key, new long[]{m, type}, temp);
                        else {
                            new Event(Event.ERR, name +
                                " duplicated key for config template: " +
                                key).send();
                            temp.close();
                            continue;
                        }
                        for (j=0; j<m; j++) { // init all generated nodes
                            str = temp.getItem(j);
                            o = temp.getProps(str);
                            if (o == null || !(o instanceof Map)) {
                                new Event(Event.ERR, name +
                                    " failed to get props for "+ key + ":" +
                                    str + " at " + j + "/" + i).send();
                                continue;
                            }
                            h = (Map) o;
                            str = (String) h.get("Name");
                            if (str == null || str.length() <= 0) {
                                new Event(Event.ERR, name +
                                    " name is not defined for "+ key + " at " +
                                    j + "/" + i).send();
                                continue;
                            }
                            if (myList.containsKey(str)) { // duplicated
                                new Event(Event.WARNING, name + ": duplicated "+
                                    group + " name of " + str + " for " +
                                    key).send();
                                continue;
                            }
                            id = addNode(mtime, h);
                            if (id >= 0) {
                                new Event(Event.INFO, name + " " + group + " " +
                                    key + ":" + str+ " instantiated").send();
                                info = myList.getMetaData(id);
                                info[GROUP_TIME] = currentTime;
                                startNode(id, type);
                            }
                            else {
                                k ++;
                                new Event(Event.ERR, name +
                                    " failed to instantiate "+ key + ":" + str+
                                    " at " + j + "/" + i).send();
                            }
                        }
                    }
                    else { // rotate or add generated objs
                        temp = (ConfigTemplate) templateList.get(key);
                        m = temp.getSize();
                        for (j=0; j<m; j++) { // loop thru all items
                            str = temp.getKey(j);
                            id = myList.getID(str);
                            if (id >= 0) { // existing one so rotate
                                myList.rotate(id);
                            }
                            else { // newly added node
                                h = temp.getProps(temp.getItem(j));
                                id = addNode(mtime, h);
                                if (id >= 0) {
                                    new Event(Event.INFO, name +" "+ group +" "+
                                        key +":"+ str + " instantiated").send();
                                    info = myList.getMetaData(id);
                                    info[GROUP_TIME] = currentTime;
                                    startNode(id, type);
                                }
                                else {
                                    k ++;
                                    new Event(Event.ERR, name +
                                        " failed to instantiate "+ key + ":" +
                                        str+ " at " + j + "/" + i).send();
                                }
                            }
                        }
                    }
                }
                else if (!(o instanceof Map)) { // regular node with changes
                    o = change.remove(key);
                    props.put(key, o);
                    if ((id = myList.getID(key)) >= 0) { // replace base
                        do { // look for all duplicates to delete
                            stopNode(id, type);
                            info = myList.getMetaData(id);
                            if (type == MF_PSTR) {
                                pstr = (MessagePersister) myList.remove(id);
                                chainupDuplicates(id, key, info,pstrMap,myList);
                                if (pstr != null)
                                    pstr.close();
                            }
                            else if (type == MF_RCVR) {
                                rcvr = (MessageReceiver) myList.remove(id);
                                chainupDuplicates(id, key, info,rcvrMap,myList);
                                if (rcvr != null)
                                    rcvr.close();
                            }
                            else { // for node
                                node = (MessageNode) myList.remove(id);
                                if (node != null)
                                    node.close();
                                new Event(Event.INFO, name + " " + group + " " +
                                    key + " has been deleted for reload on " +
                                    id).send();
                                break;
                            }
                            new Event(Event.INFO, name + " " + group +" "+ key+
                                " has been deleted for reload on " + id).send();
                        } while ((id = (int) info[GROUP_ID]) >= 0);
                        id = addNode(mtime, (Map) o);
                        if (id >= 0) { // created
                            ph.put(key, "1");
                            new Event(Event.INFO, name + " " + group +" "+ key+
                                " has been reloaded on " + id).send();
                            info = myList.getMetaData(id);
                            info[GROUP_TIME] = currentTime;
                            startNode(id, type);
                        }
                        else { // failed
                            k ++;
                            new Event(Event.ERR, name + " failed to recreate "+
                                group + " " + key + ": " + id).send();
                        }
                    }
                    else { // new one or failed one
                        id = addNode(mtime, (Map) o);
                        if (id >= 0) {
                            ph.put(key, "1");
                            new Event(Event.INFO, name + " " + group +" "+ key+
                                " has been created on " + id).send();
                            info = myList.getMetaData(id);
                            info[GROUP_TIME] = currentTime;
                            startNode(id, type);
                        }
                        else {
                            k ++;
                            new Event(Event.ERR, name + " failed to create "+
                                group + " " + key + ": " + id).send();
                        }
                    }
                }
                else { // update or create generated nodes
                    temp = (ConfigTemplate) templateList.remove(key);
                    if (temp != null)
                        m = temp.getSize();
                    else
                        m = 0;
                    for (j=0; j<m; j++) { // remove existing generated nodes
                        str = temp.getKey(j);
                        if ((id = myList.getID(str)) >= 0) {
                            stopNode(id, type);
                            if (type == MF_PSTR) {
                                pstr = (MessagePersister) myList.remove(id);
                                if (pstr != null) // clean up
                                    pstr.close();
                            }
                            else if (type == MF_RCVR) {
                                rcvr = (MessageReceiver) myList.remove(id);
                                if (rcvr != null) // clean up
                                    rcvr.close();
                            }
                            else {
                                node = (MessageNode) myList.remove(id);
                                if (node != null) // clean up
                                    node.close();
                            }
                            new Event(Event.INFO, name + " " + group +" "+ key+
                                ":"+str+" has been removed on "+id).send();
                        }
                    }
                    if (temp != null)
                        temp.close();

                    // create new generated nodes
                    try {
                        Map<String, Object> c;
                        c = Utils.cloneProperties((Map) o);
                        c.put("Property", props.get(key));
                        temp = new ConfigTemplate(c);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name + " failed to init " +
                            group + " template for " + key + ": " +
                            Event.traceStack(e)).send();
                        continue;
                    }
                    m = temp.getSize();
                    if (!templateList.containsKey(key))
                        templateList.add(key, new long[]{m, type}, temp);
                    else {
                        new Event(Event.ERR, name +
                            " duplicated key for config template: " +
                            key).send();
                        temp.close();
                        continue;
                    }
                    for (j=0; j<m; j++) { // init all generated nodes
                        str = temp.getItem(j);
                        o = temp.getProps(str);
                        if (o == null || !(o instanceof Map)) {
                            new Event(Event.ERR, name +
                                " failed to get props for "+ key + ":" +
                                str + " at " + j + "/" + i).send();
                            continue;
                        }
                        h = (Map) o;
                        str = (String) h.get("Name");
                        if (str == null || str.length() <= 0) {
                            new Event(Event.ERR, name +
                                " name is not defined for "+ key + " at " +
                                j + "/" + i).send();
                            continue;
                        }
                        if (myList.containsKey(str)) { // duplicated
                            new Event(Event.WARNING, name + ": duplicated "+
                                group + " name of " + str + " for " +
                                key).send();
                            continue;
                        }
                        id = addNode(mtime, h);
                        if (id >= 0) {
                            new Event(Event.INFO, name + " " + group + " " +
                                key + ":" + str+ " instantiated").send();
                            info = myList.getMetaData(id);
                            info[GROUP_TIME] = currentTime;
                            startNode(id, type);
                        }
                        else {
                            k ++;
                            new Event(Event.ERR, name +
                                " failed to instantiate "+ key + ":" + str+
                                " at " + j + "/" + i).send();
                        }
                    }
                }
            }
        }
        else { // no node defined, so delete all of them
            browser = myList.browser();
            while ((id = browser.next()) >= 0) { // delete all nodes
                stopNode(id, type);
                key = myList.getKey(id);
                info = myList.getMetaData(id);
                if (type == MF_PSTR) {
                    pstr = (MessagePersister) myList.remove(id);
                    chainupDuplicates(id, key, info, pstrMap, myList);
                    if (pstr != null)
                        pstr.close();
                }
                else if (type == MF_RCVR) {
                    rcvr = (MessageReceiver) myList.remove(id);
                    chainupDuplicates(id, key, info, rcvrMap, myList);
                    if (rcvr != null)
                        rcvr.close();
                }
                else { // for node
                    node = (MessageNode) myList.remove(id);
                    if (node != null)
                        node.close();
                }
                new Event(Event.INFO, name + " " + group + " " + key +
                    " has been deleted on " + id).send();
            }
        }

        return k;
    }

    /**
     * It reloads the changes for the entire flow and returns the number of
     * failures.
     */
    @SuppressWarnings("unchecked")
    public int reload(Map<String, Object> change) {
        Object o;
        Map ph, props = null;
        List list;
        Browser browser;
        MessageNode node;
        MessagePersister pstr;
        MessageReceiver rcvr;
        String key, basename = "FLOW";
        long[] info;
        int i, j, k, n, id, m = 0;
        boolean isNewMaster = false;

        if (change.containsKey(basename)) {
            props = (Map) change.remove(basename);
            if (props != null) {
                if ((o = props.get("Debug")) != null) {
                    i = Integer.parseInt((String) o);
                    if (i != debug) {
                        m += 1;
                        debug = i;
                    }
                }
                else if (debug > 0) {
                    m += 1;
                    debug = 0;
                }

                if ((o = props.get("WaitTime")) != null) {
                    i = Integer.parseInt((String) o);
                    if (i <= 0)
                        i = 500;
                    if (i != waitTime) {
                        m += 2;
                        waitTime = i;
                    }
                }
                else if (waitTime != 500L) {
                    m += 2;
                    waitTime = 500L;
                }

                if ((o = props.get("PauseTime")) != null) {
                    i = Integer.parseInt((String) o);
                    if (i <= 0)
                        i = 5000;
                    if (i != pauseTime) {
                        m += 4;
                        pauseTime = i;
                    }
                }
                else if (pauseTime != 5000) {
                    m += 4;
                    pauseTime = 5000;
                }

                if ((o = props.get("StandbyTime")) != null) {
                    i = Integer.parseInt((String) o);
                    if (i <= 0)
                        i = 15000;
                    if (i != standbyTime) {
                        m += 8;
                        standbyTime = i;
                    }
                }
                else if (standbyTime != 15000) {
                    m += 8;
                    standbyTime = 15000;
                }

                if ((o = props.get("TakebackEnabled")) != null) {
                    if ("true".equalsIgnoreCase((String) o)) {
                        if (!takebackEnabled) {
                            m += 16;
                            takebackEnabled = true;
                        }
                    }
                    else if (takebackEnabled) {
                        m += 16;
                        takebackEnabled = false;
                    }
                }
                else if (takebackEnabled) {
                    m += 16;
                    takebackEnabled = false;
                }

                if ((o = props.get("CheckpointDir")) != null) {
                    if(checkpointDir==null || !checkpointDir.equals((String)o)){
                        m += 32;
                        checkpointDir = (String) o;
                    }
                }
                else if (checkpointDir != null) {
                    m += 32;
                    checkpointDir = null;
                }

                if (m > 0 && (debug & Service.DEBUG_DIFF) > 0)
                    new Event(Event.DEBUG, name + " base properties got "+
                        "updated with mask of " + m).send();

                isNewMaster = true;
            }
            else
                props = cachedProps;
        }
        else {
            props = cachedProps;
        }

        mtime = System.currentTimeMillis();

        if (isNewMaster) { // check for deletion first
            n = change.size();
            String[] keys = change.keySet().toArray(new String[n]);
            // look for keys with null value for deletion
            for (i=0; i<n; i++) {
                key = keys[i];
                if (key == null || key.length() <= 0)
                    continue;
                if ((o = change.get(key)) == null) { // null for deletion
                    o = cachedProps.remove(key);
                    change.remove(key);

                    if (templateList.containsKey(key)) { // for generated nodes
                        ConfigTemplate temp = null;
                        AssetList myList = null;
                        String str, myNode = "none";
                        int type = -1;
                        info = templateList.getMetaData(key);
                        temp = (ConfigTemplate) templateList.remove(key);
                        if (temp != null) {
                            m = temp.getSize();
                            type = (int) info[1];
                            if (type == MF_PSTR)
                                myList = persisterList;
                            else if (type == MF_RCVR)
                                myList = receiverList;
                            else if (type == MF_NODE)
                                myList = nodeList;
                        }
                        else
                            m = 0;
                        for (j=0; j<m; j++) {
                            str = temp.getKey(j);
                            if ((id = myList.getID(str)) >= 0) {
                                stopNode(id, type);
                                if (type == MF_PSTR) {
                                    myNode = "generated persister";
                                    pstr = (MessagePersister) myList.remove(id);
                                    if (pstr != null) // clean up
                                        pstr.close();
                                }
                                else if (type == MF_RCVR) {
                                    myNode = "generated receiver";
                                    rcvr = (MessageReceiver) myList.remove(id);
                                    if (rcvr != null) // clean up
                                        rcvr.close();
                                }
                                else if (type == MF_NODE) {
                                    myNode = "generated node";
                                    node = (MessageNode) myList.remove(id);
                                    if (node != null) // clean up
                                        node.close();
                                }
                                new Event(Event.INFO, name +" "+myNode+" "+key+
                                    ":"+str+" has been removed on "+id).send();
                            }
                        }
                        if (temp != null)
                            temp.close();
                    }
                    else if ((id = persisterList.getID(key)) >= 0) {// persister
                        do { // look for all duplicates to delete
                            stopNode(id, MF_PSTR);
                            info = persisterList.getMetaData(id);
                            pstr = (MessagePersister) persisterList.remove(id);
                            chainupDuplicates(id, key, info, pstrMap,
                                persisterList);
                            if (pstr != null)
                                pstr.close();
                            new Event(Event.INFO, name + " persister " + key +
                                " has been removed on " + id).send();
                        } while ((id = persisterList.getID(key)) >= 0);
                    }
                    else if ((id = nodeList.getID(key)) >= 0) { // a node
                        stopNode(id, MF_NODE);
                        node = (MessageNode) nodeList.remove(id);
                        if (node != null) // clean up
                            node.close();
                        new Event(Event.INFO, name + " node " + key +
                            " has been removed on " + id).send();
                    }
                    else if ((id = receiverList.getID(key)) >= 0) { //a receiver
                        do { // look for all duplicates to delete
                            stopNode(id, MF_RCVR);
                            info = receiverList.getMetaData(id);
                            rcvr = (MessageReceiver) receiverList.remove(id);
                            chainupDuplicates(id, key, info, rcvrMap,
                                receiverList);
                            if (rcvr != null)
                                rcvr.close();
                            new Event(Event.INFO, name + " receiver " + key +
                                " has been removed on " + id).send();
                        } while ((id = receiverList.getID(key)) >= 0);
                    }
                }
            }

            // remove the missing generated nodes
            deleteMissingNodes(props, change, MF_PSTR);
            deleteMissingNodes(props, change, MF_NODE);
            deleteMissingNodes(props, change, MF_RCVR);

            // remove the extra duplicates of receivers
            if ((o = props.get("Receiver")) != null && o instanceof List) {
                list = (List) o;
                n = list.size();

                ph = Utils.getUniqueMap(rcvrMap.values());
                for (Iterator iter=ph.keySet().iterator(); iter.hasNext();) {
                    key = (String) iter.next();
                    o = ph.get(key);
                    k = Integer.parseInt((String) o) + 1;
                    for (i=0; i<n; i++) { // adjust the number of duplicates
                        o = list.get(i);
                        if (o == null || !(o instanceof String))
                            continue;
                        if (key.equals((String) o))
                            k --;
                    }
                    for (i=0; i<k; i++) { // remove the extra duplicates
                        if ((id = receiverList.getID(key)) < 0)
                            continue;
                        stopNode(id, MF_RCVR);
                        info = receiverList.getMetaData(id);
                        rcvr = (MessageReceiver) receiverList.remove(id);
                        chainupDuplicates(id, key, info, rcvrMap, receiverList);
                        if (rcvr != null)
                            rcvr.close();
                        new Event(Event.INFO, name + " receiver " + key +
                            " has been removed on " + id).send();
                    }
                }
                ph.clear();
            }

            // remove the extra duplicates of persisters
            if ((o = props.get("Persister")) != null && o instanceof List){
                list = (List) o;
                n = list.size();

                ph = Utils.getUniqueMap(pstrMap.values());
                for (Iterator iter=ph.keySet().iterator(); iter.hasNext();) {
                    key = (String) iter.next();
                    o = ph.get(key);
                    k = Integer.parseInt((String) o) + 1;
                    for (i=0; i<n; i++) { // adjust number of duplicates
                        o = list.get(i);
                        if (o == null || !(o instanceof String))
                            continue;
                        if (key.equals((String) o))
                            k --;
                    }
                    for (i=0; i<k; i++) { // remove the extra duplicates
                        if ((id = persisterList.getID(key)) < 0)
                            continue;
                        stopNode(id, MF_PSTR);
                        info = persisterList.getMetaData(id);
                        pstr = (MessagePersister) persisterList.remove(id);
                        chainupDuplicates(id, key, info, pstrMap,persisterList);
                        if (pstr != null)
                            pstr.close();
                        new Event(Event.INFO, name + " persister " + key +
                            " has been removed on " + id).send();
                    }
                }
                ph.clear();
            }
        }

        k = 0; // count of failures

        // for existing or new persisters
        k += reloadExistingNodes(mtime, props, change, MF_PSTR, isNewMaster);

        // for existing or new nodes
        if ((o = props.get("Node")) != null && o instanceof List) {
            StringBuffer strBuf = ((debug & Service.DEBUG_INIT) > 0) ?
                strBuf = new StringBuffer() : null;
            list = (List) o;

            n = list.size();
            for (i=0; i<n; i++) { // add or update nodes
                o = list.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                key = (String) o;
                if (key.length() <= 0)
                    continue;

                if (!change.containsKey(key)) { // existing node with no change
                    if (isNewMaster && !props.containsKey(key)) // copy it over
                        props.put(key, cachedProps.get(key));

                    if (nodeList.containsKey(key)) { // existing one
                        id = nodeList.getID(key);
                        nodeList.rotate(id);
                    }
                    else if ((o = props.get(key)) != null) { // failed before
                        id = addNode(mtime, (Map) o);
                        if (id >= 0) { // recreated
                            new Event(Event.INFO, name + " node " + key +
                                " has been recreated on " + id).send();
                            info = nodeList.getMetaData(id);
                            info[GROUP_TIME] = mtime;
                            startNode(id, MF_NODE);
                        }
                        else { // failed again
                            k ++;
                            new Event(Event.ERR, name + " failed to recreate "+
                                "node " + key + ": " + id).send();
                        }
                    }
                    else { // empty
                        new Event(Event.ERR, name + " empty properties for "+
                            "node " + key).send();
                    }
                }
                else if ((id = nodeList.getID(key)) >= 0) { // with changes
                    Map<String, Object> ch;
                    o = cachedProps.remove(key);            // original props
                    ph = (Map) change.remove(key);          // new props
                    props.put(key, ph);                     // copy it over

                    // prepare for diff on rules
                    ph = Utils.normalize(ph, "Ruleset");
                    o = Utils.normalize((Map) o, "Ruleset");
                    ch = Utils.diff("NODE", includeMap, (Map) o, ph);
                    if (ch == null || ch.size() <= 0) // with no changes
                        nodeList.rotate(id);
                    else { // with changes on rules only
                        int status, ps;
                        nodeList.rotate(id);
                        if ((debug & Service.DEBUG_DIFF) > 0)
                            showNodeChange(name +"/"+key, ch, (Map) o,
                                ((debug & Service.DEBUG_TRAN) > 0));
                        node = (MessageNode) nodeList.get(id);
                        info = nodeList.getMetaData(id);
                        status = node.getStatus();
                        ps = disableNode(id, MF_NODE);
                        try {
                            j = updateNode(node, ph, ch);
                            node.setStatus(status);
                            new Event(Event.INFO, name + " node " + key +
                                " has been updated on " + id + " with " + j +
                                " errors and is back to " + statusText[status]+
                                " from "+((ps>=0)?statusText[ps]:""+ps)).send();
                        }
                        catch (Exception e) {
                            node.setStatus(status);
                            new Event(Event.ERR, name+" failed to reload node "+
                                key + ": " + Event.traceStack(e)).send();
                        }
                        catch (Error e) {
                            node.setStatus(status);
                            new Event(Event.ERR, name+" failed to reload node "+
                                key + ": " + Event.traceStack(e)).send();
                            ch.clear();
                            Event.flush(e);
                        }
                        if ((debug & Service.DEBUG_LOAD) > 0) {
                            String str = node.getLinkName();
                            XQueue xq = (XQueue) linkList.get(str);
                            if (xq != null)
                                j = xq.getGlobalMask();
                            else
                                j = -1;
                            try {
                                Thread.sleep(waitTime);
                            }
                            catch (Exception e) {
                            }
                            info[GROUP_STATUS] = node.getStatus();
                            new Event(Event.DEBUG, name + " node " + key + ": "+
                                id + " " + info[GROUP_STATUS] + " " + j +
                                " - " + str).send(); 
                        }
                        info[GROUP_STATUS] = node.getStatus();
                        ch.clear();
                    }
                    if (ph != null)
                        ph.clear();
                    if (o != null)
                        ((Map) o).clear();
                }
                else if (cachedProps.containsKey(key)) { // recreate a node
                    ph = (Map) change.remove(key);   // new props
                    props.put(key, ph);                  // copy it over
                    id = addNode(mtime, ph);
                    if (id >= 0) {
                        new Event(Event.INFO, name + " node " + key +
                            " has been replaced on " + id).send();
                        info = nodeList.getMetaData(id);
                        info[GROUP_TIME] = mtime;
                        startNode(id, MF_NODE);
                    }
                    else {
                        k ++;
                        new Event(Event.ERR, name + " failed to replace "+
                            "node " + key + ": " + id).send();
                    }
                }
                else { // add a new node
                    ph = (Map) change.remove(key); // new props
                    props.put(key, ph);                // copy it over
                    id = addNode(mtime, ph);
                    if (id >= 0) { // created
                        new Event(Event.INFO, name + " node " + key +
                            " has been created on " + id).send();
                        info = nodeList.getMetaData(id);
                        info[GROUP_TIME] = mtime;
                        startNode(id, MF_NODE);
                    }
                    else { // failed
                        k ++;
                        new Event(Event.ERR, name + " failed to create "+
                            "node " + key + ": " + id).send();
                    }
                }
            }

            if ((debug & Service.DEBUG_INIT) > 0) {
                browser = nodeList.browser();
                while ((i = browser.next()) >= 0) {
                    info = nodeList.getMetaData(i);
                    strBuf.append("\n\t" + nodeList.getKey(i) + ": " + i +" "+
                        info[GROUP_HBEAT]/1000 + " " +
                        info[GROUP_TIMEOUT]/1000 + " " +
                        info[GROUP_RETRY] + " " +
                        info[GROUP_SIZE] + " " + info[GROUP_STATUS]);
                }
            }
        }
        else { // no node defined, so delete all nodes
            browser = nodeList.browser();
            while ((id = browser.next()) >= 0) { // delete all nodes
                key = nodeList.getKey(id);
                stopNode(id, MF_NODE);
                node = (MessageNode) nodeList.remove(id);
                if (node != null)
                    node.close();
                new Event(Event.INFO, name + " node " + key +
                    " has been deleted on " + id).send();
            }
        }

        // for existing or new receivers
        k += reloadExistingNodes(mtime, props, change, MF_RCVR, isNewMaster);

        if (change != null)
            change.clear();

        if (isNewMaster) { // reset the cached props
            cachedProps.clear();
            cachedProps = props;
        }

        return k;
    }

    /**
     * It returns a Map with only modified or new props.  The deleted
     * items will be set to null in the returned Map.  If the container
     * has been changed, it will be reflected by the object keyed by base.
     */
    public Map<String, Object> diff(Map props) {
        if (cachedProps.equals(props))
            return null;
        return Utils.diff("FLOW", componentMap, cachedProps, props);
    }

    /**
     * It takes the Map returned from diff() and analizes what have been
     * changed.  If the Map contains major changes that require to stop
     * or disable the flow, it returns true.  Otherwise, it returns false to
     * indicate the reload of the flow is good enough.
     */
    public boolean hasMajorChange(Map<String, Object> change) {
        Object o;
        Map ph, ch, props;
        Iterator iter;
        String str, value;

        if (change == null || change.size() <= 0)
            return false;

        o = change.get("FLOW");
        if (o != null)
            props = (Map) o;
        else
            props = new HashMap();

        for (String key : change.keySet()) {
            if (key.length() <= 0)
                continue;
            if ((o = change.get(key)) == null || "FLOW".equals(key))
                continue;
            if (receiverList.containsKey(key)) { // changes on existing receiver
                if ((debug & Service.DEBUG_DIFF) > 0)
                    new Event(Event.DEBUG, name + ": major change on receiver "+
                        key).send();
                return true;
            }
            else if (templateList.containsKey(key)) { // existing templates
                long[] info = templateList.getMetaData(key);
                if (info != null && info.length > 1 && info[1] == MF_RCVR) {
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change on receiver template "+ key).send();
                    return true;
                }
                else if (info != null && info.length > 1 && info[1] == MF_PSTR){
                    List list;
                    int i, n;
                    ph = (Map) o;
                    o = props.get("Persister");
                    if (o != null && o instanceof List)
                        list = (List) o;
                    else // no changes on the item list
                        list = (List) cachedProps.get("Persister");
                    n = list.size();
                    o = null;
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        if (o == null)
                            continue;
                        str = null;
                        if (o instanceof String)
                            str = (String) o;
                        else if (o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            continue;
                        if (key.equals(str)) // found it
                            break;
                    }
                    if (i < n && o instanceof Map) { // config template
                        ConfigTemplate temp;
                        int j, k;
                        Map<String, Object> h;
                        h = Utils.cloneProperties((Map) o);
                        h.put("Property", ph);
                        temp = new ConfigTemplate(h);
                        k = temp.getSize();
                        str = null;
                        value = null;
                        for (j=0; j<k; j++) {
                            str = temp.getItem(j);
                            o = temp.getProps(str);
                            if (o == null || !(o instanceof Map))
                                continue;
                            value = (String) ((Map) o).get("LinkName");
                            if (value != null && value.indexOf("${") >= 0)
                                value = MessageUtils.substitute(value);
                            if (!linkList.containsKey(value))
                                break;
                        }
                        temp.close();
                        if (j < k && value != null) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new link " + value+
                                    " for the existing persister template " +
                                    key + ":" + str).send();
                            return true;
                        }
                    }
                }
            }
            else if (persisterList.containsKey(key)) { // existing persisters
                MessagePersister pstr;
                ph = (Map) o;
                str = (String) ph.get("LinkName");
                if (str != null && str.indexOf("${") >= 0)
                    str = MessageUtils.substitute(str);
                if (!linkList.containsKey(str)) { // new link
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change with new uplink " + str +
                            " for the existing persister " + key).send();
                    return true;
                }
                pstr = (MessagePersister) persisterList.get(key);
                if (pstr != null && !str.equals(pstr.getLinkName())) {
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change on uplink " + str +
                            " for the existing persister " + key).send();
                    return true;
                }
            }
            else if (nodeList.containsKey(key)) { // existing nodes
                MessageNode node;
                ph = (Map) o;
                str = (String) ph.get("LinkName");
                if (str != null && str.indexOf("${") >= 0)
                    str = MessageUtils.substitute(str);
                if (!linkList.containsKey(str)) { // new link
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change with new outlink " + str +
                            " for the existing node " + key).send();
                    return true;
                }
                node = (MessageNode) nodeList.get(key);
                if (node != null && !str.equals(node.getLinkName())) {
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change on uplink " + str +
                            " for the existing node " + key).send();
                    return true;
                }

                ch = (Map) cachedProps.get(key);
                str = (String) ph.get("ClassName");
                value = (String) ch.get("ClassName");
                if (!str.equals(value)) { // implementation changed
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change on classname "+ str +"/"+ value +
                            " for the existing node "+key).send();
                    return true;
                }

                Map<String, Object> map = new HashMap<String, Object>();
                Map<String, Object> h = new HashMap<String, Object>();
                map.put("OutLink", ph.get("OutLink"));
                h.put("OutLink", ch.get("OutLink"));
                ch = Utils.diff("LINK", linkMap, h, map);
                map.clear();
                h.clear();
                if (ch != null && ch.size() > 0) { // with changes on outlinks
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG,name+": major change on outlinks"+
                            " for the existing node " + key).send();
                    ch.clear();
                    return true;
                }
                ch.clear();
            }
            else { // new receivers, persisters or nodes
                List list;
                int i, n, type;
                ph = (Map) o;
                if ((o = props.get("Receiver")) != null) { // check receivers
                    list = (List) o;
                    n = list.size();
                    o = null;
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        if (o == null)
                            continue;
                        str = null;
                        if (o instanceof String)
                            str = (String) o;
                        else if (o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            continue;
                        if (key.equals(str)) // found it
                            break;
                    }
                    if (i < n && o instanceof String) { // normal receiver
                        str = (String) ph.get("LinkName");
                        if (str != null && str.indexOf("${") >= 0)
                            str = MessageUtils.substitute(str);
                        if (!linkList.containsKey(str)) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new link " + str +
                                    " for the new reciever " + key).send();
                            return true;
                        }
                        continue;
                    }
                    else if (i < n && o instanceof Map) { // config template
                        ConfigTemplate temp;
                        int j, k;
                        Map<String, Object> h;
                        h = Utils.cloneProperties((Map) o);
                        h.put("Property", ph);
                        temp = new ConfigTemplate(h);
                        k = temp.getSize();
                        str = null;
                        value = null;
                        for (j=0; j<k; j++) {
                            str = temp.getItem(j);
                            o = temp.getProps(str);
                            if (o == null || !(o instanceof Map))
                                continue;
                            value = (String) ((Map) o).get("LinkName");
                            if (value != null && value.indexOf("${") >= 0)
                                value = MessageUtils.substitute(value);
                            if (!linkList.containsKey(value))
                                break;
                        }
                        temp.close();
                        if (j < k && value != null) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new link " + value+
                                    " for the new reciever template " +
                                    key + ":" + str).send();
                            return true;
                        }
                        continue;
                    }
                    else if (i < n) // not supposed to be here
                        continue;
                    // not a receiver
                }
                if ((o = props.get("Persister")) != null) { // check persisters
                    list = (List) o;
                    n = list.size();
                    o = null;
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        if (o == null)
                            continue;
                        str = null;
                        if (o instanceof String)
                            str = (String) o;
                        else if (o instanceof Map)
                            str = (String) ((Map) o).get("Name");
                        else
                            continue;
                        if (key.equals(str)) // found it
                            break;
                    }
                    if (i < n && o instanceof String) { // normal persister
                        str = (String) ph.get("LinkName");
                        if (str != null && str.indexOf("${") >= 0)
                            str = MessageUtils.substitute(str);
                        if (!linkList.containsKey(str)) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new uplink " + str +
                                    " for the new persister " + key).send();
                            return true;
                        }
                        continue;
                    }
                    else if (i < n && o instanceof Map) { // config template
                        ConfigTemplate temp;
                        int j, k;
                        Map<String, Object> h;
                        h = Utils.cloneProperties((Map) o);
                        h.put("Property", ph);
                        temp = new ConfigTemplate(h);
                        k = temp.getSize();
                        str = null;
                        value = null;
                        for (j=0; j<k; j++) {
                            str = temp.getItem(j);
                            o = temp.getProps(str);
                            if (o == null || !(o instanceof Map))
                                continue;
                            value = (String) ((Map) o).get("LinkName");
                            if (value != null && value.indexOf("${") >= 0)
                                value = MessageUtils.substitute(value);
                            if (!linkList.containsKey(value))
                                break;
                        }
                        temp.close();
                        if (j < k && value != null) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new uplink " + value+
                                    " for the new persister template " +
                                    key + ":" + str).send();
                            return true;
                        }
                        continue;
                    }
                    else if (i < n) // not supposed to be here
                        continue;
                    // not a persister
                }
                // only new nodes reach here
                str = (String) ph.get("LinkName");
                if (str == null) // not a node
                    continue;
                else if (str.indexOf("${") >= 0)
                    str = MessageUtils.substitute(str);
                if (!linkList.containsKey(str)) { // new link
                    if ((debug & Service.DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name +
                            ": major change with new uplink " + str +
                            " for the new node " + key).send();
                    return true;
                }
                o = ph.get("OutLink");
                if (o == null || !(o instanceof List)) // no outlink
                    continue;
                list = (List) o;
                n = list.size();
                for (i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof String) {
                        str = (String) o;
                        if (!linkList.containsKey(str)) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new outlink " + str +
                                    " for the new node " + key).send();
                            return true;
                        }
                    }
                    else if (o instanceof Map) {
                        str = (String) ((Map) o).get("Name");
                        if (!linkList.containsKey(str)) {
                            if ((debug & Service.DEBUG_DIFF) > 0)
                                new Event(Event.DEBUG, name +
                                    ": major change with new outlink " + str +
                                    " for the new node " + key).send();
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /** It loggs the details of the change returned by diff()  */
    public void showChange(String prefix, String base, Map<String,Object>change,
        boolean detail) {
        Object o;
        Map h;
        StringBuffer strBuf = new StringBuffer();
        int n = 0;
        if (change == null)
            return;
        for (String key : change.keySet()) {
            if (key == null || key.length() <= 0)
                continue;
            o = change.get(key);
            n ++;
            if (o == null)
                strBuf.append("\n\t" + n + ": " + key + " has been removed");
            else if ("FLOW".equals(key)) {
                strBuf.append("\n\t" + n + ": " + key + " has changes");
                if(!detail)
                    continue;
                h = Utils.getMasterProperties("FLOW", componentMap,cachedProps);
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
            else if (!cachedProps.containsKey(key))
                strBuf.append("\n\t" + n + ": " + key + " is new");
            else if ((h = (Map) cachedProps.get(key)) == null || h.size() <= 0)
                strBuf.append("\n\t" + n + ": " + key + " was empty");
            else if (h.equals((Map) o))
                strBuf.append("\n\t" + n + ": " + key + " has no diff");
            else {
                strBuf.append("\n\t" + n + ": " + key + " has been changed");
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

    /** It loggs the details of the change on node returned by diff()  */
    private void showNodeChange(String prefix, Map<String, Object> change,
        Map ph, boolean detail) {
        Object o;
        Map h;
        StringBuffer strBuf = new StringBuffer();
        int n = 0;
        if (change == null)
            return;
        for (String key : change.keySet()) {
            if (key == null || key.length() <= 0)
                continue;
            o = change.get(key);
            n ++;
            if (o == null)
                strBuf.append("\n\t" + n + ": " + key + " has been removed");
            else if ("NODE".equals(key)) {
                strBuf.append("\n\t" + n + ": " + key + " has changes");
                if(!detail)
                    continue;
                h = Utils.getMasterProperties("NODE", includeMap, ph);
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
            else if (!ph.containsKey(key))
                strBuf.append("\n\t" + n + ": " + key + " is new");
            else if ((h = (Map) ph.get(key)) == null || h.size() <= 0)
                strBuf.append("\n\t" + n + ": " + key + " was empty");
            else if (h.equals((Map) o))
                strBuf.append("\n\t" + n + ": " + key + " has no diff");
            else {
                strBuf.append("\n\t" + n + ": " + key + " has been changed");
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

    private void checkpoint(MessageNode node) {
        if (node == null || checkpointDir == null)
            return;
        String key = node.getName();
        Map<String, Object> chkpt = node.checkpoint();
        if (chkpt != null && chkpt.size() > 0) try {
            File file = new File(checkpointDir);
            if (!file.exists())
                file.mkdirs();
            file = new File(checkpointDir + FILE_SEPARATOR + key + ".json");
            PrintWriter fout = new PrintWriter(new FileOutputStream(file));
            fout.println(JSON2Map.toJSON(chkpt, "", "\n"));
            fout.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + " failed to checkpoint for " +
                key + ": " + e.toString()).send();
        }
    }

    private void restore(MessageNode node) {
        if (node == null)
            return;
        if (checkpointDir != null) { // restore from checkpoint
            String key = node.getName();
            File file = new File(checkpointDir + FILE_SEPARATOR + key +".json");
            if (!file.exists() || !file.canRead())
                return;
            try {
                FileReader fr = new FileReader(file);
                Map h = (Map) JSON2Map.parse(fr);
                fr.close();
                if (h != null) {
                    node.restore(Utils.cloneProperties(h));
                    h.clear();
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to load checkpoint for "+
                    key + ": " + Event.traceStack(e)).send();
            }
        }
    }

    public void close() {
        ConfigTemplate temp;
        Browser b;
        int i;

        stop();
        setNodeStatus(MessagePersister.PSTR_CLOSED, MF_PSTR, persisterList);
        setNodeStatus(MessageNode.NODE_CLOSED, MF_NODE, nodeList);
        setNodeStatus(MessageReceiver.RCVR_CLOSED, MF_RCVR, receiverList);

        upLink.clear();
        outLink.clear();
        rcvrMap.clear();
        pstrMap.clear();
        receiverList.clear();
        nodeList.clear();
        persisterList.clear();
        threadList.clear();
        b = templateList.browser();
        while ((i = b.next()) >= 0) {
            temp = (ConfigTemplate) templateList.get(i);
            if (temp != null)
                temp.close();
        }
        templateList.clear();
        previousStatus = flowStatus;
        flowStatus = FLOW_CLOSED;

        new Event(Event.INFO, name + ": message flow closed").send();
        if (root != null && !isStopped(root))
            stopRunning(root);
    }

    static {
        List<Object> list = new ArrayList<Object>();
        componentMap = new HashMap<String, Object>();
        list.add("Receiver");
        list.add("Node");
        list.add("Persister");
        componentMap.put("FLOW", list);
        includeMap = new HashMap<String, Object>();
        list = new ArrayList<Object>();
        list.add("Ruleset");
        includeMap.put("NODE", list);
        linkMap = new HashMap<String, Object>();
        list = new ArrayList<Object>();
        list.add("OutLink");
        linkMap.put("LINK", list);
    }
}
