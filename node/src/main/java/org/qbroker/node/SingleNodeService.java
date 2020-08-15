package org.qbroker.node;

/* SingleNodeService.java - a JMS message service with only one MessageNode */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.ReservableCells;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Service;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.persister.MessagePersister;
import org.qbroker.node.MessageNode;
import org.qbroker.event.Event;

/**
 * SingleNodeService is message service with only one MessageNode. There is
 * no MessageReceiver as compared to MessageFlow. It is for testing only.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SingleNodeService implements Service, Runnable {
    private String name;
    private int capacity = 1;
    private int pauseTime;
    private int standbyTime;
    private long mtime, waitTime;
    private MessageNode node;
    private Map<String, String> outLink;
    private XQueue root = null;
    private ReservableCells reqList;
    private int xaMode = MessageUtils.XA_CLIENT;
    private int myStatus, statusOffset, debug = 0;
    private int previousStatus;
    private int[] partition = new int[] {0, 0};
    private AssetList persisterList;
    private AssetList threadList;
    private AssetList linkList;
    private AssetList templateList;
    private Thread nodeThread;
    private ThreadGroup thg;
    private Map cachedProps = null;
    private Map<String, Object> pstrMap;
    private static Map<String, Object> linkMap;

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
    private final static String[] statusText = Service.statusText;

    /** Creates new SingleNodeService */
    public SingleNodeService(Map props, XQueue root) {
        Object o;
        Browser browser;
        long[] info;
        String className;
        List list;
        int n, i;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("name not defined"));
        name = (String) o;

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;

        if ((o = props.get("XAMode")) != null) // XQ type determined by QB
            xaMode = Integer.parseInt((String) o);

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

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        thg = new ThreadGroup(name);
        mtime = System.currentTimeMillis();
        myStatus = Service.SERVICE_READY;
        previousStatus = Service.SERVICE_CLOSED;
        statusOffset = 0 - TimeWindows.EXCEPTION;

        pstrMap = new HashMap<String, Object>();
        persisterList = new AssetList("Persister", 16);
        threadList = new AssetList("Thread", 20);
        linkList = new AssetList("Link", 20);
        templateList = new AssetList("Temp", 20);
        initialize(props);
        if (root != null) // root is provided
            this.root = root;
        else {
            this.root = new IndexedXQueue("root", node.getCapacity());
            partition[0] = 0;
            partition[1] = 0;
        }
        initXQueues();
        reqList = new ReservableCells(name, this.root.getCapacity());
        setPersisterStatus(myStatus);
    }

    public SingleNodeService(Map props) {
        this(props, null);
    }

    /**
     * It initializes all objects and threads including the ones for
     * the monitor
     */
    private void initialize(Map props) {
        int i, j, k, n, size;
        List list = null;
        Browser browser;
        long[] info;
        Object o;
        long tm;
        String key;

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

        outLink = new HashMap<String, String>();

        initPersisterList(props);
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
        }

        if ((o = props.get("Node")) == null || !(o instanceof List))
            throw(new IllegalArgumentException("Node not well defined"));
        list = (List) o;
        if (list.size() <= 0 || (o = list.get(0)) == null ||
            !(o instanceof String))
            throw(new IllegalArgumentException("Node is empty or bad item"));
        key = (String) o;
        if ((o = props.get(key)) == null || !(o instanceof Map))
            throw(new IllegalArgumentException(key + " is not well defined"));

        key = (String) ((Map) o).get("ClassName");
        o = MessageUtils.initNode((Map) o, name);
        if (o == null) {
            ((Map) o).clear();
            throw(new IllegalArgumentException(name +
                " failed to instanciate node with null"));
        }
        else if (o instanceof MessageNode) {
            node = (MessageNode) o;
            nodeThread = new Thread(thg, this, name + "/Node_0");
            nodeThread.setDaemon(true);
            nodeThread.setPriority(Thread.NORM_PRIORITY);
            if (key == null)
                key = node.getClass().getName();
            k = key.lastIndexOf('.');
            if (k < 0)
                k = 0;
            else
                k++;
            new Event(Event.INFO, name + " " + key.substring(k) + " for " +
                node.getName() + " is instantiated").send();
        }
        else if (o instanceof InvocationTargetException) {
            InvocationTargetException e = (InvocationTargetException) o;
            Throwable ex = e.getTargetException();
            if (ex == null) {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate node: "+
                    Event.traceStack(e)));
            }
            else if (ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate node: " + ee.toString() +" "+
                        Event.traceStack(ex)));
                else
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate node: " +
                        Event.traceStack(ex)));
            }
            else {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate node: " +
                    Event.traceStack(ex)));
            }
        }
        else if (o instanceof Exception) {
            Exception e = (Exception) o;
            throw(new IllegalArgumentException(name +
                " failed to instanciate node: " + Event.traceStack(e)));
        }
        else if (o instanceof Error) {
            Error e = (Error) o;
            new Event(Event.ERR, name + " failed to instantiate node: " +
                e.toString()).send();
            Event.flush(e);
        }
        else {
            throw(new IllegalArgumentException(name +
                " failed to instanciate node: "+ o.toString()));
        }
    }

    private void initPersisterList(Map props) {
        Object o;
        Map<String, Object> ph;
        List list = null;
        String key, str;
        int i, k, n, id;

        o = props.get("Persister");
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
                    templateList.add(key, new long[]{m, 1}, temp);
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
                    str = (String) ph.get("Name");
                    if (str == null || str.length() <= 0) {
                        new Event(Event.ERR, name + " name is not defined for "+
                            key + " at " + j + "/" + i).send();
                        continue;
                    }
                    id = addPersister(mtime, ph);
                    if (id < 0)
                        new Event(Event.ERR, name + " failed to add " + key +
                            ":" + str + " to list at " + j + "/" + i + ": " +
                            persisterList.size() + "/" +
                            persisterList.getCapacity()).send();
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
                id = addPersister(mtime, ph);
                if (id < 0)
                    new Event(Event.ERR, name + " failed to add " + key +
                        " to list at " + i + ": " + persisterList.size() + "/"+
                        persisterList.getCapacity()).send();
            }
        }
    }

    /** It initializes a new persister, adds it to the list and returns its id*/
    private int addPersister(long tm, Map ph) {
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
        else if (o instanceof MessagePersister) { // for persister only
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
                Thread th;
                th = new Thread(thg, this, name + "/Persister_" + id);
                th.setDaemon(true);
                th.setPriority(Thread.NORM_PRIORITY);
                k = threadList.add(name + "/Persister_" + id, null, th);
                info[GROUP_TID] = k;
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
            str = o.getClass().getName();
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
     * It disables the id-th persister and returns the disabled status upon
     * success. If no persister for the given id, it returns -1. In case of
     * timeout, it returns the previous status right before the final reset of
     * status.
     */
    private int disablePersister(int id) {
        MessagePersister pstr = null;
        String key = null, str = null;
        int status = 0;
        long[] info = null;

        info = persisterList.getMetaData(id);
        pstr = (MessagePersister) persisterList.get(id);
        if (pstr == null || info == null)
            return -1;
        status = pstr.getStatus();
        key = pstr.getLinkName();
        str = pstr.getName();

        info[GROUP_STATUS] = status;
        if (status >= Service.SERVICE_READY &&
            status <= Service.SERVICE_RETRYING) {
            int mask = 0;
            int pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
            XQueue xq = (XQueue) linkList.get(key);
            if (xq != null) { // with xq
                mask = xq.getGlobalMask();
                pstr.setStatus(Service.SERVICE_READY);
                xq.setGlobalMask(mask | XQueue.STANDBY);
                for (int j=0; j<10; j++) { // wait for disabled
                    if ((status = pstr.getStatus()) == Service.SERVICE_DISABLED)
                        break;
                    else try {
                        Thread.sleep(pt);
                    }
                    catch (Exception e) {
                    }
                }
                status = pstr.getStatus();
                mask = xq.getGlobalMask();

                if (status != Service.SERVICE_DISABLED) { // timed out
                    new Event(Event.ERR, name + ": timed out to disable " +
                        str + ": " + statusText[status]).send();
                }
            }
            else { // no xq
                new Event(Event.ERR, name + ": null xq of " + key +
                    " for " + str + ": " + statusText[status]).send();
            }
            if (status != Service.SERVICE_DISABLED)
                pstr.setStatus(Service.SERVICE_DISABLED);
            info[GROUP_STATUS] = Service.SERVICE_DISABLED;
            if (xq != null) { // remove the disable bit
                if ((mask & XQueue.STANDBY) > 0)
                    xq.setGlobalMask(mask ^ XQueue.STANDBY);
            }
        }
        else if (status >= Service.SERVICE_PAUSE &&
            status < Service.SERVICE_DISABLED) {
            pstr.setStatus(Service.SERVICE_DISABLED);
            info[GROUP_STATUS] = Service.SERVICE_DISABLED;
            status = Service.SERVICE_DISABLED;
        }

        return status;
    }

    /** stops the id-th persister and removes the thread */
    private void stopPersister(int id) {
        MessagePersister pstr = null;
        long[] info = null;
        String key;
        int k, status;

        // disable the node first
        info = persisterList.getMetaData(id);
        pstr = (MessagePersister) persisterList.get(id);
        if (pstr == null || info == null)
            return;
        key = pstr.getName();
        status = disablePersister(id);
        if (status < Service.SERVICE_PAUSE)
            new Event(Event.ERR, name + " failed to disable persister " +
                key + ": " + ((status >= 0) ? statusText[status] :
                String.valueOf(status))).send();
        pstr.setStatus(Service.SERVICE_STOPPED);

        info[GROUP_STATUS] = Service.SERVICE_STOPPED;
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

    /** starts the id-th persister and the thread */
    private void startPersister(int id) {
        MessagePersister pstr = null;
        Thread th;
        long[] info = null;
        int k;

        info = persisterList.getMetaData(id);
        pstr = (MessagePersister) persisterList.get(id);
        if (pstr == null || info == null)
            return;
        pstr.setStatus(Service.SERVICE_RUNNING);

        if ((k = (int) info[GROUP_TID]) < 0) { // init the thread
            String str = name + "/persister_" + id;
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
     * sets the status on all the persisters in persisterList
     */
    private int setPersisterStatus(int status) {
        MessagePersister pstr;
        Browser browser;
        long[] info;
        long tm = System.currentTimeMillis();
        int i;

        browser = persisterList.browser();
        while ((i = browser.next()) >= 0) {
            info = persisterList.getMetaData(i);
            pstr = (MessagePersister) persisterList.get(i);
            info[GROUP_STATUS] = pstr.getStatus();
            info[GROUP_TIME] = tm;
            pstr.setStatus(status);
        }
        return persisterList.size();
    }

    /**
     * initializes all XQueues used by the node and propagates acks
     */
    private void initXQueues() {
        String linkName, key;
        Browser browser, b;
        XQueue xq;
        boolean ack = false;
        int i, j, cc, cn, k, m, n, ipps, opps;

        m = 0;
        cc = capacity;
        k = node.getXAMode();
        if (k > 0 && (k & MessageUtils.XA_CLIENT) > 0) // update the default
            ack = true;
        cn = node.getCapacity();
        if (cn > 0)
            cc = cn;
        else
            cn = cc;

        MessageUtils.disableAck(root);
        k = root.getGlobalMask();
        if (ack && (k & XQueue.EXTERNAL_XA) == 0) // overwrite XA bit on root
            MessageUtils.enableAck(root);
        // update the default ack
        ack = ((root.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        // init all xqs linked to the node
        k = node.getNumberOfOutLinks();
        for (j=0; j<k; j++) {
            linkName = node.getLinkName(j);
            if (node.getCapacity(j) > 0)
                cc = node.getCapacity(j);
            else
                cc = cn;
            if (!linkList.containsKey(linkName)) { // not init yet
                xq = new IndexedXQueue(linkName, cc);

                if (outLink.containsKey(linkName))
                    opps = Integer.parseInt((String) outLink.get(linkName));
                else
                    opps = 0;
                linkList.add(linkName, new long[]{cc, 1, opps}, xq);
                m ++;
            }
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

    private void resetXQueues(int status) {
        Browser browser;
        XQueue xq;
        long[] info;
        int i, k, mask;

        if (status < Service.SERVICE_READY || status > Service.SERVICE_STOPPED)
            return;

        browser = linkList.browser();
        while ((i = browser.next()) >= 0) {
            xq = (XQueue) linkList.get(i);
            if (xq == null)
                continue;
            switch (status) {
              case Service.SERVICE_RUNNING:
                MessageUtils.resumeRunning(xq);
                break;
              case Service.SERVICE_PAUSE:
                MessageUtils.pause(xq);
                break;
              case Service.SERVICE_STANDBY:
                MessageUtils.standby(xq);
                break;
              case Service.SERVICE_STOPPED:
                MessageUtils.stopRunning(xq);
                break;
              case Service.SERVICE_DISABLED:
                mask = xq.getGlobalMask();
                xq.setGlobalMask(mask | XQueue.STANDBY);
                break;
              default:
                break;
            }
        }

        browser = persisterList.browser();
        while ((i = browser.next()) >= 0) {
            info = persisterList.getMetaData(i);
            k = (int) info[GROUP_TID];
            if (k >= 0) { // notify the thread
                Thread th = (Thread) threadList.get(k);
                if (th != null && th.isAlive())
                    th.interrupt();
            }
        }
    }

    private void cleanupXQueues() {
        Browser browser;
        XQueue xq;
        int i;

        browser = linkList.browser();
        while ((i = browser.next()) >= 0) {
            xq = (XQueue) linkList.get(i);
            if (xq == null)
                continue;
            xq.clear();
        }
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
            XQueue [] outLinks;
            linkName = node.getLinkName();
            xq = root;
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
                    if (status >= MessageNode.NODE_STOPPED ||
                        MessageUtils.keepRunning(xq))
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
                    if (status >= MessageNode.NODE_STOPPED ||
                        MessageUtils.keepRunning(xq))
                        break;
                    try {
                        Thread.sleep(standbyTime);
                    }
                    catch (Exception e) {
                    }
                    previous_status = status;
                    status = node.getStatus();
                }

                if (MessageUtils.isStopped(xq) ||
                    status >= MessageNode.NODE_STOPPED)
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
                node.resetMetaData(xq, outLinks);
            }

            if (node.getStatus() == MessageNode.NODE_CLOSED) { // close XQs
                for (i=0; i<outLinks.length; i++)
                    MessageUtils.pause(outLinks[i]);
                try {
                    Thread.sleep(2000);
                }
                catch (Exception e) {
                }
                for (i=0; i<outLinks.length; i++)
                    MessageUtils.stopRunning(outLinks[i]);
            }
            new Event(Event.INFO, name + ": " + node.getName() +
                " stopped on " + xq.getName()).send();
            if (node != null &&
                node.getStatus() == MessageNode.NODE_CLOSED) {
                if (!MessageUtils.isStopped(xq))
                    MessageUtils.stopRunning(xq);
                node.close();
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
                if (!MessageUtils.isStopped(xq))
                    MessageUtils.stopRunning(xq);
                pstr.close();
            }
        }
        else {
            new Event(Event.WARNING, name + ": failed to parse threadName " +
                "for the supported key: " + str).send();
        }
    }

    private void startThreads() {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = persisterList.browser();
        while ((id = browser.next()) >= 0) { // start all threads
            info = persisterList.getMetaData(id);
            if ((k = (int) info[GROUP_TID]) < 0)
                continue;
            th = (Thread) threadList.get(k);
            if (th != null)
                th.start();
        }
    }

    private void interruptThreads() {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = persisterList.browser();
        while ((id = browser.next()) >= 0) { // interrupt all threads
            info = persisterList.getMetaData(id);
            if ((k = (int) info[GROUP_TID]) < 0)
                continue;
            th = (Thread) threadList.get(k);
            if (th != null && th.isAlive())
                th.interrupt();
        }
    }

    private void joinThreads() {
        int id, k, pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
        long[] info;
        Thread th;
        Browser browser = persisterList.browser();
        while ((id = browser.next()) >= 0) { // join all threads
            info = persisterList.getMetaData(id);
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

    private void initThreads() {
        int id, k;
        long[] info;
        Thread th;
        Browser browser = persisterList.browser();
        while ((id = browser.next()) >= 0) {
            info = persisterList.getMetaData(id);
            th = new Thread(thg, this,  name + "/Persister_" + id);
            th.setDaemon(true);
            th.setPriority(Thread.NORM_PRIORITY);
            k = threadList.add(name + "/Persister_" + id, null, th);
            info[GROUP_TID] = k;
        }
    }

    /**
     * starts all threads and/or nodes
     */
    public synchronized void start() {
        if (myStatus == Service.SERVICE_READY &&
            previousStatus == Service.SERVICE_CLOSED) { //init
            previousStatus = myStatus;
            myStatus = Service.SERVICE_RUNNING;
            startThreads();
            nodeThread.start();
            try { // wait for threads start up
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message service started").send();
        }
        else if (myStatus == Service.SERVICE_CLOSED) { // flow closed
            new Event(Event.WARNING, name +
                ": can not start a closed message service").send();
        }
        else if (myStatus != Service.SERVICE_RUNNING) { // flow not running
            nodeThread = new Thread(thg, this,  name + "/Node_0");
            nodeThread.setDaemon(true);
            nodeThread.setPriority(Thread.NORM_PRIORITY);
            node.setStatus(MessageNode.NODE_RUNNING);

            initThreads();
            setPersisterStatus(MessagePersister.PSTR_RUNNING);
            resetXQueues(MessagePersister.PSTR_RUNNING);
            MessageUtils.resumeRunning(root);
            if (nodeThread != null && nodeThread.isAlive())
                nodeThread.interrupt();

            previousStatus = myStatus;
            myStatus = Service.SERVICE_RUNNING;
            startThreads();
            nodeThread.start();
            try { // wait for threads start up
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            new Event(Event.INFO, name + ": message service started").send();
        }
        else { // flow already running
            new Event(Event.INFO, name + ": service already running").send();
        }
    }

    /**
     * stops all nodes and the threads
     */
    public void stop() {
        if (myStatus == Service.SERVICE_READY &&
            previousStatus == Service.SERVICE_CLOSED) { //init
            // flow has not started yet, so abort it
            XQueue xq;
            Browser browser;
            MessagePersister pstr = null;
            String key;
            int i;

            setPersisterStatus(MessagePersister.PSTR_STOPPED);
            node.setStatus(MessageNode.NODE_STOPPED);

            browser = persisterList.browser();
            while ((i = browser.next()) >= 0) {
                pstr = (MessagePersister) persisterList.get(i);
                key = pstr.getLinkName();
                xq = (XQueue) linkList.get(key);
                pstr.persist(xq, 1);
            }

            try { // wait for threads stopped
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
            previousStatus = myStatus;
            myStatus = Service.SERVICE_STOPPED;
            new Event(Event.INFO, name + ": message service stopped").send();
        }
        else if (myStatus < Service.SERVICE_STOPPED) { // stop the flow
            int n;
            if ((root.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) { // with XA
                setPersisterStatus(MessagePersister.PSTR_STOPPED);
                resetXQueues(MessagePersister.PSTR_PAUSE);
                interruptThreads();
                node.setStatus(MessageNode.NODE_STOPPED);
                MessageUtils.pause(root);
                nodeThread.interrupt();
            }
            else { // without XA
                node.setStatus(MessageNode.NODE_STOPPED);
                setPersisterStatus(MessagePersister.PSTR_STOPPED);
                MessageUtils.pause(root);
                resetXQueues(MessagePersister.PSTR_PAUSE);
                nodeThread.interrupt();
                interruptThreads();
            }
            joinThreads();
            if (nodeThread != null && nodeThread.isAlive()) {
                int pt = (pauseTime >= 10) ? pauseTime / 10 : 5;
                for (int i=0; i<10; i++) {
                    nodeThread.interrupt();
                    try {
                        nodeThread.join(pt);
                    }
                    catch (Exception e) {
                        nodeThread.interrupt();
                    }
                    if (!nodeThread.isAlive())
                        break;
                }
            }
            cleanupXQueues();
            if (partition[1] == 0 || partition[1] == root.getCapacity())
                root.clear();
            previousStatus = myStatus;
            myStatus = Service.SERVICE_STOPPED;
            new Event(Event.INFO, name + ": message service stopped").send();
        }
        else { // flow already stopped or closed
            new Event(Event.INFO, name + ": service already stopped").send();
        }
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return "service";
    }

    public int getStatus() {
        return myStatus;
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
     * returns number of XQueues of the flow
     */
    public int getNumberOfXQueues() {
        return linkList.size();
    }

    /**
     * It processes requests according to the preconfigured rulesets. If
     * timeout is less than zero, there is no wait.  If timeout is larger than
     * zero, it will wait for the processed event until timeout.  If timeout is
     * zero, it blocks until the event is processed.  It returns 1 for success,
     * 0 for timed out and -1 for failure.  The method is MT-Safe.
     *<br><br>
     * A request is required to be a JMSEvent. If you want to send JMS messages
     * provided by other vendors, please use the root XQueue directly.
     *<br><br>
     * N.B., The input request will be modified for the response.
     */
    public int doRequest(org.qbroker.common.Event msg, int timeout) {
        int i = 0, n = -1, sid = -1, begin, len;
        int m = 1;

        if (msg == null || !(msg instanceof JMSEvent))
            return -1;

        if (timeout > 0)
            m = timeout / 500 + 1; 
        else if (timeout == 0)
            m = 100;

        begin = partition[0];
        len = partition[1];

        switch (len) {
          case 0:
            if (timeout < 0) {
                sid = reqList.reserve(500L);
            }
            else for (i=0; i<m; i++) {
                sid = reqList.reserve(500L);
                if (sid >= 0 || !MessageUtils.keepRunning(root))
                    break;
            }
            break;
          case 1:
            if (timeout < 0) {
                sid = reqList.reserve(500L, begin);
            }
            else for (i=0; i<m; i++) {
                sid = reqList.reserve(500L, begin);
                if (sid >= 0 || !MessageUtils.keepRunning(root))
                    break;
            }
            break;
          default:
            if (timeout < 0) {
                sid = reqList.reserve(500L, begin, len);
            }
            else for (i=0; i<m; i++) {
                sid = reqList.reserve(500L, begin, len);
                if (sid >= 0 || !MessageUtils.keepRunning(root))
                    break;
            }
            break;
        }

        if (sid < 0)
            return 0;

        do {
            n = root.reserve(500L, sid);
            if (n == sid || !MessageUtils.keepRunning(root))
                break;
        } while (++i < m);
        if (n != sid) {//previous request timed out and it is still not done yet
            reqList.cancel(sid);
            new Event(Event.ERR, name + " failed to reserve the cell at " +
                sid + " since a previous request is still not done yet: " +
                root.getCellStatus(sid) + " " + root.depth() + ":" +
                root.size() + "/" + root.getCapacity()).send();
            return -1;
        }
        i = root.add(msg, sid);

        for (i=0; i<m; i++) { // wait for response
            if (root.collect(500L, sid) >= 0) { // got the response
                reqList.cancel(sid);
                return 1;
            }
        }
        // request timed out
        reqList.cancel(sid);
        msg.setExpiration(System.currentTimeMillis() - 60000L);

        return 0;
    }

    public void close() {
        ConfigTemplate temp;
        Browser b;
        int i;

        stop();
        setPersisterStatus(MessagePersister.PSTR_CLOSED);
        node.setStatus(MessageNode.NODE_CLOSED);

        outLink.clear();
        pstrMap.clear();
        persisterList.clear();
        threadList.clear();
        b = templateList.browser();
        while ((i = b.next()) >= 0) {
            temp = (ConfigTemplate) templateList.get(i);
            if (temp != null)
                temp.close();
        }
        templateList.clear();
        previousStatus = myStatus;
        myStatus = Service.SERVICE_CLOSED;

        new Event(Event.INFO, name + ": message service closed").send();
        if (root != null && !MessageUtils.isStopped(root))
            MessageUtils.stopRunning(root);
    }

    /** returns an instance of SingleNodeService with the property map */
    public static SingleNodeService initService(Map props) {
        if (props == null || props.size() <= 0)
            return null;
        SingleNodeService service = new SingleNodeService(props);
        Thread c = new Thread(service, "shutdown");
        Runtime.getRuntime().addShutdownHook(c);
        return service;
    }

    public static void main(String[] args) {
        int n, timeout = 2000;
        String filename = null, data = "this is a test";
        TextEvent msg;
        SingleNodeService service = null;

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
              case 'd':
                if (i+1 < args.length)
                    data = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = 1000 * Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
            Map ph;
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map props = (Map) JSON2Map.parse(fr);
            fr.close();

            service = new SingleNodeService(props);
            service.start();
            msg = new TextEvent(data);
            n = service.doRequest(msg, timeout);
            if (n > 0)
                System.out.println("Test is successful: " + n);
            else
                System.out.println("Test failed: " + n);
            service.stop();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (service != null)
                service.close();
        }
    }

    private static void printUsage() {
        System.out.println("SingleNodeService Version 1.0 (written by Yannan Lu)");
        System.out.println("SingleNodeService: a message service with only on node for testing");
        System.out.println("Usage: java org.qbroker.node.SingleNodeService -I cfg.json");
        System.out.println("  -?: print this usage page");
        System.out.println("  -I: config file for the service");
        System.out.println("  -d: data as the content of message body");
        System.out.println("  -t: timeout in second");
    }
}
