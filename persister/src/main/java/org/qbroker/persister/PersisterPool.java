package org.qbroker.persister;

/* PersisterPool.java - a pool of various persisters */

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.ObjectMessage;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.GenericPool;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.persister.MessagePersister;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * PersisterPool listens to an XQueue for requests carried by ObjectMessages.
 * Once there is a new request, it will check the URI and the type to figure
 * out the details of the request.  If the request is to ask for a persister,
 * it first looks up the classname of the implementation based on the URI
 * and the given properties.  Then PersisterPool tries to check out the pool
 * of the classname from the cache.  If there exists the persister pool for
 * the implementation, it just checks out a new instance of the persister
 * with the given properties.  Otherwise, PersisterPool will create a new pool
 * for the classname and checks out a new instance of the persister from it.
 * It then starts up a new thread on the persister and returns the thread back.
 * The requester is supposed to monitor the status of the thread in order to
 * tell the status of the persister.  The requester is also able to control the
 * persister via the associated transmit queue.  PersisterPool will frequently
 * monitor the status of each active persisters in every heartbeat.  If any one
 * of them is stopped or closed, PersisterPool will checkin the persister and
 * clean up the cache.  If the request is to return the used thread, it will
 * check in both the thread and the persister.
 *<br/><br/>
 * URI is used to identify the destinations.  PersisterPool parses the
 * URI string and creates a Map with all properties from the key-value
 * pairs specified in the query-string.  Therefore, please carefully define
 * them in the query-string of the URI.  PersisterPool also allows the
 * default properties defined for each implementations.  If any of the default
 * properties is missing in the URI, PersisterPool will copy it to the
 * property Map before the instantiation of the MessagePersister.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PersisterPool extends Persister implements Runnable {
    private AssetList poolList;     // holding GenericPools keyed by className
    private AssetList assetList;    // {xq,uri,thread,pstr} and state info
    private Template template;
    private String rcField, uriField;
    private int retryCount, heartbeat;
    private int poolSize, maxPersister;
    private int bufferSize = 4096;
    private final static int ASSET_XQ = 0;
    private final static int ASSET_URI = 1;
    private final static int ASSET_THR = 2;
    private final static int ASSET_OBJ = 3;
    private final static int OUT_TID = 0;
    private final static int OUT_PID = 1;
    private final static int OUT_GID = 2;
    private final static int OUT_STATUS = 3;
    private final static int OUT_PSTATUS = 4;
    private final static int OUT_RETRY = 5;
    private final static int OUT_TTL = 6;
    private final static int OUT_TIME = 7;

    public PersisterPool(Map props) {
        super(props);
        int i, n;
        Object o;
        List defaultProps;

        if (uri == null || uri.length() <= 0)
            uri = "pool://localhost";

        if ((o = props.get("MaxNumberPersister")) != null)
            maxPersister = Integer.parseInt((String) o);
        else
            maxPersister = 256;

        if (capacity <= 0)
            capacity = 1;

        if ((o = props.get("DefaultPoolSize")) != null)
            poolSize = Integer.parseInt((String) o);
        else
            poolSize = capacity;

        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);

        if ((o = props.get("URIField")) != null && o instanceof String)
            uriField = (String) o;
        else
            uriField = "URI";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("Template")) != null && ((String) o).length() > 0)
            template = new Template((String) o);

        if ((o = props.get("DefaultProperty")) != null || o instanceof List)
            defaultProps = (List) o;
        else
            defaultProps = new ArrayList();

        assetList = new AssetList(linkName, maxPersister);
        poolList = new AssetList(linkName, maxPersister);

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName + " with " + maxPersister).send();

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        else
            heartbeat = 60000;
        if (heartbeat <= 0)
            heartbeat = 60000;

        n = defaultProps.size();
        GenericPool pool = null;
        Map ph;
        String className;
        int pid, size;
        for (i=0; i<n; i++) {
            o = defaultProps.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            // clone it for separation
            ph = Utils.cloneProperties((Map) o);
            o = ph.get("ClassName");
            if (o == null || !(o instanceof String)) {
                new Event(Event.WARNING, uri + ": no classname defined for " +
                    "the default property of " + i).send();
                continue;
            }
            className = (String) o;
            if (poolList.getID(className) >= 0) {
                new Event(Event.WARNING,uri+": duplicated DefaultProperty for "+
                    className).send();
                continue;
            }
            if ((o = ph.get("PoolSize")) != null && o instanceof String){
                size = Integer.parseInt((String) o);
                ph.remove("PoolSize");
            }
            else
                size = poolSize;
            try {
                pool = new GenericPool(className, 0, size, className, null,
                    null, new Object[] {ph}, new Class[]{Map.class});
            }
            catch (Exception e) {
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    className + ": " + Event.traceStack(e)).send();
                continue;
            }
            catch (NoClassDefFoundError e) {
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    className + ": " + Event.traceStack(e)).send();
                continue;
            }
            catch (UnsupportedClassVersionError e) {
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    className + ": " + Event.traceStack(e)).send();
                continue;
            }
            catch (Error e) {
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    className + ": " + Event.traceStack(e)).send();
                Event.flush(e);
            }
            pid = poolList.add(className, new long[]{i, 1, size}, pool);
        }

        retryCount = 0;
    }

    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        long count = 0;
        resetStatus(PSTR_READY, PSTR_RUNNING);

        for (;;) {
            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                count += accept(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == PSTR_PAUSE) {
                if (status > PSTR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > PSTR_PAUSE)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                status == PSTR_STANDBY) {
                if (status > PSTR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > PSTR_STANDBY)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
            if (status == PSTR_READY) {
                setStatus(PSTR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);
        stopAll();

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    @SuppressWarnings("unchecked")
    private long accept(XQueue xq, int baseTime) {
        ObjectMessage outMessage;
        Map<String, Object> props;
        Map req, ph;
        GenericPool pool;
        MessagePersister msgPstr;
        XQueue out;
        Thread thr;
        Object[] asset;
        Object o;
        byte[] buffer;
        int pid;    // pool id
        int tid;    // thread id
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String uriStr, className;
        long[] state, poolInfo;
        long currentTime, previousTime, count = 0;
        int cid, i = 0, n = 0, mask;
        int dmask = MessageUtils.SHOW_BODY | MessageUtils.SHOW_SIZE |
            MessageUtils.SHOW_DST;
        dmask ^= displayMask;
        dmask &= displayMask;

        buffer = new byte[bufferSize];

        if (baseTime <= 0)
            baseTime = pauseTime;

        currentTime = System.currentTimeMillis();
        previousTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // disabled temporarily
                break;
            if ((cid = xq.getNextCell(waitTime)) < 0) {
                if (++n > 10) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - previousTime >= heartbeat) {
                        if (assetList.size() > 0)
                            monitor(currentTime);
                        previousTime = currentTime;
                    }
                    n = 0;
                }
                continue;
            }
            n = 0;

            o = xq.browse(cid);

            if (o == null || !(o instanceof ObjectMessage)) {
                xq.remove(cid);
                new Event(Event.WARNING, "dropped a non-object msg from " +
                    xq.getName()).send();
                continue;
            }
            outMessage = (ObjectMessage) o;

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    xq.remove(cid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    continue;
                }
            }
            catch (Exception e) {
                xq.remove(cid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                continue;
            }

            if (dmask > 0) try {
                String line = MessageUtils.display(outMessage, null,
                    dmask, propertyName);
                new Event(Event.INFO, uri + ": received a request from " +
                    linkName + " with (" + line + " )").send();
            }
            catch (Exception e) {
            }

            try {
                o = outMessage.getObject();
            }
            catch (JMSException e) {
            }

            if (o == null || !(o instanceof Map)) {
                xq.remove(cid);
                new Event(Event.WARNING, "dropped a null or bad msg from " +
                    xq.getName()).send();
                continue;
            }
            else
                req = (Map) o;

            out = (XQueue) req.get("XQueue");
            ph = (Map) req.get("Properties");
            if (ph == null)
                props = new HashMap<String, Object>();
            else { // with default properties
                props = Utils.cloneProperties(ph);
                if ((displayMask & MessageUtils.SHOW_DST) > 0)
                    new Event(Event.INFO, uri+": received a request from "+
                    linkName + " with: "+ JSON2Map.toJSON(ph)).send();
            }

            uriStr = null;
            try {
                uriStr = MessageUtils.getProperty(uriField, outMessage);
                if (uriStr == null) {
                    if (template != null)
                        uriStr = MessageUtils.format(outMessage, buffer,
                            template);
                    else
                        uriStr = MessageUtils.processBody(outMessage, buffer);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (uriStr == null || uriStr.length() <= 0) {
                xq.remove(cid);
                new Event(Event.WARNING, uri + ": failed to get " +
                    "URI from msg").send();
                continue;
            }
            else if ((className = lookup(uriStr, props)) != null) {
                if ((pid = poolList.getID(className)) >= 0) {
                    pool = (GenericPool) poolList.get(pid);
                    o = pool.getInitArg(0);
                    if (o != null && o instanceof Map) { // copy defaults
                        String key;
                        ph = (Map) o;
                        Iterator iter = ph.keySet().iterator();
                        while (iter.hasNext()) {
                            key = (String) iter.next();
                            if (key == null || key.length() <= 0)
                                continue;
                            if (!props.containsKey(key))
                                props.put(key, ph.get(key));
                            else if ((o = props.get(key)) == null)
                                props.put(key, ph.get(key));
                            else if (o instanceof String &&
                                ((String) o).length() <= 0)
                                props.put(key, ph.get(key));
                        }
                    }
                    if ((displayMask & MessageUtils.SHOW_SIZE) > 0)
                        new Event(Event.INFO, uri + ": found persister pool " +
                            pid + " of " + className + " with " +
                            (String)props.get("URI") + " for " + uriStr).send();
                }
                else {
                    pool = new GenericPool(className, 0, poolSize, className,
                        null,null, new Object[]{props}, new Class[]{Map.class});
                    pid= poolList.add(className, new long[]{0,1,poolSize},pool);
                    if ((displayMask & MessageUtils.SHOW_SIZE) > 0)
                        new Event(Event.INFO, uri+": created a persister pool "+
                            pid + " of " + className + " with " +
                            (String)props.get("URI") + " for " + uriStr).send();
                }
            }
            else {
                xq.remove(cid);
                new Event(Event.WARNING, uri + ": no support on " +
                    uriStr).send();
                continue;
            }

            if (props != null) {
                props.put("LinkName", xq.getName());
                props.put("Capacity", String.valueOf(xq.getCapacity()));
                o = pool.checkout(waitTime, new Object[] {props});
            }
            else
                o = pool.checkout(waitTime);

            if (o == null) {
                xq.remove(cid);
                poolInfo = poolList.getMetaData(className);
                new Event(Event.ERR, uri +": failed to checkout persister from"+
                    " the pool of "+ pool.getName()+": "+ pool.getSize() +
                    "/" + poolInfo[2]).send();
                continue;
            }
            else if (o instanceof Exception) {
                xq.remove(cid);
                new Event(Event.ERR, uri +": failed to checkout persister from"+
                    " the pool of "+ pool.getName()+ "/" + pool.getSize() +
                    ": " + Event.traceStack((Exception) o)).send();
                continue;
            }

            msgPstr = (MessagePersister) o;

            state = new long[OUT_TIME + 1];
            asset = new Object[ASSET_OBJ + 1];
            tid = assetList.add(out.getName(), state, asset);
            state[OUT_TID] = tid;
            state[OUT_PID] = pid;
            state[OUT_TTL] = 0;
            state[OUT_RETRY] = 0;
            state[OUT_STATUS] = msgPstr.getStatus(); 
            state[OUT_TIME] = currentTime;
            if ((o = props.get("RetryTimeout")) != null) try {
                state[OUT_TTL] = 1000 * Long.parseLong((String) o);
            }
            catch (Exception e) {
            }
            thr = new Thread(this, linkName + "_" + tid);
            thr.setPriority(Thread.NORM_PRIORITY);
            asset[ASSET_XQ] = out;
            asset[ASSET_URI] = uriStr;
            asset[ASSET_THR] = thr;
            asset[ASSET_OBJ] = msgPstr;
            req.clear();
            if ((o = props.get("MaxIdleTime")) != null)
                req.put("MaxIdleTime", o);
            req.put("Thread", thr);
            thr.start();
            xq.remove(cid);
            if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                new Event(Event.INFO, uri + ": checked out a new persister of "+
                    className + " and it is running with " + pid + ":" + tid +
                    " for " + uriStr).send();
            count ++;
            outMessage = null;
        }

        return count;
    }

    /**
     * pauses all XQs of active persisters and stop all persisters
     */
    private void stopAll() {
        Object[] asset;
        XQueue xq;
        Thread thr;
        int[] list;
        int i, n, tid;

        setStatus(PSTR_STOPPED);
        n = assetList.size();
        if (n <= 0)
            return;

        list = new int[n];
        n = assetList.queryIDs(list);
        for (i=0; i<n; i++) {
            tid = list[i];
            asset = (Object[]) assetList.get(tid);
            if (asset == null || asset.length <= ASSET_OBJ)
                continue;
            xq = (XQueue) asset[ASSET_XQ];
            thr = (Thread) asset[ASSET_THR];
            MessageUtils.pause(xq);
            checkin(thr);
            cleanup(thr);
        }
        assetList.clear();
    }

    /**
     * scans all active persisters and updates their status.  It returns
     * the number of active persisters.
     */
    private int monitor(long currentTime) {
        Object o;
        Object[] asset;
        Thread thr;
        MessagePersister msgPstr;
        XQueue xq;
        GenericPool pool;
        String uriStr;
        Map<String, Object> props;
        long[] state;
        int[] list;
        int i, n, status, pid, tid;
        n = assetList.size();
        if (n <= 0)
            return 0;

        list = new int[n];
        n = assetList.queryIDs(list);
        for (i=0; i<n; i++) {
            tid = list[i];
            o = assetList.get(tid);
            if (o == null || !(o instanceof Object[])) {
                assetList.remove(tid);
                continue;
            }
            asset = (Object[]) o;
            thr = (Thread) asset[ASSET_THR];
            msgPstr = (MessagePersister) asset[ASSET_OBJ];
            xq = (XQueue) asset[ASSET_XQ];
            state = assetList.getMetaData(tid);
            pid = (int) state[OUT_PID];
            pool = (GenericPool) poolList.get(pid);
            status = msgPstr.getStatus();
            switch (status) {
              case PSTR_CLOSED:
                if (isStopped(xq)) {
                    checkin(thr);
                    if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                        new Event(Event.INFO, uri + ": the persister " + tid +
                            " is checked in to " + pool.getName() + "/" +
                            pool.getSize() + " due to stopped XQ").send();
                    cleanup(thr);
                    break;
                }
                else
                    pool.checkin(msgPstr, GenericPool.POOL_CLOSED);
                uriStr = (String) asset[ASSET_URI];
                props = new HashMap<String, Object>();
                lookup(uriStr, props);
                o = pool.getInitArg(0);
                if (o != null && o instanceof Map) { // copy defaults
                    String key;
                    Map ph = (Map) o;
                    Iterator iter = ph.keySet().iterator();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        if (!props.containsKey(key))
                            props.put(key, ph.get(key));
                    }
                }
                props.put("LinkName", xq.getName());
                props.put("Capacity", String.valueOf(xq.getCapacity()));
                o = pool.checkout(waitTime, new Object[] {props});
                if (o == null) {
                    state[OUT_RETRY] ++;
                    new Event(Event.ERR, uri +": failed to recheckout the " +
                        "persister "+tid+" from the pool of " + pool.getName()+
                        "/" + pool.getSize() + " in " + state[OUT_RETRY] +
                        " retries").send();
                }
                else if (o instanceof Exception) {
                    Exception e = (Exception) o;
                    state[OUT_RETRY] ++;
                    new Event(Event.ERR, uri +": failed to recheckout the " +
                        "persister "+tid+" from the pool of " + pool.getName()+
                        "/" + pool.getSize() + " in " + state[OUT_RETRY] +
                        " retries: " + Event.traceStack(e)).send();
                }
                else if (o instanceof MessagePersister) {
                    msgPstr = (MessagePersister) o;
                    asset[ASSET_OBJ] = msgPstr;
                    if (!thr.isAlive()) // restart the thread
                        thr.start();
                    state[OUT_RETRY] ++;
                    if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                        new Event(Event.INFO,uri+": checked out the persister "+
                            tid + " from the pool of " + pool.getName() + "/" +
                           pool.getSize()+" at retry "+state[OUT_RETRY]).send();
                    state[OUT_RETRY] = 0L;
                }
                status = msgPstr.getStatus();
                if (maxRetry > 0 && (int) state[OUT_RETRY] >= maxRetry) {
                    checkin(thr);
                    new Event(Event.ERR, uri + ": the persister " + tid +
                        " is checked in to " + pool.getName() + "/" +
                        pool.getSize() + " after "+maxRetry+" retries").send();
                    cleanup(thr);
                }
                break;
              case PSTR_STOPPED:
                checkin(thr);
                if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                    new Event(Event.INFO, uri + ": the persister " + tid +
                        " is checked in to " + pool.getName() + "/" +
                        pool.getSize()).send();
                cleanup(thr);
                break;
              case PSTR_RETRYING:
                if (state[OUT_STATUS] != status)
                    state[OUT_TIME] = currentTime;
                else if (state[OUT_TTL] > 0 &&
                    currentTime - state[OUT_TIME] >= state[OUT_TTL]) {
                    msgPstr.setStatus(PSTR_STOPPED);
                    status = msgPstr.getStatus();
                    state[OUT_TIME] = currentTime;
                    new Event(Event.INFO, uri + ": the persister " + tid +
                        " timed out of retry on " + pool.getName()).send();
                }
                break;
              case PSTR_RUNNING:
              case PSTR_PAUSE:
              case PSTR_STANDBY:
              case PSTR_DISABLED:
                if (state[OUT_STATUS] != status)
                    state[OUT_TIME] = currentTime;
                break;
              case PSTR_READY:
              default:
            }
            state[OUT_STATUS] = status;
        }

        return assetList.size();
    }

    /**
     * checks in the persister and cleans up caches
     */
    private synchronized void checkin(Thread thr) {
        Object o;
        Object[] asset;
        GenericPool pool;
        MessagePersister msgPstr;
        String threadName;
        long[] state;
        int tid = -1;
        if (thr == null)
            return;

        threadName = thr.getName();
        tid = Integer.parseInt(threadName.substring(linkName.length()+1));

        if (tid >= 0 && (o = assetList.get(tid)) != null &&
            o instanceof Object[]) { // found the asset
            asset = (Object[]) o;
            state = assetList.getMetaData(tid);
            msgPstr = (MessagePersister) asset[ASSET_OBJ];
            pool = (GenericPool) poolList.get(msgPstr.getClass().getName());
            pool.checkin(msgPstr, GenericPool.POOL_CLOSED);
            state[OUT_STATUS] = msgPstr.getStatus();
        }
        else if (tid >= 0) {
            assetList.remove(tid);
        }
    }

    /**
     * cleans up the assetList for the thread.
     */
    private void cleanup(Thread thr) {
        Object o;
        Object[] asset;
        String threadName;
        int tid = -1;
        if (thr == null)
            return;

        threadName = thr.getName();
        tid = Integer.parseInt(threadName.substring(linkName.length()+1));

        if (tid >= 0 && (o = assetList.get(tid)) != null &&
            o instanceof Object[]) { // found the asset
            asset = (Object[]) o;
            asset[ASSET_THR] = null;
            asset[ASSET_XQ] = null;
            o = asset[ASSET_OBJ];
            asset[ASSET_OBJ] = null;
            assetList.remove(tid);
            if (o != null && o instanceof MessagePersister) try {
                ((MessagePersister) o).close();
            }
            catch (Exception e) {
            }
        }
        else if (tid >= 0) {
            assetList.remove(tid);
        }
    }

    /**
     * runs the current thread to persist messages on a given XQueue.
     * If it exits abnormally, it will check-in the persister and set
     * the asset status to PSTR_CLOSED.  Otherwise, the status will be
     * set to PSTR_STOPPED to indicate the asset needs to be cleaned up.
     */
    public void run() {
        String threadName = null;
        MessagePersister msgPstr = null;
        GenericPool pool = null;
        Object o;
        Object[] asset;
        XQueue xq = null;
        int tid = -1;

        threadName = Thread.currentThread().getName();
        tid = Integer.parseInt(threadName.substring(linkName.length()+1));

        if (tid >= 0 && (o = assetList.get(tid)) != null &&
            o instanceof Object[]) { // found the asset
            asset = (Object[]) o;
            msgPstr = (MessagePersister) asset[ASSET_OBJ];
            xq = (XQueue) asset[ASSET_XQ];

            new Event(Event.INFO, uri + ": " + msgPstr.getName() +
                " started on " + xq.getName()).send();
            try {
                msgPstr.persist(xq, 0);
                new Event(Event.INFO, uri + ": " + msgPstr.getName() +
                    " stopped on " + xq.getName()).send();
            }
            catch(Exception e) {
                pool = (GenericPool) poolList.get(msgPstr.getClass().getName());
                new Event(Event.ERR, uri + ": " + msgPstr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    Event.traceStack(e)).send();
                pool.checkin(msgPstr, GenericPool.POOL_CLOSED);
                msgPstr.setStatus(PSTR_CLOSED);
            }
            catch(Error e) {
                pool = (GenericPool) poolList.get(msgPstr.getClass().getName());
                new Event(Event.ERR, uri + ": " + msgPstr.getName() +
                    " stopped on " + xq.getName() + " abnormally: " +
                    e.toString()).send();
                pool.checkin(msgPstr, GenericPool.POOL_CLOSED);
                msgPstr.setStatus(PSTR_CLOSED);
                Event.flush(e);
            }
            if (msgPstr.getStatus() != PSTR_CLOSED) {
                msgPstr.setStatus(PSTR_STOPPED);
            }
        }
    }

    /**
     * returns className and fill up parameters to props or null on failure
     */
    public static String lookup(String uriStr, Map<String, Object> props) {
        int i;

        if (uriStr == null || uriStr.length() <= 0 || props == null)
            return null;

        if ((i = uriStr.indexOf("&URI=&")) > 0) { // escape on '?'
            split("&", "=", uriStr.substring(i+6), props);
            props.put("URI", uriStr.substring(0, i));
        }
        else if ((i = uriStr.indexOf("?")) > 0) {
            split("&", "=", uriStr.substring(i+1), props);
            props.put("URI", uriStr.substring(0, i));
        }
        else {
            props.put("URI", uriStr);
        }

        try {
            mapURI(uriStr, props);
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to map " + uriStr + ": " +
                Event.traceStack(e)).send();
            return null;
        }

        return (String) props.get("ClassName");
    }

    /**
     * splits the text with the pair delimiter of s and and key delimiter of e
     * and fills up the key-value pair to Map
     */
    public static int split(String s, String e, String text,
        Map<String, Object> attr) {
        int len, i, j = 0, k, l, n = 0;
        String key, value;

        if (s == null || e == null || text == null || attr == null)
            return 0;
        else if ((i = text.indexOf(s)) < 0)
            i = text.length();

        len = s.length();
        l = e.length();
        while (i >= j) {
            key = text.substring(j, i);
            if (key != null && (k = key.indexOf(e)) >= 0) {
                value = key.substring(k+l);
                key = key.substring(0, k);
                attr.put(key, value);
                n++;
            }
            j = i + len;
            i = text.indexOf(s, j);
            if (i < 0)
                i = text.length();
        }

        return n;
    }

    /**
     * It is a utility to parse URI and update property Map to set some
     * mandatory properties, like ClassName, for persisters.
     */
    @SuppressWarnings("unchecked")
    public static String mapURI(String uri, Map props)
        throws URISyntaxException {
        String hostName, qName, path, scheme, connectionFactoryName;
        Object o;
        int port;

        if (props == null || props.isEmpty())
            return null;

        if (uri == null) { // default is wmq or file
            hostName = (String) props.get("HostName");
            qName = (String) props.get("QueueName");
            if (qName == null && props.get("TopicName") == null) {
                return null;
            }
            else if ((o = props.get("OutputFile")) != null) {
                uri = "file://" + (String) o;
                if (qName != null)
                    props.remove("QueueName");
            }
            else if (qName != null && "-".equals(qName)) { // file
                uri = "file://-";
                props.remove("QueueName");
            }
            else if (hostName != null && hostName.length() > 0) {
                uri = "wmq://" + hostName;
                if (props.get("Port") != null)
                    uri += ":" + (String) props.get("Port");
            }
            else {
                uri = "wmq://";
                if (props.get("QueueManager") != null)
                    uri += "/" + props.get("QueueManager");
                else
                    uri += "/";
            }
            props.put("URI", uri);
        }

        URI u = new URI(uri);

        scheme = u.getScheme();
        if (scheme == null)
            scheme = "wmq";
        else if ("service".equals(scheme)) {
            if (props.get("ClassName") == null)
                props.put("ClassName","org.qbroker.persister.JMXPersister");
            return "service";
        }

        hostName = u.getHost();
        path = u.getPath();
        port = u.getPort();

        connectionFactoryName = (String) props.get("ConnectionFactoryName");
        if ("wmq".equals(scheme)) {
            if (path != null && path.length() > 1)
                props.put("QueueManager", path.substring(1));

            if (hostName != null && hostName.length() > 0) {
                props.put("HostName", hostName);
                if (port > 0)
                    props.put("Port", String.valueOf(port));

                if (connectionFactoryName != null) {
                    props.put("ChannelName", connectionFactoryName);
                    props.remove("ConnectionFactoryName");
                }
            }

            if (props.get("ContextFactory") != null)
                props.remove("ContextFactory");

            if (props.get("ClassName") == null)
                props.put("ClassName", "org.qbroker.persister.JMSPersister");
        }
        else if ("file".equals(scheme)) {
            if (path == null || path.length() == 0) {
                if (props.get("ClassName") == null) {
                    props.put("ClassName",
                        "org.qbroker.persister.StreamPersister");
                    if (props.get("Operation") == null)
                        props.put("Operation", "write");
                }
            }
            else if (connectionFactoryName == null) { // file
                o = props.get("Operation");
                if ("download".equals((String) o) || "store".equals((String) o))
                    props.put("Logfile", path);
                if (props.get("ClassName") == null) {
                    if ("download".equals((String) o) ||
                        "store".equals((String) o)) {
                        props.put("ClassName",
                            "org.qbroker.persister.FilePersister");
                    }
                    else {
                        props.put("ClassName",
                            "org.qbroker.persister.StreamPersister");
                        if (props.get("Operation") == null)
                            props.put("Operation", "write");
                    }
                }
                if (props.get("ContextFactory") != null)
                    props.remove("ContextFactory");
            }
            else { // jndi
                if (props.get("ContextFactory") == null)
                    props.put("ContextFactory",
                        "com.sun.jndi.fscontext.RefFSContextFactory");
                if (props.get("ClassName") == null) {
                    if (props.get("TopicName") != null)
                        props.put("ClassName",
                            "org.qbroker.persister.JMSPublisher");
                    else
                        props.put("ClassName",
                            "org.qbroker.persister.JMSPersister");
                }
            }
        }
        else if ("t3".equals(scheme)) {
            if (hostName != null && hostName.length() > 0)
                props.put("HostName", hostName);
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory",
                    "weblogic.jndi.WLInitialContextFactory");
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null)
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                else
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
            }
            if (props.get("Principal") == null) {
                o = props.get("Username");
                props.put("Principal", o);
                props.remove("Username");
            }
            if (props.get("Credentials") == null) {
                o = props.get("Password");
                props.put("Credentials", o);
                props.remove("Password");
            }
        }
        else if ("ldap".equals(scheme)) {
            if (hostName != null && hostName.length() > 0)
                props.put("HostName", hostName);
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null)
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                else
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
            }
            if (props.get("Principal") == null) {
                o = props.get("Username");
                props.put("Principal", o);
                props.remove("Username");
            }
            if (props.get("Credentials") == null) {
                o = props.get("Password");
                props.put("Credentials", o);
                props.remove("Password");
            }
        }
        else if ("iiop".equals(scheme)) {
            String str = "com.sun.enterprise.naming.SerialInitContextFactory";
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory", str);
            if (str.equals((String) props.get("ContextFactory"))) {
                if (!props.containsKey("URLPkgs"))
                    props.put("URLPkgs", "com.sun.enterprise.naming");
                if (!props.containsKey("StateFactories"))
                    props.put("StateFactories",
                 "com.sun.corba.ee.impl.presentation.rmi.JNDIStateFactoryImpl");
            }
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null)
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                else
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
            }
            if (props.get("Principal") == null) {
                o = props.get("Username");
                props.put("Principal", o);
                props.remove("Username");
            }
            if (props.get("Credentials") == null) {
                o = props.get("Password");
                props.put("Credentials", o);
                props.remove("Password");
            }
        }
        else if ("jnp".equals(scheme)) {
            String str;
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory",
                    "org.jnp.interfaces.NamingContextFactory");
            if (props.get("URLPkgs") == null)
                props.put("URLPkgs", "org.jboss.naming");
            str = (String) props.get("ContextFactory");
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null)
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                else
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
            }
            if (props.get("Principal") == null) {
                o = props.get("Username");
                props.put("Principal", o);
                props.remove("Username");
            }
            if (props.get("Credentials") == null) {
                o = props.get("Password");
                props.put("Credentials", o);
                props.remove("Password");
            }
        }
        else if ("log".equals(scheme)) {
            if (path != null && path.length() > 0)
                props.put("Logfile", path);
            if (props.get("ClassName") == null) {
                props.put("ClassName", "org.qbroker.persister.LogPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "append");
            }
        }
        else if ("comm".equals(scheme)) {
            if (path != null && path.length() > 0)
                props.put("Device", path);
            if (props.get("ClassName") == null) {
               props.put("ClassName","org.qbroker.persister.StreamPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "write");
            }
        }
        else if ("tcp".equals(scheme) && connectionFactoryName != null) {
            String str;
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory",
                    "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            str = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
            if (str.equals((String) props.get("ContextFactory"))) {
                if (!props.containsKey("IsPhysical"))
                    props.put("IsPhysical", "true");
            }
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null) {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                    if (props.get("Operation") == null)
                        props.put("Operation", "pub");
                }
                else {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
                    if (props.get("Operation") == null)
                        props.put("Operation", "put");
                }
            }
        }
        else if ("tcp".equals(scheme)) {
            if (hostName != null && hostName.length() > 0)
                props.put("HostName", hostName);
            if (port > 0)
                props.put("Port", String.valueOf(port));
            if (props.get("ClassName") == null) {
                o = props.get("Operation");
                if (o != null && ("provide".equals((String) o) ||
                   "publish".equals((String) o))) {
                 props.put("ClassName","org.qbroker.persister.ServerPersister");
                }
                else {
                 props.put("ClassName","org.qbroker.persister.StreamPersister");
                    if (o == null)
                        props.put("Operation", "write");
                }
            }
        }
        else if ("udp".equals(scheme)) {
            if (props.get("ClassName") == null) {
                props.put("ClassName","org.qbroker.persister.PacketPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "talk");
            }
        }
        else if ("jdbc".equals(scheme)) {
            qName = (String) props.get("QueueName");
            if (props.get("ClassName") == null && qName != null) {
                props.put("ClassName", "org.qbroker.persister.JMSPersister");
            }
            else if (props.get("ClassName") == null && qName == null) {
                props.put("ClassName", "org.qbroker.persister.JDBCPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "query");
            }
            if (props.get("DBDriver") == null) {
                String ssp = u.getSchemeSpecificPart();
                if (ssp == null)
                    return scheme;
                else if (ssp.startsWith("oracle"))
                    props.put("DBDriver", "oracle.jdbc.driver.OracleDriver");
                else if (ssp.startsWith("mysql"))
                    props.put("DBDriver", "com.mysql.jdbc.Driver");
                else if (ssp.startsWith("postgresql"))
                    props.put("DBDriver", "org.postgresql.Driver");
                else if (ssp.startsWith("microsoft"))
                    props.put("DBDriver",
                        "com.microsoft.jdbc.sqlserver.SQLServerDriver");
                else if (ssp.startsWith("db2"))
                    props.put("DBDriver", "com.ibm.db2.jcc.DB2Driver");
            }
        }
        else if ("amqp".equals(scheme) && connectionFactoryName != null) {
            String str;
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory",
                    "org.qbroker.jndi.WrapperInitCtxFactory");
            str = "org.qbroker.jndi.WrapperInitCtxFactory";
            if (str.equals((String) props.get("ContextFactory"))) {
                if (!props.containsKey("IsPhysical"))
                    props.put("IsPhysical", "true");
            }
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null) {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                    if (props.get("Operation") == null)
                        props.put("Operation", "pub");
                }
                else {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
                    if (props.get("Operation") == null)
                        props.put("Operation", "put");
                }
            }
        }
        else if ("amqp".equals(scheme)) { // for RabbitMQ
            if ((o = props.get("Operation")) != null &&
                "display".equals((String) o)) {
                props.put("ClassName", "org.qbroker.persister.JMXPersister");
            }
            else {
                props.put("ClassName", "org.qbroker.persister.RMQPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "pub");
            }
        }
        else if ("redis".equals(scheme)) {
            if (props.get("ClassName") == null) {
                props.put("ClassName",
                    "org.qbroker.persister.RedisPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "query");
            }
        }
        else if ("mongodb".equals(scheme)) {
            if (props.get("ClassName") == null) {
                props.put("ClassName",
                    "org.qbroker.persister.DocumentPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "find");
            }
        }
        else if ("riak".equals(scheme)) {
            if (props.get("ClassName") == null) {
                props.put("ClassName",
                    "org.qbroker.persister.DocumentPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "store");
            }
        }
        else if ("qpid".equals(scheme)) { // hack for qpid
            // recover to path
            props.put("URI", uri.substring(6));
            if (props.get("ClassName") == null) {
                if (!props.containsKey("IsPhysical"))
                    props.put("IsPhysical", "true");
                props.put("ClassName",
                    "org.qbroker.persister.JMSPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "put");
            }
        }
        else if ("ftp".equals(scheme) || "ftps".equals(scheme) ||
            "sftp".equals(scheme)) {
            if (port > 0)
                props.put("Port", String.valueOf(port));

            if (hostName != null && hostName.length() > 0)
                props.put("HostName", hostName);
            if (props.get("ClassName") == null) {
                String str = (String) props.get("Operation");
                if ("upload".equals(str))
                    str = "org.qbroker.persister.JobPersister";
                else
                    str = "org.qbroker.persister.FilePersister";
                props.put("ClassName", str);
                if (props.get("Operation") == null)
                    props.put("Operation", "store");
            }
        }
        else if ("nntp".equals(scheme)) {
            if (port > 0)
                props.put("Port", String.valueOf(port));

            if (props.get("ClassName") == null) {
                props.put("ClassName", "org.qbroker.persister.FilePersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "post");
            }
        }
        else if ("http".equals(scheme) || "https".equals(scheme)) {
            if (port > 0)
                props.put("Port", String.valueOf(port));

            if (hostName != null && hostName.length() > 0)
                props.put("HostName", hostName);
            if (props.get("ClassName") == null) {
                props.put("ClassName", "org.qbroker.persister.FilePersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "store");
            }
        }
        else if ("snmp".equals(scheme)) {
            if (props.get("ClassName") == null) {
               props.put("ClassName","org.qbroker.persister.PacketPersister");
                if (props.get("Operation") == null)
                    props.put("Operation", "send");
            }
        }
        else if ("pcf".equals(scheme)) {
            if (path != null && path.length() > 1)
                props.put("QueueManager", path.substring(1));

            if (hostName != null && hostName.length() > 0) {
                props.put("HostName", hostName);
                if (port > 0)
                    props.put("Port", String.valueOf(port));

                if (connectionFactoryName != null) {
                    props.put("ChannelName", connectionFactoryName);
                    props.remove("ConnectionFactoryName");
                }
            }

            if (props.get("ContextFactory") != null)
                props.remove("ContextFactory");

            if (props.get("ClassName") == null)
                props.put("ClassName", "org.qbroker.persister.JMXPersister");
        }
        else if ("jmx".equals(scheme)) {
            if (props.get("ContextFactory") != null)
                props.remove("ContextFactory");

            if (props.get("ClassName") == null)
                props.put("ClassName", "org.qbroker.persister.JMXPersister");
        }
        else if ("imq".equals(scheme) && connectionFactoryName != null) {
            String str;
            if (props.get("ContextFactory") == null)
                props.put("ContextFactory",
                    "org.qbroker.jndi.WrapperInitCtxFactory");
            str = "org.qbroker.jndi.WrapperInitCtxFactory";
            if (str.equals((String) props.get("ContextFactory"))) {
                if (!props.containsKey("IsPhysical"))
                    props.put("IsPhysical", "true");
            }
            str = (String) props.get("URI");
            if (!uri.equals(str)) { // truncated by the pool so recover the uri
                props.put("URI", uri);
                if (props.containsKey("qcf"))
                    props.remove("qcf");
                if (props.containsKey("tcf"))
                    props.remove("tcf");
            }
            if (props.get("ClassName") == null) {
                if (props.get("TopicName") != null) {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPublisher");
                    if (props.get("Operation") == null)
                        props.put("Operation", "pub");
                }
                else {
                    props.put("ClassName",
                        "org.qbroker.persister.JMSPersister");
                    if (props.get("Operation") == null)
                        props.put("Operation", "put");
                }
            }
        }
        else if ("imq".equals(scheme)) {
            if (props.get("ContextFactory") != null)
                props.remove("ContextFactory");

            if (props.get("ClassName") == null)
                props.put("ClassName", "org.qbroker.persister.JMXPersister");
        }

        return scheme;
    }

    public void close() {
        stopAll();
        assetList.clear();
        poolList.clear();
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
    }

    protected void finalize() {
        close();
    }
}
