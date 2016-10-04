package org.qbroker.node;

/* Node.java - an abstract class with parital implemention of MessageNode */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.XQCallbackWrapper;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.MessageNode;
import org.qbroker.event.Event;

/**
 * Node is an abstract class of MessageNode.  It has two abstract methods,
 * propagate() and initRuleset().  Other methods can be overridden if it is
 * necessary.
 *<br/><br/>
 * There are 3 metadata lists for tracking the internal state and the stats of
 * the node.  The first one is msgList for the internal state of all the
 * outstanding messages in the uplink of the node.  An outstanding message is
 * the message still in process and not committed from the internal queue yet.
 * In most scenarios, if a message is outstanding for a long time, it means
 * the down stream of the message flow gets stuck somehow.
 *<br/><br/>
 * The next list is assetList for all the outlinks.  The metadata field of
 * OUT_STATUS is for the status of the outlink.  OUT_SIZE is for the number
 * of outstanding messages on the outlink. OUT_COUNT is for the total number
 * of committed messages on the outlink since the reset.  OUT_TIME is for
 * the time of the latest change on the metadata of the outlink.
 *<br/><br/>
 * The last one is ruleList for all the rulesets of the node.  The metadata
 * field of RULE_STATUS is for the status of the ruleset.  RULE_SIZE is for
 * the number of outstanding messages with the ruleset.  RULE_COUNT is for the
 * total number of committed messages since the reset.  RULE_PEND is for the
 * number of cached or pending messages, depending on the function of the node.
 * RULE_TIME is for the time of the latest change on the metadata.
 *<br/><br/>
 * There is also a configuration list for generating dynamic rulesets. It
 * manages a list of configuration properties for rulesets. So the node is able
 * to download or monitor the properties from certain external data sources.
 *<br/><br/>
 * By default, the interanl XQueue is not supported.  In case the node supports
 * dynamic XQueues, please make sure to overwrite the methods of
 * internalXQSupported() and takebackEnabled().
 *<br/>
 * @author yannanlu@yahoo.com
 */

public abstract class Node implements MessageNode {
    protected String name;
    protected String operation = "propagate";
    protected String linkName = null;
    protected int xaMode = 1;
    protected int capacity = 1;
    protected int displayMask = 0;
    protected int debug = 0;
    protected int status = NODE_READY;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected long waitTime = DEFAULT_WAITTIME;
    protected long chkptTimeout = DEFAULT_CHKPT_TIMEOUT;

    protected String[] displayPropertyName = null;

    // stateInfo of outstanding msgs in upLink
    protected AssetList msgList = null;
    // list for all outLinks with the outInfo keyed by outLink name
    protected AssetList assetList = null;
    // list for all rules and the ruleInfo keyed by rule name
    protected AssetList ruleList = null;
    // list for all ConfigLists for external rules
    protected AssetList cfgList = null;
    // cells for tracking outstading msgs
    protected CollectibleCells cells = null;
    protected XQCallbackWrapper cbw = null;
    protected java.lang.reflect.Method callbackMethod = null;

    private final static int DEFAULT_BUFFER_SIZE = 4096;
    private final static long DEFAULT_WAITTIME = 50L;
    private final static long DEFAULT_CHKPT_TIMEOUT = 0L;

    // for debugging
    protected final static int DEBUG_INIT = 1;
    protected final static int DEBUG_REPT = 2;
    protected final static int DEBUG_PROP = 4;
    protected final static int DEBUG_UPDT = 8;
    protected final static int DEBUG_COLL = 16;
    protected final static int DEBUG_PASS = 32;
    protected final static int DEBUG_FBAK = 64;

    // node type
    protected final static int TYPE_NONE = -1;
    protected final static int TYPE_ROUNDROBIN = -2;
    protected final static int TYPE_WEIGHTED = -3;
    protected final static int TYPE_BYPASS = -4;
    protected final static int TYPE_SCREEN = -5;
    protected final static int TYPE_AGGREGATE = -6;
    protected final static int TYPE_PARSER = -7;
    protected final static int TYPE_XSLT = -8;
    protected final static int TYPE_COLLECT = -9;
    protected final static int TYPE_SPLIT = -10;
    protected final static int TYPE_FORMAT = -11;
    protected final static int TYPE_DELIVER = -12;
    protected final static int TYPE_SORT = -13;
    protected final static int TYPE_ACTION = -14;
    protected final static int TYPE_XPATH = -15;
    protected final static int TYPE_PICKUP = -16;
    protected final static int TYPE_SPREAD = -17;
    protected final static int TYPE_CACHE = -18;
    protected final static int TYPE_PUBLISH = -19;
    protected final static int TYPE_SERVICE = -20;
    protected final static int TYPE_PIPE = -21;
    protected final static int TYPE_JSONPATH = -22;
    protected final static int TYPE_JSONT = -23;
    protected final static int TYPE_MAPREDUCE = -24;
    protected final static int TYPE_SCRIPT = -25;

    // for load level
    protected final static int LOAD_LOW = 0;
    protected final static int LOAD_MEDIUM = 1;
    protected final static int LOAD_HIGH = 2;

    // for reset
    protected final static int RESET_NONE = MessageFilter.RESET_NONE;
    protected final static int RESET_SOME = MessageFilter.RESET_SOME;
    protected final static int RESET_ALL = MessageFilter.RESET_ALL;
    protected final static int RESET_MAP = MessageFilter.RESET_MAP;

    public Node(Map props) {
        Object o;
        String key;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        if ((o = props.get("LinkName")) != null)
            linkName = (String) o;
        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        if ((o = props.get("WaitTime")) != null) {
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 50L;
        }
        if ((o = props.get("CheckpointTimeout")) != null &&
            (chkptTimeout = 1000 * Integer.parseInt((String) o)) < 0)
            chkptTimeout = 0;

        if ((o = props.get("Capacity")) == null ||
            (capacity = Integer.parseInt((String) o)) <= 0)
            capacity = 1;

        // property name for displaying candidate messages only
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

        try {
            Class<?> cls = this.getClass();
            callbackMethod = cls.getMethod("callback",
                new Class[]{String.class, String.class});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("found no callback method"));
        }
        cbw = new XQCallbackWrapper(name, this, callbackMethod);
    }

    /**
     * It initializes a new ruleset with the ruleInfo and returns the rule upon
     * success.  Otherwise, it throws an exception or returns null.
     */
    protected abstract Map initRuleset(long t, Map ph, long[] ruleInfo);

    /**
     * It picks up messages from the input XQueue and evaluates their content
     * to decide which output XQueues to propagate.
     */
    public abstract void propagate(XQueue in, XQueue[] out) throws JMSException;

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise. In case of cid < 0, the message
     * has nothing to do with the input XQueue. Nothing will be added to
     * msgList for tracking. No update on stats of ruleset either.
     * If tid < 0, msg will not be put back to uplink in case of failure.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo = null;
        int id = -1, k, mid, mask, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            if (cid >= 0) // incoming msg
                k = (tid >= 0) ? in.putback(cid) : -2;
            else
                k = -1;
            new Event(Event.ERR, name + ": asset is null on " +
                assetList.getKey(oid) + " of " + oid + " for " +
                rid + " with msg at " + cid + "," + k).send();
            try {
                Thread.sleep(500);
            }
            catch (Exception e) {
            }
            return 0;
        }
        out = (XQueue) asset[ASSET_XQ];
        outInfo = assetList.getMetaData(oid);
        len = (int) outInfo[OUT_LENGTH];
        switch (len) {
          case 0:
            shift = 0;
            for (k=0; k<1000; k++) { // reserve an empty cell
                id = out.reserve(-1L);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
          case 1:
            shift = (int) outInfo[OUT_OFFSET];
            for (k=0; k<1000; k++) { // reserve an empty cell
                id = out.reserve(-1L, shift);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
          default:
            shift = (int) outInfo[OUT_OFFSET];
            for (k=0; k<1000; k++) { // reserve an empty cell
                id = out.reserve(-1L, shift, len);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
        }
        outCapacity = (int) outInfo[OUT_CAPACITY];

        if (id >= 0 && id < outCapacity) { // id-th cell of out reserved
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[]{cid, oid, id, rid,
                        tid, currentTime}, key, cid);
                else // generated msg
                    mid = -1;
            }
            else { // id-th cell has just been empty now, replace it
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                k = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(k);
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name+" passback: " + k + " " + mid +
                        ":" + state[MSG_CID] + " "+key+" "+ruleInfo[RULE_SIZE]+
                        " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                        out.depth() + " " + msgList.size()).send();
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[] {cid, oid, id, rid,
                        tid, currentTime}, key, cid);
                else // generated msg
                    mid = -1;
            }
            if (cid >= 0 && mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = (tid >= 0) ? in.putback(cid) : -2;
                new Event(Event.ERR, name + " failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size() +","+k).send();
                return 0;
            }
            if (cid >= 0) // incoming msg
                k = out.add(msg, id, cbw);
            else // no need to track on generated msg
                k = out.add(msg, id);
            if (cid >= 0) { // incoming msg
                ruleInfo = ruleList.getMetaData(rid);
                ruleInfo[RULE_SIZE] ++;
                ruleInfo[RULE_TIME] = currentTime;
                outInfo[OUT_SIZE] ++;
                if (outInfo[OUT_STATUS] == NODE_RUNNING)
                    outInfo[OUT_TIME] = currentTime;
            }
            else { // generated msg
                outInfo[OUT_COUNT] ++;
                outInfo[OUT_TIME] = currentTime;
            }
            if ((debug & DEBUG_PASS) > 0) {
                if (cid < 0)
                    ruleInfo = ruleList.getMetaData(rid);
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ":" + cid + " " + key + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
            }
        }
        else { // reservation failed
            if (cid >= 0) // incoming msg
                k = (tid >= 0) ? in.putback(cid) : -2;
            else // generated msg
                k = -1;
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                assetList.getKey(oid) + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k).send();
            return 0;
        }
        return 1;
    }

    /**
     * It forces to route the msg to oid without putback. It returns 1 upon
     * success, 0 with msg removed from uplink and flushed to the outlink,
     * or -1 with msg dropped.
     */
    protected int forcethru(long currentTime, Message msg, XQueue in, int rid,
        int oid, int cid) {
        if (cid < 0)
            return -2;
        int k = passthru(currentTime, msg, in, rid, oid, cid, -1);
        if (k <= 0) { // force failed
            in.remove(cid);
            k = passthru(currentTime, msg, in, rid, oid, -1, 0) - 1;
        }
        return k;
    }

    /**
     * It returns the number of done messages removed from the input XQueue.
     * If milliSec < 0, there is no wait and it tries to collect all cells.
     * Otherwise, it just tries to collect only one cell.
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        int mid, rid, oid, id, l = 0;
        long[] state, outInfo, ruleInfo;
        long t;
        StringBuffer strBuf = null;
        if ((debug & DEBUG_FBAK) > 0)
            strBuf = new StringBuffer();

        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null)
                continue;
            out = (XQueue) asset[ASSET_XQ];
            id = (int) state[MSG_BID];
            in.remove(mid);
            msgList.remove(mid);
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = t;
            if ((debug & DEBUG_FBAK) > 0)
                strBuf.append("\n\t" + rid + " " + mid + "/" + state[MSG_CID] +
                    " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth() +
                    " " + msgList.size());
            l ++;
            if (milliSec >= 0) // one collection a time
                break;
        }
        if (l > 0 && (debug & DEBUG_FBAK) > 0)
            new Event(Event.DEBUG, name + " feedback: RID MID/CID OID:ID " +
                "RS OS size|depth ms - " + l + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        return l;
    }

    /**
     * It marks the cell in cells with key and id.
     */
    public void callback(String key, String id) {
        if (key == null || id == null || key.length() <= 0 || id.length() <= 0)
            return;
        if (cells != null && assetList != null && msgList != null) {
            int oid = assetList.getID(key);
            int mid = msgList.getID(oid + "/" + id);
            if (mid >= 0) { // mark the mid is ready to be collected
                cells.take(mid);
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name +": "+ key + " called back on "+
                        mid + " with " + oid + "/" + id).send();
            }
            else {
                new Event(Event.ERR, name +": "+ key+" failed to callback on "+
                    oid + "/" + id).send();
            }
        }
    }

    /**
     * It collects all the delivered messages in the given xq and returns the
     * total number of collected upon success. If the uplink is in standby
     * mode, it stops the collections and returns -1 immediatly.
     */
    protected int collectAll(long currentTime, XQueue in, XQueue out, int oid,
        long[] outInfo) {
        int[] ids;
        int i, k, mask, n = 0;
        int shift = (int) outInfo[OUT_OFFSET];
        int len = (int) outInfo[OUT_LENGTH];
        ids = new int[(int) outInfo[OUT_CAPACITY]];

        switch (len) { // make sure all cells collected
          case 0:
            i = out.collect(-1L);
            k = out.size();
            break;
          case 1:
            i = out.collect(-1L, shift);
            if (out.getCellStatus(shift) == XQueue.CELL_EMPTY)
                k = 0;
            else
                k = 1;
            break;
          default:
            for (i=0; i<ids.length; i++) // init list of ids
                ids[i] = i;
            i = out.collect(-1L, shift, len);
            k = len - out.locate(ids, shift, len);
            break;
        }

        if (i >= 0 || k > 0) { // there is something to collect
            int mid;
            do {
                mid = msgList.getID(oid + "/" + i);
                if (mid >= 0) { // i-th cell has just been empty now
                    cells.collect(-1L, mid);
                    n ++;
                    int id, m;
                    long[] state = msgList.getMetaData(mid);
                    id = (int) state[MSG_CID];
                    m = (int) state[MSG_RID];
                    in.remove(id);
                    msgList.remove(mid);
                    outInfo[OUT_SIZE] --;
                    outInfo[OUT_COUNT] ++;
                    outInfo[OUT_TIME] = currentTime;
                    state = ruleList.getMetaData(m);
                    if (state[RULE_MODE] > 0) {
                        state[RULE_SIZE] --;
                        state[RULE_COUNT] ++;
                        state[RULE_TIME] = currentTime;
                    }
                    if ((debug & DEBUG_FBAK) > 0)
                        new Event(Event.DEBUG, name + " passback: "+ m + " " +
                            mid + "/" + id+ " " + 0 + ":"+ i + " " +
                            state[RULE_SIZE]+ " " + outInfo[OUT_SIZE]+ " " +
                            out.size()+ "|"+ out.depth() +" "+
                            msgList.size()).send();
                }

                mask = in.getGlobalMask();
                if ((mask & XQueue.STANDBY) > 0)// standy temporarily
                    break;

                switch (len) {
                  case 0:
                    i = out.collect(waitTime);
                    k = out.size();
                    break;
                  case 1:
                    i = out.collect(waitTime, shift);
                    if (out.getCellStatus(shift) == XQueue.CELL_EMPTY)
                        k = 0;
                    else
                        k = 1;
                    break;
                  default:
                    for (i=0; i<ids.length; i++) // init list of ids
                        ids[i] = i;
                    i = out.collect(waitTime, shift, len);
                    k = len - out.locate(ids, shift, len);
                    break;
                }
                if (i < 0 && k <= 0) // all collected
                    break;
            } while ((mask & XQueue.KEEP_RUNNING) > 0);

            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                return -1;
        }

        return n;
    }

    /**
     * It updates the parameters of the node with the given property map.
     * Upon success, it returns the number of parameters modified.
     * It is not MT-Safe.
     */
    public int updateParameters(Map ph) {
        Object o;
        int i, n = 0;

        if ((o = ph.get("BufferSize")) != null) {
            i = Integer.parseInt((String) o);
            if (i > 0 && i != bufferSize) {
                bufferSize = i;
                n++;
            }
        }
        else if (bufferSize != DEFAULT_BUFFER_SIZE) { // reset to default
            bufferSize = DEFAULT_BUFFER_SIZE;
            n++;
        }
        if ((o = ph.get("WaitTime")) != null) {
            long t = Long.parseLong((String) o);
            if (t > 0 && t != waitTime) {
                waitTime = t;
                n++;
            }
        }
        else if (waitTime != DEFAULT_WAITTIME) { // reset to default
            waitTime = DEFAULT_WAITTIME;
            n++;
        }
        if ((o = ph.get("CheckpointTimeout")) != null) {
            long t = 1000 * Long.parseLong((String) o);
            if (t >= 0 && t != chkptTimeout) {
                chkptTimeout = t;
                n ++;
            }
        }
        else if (chkptTimeout != DEFAULT_CHKPT_TIMEOUT) { // reset to default
            chkptTimeout = DEFAULT_CHKPT_TIMEOUT;
            n ++;
        }

        return n;
    }

    /**
     * It adds a new rule to the ruleList and returns its id upon success.
     * It is not MT-Safe.
     */
    public int addRule(Map ph) {
        Object o;
        String key = null;
        Map rule = null;
        long[] ruleInfo = new long[RULE_TIME+1];
        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a rule"));
        if (ruleList == null)
            throw(new IllegalStateException(name + ": ruleList is null"));
        if (ruleList.size() >= ruleList.getCapacity())
            throw(new IllegalStateException(name + ": ruleList is full " +
                ruleList.size() +"/" + ruleList.getCapacity()));
        key = (String) ph.get("Name");
        if (key == null || key.length() <= 0 || ruleList.containsKey(key))
            return -1;
        if (getStatus() == NODE_RUNNING)
            throw(new IllegalStateException(name + " is in running state"));
        if ((o = ph.get("RuleType")) == null || !(o instanceof String) ||
            !("ConfigList".equals((String) o) ||
            "ConfigTemplate".equals((String) o))) {
            try {
                rule = initRuleset(System.currentTimeMillis(),
                    ph, ruleInfo);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + " failed to init rule " + key +
                    ": " + Event.traceStack(ex)).send();
                return -1;
            }
            if (rule != null && rule.containsKey("Name")) {
                int id = ruleList.add(key, ruleInfo, rule);
                if (id < 0) // failed to add the rule to the list
                    new Event(Event.ERR, name + " failed to add rule " +
                        key).send();
                return id;
            }
            else
                new Event(Event.ERR, name + " failed to init rule "+key).send();
        }
        else if (cfgList != null && "ConfigTemplate".equals((String) o)) {
            int i, k, m;
            ConfigTemplate cfgTemp;

            try {
                cfgTemp = new ConfigTemplate(ph);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + ": ConfigTemplate rule " + key +
                    " failed to be initialized").send();
                return -1;
            }
            k = cfgTemp.getSize();
            m = cfgList.add(key, new long[]{k, 1}, cfgTemp);
            if (m < 0) {
                new Event(Event.ERR, name + ": ConfigTemplate rule " + key +
                    " failed to be added to the list").send();
                cfgTemp.close();
            }
            else { // added
                String str;
                int id = -1;
                long tm = System.currentTimeMillis();
                for (i=0; i<k; i++) { // init all template rulesets
                    ph = cfgTemp.getProps(cfgTemp.getItem(i));
                    str = cfgTemp.getKey(i);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to init template rule " + str +
                            " at " + i +  ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.containsKey("Name")) {
                        m = ruleList.add(str, ruleInfo, rule);
                        if (m < 0) // failed to add the rule to the list
                            new Event(Event.ERR, name + ": ConfigTemplate " +
                                key + " failed to add template rule "+ str +
                                " at " + i).send();
                        else if (id < 0) // keep the first one
                            id = m;
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to init template rule " + str +
                            " at " + i).send();
                }
                return id;
            }
        }
        else if (cfgList != null) { // for external rules via ConfigList
            int i, k, m;
            ConfigList cfg;
            try {
                cfg = new ConfigList(ph);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + ": ConfigList rule " + key +
                    " failed to be initialized").send();
                return -1;
            }
            cfg.setDataField(name);
            cfg.loadList();
            k = cfg.getSize();
            m = cfgList.add(key, new long[]{k, 0}, cfg);
            if (m < 0) {
                new Event(Event.ERR, name + ": ConfigList rule " + key +
                    " failed to be added to the list").send();
                cfg.close();
            }
            else { // added
                String str;
                int id = -1;
                long tm = System.currentTimeMillis();
                for (i=0; i<k; i++) { // init all external rulesets
                    ph = cfg.getProps(cfg.getItem(i));
                    str = cfg.getKey(i);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigList " + key +
                            " failed to init external rule " + str +
                            " at " + i +  ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.containsKey("Name")) {
                        m = ruleList.add(str, ruleInfo, rule);
                        if (m < 0) // failed to add the rule to the list
                            new Event(Event.ERR, name + ": ConfigList " + key +
                                " failed to add external rule "+ str +
                                " at " + i).send();
                        else if (id < 0) // keep the first one
                            id = m;
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigList " + key +
                            " failed to init external rule "+ str +
                            " at " + i).send();
                }
                return id;
            }
        }

        return -1;
    }

    /**
     * It removes the rule from the ruleList and returns the rule id upon
     * success. It is not MT-Safe.
     */
    public int removeRule(String key, XQueue in) {
        int id = ruleList.getID(key);
        if (id == 0) // can not remove the default rule 
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long[] ruleInfo = ruleList.getMetaData(id);
            if (ruleInfo != null && ruleInfo[RULE_SIZE] > 0) // check integrity
                throw(new IllegalStateException(name+": "+key+" is busy with "+
                    ruleInfo[RULE_SIZE] + " outstangding msgs"));
            ruleList.remove(id);
            return id;
        }
        else if (cfgList != null && (id = cfgList.getID(key)) >= 0) {
            int i, m, n;
            String str;
            Object o;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            o = cfgList.get(id);
            id = -1;
            if (o instanceof ConfigTemplate) {
                ConfigTemplate cfgTemp = (ConfigTemplate) o;
                n = cfgTemp.getSize();
                for (i=0; i<n; i++) {
                    str = cfgTemp.getKey(i);
                    m = removeRule(str, in);
                    if (m < 0) // failed to remove the rule from the list
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to remove template rule "+ str +
                            " at " + i).send();
                    else if (id < 0) // keep the first one
                        id = m;
                }
            }
            else {
                ConfigList cfg = (ConfigList) o;
                n = cfg.getSize();
                for (i=0; i<n; i++) {
                    str = cfg.getKey(i);
                    m = removeRule(str, in);
                    if (m < 0) // failed to remove the rule from the list
                        new Event(Event.ERR, name + ": ConfigList " + key +
                            " failed to remove external rule "+ str +
                            " at " + i).send();
                    else if (id < 0) // keep the first one
                        id = m;
                }
            }
            return id;
        }

        return -1;
    }

    /**
     * It replaces the rule with the new rule defined by the property hashmap
     * and returns its id upon success. It is not MT-Safe.
     */
    public int replaceRule(String key, Map ph, XQueue in) {
        int id = ruleList.getID(key);
        if (id == 0) // can not replace the default rule 
            return -1;
        else if (id > 0) { // for a normal rule
            if (ph == null || ph.size() <= 0)
                throw(new IllegalArgumentException("Empty property for rule"));
            if (!key.equals((String) ph.get("Name"))) {
                new Event(Event.ERR, name + ": name not match for rule " + key +
                    ": " + (String) ph.get("Name")).send();
                return -1;
            }
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long tm = System.currentTimeMillis();
            long[] meta = new long[RULE_TIME+1];
            long[] ruleInfo = ruleList.getMetaData(id);
            Map rule = null;
            try {
                rule = initRuleset(tm, ph, meta);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + " failed to init rule " + key +
                    ": " + Event.traceStack(ex)).send();
                return -1;
            }
            if (rule != null && rule.containsKey("Name")) {
                ruleList.set(id, rule);
                tm = ruleInfo[RULE_PID];
                for (int i=0; i<RULE_TIME; i++) { // update metadata
                    switch (i) {
                      case RULE_SIZE:
                        break;
                      case RULE_PEND:
                      case RULE_COUNT:
                        if (tm == meta[RULE_PID]) // same rule type
                            break;
                      default:
                        ruleInfo[i] = meta[i];
                    }
                }
                return id;
            }
            else
                new Event(Event.ERR, name +" failed to init rule "+key).send();
        }
        else if (cfgList != null && (id = cfgList.getID(key)) >= 0) {
            int n;
            Object o;
            if (ph == null || ph.size() <= 0)
                throw(new IllegalArgumentException("Empty property for rule"));
            if (!key.equals((String) ph.get("Name"))) {
                new Event(Event.ERR, name + ": name not match for rule " + key +
                    ": " + (String) ph.get("Name")).send();
                return -1;
            }
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            o = cfgList.get(id);
            id = -1;
            if (o instanceof ConfigTemplate) {
                ConfigTemplate cfgTemp = (ConfigTemplate) o;
                if (cfgTemp.isPropertyChanged((Map) ph.get("Property"))) {
                    cfgTemp = replaceConfigTemplate(cfgTemp, ph, in);
                }
                else { // there is no change on the property template
                    Map c;
                    try {
                        c = cfgTemp.diff(ph);
                    }
                    catch (Exception e) {
                        c = null;
                        new Event(Event.ERR, name + " Config: diff failed for "+
                            key + ": " + e.toString()).send();
                    }
                    if (c == null) // no change
                        return 0;
                    o = c.get("Item");
                    if (o == null) { // no changes on items
                        if ((o = c.get("Template")) != null) // renamed
                            resetConfigTemplate(cfgTemp, (String) o);
                    }
                    else if (o instanceof String) { // swap the role
                        cfgTemp = replaceConfigTemplate(cfgTemp, ph, in);
                    }
                    else if (o instanceof Map) { // reporter changed
                        cfgTemp.resetReporter((Map) o);
                        refreshRule(key, in);
                        if ((o = c.get("Template")) != null) // renamed
                            resetConfigTemplate(cfgTemp, (String) o);
                    }
                    else if (o instanceof List) { // static list changed
                        o = ph.get("Item"); // for new list of items
                        refreshConfigTemplate(cfgTemp, (List) o, in);
                        if ((o = c.get("Template")) != null) // renamed
                            resetConfigTemplate(cfgTemp, (String) o);
                    }
                }
                n = cfgTemp.getSize();
                for (int i=0; i<n; i++) {
                    id = ruleList.getID(cfgTemp.getKey(i));
                    if (id > 0)
                        break;
                }
            }
            else {
                ConfigList cfg = (ConfigList) o;
                cfg.resetReporter(ph);
                cfg.setDataField(name);
                refreshRule(key, in);
                n = cfg.getSize();
                for (int i=0; i<n; i++) {
                    id = ruleList.getID(cfg.getKey(i));
                    if (id > 0)
                        break;
                }
            }
            return id;
        }

        return -1;
    }

    /**
     * It renames the rule and returns the rule id.
     * It is not MT-Safe.
     */
    public int renameRule(String oldkey, String key) {
        int id = ruleList.getID(oldkey);
        if (id == 0) // can not rename the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            ruleList.replaceKey(id, key);
            return id;
        }
        else if (cfgList != null && (id = cfgList.getID(oldkey)) >= 0) {
            Object o;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            o = cfgList.get(id);
            cfgList.replaceKey(id, key);
            id = -1;
            if (o instanceof ConfigTemplate) {
                ConfigTemplate cfgTemp = (ConfigTemplate) o;
                int n = cfgTemp.getSize();
                for (int i=0; i<n; i++) {
                    id = ruleList.getID(cfgTemp.getKey(i));
                    if (id > 0)
                        break;
                }
            }
            else {
                int[] ids;
                ConfigList cfg = (ConfigList) o;
                int n = cfg.getSize();
                ids = new int[n];
                for (int i=0; i<n; i++)
                    ids[i] = ruleList.getID(cfg.getKey(i));
                cfg.resetTemplate(key);
                for (int i=0; i<n; i++) {
                    if (ids[i] > 0) {
                        ruleList.replaceKey(ids[i], cfg.getKey(i));
                        if (id < 0)
                            id = ids[i];
                    }
                }
            }
            return id;
        }

        return -1;
    }

    /**
     * It moves the rule to the end of the list and returns the id.
     * It is not MT-Safe.
     */
    public int rotateRule(String key) {
        int id = ruleList.getID(key);
        if (id == 0) // can not rotate the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            if (ruleList.rotate(id) == 1)
                return id;
        }
        else if (cfgList != null && (id = cfgList.getID(key)) >= 0) {
            Object o;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            o = cfgList.get(id);
            id = -1;
            if (o instanceof ConfigTemplate) {
                ConfigTemplate cfgTemp = (ConfigTemplate) o;
                int n = cfgTemp.getSize();
                for (int i=0; i<n; i++) {
                    int m = ruleList.getID(cfgTemp.getKey(i));
                    if (m > 0 && ruleList.rotate(m) == 1) {
                        if (id < 0) // keep the first one
                            id = m;
                    }
                }
            }
            else {
                ConfigList cfg = (ConfigList) o;
                int n = cfg.getSize();
                for (int i=0; i<n; i++) {
                    int m = ruleList.getID(cfg.getKey(i));
                    if (m > 0 && ruleList.rotate(m) == 1) {
                        if (id < 0) // keep the first one
                            id = m;
                    }
                }
            }
            return id;
        }

        return -1;
    }

    /**
     * It refrshes the group of dynamic template rules or external rules for
     * a key and returns the number of rules refreshed upon success.
     * It is not MT-Safe.
     */
    public int refreshRule(String key, XQueue in) {
        Object o;
        int id = -1;
        if (cfgList != null)
            id = cfgList.getID(key);
        if (id < 0)
            return -1;
        if (getStatus() == NODE_RUNNING)
            throw(new IllegalStateException(name + " is in running state"));

        o = cfgList.get(id);
        if (o instanceof ConfigTemplate) { // found the template rule
            List list;
            ConfigTemplate cfgTemp = (ConfigTemplate) o;
            if (!cfgTemp.isDynamic())
                return 0;

            try {
                list = cfgTemp.getItemList();
            }
            catch (Exception e) {
                list = null;
                new Event(Event.ERR, name + " failed to get item list for " +
                    key).send();
            }
            if (list == null) // skipped or exceptioned
                return 0;
            else
                return refreshConfigTemplate(cfgTemp, list, in);
        }
        else { // found the external rule
            Object[] keys;
            Map ph, change;
            String str, item;
            int[] ids;
            int i, j, k, m, n, num = 0;
            ConfigList cfg = (ConfigList) o;
            change = cfg.loadList();
            if (change == null || change.size() <= 0) // no change to refresh
                return 0;
            n = ruleList.size();
            ids = new int[n];
            // cache all the ids for rotations later
            n = ruleList.queryIDs(ids);
            m = n;
            for (j=n-1; j>=0; j--) { // look for last item in the list
                str = ruleList.getKey(ids[j]);
                if (cfg.containsKey(str)) { // found the last item
                    // cache the position
                    m = j;
                    break;
                }
            }
            keys = change.keySet().toArray();
            k = keys.length;
            for (i=0; i<k; i++) {
                o = keys[i];
                if (o == null || !(o instanceof String))
                    continue;
                item = (String) o;
                str = cfg.getKey(item);
                if ((o = change.get(key)) == null) try { // null for deletion
                    change.remove(item);
                    id = removeRule(str, in);
                    num ++;
                    new Event(Event.INFO, name + " external rule " + str +
                        " has been removed on " + id + " in refreshing on " +
                        key).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " failed to remove rule "+
                        str + " in refreshing on " + key + ": " +
                        Event.traceStack(e)).send();
                }
            }
            // rotate, reload or add new ones
            k = cfg.getSize();
            for (i=0; i<k; i++) { // loop thru all items
                item = cfg.getItem(i);
                str = cfg.getKey(i);
                if (!change.containsKey(item)) { // existing rule with no change
                    id = ruleList.getID(str);
                    if (id >= 0) { // existing one so rotate
                        ruleList.rotate(id);
                        if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name + " Config: " +
                                str +" has no change for "+ str).send();
                    }
                    else try { // previously failed item
                        ph = cfg.getProps(item);
                        id = addRule(ph);
                        if (id < 0) { // failed again
                            new Event(Event.ERR, name + " failed to recreate " +
                                "external rule " + str + ": " + id).send();
                        }
                        else { // recreated
                            new Event(Event.INFO, name + ": external rule " +
                                str + " has been recreated at " + id).send();
                        }
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name + " failed to recreate " +
                            "external rule " + str + ": " +
                            Event.traceStack(e)).send();
                    }
                    continue;
                }
                else if (ruleList.containsKey(str)) { // existing rule updated
                    ph = cfg.getProps(item);
                    ruleList.rotate(ruleList.getID(str));
                    if (ph != null) {
                        id = replaceRule(str, ph, in);
                        if (id >= 0)
                            num ++;
                        ph.clear();
                    }
                    else {
                        id = -1;
                    }
                    if (id < 0) { // failed to reload
                        new Event(Event.ERR, name + " failed to reload " +
                             "external rule " + str + ": " + id).send();
                    }
                    else { // reloaded
                        new Event(Event.INFO, name + ": external rule " +
                        str + " has been reload at " + id).send();
                    }
                }
                else { // new rule
                    ph = cfg.getProps(item);
                    if (ph != null) {
                        id = addRule(ph);
                        if (id >= 0)
                            num ++;
                        ph.clear();
                    }
                    else {
                        id = -1;
                    }
                    if (id < 0) { // failed to add
                        new Event(Event.ERR, name + " failed to add " +
                             "external rule " + str + ": " + id).send();
                    }
                    else { // reloaded
                        new Event(Event.INFO, name + ": external rule " +
                        str + " has been added at " + id).send();
                    }
                }
            }
            if (k <= 0) {
                cfgList.remove(key);
                cfg.close();
            }
            else { // rotate the rest of the list
                for (j=m+1; j<n; j++)
                    ruleList.rotate(ids[j]);
            }
            return num;
        }
    }

    public boolean containsRule(String key) {
        if (ruleList.containsKey(key))
            return true;
        else if (cfgList != null)
            return cfgList.containsKey(key);
        else
            return false;
    }

    /** resets the name template and returns the number of rules renamed */
    private int resetConfigTemplate(ConfigTemplate cfgTemp, String text) {
        if (cfgTemp == null || text == null)
            return -1;
        String key = cfgTemp.getName();
        int n = cfgTemp.getSize();
        int[] ids = new int[n];
        int k = 0;
        for (int j=0; j<n; j++)
            ids[j] = ruleList.getID(cfgTemp.getKey(j));
        if (cfgTemp.resetTemplate(text)) { // reset
            for (int j=0; j<n; j++) {
                if (ids[j] >= 0) {
                    ruleList.replaceKey(ids[j], cfgTemp.getKey(j));
                    k ++;
                }
            }
        }
        else {
            new Event(Event.ERR, name + " failed to reset name template for "+
                key).send();
            k = -1;
        }
        return k;
    }

    private ConfigTemplate replaceConfigTemplate(ConfigTemplate cfgTemp,
        Map ph, XQueue in) {
        String key, str;
        int j, k, m, n;
        int[] ids;
 
        key = cfgTemp.getName();
        n = ruleList.size();
        ids = new int[n];
        // cache all the ids for rotations later
        n = ruleList.queryIDs(ids);
        m = n;
        for (j=n-1; j>=0; j--) { // look for last item in the list
            str = ruleList.getKey(ids[j]);
            if (cfgTemp.containsKey(str)) { // found the last item
                // cache the position
                m = j;
                break;
            }
        }
        removeRule(key, in);
        cfgList.remove(key);
        cfgTemp.close();
        addRule(ph);
        ConfigTemplate temp = (ConfigTemplate) cfgList.get(key);
        if (temp != null) {
            k = temp.getSize();
            if (k <= 0) { // empty list
                cfgList.remove(key);
                temp.close();
            }
            else { // rotate the rest of the list
                for (j=m+1; j<n; j++)
                    ruleList.rotate(ids[j]);
            }
        }
        return temp;
    }

    /**
     * With the given list of items, it refreshes the rules managed by the
     * config template and returns the number of rules refreshed
     */
    private int refreshConfigTemplate(ConfigTemplate cfgTemp, List list,
        XQueue in) {
        int id, j, k, m, n, num = 0;
        int[] ids;
        String key, str;
        List pl;
        Map ph;

        if (cfgTemp == null)
            return -1;
        if (list == null) // skipped or exceptioned
            return 0;
        pl = cfgTemp.diff(list);
        if (pl == null) // no changes
            return 0;
        key = cfgTemp.getName();
        n = ruleList.size();
        ids = new int[n];
        // cache all the ids for rotations later
        n = ruleList.queryIDs(ids);
        m = n;
        for (j=n-1; j>=0; j--) { // look for last item in the list
            str = ruleList.getKey(ids[j]);
            if (cfgTemp.containsKey(str)) { // found the last item
                // cache the position
                m = j;
                break;
            }
        }
        k = pl.size();
        for (j=0; j<k; j++) { // remove deleted items first
            str = (String) pl.get(j);
            if (str == null || str.length() <= 0)
                continue;
            if (cfgTemp.containsItem(str)) {
                str = cfgTemp.remove(str);
                removeRule(str, in);
                num ++;
            }
        }
        cfgTemp.updateItems(list);
        // rotate leftover or add new ones
        k = cfgTemp.getSize();
        for (j=0; j<k; j++) { // loop thru all items
            str = cfgTemp.getKey(j);
            id = ruleList.getID(str);
            if (id >= 0) { // existing one so rotate
                ruleList.rotate(id);
                continue;
            }
            // new item to add
            str = cfgTemp.getItem(j);
            ph = cfgTemp.getProps(str);
            if (ph != null) {
                id = addRule(ph);
                if (id > 0)
                    num ++;
            }
        }
        if (k <= 0) {
            cfgList.remove(key);
            cfgTemp.close();
        }
        else { // rotate the rest of the list
            for (j=m+1; j<n; j++)
                ruleList.rotate(ids[j]);
        }

        return num;
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return operation;
    }

    public String getLinkName() {
        return linkName;
    }

    public String getLinkName(int i) {
        int n = assetList.size();
        if (i >= 0 && i < n) {
            XQueue xq;
            int[] list = new int[n];
            n = assetList.queryIDs(list);
            if (i >= n)
                return null;
            return assetList.getKey(list[i]);
        }
        else
            return null;
    }

    public int getNumberOfOutLinks() {
        return assetList.size();
    }

    public int getOutLinkBoundary() {
        return -1;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getCapacity(int i) {
        int n = assetList.size();
        if (i >= 0 && i < n) {
            int[] list = new int[n];
            n = assetList.queryIDs(list);
            if (i >= n)
                return 0;
            long[] outInfo = assetList.getMetaData(list[i]);
            if (outInfo == null || outInfo.length <= OUT_QTIME)
                return 0;
            return (int) outInfo[OUT_CAPACITY];
        }
        else
            return 0;
    }

    public int getXAMode() {
        return xaMode;
    }

    public int getXAMode(int i) {
        int n = assetList.size();
        if (i >= 0 && i < n) {
            int[] list = new int[n];
            n = assetList.queryIDs(list);
            if (i >= n)
                return -1;
            Object[] asset = (Object[]) assetList.get(list[i]);
            if (asset == null || asset.length <= ASSET_XQ ||
                asset[ASSET_XQ] == null)
                return -1;
            i = ((XQueue) asset[ASSET_XQ]).getGlobalMask();
            if ((i & XQueue.EXTERNAL_XA) > 0)
                return 1;
            else
                return 0;
        }
        else
            return -1;
    }

    public synchronized int getStatus() {
        return status;
    }

    public synchronized void setStatus(int status) {
        if (status >= NODE_READY && status <= NODE_CLOSED)
            this.status = status;
    }

    public String getRuleName(int i) {
        if (i >= 0 && i < ruleList.size()) {
            int[] list;
            int k = ruleList.size();
            if (i < 0 || i >= k)
                return null;
            list = new int[k];
            k = ruleList.queryIDs(list);
            if (i >= k)
                return null;
            else
                return ruleList.getKey(list[i]);
        }
        else
            return null;
    }

    public int getNumberOfRules() {
        return ruleList.size();
    }

    public int getDisplayMask() {
        return displayMask;
    }

    public void setDisplayMask(int mask) {
        displayMask = mask;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public String[] getDisplayProperties() {
        if (displayPropertyName == null)
            return null;
        else {
            String[] keys = new String[displayPropertyName.length];
            for (int i=0; i<keys.length; i++)
                keys[i] = displayPropertyName[i];
            return keys;
        }
    }

    public void setDisplayProperties(String[] keys) {
        displayPropertyName = keys;
    }

    public int getMetaData(int type, int id, long[] data) {
        Object[] asset;
        XQueue xq;
        long[] state;
        int[] list;
        int i, k, n;
        if (data == null)
            return -1;
        n = data.length;
        switch (type) {
          case META_MSG:
            i = msgList.size();
            if (id < 0 || id >= i || n <= MSG_TIME)
                return -1;
            list = new int[i];
            i = msgList.queryIDs(list);
            if (id >= i)
                return -1;
            state = msgList.getMetaData(list[id]);
            if (state == null || state.length <= MSG_TIME)
                return -1;
            k = list[id];
            n = MSG_TIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_OUT:
            i = assetList.size();
            if (id < 0 || id >= i || n <= OUT_QTIME)
                return -1;
            list = new int[i];
            i = assetList.queryIDs(list);
            if (id >= i)
                return -1;
            state = assetList.getMetaData(list[id]);
            if (state == null || state.length <= OUT_QTIME)
                return -1;
            k = list[id];
            n = OUT_QTIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_RULE:
            i = ruleList.size();
            if (id < 0 || id >= i || n <= RULE_TIME)
                return -1;
            list = new int[i];
            i = ruleList.queryIDs(list);
            if (id >= i)
                return -1;
            state = ruleList.getMetaData(list[id]);
            if (state == null || state.length <= RULE_TIME)
                return -1;
            k = list[id];
            n = RULE_TIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_XQ:
            i = assetList.size();
            if (id < 0 || id >= i || n < 6)
                return -1;
            list = new int[i];
            i = assetList.queryIDs(list);
            if (id >= i)
                return -1;
            asset = (Object[]) assetList.get(list[id]);
            if (asset == null || asset.length <= ASSET_XQ)
                return -1;
            xq = (XQueue) asset[ASSET_XQ];
            if (xq == null)
                return -1;
            k = list[id];
            n = 6;
            data[0] = xq.depth();
            data[1] = xq.size();
            data[2] = xq.getCount();
            data[3] = xq.getMTime();
            data[4] = xq.getCapacity();
            data[5] = xq.getGlobalMask();
            break;
          default:
            n = 0;
            k = -1;
            break;
        }
        return k;
    }

    /**
     * It cleans up MetaData for all XQueues and messages.
     */
    public void resetMetaData(XQueue in, XQueue[] out) {
        Object[] asset;
        XQueue xq;
        Browser browser;
        long[] state;
        int[] list;
        int i, j, n, mid, oid, id, outCapacity;

        feedback(in, -1L);
        n = msgList.size();
        list = new int[n];
        n = msgList.queryIDs(list);
        for (i=0; i<n; i++) {
            mid = list[i];
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            id = (int) state[MSG_BID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null) {
                msgList.remove(mid);
                continue;
            }
            xq = (XQueue) asset[ASSET_XQ];
            if (xq != null) synchronized(xq) {
                if (xq.getCellStatus(id) == XQueue.CELL_OCCUPIED) {
                    xq.takeback(id);
                }
                else if (xq.getCellStatus(id) == XQueue.CELL_TAKEN) {
                    xq.remove(id);
                }
            }
            in.putback(mid);
            msgList.remove(mid);
        }

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            state = assetList.getMetaData(i);
            state[OUT_SIZE] = 0;
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            state = ruleList.getMetaData(i);
            state[RULE_SIZE] = 0;
        }
    }

    /** It returns the checkpoint map for current state */
    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        return chkpt;
    }

    /** It restores the state from a checkpointed map */
    public void restore(Map<String, Object> chkpt) {
        if (chkpt == null || chkpt.size() <= 0)
            return;
    }

    public boolean containsXQ(String key) {
        return assetList.containsKey(key);
    }

    public boolean internalXQSupported() {
        return false;
    }

    protected boolean takebackEnabled() {
        return false;
    }

    public Message takeback(String key, int id) {
        Object[] asset;
        if (!takebackEnabled())
            new Event(Event.NOTICE, name + ": takeback is disabled").send();
        else if ((asset = (Object[]) assetList.get(key)) != null) {
            XQueue xq = null;
            if (asset.length >= ASSET_XQ)
                xq = (XQueue) asset[ASSET_XQ];
            return (xq != null) ? (Message) xq.takeback(id) : null;
        }
        return null;
    }

    /**
     * It closes all resources and cleans up all list, cache, metadata, etc.
     * Once it is closed, it can not be used any more.
     */
    public void close() {
        Map rule;
        Browser browser;
        MessageFilter filter;
        int rid;
        setStatus(NODE_CLOSED);
        cells.clear();
        callbackMethod = null;
        cbw.clear();
        msgList.clear();
        assetList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filter = (MessageFilter) rule.get("Filter");
            if (filter != null)
                filter.clear();
            if (rule != null)
                rule.clear();
        }
        ruleList.clear();
        if (cfgList != null) {
            ConfigList cfg;
            browser = cfgList.browser();
            while((rid = browser.next()) >= 0) {
                cfg = (ConfigList) cfgList.get(rid);
                if (cfg != null)
                    cfg.close();
            }
            cfgList.clear();
        }
    }

    public String show(String key, int id, int type) {
        Object[] asset;
        if ((asset = (Object[]) assetList.get(key)) != null) {
            if (asset.length >= ASSET_XQ) {
                XQueue xq = (XQueue) asset[ASSET_XQ];
                if (xq != null && xq.getCellStatus(id) == XQueue.CELL_OCCUPIED){
                    Message msg = (Message) xq.browse(id);
                    return (msg != null) ? MessageUtils.show(msg, type) : null;
                }
            }
        }
        return null;
    }

    /**
     * lists all NEW messages in a specific outlink named by the key and
     * returns a Map or null if it is not defined
     */
    public Map<String, String> list(String key, int type) {
        Object[] asset;

        if ((asset = (Object[]) assetList.get(key)) != null) { // found the xq
            if (asset.length >= ASSET_XQ) {
                XQueue xq = (XQueue) asset[ASSET_XQ];
                if (xq != null)
                    return MessageUtils.listXQ(xq, type, displayPropertyName);
            }
        }

        return null;
    }

    /**
     * lists all propagating and pending messages of the node in the uplink
     * and returns a Map or null if it is not defined
     */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h;
        StringBuffer strBuf;
        String key, qname, str, text;
        int[] list;
        long[] info;
        int i, id, cid, k, n;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = new HashMap<String, String>();
        if (xq.size() <= xq.depth())
            return h;

        hasSummary = (displayPropertyName != null &&
            displayPropertyName.length > 0);
        strBuf = new StringBuffer();

        info = new long[(int) MSG_TIME+1];
        n = xq.getCapacity();
        list = new int[n];
        n = msgList.queryIDs(list);
        k = 0;
        for (i=0; i<n; i++) { // scan entire in XQ
            cid = list[i];
            if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                continue;
            msg = (Message) xq.browse(cid);
            info = msgList.getMetaData(cid);
            if (msg == null || info == null)
                continue;
            id = (int) info[MSG_BID];
            text = Event.dateFormat(new Date(info[MSG_TIME]));
            if (!hasSummary)
                str = "";
            else try {
                str = MessageUtils.display(msg, null, MessageUtils.SHOW_NOTHING,
                    displayPropertyName);
                if (str == null)
                    str = "";
            }
            catch (Exception e) {
                str = e.toString();
            }

            key = ruleList.getKey((int) info[MSG_RID]);
            if (key == null)
                key = "-";
            qname = assetList.getKey((int) info[MSG_OID]);
            if (qname == null)
                qname = "-";

            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + k + "</ID>");
                strBuf.append("<CellID>" + cid + "</CellID>");
                strBuf.append("<Status>PROPAGATING</Status>");
                strBuf.append("<Rule>" + key + "</Rule>");
                strBuf.append("<OutLink>" + qname + ":" + id + "</OutLink>");
                strBuf.append("<Time>" + Utils.escapeXML(text) + "</Time>");
                strBuf.append("<Summary>" +Utils.escapeXML(str)+ "</Summary>");
                strBuf.append("</Record>");
            }
            else if (type == Utils.RESULT_JSON) { // for json
                if (k > 0)
                    strBuf.append(", {");
                else
                    strBuf.append("{");
                strBuf.append("\"ID\":" + k);
                strBuf.append(", \"CellID\":" + cid);
                strBuf.append(", \"Status\":\"PROPAGATING\"");
                strBuf.append(", \"Rule\":\"" + key + "\"");
                strBuf.append(", \"OutLink\":\"" + qname + ":" + id + "\"");
                strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) + "\"");
                strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) + "\"");
                strBuf.append("}");
            }
            else { // for text
                h.put(linkName + "_" + k, cid + " PROPAGATING " + key +
                    " " + qname + ":" + id + " " + text + " " + str);
            }
            k ++;
        }

        if (k == 0) // no message found
            return h;
        else if (type == Utils.RESULT_XML)
            h.put("MSG", strBuf.toString());
        else if (type == Utils.RESULT_JSON)
            h.put("MSG", "[" + strBuf.toString() + "]");

        return h;
    }

    protected void finalize() {
        close();
    }
}
