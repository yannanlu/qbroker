package org.qbroker.node;

/* PipeNode.java - a MessageNode to control flow thruout similar to a pipe */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Comparator;
import java.util.Arrays;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.QList;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Utils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * PipeNode picks up JMS messages from the input XQueue and caches them first.
 * Then it dispatches them based on the priorities and thresholds for each of
 * groups.  There is only one outlink, pipe.  All the groups share the same
 * pipe with different priorities and thresholds.
 *<br/><br/>
 * PipeNode contains multiple rulesets that categorize the incoming messages
 * into non-overlapped groups.  Each group has its own priority for scheduling
 * and threshold as the maximum number of active messages waiting for feedback.
 * PipeNode also supports the dynamic threshold via the array of ActiveTime.
 * By default, all messages in the same group are unique. But if KeyTemplate
 * is defined in the ruleset, messages with the same key are treated as
 * identical. PipeNode will make sure only the first one of the identical
 * messages will be scheduled. This way, PipeNode is able to control flow
 * thruput on each group of messages.
 *<br/><br/>
 * By default, PipeNode has the cache enabled.  So all the overflown messages
 * will be cached. Due to the scheduling process, the message affinity may
 * get broken. However, if the cache is disabled, the message affinity will
 * be guaranteed. In this case, the first overflown message will block until
 * the required resource is available.
 *<br/><br/>
 * PipeNode always creates an extra ruleset, nohit, for those messages not
 * hitting any pre-defined rulesets.  It is always the first ruleset with the
 * id of 0. For nohit, there is no flow control by default. It means that
 * there is no caching on nohit messages and no scheduling on them either.
 * However, if nohit is the only ruleset and the cache is disabled, the flow
 * control is activated. The behavior of the flow control is determined by the
 * values of Heartbeat and Throughput. Throughput is the number of the messages
 * allowed to propagate within a session. The duration of a session is defined
 * via Heartbeat in second. If both Throughput and Heartbeat are larger than
 * zero, the throughput of the nohit messages will be controlled not to exceed
 * the pre-defined Throughput in a session. If Throughput is zero but Heartbeat
 * is positive, there is no flow control at all. But the ruleset will still save
 * the number of messages in the previous session into RULE_PEND for monitoring.
 * If Throughput is less than zero, the nohit messages will be delayed for
 * a while according to the value of Heartbeat. If Heartbeat is a positive
 * number, the length of the delay will be a random number generated via the
 * ordered random number generator with N = 1000000. If Heartbeat is zero,
 * however, the delay will be based on the original intervals between the
 * successive messages. In this case, the ruleset will track the internal
 * timestamps of messages and figures out the original intervals between two
 * successive messages. These intervals will be used to delay the message
 * propagations. It can be used to replay messages in their original throughput
 * rate.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PipeNode extends Node implements Comparator<int[]> {
    private long msgTime = 0L;
    private int heartbeat = 60000;
    private int throughput = 0;
    private DateFormat dateFormat = null;
    private Template temp = null;
    private TextSubstitution tsub = null;

    private XQueue pipe = null;
    private QList reqList = null;      // cached {cid, rid}
    private int[] pipe_mids;           // to track cids for pipe
    private long[] outInfo;
    private boolean cacheEnabled = true, hasFlowControl = false;

    private final static long N = 1000000;

    public PipeNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        long[] ruleInfo;
        long tm;
        int i, j, n, id, ruleSize = 512;
        String key;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "pipe";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null) {
            heartbeat = 1000 * Integer.parseInt((String) o);
            if (heartbeat <= 0)
                heartbeat = 60000;
        }

        if ((o = props.get("CacheEnabled")) != null &&
            "false".equalsIgnoreCase((String) o))
            cacheEnabled = false;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        assetList = NodeUtils.initOutLinks(tm, capacity, 1, -1, name, list);

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_OFFSET] < 0 || outInfo[OUT_LENGTH] < 0 ||
                (outInfo[OUT_LENGTH]==0 && outInfo[OUT_OFFSET]!=0) ||
                outInfo[OUT_LENGTH] + outInfo[OUT_OFFSET] >
                outInfo[OUT_CAPACITY])
                throw(new IllegalArgumentException(name +
                    ": OutLink Partition is not well defined for " +
                    assetList.getKey(i)));
            if ((debug & DEBUG_INIT) > 0)
                strBuf.append("\n\t" + assetList.getKey(i) + ": " + i + " " +
                    outInfo[OUT_CAPACITY] + " " + outInfo[OUT_OFFSET] +
                    "," + outInfo[OUT_LENGTH]);
            break;
        }

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (assetList.size() <= 0)
            throw(new IllegalArgumentException(name+": OutLink is empty"));

        pipe_mids = new int[(int) outInfo[OUT_CAPACITY]];
        for (i=0; i<pipe_mids.length; i++)
            pipe_mids[i] = -1;

        msgList = new AssetList(name, capacity);
        reqList = new QList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        cells = new CollectibleCells(name, capacity);

        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = (List) o;
        else if (!cacheEnabled) { // flow control mode on nohit msgs
            list = new ArrayList();
            if ((o = props.get("Throughput")) != null)
                throughput = Integer.parseInt((String) o);
            hasFlowControl = true;

            // for delays
            if ((o = props.get("KeyTemplate")) != null && o instanceof String) {
                temp = new Template((String) o);
                if ((o = props.get("KeySubstitution")) != null &&
                    o instanceof String)
                    tsub = new TextSubstitution((String) o);
                if ((o = props.get("TimePattern")) != null &&
                    o instanceof String)
                    dateFormat = new SimpleDateFormat((String) o);
            }
        }
        else
            list = new ArrayList();
        n = list.size();

        try { // init rulesets
            // for nohit
            key = "nohit";
            ruleInfo = new long[RULE_TIME + 1];
            for (i=0; i<=RULE_TIME; i++)
                ruleInfo[i] = 0;
            ruleInfo[RULE_STATUS] = NODE_RUNNING;
            ruleInfo[RULE_DMASK] = displayMask;
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = 0;
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            // set the default threshold to capacity for nohit
            ruleInfo[RULE_MODE] = capacity;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("PropertyName", displayPropertyName);
            if ((o = props.get("StatsLog")) != null && o instanceof String)
                rule.put("StatsLog", new GenericLogger((String) o));

            ruleList.add(key, ruleInfo, rule);
            outInfo = assetList.getMetaData(0);
            outInfo[OUT_NRULE] ++;
            outInfo[OUT_ORULE] ++;

            for (i=0; i<n; i++) { // for defined rules
                o = list.get(i);
                if (o instanceof String) {
                    o = props.get((String) o);
                    if (o == null || !(o instanceof Map)) {
                        new Event(Event.ERR, name + ": ruleset " + i + ", " +
                            (String)list.get(i)+", is not well defined").send();
                        continue;
                    }
                }
                ruleInfo = new long[RULE_TIME+1];
                rule = initRuleset(tm, (Map) o, ruleInfo);
                if (rule != null && (key = (String) rule.get("Name")) != null) {
                    if (ruleList.add(key, ruleInfo, rule) < 0) // new rule added
                        new Event(Event.ERR, name + ": ruleset " + i + ", " +
                            key + ", failed to be added").send();
                }
                else
                    new Event(Event.ERR, name + ": ruleset " + i +
                        " failed to be initialized").send();
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name+": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_EXTRA] + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_DMASK] + " - "+
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
        }
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG,name+" RuleName: RID Pri Mode Option Mask - "+
                "OutName" + strBuf.toString()).send();
    }

    public int updateParameters(Map props) {
        Object o;
        int i, n;

        n = super.updateParameters(props);
        if ((o = props.get("Heartbeat")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i > 0 && i != heartbeat) {
                heartbeat = i;
                n++;
            }
        }
        if (cacheEnabled) { // non-blocking
            if ((o = props.get("CacheEnabled")) != null &&
                "false".equalsIgnoreCase((String) o)) {
                cacheEnabled = false;
                n++;
            }
        }
        else { // blocking
            if ((o = props.get("CacheEnabled")) != null &&
                "true".equalsIgnoreCase((String) o)) {
                cacheEnabled = true;
                n++;
                if (temp != null)
                    temp = null;
                if (tsub != null)
                    tsub = null;
                if (dateFormat != null)
                    dateFormat = null;
            }
        }
        if (!cacheEnabled && (o = props.get("Throughput")) != null) {
            i = Integer.parseInt((String) o);
            if (i >= 0 && i != throughput) {
                throughput = i;
                n++;
            }

            // for delays
            if ((o = props.get("KeyTemplate")) != null && o instanceof String) {
                temp = new Template((String) o);
                if ((o = props.get("KeySubstitution")) != null &&
                    o instanceof String)
                    tsub = new TextSubstitution((String) o);
                if ((o = props.get("TimePattern")) != null &&
                    o instanceof String)
                    dateFormat = new SimpleDateFormat((String) o);
            }
        }

        return n;
    }

    /**
     * It initializes a new ruleset with the ruleInfo and returns the rule upon
     * success.  Otherwise, it throws an exception or returns null.
     */
    protected Map<String, Object> initRuleset(long tm, Map ph, long[] ruleInfo){
        Object o;
        Map<String, Object> rule;
        Iterator iter;
        List list;
        String key, str, ruleName;
        int i, k, n, id;

        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a rule"));
        if (ruleInfo == null || ruleInfo.length <= RULE_TIME)
            throw(new IllegalArgumentException("ruleInfo is not well defined"));
        ruleName = (String) ph.get("Name");
        if (ruleName == null || ruleName.length() == 0)
            throw(new IllegalArgumentException("ruleName is not defined"));

        rule = new HashMap<String, Object>();
        rule.put("Name", ruleName);

        rule.put("Filter", new MessageFilter(ph));
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        ruleInfo[RULE_OID] = 0;
        ruleInfo[RULE_PID] = TYPE_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store Priority into RULE_EXTRA field
        if ((o = ph.get("Priority")) != null)
            ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_EXTRA] = 0;

        // store default threshold into RULE_MODE
        if ((o = ph.get("Threshold")) != null && o instanceof String)
            ruleInfo[RULE_MODE] = Integer.parseInt((String) o);
        if (ruleInfo[RULE_MODE] <= 0)
            ruleInfo[RULE_MODE] = 1;

        if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
            rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String) o));
        }

        if ((o = ph.get("ActiveTime")) != null && o instanceof List) {
            TimeWindows[] tw;
            int m;
            list = (List) o;
            m = list.size();
            tw = new TimeWindows[m];
            k = 0;
            for (i=0; i<m; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                tw[k++] = new TimeWindows((Map) o);
            }
            rule.put("TimeWindows", tw);
            // store number of time windows into RULE_OPTION
            ruleInfo[RULE_OPTION] = k;
        }
        outInfo = assetList.getMetaData(0);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // for String properties
        if ((o = ph.get("StringProperty")) != null && o instanceof Map) {
            iter = ((Map) o).keySet().iterator();
            k = ((Map) o).size();
            String[] pn = new String[k];
            k = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if ((pn[k] = MessageUtils.getPropertyID(key)) == null)
                    pn[k] = key;
                k ++;
            }
            rule.put("PropertyName", pn);
        }
        else if (o == null)
            rule.put("PropertyName", displayPropertyName);

        return rule;
    }

    /**
     * picks up a message from input queue and evaluate its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null, qName = null;
        long currentTime, sessionTime, st = 0;
        long[] ruleInfo = null;
        int[] ruleMap;
        int[][] sorted_rids;
        Browser browser;
        MessageFilter[] filters = null;
        GenericLogger statsLog = null;
        Map rule = null;
        String[] propertyName = null;
        long count = 0;
        double x = 0.0, y;
        int mask, ii, shift, len, sessionCount = 0;
        int i = 0, k, mid, n, size, previousRid, dspBody;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0;  // the id of the ruleset
        int id = -1;  // the id of the empty cell in pipe
        boolean ckBody = false;
        byte[] buffer = new byte[bufferSize];

        i = in.getCapacity();
        if (capacity != i) { // assume it only occurs at startup
            new Event(Event.WARNING, name + ": " + in.getName() +
                " has the different capacity of " + i + " from " +
                capacity).send();
            capacity = i;
            msgList.clear();
            reqList.clear();
            msgList = new AssetList(name, capacity);
            reqList = new QList(name, capacity);
            cells.clear();
            cells = new CollectibleCells(name, capacity);
            // reset the default threshold for nohit
            ruleInfo = ruleList.getMetaData(0);
            ruleInfo[RULE_MODE] = capacity;
        }

        // initialize filters
        n = ruleList.size();
        filters = new MessageFilter[n];
        browser = ruleList.browser();
        ruleMap = new int[n];
        sorted_rids = new int[n][3];
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            ruleInfo = ruleList.getMetaData(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
            if (filters[i] != null) {
                if ((!ckBody) && filters[i].checkBody())
                    ckBody = true;
            }
            sorted_rids[i][0] = rid;
            sorted_rids[i][1] = (int) ruleInfo[RULE_EXTRA];
            sorted_rids[i][2] = i;
            ruleMap[i++] = rid;
        }
        Arrays.sort(sorted_rids, this);
        if (n == 1 && !cacheEnabled) {
            hasFlowControl = true;
            rule = (Map) ruleList.get(0);
            statsLog = (GenericLogger) rule.get("StatsLog");
            if (statsLog != null) {
                qName = statsLog.getLoggerName();
                if ((i = qName.lastIndexOf("/")) > 0)
                    qName = qName.substring(i+1);
                if ((i = qName.indexOf(".")) > 0)
                    qName = qName.substring(0, i);
            }
        }
        else if (hasFlowControl)
            hasFlowControl = false;

        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;

        // update assetList
        if (out.length >= 1) {
            pipe = out[0];
            if (outInfo[OUT_CAPACITY] != pipe.getCapacity()) {
                outInfo[OUT_CAPACITY] = pipe.getCapacity();
                pipe_mids = new int[(int) outInfo[OUT_CAPACITY]];
                for (i=0; i<pipe_mids.length; i++)
                    pipe_mids[i] = -1;
            }
        }
        len = (int) outInfo[OUT_LENGTH];
        shift = (int) outInfo[OUT_OFFSET];

        n = ruleMap.length;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime + heartbeat;
        previousRid = -1;
        ii = 0;
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;

            switch (len) {
              case 0:
                id = pipe.reserve(waitTime);
                break;
              case 1:
                id = pipe.reserve(waitTime, shift);
                break;
              default:
                id = pipe.reserve(waitTime, shift, len);
                break;
            }

            if (id >= 0) { // id-th cell of out is reserved or is a collectible
                mid = pipe_mids[id];
                if (mid >= 0) { // id-th cell has just been empty now
                    long[] state = msgList.getMetaData(mid);
                    currentTime = System.currentTimeMillis();
                    cid = (int) state[MSG_CID];
                    rid = (int) state[MSG_RID];
                    in.remove(mid);
                    msgList.remove(cid);
                    pipe_mids[id] = -1;
                    state = ruleList.getMetaData(rid);
                    outInfo[OUT_SIZE] --;
                    outInfo[OUT_COUNT] ++;
                    ruleInfo[RULE_SIZE] --;
                    ruleInfo[RULE_COUNT] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    if ((debug & DEBUG_FBAK) > 0)
                        new Event(Event.DEBUG, name+" passback: " + rid +
                            " " + mid+ ":" + cid + " 0/" + id + " " +
                            state[RULE_SIZE] + " " + outInfo[OUT_SIZE] + " " +
                            pipe.size() + ":" + pipe.depth() + " " +
                            msgList.size()).send();
                }
                pipe.cancel(id);
                if ((!cacheEnabled) && reqList.size() > 0) { // empty cache
                    i = feedback(in, waitTime);
                    currentTime = System.currentTimeMillis();
                    if (i > 0) // some messages are done so reset sessionTime
                        sessionTime = currentTime;
                    if (currentTime >= sessionTime) {
                        k = schedule(currentTime, in, buffer, sorted_rids);
                        sessionTime = currentTime + heartbeat;
                    }
                    continue;
                }
            }
            else { // pipe is still full
                feedback(in, waitTime);
                if (!cacheEnabled) // no cache for incoming
                    continue;
            }

            // either pipe is not full or cache is enabled
            cid = -1;
            if ((cid = in.getNextCell(waitTime)) < 0) { // no new msg
                if (msgList.size() > 0) {
                    i = feedback(in, waitTime);
                    currentTime = System.currentTimeMillis();
                    if (i > 0) // some messages are done so reset sessionTime
                        if (!hasFlowControl)
                            sessionTime = currentTime;
                }
                else
                    currentTime = System.currentTimeMillis();
                if (currentTime < sessionTime)
                    continue;
                sessionTime = currentTime + heartbeat;
                if (hasFlowControl) {
                    ruleInfo[RULE_PEND] = sessionCount;
                    if (statsLog != null) try {
                        statsLog.log(Event.dateFormat(new Date(currentTime)) +
                            " " + qName + " " + sessionCount);
                    }
                    catch (Exception ex) {
                    }
                    if (sessionCount > 0)
                        sessionCount = 0;
                }

                if (reqList.size() > 0 && !pipe.isFull())
                    k = schedule(currentTime, in, buffer, sorted_rids);

                continue;
            }
            // got a new msg
            if ((inMessage = (Message) in.browse(cid)) == null) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": null msg from " +
                    in.getName()).send();
                continue;
            }

            currentTime = System.currentTimeMillis();
            msgStr = null;
            rid = 0;
            i = 0;
            try {
                if (ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                for (i=1; i<n; i++) {
                    if (filters[i].evaluate(inMessage, msgStr)) {
                        rid = ruleMap[i];
                        break;
                    }
                }
            }
            catch (Exception e) {
                String str = name;
                Exception ex = null;
                if (e instanceof JMSException)
                    ex = ((JMSException) e).getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                new Event(Event.ERR, str + " failed to apply the filter "+
                    i+ ": " + Event.traceStack(e)).send();
                i = -1;
            }

            if (rid != previousRid) {
                ruleName = ruleList.getKey(rid);
                ruleInfo = ruleList.getMetaData(rid);
                rule = (Map) ruleList.get(rid);
                propertyName = (String[]) rule.get("PropertyName");
                previousRid = rid;
            }

            if (rid == 0) { // nohit
                if (hasFlowControl) {
                    currentTime = System.currentTimeMillis();
                    if (heartbeat <= 0) { // natural sessions
                        if (st <= 0) try {
                            String key;
                            long t;
                            if (temp != null) {
                                key = MessageUtils.format(inMessage,
                                    buffer, temp);
                                if (tsub != null && key != null)
                                    key = tsub.substitute(key);
                                if (dateFormat != null)
                                    t = dateFormat.parse(key,
                                        new ParsePosition(0)).getTime();
                                else
                                    t = Long.parseLong(key);
                            }
                            else { // default
                                t = inMessage.getJMSTimestamp();
                            }

                            if (t < msgTime - 60000) { // reset session
                                msgTime = t;
                                st = 0;
                            }
                            else if (t > msgTime) { // update the first series
                                st = t - msgTime;
                                msgTime = t;
                            }
                            else {
                                st = 0;
                            }
                            sessionTime = currentTime + st;
                        }
                        catch (JMSException e) {
                            new Event(Event.ERR, name +
                                " failed to retrieve timestamp from msg: " +
                                Event.traceStack(e)).send();
                            st = 0;
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, name +
                                " failed to get timestamp from msg: " +
                                Event.traceStack(e)).send();
                            st = 0;
                        }

                        do {
                            if (st > waitTime) { // delay up to waitTime
                                if (msgList.size() > 0)
                                    feedback(in, waitTime);
                                else try {
                                    Thread.sleep(waitTime);
                                }
                                catch (Exception e) {
                                }
                            }
                            else if (st > 1) { // delay a bit
                                if (msgList.size() > 0)
                                    feedback(in, st);
                                else try {
                                    Thread.sleep(st);
                                }
                                catch (Exception e) {
                                }
                            }
                            st = sessionTime - System.currentTimeMillis();
                        } while (st > 1 &&
                            ((mask=in.getGlobalMask()) & XQueue.STANDBY) == 0 &&
                            (mask & XQueue.KEEP_RUNNING) > 0);
                        if (st > 1) { // stopped or disabled temporarily
                            in.putback(cid);
                            break;
                        }
                        st = 0;
                    }
                    else if (throughput < 0) { // random sessions
                        if (currentTime > sessionTime + 5) { // reset session
                            st = 0;
                            x = 0.0;
                        }
                        else
                            st = sessionTime - currentTime;
                        do {
                            if (st > waitTime) { // delay up to waitTime
                                if (msgList.size() > 0)
                                    feedback(in, waitTime);
                                else try {
                                    Thread.sleep(waitTime);
                                }
                                catch (Exception e) {
                                }
                            }
                            else if (st > 0) { // delay a bit
                                if (msgList.size() > 0)
                                    feedback(in, st);
                                else try {
                                    Thread.sleep(st);
                                }
                                catch (Exception e) {
                                }
                            }
                            st = sessionTime - System.currentTimeMillis();
                        } while (st > 1 &&
                            ((mask=in.getGlobalMask()) & XQueue.STANDBY) == 0 &&
                            (mask & XQueue.KEEP_RUNNING) > 0);
                        if (st > 1) { // stopped or disabled temporarily
                            in.putback(cid);
                            break;
                        }
                        y = - Math.log(1.0 - Math.random() * (1.0 -
                            Math.exp(-(N - x))));
                        if (x + x >= N) { // reset session at half way
                            x = 0.0;
                            st = 0;
                        }
                        else {
                            x = x + y;
                            st = (long) (y * heartbeat);
                        }
                        sessionTime = System.currentTimeMillis() + st;
                    }
                    else if (currentTime >= sessionTime) { // time is up
                        sessionTime = currentTime + heartbeat;
                        ruleInfo[RULE_PEND] = sessionCount;
                        if (statsLog != null) try {
                           statsLog.log(Event.dateFormat(new Date(currentTime))+
                                " " + qName + " " + sessionCount);
                        }
                        catch (Exception ex) {
                        }
                        if (sessionCount > 0)
                            sessionCount = 0;
                    }
                    else if (throughput == 0) { // no limit on thruput
                        st = 0;
                    }
                    else if (sessionCount >= throughput) { // slow down
                        in.putback(cid);
                        if (msgList.size() > 0)
                            feedback(in, waitTime);
                        else try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception e) {
                        }
                        continue;
                    }
                    else { // check if it needs to slow down 
                        st = sessionCount * heartbeat / throughput -
                            (currentTime + heartbeat - sessionTime);
                        if (st > waitTime) { // slow down
                            in.putback(cid);
                            if (msgList.size() > 0)
                                feedback(in, waitTime);
                            else try {
                                Thread.sleep(waitTime);
                            }
                            catch (Exception e) {
                            }
                            continue;
                        }
                        else if (st > 1) { // slow down for a bit
                            if (msgList.size() > 0)
                                feedback(in, st);
                            else try {
                                Thread.sleep(st);
                            }
                            catch (Exception e) {
                            }
                            continue;
                        }
                    }
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=0").send();

                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                        msgStr = MessageUtils.processBody(inMessage,buffer);
                    new Event(Event.INFO, name + ": " + ruleName +
                        " dispatched msg "+ (count + 1) + ":" +
                        MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: " +e.toString()).send();
                }

                k = passthru(currentTime, inMessage, in, pipe, outInfo,
                    rid, 0, cid, String.valueOf(cid));
                if (k > 0) {
                    count ++;
                    sessionCount ++;
                }
                else
                    in.putback(cid);
                feedback(in, -1L);
            }
            else { // add it to the cache first
                i = reqList.reserve(cid);
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " cache=" + i).send();
                if (i >= 0) {
                    reqList.add(new int[]{cid, rid}, i);
                    ruleInfo[RULE_PEND] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    feedback(in, -1L);
                }
                else {
                    in.putback(cid);
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to reserve cache at " + cid +": "+ i +" "+
                        reqList.size()+ "/" + reqList.getCapacity()).send();
                    feedback(in, waitTime + waitTime);
                }
            }
            inMessage = null;
            if (reqList.size() > 0 && !pipe.isFull())
                k = schedule(currentTime, in, buffer, sorted_rids);
        }
    }

    /**
     * schedules each pending msg based on its priority and available resources.
     */
    private int schedule(long currentTime, XQueue in, byte[] buffer,
        int[][] sorted_rids) {
        Message inMessage;
        String key, ruleName;
        TextSubstitution sub = null;
        Template temp = null;
        TimeWindows[] tw = null;
        Map rule;
        Browser browser;
        long[] ruleInfo;
        int[] meta;
        int id, cid, rid;
        int i, j, k, m, n, count = 0;
        boolean dspBody = ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0);

        if (pipe.isFull() || reqList.size() <= 0)
            return count;

        n = sorted_rids.length;
        for (i=0; i<n; i++) {
            rid = sorted_rids[i][0];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0) // no jobs pending
                continue;
            rule = (Map) ruleList.get(rid);
            tw = (TimeWindows[]) rule.get("TimeWindows");
            temp = (Template) rule.get("KeyTemplate");
            sub = (TextSubstitution) rule.get("KeySubstitution");
            k = (int) ruleInfo[RULE_MODE];
            if (ruleInfo[RULE_OPTION] > 0 && k > 0) { // update threshold
                int[] th;
                m = (int) ruleInfo[RULE_OPTION];
                for (j=0; j<m; j++) {
                    th = tw[j].getCurrentThreshold(currentTime);
                    if (th != null && th.length > 0) {
                        k = th[0] / 1000;
                        break;
                    }
                }
            }
            if (ruleInfo[RULE_SIZE] >= k) // max out
                continue;
            ruleName = ruleList.getKey(rid);
            k -= (int) ruleInfo[RULE_SIZE];
            browser = reqList.browser();
            for (j=0; j<k; j++) {
                if (j > 0)
                    browser.reset();
                while ((id = browser.next()) >= 0) {
                    meta = (int[]) reqList.browse(id);
                    if (rid != meta[1])
                        continue;
                    cid = meta[0];
                    inMessage = (Message) in.browse(cid);
                    key = null;
                    if (temp != null) try {
                        key = MessageUtils.format(inMessage, buffer, temp);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to retrieve key from msg at "+cid).send();
                        key = null;
                    }
                    if (key == null) // reset to default key
                        key = String.valueOf(cid);
                    else if (sub != null)
                        key = sub.substitute(key);
                    if (msgList.containsKey(key))
                        continue;

                    if (displayMask > 0) try { // display the message
                        String msgStr = null;
                        if (dspBody)
                            msgStr = MessageUtils.processBody(inMessage,buffer);
                        new Event(Event.INFO, name + ": " + ruleName +
                            " dispatched msg "+ (count + 1) + ":" +
                            MessageUtils.display(inMessage, msgStr,
                            displayMask, displayPropertyName)).send();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " failed to display msg: " +e.toString()).send();
                    }

                    k = passthru(currentTime, inMessage, in, pipe, outInfo,
                        rid, 0, cid, key);
                    if (k > 0) {
                        reqList.getNextID(id);
                        reqList.remove(id);
                        count ++;
                        ruleInfo[RULE_PEND] --;
                    }
                    else
                        break;
                }
                if (pipe.isFull() || reqList.size() <= 0)
                    return count;
            }
            if (pipe.isFull() || reqList.size() <= 0)
                return count;
        }

        return count;
    }

    /**
     * passes the message from an input XQ over to an output XQ
     */
    private int passthru(long currentTime, Message msg, XQueue in,
        XQueue out, long[] outInfo, int rid, int oid, int cid, String key) {
        long[] state, ruleInfo;
        int id = -1, k, mid, mask, len, shift, outCapacity;
        len = (int) outInfo[OUT_LENGTH];
        shift = (int) outInfo[OUT_OFFSET];
        switch (len) {
          case 0:
            for (k=0; k<1000; k++) { // reserve an empty cell
                id = out.reserve(-1);
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
            for (k=0; k<1000; k++) { // reserve the empty cell
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
            for (k=0; k<1000; k++) { // reserve an partitioned empty cell
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
            mid = pipe_mids[id];
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                mid = msgList.add(key, new long[]{cid, oid, id, rid, 0,
                    currentTime}, key, cid);
                pipe_mids[id] = mid;
            }
            else { // id-th cell has just been empty now, replace it
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                msgList.remove(mid);
                pipe_mids[id] = -1;
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
                        ":" + state[MSG_CID] + " 0/"+id+" "+ruleInfo[RULE_SIZE]+
                        " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                        out.depth() + " " + msgList.size()).send();
                mid = msgList.add(key, new long[] {cid, oid, id, rid, 0,
                    currentTime}, key, cid);
                pipe_mids[id] = mid;
            }
            if (mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with 0/" + id + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            k = out.add(msg, id, cbw);
            if (k >= 0) { // success
                outInfo[OUT_SIZE] ++;
                if (outInfo[OUT_STATUS] == NODE_RUNNING)
                    outInfo[OUT_TIME] = currentTime;
                ruleInfo = ruleList.getMetaData(rid);
                ruleInfo[RULE_SIZE] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if ((debug & DEBUG_PASS) > 0)
                    new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                        pipe_mids[id] + ":" + cid + " 0/" +id+ " " +
                        ruleInfo[RULE_SIZE]+ " " + outInfo[OUT_SIZE] + " " +
                        out.size() + ":" + out.depth() + " " +
                        msgList.size()).send();
            }
            else { // failure
                out.cancel(id);
                pipe_mids[id] = -1;
                msgList.remove(mid);
                new Event(Event.ERR, name + ": failed to add a msg to " +
                    out.getName() + " at " + id + " for " +
                    ruleList.getKey(rid) + ": " + outInfo[OUT_SIZE] + "/" +
                    out.size() + "/" + out.depth()).send();
                return 0;
            }
        }
        else { // reservation failed
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                out.getName() + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE]).send();
            return 0;
        }
        return 1;
    }

    /**
     * returns the number of done messages removed from in
     */
    protected int feedback(XQueue in, long milliSec) {
        int id, cid, rid, mid, l = 0;
        long t;
        boolean inDetail = ((debug & DEBUG_FBAK) > 0);
        long[] ruleInfo, state;
        StringBuffer strBuf = (inDetail) ? new StringBuffer() : null;

        if (msgList.size() <= 0)
            return 0;

        if (milliSec == 0) // no wait forever
            milliSec = -1;

        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            cid = (int) state[MSG_CID];
            rid = (int) state[MSG_RID];
            id = (int) state[MSG_BID];
            in.remove(cid);
            msgList.remove(mid);
            pipe_mids[id] = -1;
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = t;
            if (inDetail)
                strBuf.append("\n\t" + rid + " " + mid + "/" + cid +
                    " 0:" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + pipe.size() +"|"+ pipe.depth()+
                    " " + msgList.size());
            l ++;
            if (milliSec >= 0) // one collection a time
                break;
        }

        if (l > 0 && inDetail)
            new Event(Event.DEBUG, name + " feedback: RID MID/CID OID:BID " +
                "RS OS size|depth ms - " + l + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        return l;
    }

    /**
     * It marks the cell in cells with key and id.
     */
    public void callback(String key, String id) {
        int mid;
        if (id == null || id.length() <= 0)
            return;
        try {
            mid = Integer.parseInt(id);
        }
        catch (Exception e) {
            mid = -1;
        }
        if (mid >= 0) {
            mid = pipe_mids[mid];
            if (mid >= 0) { // mark the mid is ready to be collected
                cells.take(mid);
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name +": "+ key + " called back on "+
                        mid + " with 0/" + id).send();
            }
            else
                new Event(Event.ERR, name +": "+ key+" failed to callback on "+
                    "0:" + id).send();
        }
        else
            new Event(Event.ERR, name +": "+ key+" failed to callback on 0:"+
                id).send();
    }

    /**
     * cleans up MetaData for all XQs and messages
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

        for (i=0; i<pipe_mids.length; i++)
            pipe_mids[i] = -1;

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

    /** lists all propagating and pending messages */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h, ph;
        StringBuffer strBuf;
        String key, qname, str, text;
        int[] list, info;
        int i, id, cid, k, n;
        long tm;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (reqList.size() <= 0 || h == null)
            return h;
        else if (h.size() <= 0) { // empty
            if (xq.size() <= xq.depth())
                return h;
            strBuf = new StringBuffer();
            k = 0;
            ph = new HashMap<String, String>();
        }
        else { // try to count k
            if (xq.size() <= xq.depth())
                return new HashMap<String, String>();
            ph = new HashMap<String, String>();
            strBuf = new StringBuffer();
            if (type == Utils.RESULT_XML) {
               strBuf.append((String) h.remove("MSG"));
               n = 0;
               while ((i = strBuf.indexOf("<CellID>", n)) >= 0) {
                   n = i+8;
                   i = strBuf.indexOf("</", n);
                   if (i > n)
                       ph.put(strBuf.substring(n, i), null);;
                   n = i;
               }
               k = ph.size();
            }
            else if (type == Utils.RESULT_JSON) {
               strBuf.append((String) h.remove("MSG"));
               n = 0;
               while ((i = strBuf.indexOf(", \"CellID\":", n)) >= 0) {
                   n = i+11;
                   i = strBuf.indexOf(", ", n);
                   if (i > n)
                       ph.put(strBuf.substring(n, i), null);;
                   n = i;
               }
               k = ph.size();
               n = strBuf.length();
               if (k > 0 && n > 2) { // trim the strBuf
                   strBuf.deleteCharAt(n-1);
                   strBuf.deleteCharAt(0);
               }
            }
            else {
                k = h.size();
            }
        }

        hasSummary = (displayPropertyName != null &&
            displayPropertyName.length > 0);

        n = reqList.getCapacity();
        list = new int[n];
        n = reqList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) { // scan the list
            cid = list[i];
            if (ph.containsKey(String.valueOf(cid))) // duplicate
                continue;
            if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                continue;
            msg = (Message) xq.browse(cid);
            info = (int[]) reqList.browse(cid);
            if (msg == null || info == null)
                continue;
            try {
                tm = msg.getJMSTimestamp();
            }
            catch (Exception e) {
                tm = 0;
            }
            text = Event.dateFormat(new Date(tm));
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

            key = ruleList.getKey(info[1]);
            if (key == null)
                key = "-";

            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + k + "</ID>");
                strBuf.append("<CellID>" + cid + "</CellID>");
                strBuf.append("<Status>PENDING</Status>");
                strBuf.append("<Rule>" + key + "</Rule>");
                strBuf.append("<OutLink>-</OutLink>");
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
                strBuf.append(", \"Status\":\"PENDING\"");
                strBuf.append(", \"Rule\":\"" + key + "\"");
                strBuf.append(", \"OutLink\":\"-\"");
                strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) + "\"");
                strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) + "\"");
                strBuf.append("}");
            }
            else { // for text
                h.put(linkName + "_" + k, cid + " PENDING " + key +
                    " - " + text + " " + str);
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

    public void close() {
        Browser browser;
        Object[] asset;
        Map rule;
        int rid;
        setStatus(NODE_CLOSED);
        if (cells != null)
            cells.clear();
        callbackMethod = null;
        cbw.clear();
        msgList.clear();
        assetList.clear();
        reqList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null)
                rule.clear();
        }
        ruleList.clear();
    }

    public int compare(int[] a, int[] b) {
        int i, j;
        i = a[1];
        j = b[1];
        if (i > j)
            return -1;
        else if (i < j)
            return 1;
        else {
            i = a[2];
            j = b[2];
            if (i > j)
                return 1;
            else if (i < j)
                return -1;
            else
                return 0;
        }
    }

    protected void finalize() {
        close();
    }
}
