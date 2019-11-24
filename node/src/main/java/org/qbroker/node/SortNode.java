package org.qbroker.node;

/* SortNode.java - a MessageNode sorting JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.KeyChain;
import org.qbroker.common.QuickCache;
import org.qbroker.common.IntKeyChain;
import org.qbroker.common.LongKeyChain;
import org.qbroker.common.FloatKeyChain;
import org.qbroker.common.DoubleKeyChain;
import org.qbroker.common.StringKeyChain;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * SortNode picks up JMS messages from an input XQ and withholds them for the
 * baking process. Once the messages are fully baked, it sorts them according
 * to their sorting keys and the pre-defined rulesets. It routes them to four
 * outlinks: done for sorted messages, bypass for messages which are out of
 * order, nohit for those messages do not belong to any rulesets and failure
 * for the messages failed in the baking or sorting process. Since SortNode
 * does not consume any messages, any incoming message has to find a way out
 * via one of the four outlinks.
 *<br><br>
 * SortNode contains a number of pre-defined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the sorting
 * options for the messages in each group as well as the parameters used
 * in the baking process.  All the groups must share the same key type.
 * But different groups may have their own way to construct the sorting key.
 * The number of messages withheld for baking process is tracked via the
 * RULE_PEND field.  Furthermore, SortNode always creates one extra ruleset,
 * nohit.  The ruleset of nohit is for all the messages not hitting any of
 * the patterns.
 *<br><br>
 * SortNode supports both the dynamic session and the baking process.
 * It is determined by BakeTime in millisecond and SessionTimeout in second.
 * If both BakeTime and SessionTimeout are set to 0 as the default, the session
 * will be maintained as static without cache.  In this case, the messages with
 * the keys of the first series will be routed to done. The first series of keys
 * are a set of keys whose value are always larger than those of previous ones.
 * Other messages will be treated as out of order and are routed to bypass.
 * If BakeTime is 0 but SessionTimeout is larger than zero, the session will be
 * maintained dynamically with cache.  In this case, SortNode will withhold all
 * incoming messages and tracks the mtime of the cache.  If the cache has no
 * changes within the recent SessionTimeout seconds, all the messages in the
 * cache will be flushed out with the sorted order.  If BakeTime is larger than
 * zero, the dynamic session will be controlled by the baking process.  In this
 * case, the incoming messages will be baked rather than statically withheld.
 * During the baking process, some of the incoming messages will be routed out
 * right away. But majority of them will be withheld for a certain time peroid.
 *<br><br>
 * BakeTime is the minimum time for the reference message of a session to be
 * withheld in case other low-key messages arriving late. The reference message
 * is the most baked message with a higher key value than the threshold which
 * is the highest key value for the previous session. We are not sure that how
 * many messages with lower key values will come after the reference message.
 * However, we are sure after a certain time (BakeTime), the chance to have
 * messages with lower key values is minimal.  Therefore, SortNode keeps the
 * reference message and the other new messages in the cache to bake.  Once
 * time is up, there is at least one fully baked message in the session.
 * SortNode will look for the highest key value among all the fully baked
 * messages. The highest value will be saved as the threshold for the next
 * session. All the messages with either lower key values or same key value
 * as compared to the reference message will be flushed out according to the
 * ascending order of their keys. If there are messages left, they are not
 * fully baked yet and will be baked once more in the next session. In this
 * case, SortNode will find the most baked message as the reference message
 * for the new session. Otherwise, the first message will be the reference
 * message in the new session. This way, SortNode will smooth out the
 * fluctuations on the key values of message stream and recovers their
 * original order.
 *<br><br>
 * SortNode also supports the claim mode.  If there is at least one claim
 * ruleset defined, the node has its claim mode on. A claim ruleset is
 * similar to a baking ruleset. The only difference on the configurations is
 * that the former has also defined RuleType as "claim". However, the impact
 * on the node is huge, even for the baking rulesets. With the claim mode
 * enabled, the routing policy of the node will be totally different from the
 * case where the claim mode is off by default. The claim rule is designed
 * for active-active message flow clusters only. It assumes that both the
 * master and the worker flows are processing the same set of incoming messages.
 * Meanwhile, the master flow also escalates each processed message to the
 * worker flow via the cluster communication channels. Therefore, the SortNode
 * on the claim mode is supposed to be paired with an instance of ActionNode
 * on the same flow. Even though both the SortNode and the ActionNode are
 * running on the same flow and they shared the same uplink, their status are
 * mutually exclusive on any given instance of the flow. On the master flow,
 * the SortNode is always disabled while the ActionNode is always enabled. The
 * ActionNode will process all the incoming messages and escalates each of them
 * to the worker flow. On the worker flow, the ActionNode is always disabled
 * while the SortNode is always enabled. Therefore, the SortNode processes
 * the incoming messages. Since the container of the worker flow will forward
 * the escalated messages to the SortNode, they will be processed by the
 * SortNode to claim the baked incoming messages.
 *<br><br>
 * For a SortNode with the claim mode on, it allows the baking rules and the
 * claim rules to define IDTemplate. The IDTemplate specifies the way to
 * extract a unique ID from the messages.  These IDs will be cached into a
 * separate cache to indicate those messages have been processed on the master
 * flow. There is no requirement on the order for the IDs. But if there is no
 * IDTemplate defined, the ID will be same as the key. The messages of claim
 * rulesets will be used to terminate the baking process if their keys match
 * any cached keys. On the other hand, if the incoming message has the same ID
 * in the ID cache, it will not be baked at all since it has already been
 * processed on the master. All the cached incoming messages with the lower or
 * equal key values will be flushed to the bypass outlink. We called the cached
 * messages are claimed rather than fully baked. The claiming messages will be
 * routed to the nohit outlink. For those fully baked messages, SortNode will
 * route them to the done outlink since they have not been claimed within the
 * time window. The rest of incoming messages are still in the baking process.
 * Since they are not claimed yet, it means they are not processed by the
 * master flow yet. When a failover occurs, the worker flow will be promoted
 * to the new master after the original master flow has been demoted to the new
 * worker or stopped. The container will swap the status of the SortNode
 * and the ActionNode. All the unclaimed messages will be rolled back to the
 * uplink. So they will be processed by the ActionNode for escalations. This
 * way, there is no message lost or double fed. If you are going to implement
 * the active-active message flow cluster with a SortNode on the claim mode and
 * an ActionNode for escalations, please make sure the uplink of the SortNode
 * has the 2nd half of the cells unused. The unused partition will be reserved
 * for escalations so that the container is able to forward the escalations.
 *<br><br>
 * SortNode will not consume any messages. But it may remove the withheld
 * messages from the uplink for certain rulesets. If EXTERNAL_XA bit is set
 * on the uplink, those removed messages will be acknowledged as well.
 * Eventually, those removed and withheld messages will continue to propagate
 * downstreams as usual. This feature is controlled by XAMode of the ruleset.
 * If XAMode is not defined in a ruleset, the ruleset will inherit it from
 * the node, which will be 1 by default. If it is set to 0, all the withheld
 * messages for the ruleset will be acknowledged and removed from the uplink.
 * There is a big consequence to disable XA on a ruleset. Please be extremely
 * careful if you wnat to disable XA on any ruleset.
 *<br><br>
 * Here are considerations on when to disable XA on a ruleset. First, you may
 * want SortNode to withhold more messages than the capacity of the uplink.
 * Second, the source JMS servers may not be able to handle large amount of
 * unacknowledged messages. In these cases, XAMode of certain rulesets may be
 * set to zero explicitly to disable the XA. As you know, most of the JMS
 * vendors implement message acknowledgement via sessions. The acknowledgement
 * by SortNode may upset the XA control of the message flow.
 *<br><br>
 * You are free to choose any names for the four fixed outlinks.  But
 * SortNode always assumes the first outlink for done, the second for bypass,
 * the third for failure and the last for nohit.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SortNode extends Node {
    private int keyType = -1;
    private int bakeTime = DEFAULT_BAKETIME;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    private int sessionSize = DEFAULT_SESSION_SIZE;
    private int x0;         // reference key for int
    private long y0;        // reference key for long
    private float a0;       // reference key for float
    private double b0;      // reference key for double
    private String key0;    // reference key for String

    private KeyChain cache = null; // for storing keys
    private QuickCache idCache = null;  // for claiming ids
    private int[] outLinkMap;
    private boolean sortReversed = false;
    private boolean withClaim = false, isBaking = false, isDynamic = false;
    private final static int DEFAULT_BAKETIME = 0;
    private final static int DEFAULT_SESSION_TIMEOUT = 0;
    private final static int DEFAULT_SESSION_SIZE = 40960;
    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int ONHOLD_OUT = 4;

    public SortNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        String key;
        long[] outInfo, ruleInfo;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "sort";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("SessionSize")) != null)
            sessionSize = Integer.parseInt((String) o);
        if ((o = props.get("BakeTime")) != null)
            bakeTime = Integer.parseInt((String) o);

        if ((o = props.get("SortReversed")) != null && o instanceof String &&
            "true".equals(((String) o).toLowerCase()))
            sortReversed = true;

        if (bakeTime > 0) { // for baking process
            sortReversed = false;
            isBaking = true;
            sessionTimeout = bakeTime;
        }
        if (sessionTimeout <= 0)
            sessionTimeout = 0;
        else
            isDynamic = true;

        if (sessionSize <= 0) {
            sessionSize = 40960;
        }

        if ((o = props.get("SortKeyType")) != null && o instanceof String) {
            if ("integer".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_INT;
                cache = new IntKeyChain(name, sessionSize);
            }
            else if ("long".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_LONG;
                cache = new LongKeyChain(name, keyType, sessionSize);
            }
            else if ("float".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_FLOAT;
                cache = new FloatKeyChain(name, sessionSize);
            }
            else if ("double".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_DOUBLE;
                cache = new DoubleKeyChain(name, sessionSize);
            }
            else if ("string".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_STRING;
                cache = new StringKeyChain(name, sessionSize);
            }
            else if ("time".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_TIME;
                cache = new LongKeyChain(name, keyType, sessionSize);
            }
            else if ("sequence".equals(((String) o).toLowerCase())) {
                keyType = KeyChain.KEY_SEQUENCE;
                cache = new LongKeyChain(name, keyType, sessionSize);
            }
        }
        if (keyType < 0)
            throw(new IllegalArgumentException(name +
                ": SortKeyType is not well defined"));

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[BYPASS_OUT] = overlap[0];
        outLinkMap[FAILURE_OUT] = overlap[1];
        outLinkMap[NOHIT_OUT] = overlap[2];

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
        }

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        i = outLinkMap[NOHIT_OUT];
        i = (i >= outLinkMap[FAILURE_OUT]) ? i : outLinkMap[FAILURE_OUT];
        i = (i >= outLinkMap[BYPASS_OUT]) ? i : outLinkMap[BYPASS_OUT];
        if (++i > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        // for claim rules only
        idCache = new QuickCache(name, QuickCache.META_DEFAULT, 0, 0);
        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        cells = new CollectibleCells(name, capacity);

        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = (List) o;
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
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("PropertyName", displayPropertyName);
            ruleList.add(key, ruleInfo, rule);
            outInfo = assetList.getMetaData(outLinkMap[NOHIT_OUT]);
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
            throw(new IllegalArgumentException(name + ": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            ruleInfo = ruleList.getMetaData(i);
            if (withClaim) // disable XA for claim mode
                ruleInfo[RULE_MODE] = 0;
            if ((debug & DEBUG_INIT) > 0)
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_DMASK] + " " +
                    ruleInfo[RULE_TTL] / 1000 + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
        }
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG,name+
                " RuleName: RID PID Mode Option Mask TTL - OutName"+
                strBuf.toString()).send();

        // key for reference message
        x0 = Integer.MIN_VALUE;
        y0 = Long.MIN_VALUE;
        a0 = Float.MIN_VALUE;
        b0 = Double.MIN_VALUE;
        key0 = "";
    }

    public int updateParameters(Map props) {
        Object o;
        int i, n;

        n = super.updateParameters(props);
        if ((o = props.get("BakeTime")) != null) {
            i = Integer.parseInt((String) o);
            if (i > 0 && bakeTime > 0 && i != bakeTime) {
                bakeTime = i;
                n++;
            }
        }
        else if (bakeTime != DEFAULT_BAKETIME) { // reset to default
            bakeTime = DEFAULT_BAKETIME;
            n++;
        }
        if ((o = props.get("SessionTimeout")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != sessionTimeout) {
                sessionTimeout = i;
                n++;
            }
        }
        else if (sessionTimeout != DEFAULT_SESSION_TIMEOUT) { //reset to default
            sessionTimeout = DEFAULT_SESSION_TIMEOUT;
            n++;
        }
        if ((o = props.get("SessionSize")) != null) {
            i = Integer.parseInt((String) o);
            if (i > 0 && i != sessionSize) {
                sessionSize = i;
                n++;
            }
        }
        else if (sessionSize != DEFAULT_SESSION_SIZE) { // reset to default
            sessionSize = DEFAULT_SESSION_SIZE;
            n++;
        }

        if (bakeTime > 0) { // for baking process
            sortReversed = false;
            isBaking = true;
            sessionTimeout = bakeTime;
        }
        if (sessionTimeout <= 0)
            sessionTimeout = 0;
        else
            isDynamic = true;

        if (sessionSize <= 0)
            sessionSize = 40960;

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
        String key, str, ruleName, preferredOutName;
        long[] outInfo;
        int i, j, k, n, id;

        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a rule"));
        if (ruleInfo == null || ruleInfo.length <= RULE_TIME)
            throw(new IllegalArgumentException("ruleInfo is not well defined"));
        ruleName = (String) ph.get("Name");
        if (ruleName == null || ruleName.length() == 0)
            throw(new IllegalArgumentException("ruleName is not defined"));

        rule = new HashMap<String, Object>();
        rule.put("Name", ruleName);
        preferredOutName = (String) ph.get("PreferredOutLink");
        if(preferredOutName !=null && !assetList.containsKey(preferredOutName)){
            preferredOutName = assetList.getKey(outLinkMap[NOHIT_OUT]);
            new Event(Event.WARNING, name + ": OutLink for " +
                ruleName + " not well defined, use the default: "+
                preferredOutName).send();
        }

        rule.put("Filter", new MessageFilter(ph));
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        if ((o = ph.get("ResetOption")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = RESET_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if (preferredOutName != null) { // bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else { // for sort ruleset
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
                rule.put("KeyTemplate", new Template((String) o));
                if((o=ph.get("KeySubstitution")) != null && o instanceof String)
                    rule.put("KeySubstitution",new TextSubstitution((String)o));
            }

            if ((o = ph.get("TimePattern")) != null && o instanceof String)
                rule.put("DateFormat", new SimpleDateFormat((String) o));

            if ((o = ph.get("IDTemplate")) != null && o instanceof String)
                rule.put("IDTemplate", new Template((String) o));

             // store XAMode into RULE_MODE
            if ((o = ph.get("XAMode")) != null)
                ruleInfo[RULE_MODE] = Long.parseLong((String) o);
            else
                ruleInfo[RULE_MODE] = xaMode;

            if ((o = ph.get("RuleType")) != null &&
                "claim".equalsIgnoreCase((String) o)) {
                withClaim = true;
                if ((o = ph.get("TimeToLive")) != null)
                    ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
                ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
                ruleInfo[RULE_PID] = TYPE_ACTION;
            }
            else {
                ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
                ruleInfo[RULE_PID] = TYPE_SORT;
            }
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
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
     * It evaluates the key and compares it with the threshold.  If it is
     * larger than the threshold, the message will be withheld to bake.
     * If it is less than the threshold, the message will be routed to
     * BYPASS_OUT. Otherwise, the message will be routed to RESULT_OUT at once.
     */
    private int bake(long currentTime, String key, int cid, int rid,
        DateFormat dateFormat, Message inMessage) {
        Date d;
        int i, delta;
        int x;
        long y;
        float a;
        double b;
        if (key == null || key.length() <= 0)
            return FAILURE_OUT;

        delta = 0;
        try {
            i = ONHOLD_OUT;
            switch (keyType) {
              case KeyChain.KEY_INT:
                x = Integer.parseInt(key);
                if (isBaking) { // bake process
                    if (x > x0) { // larger than threshold so keep it
                        cache.insert(new Integer(x), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else if (x == x0) { // same as the threshold so let it go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass 
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Integer(x), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (x > x0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        x0 = x;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_LONG:
                y = Long.parseLong(key);
                if (isBaking) { // dynamic session
                    if (y > y0) { // larger than threshold so keep it
                        cache.insert(new Long(y), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else if (y == y0) { // same as the threshold so let go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Long(y), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (y > y0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        y0 = y;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_FLOAT:
                a = Float.parseFloat(key);
                if (isBaking) { // dynamic session
                    if (a > a0) { // larger than threshold so keep it
                        cache.insert(new Float(a), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else if (a == a0) { // same as the threshold so let go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Float(a), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (a > a0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        a0 = a;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_DOUBLE:
                b = Double.parseDouble(key);
                if (isBaking) { // dynamic session
                    if (b > b0) { // larger than threshold so keep it
                        cache.insert(new Double(b), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else if (b == b0) { // same as the threshold so let go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Double(b), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (b > b0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        b0 = b;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_STRING:
                if (isBaking) { // dynamic session
                    delta = key.compareTo(key0);
                    if (delta > 0) { // larger than threshold so keep it
                        cache.insert(key, currentTime, new int[]{cid, rid},
                            inMessage);
                    }
                    else if (delta == 0) { // same as the threshold so let go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass 
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(key, currentTime, new int[]{cid, rid}, inMessage);
                }
                else { // first series without cahce
                    if (key.compareTo(key0) > 0) { // larger than threshold
                        i = RESULT_OUT;
                        key0 = key;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_TIME:
                d = dateFormat.parse(key, new ParsePosition(0));
                if (d != null)
                    y = d.getTime();
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to parse " + key + " with "+ key.length()).send();
                    return i;
                }
                if (isBaking) { // dynamic session
                    if (y > y0) { // larger than threshold so keep it
                        cache.insert(new Long(y), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else if (y == y0) { // same as the threshold so let go
                        i = RESULT_OUT;
                    }
                    else { // less than threshold so let bypass
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Long(y), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (y > y0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        y0 = y;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              case KeyChain.KEY_SEQUENCE:
                y = Long.parseLong(key);
                if (isBaking) { // dynamic session
                    if (y == y0 + 1 && !withClaim) { // next consecutive number
                        i = RESULT_OUT;
                        y0 = y;
                    }
                    else if (y > y0) { // larger than threshold so keep it
                        cache.insert(new Long(y), currentTime,
                            new int[]{cid, rid}, inMessage);
                    }
                    else { // less than or eq to threshold so let bypass
                        i = (withClaim) ? RESULT_OUT : BYPASS_OUT;
                    }
                }
                else if (isDynamic) { // just cache
                    cache.add(new Long(y), currentTime, new int[]{cid, rid},
                        inMessage);
                }
                else { // first series without cahce
                    if (y > y0) { // larger than threshold so keep it
                        i = RESULT_OUT;
                        y0 = y;
                    }
                    else { // less than threshold so let bypass 
                        i = BYPASS_OUT;
                    }
                }
                break;
              default:
                i = FAILURE_OUT;
            }
        }
        catch (Exception e) {
            i = FAILURE_OUT;
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to parse " + key + "\n" + Event.traceStack(e)).send();
        }
        return i;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String id, msgStr = null, ruleName = null;
        Object o;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        Template template = null, idTemp = null;
        TextSubstitution sub = null;
        DateFormat dateFormat = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, st, previousTime, ttl, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, size, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        boolean acked = ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0); 
        boolean ckBody = false;
        byte[] buffer = new byte[bufferSize];

        i = in.getCapacity();
        if (capacity != i) { // assume it only occurs at startup
            new Event(Event.WARNING, name + ": " + in.getName() +
                " has the different capacity of " + i + " from " +
                capacity).send();
            capacity = i;
            msgList.clear();
            msgList = new AssetList(name, capacity);
            cells.clear();
            cells = new CollectibleCells(name, capacity);
        }

        // initialize filters
        n = ruleList.size();
        filters = new MessageFilter[n];
        browser = ruleList.browser();
        ruleMap = new int[n];
        ttl = sessionTimeout;
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
            if (filters[i] != null) {
                if ((!ckBody) && filters[i].checkBody())
                    ckBody = true;
            }
            ruleMap[i++] = rid;
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_TTL] > 0 && ruleInfo[RULE_TTL] > ttl)
                ttl = ruleInfo[RULE_TTL];
        }
        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;

        // update assetList
        n = out.length;
        for (i=0; i<n; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

        size = cache.size();
        n = ruleMap.length;
        previousRid = -1;
        previousTime = 0L;
        ii = 0;
        wt = 5L;
        sz = msgList.size();
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((cid = in.getNextCell(wt)) < 0) {
                if (++ii >= 10) {
                    feedback(in, -1L);
                    sz = msgList.size();
                    if (sz <= 0)
                        wt = waitTime;
                    ii = 0;
                }
                else {
                    if (sz > 0)
                        feedback(in, -1L);
                    continue;
                }
            }
            currentTime = System.currentTimeMillis();
            if (isDynamic && // for baking or dynamic sessions
                currentTime - previousTime >= sessionTimeout) {
                if (size > 0) { // flush expired cache
                    st = currentTime - previousTime;
                    i = flush(currentTime, in, buffer);
                    size = cache.size();
                    if ((debug & DEBUG_REPT) > 0)
                        new Event(Event.DEBUG, name + ": flushed " +
                            i + " msgs due to timeout: " +
                            (st - sessionTimeout) + "/" + size).send();
                    currentTime = System.currentTimeMillis();
                    if (size > 0) // reset previousTime
                        previousTime = (isBaking) ? cache.getCTime() :
                            cache.getMTime();
                    else
                        previousTime = currentTime;
                }
                else
                    previousTime = currentTime;
                if (withClaim && idCache.size() > 0 && // check id cache
                    currentTime >= ttl + idCache.getMTime()) {
                    i = disfragment(currentTime);
                    if (i > 0 && (debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + ": " + i +
                            " ids expired").send();
                }
            }
            if (cid < 0)
                continue;

            wt = 5L;
            if ((inMessage = (Message) in.browse(cid)) == null) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": " + Event.traceStack(
                   new JMSException("null msg from " + in.getName()))).send();
                continue;
            }

            filter = null;
            msgStr = null;
            rid = 0;
            i = 0;
            try {
                if (ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                for (i=1; i<n; i++) {
                    if (filters[i].evaluate(inMessage, msgStr)) {
                        rid = ruleMap[i];
                        filter = filters[i];
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
                new Event(Event.ERR, str + " failed to apply the filter "+ i+
                    ": " + Event.traceStack(e)).send();
                i = -1;
            }

            if (rid != previousRid) {
                ruleName = ruleList.getKey(rid);
                ruleInfo = ruleList.getMetaData(rid);
                rule = (Map) ruleList.get(rid);
                propertyName = (String[]) rule.get("PropertyName");
                if (ruleInfo[RULE_PID] != TYPE_BYPASS) {
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                    dateFormat = (DateFormat) rule.get("DateFormat");
                    if (withClaim)
                        idTemp = (Template) rule.get("IDTemplate");
                }
                previousRid = rid;
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_SORT) {//bake engine for sorting
                String key;
                i = cache.removable();
                if (size + i >= sessionSize) { // cache is full
                    if (i > 0) // try to truncate
                        i = cache.truncate(0);
                    if (i <= 0) {
                        in.putback(cid);
                        new Event(Event.WARNING, name + ": cache is full: " +
                            (size - sessionSize) + "/" + size).send();
                        try {
                            Thread.sleep(10*waitTime);
                        }
                        catch (Exception e) {
                        }
                        continue;
                    }
                }

                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (withClaim) { // check id cache first
                    if (idTemp != null)
                        id = MessageUtils.format(inMessage, buffer, idTemp);
                    else
                        id = key;

                    if (idCache.containsKey(id)) { // msg already processed
                        i = BYPASS_OUT;
                    }
                    else { // for baking without msg
                        i = bake(currentTime, key, cid, rid, dateFormat, null);
                    }
                }
                else { // for normal baking
                    i = bake(currentTime, key, cid, rid, dateFormat, inMessage);
                }

                if (i == ONHOLD_OUT) { // new msg added to cache
                    ruleInfo[RULE_PEND] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    if (ruleInfo[RULE_MODE] <= 0) { // XA is off 
                        if (acked) try {
                            inMessage.acknowledge();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName +
                                " failed to ack msg at " + cid + " on " + key+
                                ": " + Event.traceStack(e)).send();
                        }
                        if (!withClaim) // msg cached for normal baking
                            in.remove(cid);
                        // for claim mode, msg is not cached, so it can not be
                        // removed here. Claimed msg will be removed in claim()
                    }
                    size = cache.size();
                    if (size == 1) // reset start-time
                        previousTime = (isBaking) ? cache.getCTime() :
                            cache.getMTime();
                    if ((debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + " propagate: " + key +
                            " = "+cid+ ":" + rid +" "+ i +"/"+ size).send();
                    if (size >= sessionSize) {
                        currentTime = System.currentTimeMillis();
                        i = flush(currentTime, in, buffer);
                        size = cache.size();
                        if ((debug & DEBUG_REPT) > 0)
                            new Event(Event.DEBUG, name + ": flushed " + i +
                                " msgs due to oversize: "+(i+size-sessionSize)+
                                "/" + size).send();
                        currentTime = System.currentTimeMillis();
                        if (size > 0) // reset previousTime
                            previousTime = (isBaking) ? cache.getCTime() :
                                cache.getMTime();
                        else
                            previousTime = currentTime;
                    }
                    continue;
                }
                else if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + " propagate: " + key +
                        " = " + cid + ":" + rid +" "+ i +"/"+ size).send();

                if (i == RESULT_OUT && keyType == KeyChain.KEY_SEQUENCE &&
                    size > 0) {
                    currentTime = System.currentTimeMillis();
                    i = flush(currentTime, in, buffer);
                    size = cache.size();
                    if ((debug & DEBUG_REPT) > 0)
                        new Event(Event.DEBUG, name + ": flushed " + i +
                            " msgs for sequence: " + y0 + "/" + size).send();
                    currentTime = System.currentTimeMillis();
                    if (size > 0) // reset previousTime
                        previousTime = (isBaking) ? cache.getCTime() :
                            cache.getMTime();
                    else
                        previousTime = currentTime;
                }
                oid = outLinkMap[i];
            }
            else if (ruleInfo[RULE_PID] == TYPE_ACTION) { // for claim
                String key;
                int j;
                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (idTemp != null)
                    id = MessageUtils.format(inMessage, buffer, idTemp);
                else
                    id = key;

                if (id == null || id.length() <= 0) {
                    i = FAILURE_OUT;
                }
                else if (!idCache.containsKey(id)) { // first appearance
                    try {
                        i = idCache.insert(id, currentTime,
                            (int) ruleInfo[RULE_TTL], new int[]{rid}, null);
                        ruleInfo[RULE_PEND] ++;
                        i = NOHIT_OUT;
                        if (idCache.size() == 1) // reset start-time of session
                            idCache.setStatus(idCache.getStatus(),currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to insert " + id + ": " +
                            Event.traceStack(e)).send();
                    }
                }
                else if (idCache.isExpired(id, currentTime)) { // expired
                    int[] meta = idCache.getMetaData(id);
                    long[] info;
                    if (meta != null && meta.length > 0 &&
                        (info = ruleList.getMetaData(meta[0])) != null)
                        info[RULE_PEND] --;
                    try {
                        i = idCache.insert(id, currentTime,
                            (int) ruleInfo[RULE_TTL], new int[]{rid}, null);
                        ruleInfo[RULE_PEND] ++;
                        i = NOHIT_OUT;
                        if (idCache.size() == 1) // reset start-time of session
                            idCache.setStatus(idCache.getStatus(), currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to reinsert " + id + ": " +
                            Event.traceStack(e)).send();
                    }
                }
                else { // existing id
                    i = NOHIT_OUT;
                }

                j = claim(currentTime, key, ruleName, dateFormat, in,
                    outLinkMap[BYPASS_OUT], buffer);
                if (j > 0) {
                    size = cache.size();
                    if (size > 0)
                        previousTime = cache.getCTime();
                    if ((debug & DEBUG_REPT) > 0)
                        new Event(Event.DEBUG, name + ": " + ruleName +
                            " claimed " + j + " msgs out of " + size).send();
                }
                oid = outLinkMap[i];
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + " propagate: " + key +
                        " = " + cid + ":" + rid +" "+ i +"/"+ size).send();
            }
            else { // preferred or nohit
                oid = (int) ruleInfo[RULE_OID];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName + " bypassed msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try { // post format
                switch ((int) ruleInfo[RULE_OPTION]) {
                  case RESET_MAP:
                    MessageUtils.resetProperties(inMessage);
                    if (inMessage instanceof MapMessage)
                       MessageUtils.resetMapBody((MapMessage) inMessage);
                    break;
                  case RESET_ALL:
                    MessageUtils.resetProperties(inMessage);
                    break;
                  case RESET_SOME:
                    if (!(inMessage instanceof JMSEvent))
                        MessageUtils.resetProperties(inMessage);
                    break;
                  case RESET_NONE:
                  default:
                    break;
                }

                filter.format(inMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to format the msg: " + Event.traceStack(e)).send();
            }

            if (ruleInfo[RULE_PID] == TYPE_SORT ||
                ruleInfo[RULE_PID] == TYPE_ACTION) {//no putbcak for sort, claim
                i = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
                if (i > 0)
                    count ++;
                else { // failed to passthru so force it to FAILURE_OUT
                    i = forcethru(currentTime, inMessage, in, rid,
                        outLinkMap[FAILURE_OUT], cid);
                    if (i < 0)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg at " + cid +
                            " with msg dropped").send();
                    else {
                        count ++;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg at " + cid +
                            " with msg " +((i > 0) ? "redirected" : "flushed")+ 
                            " to " + outLinkMap[FAILURE_OUT]).send();
                    }
                }
            }
            else {
                i = passthru(currentTime, inMessage, in, rid, oid, cid, 0);
                if (i > 0)
                    count ++;
            }
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * returns the number of messages claimed successfully
     */
    private int claim(long currentTime, String key, String ruleName,
        DateFormat dateFormat, XQueue in, int oid, byte[] buffer) {
        int i, j, k, m, n, cid, rid, count = 0, previousRid = -1;
        int[] meta;
        long[] ruleInfo = null;
        Message inMessage;
        Map rule;
        Date d;
        int x;
        long y;
        float a;
        double b;
        if (key == null || key.length() <= 0)
            return 0;
        n = cache.size();
        if (n <= 0)
            return 0;

        i = 0;
        try {
            switch (keyType) {
              case KeyChain.KEY_INT:
                x = Integer.parseInt(key);
                if (x <= x0) // already missed
                    return 0;
                x0 = x;
                i = cache.find(new Integer(x));
                break;
              case KeyChain.KEY_LONG:
                y = Long.parseLong(key);
                if (y <= y0) // already missed
                    return 0;
                y0 = y;
                i = cache.find(new Long(y));
                break;
              case KeyChain.KEY_FLOAT:
                a = Float.parseFloat(key);
                if (a <= a0) // already missed
                    return 0;
                a0 = a;
                i = cache.find(new Float(a));
                break;
              case KeyChain.KEY_DOUBLE:
                b = Double.parseDouble(key);
                if (b <= b0) // already missed
                    return 0;
                b0 = b;
                i = cache.find(new Double(b));
                break;
              case KeyChain.KEY_TIME:
                d = dateFormat.parse(key, new ParsePosition(0));
                if (d != null)
                    y = d.getTime();
                else {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to parse "+key+ " with "+ key.length()).send();
                    return 0;
                }
                if (y <= y0) // already missed
                    return 0;
                y0 = y;
                i = cache.find(new Long(y));
                break;
              case KeyChain.KEY_STRING:
              default:
                if (key.compareTo(key0) <= 0)
                    return 0;
                key0 = key;
                i = cache.find(key);
                break;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to parse " + key + "\n" + Event.traceStack(e)).send();
            return 0;
        }

        if (i == -1)
            m = 0;
        else if (i >= 0)
            m = i + 1;
        else
            m = -i - 1;
        
        if ((debug & DEBUG_UPDT) > 0)
            new Event(Event.DEBUG, name + " " + ruleName + ": claiming " + m +
                " cached msgs out of " + n + " by " + key).send();

        for (i=0; i<m; i++) { // loop thru all fully baked keys
            meta = cache.getMetaData(i);
            if (meta == null || meta.length < 2) { // bad meta data
                new Event(Event.ERR, name + " " + ruleName +
                    ": claimed null metadata at " + i).send();
                continue;
            }
            cid = meta[0];
            rid = meta[1];
            inMessage = (Message) in.browse(cid);
            // remove the claimed msg here after its retrieval
            in.remove(cid);
            if (inMessage == null) { // bad msg
                new Event(Event.ERR, name + " " + ruleName +
                    ": claimed a null msg at " + cid).send();
                continue;
            }
            if (rid != previousRid) {
                rule = (Map) ruleList.get(rid);
                ruleInfo = ruleList.getMetaData(rid);
                previousRid = rid;
            }
            if (displayMask > 0) try { // display the message
                String msgStr = null;
                if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName +" claimed " +
                    (count + 1) + ":" + MessageUtils.display(inMessage,
                    msgStr, displayMask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            // claimed msg has been just removed from uplink
            // so flush the msg without tracking via msgList
            j = passthru(currentTime, inMessage, in, rid, oid, -1, 0);
            if (j > 0) {
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                count ++;
            }
            else {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to claim the msg at " + i).send();
            }
        }

        if (m > 0) // truncate
            cache.truncate(m);

        return count;
    }

    /**
     * It flushes the fully baked messages and returns the number of messages
     * flushed successfully
     */
    private int flush(long currentTime, XQueue in, byte[] buffer) {
        int i, k, m, n, cid, rid, count = 0, dspBody, previousRid = -1;
        int[] meta;
        long[] ruleInfo = null;
        Message inMessage;
        MessageFilter filter = null;
        String[] propertyName = null;
        Map rule;
        String ruleName = null;
        Object o;
        n = cache.size();
        if (n <= 0)
            return 0;

        if (isBaking) { // baking process
            m = cache.locate(currentTime, bakeTime);
            if (m > 0) { // update leading key which is fully baked
                o = cache.getKey(m-1);
                setCachedKey(o);
            }
        }
        else { // no need to check
            cache.sort();
            m = n;
        }

        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;
        if ((debug & DEBUG_UPDT) > 0)
            new Event(Event.DEBUG, name + " flush: located "+m+ " msgs out of "+
                n + " since " + (new Date(cache.getCTime())).toString()).send();

        for (i=0; i<m; i++) { // loop thru all fully baked keys
            k = (sortReversed) ? m - i - 1 : i;
            meta = cache.getMetaData(k);
            if (meta == null || meta.length < 2) { // bad meta data
                new Event(Event.ERR, name+": null metadata in flush "+k).send();
                continue;
            }
            cid = meta[0];
            rid = meta[1];
            if (withClaim) // msg has not been removed from in yet
                inMessage = (Message) in.browse(cid);
            else // msg stored in cache
                inMessage = (Message) cache.get(k);
            if (inMessage == null) { // bad msg
                new Event(Event.ERR, name + ": null msg in flush " + k).send();
                continue;
            }
            if (rid != previousRid) {
                ruleName = ruleList.getKey(rid);
                rule = (Map) ruleList.get(rid);
                filter = (MessageFilter) rule.get("Filter");
                propertyName = (String[]) rule.get("PropertyName");
                ruleInfo = ruleList.getMetaData(rid);
                previousRid = rid;
            }
            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                String msgStr = null;
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName +" sorted " +
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try { // post format
                switch ((int) ruleInfo[RULE_OPTION]) {
                  case RESET_MAP:
                    MessageUtils.resetProperties(inMessage);
                    if (inMessage instanceof MapMessage)
                       MessageUtils.resetMapBody((MapMessage) inMessage);
                    break;
                  case RESET_ALL:
                    MessageUtils.resetProperties(inMessage);
                    break;
                  case RESET_SOME:
                    if (!(inMessage instanceof JMSEvent))
                        MessageUtils.resetProperties(inMessage);
                    break;
                  case RESET_NONE:
                  default:
                    break;
                }

                filter.format(inMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to format the msg: "+ Event.traceStack(e)).send();
            }

            if (ruleInfo[RULE_MODE] > 0 || withClaim) { // XA is ON or for claim
                int oid = (withClaim) ? outLinkMap[FAILURE_OUT] : RESULT_OUT;
                int j = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
                if (j > 0) {
                    count ++;
                    ruleInfo[RULE_PEND] --;
                }
                else { // failed to passthru
                    j = forcethru(currentTime, inMessage, in, rid,
                        outLinkMap[FAILURE_OUT], cid);
                    if (j > 0)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg at " + cid +
                            " with msg redirected to " +
                            outLinkMap[FAILURE_OUT]).send();
                    else if (j == 0)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg at " + cid +
                            " with msg flushed to " +
                            outLinkMap[FAILURE_OUT]).send();
                    else
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg at " + cid +
                            " with msg dropped").send();
                }
            }
            else { // XA is off
                int oid = RESULT_OUT;
                int j = passthru(currentTime, inMessage, in, rid, oid, -1, 0);
                if (j > 0) {
                    ruleInfo[RULE_PEND] --;
                    ruleInfo[RULE_COUNT] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    count ++;
                }
                else {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to flush the msg at " + k).send();
                }
            }
        }
        if (count > 0)
            feedback(in, -1L);

        if (!isBaking) // no baking
            cache.clear();
        else if (m > 0) // truncate
            cache.truncate(m);

        return count;
    }

    /**
     * It checks all keys of the ID cache to see if any of them has
     * been expired or not.  If yes, it will update the RULE_PEND and
     * disfragment the cache.  Meanwhile, it will look for the next
     * expiration time and resets the MTime of the cache.  The number
     * of expired keys will be returned upon success.
     */
    private int disfragment(long currentTime) {
        int k, rid;
        int[] info;
        long t, tm;
        long[] ruleInfo;
        Set<String> keys = idCache.keySet();
        tm = currentTime + sessionTimeout;
        k = 0;
        for (String key : keys) {
            if (!idCache.isExpired(key, currentTime)) {
                t = idCache.getTimestamp(key) + idCache.getTTL(key);
                if (t < tm)
                    tm = t;
                continue;
            }
            info = idCache.getMetaData(key);
            if (info == null)
                continue;
            rid = info[0];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo != null) {
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_TIME] = currentTime;
            }
            k ++;
        }
        if (k > 0)
            idCache.disfragment(currentTime);
        idCache.setStatus(idCache.getStatus(), tm - sessionTimeout);

        return k;
    }

    public int getKeyType() {
        return keyType;
    }

    /**
     * It returns the key value cached for bake process.  It is used as
     * the threshold to determine which messages should be baked.  According
     * to the keyType, the value will be wrapped into the Object of the
     * same data type, like Integer for int, etc.
     */
    public Object getCachedKey() {
        switch (keyType) {
          case KeyChain.KEY_INT:
            return new Integer(x0);
          case KeyChain.KEY_TIME:
          case KeyChain.KEY_SEQUENCE:
          case KeyChain.KEY_LONG:
            return new Long(y0);
          case KeyChain.KEY_FLOAT:
            return new Float(a0);
          case KeyChain.KEY_DOUBLE:
            return new Double(b0);
          case KeyChain.KEY_STRING:
          default:
            return key0;
        }
    }

    /**
     * It sets the key value cached for bake process.  It is used as
     * the threshold to determine which messages should be baked.  According
     * to the keyType, the value will be wrapped into the Object of the
     * same data type, like Integer for int, etc.
     */
    public void setCachedKey(Object o) {
        switch (keyType) {
          case KeyChain.KEY_INT:
            if (o instanceof Integer)
                x0 = ((Integer) o).intValue();
            else
                throw(new IllegalArgumentException(name + " different type: "+
                    KeyChain.KEY_INT));
            break;
          case KeyChain.KEY_TIME:
          case KeyChain.KEY_SEQUENCE:
          case KeyChain.KEY_LONG:
            if (o instanceof Long)
                y0 = ((Long) o).longValue();
            else
                throw(new IllegalArgumentException(name + " different type: "+
                    KeyChain.KEY_LONG));
            break;
          case KeyChain.KEY_FLOAT:
            if (o instanceof Float)
                a0 = ((Float) o).floatValue();
            else
                throw(new IllegalArgumentException(name + " different type: "+
                    KeyChain.KEY_FLOAT));
            break;
          case KeyChain.KEY_DOUBLE:
            if (o instanceof Double)
                b0 = ((Double) o).doubleValue();
            else
                throw(new IllegalArgumentException(name + " different type: "+
                    KeyChain.KEY_DOUBLE));
            break;
          case KeyChain.KEY_STRING:
          default:
            if (o instanceof String)
                key0 = (String) o;
            else
                throw(new IllegalArgumentException(name + " different type: "+
                    KeyChain.KEY_STRING));
            break;
        }
    }

    /**
     * cleans up MetaData for all XQs and messages
     */
    public void resetMetaData(XQueue in, XQueue[] out) {
        int[] list;
        int i, n, mid;

        super.resetMetaData(in, out);
        if (!withClaim)
            return;

        // rollback cached msgs
        n = cache.size();
        for (i=n-1; i>=0; i--) {
            list = cache.getMetaData(i);
            if (list == null || list.length < 3)
                continue;
            mid = list[1];
            if (mid < 0)
                continue;
            in.putback(mid);
        }
        cache.clear();
        idCache.clear();
    }

    /** lists all propagating and cached messages */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h, ph;
        StringBuffer strBuf;
        String key, str, text;
        long[] ruleInfo;
        int[] info;
        int i, cid, k, n, rid;
        long tm;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (cache.size() <= 0 || h == null)
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

        n = cache.size();
        if (n <= 0)
            return h;

        hasSummary = (displayPropertyName != null &&
            displayPropertyName.length > 0);

        for (i=0; i<n; i++) {
            info = (int[]) cache.getMetaData(i);
            if (info == null)
                continue;
            rid = info[2];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0)
                continue;

            if (ruleInfo[RULE_MODE] > 0) { // xa is on
                cid = info[1];
                if (ph.containsKey(String.valueOf(cid))) // duplicate
                    continue;
                if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                    continue;
                msg = (Message) xq.browse(cid);
            }
            else { // xa is off
                cid = -1;
                msg = (Message) cache.get(i);
            }
            if (msg == null)
                continue;

            key = ruleList.getKey(rid);
            if (key == null)
                key = "-";

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
                str = MessageUtils.display(msg, null,
                    MessageUtils.SHOW_NOTHING, displayPropertyName);
                if (str == null)
                    str = "";
            }
            catch (Exception e) {
                str = e.toString();
            }

            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + k + "</ID>");
                strBuf.append("<CellID>" + cid + "</CellID>");
                strBuf.append("<Status>PENDING</Status>");
                strBuf.append("<Rule>" + key + "</Rule>");
                strBuf.append("<OutLink>-</OutLink>");
                strBuf.append("<Time>" + Utils.escapeXML(text) + "</Time>");
               strBuf.append("<Summary>"+Utils.escapeXML(str)+"</Summary>");
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
        super.close();
        cache.clear();
        idCache.clear();
    }

    protected void finalize() {
        close();
    }
}
