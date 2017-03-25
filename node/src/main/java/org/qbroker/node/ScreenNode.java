package org.qbroker.node;

/* ScreenNode.java - a MessageNode screening JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.QList;
import org.qbroker.common.AssetList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.KeyChain;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.event.Event;

/**
 * ScreenNode picks up the JMS messages from an input XQ and screens them
 * according to their content and the pre-defined rulesets.  It filters them
 * out into four outlinks: done for the messages passing through the screening
 * process, nohit for those messages do not belong to any pre-defined rulesets,
 * failure for the messages failed in the screening process and bypass for the
 * those messages falling out of screen process. Since ScreenNode does not
 * consume any messages, any in-coming messages has to find a way out via one
 * of the four outlinks.
 *<br/><br/>
 * ScreenNode contains a number of pre-defined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each ruleset
 * defines a unique message group.  The ruleset also defines the screening
 * options for the messages in each group as well as other parameters used
 * in the screening process, such as TimeToLive and XAMode. With different
 * screen options, ScreenNode withholds messages until their sessions time
 * out. Therefore, each group maintains its own cache and the session to
 * withhold messages temporarily. KeyTemplate is used to extract the keys
 * from of the messages. These keys will be used to track the withheld
 * messages in the cache. TimeToLive determines when to expire the session
 * for the cache. Further more, ScreenNode always creates one extra ruleset,
 * nohit. The ruleset of nohit is for all the messages not hitting any of
 * the patterns.
 *<br/><br/>
 * ScreenNode will not consume any messages. But it may remove the withheld
 * messages from the uplink for certain rulesets. If EXTERNAL_XA bit is set
 * on the uplink, those removed messages will be acknowledged as well.
 * Eventually, those removed and withheld messages will continue to propagate
 * downstreams as usual. This feature is controlled by XAMode of the ruleset.
 * If XAMode is not defined in a ruleset, the ruleset will inherit it from
 * the node, which will be 1 by default. If it is set to 0, all the withheld
 * messages for the ruleset will be acknowledged and removed from the uplink.
 * There is a big consequence to disable XA on a ruleset. Please be extremely
 * careful if you wnat to disable XA on any ruleset.
 *<br/><br/>
 * Here are considerations on when to disable XA on a ruleset. First, you may
 * want ScreenNode to withhold more messages than the capacity of the uplink.
 * Second, the source JMS servers may not be able to handle large amount of
 * unacknowledged messages. In these cases, XAMode of certain rulesets may be
 * set to zero explicitly to disable the XA. As you know, most of the JMS
 * vendors implement message acknowledgement via sessions. The acknowledgement
 * by ScreenNode may upset the XA control of the message flow.
 *<br/><br/>
 * ScreenNode also supports the termination rulesets. A terminate ruleset is
 * similar to a screen ruleset.  The only difference is that the former has
 * also defined TargetRule that specifies the name of a screen ruleset for
 * caching terminations. Its KeyTemplate is used to generate the screen key
 * so that the withheld message will be flushed to terminate its session. Each
 * message of a termination ruleset will be routed to the outlink of done.
 *<br/></br>
 * ScreenNode is also able to monitor the load level report of its first
 * outlink if its report is defined. In this case, all cache sessions will be
 * frozen temporarily if the load level is too high.  It means ScreenNode will
 * ignore all TTLs and treats all cache sessions without expirations.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * ScreenNode always assumes the first outlink for done, the second for bypass,
 * the third for failure and the last for nohit.  Any two or more outlinks can
 * share the same outlink name.  It means these outlinks are sharing the same
 * output channel.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ScreenNode extends Node {
    private int sessionSize = 64;
    private int heartbeat = 60000;
    private int cacheStatus;
    private String reportName = null;
    private int[] threshold, outLinkMap;
    private QList cacheList = null;    // active caches with rid

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int ONHOLD_OUT = 4;
    public final static int OPTION_NONE = 0;
    public final static int OPTION_MIN = 1;
    public final static int OPTION_MAX = 2;
    public final static int OPTION_FIRST = 3;
    public final static int OPTION_LAST = 4;
    public final static int OPTION_FIRST_MIN = 5;
    public final static int OPTION_FIRST_MAX = 6;
    public final static int OPTION_FIRST_LAST = 7;
    public final static int OPTION_END = 7;

    public ScreenNode(Map props) {
        super(props);
        Object o;
        List list;
        String key;
        Browser browser;
        Map<String, Object> rule;
        long[] outInfo, ruleInfo;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "screen";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        if (heartbeat <= 0)
            heartbeat = 60000;
        if ((o = props.get("SessionSize")) != null)
            sessionSize = Integer.parseInt((String) o);
        if ((o = props.get("Threshold")) != null) {
            threshold =  TimeWindows.parseThreshold((String) o);
            threshold[0] /= 1000;
            threshold[1] /= 1000;
            threshold[2] /= 1000;
        }
        else {
            threshold = new int[] {10, 50, 100};
        }

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[FAILURE_OUT] = overlap[0];
        outLinkMap[NOHIT_OUT] = overlap[1];

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

        if (outLinkMap[NOHIT_OUT] >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        // set reportName for RESULT_OUT
        o = assetList.get(outLinkMap[RESULT_OUT]);
        reportName = (String) ((Object[]) o)[ASSET_URI];
        cacheStatus = QuickCache.CACHE_ON;

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        cacheList = new QList(name, ruleSize);
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
            throw(new IllegalArgumentException(name+": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_TTL]/1000 + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_EXTRA] + "  "+ ruleInfo[RULE_DMASK] + " - "+
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name +
                " RuleName: RID PID OPTION TTL MODE TYPE MASK - OutName" +
                strBuf.toString()).send();
        }
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
        if ((o = props.get("SessionSize")) != null) {
            i = Integer.parseInt((String) o);
            if (i >= 0 && i != sessionSize) {
                sessionSize = i;
                n++;
            }
        }
        if ((o = props.get("Threshold")) != null) {
            int[] th =  TimeWindows.parseThreshold((String) o);
            th[0] /= 1000;
            th[1] /= 1000;
            th[2] /= 1000;
            if (th[0] != threshold[0]) {
                threshold[0] = th[0];
                n++;
            }
            if (th[1] != threshold[1]) {
                threshold[1] = th[1];
                n++;
            }
            if (th[2] != threshold[2]) {
                threshold[2] = th[2];
                n++;
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
        QuickCache cache;
        String key, str, ruleName, preferredOutName;
        long[] outInfo;
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

        // TimeToLive is for session
        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000*Integer.parseInt((String) o);

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("TargetRule")) != null) { //for termination ruleset
            str = (String) o;
            rule.put("TargetRule", str);
            i = ruleList.getID(str);
            if (i <= 0) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to find TargetRule " + str).send();
                ruleInfo[RULE_PID] = 0;
            }
            else {
                long[] ri = ruleList.getMetaData(i);
                if ((int) ri[RULE_PID] != TYPE_SCREEN)
                    new Event(Event.ERR, name + ": " + ruleName +
                        " found TargetRule " + str + " with wrong option " +
                        ri[RULE_PID]).send();
                ruleInfo[RULE_PID] = i;
            }
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
                rule.put("KeyTemplate", new Template((String) o));
                if((o = ph.get("KeySubstitution"))!=null && o instanceof String)
                    rule.put("KeySubstitution",new TextSubstitution((String)o));
            }
            else {
                new Event(Event.WARNING, name + ": KeyTemplate is not well " +
                    " defined. Instead, the default will be used for " +
                    ruleName).send();
            }
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
        }
        else { // for screen ruleset
            int option;
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
                rule.put("KeyTemplate", new Template((String) o));
                if((o = ph.get("KeySubstitution"))!=null && o instanceof String)
                    rule.put("KeySubstitution",new TextSubstitution((String)o));
            }
            else {
                new Event(Event.WARNING, name + ": KeyTemplate is not well " +
                    " defined. Instead, the default will be used for " +
                    ruleName).send();
            }

            if ((o = ph.get("TimePattern")) != null && o instanceof String)
                rule.put("DateFormat", new SimpleDateFormat((String) o));

            if ((o = ph.get("FieldName")) != null && o instanceof String) {
                str = MessageUtils.getPropertyID((String) o);
                if (str == null)
                    str = (String) o;
                rule.put("FieldName", str);
            }

            if ((o = ph.get("DataType")) != null && o instanceof String) {
                if ("integer".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_INT;
                else if ("long".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_LONG;
                else if ("float".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_FLOAT;
                else if ("double".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_DOUBLE;
                else if ("string".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_STRING;
                else if ("time".equals(((String) o).toLowerCase()))
                    option = KeyChain.KEY_TIME;
                else
                    option = KeyChain.KEY_STRING;
            }
            else
                option = KeyChain.KEY_STRING;
            // store data type in RULE_EXTRA field
            ruleInfo[RULE_EXTRA] = option;

            // store xaMode in RULE_MODE field
            if ((o = ph.get("XAMode")) != null)
                ruleInfo[RULE_MODE] = Long.parseLong((String) o);
            else
                ruleInfo[RULE_MODE] = xaMode;

            cache = null;
            if ((o = ph.get("ScreenOption")) != null && o instanceof String) {
                if ("min".equals(((String) o).toLowerCase()))
                    option = OPTION_MIN;
                else if ("max".equals(((String) o).toLowerCase()))
                    option = OPTION_MAX;
                else if ("first".equals(((String) o).toLowerCase()))
                    option = OPTION_FIRST;
                else if ("last".equals(((String) o).toLowerCase()))
                    option = OPTION_LAST;
                else if ("first_last".equals(((String) o).toLowerCase()))
                    option = OPTION_FIRST_LAST;
                else
                    option = OPTION_NONE;
            }
            else
                option = OPTION_NONE;
            // store screen option in RULE_OPTION field
            ruleInfo[RULE_OPTION] = option;

            if (option == OPTION_FIRST_LAST)
                cache = new QuickCache(ruleName, QuickCache.META_MCMT, 0, 0);
            else if (option != OPTION_NONE)
                cache = new QuickCache(ruleName, QuickCache.META_DEFAULT, 0, 0);
            if (cache != null)
                rule.put("Cache", cache);
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_SCREEN;
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
     * It removes the rule from the ruleList and returns the rule id upon
     * success. It is not MT-Safe.
     */
    public int removeRule(String key, XQueue in) {
        int id = ruleList.getID(key);
        if (id > 0) { // can not delete the default rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long[] ruleInfo = ruleList.getMetaData(id);
            Object o;
            if ((o = cacheList.browse(id)) != null) { // flush old cache
                byte[] buffer = new byte[8192];
                long tm = System.currentTimeMillis();
                tm = flush(tm, in, id, buffer, (QuickCache) o);
                ((QuickCache) o).clear();
                if (cacheList.getNextID(id) >= 0)
                    cacheList.remove(id);
            }
            if (ruleInfo != null && ruleInfo[RULE_SIZE] > 0) // check integrity
                throw(new IllegalStateException(name+": "+key+" is busy with "+
                    ruleInfo[RULE_SIZE] + " outstangding msgs"));
            ruleList.remove(id);
            return id;
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            int i, n;
            String str;
            ConfigList cfg;
            Object o;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long tm = System.currentTimeMillis();
            cfg = (ConfigList) cfgList.remove(key);
            n = cfg.getSize();
            for (i=0; i<n; i++) {
                str = cfg.getKey(i);
                id = ruleList.getID(str);
                if ((o = cacheList.browse(id)) != null) { // flush old cache
                    byte[] buffer = new byte[8192];
                    tm = flush(tm, in, id, buffer, (QuickCache) o);
                    ((QuickCache) o).clear();
                    if (cacheList.getNextID(id) >= 0)
                        cacheList.remove(id);
                }
                ruleList.remove(id);
            }
            return id;
        }

        return -1;
    }

    /**
     * It replaces the existing rule of the key and returns its id upon success.
     * It is not MT-Safe.
     */
    public int replaceRule(String key, Map ph, XQueue in) {
        int id = ruleList.getID(key);
        if (id > 0) {
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            String str;
            long tm = System.currentTimeMillis();
            long[] meta = new long[RULE_TIME+1];
            long[] ruleInfo = ruleList.getMetaData(id);
            Map rule = initRuleset(tm, ph, meta);
            if (rule != null && rule.containsKey("Name")) {
                Object o;
                StringBuffer strBuf = ((debug & DEBUG_DIFF) <= 0) ? null :
                    new StringBuffer();
                if ((o = cacheList.browse(id)) != null) { // flush old cache
                    byte[] buffer = new byte[8192];
                    tm = flush(tm, in, id, buffer, (QuickCache) o);
                    ((QuickCache) o).clear();
                    if (cacheList.getNextID(id) >= 0)
                        cacheList.remove(id);
                }

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
                    if ((debug & DEBUG_DIFF) > 0)
                        strBuf.append(" " + ruleInfo[i]);
                }
                if ((debug & DEBUG_DIFF) > 0)
                    new Event(Event.DEBUG, name + "/" + key + " ruleInfo:" +
                        strBuf).send();
                return id;
            }
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            int n;
            ConfigList cfg;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            cfg = (ConfigList) cfgList.get(key);
            cfg.resetReporter(ph);
            cfg.setDataField(name);
            refreshRule(key, in);
            n = cfg.getSize();
            if (n > 0)
                id = ruleList.getID(cfg.getKey(0));
            else
                id = 0;
            return id;
        }

        return -1;
    }

    /**
     * Screen engine screens each incoming messages and decides what to do with
     * each of them.  The messages may be cached and put on-hold according to
     * the screenOption of the node.  If the message is on-hold, it
     * returns null.  Otherwise, it returns the message to be propageted.
     * The returned message may not be the current one.  If the message is
     * from the cache, its original cid will be stored in the cell of RULE_GID.
     * The caller has to retrieve the original cid before propagation.
     * Screening result also includes the map index for outLinks.  It is always
     * stored in the cell of RULE_OID.  But it is not the real oid.  The caller
     * has to convert it to oid with outLinkMap[].
     */
    private Message screen(String key, long currentTime, int cid, int rid,
        long[] ruleInfo, Map rule, QuickCache cache, Message inMessage) {
        Message msg;
        DateFormat dateFormat;
        int i = RESULT_OUT;
        int[] ts;
        long x, y;
        double u, v;
        String vname, value;
        boolean comp = false;

        if (cache == null) { // no cache at all
            ruleInfo[RULE_OID] = i;
            ruleInfo[RULE_GID] = cid;
            return inMessage;
        }
        try {
            switch ((int) ruleInfo[RULE_OPTION]) {
              case OPTION_FIRST:
                // ttl is set to 0 for session control
                i = cache.insert(key, currentTime, 0, new int[]{-1, rid}, null);
                if (i > 0) {
                    if (cache.size() == 1){//reset start-time of dynamic session
                        cache.setStatus(cache.getStatus(), currentTime);
                        cacheList.reserve(rid);
                        cacheList.add(cache, rid);
                    }
                    i = RESULT_OUT;
                }
                else if (i == 0)
                    i = RESULT_OUT;
                else
                    i = BYPASS_OUT;
                msg = inMessage;
                break;
              case OPTION_LAST:
                msg = (Message) cache.get(key, currentTime);
                if (msg != null) { // existing key
                    ts = cache.getMetaData(key);
                    // ttl is set to 0 for session control
                    i = cache.update(key, currentTime, 0, new int[]{cid, rid},
                        inMessage);
                    if (i > 0) // the replaced object has already expired
                        i = RESULT_OUT;
                    else // the replaced object has not expired yet
                        i = BYPASS_OUT;
                    cid = ts[0];
                    rid = ts[1];
                }
                else { // new key
                    // ttl is set to 0 for session control
                    cache.insert(key, currentTime, 0, new int[] {cid, rid},
                        inMessage);
                    if (cache.size() == 1){//reset start-time of dynamic session
                        cache.setStatus(cache.getStatus(), currentTime);
                        cacheList.reserve(rid);
                        cacheList.add(cache, rid);
                    }
                    i = ONHOLD_OUT;
                }
                break;
              case OPTION_FIRST_LAST:
                ts = cache.getMetaData(key);
                // ttl must be set to 1 for FISRT_LAST rule
                msg = (Message) cache.replace(key, currentTime, 1,
                    new int[] {cid, rid}, inMessage);
                if (msg == inMessage) { // current is the first msg
                    if (cache.size() == 1){//reset start-time of dynamic session
                        cache.setStatus(cache.getStatus(), currentTime);
                        cacheList.reserve(rid);
                        cacheList.add(cache, rid);
                    }
                    i = RESULT_OUT;
                }
                else if (msg != null) { // returned msg is the second or beyond 
                    cid = ts[0];
                    rid = ts[1];
                    i = BYPASS_OUT;
                }
                else { // current msg is the second
                    i = ONHOLD_OUT;
                }
                break;
              case OPTION_MAX:
                msg = (Message) cache.get(key, currentTime);
                if (msg != null) {
                    vname = (String) rule.get("FieldName");
                    switch ((int) ruleInfo[RULE_EXTRA]) {
                      case KeyChain.KEY_INT:
                      case KeyChain.KEY_LONG:
                        value = MessageUtils.getProperty(vname, msg);
                        x = Long.parseLong(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        y = Long.parseLong(value);
                        comp = (y > x);
                        break;
                      case KeyChain.KEY_FLOAT:
                      case KeyChain.KEY_DOUBLE:
                        value = MessageUtils.getProperty(vname, msg);
                        u = Double.parseDouble(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        v = Double.parseDouble(value);
                        comp = (v > u);
                        break;
                      case KeyChain.KEY_STRING:
                        value = MessageUtils.getProperty(vname, msg);
                        vname = MessageUtils.getProperty(vname, inMessage);
                        comp = (vname.compareTo(value) > 0);
                        break;
                      case KeyChain.KEY_TIME:
                        value = MessageUtils.getProperty(vname, msg);
                        dateFormat = (DateFormat) rule.get("DateFormat");
                        if (dateFormat != null)
                            x = dateFormat.parse(value).getTime();
                        else
                            x = Long.parseLong(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        if (dateFormat != null)
                            y = dateFormat.parse(value).getTime();
                        else
                            y = Long.parseLong(value);
                        comp = (y > x);
                        break;
                      default:
                        comp = false;
                    }
                    if (comp) { // keep the current one
                        ts = cache.getMetaData(key);
                        // ttl is set to 0 for session control
                        i = cache.update(key, currentTime, 0,
                            new int[] {cid, rid}, inMessage);
                        if (i > 0) // the replaced object has already expired
                            i = RESULT_OUT;
                        else // the replaced object has not expired yet
                            i = BYPASS_OUT;
                        cid = ts[0];
                        rid = ts[1];
                    }
                    else if (cache.isExpired(key, currentTime)) { // expired
                        ts = cache.getMetaData(key);
                        // ttl is set to 0 for session control
                        cache.update(key, currentTime, 0, new int[] {cid, rid},
                            inMessage, currentTime);
                        i = RESULT_OUT;
                        cid = ts[0];
                        rid = ts[1];
                    }
                    else { // bypass the current one
                        msg = inMessage;
                        i = BYPASS_OUT;
                    }
                }
                else {
                    // ttl is set to 0 for session control
                    i = cache.insert(key, currentTime, 0, new int[] {cid, rid},
                        inMessage);
                    if (cache.size() == 1){//reset start-time of dynamic session
                        cache.setStatus(cache.getStatus(), currentTime);
                        cacheList.reserve(rid);
                        cacheList.add(cache, rid);
                    }
                    i = ONHOLD_OUT;
                }
                break;
              case OPTION_MIN:
                msg = (Message) cache.get(key, currentTime);
                if (msg != null) {
                    vname = (String) rule.get("FieldName");
                    switch ((int) ruleInfo[RULE_EXTRA]) {
                      case KeyChain.KEY_INT:
                      case KeyChain.KEY_LONG:
                        value = MessageUtils.getProperty(vname, msg);
                        x = Long.parseLong(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        y = Long.parseLong(value);
                        comp = (y < x);
                        break;
                      case KeyChain.KEY_FLOAT:
                      case KeyChain.KEY_DOUBLE:
                        value = MessageUtils.getProperty(vname, msg);
                        u = Double.parseDouble(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        v = Double.parseDouble(value);
                        comp = (v < u);
                        break;
                      case KeyChain.KEY_STRING:
                        value = MessageUtils.getProperty(vname, msg);
                        vname = MessageUtils.getProperty(vname, inMessage);
                        comp = (vname.compareTo(value) < 0);
                        break;
                      case KeyChain.KEY_TIME:
                        value = MessageUtils.getProperty(vname, msg);
                        dateFormat = (DateFormat) rule.get("DateFormat");
                        if (dateFormat != null)
                            x = dateFormat.parse(value).getTime();
                        else
                            x = Long.parseLong(value);
                        value = MessageUtils.getProperty(vname, inMessage);
                        if (dateFormat != null)
                            y = dateFormat.parse(value).getTime();
                        else
                            y = Long.parseLong(value);
                        comp = (y < x);
                        break;
                      default:
                        comp = false;
                    }
                    if (comp) { // keep the current one
                        ts = cache.getMetaData(key);
                        // ttl is set to 0 for session control
                        i = cache.update(key, currentTime, 0,
                            new int[] {cid, rid}, inMessage);
                        if (i > 0) // the replaced object has already expired
                            i = RESULT_OUT;
                        else // the replaced object has not expired yet
                            i = BYPASS_OUT;
                        cid = ts[0];
                        rid = ts[1];
                    }
                    else if (cache.isExpired(key, currentTime)) { // expired
                        ts = cache.getMetaData(key);
                        // ttl is set to 0 for session control
                        cache.update(key, currentTime, 0, new int[] {cid, rid},
                            inMessage, currentTime);
                        i = RESULT_OUT;
                        cid = ts[0];
                        rid = ts[1];
                    }
                    else { // bypass the current one
                        msg = inMessage;
                        i = BYPASS_OUT;
                    }
                }
                else {
                    // ttl is set to 0 for session control
                    i = cache.insert(key, currentTime, 0, new int[] {cid, rid},
                        inMessage);
                    if (cache.size() == 1){//reset start-time of dynamic session
                        cache.setStatus(cache.getStatus(), currentTime);
                        cacheList.reserve(rid);
                        cacheList.add(cache, rid);
                    }
                    i = ONHOLD_OUT;
                }
                break;
              case OPTION_NONE:
              default:
                i = RESULT_OUT;
                msg = inMessage;
                break;
            }
        }
        catch (Exception e) {
            i = FAILURE_OUT;
            msg = inMessage;
            new Event(Event.ERR, name + ": " + (String) rule.get("Name") +
                " failed to screen on " + key + ": " +
                Event.traceStack(e)).send();
        }

        ruleInfo[RULE_OID] = i;
        ruleInfo[RULE_GID] = cid;
        return msg;
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
        QuickCache cache = null;
        Template template = null;
        TextSubstitution sub = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, size, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        boolean acked = ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isIncoming = true;
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
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
            if (filters[i] != null) {
                if ((!ckBody) && filters[i].checkBody())
                    ckBody = true;
            }
            ruleMap[i++] = rid;
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
        outInfo = assetList.getMetaData(outLinkMap[BYPASS_OUT]);

        n = ruleMap.length;
        previousTime = 0L;
        previousRid = -1;
        size = 0;
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
            if (currentTime - previousTime >= heartbeat) {
                if (cacheList.size() > 0) {
                    size = update(currentTime, in, buffer);
                    if (size > 0)
                        currentTime = System.currentTimeMillis();
                }
                previousTime = currentTime;
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
                new Event(Event.ERR, str + " failed to apply the filter "+ i+
                    ": " + Event.traceStack(e)).send();
                i = -1;
            }

            if (rid != previousRid) {
                ruleName = ruleList.getKey(rid);
                ruleInfo = ruleList.getMetaData(rid);
                rule = (Map) ruleList.get(rid);
                propertyName = (String[]) rule.get("PropertyName");
                if (ruleInfo[RULE_PID] == TYPE_SCREEN) {
                    cache = (QuickCache) rule.get("Cache");
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                }
                else if (ruleInfo[RULE_PID] >= 0) { // termination ruleset
                    String str = (String) rule.get("TargetRule");
                    long[] ri = ruleList.getMetaData((int) ruleInfo[RULE_PID]);
                    Map h = (Map) ruleList.get((int) ruleInfo[RULE_PID]);
                    cache = (h != null) ? (QuickCache) h.get("Cache") : null;
                    if (cache == null)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to find TargetRule " + str + " at " +
                            ruleInfo[RULE_PID]).send();
                    else if ((int) ri[RULE_PID] != TYPE_SCREEN) {
                        cache = null;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " found TargetRule " + str + " with wrong option " +
                            ri[RULE_PID] + " at " + ruleInfo[RULE_PID]).send();
                    }
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                }
                previousRid = rid;
            }

            isIncoming = true;
            if (i < 0) // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
            else if ((int) ruleInfo[RULE_PID] == TYPE_SCREEN) { // for screening
                Message msg;
                String key;

                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);
                msg = screen(key, currentTime, cid, rid, ruleInfo, rule,
                    cache, inMessage);

                // extract cid and out info from the ruleInfo
                i = (int) ruleInfo[RULE_OID];
                cid = (int) ruleInfo[RULE_GID];

                if (i == ONHOLD_OUT) { // check session if msg on hold
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
                        // remove from uplink since XA is off
                        in.remove(cid);
                    }
                    size = cache.size();
                    if ((debug & DEBUG_REPT) > 0)
                        new Event(Event.DEBUG, name + " propagate: " + key +
                            " = " + cid + ":" + rid +" "+ i +"/"+ size).send();
                    if (size >= sessionSize) {
                        int k = size - sessionSize;
                        currentTime = System.currentTimeMillis();
                        size = flush(currentTime, in, rid, buffer, cache);
                        if ((debug & DEBUG_COLL) > 0)
                            new Event(Event.DEBUG, name + ": flushed " +
                                size + " msgs due to oversize: " + k).send();
                    }
                    continue;
                }
                else if (i == RESULT_OUT && // for first cached msg
                    (ruleInfo[RULE_OPTION] == OPTION_FIRST ||
                    ruleInfo[RULE_OPTION] == OPTION_FIRST_LAST)) {
                    size = cache.size();
                    if ((debug & DEBUG_REPT) > 0)
                        new Event(Event.DEBUG, name + " propagate: " + key +
                            " = " + cid + ":" + rid +" "+ i +"/"+ size).send();
                    if (size >= sessionSize) {
                        int k = size - sessionSize;
                        currentTime = System.currentTimeMillis();
                        size = flush(currentTime, in, rid, buffer, cache);
                        if ((debug & DEBUG_COLL) > 0)
                            new Event(Event.DEBUG, name + ": flushed " +
                                size + " msgs due to oversize: " + k).send();
                    }
                }
                else if (i == BYPASS_OUT) { // for msg with bypass
                    if (msg != inMessage) { // check the returned msg
                        inMessage = msg;
                        isIncoming = false;
                    }
                    if ((debug & DEBUG_REPT) > 0) {
                        new Event(Event.DEBUG, name + " propagate: " + key +
                            " = " + cid + ":" + rid +" "+ i + "/" +
                            ((cache != null) ? cache.size() : 0)).send();
                    }
                }
                else if ((debug & DEBUG_REPT) > 0) {
                    new Event(Event.DEBUG, name + " propagate: " + key +
                        " = " + cid + ":" + rid +" "+ i + "/" +
                        ((cache != null) ? cache.size() : 0)).send();
                }

                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
                ruleInfo[RULE_GID] = 0;
            }
            else if (ruleInfo[RULE_PID] >= 0) { // for termination ruleset
                Message msg;
                String key;

                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (cache != null &&
                    (msg = (Message) cache.get(key, currentTime)) != null) {
                    if (!cache.isExpired(key, currentTime)) { // termination
                        int j;
                        long[] ri;
                        int[] ts;
                        String rn;
                        i = (int) ruleInfo[RULE_PID];
                        rn = ruleList.getKey(i);
                        ri = ruleList.getMetaData(i);
                        ts = cache.getMetaData(key);
                        cache.touch(key, 0L, currentTime);
                        if ((debug & DEBUG_PROP) > 0)
                            new Event(Event.DEBUG, name + " propagate: cid=" +
                                ts[0] + " rid=" + i + " oid=" +
                                outLinkMap[BYPASS_OUT] +
                                " status=" + cacheStatus).send();
                        if (ri[RULE_DMASK] > 0) try {  // display the message
                            String str = null;
                            Map h = (Map) ruleList.get(i);
                            String[] pn = (String[]) h.get("PropertyName");
                            if ((ruleInfo[RULE_DMASK] & dspBody) > 0)
                                str = MessageUtils.processBody(msg, buffer);
                            new Event(Event.INFO, name + ": " + rn +
                                " terminated screened msg " + (count + 1) + ":"+
                                MessageUtils.display(msg, str,
                                (int) ri[RULE_DMASK], pn)).send();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + rn +
                                " failed to display msg: "+e.toString()).send();
                        }
                        if (ri[RULE_MODE] > 0) { // XA is on
                            j = passthru(currentTime, msg, in, i,
                                outLinkMap[BYPASS_OUT], ts[0], -1);
                            if (j > 0) {
                                ri[RULE_PEND] --;
                                count ++;
                            }
                            else { // failed to passthru,force it to FAILURE_OUT
                                j = forcethru(currentTime, msg, in, i,
                                    outLinkMap[FAILURE_OUT], ts[0]);
                                if (j > 0)
                                    new Event(Event.ERR, name + ": " + rn +
                                        " failed to passthru the msg for "+key+
                                        " with msg redirected to " +
                                        outLinkMap[FAILURE_OUT]).send();
                                else if (j == 0)
                                    new Event(Event.ERR, name + ": " + rn +
                                        " failed to passthru the msg for "+key+
                                        " with msg flushed to " +
                                        outLinkMap[FAILURE_OUT]).send();
                                else
                                    new Event(Event.ERR, name + ": " + rn +
                                        " failed to passthru the msg for "+key+
                                        " with msg dropped").send();
                            }
                        }
                        else { // it had already been removed from the uplink
                            j = passthru(currentTime, msg, in, i,
                                outLinkMap[BYPASS_OUT], -1, 0);
                            if (j > 0) {
                                ri[RULE_PEND] --;
                                ri[RULE_COUNT] ++;
                                ri[RULE_TIME] = currentTime;
                                count ++;
                            }
                            else {
                                new Event(Event.ERR, name + ": " + rn +
                                    " failed to flush the msg for "+key).send();
                            }
                        }
                    }
                }
                oid = outLinkMap[RESULT_OUT];
            }
            else { // preferred ruleset or nohit
                oid = (int) ruleInfo[RULE_OID];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " status=" +
                    cacheStatus).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName + " screened msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            if ((int) ruleInfo[RULE_PID] != TYPE_SCREEN) { // bypass
                count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
                feedback(in, -1L);
                sz = msgList.size();
            }
            else if (isIncoming || ruleInfo[RULE_MODE] > 0) { // normal or XA
                int j = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
                if (j > 0)
                    count ++;
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
                feedback(in, -1L);
                sz = msgList.size();
            }
            else { // flush withheld msg without feedback
                i = passthru(currentTime, inMessage, in, rid, oid, -1, 0);
                if (i > 0) {
                    ruleInfo[RULE_COUNT] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    count ++;
                }
                else {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to flush the msg to bypass").send();
                }
            }
            inMessage = null;
        }
    }


    /**
     * returns number of withheld messages flushed
     */
    private int flush(long currentTime, XQueue in, int rid, byte[] buffer,
        QuickCache cache) {
        int i, cid, oid, option, count = 0, dspBody;
        String[] keys, propertyName;
        long[] ruleInfo;
        int[] ts;
        Message inMessage;
        Map rule;
        String ruleName;
        count = cache.size();
        // reset start time on the cache
        cache.setStatus(cache.getStatus(), currentTime);
        if (count <= 0)
            return 0;

        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;
        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        option = (int) ruleInfo[RULE_OPTION];
        if (option == OPTION_FIRST) { // cleanup cache and remove it
            cache.clear();
            cacheList.getNextID(rid);
            cacheList.remove(rid);
            return 0;
        }
        else if (option == OPTION_FIRST_LAST) { // cleanup FIRST, reset LAST
            // cached keys are either FIRST or LAST
            cache.disfragment(currentTime);
            // leftover keys of FIRST are reset from LAST
            keys = cache.sortedKeys();
            count -= keys.length;
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + " flush: " + count +
                    " msgs expired and " + keys.length + " leftover").send();
        }
        else {
            keys = cache.sortedKeys();
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + " flush: " + count +
                    " msgs").send();
        }

        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        oid = RESULT_OUT;
        count = 0;
        for (i=0; i<keys.length; i++) {
            cid = -1;
            inMessage = (Message) cache.get(keys[i], currentTime);
            ts = cache.getMetaData(keys[i]);
            if (inMessage == null) {
                new Event(Event.WARNING, name + ": null msg in flush").send();
                continue;
            }
            if (ts == null || ts.length < 2) {
                new Event(Event.WARNING, name + ": empty metadata").send();
                continue;
            }
            else {
                cid = ts[0];
                if ((debug & DEBUG_REPT) > 0)
                    new Event(Event.DEBUG, name + " flush: " + keys[i] + " = "+
                        cid + ":"+ rid + " "+ oid + "/" + (count+1)).send();

                if (ruleInfo[RULE_DMASK] > 0) try {  // display the message
                    String msgStr = null;
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO, name + ": " + ruleName +
                        " flushed screened msg " + (count + 1) + ":" +
                        MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: " + e.toString()).send();
                }
            }

            if (ruleInfo[RULE_MODE] > 0) { // XA is on
                int j = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
                if (j > 0) {
                    ruleInfo[RULE_PEND] --;
                    count ++;
                }
                else { // failed to passthru, so force it to FAILURE_OUT
                    j = forcethru(currentTime, inMessage, in, rid,
                        outLinkMap[FAILURE_OUT], cid);
                    if (j > 0)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg for " +keys[i] +
                            " with msg redirected to " +
                            outLinkMap[FAILURE_OUT]).send();
                    else if (j == 0)
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg for " +keys[i] +
                            " with msg flushed to " +
                            outLinkMap[FAILURE_OUT]).send();
                    else
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to passthru the msg for " +keys[i] +
                            " with msg dropped").send();
                }
            }
            else { // it had already been removed from the uplink
                int j = passthru(currentTime, inMessage, in, rid, oid, -1, 0);
                if (j > 0) {
                    ruleInfo[RULE_PEND] --;
                    ruleInfo[RULE_COUNT] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    count ++;
                }
                else {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to flush the msg for " + keys[i]).send();
                }
            }
        }
        if (count > 0)
            feedback(in, -1L);
        ruleInfo[RULE_PEND] = 0;
        if (option != OPTION_FIRST_LAST) {
            cache.clear();
            cacheList.getNextID(rid);
            cacheList.remove(rid);
        }
        else { // for FIRST_LAST
            if (cache.size() <= 0) { // remove the empty cache
                cacheList.getNextID(rid);
                cacheList.remove(rid);
            }
        }

        return count;
    }

    /**
     * queries the status of the primary destination and queue depth via local
     * data. It updates the cache status so that all cache sessions will be
     * adjusted according to the load level of the primary destination.
     * If the cache session is enabled, it also monitors the cache mtime so that
     * the SCREEN cache will be flushed regularly.  It returns total number of
     * msgs flushed
     */
    private int update(long currentTime, XQueue in, byte[] buffer) {
        StringBuffer strBuf = null;
        Browser browser;
        QuickCache cache;
        Map r = null;
        Object o;
        long[] outInfo, ruleInfo;
        long dt;
        int i, d, oid, rid, n, size, count = 0;
        boolean inDetail = ((debug & DEBUG_UPDT) > 0);
        n = cacheList.size();
        if (n <= 0)
            return 0;
        int[] list = new int[n];

        oid = outLinkMap[RESULT_OUT];
        outInfo = assetList.getMetaData(oid);
        if (reportName != null) // report defined
            r = (Map) NodeUtils.getReport(reportName);

        if (reportName == null) // no report defined
            r = null;
        else if (r == null || (o = r.get("TestTime")) == null)
            new Event(Event.WARNING, name + ": no report found for " +
                reportName).send();
        else try { // check report
            dt = Long.parseLong((String) o);
            if (dt > outInfo[OUT_QTIME]) { // recent report
                outInfo[OUT_QTIME] = dt;
                if ((o = r.get("DstStatus")) != null)
                    status = Integer.parseInt((String) o);
                else // set the default
                    status = 1;
                if ("0".equals((String) r.get("Status")) && status >= 0) {
                    outInfo[OUT_QDEPTH] =
                        Long.parseLong((String)r.get("CurrentDepth"));
                    outInfo[OUT_QIPPS] =
                        Long.parseLong((String) r.get("IppsCount"));
                }
                else {
                    outInfo[OUT_QDEPTH] = threshold[LOAD_HIGH] + 600000;
                    outInfo[OUT_QIPPS] = 0;
                }
                outInfo[OUT_QSTATUS] = status;
            }

            d = (int) outInfo[OUT_QDEPTH];
            if (d < threshold[LOAD_LOW]) { // low load
                if (cacheStatus == QuickCache.CACHE_OFF) { // enable session
                    cacheStatus = QuickCache.CACHE_ON;
                    new Event(Event.INFO, name + ": cache update has been " +
                        "enabled with " + n + " active caches").send();
                }
            }
            else if (d >= threshold[LOAD_MEDIUM]) { // high load
                if (cacheStatus == QuickCache.CACHE_ON) { // disable session
                    cacheStatus = QuickCache.CACHE_OFF;
                    new Event(Event.INFO, name + ": cache update has been " +
                        "disabled with " + n + " active caches").send();
                }
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to extrieve report data"+
                " from "+ reportName +": "+ Event.traceStack(e)).send();
        }

        if (cacheStatus == QuickCache.CACHE_OFF) // cache update disabled
            return 0;

        if (inDetail)
            strBuf = new StringBuffer();

        n = cacheList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            rid = list[i];
            cache = (QuickCache) cacheList.browse(rid);
            if (cache.size() <= 0) { // remove the empty cache 
                cacheList.getNextID(rid);
                cacheList.remove(rid);
                continue;
            }
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PID] == TYPE_BYPASS || ruleInfo[RULE_TTL] <= 0)
                continue;
            dt = currentTime - (cache.getMTime() + ruleInfo[RULE_TTL]);
            if (dt >= 0) { // cache expired
                size = flush(currentTime, in, rid, buffer, cache);
                count += size;
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                        " flushed " +size+ " msgs due to timeout: "+dt).send();
            }
            if (inDetail)
                strBuf.append("\n\t" + ruleList.getKey(rid) + ": " + rid + " " +
                    ruleInfo[RULE_OID] + " " + ruleInfo[RULE_PID] + " " +
                    ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_SIZE] + " " +
                    Event.dateFormat(new Date(ruleInfo[RULE_TIME])));
        }

        if (inDetail && strBuf.length() > 0) {
            strBuf.append("\n\t In: Status Type Size Depth: " +
                in.getGlobalMask() + " " + 1 + " " +
                in.size() + " " + in.depth());
            strBuf.append("\n\tOut: OID Status NRule Size QDepth Time");
            strBuf.append("\n\t" + assetList.getKey(oid) + ": " + oid + " " +
                outInfo[OUT_STATUS] + " " + outInfo[OUT_NRULE] + " " +
                outInfo[OUT_SIZE] + " " + outInfo[OUT_QDEPTH] + " " +
                Event.dateFormat(new Date(outInfo[OUT_TIME])));
            new Event(Event.DEBUG, name + " RuleName: RID OID PID Option " +
                "Size Time count=" + count + strBuf.toString()).send();
        }

        return count;
    }

    public void resetMetaData(XQueue in, XQueue[] out) {
        Browser browser;
        QuickCache cache;
        int i;

        super.resetMetaData(in, out);
        browser = cacheList.browser();
        while ((i = browser.next()) >= 0) {
            cache = (QuickCache) cacheList.browse(i);
            if (cache != null)
                cache.clear();
        }
        cacheList.clear();
    }

    /** lists all propagating and withheld messages or keys */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h, ph;
        QuickCache cache = null;
        StringBuffer strBuf;
        String key, str, text;
        String[] keys;
        long[] ruleInfo;
        int[] list, info;
        int i, j, id, cid, k, n, rid;
        long tm, currentTime;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (cacheList.size() <= 0 || h == null)
            return h;
        else if (h.size() <= 0) { // empty
            strBuf = new StringBuffer();
            k = 0;
            ph = new HashMap<String, String>();
        }
        else { // try to count k
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

        currentTime = System.currentTimeMillis();
        n = cacheList.getCapacity();
        list = new int[n];
        n = cacheList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) { // scan the list
            rid = list[i];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0)
                continue;
            cache = (QuickCache) cacheList.browse(rid);
            if (cache == null || cache.size() <= 0)
                continue;
            keys = cache.sortedKeys();
            if (keys == null || keys.length <= 0)
                continue;

            key = ruleList.getKey(rid);
            if (key == null)
                key = "-";

            for (j=0; j<keys.length; j++) {
                info = (int[]) cache.getMetaData(keys[j]);
                if (info == null)
                    continue;
                if (cache.isExpired(keys[j], currentTime))
                    continue;
                if (ruleInfo[RULE_OPTION] != OPTION_FIRST) {
                    if (ruleInfo[RULE_MODE] > 0) { // xa is on 
                        cid = info[0];
                        if (ph.containsKey(String.valueOf(cid))) // duplicate
                            continue;
                        if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                            continue;
                        msg = (Message) xq.browse(cid);
                    }
                    else { // xa is off
                        cid = -1;
                        msg = (Message) cache.get(keys[j], currentTime);
                    }
                    if (msg == null)
                        continue;
                    try {
                        tm = msg.getJMSTimestamp();
                    }
                    catch (Exception e) {
                        tm = 0;
                    }
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
                }
                else {
                    cid = -1;
                    tm = cache.getTimestamp(keys[j]);
                    str = keys[j];
                }
                text = Event.dateFormat(new Date(tm));

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
                    strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) +
                        "\"");
                    strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) +
                        "\"");
                    strBuf.append("}");
                }
                else { // for text
                    h.put(linkName + "_" + k, cid + " PENDING " + key +
                        " - " + text + " " + str);
                }
                k ++;
            }
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
        QuickCache cache;
        Map rule;
        int id;
        setStatus(NODE_CLOSED);
        cells.clear();
        msgList.clear();
        assetList.clear();
        browser = ruleList.browser();
        while((id = browser.next()) >= 0) {
            rule = (Map) ruleList.get(id);
            if (rule != null) {
                cache = (QuickCache) rule.get("Cache");
                if (cache != null)
                    cache.clear();
                rule.clear();
            }
        }
        ruleList.clear();
        cacheList.clear();
    }

    protected void finalize() {
        close();
    }
}
