package org.qbroker.node;

/* DistinguishNode.java - a MessageNode distinguishing JMS messages */

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
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * DistinguishNode picks up JMS messages from an input XQ and distinguishes
 * them according to their keys and optional versions.  It filters them into
 * four outlinks: done for all messages of newer versions, bypass for those
 * messages either duplicate or older versions, failure for those messages
 * failed in the distinguishing processes, and nohit for all the messages
 * that do not hit any explicitly defined ruleset.
 *<br/><br/>
 * DistinguishNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also specifies the parameters
 * to control the message filtering process on the group.  Furthermore,
 * the content filtering can be enabled or disabled dynamically via the
 * sessions and monitors.  If SessionTimeout is set to 0, it will only do
 * static filtering.  The number of the keys cached is tracked via the
 * RULE_PEND field. If VersionTemplate is defined in a ruleset, DistinguishNode
 * will retrieve the version info from the messages to enforce the version
 * control. It means the messages with older versions will be treated as the
 * duplicates. DistinguishNode also creates one extra ruleset by default for
 * all nohit messages.  The reserved word, nohit, is the name of this default
 * ruleset.
 *<br/><br/>
 * DistinguishNode also supports the cache mode. If a ruleset is defined with
 * its RuleType as "cache", another reserved word, the node is on cache mode.
 * The cache rule is designed for active-passive message flow clusters.  It
 * assumes that the master escalates each processed message to the worker flow.
 * On the worker side, its DistinguishNode processes the escalated messages
 * from the master to cache the keys of them. It keeps the state cache on the
 * worker flow in sync with the master.  When the failover occurs, the worker
 * flow with the current cache will be ready to be promoted to the master.
 * Since the cache on the new master is current, any processed message will not
 * be routed to the done outlink. The cache rule on worker flow has a different
 * routing behavior. Since all the messages have already been processed on the
 * master, the messages with the unique keys will be routed to nohit outlink.
 * The rest of the messages will be routed to failure outlink, if there is any.
 * If DistinguishNode is on cache mode, its uplink must have the 2nd half of
 * the cells unused. The unused partition will be reserved for escalations.
 *<br/><br/>
 * DistinguishNode also supports checkpointing of the internal data cache via
 * the container.  In order to checkpoint, the CheckpointDir and SAXParser
 * must be defined on the container level.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * DistiguishNode always assumes the first outlink for done, the second for
 * bypass, the third for failure and the last for nohit.  The outlink of
 * failure or nohit can share the same name with the first two others.  It
 * means these outlinks may share the same output channel.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DistinguishNode extends Node {
    private int heartbeat = 60000;
    private int sessionTimeout = 300000;
    private int cacheTimeout = 0;
    private int cacheThreshold = 0;
    private int cacheStatus;

    private String reportName = null;
    private QuickCache cache = null;          // {rid}
    private int[] threshold, outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;

    public DistinguishNode(Map props) {
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
            operation = "distinguish";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("CacheTimeout")) != null)
            cacheTimeout = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("CacheThreshold")) != null)
            cacheThreshold = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("Threshold")) != null) {
            threshold =  TimeWindows.parseThreshold((String) o);
            threshold[0] /= 1000;
            threshold[1] /= 1000;
            threshold[2] /= 1000;
        }
        else {
            threshold = new int[] {10, 50, 100};
        }

        cache = new QuickCache(name, QuickCache.META_DEFAULT, cacheTimeout,
            cacheThreshold);

        if ((o = props.get("InitCacheStatus")) != null &&
            "off".equals(((String) o).toLowerCase()))
            cacheStatus = QuickCache.CACHE_OFF;
        else
            cacheStatus = QuickCache.CACHE_ON;

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
                (outInfo[OUT_LENGTH] == 0 && outInfo[OUT_OFFSET] != 0) ||
                outInfo[OUT_LENGTH]+outInfo[OUT_OFFSET] > outInfo[OUT_CAPACITY])
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
            throw(new IllegalArgumentException(name+": failed to init rule " +
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                    ruleInfo[RULE_MODE] + " " + ruleInfo[RULE_DMASK] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID TTL MODE MASK - OutName" +
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
        if ((o = props.get("SessionTimeout")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != sessionTimeout) {
                sessionTimeout = i;
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
        String key, str, ruleName, preferredOutName;
        long[] outInfo;
        int i, j, k;

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

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
            rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String) o));

            if ((o = ph.get("VersionTemplate")) != null && o instanceof String){
                rule.put("VersionTemplate", new Template((String) o));
                if ((o = ph.get("TimePattern")) != null && o instanceof String)
                    rule.put("DateFormat", new SimpleDateFormat((String) o));
                ruleInfo[RULE_MODE] = 1;
            }

            if ((o = ph.get("RuleType")) != null &&
                "cache".equalsIgnoreCase((String) o)) {
                ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
                ruleInfo[RULE_PID] = TYPE_CACHE;
            }
            else {
                ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
                ruleInfo[RULE_PID] = TYPE_SCREEN;
            }
        }
        else { // for bypass
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
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
            if (ruleInfo != null && ruleInfo[RULE_SIZE] > 0) // check integrity
                throw(new IllegalStateException(name+": "+key+" is busy with "+
                    ruleInfo[RULE_SIZE] + " outstangding msgs"));
            expire(System.currentTimeMillis(), id, true);
            ruleList.remove(id);
            return id;
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            int i, n, k = 0;
            long tm;
            String str;
            ConfigList cfg;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            tm = System.currentTimeMillis();
            cfg = (ConfigList) cfgList.remove(key);
            n = cfg.getSize();
            for (i=0; i<n; i++) {
                str = cfg.getKey(i);
                id = ruleList.getID(str);
                k += expire(tm, id, false);
                ruleList.remove(id);
            }
            if (k > 0)
                disfragment(tm);
            return id;
        }

        return -1;
    }

    /**
     * returns the number of keys expired for the rule 
     */
    private int expire(long currentTime, int rid, boolean disfrag) {
        int i, n, count = 0;
        int[] ts;
        String[] keys;
        if (cache.size() <= 0)
            return 0;
        
        keys = cache.sortedKeys();
        n = keys.length;
        for (i=0; i<n; i++) {
           ts = cache.getMetaData(keys[i]);
           if (ts == null || ts.length < 1)
               continue;
           if (ts[0] != rid)
               continue;
           if (cache.getTTL(keys[i]) <= 0) // reset TTL on evergreen
               cache.update(keys[i], currentTime, 100, ts, null);
           cache.expire(keys[i], currentTime);
           count ++;
        }
        if (count > 0 && disfrag)
            disfragment(currentTime);
        return count;
    }

    /**
     * picks up a message from input queue and evaluate its content to
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
        Template template = null, versionTemp = null;
        TextSubstitution sub = null;
        DateFormat df = null;
        ParsePosition pp = new ParsePosition(0);
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
        boolean ckBody = false, hasVersion = false;
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
            if (currentTime - previousTime >= heartbeat) {
                if (sessionTimeout > 0)
                    cacheStatus = update(currentTime, in);
                previousTime = currentTime;
            }
            if (cid < 0)
                continue;

            wt = 5L;
            if ((inMessage = (Message) in.browse(cid)) == null) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": " + Event.traceStack(
                   new JMSException("null msg from "+in.getName()))).send();
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
                if (ruleInfo[RULE_PID] != TYPE_BYPASS) {
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                    if (ruleInfo[RULE_MODE] == 1) {
                        df = (DateFormat) rule.get("DateFormat");
                        versionTemp = (Template) rule.get("VersionTemplate");
                        hasVersion = true;
                    }
                    else
                        hasVersion = false;
                }
                previousRid = rid;
            }

            if (i < 0) // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
            else if (ruleInfo[RULE_PID] == TYPE_SCREEN &&
                cacheStatus == QuickCache.CACHE_ON) { // cache is enabled
                String versionStr = null, str;
                String key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (hasVersion) try { // with version
                    versionStr = MessageUtils.format(inMessage,
                        buffer, versionTemp);
                    if (df != null && versionStr != null) {
                        pp.setIndex(0);
                        versionStr = String.valueOf(df.parse(versionStr,
                            pp).getTime());
                    }
                    if (versionStr == null)
                        throw(new NullPointerException("null versionStr"));
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to get version string from '" + versionStr +
                        "' for " + key + ": " + e.toString()).send();
                    versionStr = "";
                }

                if (key == null || key.length() <= 0) {
                    i = FAILURE_OUT;
                }
                else if (!cache.containsKey(key)) { // first appearance
                    try {
                        i = cache.insert(key, currentTime,
                            (int) ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[RESULT_OUT]}, versionStr);
                        ruleInfo[RULE_PEND] ++;
                        i = RESULT_OUT;
                        if (cacheTimeout == 0 && cache.size() == 1)
                            // reset start-time of dynamic session
                            cache.setStatus(cache.getStatus(), currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to insert " + key +
                            ((hasVersion) ? " with " + versionStr : "") +
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else if (cache.isExpired(key, currentTime)) { // expired
                    int[] state = cache.getMetaData(key);
                    long[] info;
                    if (state != null && state.length > 0 &&
                        (info = ruleList.getMetaData(state[0])) != null)
                        info[RULE_PEND] --;
                    try {
                        i = cache.insert(key, currentTime,
                            (int) ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[RESULT_OUT]}, versionStr);
                        ruleInfo[RULE_PEND] ++;
                        i = RESULT_OUT;
                        if (cacheTimeout == 0 && cache.size() == 1)
                            // reset start-time of dynamic session
                            cache.setStatus(cache.getStatus(), currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to reinsert " + key +
                            ((hasVersion) ? " with " + versionStr : "") +
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else if (hasVersion && (str=(String)cache.get(key,currentTime))
                    != null && versionStr.compareTo(str) > 0) {// newer version
                    try {
                        cache.replace(key, currentTime, (int)ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[RESULT_OUT]}, versionStr);
                        i = RESULT_OUT;
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to replace " + key + " with " +versionStr+
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else { // existing key with old version or without version
                    cache.touch(key, currentTime, currentTime);
                    i = BYPASS_OUT;
                }

                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name+" propagated: " + key +
                        " = " + cid + ":" + rid + " " + i).send();
                oid = outLinkMap[i];
            }
            else if (ruleInfo[RULE_PID] == TYPE_SCREEN) { // disabled
                oid = outLinkMap[RESULT_OUT];
            }
            else if (ruleInfo[RULE_PID] == TYPE_CACHE) { // cache rule
                String versionStr = null, str;
                String key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (hasVersion) try { // with version
                    versionStr = MessageUtils.format(inMessage,
                        buffer, versionTemp);
                    if (df != null && versionStr != null) {
                        pp.setIndex(0);
                        versionStr = String.valueOf(df.parse(versionStr,
                            pp).getTime());
                    }
                    if (versionStr == null)
                        throw(new NullPointerException("null versionStr"));
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to get version string from '" + versionStr +
                        "' for " + key + ": " + e.toString()).send();
                    versionStr = "";
                }

                if (key == null || key.length() <= 0) {
                    i = FAILURE_OUT;
                }
                else if (!cache.containsKey(key)) { // first appearance
                    try {
                        i = cache.insert(key, currentTime,
                            (int) ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[NOHIT_OUT]}, versionStr);
                        ruleInfo[RULE_PEND] ++;
                        i = NOHIT_OUT;
                        if (cacheTimeout == 0 && cache.size() == 1)
                            // reset start-time of dynamic session
                            cache.setStatus(cache.getStatus(), currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to insert " + key +
                            ((hasVersion) ? " with " + versionStr : "") +
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else if (cache.isExpired(key, currentTime)) { // expired
                    int[] state = cache.getMetaData(key);
                    long[] info;
                    if (state != null && state.length > 0 &&
                        (info = ruleList.getMetaData(state[0])) != null)
                        info[RULE_PEND] --;
                    try {
                        i = cache.insert(key, currentTime,
                            (int) ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[NOHIT_OUT]}, versionStr);
                        ruleInfo[RULE_PEND] ++;
                        i = NOHIT_OUT;
                        if (cacheTimeout == 0 && cache.size() == 1)
                            // reset start-time of dynamic session
                            cache.setStatus(cache.getStatus(), currentTime);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to reinsert " + key +
                            ((hasVersion) ? " with " + versionStr : "") +
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else if (hasVersion && (str=(String)cache.get(key,currentTime))
                    != null && versionStr.compareTo(str) > 0) {// newer version
                    try {
                        cache.replace(key, currentTime, (int)ruleInfo[RULE_TTL],
                            new int[]{rid, outLinkMap[NOHIT_OUT]}, versionStr);
                        i = NOHIT_OUT;
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to replace " + key + " with " +versionStr+
                            ": " + Event.traceStack(e)).send();
                    }
                }
                else { // existing key with old version or without version
                    cache.touch(key, currentTime, currentTime);
                    i = FAILURE_OUT;
                }

                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name+" propagated: " + key +
                        " = " + cid + ":" + rid + " " + i).send();
                oid = outLinkMap[i];
            }
            else { // preferred ruleset or nohit
                oid = (int) ruleInfo[RULE_OID];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " status=" +
                    cacheStatus).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName +
                    " distinguished msg " + (count + 1) + ":" +
                    MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * queries the status of the output destination and queue depth via local
     * data. It updates the cache status so that the DISTINCT ruleset will be
     * turned on or off according to the load level of the primary destination.
     */
    private int update(long currentTime, XQueue in) {
        int i, k, d, oid;
        String str = null;
        StringBuffer strBuf;
        long mtime;
        int status;
        Object o;
        Map r = null;
        Browser browser;
        long[] outInfo, ruleInfo;

        oid = outLinkMap[RESULT_OUT];
        outInfo = assetList.getMetaData(oid);

        if (reportName != null) // report defined
            r = (Map) NodeUtils.getReport(reportName);

        if (reportName == null) // no report defined
            r = null;
        else if (r == null || (o = r.get("TestTime")) == null)
            new Event(Event.WARNING, name + ": no report found for " +
                reportName).send();
        else try {
            mtime = Long.parseLong((String) o);
            if (mtime > outInfo[OUT_QTIME]) { // recent report
                outInfo[OUT_QTIME] = mtime;
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
            status = cache.getStatus();
            mtime = cache.getMTime();
            if (d < threshold[LOAD_LOW]) { // low load
                if (status == QuickCache.CACHE_ON &&
                    currentTime - mtime >= sessionTimeout) { //disable DISTINCT
                    disfragment(currentTime);
                    cache.setStatus(QuickCache.CACHE_OFF, currentTime);
                    status = QuickCache.CACHE_OFF;
                    str = "OFF";
                }
            }
            else if (d >= threshold[LOAD_MEDIUM]) { // high load
                if (status == QuickCache.CACHE_OFF) { // enable DISTINCT
                    cache.clear();
                    status = QuickCache.CACHE_ON;
                    cache.setStatus(QuickCache.CACHE_ON, currentTime);
                    str = "ON";
                }
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to extrieve report data"+
                " from "+ reportName +": "+ Event.traceStack(e)).send();
        }

        status = cache.getStatus();
        if (str != null) // log the current status if it is changed
            new Event(Event.INFO, name + ": cache has been switched to " +
                str + " with "+ cache.size() + " active entries").send();
        else if (status == QuickCache.CACHE_ON) { // cache is always on
            mtime = cache.getMTime();
            if (cacheTimeout == 0 && sessionTimeout > 0 &&
                currentTime >=  mtime + sessionTimeout) { // check cache
                k = disfragment(currentTime);
                if (k > 0 && (debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + ": " + k +
                        " keys expired").send();
            }
        }

        if ((debug & DEBUG_UPDT) > 0) {
            strBuf = new StringBuffer("RuleName: RID OID PID Size Time " +
                "status=" + status + " cached=" + cache.size());
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i +
                    " " + ruleInfo[RULE_OID] + " " + ruleInfo[RULE_PID] +
                    " " + ruleInfo[RULE_SIZE] + " " +
                    Event.dateFormat(new Date(ruleInfo[RULE_TIME])));
            }
            strBuf.append("\n\t In: Status Type Size Depth: " +
                in.getGlobalMask() + " " + 1 + " " +
                in.size() + " " + in.depth());
            strBuf.append("\n\tOut: OID Status NRule Size QDepth Time");
            strBuf.append("\n\t" + assetList.getKey(oid) + ": " + oid +
                " " + outInfo[OUT_STATUS] + " " + outInfo[OUT_NRULE] +
                " " + outInfo[OUT_SIZE] +" "+ outInfo[OUT_QDEPTH]+
                " " + Event.dateFormat(new Date(outInfo[OUT_TIME])));
            new Event(Event.DEBUG, name + " " + strBuf.toString()).send();
        }

        return status;
    }

    /**
     * It checks all keys to see if any of them has been expired
     * or not.  If yes, it will update the RULE_PEND and disfragment
     * the cache.  Meanwhile, it will look for the next expiration
     * time and resets the MTime of the cache.  The number of expired
     * keys will be returned upon success.
     */
    private int disfragment(long currentTime) {
        int k, rid;
        int[] info;
        long t, tm;
        long[] ruleInfo;
        Object o;
        Set<String> keys = cache.keySet();
        tm = currentTime + sessionTimeout;
        k = 0;
        for (String key : keys) {
            if (!cache.isExpired(key, currentTime)) {
                t = cache.getTimestamp(key) + cache.getTTL(key);
                if (t < tm)
                    tm = t;
                continue;
            }
            info = cache.getMetaData(key);
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
            cache.disfragment(currentTime);
        cache.setStatus(cache.getStatus(), tm - sessionTimeout);

        return k;
    }

    /** lists all propagating messages and cached keys */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Map<String, String> h, ph;
        StringBuffer strBuf;
        String key, qname, str, text;
        String[] keys;
        long[] ruleInfo;
        int[] info;
        int i, cid, k, n, oid, rid;
        long tm, currentTime;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (cache.size() <= 0 || h == null)
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

        if (cache.size() <= 0)
            return h;
        keys = cache.sortedKeys();
        if (keys == null || keys.length <= 0)
            return h;

        currentTime = System.currentTimeMillis();
        for (i=0; i<keys.length; i++) {
            if (cache.isExpired(keys[i], currentTime))
                continue;
            info = (int[]) cache.getMetaData(keys[i]);
            if (info == null)
                continue;
            tm = cache.getTimestamp(keys[i]);
            rid = info[0];
            oid = info[1];
            cid = -1;
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0)
                continue;

            key = ruleList.getKey(rid);
            if (key == null)
                key = "-";
            qname = assetList.getKey(oid);
            if (qname == null)
                qname = "-";
            text = Event.dateFormat(new Date(tm));
            str = keys[i];

            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + k + "</ID>");
                strBuf.append("<CellID>" + cid + "</CellID>");
                strBuf.append("<Status>PENDING</Status>");
                strBuf.append("<Rule>" + key + "</Rule>");
                strBuf.append("<OutLink>"+qname+"</OutLink>");
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
                strBuf.append(", \"OutLink\":\"" + qname + "\"");
                strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) + "\"");
                strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) + "\"");
                strBuf.append("}");
            }
            else { // for text
                h.put(linkName + "_" + k, cid + " PENDING " + key +
                    " " + qname + " " + text + " " + str);
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

    /** returns the checkpoint hashmap for current state */
    public Map<String, Object> checkpoint() {
        Map<String, Object> h, chkpt = new HashMap<String, Object>();
        List<Object> list = new ArrayList<Object>();
        int[] meta;
        String[] keys;
        String key, value;
        int i, n, rid, ttl;
        long currentTime, mtime;

        if (cache.size() <= 0)
            return chkpt;
        keys = cache.sortedKeys();
        if (keys == null || keys.length <= 0)
            return chkpt;

        currentTime = System.currentTimeMillis();
        for (i=0; i<keys.length; i++) {
            key = keys[i];
            if (cache.isExpired(key, currentTime))
                continue;
            h = new HashMap<String, Object>();
            meta = cache.getMetaData(key);
            value = (String) cache.get(key);
            mtime = cache.getTimestamp(key);
            ttl = cache.getTTL(key);
            rid = meta[0];
            h.put("Key", key);
            h.put("Value", value);
            h.put("MTime", String.valueOf(mtime));
            h.put("TTL", String.valueOf(ttl));
            h.put("MetaData", rid+","+meta[1]);
            h.put("RuleName", ruleList.getKey(rid));
            list.add(h);
        }
        n = list.size();
        if (n > 0) { // add name and time for checkpoint
            chkpt.put("Name", name);
            chkpt.put("Total", String.valueOf(list.size()));
            chkpt.put("CheckpointTime", String.valueOf(currentTime));
            chkpt.put("MTime", String.valueOf(cache.getMTime()));
            chkpt.put("Status", String.valueOf(cache.getStatus()));
            chkpt.put("Cache", list);
            new Event(Event.INFO, name + " checkpointed " + n +
                " data points").send();
        }
        return chkpt;
    }

    /** restores the state from checkpoint hashmap */
    public void restore(Map<String, Object> chkpt) {
        Object o;
        Map h;
        List list;
        String key, value, ruleName;
        long[] info;
        long mtime, tm;
        int i, n, rid, oid, ttl, status;
        if (chkpt == null || chkpt.size() <= 0)
            return;
        o = chkpt.get("Total");
        if (o != null && o instanceof String) { // check Total
            if ((i = Integer.parseInt((String) o)) <= 0)
                return;
        }
        o = chkpt.get("CheckpointTime");
        if (o != null && o instanceof String)
            tm = Long.parseLong((String) o);
        else
            tm = 0;
        o = chkpt.get("Cache");
        if (o != null && o instanceof List) { // data list
            list = (List) o;
            n = list.size();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                h = (Map) o;
                if (h.size() <= 0)
                    continue;
                if ((o = h.get("Key")) == null)
                    continue;
                key = (String) o;
                if ((o = h.get("Value")) == null)
                    continue;
                value = (String) o;
                if ((o = h.get("RuleName")) == null)
                    continue;
                ruleName = (String) o;
                if ((o = h.get("MTime")) == null)
                    continue;
                mtime = Long.parseLong((String) o);
                if ((o = h.get("TTL")) == null)
                    continue;
                ttl = Integer.parseInt((String) o);
                rid = ruleList.getID(ruleName);
                if (rid < 0)
                    rid = 0;
                info = ruleList.getMetaData(rid);
                if (info[RULE_PID] == TYPE_SCREEN)
                    oid = outLinkMap[RESULT_OUT];
                else
                    oid = outLinkMap[NOHIT_OUT];
                cache.insert(key, mtime, ttl, new int[]{rid, oid}, value);
            }
            // set status
            if ((n = cache.size()) > 0) {
                if ((o = chkpt.get("MTime")) != null)
                    mtime = Long.parseLong((String) o);
                else
                    mtime = System.currentTimeMillis();
                if ((o = chkpt.get("Status")) != null)
                    status = Integer.parseInt((String) o);
                else
                    status = cache.getStatus();
                cache.setStatus(status, mtime);
                new Event(Event.INFO, name + " restored " + n +
                    " data points from checkpoint at " +
                    Event.dateFormat(new Date(tm))).send();
            }
        }
    }

    public void close() {
        super.close();
        cache.clear();
    }
}
