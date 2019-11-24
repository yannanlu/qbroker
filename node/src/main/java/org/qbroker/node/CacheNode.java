package org.qbroker.node;

/* CacheNode.java - a MessageNode caching responses via JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.QList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.GroupedCache;
import org.qbroker.common.SQLUtils;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * CacheNode picks up JMS messages as the requests and looks for cached
 * responses according to their content and predefined rulesets.  If a valid
 * cache is available, its content will be loaded to the incoming message
 * before it is routed to the outlink of done.  If there is no cache or
 * the cache has expired, the incoming message will be routed to the
 * right outlink as a request.  Once the response is collected, CacheNode
 * evaluates the return code and caches the response if it is a success.
 * The message will be routed to the outlink of done.  In case of failure, the
 * incoming messages will be routed to the outlink failure.  If none of the
 * rulesets matches the incoming messages, they will be put to the outlink of
 * nohit. The rest of the outlinks are collectibles to fulfill requests.
 *<br><br>
 * CacheNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups via the property filter.
 * Therefore, each ruleset defines a unique message group. If the associated
 * outlink is none of the fixed outlinks and KeyTemplate is also defined in
 * a ruleset, it is a ruleset of dynamic cache. That outlink is a collectible
 * used to fulfill requests for caching. In a dynamic cache ruleset, other cache
 * parameters may be defined as well, such as TimeToLive, RequestType,
 * DataField, Template and Substitution, etc. TimeToLive controls how long to
 * cache the result. DataField, Template and Substitution are used to modify
 * the request for cache. By default, the request is assumed already built-in.
 * If the associated outlink is one the fixed outlinks and KeyTemplate is
 * defined also, the ruleset is of static cache. In this case, StaticCache can
 * be predefined in the ruleset. It is just a map with predefined key-value
 * pairs. Each cache ruleset maintains its own dedicated cache. Hence,
 * different message groups may have their own ways for the keys and policies.
 * Further more, CacheNode always creates an extra ruleset, nohit.  Ruleset
 * of nohit is for all the messages not hitting any of the predefined rulesets.
 * For a cache ruleset, the number of collected responses is stored in
 * RULE_PEND.  So it is easy to track the hit ratio of the cache.
 *<br><br>
 * CacheNode also supports the invalidation rulesets.  The invalidation ruleset
 * is simliar to the cache ruleset.  The only difference is that the former
 * has also defined TargetRule. TargetRule specifies the name of a cache
 * ruleset for invaliadations. Its KeyTemplate is used to generate group keys
 * for invalidations. Each message of an invalidation ruleset will be routed
 * to its outlink directly. Once the response is successfully collected,
 * CacheNode will invalidate the cache of the target ruleset based on the
 * group keys. When you define an invalidation ruleset, make sure the ruleset
 * referenced by the TargetRule has already been defined before the
 * invalidation ruleset.
 *<br><br>
 * For static cache, CacheNode supports the update ruleset with TargetRule
 * defined for a static cahce rule. When CacheNode invokes an update ruleset,
 * The message payload will be saved into the cache with the given key. So
 * the static cache will cache the data fed by the incoming messages.
 *<br><br>
 * If a ruleset has its preferred outlink to one of the collectibles but
 * has no KeyTemplate defined, it is a simple rule to collect responses.
 * In this rule, CacheNode just routs the requests to the outlink and collects
 * the responses. There is no caching involved with this type of rules.
 *<br><br>
 * CacheNode maintains a dedicated cache for each of the cache rules. But it
 * is assumed that the caching keys are unique across all caches. If you run
 * into key conflict with CacheNode, please use a different instance of
 * CacheNode.
 *<br><br>
 * CacheNode has two properties to control timing. They are Heartbeat in sec
 * and SessionTimeout in sec. Heartbeat is the frequncy to run the sanity
 * check on pending requests. By default, it is 120 sec. SessionTimeout
 * controls the caching time for the templates of JDBC queries. By default,
 * it is 3600 sec.
 *<br><br>
 * You are free to choose any names for the three fixed outlinks.  But
 * CacheNode always assumes the first outlink for done, the second for failure
 * and the third for nohit. The rest of the outlinks are for workers. It is OK
 * for those three fixed outlinks to share the same name. Please make sure the
 * first fixed outlink has the actual capacity no less than that of the uplink.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class CacheNode extends Node {
    private int maxGroup = 1024;
    private int sessionTimeout = 3600000;
    private int heartbeat = 120000;
    private String rcField;
    private int[] outLinkMap;

    private QList pendList = null;     // list of messages pending for response
    private QuickCache templateCache;  // cache of templates for keys

    public final static int OPTION_NONE = 0;
    public final static int OPTION_JDBC = 1;
    public final static int OPTION_HTTP = 2;
    public final static int OPTION_FILE = 3;
    public final static int OPTION_FTP = 4;
    public final static int OPTION_TCP = 5;
    public final static int OPTION_UDP = 6;
    public final static int OPTION_JMS = 7;
    public final static int OPTION_JMX = 8;
    public final static int OPTION_RIAK = 9;
    public final static int OPTION_MONGO = 10;
    public final static int OPTION_REDIS = 11;
    public final static int OPTION_STATIC = 12;
    private final static int ONHOLD_OUT = -1;
    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;
    private int BOUNDARY = NOHIT_OUT + 1;

    public CacheNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        long[] outInfo, ruleInfo;
        String key;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "cache";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("SessionTimeout")) != null) {
            sessionTimeout = 1000 * Integer.parseInt((String) o);
            if (sessionTimeout < 0)
                sessionTimeout = 3600000;
        }
        if ((o = props.get("Heartbeat")) != null) {
            heartbeat = 1000 * Integer.parseInt((String) o);
            if (heartbeat <= 0)
                heartbeat = 120000;
        }
        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("MaxGroup")) == null ||
            (maxGroup = Integer.parseInt((String) o)) <= 0)
            maxGroup = 1024;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, FAILURE_OUT, NOHIT_OUT};
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

        BOUNDARY = outLinkMap[NOHIT_OUT];
        BOUNDARY = (BOUNDARY >= outLinkMap[FAILURE_OUT]) ? BOUNDARY :
            outLinkMap[FAILURE_OUT];
        BOUNDARY ++;
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pendList = new QList(name, capacity);
        templateCache = new QuickCache(name, QuickCache.META_ATAC, 0, 0);
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

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            ruleInfo = ruleList.getMetaData(i);
            if (ruleInfo[RULE_PID] == 0) { // reset target rule id
                Map ph = (Map) ruleList.get(i);
                key = (String) ph.get("TargetRule");
                j = ruleList.getID(key);
                if (j > 0)
                    ruleInfo[RULE_PID] = j;
                else
                    throw(new IllegalArgumentException(name +
                        ": can not find the target rule " + key + " for rule "+
                        ruleList.getKey(i))); 
            }
            if ((debug & DEBUG_INIT) > 0) {
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                    ruleInfo[RULE_GID] + " " + ruleInfo[RULE_EXTRA] + " " +
                    ruleInfo[RULE_MODE] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_DMASK] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
        }
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG, name+
                " RuleName: RID PID TTL GID EXTRA MODE OPTION MASK - OutName" +
                strBuf.toString()).send();
    }

    public int updateParameters(Map props) {
        Object o;
        int i, n;

        n = super.updateParameters(props);
        if ((o = props.get("SessionTimeout")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != sessionTimeout) {
                sessionTimeout = i;
                n++;
            }
        }
        if ((o = props.get("Heartbeat")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != heartbeat) {
                heartbeat = i;
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
        GroupedCache cache;
        Map<String, Object> rule;
        Iterator iter;
        List list;
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
        if(preferredOutName ==null || !assetList.containsKey(preferredOutName)){
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

        // TimeToLive is for cache
        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        // store RCRequired in RULE_MODE field
        if ((o = ph.get("RCRequired")) != null && "false".equals((String) o))
            ruleInfo[RULE_MODE] = 0;
        else
            ruleInfo[RULE_MODE] = 1;

        ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
        ruleInfo[RULE_PID] = TYPE_BYPASS;

        if (ruleInfo[RULE_OID] < BOUNDARY) { // check static cache
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
                rule.put("KeyTemplate", new Template((String) o));
                if((o=ph.get("KeySubstitution")) != null && o instanceof String)
                    rule.put("KeySubstitution",new TextSubstitution((String)o));

                // store request type in RULE_GID field
                ruleInfo[RULE_GID] = OPTION_STATIC;
                if ((o = ph.get("TargetRule")) != null && o instanceof String) {
                    rule.put("TargetRule", (String) o);
                    i = ruleList.getID((String) o);
                    if (i > 0) {
                        ruleInfo[RULE_PID] = TYPE_NONE;
                        o = ruleList.get(i);
                        rule.put("Cache", ((Map) o).get("Cache")); 
                        ruleInfo[RULE_TTL] = ruleList.getMetaData(i)[RULE_TTL];
                    }
                    else {
                        ruleInfo[RULE_PID] = TYPE_BYPASS;
                        new Event(Event.WARNING, name + ": Target Rule for " +
                            ruleName+" not well defined, use default instead: "+
                            (String) o).send();
                    }
                }
                else { // for cache
                    ruleInfo[RULE_PID] = TYPE_CACHE;

                    cache = new GroupedCache(ruleName, maxGroup,
                        QuickCache.META_DEFAULT, 0, 0);
                    rule.put("Cache", cache);
                    if((o = ph.get("StaticCache")) != null && o instanceof Map){
                        // load static cache
                        Map map = (Map) o;
                        for (Object obj : map.keySet()) {
                            key = (String) obj;
                            if (key == null || key.length() <= 0)
                                continue;
                            o = map.get(key);
                            if (o == null || !(o instanceof String))
                                continue;
                            cache.insert(key, tm, 0, null,
                                new String[]{ruleName}, o);
                            ruleInfo[RULE_PEND] ++;
                        }
                        ruleInfo[RULE_TTL] = 0;
                    }
                }
            }
        }
        else if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
            // ruleset for dynamic cache or invalidation
            rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String)o));

            int option;
            if ((o = ph.get("RequestType")) != null && o instanceof String)
                option = getRequestType((String) o);
            else
                option = OPTION_NONE;
            // store request type in RULE_GID field
            ruleInfo[RULE_GID] = option;

            if ((o = ph.get("TargetRule")) != null && o instanceof String) {
                rule.put("TargetRule", (String) o);
                i = ruleList.getID((String) o);
                if (i > 0)
                    ruleInfo[RULE_PID] = i;
                else {
                    ruleInfo[RULE_PID] = TYPE_COLLECT;
                    new Event(Event.WARNING, name + ": Target Rule for " +
                        ruleName + " not well defined, use default instead: " +
                        (String) o).send();
                }
            }
            else { // cache ruleset
                ruleInfo[RULE_PID] = TYPE_CACHE;
                cache = new GroupedCache(ruleName, maxGroup,
                    QuickCache.META_DEFAULT, 0, 0);
                rule.put("Cache", cache);
            }

            if ((o = ph.get("FieldName")) != null && o instanceof String) {
                String dataField;
                str = MessageUtils.getPropertyID((String) o);
                if (str == null)
                    str = (String) o;
                dataField = str;
                if ((o = ph.get("Template")) != null && o instanceof String) {
                    str = (String) o;
                }
                else
                    str = "##" + dataField + "##";
                rule.put("DataField", dataField);
                rule.put("Template", new Template(str));
                if ((o = ph.get("Substitution")) != null && o instanceof String)
                   rule.put("Substitution", new TextSubstitution((String) o));

                // mark RULE_EXTRA field
                ruleInfo[RULE_EXTRA] = 1;
            }
        }
        else { // just collect without cache
            ruleInfo[RULE_PID] = TYPE_COLLECT;

            // store RCrequired in RULE_MODE field
            if((o = ph.get("RCRequired")) != null && "false".equals((String) o))
                ruleInfo[RULE_MODE] = 0;
            else
                ruleInfo[RULE_MODE] = 1;

            if ((o = ph.get("FieldName")) != null && o instanceof String) {
                String dataField;
                str = MessageUtils.getPropertyID((String) o);
                if (str == null)
                    str = (String) o;
                dataField = str;
                if ((o = ph.get("Template")) != null && o instanceof String) {
                    str = (String) o;
                }
                else
                    str = "##" + dataField + "##";
                rule.put("DataField", dataField);
                rule.put("Template", new Template(str));
                if ((o = ph.get("Substitution")) != null && o instanceof String)
                   rule.put("Substitution", new TextSubstitution((String) o));

                // mark RULE_EXTRA field
                ruleInfo[RULE_EXTRA] = 1;
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
     * With the given key of the incoming message, it checks if there is a
     * cached object available.  If the cache is available and valid, it
     * loads the cache to the message and returns RESULT_OUT as the oid to
     * indicate the message is loaded and ready to be delivered to the oid.  
     * If there is no cache, it initializes the empty cache and returns
     * BOUNDARY to indicate the message is a request and the oid should
     * be determined from the ruleset.  If the cache is invalid, it returns
     * ONHOLD_OUT to indicate the message is put on-hold and pending on
     * the previous request to be fulfilled.
     */
    private int cache(String key, long currentTime, int cid, int rid,
        long[] ruleInfo, GroupedCache cache, Message inMessage) {
        Object o;
        String[] group;
        int i = BOUNDARY;

        if (cache == null) // no cache at all
            return i;

        if ((o = cache.get(key, currentTime)) != null) { // got cached object
            try { // load the cache object to msg
                inMessage.clearBody();
                if (inMessage instanceof TextMessage) {
                    ((TextMessage) inMessage).setText((String) o);
                }
                else {
                    String str = (String) o;
                    ((BytesMessage) inMessage).writeBytes(str.getBytes());
                }
                i = RESULT_OUT;
            }
            catch (Exception e) {
                i = BOUNDARY;
                new Event(Event.ERR, name +": "+ ruleList.getKey(rid) +
                    " failed to load cache on " + key + ": " +
                    Event.traceStack(e)).send();
            }
        }
        else if (!cache.containsKey(key)) { // first query
            switch ((int) ruleInfo[RULE_GID]) {
              case OPTION_JDBC:
                group = SQLUtils.getTables(key);
                break;
              default:
                group = new String[]{key};
                break;
            }
            i = cache.insert(key, currentTime, 0, new int[]{cid, rid},
                group, null);
            i = pendList.reserve(cid);
            pendList.add(key, cid);
            i = BOUNDARY;
        }
        else if (cache.isExpired(key, currentTime)) { // already expired
            switch ((int) ruleInfo[RULE_GID]) {
              case OPTION_JDBC:
                group = SQLUtils.getTables(key);
                break;
              default:
                group = new String[]{key};
                break;
            }
            i = cache.insert(key, currentTime, 0, new int[]{cid, rid},
                group, null);
            i = pendList.reserve(cid);
            pendList.add(key, cid);
            i = BOUNDARY;
        }
        else { // not expired yet but object is null and in pending state
            i = pendList.reserve(cid);
            pendList.add(key, cid);
            i = ONHOLD_OUT;
        }
        return i;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null, key = null, dataField = null;
        Object o;
        Object[] asset;
        Map rule = null;
        GroupedCache cache = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        Template temp = null, tmp = null;
        TextSubstitution tsub = null, sub = null;
        String[] propertyName = null;
        long[] outInfo = null, ruleInfo = null;
        int[] ruleMap;
        long currentTime, previousTime, st, wt;
        long count = 0;
        int mask, ii, sz, dspBody, i = 0, n, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
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
            pendList.clear();
            pendList = new QList(name, capacity);
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

        currentTime = System.currentTimeMillis();
        previousTime = currentTime;
        n = ruleMap.length;
        previousRid = -1;
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
                if (pendList.size() > 0) // sanity check pendList
                    check(currentTime, in, buffer);
                previousTime = currentTime;
                if (sessionTimeout > 0 &&
                    currentTime - templateCache.getMTime() >= sessionTimeout) {
                    templateCache.disfragment(currentTime);
                    templateCache.setStatus(templateCache.getStatus(),
                        currentTime);
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
                if (ruleInfo[RULE_PID] == TYPE_CACHE ||
                    ruleInfo[RULE_PID] == TYPE_NONE ||
                    ruleInfo[RULE_PID] > 0) { // for cache or invalidation
                    temp = (Template) rule.get("KeyTemplate");
                    tsub = (TextSubstitution) rule.get("KeySubstitution");
                    cache = (GroupedCache) rule.get("Cache");
                }
                if (ruleInfo[RULE_EXTRA] > 0) { // for data field
                    tmp = (Template) rule.get("Template");
                    sub = (TextSubstitution) rule.get("Substitution");
                    dataField = (String) rule.get("DataField");
                }
                previousRid = rid;
                oid = (int) ruleInfo[RULE_OID];
                outInfo = assetList.getMetaData(oid);
            }

            if ((int) ruleInfo[RULE_PID] != TYPE_BYPASS) try {
                key = null;
                if ((int) ruleInfo[RULE_PID] != TYPE_COLLECT) { // cache key
                    key = MessageUtils.format(inMessage, buffer, temp);
                    if (tsub != null && key != null)
                        key = tsub.substitute(key);
                }

                switch ((int) ruleInfo[RULE_OPTION]) { // reset props
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
            }
            catch (Exception e) {
                if (key != null || (int) ruleInfo[RULE_PID] == TYPE_COLLECT)
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to reset props for " + key + ": " +
                        Event.traceStack(e)).send();
                else
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the cache key: " +
                        Event.traceStack(e)).send();
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
            }
            else if ((int) ruleInfo[RULE_PID] == TYPE_CACHE) { // for cache
                if (key == null)
                    i = FAILURE_OUT;
                else if (ruleInfo[RULE_GID] >= OPTION_STATIC) { // static cache
                    if ((o = cache.get(key, currentTime)) != null) {
                        // got cached object
                        try { // load the cache object to msg
                            inMessage.clearBody();
                            if (inMessage instanceof TextMessage) {
                                ((TextMessage) inMessage).setText((String) o);
                            }
                            else {
                                String str = (String) o;
                           ((BytesMessage)inMessage).writeBytes(str.getBytes());
                            }
                            MessageUtils.setProperty(rcField, "0", inMessage);
                            i = RESULT_OUT;
                        }
                        catch (Exception e) {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to load cache on " + key + ": " +
                                Event.traceStack(e)).send();
                        }
                    }
                    else { // no key in cache or key expired
                        i = NOHIT_OUT;
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " has no cache found for " + key).send();
                    }
                }
                else { // for dynamic caches
                    if (key.indexOf("%%") < 0) // key is not a template
                        i = cache(key, currentTime, cid, rid, ruleInfo, cache,
                            inMessage);
                    else try { // key is a template
                        Template template = null;
                        if ((o = templateCache.get(key, currentTime)) != null) {
                            template = (Template) o;
                            key = MessageUtils.format(inMessage, buffer,
                                template);
                        }
                        else if (!templateCache.containsKey(key)) { //first time
                            template = new Template(key, "%%[^%]+%%");
                            if (template.numberOfFields() > 0) {
                                templateCache.insert(key, currentTime,
                                    sessionTimeout, null, template);
                                key = MessageUtils.format(inMessage, buffer,
                                    template);
                            }
                        }
                        else if (templateCache.isExpired(key, currentTime)) {
                            template = new Template(key, "%%[^%]+%%");
                            if (template.numberOfFields() > 0) {
                                templateCache.insert(key, currentTime,
                                    sessionTimeout, null, template);
                                key = MessageUtils.format(inMessage, buffer,
                                    template);
                            }
                        }
                        else {
                            templateCache.expire(key, currentTime);
                            template = new Template(key, "%%[^%]+%%");
                            if (temp.numberOfFields() > 0) {
                                templateCache.insert(key, currentTime,
                                    sessionTimeout, null, template);
                                key = MessageUtils.format(inMessage, buffer,
                                    template);
                            }
                        }
                        i = cache(key, currentTime, cid, rid, ruleInfo, cache,
                            inMessage);
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name +": "+ ruleName +
                            " failed to reformat " + key + ": " +
                            Event.traceStack(e)).send();
                    }
                }

                if (i == ONHOLD_OUT) { // msg on pending
                    if ((debug & DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG,name + " pending: " + key +" = "+
                             cid + ":" + rid + "/" + pendList.size() + " " +
                             ruleInfo[RULE_PEND] + " " + cache.size()).send();
                    continue;
                }
                else if (i >= BOUNDARY) { // for empty or invalid cache
                    String errStr = null;
                    filter = null;
                    if (ruleInfo[RULE_EXTRA] > 0) try { // set data field
                        String str= MessageUtils.format(inMessage, buffer, tmp);
                        if (sub != null)
                            str = sub.substitute(str);
                        if ("body".equals(dataField)) {
                            inMessage.clearBody();
                            if (inMessage instanceof TextMessage)
                                ((TextMessage) inMessage).setText(str);
                            else if (inMessage instanceof BytesMessage)
                           ((BytesMessage)inMessage).writeBytes(str.getBytes());
                            else
                                errStr = " failed to set body: bad msg family";
                        }
                        else
                            MessageUtils.setProperty(dataField, str, inMessage);
                    }
                    catch (Exception e) {
                        errStr = " failed to set data field on " + dataField +
                            ": " + e.toString();
                    }

                    if (errStr != null) { // failure
                        oid = outLinkMap[FAILURE_OUT];
                        pendList.takeback(cid);
                        cache.update(key, currentTime, 1000,
                            new int[]{cid, rid}, null);
                        cache.expire(key, currentTime);
                        try {
                            MessageUtils.setProperty(rcField, "-1", inMessage);
                        }
                        catch (Exception e) {
                            errStr += ", and failed to set rc also";
                        }
                        new Event(Event.ERR, name + ": " + ruleName +
                            errStr).send();
                    }
                    else
                        oid = (int) ruleInfo[RULE_OID];
                }
                else if (i == RESULT_OUT) { // cache loaded
                    oid = outLinkMap[RESULT_OUT];
                }
                else { // failure or nohit
                    oid = outLinkMap[i];
                    filter = null;
                    try {
                        MessageUtils.setProperty(rcField,
                            ((i == FAILURE_OUT) ? "-1" : "1"), inMessage);
                    }
                    catch (Exception e) {
                    }
                }
            }
            else if (ruleInfo[RULE_PID] == TYPE_NONE) { // for update on cache
                i = 0;
                if (key == null || key.length() <= 0)
                    i = FAILURE_OUT;
                else try { // retrieve the payload and update cache
                    int ttl = (int) ruleInfo[RULE_TTL];
                    if (!ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer); 
                    if (cache.containsKey(key)) // update
                        cache.update(key, currentTime, ttl, null,
                            new String(msgStr));
                    else { // insert
                        String str = (String) rule.get("TargetRule");
                        cache.insert(key, currentTime, ttl, null,
                            new String[]{str}, new String(msgStr));
                        ruleList.getMetaData(str)[RULE_PEND] ++;
                    }
                    ruleInfo[RULE_PEND] ++;
                    MessageUtils.setProperty(rcField, "0", inMessage);
                }
                catch (Exception e) {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to update cache for " + key + ": " +
                        Event.traceStack(e)).send();
                }
                if (i == FAILURE_OUT) {
                    oid = outLinkMap[FAILURE_OUT];
                    filter = null;
                    try {
                        MessageUtils.setProperty(rcField, "-1", inMessage);
                    }
                    catch (Exception e) {
                    }
                }
                else {
                    String str = (String) rule.get("TargetRule");
                    oid = (int) ruleInfo[RULE_OID];
                    if ((debug & DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name + " updating: " + str +
                             " with " + key + " = "+ cid + ":" + rid + " " +
                             ruleInfo[RULE_PEND] + " " + cache.size()).send();
                }
            }
            else if (ruleInfo[RULE_PID] > 0) { // for invalidations
                String errStr = null;
                filter = null;
                if (key != null && key.length() > 0) {
                   //reserve a cell in pendList to store group keys from the key
                    pendList.reserve(cid);
                    switch ((int) ruleInfo[RULE_GID]) {//get group keys from key
                      case OPTION_JDBC:
                        pendList.add(SQLUtils.getTables(key), cid);
                        break;
                      default:
                        pendList.add(new String[]{key}, cid);
                    }
                    if (ruleInfo[RULE_EXTRA] > 0) try { // set data field
                        String str= MessageUtils.format(inMessage, buffer, tmp);
                        if (sub != null)
                            str = sub.substitute(str);
                        if ("body".equals(dataField)) {
                            inMessage.clearBody();
                            if (inMessage instanceof TextMessage)
                                ((TextMessage) inMessage).setText(str);
                            else if (inMessage instanceof BytesMessage)
                           ((BytesMessage)inMessage).writeBytes(str.getBytes());
                            else
                                errStr = " failed to set body: bad msg family";
                        }
                        else
                            MessageUtils.setProperty(dataField, str, inMessage);
                    }
                    catch (Exception e) {
                        errStr = " failed to set data field on " + dataField +
                            ": " + e.toString();
                    }
                    if (errStr != null) { // failed
                        pendList.takeback(cid);
                        oid = outLinkMap[FAILURE_OUT];
                        try {
                            MessageUtils.setProperty(rcField, "-1", inMessage);
                        }
                        catch (Exception e) {
                            errStr += ", and failed to set rc also";
                        }
                        new Event(Event.ERR, name +": "+ruleName+errStr).send();
                    }
                    else
                        oid = (int) ruleInfo[RULE_OID];
                }
                else { // key is null or empty
                    oid = outLinkMap[FAILURE_OUT];
                    try {
                        MessageUtils.setProperty(rcField, "-1", inMessage);
                    }
                    catch (Exception e) {
                    }
                }
            }
            else if (ruleInfo[RULE_PID] == TYPE_COLLECT) { // for collect
                String errStr = null;
                filter = null;
                if (ruleInfo[RULE_EXTRA] > 0) try { // set data field
                    String str = MessageUtils.format(inMessage, buffer, tmp);
                    if (sub != null)
                        str = sub.substitute(str);
                    if ("body".equals(dataField)) {
                        inMessage.clearBody();
                        if (inMessage instanceof TextMessage)
                            ((TextMessage) inMessage).setText(str);
                        else if (inMessage instanceof BytesMessage)
                           ((BytesMessage)inMessage).writeBytes(str.getBytes());
                        else
                            errStr = " failed to set body: bad msg family";
                    }
                    else
                        MessageUtils.setProperty(dataField, str, inMessage);
                }
                catch (Exception e) {
                    errStr = " failed to set data field on " + dataField +
                        ": " + e.toString();
                }

                if (errStr != null) { // failed
                    oid = outLinkMap[FAILURE_OUT];
                    try {
                        MessageUtils.setProperty(rcField, "-1", inMessage);
                    }
                    catch (Exception e) {
                        errStr += ", and failed to set rc also";
                    }
                    new Event(Event.ERR, name + ": " + ruleName +errStr).send();
                }
                else
                    oid = (int) ruleInfo[RULE_OID];
            }
            else { // for bypass
                oid = (int) ruleInfo[RULE_OID];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid).send();

            // display message
            if (oid < BOUNDARY && ruleInfo[RULE_DMASK] > 0) try {
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleName + " processed msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try {
                filter.format(inMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to format the msg: " + Event.traceStack(e)).send();
            }

            i = passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            if (i > 0)
                count ++;
            else if (oid >= BOUNDARY && ruleInfo[RULE_PID] != TYPE_COLLECT) {
                // rollback
                pendList.takeback(cid);
                cache.update(key, currentTime, 1000, new int[]{cid, rid}, null);
                cache.expire(key, currentTime);
            }
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * It collects the message at mid and moves it to its next output XQueue
     * so that a new message can be added to the vacant cell.  It returns
     * the cell id of the new output XQueue upon success or a negative number
     * if there is no vacancies in the new output XQueue. If the vacant cell
     * of out is reused, it returns either -2 to skip the cancellation of the
     * previous reservation or -3 for no previous reservation. Otherwise, the
     * cell will be vacant, and either RESULT_OUT for a good response or
     * FAILURE_OUT for a bad respones is returned.
     */
    private int collect(long currentTime, XQueue in, int mid, XQueue out) {
        long[] state, outInfo, ruleInfo;
        Object[] asset = null;
        Object o;
        Map rule;
        GroupedCache cache;
        XQueue xq = out;
        Message msg;
        byte[] buffer = new byte[bufferSize];
        MessageFilter filter = null;
        String[] propertyName = null;
        String msgStr = null, str = null, ruleName, key = null;
        boolean bypass = true;
        long t;
        int id = -1, i, j, k, m, cid, oid, pid, rid, rc;
        int len, shift, outCapacity;
        if (mid < 0 || mid >= capacity)
            return -1;
        state = msgList.getMetaData(mid);
        if (state == null || (pid = (int) state[MSG_OID]) < BOUNDARY)
            return -1;
        id = (int) state[MSG_BID];
        rid = (int) state[MSG_RID];
        cid = (int) state[MSG_CID];
        i = id;

        ruleInfo = ruleList.getMetaData(rid);
        if (ruleInfo[RULE_PID] >= 0) { // for invalidation
            rule = (Map) ruleList.get((int) ruleInfo[RULE_PID]);
            cache = (GroupedCache) rule.get("Cache");
            rule = (Map) ruleList.get(rid);
        }
        else { // for cache or collect
            rule = (Map) ruleList.get(rid);
            cache = (GroupedCache) rule.get("Cache");
        }
        ruleName = ruleList.getKey(rid);
        filter = (MessageFilter) rule.get("Filter"); 
        propertyName = (String[]) rule.get("PropertyName");
        
        msg = (Message) in.browse(cid);
        if (msg == null)
            rc = -3;
        else try {
            str = MessageUtils.getProperty(rcField, msg);
            if (str != null)
                rc = Integer.parseInt(str);
            else if (ruleInfo[RULE_MODE] > 0) // RC is required
                rc = -2;
            else
                rc = 0;
        }
        catch (Exception e) {
            rc = -4;
        }

        if (str == null && msg != null) try { // set rc if it is empty
            MessageUtils.setProperty(rcField, (rc == 0) ? "0" : "-2", msg);
        }
        catch (Exception e) {
        }

        if (rc != 0) { // request failed somehow
            filter = null;
            oid = outLinkMap[FAILURE_OUT];
            new Event(Event.ERR, name + ": " + ruleName +
                " got a failed request from " + out.getName() +": "+ rc).send();
        }
        else
            oid = outLinkMap[RESULT_OUT];

        outInfo = assetList.getMetaData(oid);
        int ttl = (int) ruleInfo[RULE_TTL];
        int[] meta = null;
        if (ruleInfo[RULE_PID] == TYPE_CACHE) { // for cache
            key = (String) msgList.get(mid);
            meta = cache.getMetaData(key);
            bypass = false;
            if (meta == null || cid != meta[0]) // bypass only if not the owner
                bypass = true;
            else if (rc == 0) try { // update cache 
                msgStr = MessageUtils.processBody(msg, buffer);
            }
            catch (Exception e) {
                msgStr = null;
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to retrieve response from msg on " + key +
                    ": " + e.toString()).send();
            }
        }
        else if (ruleInfo[RULE_PID] > 0) { // for invalidation
            // retrieve group keys for invalidation
            String[] keys = (String[]) msgList.get(mid);
            key = keys[0];
            if (rc != 0) // request failed so disable the invalidation
                keys = new String[0];
            m = 0;
            for (j=0; j<keys.length; j++) { // invalidate the groups via keys
                if (keys[j] == null || keys[j].length() <= 0)
                    continue;
                k = cache.groupID(keys[j]);
                if (k >= 0) {
                    cache.invalidateGroup(k, currentTime);
                    m ++;
                }
            }
            if ((debug & DEBUG_COLL) > 0 && m > 0)
                new Event(Event.DEBUG, name+": invalidated " +m+ " groups on "+
                    ruleList.getKey((int) ruleInfo[RULE_PID])).send();
        }
        else { // for collect
            key = (String) msgList.get(mid);
        }

        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " collect: " + pid + "/" + oid +
                ": " + rc + " " +  ((bypass) ? 1 : 0) + " for " + key).send();

        if (ruleInfo[RULE_DMASK] > 0) try { // display message
            new Event(Event.INFO, name + ": " + ruleName + " collected 1 msg: "+
                MessageUtils.display(msg, msgStr, (int) ruleInfo[RULE_DMASK],
                propertyName)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " + ruleName +
                " failed to display collected msg: " + e.toString()).send();
        }

        if (filter != null && filter.hasFormatter()) try { // post format
            filter.format(msg, buffer);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to format the msg: " + Event.traceStack(e)).send();
        }

        asset = (Object[]) assetList.get(oid);
        out = (XQueue) asset[ASSET_XQ];
        len = (int) outInfo[OUT_LENGTH];
        m = 100;
        switch (len) {
          case 0:
            shift = 0;
            for (j=0; j<m; j++) { // reserve any empty cell
                id = out.reserve(waitTime);
                if (id >= 0)
                    break;
            }
            break;
          case 1:
            shift = (int) outInfo[OUT_OFFSET];
            for (j=0; j<m; j++) { // reserve the empty cell
                id = out.reserve(waitTime, shift);
                if (id >= 0)
                    break;
            }
            break;
          default:
            shift = (int) outInfo[OUT_OFFSET];
            for (j=0; j<m; j++) { // reserve an partitioned empty cell
                id = out.reserve(waitTime, shift, len);
                if (id >= 0)
                    break;
            }
            break;
        }
        outCapacity = (int) outInfo[OUT_CAPACITY];

        t = System.currentTimeMillis();
        if (id >= 0 && id < outCapacity) { // id-th cell of oid reserved
            k = -1;
            str = oid + "/" + id;
            m = msgList.getID(str);
            if (m >= 0) { // passback the empty cell
                long[] rf;
                cells.collect(-1L, m);
                k = (int) msgList.getMetaData(m)[MSG_RID];
                msgList.remove(m);
                in.remove(m);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                rf = ruleList.getMetaData(k);
                rf[RULE_SIZE] --;
                rf[RULE_COUNT] ++;
                rf[RULE_TIME] = t;
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name + " passback: " + k + " " +
                        m + "/" + m +" "+ oid + ":" + id + " " +
                        rf[RULE_SIZE] + " " + outInfo[OUT_SIZE]+
                        " " + out.size() + "|" + out.depth() + " " +
                        msgList.size()).send();
            }

            // update the key for reverse lookup
            msgList.remove(mid);
            j = msgList.add(str, state, str, mid);
            k = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;

            // update the state of the responder XQ
            k = (int) state[MSG_BID];
            outInfo = assetList.getMetaData(pid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            ruleInfo[RULE_PEND] ++;
            ruleInfo[RULE_TIME] = t;
            state[MSG_BID] = id;
            state[MSG_OID] = oid;
            state[MSG_TID] = 0;
            if ((debug & DEBUG_PASS) > 0)
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ": " + mid + " " + str + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
        }
        else { // failed to reserve a cell on out
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to reserve a cell for collected msg with " + key +
                ": " + oid +"/" +id).send();
            return -1;
        }

        if (ruleInfo[RULE_PID] != TYPE_CACHE) // for invalidation or collect
            return id;
        else if (bypass)
            return id;
        else if (msgStr != null) { // update cache, flush other pending messages
            if (msg instanceof TextMessage) // cache a copy
                cache.update(key, currentTime, ttl, meta, new String(msgStr));
            else
                cache.update(key, currentTime, ttl, meta, msgStr);

            int n = pendList.size();
            int[] list = new int[n];
            n = pendList.queryIDs(list, XQueue.CELL_OCCUPIED);
            k = 0;
            for (j=0; j<n; j++) {
                cid = list[j];
                o = pendList.browse(cid);
                if (o == null) {
                    pendList.takeback(cid);
                    continue;
                }
                if (!(o instanceof String)) // skip group keys for invalidation
                    continue;
                str = (String) o;
                if (!key.equals(str))
                    continue;
                msg = (Message) in.browse(cid);
                if (msg == null) {
                    pendList.takeback(cid);
                    continue;
                }

                try { // flush all pending msg
                    if (msg instanceof TextMessage) {
                        msg.clearBody();
                        ((TextMessage) msg).setText(msgStr);
                        MessageUtils.setProperty(rcField, "0", msg);
                    }
                    else if (msg instanceof BytesMessage) {
                        msg.clearBody();
                        ((BytesMessage) msg).writeBytes(msgStr.getBytes());
                        MessageUtils.setProperty(rcField, "0", msg);
                    }
                    else {
                        MessageUtils.setProperty(rcField, "-1", msg);
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to load response to msg due to " +
                            "wrong msg family").send();
                    }
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to load response to msg: " +
                        e.toString()).send();
                    continue;
                }

                if (ruleInfo[RULE_DMASK] > 0) try { // display message
                    new Event(Event.INFO, name + ": " + ruleName +
                        " flushed 1 msg: " + MessageUtils.display(msg, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display flushed msg: " +
                        e.toString()).send();
                }

                if (filter != null && filter.hasFormatter()) try { //post format
                    filter.format(msg, buffer);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the msg: " +
                        Event.traceStack(e)).send();
                }

                if (passthru(currentTime, msg, in, rid, oid, cid, -1) > 0) {
                    // add cache hit count and remove pending
                    pendList.takeback(cid);
                    k ++;
                }
                else {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to flush a msg at " +cid+ " for "+key).send();
                }
            }
            if ((debug & DEBUG_COLL) > 0 && k > 0)
                new Event(Event.DEBUG, name + " flush: " + k + "/" +
                    pendList.size() + " for " + key).send();
        }
        else { // failed or empty response for cache so clean up pending msgs
            int n = pendList.size();
            int[] list = new int[n];
            n = pendList.queryIDs(list, XQueue.CELL_OCCUPIED);
            for (j=0; j<n; j++) {
                cid = list[j];
                o = pendList.browse(cid);
                if (o == null) {
                    pendList.takeback(cid);
                    continue;
                }
                if (!(o instanceof String)) // skip group keys for invalidation
                    continue;
                str = (String) o;
                if (!key.equals(str))
                    continue;
                msg = (Message) in.browse(cid);
                if (msg == null) {
                    pendList.takeback(cid);
                    continue;
                }
                // retry on the first pending msg
                oid = (int) ruleInfo[RULE_OID];
                id = i;
                out = xq;
                outInfo = assetList.getMetaData(oid);
                // update ownership
                meta[0] = cid;
                cache.update(key, currentTime, 0, meta, null);
                // passthru msg with recycled id
                str = oid + "/" + id;
                k = out.getCellStatus(id);
                if (k == XQueue.CELL_RESERVED) { // id already reserved
                    m = msgList.add(str, new long[]{cid, oid, id, rid, 0,
                        currentTime}, str, cid);
                    out.add(msg, id, cbw);
                    outInfo[OUT_SIZE] ++;
                    // id has been reused so return -2 to notify the caller
                    return -2;
                }
                else if (id == out.reserve(waitTime, id)) { // id just reserved
                    m = msgList.add(str, new long[]{cid, oid, id, rid, 0,
                        currentTime}, str, cid);
                    out.add(msg, id, cbw);
                    outInfo[OUT_SIZE] ++;
                    // id has been reused so return -1 to notify the caller
                    return -3;
                }
                else { // something really wrong thus failed to reserve the id
                    new Event(Event.CRIT, name + ": " + ruleName +
                        " bad status for cell " + id + " of " +
                        out.getName() + ": " + k + "/" + out.getCellStatus(id)+
                        " while retry on " + key + ": " + cid).send();
                    break;
                }
            }
            // no pending found so expire the cache
            cache.update(key, currentTime, 1000, new int[]{mid, rid}, null);
            cache.expire(key, currentTime);
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + ": " + ruleName +
                    " expired the cache for " + key).send();
        }

        return id;
    }

    /**
     * It looks for any pending request that is either fulfilled or expired.
     * If such a pending request is found, it will be removed from the pending
     * list after its passthru. At the end, the method returns the total number
     * of processed requests.
     *<br><br>
     * This method is not efficient. So use it in low frequency.
     */
    private int check(long currentTime, XQueue in, byte[] buffer) {
        Message msg;
        Object o;
        GroupedCache cache = null;
        Map rule = null;
        String key, msgStr;
        long[] ruleInfo = null;
        int i, k, count, cid, oid, rid;
        Browser b, browser;

        b = pendList.browser();
        browser = ruleList.browser();

        count = 0;
        while ((cid = b.next()) >= 0) {
            o = pendList.browse(cid);
            if (o == null) {
                pendList.takeback(cid);
                continue;
            }
            if (!(o instanceof String)) // skip group keys for invalidation
                continue;
            key = (String) o;

            msg = (Message) in.browse(cid);
            if (msg == null) {
                pendList.takeback(cid);
                continue;
            }

            browser.reset();
            rid = 0;
            while ((k = browser.next()) > 0) { // search for cache with the key
                ruleInfo = ruleList.getMetaData(k);
                if (ruleInfo[RULE_OID] < BOUNDARY ||
                    ruleInfo[RULE_PID] != TYPE_CACHE) // only check cache rules
                    continue;
                rule = (Map) ruleList.get(k);
                cache = (GroupedCache) rule.get("Cache");
                if (cache.containsKey(key)) {
                    rid = k;
                    break;
                }
            }
            if (rid == 0) { // no cache contains key, default to NOHIT
                oid = outLinkMap[NOHIT_OUT];
                msgStr = null;
                if (rule == null) { // make sure rule and ruleInfo is set
                    rule = (Map) ruleList.get(0);
                    ruleInfo = ruleList.getMetaData(0);
                }
            }
            else if ((o = cache.get(key, currentTime)) != null) { // valid cache
                oid = outLinkMap[RESULT_OUT];
                msgStr = (String) o;
            }
            else if (cache.isExpired(key, currentTime)) { // already expired
                oid = (int) ruleInfo[RULE_OID];
                msgStr = null;
            }
            else { // pending for response
                continue;
            }

            if (oid >= BOUNDARY) { // retry
                cache.update(key, currentTime, 0, new int[]{cid, rid}, null);
            }
            else { // flush the pending msg
                MessageFilter filter = (MessageFilter) rule.get("Filter");
                String[] propertyName = (String[]) rule.get("PropertyName");
                String ruleName = ruleList.getKey(rid);
                if (rid > 0) try { // load cache to msg
                    if (msg instanceof TextMessage) {
                        msg.clearBody();
                        ((TextMessage) msg).setText(msgStr);
                        MessageUtils.setProperty(rcField, "0", msg);
                    }
                    else if (msg instanceof BytesMessage) {
                        msg.clearBody();
                        ((BytesMessage) msg).writeBytes(msgStr.getBytes());
                        MessageUtils.setProperty(rcField, "0", msg);
                    }
                    else {
                        MessageUtils.setProperty(rcField, "-1", msg);
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to load response to msg due to " +
                            "wrong msg family").send();
                    }
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to load response to msg: " +
                        e.toString()).send();
                    continue;
                }

                if (ruleInfo[RULE_DMASK] > 0) try { // display message
                    new Event(Event.INFO, name + ": " + ruleName +
                        " flushed 1 msg: " + MessageUtils.display(msg, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display flushed msg: " +
                        e.toString()).send();
                }

                if (filter != null && filter.hasFormatter()) try { //post format
                    switch ((int) ruleInfo[RULE_OPTION]) {
                      case RESET_MAP:
                        MessageUtils.resetProperties(msg);
                        if (msg instanceof MapMessage)
                           MessageUtils.resetMapBody((MapMessage) msg);
                        break;
                      case RESET_ALL:
                        MessageUtils.resetProperties(msg);
                        break;
                      case RESET_SOME:
                        if (!(msg instanceof JMSEvent))
                            MessageUtils.resetProperties(msg);
                        break;
                      case RESET_NONE:
                      default:
                        break;
                    }

                    filter.format(msg, buffer);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the msg: " +
                        Event.traceStack(e)).send();
                }
            }

            i = passthru(currentTime, msg, in, rid, oid, cid, 0);
            if (i > 0) { // passthru is done
                count ++;
                pendList.takeback(cid);
            }
        }

        return count;
    }

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise. If tid is less than 0, msg will
     * not be put back to uplink in case of failure.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo;
        int id = -1, k, mid, mask, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            if (tid >= 0) // normal
                k = in.putback(cid);
            else // putback disabled
                k = -1;
            new Event(Event.ERR, name + ": asset is null on " +
                assetList.getKey(oid) + " of " + oid + " for " +
                rid + " with msg at " + cid).send();
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
            shift = (int) outInfo[OUT_OFFSET];
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
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            ruleInfo = ruleList.getMetaData(rid);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                if (oid < BOUNDARY || ruleInfo[RULE_PID] == TYPE_COLLECT)
                    mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                        currentTime}, key, cid);
                else // for requests
                    mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                        currentTime}, pendList.takeback(cid), cid);
            }
            else if (oid < BOUNDARY) { // an exit msg
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
                mid = msgList.add(key, new long[] {cid, oid, id, rid, tid,
                    currentTime}, key, cid);
            }
            else { // it is a response msg to be collected
                cells.collect(-1L, mid);
                k = collect(currentTime, in, mid, out);
                if (k >= 0) {
                    if (ruleInfo[RULE_PID] == TYPE_COLLECT) // for collect
                        mid = msgList.add(key, new long[]{cid, oid, id, rid,
                            tid, currentTime}, key, cid);
                    else // a collectible
                        mid = msgList.add(key, new long[]{cid, oid, id, rid,
                            tid, currentTime}, pendList.takeback(cid), cid);
                }
                else { // failed to collect the msg
                    if (k != -2) // id is not reused
                        out.cancel(id);
                    if (tid >= 0)
                        k = in.putback(cid);
                    else
                        k = -1;
                    new Event(Event.ERR, name+": failed to collect the msg at "+
                        mid + ":" + cid + " with " + key + " for " +
                        ruleList.getKey(rid) + ": " + msgList.size()).send();
                    return 0;
                }
            }
            if (mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                if (tid >= 0) // normal
                    k = in.putback(cid);
                else
                    k = -1;
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            k = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = currentTime;
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] ++;
            ruleInfo[RULE_TIME] = currentTime;
            if ((debug & DEBUG_PASS) > 0)
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ":" + cid + " " + key + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
        }
        else { // reservation failed
            if (tid >= 0) // noraml
                k = in.putback(cid);
            else // putback disabled
                k = -1;
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                assetList.getKey(oid) + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k + " " + tid).send();
            return 0;
        }
        return 1;
    }

    /**
     * It returns the number of done messages removed from the input XQueue.
     * If milliSec is less than 0, there is no wait and it tries to collect all
     * cells. Otherwise, it just tries to collect the first collectible cell.
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        String key;
        int k, mid, rid, oid, id, l = 0;
        long[] state, outInfo, ruleInfo;
        long t;
        StringBuffer strBuf = null;
        if ((debug & DEBUG_FBAK) > 0)
            strBuf = new StringBuffer();

        k = 0;
        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null)
                continue;
            out = (XQueue) asset[ASSET_XQ];
            id = (int) state[MSG_BID];
            if (oid >= BOUNDARY) { // for collectible messages
                collect(t, in, mid, out);
                continue;
            }
            // for exit messages
            in.remove(mid);
            msgList.remove(mid);
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = t;
            if ((debug & DEBUG_FBAK) > 0)
                strBuf.append("\n\t" + rid + " " + mid + "/" + state[MSG_CID] +
                    " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth() +
                    " " + msgList.size());
            l ++;
            if (milliSec >= 0) // only one collection a time
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
     * cleans up MetaData for all XQs and messages
     */
    public void resetMetaData(XQueue in, XQueue[] out) {
        Object[] asset;
        XQueue xq;
        Map rule;
        GroupedCache cache;
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

        browser = pendList.browser();
        while ((i = browser.next()) >= 0) {
            in.putback(i);
        }
        pendList.clear();

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            state = assetList.getMetaData(i);
            state[OUT_SIZE] = 0;
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            state = ruleList.getMetaData(i);
            state[RULE_SIZE] = 0;
            rule = (Map) ruleList.get(i);
            cache = (GroupedCache) rule.get("Cache");
            if (cache != null)
                cache.clear();
        }
    }

    /**
     * returns the BOUNDARY which is the lowest id of outlinks for collectibles
     * so that any outlink with either the same id or a higher id is for
     * collectibles. The EXTERNAL_XA bit of all outlinks for collectibles has
     * to be disabled by the container to stop ack propagations downsstream.
     */
    public int getOutLinkBoundary() {
        return BOUNDARY;
    }

    public void close() {
        Map rule;
        Browser browser;
        GroupedCache cache;
        int i;

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            rule = (Map) ruleList.get(i);
            cache = (GroupedCache) rule.get("Cache");
            if (cache != null)
                cache.clear();
        }
        pendList.clear();
        templateCache.clear();
        super.close();
    }

    protected void finalize() {
        close();
    }

    /** returns the id for the request or 0 if the type is not supported */
    protected static final int getRequestType(String type) {
        int op = OPTION_NONE;
        if (type != null && type.length() > 0) {
            switch (type.charAt(0)) {
              case 'f':
              case 'F':
                if ("file".equalsIgnoreCase(type))
                  op = OPTION_FILE;
                else if ("ftp".equalsIgnoreCase(type))
                  op = OPTION_FTP;
                break;
              case 'h':
              case 'H':
                if ("http".equalsIgnoreCase(type))
                    op = OPTION_HTTP;
                break;
              case 'j':
              case 'J':
                if ("jdbc".equalsIgnoreCase(type))
                    op = OPTION_JDBC;
                else if ("jms".equalsIgnoreCase(type))
                    op = OPTION_JMS;
                else if ("jmx".equalsIgnoreCase(type))
                    op = OPTION_JMX;
                break;
              case 'm':
              case 'M':
                if ("mongo".equalsIgnoreCase(type))
                    op = OPTION_MONGO;
                break;
              case 'r':
              case 'R':
                if ("riak".equalsIgnoreCase(type))
                    op = OPTION_RIAK;
                else if ("redis".equalsIgnoreCase(type))
                    op = OPTION_REDIS;
                break;
              case 's':
              case 'S':
                if ("static".equalsIgnoreCase(type))
                    op = OPTION_STATIC;
                break;
              case 't':
              case 'T':
                if ("tcp".equalsIgnoreCase(type))
                    op = OPTION_TCP;
                break;
              case 'u':
              case 'U':
                if ("udp".equalsIgnoreCase(type))
                    op = OPTION_UDP;
                break;
              default:
                break;
            }
        }
        return op;
    }
}
