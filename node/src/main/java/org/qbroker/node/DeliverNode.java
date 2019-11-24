package org.qbroker.node;

/* DeliverNode.java - a MessageNode delivering JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.StringReader;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * DeliverNode listens to an input XQ for JMS messages with an arbitray URI
 * as their destinations.  For each incoming message, DeliverNode looks up
 * its URI in the local cache for any XQueue as the transmit queue established
 * for the destination. If there is an XQueue in the cache for the destination,
 * DeliverNode just puts the incoming message to the XQueue as the delivery.
 * Otherwise, DeliverNode will generate a request with the URI and a newly
 * created XQueue for the destination. The request will be sent to the outlink
 * connected to a PersisterPool for a new persister thread. Once the persister
 * is instantiated, DeliverNode will collect the response from the
 * PersisterPool.  The response is supposed to contain the persister thread
 * for delivering the messages to the specific destination.  DeliverNode
 * caches the thread and the id of the XQueue using the URI as the key.
 * Therefore, DeliverNode is able to deliver JMS messages to arbitrary
 * destinations based to their URIs and the predefined rulesets on-demand.
 * <br><br>
 * It has two types of outlinks, position-fixed and non-fixed.
 * There are three position-fixed outlinks: pool for all requests to the
 * PersisterPool, failure for the messages failed in the delivery process,
 * nohit for those messages not covered by any rulesets.  The non-fixed
 * outlinks are transmit queues for the on-demand destinations.
 *<br><br>
 * DeliverNode also contains a number of predefined rulesets.  These
 * rulesets categorize messages into non-overlapping groups.  Therefore,
 * each rule defines a unique message group.  The ruleset also specifies
 * the way to construct URI and properties for the new persisters.  For those
 * messages falling off all defined rulesets, DeliverNode always creates
 * an extra ruleset, nohit, to handle them.  Therefore all the nohit messages
 * will be routed to nohit outlink.  The downstream node at nohit is supposed
 * to handle all nohit messages.
 *<br><br>
 * URI is used to identify destinations.  In order to construct the URI for an
 * arbitrary destination, each ruleset contains two sets of format operations.
 * The first one is the array of templates with the name of URITemplate.
 * The other is the array of substitutions with the name of URISubstitution.
 * URITemplate appends the previous non-empty text to URI and sets the next
 * initial text for its corresponding URISubstitutions to modify.
 * The associations between the URITemplates and URISubstitutions are based on
 * their positions.  Either URITemplate or URISubstitution can be null for
 * no action and a place holder.  Therefore, you can insert multiple null
 * URITemplates so that the associated URISubstitutions will be able to modify
 * the same text in turns.  If any of the operations fails, the message will
 * be routed to failure outlink.  Besides, a ruleset may contain a hash of
 * DefaultProperty as static or dynamic properties for the new persisters.
 * If the URI string is not in the cache, DeliverNode will try to resolve all
 * the dynamic variables in the hash of DefaultProperty from the incoming
 * message first. Then it adds the DefaultProperty to the new request for the
 * new persister. So the extra properties are able to passed over to the
 * persister pool.
 *<br><br>
 * For each new destination, DeliverNode creates an Object message as the
 * request containing the URI and the XQueue, as well as the other
 * properties provided they are defined.  The request is sent to the
 * PersisterPool via the pool outlink.  Then DeliverNode frequently checks the
 * response for each outstanding requests.  The response is supposed to have
 * the status and the persister thread for the new destination.  DeliverNode
 * will use the thread to monitor its status.  If the response does not have
 * the thread, DeliverNode will route the messages to the failure outlink and
 * remove the XQueue and the URI from the cache. The samething will happen if
 * the request for a new persister times out.  MaxRetry is used to control
 * when to timeout the request to the pool.  It also controls the timeout on
 * a dead persister thread.
 *<br><br>
 * DeliverNode also maintains an active set of XQueues as the transmit queues
 * for all destinations.  Behind each XQueue, there is at least one persister
 * thread delivering the messages to the destinations.  The messages may be
 * stuck in the XQueue until they are delivered, as long as the persister is
 * not stopped.  However, if there is no message in the queue, DeliverNode
 * will mark it idle.  All the transmit queues are monitored frequently in
 * every heartbeat.  If one of them has been idle for over MaxIdleTime, its
 * queue will be stopped. Its persister thread and the transmit queue will be
 * removed from the cache.
 *<br><br>
 * You are free to choose any names for the three fixed outlinks.  But
 * DeliverNode always assumes the first outlink for pool, the second for
 * failure and the third for nohit.  The name for nohit outlink is allowed to
 * be same as that of failure outlink.  But neither nohit nor failure is
 * allowed to share their names with the pool.  The rest of the outlinks are
 * for the on-demand destinations.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DeliverNode extends Node {
    private int heartbeat;
    private int maxPersister;
    private int maxRetry = 2;
    private String poolNamePatternStr = null;

    private AssetList reqList = null;  //{oid,rid} for tracking outstanding reqs
    private Map<String, Object> templateMap, substitutionMap;
    private boolean takebackEnabled = false;

    private final static int POOL_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private int NOHIT_OUT = 2;
    private int BOUNDARY = NOHIT_OUT + 1;

    public DeliverNode(Map props) {
        super(props);
        Object o;
        List list;
        long tm;
        int i, j, k, n, id, ruleSize = 512;
        String key, saxParser = null;
        Browser browser;
        Map<String, Object> rule;
        long[] outInfo, ruleInfo;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "deliver";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("MaxNumberPersister")) != null)
            maxPersister = Integer.parseInt((String) o);
        else
            maxPersister = 256;
        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);
        if (maxRetry <= 0)
            maxRetry = 2;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        else
            heartbeat = 60000;
        if (heartbeat <= 0)
            heartbeat = 60000;

        if ((o = props.get("TakebackEnabled")) != null &&
            "true".equalsIgnoreCase((String) o))
            takebackEnabled = true;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, maxPersister,
            overlap, name, list);
        NOHIT_OUT = overlap[0];

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));
        if (overlap[0] == POOL_OUT)
            throw(new IllegalArgumentException(name + ": bad overlap outlink "+
                overlap[0]));

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            outInfo = assetList.getMetaData(i);
            if (i == POOL_OUT) // for tracking request
                reqList = new AssetList(name, (int) outInfo[OUT_CAPACITY]);
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

        BOUNDARY = NOHIT_OUT + 1;
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        templateMap = new HashMap<String, Object>();
        substitutionMap = new HashMap<String, Object>();

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
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("PropertyName", displayPropertyName);
            ruleList.add(key, ruleInfo, rule);
            outInfo = assetList.getMetaData(NOHIT_OUT);
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_DMASK] + " " +
                    ruleInfo[RULE_EXTRA] + " " + ruleInfo[RULE_TTL]/1000 +
                    " - " + assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID MODE OPTION MASK EXTRA TTL - OutName" +
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
        if ((o = props.get("MaxRetry")) != null) {
            i = Integer.parseInt((String) o);
            if (i > 0 && i != maxRetry) {
                maxRetry = i;
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
        if(preferredOutName !=null && (i=assetList.getID(preferredOutName))<=0){
            preferredOutName = assetList.getKey(NOHIT_OUT);
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

        if ((o = ph.get("Capacity")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = RESET_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store Capacity into RULE_MODE field
        if ((o = ph.get("Capacity")) != null)
            ruleInfo[RULE_MODE] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_MODE] = capacity;

        // store MaxIdleTime into RULE_TTL field
        if ((o = ph.get("MaxIdleTime")) != null)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_TTL] = 0;

        // store MaxRetry into RULE_EXTRA field
        if ((o = ph.get("MaxRetry")) != null)
            ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_EXTRA] = maxRetry;
        if (ruleInfo[RULE_EXTRA] <= 0)
            ruleInfo[RULE_EXTRA] = maxRetry;

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else {
            ruleInfo[RULE_OID] = POOL_OUT;
            ruleInfo[RULE_PID] = TYPE_DELIVER;

            k = 0;
            if ((o = ph.get("URITemplate")) != null && o instanceof List) {
                k = ((List) o).size();
                if ((o = ph.get("URISubstitution")) != null &&
                    o instanceof List) {
                    i = ((List) o).size();
                    if (k < i)
                        k = i;
                }
            }
            else if ((o = ph.get("URISubstitution")) != null &&
                o instanceof List) {
                k = ((List) o).size();
            }

            Template[] template = new Template[k];
            TextSubstitution[] substitution = new TextSubstitution[k];
            for (i=0; i<k; i++) {
                template[i] = null;
                substitution[i] = null;
            }
            if ((o = ph.get("URITemplate")) != null && o instanceof List) {
                list = (List) o;
                k = list.size();
                for (i=0; i<k; i++) {
                    if ((o = list.get(i)) == null || !(o instanceof String))
                        continue;
                    str = (String) o;
                    if (templateMap.containsKey(str))
                        template[i] = (Template) templateMap.get(str);
                    else {
                        template[i] = new Template(str);
                        templateMap.put(str, template[i]);
                    }
                }
            }

            if ((o = ph.get("URISubstitution")) != null && o instanceof List) {
                list = (List) o;
                k = list.size();
                for (i=0; i<k; i++) {
                    if ((o = list.get(i)) == null || !(o instanceof String))
                        continue;
                    str = (String) o;
                    if (str.length() <= 0)
                        continue;
                    if (substitutionMap.containsKey(str))
                        substitution[i] =
                            (TextSubstitution) substitutionMap.get(str);
                    else {
                        substitution[i] = new TextSubstitution(str);
                        substitutionMap.put(str, substitution[i]);
                    }
                }
                if (template[0] == null) { // for setting initial text
                    str = "##URI##";
                    if (templateMap.containsKey(str))
                        template[0] = (Template) templateMap.get(str);
                    else {
                        template[0] = new Template(str);
                        templateMap.put(str, template[0]);
                    }
                }
            }
            rule.put("URITemplate", template);
            rule.put("URISubstitution", substitution);

            if ((o = ph.get("DefaultProperty")) != null &&
                o instanceof Map) { // default properties
                Template temp = new Template(JSON2Map.toJSON((Map) o));
                if (temp.numberOfFields() <= 0) // not a template
                    rule.put("DefaultProperty", Utils.cloneProperties((Map) o));
                else
                    rule.put("DefaultProperty", temp);
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
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, uriStr, ruleName = null;
        Object o;
        Object[] asset;
        XQueue pool;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        Template[] tmp = null;
        TextSubstitution[] sub = null;
        long currentTime, previousTime, wt;
        long[] outInfo, poolInfo, ruleInfo = null;
        int[] ruleMap;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, min = 0, n, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        boolean xa, ckBody = false;
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

        xa = ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        // update assetList
        n = out.length;
        if (n >= BOUNDARY) for (i=0; i<BOUNDARY; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }
        else
           throw new IllegalArgumentException(name +
               ": outLinks not enough " + n + "/" + BOUNDARY);

        pool = out[POOL_OUT];
        poolNamePatternStr = pool.getName() + "_\\d+";
        i = pool.getCapacity();
        poolInfo = assetList.getMetaData(POOL_OUT);
        if (i != (int) poolInfo[OUT_CAPACITY]) {
            poolInfo[OUT_CAPACITY] = i;
            reqList = new AssetList(name, i);
        }

        n = ruleMap.length;
        currentTime = System.currentTimeMillis();
        previousTime = currentTime;
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
            if (poolInfo[OUT_SIZE] > 0)
                collect(in, pool, poolInfo);
            currentTime = System.currentTimeMillis();
            if (currentTime - previousTime >= heartbeat) {
                monitor(currentTime, in, assetList);
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
                if (ruleInfo[RULE_PID] == TYPE_DELIVER) {
                    tmp = (Template[]) rule.get("URITemplate");
                    sub = (TextSubstitution[]) rule.get("URISubstitution");
                }
                previousRid = rid;
            }

            oid = (int) ruleInfo[RULE_OID];
            uriStr = null;
            if (i < 0) { // failed to apply filters
                oid = FAILURE_OUT;
                filter = null;
            }
            else if (oid == POOL_OUT &&
                TYPE_DELIVER == (int) ruleInfo[RULE_PID]) { // dynamic
                uriStr = MessageUtils.format(name + ": " + ruleName +
                    " URI at ", tmp, sub, buffer, inMessage);

                if (uriStr == null || uriStr.length() <= 0)
                    oid = FAILURE_OUT;
                else if ((oid = assetList.getID(uriStr)) >= 0) {
                    uriStr = null;
                }
                else if ((oid = assetList.add(uriStr, new long[OUT_QTIME + 1],
                    new Object[ASSET_THR + 1])) >= 0) { // new asset
                    outInfo = assetList.getMetaData(oid);
                    for (i=0; i<=OUT_QTIME; i++)
                        outInfo[i] = 0;
                    outInfo[OUT_TIME] = currentTime;
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_READY;
                    outInfo[OUT_CAPACITY] = ruleInfo[RULE_MODE];
                    // set the default MaxIdleTime
                    outInfo[OUT_ORULE] = ruleInfo[RULE_TTL];
                    // set MaxRetry
                    outInfo[OUT_EXTRA] = ruleInfo[RULE_EXTRA];
                    XQueue xq = new IndexedXQueue(pool.getName() + "_" + oid,
                        (int) outInfo[OUT_CAPACITY]);
                    if (xa) { // set XA bit if in has xa
                        i = xq.getGlobalMask() | XQueue.EXTERNAL_XA;
                        xq.setGlobalMask(i);
                    }
                    asset = (Object[]) assetList.get(oid);
                    asset[ASSET_XQ] = xq;
                    asset[ASSET_URI] = xq.getName();
                    asset[ASSET_THR] = null;
                    i = -1;
                    for (int k=0; k<100; k++) {
                        // reserve any empty cell
                        i = pool.reserve(waitTime);
                        if (i >= 0 ||
                            (in.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                            break;
                        else
                            collect(in, pool, poolInfo);
                    }
                    if (i >= 0) { // reserved a cell for the request to pool
                        ObjectMessage msg = null;
                        HashMap<String, Object> bag =
                            new HashMap<String, Object>();
                        if (reqList.existsID(i)) { // collect it first
                            int k;
                            long[] meta = reqList.getMetaData(i);
                            StringBuffer sb = null;
                            if ((debug & DEBUG_COLL) > 0)
                                sb = new StringBuffer();
                            poolInfo[OUT_SIZE] --;
                            poolInfo[OUT_COUNT] ++;
                            poolInfo[OUT_TIME] = currentTime;
                            k = collect(currentTime, (int)meta[0], (int)meta[1],
                                in, (ObjectMessage) reqList.remove(i), sb);
                            if ((debug & DEBUG_COLL) > 0 && sb.length() > 0)
                                new Event(Event.DEBUG, name +
                                    " propagate: Name OID RID Status TTL" +
                                    " URI - " +k+ " persister collected from "+
                                    pool.getName() + " with " +
                                    poolInfo[OUT_SIZE]+" "+pool.depth()+":"+
                                    pool.size() + sb.toString()).send();
                        }
                        bag.put("XQueue", xq);
                        if ((o = rule.get("DefaultProperty")) == null)
                            ; // doing nothing
                        else if (o instanceof Map) // default properties
                            bag.put("Properties", (Map) o);
                        else if (o instanceof Template) try { // template
                            String str;
                            Template temp = (Template) o;
                            str = MessageUtils.format(inMessage, buffer, temp);
                            StringReader sin = new StringReader(str);
                            bag.put("Properties", (Map) JSON2Map.parse(sin));
                            sin.close();
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, name +": " + ruleName +
                                " failed to rebuild properties for " + uriStr +
                                ": " + Event.traceStack(e)).send();
                        }
                        if ((debug & DEBUG_INIT) > 0 &&
                            (o = bag.get("Properties")) != null)
                            new Event(Event.DEBUG, name + ": " + ruleName +
                                " created an ObjectMessage for " + uriStr +
                                ": " + JSON2Map.toJSON((Map) o)).send();
                        try {
                            msg = new ObjectEvent();
                            msg.setStringProperty("URI", uriStr);
                            msg.setObject(bag);
                        }
                        catch (Exception e) {
                            msg = null;
                            pool.cancel(i);
                            assetList.remove(oid);
                            oid = FAILURE_OUT;
                            xq.clear();
                            bag.clear();
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to create an ObjectMessage for " +
                                uriStr + ": " + e.toString()).send();
                        }
                        if (msg != null) {
                            int k = reqList.add(String.valueOf(i),
                                new long[]{rid, oid}, msg, i);
                            if (k >= 0) {
                                pool.add(msg, i);
                                poolInfo[OUT_SIZE] ++;
                                poolInfo[OUT_TIME] = currentTime;
                                poolInfo[OUT_QTIME] = currentTime;
                            }
                            else {
                                pool.cancel(i);
                                assetList.remove(oid);
                                oid = FAILURE_OUT;
                                xq.clear();
                                bag.clear();
                                new Event(Event.ERR, name + ": " + ruleName +
                                    " failed to add request to list for " +
                                    uriStr + ": " + reqList.getKey(i)).send();
                            }
                        }
                    }
                    else { // failure
                        assetList.remove(oid);
                        oid = FAILURE_OUT;
                        xq.clear();
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to send out request of " + uriStr).send();
                    }
                }
                else { // failure
                    oid = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to add asset for request of " + uriStr).send();
                }
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + "/" + assetList.size() +
                    ((uriStr != null) ? ": " + uriStr : "")).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleName + " delivered msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            if (oid > POOL_OUT && filter != null && filter.hasFormatter()) try {
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

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            if (poolInfo[OUT_SIZE] > 0)
                collect(in, pool, poolInfo);
            inMessage = null;
        }
    }

    /**
     * It marks the cell in cells with key and id.
     */
    public void callback(String key, String id) {
        int mid;
        if (key == null || id == null || key.length() <= 0 || id.length() <= 0)
            return;
        int oid = assetList.getID(key);
        if (oid < 0 && key.matches(poolNamePatternStr)) { // dynamic outlink
            if ((oid = key.lastIndexOf("_")) > 0)
                mid = msgList.getID(key.substring(oid+1) + "/" + id);
            else
                mid = -1;
        }
        else
            mid = msgList.getID(oid + "/" + id);
        if (mid >= 0) { // mark the mid is ready to be collected
            cells.take(mid);
            if ((debug & DEBUG_FBAK) > 0)
                new Event(Event.DEBUG, name +": "+ key + " called back on "+
                    mid + " with " + oid + "/" + id).send();
        }
        else
            new Event(Event.ERR, name +": "+ key+" failed to callback on "+
                oid + ":" + id).send();
    }

    /**
     * collects all persister threads for new XQueues and returns number of
     * collected persisters
     */
    private int collect(XQueue in, XQueue out, long[] poolInfo) {
        StringBuffer strBuf = null;
        long[] meta;
        long tm;
        int k, cid, rid, oid;

        if ((debug & DEBUG_COLL) > 0)
            strBuf = new StringBuffer();

        k = 0;
        tm = System.currentTimeMillis();
        while ((cid = out.collect(-1L)) >= 0) {
            meta = reqList.getMetaData(cid);
            if (meta == null || meta.length < 2)
                continue;
            poolInfo[OUT_SIZE] --;
            poolInfo[OUT_COUNT] ++;
            poolInfo[OUT_TIME] = tm;
            rid = (int) meta[0];
            oid = (int) meta[1];
            k += collect(tm, rid, oid, in, (ObjectMessage) reqList.remove(cid),
                strBuf);
        }
        if (k > 0 && (debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " collect: Name OID RID Status " +
                "TTL URI - " + k + " persisters collected from " +
                out.getName() + " with " + poolInfo[OUT_SIZE] + " " +
                out.depth() + ":" + out.size() + strBuf.toString()).send();

        return k;
    }

    /**
     * collects the persister thread from the msg and returns 1 upon success
     * or 0 otherwise
     */
    private int collect(long tm, int rid, int oid, XQueue in, ObjectMessage msg,
        StringBuffer strBuf) {
        Map bag;
        Object[] asset;
        XQueue xq;
        String uriStr;
        long[] outInfo;
        int k = 0;
        if (oid < 0 || msg == null) // bad arguments
            return 0;
        asset = (Object[]) assetList.get(oid);
        if (asset == null || asset.length <= ASSET_THR) // asset already trashed
            return 0;
        uriStr = assetList.getKey(oid);
        xq = (XQueue) asset[ASSET_XQ];
        outInfo = assetList.getMetaData(oid);
        try {
            bag = (Map) msg.getObject();
            msg.clearBody();
        }
        catch (Exception e) {
            bag = null;
            uriStr += ": " + Event.traceStack(e);
        }
        if (bag == null || !bag.containsKey("Thread")) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to collect persister " + oid + " for "+ uriStr).send();
            if (bag != null)
                bag.clear();
            if ((k = xq.size()) > 0)
                move(tm, in, xq, oid, FAILURE_OUT);
            assetList.remove(oid);
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " removed persister " + oid + " due to the stopped XQ " +
                xq.getName() + " and gone with " + k + " msgs").send();
            xq.clear();
            asset[ASSET_XQ] = null;
            asset[ASSET_URI] = null;
            asset[ASSET_THR] = null;
            return 0;
        }
        if (bag.containsKey("MaxIdleTime")) // overwrite the default
            outInfo[OUT_ORULE] =
                1000 * Integer.parseInt((String) bag.get("MaxIdleTime"));
        asset[ASSET_THR] = bag.get("Thread");
        MessageUtils.resumeRunning(xq);
        outInfo[OUT_STATUS] = NODE_RUNNING;
        outInfo[OUT_QTIME] = tm;
        bag.clear();
        if ((debug & DEBUG_COLL) > 0 && strBuf != null)
            strBuf.append("\n\t" + xq.getName() + ": " + oid + " " +
                rid + " " + outInfo[OUT_STATUS] + " " +
                (outInfo[OUT_ORULE]/1000) + " " + uriStr);
        return 1;
    }

    /**
     * moves the messages from an XQueue to another and stops the source xq
     */
    private int move(long currentTime, XQueue in, XQueue out, int from, int to){
        Message msg;
        int[] list;
        long[] state, outInfo, ruleInfo;
        int i, id, n, cid, rid, count = 0;
        outInfo = assetList.getMetaData(from);
        if (outInfo == null)
            return -1;
        outInfo[OUT_SIZE] = 0;
        outInfo[OUT_DEQ] = 0;
        outInfo[OUT_COUNT] = 0;
        outInfo[OUT_STATUS] = NODE_STOPPED;
        MessageUtils.pause(out);
        n = msgList.size();
        list = new int[n];
        if (n > 0)
            n = msgList.queryIDs(list);
        for (i=0; i<n; i++) { // scan all outstanding msg
            cid = list[i];
            state = msgList.getMetaData(cid);
            if ((int) state[MSG_OID] != from)
                continue;
            rid = (int) state[MSG_RID];
            msg = (Message) out.browse((int) state[MSG_BID]);
            msgList.remove(cid);
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = currentTime;
            count += passthru(currentTime, msg, in, rid, to, cid, 0);
        }
        MessageUtils.stopRunning(out);

        return count;
    }

    /**
     * monitors all output destinations by checking their transmit queue depth
     * and the status of the persister threads, and updates their state info.
     * If any queue is stuck for too long or its thread is not RUNNING,
     * it will take actions to migrate all the stuck messages to the failure
     * outLink and remove the destination.  It returns the total number of
     * removed persisters.
     */
    private int monitor(long currentTime, XQueue in, AssetList assetList) {
        String uriStr;
        XQueue xq;
        Thread thr;
        Object[] asset;
        StringBuffer strBuf = null;
        long[] outInfo;
        long st;
        int i, k, l, n, mask, retry, oid, maxIdleTime;
        int NOT_STOPPED = XQueue.KEEP_RUNNING|XQueue.STANDBY|XQueue.PAUSE;
        boolean isDebug = ((debug & DEBUG_REPT) > 0);

        n = assetList.size();
        if (n <= BOUNDARY)
            return 0;

        int[] list = new int[n];
        n = assetList.queryIDs(list);
        if (isDebug)
            strBuf = new StringBuffer();

        k = 0;
        for (i=0; i<n; i++) {
            oid = list[i];
            if (oid < BOUNDARY) // leave fixed outLinks alone
                continue;
            asset = (Object[]) assetList.get(oid);
            if (asset == null || asset.length <= ASSET_THR)
                continue;
            uriStr = assetList.getKey(oid);
            xq = (XQueue) asset[ASSET_XQ];
            thr = (Thread) asset[ASSET_THR];
            outInfo = assetList.getMetaData(oid);
            if (isDebug) {
                strBuf.append("\n\t" + xq.getName() + ": " + oid + " " +
                    outInfo[OUT_STATUS] + " " + outInfo[OUT_SIZE] + " " +
                    outInfo[OUT_DEQ] + " " + outInfo[OUT_QSTATUS] + " " +
                    xq.depth() + ":" + xq.size() + "/" + msgList.size() + " "+
                    Event.dateFormat(new Date(outInfo[OUT_QTIME])));
            }
            retry = (int) outInfo[OUT_EXTRA];
            if (thr == null) { // pstr is not instantiated yet
                if ((int) outInfo[OUT_NRULE] < retry) {
                    outInfo[OUT_NRULE] ++;
                    continue;
                }
                else {
                    new Event(Event.WARNING, name + ": persister " + oid +
                        " is not instantiated yet on " + xq.getName() +
                        " with " + outInfo[OUT_SIZE] + " " + xq.depth() + ":" +
                        xq.size() + " " + msgList.size()).send();
                    if (xq.size() > 0)
                        move(currentTime, in, xq, oid, FAILURE_OUT);
                    else
                        MessageUtils.stopRunning(xq);
                }
            }
            else if (!thr.isAlive()) { // thread is not running
                new Event(Event.WARNING, name + ": thread of persister " +
                    oid + " is stopped on " + uriStr + " with " +
                    outInfo[OUT_SIZE] + " " + xq.depth() + ":" +
                    xq.size() + " " + msgList.size()).send();
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_STOPPED;
                if (xq.size() > 0)
                    move(currentTime, in, xq, oid, FAILURE_OUT);
                else
                    MessageUtils.stopRunning(xq);
            }
            outInfo[OUT_NRULE] = 0;

            mask = xq.getGlobalMask();
            if ((mask & NOT_STOPPED) == 0) {
                new Event(Event.NOTICE, name + ": persister " + oid +
                    " is removed due to the stopped XQ " + xq.getName() +
                    " and gone with " + xq.size() + " msgs").send();
                if (xq.size() > 0)
                    move(currentTime, in, xq, oid, FAILURE_OUT);
                xq.clear();
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_CLOSED;
                outInfo[OUT_DEQ] = 0;
                assetList.remove(oid);
                asset[ASSET_XQ] = null;
                asset[ASSET_URI] = null;
                asset[ASSET_THR] = null;
                k ++;
                continue;
            }
            else if ((mask & XQueue.STANDBY) > 0) {
                if ((int) outInfo[OUT_STATUS] != NODE_STANDBY) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_STANDBY;
                }
                outInfo[OUT_DEQ] = 0;
                continue;
            }
            else if ((mask & XQueue.PAUSE) > 0) {
                if ((int) outInfo[OUT_STATUS] != NODE_PAUSE) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_PAUSE;
                }
                outInfo[OUT_DEQ] = 0;
                continue;
            }

            maxIdleTime = (int) outInfo[OUT_ORULE];
            switch ((int) outInfo[OUT_STATUS]) {
              case NODE_DISABLED:
                if (outInfo[OUT_SIZE] > 0L || outInfo[OUT_DEQ] > 0L) { //trigger
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                    outInfo[OUT_QTIME] = currentTime;
                }
                else { // expired
                    new Event(Event.NOTICE, name + ": persister " + oid +
                        " is removed due to its expiration and gone with " +
                        xq.size() + " msgs").send();
                    if (xq.size() > 0)
                        move(currentTime, in, xq, oid, FAILURE_OUT);
                    else
                        MessageUtils.stopRunning(xq);
                    xq.clear();
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_CLOSED;
                    outInfo[OUT_DEQ] = 0;
                    assetList.remove(oid);
                    asset[ASSET_XQ] = null;
                    asset[ASSET_URI] = null;
                    asset[ASSET_THR] = null;
                    k ++;
                }
                break;
              case NODE_STOPPED:
                outInfo[OUT_STATUS] = NODE_RUNNING;
                outInfo[OUT_QTIME] = currentTime;
                break;
              case NODE_RUNNING:
                if (outInfo[OUT_DEQ] > 0L) { // flow is OK
                    outInfo[OUT_QSTATUS] = 0;
                    break;
                }

                // stuck or idle
                st = currentTime - outInfo[OUT_TIME];
                if (outInfo[OUT_SIZE] > 0L) { // stuck
                    if (outInfo[OUT_QSTATUS] > 0) { // stuck for a while
                        outInfo[OUT_QTIME] = currentTime;
                        outInfo[OUT_STATUS] = NODE_RETRYING;
                        outInfo[OUT_QSTATUS] = 0;
                        new Event(Event.WARNING, name + ": persister " + oid +
                            " is stuck with " + outInfo[OUT_SIZE] + " " +
                            xq.depth() + ":" + xq.size()).send();
                    }
                    else
                        outInfo[OUT_QSTATUS] = 1;
                }
                else if (maxIdleTime > 0 && st >= maxIdleTime) {//idled too long
                    outInfo[OUT_QSTATUS] = 0;
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_DISABLED;
                    if ((debug & DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name + ": persister " + oid +
                            " is disabled with " + outInfo[OUT_SIZE] + " " +
                            xq.depth() + ":" + xq.size()).send();
                }
                else
                    outInfo[OUT_QSTATUS] = 0;
                break;
              case NODE_RETRYING:
                st = currentTime - outInfo[OUT_QTIME];
                if (outInfo[OUT_DEQ] > 0L ||
                    (outInfo[OUT_SIZE] == 0L && outInfo[OUT_DEQ] == 0L)) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                    new Event(Event.INFO, name + ": persister " + oid +
                        " is moving again with " + outInfo[OUT_SIZE] + " " +
                        xq.depth() + ":" + xq.size()).send();
                }
                else if (st > retry * heartbeat) { // stuck for too long
                    new Event(Event.NOTICE, name + ": persister " + oid +
                        " is removed due to its stuck and gone with " +
                        xq.size() + " msgs").send();
                    if (xq.size() > 0)
                        move(currentTime, in, xq, oid, FAILURE_OUT);
                    else
                        MessageUtils.stopRunning(xq);
                    xq.clear();
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_CLOSED;
                    outInfo[OUT_DEQ] = 0;
                    assetList.remove(oid);
                    asset[ASSET_XQ] = null;
                    asset[ASSET_URI] = null;
                    asset[ASSET_THR] = null;
                    k ++;
                }
                break;
              case NODE_PAUSE:
              case NODE_STANDBY:
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_RUNNING;
                break;
              case NODE_READY:
              default:
                break;
            }
            if (isDebug && outInfo[OUT_STATUS] != NODE_RUNNING) {
                strBuf.append("\t" + outInfo[OUT_STATUS] + " " +
                    outInfo[OUT_SIZE] + " " + outInfo[OUT_DEQ] + " " +
                    outInfo[OUT_QSTATUS] + " " + xq.getGlobalMask());
            }
            // reset number of deq in OUT_DEQ
            outInfo[OUT_DEQ] = 0L;
        }
        if (isDebug && strBuf.length() > 0) {
            new Event(Event.DEBUG, name + " monitor status on Out: " +
                "OID Status Size Deq QStatus Depth:QSize/MSize QTime - In: " +
                in.getGlobalMask() + " " + in.size() + "," + in.depth() +
                strBuf.toString()).send();
        }

        return k;
    }

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo;
        int id = -1, k, mid, mask, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            k = in.putback(cid);
            new Event(Event.ERR, name+": asset is null on " + oid + "/" +
                rid + " for msg at " + cid).send();
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
            for (k=0; k<1000; k++) { // reserve any empty cell
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

        if (id >= 0 && id < outCapacity) { // id-th cell of out is reserved
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                    currentTime}, key, cid);
            }
            else { // id-th cell is just empty, replace it
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                if (oid >= BOUNDARY) // increase number of deq in OUT_DEQ
                    outInfo[OUT_DEQ] ++;
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
                mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                    currentTime}, key, cid);
            }
            if (mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = in.putback(cid);
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            k = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
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
            k = in.putback(cid);
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                out.getName() + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k).send();
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
            if (oid >= BOUNDARY) // increase number of deq in OUT_DEQ
                outInfo[OUT_DEQ] ++;
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
     * stops all dynamic XQs and cleans up associated assets and the pstrs
     */
    private void stopAll() {
        int i, k, n;
        Object[] asset;
        XQueue xq;
        int[] list;
        n = assetList.size();
        if (n <= BOUNDARY)
            return;
        list = new int[n];
        n = assetList.queryIDs(list);
        for (i=0; i<n; i++) {
            k = list[i];
            if (k < BOUNDARY)
                continue;
            asset = (Object[]) assetList.get(k);
            if (asset == null || asset.length <= ASSET_THR)
                continue;
            xq = (XQueue) asset[ASSET_XQ];
            if (xq != null) {
                MessageUtils.stopRunning(xq);
                xq.clear();
            }
            assetList.remove(k);
            asset[ASSET_XQ] = null;
            asset[ASSET_URI] = null;
            asset[ASSET_THR] = null;
        }
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

        stopAll();
        reqList.clear();
    }

    /** always returns true since it supports dynamic XQs */
    public boolean internalXQSupported() {
        return true;
    }

    protected boolean takebackEnabled() {
        return takebackEnabled;
    }

    /** returns 0 to have the ack propagation skipped on the first outlink */
    public int getOutLinkBoundary() {
        return 0;
    }

    public void close() {
        super.close();
        reqList.clear();
        templateMap.clear();
        substitutionMap.clear();
    }

    protected void finalize() {
        close();
    }
}
