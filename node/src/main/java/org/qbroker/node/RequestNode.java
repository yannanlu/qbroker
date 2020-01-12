package org.qbroker.node;

/* RequestNode.java - a MessageNode supports async JMS requests */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * RequestNode picks up JMS messages as requests or their async responses from
 * the uplink and processes them according to their content and the pre-defined
 * rulesets. There are four fixed outlinks, done, bypass, failure, and nohit,
 * as well as the request outlink for collectiables. For a request message,
 * RequestNode routes it to the request outlink first. Once the message is
 * collected from the outlink, it will be cached with a unique key waiting for
 * the asynchonous reply. When a response message finally arrives, RequestNode
 * extracts the unique key from it and looks for the corresponding request
 * message from the cache. If the request message is found, RequestNode loads
 * the response to the request message and routes it to the outlink of done.
 * The response message will be routed to the outlink of bypass afterwards.
 * This is called that the request message has been claimed by its response.
 * If there is no corresponding request found for the response message, it will
 * be routed to the outlink of nohit. In case that a cached key has expired,
 * its request message will be flushed to the outlink of failure. Eventually,
 * all the claimed keys or expired keys will be removed from the cache. If an
 * incoming message is neither a request nor a response, RequestNode will treat
 * it as a bypass and routes it to one of the four fixed outlinks without any
 * delay. Since RequestNode does not consume any message, any incoming message
 * has to find a way out via one of the four fixed outlinks.
 *<br><br>
 * RequestNode contains a number of pre-defined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the cache or claim
 * options for the messages in each group as well as the other parameters used
 * in the process. Furthermore, RequestNode always creates one extra ruleset,
 * nohit.  The ruleset of nohit is for all the messages not hitting any of
 * the filters.
 *<br><br>
 * You are free to choose any names for the first four fixed outlinks.  But
 * RequestNodee always assumes the first outlink for done, the second for
 * bypass, the third for failure and the fourth for nohit. The rest of the
 * outlinks are for collectibles. It is OK for those first four fixed outlinks
 * to share the same name. Please make sure the first fixed outlink has the
 * actual capacity no less than that of the uplink.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class RequestNode extends Node {
    private int delay = DEFAULT_DELAY;
    private int heartbeat = DEFAULT_DELAY + DEFAULT_DELAY;

    private QuickCache cache = null;   // for request keys
    private int[] outLinkMap;
    private final static int DEFAULT_DELAY = 10000;
    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private int BOUNDARY = NOHIT_OUT + 1;

    public RequestNode(Map props) {
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
            operation = "request";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);

        if (heartbeat <= 0)
            heartbeat = DEFAULT_DELAY + DEFAULT_DELAY;
        else
            delay = heartbeat / 2;

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

        BOUNDARY = outLinkMap[NOHIT_OUT];
        BOUNDARY = (BOUNDARY >= outLinkMap[FAILURE_OUT]) ? BOUNDARY :
            outLinkMap[FAILURE_OUT];
        BOUNDARY = (BOUNDARY >= outLinkMap[BYPASS_OUT]) ? BOUNDARY :
            outLinkMap[BYPASS_OUT];
        BOUNDARY ++;

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        cache = new QuickCache(name, QuickCache.META_DEFAULT, 0, 0);
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
            if ((debug & DEBUG_INIT) > 0)
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_DMASK] + " " + ruleInfo[RULE_EXTRA] + " " +
                    ruleInfo[RULE_TTL] / 1000 + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
        }
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG,name+
                " RuleName: RID PID Option Mask Extra TTL - OutName"+
                strBuf.toString()).send();
    }

    public int updateParameters(Map props) {
        Object o;
        int i, n;

        n = super.updateParameters(props);
        if ((o = props.get("Heartbeat")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != heartbeat) {
                heartbeat = i;
                delay = heartbeat / 2;
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
        if(preferredOutName !=null) {
            if (!assetList.containsKey(preferredOutName)) {
                preferredOutName = assetList.getKey(outLinkMap[NOHIT_OUT]);
                new Event(Event.WARNING, name + ": OutLink for " +
                    ruleName + " not well defined, use the default: "+
                    preferredOutName).send();
            }
            else if (assetList.getID(preferredOutName) >= BOUNDARY) {
                preferredOutName = assetList.getKey(outLinkMap[NOHIT_OUT]);
                new Event(Event.WARNING, name + ": OutLink for " +
                    ruleName + " out of allowed range, use the default: "+
                    preferredOutName).send();
            }
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

        if ((o = ph.get("DisplayMask")) != null && o instanceof String) {
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
            rule.put("DisplayMask", o);
        }
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if ((o = ph.get("TimeToLive")) != null)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        if (preferredOutName != null) { // bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
            rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution",new TextSubstitution((String)o));

            if ((o = ph.get("RuleType")) != null &&
                "reply".equalsIgnoreCase((String) o)) {
                ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
                ruleInfo[RULE_PID] = TYPE_ACTION;
            }
            else {
                 // store ReplyDMask into RULE_EXTRA
                if ((o = ph.get("ReplyDMask")) != null)
                    ruleInfo[RULE_EXTRA] = Long.parseLong((String) o);
                if ((o = ph.get("ReplyQTemplate")) != null) { // for ReplyQ
                    rule.put("ReplyQTemplate", new Template((String) o));
                    if ((o = ph.get("ReplyQSubstitution")) != null &&
                        o instanceof String)
                        rule.put("ReplyQSubstitution",
                            new TextSubstitution((String)o));
                    if ((o = ph.get("ReplyQField")) != null &&
                        o instanceof String)
                        rule.put("ReplyQField", (String) o);
                    else
                        rule.put("ReplyQField", "ReplyQ");
                }
                if (ruleInfo[RULE_TTL] <= 0)
                    ruleInfo[RULE_TTL] = delay;

                ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
                ruleInfo[RULE_PID] = TYPE_CACHE;
            }
        }
        else { // default to nohit
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
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
    /**
     * It expires the cached key first. Then it tries to copy the msg to the
     * pending request and applies the post formatter. In the end, the request
     * message will be flushed to the outlink of oid. It returns 1 if the
     * requested message is fully claimed, or 0 if it is half claimed.
     * Otherwise, it returns a negative number for failures. In case of half
     * claimed, the request message will be formatted but failed to be flushed.
     * Since the key has expired, it will be flushed eventually.
     */
    private int claim(long currentTime, String key, String ruleName, int cid,
        int rid, Message msg, XQueue in, int oid, byte[] buffer) {
        int i, dmask;
        long[] ruleInfo = null;
        Message inMessage;
        MessageFilter filter = null;
        Map rule;
        String msgStr = null;
        String[] propertyName = null;

        // expire it first
        cache.expire(key, currentTime);
        ruleInfo = ruleList.getMetaData(rid);
        inMessage = (Message) in.browse(cid);
        if (inMessage == null) { // bad msg
            new Event(Event.ERR, name + " " + ruleName +
                ": claimed a null msg at " + cid).send();
            return -2;
        }
        else try { // copy the reply msg to the request message
            // user properties
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

            Enumeration propNames = msg.getPropertyNames();
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.equals("null"))
                    continue;
                if (key.startsWith("JMS"))
                    continue;
                inMessage.setObjectProperty(key, msg.getObjectProperty(key));
            }

            // msg body
            if (inMessage instanceof TextMessage) { // for TextMessage
                inMessage.clearBody();
                if (msg instanceof TextMessage)
                    msgStr = ((TextMessage) msg).getText();
                else
                    msgStr = MessageUtils.processBody(msg, buffer);
                ((TextMessage) inMessage).setText(msgStr);
            }
            else if (inMessage instanceof BytesMessage) { // for BytesMessage
                inMessage.clearBody();
                if (msg instanceof TextMessage)
                    msgStr = ((TextMessage) msg).getText();
                else
                    msgStr = MessageUtils.processBody(msg, buffer);
                ((BytesMessage) inMessage).writeBytes(msgStr.getBytes());
            }
            else {
                new Event(Event.ERR, name + " " + ruleName +
                    " failed to copy the reply msg to " +
                    inMessage.getClass().getName()).send();
                return -3;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to copy the reply msg: " + Event.traceStack(e)).send();
            return -4;
        }

        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        filter = (MessageFilter) rule.get("Filter");
        dmask = (int) ruleInfo[RULE_EXTRA];

        if (filter != null && filter.hasFormatter()) try { // post formatter
            filter.format(inMessage, buffer);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to format the request: " + Event.traceStack(e)).send();
            return -5;
        }

        if (dmask > 0) try { // display the message
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " claimed a request:" + MessageUtils.display(inMessage,
                msgStr, dmask, propertyName)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                " failed to display msg: " + e.toString()).send();
        }

        i = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
        if (i > 0)
            ruleInfo[RULE_PEND] --;
        return i;
    }

    /**
     * flushes a message at cid with the key to the given outlink. It returns
     * 1 for success or 0 for retry. Otherwise, it returns a negative number
     * for failures. 
     */
    private int flush(long currentTime, String key, int cid, int rid, int nc,
        XQueue in, int oid) {
        int i, dmask;
        long[] ruleInfo = null;
        Message inMessage;
        Map rule;
        String[] propertyName = null;
        String ruleName = ruleList.getKey(rid);

        inMessage = (Message) in.browse(cid);
        if (inMessage == null) { // bad msg
            new Event(Event.ERR, name + " " + ruleName +
                ": flushed a null msg at " + cid).send();
            return -1;
        }

        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        ruleInfo = ruleList.getMetaData(rid);
        dmask = (nc > 0) ? (int) ruleInfo[RULE_EXTRA] :
            (int) ruleInfo[RULE_DMASK];

        if (dmask > 0) try { // display the message
            new Event(Event.INFO, name + ": " + ruleName +
                " flushed a request:" + MessageUtils.display(inMessage,
                null, dmask, propertyName)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                " failed to display msg: " + e.toString()).send();
        }

        i = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
        if (i > 0)
            ruleInfo[RULE_PEND] --;
        return i;
    }

    /*
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String id, msgStr = null, ruleName = null;
        String msgKey = null, fieldName = null;
        Object o;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        Template template = null, idTemp = null;
        TextSubstitution sub = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, st, previousTime, wt;
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
            if (currentTime - previousTime >= heartbeat) {
                if (cache.size() > 0 &&
                    currentTime >=  cache.getMTime() + delay) { // check cache
                    int k = disfragment(currentTime, in);
                    if (k > 0 && (debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + ": " + k +
                            " keys flushed").send();
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
                if (ruleInfo[RULE_PID] == TYPE_ACTION) {
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                }
                else if (ruleInfo[RULE_PID] == TYPE_CACHE) {
                    template = (Template) rule.get("ReplyQTemplate");
                    sub = (TextSubstitution) rule.get("ReplyQSubstitution");
                    fieldName = (String) rule.get("ReplyQField");
                }
                previousRid = rid;
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_ACTION) { // for reply
                String key;
                i = BYPASS_OUT;
                try {
                    key = MessageUtils.format(inMessage, buffer, template);
                    if (sub != null && key != null)
                        key = sub.substitute(key);
                }
                catch (Exception e) {
                    key = null;
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the key from reply msg: " +
                        Event.traceStack(e)).send();
                }

                if (i == FAILURE_OUT)
                    key = null;
                else if (key == null || key.length() <= 0) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the key from reply msg").send();
                    i = FAILURE_OUT;
                }
                else if (!cache.containsKey(key)) { // request timed out?
                    if (!key.equals(msgKey)) { // rollback for a retry
                        msgKey = key;
                        feedback(in, 100L);
                        in.putback(cid);
                        continue;
                    }
                    else { // request timed out already
                        new Event(Event.ERR, name + ": " + ruleName +
                            " request timed out already for " + key).send();
                        i = NOHIT_OUT;
                    }
                }
                else if (cache.isExpired(key, currentTime)) { // already expired
                    int[] meta = cache.getMetaData(key);
                    if (meta != null && meta.length > 2 && meta[0] >= 0) {
                        i = (meta[2] > 0) ? RESULT_OUT : FAILURE_OUT;
                        i = flush(currentTime, key, meta[0], meta[1], meta[2],
                            in, outLinkMap[i]);
                        if (i > 0) { // flushed
                            meta[0] = -1;
                        }
                        else if (i == 0) { // failed on flush with retry
                            if (meta[2] > 0) // retry for half claimed msg
                                meta[2] ++;
                            else // retry for failure
                                meta[2] --;
                        }
                        else {
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to flush msg for " + key +
                                " with " + i).send();
                        }
                    }
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " request has expired for " + key).send();
                    i = NOHIT_OUT;
                }
                else { // found existing key to claim
                    int[] meta = cache.getMetaData(key);
                    if (meta != null && meta.length > 2 && meta[0] >= 0) {
                        i = claim(currentTime, key, ruleName, meta[0], meta[1],
                            inMessage, in, outLinkMap[RESULT_OUT], buffer);
                        if (i > 0) { // claimed
                            meta[0] = -1;
                            ruleInfo[RULE_PEND] ++;
                            if ((debug & DEBUG_REPT) > 0)
                                new Event(Event.DEBUG, name + ": " + ruleName +
                                    " claimed " + i + " msg").send();
                            i = BYPASS_OUT;
                        }
                        else if (i == 0) { // half claimed with retry
                            meta[2] ++;
                            i = BYPASS_OUT;
                        }
                        else { // failed
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to claim msg for " + key +
                                " with " + i).send();
                            i = FAILURE_OUT;
                        }
                    }
                    else if (meta != null && meta.length > 2) { // not expired?
                        cache.expire(key, currentTime);
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " expired a request already claimed " + key).send();
                        i = NOHIT_OUT;
                    }
                    else {
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to claim a request with null meta for " +
                            key).send();
                        i = FAILURE_OUT;
                    }
                }
                if (i != BYPASS_OUT) // disable post formatter
                    filter = null;
                oid = outLinkMap[i];
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + " propagate: " + key +
                        " = " + cid + ":" + rid +" "+ i).send();
            }
            else if (ruleInfo[RULE_PID] == TYPE_CACHE) { // for requests
                String key;
                i = 0;
                if (template != null) try { // for replyQueue
                    key = MessageUtils.format(inMessage, buffer, template);
                    if (sub != null && key != null)
                        key = sub.substitute(key);
                    MessageUtils.setProperty(fieldName, key, inMessage);
                }
                catch (JMSException e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to set " + fieldName + ": " +
                        Event.traceStack(e)).send();
                    i = FAILURE_OUT;
                }

                if (i != FAILURE_OUT) try { // for correlation ID
                    key = inMessage.getJMSMessageID();
                    if (key == null || key.length() <= 0)
                        key = "TBD_MSGID";
                    inMessage.setJMSCorrelationID(key);
                }
                catch (JMSException e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to set corrID: " + Event.traceStack(e)).send();
                    i = FAILURE_OUT;
                }
                // disable post formatter
                filter = null;
                if (i == FAILURE_OUT)
                    oid = outLinkMap[i];
                else
                    oid = (int) ruleInfo[RULE_OID];
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
                new Event(Event.INFO, name + ": " + ruleName +" requested msg "+
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

            i = passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            if (i > 0)
                count ++;
            else // rolled back for retry
                new Event(Event.WARNING, name + ": " + ruleName +
                    " rolled back msg at " + cid).send();
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * It checks all keys to see if any of them has expired or not. In case
     * that a key has expired but the message has not flushed yet, it will try
     * flush the message. If all messages with expired keys have been flushed, 
     * it will disgragment the cache for clean up. Meanwhile, it looks for
     * the next expiration time and resets the MTime of the cache. The number
     * of disfragmented keys will be returned upon success.
     */
    private int disfragment(long currentTime, XQueue in) {
        int n, k, i;
        int[] info;
        long t, tm = currentTime + heartbeat;
        Set<String> keys = cache.keySet();
        k = 0;
        n = 0;
        for (String key : keys) {
            if (!cache.isExpired(key, currentTime)) {
                t = cache.getTimestamp(key) + cache.getTTL(key);
                if (t < tm)
                    tm = t;
                continue;
            }
            k ++;
            info = cache.getMetaData(key);
            if (info == null || info.length < 3)
                continue;
            if (info[0] >= 0) { // not flushed yet
                i = (info[2] > 0) ? RESULT_OUT : FAILURE_OUT;
                i = flush(currentTime, key, info[0], info[1], info[2],
                    in, outLinkMap[i]);
                if (i > 0) { // flushed
                    info[0] = -1;
                }
                else if (i == 0) { // failed on flush with retry
                    n ++;
                    if (info[2] > 0) // retry for half claimed msg
                        info[2] ++;
                    else // retry for failure
                        info[2] --;
                }
                else {
                    n ++;
                    new Event(Event.ERR, name + ": " +ruleList.getKey(info[1])+ 
                        " failed to flush msg " + n + " for " + key +
                        " with " + i).send();
                }
            }
        }
        if (n == 0 && k > 0) {
            n = cache.size();
            cache.disfragment(currentTime);
            n -= cache.size();
        }
        if (cache.size() > 0)
            cache.setStatus(cache.getStatus(), tm);

        return n;
    }

    /**
     * It collects the message at mid and saves it to the cache with the key
     * formatted out of the message. Upon success, it returns 1. In case that
     * mid is out of range, it returns 0. Otherwise, it returns a negative
     * number to indicate the failure.
     */
    private int collect(long currentTime, int mid, XQueue in, long[] outInfo) {
        long[] state, ruleInfo;
        Map rule;
        Message msg;
        Template template;
        TextSubstitution sub;
        byte[] buffer = new byte[8192];
        String ruleName, key = null;
        int cid, rid;
        if (mid < 0 || mid >= capacity)
            return 0;
        state = msgList.getMetaData(mid);
        msgList.remove(mid);
        outInfo[OUT_SIZE] --;
        outInfo[OUT_COUNT] ++;
        if (outInfo[OUT_STATUS] == NODE_RUNNING)
            outInfo[OUT_TIME] = currentTime;
        if (state == null || (int) state[MSG_OID] < BOUNDARY)
            return -1;
        rid = (int) state[MSG_RID];
        cid = (int) state[MSG_CID];
        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        ruleInfo[RULE_SIZE] --;
        ruleInfo[RULE_PEND] ++;
        ruleInfo[RULE_TIME] = currentTime;

        rule = (Map) ruleList.get(rid);
        msg = (Message) in.browse(cid);
        if (msg == null) { // request is null somehow
            new Event(Event.ERR, name + ": " + ruleName +
                " collected a null request at " + cid + " from " +
                in.getName()).send();
            return -2;
        }

        template = (Template) rule.get("KeyTemplate");
        sub = (TextSubstitution) rule.get("KeySubstitution");
        try {
            key = MessageUtils.format(msg, buffer, template);
            if (sub != null && key != null)
                key = sub.substitute(key);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to format the key: " + Event.traceStack(e)).send();
            return -3;
        }

        if (key != null && key.length() > 0) { // add to cache
            cache.insert(key, currentTime, (int) ruleInfo[RULE_TTL],
                new int[]{cid, rid, 0}, key);
            if (cache.size() == 1) {
                cache.setStatus(cache.getStatus(),
                    currentTime + ruleInfo[RULE_TTL]);
            }
            else {
                long t = cache.getMTime();
                if (t > currentTime + ruleInfo[RULE_TTL]) // reset min mtime
                    cache.setStatus(cache.getStatus(),
                        currentTime + ruleInfo[RULE_TTL]);
            }
            return 1;
        }
        else {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to format the key").send();
            return -3;
        }
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
                mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                    currentTime}, key, cid);
            }
            else if (oid < BOUNDARY) { // an exiting msg
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                if (outInfo[OUT_STATUS] == NODE_RUNNING)
                    outInfo[OUT_TIME] = currentTime;
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
            else { // it is a request msg to be collected
                cells.collect(-1L, mid);
                k = collect(currentTime, mid, in, outInfo);
                if (k > 0) {
                    mid = msgList.add(key, new long[]{cid, oid, id, rid,
                        tid, currentTime}, key, cid);
                }
                else { // failed to collect the msg
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
            outInfo = assetList.getMetaData(oid);
            if (oid >= BOUNDARY) { // for collectible messages
                collect(t, mid, in, outInfo);
                continue;
            }
            // for exit messages
            in.remove(mid);
            msgList.remove(mid);
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
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
        int[] list;
        int mid;
        Set<String> keys = cache.keySet();

        super.resetMetaData(in, out);

        // rollback cached msgs
        for (String key : keys) {
            list = cache.getMetaData(key);
            if (list == null || list.length < 3)
                continue;
            mid = list[1];
            if (mid < 0)
                continue;
            in.putback(mid);
        }
        cache.clear();
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

        Set<String> keys = cache.keySet();
        for (String ky : keys) {
            info = (int[]) cache.getMetaData(ky);
            if (info == null)
                continue;
            rid = info[1];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0)
                continue;

            cid = info[0];
            msg = (Message) xq.browse(cid);
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
        super.close();
        cache.clear();
    }

    protected void finalize() {
        close();
    }
}
