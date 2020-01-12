package org.qbroker.node;

/* ActionNode.java - a generic MessageNode reacting upon JMS messages */

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
import javax.jms.MessageFormatException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * ActionNode processes incoming JMS messages and takes the predefined actions
 * according to the rulesets and the content of the incoming messages.  As the
 * escalation of actions, ActionNode may generate a new JMS message.  All the
 * escalation messages will be routed to the outlink of done.  For all the
 * incoming messages, ActionNode routes them into three outlinks: bypass for
 * all the processed incoming messages, nohit for those messages do not belong
 * to any rulesets, failure for the incoming messages failed in the process.
 * However, the outlink of done can be shared with other outlinks.
 *<br><br>
 * ActionNode contains a number of predefined rulesets.  These rulesets
 * categorize incoming messages into non-overlapping groups.  Therefore, each
 * rule defines a unique message group.  The ruleset also defines its actions
 * and the rules to generate new messages for the group.  Different groups may
 * have different actions and/or different parameters.  For an escalation
 * message, ActionOrder determines its delivery order relative to the incoming
 * message.  ActionOrder takes one of the three values: "none", "first" and
 * "last".  By default, it is set to "none" and there is no order defined on
 * the newly generated messages.  If it is set to "first", ActionNode will
 * send out the new message first and wait for its delivery.  Then it will
 * have the incoming message going through the bypass outlink.  If it is set
 * to "last", the incoming message will go through the bypass outlink first.
 * Upon the completion of its delivery, ActionNode sends the escalation
 * message through the outlink of done. In case of "first" or "last", the
 * ruleset also allows ActionDelay defined in millisec. It specifies how long
 * the escalation gets delayed for the ruleset of "last". For the ruleset of
 * "first", the delay happens after the escalation. EscalationMask controls
 * the display mask on escalation messages.
 *<br><br>
 * Apart from the user defined rulesets, ActionNode always creates one extra
 * ruleset, nohit.  The ruleset of nohit is for those messages not hitting any
 * patterns.  Please remember, there is no one-to-one relationship between the
 * candidate messages and the escalation messages.  The total number of
 * escalation messages will be stored into the RULE_PEND field of the rulesets.
 *<br><br>
 * Each ruleset contains the name of the rule, the classname of the action and
 * a Map of ActionArgument.  The ActionArgument has the configuration
 * parameters for the constructor.  If the classname is not defined in a rule,
 * the default action will be used so that it just converts the msg into a
 * different JMS message type, including the same type for copy.  In case of
 * the same type for the escalation, the rule can have a formatter to format
 * the escalation messages.  On the other hand, developers can plugin their
 * own actions.  The requirement is minimum.  The class should have a public
 * method of escalate() that takes a JMS Message as the only argument and
 * returns a Java Object.  The returned object must be null for bypass or
 * the new JMS message for escalations.  If something is wrong, the returned
 * object should be a String as the text of the error.
 *<br><br>
 * The plugin must also have a constructor taking a Map, or a List, or just a
 * String as the only argument for configurations. Based on the data type of
 * the constructor argument, developers should define configuration parameters
 * in the base of ActionArgument.  ActionNode will pass the data to the plugin's
 * constructor as an opaque object during the instantiation of the plugin.
 * In the normal operation, ActionNode will invoke the method to process the
 * incoming messages.  It is OK for the method to modify the incoming messages.
 * But it is not recommented to acknowledge them.  If any of the operations
 * fails, the incoming message will be routed to failure outlink.
 *<br><br>
 * In case a plugin needs to connect to external resources for dynamic
 * escalations, it should define an extra method of close() to close all
 * the external resources gracefully.  Its method of escalate() should also be
 * able to detect the disconnections and cleanly reconnect to the resources
 * automatically.  If the container wants to stop the node, it will call the
 * resetMetaData() in which the methods of close() of all actions will be
 * called in order to release all external resources.
 *<br><br>
 * You are free to choose any names for the four fixed outlinks.  But
 * ActionNode always assumes the first outlink for done, the second for bypass,
 * the third for failure and the last for nohit.  Any two or more outlinks can
 * share the same outlink name.  It means these outlinks are sharing the same
 * output channel.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ActionNode extends Node {
    private int[] outLinkMap;

    private AssetList pluginList = null; // plugins
    private Map<String, Object> templateMap, substitutionMap;

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int JMS_COPY = 0;
    private final static int JMS_TEXT = 1;
    private final static int JMS_BYTES = 2;
    private final static int JMS_MAP = 3;
    private final static int JMS_OBJECT = 4;
    private final static int JMS_STREAM = 5;

    public ActionNode(Map props) {
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
            operation = "escalate";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

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

        templateMap = new HashMap<String, Object>();
        substitutionMap = new HashMap<String, Object>();

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pluginList = new AssetList(name, ruleSize);
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_GID] + " " + ruleInfo[RULE_EXTRA] + " " +
                    ruleInfo[RULE_DMASK] + " " + ruleInfo[RULE_OPTION] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID MODE GID EXTRA MASK OPTION - OutName" +
                strBuf.toString()).send();
        }
    }

    /**
     * It initializes a new ruleset with the ruleInfo and returns the rule upon
     * success.  Otherwise, it throws an exception or returns null.
     */
    protected Map<String, Object> initRuleset(long tm, Map ph, long[] ruleInfo){
        Object o;
        Map<String, Object> rule, hmap;
        String key, str, ruleName, preferredOutName;
        long[] outInfo;
        int i, n, id;

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

        hmap = new HashMap<String, Object>();
        hmap.put("Name", ruleName);
        if ((o = ph.get("JMSPropertyGroup")) != null)
            hmap.put("JMSPropertyGroup", o);
        if ((o = ph.get("XJMSPropertyGroup")) != null)
            hmap.put("XJMSPropertyGroup", o);
        if ((o = ph.get("PatternGroup")) != null)
            hmap.put("PatternGroup", o);
        if ((o = ph.get("XPatternGroup")) != null)
            hmap.put("XPatternGroup", o);
        if ((o = ph.get("FormatterArgument")) != null &&
            o instanceof List && !ph.containsKey("ClassName")) {
            hmap.put("FormatterArgument", o);
            if ((o = ph.get("ResetOption")) != null)
                hmap.put("ResetOption", o);
            else
                hmap.put("ResetOption", "0");
            hmap.put("TemplateMap", templateMap);
            hmap.put("SubstitutionMap", substitutionMap);
        }
        rule.put("Filter", new MessageFilter(hmap));
        hmap.clear();
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String) {
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
            rule.put("DisplayMask", o);
        }
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store escalation mask into RULE_OPTION
        if ((o = ph.get("EsclationMask")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = ruleInfo[RULE_DMASK];

        // action order stored in RULE_EXTRA
        if ((o = ph.get("ActionOrder")) != null) {
            if ("first".equals(((String) o).toLowerCase()))
                ruleInfo[RULE_EXTRA] = 1;
            else if ("last".equals(((String) o).toLowerCase()))
                ruleInfo[RULE_EXTRA] = -1;
        }

        // action delay in ms stored in RULE_GID
        if ((o = ph.get("ActionDelay")) != null)
            ruleInfo[RULE_GID] = Long.parseLong((String) o);

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // for plug-in
            str = (String) o;
            if ((o = ph.get("ActionArgument")) != null) {
                if (o instanceof List)
                    str += "::" + JSON2Map.toJSON((List) o, null, null);
                else if (o instanceof Map)
                    str += "::" + JSON2Map.toJSON((Map) o, null, null);
                else
                    str += "::" + (String) o;
            }

            if (pluginList.containsKey(str)) {
                long[] meta;
                id = pluginList.getID(str);
                meta = pluginList.getMetaData(id);
                // increase the reference count
                meta[0] ++;
            }
            else {
                o = MessageUtils.getPlugins(ph, "ActionArgument", "escalate",
                    new String[]{"javax.jms.Message"}, "close", name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            // plugin id stored in PID
            ruleInfo[RULE_PID] = id;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
        }
        else { // for default action
            ruleInfo[RULE_PID] = TYPE_ACTION;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            if ((o = ph.get("MessageType")) != null) { //for msg convertion
                str = (String) o;
                if ("jms_bytes".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = JMS_BYTES;
                else if ("jms_text".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = JMS_TEXT;
                else if ("jms_map".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = JMS_MAP;
                else if ("jms_object".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = JMS_OBJECT;
                else
                    ruleInfo[RULE_MODE] = JMS_COPY;
            }
            else
                ruleInfo[RULE_MODE] = JMS_COPY;
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // for String properties
        if ((o = ph.get("StringProperty")) != null && o instanceof Map) {
            int k = ((Map) o).size();
            String[] pn = new String[k];
            k = 0;
            for (Object obj : ((Map) o).keySet()) {
                key = (String) obj;
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
        if (id == 0) // can not remove the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long[] ruleInfo = ruleList.getMetaData(id);
            if (ruleInfo != null && ruleInfo[RULE_SIZE] > 0) // check integrity
                throw(new IllegalStateException(name+": "+key+" is busy with "+
                    ruleInfo[RULE_SIZE] + " outstangding msgs"));
            Map h = (Map) ruleList.remove(id);
            if (h != null) {
                MessageFilter filter = (MessageFilter) h.remove("Filter");
                if (filter != null)
                    filter.clear();
                h.clear();
            }
            if (ruleInfo[RULE_PID] >= 0) { // update reference count of plugin
                long[] meta = pluginList.getMetaData((int) ruleInfo[RULE_PID]);
                if (meta == null)
                    throw(new IllegalStateException(name + ": " + key +
                        " has no plugin found at " + ruleInfo[RULE_PID]));
                meta[0] --;
                if (meta[0] <= 0) { // reference count is 0, check closer
                    Object[] asset =
                        (Object[]) pluginList.remove((int) ruleInfo[RULE_PID]);
                    if (asset != null && asset.length >= 3) { // invoke closer
                        java.lang.reflect.Method closer;
                        closer = (java.lang.reflect.Method) asset[2];
                        if (closer != null && asset[1] != null) try {
                            closer.invoke(asset[1], new Object[] {});
                        }
                        catch (Throwable t) {
                        }
                    }
                }
            }
            return id;
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.removeRule(key, in);
        }
        return -1;
    }

    /**
     * It replaces the existing rule of the key and returns its id upon success.
     * It is not MT-Safe.
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
            Map rule = initRuleset(tm, ph, meta);
            if (rule != null && rule.containsKey("Name")) {
                StringBuffer strBuf = ((debug & DEBUG_DIFF) <= 0) ? null :
                    new StringBuffer();
                Map h = (Map) ruleList.set(id, rule);
                if (h != null) {
                    MessageFilter filter = (MessageFilter) h.remove("Filter");
                    if (filter != null)
                        filter.clear();
                    h.clear();
                }
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
                if (tm >= 0) { // update reference count of plugin for old rule
                    meta = pluginList.getMetaData((int) tm);
                    if (meta == null)
                        throw(new IllegalStateException(name + ": " + key +
                            " has no plugin found at " + tm));
                    meta[0] --;
                    if (meta[0] <= 0) { // reference count is 0, check closer
                        Object[] asset = (Object[]) pluginList.remove((int) tm);
                        if (asset != null && asset.length >= 3) {//invoke closer
                            java.lang.reflect.Method closer;
                            closer = (java.lang.reflect.Method) asset[2];
                            if (closer != null && asset[1] != null) try {
                                closer.invoke(asset[1], new Object[] {});
                            }
                            catch (Throwable t) {
                            }
                        }
                    }
                }
                if ((debug & DEBUG_DIFF) > 0)
                    new Event(Event.DEBUG, name + "/" + key + " ruleInfo:" +
                        strBuf).send();
                return id;
            }
            else
                new Event(Event.ERR, name + " failed to init rule "+key).send();
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.replaceRule(key, ph, in);
        }
        return -1;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage, msg = null;
        String msgStr = null, ruleName = null;
        Object o = null;
        Object[] asset;
        Map rule;
        Browser browser;
        MessageFilter[] filters = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, previousRid;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
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
                else if (sz > 0)
                    feedback(in, -1L);
                continue;
            }

            wt = 5L;
            if ((inMessage = (Message) in.browse(cid)) == null) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": " + Event.traceStack(
                   new JMSException("null msg from " + in.getName()))).send();
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
                new Event(Event.ERR, str + " failed to apply the filter "+ i+
                    ": " + Event.traceStack(e)).send();
                i = -2;
            }

            if (rid != previousRid) {
                ruleName = ruleList.getKey(rid);
                ruleInfo = ruleList.getMetaData(rid);
                rule = (Map) ruleList.get(rid);
                propertyName = (String[]) rule.get("PropertyName");
            }

            if (i < 0) { // failed on filter
                oid = outLinkMap[FAILURE_OUT];
            }
            else if (TYPE_BYPASS == (int) ruleInfo[RULE_PID]) { // bypass
                i = -1;
                oid = (int) ruleInfo[RULE_OID];
            }
            else { // for actions
                // retrieve action ID from PID 
                int k = (int) ruleInfo[RULE_PID];
                o = null;
                i = BYPASS_OUT;
                try {
                    if (k >= 0) { // for plug-ins
                        java.lang.reflect.Method method;
                        Object obj;
                        asset = (Object[]) pluginList.get(k);
                        obj = asset[1];
                        method = (java.lang.reflect.Method) asset[0];
                        if (method != null)
                            o = method.invoke(obj, new Object[] {inMessage});
                    }
                    else switch ((int) ruleInfo[RULE_MODE]) { // for convertion
                      case JMS_BYTES:
                        o = MessageUtils.convert(inMessage, 0, buffer);
                        break;
                      case JMS_TEXT:
                        o = MessageUtils.convert(inMessage, 1, buffer);
                        break;
                      case JMS_MAP:
                        o = MessageUtils.convert(inMessage, 2, buffer);
                        break;
                      case JMS_OBJECT:
                        o = MessageUtils.convert(inMessage, 3, buffer);
                        break;
                      default:
                        o = MessageUtils.duplicate(inMessage, buffer);
                    }
                }
                catch (Exception e) {
                    String str = name + ": " + ruleName;
                    Exception ex = null;
                    if (e instanceof JMSException)
                        ex = ((JMSException) e).getLinkedException();
                    if (ex != null)
                        str += " Linked exception: " + ex.toString() + "\n";
                    o = null;
                    i = FAILURE_OUT;
                    new Event(Event.ERR, str + " failed to escalate msg: "+
                        Event.traceStack(e)).send();
                }
                catch (Error e) {
                    String str = name + ": " + ruleName;
                    new Event(Event.ERR, str + " failed to escalate msg: "+
                        e.toString()).send();
                    if ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        in.remove(cid);
                        Event.flush(e);
                    }
                    in.remove(cid);
                    Event.flush(e);
                }

                if (o == null) ; // empty acion or failure
                else if (o instanceof Message) { // action msg from plugin
                    msg = (Message) o;
                    if (ruleInfo[RULE_EXTRA] > 0) { // action first
                        oid = RESULT_OUT;
                        // use cid to track the flushed msg externally
                        k = flush(currentTime, cid, rid, msg, in, out[oid],
                            oid, buffer);
                        if (k <= 0)
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to flush escalation msg to " +
                                oid + " so it got dropped").send();
                        else { // flush is done
                            long[] state = msgList.getMetaData(cid);
                            k = (int) state[MSG_BID];
                            // clean up msgList right away
                            msgList.remove(cid);
                            do { // wait for its delivery to complete
                                if (out[oid].collect(10*waitTime, k) >= 0)
                                    break;
                                mask = in.getGlobalMask();
                            } while((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.STANDBY) == 0);
                            if (ruleInfo[RULE_GID] > 0 && // for delay
                                (mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.STANDBY) == 0) try {
                                Thread.sleep(ruleInfo[RULE_GID]);
                            }
                            catch (Exception e) {
                            }
                        }
                    }
                    i = BYPASS_OUT;
                }
                else { // error msg from pligin
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to escalate msg: " + o.toString()).send();
                }
                oid = outLinkMap[i];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " i=" + i).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName + " checked msg "+
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception ex) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: "+Event.traceStack(ex)).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            if (ruleInfo[RULE_EXTRA] == 0 && msg != null) {
                oid = RESULT_OUT; 
                i = flush(currentTime, -1, rid, msg, in, out[oid], oid, buffer);
                if (i <= 0)
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to flush escalation msg at " + cid).send();
            }
            else if (ruleInfo[RULE_EXTRA] < 0 && msg != null) {
                if (msgList.existsID(cid)) { // cache the action msg
                    o = msgList.set(cid, msg);
                }
            }
            msg = null;
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * It displays and sets properties on the escalation message according to
     * its ruleset and puts it to the specified outlink. It returns the number
     * of messages flushed. If cid >= 0, it will update msgList to track its
     * delivery. There is no rollback to cid in any case.
     */
    private int flush(long currentTime, int cid, int rid, Message msg,
        XQueue in, XQueue out, int oid, byte[] buffer) {
        int i, dmask;
        long[] ruleInfo;
        String ruleName;
        Map rule;
        MessageFilter filter = null;
        String[] propertyName;

        if (msg == null) {
            new Event(Event.WARNING, name + " null msg in flush: cid=" + cid +
                " rid=" + rid + " oid=" + oid).send();
            return 0;
        }
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " flush: cid=" + cid +
                " rid=" + rid + " oid=" + oid).send();

        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        rule = (Map) ruleList.get(rid);
        filter = (MessageFilter) rule.get("Filter");
        propertyName = (String[]) rule.get("PropertyName");
        // retrieve displayMask from RULE_OPTION for escalations
        dmask = (int) ruleInfo[RULE_OPTION];

        if (dmask > 0) try { // display the message
            String msgStr = null;
            if ((dmask & (MessageUtils.SHOW_BODY | MessageUtils.SHOW_SIZE)) > 0)
                msgStr = MessageUtils.processBody(msg, buffer);
            new Event(Event.INFO, name + ": " + ruleName +" escalated a msg: " +
                MessageUtils.display(msg, msgStr, dmask, propertyName)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " + ruleName +
                " failed to display msg: " + e.toString()).send();
        }

        if (filter != null && filter.hasFormatter()) try { // post format
            filter.format(msg, buffer);
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to format the msg: " + Event.traceStack(e)).send();
        }

        // flush the escalation msg
        i = passthru(currentTime, msg, in, rid, oid, cid, -1);
        if (i > 0) { // update escalation count on the rule
            ruleInfo[RULE_PEND] ++;
            ruleInfo[RULE_TIME] = currentTime;
        }

        return i;
    }

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise. In case of cid less than 0, the
     * message has nothing to do with the input XQueue. Nothing will be added
     * to msgList for tracking. No callbacks and no update on rule stats either.
     * If tid is less than 0, message will not be put back to uplink in case of
     * failure. There is no callbacks and no update on rule stats either. In
     * case of that cid is larger than or equal to 0 and tid is less than 0, it
     * will add msg stats to msgList for external tracking. But the end user
     * has to clean it up after the usage since it is not collectible by
     * feedback() without the callback.
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
                rid + " with msg at " + cid + "/" + tid).send();
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
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[]{cid, oid, id, rid,
                        tid, currentTime}, key, cid);
                else // generated msg
                    mid = -1;
            }
            else { // id-th cell has just been empty now, replace it
                Object o;
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                o = msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                k = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(k);
                if (ruleInfo[RULE_EXTRA] < 0 && o instanceof Message) {
                    mask = in.getGlobalMask();
                    if(ruleInfo[RULE_GID] > 0 && (mask & XQueue.STANDBY) == 0 &&
                        (mask & XQueue.KEEP_RUNNING) > 0) try { // for delay
                        Thread.sleep(ruleInfo[RULE_GID]);
                    }
                    catch (Exception e) {
                    }
                    asset = (Object[]) assetList.get(RESULT_OUT);
                    int i = flush(currentTime, mid, k, (Message) o, in,
                        (XQueue) asset[ASSET_XQ], RESULT_OUT, new byte[4096]);
                    if (i <= 0)
                        new Event(Event.ERR, name + ": " + ruleList.getKey(k) +
                            " failed to flush escalation msg at " + mid).send();
                }
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
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size() +","+k).send();
                return 0;
            }
            if (cid >= 0 && tid >= 0) { // incoming msg
                k = out.add(msg, id, cbw);
                ruleInfo = ruleList.getMetaData(rid);
                ruleInfo[RULE_SIZE] ++;
                ruleInfo[RULE_TIME] = currentTime;
                outInfo[OUT_SIZE] ++;
                if (outInfo[OUT_STATUS] == NODE_RUNNING)
                    outInfo[OUT_TIME] = currentTime;
            }
            else { // generated msg
                k = out.add(msg, id);
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
            return 1;
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
    }

    /**
     * It returns the number of done messages removed from the input XQueue.
     * If milliSec is less than 0, there is no wait and it tries to collect all
     * cells. Otherwise, it just tries to collect the first collectible cell.
     */
    protected int feedback(XQueue in, long milliSec) {
        Object o;
        Object[] asset;
        XQueue out;
        byte[] buffer = new byte[4096];
        int mid, rid, oid, id, l = 0;
        long[] state, outInfo, ruleInfo;
        long t, wt = 0L;
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
            o = msgList.remove(mid);
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
            // check for action msg
            if (ruleInfo[RULE_EXTRA] < 0 && o instanceof Message) {
                if (ruleInfo[RULE_GID] > wt) { // for delay only once
                    int mask = in.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) == 0) try {
                        Thread.sleep(ruleInfo[RULE_GID] - wt);
                    }
                    catch (Exception e) {
                    }
                    // reset wt for its maximum value
                    wt = ruleInfo[RULE_GID];
                }
                asset = (Object[]) assetList.get(RESULT_OUT);
                int i = flush(t, -1, rid, (Message) o, in,
                    (XQueue) asset[ASSET_XQ], RESULT_OUT, buffer);
                if (i <= 0)
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                        " failed to flush escalation msg to " + RESULT_OUT +
                        " so it got dropped").send();
            }
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

    public void close() {
        java.lang.reflect.Method closer;
        Browser browser;
        Object[] asset;
        int id;

        super.close();
        browser = pluginList.browser();
        while ((id = browser.next()) >= 0) {
            asset = (Object[]) pluginList.get(id);
            if (asset == null || asset.length < 3)
                continue;
            closer = (java.lang.reflect.Method) asset[2];
            if (closer != null && asset[1] != null) try {
                closer.invoke(asset[1], new Object[] {});
            }
            catch (Throwable t) {
            }
        }
        pluginList.clear();
        templateMap.clear();
        substitutionMap.clear();
    }

    protected void finalize() {
        close();
    }
}
