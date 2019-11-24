package org.qbroker.node;

/* SwitchNode.java - a MessageNode switching JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import javax.jms.Message;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.HashChain;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.CollectibleCells;
import org.qbroker.monitor.ConfigList;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * SwitchNode switches JMS messages to various destinations according to
 * their content, and preconfigured rulesets.  SwitchNode contains
 * a number of predefined rulesets.  These rulesets categorize messages into
 * non-overlapping groups.  Therefore, each rule defines a unique message
 * group.  The ruleset also specifies the association between the groups and
 * the outlinks.  A ruleset can be also assosiated to a shared report.  So
 * that the routing rules may be changed on the fly.
 *<br><br>
 * SwitchNode always adds an extra ruleset for the nohit messages.  It assumes
 * the last outlink is always for nohit, unless NohitOutLink is defined.
 * A ruleset is supposed to specify the type of the ruleset.  Currently,
 * SwitchNode supports 3 types, preferred, sticky and weighted.  The type of
 * preferred routes the messages to the preferred outlink.  The type of sticky
 * is also a static routing rule which uses the hash value to decide which
 * outlink to route.  The type of weighted is for dynamic routing with plugin
 * support.
 *<br><br>
 * SwitchNode also allows the developers to plug-in their own dynamic routing
 * rules.  In this case, the full ClassName of the router and its
 * RouterArguments must be well defined in the rulesets.  The requirement on
 * the plug-ins is minimum.  The class must have a public method of
 * route(Message msg) that takes JMS Message as the only argument.  The return
 * object must be an Integer for the id of the route.  In any case, it should
 * never acknowledge any messages. It must also have a constructor taking
 * a Map, or a List, or just a String as the only argument for the
 * configurations.  SwitchNode will invoke the public method to get the route
 * id for each incoming messages. You can define PreferredOutLink in the
 * Ruleset as the default outlink for failures.  Otherwise, the default
 * outlink will be set to NOHIT.
 *<br><br>
 * In case a plugin needs to connect to external resources for dynamic
 * routing process, it should define an extra method of close() to close all
 * the external resources gracefully.  Its route method should also be able
 * to detect the disconnections and cleanly reconnect to the resources
 * automatically.  If the container wants to stop the node, it will invoke
 * resetMetaData() in which the method of close() of all dynamic routers will
 * be invoked in order to release all external resources.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SwitchNode extends Node {
    private int heartbeat = 60000;
    private int sessionTimeout = 300000;

    private AssetList pluginList = null; // plugin method and object 
    private HashChain hashChain = null;  // for consistent hash

    private final static int RULE_ON = 0;
    private final static int RULE_OFF = 1;
    private int NOHIT_OUT = 0;

    public SwitchNode(Map props) {
        super(props);
        Object o;
        List list;
        long tm;
        int i, j, n, id, ruleSize = 512;
        String key;
        Map<String, Object> rule;
        Map ph;
        Browser browser;
        long[] outInfo, ruleInfo;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "switch";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        assetList = NodeUtils.initOutLinks(tm, capacity, n, -1, name, list);

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
            NOHIT_OUT = i;
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

        if (assetList.size() <= 0)
            throw(new IllegalArgumentException(name+": OutLink is empty"));

        if ((o = props.get("NohitOutLink")) != null && o instanceof String) {
            if ((id = assetList.getID((String) o)) >= 0)
                NOHIT_OUT = id;
            else
                new Event(Event.WARNING, name + ": NohitOutLink is not well " +
                    "defined, use the default: " +
                    assetList.getKey(NOHIT_OUT)).send();
        }

        cfgList = new AssetList(name, 64);
        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pluginList = new AssetList(name, ruleSize);
        cells = new CollectibleCells(name, capacity);
        n = assetList.getCapacity();
        hashChain = new HashChain(name, n + n);
        n = assetList.size();
        for (i=0; i<n; i++)
            hashChain.add(assetList.getKey(i));

        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();
        n = list.size();

        try { // init rulesets
            String str;
            ConfigList cfg;
            int k, m;
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
                ph = (Map) o;
                if((o = ph.get("RuleType")) == null || !(o instanceof String) ||
                    !"ConfigList".equals((String) o)) {
                    ruleInfo = new long[RULE_TIME+1];
                    rule = initRuleset(tm, ph, ruleInfo);
                    if(rule != null && (key=(String) rule.get("Name")) != null){
                        if(ruleList.add(key, ruleInfo, rule) < 0)//failed to add
                            new Event(Event.ERR, name + ": ruleset " + i + ", "+
                                key + ", failed to be added").send();
                    }
                    else
                        new Event(Event.ERR, name + ": ruleset " + i +
                            " failed to be initialized").send();
                    continue;
                }
                try { // for external rules via ConfigList
                    cfg = new ConfigList(ph);
                }
                catch (Exception ex) {
                    new Event(Event.ERR, name + ": ConfigList " + i +
                        " failed to be initialized").send();
                    continue;
                }
                key = cfg.getName();
                cfg.setDataField(name);
                cfg.loadList();
                k = cfg.getSize();
                m = cfgList.add(key, new long[]{k, 0}, cfg);
                if (m < 0) {
                    new Event(Event.ERR, name + ": ConfigList " + key +
                        " failed to be added to the list").send();
                    cfg.close();
                    continue;
                }
                for (j=0; j<k; j++) { // init all external rulesets
                    str = cfg.getItem(j);
                    ph = cfg.getProps(str);
                    key = cfg.getKey(j);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+" failed to init external rule "+key+
                            " at " + j + ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.size() > 0) {
                        if(ruleList.add(key, ruleInfo, rule) < 0)//failed to add
                            new Event(Event.ERR, name + ": ConfigList " +
                                cfg.getName()+ " failed to add external rule " +
                                key + " at " + j).send();
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+ " failed to init external rule "+
                            key + " at " + j).send();
                }
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + ": failed to init rule " +
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_GID] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID GID - OutName"+
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

        return n;
    }

    /**
     * It initializes a new ruleset with the ruleInfo and returns the rule upon
     * success.  Otherwise, it throws an exception or returns null.
     */
    protected Map<String, Object> initRuleset(long tm, Map ph, long[] ruleInfo){
        Object o;
        Map<String, Object> rule;
        String key, str, ruleName, ruleType, preferredOutName;
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
        rule.put("ReportName",  ph.get("ReportName"));
        ruleType = (String) ph.get("RuleType");
        if (ruleType == null)
            ruleType = "preferred";
        preferredOutName = (String) ph.get("PreferredOutLink");
        if(preferredOutName ==null || !assetList.containsKey(preferredOutName)){
            preferredOutName = assetList.getKey(NOHIT_OUT);
            if (!"weighted".equals(ruleType.toLowerCase()) &&
                !"sticky".equals(ruleType.toLowerCase())) {
                ruleType = "preferred";
                new Event(Event.WARNING, name + ": OutLink for " +
                    ruleName + " not well defined, use the default: " +
                    preferredOutName).send();
            }
        }
        else if (!"weighted".equals(ruleType.toLowerCase()) &&
            !"sticky".equals(ruleType.toLowerCase()))
            ruleType = "preferred";

        if ("sticky".equals(ruleType.toLowerCase())) {
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String)
                rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String)o));
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

        str = null;
        if ("weighted".equals(ruleType.toLowerCase())) {
            if ((o = ph.get("ClassName")) != null)
                str = (String) o;
            else {
                ruleType = "preferred";
                new Event(Event.WARNING, name + ": ClassName for " +
                    ruleName + " not defined, switch to " + ruleType).send();
            }
        }

        if ("weighted".equals(ruleType.toLowerCase())) { // for plugin
            if ((o = ph.get("RouterArgument")) != null) {
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
                o = MessageUtils.getPlugins(ph, "RouterArgument", "route",
                    new String[]{"javax.jms.Message"}, "close", name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            // store the plugin ID in GID for the dynamic ruleset
            ruleInfo[RULE_GID] = id;
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_WEIGHTED;
        }
        else if ("sticky".equals(ruleType.toLowerCase())) { // sticky
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_NONE;
            ruleInfo[RULE_GID] = -1;
        }
        else { // preferred
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            ruleInfo[RULE_GID] = -1;
        }

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
     * picks up a message from input queue and evaluate its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        Object o;
        Object[] asset;
        String msgStr = null, ruleName = null;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, k, n, size, previousRid;
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
        k = out.length;
        for (i=0; i<k; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

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
                if ( sessionTimeout > 0)
                    update(currentTime, in);
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
                previousRid = rid;
            }

            if (TYPE_WEIGHTED == (int) ruleInfo[RULE_PID]) { // plugin
                // retrieve router ID from GID for dynamic routers
                java.lang.reflect.Method method;
                Object obj;
                asset = (Object[]) pluginList.get((int) ruleInfo[RULE_GID]);
                obj = asset[1];
                method = (java.lang.reflect.Method) asset[0];
                if (method == null) {
                    oid = (int) ruleInfo[RULE_OID];
                    new Event(Event.ERR, name + ": " + ruleName +
                        " has no method for routing").send();
                }
                else try {
                    o = method.invoke(obj, new Object[] {inMessage});
                    if (o != null && o instanceof Integer)
                        oid = ((Integer) o).intValue();
                    else
                        oid = -1;
                }
                catch (Exception e) {
                    String str = name + ": " + ruleName;
                    Exception ex = null;
                    if (e instanceof JMSException)
                        ex = ((JMSException) e).getLinkedException();
                    if (ex != null)
                        str += " Linked exception: " + ex.toString() + "\n";
                    new Event(Event.ERR, str + " failed to route the msg: "+
                        Event.traceStack(e)).send();
                    oid = (int) ruleInfo[RULE_OID];
                }
                catch (Error e) {
                    String str = name + ": " + ruleName;
                    new Event(Event.ERR, str + " failed to route the msg: "+
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
                if (oid < 0 || oid >= out.length) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " got bad oid from the plugin: " + oid).send();
                    oid = (int) ruleInfo[RULE_OID];
                }
            }
            else if (ruleInfo[RULE_PID] == TYPE_NONE) { // sticky
                String key;
                TextSubstitution sub = null;
                Template template = (Template) rule.get("KeyTemplate");
                sub = (TextSubstitution) rule.get("KeySubstitution");
                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (key != null)
                    oid = hashChain.map(key);
                else
                    oid = (int) ruleInfo[RULE_OID];
            }
            else { // preferred or nohit
                oid = (int) ruleInfo[RULE_OID];
            }

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " i=" + i).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName + " switched msg "+
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

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * queries the report for each rules and update the ruleset so that
     * its destination can be controlled by the report.
     */
    private int update(long currentTime, XQueue in) {
        int i, j, k, n, l=0;
        StringBuffer strBuf = null;
        String reportName;
        long[] ruleInfo;
        long st;
        Object o;
        Map<String, Object> r;
        boolean inDetail = ((debug & DEBUG_REPT) > 0);
        if (inDetail)
            strBuf = new StringBuffer();
        Browser browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            o = ruleList.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            reportName = (String) ((Map) o).get("ReportName");
            if (reportName == null)
                continue;
            r = NodeUtils.getReport(reportName);
            if (r == null || (o = r.get("TestTime")) == null) {
                new Event(Event.WARNING, name + ": no report found for " +
                    reportName).send();
                continue;
            }
            ruleInfo = ruleList.getMetaData(i);
            l ++;
            try {
                if ((o = r.get("Status")) != null)
                    ruleInfo[RULE_STATUS] = Integer.parseInt((String) o);
                else // set the default
                    ruleInfo[RULE_STATUS] = RULE_ON;
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to extrieve report data"+
                    " from "+ reportName +": "+ Event.traceStack(e)).send();
            }
            if (inDetail)
                strBuf.append("\n\t" + reportName + ": " + i + " " +
                    (String) r.get("Status")+" "+ruleInfo[RULE_STATUS]+
                    " " + ruleInfo[RULE_SIZE]+" "+ruleInfo[RULE_COUNT]);
        }
        if (inDetail && l > 0)
            new Event(Event.DEBUG,name+ " ReportName: RID Status RStatus Size "+
                "Count"+ strBuf.toString()).send();

        return l;
    }

    public void close() {
        Browser browser;
        Object[] asset;
        java.lang.reflect.Method closer;
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
        hashChain.clear();
    }

    protected void finalize() {
        close();
    }
}
