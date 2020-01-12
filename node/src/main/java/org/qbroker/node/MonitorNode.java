package org.qbroker.node;

/* MonitorNode.java - a MessageNode waiting on certain occurrences */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Date;
import java.io.StringReader;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorGroup;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.EventSelector;
import org.qbroker.event.Event;

/**
 * MonitorNode listens to an input XQ for JMS messages as requests for
 * certain occurrences.  If a new request is for a new occurrence, it will
 * create the Monitor object according to the ruleset and the content.  Upon
 * success, the incoming message will be routed to the outlink of bypass if
 * the on-wait option is disabled for the ruleset. If it fails to create the
 * object, the message will be routed to the outlink of failure. If there is
 * no ruleset to handle the message, it will be routed to the outlink of nohit.
 * Meanwhile, MonitorNode periodically sends all active occurrences as tasks
 * to the outlink of pool. On the other end of the outlink of pool, a number of
 * instances of MonitorPersister will be listening on the outlink for tasks.
 * They will run each occurrence tasks and updates the results of the tasks.
 * Each task will be collected as the result of the monitor. The result will be
 * evaluated and escalated if it is necessary. The escalation messages will be
 * routed to the outlink of done. If the result matches the predefined patterns
 * or its lifetime ends, MonitorNode will terminate the occurrence and
 * generates a new message as the final escalation. Then the occurrence will be
 * destroyed. The outlink of done can be shared with with other outlinks except
 * for pool.
 *<br><br>
 * MonitorNode also contains a number of predefined rulesets.  These
 * rulesets categorize messages into non-overlapping groups via the filters.
 * Therefore, each rule defines a unique message group.  If a ruleset is not
 * of bypass, it is supposed to define a template to construct properties for
 * new occurrences.  It also contains multiple parameters for the node to
 * manage active occurrences.  For example, TimeToLive specifies the life time
 * in seconds of the new occurrence.  Once the occurrence passes that time span,
 * MonitorNode will escalate the final event and terminates it.  Option is
 * another parameter which controls when to deliver the original request.  By
 * default, it is zero and there is no wait on the final event.  If it is 1,
 * MonitorNode will hold the request and waits until the final event is
 * escalated.  Heartbeat determines how often to run the occurrence.  The
 * non-bypass rulesets may also have a list of EventPatterns to determine when
 * to escalate the final event for the occurrence and to terminate the request.
 * The total number of active occurrences is stored into the RULE_PEND field.
 * For those messages falling off all defined rulesets, MonitorNode always
 * creates an extra ruleset, nohit, to handle them.  Therefore all the nohit
 * messages will be routed to the outlink of nohit.
 *<br><br>
 * You are free to choose any names for the five fixed outlinks.  But
 * MonitorNode always assumes the first outlink for pool, the second for
 * done, the third for bypass, the fourth for failure and the last for nohit.
 * It is OK for the last four outlinks to share the same name. But the name of
 * the first outlink has to be unique.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MonitorNode extends Node {
    private int defaultHeartbeat;
    private String rcField;

    private AssetList reqList = null; // {rid,tid} to track active tasks on pool
    private XQueue pool;
    private long[] poolInfo;
    private int[] heartbeat;
    private int[] outLinkMap;

    private final static int TASK_ID = 0;
    private final static int TASK_GID = 1;
    private final static int TASK_RC = 2;
    private final static int TASK_STATUS = 3;
    private final static int TASK_COUNT = 4;
    private final static int TASK_RETRY = 5;
    private final static int TASK_TIME = 6;
    private final static int POOL_OUT = 0;
    private final static int RESULT_OUT = 1;
    private final static int BYPASS_OUT = 2;
    private final static int FAILURE_OUT = 3;
    private final static int NOHIT_OUT = 4;

    public MonitorNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        Map ph;
        long[] outInfo, ruleInfo;
        String key, saxParser = null;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "monitor";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        if ((o = props.get("Heartbeat")) != null)
            defaultHeartbeat = 1000 * Integer.parseInt((String) o);
        else
            defaultHeartbeat = 60000;

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "status";

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n,
            overlap, name, list);
        outLinkMap = new int[]{POOL_OUT, RESULT_OUT, BYPASS_OUT, FAILURE_OUT,
            NOHIT_OUT};
        outLinkMap[BYPASS_OUT] = overlap[0];
        outLinkMap[FAILURE_OUT] = overlap[1];
        outLinkMap[NOHIT_OUT] = overlap[2];

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));
        if (overlap[0] < outLinkMap[RESULT_OUT] ||
            overlap[1] < outLinkMap[RESULT_OUT] ||
            overlap[2] < outLinkMap[RESULT_OUT])
            throw(new IllegalArgumentException(name + ": bad overlap outlink "+
                overlap[0] + ", " + overlap[1] + " or " + overlap[2]));

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            outInfo = assetList.getMetaData(i);
            if (i == outLinkMap[POOL_OUT]) // for tracking request
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

        cfgList = new AssetList(name, 64);
        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        cells = new CollectibleCells(name, capacity);

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
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
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
                    else // rule failed
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
            throw(new IllegalArgumentException(name + ": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        int hmax = 0;
        int[] hbeat = new int[ruleList.size()];
        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            ruleInfo = ruleList.getMetaData(i);
            hbeat[i] = (int) ruleInfo[RULE_MODE];
            if (hbeat[i] <= 0)
                hbeat[i] = defaultHeartbeat;
            if (hbeat[i]> hmax)
                hmax = hbeat[i];
            if ((debug & DEBUG_INIT) == 0)
                continue;
            strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_MODE]/1000 +" - "+
                assetList.getKey((int) ruleInfo[RULE_OID]));
        }
        heartbeat = MonitorUtils.getHeartbeat(hbeat);
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG,name+" RuleName: RID PID TTL OPTION HB - " +
                "OutName" + strBuf.toString()).send();
            strBuf = new StringBuffer();
            for (i=0; i<heartbeat.length; i++)
                strBuf.append("\n\t" + i + " " + heartbeat[i]/1000);
            new Event(Event.DEBUG, name + " HEARTBEAT: HID HB - " + hmax +
                strBuf.toString()).send();
        }
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
        if(preferredOutName !=null && (i=assetList.getID(preferredOutName))<=0){
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

        // store TimeToLive into RULE_TTL field
        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_TTL] = 0;

        // store Heartbeat into RULE_MODE field
        if ((o = ph.get("Heartbeat")) != null && o instanceof String)
            ruleInfo[RULE_MODE] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_MODE] = defaultHeartbeat;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String) {
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
            rule.put("DisplayMask", o);
        }
        else
            ruleInfo[RULE_DMASK] = 0;

        if (preferredOutName != null) {
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
        }
        else if ((o = ph.get("Monitor")) != null && o instanceof Map) {
            str = "<Monitor>" + JSON2Map.toXML((Map) o) +
                "</Monitor>";
            rule.put("Template", new Template(str));
            // store on-wait Option into RULE_OPTION field
            if ((o = ph.get("Option")) != null)
                ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
            // store Capacity into RULE_EXTRA field
            if ((o = ph.get("Capacity")) != null)
                ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_EXTRA] = 64;
            // store MaxRetry into RULE_GID field
            if ((o = ph.get("MaxRetry")) != null)
                ruleInfo[RULE_GID] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_GID] = 2;
            rule.put("TaskList", new AssetList(ruleName,
                (int) ruleInfo[RULE_EXTRA]));

            if (ph.containsKey("EventPattern"))
                rule.put("Selector", new EventSelector(ph));
            ruleInfo[RULE_PID] = TYPE_ACTION;
            ruleInfo[RULE_OID] = outLinkMap[POOL_OUT];
        }
        else {
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // copy selected properties
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
        else { // copy all properties
            rule.put("PropertyName", new String[0]);
        }

        return rule;
    }

    /** cleans up the tasks in the given taskList */
    private int cleanupTasks(AssetList taskList) {
        Browser browser;
        Map task;
        MonitorReport report;
        int id, n = 0;
        if (taskList == null)
            return -1;
        if (taskList.size() <= 0)
            return 0;
        browser = taskList.browser();
        while ((id = browser.next()) >= 0) {
            task = (Map) taskList.get(id);
            if (task == null || task.size() <= 0)
                continue;
            n ++;
            report = (MonitorReport) task.remove("Report");
            if (report != null)
                report.destroy();
            task.clear();
        }
        taskList.clear();
        return n;
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
                AssetList list = (AssetList) h.remove("TaskList");
                if (list != null)
                    cleanupTasks(list);
                h.clear();
            }
            return id;
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.removeRule(key, in);
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
            Map rule = initRuleset(tm, ph, meta);
            if (rule != null && rule.containsKey("Name")) {
                StringBuffer strBuf = ((debug & DEBUG_DIFF) <= 0) ? null :
                    new StringBuffer();
                Map h = (Map) ruleList.set(id, rule);
                if (h != null) {
                    MessageFilter filter = (MessageFilter) h.remove("Filter");
                    if (filter != null)
                        filter.clear();
                    AssetList list = (AssetList) h.remove("TaskList");
                    if (list != null)
                        cleanupTasks(list);
                    h.clear();
                }
                tm = ruleInfo[RULE_PID];
                for (int i=0; i<RULE_TIME; i++) { // update metadata
                    switch (i) {
                      case RULE_SIZE:
                        break;
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
            else
                new Event(Event.ERR, name + " failed to init rule "+key).send();
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.replaceRule(key, ph, in);
        }
        return -1;
    }

    /**
     * It initializes the monitor task based on the template and the msg and
     * returns the occurrence as a new task. 
     */
    private Map<String, Object> initTask(Template template, byte[] buffer,
        Message msg)
        throws JMSException {
        Map ph;
        String key, text;
        if (template == null || msg == null)
            return null;
        text = MessageUtils.format(msg, buffer, template);
        try {
            StringReader ins = new StringReader(text);
            ph = (Map) JSON2Map.parse(ins);
            ins.close();
        }
        catch (Exception e) {
            throw(new JMSException("failed to init task: " + e.toString()));
        }
        return MonitorGroup.initMonitor(ph, name);
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null;
        StringBuffer strBuf = null;
        Object o;
        Map rule;
        Browser browser;
        MessageFilter[] filters = null;
        Template template = null;
        AssetList taskList = null;
        String[] propertyName = null, propertyValue = null;
        long[] outInfo = null, ruleInfo = null, taskInfo;
        int[] ruleMap;
        long currentTime, sessionTime;
        long count = 0;
        int mask, ii, hbeat, retry;
        int i = 0, id, hid, tid, m, n;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        boolean dspBody = false, inDetail = ((debug & DEBUG_REPT) > 0);
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

        // initialize patterns
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

        // update assetList
        n = out.length;
        for (i=0; i<n; i++) {
            Object[] asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }
        pool = out[outLinkMap[POOL_OUT]];
        poolInfo = assetList.getMetaData(outLinkMap[POOL_OUT]);
        if (pool.getCapacity() != reqList.getCapacity()) { // rereate reqList
            reqList.clear();
            reqList = new AssetList(name, pool.getCapacity());
        }

        n = ruleMap.length;
        strBuf = new StringBuffer();
        // set all groups active at startup but the first
        hid = heartbeat.length - 1;
        hbeat = heartbeat[hid];
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        ii = 0;
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((cid = in.getNextCell(waitTime)) >= 0) { // got a new msg
                currentTime = System.currentTimeMillis();

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
                    new Event(Event.ERR, str + " failed to apply the filter "+i+
                        ": " + Event.traceStack(e)).send();
                    i = -1;
                }

                ruleName = ruleList.getKey(rid);
                ruleInfo = ruleList.getMetaData(rid);

                if (i < 0) // failed on filter
                    oid = outLinkMap[FAILURE_OUT];
                else if (TYPE_ACTION != (int) ruleInfo[RULE_PID]) // bypass
                    oid = (int) ruleInfo[RULE_OID];
                else try { // to init the monitor and to add it to taskList
                    rule = (Map) ruleList.get(rid);
                    propertyName = (String[]) rule.get("PropertyName");
                    template = (Template) rule.get("Template");
                    Map<String,Object> task=initTask(template,buffer,inMessage);
                    if (task != null && task.size() > 0) {
                        String key = (String) task.get("Name");
                        TimeWindows tw = (TimeWindows) task.get("TimeWindow");
                        if (ruleInfo[RULE_TTL] > 0)
                            tw.setStopTime(currentTime + ruleInfo[RULE_TTL]);
                        if (propertyName != null) // copy properties over
                            copyProperties(propertyName, task, inMessage);
                        taskList = (AssetList) rule.get("TaskList");
                        id = taskList.add(key, new long[TASK_TIME+1], task);
                        if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name + " " + ruleName +
                                " added a new task of " + key + " from " + cid +
                                " to " + id + " with " + taskList.size() + " " +
                                ruleInfo[RULE_PEND]).send();
                        if (id >= 0) {
                            taskInfo = taskList.getMetaData(id);
                            for (i=0; i<=TASK_TIME; i++)
                                taskInfo[i] = 0;
                            taskInfo[TASK_ID] = -1;
                            taskInfo[TASK_GID] = -1;
                            taskInfo[TASK_STATUS] = NODE_READY;
                            taskInfo[TASK_TIME] = currentTime;
                            ruleInfo[RULE_PEND] ++;
                            ruleInfo[RULE_TIME] = currentTime;
                            if (ruleInfo[RULE_OPTION] > 0) { // on wait
                                // hold the message until it is done 
                                taskInfo[TASK_GID] = cid;
                                continue;
                            }
                            oid = outLinkMap[BYPASS_OUT];
                        }
                        else { // failed to add to the list
                            MonitorReport report;
                            report = (MonitorReport) task.get("Report");
                            if (report != null)
                                report.destroy();
                            task.clear();
                            if (taskList.getID(key) < 0) {
                                new Event(Event.ERR, name + ": " + ruleName +
                                    " failed to add new task of " + key +
                                    " " + taskList.size() + "/" +
                                    ruleInfo[RULE_EXTRA]).send();
                                oid = outLinkMap[FAILURE_OUT];
                            }
                            else {
                                new Event(Event.WARNING, name +": "+ruleName+
                                    " has an active task for " + key).send();
                                oid = outLinkMap[BYPASS_OUT];
                            }
                        }
                    }
                    else { // failed to init occurrence
                        oid = outLinkMap[FAILURE_OUT];
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to init a new task").send();
                    }
                }
                catch (Exception e) {
                    oid = outLinkMap[FAILURE_OUT];
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to create a new task: " +
                        Event.traceStack(e)).send();
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid).send();

                if (displayMask > 0) try {// display message
                    if (dspBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO, name+": "+ruleName+" processed msg "+
                        (count + 1)+":"+MessageUtils.display(inMessage, msgStr,
                        displayMask, displayPropertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: " + e.toString()).send();
                }

                count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
                feedback(in, -1L);
            }

            currentTime = System.currentTimeMillis();
            if (currentTime < sessionTime) { // session not due yet
                if (pool.collectible() > 0)
                    collect(in, outLinkMap[RESULT_OUT]);
                if (msgList.size() > 0)
                    feedback(in, -1L);
                continue;
            }

            inDetail = ((debug & DEBUG_REPT) > 0);
            m = 0;
            browser.reset();
            while ((rid = browser.next()) >= 0) { // update active rulesets
                ruleInfo = ruleList.getMetaData(rid);
                if (ruleInfo[RULE_PID] != TYPE_ACTION)
                    continue;
                if ((hbeat % (int) ruleInfo[RULE_MODE]) != 0) // not active
                    continue;
                if (ruleInfo[RULE_PEND] <= 0) // empty rule
                    continue;

                ruleName = ruleList.getKey(rid);
                rule = (Map) ruleList.get(rid);
                taskList = null;
                if (rule == null || rule.size() <= 0) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " is empty").send();
                    continue;
                }
                taskList = (AssetList) rule.get("TaskList");
                if (taskList == null) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " has no taskList").send();
                    continue;
                }
                if (inDetail)
                    new Event(Event.DEBUG, name + ": " + ruleName +
                        " is active with " + taskList.size() + " tasks").send();
                if (taskList.size() <= 0)
                    continue;
                retry = (int) ruleInfo[RULE_GID];
                Browser b = taskList.browser();
                while ((tid = b.next()) >= 0) { // dispatch tasks
                    taskInfo = taskList.getMetaData(tid);
                    id = (int) taskInfo[TASK_ID];
                    if (id < 0) { // ready for next run
                        ObjectEvent msg = new ObjectEvent();
                        msg.setPriority(Event.INFO);
                        msg.setAttribute("name", taskList.getKey(tid));
                        msg.setBody(taskList.get(tid));
                        id = pool.reserve(waitTime);
                        if (id >= 0) { // reserved
                            if (reqList.existsID(id)) { // preoccupied?
                                collect(currentTime, id, in,
                                    outLinkMap[RESULT_OUT]);
                            }
                            pool.add(msg, id);
                            taskInfo[TASK_ID] = id;
                            taskInfo[TASK_STATUS] = NODE_RUNNING;
                            taskInfo[TASK_TIME] = currentTime;
                            taskInfo[TASK_RETRY] = 0;
                            reqList.add(String.valueOf(id),
                                new long[]{rid, tid}, msg, id);
                            poolInfo[OUT_SIZE] ++;
                            poolInfo[OUT_TIME] = currentTime;
                            poolInfo[OUT_QTIME] = currentTime;
                            m ++;
                            if (inDetail)
                                strBuf.append("\n\t" + ruleName + "/" +
                                    taskList.getKey(tid) + ": " + tid + " " +
                                    id + " " + taskInfo[TASK_RC] + " " +
                                    taskInfo[TASK_COUNT]);
                        }
                        else { // failed to reserve
                            new Event(Event.ERR, name + " "+ ruleName + ":" +
                                taskList.getKey(tid) + " failed to reserve "+
                                "from " + pool.getName() + " " +  pool.depth()+
                                ":" + pool.size() + "/" + id).send();
                        }
                    }
                    else if (ruleInfo[RULE_GID] > 0 &&
                        taskInfo[TASK_RETRY] >= ruleInfo[RULE_GID]) { // stuck
                        taskInfo[TASK_RETRY] ++;
                        if (taskInfo[RULE_STATUS] == NODE_RUNNING ||
                            taskInfo[RULE_STATUS] == NODE_READY)
                            taskInfo[RULE_STATUS] = NODE_RETRYING;
                        new Event(Event.ERR, name + " "+ ruleName + ":" +
                            taskList.getKey(tid) + " timed out and stuck "+
                            taskInfo[TASK_RETRY] + " times at " + id).send();
                    }
                    else {
                        taskInfo[TASK_RETRY] ++;
                        new Event(Event.WARNING, name + " "+ ruleName + ":" +
                            taskList.getKey(tid) + " timed out and stuck "+
                            taskInfo[TASK_RETRY] + " times at " + id).send();
                    }
                }
            }

            if (pool.collectible() > 0)
                collect(in, outLinkMap[RESULT_OUT]);
            if (msgList.size() > 0)
                feedback(in, -1L);

            if (inDetail && strBuf.length() > 0) {
                new Event(Event.DEBUG, name + " RULE/KEY: TID ID RC COUNT - " +
                    m + " " + hid + " " + hbeat + " " + pool.size() + ":" +
                    pool.depth() + " " + reqList.size() +
                    strBuf.toString()).send();
                strBuf = new StringBuffer();
            }

            hid ++;
            if (hid >= heartbeat.length) { // reset session
                hid = 0;
                sessionTime += heartbeat[hid];
            }
            else {
                sessionTime += heartbeat[hid] - hbeat;
            }
            hbeat = heartbeat[hid];
            currentTime = System.currentTimeMillis();
            if (currentTime > sessionTime) // reset sessionTime
                sessionTime = currentTime;
        }
    }

    private int copyProperties(String[] propertyName, Map<String, Object> task,
        Message msg)
        throws JMSException {
        String key, str;
        List<String> keys = new ArrayList<String>();
        List<String> list = new ArrayList<String>();
        int i, n;
        if (propertyName == null || msg == null || task == null)
            return 0;
        n = propertyName.length;
        if (n > 0) { // copy selected properties
            for (i=0; i<n; i++) {
                key = propertyName[i];
                if (key == null)
                    continue;
                str = MessageUtils.getPropertyID(key);
                if (str == null)
                    str = MessageUtils.getProperty(key, msg);
                else
                    str = MessageUtils.getProperty(str, msg);

                if (str != null) {
                    keys.add(key);
                    list.add(str);
                }
            }
        }
        else { // copy all properties
            Enumeration propNames = msg.getPropertyNames();
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.equals("null"))
                    continue;
                str = MessageUtils.getProperty(key, msg);

                if (str != null) {
                    keys.add(key);
                    list.add(str);
                }
            }
            key = MessageUtils.getPropertyID("JMSType");
            str = MessageUtils.getProperty(key, msg);
            if (str != null) {
                keys.add(key);
                list.add(str);
            }
        }
        n = list.size();
        if (n > 0) {
            task.put("PropertyName", keys.toArray(new String[n]));
            task.put("PropertyValue", list.toArray(new String[n]));
            list.clear();
            keys.clear();
        }
        return n;
    }

    /**
     * It collects collectible messages from pool and escalates them one by
     * one.  Upon success, it returns the number of messages successfully
     * collected.
     */
    private int collect(XQueue in, int oid) {
        long tm;
        int id = -1, k, n;
        n = 0;
        while ((id = pool.collect(-1L)) >= 0) {
            tm = System.currentTimeMillis();
            k = collect(tm, id, in, oid);
            if (k >= 0)
                n ++;
            else
                new Event(Event.ERR, name+" failed to collect from "+id).send();
        }
        if (n > 0 && (debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " collected " + n + " tasks").send();
        return n;
    }

    /**
     * It evaluates the collected message at mid and flushes escalation msg to
     * the output XQueue of oid.  It returns the number of escalations flushed
     * to the oid upon success or -1 if there is an unexpected error.
     */
    private int collect(long currentTime, int mid, XQueue in, int oid) {
        long[] ruleInfo, taskInfo, state;
        Object o;
        Event event = null;
        EventSelector selector;
        TextEvent outMessage;
        ObjectMessage msg;
        AssetList taskList;
        Map rule, task;
        String[] propertyName = null, propertyValue = null;
        String key, status;
        int i = 0, rc, cid, rid, tid, dmask;
        if (mid < 0 || mid >= capacity)
            return -1;
        state = reqList.getMetaData(mid);
        msg = (ObjectMessage) reqList.remove(mid);
        if (state == null || state.length < 2)
            return -1;
        rid = (int) state[0];
        tid = (int) state[1];
        poolInfo[OUT_SIZE] --;
        poolInfo[OUT_COUNT] ++;
        poolInfo[OUT_TIME] = currentTime;

        task = null;
        if (msg != null) try {
            task = (Map) msg.getObject();
            event = (Event) msg;
            msg.clearBody();
        }
        catch (Exception e) {
            task = null;
        }

        try {
            rc = Integer.parseInt(event.getAttribute(rcField));
        }
        catch (Exception e) {
            rc = TimeWindows.BLACKEXCEPTION;
        }

        status = TimeWindows.STATUS_TEXT[rc - TimeWindows.EXCEPTION];
        rule = (Map) ruleList.get(rid);
        selector = (EventSelector) rule.get("Selector");
        taskList = (AssetList) rule.get("TaskList");
        key = taskList.getKey(tid);
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                " collecting " + key + " from " + mid + " on " + status).send();
        taskInfo = taskList.getMetaData(tid);
        if (taskInfo != null) { // update taskInfo
            taskInfo[TASK_ID] = -1;
            taskInfo[TASK_STATUS] = NODE_READY;
            taskInfo[TASK_RC] = rc;
            taskInfo[TASK_TIME] = currentTime;
            taskInfo[TASK_COUNT] ++;
        }
        if (task == null || !task.containsKey("Name")) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to collect event for " + key + " from " + mid).send();
            return -1;
        }
        if (rc == TimeWindows.EXPIRED) { // expired
            outMessage = new TextEvent();
            outMessage.setAttribute("status", status);
            outMessage.setAttribute("name", key);
        }
        else if ((o = task.remove("Event")) != null) { // for escalation
            event = (Event) o;
            try {
                outMessage = TextEvent.toTextEvent(event);
            }
            catch (Exception e) {
                outMessage = new TextEvent();
                outMessage.setAttribute("status", status);
                outMessage.setAttribute("name", key);
            }
            if (selector == null ||
                !selector.evaluate(currentTime, event)) { // no state change
                // flush the escalation msg
                i = passthru(currentTime, (Message) outMessage, in, rid,
                    oid, -1, 0);
                return i;
            }
        }
        else { // check state change
            event = (Event) msg;
            event.setAttribute("status", status);
            if (selector == null ||
                !selector.evaluate(currentTime, event)) { // no state change
                return 0;
            }
            // for escalation and cleanup
            outMessage = new TextEvent();
            outMessage.setAttribute("status", status);
            outMessage.setAttribute("name", key);
        }

        // set the properties copied from the original message
        propertyName = (String[]) task.get("PropertyName");
        propertyValue = (String[]) task.get("PropertyValue");
        if (propertyName != null && propertyValue != null) try {
            int ic = -1;
            for (i=0; i<propertyName.length; i++) {
                if (MessageUtils.setProperty(propertyName[i],
                    propertyValue[i], (Message) outMessage) != 0)
                    ic = i;
            }
            if (ic >= 0)
                new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                    " failed to set property of "+ propertyName[ic]).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " +ruleList.getKey(rid) +
                " failed to set properties: " + Event.traceStack(e)).send();
        }
        cid = (int) taskInfo[TASK_GID];
        ruleInfo = ruleList.getMetaData(rid);
        dmask = (int) ruleInfo[RULE_DMASK];
        if (dmask > 0) try {
            String line = MessageUtils.display((Message) outMessage, null,
                dmask, null);
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " terminated the task " + key + " and escalated a msg:" +
                line).send();
        }
        catch (Exception e) {
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " terminated the task " + key + " and escalated a msg").send();
        }
        // flush the final escalation msg
        i = passthru(currentTime, (Message) outMessage, in, rid, oid, -1, 0);
        if (i <= 0)
            new Event(Event.ERR, name + ": " +ruleList.getKey(rid) +
                " failed to flush final escalation msg to " + oid).send();
        // destroy the task and update ruleInfo
        ruleInfo[RULE_PEND] --;
        ruleInfo[RULE_TIME] = currentTime;
        MonitorReport report = (MonitorReport) task.get("Report");
        if (report != null)
            report.destroy();
        task.clear();
        taskList.remove(tid);
        if (cid >= 0) { // wait is done so bypass the original request
            int j = passthru(currentTime, (Message) in.browse(cid), in, rid,
                outLinkMap[BYPASS_OUT], cid, 0);
            if (j <= 0)
                new Event(Event.ERR, name + ": " +ruleList.getKey(rid) +
                    " failed to passthru original msg to " +
                    outLinkMap[BYPASS_OUT]).send();
        }
        return i;
    }

    /** returns summary from the properties of a given task */
    private String getSummary(Map task, String[] propertyName) {
        Map<String, Object> h;
        StringBuffer strBuf;
        String[] pn = null, pv = null;
        String value;
        int i;

        if (task == null)
            return "";

        pn = (String[]) task.get("PropertyName");
        pv = (String[]) task.get("PropertyValue");
        if (pn == null || pv == null || pn.length <= 0)
            return "";

        h = new HashMap<String, Object>();
        for (i=0; i<pn.length; i++) {
            if (pn[i] != null && pn[i].length() > 0)
                h.put(pn[i], pv[i]);
        }

        strBuf = new StringBuffer();
        if (propertyName != null) {
            for (i=0; i<propertyName.length; i++) {
                if (h.containsKey(propertyName[i])) {
                    value = (String) h.get(propertyName[i]);
                    if (value != null) {
                        if (strBuf.length() > 0)
                            strBuf.append(" ~ ");
                        strBuf.append(propertyName[i] + ": " + value);
                    }
                }
            }
        }
        h.clear();

        return strBuf.toString();
    }

    /** list all propagating messages and pending jobs */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h, ph;
        Map rule;
        StringBuffer strBuf;
        String key, qname, str, text;
        long[] info;
        int i, id, cid, k, n, rid;
        long tm;
        AssetList list;
        Browser browser, b;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (h == null)
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

        k = 0;
        qname = pool.getName();
        browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) {
            info = (long[]) ruleList.getMetaData(rid);
            if (info[RULE_PEND] <= 0)
                continue;
            rule = (Map) ruleList.get(rid);
            list = (AssetList) rule.get("TaskList");
            if (list == null || list.size() <= 0)
                continue;
            key = ruleList.getKey(rid);
            if (key == null)
                key = "-";
            b = list.browser();
            while ((id = b.next()) >= 0) {
                info = (long[]) list.getMetaData(id);
                cid = (int) info[TASK_GID];
                if (cid < 0) { // msg is not cached
                    if (hasSummary)
                        str = getSummary((Map) list.get(id),
                            displayPropertyName);
                    else
                        str = "";
                    tm = info[TASK_TIME];
                }
                else if (ph.containsKey(String.valueOf(cid))) // duplicate
                    continue;
                else {
                    if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                        continue;
                    msg = (Message) xq.browse(cid);
                    if (msg == null || info == null)
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
                text = Event.dateFormat(new Date(tm));
                if (type == Utils.RESULT_XML) { // for xml
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<ID>" + k + "</ID>");
                    strBuf.append("<CellID>" + cid + "</CellID>");
                    strBuf.append("<Status>PENDING</Status>");
                    strBuf.append("<Rule>" + key + "</Rule>");
                    strBuf.append("<OutLink>" + qname + "</OutLink>");
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
                    strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) +
                        "\"");
                    strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) +
                        "\"");
                    strBuf.append("}");
                }
                else { // for text
                    h.put(linkName + "_" + k, cid + " PENDING " + key +
                    " " + qname + " " + text + " " + str);
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

    /** returns 0 to have the ack propagation skipped on the first outlink */
    public int getOutLinkBoundary() {
        return 0;
    }

    public void close() {
        int rid;
        Map rule;
        Browser browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null) {
                cleanupTasks((AssetList) rule.get("TaskList"));
            }
        }
        super.close();
        reqList.clear();
    }

    protected void finalize() {
        close();
    }
}
