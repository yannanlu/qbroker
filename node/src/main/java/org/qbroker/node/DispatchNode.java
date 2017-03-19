package org.qbroker.node;

/* DispatchNode.java - a MessageNode dispatching JMS messages */

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
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * DispatchNode dispatches JMS messages to various destinations according to
 * their content, and preconfigured rulesets. DispatchNode contains one or more 
 * outlinks as the destinations. Those outlinks are grouped exclusively
 * according to their group name. Within each group, all the oulinks are
 * equivalent and backing up of each other in case of failure or high load.
 *<br/><br/>
 * DispatchNode uses the predefined rulesets to dispatch incoming messages.
 * These rulesets categorize messages into non-overlapping groups. Therefore,
 * each rule defines a unique message group. The ruleset also specifies the
 * association between the message group and the outlinks. For those messages
 * falling off all defined rulesets, DispatchNode always creates an extra
 * ruleset, nohit, to handle them. By default, nohit ruleset always uses the
 * last outlink unless there is one defined explicitly.
 *<br/><br/>
 * Currently, DispatchNode supports only four types of dynamic switchings:
 * roundrobin, weighted, preferred and sticky. All the switchings are based on
 * the same outlink groups. DispatchNode assumes all the destinations are
 * equivalent and mutually replaceable within the same group. Therefore, there
 * is no fixed association between a ruleset and the outlinks in the same group.
 * For the type of roundrobin, DispatchNode picks up next available outlink in
 * the group for each incoming message roundrobinly. For the type of weighted,
 * DispatchNode looks for the outlink with lowest load in the group for each
 * incoming message. For the type of preferred, the association between the
 * ruleset and the outlinks in the group changes only if there is a failover
 * due to either the load or the availability of the destinations. For the type
 * of sticky, the association between the rulesets and the outlinks is
 * determined by the hash value of the key that is retrieved from the incoming
 * message. If the outlink has a high load or it is not available, sticky
 * messages will be routed to its redirect outlink. For nohit ruleset, the
 * default switching type is roundrobin unless its outlink has been defined
 * explicitly.
 *<br/><br/>
 * In case of a high load or a failure from the report on a given outlink, its
 * status will be set to standby mode for load balance and high availability.
 * Meanwhile, DispatchNode will failover all stuck messages to other available
 * outlinks within the group and updates the outlink associations of the
 * rulesets. When the outlink becomes available again, it will failback those
 * outstanding messages to their oringial outlink and resumes the original
 * outlink associations. Dynamic load balance, failover and failback are
 * supported via the sessions and monitor reports. If SessionTimeout is
 * non-zero, please make sure all outlinks having their reports defined.
 * Otherwise, SessionTimeout will be reset to zero to prevent outlinks from
 * being stuck forever. On the other hand, if SessionTimeout is set to zero,
 * it will only do static switching and load balancing. DispatchNode supports
 * static reports with a non-zero SessionTimeout. In this case, the static
 * report must contain ReportType with the value of "static" in lower case.
 *<br/><br/>
 * Threshold contains 3 values for low, medium and high load.  The load for
 * each outlink is supposed to be updated by reporters.  DispatchNode checks
 * the load for each outlink every heartbeat and controls the pace of the
 * dispatch process.  If the load is between low and medium, DispatchNode will
 * skip the outlink every other time to slow down its pace. Meanwhile, it
 * also monitors the session count of dispatched messages so that the total
 * number of dispatched messages plus the load of the destination will not
 * exceed the value of high load within each session. If the load is above
 * medium, the outlink will be marked as standby mode. In the meantime, all the
 * messages in the outlink will be migrated to another outlink with lower load
 * in the group.
 *<br/><br/>
 * In order for DispatchNode to check the report for each outlink, ReportName
 * has to be defined in each OutLink. DispatchNode looks up the report via the
 * report name every heartbeat. First, it checks the timestamp of the report.
 * If the report is newly updated, it will check the status of the report.
 * If the status is 0 (Normal), the node will query the current depth for the
 * destination with the key of CurrentDepth. If it is defined, the number will
 * be treated as the current depth. It will be used for flow control and load
 * balance. Otherwise, it will be assumed to be zero. In case of the static
 * report, DispatchNode will use the local flow rate for each session. Anyway,
 * the reports can be used to control the load balance and flow rate.
 *<br/><br/>
 * There is no limit on the number of outlinks. All the outlinks will take part
 * in the load balance process.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DispatchNode extends Node {
    private int heartbeat = 60000;
    private int sessionTimeout = 0;
    private int[] threshold;
    private int NOHIT_OUT = 0;
    private AssetList groupList = null;
    private HashChain[] hashChain = null;

    public DispatchNode(Map props) {
        super(props);
        Object o;
        List list;
        String key;
        Browser browser;
        Map<String, Object> rule;
        Map ph;
        long[] outInfo, ruleInfo;
        long tm;
        int[] ids;
        int i, j, n, id, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "dispatch";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000*Integer.parseInt((String) o);
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

        ids = new int[n];
        for (j=0; j<n; j++)
            ids[j] = -1;
        groupList = new AssetList(name, capacity);
        groupList.add("", new long[]{0, -1, threshold[2]}, ids);

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            Object[] asset = (Object[]) assetList.get(i);
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_OFFSET] < 0 || outInfo[OUT_LENGTH] < 0 ||
                (outInfo[OUT_LENGTH]==0 && outInfo[OUT_OFFSET]!=0) ||
                outInfo[OUT_LENGTH] + outInfo[OUT_OFFSET] >
                outInfo[OUT_CAPACITY])
                throw(new IllegalArgumentException(name +
                    ": OutLink Partition is not well defined for " +
                    assetList.getKey(i)));
            NOHIT_OUT = i;
            // disable redirect for sticky
            outInfo[OUT_QIPPS] = -1;
            if ((debug & DEBUG_INIT) > 0)
                strBuf.append("\n\t" + assetList.getKey(i) + ": " + i + " " +
                    outInfo[OUT_CAPACITY] + " " + outInfo[OUT_OFFSET] +
                    "," + outInfo[OUT_LENGTH]);

            if (asset == null || asset.length <= ASSET_THR)
                continue;
            if (sessionTimeout > 0) { // check required reports
                key = (String) asset[ASSET_URI];
                if (key == null || key.length() <= 0) {
                    // no report defined
                    new Event(Event.WARNING, name + ": outlink " + i +
                        ", " + assetList.getKey(i) + ", has no report defined"+
                        ", hence SessionTimeout is reset to 0").send();
                    sessionTimeout = 0;
                }
            }
            // check groups
            key = (String) asset[ASSET_THR];
            if (key == null || key.length() <= 0) { // default group
                long[] meta = groupList.getMetaData(0);
                ids = (int[]) groupList.get(0);
                ids[(int) meta[0]] = i;
                meta[0] ++;
                j = 0;
            }
            else if ((j = groupList.getID(key)) > 0) { // existing group
                long[] meta = groupList.getMetaData(j);
                ids = (int[]) groupList.get(j);
                ids[(int) meta[0]] = i;
                meta[0] ++;
            }
            else { // new group
                ids = new int[n];
                ids[0] = i;
                for (j=1; j<n; j++)
                     ids[j] = -1;
                j = groupList.add(key, new long[]{1,-1,threshold[2]}, ids);
            }
            if ((debug & DEBUG_INIT) > 0)
                strBuf.append(" " + j);
        }

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                "gid - " + linkName + " " + capacity +strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (assetList.size() <= 0)
            throw(new IllegalArgumentException(name+": OutLink is empty"));

        hashChain = new HashChain[groupList.size()];
        browser = groupList.browser();
        while ((i = browser.next()) >= 0) { // replace ids with compact list
            int[] idn;
            long[] meta = groupList.getMetaData(i);
            n = (int) meta[0];
            idn = new int[n];
            ids = (int[]) groupList.set(i, idn);
            hashChain[i] = new HashChain(name +"_"+ groupList.getKey(i), 4*n);
            for (j=0; j<n; j++) {
                idn[j] = ids[j];
                hashChain[i].add(assetList.getKey(idn[j]));
            }
        }

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
            ruleInfo[RULE_DMASK] = displayMask;
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = ruleInfo[RULE_OID];
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
                            new Event(Event.ERR, name + ": ruleset " + i +
                                ", " + key + ", failed to be added").send();
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
            throw(new IllegalArgumentException(name+": failed to init rule: "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL] + " " +
                    ruleInfo[RULE_GID] + " " + ruleInfo[RULE_EXTRA] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID TTL GID EXTRA " +
                "- OutName"+ strBuf.toString()).send();
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
                if (sessionTimeout > 0) { // check required reports
                    Browser b = assetList.browser();
                    while ((i = b.next()) >= 0) {
                        Object[] asset = (Object[]) assetList.get(i);
                        if (asset == null || asset.length <= ASSET_URI)
                            continue;
                        String reportName = (String) asset[ASSET_URI];
                        if (reportName == null || reportName.length() <= 0) {
                            // no report defined
                            new Event(Event.WARNING, name + ": outlink " + i +
                                ", " + assetList.getKey(i) +
                                ", has no report defined" +
                                ", hence SessionTimeout is reset to 0").send();
                            sessionTimeout = 0;
                            break;
                        }
                    }
                }
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
        String key, str, ruleName, ruleType, preferredOutName;
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
        rule.put("Name",  ruleName);
        rule.put("ReportName",  ph.get("ReportName"));
        ruleType = (String) ph.get("RuleType");
        if (ruleType == null)
            ruleType = "preferred";
        else if (!"roundrobin".equals(ruleType.toLowerCase()) &&
            !"weighted".equals(ruleType.toLowerCase()) &&
            !"sticky".equals(ruleType.toLowerCase()))
            ruleType = "preferred";

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

        if ((o = ph.get("OutLinkGroup")) != null && o instanceof String &&
            (str = (String) o).length() > 0) { // for outlink group
            if ((j = groupList.getID(str)) <= 0) { // groupName NOT well defined
                new Event(Event.ERR, name + ": groupName for " + ruleName +
                    " is not well defined: " + str).send();
                j = 0;
            }
            long[] meta = groupList.getMetaData(j);
            // store size of group into RULE_EXTRA
            ruleInfo[RULE_EXTRA] = meta[0];
            // store the group ID into RULE_GID
            ruleInfo[RULE_GID] = j;
        }
        else { // default group
            long[] meta = groupList.getMetaData(0);
            // store size of group into RULE_EXTRA
            ruleInfo[RULE_EXTRA] = meta[0];
            // store the group ID into RULE_GID
            ruleInfo[RULE_GID] = 0;
        }

        preferredOutName = (String) ph.get("PreferredOutLink");
        if (preferredOutName==null || !assetList.containsKey(preferredOutName)){
            int[] ids = (int[]) groupList.get((int) ruleInfo[RULE_GID]);
            if (ids == null || ids.length <= 0)
                preferredOutName = assetList.getKey(NOHIT_OUT);
            else
                preferredOutName = assetList.getKey(ids[0]);
            if ("prefered".equals(ruleType))
                new Event(Event.WARNING, name + ": OutLink for " +
                    ruleName+" not well defined, use the default: "+
                    preferredOutName).send();
        }
        else { // verify the preferredOutName
            int[] ids = (int[]) groupList.get((int) ruleInfo[RULE_GID]);
            j = assetList.getID(preferredOutName);
            if (ids == null || ids.length <= 0)
                preferredOutName = assetList.getKey(NOHIT_OUT);
            else {
                for (i=0; i<ids.length; i++) {
                    if (j == ids[i])
                        break;
                }
                if (i >= ids.length)
                    preferredOutName = assetList.getKey(ids[0]);
            }
            if (j != assetList.getID(preferredOutName))
                new Event(Event.WARNING, name + ": OutLink for " +
                    ruleName+" out of the group, use the default: "+
                    preferredOutName).send();
        }

        ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);

        if ("roundrobin".equals(ruleType.toLowerCase())) {
            ruleInfo[RULE_PID] = TYPE_ROUNDROBIN;
        }
        else if ("weighted".equals(ruleType.toLowerCase())) {
            ruleInfo[RULE_PID] = TYPE_WEIGHTED;
        }
        else if ("sticky".equals(ruleType.toLowerCase())) {
            if ((o = ph.get("KeyTemplate")) != null && o instanceof String)
                rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String) o));
            ruleInfo[RULE_PID] = TYPE_NONE;
        }
        else { // for prefer or bypass 
            ruleInfo[RULE_PID] = ruleInfo[RULE_OID];
            outInfo[OUT_NRULE] ++;
            outInfo[OUT_ORULE] ++;
        }

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
        String msgStr = null, ruleName = null;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        long[][] info = null;
        int[] ruleMap;
        long currentTime, previousTime, st, wt;
        long count = 0;
        int mask, ii, sz, tid, retryCount = 0, dspBody;
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
        info = new long[k][];
        for (i=0; i<k; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            info[i] = outInfo;
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

        if (sessionTimeout > 0) { // wait for reports
            for (i=0; i<k; i++) { // set pause first
                outInfo = assetList.getMetaData(i);
                if (outInfo[OUT_STATUS] == NODE_PAUSE)
                    MessageUtils.pause(out[i]);
            }
            currentTime = System.currentTimeMillis();
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
                if (sessionTimeout > 0)
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

            // applying switch rule
            if (ruleInfo[RULE_PID] == TYPE_ROUNDROBIN) { // roundrobin
                long d, dd = threshold[LOAD_MEDIUM];
                int[] ids = (int[]) groupList.get((int) ruleInfo[RULE_GID]);
                boolean hasFlipped = false;
                k = ids.length;
                oid = (int) ruleInfo[RULE_OID];
                for (i=0; i<k; i++) { // look for the previous oid
                    if (oid == ids[i])
                        break;
                }
                if (i >= k) { // previous oid not found
                    i = (int) ruleInfo[RULE_GID];
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to locate the previous outlink " + oid + "/" +
                        k + " for group " + groupList.getKey(i)).send();
                }
                else if (sessionTimeout <= 0) { // static session
                    oid = ids[(i + 1) % k];
                }
                else for (int j=0; j<k; j++) { // dynamic switch to next
                    oid = ids[(i+j+1) % k];
                    outInfo = info[oid];
                    // check availability
                    if (outInfo[OUT_STATUS] == NODE_STANDBY ||
                        outInfo[OUT_QSTATUS] != 0)//disabled or failed on report
                        continue;
                    if (outInfo[OUT_STATUS] == NODE_RUNNING)
                        break;
                    // estimate load for PAUSE status
                    d = outInfo[OUT_QDEPTH] - outInfo[OUT_DEQ] +
                        outInfo[OUT_SIZE] + outInfo[OUT_EXTRA] +
                        out[oid].getCount();
                    if (d < threshold[LOAD_LOW]) { // slowing down a bit
                        if (outInfo[OUT_MODE] < 0) { // every other msg
                            outInfo[OUT_MODE] = 0;
                            break;
                        }
                        else if (!hasFlipped) {
                            outInfo[OUT_MODE] = -1;
                            hasFlipped = true;
                        }
                        if (d < dd) { // look for lowest oid
                            dd = d;
                            j = oid;
                        }
                    }
                    else if (d < dd) { // look for lowest oid
                        dd = d;
                        j = oid;
                    }
                }
                ruleInfo[RULE_OID] = oid;

                // non-sticky
                tid = -1;
            }
            else if (ruleInfo[RULE_PID] == TYPE_WEIGHTED) { // load balance
                long d, dd = threshold[LOAD_MEDIUM];
                int[] ids = (int[]) groupList.get((int) ruleInfo[RULE_GID]);
                oid = -1;
                if (sessionTimeout > 0) for (int j : ids) { // look for min load
                    outInfo = info[j];
                    if (outInfo[OUT_STATUS] == NODE_STANDBY ||
                        outInfo[OUT_QSTATUS] != 0)//disabled or failed on report
                        continue;
                    d = outInfo[OUT_QDEPTH] - outInfo[OUT_DEQ] +
                        outInfo[OUT_SIZE] + outInfo[OUT_EXTRA] +
                        out[j].getCount();
                    if (d < dd) {
                        oid = j;
                        dd = d;
                    }
                }
                if (oid < 0) // not found so use the default
                    oid = (int) ruleInfo[RULE_OID];
                ruleInfo[RULE_OID] = oid;

                // non-sticky
                tid = -1;
            }
            else if (ruleInfo[RULE_PID] == TYPE_NONE) { // sticky
                String key;
                TextSubstitution sub = null;
                Template template = (Template) rule.get("KeyTemplate");
                sub = (TextSubstitution) rule.get("KeySubstitution"); 
                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (key != null) // got the key
                    oid = hashChain[(int) ruleInfo[RULE_GID]].map(key);
                else // default outLink
                    oid = (int) ruleInfo[RULE_OID];

                if (sessionTimeout > 0) { // check redirect
                    outInfo = info[oid];
                    if ((outInfo[OUT_STATUS] == NODE_STANDBY ||
                        outInfo[OUT_QSTATUS] != 0) && outInfo[OUT_QIPPS] >= 0)
                        // redirect temporarily
                        oid = (int) outInfo[OUT_QIPPS];
                }

                // save sticky oid to tid
                tid = oid;
            }
            else { // preferred
                oid = (int) ruleInfo[RULE_OID];
                if (sessionTimeout > 0) { // check redirect
                    outInfo = info[oid];
                    if ((outInfo[OUT_STATUS] == NODE_STANDBY ||
                        outInfo[OUT_QSTATUS] != 0) && outInfo[OUT_QIPPS] >= 0)
                        // redirect temporarily
                        oid = (int) outInfo[OUT_QIPPS];
                }

                // non-sticky
                tid = -1;
            }
            outInfo = info[oid];

            // make sure the outLink is not in STANDBY mode, otherwise putback
            // and pause for a while on retries
            if (outInfo[OUT_STATUS] == NODE_STANDBY ||
                outInfo[OUT_QSTATUS] != 0) { // in standby or failed on report
                i = in.putback(cid);
                st = heartbeat - (currentTime - previousTime);
                if (retryCount++ < 2)
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " has high load on " + assetList.getKey(oid) + " " +
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_QSTATUS] + ":" + outInfo[OUT_NRULE] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " " + st).send();
                else if ((retryCount % 10) == 0)
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " has high load on " + assetList.getKey(oid) + " " +
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_QSTATUS] + ":" + outInfo[OUT_NRULE] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " on " + retryCount +" retries").send();
                else if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + ": " + ruleName +
                        " has high load on " + assetList.getKey(oid) + " " +
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_QSTATUS] + ":" + outInfo[OUT_NRULE] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " on " + retryCount +" retries").send();

                while (st > 0) { // sleep for a while to retry
                    mask = in.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // disabled temporarily
                        break;
                    st -= waitTime;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (InterruptedException e) {
                    }
                }
                continue;
            }
            else if (sessionTimeout > 0 && threshold[LOAD_MEDIUM] <=
                outInfo[OUT_QDEPTH] - outInfo[OUT_DEQ] + outInfo[OUT_SIZE] +
                outInfo[OUT_EXTRA] + out[oid].getCount()) {// high session count
                i = in.putback(cid);
                st = heartbeat - (currentTime - previousTime);
                if (retryCount++ < 2)
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " has high session count on "+assetList.getKey(oid)+" "+
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_NRULE] + ":" + threshold[LOAD_MEDIUM] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " " + st).send();
                else if ((retryCount % 10) == 0)
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " has high session count on "+assetList.getKey(oid)+" "+
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_NRULE] + ":" + threshold[LOAD_MEDIUM] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " on " + retryCount +" retries").send();
                else if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + ": " + ruleName +
                        " has high session count on "+assetList.getKey(oid)+" "+
                        oid + "/" + rid + ": " + outInfo[OUT_STATUS] + " " +
                        outInfo[OUT_NRULE] + ":" + threshold[LOAD_MEDIUM] +" "+
                        outInfo[OUT_QDEPTH] + "," + outInfo[OUT_DEQ] + "," +
                        outInfo[OUT_SIZE] + " " + outInfo[OUT_EXTRA] + " " +
                        out[oid].size() + "," + out[oid].getCount() + ": " +
                        cid + "/" + i + " on " + retryCount +" retries").send();

                while (st > 0) { // sleep for a while to retry
                    mask = in.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // disabled temporarily
                        break;
                    st -= waitTime;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (InterruptedException e) {
                    }
                }
                continue;
            }
            retryCount = 0;

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " i=" + i).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " +ruleName+ " dispatched msg "+
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
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

            i = passthru(currentTime, inMessage, in, rid, oid, cid, tid);
            if (i > 0) {
                count ++;
                ruleInfo[RULE_PEND] ++;
            }
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * It queries the report for each destination and checks the report status
     * and queue depth. It then updates the output info and migrates the
     * messages and rulesets from a high-load destination to a low-load
     * destination according to the preconfigured rules. It will not manipulate
     * the status of the destinations, except for calling reset for enq number
     * and updating the metadata on them for internal uses.
     *<br/><br/>
     * It supports both failover and failback.
     */
    private int update(long currentTime, XQueue in) {
        int i, m, n, d, md, min, ns, ms, l, s, mydebug = debug, total = 0;
        int[] ids;
        StringBuffer strBuf = null;
        long st, preDepth = 0;
        long[] outInfo, ruleInfo, meta;
        Object o;
        Object[] asset;
        Browser browser;
        XQueue xq;
        Map r;
        String reportName;
        if ((mydebug & DEBUG_UPDT) > 0 || (mydebug & DEBUG_REPT) > 0)
            strBuf = new StringBuffer();

        browser = groupList.browser();
        while ((i = browser.next()) >= 0) { // go thru all groups of outlinks
          ids = (int[]) groupList.get(i);
          md = threshold[LOAD_HIGH] + 600000;
          min = -1;
          l = 0;
          for (int oid : ids) { // check report and update load
            asset = (Object[]) assetList.get(oid);
            if (asset == null || asset.length <= ASSET_URI) {
                new Event(Event.ERR, name + ": empty asset for " + oid).send();
                continue;
            }
            reportName = (String) asset[ASSET_URI];
            if (reportName == null) { // no report defined
                new Event(Event.WARNING, name + ": report name is null for " +
                    oid).send();
                continue;
            }
            r = (Map) NodeUtils.getReport(reportName);
            if (r == null || (o = r.get("TestTime")) == null) {
                new Event(Event.WARNING, name + ": no report found for " +
                    reportName).send();
                continue;
            }
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_MODE] = 0; // mark the report not updated yet
            try {
                xq = (XQueue) asset[ASSET_XQ];
                st = Long.parseLong((String) o);
                if (st > outInfo[OUT_QTIME]) { // report updated recently
                    l ++;
                    outInfo[OUT_MODE] = 1; // report updated
                    preDepth = (int) outInfo[OUT_QDEPTH];
                    outInfo[OUT_QTIME] = st;
                    if ((o = r.get("Status")) != null)
                        s = Integer.parseInt((String) o);
                    else // set the default
                        s = 1;
                    outInfo[OUT_QSTATUS] = s;
                    // add current enq to OUT_EXTRA
                    outInfo[OUT_EXTRA] += xq.reset();
                    if ((o = r.get("CurrentDepth")) != null) { // queue
                        if ("0".equals((String) r.get("Status")) && s >= 0) {
                            outInfo[OUT_QDEPTH] = Long.parseLong((String) o);
                            // estimate the deq
                            outInfo[OUT_DEQ] = preDepth + outInfo[OUT_EXTRA] -
                                outInfo[OUT_QDEPTH];
                        }
                    }
                    else if (s == 0) { // report has no currentDepth
                        outInfo[OUT_QDEPTH] = preDepth;
                        // estimate the deq
                        outInfo[OUT_DEQ] = outInfo[OUT_EXTRA];
                    }
                    outInfo[OUT_EXTRA] = 0;
                    if (outInfo[OUT_DEQ] < 0)
                        outInfo[OUT_DEQ] = 0;
                }
                else if ("static".equals((String) r.get("ReportType")) &&
                    sessionTimeout > 0 && // for static report
                    currentTime - outInfo[OUT_QTIME] >= sessionTimeout) {
                    outInfo[OUT_QTIME] = currentTime;
                    // add current enq to OUT_EXTRA
                    outInfo[OUT_EXTRA] += xq.reset();
                    // assume depth is same as the previous value 
                    outInfo[OUT_QDEPTH] = preDepth;
                    // estimate the deq
                    outInfo[OUT_DEQ] = outInfo[OUT_EXTRA];
                    outInfo[OUT_EXTRA] = 0;
                    if (outInfo[OUT_DEQ] < 0)
                        outInfo[OUT_DEQ] = 0;
                    outInfo[OUT_MODE] = 1; // report updated
                    l ++;
                }
                else { // session not due yet so update enq only
                    // add current enq to OUT_EXTRA
                    outInfo[OUT_EXTRA] += xq.reset();
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to retrieve report data"+
                    " from "+ reportName +": "+ Event.traceStack(e)).send();
            }
            if ((mydebug & DEBUG_REPT) > 0 && outInfo[OUT_MODE] > 0)
                strBuf.append("\n\t" + reportName + ": " + oid + " " +
                    outInfo[OUT_QSTATUS] + " " + outInfo[OUT_QDEPTH] + " " +
                    outInfo[OUT_EXTRA] + " " + outInfo[OUT_DEQ] + " " +
                    outInfo[OUT_SIZE] + " " + outInfo[OUT_QIPPS] + " " +
                    outInfo[OUT_STATUS] + " " +
                    Event.dateFormat(new Date(outInfo[OUT_QTIME])));
            // looking for min with the minimum load
            if (md > outInfo[OUT_QDEPTH] && outInfo[OUT_QSTATUS] == 0) {
                md = (int) outInfo[OUT_QDEPTH];
                min = oid;
            }
          }
          if ((mydebug & DEBUG_REPT) > 0) {
            if (l > 0) // got new report
                new Event(Event.DEBUG, name+" ReportName: OID QStatus "+
                    "QDepth ENQ DEQ Size QRedirect Status QTime (gid=" + i +
                    ",min=" + min + ")" + strBuf.toString()).send();
            strBuf = new StringBuffer();
          }

          if (l > 0 && sessionTimeout > 0) { // check report time on 1st oid
            outInfo = assetList.getMetaData(ids[0]);
            if (outInfo[OUT_MODE] > 0) { // reset RULE_PEND on all rules
                int j;
                Browser b = ruleList.browser();
                while ((j = b.next()) >= 0) {
                    ruleInfo = ruleList.getMetaData(j);
                    if (ruleInfo[RULE_GID] == i)
                        ruleInfo[RULE_PEND] = 0;
                }
            }
          }

          m = 0;
          for (int oid : ids) { // check load on each outLink
            asset = (Object[]) assetList.get(oid);
            if (asset == null || asset.length <= ASSET_URI)
                continue;
            xq = (XQueue) asset[ASSET_XQ];
            outInfo = assetList.getMetaData(oid);
            d = (outInfo[OUT_QSTATUS] == 0) ? (int) outInfo[OUT_QDEPTH] : -1;
            // current number of rulesets on the outLink of oid
            ns = (int) outInfo[OUT_NRULE];
            // previous number of rulesets on the outLink of oid
            ms = (int) outInfo[OUT_ORULE];
            if (d < 0 || d >= threshold[LOAD_MEDIUM]) {
                // failed report or high load
                if (outInfo[OUT_STATUS] == NODE_RUNNING) { // pause
                    outInfo[OUT_STATUS] = NODE_PAUSE;
                    outInfo[OUT_TIME] = currentTime;
                }
                else if (outInfo[OUT_STATUS] == NODE_PAUSE && // standby
                    currentTime - outInfo[OUT_TIME] >= sessionTimeout) {
                    outInfo[OUT_STATUS] = NODE_STANDBY;
                    outInfo[OUT_TIME] = currentTime;
                    meta = groupList.getMetaData(i);
                    if (oid != min && md < threshold[LOAD_LOW]) {
                        // migrate to min
                        migrate(currentTime, oid, min, in);
                        outInfo[OUT_ORULE] = ns;
                        // reset enq count
                        xq.reset();
                        outInfo[OUT_EXTRA] = 0;
                        m ++;
                    }
                    else // mark for retry
                        outInfo[OUT_QIPPS] = -1;
                }
                else if (outInfo[OUT_STATUS] == NODE_STANDBY) { // retry
                    if (outInfo[OUT_QIPPS] < 0) { // not migrated yet
                        if (oid != min && md < threshold[LOAD_LOW]) {
                            migrate(currentTime, oid, min, in);
                            outInfo[OUT_ORULE] = ns;
                            // reset enq count
                            xq.reset();
                            outInfo[OUT_EXTRA] = 0;
                            m ++;
                        }
                    }
                    else if (outInfo[OUT_SIZE] > 0) { // retry on leftover
                        int k = (int) outInfo[OUT_QIPPS];
                        long[] info = assetList.getMetaData(k);
                        if (info[OUT_QSTATUS] == 0 &&
                            info[OUT_QDEPTH] < threshold[LOAD_MEDIUM]) {
                            migrate(currentTime, oid, k, in);
                            // reset enq count
                            xq.reset();
                            outInfo[OUT_EXTRA] = 0;
                            m ++;
                        }
                    }
                }
            }
            else if (d < threshold[LOAD_LOW]) { // low load
                if (outInfo[OUT_QIPPS] >= 0) { // failback
                    outInfo[OUT_STATUS] = NODE_PAUSE;
                    outInfo[OUT_TIME] = currentTime;
                    migrate(currentTime, -1, oid, in);
                    // reset previous number of rulesets
                    outInfo[OUT_ORULE] = -1;
                    m ++;
                }
                else if (ms < 0) { // pause in failback
                    if (outInfo[OUT_STATUS] == NODE_PAUSE &&
                        currentTime - outInfo[OUT_TIME] >= sessionTimeout) {
                        outInfo[OUT_STATUS] = NODE_RUNNING;
                        outInfo[OUT_TIME] = currentTime;
                        outInfo[OUT_ORULE] = ns;
                    }
                }
                else if (outInfo[OUT_STATUS] == NODE_PAUSE ||
                    outInfo[OUT_STATUS] == NODE_STANDBY) { // running
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                    outInfo[OUT_TIME] = currentTime;
                }
            }
            else { // medium load
                if (outInfo[OUT_STATUS] != NODE_PAUSE) {
                    outInfo[OUT_STATUS] = NODE_PAUSE;
                    outInfo[OUT_TIME] = currentTime;
                }
            }

            if ((mydebug & DEBUG_UPDT) > 0 && outInfo[OUT_TIME] == currentTime)
                strBuf.append("\n\t" + assetList.getKey(oid) + ": " + oid +" "+
                    outInfo[OUT_STATUS] + " " + outInfo[OUT_QSTATUS] + " " +
                    outInfo[OUT_QIPPS] + " " + outInfo[OUT_NRULE] + " " +
                    outInfo[OUT_SIZE] + " " + outInfo[OUT_QDEPTH] + " " +
                    Event.dateFormat(new Date(outInfo[OUT_TIME])));
          }
          if (m > 0) { // summarize migration changes on outlinks
            strBuf = new StringBuffer();
            for (int oid : ids) {
                outInfo = assetList.getMetaData(oid);
                strBuf.append("\n\t" + assetList.getKey(oid) + ": " + oid +" "+
                    outInfo[OUT_STATUS] + " " + outInfo[OUT_QSTATUS] + " " +
                    outInfo[OUT_QIPPS] + " " + outInfo[OUT_NRULE] + " " +
                    outInfo[OUT_SIZE] + " " + outInfo[OUT_QDEPTH] + " " +
                    Event.dateFormat(new Date(outInfo[OUT_TIME])));
            }
            new Event(Event.INFO, name + " migration changes on Out for " + i +
                ": OID Status QStatus QRedirect NRule Size QDepth Time - In: "+
                in.getGlobalMask() + " " + in.size() + "," + in.depth() +
                strBuf.toString()).send();
          }
          else if ((mydebug & DEBUG_UPDT) > 0 && strBuf.length() > 0) {
            new Event(Event.DEBUG, name + " state changes on Out for " + i +
                ": OID Status QStatus QRedirect NRule Size QDepth Time - In: "+
                in.getGlobalMask() + " " + in.size() + "," + in.depth() +
                strBuf.toString()).send();
          }
          total += m;
        }
        return total;
    }

    /**
     * It migrates the messages and preferred rulesets from a destination to
     * another destination and updates the outInfo and ruleInfo as well as the
     * redirects for each outlink. Upon success, it returns number of messages
     * migrated.
     *<br/><br/>
     * In case of from = -1, it migrates all the pending messages whose PID
     * (preferred oid) is same as to.  Otherwise, it migrates all messages in
     * from to their failover outlink.
     */
    private int migrate(long currentTime, int from, int to, XQueue in) {
        int i, j, n, rid, id, mid, ns, oid, l=0;
        Object[] asset;
        XQueue xq, out;
        Browser browser;
        StringBuffer sf, strBuf = null;
        long[] outInfo, ruleInfo, state;
        int[] list;
        Message msg;
        if (from == to)
            return 0;

        // update preferred rules first
        strBuf = new StringBuffer();
        sf = new StringBuffer();
        n = 0;
        ns = 0;
        outInfo = assetList.getMetaData(to);
        if (from < 0) { // failback to preferred oid
            browser = ruleList.browser();
            while ((rid = browser.next()) >= 0) { // swap OID first
                ruleInfo = ruleList.getMetaData(rid);
                if (ruleInfo[RULE_PID] != to) // not preferred on to
                    continue;
                oid = (int) ruleInfo[RULE_OID];
                state = assetList.getMetaData(oid);
                state[OUT_NRULE] --;
                outInfo[OUT_NRULE] ++;
                ruleInfo[RULE_OID] = to;
                ruleInfo[RULE_TIME] = currentTime;
                if (strBuf.length() > 0)
                    strBuf.append(",");
                strBuf.append(rid);
                ns ++;
                sf.append("\n\t" + ruleList.getKey(rid) + ": " + rid + " " +
                    ruleInfo[RULE_OID] + " " + ruleInfo[RULE_PID] + " " +
                    ruleInfo[RULE_SIZE] + " " +
                    Event.dateFormat(new Date(ruleInfo[RULE_TIME])));
            }
            n = msgList.size();
            if (n <= 0) // disable redirect
                outInfo[OUT_QIPPS] = -1;
        }
        else { // failover
            state = assetList.getMetaData(from);
            browser = ruleList.browser();
            while ((rid = browser.next()) >= 0) { // locate OID
                ruleInfo = ruleList.getMetaData(rid);
                if (ruleInfo[RULE_PID] < 0) // non-preferred
                    continue;
                if (ruleInfo[RULE_OID] != from) // not preferred on from
                    continue;

                state[OUT_NRULE] --;
                outInfo[OUT_NRULE] ++;
                ruleInfo[RULE_OID] = to;
                ruleInfo[RULE_TIME] = currentTime;

                if (strBuf.length() > 0)
                    strBuf.append(",");
                strBuf.append(rid);
                ns ++;
                sf.append("\n\t" + ruleList.getKey(rid) + ": " + rid + " " +
                    ruleInfo[RULE_OID] + " " + ruleInfo[RULE_PID] + " " +
                    ruleInfo[RULE_SIZE] + " " +
                    Event.dateFormat(new Date(ruleInfo[RULE_TIME])));
            }
            outInfo[OUT_QIPPS] = -1;
            n = (int) state[OUT_SIZE];
            if (n <= 0) { // enable redirect
                state[OUT_QIPPS] = to;
                browser = assetList.browser();
                while ((oid = browser.next()) >= 0) { // update redirects
                    state = assetList.getMetaData(oid);
                    if (state[OUT_QIPPS] == from)
                        state[OUT_QIPPS] = to;
                }
            }
        }
        if (n <= 0) { // no message needs to be checked for migrations
            // log the current state after migration
            strBuf.insert(0, name + ": Migration: " + 0 + " msgs in " + ns +
                " rulesets (");
            strBuf.append(") switched from " + from + " to " + to);
            if (sf.length() > 0)
                strBuf.append("\n\t Rule: RID OID PID Size Time"+sf.toString());
            strBuf.append("\n\t In: Status Type Size Depth: "+in.getGlobalMask()
                + " "+ 1 +" "+in.size() + " " + in.depth());
            new Event(Event.INFO, strBuf.toString()).send();
            return 0;
        }

        // migrate messages
        n = msgList.size();
        list = new int[n];
        n = msgList.queryIDs(list);
        l = 0;
        asset = (Object[]) assetList.get(to);
        out = (XQueue) asset[ASSET_XQ];
        if (from < 0) { // failback
            long[] info;
            for (i=0; i<n; i++) {
                mid = list[i];
                state = msgList.getMetaData(mid);
                oid = (int) state[MSG_OID];
                if (oid == to) // no need to migrate, but it may be a leftover
                    continue;
                rid = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(rid);
                // only migrate msgs for preferred or sticky
                if (ruleInfo[RULE_PID] != TYPE_NONE && ruleInfo[RULE_PID] != to)
                    continue;
                if (ruleInfo[RULE_PID] == TYPE_NONE && state[MSG_TID] != to)
                    continue;
                asset = (Object[]) assetList.get(oid);
                xq = (XQueue) asset[ASSET_XQ];
                id = (int) state[MSG_BID];
                msg = (Message) xq.takeback(id);
                if (msg == null) // msg taken already so skip it
                    continue;
                info = assetList.getMetaData(oid);
                j = passthru(currentTime, msg, in, out, rid, to, mid,
                    state, outInfo);
                if (j > 0) { // migrated
                    l ++;
                    info[OUT_SIZE] --;
                    if ((debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + " migrate: " + rid + " " +
                            mid + "/" + state[MSG_CID] + " " + oid + ":" + id +
                            " " + ruleInfo[RULE_SIZE] + " " + outInfo[OUT_SIZE]+
                            " "+ xq.size() + ":" + xq.depth() + " - " + to +
                            ":" + msgList.size()).send();
                }
                else { // failed
                    j = xq.reserve(-1L, id);
                    if (j == id)
                        xq.add(msg, id);
                    new Event(Event.WARNING, name +
                        ": failed to migrate msg at " + mid + "/" +
                        state[MSG_CID] + " " + oid + ":" + id + "?" + j + " " +
                        ruleInfo[RULE_SIZE] + " " + outInfo[OUT_SIZE] + " " +
                        xq.size() + ":" + xq.depth() + " - " + to + ":" +
                        msgList.size()).send();
                }
            }
            // disable redirect
            outInfo[OUT_QIPPS] = -1;
        }
        else { // failover
            long[] info;
            info = assetList.getMetaData(from);
            asset = (Object[]) assetList.get(from);
            xq = (XQueue) asset[ASSET_XQ];
            for (i=0; i<n; i++) { // look for msgs to migrate
                mid = list[i];
                state = msgList.getMetaData(mid);
                if (state[MSG_OID] != from)
                    continue;
                id = (int) state[MSG_BID];
                msg = (Message) xq.takeback(id);
                if (msg == null) // msg taken already so skip it
                    continue;
                rid = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(rid);
                j = passthru(currentTime, msg, in, out, rid, to, mid,
                    state, outInfo);
                if (j > 0) { // migrated
                    l ++;
                    info[OUT_SIZE] --;
                    if ((debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + " migrate: " + rid + " " +
                            mid + "/" + state[MSG_CID] + " " + from + ":" + id +
                            " "+ ruleInfo[RULE_SIZE] + " " + outInfo[OUT_SIZE] +
                            " "+ xq.size() + ":" + xq.depth() + " - " + to +
                            ":" + msgList.size()).send();
                }
                else { // failed
                    j = xq.reserve(-1L, id);
                    if (j == id)
                        xq.add(msg, id);
                    new Event(Event.WARNING, name +
                        ": failed to migrate msg at " + mid + "/" +
                        state[MSG_CID] + " " + from + ":" + id + "?" + j + " "+
                        ruleInfo[RULE_SIZE] + " " + outInfo[OUT_SIZE] + " " +
                        xq.size() + ":" + xq.depth() + " - " + to + ":" +
                        msgList.size()).send();
                }
            }
            // disable redirect
            outInfo[OUT_QIPPS] = -1;
            info[OUT_QIPPS] = to;
            browser = assetList.browser();
            while ((oid = browser.next()) >= 0) { // update redirects
                info = assetList.getMetaData(oid);
                if (info[OUT_QIPPS] == from)
                    info[OUT_QIPPS] = to;
            }
        }

        // log the current state after migration
        strBuf.insert(0, name + ": Migration: " + l + " msgs in " + ns +
            " rulesets (");
        strBuf.append(") switched from " + from + " to " + to);
        if (sf.length() > 0)
            strBuf.append("\n\t Rule: RID OID PID Size Time" + sf.toString());
        strBuf.append("\n\t In: Status Type Size Depth: " + in.getGlobalMask() +
            " " + 1 + " " + in.size() + " " + in.depth());
        new Event(Event.INFO, strBuf.toString()).send();

        return l;
    }

    /**
     * passes the message to the output XQ and updates the info
     */
    private int passthru(long currentTime, Message msg, XQueue in, XQueue out,
        int rid, int oid, int cid, long[] state, long[] outInfo) {
        long[] ruleInfo;
        int id = -1, k, mid, mask, len, shift, outCapacity;
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
            if (mid >= 0) { // id-th cell has just been empty now, replace it
                long[] info = msgList.getMetaData(mid);
                cells.collect(-1L, mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                k = (int) info[MSG_RID];
                ruleInfo = ruleList.getMetaData(k);
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name+" passback: " + k + " " + mid +
                        ":" + state[MSG_CID] + " "+key+" "+ruleInfo[RULE_SIZE]+
                        " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                        out.depth() + " " + msgList.size()).send();
            }
            k = out.add(msg, id, cbw);
            msgList.replaceKey(cid, key);
            state[MSG_BID] = id;
            state[MSG_OID] = oid;
            outInfo[OUT_SIZE] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = currentTime;
        }
        else { // reservation failed
            k = in.putback(cid);
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                assetList.getKey(oid) + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k).send();
            return 0;
        }
        return 1;
    }

    /** overwrites the method of parent due to the mask stored in OUT_STATUS */
    public int getMetaData(int type, int id, long[] data) {
        Object[] asset;
        XQueue xq;
        long[] state;
        int[] list;
        int i, k, n;
        if (data == null)
            return -1;
        n = data.length;
        switch (type) {
          case META_MSG:
            i = msgList.size();
            if (id < 0 || id >= i || n <= MSG_TIME)
                return -1;
            list = new int[i];
            i = msgList.queryIDs(list);
            if (id >= i)
                return -1;
            state = msgList.getMetaData(list[id]);
            if (state == null || state.length <= MSG_TIME)
                return -1;
            k = list[id];
            n = MSG_TIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_OUT:
            i = assetList.size();
            if (id < 0 || id >= i || n <= OUT_QTIME)
                return -1;
            list = new int[i];
            i = assetList.queryIDs(list);
            if (id >= i)
                return -1;
            state = assetList.getMetaData(list[id]);
            if (state == null || state.length <= OUT_QTIME)
                return -1;
            k = list[id];
            n = OUT_QTIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_RULE:
            i = ruleList.size();
            if (id < 0 || id >= i || n <= RULE_TIME)
                return -1;
            list = new int[i];
            i = ruleList.queryIDs(list);
            if (id >= i)
                return -1;
            state = ruleList.getMetaData(list[id]);
            if (state == null || state.length <= RULE_TIME)
                return -1;
            k = list[id];
            n = RULE_TIME + 1;
            for (i=0; i<n; i++)
                data[i] = state[i];
            break;
          case META_XQ:
            i = assetList.size();
            if (id < 0 || id >= i || n < 6)
                return -1;
            list = new int[i];
            i = assetList.queryIDs(list);
            if (id >= i)
                return -1;
            asset = (Object[]) assetList.get(list[id]);
            if (asset == null || asset.length <= ASSET_XQ)
                return -1;
            xq = (XQueue) asset[ASSET_XQ];
            if (xq == null)
                return -1;
            k = list[id];
            n = 6;
            data[0] = xq.depth();
            data[1] = xq.size();
            data[2] = xq.getCount();
            data[3] = xq.getMTime();
            data[4] = xq.getCapacity();
            data[5] = xq.getGlobalMask();
            break;
          default:
            n = 0;
            k = -1;
            break;
        }
        return k;
    }

    public void close() {
        super.close();
        groupList.clear();
        if (hashChain != null) {
            for (HashChain hc : hashChain)
                hc.clear();
        }
    }

    protected void finalize() {
        close();
    }
}
