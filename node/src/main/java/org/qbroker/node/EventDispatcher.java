package org.qbroker.node;

/* EventDispatcher.java - a MessageNode dispaching events */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.CachedList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.MessageMailer;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.EventSelector;
import org.qbroker.event.EventActionGroup;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * EventDispatcher dispatches the incoming events to multiple destinations
 * according to their content and the registered subscriptions.  It supports
 * generic JMS messages.
 *<br/><br/>
 * EventDispatcher contains a number of subscriptions which define a filter
 * and a set of actions. Internally, EventDispatcher also creates a topic
 * pattern based on the filter for each subscription. The topic pattern will
 * be used as a pre-filter on the topic of incoming events. For each incoming
 * event, EventDispatcher extracts the topic first. Then it looks up the topic
 * cache for a list of candidate subscriptions on the topic. If the list is
 * empty, there is no subscibers for the event. The incoming event will be
 * routed to the outlink of nohit. Otherwise, EventDispatcher just loops
 * through the list and applies the filter of each candidates on the event.
 * If it is a match and the subscription is active, EventDispatcher will create
 * an ObjectEvent with the event and actions attached. The new message
 * will be sent to the outlink of pool as a request to invoke all the actions
 * of the subscription on the incoming event. At least one instance of
 * EventPersister is supposed to be linked to the outlink of pool. This
 * operation continues until all candidates in the list are checked. If there
 * is no request event generated, the original incoming event will be routed
 * to the outlink of nohit. Otherwise, it will be routed to the outlink of
 * bypass. Due to the MT-Safty, any messages routed to the outlink of bypass
 * should not be modified downstream. Otherwise, EventPersister may throw
 * exceptions in delivery process. If those messages need to be modified,
 * please work on their copy via DuplicateNode and leave the original messages
 * alone. In case of any failure, the original incoming event will be sent to
 * the outlink of failure.
 *<br/><br/>
 * A subscription is defined in a ruleset.  Each ruleset contains a unique
 * name of the subscription, an EventSelector to match events, a TimeWindows
 * to define active time of the subscription and a group of EventActions for
 * the subscription.
 *<br/><br/>
 * EventDispatcher always creates two extra rulesets.  The first is the
 * nohit ruleset for those events not hitting any matches.  The second is the
 * candidate ruleset for all events matching to at least one of subscriptions. 
 * Since a candidate event may hit number of subscriptions and generates a
 * task event for every hit subscriptions, the stats count for the candidate
 * ruleset will be the number of the incoming events rather than the
 * number of the task events.  The number of task events will be counted
 * by their own rulesets.  The DisplayMask and StringProperty of the ruleset
 * are used to display the details of dispatched event for the ruleset.
 * The stats of the rules are stored in the fields of RULE_SIZE, RULE_COUNT
 * and RULE_PEND.  RULE_PEND is for number of cached topics, RULE_SIZE for
 * number of preliminary hits, whereas RULE_COUNT is for number of real hits.
 *<br/><br/>
 * SessionTimeout determines how often to clean up expired topics from the
 * cache.  If SessionTimeout is larger than zero, any cached topics will be
 * expired if their idle time exceeds SessionTimeout.  Those expired topics
 * will be removed from the cache in next session to save resources.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * EventDispatcher always assumes the first outlink for the out group,
 * the second for the bypass group, the third for the failure group and
 * the last is for the nohit group.  Any two or more outlinks can share
 * the same outlink name.  It means these outlinks are sharing the same
 * output channel.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventDispatcher extends Node {
    protected int sessionTimeout = 86400000;

    // template and substitution for topics
    private Template topicTemplate = null;

    private Perl5Compiler pc = null;
    private int[] outLinkMap;
    private long[] ri;

    private final static int NOHIT_RULE = 0;
    private final static int CANDIDATE_RULE = 1;
    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;

    public EventDispatcher(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        Map ph;
        String key, str;
        long[] outInfo, ruleInfo;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "dispatch";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        if ((o = props.get("SessionTimeout")) != null) {
            sessionTimeout = 1000 * Integer.parseInt((String) o);
            if (sessionTimeout < 0)
                sessionTimeout = 86400000;
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
                " failed to init OutLinks"));
        if (overlap[0] < BYPASS_OUT || overlap[1] < BYPASS_OUT)
            throw(new IllegalArgumentException(name + ": bad overlap outlink "+
                overlap[0] + " or " + overlap[1]));

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

        try { // init perl compiler and matcher
            pc = new Perl5Compiler();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init Perl compiler or matcher: " + e.toString()));
        }

        cfgList = new AssetList(name, 64);
        msgList = new AssetList(name, capacity);
        ruleList = new CachedList(name, ruleSize, QuickCache.META_ATAC, 0, 0);
        key = "##priority## ##site## ##category## ##hostname## ##name##";
        topicTemplate = new Template(key);
        cells = new CollectibleCells(name, capacity);

        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();
        n = list.size();

        try { // init rulesets
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
            rule.put("TopicPattern", "^$");
            o = pc.compile("^$");
            ((CachedList) ruleList).add(key, ruleInfo, (Pattern) o, rule);
            ruleInfo[RULE_GID] = NOHIT_RULE;
            outInfo = assetList.getMetaData(outLinkMap[NOHIT_OUT]);
            outInfo[OUT_NRULE] ++;
            outInfo[OUT_ORULE] ++;

            // for candidate
            key = "candidate";
            ruleInfo = new long[RULE_TIME + 1];
            for (i=0; i<=RULE_TIME; i++)
                ruleInfo[i] = 0;
            ruleInfo[RULE_STATUS] = NODE_RUNNING;
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_NONE;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("TopicPattern", "^$");
            ((CachedList) ruleList).add(key, ruleInfo, (Pattern) o, rule);
            ruleInfo[RULE_GID] = CANDIDATE_RULE;
            outInfo = assetList.getMetaData(outLinkMap[BYPASS_OUT]);
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
                    !("ConfigList".equals((String) o) ||
                    "ConfigTemplate".equals((String) o))) {
                    ruleInfo = new long[RULE_TIME+1];
                    rule = initRuleset(tm, ph, ruleInfo);
                    if(rule != null && (key=(String) rule.get("Name")) != null){
                        str = (String) rule.get("TopicPattern");
                        j = ((CachedList) ruleList).add(key, ruleInfo,
                            pc.compile(str), rule);
                        if (j > 0) // rule added
                            ruleInfo[RULE_GID] = j;
                        else
                            new Event(Event.ERR, name + ": ruleset " + i +
                                ", " + key + ", failed to be added").send();
                    }
                    else
                        new Event(Event.ERR, name + ": ruleset " + i +
                            " failed to be initialized").send();
                    continue;
                }
                else if ("ConfigTemplate".equals((String) o)) {
                    ConfigTemplate cfgTemp;
                    try {
                        cfgTemp = new ConfigTemplate(ph);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigTemplate " + i +
                            " failed to be initialized").send();
                        continue;
                    }
                    key = cfgTemp.getName();
                    k = cfgTemp.getSize();
                    m = cfgList.add(key, new long[]{k, 1}, cfgTemp);
                    if (m < 0) {
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to be added to the list").send();
                        cfgTemp.close();
                        continue;
                    }
                    for (int jj=0; jj<k; jj++) { // init all template rulesets
                        str = cfgTemp.getItem(jj);
                        ph = cfgTemp.getProps(str);
                        key = cfgTemp.getKey(jj);
                        ruleInfo = new long[RULE_TIME+1];
                        try {
                            rule = initRuleset(tm, ph, ruleInfo);
                        }
                        catch (Exception ex) {
                            new Event(Event.ERR, name + ": ConfigTemplate " +
                                cfgTemp.getName() +
                                " failed to init template rule " + key +
                                " at " + jj + ": "+Event.traceStack(ex)).send();
                            continue;
                        }
                        if (rule != null && rule.size() > 0) {
                            j = ((CachedList) ruleList).add(key, ruleInfo,
                                pc.compile((String) rule.get("TopicPattern")),
                                rule);
                            if (j > 0) // rule added
                                ruleInfo[RULE_GID] = j;
                            else
                                new Event(Event.ERR, name + ": ConfigTemplate "+
                                    cfgTemp.getName() +
                                    " failed to add template rule " + key +
                                    " at " + jj).send();
                        }
                        else
                            new Event(Event.ERR, name + ": ConfigTemplate " +
                                cfgTemp.getName() +
                                " failed to init template rule "+ key +
                                " at " + jj).send();
                    }
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
                for (int jj=0; jj<k; jj++) { // init all external rulesets
                    str = cfg.getItem(jj);
                    ph = cfg.getProps(str);
                    key = cfg.getKey(jj);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+" failed to init external rule "+key+
                            " at " + jj + ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.size() > 0) {
                        j = ((CachedList) ruleList).add(key, ruleInfo,
                            pc.compile((String)rule.get("TopicPattern")), rule);
                        if (j > 0) // rule added
                            ruleInfo[RULE_GID] = j;
                        else
                            new Event(Event.ERR, name + ": ConfigList " +
                                cfg.getName()+ " failed to add external rule " +
                                key + " at " + jj).send();
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+ " failed to init external rule "+
                            key + " at " + jj).send();
                }
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                o = ruleList.get(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_DMASK] + " " +
                    ruleInfo[RULE_TTL]/1000 + " " +
                    assetList.getKey((int) ruleInfo[RULE_OID]) + " / " +
                    (String) ((Map) o).get("TopicPattern"));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID DMASK TTL - " +
                "OutName / Pattern" + strBuf.toString()).send();
        }

        status = NODE_READY;
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
        String key, str, ruleName;
        String[] keys = topicTemplate.getSequence();
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

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        // displayMask of ruleset stored in dmask
        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);

        if ((o = ph.get("EventPattern")) != null && o instanceof List) {
            list = (List) o;
            str = "";
            for (i=0; i<keys.length; i++) { // build topic pattern
                key = EventSelector.coarseGrain(keys[i], list);
                if (i > 0)
                    str += " ";
                str += key.substring(1, key.length()-1);
            }
        }
        else
            str = ".*";

        rule.put("TopicPattern", "^" + str + "$");
        try {
            rule.put("Filter", new EventSelector(ph));
            rule.put("Action", new EventActionGroup(ph));
            rule.put("TimeWindows",
                new TimeWindows((Map) ph.get("ActiveTime")));
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to init " + ruleName +
                ": " + e.toString()).send();
            return null;
        }

        ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
        ruleInfo[RULE_PID] = TYPE_PUBLISH;
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // for StringProperty
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

        return rule;
    }

    /**
     * It adds a new rule to the ruleList and returns its id upon success.
     * It is not MT-Safe.
     */
    public int addRule(Map ph) {
        Object o;
        String key = null;
        Map rule = null;
        long[] ruleInfo = new long[RULE_TIME+1];
        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a rule"));
        if (ruleList == null || ruleList.size() >= ruleList.getCapacity())
            throw(new IllegalStateException(name + ": ruleList is full"));
        key = (String) ph.get("Name");
        if (key == null || key.length() <= 0 || ruleList.containsKey(key))
            return -1;
        if (getStatus() == NODE_RUNNING)
            throw(new IllegalStateException(name + " is in running state"));
        if ((o = ph.get("RuleType")) == null || !(o instanceof String) ||
            !("ConfigList".equals((String) o) ||
            "ConfigTemplate".equals((String) o))) {
            try {
                rule = initRuleset(System.currentTimeMillis(), ph, ruleInfo);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + " failed to init rule " + key +
                    ": " + Event.traceStack(ex)).send();
                return -1;
            }
            if (rule != null && rule.containsKey("Name")) {
                Pattern ps;
                try {
                    ps = pc.compile((String) rule.get("TopicPattern"));
                }
                catch (Exception ex) {
                    new Event(Event.ERR, name +
                        " failed to compile pattern for " + key +
                        ": " + Event.traceStack(ex)).send();
                    return -1;
                }
                int id = ((CachedList) ruleList).add(key, ruleInfo, ps, rule);
                if (id > 0) {
                    ruleInfo[RULE_GID] = id;
                    ruleInfo[RULE_PEND] =
                        ((CachedList) ruleList).getTopicCount(id);
                }
                else // failed to add the rule to the list
                    new Event(Event.ERR, name + " failed to add rule " +
                        key).send();
                return id;
            }
            else
                new Event(Event.ERR, name + " failed to init rule "+key).send();
        }
        else if (cfgList != null && "ConfigTemplate".equals((String) o)) {
            int i, k, m;
            ConfigTemplate cfgTemp;

            try {
                cfgTemp = new ConfigTemplate(ph);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + ": ConfigTemplate rule " + key +
                    " failed to be initialized").send();
                return -1;
            }
            k = cfgTemp.getSize();
            m = cfgList.add(key, new long[]{k, 1}, cfgTemp);
            if (m < 0) {
                new Event(Event.ERR, name + ": ConfigTemplate rule " + key +
                    " failed to be added to the list").send();
                cfgTemp.close();
            }
            else { // added
                String str;
                int id = -1;
                long tm = System.currentTimeMillis();
                for (i=0; i<k; i++) { // init all template rulesets
                    ph = cfgTemp.getProps(cfgTemp.getItem(i));
                    str = cfgTemp.getKey(i);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to init template rule " + str +
                            " at " + i +  ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.containsKey("Name")) {
                        Pattern ps;
                        try {
                            ps = pc.compile((String) rule.get("TopicPattern"));
                        }
                        catch (Exception ex) {
                            new Event(Event.ERR, name + ": ConfigTemplate " +
                                key + " failed to compile pattern for " +
                                str + " at " + i +  ": " +
                                Event.traceStack(ex)).send();
                            continue;
                        }
                        id = ((CachedList) ruleList).add(str,ruleInfo,ps,rule);
                        if (id > 0) {
                            ruleInfo[RULE_GID] = id;
                            ruleInfo[RULE_PEND] =
                                ((CachedList) ruleList).getTopicCount(id);
                        }
                        else // failed to add the rule to the list
                            new Event(Event.ERR, name + ": ConfigTemplate " +
                                key + " failed to add template rule "+
                                str + " at " + i).send();
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigTemplate " + key +
                            " failed to init template rule "+ str +
                            " at " + i).send();
                }
                return id;
            }
        }
        else if (cfgList != null) { // for external rules via ConfigList
            int i, k, m;
            ConfigList cfg;
            try {
                cfg = new ConfigList(ph);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + ": ConfigList rule " + key +
                    " failed to be initialized").send();
                return -1;
            }
            cfg.setDataField(name);
            cfg.loadList();
            k = cfg.getSize();
            m = cfgList.add(key, new long[]{k, 0}, cfg);
            if (m < 0) {
                new Event(Event.ERR, name + ": ConfigList rule " + key +
                    " failed to be added to the list").send();
                cfg.close();
            }
            else { // added
                String str;
                int id = -1;
                long tm = System.currentTimeMillis();
                for (i=0; i<k; i++) { // init all external rulesets
                    ph = cfg.getProps(cfg.getItem(i));
                    str = cfg.getKey(i);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigList " + key +
                            " failed to init external rule " + str +
                            " at " + i +  ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.containsKey("Name")) {
                        Pattern ps;
                        try {
                            ps = pc.compile((String) rule.get("TopicPattern"));
                        }
                        catch (Exception ex) {
                            new Event(Event.ERR, name + ": ConfigList " + key +
                                " failed to compile pattern for " + str +
                                " at " + i +  ": " +
                                Event.traceStack(ex)).send();
                            continue;
                        }
                        id = ((CachedList) ruleList).add(str,ruleInfo,ps,rule);
                        if (id > 0) {
                            ruleInfo[RULE_GID] = id;
                            ruleInfo[RULE_PEND] =
                                ((CachedList) ruleList).getTopicCount(id);
                        }
                        else // failed to add the rule to the list
                            new Event(Event.ERR, name + ": ConfigList " + key +
                                " failed to add external rule "+ str +
                                " at " + i).send();
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigList " + key +
                            " failed to init external rule "+ str +
                            " at " + i).send();
                }
                return id;
            }
        }

        return -1;
    }

    /**
     * It removes the rule from the ruleList and returns the rule id upon
     * success. It is not MT-Safe. There is no RULE_SIZE checking.
     */
    public int removeRule(String key, XQueue in) {
        int id = ruleList.getID(key);
        if (id == 0) // can not remove the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            ruleList.remove(id);
        }
        else if (cfgList != null && (id = cfgList.getID(key)) >= 0) {
            id = super.removeRule(key, in);
        }

        return id;
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
            Map rule = (Map) ruleList.get(id);
            String tp = (String) rule.get("TopicPattern");
            long tm = System.currentTimeMillis();
            long[] meta = new long[RULE_TIME+1];
            try {
                rule = initRuleset(tm, ph, meta);
            }
            catch (Exception ex) {
                new Event(Event.ERR, name + " failed to init rule " + key +
                    ": " + Event.traceStack(ex)).send();
                return -1;
            }
            if (rule != null && rule.containsKey("Name")) {
                String str = (String) rule.get("TopicPattern");
                if (tp.equals(str)) { // same TopicPattern
                    StringBuffer strBuf = ((debug & DEBUG_DIFF) <= 0) ? null :
                        new StringBuffer();
                    long[] ruleInfo = ruleList.getMetaData(id);
                    ruleList.set(key, rule);
                    for (int i=0; i<RULE_TIME; i++) { // update metadata
                        switch (i) {
                          case RULE_GID:
                          case RULE_SIZE:
                          case RULE_PEND:
                          case RULE_COUNT:
                            break;
                          default:
                            ruleInfo[i] = meta[i];
                        }
                        strBuf.append(" " + ruleInfo[i]);
                    }
                    if ((debug & DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name + "/" + key + " ruleInfo:" +
                            strBuf).send();
                }
                else { // TopicPattern changed
                    Pattern ps;
                    try {
                        ps = pc.compile(str);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name +
                            " failed to compile pattern for " + key +
                            ": " + Event.traceStack(ex)).send();
                        return -1;
                    }
                    ruleList.remove(key);
                    id = ((CachedList) ruleList).add(key, meta, ps, rule, id);
                    if (id <= 0) { // failed to add the rule to the list
                        new Event(Event.ERR, name + " failed to replace rule " +
                            key).send();
                        return -1;
                    }
                    meta[RULE_GID] = id;
                    meta[RULE_PEND] = ((CachedList) ruleList).getTopicCount(id);
                    if ((debug & DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name + "/" + key +
                            ": topicPattern has been changed to " + str).send();
                }
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

    /** updates metadatat of all rules due to cleanups of all expired topics */
    private void updatePendingCount(long currentTime) {
        int rid;
        long[] ruleInfo;
        Browser browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) { // check every rule
            if (rid <= CANDIDATE_RULE) // skip nohit and candidate
                continue;
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_PEND] = ((CachedList) ruleList).getTopicCount(rid);
        }
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        Event event = null;
        ObjectEvent msg;
        Map rule = null;
        String msgStr = null, ruleName = null, topicStr = null;
        Object[] asset;
        CachedList subList;
        EventSelector[] filter = null;
        EventActionGroup[] action = null;
        TimeWindows[] tw = null;
        Browser browser, b;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, k, n, size;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
        int oid = 0; // the id of the output queue
        boolean isEvent = true, isNewTopic = false;
        boolean dspBody = false;
        byte[] buffer = new byte[bufferSize];
        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0))
            dspBody = true;

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
        ruleMap = new int[ruleList.getCapacity()];
        for (i=0; i<ruleMap.length; i++)
            ruleMap[i] = -1;
        n = ruleList.size();
        filter = new EventSelector[n];
        action = new EventActionGroup[n];
        tw = new TimeWindows[n];
        browser = ruleList.browser();
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filter[i] = (EventSelector) rule.get("Filter");
            action[i] = (EventActionGroup) rule.get("Action");
            tw[i] = (TimeWindows) rule.get("TimeWindows");
            ruleMap[rid] = i++;
        }
        ri = ruleList.getMetaData(CANDIDATE_RULE);

        // update assetList
        n = out.length;
        for (i=0; i<n; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

        subList = (CachedList) ruleList;
        previousTime = System.currentTimeMillis();
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
            if (sessionTimeout > 0 &&
                currentTime - previousTime >= sessionTimeout) {
                String[] keys = subList.disfragment(currentTime);
                previousTime = currentTime;
                if (keys != null && keys.length > 0) {
                    ri[RULE_PEND] -= keys.length;
                    ri[RULE_TIME] = currentTime;
                    updatePendingCount(currentTime);
                    new Event(Event.INFO, name + ": cleaned up " +
                        keys.length + " expired topics").send();
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

            event = null;
            if (inMessage instanceof TextEvent) {
                isEvent = true;
                event = (Event) inMessage;
            }
            else try { // copy the message into event
                isEvent = false;
                event = (Event) MessageUtils.duplicate(inMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to copy msg: " +
                    Event.traceStack(e)).send();
                oid = outLinkMap[FAILURE_OUT];
                rid = NOHIT_RULE;
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: cid="+cid+
                        " rid=" + rid + " oid=" + oid).send();
                count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
                feedback(in, -1L);
                sz = msgList.size();
                continue;
            }

            if (isEvent || event instanceof TextEvent) { // for TextEvent
                if (((TextEvent)event).getLogMode() != Event.LOG_JMS) try {
                    // not from toEvent, so set attribute of "text"
                    event.setPriority(event.getPriority());
                    event.setAttribute("text", ((TextEvent) event).getText());
                    ((TextEvent) event).setLogMode(Event.LOG_JMS);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " failed to reset event: " +
                        Event.traceStack(e)).send();
                    event.log();
                    oid = outLinkMap[FAILURE_OUT];
                    rid = NOHIT_RULE;
                    if ((debug & DEBUG_PROP) > 0)
                        new Event(Event.DEBUG, name + " propagate: cid="+cid+
                            " rid=" + rid + " oid=" + oid).send();
                    count += passthru(currentTime, inMessage, in,rid,oid,cid,0);
                    feedback(in, -1L);
                    sz = msgList.size();
                    continue;
                }
            }
            event.log();

            topicStr = EventUtils.format(event, topicTemplate);

            isNewTopic = false;
            b = null;
            if (topicStr == null || topicStr.length() <= 0) {
                i = FAILURE_OUT;
            }
            else if ((b = subList.browser(topicStr, currentTime)) != null) {
                i = RESULT_OUT;
            }
            else if (!subList.containsTopic(topicStr)) { // first appearance
                b = subList.insertTopic(topicStr, currentTime,
                    sessionTimeout, null);
                isNewTopic = true;
                ri[RULE_PEND] ++; // added a new candidate
                ri[RULE_TIME] = currentTime;
                i = RESULT_OUT;
            }
            else if (subList.isExpired(topicStr, currentTime)) { // expired
                b = subList.insertTopic(topicStr, currentTime,
                    sessionTimeout, null);
                i = RESULT_OUT;
            }
            else { // null browser
                subList.expire(topicStr, currentTime);
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": null browser for topic (" +
                    topicStr + ")").send();
            }

            size = 0;
            if (b == null || i == FAILURE_OUT) { // failed somehow 
                i = FAILURE_OUT;
                rid = NOHIT_RULE;
            }
            else if (isEvent) { // original is an event
                Map<String, Object> task;
                Event ev = null;
                while ((rid = b.next()) > CANDIDATE_RULE) { // reserve for hits
                    ruleInfo = ruleList.getMetaData(rid);
                    ruleInfo[RULE_SIZE] ++;
                    if (isNewTopic) // added a new cached topic
                        ruleInfo[RULE_PEND] ++;
                    k = ruleMap[rid];
                    if (k< 0 || !filter[k].evaluate(currentTime, event))
                        continue;
                    if (tw != null &&
                        tw[k].check(currentTime, 0L) != TimeWindows.NORMAL)
                        continue;
                    if (size == 0) try { // make a copy for actions 
                        ev = (Event) MessageUtils.duplicate(inMessage, buffer);
                        if (((TextEvent) ev).getLogMode() != Event.LOG_JMS) {
                            // not from toEvent, so set attribute of "text"
                            ev.setPriority(event.getPriority());
                            ev.setAttribute("text", ((TextEvent) ev).getText());
                            ((TextEvent) ev).setLogMode(Event.LOG_JMS);
                        }
                    }
                    catch (Exception e) {
                        ev = event;
                    }
                    msg = new ObjectEvent();
                    msg.setPriority(Event.INFO);
                    msg.setAttribute("name", ruleList.getKey(rid));
                    task = new HashMap<String, Object>();
                    task.put("Event", ev);
                    task.put("Action", action[k]);
                    msg.setBody(task);
                    size += flush(currentTime, cid, rid, msg, in, out[0],
                        0, topicStr);
                }
                if (size > 0) {
                    i = BYPASS_OUT;
                    rid = CANDIDATE_RULE;
                }
                else { // no subscribers
                    i = NOHIT_OUT;
                    rid = NOHIT_RULE;
                }
            }
            else { // original is not an event
                Map<String, Object> task;
                while ((rid = b.next()) > CANDIDATE_RULE) { // reserve for hits
                    ruleInfo = ruleList.getMetaData(rid);
                    ruleInfo[RULE_SIZE] ++;
                    if (isNewTopic) // added a new cached topic
                        ruleInfo[RULE_PEND] ++;
                    k = ruleMap[rid];
                    if (k < 0 || !filter[k].evaluate(currentTime, event))
                        continue;
                    if (tw[k] != null &&
                        tw[k].check(currentTime, 0L) != TimeWindows.NORMAL)
                        continue;
                    msg = new ObjectEvent();
                    msg.setPriority(Event.INFO);
                    msg.setAttribute("name", ruleList.getKey(rid));
                    task = new HashMap<String, Object>();
                    task.put("Event", event);
                    task.put("Action", action[k]);
                    msg.setBody(task);
                    size += flush(currentTime, cid, rid, msg, in, out[0],
                        0, topicStr);
                }
                if (size > 0) {
                    i = BYPASS_OUT;
                    rid = CANDIDATE_RULE;
                }
                else { // no subscribers
                    i = NOHIT_OUT;
                    rid = NOHIT_RULE;
                }
            }

            oid = outLinkMap[i];
            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid).send();

            if (size > 0 && (displayMask & MessageUtils.SHOW_BODY) > 0) {
                new Event(Event.INFO, name + ": dispatched a msg to " + size +
                    " subscribers with topic: (" + topicStr + ")").send();
            }
            else if (size <= 0 && displayMask > 1) try { // display nohit msg
                new Event(Event.INFO, name + ": skipped a nohit msg of topic ("+
                    topicStr + ") with:" + MessageUtils.display(inMessage,
                    null, displayMask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name +" failed to display nohit msg" +
                    " with (" + topicStr + "): " + e.toString()).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * displays the task message according to the ruleset and
     * puts it to the specified output link
     */
    private int flush(long currentTime, int cid, int rid, Message msg,
        XQueue in, XQueue out, int oid, String topicStr) {
        int i, dmask;
        long[] ruleInfo;
        Map rule;
        String[] pn;

        if (msg == null) {
            new Event(Event.WARNING, name + " null msg in flush: cid=" + cid +
                " rid=" + rid + " oid=" + oid).send();
            return 0;
        }
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " flush: cid=" + cid +
                " rid=" + rid + " oid=" + oid).send();

        ruleInfo = ruleList.getMetaData(rid);
        rule = (Map) ruleList.get(rid);
        pn = (String[]) rule.get("PropertyName");
        // retrieve displayMask from RULE_DMASK
        dmask = (int) ruleInfo[RULE_DMASK];

        if (dmask > 0) try { // display the task message
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " dispatched a msg:"+ MessageUtils.display(msg, topicStr,
                dmask, pn)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                " failed to display msg: " + e.toString()).send();
        }

        // flush the dispatched msg
        i = passthru(currentTime, msg, in, rid, oid, -1, 0);

        if (i > 0) {
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = currentTime;
            return 1;
        }
        else {
            new Event(Event.ERR, name +
                ": failed to flush the dispatched msg with " + topicStr +
                " for " + ruleList.getKey(rid)).send();
            return 0;
        }
    }

    /** returns the BOUNDARY separating pool from other fixed outlinks */
    public int getOutLinkBoundary() {
        return 0;
    }

    public void close() {
        Map rule;
        Browser browser;
        int rid;
        setStatus(NODE_CLOSED);
        cells.clear();
        msgList.clear();
        assetList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null)
                rule.clear();
        }
        ruleList.clear();
        topicTemplate = null;
        pc = null;
        if (cfgList != null) {
            ConfigList cfg;
            browser = cfgList.browser();
            while((rid = browser.next()) >= 0) {
                cfg = (ConfigList) cfgList.get(rid);
                if (cfg != null)
                    cfg.close();
            }
            cfgList.clear();
        }
    }

    protected void finalize() {
        close();
    }
}
