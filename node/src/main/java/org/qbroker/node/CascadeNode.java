package org.qbroker.node;

/* CascadeNode.java - a MessageNode applies multiple rules on JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.StringReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.CachedList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.QList;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.SimpleParser;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.monitor.ConfigList;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONTemplate;
import org.qbroker.jms.JSONFormatter;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.EventSelector;
import org.qbroker.event.Event;

/**
 * CascadeNode picks up a JMS messages from the input XQueue and extracts
 * the group key from it. It looks up the group cache for a cached list of
 * rulesets on the group key.  If the list is not empty, CascadeNode just loops
 * through the list and applies each filter on the message. If a ruleset gets a
 * hit, it will be used to evaluate the message. Therefore, the incoming
 * message may be evaluated by multiple rulesets in the natural order.
 *<br><br>
 * There are three fixed outlinks, done, failure and nohit. All evaluated
 * message will be routed to the outlink of done. Any failure will cause
 * the incoming message routed to the outlink of failure.  If there is no
 * ruleset got hit, the incoming message will be routed to the outlink of nohit.
 *<br><br>
 * CascadeNode contains a number of rulesets.  According to GroupKeyTemplate,
 * each ruleset defines its own group key with a set of property patterns. The
 * group key will be used to group rulesets. A ruleset may have an active time
 * window for blackout.  The number of the rulesets and their content may
 * change dynamically on demand.
 *<br><br>
 * CascadeNode always creates two extra rulesets.  The first one is the nohit
 * ruleset for those messages whose group key has no hit.  The second is
 * the candidate ruleset for all messages whose group key has at least one hit.
 * Since a candidate message may hit any number of rulesets, the stats count
 * for the candidate ruleset will be the number of the incoming messages rather
 * than the number of the rules.  The number of rulesets will be counted
 * by their own rulesets.  The DisplayMask and StringProperty of the ruleset
 * are used to display the details of cascated messages for the ruleset.
 * The stats of the rules are stored in the fields of RULE_SIZE, RULE_COUNT
 * and RULE_PEND.  RULE_PEND is for number of cached rulesets, RULE_SIZE for
 * number of preliminary hits, whereas RULE_COUNT is for number of real hits.
 *<br><br>
 * SessionTimeout determines how often to clean up expired group keys from the
 * cache.  If SessionTimeout is larger than zero, any cached group keys will be
 * expired if their idle time exceeds SessionTimeout.  Those expired group keys
 * will be removed from the cache in next session to save resources.
 * GroupKeyTemplate defines a template with multiple property names delimited
 * by a space char. It is required for extracting group key from a message.
 *<br><br>
 * You are free to choose any names for all three outlinks.  But CascadeNode
 * always assumes the first outlink for done, the second for failure, the
 * last for nohit.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class CascadeNode extends Node {
    protected int sessionTimeout = 86400000;

    // template for group key
    private Template groupKeyTemplate = null;
    private Perl5Compiler pc = null;

    private long[] ri;
    private int[] outLinkMap;

    private final static int NOHIT_RULE = 0;
    private final static int CANDIDATE_RULE = 1;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public CascadeNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        Map ph;
        long[] outInfo, ruleInfo;
        String key, str;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("GroupKeyTemplate")) != null && o instanceof String){
            str = (String) o;
            i = str.indexOf("JMSType");
            if (i > 0) // replacing JMSType
                str = str.substring(0, i) + MessageUtils.SHOW_MSGTYPE +
                    str.substring(i + 7); 
            groupKeyTemplate = new Template(str);
        }
        else
         throw(new IllegalArgumentException("GroupKeyTemplate is not defined"));

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "cascade";

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
        outLinkMap = new int[]{RESULT_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[FAILURE_OUT] = overlap[0];
        outLinkMap[NOHIT_OUT] = overlap[1];

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                " failed to init OutLinks"));

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
        if (++i > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        try { // init perl compiler and matcher
            pc = new Perl5Compiler();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init Perl compiler: " + e.toString()));
        }

        cfgList = new AssetList(name, 64);
        msgList = new AssetList(name, capacity);
        ruleList = new CachedList(name, ruleSize, QuickCache.META_ATAC, 0, 0);
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
            rule.put("GroupKeyPattern", "^$");
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
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
            ruleInfo[RULE_PID] = TYPE_NONE;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("GroupKeyPattern", "^$");
            ((CachedList) ruleList).add(key, ruleInfo, (Pattern) o, rule);
            ruleInfo[RULE_GID] = CANDIDATE_RULE;
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
                    !("ConfigList".equals((String) o) ||
                    "ConfigTemplate".equals((String) o))) {
                    ruleInfo = new long[RULE_TIME+1];
                    rule = initRuleset(tm, ph, ruleInfo);
                    if(rule != null && (key=(String) rule.get("Name")) != null){
                        str = (String) rule.get("GroupKeyPattern");
                        j = ((CachedList) ruleList).add(key, ruleInfo,
                            pc.compile(str), rule);
                        if (j > 0) // new rule added
                            ruleInfo[RULE_GID] = j;
                        else
                            new Event(Event.ERR, name + ": ruleset " + i +
                                ", "+ key + ", failed to be added").send();
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
                                pc.compile((String)rule.get("GroupKeyPattern")),
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
                            pc.compile((String)rule.get("GroupKeyPattern")),
                            rule);
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                    ruleInfo[RULE_DMASK] + " " +
                    assetList.getKey((int) ruleInfo[RULE_OID]) + " / " +
                    (String) ((Map) o).get("GroupKeyPattern"));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID TTL DMASK - " +
                "OutName / Pattern" + strBuf.toString()).send();
        }
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
        String[] keys = groupKeyTemplate.getSequence();
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

        if ((o = ph.get("JMSPropertyGroup")) != null && o instanceof List) {
            list = (List) o;
            str = "";
            for (i=0; i<keys.length; i++) { // build group key pattern
                key = keys[i];
                if (key.equals(String.valueOf(MessageUtils.SHOW_MSGTYPE)))
                    key = "JMSType";
                key = EventSelector.coarseGrain(key, list);
                if (i > 0)
                    str += " ";
                str += key.substring(1, key.length()-1);
            }
        }
        else
            str = ".*";
        rule.put("GroupKeyPattern", "^" + str + "$");
        rule.put("Filter", new MessageFilter(ph));
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);

        if ((o = ph.get("ResetOption")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);

        if ((o = ph.get("ActiveTime")) != null && o instanceof Map) {
            rule.put("TimeWindows", new TimeWindows((Map) o));
        }

        if ((o = ph.get("ParserArgument")) != null && o instanceof List) {
            // simple parser
            ruleInfo[RULE_PID] = TYPE_PARSER;
            rule.put("Parser", new SimpleParser((List) o));
            ruleInfo[RULE_EXTRA] = ((List) o).size();
            if ((o = (String) ph.get("FieldName")) != null &&
                o instanceof String && !"body".equals((String) o))
                rule.put("FieldName",  o);
        }
        else if((o = ph.get("JSONFormatter")) != null && o instanceof List) {
            ruleInfo[RULE_PID] = TYPE_JSONT;
            JSONFormatter ft = new JSONFormatter(ph);
            rule.put("JSONFormatter", ft);
            ruleInfo[RULE_EXTRA] = ft.getSize();
        }
        else if ((o = ph.get("JSONPath")) != null && o instanceof Map) {
            ruleInfo[RULE_PID] = TYPE_JSONPATH;
            Template temp;
            Map<String, Object> expr = new HashMap<String, Object>();
            iter = ((Map) o).keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                str = (String) ((Map) o).get(key);
                temp = new Template(str);
                if (temp == null || temp.numberOfFields() <= 0)
                    expr.put(key, str);
                else
                    expr.put(key, temp);
            }
            rule.put("JSONPath", expr);
            if (expr.size() == 1 && (o = ph.get("ListPath")) != null) {
                str = (String) o;
                rule.put("ListPath", str);
                if ((o = ph.get("Delimiter")) != null)
                    rule.put("Delimiter", o);
                ruleInfo[RULE_MODE] = 1;
            }

            if ((o = ph.get("TimestampKey")) != null && o instanceof String) {
                str = MessageUtils.getPropertyID((String) o);
                if (str == null)
                    rule.put("TSKey", o);
                else
                    rule.put("TSKey", str);
                if ((o = ph.get("TimePattern")) != null)
                    rule.put("DateFormat", new SimpleDateFormat((String) o));
                ruleInfo[RULE_EXTRA] = 1;
            }
        }
        else if((o = ph.get("FormatterArgument")) != null && o instanceof List){
            ruleInfo[RULE_PID] = TYPE_FORMAT;
        }

        ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];

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
                    ps = pc.compile((String) rule.get("GroupKeyPattern"));
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
                            ps=pc.compile((String) rule.get("GroupKeyPattern"));
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
                            ps=pc.compile((String) rule.get("GroupKeyPattern"));
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
            String tp = (String) rule.get("GroupKeyPattern");
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
                String str = (String) rule.get("GroupKeyPattern");
                if (tp.equals(str)) { // same groupKeyPattern
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
                        if ((debug & DEBUG_DIFF) > 0)
                            strBuf.append(" " + ruleInfo[i]);
                    }
                    if ((debug & DEBUG_DIFF) > 0)
                        new Event(Event.DEBUG, name + "/" + key + " ruleInfo:" +
                            strBuf).send();
                }
                else { // GroupKeyPattern changed
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
                        new Event(Event.DEBUG, name + "/" + key + ": " +
                            "groupKeyPattern has been changed to " +str).send();
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

    /** updates metadata of rules due to cleanups on all expired group keys */
    private void updatePendingCount(long currentTime) {
        int rid;
        long[] ruleInfo;
        Browser browser;
        browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) { // check every rule
            if (rid <= CANDIDATE_RULE) // skip nohit and candidate
                continue;
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_PEND] = ((CachedList) ruleList).getTopicCount(rid);
        }
    }

    /**
     * It evaluates the parsed JSON data based on the JSONPath expressions and
     * sets message properties with the evaluation data.  Upon success, it
     * returns RESULT_OUT as the index of the outlink.  Otherwise, FAILURE_OUT
     * is returned.
     */
    private int jparse(Object json, long currentTime, int rid,
        Map expression, byte[] buffer, Message msg) {
        Iterator iter;
        Map map, ph = null;
        List pl = null;
        Object o;
        String key = null, value, path;
        boolean isArray;

        if (json == null)
            return FAILURE_OUT;

        if (json instanceof List) { // for array
            pl = (List) json;
            if (pl.size() <= 0)
                return FAILURE_OUT;
            isArray = true;
        }
        else {
            ph = (Map) json;
            if (ph.size() <= 0)
                return FAILURE_OUT;
            isArray = false;
        }

        if (expression != null && expression.size() > 0) try {
            for (iter=expression.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = expression.get(key);
                if (o == null)
                    continue;
                else if (o instanceof String)
                    path = (String) o;
                else if (o instanceof Template)
                    path = MessageUtils.format(msg, buffer, (Template) o);
                else
                    continue;
                if (isArray)
                    o = JSON2Map.get(pl, path);
                else
                    o = JSON2Map.get(ph, path);
                if (o == null)
                    continue;
                else if (o instanceof String)
                    value = (String) o;
                else if (o instanceof Map)
                    value = JSON2Map.toJSON((Map) o, null, "");
                else if (o instanceof List)
                    value = JSON2Map.toJSON((List) o, null, "");
                else
                    value = o.toString();
                MessageUtils.setProperty(key, value, msg);
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to format msg for json path '" + key + "': " +
                Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to evaluate json path '" + key + "': " +
                Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                " completed evaluation on " + expression.size() +
                " json paths").send();

        return RESULT_OUT;
    }

    private int evaluate(long currentTime, Message message, int rid,
        long pid, byte[] buffer) {
        Map rule;
        String msgStr = null;
        int ic, i;

        i = RESULT_OUT;
        if (pid == TYPE_JSONPATH) {
            rule = (Map) ruleList.get(rid);
            Map expr = (Map) rule.get("JSONPath");
            if (expr == null || expr.size() <= 0) {
                i = FAILURE_OUT;
            }
            else try {
                msgStr = MessageUtils.processBody(message, buffer);
                StringReader sr = new StringReader(msgStr);
                Map json = (Map) JSON2FmModel.parse(sr);
                sr.close();

                i = jparse(json, currentTime, rid, expr, buffer, message);
                json.clear();
            }
            catch (IOException e) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to parse json payload: " +
                    Event.traceStack(e)).send();
            }
            catch (JMSException e) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to get json payload: " +
                    Event.traceStack(e)).send();
            }
        }
        else if (pid == TYPE_JSONT) {
            rule = (Map) ruleList.get(rid);
            JSONFormatter ft = (JSONFormatter) rule.get("JSONFormatter");
            if (ft == null || ft.getSize() <= 0) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " no json formatter defined").send();
            }
            else try {
                msgStr = MessageUtils.processBody(message, buffer);
                StringReader sr = new StringReader(msgStr);
                Map json = (Map) JSON2FmModel.parse(sr);
                sr.close();

                ic = ft.format(json, message);
                if (ic < 0)
                    i = FAILURE_OUT;
                json.clear();
            }
            catch (IOException e) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to parse json payload: " +
                    Event.traceStack(e)).send();
            }
            catch (JMSException e) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to get json payload: " +
                    Event.traceStack(e)).send();
            }
        }
        else if (pid == TYPE_PARSER) {
            rule = (Map) ruleList.get(rid);
            String fieldName = (String) rule.get("FieldName");
            SimpleParser parser = (SimpleParser) rule.get("Parser");
            if (parser == null) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " no parser defined").send();
            }
            else try {
                if (fieldName != null)
                    msgStr = MessageUtils.getProperty(fieldName, message);
                else if (msgStr == null)
                    msgStr = MessageUtils.processBody(message, buffer);
                Object o = parser.parse(msgStr);
                if (o == null)
                    i = FAILURE_OUT;
                else if (o instanceof Map) {
                    String key, value;
                    Map h = (Map) o;
                    Iterator iter = h.keySet().iterator();
                    h.remove("body");
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        o = h.get(key);
                        if (o == null || !(o instanceof String))
                            continue;
                        value = (String) o;
                        ic = MessageUtils.setProperty(key, value, message);
                        if (ic != 0) {
                            new Event(Event.ERR, name +
                                " failed to set property of: "+
                                key + " for " + ruleList.getKey(rid)).send();
                            i = FAILURE_OUT;
                            break;
                        }
                    }
                    h.clear();
                }
            }
            catch (JMSException e) {
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to get json payload: " +
                    Event.traceStack(e)).send();
            }
        }

        return i;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, uriStr, groupKey, ruleName = null;
        Object o;
        Object[] asset;
        Map rule = null;
        MessageFilter[] filters = null;
        TimeWindows[] tw;
        CachedList subList;
        Browser browser, b;
        Template template = null;
        String[] propertyName = null;
        int[] ruleMap;
        long[] outInfo = null, ruleInfo = null;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, id, k, n, size, retry = 0;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
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
        ruleMap = new int[ruleList.getCapacity()];
        for (i=0; i<ruleMap.length; i++)
            ruleMap[i] = -1;
        n = ruleList.size();
        filters = new MessageFilter[n];
        tw = new TimeWindows[n];
        browser = ruleList.browser();
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
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
                        keys.length + " expired group keys").send();
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

            groupKey = MessageUtils.format(inMessage, buffer, groupKeyTemplate);

            b = null;
            if (groupKey == null || groupKey.length() <= 0) {
                i = FAILURE_OUT;
            }
            else if ((b = subList.browser(groupKey, currentTime)) != null) {
                i = RESULT_OUT;
            }
            else if (!subList.containsTopic(groupKey)) { // first appearance
                b = subList.insertTopic(groupKey, currentTime,
                    sessionTimeout, null);
                ri[RULE_PEND] ++;
                ri[RULE_TIME] = currentTime;
                i = RESULT_OUT;
            }
            else if (subList.isExpired(groupKey, currentTime)) { // expired
                b = subList.insertTopic(groupKey, currentTime,
                    sessionTimeout, null);
                i = RESULT_OUT;
            }
            else { // null browser
                subList.expire(groupKey, currentTime);
                i = FAILURE_OUT;
                new Event(Event.ERR, name + ": null browser for group key (" +
                    groupKey + ")").send();
            }

            size = 0;
            if (i == RESULT_OUT) { // found a list
                b.reset();
                while ((rid = b.next()) > CANDIDATE_RULE) {
                    ruleInfo = ruleList.getMetaData(rid);
                    k = ruleMap[rid];
                    if (k < 0 || !filters[k].evaluate(inMessage, null))
                        continue;
                    if (tw[k] != null &&
                        tw[k].check(currentTime, 0L) != TimeWindows.NORMAL)
                        continue;
                    if (size <= 0) try {
                        switch ((int) ruleInfo[RULE_OPTION]) {
                          case RESET_MAP:
                            MessageUtils.resetProperties(inMessage);
                            if (inMessage instanceof MapMessage)
                               MessageUtils.resetMapBody((MapMessage)inMessage);
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
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to reset the msg: "+
                            Event.traceStack(e)).send();
                    }
                    size ++;
                    i=evaluate(currentTime, inMessage, rid, ruleInfo[RULE_PID],
                        buffer);
                    if (i == FAILURE_OUT)
                        break;

                    // post format
                    if (filters[k] != null && filters[k].hasFormatter()) try {
                        filters[k].format(inMessage, buffer);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to format the msg: "+
                            Event.traceStack(e)).send();
                    }

                    ruleInfo[RULE_COUNT] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                    if (ruleInfo[RULE_DMASK] > 0) {
                        new Event(Event.INFO, name + ": cascaded a msg to " +
                            ruleName + ":" + MessageUtils.display(inMessage,
                            groupKey, (int) ruleInfo[RULE_DMASK], 
                            (String[]) rule.get("PropertyName"))).send();
                    }
                }
                rid = CANDIDATE_RULE;
            }
            else if (i == NOHIT_OUT) { // no subscribers
                i = NOHIT_OUT;
                rid = NOHIT_RULE;
            }
            else // for failure
                rid = NOHIT_RULE;

            retry = 0;
            oid = outLinkMap[i];
            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid).send();

            if (size > 0 && (displayMask & MessageUtils.SHOW_BODY) > 0) {
                new Event(Event.INFO, name +": cascaded a msg to " + size +
                    " rulesets with group key: (" + groupKey + ")").send();
            }
            else if (size <= 0 && displayMask > 1) try { // display nohit msg
                new Event(Event.INFO,name+": skipped a nohit msg of group key ("
                    + groupKey + ") with:" + MessageUtils.display(inMessage,
                    null, displayMask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name +" failed to display msg with ("+
                    groupKey + "): " + e.toString()).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
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
        groupKeyTemplate = null;
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
