package org.qbroker.node;

/* AggregateNode.java - a MessageNode aggregating JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.QList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DataSet;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.Aggregation;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * AggregateNode aggregates JMS messages according to their content
 * and the predefined rulesets.  It filters them into three outlinks: bypass
 * for all the aggregated messages, nohit for those messages do not belong
 * to any rulesets, failure for the messages failed at the aggregations.
 * AggregateNode will create a new message, a TextMessage, as the result of
 * the aggregation for each unique key in every session.  These new messages
 * will be flushed to the outlink of done when their sessions terminate.
 *<br/><br/>
 * AggregateNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each ruleset
 * defines a unique message group.  A ruleset also defines the unique keys
 * for the group and how to aggregate the messages sharing the same keys.
 * Each group has its own cache for aggregations. The aggregation process is
 * maintained by sessions with certain lifetime. For each unique key, there is
 * a cached message with aggregated data. A ruleset also defines parameters
 * used for the aggregation, such as DynamicSession, TimeToLive, SessionSize,
 * CountField and TerminatorField, etc. The lifetime of a session is
 * determined by TimeToLive in seconds. Therefore, AggregateNode knows when to
 * terminate an aggregation session. However, a session can belong to a key or
 * all the keys in a ruleset. If DynamicSession is set to true on a ruleset,
 * each key of that ruleset will have its own session. When the session is
 * terminated, only the cached message for that key will be flushed to the
 * outlink of done. By default, DynamicSession is set to false. It means all
 * the keys in a ruleset will share the same session by default. In this case,
 * if the session is terminated, all the cached messages for that ruleset
 * will be flushed out at the same time.
 *<br/><br/>
 * The aggregation on message body is defined separately in BodyAggregation. It
 * supports the first, last, append for text, and merge for JSON or XML payload.
 * In case of merge for JSON payload, JSONPath is required in the hash. JSONPath
 * defines a JSONPath expression that is used to select the data from the
 * responses. The selected data will be merged as a child node to the parent
 * node in the aggregated JSON. In case of merge for XML payload, XPath is
 * required in the hash. XPath defines an XPath expression that is used to
 * select the data from the responses. The selected data will be merged as a
 * child node to the parent node in the aggregated XML.
 *<br/><br/>
 * Currently, AggregateNode supports 4 different ways to terminate a session.
 * First, TimeToLive specifies the lifetime of any key in the cache for the
 * ruleset. If a key expires in the cache, its session is terminated. The
 * expired keys will not participate the aggregation process. But they may
 * not be flushed out until the next flush operation. Second, if the total
 * number of cached keys in a ruleset exceeds the SessionSize, AggregateNode
 * will try to find any expired keys to flush to save space. If CountField is
 * defined in a ruleset, AggregateNode will retrieve the total count from
 * the incoming messages. The total count will be used to terminate the
 * aggration session on the key. If TerminatorField is defined, AggregateNode
 * will check that property on each incoming message. If an incoming message
 * has a non-empty value on that property, it will be treated as the terminator.
 * The aggregation session for the key will be terminated by the terminator.
 * In this case, if the DynamicSession is enabled and CopyOver is set to true,
 * AggregateNode will copy the aggregated result over to the terminate message.
 *<br/><br/>
 * AggregateNode also supports plugins for customized aggregations.  In order
 * to support plugins, ClassName of the implementation should be defined in
 * the ruleset.  The class should have a public method of aggregate() that
 * takes two JMS messages as the arguments.  The first message is the incoming
 * one.  The second message is the cached message for aggregations.  The method
 * is supposed to return null for success or an error text for failures.  It
 * should not modify the incoming message by all means.
 *<br/><br/>
 * Apart from user defined rulesets, AggregateNode always creates one extra
 * ruleset, nohit.  The ruleset of nohit is for those messages not hitting any
 * matches.  The number of aggregation messages is storded into the RULE_PEND
 * field.
 *<br/><br/>
 * AggregateNode also supports single key sessions.  In order to enable the
 * single key session, SessionSize has to be set to 1.  In this case,
 * AggregateNode is assuming that all keys of input messages are already 
 * grouped in packs.  It means all messages with the same key stay together.
 * Any new key will terminate the previous aggregation session.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * AggregateNode always assumes the first outlink for done, the second for
 * bypass group, the third for failure and the last for nohit.  Any two or
 * more outlinks can share the same outlink name.  It means these outlinks
 * are sharing the same output channel.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class AggregateNode extends Node {
    private int heartbeat = 60000;
    private int cacheStatus;
    private String reportName = null;
    private AssetList pluginList = null; // pluins
    private QList cacheList = null;    // active caches with rid
    private XPath xpath = null;
    private DocumentBuilder builder = null;
    private Transformer defaultTransformer = null;

    private int sessionSize = 64;
    private int[] outLinkMap;
    private int[] threshold;

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int ONSET_OUT = 4;

    public AggregateNode(Map props) {
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
            operation = "aggregate";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000*Integer.parseInt((String) o);
        if ((o = props.get("SessionSize")) != null)
            sessionSize = Integer.parseInt((String) o);
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

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[FAILURE_OUT] = overlap[0];
        outLinkMap[NOHIT_OUT] = overlap[1];

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));
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

        // set reportName for RESULT_OUT
        o = assetList.get(outLinkMap[RESULT_OUT]);
        reportName = (String) ((Object[]) o)[ASSET_URI];
        cacheStatus = QuickCache.CACHE_ON;

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pluginList = new AssetList(name, ruleSize);
        cacheList = new QList(name, ruleSize);
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
            Aggregation aggr;
            Map ph;
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ph = (Map) ruleList.get(i);
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 + " " +
                    ruleInfo[RULE_MODE] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_EXTRA] + " " + ruleInfo[RULE_GID] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
                aggr = (Aggregation) ph.get("Aggregation");
                if (aggr != null)
                    strBuf.append(aggr.displayList("\n\t  "));
            }
            new Event(Event.DEBUG, name +
                " RuleName: RID PID TTL Mode Option Extra GID - OutName" +
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
        if ((o = props.get("SessionSize")) != null) {
            i = Integer.parseInt((String) o);
            if (i >= 0 && i != sessionSize) {
                sessionSize = i;
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

        // displayMask of ruleset stored in dmask 
        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store aggregation mask into RULE_OPTION
        if ((o = ph.get("AggregationMask")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = ruleInfo[RULE_DMASK];

        // TimeToLive is for a key or a session
        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000*Integer.parseInt((String) o);

        // TouchEnabled to update the mtime
        if ((o = ph.get("TouchEnabled")) != null &&
            "true".equalsIgnoreCase((String) o))
            ruleInfo[RULE_GID] += 1;

        // DynamicSession for keys
        if ((o = ph.get("DynamicSession")) != null &&
            "true".equalsIgnoreCase((String) o))
            ruleInfo[RULE_MODE] = 1;

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // plug-in
            str = (String) o;
            if ((o = ph.get("AggregatorArgument")) != null) {
                Map map;
                if (o instanceof List) {
                    list = (List) o;
                    k = list.size();
                    for (i=0; i<k; i++) {
                        if ((o = list.get(i)) == null)
                            continue;
                        if (!(o instanceof Map))
                            continue;
                        map = (Map) o;
                        if (map.size() <= 0)
                            continue;
                        iter = map.keySet().iterator();
                        if ((o = iter.next()) == null)
                            continue;
                        str += "::" + (String) o;
                        str += "::" + (String) map.get((String) o);
                    }
                }
                else if (o instanceof Map) {
                    str += (String) ((Map) o).get("Name");
                }
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
                o = MessageUtils.getPlugins(ph,"AggregatorArgument","aggregate",
                    new String[]{"javax.jms.Message", "javax.jms.Message"},
                    "close", name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            if ((o = ph.get("KeyTemplate")) != null && o instanceof String)
                rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null && o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String) o));

            if ((o = ph.get("CountField")) != null && o instanceof String)
                rule.put("CountField", (String) o);
            if ((o = ph.get("TerminatorField")) != null && o instanceof String){
                rule.put("TerminatorField", (String) o);
                if ((o = ph.get("CopyOver")) != null && o instanceof String &&
                    "true".equalsIgnoreCase((String) o))
                    ruleInfo[RULE_GID] += 2; // store to GID
            }

            // sessionSize of ruleset stored in extra 
            if ((o = ph.get("SessionSize")) != null && o instanceof String)
                ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_EXTRA] = sessionSize;

            rule.put("Cache",
                new QuickCache(ruleName, QuickCache.META_DEFAULT, 0, 0));

            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            // plugin id stored in PID
            ruleInfo[RULE_PID] = id;
        }
        else if ((o = ph.get("Aggregation")) != null &&
            o instanceof List) { // for aggregate ruleset
            Aggregation aggr = new Aggregation(ruleName, (List) o);
            rule.put("Aggregation", aggr);

            if ((o = ph.get("BodyAggregation")) != null && o instanceof Map) {
                Map<String, Object> map = Utils.cloneProperties((Map) o);
                if (aggr.setBodyMap(map) == null) {
                    new Event(Event.ERR, name + ": BodyAggregation for " +
                        ruleName + " not well defined").send();
                }
                if (aggr.getBodyType() == Utils.RESULT_XML) try {
                    if (xpath == null) {
                        DocumentBuilderFactory factory =
                            DocumentBuilderFactory.newInstance();
                        factory.setNamespaceAware(true);
                        builder = factory.newDocumentBuilder();
                        xpath = XPathFactory.newInstance().newXPath();
                        TransformerFactory tFactory =
                            TransformerFactory.newInstance();
                        defaultTransformer = tFactory.newTransformer();
                        defaultTransformer.setOutputProperty(OutputKeys.INDENT,
                            "yes");
                    }
                    str = (String) map.get("XPath");
                    map.put("XPath", xpath.compile(str));
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(ruleName + " failed " +
                        "to instantiate XPath: " + Event.traceStack(ex)));
                }
            }

            if ((o = ph.get("KeyTemplate")) != null && o instanceof String)
                rule.put("KeyTemplate", new Template((String) o));
            if ((o = ph.get("KeySubstitution")) != null &&
                o instanceof String)
                rule.put("KeySubstitution", new TextSubstitution((String) o));

            if ((o = ph.get("CountField")) != null && o instanceof String)
                rule.put("CountField", (String) o);
            if ((o = ph.get("TerminatorField")) != null && o instanceof String){
                rule.put("TerminatorField", (String) o);
                if ((o = ph.get("CopyOver")) != null && o instanceof String &&
                    "true".equalsIgnoreCase((String) o))
                    ruleInfo[RULE_GID] += 2; // store to GID
            }

            // sessionSize of ruleset stored in extra 
            if ((o = ph.get("SessionSize")) != null && o instanceof String)
                ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_EXTRA] = sessionSize;

            rule.put("Cache",
                new QuickCache(ruleName, QuickCache.META_DEFAULT, 0, 0));

            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_AGGREGATE;
        }
        else { // bypass
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

    private int copyCache(Template tmp, TextSubstitution sub, QuickCache s,
        QuickCache t) {
        int i, n;
        long tm;
        int[] meta;
        String key;
        byte[] buffer = new byte[4096];
        Object[] keys;
        TextMessage msg;
        if (s == null || s.size() <= 0)
            return 0;
        if (t == null)
            return -1;

        keys = s.sortedKeys();
        n = keys.length;
        for (i=0; i<n; i++) {
            key = (String) keys[i];
            meta = (int[]) s.getMetaData(key);
            msg = (TextMessage) s.get(key);
            tm = s.getTimestamp(key);
            try {
                key = MessageUtils.format(msg, buffer, tmp);
            }
            catch (Exception e) {
                key = (String) keys[i];
            }
            if (sub != null && key != null)
                key = sub.substitute(key);
            t.insert(key, tm, 0, meta, msg);
        }

        if (t.size() > 0)
            t.setStatus(s.getStatus(), s.getMTime());

        return n;
    }

    /**
     * It replaces the existing rule of the key and returns its id upon success.
     * It is not MT-Safe.
     */
    public int replaceRule(String key, Map ph, XQueue in) {
        int id = ruleList.getID(key);
        if (id > 0) {
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            String str;
            long tm = System.currentTimeMillis();
            long[] meta = new long[RULE_TIME+1];
            long[] ruleInfo = ruleList.getMetaData(id);
            Map rule = initRuleset(tm, ph, meta);
            if (rule != null && rule.containsKey("Name")) {
                Object o;
                o = ruleList.set(id, rule);
                if (o != null && o instanceof Map) {
                    Aggregation aggr =
                        (Aggregation) ((Map) o).remove("Aggregation");
                    if (aggr != null && aggr.hasBody())
                        aggr.clearBodyMap();
                    ((Map) o).clear();
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
                }
                if ((o = cacheList.remove(id)) != null) { // active cache
                    QuickCache cache = (QuickCache) rule.get("Cache");
                    int n = copyCache((Template) rule.get("KeyTemplate"),
                        (TextSubstitution) rule.get("KeySubstitution"),
                        (QuickCache) o, cache);
                    cacheList.reserve(id);
                    cacheList.add(cache, id);
                    ((QuickCache) o).clear();
                }
                if ((o = pluginList.get(id)) != null) try { // has plugin
                    java.lang.reflect.Method closer;
                    closer = (java.lang.reflect.Method) ((Object[]) o)[2];
                    if (closer != null && ((Object[]) o)[1] != null) {
                        closer.invoke(((Object[]) o)[1], new Object[] {});
                    }
                }
                catch (Throwable t) {
                }

                return id;
            }
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            int n;
            ConfigList cfg;
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            cfg = (ConfigList) cfgList.get(key);
            cfg.resetReporter(ph);
            cfg.setDataField(name);
            refreshRule(key, in);
            n = cfg.getSize();
            if (n > 0)
                id = ruleList.getID(cfg.getKey(0));
            else
                id = 0;
            return id;
        }

        return -1;
    }

    /**
     * Aggregate engine aggregates each incoming messages according to the
     * aggregation operations.  It extracts data from the incoming message
     * and aggregates the values into a cached text message on the key.  It
     * returns the index of OutLink which should be mapped into the oid via
     * linkMap[].
     */
    private int aggregate(String key, Aggregation aggr, long currentTime,
        int rid, int total, Message inMessage, byte[] buffer, QuickCache cache){
        int i = BYPASS_OUT, k;
        String msgStr;
        TextMessage msg;

        if (aggr == null || aggr.getSize() <= 0 || cache == null)
            return i;

        msg = (TextMessage) cache.get(key, currentTime);
        try {
            if (msg == null) { // initialize the first one
                Reader sr = null;
                long[] info = ruleList.getMetaData(rid);
                int ttl = (info[RULE_MODE] <= 0) ? 0 : (int) info[RULE_TTL];
                msg = new TextEvent();
                msg.setJMSTimestamp(inMessage.getJMSTimestamp());
                i = aggr.initialize(currentTime, inMessage, msg);
                if (i == 0 && aggr.hasBody()) switch (aggr.getBodyOperation()) {
                  case Aggregation.AGGR_FIRST:
                  case Aggregation.AGGR_LAST:
                  case Aggregation.AGGR_APPEND:
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null)
                        msgStr = "";
                    msg.setText(msgStr);
                    break;
                  case Aggregation.AGGR_MERGE:
                    k = aggr.getBodyType();
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null || msgStr.length() <= 0) {
                        new Event(Event.ERR, name + " empty payload on "+
                            key + " for " + ruleList.getKey(rid)).send();
                        i = -1;
                    }
                    else try {
                        sr = new StringReader(msgStr);
                        if (k == Utils.RESULT_XML) { // xml payload
                            Document doc = builder.parse(new InputSource(sr));
                            ((Event) msg).setBody(doc);
                        }
                        else if (k == Utils.RESULT_JSON) { // json payload
                            Object o = JSON2Map.parse(sr);
                            ((Event) msg).setBody(o);
                        }
                        sr.close();
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name +" failed to parse body on "+
                            key + " for " + ruleList.getKey(rid) + ": " +
                            Event.traceStack(ex)).send();
                        i = -1;
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  default:
                }
                cache.insert(key, currentTime, ttl, new int[]{rid,1,total},msg);
                if (cache.size() == 1) { // reset start-time for dynamic session
                    cache.setStatus(cache.getStatus(), currentTime);
                    // add the cache to cacheList
                    cacheList.reserve(rid);
                    cacheList.add(cache, rid);
                }
                if (i != 0) // failed
                    i = FAILURE_OUT;
                else if (total <= 0 || 1 < total) // normal
                    i = ONSET_OUT;
                else // reach the total count
                    i = RESULT_OUT;
            }
            else { // initialized already
                Reader sr = null;
                String str;
                int[] meta = cache.getMetaData(key);
                i = aggr.aggregate(currentTime, inMessage, msg);
                if (i == 0 && aggr.hasBody()) switch (aggr.getBodyOperation()) {
                  case Aggregation.AGGR_LAST:
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    msg.setText(msgStr);
                    break;
                  case Aggregation.AGGR_APPEND:
                    str = MessageUtils.processBody(inMessage, buffer);
                    if (str == null) // no such value, use default
                        str = (String) aggr.getFromBody("DefaultValue");
                    if (str == null || str.length() <= 0)
                        break;
                    msgStr = msg.getText();
                    if (msgStr == null)
                        msgStr = "";
                    if (aggr.hasKeyInBody("Delimiter")) {
                        if (msgStr.length() > 0)
                            str = (String) aggr.getFromBody("Delimiter") + str;
                    }
                    msg.setText(msgStr + str);
                    break;
                  case Aggregation.AGGR_MERGE:
                    k = aggr.getBodyType();
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null || msgStr.length() <= 0)
                        break;
                    try {
                        sr = new StringReader(msgStr);
                        if (k == Utils.RESULT_XML) { // xml payload
                            Document doc = (Document) ((Event) msg).getBody();
                            i = aggr.merge(sr, doc, builder);
                        }
                        else if (k == Utils.RESULT_JSON) { // json payload
                            Object o = ((Event) msg).getBody();
                            if (aggr.hasKeyInBody("FieldName")) {
                                str = (String) aggr.getFromBody("FieldName");
                                str = MessageUtils.getProperty(str, inMessage);
                            }
                            else
                                str = null;
                            if (o instanceof Map)
                                i = aggr.merge(sr, str, (Map) o);
                            else // not implemented in Aggregation yet
                                i = -1;
                        }
                        i = 0;
                        sr.close();
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name+ " failed to merge body for "+
                            ruleList.getKey(rid) + ": " +
                            Event.traceStack(e)).send();
                        i = -1;
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  case Aggregation.AGGR_FIRST:
                  default:
                }
                // update internal count for the key
                meta[1] ++;
                total = meta[2];
                if (i != 0) // failed
                    i = FAILURE_OUT;
                else if (total <= 0 || meta[1] < total) // normal
                    i = BYPASS_OUT;
                else // reach the total count
                    i = RESULT_OUT;
            }
        }
        catch (Exception e) {
            i = FAILURE_OUT;
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to aggregate on "+key+"\n"+
                Event.traceStack(e)).send();
        }

        return i;
    }

    /**
     * It picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null;
        String countField = null, termField = null;
        Object[] asset;
        Map rule = null;
        Browser browser;
        QuickCache cache = null;
        MessageFilter[] filters = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        Template template = null;
        TextSubstitution sub = null;
        Aggregation aggr = null;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, size = 0, previousRid, total;
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
        n = out.length;
        for (i=0; i<n; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }
        outInfo = assetList.getMetaData(0);

        n = ruleMap.length;
        previousTime = 0L;
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
                if (cacheList.size() > 0) {
                    size = update(currentTime, in, out[0], buffer);
                    currentTime = System.currentTimeMillis();
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
                if (ruleInfo[RULE_PID] == TYPE_AGGREGATE) {
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                    countField = (String) rule.get("CountField");
                    termField = (String) rule.get("TerminatorField");
                    cache = (QuickCache) rule.get("Cache");
                    aggr = (Aggregation) rule.get("Aggregation");
                }
                else if (ruleInfo[RULE_PID] >= 0) {
                    template = (Template) rule.get("KeyTemplate");
                    sub = (TextSubstitution) rule.get("KeySubstitution");
                    countField = (String) rule.get("CountField");
                    termField = (String) rule.get("TerminatorField");
                    cache = (QuickCache) rule.get("Cache");
                    aggr = null;
                }
                previousRid = rid;
            }

            if (i < 0) { // failure
                oid = outLinkMap[FAILURE_OUT];
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
            }
            else if (rid == 0) { // nohit
                i = NOHIT_OUT;
                oid = outLinkMap[i];
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO,name +": "+ruleName+" aggregated msg "+
                        (count+1) +":"+ MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception ex) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: "+Event.traceStack(ex)).send();
                }
            }
            else if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // bypass
                oid = (int) ruleInfo[RULE_OID];
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO,name +": "+ruleName+" aggregated msg "+
                        (count+1) +":"+ MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception ex) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: "+Event.traceStack(ex)).send();
                }
            }
            else try { // aggregate engine
                String key;
                int k = (int) ruleInfo[RULE_PID];
                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);

                if (termField != null) { // check session terminate property
                    int[] info;
                    String str = null;
                    str = MessageUtils.getProperty(termField, inMessage);
                    if (str != null && str.length() > 0) {
                        if (ruleInfo[RULE_MODE] == 0) // flush the session
                            size = flush(currentTime, in, out[0], rid, buffer,
                                cache);
                        else if ((info = cache.getMetaData(key)) == null)
                            size = 0;
                        else if (info.length <= 1 || info[1] <= 0) // flushed
                            size = 0;
                        else { // flush the msg of the key for dynamic session
                            MessageFilter filter;
                            int dmask = (int) ruleInfo[RULE_OPTION];
                            TextEvent msg = (TextEvent) cache.get(key, 0);
                            if ((ruleInfo[RULE_GID] & 2) > 0) try {
                                MessageUtils.copy(msg, inMessage, buffer);
                            }
                            catch (Exception e) {
                                new Event(Event.WARNING, name + ": " + ruleName+
                                    " failed to copy msg into terminator: " +
                                    Event.traceStack(e)).send();
                            }
                            size = 0;
                            if (aggr != null)
                                recoverMsgBody(aggr.getBodyType(),ruleName,msg);
                            if (dmask > 0) try {  // display the message
                                if ((dmask & dspBody) > 0)
                                    str = MessageUtils.processBody(msg, buffer);
                                else
                                    str = null;
                                new Event(Event.INFO, name + ": " + ruleName +
                                    " flushed aggregated msg " + (size + 1) +
                                    ":" + MessageUtils.display(msg, str, dmask,
                                    propertyName)).send();
                            }
                            catch (Exception e) {
                                new Event(Event.WARNING, name + ": " + ruleName+
                                    " failed to display msg: " +
                                    e.toString()).send();
                            }

                            // post format
                            filter = (MessageFilter) rule.get("Filter");
                            if (filter != null && filter.hasFormatter()) try {
                                filter.format(msg, buffer);
                            }
                            catch (JMSException e) {
                                new Event(Event.ERR, name + ": " + ruleName +
                                    " failed to format the msg: " +
                                    Event.traceStack(e)).send();
                            }

                            if(passthru(currentTime, msg, in, rid, 0, -1, 0)>0){
                                size ++;
                                ruleInfo[RULE_PEND] --;
                                ruleInfo[RULE_TIME] = currentTime;
                                // disable the expired key
                                info[1] = 0;
                                cache.touch(key, currentTime-ruleInfo[RULE_TTL]-
                                    10000, currentTime);
                            }
                            else
                                new Event(Event.ERR, name + ": " + ruleName+
                                    " failed to flush the msg with the key " +
                                    key).send();
                        }
                        currentTime = System.currentTimeMillis();
                        new Event(Event.INFO, name + ": flushed " + size +
                            " msgs due to session terminated on: " +key).send();
                        previousTime = currentTime;
                        oid = outLinkMap[BYPASS_OUT];
                        if ((debug & DEBUG_PROP) > 0)
                            new Event(Event.DEBUG, name+" propagate: cid="+cid+
                                " rid=" + rid + " oid=" + oid + " i=" + 0 +
                                " status=" + cacheStatus).send();
                        if (ruleInfo[RULE_DMASK] > 0) try {//display the message
                            if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                                msgStr = MessageUtils.processBody(inMessage,
                                    buffer);
                            new Event(Event.INFO, name + ": " + ruleName+
                                " aggregated msg "+ (count+1) +":"+
                                MessageUtils.display(inMessage, msgStr,
                                (int) ruleInfo[RULE_DMASK],
                                propertyName)).send();
                        }
                        catch (Exception ex) {
                            new Event(Event.WARNING, name + ": " + ruleName +
                                " failed to display flushed msg: "+
                                Event.traceStack(ex)).send();
                        }
                        count += passthru(currentTime, inMessage, in, rid, oid,
                            cid, 0);
                        feedback(in, -1L);
                        sz = msgList.size();
                        continue;
                    }
                }
                else if (!cache.containsKey(key)) { // a new key
                    size = (int) ruleInfo[RULE_EXTRA];
                    if ( size > 0 && cache.size() >= size) {
                        size = flush(currentTime, in, out[0], rid,buffer,cache);
                        new Event(Event.INFO, name + ": flushed " + size +
                            " msgs due to oversize: " +
                            (cache.size()+size)).send();
                        currentTime = System.currentTimeMillis();
                        previousTime = currentTime;
                    }
                }
                else if (cache.isExpired(key)) { // key has expired
                    int[] info;
                    String str = null;
                    if ((info = cache.getMetaData(key)) != null &&
                        info.length > 1 && info[1] > 0) {//flush the expired key
                        MessageFilter filter;
                        int dmask = (int) ruleInfo[RULE_OPTION];
                        TextEvent msg = (TextEvent) cache.get(key, 0);
                        size = 0;
                        if (aggr != null)
                            recoverMsgBody(aggr.getBodyType(), ruleName, msg);
                        if (dmask != 0) try {  // display the message
                            if ((dmask & dspBody) > 0)
                                str = MessageUtils.processBody(msg, buffer);
                            else
                                str = null;
                            new Event(Event.INFO, name + ": " + ruleName +
                                " flushed aggregated msg " + (size + 1) +
                                ":" + MessageUtils.display(msg, str, dmask,
                                propertyName)).send();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName+
                                " failed to display msg: " +
                                e.toString()).send();
                        }

                        // post format
                        filter = (MessageFilter) rule.get("Filter");
                        if (filter != null && filter.hasFormatter()) try {
                            filter.format(msg, buffer);
                        }
                        catch (JMSException e) {
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to format the msg: " +
                                Event.traceStack(e)).send();
                        }

                        if (passthru(currentTime, msg, in, rid, 0, -1, 0) > 0) {
                            size ++;
                            ruleInfo[RULE_PEND] --;
                            ruleInfo[RULE_TIME] = currentTime;
                            // disable the expired key
                            info[1] = 0;
                            new Event(Event.INFO, name + ": flushed " + size +
                                " msgs due to overtime").send();
                        }
                        else
                            new Event(Event.ERR, name + ": " + ruleName+
                                " failed to flush the msg with the key " +
                                key).send();
                        currentTime = System.currentTimeMillis();
                        previousTime = currentTime;
                    }
                }

                total = 0;
                if (countField != null) try { //check session terminate property
                    String str = null;
                    str = MessageUtils.getProperty(countField, inMessage);
                    total = Integer.parseInt(str);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to parse total count: "+ e.toString()).send();
                    total = 0;
                }

                if (k < 0) { // default aggregation
                    i = aggregate(key, aggr, currentTime, rid, total,
                        inMessage, buffer, cache);
                }
                else { // for plugins
                    java.lang.reflect.Method method;
                    Message msg;
                    Object o, obj;
                    asset = (Object[]) pluginList.get(k);
                    obj = asset[1];
                    method = (java.lang.reflect.Method) asset[0];
                    if (method == null) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " null method for plugin: "+ k).send();
                    }
                    else if((msg=(Message)cache.get(key,currentTime)) == null){
                        // initialize the first one 
                        int ttl = (ruleInfo[RULE_MODE] <= 0) ? 0 :
                            (int) ruleInfo[RULE_TTL];
                        msg = new TextEvent();
                        msg.setJMSTimestamp(inMessage.getJMSTimestamp());
                        o = method.invoke(obj, new Object[] {inMessage, msg});
                        if (o == null) {
                            cache.insert(key, currentTime, ttl,
                                new int[]{rid, 1, total}, msg);
                            if (cache.size() == 1) { // reset start-time
                                cache.setStatus(cache.getStatus(), currentTime);
                                // add the cache to cacheList
                                cacheList.reserve(rid);
                                cacheList.add(cache, rid);
                            }
                            if (total <= 0 || 1 < total) // normal
                                i = ONSET_OUT;
                            else // reach the total count
                                i = RESULT_OUT;
                        }
                        else {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to aggregate on key " + key +
                                ": " + o.toString()).send();
                        }
                    }
                    else { // already initialized
                        int[] meta = cache.getMetaData(key);
                        o = method.invoke(obj, new Object[] {inMessage, msg});
                        // update internal count for the key
                        meta[1] ++;
                        total = meta[2];
                        if (o != null) {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to aggregate on key " + key +
                                ": " + o.toString()).send();
                        }
                        else if (total <= 0 || meta[1] < total) // normal
                            i = BYPASS_OUT;
                        else // reach the total count
                            i = RESULT_OUT;
                    }
                }

                if (i == ONSET_OUT) {
                    oid = outLinkMap[BYPASS_OUT];
                    if ((ruleInfo[RULE_GID] & 1) > 0) // update mtime on the key
                        cache.touch(key, currentTime, currentTime);
                }
                else if (i != RESULT_OUT)
                    oid = outLinkMap[i];
                else { // terminated by count
                    int[] info;
                    String str;
                    if (ruleInfo[RULE_MODE] == 0) // flush the session
                        size = flush(currentTime, in, out[0], rid, buffer,
                            cache);
                    else if ((info = cache.getMetaData(key)) == null)
                        size = 0;
                    else if (info.length <= 1 || info[1] <= 0) // flushed
                        size = 0;
                    else { // flush the msg of the key for dynamic session
                        MessageFilter filter;
                        int dmask = (int) ruleInfo[RULE_OPTION];
                        TextEvent msg = (TextEvent) cache.get(key, 0);
                        size = 0;
                        if (aggr != null)
                            recoverMsgBody(aggr.getBodyType(), ruleName, msg);
                        if (dmask != 0) try {  // display the message
                            if ((dmask & dspBody) > 0)
                                str = MessageUtils.processBody(msg, buffer);
                            else
                                str = null;
                            new Event(Event.INFO, name + ": " + ruleName +
                                " flushed aggregated msg " + (count + 1) +
                                ":" + MessageUtils.display(msg, str, dmask,
                                propertyName)).send();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName+
                                " failed to display flushed msg: " +
                                e.toString()).send();
                        }

                        // post format
                        filter = (MessageFilter) rule.get("Filter");
                        if (filter != null && filter.hasFormatter()) try {
                            filter.format(msg, buffer);
                        }
                        catch (JMSException e) {
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to format the msg: " +
                                Event.traceStack(e)).send();
                        }

                        if (passthru(currentTime, msg, in, rid, 0, -1, 0) > 0) {
                            size ++;
                            ruleInfo[RULE_PEND] --;
                            ruleInfo[RULE_TIME] = currentTime;
                            // disable the expired key
                            info[1] = 0;
                            cache.touch(key, currentTime-ruleInfo[RULE_TTL]-
                                10000, currentTime);
                        }
                        else
                            new Event(Event.ERR, name + ": " + ruleName+
                                " failed to flush the msg with the key " +
                                key).send();
                    }
                    currentTime = System.currentTimeMillis();
                    new Event(Event.INFO, name + ": flushed " + size +
                        " msgs due to total count on: " +key).send();
                    previousTime = currentTime;
                    oid = outLinkMap[BYPASS_OUT];
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i +
                        " status=" + cacheStatus).send();

                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO,name +": "+ruleName+" aggregated msg "+
                        (count+1) +":"+ MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception ex) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: "+Event.traceStack(ex)).send();
                }

                if (i == ONSET_OUT) { // new aggregate message added to cache
                    size = cache.size();
                    ruleInfo[RULE_PEND] ++;
                    ruleInfo[RULE_TIME] = currentTime;
                }
            }
            catch (Exception e) {
                String str = name + ": " + ruleName;
                Exception ex = null;
                if (e instanceof JMSException)
                    ex = ((JMSException) e).getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                i = FAILURE_OUT;
                oid = outLinkMap[i];
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
                new Event(Event.ERR, str + " failed to escalate msg: "+
                    Event.traceStack(e)).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /** recovers the message body from XML or JSON object */
    private void recoverMsgBody(int k, String ruleName, TextEvent msg) {
        if (k == Utils.RESULT_JSON) {
            Object o = msg.getBody();
            if (o instanceof List)
                ((Event) msg).setBody(JSON2Map.toJSON((List) o, "", "\n"));
            else
                ((Event) msg).setBody(JSON2Map.toJSON((Map) o, "", "\n"));
        }
        else if (k == Utils.RESULT_XML) try {
            Document doc = (Document) msg.getBody();
            StreamResult result = new StreamResult(new StringWriter());
            defaultTransformer.transform(new DOMSource(doc), result);
            msg.setText(result.getWriter().toString());
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleName +
                " failed to format msg: " + e.toString()).send();
            ((Event) msg).setBody("");
        }
    }

    /**
     * It displays and sets properties on the aggregation messages according to
     * its ruleset and puts them to the specified output link before
     * returning the number of messages flushed after the disfragmentation
     */
    private int flush(long currentTime, XQueue in, XQueue out, int rid,
        byte[] buffer, QuickCache cache) {
        int i, k, cid, dmask, oid, count = 0, dspBody;
        String[] keys, propertyName;
        long[] ruleInfo, outInfo;
        TextEvent inMessage;
        Aggregation aggr;
        MessageFilter filter;
        Map rule;
        String ruleName;
        count = cache.size();
        if (count <= 0)
            return 0;

        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;
        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        rule = (Map) ruleList.get(rid);
        filter = (MessageFilter) rule.get("Filter");
        propertyName = (String[]) rule.get("PropertyName");
        aggr = (Aggregation) rule.get("Aggregation");
        k = (aggr != null) ? aggr.getBodyType() : 0;
        // retrieve displayMask from RULE_OPTION
        dmask = (int) ruleInfo[RULE_OPTION];
        oid = outLinkMap[RESULT_OUT];
        outInfo = assetList.getMetaData(oid);
        if (ruleInfo[RULE_MODE] == 0) // static session
            keys = cache.sortedKeys();
        else { // dynamic sessions
            Object o;
            int[] info;
            long t, tm = currentTime + ruleInfo[RULE_TTL];
            List<String> list = new ArrayList<String>();
            for (String key : cache.keySet()) {
                if (!cache.isExpired(key, currentTime)) {
                    t = cache.getTimestamp(key) + cache.getTTL(key);
                    if (t < tm)
                        tm = t;
                    continue;
                }
                info = cache.getMetaData(key);
                if (info == null || info.length < 2 || info[1] <= 0)
                    continue;
                // disable the expired key
                info[1] = 0;
                list.add(key);
            }
            keys = list.toArray(new String[list.size()]);
            if (keys.length > 0)
                cache.setStatus(cache.getStatus(), tm);
        }

        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " flush: " + keys.length +
                " msgs for " + ruleName).send();

        count = 0;
        for (i=0; i<keys.length; i++) {
            cid = -1;
            inMessage = (TextEvent) cache.get(keys[i], 0);
            if (inMessage == null) {
                new Event(Event.WARNING, name + ": null msg in flush for "+
                    keys[i]).send();
                continue;
            }
            recoverMsgBody(k, ruleName, inMessage);
            if (dmask != 0) try {  // display the message
                String msgStr = null;
                if ((dmask & dspBody) > 0)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name + ": " + ruleName +
                    " flushed aggregated msg " + (count + 1) + ":" +
                    MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName)).send();
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

            cid = passthru(currentTime, inMessage, in, rid, 0, -1, 0);
            if (cid > 0) {
                count ++;
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_TIME] = currentTime;
            }
            else
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to flush the msg with the key " + keys[i]).send();
        }
        if (ruleInfo[RULE_MODE] == 0) // static session
            cache.clear();

        if (cache.size() <= 0) { // remove the empty cache from the list
            cacheList.getNextID(rid);
            cacheList.remove(rid);
        }
        else { // for dynamic sessions
            cache.disfragment(currentTime);
        }

        return count;
    }

    /**
     * queries the status of the primary destination and queue depth via local
     * data. It updates the cache status so that all cache sessions will be
     * adjusted according to the load level of the primary destination.
     * If the cache session is enabled, it also monitors the cache mtime so that
     * the cache will be flushed regularly.  It returns total number of msgs
     * flushed
     */
    private int update(long currentTime, XQueue in, XQueue out, byte[] buffer) {
        StringBuffer strBuf = null;
        Browser browser;
        QuickCache cache;
        Map r = null;
        Object o;
        long[] outInfo, ruleInfo;
        long dt;
        int i, d, oid, rid, n, size, count = 0;
        n = cacheList.size();
        if (n <= 0)
            return 0;
        int[] list = new int[n];

        oid = outLinkMap[RESULT_OUT];
        outInfo = assetList.getMetaData(oid);
        if (reportName != null) // report defined
            r = (Map) NodeUtils.getReport(reportName);

        if (reportName == null) // no report defined
            r = null;
        else if (r == null || (o = r.get("TestTime")) == null)
            new Event(Event.WARNING, name + ": no report found for " +
                reportName).send();
        else try { // check report
            dt = Long.parseLong((String) o);
            if (dt > outInfo[OUT_QTIME]) { // recent report
                outInfo[OUT_QTIME] = dt;
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
            if (d < threshold[LOAD_LOW]) { // low load
                if (cacheStatus == QuickCache.CACHE_OFF) { // enable session
                    cacheStatus = QuickCache.CACHE_ON;
                    new Event(Event.INFO, name + ": cache update has been " +
                        "enabled with " + n + " active caches").send();
                }
            }
            else if (d >= threshold[LOAD_MEDIUM]) { // high load
                if (cacheStatus == QuickCache.CACHE_ON) { // disable session
                    cacheStatus = QuickCache.CACHE_OFF;
                    new Event(Event.INFO, name + ": cache update has been " +
                        "disabled with " + n + " active caches").send();
                }
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to extrieve report data"+
                " from "+ reportName +": "+ Event.traceStack(e)).send();
        }

        if (cacheStatus == QuickCache.CACHE_OFF) // cache update disabled
            return 0;

        if ((debug & DEBUG_UPDT) > 0)
            strBuf = new StringBuffer();

        n = cacheList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            rid = list[i];
            cache = (QuickCache) cacheList.browse(rid);
            if (cache.size() <= 0) {
                cacheList.getNextID(rid);
                cacheList.remove(rid);
                continue;
            }
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PID] == TYPE_NONE || ruleInfo[RULE_TTL] <= 0)
                continue;
            dt = currentTime - (cache.getMTime() + ruleInfo[RULE_TTL]);
            if (dt >= 0) { // cache expired
                size = flush(currentTime, in, out, rid, buffer, cache);
                count += size;
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                        " flushed " +size+ " msgs due to timeout: "+dt).send();
            }
            if ((debug & DEBUG_UPDT) > 0)
                strBuf.append("\n\t" + ruleList.getKey(rid) + ": " + rid + " " +
                    ruleInfo[RULE_OID] + " " + ruleInfo[RULE_PID] + " " +
                    ruleInfo[RULE_TTL] + " " + ruleInfo[RULE_SIZE] + " " +
                    Event.dateFormat(new Date(ruleInfo[RULE_TIME])));
        }

        if ((debug & DEBUG_UPDT) > 0) {
            strBuf.append("\n\t In: Status Type Size Depth: " +
                in.getGlobalMask() + " " + 1 + " " +
                in.size() + " " + in.depth());
            strBuf.append("\n\tOut: OID Status NRule Size QDepth Time");
            strBuf.append("\n\t" + assetList.getKey(oid) + ": " + oid + " " +
                outInfo[OUT_STATUS] + " " + outInfo[OUT_NRULE] + " " +
                outInfo[OUT_SIZE] + " " + outInfo[OUT_QDEPTH] + " " +
                Event.dateFormat(new Date(outInfo[OUT_TIME])));
            new Event(Event.DEBUG, name + " RuleName: RID OID PID TTL " +
                "Size Time count=" + count + strBuf.toString()).send();
        }

        return count;
    }

    public void close() {
        Browser browser;
        QuickCache cache;
        Aggregation aggr;
        Object[] asset;
        Map rule;
        int rid;
        setStatus(NODE_CLOSED);
        cells.clear();
        msgList.clear();
        assetList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null) {
                aggr = (Aggregation) rule.get("Aggregation");
                if (aggr != null && aggr.hasBody())
                    aggr.clearBodyMap();
                cache = (QuickCache) rule.get("Cache");
                if (cache != null)
                    cache.clear();
                rule.clear();
            }
        }
        ruleList.clear();
        cacheList.clear();

        java.lang.reflect.Method closer;
        browser = pluginList.browser();
        while ((rid = browser.next()) >= 0) {
            asset = (Object[]) pluginList.get(rid);
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
    }
}
