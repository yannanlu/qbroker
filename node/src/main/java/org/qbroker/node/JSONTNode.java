package org.qbroker.node;

/* JSONTNode.java - a MessageNode transforming JSON payload of JMS Messages */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.QuickCache;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONTemplate;
import org.qbroker.jms.JSONFormatter;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * JSONTNode transforms JSON payload of JMS messages and the properties
 * according to rulesets. It filters messages into three outlinks: done
 * for all the processed messages, nohit for those messages do not belong
 * to any rulesets, failure for the messages failed in the transformations.
 *<br><br>
 * JSONTNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the transformation
 * parameters, such URITemplate for building the path to a JSON template file,
 * and TTL for the cache.
 *<br><br>
 * If a list of JSONFormatter is defined in the rule, the rule is for the
 * default formatter. In this case, the format operations are defined in
 * each of the items. JSONTNode will run them in the order and saves the
 * result to the message body.
 *<br><br>
 * If URITemplate is defined, it supports a simple JSON template with sections
 * and filters. The template file, referenced by URITemplate, will be compiled
 * and cached as JSON template. The format result will be converted to JSON
 * and set back to the message body. The cache count of templates for the
 * rule will be stored to RULE_PEND. The cache count will be updated when the
 * session times out which is determined by SessionTimeout.
 *<br><br>
 * JSONTNode allows developers to plugin their own formatters by specifying
 * the ClassName for the plugin.  The requirement is minimum.  The class should
 * have a public method of format() that takes a parsed JSON map and a parameter
 * map as the arguments. The return object must be a String of null meaning OK,
 * or an error message otherwise.  It must have a constructor taking a Map
 * as the only argument for configurations. Based on the map properties for
 * the constructor, developers should define configuration parameters in the
 * base of JSONFormatter. JSONTNode will pass the data to the plugin's
 * constructor as an opaque object during the instantiation of the plugin.
 * In the normal operation, JSONTNode will invoke the method to format
 * the json payload. The method should never acknowledge any message in any
 * case.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JSONTNode extends Node {
    private int sessionTimeout = 0;

    private QuickCache cache = null;     // for storing JSON templates
    private AssetList pluginList = null; // plugin method and object
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public JSONTNode(Map props) {
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
            operation = "format";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);

        cache = new QuickCache(name, QuickCache.META_ATAC, 0, 0);

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

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        i = outLinkMap[NOHIT_OUT];
        i = (i >= outLinkMap[FAILURE_OUT]) ? i : outLinkMap[FAILURE_OUT];
        if (++i > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

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
            throw(new IllegalArgumentException(name + ": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_TTL]/1000 + " " + ruleInfo[RULE_EXTRA] + " "+
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG,name+" RuleName: RID PID Option TTL EXTRA - "+
                "OutName" + strBuf.toString()).send();
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
        String key, str, ruleName, preferredOutName;
        char c = System.getProperty("path.separator").charAt(0);
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

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // plugin
            str = (String) o;
            if ((o = ph.get("JSONFormatter")) != null && o instanceof Map)
                str += "::" + JSON2Map.toJSON((Map) o, null, null);
            else
                throw(new IllegalArgumentException(ruleName +
                    ": JSONFormatter is not well defined for " + str));

            if (pluginList.containsKey(str)) {
                long[] meta;
                id = pluginList.getID(str);
                meta = pluginList.getMetaData(id);
                // increase the reference count
                meta[0] ++;
            }
            else {
                o = MessageUtils.getPlugins(ph, "JSONFormatter", "format",
                    new String[]{"java.util.Map","java.util.Map"},"close",name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            if ((o = ph.get("Parameter")) != null && o instanceof Map) {
                Template temp;
                Map<String, Object> params = new HashMap<String, Object>();
                Iterator iter = ((Map) o).keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    str = (String) ((Map) o).get(key);
                    temp = new Template(str);
                    if (temp == null || temp.numberOfFields() <= 0)
                        params.put(key, str);
                    else {
                        params.put(key, temp);
                        ruleInfo[RULE_EXTRA] ++;
                    }
                }
                rule.put("Parameter", params);
            }
            else {
                rule.put("Parameter", new HashMap<String, Object>());
            }

            // store the plugin ID in PID for the plugin
            ruleInfo[RULE_PID] = id;
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
        }
        else if ((o = ph.get("JSONFormatter")) != null && o instanceof List) {
            JSONFormatter ft = new JSONFormatter(ph);
            rule.put("Formatter", ft);
            ruleInfo[RULE_EXTRA] = ft.getSize();
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_JSONPATH;
        }
        else if ((o = ph.get("URITemplate")) != null && o instanceof String) {
            str = (String) o;
            rule.put("Template", new Template(str));
            if (( o = ph.get("URISubstitution")) != null && o instanceof String)
                rule.put("Substitution", new TextSubstitution((String)o));

            if ((o = ph.get("Parameter")) != null && o instanceof Map) {
                Template temp;
                Map<String, Object> params = new HashMap<String, Object>();
                Iterator iter = ((Map) o).keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    str = (String) ((Map) o).get(key);
                    temp = new Template(str);
                    if (temp == null || temp.numberOfFields() <= 0)
                        params.put(key, str);
                    else {
                        params.put(key, temp);
                        ruleInfo[RULE_EXTRA] ++;
                    }
                }
                rule.put("Parameter", params);
            }
            else {
                rule.put("Parameter", new HashMap());
            }

            ruleInfo[RULE_PID] = TYPE_JSONT;
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
        }
        else { // default to bypass
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
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
                JSONFormatter fm = (JSONFormatter) h.remove("Formatter");
                if (fm != null)
                    fm.clear();
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
                    JSONFormatter fm = (JSONFormatter) h.remove("Formatter");
                    if (fm != null)
                        fm.clear();
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

    @SuppressWarnings("unchecked")
    private int format(long currentTime, String uri, Map json, int rid,
        int ttl, Map params, Message msg) {
        int i;
        JSONTemplate template;
        String text = null;

        if (uri == null || uri.length() <= 0 || json == null)
            return FAILURE_OUT;

        template = (JSONTemplate) cache.get(uri, currentTime);
        if (template == null) try { // initialize the first one
            long[] info;
            FileReader fr = new FileReader(uri);
            Map ph = (Map) JSON2FmModel.parse(fr);
            fr.close();
            if (ph.get("name") == null)
                ph.put("name", uri);
            template = new JSONTemplate(ph);
            if (template != null) {
                i = cache.insert(uri, currentTime, ttl, new int[]{rid, 0},
                    template);
                info = ruleList.getMetaData(rid);
                if (info != null)
                    info[RULE_PEND] ++;
                if (ttl > 0) { // check mtime of the cache
                    long tm = cache.getMTime();
                    if (tm > currentTime + ttl)
                        cache.setStatus(cache.getStatus(), currentTime + ttl);
                }
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG,name+": a new template compiled for "+
                        uri + ": " + i).send();
            }
            else {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to load the template from " + uri).send();
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name +": " + ruleList.getKey(rid) +
               " failed to compile template for " + uri + ": " +
               Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        if (params != null) { // set parameters
            String str;
            Iterator iter = params.keySet().iterator();
            template.clearParameter();
            while (iter.hasNext()) { // set parameters
                str = (String) iter.next();
                if (str == null || str.length() <= 0)
                    continue;
                template.setParameter(str, params.get(str));
            }
        }

        try {
            text = template.format(json);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
               " failed to format json with template of " + uri + ": " +
               Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(text);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(text.getBytes());
            else {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to set body: bad msg family").send();
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to load JSON to msg: " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        return RESULT_OUT;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        String msgStr = null, ruleName = null;
        Map rule = null, params = null;
        Browser browser;
        Object[] asset;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        JSONTemplate template = null;
        JSONFormatter formatter = null;
        Template temp = null;
        TextSubstitution tsub = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, k, n, previousRid;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
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

        // initialize filters
        n = ruleList.size();
        filters = new MessageFilter[n];
        browser = ruleList.browser();
        ruleMap = new int[n];
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
            ruleMap[i++] = rid;
        }

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
            if (sessionTimeout > 0 &&
                currentTime - previousTime >= sessionTimeout) {
                if (currentTime >= cache.getMTime()) { // something expired
                    i = updateCacheCount(currentTime);
                    if (i > 0 && (debug & DEBUG_COLL) > 0)
                        new Event(Event.DEBUG, name + ": " + i +
                            " templates expired").send();
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
            rid = 0;
            i = 0;
            try {
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
                if (ruleInfo[RULE_PID] == TYPE_JSONT) {
                    temp = (Template) rule.get("Template");
                    tsub = (TextSubstitution) rule.get("Substitution");
                    params = (Map) rule.get("Paramenter");
                }
                else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) {
                    formatter = (JSONFormatter) rule.get("Formatter");
                }
                previousRid = rid;
            }

            if (i > 0) try {
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
                i = -1;
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to reset the msg: "+ Event.traceStack(e)).send();
            }

            if (i < 0)  { // failed to apply filters or failure on reset
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) { // for formatter
                Map json = null;
                i = RESULT_OUT;
                if (formatter == null) {
                    i = FAILURE_OUT;
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " got an empty formatter").send();
                }
                else if (msgStr == null) {
                    i = FAILURE_OUT;
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " got null content").send();
                }
                else try { // parse json payload and format msg
                    StringReader sr = new StringReader(msgStr);
                    json = (Map) JSON2FmModel.parse(sr);
                    sr.close();

                    i = formatter.format(json, inMessage);
                    i = (i < 0) ? FAILURE_OUT : RESULT_OUT;
                }
                catch (Exception e) {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the msg: "+
                        Event.traceStack(e)).send();
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid +" "+ i).send();

                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else if (ruleInfo[RULE_PID] == TYPE_JSONT) { // for json template
                Map json = null;
                String uri = null;
                if (temp != null) try { // retrieve uri
                    uri = MessageUtils.format(inMessage, buffer, temp);
                    if (tsub != null)
                        uri = tsub.substitute(uri);
                }
                catch (Exception e) {
                    uri = null;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to retrieve uri: "+Event.traceStack(e)).send();
                }

                if (uri != null && uri.length() > 0) { // uri is defined
                    i = RESULT_OUT;

                    if (msgStr == null)
                        i = FAILURE_OUT;
                    else if (i == RESULT_OUT) try { // parse json payload
                        StringReader sr = new StringReader(msgStr);
                        json = (Map) JSON2FmModel.parse(sr);
                        sr.close();
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to parse JSON payload: " +
                            Event.traceStack(e)).send();
                    }

                    if (i == RESULT_OUT && params != null &&
                        ruleInfo[RULE_EXTRA] > 0) { // populate params
                        String str;
                        Object o;
                        Iterator iter = params.keySet().iterator();
                        Map<String, Object> ph = new HashMap<String, Object>();
                        while (iter.hasNext()) { // set parameters
                            str = (String) iter.next();
                            if (str == null || str.length() <= 0)
                                continue;
                            o = params.get(str);
                            if (o == null)
                                continue;
                            else if (!(o instanceof Template))
                                ph.put(str, o);
                            else try {
                                ph.put(str, MessageUtils.format(inMessage,
                                    buffer, (Template) o));
                            }
                            catch (Exception e) {
                                i = FAILURE_OUT;
                                new Event(Event.ERR, name + ": " + ruleName +
                                    " failed to get params for " + str + ": "+
                                    Event.traceStack(e)).send();
                                break;
                            }
                        }

                        if (i == RESULT_OUT) {
                            i = format(currentTime, uri, json, rid,
                                (int) ruleInfo[RULE_TTL], ph, inMessage);
                        }
                    }
                    else if (i == RESULT_OUT) {
                        i = format(currentTime, uri, json, rid,
                            (int) ruleInfo[RULE_TTL], params, inMessage);
                    }
                }
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " got an empty uri for the json template").send();
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid +" "+ i).send();

                if (i == FAILURE_OUT) // disable post formatter
                    filter = null;
                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else if ((k = (int) ruleInfo[RULE_PID]) >= 0) { // for plugins
                Map json = null;
                asset = (Object[]) pluginList.get(k);
                Object obj = asset[1];
                Method method = (Method) asset[0];
                if (method != null) { // method is defined
                    Map<String, Object> ph = null;
                    i = RESULT_OUT;

                    if (msgStr == null)
                        i = FAILURE_OUT;
                    if (i == RESULT_OUT) try { // parse json payload
                        StringReader sr = new StringReader(msgStr);
                        json = (Map) JSON2FmModel.parse(sr);
                        sr.close();
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to parse JSON payload: " +
                            Event.traceStack(e)).send();
                    }

                    if (i == RESULT_OUT && params != null &&
                        ruleInfo[RULE_EXTRA] > 0) { // populate params
                        String str;
                        Object o;
                        Iterator iter = params.keySet().iterator();
                        ph = new HashMap<String, Object>();
                        while (iter.hasNext()) { // set parameters
                            str = (String) iter.next();
                            if (str == null || str.length() <= 0)
                                continue;
                            o = params.get(str);
                            if (o == null)
                                continue;
                            else if (!(o instanceof Template))
                                ph.put(str, o);
                            else try {
                                ph.put(str, MessageUtils.format(inMessage,
                                    buffer, (Template) o));
                            }
                            catch (Exception e) {
                                i = FAILURE_OUT;
                                new Event(Event.ERR, name + ": " + ruleName +
                                    " failed to get params for " + str + ": "+
                                    Event.traceStack(e)).send();
                                break;
                            }
                        }
                    }

                    if (i == RESULT_OUT) try {
                        String str = (String) method.invoke(obj,
                            new Object[]{json, ph});
                        if (str != null) {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to invoke plugin: "+ str).send();
                        }
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to invoke plugin: "+
                            Event.traceStack(e)).send();
                    }

                    if (i == RESULT_OUT) try {
                        String str = JSON2FmModel.toJSON(json, null, null);
                        inMessage.clearBody();
                        if (inMessage instanceof TextMessage)
                            ((TextMessage) inMessage).setText(str);
                        else if (inMessage instanceof BytesMessage)
                           ((BytesMessage)inMessage).writeBytes(str.getBytes());
                        else {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to set body: bad msg family").send();
                        }
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to load JSON to msg: " +
                            Event.traceStack(e)).send();
                    }
                }
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " got a null method for the plugin").send();
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid +" "+ i).send();

                if (i == FAILURE_OUT) // disable post formatter
                    filter = null;
                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else { // preferred ruleset or nohit
                if (i == 0) // disable post formatter for nohit
                    filter = null;
                oid = (int) ruleInfo[RULE_OID];
            }

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                new Event(Event.INFO, name +": "+ ruleName +" formatted msg "+
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try { // post format
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
     * updates the cache count for each rules and disfragments cache items
     */
    private int updateCacheCount(long currentTime) {
        int k, n, rid;
        long t, tm;
        int[] info, count;
        long[] ruleInfo;
        Set<String> keys;
        if (cache.size() <= 0) {
            cache.setStatus(cache.getStatus(), currentTime + sessionTimeout);
            return 0;
        }
        k = ruleList.size();
        if (k <= 1) { // no rules except for nohit
            cache.clear();
            cache.setStatus(cache.getStatus(), currentTime + sessionTimeout);
            return 0;
        }
        tm = cache.getMTime() + sessionTimeout;
        count = new int[k];
        for (rid=0; rid<k; rid++)
            count[rid] = 0;
        tm = cache.getMTime();
        n = 0;
        keys = cache.keySet();
        for (String key : keys) {
            if (cache.isExpired(key, currentTime)) {
                n ++;
                continue;
            }
            info = cache.getMetaData(key);
            if (info == null || info.length <= 0)
                continue;
            rid = info[0];
            if (rid < k)
                count[rid] ++;
            t = cache.getTTL(key);
            if (t > 0) { // not evergreeen
                t += cache.getTimestamp(key);
                if (tm > t)
                    tm = t;
            }
        }

        for (rid=0; rid<k; rid++) { // update cache count for rules
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo == null)
                continue;
            ruleInfo[RULE_PEND] = count[rid];
        }

        if (n > 0)
            cache.disfragment(currentTime);
        cache.setStatus(cache.getStatus(), tm);

        return n;
    }

    /** overwrites the implementation for storing total cache in NOHIT */
    public int getMetaData(int type, int id, long[] data) {
        int i = super.getMetaData(type, id, data);
        if (type != META_RULE || i != 0)
            return i;
        data[RULE_PEND] = cache.size();
        return i;
    }

    public void close() {
        Browser browser;
        JSONFormatter formatter;
        Map rule;
        int rid;
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null) {
                formatter = (JSONFormatter) rule.remove("Formatter");
                if (formatter != null)
                    formatter.clear();
            }
        }
        super.close();
        cache.clear();
        pluginList.clear();
    }

    protected void finalize() {
        close();
    }
}
