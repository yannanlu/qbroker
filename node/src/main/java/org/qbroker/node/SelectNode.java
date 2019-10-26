package org.qbroker.node;

/* SelectNode.java - a MessageNode selecting content of messages into a batch */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Date;
import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import javax.xml.parsers.DocumentBuilder;
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
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.MessageInputStream;
import org.qbroker.jms.MessageStream;
import org.qbroker.jms.TextEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * SelectNode parses the payload of JMS Messages and selects the portions of
 * the content according to predefined rules.  For each selected chunk of the
 * content, SelectNode creates a new TextMessage and stores the data into it.
 * It also copies the properties of the original message into the new message
 * and passes it through the outlink of done.  According to the rulesets,
 * SelectNode routes the incoming messages into three outlinks: bypass for
 * those original messages, nohit for those messages do not belong to any
 * rulesets, failure for the messages failed at the selection process.
 * However, the outlink of done can be shared with other outlinks. Since the
 * payload of the message may be very large, SelectNode tries to ignore any
 * filters on message body. It will not display message body of the incoming
 * messages either.
 *<br/><br/>
 * SelectNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each ruleset
 * defines a unique message group.  The ruleset also defines the operations
 * to select content from the messages and other parameters for the group.
 * If a ruleset has the preferredOutLink defined, SelectNode will route the
 * messages to its outlink without any actions. Otherwise, each selected item
 * will be built into a message for delivery. StringProperty determines what
 * properties to be copied over from the original message to the new messages.
 * If it is null, nothing will be copied over.  If it is empty, all properties
 * will be copied over.  Otherwise, only those properties defined in
 * StringProperty will be copied over. If StoreField is defined for a ruleset,
 * it must be a full path to a local file. SelectNode will retrieve the content
 * from the external file. This is good in case the size of content is too
 * large for a message. All the delivered new messages will be tracked by
 * RULE_PEND of their rulesets.
 *<br/><br/>
 * The default operation is to split the content via the given delimiters.
 * The split items will be selected for the ruleset. Alternatively, SelectNode
 * also supports other 3x2 different ways to select items from the message
 * payload. They are based on Pattern, XPath or JSONPath. For example,
 * if Pattern is defined in a ruleset, SelectNode will parse the content with
 * the pattern. All the matched items will be selected for the ruleset. If
 * XPath is defined, SelectNode will use it to select DOM nodes from
 * the XML document. The text representation of each selected DOM node will be
 * stored to the outgoing message. Both Pattern and XPath support selections
 * with dynamic variables. For either Pattern or XPath, the value can be a Map
 * contains multiple static patterns or XPaths. SelectNode will use each of
 * them to select items. Their keys will be set to the TagField on the new
 * messages.
 *<br/><br/>
 * SelectNode also allows developers to plug-in their own methods to select.
 * In this case, the full ClassName of the selection implementation and its
 * SelectorArguments must be well defined in the rulesets.  The requirement on
 * the plug-ins is minimum.  The class must have a public method of
 * select(String text) that takes a String as the only argument. The returned
 * object must be an array of String for selected items on success.  Each of
 * the array element will be stored into the newly created message. In case
 * of failure, null should be returned. It should also have a constructor
 * taking a Map, or a List, or just a String as the only argument for the
 * configurations.  SelectNode will invoke its public method to get the list
 * of the selected items from either message body or certain message property
 * of each incoming message.
 *<br/><br/>
 * If CountField is defined for a ruleset, the total count of selected items
 * will be set to the specified field of the incoming messages. In case of the
 * split operation with given delimiters, the total count will be set at the
 * end of the operation. It means those new messages will not have this
 * property set. For other cases, the total count will be set right after the
 * items are selected. Therefore, the total count can be copied over to the
 * newly created messages.  The downstream can use this count to terminate the
 * aggregation sessions or loops. In order to set the property at CountField,
 * the incoming message must be writeable. In most of the cases, you can use
 * the receivers to set a dummy property on all incoming messages since
 * it will make them writeable.
 *<br/><br/>
 * If XAMode is defined in a rulset, it will overwrite the default value
 * defined on the node level. If the XAMode of a ruleset is on, SelectNode
 * will make sure all the outgoing messages are delivered before routing the
 * original message to bypass. Otherwise, the original message will be routed
 * to bypass right after those newly created messages dispatched. In this case,
 * SelectNode will not wait on their delivery.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * SelectNode always assumes the first outlink for done, the second for bypass,
 * the third for failure and the last for nohit.  They all can share the same
 * name, too.  For the first outlink, if it is partitioned, you must define
 * the same Partition explicitly on every selection ruleset.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SelectNode extends Node {
    private int[] outLinkMap;
    private AssetList pluginList = null; // plugin method and object
    private Perl5Compiler pc = null;
    private Perl5Matcher pm = null;
    private XPath xpath = null;
    private DocumentBuilder builder = null;

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int STORE_MSG = 0;
    private final static int STORE_FILE = 1;

    public SelectNode(Map props) {
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
            operation = "select";
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
                    ruleInfo[RULE_MODE] + " " + ruleInfo[RULE_EXTRA] + " " +
                    ruleInfo[RULE_TTL] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name +
                " RuleName: RID PID Option Mode Extra TTL - OutName"+
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
        Template temp;
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
        rule.put("Filter", new MessageFilter(hmap));
        hmap.clear();
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        // displayMask of ruleset stored in dmask
        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        // xa mode of ruleset stored in mode
        if ((o = ph.get("XAMode")) != null && o instanceof String)
            ruleInfo[RULE_MODE] = Integer.parseInt((String) o);
        else // default
            ruleInfo[RULE_MODE] = xaMode;

        // store option of ruleset stored in option
        if ((o = ph.get("StoreField")) != null && o instanceof String) {
            rule.put("StoreField", (String) o);
            ruleInfo[RULE_OPTION] = STORE_FILE;
        }
        else
            ruleInfo[RULE_OPTION] = STORE_MSG;

        if ((o = ph.get("CountField")) != null && o instanceof String)
            rule.put("CountField", (String) o);

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // for plugin
            str = (String) o;
            if ((o = ph.get("SelectorArgument")) != null) {
                if (o instanceof List)
                    str += "::" + JSON2Map.toJSON((Map) o, null, null);
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
                o = MessageUtils.getPlugins(ph, "SelectorArgument", "select",
                    new String[]{"java.lang.String"}, null, name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            // store the plugin ID in PID
            ruleInfo[RULE_PID] = id;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];

        }
        else if ((o = ph.get("Pattern")) != null && // for pattern list
            o instanceof String && ((String) o).length() > 0) {
            try { // compile the pattern
                if (pc == null) {
                    pc = new Perl5Compiler();
                    pm = new Perl5Matcher();
                }
                temp = new Template((String) o);
                if (temp != null && temp.numberOfFields() > 0){//dynamic pattern
                    rule.put("Pattern", temp);
                    ruleInfo[RULE_EXTRA] = -1;
                }
                else // static pattern
                    rule.put("Pattern", pc.compile("(" + (String) o + ")"));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": " + ruleName +
                    " failed to compile the pattern: " + e.toString()));
            }
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_PARSER;
        }
        else if ((o = ph.get("Pattern")) != null && // for pattern map
            o instanceof Map && ((Map) o).size() > 0) {
            Map<String, Object> map = Utils.cloneProperties((Map) o);
            ruleInfo[RULE_EXTRA] = map.size();
            String[] keys = map.keySet().toArray(new String[map.size()]);
            try { // compile the pattern
                if (pc == null) {
                    pc = new Perl5Compiler();
                    pm = new Perl5Matcher();
                }
                for (String ky : keys) {
                    if (ky == null || ky.length() <= 0)
                        continue;
                    o = map.remove(ky);
                    if (o == null || !(o instanceof String) ||
                        ((String) o).length() <= 0)
                        continue;
                    map.put(ky, pc.compile("(" + (String) o + ")"));
                }
                if (map.size() == ruleInfo[RULE_EXTRA])
                    rule.put("XPath", map);
                else
                    throw(new IllegalArgumentException(name + ": " + ruleName +
                        " lost components for pattern: " + map.size()));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": " + ruleName +
                    " failed to compile the pattern: " + e.toString()));
            }
            if ((o = ph.get("TagField")) != null && o instanceof String)
                rule.put("TagField", (String) o);
            else
                rule.put("TagField", "ItemTag");
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_PARSER;
        }
        else if ((o = ph.get("XPath")) != null && // for xpath list
            o instanceof String && ((String) o).length() > 0) {
            try { // compile the expression 
                if (xpath == null) {
                    builder = Utils.getDocBuilder();
                    xpath = XPathFactory.newInstance().newXPath();
                }
                temp = new Template((String) o);
                if (temp != null && temp.numberOfFields() > 0) { //dynamic xpath
                    rule.put("XPath", temp);
                    ruleInfo[RULE_EXTRA] = -1;
                }
                else // static xpath
                    rule.put("XPath", xpath.compile((String) o));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": " + ruleName +
                    " failed to compile the xpath: " + e.toString()));
            }
            if ((o = ph.get("CheckNameSpace")) != null &&
                "true".equalsIgnoreCase((String) o))
                ruleInfo[RULE_TTL] = 1;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_XPATH;
        }
        else if ((o = ph.get("XPath")) != null && // for xpath map
            o instanceof Map && ((Map) o).size() > 0) {
            Map<String, Object> map = Utils.cloneProperties((Map) o);
            ruleInfo[RULE_EXTRA] = map.size();
            String[] keys = map.keySet().toArray(new String[map.size()]);
            try { // compile the expression 
                if (xpath == null) {
                    builder = Utils.getDocBuilder();
                    xpath = XPathFactory.newInstance().newXPath();
                }
                for (String ky : keys) {
                    if (ky == null || ky.length() <= 0)
                        continue;
                    o = map.remove(ky);
                    if (o == null || !(o instanceof String) ||
                        ((String) o).length() <= 0)
                        continue;
                    map.put(ky, xpath.compile((String) o));
                }
                if (map.size() == ruleInfo[RULE_EXTRA])
                    rule.put("XPath", map);
                else
                    throw(new IllegalArgumentException(name + ": " + ruleName +
                        " lost components for xpath: " + map.size()));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": " + ruleName +
                    " failed to compile the xpath: " + e.toString()));
            }
            if ((o = ph.get("TagField")) != null && o instanceof String)
                rule.put("TagField", (String) o);
            else
                rule.put("TagField", "ItemTag");
            if ((o = ph.get("CheckNameSpace")) != null &&
                "true".equalsIgnoreCase((String) o))
                ruleInfo[RULE_TTL] = 1;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_XPATH;
        }
        else if ((o = ph.get("JSONPath")) != null && // for JSON list
            o instanceof String && ((String) o).length() > 0) {
            rule.put("JSONPath", (String) o);
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_JSONPATH;
        }
        else if ((o = ph.get("JSONPath")) != null && // for JSON map
            o instanceof Map && ((Map) o).size() > 0) {
            Map<String, Object> map = Utils.cloneProperties((Map) o);
            ruleInfo[RULE_EXTRA] = map.size();
            rule.put("JSONPath", map);
            if ((o = ph.get("TagField")) != null && o instanceof String)
                rule.put("TagField", (String) o);
            else
                rule.put("TagField", "ItemTag");
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_JSONPATH;
        }
        else { // for split ruleset
            MessageStream ms = new MessageStream(ph);
            rule.put("MessageStream", ms);
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_SPLIT;
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // StringProperties control what to copied over
        if ((o = ph.get("StringProperty")) != null &&
            o instanceof Map) { // copy selected properties
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
        else if (o != null) // copy all properties
            rule.put("PropertyName", new String[0]);

        return rule;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        InputStream bin;
        String ruleName = null, storeField = null;
        String countField = null, tagField = null, keyPath = null;
        MessageStream ms = null;
        Object o;
        Object[] asset;
        Map rule = null;
        Map map = null;
        Browser browser;
        XPathExpression xpe = null;
        Pattern pattern = null;
        MessageFilter[] filters = null;
        String[] propertyName = null, propertyValue = null;
        byte[] buffer = new byte[4096];
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, wt;
        long count = 0;
        int mask, ii, sz, dmask = 0;
        int i = 0, n, previousRid, size;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
        int oid = 0; // the id of the output queue
        boolean xa = ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

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

        n = ruleMap.length;
        outInfo = assetList.getMetaData(0);
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
            rid = 0;
            i = 0;
            try {
                for (i=1; i<n; i++) {
                    if (filters[i].evaluate(inMessage, null)) {
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
                if (ruleInfo[RULE_PID] != TYPE_BYPASS) {
                    ms = (MessageStream) rule.get("MessageStream");
                    storeField = (String) rule.get("StoreField");
                    countField = (String) rule.get("CountField");
                    propertyName = (String[]) rule.get("PropertyName");
                    if (ruleInfo[RULE_PID] == TYPE_PARSER) {
                        if (ruleInfo[RULE_EXTRA] == 0) // static pattern
                            pattern = (Pattern) rule.get("Pattern");
                        else if (ruleInfo[RULE_EXTRA] > 0) { // pattern map
                            map = (Map) rule.get("Pattern");
                            tagField = (String) rule.get("TagField");
                        }
                    }
                    else if (ruleInfo[RULE_PID] == TYPE_XPATH) {
                        if (ruleInfo[RULE_EXTRA] == 0) // static xpath
                            xpe = (XPathExpression) rule.get("XPath");
                        else if (ruleInfo[RULE_EXTRA] > 0) { // xpath map
                            map = (Map) rule.get("XPath");
                            tagField = (String) rule.get("TagField");
                        }
                    }
                    else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) {
                        if (ruleInfo[RULE_EXTRA] == 0) { // static jsonpath
                            keyPath = (String) rule.get("JSONPath");
                            tagField = null;
                        }
                        else if (ruleInfo[RULE_EXTRA] > 0) { // jsonpath map
                            map = (Map) rule.get("JSONPath");
                            tagField = (String) rule.get("TagField");
                        }
                    }
                }
                previousRid = rid;
            }

            size = 0;
            if (i < 0) // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
            if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // for bypass or nohit
                oid = (int) ruleInfo[RULE_OID];
            }
            else try { // select content from message
                File file = null;
                String[] batch = null, keys = null;
                int k;

                if (ruleInfo[RULE_MODE] > 0) { // make sure out is empty
                    k = collectAll(currentTime, in, out[0], 0, outInfo);
                    if (k < 0) { // standby temporarily
                        in.putback(cid);
                        break;
                    }
                }

                dmask = (int) ruleInfo[RULE_DMASK];
                if (ruleInfo[RULE_PID] >= 0) { // for plugins
                    java.lang.reflect.Method method;
                    Object obj;
                    String msgStr;

                    // retrieve plugin ID from PID
                    k = (int) ruleInfo[RULE_PID];
                    asset = (Object[]) pluginList.get(k);
                    obj = asset[1];
                    method = (java.lang.reflect.Method) asset[0];
                    if (method == null)
                        throw new IllegalArgumentException(ruleName +
                            ": null select method");
                    if (ruleInfo[RULE_OPTION] == STORE_FILE) { // load content
                        String filename;
                        filename = MessageUtils.getProperty(storeField,
                            inMessage);
                        if (filename == null)
                            throw new IOException(name +
                                " no filename found for " + ruleName +
                                " at " + storeField);
                        file = new File(filename);
                        bin = new FileInputStream(file);
                        msgStr = Utils.read(bin, buffer);
                        bin.close();
                    }
                    else { // retrieve content
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    }
                    try {
                        o = method.invoke(obj, new Object[] {msgStr});
                        msgStr = null;
                    }
                    catch (Exception e) {
                        msgStr = null;
                        throw new JMSException(ruleName + " plugin failed: "+
                             e.toString());
                    }
                    if (o == null || !(o instanceof String[]))
                       throw new JMSException(ruleName+" plugin returned null");
                    batch = (String[]) o;
                    bin = null;
                }
                else if (ruleInfo[RULE_PID] == TYPE_PARSER) { // for pattern
                    int bn;
                    String msgStr;
                    PatternMatcherInput pin;
                    if (ruleInfo[RULE_EXTRA] < 0) { // dynamic pattern
                        Template temp = (Template) rule.get("Pattern");
                        msgStr = MessageUtils.format(inMessage, buffer, temp);
                        pattern = pc.compile(msgStr);
                    }
                    if (ruleInfo[RULE_OPTION] == STORE_FILE) { // load content
                        String filename;
                        filename = MessageUtils.getProperty(storeField,
                            inMessage);
                        if (filename == null)
                            throw new IOException(name +
                                " no filename found for " + ruleName +
                                " at " + storeField);
                        file = new File(filename);
                        bin = new FileInputStream(file);
                        msgStr = Utils.read(bin, buffer);
                        bin.close();
                    }
                    else { // retrieve content
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    }

                    if (ruleInfo[RULE_EXTRA] <= 0) { // pattern list
                        List<String> list = new ArrayList<String>();
                        pin = new PatternMatcherInput(msgStr);
                        while (pm.contains(pin, pattern)) {
                            msgStr = pm.getMatch().group(1);
                            if (msgStr != null && msgStr.length() > 0)
                                list.add(msgStr);
                        }
                        pin = null;
                        msgStr = null;
                        bn = list.size();
                        batch = list.toArray(new String[bn]);
                        list.clear();
                    }
                    else { // pattern map
                        StringBuffer strBuf;
                        String str;
                        asset = map.keySet().toArray();
                        bn = (int) ruleInfo[RULE_EXTRA];
                        batch = new String[bn];
                        for (i=0; i<bn; i++) {
                            keys[i] = (String) asset[i];
                            pattern = (Pattern) map.get(keys[i]);
                            pin = new PatternMatcherInput(msgStr);
                            strBuf = new StringBuffer();
                            while (pm.contains(pin, pattern)) {
                                str = pm.getMatch().group(1);
                                if (str != null && str.length() > 0)
                                    strBuf.append(str);
                            }
                            pin = null;
                            batch[i] = strBuf.toString();
                        }
                        msgStr = null;
                    }
                    bin = null;
                }
                else if (ruleInfo[RULE_PID] == TYPE_XPATH) { // for xpath
                    int bn;
                    String msgStr;
                    Reader rin;
                    Document doc;
                    if (ruleInfo[RULE_EXTRA] < 0) { // dynamic xpath 
                        Template temp = (Template) rule.get("XPath");
                        msgStr = MessageUtils.format(inMessage, buffer, temp);
                        xpe = xpath.compile(msgStr);
                    }
                    if (ruleInfo[RULE_OPTION] == STORE_FILE) {
                        String filename;
                        filename = MessageUtils.getProperty(storeField,
                            inMessage);
                        if (filename == null)
                            throw new IOException(name +
                                " no filename found for " + ruleName +
                                " at " + storeField);
                        file = new File(filename);
                        rin = new FileReader(file);
                    }
                    else {
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                        rin = new StringReader(msgStr);
                    }

                    doc = builder.parse(new InputSource(rin));
                    if (ruleInfo[RULE_EXTRA] <= 0) { // xpath list
                        o = xpe.evaluate(doc, XPathConstants.NODESET);
                        msgStr = null;
                        rin.close();
                        if (o != null && o instanceof NodeList) {
                            String nsAttrs = null;
                            NodeList list = (NodeList) o;
                            bn = list.getLength();
                            if (ruleInfo[RULE_TTL] > 0 && bn > 0)
                                nsAttrs=Utils.getParentNameSpaces(list.item(0));
                            batch = new String[bn];
                            for (i=0; i<bn; i++) {
                                batch[i] = Utils.nodeToText(list.item(i));
                                if (nsAttrs != null) { // add name space attrs
                                    int j = batch[i].indexOf(">");
                                    if (j <= 0)
                                        continue;
                                    batch[i] = batch[i].substring(0, j) + " " +
                                        nsAttrs + batch[i].substring(j);
                                }
                            }
                        }
                        else
                            batch = new String[0];
                    }
                    else { // xpath map
                        String nsAttrs;
                        NodeList list;
                        asset = map.keySet().toArray();
                        bn = (int) ruleInfo[RULE_EXTRA];
                        batch = new String[bn];
                        for (i=0; i<bn; i++) {
                            keys[i] = (String) asset[i];
                            xpe = (XPathExpression) map.get(keys[i]);
                            list = (NodeList) xpe.evaluate(doc,
                                XPathConstants.NODESET);
                            if (ruleInfo[RULE_TTL] > 0 && list != null &&
                                list.getLength() > 0)
                                nsAttrs=Utils.getParentNameSpaces(list.item(0));
                            else
                                nsAttrs = null;
                            batch[i] = "<" + keys[i] +
                                ((nsAttrs == null) ? ">" : " " + nsAttrs + ">")+
                                Utils.nodeListToText(list)+"</" + keys[i] + ">";
                        }
                        msgStr = null;
                        rin.close();
                    }
                    bin = null;
                }
                else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) { // for jsonpath
                    int bn;
                    String msgStr;
                    Reader rin;
                    if (ruleInfo[RULE_EXTRA] <= 0) { // json path list
                        if (ruleInfo[RULE_OPTION] == STORE_FILE) {
                            String filename;
                            filename = MessageUtils.getProperty(storeField,
                                inMessage);
                            if (filename == null)
                                throw new IOException(name +
                                    " no filename found for " + ruleName +
                                    " at " + storeField);
                            file = new File(filename);
                            rin = new FileReader(file);
                        }
                        else {
                            msgStr = MessageUtils.processBody(inMessage,buffer);
                            if (msgStr != null && msgStr.length() > 0)
                                rin = new StringReader(msgStr);
                            else
                                rin = new StringReader("{}");
                        }

                        o = JSON2Map.parse(rin);
                        rin.close();
                        if (o != null) { // json parsed
                            if (o instanceof Map) // for json path
                                o = JSON2Map.get((Map) o, keyPath);
                            else
                                o = JSON2Map.get((List) o, keyPath);
                        }
                        msgStr = null;
                        if (o == null)
                            batch = new String[0];
                        else if (o instanceof List) { // array
                            List list = (List) o;
                            bn = list.size();
                            batch = new String[bn];
                            for (i=0; i<bn; i++) {
                                o = list.get(i);
                                if (o == null)
                                    batch[i] = "";
                                else if (o instanceof String)
                                    batch[i] = (String) o;
                                else if (o instanceof Map)
                                    batch[i] = JSON2Map.toJSON((Map) o,"","\n");
                                else if (o instanceof List)
                                    batch[i] = JSON2Map.toJSON((List)o,"","\n");
                                else
                                    batch[i] = o.toString();
                            }
                        }
                        else if (o instanceof Map) {
                            batch = new String[1];
                            batch[0] = JSON2Map.toJSON((Map) o, "", "\n");
                        }
                        else { // for string or others
                            batch = new String[1];
                            if (o instanceof String)
                                batch[0] = (String) o;
                            else
                                batch[0] = o.toString();
                        }
                    }
                    else if (ruleInfo[RULE_OPTION] != STORE_FILE) { // json
                        String str;
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                        if (msgStr == null || msgStr.length() <= 0)
                            msgStr = "{}";
                        asset = map.keySet().toArray();
                        bn = (int) ruleInfo[RULE_EXTRA];
                        batch = new String[bn];
                        for (i=0; i<bn; i++) {
                            keys[i] = (String) asset[i];
                            str = (String) map.get(keys[i]);
                            rin = new StringReader(msgStr);
                            o = JSON2Map.parse(rin);
                            rin.close();
                            if (o != null) { // parsed
                                if (o instanceof List)
                                    o = JSON2Map.get((List) o, str);
                                else
                                    o = JSON2Map.get((Map) o, str);
                            }
                            if (o == null)
                                batch[i] = "";
                            else if (o instanceof String)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    (String) o + "}";
                            else if (o instanceof Map)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    JSON2Map.toJSON((Map) o, "", "\n")+ "}";
                            else if (o instanceof List)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    JSON2Map.toJSON((List)o,"","\n") + "}";
                            else
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    o.toString() + "}";
                        }
                        msgStr = null;
                    }
                    else { // jsonpath map and stored to file
                        String str;
                        asset = map.keySet().toArray();
                        bn = (int) ruleInfo[RULE_EXTRA];
                        batch = new String[bn];
                        msgStr = MessageUtils.getProperty(storeField,inMessage);
                        if (msgStr == null)
                            throw new IOException(name +
                                " no filename found for " + ruleName +
                                " at " + storeField);
                        file = new File(msgStr);
                        msgStr = null;
                        for (i=0; i<bn; i++) {
                            keys[i] = (String) asset[i];
                            str = (String) map.get(keys[i]);
                            rin = new FileReader(file);
                            o = JSON2Map.parse(rin);
                            rin.close();
                            if (o != null) {
                                if (o instanceof List)
                                    o = JSON2Map.get((List) o, str);
                                else
                                    o = JSON2Map.get((Map) o, str);
                            }
                            if (o == null)
                                batch[i] = "";
                            else if (o instanceof String)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    (String) o + "}";
                            else if (o instanceof Map)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    JSON2Map.toJSON((Map) o, "", "\n")+ "}";
                            else if (o instanceof List)
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    JSON2Map.toJSON((List)o,"","\n") + "}";
                            else
                                batch[i] = "{\"" + keys[i] + "\":" +
                                    o.toString() + "}";
                        }
                    }
                    bin = null;
                }
                else if (ruleInfo[RULE_OPTION] == STORE_FILE) {
                    String filename;
                    filename = MessageUtils.getProperty(storeField,
                        inMessage);
                    if (filename == null)
                        throw new IOException(name + " no filename found for " +
                            ruleName + " at " + storeField);
                    file = new File(filename);
                    bin = new FileInputStream(file);
                }
                else if (inMessage instanceof TextMessage)
                    bin = new ByteArrayInputStream(
                        ((TextMessage) inMessage).getText().getBytes());
                else if (inMessage instanceof BytesMessage)
                    bin = new MessageInputStream((BytesMessage) inMessage);
                else
                    throw(new JMSException(name +" message from "+ in.getName()+
                        " not the type of Text or Bytes"));

                // set count to countField
                if (countField != null && ms == null) try {
                    i = (batch != null) ? batch.length : -1;
                    MessageUtils.setProperty(countField, String.valueOf(i),
                        inMessage);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to set count of " + i + " to " +countField+
                        ": " + Event.traceStack(e)).send();
                }

                if (propertyName == null) { // nothing to copy over
                    if (ms != null)
                        size = (int) ms.read(bin, out[0], null, null);
                    else if (keys != null)
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            null, null, tagField, keys);
                    else
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            null, null, null, null);
                }
                else if (propertyName.length > 0) { // something to copy over
                    propertyValue = new String[propertyName.length];
                    for (i=0; i<propertyName.length; i++)
                        propertyValue[i] =
                            MessageUtils.getProperty(propertyName[i],inMessage);

                    if (ms != null)
                        size = (int) ms.read(bin, out[0], propertyName,
                            propertyValue);
                    else if (keys != null)
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            propertyName, propertyValue, tagField, keys);
                    else
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            propertyName, propertyValue, null, null);
                }
                else { // copy all properties over including correlationID
                    String key, str;
                    List<String> list = new ArrayList<String>();
                    Enumeration propNames = inMessage.getPropertyNames();
                    while (propNames.hasMoreElements()) {
                        key = (String) propNames.nextElement();
                        if (key == null || key.equals("null"))
                            continue;
                        list.add(key);
                    }
                    str = inMessage.getJMSType();
                    if (str != null && str.length() > 0)
                        list.add(MessageUtils.getPropertyID("JMSType"));
                    str = inMessage.getJMSCorrelationID();
                    if (str == null || str.length() <= 0) {
                        str = inMessage.getJMSMessageID();
                        if (str != null && str.length() > 0)
                            inMessage.setJMSCorrelationID(str);
                    }
                    if (str != null && str.length() > 0)
                       list.add(MessageUtils.getPropertyID("JMSCorrelationID"));
                    i = list.size();
                    if (i > 0) {
                        String[] pNames = new String[i];
                        String[] pValues = new String[i];
                        for (int j=0; j<i; j++) {
                            pNames[j] = list.get(j);
                            pValues[j] =
                                MessageUtils.getProperty(pNames[j], inMessage);
                        }
                        list.clear();
                        if (ms != null)
                            size = (int) ms.read(bin, out[0], pNames, pValues);
                        else if (keys != null)
                            size = flush(currentTime, batch, in, rid, 0, dmask,
                                pNames, pValues, tagField, keys);
                        else
                            size = flush(currentTime, batch, in, rid, 0, dmask,
                                pNames, pValues, null, null);
                    }
                    else if (ms != null)
                        size = (int) ms.read(bin, out[0], null, null);
                    else if (keys != null)
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            null, null, tagField, keys);
                    else
                        size = flush(currentTime, batch, in, rid, 0, dmask,
                            null, null, null, null);
                }
                if (size > 0) // reset count since it has already been updated
                    outInfo[OUT_COUNT] -= size;

                if (ms == null)
                    batch = null;
                else {
                    if (bin != null) try {
                        bin.close();
                        bin = null;
                    }
                    catch (Exception ex) {
                    }

                    if (file != null) try {
                        file.delete();
                    }
                    catch (Exception ex) {
                    }
                }

                // update stats
                if (size > 0 && ruleInfo[RULE_MODE] > 0) { // wait to collect
                    currentTime = System.currentTimeMillis();
                    outInfo[OUT_SIZE] += size;
                    outInfo[OUT_TIME] = currentTime;
                    ruleInfo[RULE_SIZE] += size;
                    ruleInfo[RULE_TIME] = currentTime;
                    k = collectAll(currentTime, in, out[0], 0, outInfo);
                    currentTime = System.currentTimeMillis();
                    outInfo[OUT_SIZE] -= size;
                    outInfo[OUT_COUNT] += size;
                    outInfo[OUT_TIME] = currentTime;
                    ruleInfo[RULE_SIZE] -= size;
                    ruleInfo[RULE_COUNT] += size;
                    ruleInfo[RULE_TIME] = currentTime;
                }
                else if (size > 0) { // no wait so update stats only
                    currentTime = System.currentTimeMillis();
                    outInfo[OUT_COUNT] += size;
                    outInfo[OUT_TIME] = currentTime;
                    ruleInfo[RULE_COUNT] += size;
                    ruleInfo[RULE_TIME] = currentTime;
                }

                // it assumes the message is writeable, see comments at top
                if (countField != null && ms != null) try { // set count
                    MessageUtils.setProperty(countField, String.valueOf(size),
                        inMessage);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to set count of " + size + " to " +countField+
                        ": " + Event.traceStack(e)).send();
                }
                oid = outLinkMap[BYPASS_OUT];
            }
            catch (Exception e) {
                oid = outLinkMap[FAILURE_OUT];
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to select the msg: " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to select the msg: "+
                    Event.traceStack(e)).send();
                if (xa) try {
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

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " size=" + size).send();

            if (displayMask > 0) try { // display the message
                new Event(Event.INFO, name + ": " + ruleName + " selected " +
                    size + " msgs from the msg " +
                    (count+1) + ":" + MessageUtils.display(inMessage, null,
                    displayMask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * loads the content of list into new JMS messages and flushes them
     * to the given outLink.  It returns the number of messages flushed.
     */
    private int flush(long currentTime, String[] list, XQueue in, int rid,
        int oid, int dmask, String[] pn, String[] pv, String tag,
        String[] keys) throws JMSException {
        int i, j, n, count = 0;
        TextMessage outMessage;
        String ruleName;
        long[] ruleInfo;

        if (in == null || list == null || (n = list.length) <= 0)
            return 0;

        ruleName = ruleList.getKey(rid); 
        ruleInfo = ruleList.getMetaData(rid);

        for (i=0; i<n; i++) {
            outMessage = new TextEvent();
            outMessage.setJMSTimestamp(currentTime);
            outMessage.setText(list[i]);

            if (pn != null && pv != null) {
                for (j=0; j<pn.length; j++)
                    MessageUtils.setProperty(pn[j], pv[j], outMessage);
            }
            if (keys != null && tag != null) // tag for map
                MessageUtils.setProperty(tag, keys[i], outMessage);

            if (dmask > 0) try { // display the message
                new Event(Event.INFO, name + ": " + ruleName +
                    " flushed msg " + i + "/" + count +
                    ":" + MessageUtils.display(outMessage, list[i],
                    dmask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display flushed msg: " +e.toString()).send();
            }

            j = passthru(currentTime, outMessage, in, rid, oid, -1, 0);
            if (j > 0) {
                count ++;
                ruleInfo[RULE_PEND] ++;
            }
            else
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to flush msg " + i).send();
        }
        return count;
    }

    public void close() {
        java.lang.reflect.Method closer;
        Browser browser;
        Object[] asset;
        int id;

        super.close();

        if (builder != null)
            builder = null;
        if (xpath != null)
            xpath = null;
        if (pc != null)
            pc = null;
        if (pm != null)
            pm = null;

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
    }

    protected void finalize() {
        close();
    }
}
