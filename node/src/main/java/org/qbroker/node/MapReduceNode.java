package org.qbroker.node;

/* MapReduceNode.java - a MessageNode mapping/reducing via JMS messages */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Date;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
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
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Aggregation;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * MapReduceNode picks up JMS messages as the requests and creates multiple
 * copies of the message as the requests according to the predefined rulesets.
 * Those newly created messages will be sent to multiple outlinks. The node
 * collects the responses from them and aggregates the results according to
 * the reducing policies. If all the responses are collected and aggregated,
 * the final result will be loaded to the incoming message as the response.
 * The incoming message will be routed to the outlink of done. In case of
 * failure, the incoming messages will be routed to the outlink of failure.  If
 * none of the rulesets matches the incoming messages, they will be put to the
 * outlink of nohit.  The rest of the outlinks are collectibles for responses.
 *<br/><br/>
 * MapReduceNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups via the property filter.
 * Therefore, each ruleset defines a unique message group.  If the associated
 * outlink is none of the fixed outlinks and Aggregation is defined in a
 * ruleset, it is a ruleset of map-reduce. There may be other parameters for
 * the map and reduce operations. For example, TimeToLive and Quorum are two
 * important parameters for the reduce process.
 *<br/><br/>
 * The aggregation on responses is defined in BodyAggregation. It supports the
 * first, last, append for text, and merge or union for JSON or XML payload,
 * etc. In case of merge for JSON payload, JSONPath is required in the hash.
 * JSONPath specifies a JSONPath expression that is used to select the data
 * from the responses. The selected data will be merged as a child node to the
 * parent node of the JSON document as the result. In case of merge for XML
 * payload, XPath is required in the hash. XPath specifies an XPath expression
 * that is used to select the data from the responses. The selected data will
 * be merged as a child node to the parent node of the XML document as the
 * result.
 *<br/><br/>
 * The default map process is to copy the message body as the request for each
 * selected outlink. The selected outlinks are defined via the list of
 * SelectedOutLink. If it is defined in a ruleset, its member can be either
 * the name of an outlink or a map containing LinkTemplate and FieldName, etc.
 * If it is not defined for a map-reduce rule, the request will be duplicated
 * to all collectibles. If a member of SelectedOutLink is a map, the value of
 * LinkTemplate will be used to determine the name of the outlink dynamically
 * or statically, depending on whether it contains variables referencing to
 * certain properties of the message. The value of FieldName will be the
 * property name of the message for the request. In this case, the node
 * formats the request out of message and sets the result to that property.
 * Therefore, it may contain Template and Substitution. This way, each outlink
 * may have its own request.
 *<br/><br/>
 * If SelectedOutLink is a string, the ruleset expects a JSONPath defined for
 * selecting a list of items from the JSON payload. In this case, each selected
 * content will be treated as a sub-request. All the sub-requests will be
 * mapped to a single outlink specified by SelectedOutLink. All the properties
 * will be copied over to each of the sub-request.
 *<br/><br/>
 * You are free to choose any names for the three fixed outlinks.  But
 * MapReduceNode always assumes the first outlink for done, the second for
 * failure and the third for nohit.  The rest of the outlinks are for responses.
 * It is OK for those three fixed outlinks to share the same name.  Please make
 * sure each of the fixed outlinks has the actual capacity no less than that of
 * the input XQueue.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MapReduceNode extends Node {
    private int maxGroup = 1024;
    private String rcField;
    private long[] ri = null;

    private AssetList pendList = null; // list of messages pending for response
    private XQueue reqList = null;     // list of request msgs
    private XPath xpath = null;
    private DocumentBuilder builder = null;
    private Transformer defaultTransformer = null;
    private java.lang.reflect.Method ackMethod = null;

    private final static int RESULT_OUT = 0;
    private int FAILURE_OUT = 1;
    private int NOHIT_OUT = 2;
    private int BOUNDARY = 2;
    private final static int REQ_CID = 0;
    private final static int REQ_RID = 1;
    private final static int REQ_OID = 2;
    private final static int REQ_SUCCESS = 3;
    private final static int REQ_FAILURE = 4;
    private final static int REQ_TOTAL = 5;
    private final static int REQ_QUORUM = 6;
    private final static int REQ_TTL = 7;
    private final static int REQ_TIME = 8;

    public MapReduceNode(Map props) {
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
            operation = "mapreduce";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        try {
            Class<?> cls = this.getClass();
            ackMethod = cls.getMethod("acknowledge", new Class[]{long[].class});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("found no ack method"));
        }

        if ((o = props.get("MaxGroup")) == null ||
            (maxGroup = Integer.parseInt((String) o)) <= 0)
            maxGroup = 1024;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        NOHIT_OUT = overlap[0];

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

        BOUNDARY = (NOHIT_OUT >= FAILURE_OUT) ? NOHIT_OUT : FAILURE_OUT;
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY + " " +
                assetList.getKey(RESULT_OUT)+" "+assetList.getKey(FAILURE_OUT)+
                " " + assetList.getKey(NOHIT_OUT) + strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pendList = new AssetList(name, capacity);
        n = assetList.size() - BOUNDARY - 1;
        reqList = new IndexedXQueue(name, n * capacity);
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
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
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

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            ruleInfo = ruleList.getMetaData(i);
            if ((debug & DEBUG_INIT) > 0) {
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                    ruleInfo[RULE_OPTION] + " " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
        }
        if ((debug & DEBUG_INIT) > 0)
            new Event(Event.DEBUG, name+" RuleName: RID PID TTL OPTION - " +
                "OutName" + strBuf.toString()).send();
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
            preferredOutName = assetList.getKey(NOHIT_OUT);
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

        // TimeToLive is for cache
        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_TTL] = 60000;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("Aggregation")) != null && o instanceof List) {
            // for aggregation
            int m = 0;
            int[] oids;
            Aggregation aggr = new Aggregation(ruleName, (List) o);
            rule.put("Aggregation", aggr);

            // for body aggregation
            if ((o = ph.get("BodyAggregation")) != null && o instanceof Map) {
                Map<String, Object> map = Utils.cloneProperties((Map) o);
                if (aggr.setBodyMap(map) == null) {
                    new Event(Event.ERR, name + ": BodyAggregation for " +
                        ruleName + " not well defined").send();
                }

                if ((o = ph.get("KeyTemplate")) != null && o instanceof String){
                    map.put("Template", new Template((String) o));
                    if ((o = ph.get("KeySubstitution")) != null)
                        map.put("Substitution",new TextSubstitution((String)o));
                }

                // MODE stores the body type
                ruleInfo[RULE_MODE] = aggr.getBodyType();
                if (ruleInfo[RULE_MODE] == Utils.RESULT_XML) try { // for XML
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
                    if((o=ph.get("XPathExpression"))!=null && o instanceof Map){
                        Map<String, Object> expr = new HashMap<String,Object>();
                        XPathExpression xpe = null;
                        iter = ((Map) o).keySet().iterator();
                        while (iter.hasNext()) {
                            key = (String) iter.next();
                            if (key == null || key.length() <= 0)
                                continue;
                            str = (String) ((Map) o).get(key);
                            xpe = xpath.compile(str);
                            expr.put(key, xpe);
                        }
                        map.put("XPathExpression", expr);
                    }
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(ruleName+" failed "+
                        "to instantiate XPath: " + Event.traceStack(ex)));
                }
            }

            // for maps
            if ((o = ph.get("SelectedOutLink")) != null && o instanceof List) {
                // mapping to a list of outlinks
                list = (List) o;
                k = list.size();
                oids = new int[k];
                Template[] ltmp = new Template[k];
                TextSubstitution[] lsub = new TextSubstitution[k];
                String[] dataField = new String[k];
                Template[] temp = new Template[k];
                TextSubstitution[] tsub = new TextSubstitution[k];
                for (i=0; i<k; i++) {
                    o = list.get(i);
                    ltmp[i] = null;
                    lsub[i] = null;
                    dataField[i] = null;
                    temp[i] = null;
                    tsub[i] = null;
                    if (o == null)
                        key = null;
                    else if (o instanceof Map) { // build request for mapping
                        Map map = (Map) o;
                        key = (String) map.get("LinkTemplate");
                        if (key != null && key.length() > 0) { // for link
                            ltmp[i] = new Template(key);
                            if (ltmp[i].numberOfFields() <= 0)
                                ltmp[i] = null;
                            else { // dynamic link
                                if ((o = map.get("LinkSubstitution")) != null &&
                                    o instanceof String)
                                    lsub[i] = new TextSubstitution((String) o);
                                if (!rule.containsKey("LinkTemplate")) {
                                    rule.put("LinkTemplate", ltmp);
                                    rule.put("LinkSubstitution", tsub);
                                }
                            }
                            if ((o = map.get("FieldName")) != null &&
                                o instanceof String) {
                                str = MessageUtils.getPropertyID((String) o);
                                if (str == null)
                                    str = (String) o;
                                dataField[i] = str;
                                if ((o = map.get("Template")) != null &&
                                    o instanceof String) {
                                    str = (String) o;
                                }
                                else
                                    str = "##" + dataField[i] + "##";
                                temp[i] = new Template(str);
                                if ((o = map.get("Substitution")) != null &&
                                    o instanceof String)
                                    tsub[i] = new TextSubstitution((String) o);
                                if (!rule.containsKey("DataField")) {
                                    rule.put("DataField", dataField);
                                    rule.put("Template", temp);
                                    rule.put("Substitution", tsub);
                                }
                            }
                        }
                    }
                    else { // copy request for mapping
                        key = (String) o;
                    }
                    if (ltmp[i] != null) // dynamic link
                        id = -1;
                    else if (key == null || key.length() <= 0 ||
                        (id = assetList.getID(key)) <= BOUNDARY) {
                        new Event(Event.WARNING, name+": SelectedOutLink" +
                            "[" + i + "]=" + key + " for "+ ruleName +
                            " not defined, skip it for now").send();
                        continue;
                    }
                    oids[m++] = id;
                }
                if (m < k) { // with maps
                    new Event(Event.ERR, name+": SelectedOutLink" +
                        " for "+ ruleName + " is bad").send();
                    return null;
                }
                ruleInfo[RULE_PID] = TYPE_MAPREDUCE;

                // store number of mapping links into RULE_OPTION
                ruleInfo[RULE_OPTION] = m;
                // EXTRA tracks number of outlinks that are not full yet
                ruleInfo[RULE_EXTRA] = m;

                // GID stores quorum
                if ((o = ph.get("Quorum")) != null)
                    ruleInfo[RULE_GID] = Integer.parseInt((String) o);
                if (ruleInfo[RULE_GID] <= 0 || ruleInfo[RULE_GID] > m)
                    ruleInfo[RULE_GID] = m;
            }
            else if (o != null && o instanceof String) { // for select
                m = 1;
                oids = new int[m];
                key = (String) o;
                id = assetList.getID(key);
                oids[0] = id;
                if (id <= BOUNDARY) {
                    new Event(Event.ERR, name + ": SelectedOutLink[0]=" +
                        key + " for " + ruleName + " not well defined").send();
                    return null;
                }
                else if ((o = ph.get("JSONPath")) != null && // for JSON list
                    o instanceof String && ((String) o).length() > 0) {
                    rule.put("JSONPath", (String) o);
                    ruleInfo[RULE_PID] = TYPE_JSONPATH;

                    // store number of mapping links into RULE_OPTION
                    ruleInfo[RULE_OPTION] = m;
                    // EXTRA tracks number of outlinks that are not full yet
                    ruleInfo[RULE_EXTRA] = m;

                    // GID stores quorum
                    if ((o = ph.get("Quorum")) != null)
                        ruleInfo[RULE_GID] = Integer.parseInt((String) o);
                    if (ruleInfo[RULE_GID] < 0)
                        ruleInfo[RULE_GID] = 0;
                }
                else {
                    ruleInfo[RULE_PID] = TYPE_MAPREDUCE;

                    // store number of mapping links into RULE_OPTION
                    ruleInfo[RULE_OPTION] = m;
                    // EXTRA tracks number of outlinks that are not full yet
                    ruleInfo[RULE_EXTRA] = m;

                    // GID stores quorum
                    if ((o = ph.get("Quorum")) != null)
                        ruleInfo[RULE_GID] = Integer.parseInt((String) o);
                    if (ruleInfo[RULE_GID] <= 0 || ruleInfo[RULE_GID] > m)
                        ruleInfo[RULE_GID] = m;
                }
            }
            else { // for default mapping
                m = assetList.size() - BOUNDARY - 1;
                oids = new int[m];
                for (i=0; i<m; i++) {
                    oids[i] = i + BOUNDARY + 1;
                }
                ruleInfo[RULE_PID] = TYPE_MAPREDUCE;

                // store number of mapping links into RULE_OPTION
                ruleInfo[RULE_OPTION] = m;
                // EXTRA tracks number of outlinks that are not full yet
                ruleInfo[RULE_EXTRA] = m;

                // GID stores quorum
                if ((o = ph.get("Quorum")) != null)
                    ruleInfo[RULE_GID] = Integer.parseInt((String) o);
                if (ruleInfo[RULE_GID] <= 0 || ruleInfo[RULE_GID] > m)
                    ruleInfo[RULE_GID] = m;
            }
            rule.put("SelectedOutLink", oids);

            ruleInfo[RULE_OID] = RESULT_OUT;
        }
        else {
            ruleInfo[RULE_OID] = NOHIT_OUT;
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
     * It generates a new message for each selected content and flushes the
     * the new messages to the selected outlink as the requests. It creates a
     * data cache with the given key for collecting the responses. It returns
     * the number of the mapped messages or -1 for failure.
     */
    private int flush(String key, long currentTime, int cid, int rid, int dmask,
        String[] batch, XQueue in, int[] oids, String[] pn, String[] pv) {
        Object o;
        Object[] asset;
        Message msg;
        Message[] list;
        XQueue xq;
        long[] outInfo, ruleInfo;
        int[] ids, jds;
        int i, k, l, m, n, id, jd, oid, len, shift;

        if (batch == null || oids == null || oids.length < 1)
            return -1;

        oid = oids[0];
        asset = (Object[]) assetList.get(oid);
        outInfo = assetList.getMetaData(oid);
        if (asset == null || outInfo == null) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " got a bad outlink for flush: " + oid).send();
            return -1;
        }
        xq = (XQueue) asset[ASSET_XQ];
        len = (int) outInfo[OUT_LENGTH];

        n = batch.length;
        ids = new int[n];
        jds = new int[n];
        list = new Message[n];
        m = 0;
        for (i=0; i<n; i++) {
            ids[i] = -1;
            jds[i] = -1;
            list[i] = null;
            switch (len) {
              case 0:
                id = xq.reserve(waitTime);
                break;
              case 1:
                shift = (int) outInfo[OUT_OFFSET];
                id = xq.reserve(waitTime, shift);
                break;
              default:
                shift = (int) outInfo[OUT_OFFSET];
                id = xq.reserve(waitTime, shift, len);
                break;
            }
            if (id < 0) {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to reserve a cell in " + xq.getName()).send();
                continue;
            }
            try {
                msg = new TextEvent(batch[i]);
                msg.setJMSCorrelationID(key);
                if (pn != null && pv != null) { // copy properties
                    for (int j=0; j<pn.length; j++)
                        MessageUtils.setProperty(pn[j], pv[j], msg);
                }
            }
            catch (Exception e) {
                xq.cancel(id);
                for (int j=0; j<i; j++)
                    if (ids[j] >= 0)
                        xq.cancel(ids[j]);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to copy msg to " + xq.getName() + ": " +
                    Event.traceStack(e)).send();
                return -1;
            }

            jd = reqList.reserve(waitTime);
            if (jd < 0) { // failure
                xq.cancel(id);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to reserve a cell in reqList for " + oid).send();
                continue;
            }
            ids[i] = id;
            jds[i] = jd;
            list[i] = msg;
            m ++;
        }

        l = 0;
        for (i=0; i<n; i++) {
            id = ids[i];
            if (id < 0)
                continue;
            jd = jds[i];
            if (jd < 0)
                continue;
            msg = list[i];
            list[i] = null;
            ((JMSEvent) msg).setAckObject(this, ackMethod, new long[]{jd});
            reqList.add(new Object[]{msg, new long[]{cid, oid, id, rid, dmask}},
                jd);
            reqList.getNextCell(-1L, jd);
            k = xq.add(msg, id);
            if (k < 0) {
                xq.cancel(id);
                reqList.remove(jd);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to add a msg to " + xq.getName() + " " + k + " " +
                    outInfo[OUT_ORULE] + " " + xq.depth() + "/" +
                    xq.size()).send();
            }
            else {
                l ++;
                // track number of requests in NOHIT PEND
                ri[RULE_PEND] ++;
                outInfo[OUT_SIZE] ++;
                outInfo[OUT_TIME] = currentTime;
                if (outInfo[OUT_LENGTH] == 0 &&
                    outInfo[OUT_SIZE] >= outInfo[OUT_CAPACITY])
                    m --;
                else if (outInfo[OUT_LENGTH] > 0 &&
                    outInfo[OUT_SIZE] >= outInfo[OUT_LENGTH])
                    m --;
            }
        }
        ruleInfo = ruleList.getMetaData(rid);
        // update EXTRA for number of outlinks that are not full yet
        ruleInfo[RULE_EXTRA] = m;

        return l;
    }

    /**
     * It duplicates the incoming message to each of the selected outlinks as
     * the request and creates a data cache with the given key for collecting
     * the responses. It returns the number of the mapped messages or -1 for
     * failure.
     */
    private int map(String key, long currentTime, int cid, int rid,
        int dmask, Message message, XQueue in, int n, int[] oids,
        Template[] ltmp, TextSubstitution[] lsub, String[] dataField,
        Template[] temp, TextSubstitution[] tsub, byte[] buffer) {
        Object o;
        Object[] asset;
        Message msg;
        Message[] list;
        XQueue xq;
        XQueue[] xqs;
        String text = null;
        long[] outInfo, ruleInfo;
        int[] ids, jds, ods;
        int i, k, l, m, id, jd, oid, len, shift;
        if (message == null || oids == null || n <= 0 || oids.length < n)
            return -1;

        ids = new int[n];
        jds = new int[n];
        ods = new int[n];
        xqs = new XQueue[n];
        list = new Message[n];
        m = 0;
        for (i=0; i<n; i++) {
            ids[i] = -1;
            jds[i] = -1;
            ods[i] = -1;
            xqs[i] = null;
            list[i] = null;
            if (oids[i] > BOUNDARY) // static links
                oid = oids[i];
            else if (ltmp == null || ltmp[i] == null) {
                for (int j=0; j<i; j++)
                    if (xqs[j] != null && ids[j] >= 0)
                        xqs[j].cancel(ids[j]);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " empty link template for " + i).send();
                return -1;
            }
            else try { // dynamic links
                text = MessageUtils.format(message, buffer, ltmp[i]);
                if (lsub[i] != null)
                    text = lsub[i].substitute(text);
                oid = assetList.getID(text);
                if (oid <= BOUNDARY) { // not a collectible
                    for (int j=0; j<i; j++)
                        if (xqs[j] != null && ids[j] >= 0)
                            xqs[j].cancel(ids[j]);
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                        " failed to get link " + i + ": " + text).send();
                    return -1;
                }
            }
            catch (Exception e) {
                for (int j=0; j<i; j++)
                    if (xqs[j] != null && ids[j] >= 0)
                        xqs[j].cancel(ids[j]);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to map link " + i + ": " + e.toString()).send();
                return -1;
            }
            asset = (Object[]) assetList.get(oid);
            outInfo = assetList.getMetaData(oid);
            m ++;
            if (asset == null || outInfo == null) {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " got a bad outlink at " + i + ": " + oid).send();
                continue;
            }
            xq = (XQueue) asset[ASSET_XQ];
            len = (int) outInfo[OUT_LENGTH];
            switch (len) {
              case 0:
                id = xq.reserve(waitTime);
                break;
              case 1:
                shift = (int) outInfo[OUT_OFFSET];
                id = xq.reserve(waitTime, shift);
                break;
              default:
                shift = (int) outInfo[OUT_OFFSET];
                id = xq.reserve(waitTime, shift, len);
                break;
            }
            ids[i] = id;
            if (id < 0) {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to reserve a cell in " + xq.getName()).send();
                m --;
                continue;
            }
            try {
                msg = MessageUtils.duplicate(message, buffer);
                msg.setJMSCorrelationID(key);
            }
            catch (Exception e) {
                xq.cancel(id);
                for (int j=0; j<i; j++)
                    if (xqs[j] != null && ids[j] >= 0)
                        xqs[j].cancel(ids[j]);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to copy msg to " + assetList.getKey(oid) + ": " +
                    Event.traceStack(e)).send();
                return -1;
            }
            if (dataField != null && dataField[i] != null) { // for template
                try {
                    text = MessageUtils.format(msg, buffer, temp[i]);
                    if (tsub[i] != null)
                        text = tsub[i].substitute(text);
                }
                catch (Exception e) {
                    xq.cancel(id);
                    for (int j=0; j<i; j++)
                        if (xqs[j] != null && ids[j] >= 0)
                            xqs[j].cancel(ids[j]);
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                        ": failed to evaluate template for " +
                        assetList.getKey(oid) + ": " + e.toString()).send();
                    return -1;
                }
                if (text == null || text.length() <= 0) {
                    xq.cancel(id);
                    for (int j=0; j<i; j++)
                        if (xqs[j] != null && ids[j] >= 0)
                            xqs[j].cancel(ids[j]);
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid)+
                        " failed to format sub-request for " +
                        assetList.getKey(oid)).send();
                    return -1;
                }
                else try { // reset payload for sub-request
                    if ("body".equals(dataField[i])) {
                        if (msg instanceof TextMessage) {
                            ((TextMessage) msg).setText(text);
                        }
                        else if (msg instanceof BytesMessage) {
                            msg.clearBody();
                            ((BytesMessage) msg).writeBytes(text.getBytes());
                        }
                        else {
                            xq.cancel(id);
                            for (int j=0; j<i; j++)
                                if (xqs[j] != null && ids[j] >= 0)
                                    xqs[j].cancel(ids[j]);
                            new Event(Event.ERR, name+": "+ruleList.getKey(rid)+
                                " failed to set body: bad msg family").send();
                            return -1;
                        }
                    }
                    else
                        MessageUtils.setProperty(dataField[i], text, msg);
                }
                catch (Exception e) {
                    xq.cancel(id);
                    for (int j=0; j<i; j++)
                        if (xqs[j] != null && ids[j] >= 0)
                            xqs[j].cancel(ids[j]);
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                        " failed to reset payload on msg for " +
                        assetList.getKey(oid) + ": " + e.toString()).send();
                    return -1;
                }
            }
            jd = reqList.reserve(waitTime);
            if (jd < 0) { // failure
                xq.cancel(id);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to reserve a cell in reqList for " + oid).send();
                continue;
            }
            ods[i] = oid;
            jds[i] = jd;
            xqs[i] = xq;
            list[i] = msg;
        }

        l = 0;
        for (i=0; i<n; i++) {
            id = ids[i];
            if (id < 0)
                continue;
            jd = jds[i];
            if (jd < 0)
                continue;
            oid = ods[i];
            xq = xqs[i];
            xqs[i] = null;
            msg = list[i];
            list[i] = null;
            outInfo = assetList.getMetaData(oid);
            ((JMSEvent) msg).setAckObject(this, ackMethod, new long[]{jd});
            reqList.add(new Object[]{msg, new long[]{cid, oid, id, rid, dmask}},
                jd);
            reqList.getNextCell(-1L, jd);
            k = xq.add(msg, id);
            if (k < 0) {
                xq.cancel(id);
                reqList.remove(jd);
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to add a msg to " + xq.getName() + " " + k + " " +
                    outInfo[OUT_ORULE] + " " + xq.depth() + "/" +
                    xq.size()).send();
            }
            else {
                l ++;
                // track number of requests in NOHIT PEND
                ri[RULE_PEND] ++;
                outInfo[OUT_SIZE] ++;
                outInfo[OUT_TIME] = currentTime;
                if (outInfo[OUT_LENGTH] == 0 &&
                    outInfo[OUT_SIZE] >= outInfo[OUT_CAPACITY])
                    m --;
                else if (outInfo[OUT_LENGTH] > 0 &&
                    outInfo[OUT_SIZE] >= outInfo[OUT_LENGTH])
                    m --;
            }
        }
        ruleInfo = ruleList.getMetaData(rid);
        // update EXTRA for number of outlinks that are not full yet
        ruleInfo[RULE_EXTRA] = m;
        return l;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null, keyPath = null;
        Object o;
        Object[] asset;
        String[] dataField = null;
        Template[] temp = null, ltmp = null;
        TextSubstitution[] tsub = null, lsub = null;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        long[] outInfo = null, ruleInfo = null;
        int[] ruleMap, oids = null;
        long currentTime, st, wt;
        long count = 0;
        int mask, ii, sz, retryCount = 0, reqCap = reqList.getCapacity();
        int i = 0, n, size, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        boolean dspBody = false, ckBody = false;
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
        ri = ruleList.getMetaData(0);
        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0) && !ckBody)
            dspBody = true;

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
                    if (pendList.size() > 0 || reqList.depth() > 0) {
                        currentTime = System.currentTimeMillis();
                        collect(currentTime, in, out[0], buffer);
                        expire(currentTime, in, out[0], buffer);
                    }
                    if (sz > 0)
                        feedback(in, -1L);
                    continue;
                }
            }
            currentTime = System.currentTimeMillis();
            if (pendList.size() > 0 || reqList.depth() > 0) {
                collect(currentTime, in, out[0], buffer);
                expire(currentTime, in, out[0], buffer);
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
                if (ruleInfo[RULE_PID] == TYPE_MAPREDUCE) {
                    oids = (int[]) rule.get("SelectedOutLink");
                    ltmp = (Template[]) rule.get("LinkTemplate");
                    lsub = (TextSubstitution[]) rule.get("LinkSubstitution");
                    dataField = (String[]) rule.get("DataField");
                    if (dataField != null) {
                        temp = (Template[]) rule.get("Template");
                        tsub = (TextSubstitution[]) rule.get("Substitution");
                    }
                }
                else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) {
                    oids = (int[]) rule.get("SelectedOutLink");
                    keyPath = (String) rule.get("JSONPath");
                }
                previousRid = rid;
                oid = (int) ruleInfo[RULE_OID];
                outInfo = assetList.getMetaData(oid);
            }

            if (i < 0) // failed to apply filters
                oid = FAILURE_OUT;
            else if ((int) ruleInfo[RULE_PID] == TYPE_MAPREDUCE) { // for M/R
                String key;
                long ttl, quorum;
                int m = (int) ruleInfo[RULE_OPTION];
                if (m > reqCap - reqList.size() || m > ruleInfo[RULE_EXTRA]) {
                    in.putback(cid);
                    if (retryCount++ < 2)
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " has at least one outlink filled up: " +
                            m + "," + ruleInfo[RULE_EXTRA] + " " +
                            reqList.size() + "/" + reqCap).send();
                    else try {
                        Thread.sleep(1000);
                    }
                    catch (Exception e) {
                    }
                    continue;
                }
                ttl = ruleInfo[RULE_TTL];
                quorum = ruleInfo[RULE_GID];
                key = String.valueOf(cid) + ":" + currentTime;
                i = pendList.add(key, new long[]{cid, rid, -1, 0, 0, m, quorum,
                    ttl, currentTime}, key, cid);
                if (i < 0) { // removed the old object first
                    pendList.remove(cid);
                    i = pendList.add(key, new long[]{cid, rid, -1, 0, 0, m,
                        quorum, ttl, currentTime}, key, cid);
                }
                if (i >= 0) { 
                    int dmask = (int) ruleInfo[RULE_DMASK];
                    i = map(key, currentTime, cid, rid, dmask, inMessage, in,
                        m, oids, ltmp, lsub, dataField, temp, tsub, buffer);
                    if (i > 0) {
                        ruleInfo[RULE_PEND] ++;
                        ruleInfo[RULE_SIZE] ++;
                        ruleInfo[RULE_TIME] = currentTime;
                        continue;
                    }
                    else
                        pendList.remove(cid);
                }
                // failed on map process
                oid = FAILURE_OUT;
            }
            else if ((int) ruleInfo[RULE_PID] == TYPE_JSONPATH) { // for select
                String[] batch;
                String key;
                long ttl, quorum;
                try {
                    if (!ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    StringReader rin = new StringReader(msgStr);
                    o = JSON2FmModel.parse(rin);
                    rin.close();
                    if (o != null) { // json parsed
                        if (o instanceof Map) // for json path
                            o = JSON2FmModel.get((Map) o, keyPath);
                        else
                            o = JSON2FmModel.get((List) o, keyPath);
                    }
                    if (o == null)
                        batch = new String[0];
                    else if (o instanceof List) { // array
                        List list = (List) o;
                        int bn = list.size();
                        batch = new String[bn];
                        for (i=0; i<bn; i++) {
                            o = list.get(i);
                            if (o == null)
                                batch[i] = "";
                            else if (o instanceof String)
                                batch[i] = (String) o;
                            else if (o instanceof Map)
                                batch[i] = JSON2FmModel.toJSON((Map) o,"","\n");
                            else if (o instanceof List)
                                batch[i] = JSON2FmModel.toJSON((List)o,"","\n");
                            else
                                batch[i] = o.toString();
                        }
                    }
                    else if (o instanceof Map) {
                        batch = new String[1];
                        batch[0] = JSON2FmModel.toJSON((Map) o, "", "\n");
                    }
                    else { // for string or others
                        batch = new String[1];
                        if (o instanceof String)
                            batch[0] = (String) o;
                        else
                            batch[0] = o.toString();
                    }
                }
                catch (Exception e) {
                    batch = null;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to select content: " +
                        Event.traceStack(e)).send();
                }
                if (batch != null && batch.length > 0) { // select is done
                    ttl = ruleInfo[RULE_TTL];
                    if (ruleInfo[RULE_GID] > 0)
                        quorum = (int) ruleInfo[RULE_GID];
                    else
                        quorum = batch.length;
                    key = String.valueOf(cid) + ":" + currentTime;
                    i = pendList.add(key, new long[]{cid, rid, -1, 0, 0,
                        batch.length, quorum, ttl, currentTime}, key, cid);
                    if (i < 0) { // removed the old object first
                        pendList.remove(cid);
                        i = pendList.add(key, new long[]{cid, rid, -1, 0, 0,
                            batch.length, quorum, ttl, currentTime}, key, cid);
                    }
                    if (i >= 0) try { // try to copy properties for select
                        String str;
                        String[] pn = null, pv = null;
                        List<String> list = new ArrayList<String>();
                        int dmask = (int) ruleInfo[RULE_DMASK];
                        Enumeration propNames = inMessage.getPropertyNames();
                        while (propNames.hasMoreElements()) {
                            str = (String) propNames.nextElement();
                            if (str == null || str.equals("null"))
                                continue;
                            list.add(str);
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
                            pn = new String[i];
                            pv = new String[i];
                            for (int j=0; j<i; j++) {
                                pn[j] = list.get(j);
                                pv[j] =
                                    MessageUtils.getProperty(pn[j], inMessage);
                            }
                            list.clear();
                        }
                        i = flush(key, currentTime, cid, rid, dmask, batch,
                            in, oids, pn, pv);
                        if (i > 0) {
                            ruleInfo[RULE_PEND] ++;
                            ruleInfo[RULE_SIZE] ++;
                            ruleInfo[RULE_TIME] = currentTime;
                            continue;
                        }
                        else
                            pendList.remove(cid);
                    }
                    catch (Exception e) {
                        pendList.remove(cid);
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to copy properties: " +
                            Event.traceStack(e)).send();
                    }
                }
                // failed on map process
                oid = FAILURE_OUT;
            }
            else { // preferred ruleset or nohit
                oid = (int) ruleInfo[RULE_OID];
            }
            retryCount = 0;

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid).send();

            if (displayMask > 0) try { // display message
                if (dspBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleName + " processd msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    displayMask, displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * It converts the cached data to text and sets the text to the message
     * body. Upon success, oid of RESULT_OUT is returned. Otherwise, oid of
     * FAILURE_OUT is returned to indicate failure.
     */
    private int convert(int cid, int rid, int k, Aggregation aggr,
        Message inMessage) {
        String text;
        int oid;

        Object o = pendList.get(cid);

        if (o == null)
            oid = FAILURE_OUT;
        else try { // load response
            oid = RESULT_OUT;
            if (k == Utils.RESULT_JSON) {
                Map map = (Map) o;
                map.remove("__cache__");
                // convert to tabular format
                if ((o = aggr.getFromBody("TargetJSONPath")) != null &&
                    JSON2FmModel.JSON_ARRAY == aggr.getBodyDataType()) {
                    o = JSON2FmModel.get(map, (String) o);
                    text = JSON2FmModel.toJSON((List) o, null, null);
                }
                else
                    text = JSON2FmModel.toJSON(map);
                map.clear();
            }
            else if (k == Utils.RESULT_XML) {
                Document doc = (Document) o;
                StreamResult result = new StreamResult(new StringWriter());
                defaultTransformer.transform(new DOMSource(doc), result);
                text = result.getWriter().toString();
            }
            else
                text = (String) o;

            if (inMessage instanceof TextMessage) {
                ((TextMessage) inMessage).setText(text);
            }
            else if (inMessage instanceof BytesMessage) {
                inMessage.clearBody();
                ((BytesMessage) inMessage).writeBytes(text.getBytes());
            }
            else {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid)+
                    " failed to set body: bad msg family").send();
                oid = FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " +ruleList.getKey(rid)+
                " failed to set body: " + Event.traceStack(e)).send();
            oid = FAILURE_OUT;
        }
        return oid;
    }

    /**
     * Reduce engine aggregates the response message according to the
     * aggregation policies.  It extracts data from the response message
     * and aggregates the values into the data cache based on the key.  If
     * all the responses are aggregated, it removes the pending cache from
     * the pendList and sets the result to the original request message. It
     * returns the oid upon completion of the request or -1 otherwise.
     */
    private int reduce(long currentTime, int cid, int rid, Message request,
        Aggregation aggr, byte[] buffer, Message inMessage) {
        int i, k, rc;
        long[] meta;
        String msgStr, str;

        if (aggr == null || aggr.getSize() <= 0 || request == null)
            return -1;

        try {
            str = MessageUtils.getProperty(rcField, inMessage);
            if (str != null)
                rc = Integer.parseInt(str);
            else
                rc = -1;
        }
        catch (Exception e) {
            str = "-1";
            rc = -1;
        }
        meta = pendList.getMetaData(cid);
        if (meta == null || meta.length <= REQ_TIME) {
            new Event(Event.ERR, name+ " " + ruleList.getKey(rid) +
                " bad meta data of request cache for " + cid).send();
            return -1;
        }

        if (rc != 0) { // request failed
            meta[REQ_FAILURE] ++;
            try {
                MessageUtils.setProperty(rcField, str, request);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " " + ruleList.getKey(rid) +
                    " failed to set rc: " + str).send();
            }
            if (meta[REQ_FAILURE] + meta[REQ_QUORUM] > meta[REQ_TOTAL]) {
                // failed too many responses
                return FAILURE_OUT;
            }
        }
        else try { // request successful
            Reader sr = null;
            if (meta[REQ_SUCCESS] <= 0) { // first response
                meta[REQ_SUCCESS] = 1;
                i = aggr.initialize(currentTime, inMessage, request);
                if (i == 0 && aggr.hasBody()) switch (aggr.getBodyOperation()) {
                  case Aggregation.AGGR_LAST:
                  case Aggregation.AGGR_APPEND:
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null)
                        msgStr = "";
                    pendList.set(cid, msgStr);
                    break;
                  case Aggregation.AGGR_MERGE:
                    k = aggr.getBodyType();
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null || msgStr.length() <= 0) {
                        new Event(Event.ERR, name + " empty payload for "+
                            ruleList.getKey(rid)).send();
                    }
                    else try {
                        sr = new StringReader(msgStr);
                        if (k == Utils.RESULT_XML) { // XML payload
                            Document doc = builder.parse(new InputSource(sr));
                            pendList.set(cid, doc);
                        }
                        else if (k == Utils.RESULT_JSON) { // JSON payload
                            str = (String) aggr.getFromBody("TargetJSONPath");
                            if (str != null && str.length() > 1) {
                                // target jpath defined
                                Map<String, Object> map =
                                    new HashMap<String, Object>();
                                Object o = JSON2FmModel.parse(sr);
                                if (o != null && !(o instanceof List) &&
                                    JSON2FmModel.JSON_ARRAY ==
                                    aggr.getBodyDataType()) {
                                    List<Object> list = new ArrayList<Object>();
                                    list.add(o);
                                    o = list;
                                }
                                map.put(str.substring(1), o);
                                pendList.set(cid, map);
                            }
                            else { // use the original map
                                pendList.set(cid, JSON2FmModel.parse(sr));
                            }
                        }
                        sr.close();
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name +" failed to parse body for "+
                            ruleList.getKey(rid) + ": " +
                            Event.traceStack(ex)).send();
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  case Aggregation.AGGR_UNION:
                    k = aggr.getBodyType();
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null || msgStr.length() <= 0) {
                        new Event(Event.ERR, name + " empty payload for "+
                            ruleList.getKey(rid)).send();
                    }
                    else try {
                        sr = new StringReader(msgStr);
                        if (k == Utils.RESULT_XML) { // XML payload
                            Document doc = builder.parse(new InputSource(sr));
                            pendList.set(cid, doc);
                        }
                        else if (k == Utils.RESULT_JSON) { // JSON payload
                            Map map = (Map) JSON2FmModel.parse(sr);
                            pendList.set(cid, map);
                            aggr.initCache(map);
                        }
                        sr.close();
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name +" failed to parse body for "+
                            ruleList.getKey(rid) + ": " +
                            Event.traceStack(ex)).send();
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  case Aggregation.AGGR_FIRST:
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        msgStr = "";
                    pendList.set(cid, msgStr);
                    return convert(cid, rid, Utils.RESULT_TEXT, aggr, request);
                  default:
                }
            }
            else { // extra responses
                meta[REQ_SUCCESS] ++;
                // first aggregation selects responses for body aggregation
                i = aggr.compare(0, inMessage, request);
                aggr.aggregate(currentTime, inMessage, request);
                if (i > 0 && aggr.hasBody()) switch (aggr.getBodyOperation()) {
                  case Aggregation.AGGR_LAST:
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        msgStr = "";
                    pendList.set(cid, msgStr);
                    break;
                  case Aggregation.AGGR_APPEND:
                    str = MessageUtils.processBody(inMessage, buffer);
                    if (str == null) // no such value, use default
                        str = (String) aggr.getFromBody("DefaultValue");
                    if (str == null || str.length() <= 0)
                        break;
                    msgStr = (String) pendList.get(cid);
                    if (msgStr == null)
                        msgStr = "";
                    if (aggr.hasKeyInBody("Delimiter")) {
                        if (msgStr.length() > 0)
                            str = (String) aggr.getFromBody("Delimiter") + str;
                    }
                    pendList.set(cid, msgStr + str);
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
                        if (k == Utils.RESULT_XML) { // XML payload
                            Document doc = (Document) pendList.get(cid);
                            i = aggr.merge(sr, doc, builder);
                        }
                        else if (k == Utils.RESULT_JSON) { // JSON payload
                            Map map = (Map) pendList.get(cid);
                            if (aggr.hasKeyInBody("FieldName")) {
                                str = (String) aggr.getFromBody("FieldName");
                                str = MessageUtils.getProperty(str, inMessage);
                            }
                            else
                                str = null;
                            i = aggr.merge(sr, str, map);
                        }
                        sr.close();
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name+ " failed to merge body for "+
                            ruleList.getKey(rid) + ": " +
                            Event.traceStack(e)).send();
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  case Aggregation.AGGR_UNION:
                    k = aggr.getBodyType();
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null) // no such value, use default
                        msgStr = (String) aggr.getFromBody("DefaultValue");
                    if (msgStr == null || msgStr.length() <= 0)
                        break;
                    try {
                        sr = new StringReader(msgStr);
                        if (k == Utils.RESULT_XML) { // XML payload
                            Document doc = (Document) pendList.get(cid);
                            i = aggr.merge(sr, doc, builder);
                        }
                        else if (k == Utils.RESULT_JSON) { // JSON payload
                            Map map = (Map) pendList.get(cid);
                            i = aggr.union(sr, map);
                        }
                        sr.close();
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name+ " failed to union body for "+
                            ruleList.getKey(rid) + ": " +
                            Event.traceStack(e)).send();
                        if (sr != null)
                            sr.close();
                    }
                    break;
                  default:
                }
            }

            // check number of responses collected
            if (meta[REQ_SUCCESS] >= meta[REQ_QUORUM]) {
                // enough responses collected
                return convert(cid, rid, aggr.getBodyType(), aggr, request);
            }
            else if (meta[REQ_SUCCESS] + meta[REQ_FAILURE] >= meta[REQ_TOTAL]) {
                // all responses collected but not enough
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to aggregate on " + pendList.getKey(cid) + "\n" +
                Event.traceStack(e)).send();
        }

        return -1;
    }

    /**
     * It collects the response message for a given request and reduces the
     * responses according to the ruleset. Once the request has its all
     * responses collected, it will be sent to either RESULT_OUT or
     * FAILURE_OUT as the final result. It returns 1 upon success or 0
     * otherwise to indicate that the request is not done yet.
     */
    private int collect(long currentTime, int mid, int oid, int rid, int dmask,
        Message inMessage, XQueue in, XQueue out, byte[] buffer) {
        Map rule;
        Aggregation aggr;
        Message request;
        String[] propertyName;
        String msgStr = null, key = null;
        long[] ruleInfo, meta;
        int k, count;

        if (mid < 0 || rid < 0 || inMessage == null)
            return -1;

        count = 0;
        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        ruleInfo = ruleList.getMetaData(rid);
        if (ruleInfo[RULE_EXTRA] < ruleInfo[RULE_OPTION]) // update extra
            ruleInfo[RULE_EXTRA] = checkLinks((int) ruleInfo[RULE_OPTION],
                (int[]) rule.get("SelectedOutLink"));
        try {
            key = inMessage.getJMSCorrelationID();
        }
        catch (Exception e) {
            key = null;
            new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                " failed to retrieve the key from response in " +
                assetList.getKey(oid)).send();
        }
        if (key == null || mid != pendList.getID(key)) { // too late
            new Event(Event.NOTICE, name + ": " + ruleList.getKey(rid) +
                " dropped a late response from " + assetList.getKey(oid) + ": "+
                key).send();
            return 0;
        }

        meta = pendList.getMetaData(mid);
        if (dmask > 0) try {// display reduced message
            if ((dmask & MessageUtils.SHOW_BODY) > 0)
                msgStr = MessageUtils.processBody(inMessage, buffer);
            new Event(Event.INFO, name +": "+ ruleList.getKey(rid) +
                " collected a response from " + assetList.getKey(oid) +
                " with " + meta[REQ_CID] + "," + meta[REQ_SUCCESS] + " " +
                meta[REQ_FAILURE] + "/" + meta[REQ_TOTAL] + ":" +
                MessageUtils.display(inMessage, msgStr, dmask,
                propertyName)).send();
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": " +ruleList.getKey(rid)+
                " failed to display reduced msg: " + e.toString()).send();
        }

        aggr = (Aggregation) rule.get("Aggregation");
        request = (Message) in.browse(mid);
        oid = reduce(currentTime, mid, rid, request, aggr, buffer, inMessage);
        if (oid >= 0) { // all responses are collected
            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + mid +
                    " rid=" + rid + " oid=" + oid).send();

            if (displayMask > 0) try { // display message
                String text = "";
                if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                    text = MessageUtils.processBody(request, buffer);
                new Event(Event.INFO, name +": "+ ruleList.getKey(rid) +
                    " map-reduced a request to " + oid + ":" +
                    MessageUtils.display(request, text, displayMask,
                    displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                    " failed to display msg: " + e.toString()).send();
            }

            count = passthru(currentTime, request, in, rid, oid, mid, 0);
            if (count > 0) { // passthru is done
                pendList.remove(mid);
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_SIZE] --;
            }
            else { // passthru failed so save the oid for retry
                meta[REQ_OID] = oid;
            }
        }
        return count;
    }

    /**
     * It looks for any response ready to be collected for all outstanding
     * requests and collects them one by one. Number of collected responses
     * is returned at the end.
     */
    private int collect(long currentTime, XQueue in, XQueue out, byte[] buffer){
        Object o;
        Message msg = null;
        long[] state, outInfo;
        int i, cid, oid, rid, mid, mask, dmask, count;

        count = 0;
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((cid = reqList.getNextCell(-1L)) < 0)
                break;
            o = reqList.browse(cid);
            reqList.remove(cid);
            if (o == null || !(o instanceof Object[]) || ((Object[])o).length<2)
                continue;
            msg = (Message) ((Object[]) o)[0];
            state = (long[]) ((Object[]) o)[1];
            mid = (int) state[MSG_CID];
            oid = (int) state[MSG_OID];
            rid = (int) state[MSG_RID];
            dmask = (int) state[MSG_TID];
            // reset CID to indicate that it is collected
            state[MSG_CID] = -1;
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            outInfo[OUT_TIME] = currentTime;
            ri[RULE_PEND] --;
            i = collect(currentTime, mid, oid, rid, dmask, msg, in, out,buffer);
            if (i > 0)
                count ++;
        }

        return count;
    }

    /**
     * It looks for any pending request that is either fulfilled or expired.
     * If such a pending request is found, it will be removed from the pending
     * list after its passthru. At the end, the method returns the total number
     * of processed requests.
     */
    private int expire(long currentTime, XQueue in, XQueue out, byte[] buffer) {
        Message inMessage;
        Map rule;
        Aggregation aggr;
        long[] meta, ruleInfo;
        int i, j, k, m, count, mid, oid, rid;
        Browser b = pendList.browser();

        count = 0;
        while ((k = b.next()) >= 0) {
            meta = pendList.getMetaData(k);
            if (meta[REQ_OID] < 0 && currentTime < meta[REQ_TTL]+meta[REQ_TIME])
                continue; // neither a retry nor expired yet

            mid = (int) meta[REQ_CID];
            rid = (int) meta[REQ_RID];
            m = (int) meta[REQ_TOTAL];
            rule = (Map) ruleList.get(rid);
            aggr = (Aggregation) rule.get("Aggregation");
            ruleInfo = ruleList.getMetaData(rid);
            inMessage = (Message) in.browse(mid);

            if (meta[REQ_OID] >= 0) // failed on the previous passthru
                oid = (int) meta[REQ_OID];
            else try { // cache expired
                oid = FAILURE_OUT;
                MessageUtils.setProperty(rcField, "-1", inMessage);
            }
            catch (Exception e) {
                oid = FAILURE_OUT;
            }

            new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                " cleaned up a pending request with " + meta[REQ_CID] + "," +
                meta[REQ_SUCCESS] + " " + meta[REQ_FAILURE] + "/" + m + ": " +
                oid + "/" + meta[REQ_OID] + " " + meta[REQ_QUORUM] + " " +
                meta[REQ_TTL]/1000).send();

            if (displayMask > 0) try { // display message
                String text = "";
                if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                    text = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleList.getKey(rid) +
                    " map-reduced a expired request to " + oid + ":" +
                    MessageUtils.display(inMessage, text, displayMask,
                    displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleList.getKey(rid)+
                    " failed to display msg: " + e.toString()).send();
            }
            i = passthru(currentTime, inMessage, in, rid, oid, mid, 0);
            if (i > 0) { // passthru is done
                count ++;
                pendList.remove(mid);
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_SIZE] --;
            }
            if (ruleInfo[RULE_EXTRA] < ruleInfo[RULE_OPTION])
                ruleInfo[RULE_EXTRA] = checkLinks((int) ruleInfo[RULE_OPTION],
                    (int[]) rule.get("SelectedOutLink"));
        }

        return count;
    }

    /**
     * It returns number of outLinks in oids that are not full yet.
     */
    private int checkLinks(int n, int[] oids) {
        Object[] asset;
        XQueue xq;
        long[] outInfo;
        int i, m, oid;
        if (oids == null || oids.length < n)
            return -1;
        m = 0;
        for (i=0; i<n; i++) {
            oid = oids[i];
            asset = (Object[]) assetList.get(oid);
            outInfo = assetList.getMetaData(oid);
            if (asset == null || outInfo == null) {
                m ++;
                new Event(Event.ERR, name+": bad out list for "+oid).send();
                continue;
            }
            xq = (XQueue) asset[ASSET_XQ];
            if (outInfo[OUT_LENGTH] == 0 &&
                outInfo[OUT_SIZE] < outInfo[OUT_CAPACITY])
                m ++;
            else if (outInfo[OUT_LENGTH] > 0 &&
                outInfo[OUT_SIZE] < outInfo[OUT_LENGTH])
                m ++;
        }
        return m;
    }

    /**
     * returns -(BOUNDARY+1) to have the container enable ack regardlessly
     */
    public int getOutLinkBoundary() {
        return -BOUNDARY-1;
    }

    /**
     * cleans up MetaData for all XQs and messages
     */
    public void resetMetaData(XQueue in, XQueue[] out) {
        Object[] asset;
        XQueue xq;
        Map rule;
        Browser browser;
        long[] state;
        int[] list;
        int i, j, n, mid, oid, id, outCapacity;

        feedback(in, -1L);
        n = msgList.size();
        list = new int[n];
        n = msgList.queryIDs(list);
        for (i=0; i<n; i++) {
            mid = list[i];
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            id = (int) state[MSG_BID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null) {
                msgList.remove(mid);
                continue;
            }
            xq = (XQueue) asset[ASSET_XQ];
            if (xq != null) synchronized(xq) {
                if (xq.getCellStatus(id) == XQueue.CELL_OCCUPIED) {
                    xq.takeback(id);
                }
                else if (xq.getCellStatus(id) == XQueue.CELL_TAKEN) {
                    xq.remove(id);
                }
            }
            in.putback(mid);
            msgList.remove(mid);
        }

        browser = pendList.browser();
        while ((i = browser.next()) >= 0) {
            in.putback(i);
        }
        pendList.clear();

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            state = assetList.getMetaData(i);
            state[OUT_SIZE] = 0;
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            state = ruleList.getMetaData(i);
            state[RULE_SIZE] = 0;
            rule = (Map) ruleList.get(i);
        }
    }

    /** acknowledges the request msg only */
    public void acknowledge(long[] state) throws JMSException {
        if (state != null && state.length > 0 && reqList != null) {
            int k = reqList.putback((int) state[0]);
            if (k <= 0) // ack failed
                new Event(Event.ERR, name + ": failed to ack the msg at " +
                    state[0] + ": " + k + "/" +
                    reqList.getCellStatus((int) state[0])).send();
        }
    }

    public void close() {
        super.close();
        pendList.clear();
        reqList.clear();
    }

    protected void finalize() {
        close();
    }
}
