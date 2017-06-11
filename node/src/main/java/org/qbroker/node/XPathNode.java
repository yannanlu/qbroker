package org.qbroker.node;

/* XPathNode.java - a MessageNode retrieving data from JMS messages via XPath */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.StringReader;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
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
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.event.Event;

/**
 * XPathNode parses XML payload of JMS Messages, retrieves text data from
 * the XML payload according to the predifined XPath expressions.  It then
 * sets them into message as the properties.  The original XML payload should 
 * be always stored in the message body.  XPathNode will not modify the XML
 * payload.
 *<br/><br/>
 * XPathNode supports dynamic setting of xpath expressions.  It means you can
 * reference properties of the message in your xpath expressions.  XPathNode
 * will retrieve the data from the incoming message and compiles the expressions
 * before the evaluation.
 *<br/><br/>
 * XPathNode allows one of the xpath expressions to select a list of items.
 * In this case, ListKey has to be defined. It specifies the key for that
 * special xpath expression. The expression will be applied in the context of
 * XPathConstants.NODESET. XPathNode will retrieves the data from each item
 * and appends them one by one with a predefined delimiter.
 *<br/><br/>
 * This requires Xalan-J 2.7.0 due to JAXP 1.3 for XPath support.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class XPathNode extends Node {
    private DocumentBuilder builder = null;
    private XPath xpath = null;
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public XPathNode(Map props) {
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
            operation = "xparse";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        try {
            xpath = XPathFactory.newInstance().newXPath();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to get XPath: " +
                e.getMessage()));
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

        try {
            DocumentBuilderFactory fa = DocumentBuilderFactory.newInstance();
            fa.setNamespaceAware(true);
            builder = fa.newDocumentBuilder();
        }
        catch(ParserConfigurationException e) {
            throw(new IllegalArgumentException(name+": DOM builder failed"));
        }

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_MODE] + " " + ruleInfo[RULE_DMASK] + " " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name +
                " RuleName: RID PID Option Mode DMask - " +
                "OutName" + strBuf.toString()).send();
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
        int i, j, k, n, id;

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

        if ((o = ph.get("ResetOption")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = RESET_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if (preferredOutName != null) { // bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("XPathExpression")) == null ||
            !(o instanceof Map)) { // default to bypass
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else try {
            Template temp;
            XPathExpression xpe = null;
            Map<String, Object> expr = new HashMap<String, Object>();
            iter = ((Map) o).keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                str = (String) ((Map) o).get(key);
                temp = new Template(str);
                if (temp == null || temp.numberOfFields() <= 0) {
                    xpe = xpath.compile(str);
                    expr.put(key, xpe);
                }
                else
                    expr.put(key, temp);
            }
            rule.put("XPathExpression", expr);
            if ((o = ph.get("ListKey")) != null && expr.containsKey(o)) {
                // XPath for ListKey expects a NodeList
                rule.put("ListKey", (String) o);
                if ((o = ph.get("Delimiter")) != null)
                    rule.put("Delimiter", o);
                else
                    rule.put("Delimiter", "");
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

            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_XPATH;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(ruleName +
                " failed to compile XPath Expression: " + e.toString()));
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
     * It evaluates the XML content based on the XPath expressions and
     * sets message properties with the evaluation data.  Upon success, it
     * returns RESULT_OUT as the index of the outlink.  Otherwise, FAILURE_OUT
     * is returned.
     */
    private int evaluate(String xml, long currentTime, int rid,
        Map expression, String listKey, String d, byte[] buffer, Message msg) {
        Iterator iter;
        Object o;
        XPathExpression xpe;
        Document doc;
        String key = null, value;

        if (xml == null || xml.length() <= 0 || xpath == null || builder==null)
            return FAILURE_OUT;

        StringReader sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
        }
        catch (IOException e) {
            new Event(Event.ERR, name + " failed to parse XML payload " +
                "for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (SAXException e) {
            new Event(Event.ERR, name + " failed to build XML DOM " +
                "for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        sr.close();

        if (expression != null && expression.size() > 0) try {
            if (listKey != null) { // one of keys expects a NodeList
                NodeList list;
                StringBuffer strBuf;
                int i, n;
                for (iter=expression.keySet().iterator(); iter.hasNext();) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    o = expression.get(key);
                    if (o == null)
                        continue;
                    else if (o instanceof XPathExpression)
                        xpe = (XPathExpression) o;
                    else if (o instanceof Template) {
                        value = MessageUtils.format(msg, buffer, (Template) o);
                        xpe = xpath.compile(value);
                    }
                    else
                        continue;
                    if (!key.equals(listKey)) { // not a listKey
                        value = xpe.evaluate(doc);
                        MessageUtils.setProperty(key, value, msg);
                        continue;
                    }
                    // listKey
                    list = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);
                    strBuf = new StringBuffer();
                    n = list.getLength();
                    for (i=0; i<n; i++) {
                        value = Utils.nodeToText(list.item(i));
                        if (i > 0)
                            strBuf.append(d);
                        strBuf.append(value);
                    }
                    MessageUtils.setProperty(key, strBuf.toString(), msg);
                }
            }
            else {
                for (iter=expression.keySet().iterator(); iter.hasNext();) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    o = expression.get(key);
                    if (o == null)
                        continue;
                    else if (o instanceof XPathExpression)
                        xpe = (XPathExpression) o;
                    else if (o instanceof Template) {
                        value = MessageUtils.format(msg, buffer, (Template) o);
                        xpe = xpath.compile(value);
                    }
                    else
                        continue;
                    value = xpe.evaluate(doc);
                    MessageUtils.setProperty(key, value, msg);
                }
            }
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR, name + " failed to evaluate xpath '" +
                key + "' of " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + " failed to format msg for xpath '" +
                key + "' of " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " evaluation is done on " + rid +
                " with " + expression.size() + " xpaths").send();

        return RESULT_OUT;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        String msgStr = null, ruleName = null, key = null, lky = null, d = null;
        Object o = null;
        Object[] asset;
        Map rule = null, expression = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, n, previousRid;
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
                if (ruleInfo[RULE_PID] == TYPE_XPATH) {
                    expression = (Map) rule.get("XPathExpression");
                    if (ruleInfo[RULE_MODE] == 1) {
                        d = (String) rule.get("Delimiter");
                        lky = (String) rule.get("ListKey");
                    }
                }
                previousRid = rid;
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_XPATH) {
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

                i = evaluate(msgStr, currentTime, rid, expression, lky, d,
                    buffer, inMessage);

                if (i != FAILURE_OUT && ruleInfo[RULE_EXTRA] > 0) try {
                    // reset timestamp
                    long st;
                    String value;
                    DateFormat dateFormat = null;
                    key = (String) rule.get("TSKey");
                    dateFormat = (DateFormat) rule.get("DateFormat");
                    value = MessageUtils.getProperty(key, inMessage);
                    if (dateFormat != null)
                        st = dateFormat.parse(value).getTime();
                    else
                        st = Long.parseLong(value);
                    inMessage.setJMSTimestamp(st);
                }
                catch (Exception ex) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to set timestamp from " + key +
                        ": " + ex.toString()).send();
                    i = FAILURE_OUT;
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid +" "+ i).send();

                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else { // preferred ruleset or nohit
                if (filter != null && filter.hasFormatter()) try {
                    // for post format
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
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to reset the msg: "+
                        Event.traceStack(e)).send();
                }
                oid = (int) ruleInfo[RULE_OID];

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid).send();
            }

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                new Event(Event.INFO, name + ": " + ruleName + " xparsed msg " +
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
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

    public void close() {
        super.close();
        builder = null;
        xpath = null;
    }

    protected void finalize() {
        close();
    }
}
