package org.qbroker.node;

/* XSLTNode.java - a MessageNode transforming JMS messages with XSLs */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
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
import org.w3c.dom.Node;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.QuickCache;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.Utils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * XSLTNode transforms XML payload of JMS TextMessages into various
 * formats based on the XSL templates.  The URI of the xslt template should be
 * specified in the properties of the message.   The URI has to be accessible
 * from the filesystem or the supported storage.  The original XML payload
 * should be always stored in the message body.
 *<br><br>
 * XSLTNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the transformation
 * parameters, a template for the path to the XSL template files and their TTL.
 * The cache count of templates for the rule will be stored to RULE_PEND. It
 * will be updated when the session time exceeds the given SessionTimeout.
 * XSLTNode caches every xslt templates dynamically.  You can set the TTL for
 * each rule so that its templets will expire and will be removed automatically.
 * For parameters, XSLTNode supports dynamic setting of parameters.  It means
 * you can reference properties of the message in your parameters.  XSLTNode
 * will retrieve the data from incoming message and set the parameters before
 * the transformation.
 *<br><br>
 * If TargetXPath is defined, XSLTNode expects another xml document stored in
 * the field specified by XMLField. It will extract the xml content according
 * to SourceXPath and merges the result into the XML payload of the message
 * at the position specified by TargetXPath.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class XSLTNode extends org.qbroker.node.Node {
    private int sessionTimeout = 0;
    private TransformerFactory tFactory = null;
    private DocumentBuilder builder = null;
    private XPath xpath = null;
    private Transformer defaultTransformer = null;

    private QuickCache cache = null;  // for storing XSLTs
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public XSLTNode(Map props) {
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
            operation = "transform";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);

        try {
            tFactory = TransformerFactory.newInstance();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to get XSLT factory: " + e.getMessage()));
        }

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
                    ruleInfo[RULE_TTL]/1000 + " " + ruleInfo[RULE_MODE] + " - "+
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID Option TTL Mode - "+
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
        Iterator iter;
        List list;
        String key, str, ruleName, preferredOutName;
        char c = System.getProperty("path.separator").charAt(0);
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
        else { // for xslt
            if ((o = ph.get("URITemplate")) != null && o instanceof String) {
                str = (String) o;
                rule.put("URITemplate", new Template(str));
                if((o=ph.get("URISubstitution")) != null && o instanceof String)
                    rule.put("URISubstitution",new TextSubstitution((String)o));
            }
            else if ((o = ph.get("FieldName")) != null && o instanceof String) {
                rule.put("URITemplate", new Template("##" + (String) o + "##"));
            }
            else if (!ph.containsKey("TargetXPath")) // not XMerge
                throw(new IllegalArgumentException(name + " " + ruleName +
                    ": URITemplate or FieldName is not defined"));

            if ((o = ph.get("TargetXPath")) != null) { // for XMerge
                XPathExpression xpe;
                str = (String) o;

                if (builder == null) try {
                    builder = Utils.getDocBuilder();
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(ruleName + " failed " +
                         "to init builder: " + Event.traceStack(ex)));
                }
                catch (Error ex) {
                    throw(new IllegalArgumentException("failed to get " +
                        "builder for "+ ruleName +": "+ Event.traceStack(ex)));
                }

                try {
                    if (xpath == null)
                        xpath = XPathFactory.newInstance().newXPath();
                    xpe = xpath.compile(str);
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(name + ": failed to " +
                        "compile XPath of '" + str + "' for " + ruleName));
                }
                rule.put("Target", xpe);

                if ((o = ph.get("SourceXPath")) != null) {
                    str = (String) o;
                }
                else
                    str = "/*/*";
                try {
                    xpe = xpath.compile(str);
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(name + ": failed to " +
                        "compile XPath of '" + str + "' for " + ruleName));
                }
                rule.put("Source", xpe);

                if (defaultTransformer == null) try {
                    defaultTransformer = tFactory.newTransformer();
                    defaultTransformer.setOutputProperty(OutputKeys.INDENT,
                        "yes");
                }
                catch (TransformerConfigurationException e) {
                    throw(new IllegalArgumentException(name + " failed " +
                         "to get the default transformer: " +
                         Event.traceStack(e)));
                }
                catch (Error e) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XSLT factory for "+ name +" "+Event.traceStack(e)));
                }

                if((o = ph.get("ReplaceMode")) != null && o instanceof String &&
                    "true".equalsIgnoreCase((String) o))
                    ruleInfo[RULE_MODE] = 1;

                if ((o = ph.get("XMLField")) != null && o instanceof String)
                    rule.put("XMLField", (String) o);
                else if (ruleInfo[RULE_MODE] == 0)
                    throw(new IllegalArgumentException(name + " " + ruleName +
                        ": XMLField is not defined"));

                ruleInfo[RULE_PID] = TYPE_XPATH;
            }
            else if ((o=ph.get("XSLParameter")) != null && o instanceof Map) {
                Template temp;
                Map<String, Object> params = new HashMap<String, Object>();
                iter = ((Map) o).keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    str = (String) ((Map) o).get(key);
                    temp = new Template(str);
                    if (temp == null || temp.size() <= 0)
                        params.put(key, str);
                    else {
                        params.put(key, temp);
                        ruleInfo[RULE_EXTRA] ++;
                    }
                }
                rule.put("XSLParameter", params);
                ruleInfo[RULE_PID] = TYPE_XSLT;
            }
            else {
                rule.put("XSLParameter", new HashMap());
                ruleInfo[RULE_PID] = TYPE_XSLT;
            }

            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
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
     * It transforms the XML content according to the template of uri and the
     * parameters.  The message will be modified as the result of the
     * transformation.  Upon success, it returns RESULT_OUT as the index
     * of the outlink.  Otherwise, FAILURE_OUT is returned.
     */
    private int transform(String uri, String xml, long currentTime, int rid,
        int ttl, Map params, byte[] buffer, Message msg) {
        int i;
        Iterator iter;
        Object o;
        Templates template;
        Transformer transformer;
        String key = null, value;

        if (xml == null || xml.length() <= 0 || tFactory == null || msg == null)
            return FAILURE_OUT;

        template = (Templates) cache.get(uri, currentTime);
        if (template == null) try { // initialize the first one
            long[] info;
            template = tFactory.newTemplates(new StreamSource(uri));
            i = cache.insert(uri, currentTime, ttl, new int[]{rid}, template);
            info = ruleList.getMetaData(rid);
            if (info != null)
                info[RULE_PEND] ++;
            if (ttl > 0) { // check mtime of the cache
                long tm = cache.getMTime();
                if (tm > currentTime + ttl)
                    cache.setStatus(cache.getStatus(), currentTime + ttl);
            }
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + ": xsl template compiled for "+
                    uri + ": " + i).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, name +" failed to compile template of " + uri +
               " for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        try { // set parameters
            transformer = template.newTransformer();
//            transformer.clearParameters();
            if (params != null && params.size() > 0) { // with parameters
                for (iter=params.keySet().iterator(); iter.hasNext();) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    o = params.get(key);
                    if (o == null || !(o instanceof String))
                        continue;
                    transformer.setParameter(key, (String) o);
                }
            }
        }
        catch (TransformerException e) {
            new Event(Event.ERR, name +" failed to get transformer of '" + key +
               "' for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        StringReader sr = new StringReader(xml);
        try {
            StringWriter sw = new StringWriter();
            transformer.transform(new StreamSource(sr), new StreamResult(sw));
            value = sw.toString();
            sw.close();
        }
        catch (IOException e) {
            new Event(Event.ERR, name +" failed to write for " + uri +
               ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (TransformerException e) {
            new Event(Event.ERR, name +" transform failed for " + uri +
               ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        sr.close();

        if (value != null && (i = value.length()) > 0) try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(value);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(value.getBytes(), 0, i);
            else {
                new Event(Event.ERR, name+": unsupported type for " +
                    uri).send();
                return FAILURE_OUT;
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, name +" failed to set message body for "+uri+
               ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + " transform is done on " +
                uri + " with "+ value.length() + " bytes").send();

        return RESULT_OUT;
    }

    /**
     * It extracts the XML content from the source XML specified by the key 
     * based on the source XPath expression, and merges the xml content into
     * the XML payload of the message at the position specified by the target
     * XPath expression. Upon success, it returns RESULT_OUT as the index of
     * the outlink.  Otherwise, FAILURE_OUT is returned.
     */
    private int xmerge(long currentTime, String rule, String key, String xml,
        int option, XPathExpression source, XPathExpression target,
        Transformer transformer, byte[] buffer, Message msg) {
        XPathExpression xpe;
        NodeList list, nodes;
        Document doc;
        Object o;
        String str = null, value;
        int i, n;

        if (xml == null || xml.length() <= 0 || builder == null ||
            transformer == null)
            return FAILURE_OUT;

        if (key != null) try {
            str = MessageUtils.getProperty(key, msg);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to get XML source from '" +
                key + "' for " + rule + ": " + Event.traceStack(e)).send();
            str = null;
        }

        if (str == null)
            return FAILURE_OUT;

        StringReader sr = new StringReader(str);
        try {
            doc = builder.parse(new InputSource(sr));
            list = (NodeList) source.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, name + " failed to parse XML source in '" +
                key + "' for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (SAXException e) {
            new Event(Event.ERR, name +" failed to build source XML DOM from '"+
                key + "' for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR, name + " failed to evaluate source xpath on '"+
                key + "' for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        sr.close();

        sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
            nodes = (NodeList) target.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, name + " failed to parse XML payload " +
                "for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (SAXException e) {
            new Event(Event.ERR, name + " failed to build XML DOM " +
                "for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR,name+" failed to evaluate xpath on paylaod for "
                + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        sr.close();

        o = null;
        try {
            n = nodes.getLength();
            if (n > 0)
                o = nodes.item(0).getParentNode();
            if (o != null) { // found the parent node
                Node parent = (org.w3c.dom.Node) o;
                if (option > 0) { // remove the selected nodes
                    for (i=0; i<n; i++) // remove all nodes from the target
                        parent.removeChild(nodes.item(i));
                }
                n = list.getLength();
                for (i=0; i<n; i++) // append all nodes from the source
                    parent.appendChild(doc.importNode(list.item(i), true));
                if (n > 0 || option > 0) { // target changed
                    StreamResult result = new StreamResult(new StringWriter());
                    transformer.transform(new DOMSource(doc), result);
                    msg.clearBody();
                    if (msg instanceof TextMessage)
                     ((TextMessage) msg).setText(result.getWriter().toString());
                    else
      ((BytesMessage) msg).writeBytes(result.getWriter().toString().getBytes());
                }
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + " failed to format msg with xml for " +
                rule + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to merge xml for " +
                rule + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        return RESULT_OUT;
    }

    private int xcut(long currentTime, String rule, String xml,
        XPathExpression xpe, Transformer transformer, byte[] buffer,
        Message msg) {
        NodeList nodes;
        Document doc;
        Object o;
        int i, n;

        if (xml == null || xml.length() <= 0 || builder == null ||
            transformer == null || xpe == null)
            return FAILURE_OUT;


        StringReader sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
            nodes = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, name + " failed to parse XML payload " +
                "for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (SAXException e) {
            new Event(Event.ERR, name + " failed to build XML DOM " +
                "for " + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR,name+" failed to evaluate xpath on paylaod for "
                + rule + ": " + Event.traceStack(e)).send();
            sr.close();
            return FAILURE_OUT;
        }
        sr.close();

        o = null;
        try {
            n = nodes.getLength();
            if (n > 0)
                o = nodes.item(0).getParentNode();
            if (o != null) { // found the parent node
                Node parent = (org.w3c.dom.Node) o;
                for (i=0; i<n; i++) // remove all nodes from the target
                    parent.removeChild(nodes.item(i));
                if (n > 0) { // xml changed
                    StreamResult result = new StreamResult(new StringWriter());
                    transformer.transform(new DOMSource(doc), result);
                    msg.clearBody();
                    if (msg instanceof TextMessage)
                     ((TextMessage) msg).setText(result.getWriter().toString());
                    else
      ((BytesMessage) msg).writeBytes(result.getWriter().toString().getBytes());
                }
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + " failed to format msg with xml for " +
                rule + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to cut xml for " +
                rule + ": " + Event.traceStack(e)).send();
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
        String msgStr = null, ruleName = null, xmlField = null;
        Object o = null;
        Object[] asset;
        Map rule = null, params = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        XPathExpression source = null, target = null;
        Template temp = null;
        TextSubstitution tsub = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        Iterator iter;
        long currentTime, previousTime, wt;
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
                if (ruleInfo[RULE_PID] == TYPE_XSLT) {
                    temp = (Template) rule.get("URITemplate");
                    tsub = (TextSubstitution) rule.get("URISubstitution");
                    params = (Map) rule.get("XSLParameter");
                }
                else if (ruleInfo[RULE_PID] == TYPE_XPATH) {
                    xmlField = (String) rule.get("XMLField");
                    source = (XPathExpression) rule.get("Source");
                    target = (XPathExpression) rule.get("Target");
                }
                previousRid = rid;
            }

            if (i >= 0) try { // normal format
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
                i = -2;
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to reset the msg: "+ Event.traceStack(e)).send();
            }

            if (i < 0) { // failed to apply filters or failure on reset
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_XSLT) { // for xslt
                String uri = null;
                if (temp != null) {
                    uri = MessageUtils.format(inMessage, buffer, temp);
                    if (tsub != null)
                        uri = tsub.substitute(uri);
                }

                if (uri != null && uri.length() > 0) {
                    i = RESULT_OUT;

                    // populate params
                    if (params != null && ruleInfo[RULE_EXTRA] > 0) {
                        String str;
                        iter = params.keySet().iterator();
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
                            i = transform(uri, msgStr, currentTime, rid,
                                (int)ruleInfo[RULE_TTL], ph, buffer, inMessage);
                        }
                    }
                    else {
                        i = transform(uri, msgStr, currentTime, rid,
                            (int)ruleInfo[RULE_TTL], params, buffer, inMessage);
                    }
                }
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": empty uri for the file on "+
                        ruleName).send();
                }

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " + uri +
                         " = " + cid + ":" + rid +" "+ i).send();

                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else if (ruleInfo[RULE_PID] == TYPE_XPATH) { // for xmerge
                i = RESULT_OUT;
                int m = (int) ruleInfo[RULE_MODE];
                if (xmlField != null) // xmerge
                    i = xmerge(currentTime, ruleName, xmlField, msgStr, m,
                        source,target,defaultTransformer,buffer,inMessage);
                else // xcut
                    i = xcut(currentTime, ruleName, msgStr, target,
                        defaultTransformer, buffer, inMessage);

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " + xmlField +
                         " = " + cid + ":" + rid +" "+ i).send();

                oid = outLinkMap[i];
                ruleInfo[RULE_OID] = oid;
            }
            else { // preferred ruleset or nohit
                oid = (int) ruleInfo[RULE_OID];
            }

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                new Event(Event.INFO, name +": "+ ruleName +" transformed msg "+
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
        super.close();
        cache.clear();
        tFactory = null;
        builder = null;
        xpath = null;
        defaultTransformer = null;
    }

    protected void finalize() {
        close();
    }
}
