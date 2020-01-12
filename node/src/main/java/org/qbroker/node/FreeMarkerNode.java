package org.qbroker.node;

/* FreeMarkerNode.java - a MessageNode with FreeMarker template engine */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Date;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.xml.sax.InputSource;
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
import org.qbroker.json.JSON2FmModel;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * FreeMarkerNode extracts the path of a FreeMarker template and the data from
 * the incoming JMS message and applies the FreeMarker template to the data.
 * The output will be set to the message body as the result. The message will
 * be routed to the first outlink.
 *<br><br>
 * FreeMarkerNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the transformation
 * parameters, such as DataType, RootNodeName and a template to build the the
 * paths to the FreeMarker template files, as well as their TTL.
 * The cache count of templates for the rule will be stored to RULE_PEND. It
 * will be updated when the session time exceeds the given SessionTimeout.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class FreeMarkerNode extends org.qbroker.node.Node {
    private int sessionTimeout = 0;
    private freemarker.template.Configuration fmCfg = null;

    private QuickCache cache = null;  // for storing FreeMarker templates
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;
    public final static int FM_PROPS = 0;
    public final static int FM_JSON = 1;
    public final static int FM_XML = 2;
    public final static int FM_JMS = 3;

    public FreeMarkerNode(Map props) {
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

        try {
            fmCfg = new freemarker.template.Configuration();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to get FreeMarker configuration: " + e.getMessage()));
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
                    ruleInfo[RULE_TTL]/1000 + " " + ruleInfo[RULE_DMASK] + " " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG,name+" RuleName: RID PID Option TTL DMask - "+
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
        else { // for FreeMarker
            if ((o = ph.get("URITemplate")) != null && o instanceof String) {
                str = (String) o;
                rule.put("URITemplate", new Template(str));
                if((o=ph.get("URISubstitution")) != null && o instanceof String)
                    rule.put("URISubstitution",new TextSubstitution((String)o));
            }
            else
                throw(new IllegalArgumentException(name + " " + ruleName +
                    ": URITemplate or FieldName is not defined"));

            if ((o = ph.get("DataType")) != null && o instanceof String) {
                str = (String) o;
                if ("json".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = FM_JSON;
                else if ("props".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = FM_PROPS;
                else if ("xml".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = FM_XML;
                else if ("jms".equalsIgnoreCase(str))
                    ruleInfo[RULE_MODE] = FM_JMS;
                else {
                    ruleInfo[RULE_MODE] = FM_JMS;
                    new Event(Event.ERR, name +" unsupported data type for " +
                        ruleName + ": " + str).send();
                }
            }
            else
                ruleInfo[RULE_MODE] = FM_JMS;

            if ((o = ph.get("RootNodeName")) != null && o instanceof String) {
                str = (String) o;
                rule.put("RootNodeName", str);
                if (ruleInfo[RULE_MODE] == FM_XML && str.length() <= 0)
                    new Event(Event.WARNING, name + " empty RootNodeName for " +
                        ruleName).send();
            }
            ruleInfo[RULE_PID] = TYPE_JSONT;
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

    private int format(String uri, long currentTime, int rid, int ttl,
        Map data, Message msg) {
        int i;
        StringWriter out;
        freemarker.template.Template template = null;

        if (uri == null || uri.length() <= 0 || msg == null)
            return FAILURE_OUT;

        template = (freemarker.template.Template) cache.get(uri, currentTime);
        if (template == null) try { // initialize the first one
            long[] info;
            template = fmCfg.getTemplate(uri);
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
                new Event(Event.DEBUG, name + ": a new template compiled for "+
                    uri + ": " + i).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, name +" failed to compile template of " + uri +
               " for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        out = new StringWriter();
        try {
            template.process(data, out);
        }
        catch (freemarker.template.TemplateException e) {
            new Event(Event.ERR, name +" got template error on " + uri +
               " for rule " + rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
        catch (Exception e) {
            return FAILURE_OUT;
        }
 
        try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(out.toString());
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(out.toString().getBytes());
            else {
                new Event(Event.ERR, name + ": rule of " + rid +
                    " failed to set body: bad msg family").send();
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name +" failed to load msg for rule " +
                rid + ": " + Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        return RESULT_OUT;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    @SuppressWarnings("unchecked")
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        String msgStr = null, ruleName = null, rootName = null;
        Object o = null;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
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
                    rootName = (String) rule.get("RootNodeName");
                }
                previousRid = rid;
            }

            if (i >= 0) try {
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
            else if (ruleInfo[RULE_PID] == TYPE_JSONT) { // for FreeMarker
                String uri = null;
                if (temp != null) {
                    uri = MessageUtils.format(inMessage, buffer, temp);
                    if (tsub != null)
                        uri = tsub.substitute(uri);
                }

                if (uri != null && uri.length() > 0) {
                    Map data = null;
                    i = RESULT_OUT;

                    if (ruleInfo[RULE_MODE] == FM_JMS) {
                        try {
                            if (rootName != null) {
                                data = new HashMap();
                                data.put(rootName, getFMModel(inMessage));
                            }
                            else
                                data = getFMModel(inMessage);
                        }
                        catch (JMSException e) {
                            i = FAILURE_OUT;
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to get data model from msg: " +
                                e.toString()).send();
                        }
                    }
                    else if (msgStr == null || msgStr.length() <= 0) {
                        data = new HashMap();
                    }
                    else try { // parse the data
                        Properties ph;
                        ByteArrayInputStream bin = null;
                        InputSource sin = null;
                        switch ((int) ruleInfo[RULE_MODE]) {
                          case FM_XML:
                            sin = new InputSource(new StringReader(msgStr));
                            data.put(rootName,
                                freemarker.ext.dom.NodeModel.parse(sin));
                            break;
                          case FM_JSON:
                            data =
                              (Map)JSON2FmModel.parse(new StringReader(msgStr));
                            if (rootName != null)
                                data.put(rootName, data);
                            else
                                data = (Map) data;
                            break;
                          case FM_PROPS:
                            bin = new ByteArrayInputStream(msgStr.getBytes());
                            ph = new Properties();
                            ph.load(bin);
                            bin.close();
                            if (rootName != null)
                                data.put(rootName, ph);
                            else
                                data = ph;
                            break;
                          default:
                        }
                    }
                    catch (IOException e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to get data model from payload: " +
                            Event.traceStack(e)).send();
                    }
                    catch (Exception e) {
                        i = FAILURE_OUT;
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to get data model from payload: " +
                            Event.traceStack(e)).send();
                    }

                    if (i != FAILURE_OUT)
                        i = format(uri, currentTime, rid,
                            (int) ruleInfo[RULE_TTL], data, inMessage);

                    if (data != null)
                        data.clear();
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
            else { // preferred ruleset or nohit
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

    @SuppressWarnings("unchecked")
    public static Map getFMModel(Message msg) throws JMSException {
        String key;
        Map map = new HashMap();
        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS"))
                continue;
            if (msg instanceof JMSEvent && "text".equals(key))
                continue;
            map.put(key, msg.getObjectProperty(key));
        }
        return map;
    }

    public void close() {
        super.close();
        cache.clear();
        if (fmCfg != null)
            fmCfg = null;
    }

    protected void finalize() {
        close();
    }
}
