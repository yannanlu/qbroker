package org.qbroker.node;

/* ScriptNode.java - a MessageNode with script support on JMS Messages */

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
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.Invocable;
import javax.script.Compilable;
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
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * ScriptNode extracts the path of a JSR223 compliant script and the JSON data
 * from the incoming JMS message. It invokes the script on the parsed JSON data
 * to format the payload. The formatted JSON data will be set back to the
 * message body as the result. The message will be routed to the first outlink
 * upon success. Due to JSR223, it requires Java 6 to compile and to run. Make
 * sure all the jars for the ScriptEngines are loaded.
 *<br/><br/>
 * ScriptNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the format
 * parameters, such URITemplate to build the path to the script file,
 * ScriptEngine and TTL for the cache.
 *<br/><br/>
 * If ScriptEngine is defined for a rule, ScriptNode will format the JSON data
 * with the script specified in the messages. The script is supposed to have
 * a function of format defined. It takes two Java Map objects as the only
 * arguments. The first Map is the representation of the JSON data. The
 * function is supposed to modify the Map. The second Map contains the
 * parameters for the function. The updated Map will be converted back to JSON
 * payload of the message. The cache count of engines for the rule will be
 * stored to RULE_PEND. It will be updated when the session time exceeds the
 * given SessionTimeout.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ScriptNode extends Node {
    private int sessionTimeout = 0;
    private ScriptEngineManager factory;

    private QuickCache cache = null;  // for storing script engines
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public ScriptNode(Map props) {
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
            operation = "invoke";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);

        factory = new ScriptEngineManager();
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

        if (outLinkMap[NOHIT_OUT] >= assetList.size())
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

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("URITemplate")) != null && o instanceof String) {
            str = (String) o;
            rule.put("Template", new Template(str));
            if (( o = ph.get("URISubstitution")) != null && o instanceof String)
                rule.put("Substitution", new TextSubstitution((String)o));

            if ((o = ph.get("Parameter")) != null && o instanceof Map) {
                Template temp;
                Map<String, Object> params = new HashMap<String, Object>();
                iter = ((Map) o).keySet().iterator();
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

            if ((o = ph.get("ScriptEngine")) != null && o instanceof String) {
                factory.getEngineByName((String) o);
                ruleInfo[RULE_PID] = TYPE_SCRIPT;
            }
            else {
                ruleInfo[RULE_PID] = TYPE_BYPASS;
            }
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

    private int invoke(long currentTime, String uri, Map json, int rid,
        int ttl, String engineName, Map params, Message msg) {
        int i;
        Invocable script;
        String text = null;

        if (uri == null || uri.length() <= 0 || json == null)
            return FAILURE_OUT;

        script = (Invocable) cache.get(uri, currentTime);
        if (script == null) try { // initialize the first one
            ScriptEngine engine = null;
            long[] info;
            engine = factory.getEngineByName(engineName);
            if (engine != null) {
                FileReader fr = new FileReader(uri);
                if (engine instanceof Compilable)
                    ((Compilable) engine).compile(fr).eval();
                else
                    engine.eval(fr);
                fr.close();
                i = cache.insert(uri, currentTime, ttl, new int[]{rid, 1},
                    engine);
                info = ruleList.getMetaData(rid);
                if (info != null)
                    info[RULE_PEND] ++;
                if (ttl > 0) { // check mtime of the cache
                    long tm = cache.getMTime();
                    if (tm > currentTime + ttl)
                        cache.setStatus(cache.getStatus(), currentTime + ttl);
                }
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name+": a new script compiled for "+
                        uri + ": " + i).send();
            }
            else {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to get the script engine of " + engineName +
                    " for "+ uri).send();
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name +": " + ruleList.getKey(rid) +
               " failed to compile script for " + uri + ": " +
               Event.traceStack(e)).send();
            return FAILURE_OUT;
        }

        try {
            script.invokeFunction("format", json, params);
        }
        catch (Exception e) {
            new Event(Event.ERR, name +": " +ruleList.getKey(rid) +
               " failed to invoke function of format in " + uri + ": " +
               Event.traceStack(e)).send();
            return FAILURE_OUT;
        }
 
        text = JSON2FmModel.toJSON(json, null, null);

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
        String msgStr = null, ruleName = null, engineName = null;
        Map rule = null, params = null;
        Browser browser;
        Object[] asset;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        Template temp = null;
        TextSubstitution tsub = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
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
                            " scripts expired").send();
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
                if (ruleInfo[RULE_PID] == TYPE_SCRIPT) {
                    temp = (Template) rule.get("Template");
                    tsub = (TextSubstitution) rule.get("Substitution");
                    params = (Map) rule.get("Paramenter");
                    engineName = (String) rule.get("ScriptEngine");
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

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_SCRIPT) { // for script engine
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

                if (uri != null && uri.length() > 0) {
                    Map<String, Object> ph = null;
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
                        ruleInfo[RULE_EXTRA] > 0) { // polulate params
                        String str;
                        Object o;
                        Iterator iter = params.keySet().iterator();
                        ph = new HashMap<String, Object>();
                        while (iter.hasNext()) { // set parameters
                            str = (String) iter.next();
                            if (str == null || str.length() <= 0)
                                continue;
                            o = params.get(str);
                            if (!(o instanceof Template))
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

                    if (i == RESULT_OUT) {
                        i = invoke(currentTime, uri, json, rid,
                            (int)ruleInfo[RULE_TTL], engineName, ph, inMessage);
                    }
                }
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " got an empty uri for the script").send();
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
                new Event(Event.INFO, name +": "+ ruleName +
                    " invokded the script on msg "+
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
                   " failed to format the msg: " + Event.traceStack(e)).send();
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
        if (factory != null)
            factory = null;
    }

    protected void finalize() {
        close();
    }
}
