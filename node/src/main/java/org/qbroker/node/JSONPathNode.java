package org.qbroker.node;

/* JSONPathNode.java - a MessageNode retrieving data from JMS messages via JSONPath */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.StringReader;
import java.io.IOException;
import org.apache.oro.text.regex.Perl5Matcher;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.DataSet;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * JSONPathNode parses JSON payload of JMS Messages, retrieves text data from
 * the JSON payload according to the predefined JSONPath expressions. It then
 * sets them into message as the properties. The original JSON payload should 
 * be always stored in the message body. JSONPathNode will not modify the JSON
 * payload.
 *<br/><br/>
 * JSONPathNode contains a number of predefined rulesets. These rulesets
 * categorize messages into non-overlapping groups. Therefore, each rule
 * defines a unique message group. The ruleset also defines the JSONPath
 * expressions to retrieve content from the JSON payload.
 *<br/><br/>
 * JSONPathNode supports dynamic setting of JSON path expressions. It means
 * you can reference properties of the message in your json path expressions.
 * JSONPathNode will retrieve the data from the incoming message and compiles
 * the expressions before the evaluation. If TimestampKey is defined in a rule,
 * it will be used to retrieve the timestamp from the json payload to reset
 * JMSTimestamp of the message, provided TimePattern is also defined.
 *<br/><br/>
 * Currently, only the simple JSON path starting with '.' is supported.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONPathNode extends Node {
    private int[] outLinkMap;
    private Perl5Matcher pm = null;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public JSONPathNode(Map props) {
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
            operation = "jparse";
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

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[FAILURE_OUT] = overlap[0];
        outLinkMap[NOHIT_OUT] = overlap[1];

        pm = new Perl5Matcher();
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
        else if ((o = ph.get("JSONPath")) == null || !(o instanceof Map)) {
            // default to bypass
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else try { // multiple json path
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

            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_JSONPATH;
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
     * It evaluates the parsed JSON data based on the JSONPath expressions and
     * sets message properties with the evaluation data.  Upon success, it
     * returns RESULT_OUT as the index of the outlink.  Otherwise, FAILURE_OUT
     * is returned.
     */
    private int evaluate(Object json, long currentTime, int rid,
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

    /**
     * It evaluates the parsed JSON data based on the JSONPath expressions and
     * sets message properties with the evaluation data.  Upon success, it
     * returns RESULT_OUT as the index of the outlink.  Otherwise, FAILURE_OUT
     * is returned.
     */
    private int evaluate(Object json, long currentTime, int rid, String d,
        Map expression, String listPath, Message msg) {
        Iterator iter;
        Map map, ph = null;
        List list, pl = null;
        Object o;
        String key = null, str, path;
        StringBuffer strBuf;
        int i, n = 0;
        boolean isArray;

        if (json == null || listPath == null)
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

        strBuf = new StringBuffer();
        if (expression != null && expression.size() > 0) try {
            iter = expression.keySet().iterator();
            key = (String) iter.next();
            if (isArray)
                list = (List) JSON2Map.get(pl, listPath);
            else
                list = (List) JSON2Map.get(ph, listPath);
            if (list == null)
                list = new ArrayList();
            n = list.size();
            path = (String) expression.get(key);
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null)
                    continue;
                else if (o instanceof Map) {
                    o = JSON2Map.get((Map) o, path);
                }
                else if (o instanceof List) {
                    o = JSON2Map.get((List) o, path);
                }
                else // not a container
                    continue;
                if (o == null)
                    continue;
                else if (o instanceof String)
                    str = (String) o;
                else if (o instanceof Map)
                    str = JSON2Map.toJSON((Map) o, null, "");
                else if (o instanceof List)
                    str = JSON2Map.toJSON((List) o, null, "");
                else
                    str = o.toString();
                if (i > 0)
                    strBuf.append(d);
                strBuf.append(str);
            }
            MessageUtils.setProperty(key, strBuf.toString(), msg);
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
                " completed evaluation on " + n + " items: " + strBuf).send();

        return RESULT_OUT;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        String msgStr = null, ruleName = null, key = null, d = null,path = null;
        Object o = null, json = null;
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
            msgStr = null;
            rid = 0;
            i = 0;
            try {
                StringReader sr = null;
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
                if (ruleInfo[RULE_PID] == TYPE_JSONPATH) {
                    expression = (Map) rule.get("JSONPath");
                    if (ruleInfo[RULE_MODE] == 1) {
                        d = (String) rule.get("Delimiter");
                        path = (String) rule.get("ListPath");
                    }
                }
                else if (ruleInfo[RULE_PID] == TYPE_JSONT) {
                    path = (String) rule.get("JSONPath");
                    d = (String) rule.get("KeyField");
                }
                else if (ruleInfo[RULE_PID] == TYPE_JSONT + TYPE_JSONPATH) {
                    expression = (Map) rule.get("JSONPath");
                }
                previousRid = rid;
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (ruleInfo[RULE_PID] == TYPE_JSONPATH) {
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

                StringReader sr = null;
                try { // json not parsed yet
                    sr = new StringReader(msgStr);
                    json = JSON2Map.parse(sr);
                    sr.close();
                }
                catch (Exception e) {
                    json = null;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to parse JSON payload: " +
                        Event.traceStack(e)).send();
                    if (sr != null)
                        sr.close();
                }

                if (ruleInfo[RULE_MODE] == 0) // default
                    i = evaluate(json, currentTime, rid, expression, buffer,
                        inMessage);
                else // with list path
                    i = evaluate(json, currentTime, rid, d, expression, path,
                        inMessage);

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

                json = null;
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
                        " failed to format the msg: "+
                        Event.traceStack(e)).send();
                }

                oid = (int) ruleInfo[RULE_OID];

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: " +
                         cid + ":" + rid).send();
            }

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                new Event(Event.INFO, name + ": " + ruleName + " jparsed msg " +
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
}
