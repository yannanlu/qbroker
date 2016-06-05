package org.qbroker.node;

/* ParserNode.java - a MessageNode parsing JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * ParserNode parses JMS Message body and extracts properties out of it.
 * According to the rulesets, it filters messages into three outlinks: done
 * for all the parsed messages, nohit for those messages do not belong
 * to any rulesets, failure for the messages failed at the parsing process.
 * Those parsed messages will have variables extracted from the body and
 * put to the message properties for easy access.
 *<br/><br/>
 * ParserNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the parser and
 * its parsing rules for the group.  Different groups may have different
 * parser or different parser parameters.  Due to JMS specifications, you
 * have to clear user properties before setting user properties on a
 * readonly message.  You can specify ResetOption in a ruleset so that
 * the message header will be reset for modifications.  Its value is 0 for
 * no reset, 1 for optional reset and 2 for absolute reset.  With each
 * ruleset, you can also specify the data to be parsed via FieldName.
 * By default, the FieldName is set to "body", indicating to parse the
 * message body. A ruleset can also define TimestampKey and TimePattern to
 * reset JMSTimestamp of the messages for the group.  On the node level,
 * DisplayMask and StringProperty control the display result of outgoing
 * messages.
 *<br/><br/>
 * ParserNode supports a post formatter defined within the filter for the
 * parser rules. Even if the rule is a bypass one, it can also have a post
 * formatter as long as its preferred outlink is the first outlink. In case of
 * failure or nohit, the post formatter will be disabled.
 *<br/><br/>
 * ParserNode allows the developers to plugin their own parsers.
 * The requirement is minimum.  The class should have a public method of
 * parse() that takes the String to be parsed as the only argument.  The
 * returned object has to be either a Map or an Event.  It must have
 * a constructor taking a Map with a unique value for the key of Name,
 * a List or a String as the single argument for configurations.
 * ParserNode will invoke the method to parse the message body or certain
 * message property as a text.
 *<br/><br/>
 * You are free to choose any names for the three fixed outlinks.  But
 * ParserNode always assumes the first outlink for done, the second for failure
 * and the last for nohit.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ParserNode extends Node {
    private AssetList pluginList = null; // plugin method and object
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public ParserNode(Map props) {
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
            operation = "parse";
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
            throw(new IllegalArgumentException(name+": failed to init rule " +
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            Map ph;
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                ph = (Map) ruleList.get(i);
                key = (String) ph.get("FieldName");
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_MODE] + " " + ((key == null) ? "body" : key)+
                    " - " + assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID Option Mode Data "+
                "- OutName"+ strBuf.toString()).send();
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

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // for plugin
            str = (String) o;

            // store KeepBody control into RULE_MODE
            if ((o = ph.get("KeepBody")) != null && "true".equals((String) o))
                ruleInfo[RULE_MODE] = 1;

            if ((o = (String) ph.get("FieldName")) != null &&
                o instanceof String && !"body".equals((String) o))
                rule.put("FieldName",  o);
            else if ((o = (String) ph.get("MapKey")) != null)
                rule.put("MapKey",  o);

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

            if ((o = ph.get("ParserArgument")) != null) {
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
                o = MessageUtils.getPlugins(ph, "ParserArgument", "parse",
                    new String[]{"java.lang.String"}, null, name);
                id = pluginList.add(str, new long[]{1}, o);
            }

            // store the plugin ID in PID for the plugin
            ruleInfo[RULE_PID] = id;
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
        }
        else { // no parser defined so bypass
            ruleInfo[RULE_PID] = TYPE_BYPASS;
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
     * picks up a message from input queue and evaluate its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage;
        String key, value, msgStr = null, ruleName = null, fieldName = null;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, wt;
        Object o = null;
        Iterator iter;
        long count = 0;
        int mask, ii, sz;
        int i = 0, ic = 0, n, previousRid;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
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
                if (ckBody)
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
                if (ruleInfo[RULE_PID] != TYPE_BYPASS)
                    fieldName = (String) rule.get("FieldName");
                previousRid = rid;
            }

            if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                filter = null;
            }
            else if (TYPE_BYPASS == (int) ruleInfo[RULE_PID]) { // bypass
                i = (int) ruleInfo[RULE_OID];
                if (i != RESULT_OUT) // disable post format
                    filter = null;
                else if (filter != null && filter.hasFormatter()) try {
                    //for post format
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
                    " failed to reset the msg: "+ Event.traceStack(e)).send();
                }
                oid = outLinkMap[i];
            }
            else try {
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

                // retrieve parser ID from PID 
                int k = (int) ruleInfo[RULE_PID];
                if (fieldName != null)
                    msgStr = MessageUtils.getProperty(fieldName, inMessage);
                else if (inMessage instanceof MapMessage) {
                    key = (String) rule.get("MapKey");
                    if (key != null && key.length() > 0)
                        msgStr = ((MapMessage) inMessage).getString(key);
                    else
                        msgStr = "";
                }
                else if (msgStr == null)
                    msgStr = MessageUtils.processBody(inMessage, buffer);

                java.lang.reflect.Method method;
                Object obj;
                asset = (Object[]) pluginList.get(k);
                obj = asset[1];
                method = (java.lang.reflect.Method) asset[0];
                o = method.invoke(obj, new Object[] {msgStr});

                String text = null;
                if (o == null) { // parser failed
                    i = FAILURE_OUT;
                }
                else if (o instanceof Event) { // for event
                    Event event = (Event) o;
                    i = RESULT_OUT;
                    inMessage.setJMSTimestamp(event.getTimestamp());
                    inMessage.setJMSPriority(9-event.getPriority());
                    event.removeAttribute("priority");
                    text = event.removeAttribute("text");
                    iter = event.getAttributeNames();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        value = event.getAttribute(key);
                        if (value == null)
                            continue;
                        ic = MessageUtils.setProperty(key, value, inMessage);
                        if (ic != 0) {
                            new Event(Event.WARNING,
                                "failed to set property of: " + key +
                                " for " + name).send();
                            i = FAILURE_OUT;
                        }
                    }
                    event.clearAttributes();
                }
                else if (o instanceof Map) {
                    Map h = (Map) o;
                    i = RESULT_OUT;
                    text = (String) h.remove("body");
                    iter = h.keySet().iterator();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        o = h.get(key);
                        if (o == null || !(o instanceof String))
                            continue;
                        value = (String) o;
                        ic = MessageUtils.setProperty(key, value, inMessage);
                        if (ic != 0) {
                            new Event(Event.ERR, "failed to set property of: "+
                                key + " for " + name).send();
                            i = FAILURE_OUT;
                        }
                    }
                    h.clear();
                    key = null;
                    if (ruleInfo[RULE_EXTRA] > 0) try { // reset timestamp
                        long st;
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
                }
                else {
                    i = FAILURE_OUT;
                }

                if (i != RESULT_OUT) // disable post format
                    filter = null;
                else if (text != null || ruleInfo[RULE_MODE] == 0) {//clear body
                    inMessage.clearBody();
                    if (text != null && text.length() > 0) { // reset body
                        if (inMessage instanceof TextMessage)
                            ((TextMessage) inMessage).setText(text);
                        else if (inMessage instanceof BytesMessage)
                          ((BytesMessage)inMessage).writeBytes(text.getBytes());
                    }
                }
                oid = outLinkMap[i];
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
                filter = null;
                new Event(Event.ERR, str + " failed to parse the msg: "+
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                String str = name + ": " + ruleName;
                new Event(Event.ERR, str +" failed to parse the msg: "+
                    e.toString()).send();
                if ((in.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) try {
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
                    " rid=" + rid + " oid=" + oid + " i=" + i).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                int dmask = (int) ruleInfo[RULE_DMASK];
                if (i == FAILURE_OUT)
                    dmask |= MessageUtils.SHOW_BODY;
                new Event(Event.INFO, name + ": " + ruleName + " parsed msg " +
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    dmask, propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            if (oid == RESULT_OUT &&
                filter != null && filter.hasFormatter()) try { // post format
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
        java.lang.reflect.Method closer;
        Browser browser;
        Object[] asset;
        int id;

        super.close();
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
}
