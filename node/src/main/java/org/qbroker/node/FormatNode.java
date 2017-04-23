package org.qbroker.node;

/* FormatNode.java - a MessageNode formatting JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.AssetList;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * FormatNode formats JMS Message body and header fields according to rulesets.
 * Based on the rulesets, it filters messages into three outlinks: done
 * for all the formatted messages, nohit for those messages do not belong
 * to any rulesets, failure for the messages failed in the formatting process.
 *<br/><br/>
 * FormatNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each rule
 * defines a unique message group.  The ruleset also defines the formatter and
 * its formatting rules for the group.  Different groups may have different
 * formatter or different format parameters.  Due to JMS specifications, you
 * have to clear user properties before setting user properties on a
 * readonly message.  You can specify ResetOption in a ruleset so that
 * the message header will be reset for modifications.  Its value is 0 for
 * no reset, 1 for optional reset and 2 for guaranteed reset.
 *<br/><br/>
 * Each ruleset has an array of FormatterArgument. A FormatterArgument contains
 * a name specifying what to be formatted and two sets of format operations.
 * The first operation set lists format templates in the name of Template.
 * The other is the array of substitutions with the name of Substitution.
 * Each Template appends the previous non-empty text to the variable and sets
 * the next initial text for its corresponding Substitutions.  If the first
 * template is null, the initial text will be the variable itself.
 * Each Substitution modifies the text before it is appended to the variable.
 * The associations between the Templates and Substitutions are based on
 * their positions.  Either Template or Substitution can be null for
 * no action and a place holder.  Therefore, you can insert multiple null
 * Templates so that the associated Substitutions will be able to modify
 * the same text in turns.  FormatNode will apply all the operations of
 * FormatterArguments on the incoming message in the order of the list.  If any
 * of the operations fails, the message will be routed to failure outlink.
 *<br/><br/> 
 * If ClassName is not defined in the rule, the rule is for the default
 * formatter. In this case, it accepts a template via URITemplate for the
 * path to a template file which will be used to format the message body.
 * If URITemplate is defined, the rule will load the template from the file
 * and cache it at the first application to the message.  The format result
 * will be set back to the message body. The cache count of templates for the
 * rule will be stored to RULE_PEND. The cache count will be updated when the
 * session times out which is determined by SessionTimeout. A static template
 * file can also be specified via TemplateFile.
 *<br/><br/>
 * FormatNode allows developers to plugin their own formatters by specifying
 * the ClassName for the plugin.  The requirement is minimum.  The class should
 * have a public method of format() that takes a JMS Message to be formatted as
 * the only argument.  The return object must be a String of null meaning OK,
 * or an error message otherwise.  It must have a constructor taking a Map
 * as the only argument for configurations. Based on the map properties for
 * the constructor, developers should define configuration parameters in the
 * base of FormatterArgument.  FormatNode will pass the data to the plugin's
 * constructor as an opaque object during the instantiation of the plugin.
 * In the normal operation, FormatNode will invoke the method to format the
 * incoming messages.  The method should never acknowledge any message in any
 * case.
 *<br/><br/>
 * In case a plugin needs to connect to external resources for the dynamic
 * format process, it should define an extra method of close() to close all
 * the external resources gracefully.  Its format method should also be able
 * to detect the disconnections and cleanly reconnect to the resources
 * automatically.  If the container wants to stop the node, it will call the
 * resetMetaData() in which the methods of close() of all formatters will be
 * called in order to release all external resources.
 *<br/><br/>
 * You are free to choose any names for the three fixed outlinks.  But
 * FormatNode always assumes the first outlink for done, the second for failure
 * and the last for nohit.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FormatNode extends Node {
    private int sessionTimeout = 0;
    private int[] outLinkMap;

    private AssetList pluginList = null; // plugin method and object
    private Map<String, Object> templateMap, substitutionMap;
    private QuickCache cache = null;     // for storing templates

    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public FormatNode(Map props) {
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
            operation = "format";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);

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

        templateMap = new HashMap<String, Object>();
        substitutionMap = new HashMap<String, Object>();
        cache = new QuickCache(name, QuickCache.META_ATAC, 0, 0);

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
            throw(new IllegalArgumentException(name+": failed to init rule: "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_TTL] + " " + ruleInfo[RULE_MODE] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID Reset TTL Chop "+
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
        Map<String, Object> rule, hmap;
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
        if ((o = ph.get("PatternGroup")) != null)
            hmap.put("PatternGroup", o);
        if ((o = ph.get("XPatternGroup")) != null)
            hmap.put("XPatternGroup", o);
        if ((o = ph.get("FormatterArgument")) != null &&
            o instanceof List && !ph.containsKey("ClassName")) {
            hmap.put("FormatterArgument", o);
            if ((o = ph.get("ResetOption")) != null)
                hmap.put("ResetOption", o);
            else
                hmap.put("ResetOption", "0");
            hmap.put("TemplateMap", templateMap);
            hmap.put("SubstitutionMap", substitutionMap);
        }
        rule.put("Filter", new MessageFilter(hmap));
        hmap.clear();
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

        if ((o = ph.get("TimeToLive")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("ClassName")) != null) { // plugin
            str = (String) o;
            if ((o = ph.get("FormatterArgument")) != null && o instanceof Map)
                str += "::" + JSON2Map.toJSON((Map) o, null, null);
            else
                throw(new IllegalArgumentException(ruleName +
                    ": FormatterArgument is not a map for " + str));

            if (pluginList.containsKey(str)) {
                long[] meta;
                id = pluginList.getID(str);
                meta = pluginList.getMetaData(id);
                // increase the reference count
                meta[0] ++;
            }
            else {
                o = MessageUtils.getPlugins(ph, "FormatterArgument", "format",
                    new String[]{"javax.jms.Message"}, "close", name);
                id = pluginList.add(str, new long[]{1}, o);
            }
            // store the plugin ID in PID for the plugin
            ruleInfo[RULE_PID] = id;
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
        }
        else if ((o = ph.get("FormatterArgument")) != null &&
            o instanceof List) { // default formatter already created
            if ((o = ph.get("URITemplate")) != null) {
                str = (String) o;
                rule.put("Template", new Template(str));
                if ((o = ph.get("URISubstitution")) != null)
                    rule.put("Substitution", new TextSubstitution((String) o));
            }
            else if ((o = ph.get("TemplateFile")) != null) {
                str = (String) o;
                rule.put("Template", new Template(str));
                if ((o = ph.get("NeedChop")) != null &&
                    o instanceof String && "true".equals((String) o))
                    ruleInfo[RULE_MODE] = 1;
            }
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_FORMAT;
        }
        else if ((o = ph.get("URITemplate")) != null) {
            str = (String) o;
            rule.put("Template", new Template(str));
            if ((o = ph.get("URISubstitution")) != null)
                rule.put("Substitution", new TextSubstitution((String) o));
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_FORMAT;
        }
        else if ((o = ph.get("TemplateFile")) != null) {
            str = (String) o;
            rule.put("Template", new Template(str));
            if ((o = ph.get("NeedChop")) != null &&
                o instanceof String && "true".equals((String) o))
                ruleInfo[RULE_MODE] = 1;
            ruleInfo[RULE_OID] = outLinkMap[RESULT_OUT];
            ruleInfo[RULE_PID] = TYPE_FORMAT;
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

    private int format(String uri, long currentTime, int rid, int ttl,
        byte[] buffer, Message msg) {
        int i;
        String text;
        Template template = null;

        if (uri == null || uri.length() <= 0 || msg == null)
            return FAILURE_OUT;

        template = (Template) cache.get(uri, currentTime);
        if (template == null) try { // initialize the first one
            long[] info = ruleList.getMetaData(rid);
            if (info != null && info[RULE_MODE] > 0)
                template = new Template(new File(uri), true);
            else
                template = new Template(new File(uri));
            i = cache.insert(uri, currentTime, ttl, new int[]{rid}, template);
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

        try {
            text = MessageUtils.format(msg, buffer, template);
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(text);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(text.getBytes());
            else {
                new Event(Event.ERR, name + ": rule of " + rid +
                    " failed to set body: bad msg family").send();
                return FAILURE_OUT;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name +" failed to format msg for rule " +
                rid + ": " + Event.traceStack(e)).send();
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
        long currentTime, previousTime, wt;
        Object o = null;
        Iterator iter;
        long count = 0;
        int mask, ii, sz;
        int i = 0, n, previousRid;
        int rid = -1; // the rule id
        int cid = -1; // the cell id of the message in input queue
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

            msgStr = null;
            filter = null;
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
                temp = (Template) rule.get("Template");
                tsub = (TextSubstitution) rule.get("TextSubstitution");
                propertyName = (String[]) rule.get("PropertyName");
                previousRid = rid;
            }

            if (i < 0) // failed on filter
                i = FAILURE_OUT;
            else if (TYPE_BYPASS == (int) ruleInfo[RULE_PID]) // for bypass
                i = -1;
            else if (TYPE_FORMAT == (int) ruleInfo[RULE_PID]) {
                try {
                    i = RESULT_OUT;
                    if (filter.hasFormatter()) {
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
                        i = filter.format(inMessage, buffer);
                        if (i <= 0)
                            i = FAILURE_OUT;
                        else
                            i = RESULT_OUT;
                    }

                    if (i == RESULT_OUT && temp != null) { // with template
                        String uri;
                        uri = MessageUtils.format(inMessage, buffer, temp);
                        if (tsub != null)
                            uri = tsub.substitute(uri);
                        if (uri != null && uri.length() > 0)
                            i = format(uri, currentTime, rid,
                                (int) ruleInfo[RULE_TTL], buffer, inMessage);
                        else {
                            i = RESULT_OUT;
                            new Event(Event.ERR, name +
                                ": empty uri for the file on "+ruleName).send();
                        }
                    }
                }
                catch (Exception e) {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the msg: "+
                        Event.traceStack(e)).send();
                }
            }
            else try { // for plug-ins
                // retrieve formatter ID from PID
                int k = (int) ruleInfo[RULE_PID];
                if (k >= 0) { // for plug-ins
                    java.lang.reflect.Method method;
                    Object obj;
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
                    asset = (Object[]) pluginList.get(k);
                    obj = asset[1];
                    method = (java.lang.reflect.Method) asset[0];
                    if (method != null)
                        o = method.invoke(obj, new Object[] {inMessage});
                    else
                        o = "plugin method is null";
                }
                else
                    o = "no plugin defined";

                if (o == null)
                    i = RESULT_OUT;
                else {
                    i = FAILURE_OUT;
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to format the msg: "+ o.toString()).send();
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
                new Event(Event.ERR, str + " failed to format the msg: "+
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                String str = name + ": " + ruleName;
                new Event(Event.ERR, str + " failed to format the msg: "+
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
            if (i < 0) // for bypass
                oid = (int) ruleInfo[RULE_OID];
            else
                oid = outLinkMap[i];

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " i=" + i).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if (!ckBody && ((int) ruleInfo[RULE_DMASK] &
                    (MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE)) > 0)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleName + " formatted msg "+
                    (count+1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
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
     * updates the cache count for each rules and disfragments cache items
     */
    private int updateCacheCount(long currentTime) {
        int k, n, rid;
        long t, tm;
        int[] info, count;
        long[] ruleInfo;
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
        for (String key : cache.keySet()) {
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
        templateMap.clear();
        substitutionMap.clear();
        cache.clear();
    }

    protected void finalize() {
        close();
    }
}
