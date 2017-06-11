package org.qbroker.node;

/* CollectNode.java - a MessageNode collecting responses via of JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.QList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.node.Node;
import org.qbroker.event.Event;

/**
 * CollectNode listens to an input XQ for JMS messages and assigns them
 * to various destinations as the requests according to their content and
 * preconfigured rulesets.  It then collects the responses for each outstanding
 * requests and routes them to their next destinations.  It has two types of
 * outlinks, position-fixed and non-fixed.  There are three position-fixed
 * outlinks: done for all completed requests, failure for the messages failed
 * in the collecting process, nohit for those messages not covered by any
 * rulesets.  The non-fixed outlinks are called collectibles for responses.
 *<br/><br/>
 * CollectNode also contains a number of predefined rulesets.  These
 * rulesets categorize requests into non-overlapping groups.  Therefore, each
 * rule defines a unique request group.  The ruleset also specifies the
 * the initial outlink for the group.  It may also contains a task list that
 * defines the association between each task and its next outlink.  Therefore
 * a ruleset is just like a workflow.  For those messages falling off all
 * defined rulesets, CollectNode always creates an extra ruleset, nohit,
 * to handle them.  All the nohit requests will be routed to nohit outlink.
 *<br/><br/>
 * For each incoming request, CollectNode sends it to its associated worker
 * via the initial outlink.  Then CollectNode frequently checks the response for
 * each outstanding requests.  The response is supposed to have the return code
 * and the requested content.  Once the response is collected, CollectNode
 * proceeds the next task of the proconfigured task list, based on the return
 * code of the response. A task list is a sequence of tasks just like a
 * workflow.  Each task contains three outlinks for three different scenarios:
 * success, failure and exception, respectively.  It evalues the return code of
 * the previous task and determines which outlink to route the message. In case
 * of success, the message will be routed to the outlink of success before
 * executing the current task.  In case of failure or exception, the routing
 * behavior depends on the outlink for the situation.  If the outlink is one of
 * the fixed outlinks, the message will be routed to that outlink. As the
 * result, the current task aborts and the workflow terminates. If the outlink
 * is a collectible and it is same as the one for success, CollectNode will
 * just ignore any failure or exception.  As the result, it proceeds the
 * current task as if the previous task is a success. Otherwise, CollectNode
 * branches the workflow and jumps to the first leading task of the task list.
 * A leading task of a branch is the task immediately behind any exit task.
 * An exit task is the task with non-collectible outlink on success.
 * In this case, CollectNode will find the first leading task with the same
 * outlink on success for the branch, skips all tasks in between.  Therefore
 * a request message will be able to go through a list of outlinks step by step
 * as configured.
 *<br/><br/>
 * As an option, a task can also specify a single data field of the message to
 * be formattable.  In order to do that, you need to define a single Template
 * and/or a single TextSubstitution for that field.  If the response of the
 * previous task is a success, the message will be formatted before it is put
 * to next outlink for success.  If the response is a failure or an exception,
 * the formatter rule will be bypassed. However, a task may define an ErrorCode
 * to overwrite the return code of its response on a failure or an exception
 * scenario. In this case, CollectNode will store the defined ErrorCode into
 * the ReturnCode property of the message before putting it into the outlink
 * for either failure or exception.  Since the ruleset itself is the default
 * task, it can also have the data field, the template and the substitution
 * defined. If all tasks have completed without errors, the ReturnCode property
 * of the message will be reset to null before its final exit.
 *<br/><br/>
 * In an MT env, it may get deadlocked due to circular dependencies among
 * the task outlinks.  CollectNode implements walk-sweep method to collect
 * messages with circular dependencies.  But it may take long time for an
 * outlink to have vacancies.  If a new message can not find vacancy in
 * its next task queue,  CollectNode will keep retry up to 10 times and then
 * roll the message back to the input XQueue. The message will be dequeued
 * again and the same format rule will be applied once more before to put it
 * to its task queue.  Therefore, please make sure the first format rule of
 * a ruleset is stateless.  If you have a stateful format rule as the default
 * task, please create a stateless task and add the new task before it.
 *<br/><br/>
 * You are free to choose any names for the three fixed outlinks.  But
 * CollectNode always assumes the first outlink for done, the second for
 * failure and the third for nohit.  The rest of the outlinks are for workers.
 * It is OK for those three fixed outlinks to share the same name. Please make
 * sure the first fixed outlink has the actual capacity no less than that of
 * the uplink.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class CollectNode extends Node {
    private String tidField = null;
    private String rcField;
    private int[] outLinkMap;
    private AssetList cellList = null; // used for tracking circled collectibles
    private Map<String, Object> templateMap, substitutionMap;

    private final static int SUCCESS = 0;
    private final static int FAILURE = 1;
    private final static int EXCEPTION = 2;
    private final static int DMASK = 3;
    private final static int RESULT_OUT = 0;
    private final static int FAILURE_OUT = 1;
    private final static int NOHIT_OUT = 2;
    private int BOUNDARY = NOHIT_OUT + 1;

    public CollectNode(Map props) {
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
            operation = "collect";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";
        if ((o = props.get("TidField")) != null && o instanceof String)
            tidField = (String) o;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n,
            overlap, name, list);
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

        BOUNDARY = outLinkMap[NOHIT_OUT];
        BOUNDARY = (BOUNDARY >= outLinkMap[FAILURE_OUT]) ? BOUNDARY :
            outLinkMap[FAILURE_OUT];
        BOUNDARY ++;
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        templateMap = new HashMap<String, Object>();
        substitutionMap = new HashMap<String, Object>();

        msgList = new AssetList(name, capacity);
        cellList = new AssetList("cell", capacity);
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
            String[] dataField;
            Template[] template;
            Map ph;
            int[][] taskInfo;
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 +" "+
                    ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_DMASK] + " - "+
                    assetList.getKey((int) ruleInfo[RULE_OID]));
                ph = (Map) ruleList.get(i);
                if (ph == null)
                    continue;
                taskInfo = (int[][]) ph.get("TaskInfo");
                if (taskInfo != null) {
                    for (j=0; j<taskInfo.length; j++)
                        strBuf.append(" " + taskInfo[j][0] + "/" +
                            taskInfo[j][1] + "/" + taskInfo[j][2]);
                }
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID TTL OPTION MASK - OutName" +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
            browser.reset();
            while ((i = browser.next()) >= 0) {
                ph = (Map) ruleList.get(i);
                if (ph == null)
                    continue;
                dataField = (String[]) ph.get("FieldName");
                template = (Template[]) ph.get("Template");
                if (dataField == null || (dataField.length == 1 &&
                    dataField[0] == null))
                    continue;
                strBuf.append("\n\t");
                strBuf.append(i + "/" + dataField.length + " ");
                for (j=0; j<dataField.length; j++) {
                    if (dataField[j] != null) {
                        strBuf.append(dataField[j] + ": ");
                        if (template[j] != null)
                            strBuf.append(template[j].copyText() + " | ");
                        else
                            strBuf.append(" | ");
                    }
                    else
                        strBuf.append(": | ");
                }
            }
            if (strBuf.length() > 0)
                new Event(Event.DEBUG, name + " RID/Len: FieldName Template |" +
                    strBuf.toString()).send();
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
        int[][] taskInfo;
        String[] dataField, errorCode;
        Template[] template;
        TextSubstitution[] substitution;
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
        if(preferredOutName ==null || !assetList.containsKey(preferredOutName)){
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

        // store Timeout into RULE_TTL field
        if ((o = ph.get("Timeout")) != null)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_TTL] = 0;

        if ((o = ph.get("ResetOption")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = RESET_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
        ruleInfo[RULE_PID] = TYPE_COLLECT;

        k = 0;
        if ((o = ph.get("NextTask")) != null && o instanceof List){
            list = (List) o;
            k = list.size();
            taskInfo = new int[k][];
            dataField = new String[k+1];
            errorCode = new String[k+1];
            template = new Template[k+1];
            substitution = new TextSubstitution[k+1];
            rule.put("TaskInfo", taskInfo);
        }
        else {
            list = new ArrayList();
            taskInfo = null;
            dataField = new String[1];
            errorCode = new String[1];
            template = new Template[1];
            substitution = new TextSubstitution[1];
        }
        rule.put("FieldName", dataField);
        rule.put("ErrorCode", errorCode);
        rule.put("Template", template);
        rule.put("Substitution", substitution);

        for (i=0; i<k; i++) { // mapping rule for the tasks
            taskInfo[i] = new int[]{outLinkMap[RESULT_OUT],
                outLinkMap[FAILURE_OUT], outLinkMap[NOHIT_OUT], 0};
            if ((o = list.get(i)) == null || !(o instanceof Map))
                continue;
            str = (String) ((Map) o).get("Success");
            if (str != null) {
                if (assetList.containsKey(str))
                    taskInfo[i][SUCCESS] = assetList.getID(str);
                else
                    new Event(Event.ERR, name + ": Success outLink of task " +
                        i + " in " + ruleName + " is not defined").send();
            }
            str = (String) ((Map) o).get("Failure");
            if (str != null) {
                if (assetList.containsKey(str))
                    taskInfo[i][FAILURE] = assetList.getID(str);
                else
                    new Event(Event.ERR, name + ": Failure outLink of task " +
                        i + " in " + ruleName + " is not defined").send();
            }
            str = (String) ((Map) o).get("Exception");
            if (str != null) {
                if (assetList.containsKey(str))
                    taskInfo[i][EXCEPTION] = assetList.getID(str);
                else
                    new Event(Event.ERR, name + ": Exception outLink of task " +
                        i + " in " + ruleName + " is not defined").send();
            }
            str = (String) ((Map) o).get("DisplayMask");
            if (str != null && str.length() > 0) try {
                taskInfo[i][DMASK] = Integer.parseInt(str);
            }
            catch (Exception e) {
            }
            dataField[i+1] = null;
            errorCode[i+1] = null;
            template[i+1] = null;
            substitution[i+1] = null;
            str = (String) ((Map) o).get("ErrorCode");
            if (str != null && str.length() > 0)
                errorCode[i+1] = new String(str);
            str = (String) ((Map) o).get("FieldName");
            if (str != null && str.length() > 0) {
                dataField[i+1] = MessageUtils.getPropertyID(str);
                if (dataField[i+1] == null)
                    dataField[i+1] = str;
                str = (String) ((Map) o).get("Template");
                if (str == null || str.length() <= 0)
                    str = "##" + dataField[i+1] + "##";

                if (templateMap.containsKey(str))
                    template[i+1] = (Template) templateMap.get(str);
                else {
                    template[i+1] = new Template(str);
                    templateMap.put(str, template[i+1]);
                }
                str = (String) ((Map) o).get("Substitution");
                if (str == null || str.length() <= 0)
                    continue;
                if (substitutionMap.containsKey(str))
                    substitution[i+1] =
                        (TextSubstitution) substitutionMap.get(str);
                else {
                    substitution[i+1] = new TextSubstitution(str);
                    substitutionMap.put(str, substitution[i+1]);
                }
            }
        }

        // for the default task
        str = (String) ph.get("ErrorCode");
        if (str != null && str.length() > 0)
            errorCode[0] = new String(str);
        else
            errorCode[0] = null;
        if ((o = ph.get("FieldName")) != null && o instanceof String) {
            dataField[0] = MessageUtils.getPropertyID((String) o);
            if (dataField[0] == null)
                dataField[0] = (String) o;
            if ((o = ph.get("Template")) != null && o instanceof String) {
                str = (String) o;
            }
            else
                str = "##" + dataField[0] + "##";
            if (templateMap.containsKey(str))
                template[0] = (Template) templateMap.get(str);
            else {
                template[0] = new Template(str);
                templateMap.put(str, template[0]);
            }
            if ((o = ph.get("Substitution")) != null &&
                o instanceof String) {
                str = (String) o;
                if (substitutionMap.containsKey(str))
                    substitution[0] =
                        (TextSubstitution) substitutionMap.get(str);
                else {
                    substitution[0] = new TextSubstitution(str);
                    substitutionMap.put(str, substitution[0]);
                }
            }
        }
        else {
            dataField[0] = null;
            errorCode[0] = null;
            template[0] = null;
            substitution[0] = null;
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
     * formats the dataField from the content of the message according to
     * the predefined template and the substitution.
     */
    private String format(int rid, int tid, String[] dataField,
        Template[] template, TextSubstitution[] substitution,
        byte[] buffer, Message inMessage) {
        int n;
        String text = null;
        if (dataField == null || template == null || substitution == null)
            return null;
        n = template.length;
        if (n <= 0 || tid < 0 || tid >= n)
            return null;

        if (template[tid] != null) try { // format
            text = MessageUtils.format(inMessage, buffer, template[tid]);
        }
        catch (JMSException e) {
            String str = name + ": " + ruleList.getKey(rid) + " at " + tid;
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += " Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + " failed to format " + text +
                ": " + Event.traceStack(e)).send();
            return null;
        }
        catch (Exception e) {
            String str = name + ": " + ruleList.getKey(rid) + " at " + tid;
            new Event(Event.ERR, str + " failed to format " + text +
                ": " + Event.traceStack(e)).send();
            return null;
        }
        if (text == null)
            text = "";

        if (substitution[tid] != null) try { // substitute
            text = substitution[tid].substitute(text);
        }
        catch (Exception e) {
            String str = name + ": " + ruleList.getKey(rid) + " at " + tid;
            new Event(Event.ERR, str + " failed to substitute " + text +
                ": " + Event.traceStack(e)).send();
            return null;
        }

        try {
            if ("body".equals(dataField[tid])) {
                inMessage.clearBody();
                if (inMessage instanceof TextMessage)
                    ((TextMessage) inMessage).setText(text);
                else
                    ((BytesMessage) inMessage).writeBytes(text.getBytes());
            }
            else
                n = MessageUtils.setProperty(dataField[tid], text, inMessage);
        }
        catch (JMSException e) {
            String str = name + ": " + ruleList.getKey(rid) + " at " + tid;
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += " Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + " failed to set property " +
                dataField[tid] + ": " + Event.traceStack(e)).send();
            return null;
        }
        catch (Exception e) {
            String str = name + ": " + ruleList.getKey(rid) + " at " + tid;
            new Event(Event.ERR, str + " failed to set property " +
                dataField[tid] + ": " + Event.traceStack(e)).send();
            return null;
        }

        return text;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, key, ruleName = null;
        Object o;
        Object[] asset;
        Map rule;
        Browser browser;
        MessageFilter[] filters = null;
        Template[] template = null;
        TextSubstitution[] substitution = null;
        String[] propertyName = null, dataField = null;
        long[] outInfo = null, ruleInfo = null;
        int[] ruleMap;
        long currentTime, st, wt;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, size, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
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
            cellList.clear();
            msgList = new AssetList(name, capacity);
            cellList = new AssetList("cell", capacity);
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
        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;

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
            currentTime = System.currentTimeMillis();

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
                propertyName = (String[]) rule.get("PropertyName");
                previousRid = rid;
                if (i >= 0)
                    oid = (int) ruleInfo[RULE_OID];
                else // failed to apply filters
                    oid = outLinkMap[FAILURE_OUT];
                outInfo = assetList.getMetaData(oid);
                if (oid >= BOUNDARY) {
                    dataField = (String[]) rule.get("FieldName");
                    template = (Template[]) rule.get("Template");
                    substitution = (TextSubstitution[])rule.get("Substitution");
                }
            }
            else if (i < 0) { // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
                outInfo = assetList.getMetaData(oid);
            }
            else { // in case oid pointing to FAILURE
                oid = (int) ruleInfo[RULE_OID];
                outInfo = assetList.getMetaData(oid);
            }

            if (oid >= BOUNDARY && dataField[0] != null) { // format
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
                key = format(rid, 0, dataField, template, substitution,
                    buffer, inMessage);
            }
            else
                key = null;

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + oid +
                    ((key != null) ? ": " + key : "")).send();

            if (oid < BOUNDARY && ruleInfo[RULE_DMASK] > 0) try { //display msg
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name +": "+ ruleName + " collected msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
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
     * It collects the message at mid and moves it to its next output XQueue
     * so that a new message can be added to the vacant cell.  It returns
     * 1 if the cell is vacant or 0 if the cell is reused or -1 if there is no
     * vacancies in the new output XQueue.
     *<br/><br/>
     * If it is not negative, the argument of option specifies the id of
     * a known empty cell in the new output XQueue.  If msg is not null,
     * it will be added to the empty cell on the next outlink.  Otherwise,
     * the message will be retrieved from the cell on the current outlink.
     * If option is -1, it means the current cell is not reserved. Otherwise,
     * the current cell is reserved already.
     */
    private int collect(XQueue in, int mid, int option, Message msg) {
        long[] state, outInfo, ruleInfo;
        Object[] asset = null;
        XQueue out;
        Map rule;
        String[] propertyName = null;
        Template[] template;
        TextSubstitution[] substitution;
        int[][] taskInfo;
        String[] dataField, errorCode;
        String str = null, ruleName, key;
        long t;
        int id = -1, i = 0, j, k, m, oid, pid, rid, tid, nid, rc;
        int len, shift, outCapacity;
        byte[] buffer = new byte[bufferSize];
        if (mid < 0 || mid >= capacity)
            return -1;
        state = (long[]) msgList.getMetaData(mid);
        if (state == null || (pid = (int) state[MSG_OID]) < BOUNDARY)
            return -1;
        id = (int) state[MSG_BID];
        rid = (int) state[MSG_RID];
        tid = (int) state[MSG_TID];
        oid = (int) state[MSG_CID]; // oid for next task

        if (msg == null) {
            asset = (Object[]) assetList.get(pid);
            out = (XQueue) asset[ASSET_XQ];
            msg = (Message) in.browse(mid);
        }
        if (msg == null)
            rc = -1;
        else try {
            str = MessageUtils.getProperty(rcField, msg);
            if (str != null)
                rc = Integer.parseInt(str);
            else
                rc = -1;
        }
        catch (Exception e) {
            rc = -1;
        }

        ruleName = ruleList.getKey(rid);
        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        taskInfo = (int[][]) rule.get("TaskInfo");
        dataField = (String[]) rule.get("FieldName");
        errorCode = (String[]) rule.get("ErrorCode");
        template = (Template[]) rule.get("Template");
        substitution = (TextSubstitution[]) rule.get("Substitution");
        if (oid < 0) { // oid needs to be determined via taskInfo 
            if (taskInfo != null && tid >= 0 && taskInfo.length > tid) {
                if (rc == 0)
                    oid = taskInfo[tid][SUCCESS];
                else if (rc > 0)
                    oid = taskInfo[tid][FAILURE];
                else
                    oid = taskInfo[tid][EXCEPTION];

                if ((debug & DEBUG_UPDT) > 0) { // try to display msg
                    i = taskInfo[tid][DMASK];
                    if (i > 0 && msg != null) try { // display the message
                        str = ((i & MessageUtils.SHOW_BODY) == 0) ? null :
                            MessageUtils.processBody(msg, buffer);
                        new Event(Event.DEBUG, name + ": " + ruleName +
                            " collected a msg at task " + tid + ":" +
                            MessageUtils.display(msg, str, i, null)).send();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " failed to display collected msg at task "+ tid +
                            ": " + e.toString()).send();
                    }
                }
            }
            else {
                if (rc == 0)
                    oid = outLinkMap[RESULT_OUT];
                else
                    oid = outLinkMap[FAILURE_OUT];
            }
        }

        nid = tid;
        if (rc != 0 && oid >= BOUNDARY) { // for branch support
            if (oid != taskInfo[tid][SUCCESS]) { // look for leading task
                for (i=tid; i<taskInfo.length; i++) {
                    if (taskInfo[i][SUCCESS] < BOUNDARY) { // exit point
                        if (i+1 < taskInfo.length &&
                            oid == taskInfo[i+1][SUCCESS]) // found it
                            break;
                    }
                }
                if (i < taskInfo.length) { // found leading task of branch
                    nid = i+1;
                    if ((debug & DEBUG_COLL) > 0)
                        new Event(Event.INFO, name+": "+ruleName+ " branching "+
                            mid + " " + tid + " " + rc + " " + oid + ":" + pid +
                            ":" + nid + " " + msgList.size()).send();
                    rc = 0;
                }
                else { // no such branch is configured, so switch to NOHIT
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to find leading task for "+
                        assetList.getKey(oid) + " at " + tid + "/" + rc).send();
                    oid = outLinkMap[NOHIT_OUT];
                }
            }
            else // ignore failures or exceptions
                rc = 0;
        }

        if (oid < BOUNDARY)
            m = 100;
        else
            m = 10;

        asset = (Object[]) assetList.get(oid);
        out = (XQueue) asset[ASSET_XQ];
        outInfo = assetList.getMetaData(oid);
        len = (int) outInfo[OUT_LENGTH];
        if (oid == pid) { // back to the same outlink
            if (option == -1) // current cell is not reserved
                id = out.reserve(waitTime, id);
        }
        else if (option < 0) switch (len) {
          case 0:
            shift = 0;
            for (i=0; i<m; i++) { // reserve any empty cell
                id = out.reserve(waitTime);
                if (id >= 0)
                    break;
            }
            break;
          case 1:
            shift = (int) outInfo[OUT_OFFSET];
            for (i=0; i<m; i++) { // reserve the empty cell
                id = out.reserve(waitTime, shift);
                if (id >= 0)
                    break;
            }
            break;
          default:
            shift = (int) outInfo[OUT_OFFSET];
            for (i=0; i<m; i++) { // reserve an partitioned empty cell
                id = out.reserve(waitTime, shift, len);
                if (id >= 0)
                    break;
            }
            break;
        }
        else {
            shift = option;
            id = out.reserve(waitTime, shift);
        }
        outCapacity = (int) outInfo[OUT_CAPACITY];

        if (id < 0) // reservation on oid failed due to no empty cell found
            return -1;
        else if (oid == pid) { // backfill its own cell on the same outlink
            t = System.currentTimeMillis();
            if (rc == 0 && dataField[nid+1] != null) // for success
                str = format(rid, nid+1, dataField, template, substitution,
                    buffer, msg);
            if (tidField != null) try {
                MessageUtils.setProperty(tidField, rid + ":" + (nid+1), msg);
            }
            catch (Exception e) {
            }
            i = out.add(msg, id, cbw);
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;

            outInfo[OUT_COUNT] ++;
            state[MSG_CID] = -1;
            state[MSG_TID] = nid+1;
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + ": " + ruleName + " collecting " +
                    mid + " " + nid + " " + rc + " " + oid + "/" + id + "/" +
                    id + " " + mid + " " + msgList.size()).send();
            // the current cell is reused
            return 0;
        }
        else if (id < outCapacity) { // id-th cell of oid reserved
            k = -1;
            t = System.currentTimeMillis();
            key = oid + "/" + id;
            m = msgList.getID(key);
            if (m >= 0 && oid >= BOUNDARY) { // next task cell not empty yet
                out.cancel(id);
                // store oid into MSG_CID for possible circular collection
                state[MSG_CID] = oid;
                return -1;
            }
            else if (m >= 0) { // passback the empty non-task cell
                cells.collect(-1L, m);
                k = (int) msgList.getMetaData(m)[MSG_RID];
                msgList.remove(m);
                in.remove(m);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                ruleInfo = ruleList.getMetaData(k);
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = t;
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name + " passback: " + k + " " +
                        m + "/" + m +" "+ oid + ":" + id + " " +
                        ruleInfo[RULE_SIZE] + " " + outInfo[OUT_SIZE]+
                        " " + out.size() + "|" + out.depth() + " " +
                        msgList.size()).send();
            }

            if (oid >= BOUNDARY && rc == 0 && dataField[nid+1] != null)
                str = format(rid, nid+1, dataField, template, substitution,
                    buffer, msg);
            else if (oid < BOUNDARY) { // taskflow exit
                String rCode = String.valueOf(rc);
                try {
                    if (oid == outLinkMap[RESULT_OUT]) { // cleanup RC field
                        if (msg.propertyExists(rcField))
                            MessageUtils.setProperty(rcField, null, msg);
                    }
                    else if (errorCode[nid] != null) { // reset RC field
                        MessageUtils.setProperty(rcField, errorCode[nid], msg);
                        rCode = errorCode[nid];
                    }
                }
                catch (Exception e) {
                }

                ruleInfo = ruleList.getMetaData(rid);
                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    str = null;
                    if ((ruleInfo[RULE_DMASK] & (MessageUtils.SHOW_BODY |
                        MessageUtils.SHOW_SIZE)) > 0)
                        str = MessageUtils.processBody(msg, buffer);
                    new Event(Event.INFO, name + ": " + ruleName +
                        " collected a msg at task " + nid + " with " + rCode +
                        ":" + MessageUtils.display(msg, str,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: " + e.toString()).send();
                }
            }
            if (oid >= BOUNDARY && tidField != null) try {
                MessageUtils.setProperty(tidField, rid + ":" + (nid+1), msg);
            }
            catch (Exception e) {
            }
            // update the key for reverse lookup
            msgList.remove(mid);
            j = msgList.add(key, state, key, mid);
            i = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;

            // update the state of the responder XQ
            i = (int) state[MSG_BID];
            outInfo = assetList.getMetaData(pid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            state[MSG_CID] = -1;
            state[MSG_BID] = id;
            state[MSG_OID] = oid;
            state[MSG_TID] = nid+1;
            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + ": " + ruleName + " collecting " +
                    mid + " " + nid + " " + rc + " " + oid + "/" + id + " " +
                    k + " " + m + " " + pid + ":" + i + " " + j + " " +
                    msgList.size()).send();
            return 1;
        }

        return -1;
    }

    /**
     * It collects a group of messages depending on each others in a circle
     * and returns the number of messages successfully collected.
     *<br/><br/>
     * It is assumed that the list contains message ID for all collecables
     * in its metadata.  The current oid and the next oid of a collecable
     * can be retrieved from msgList via its message ID.
     */
    private int collect(XQueue in, AssetList list) {
        QList group;
        Browser browser;
        StringBuffer strBuf = null;
        long[] state, state0 = null;
        int[] oids, nids;
        int id = -1, i, j, k, m, n, mid, oid, nid, cid;
        boolean inDetail = ((debug & DEBUG_REPT) > 0);
        if (list == null || (n = list.size()) <= 1)
            return 0;
        browser = list.browser();
        k = assetList.size();
        // group is for dependency counts of outLinks
        group = new QList("group", k);
        nids = new int[n];
        n = list.queryIDs(nids);
        for (i=0; i<n; i++) { // initialize group
            mid = nids[i];
            if (!msgList.existsID(mid))
                continue;
            state = msgList.getMetaData(mid);
            // retrieve the current oid for the collectible
            oid = (int) state[MSG_OID];
            // retrieve the next oid stored in MSG_CID
            nid = (int) state[MSG_CID];
            if (oid < 0 || nid < 0 || oid == nid) {
                list.remove(mid);
                continue;
            }
            // count for collectibles
            if (group.getType(oid) == XQueue.CELL_EMPTY) {
                group.reserve(oid);
                // init the array of count for collectibles and dependents
                group.add(new int[] {1, 0}, oid);
            }
            else { // just increase the count of collectibles
                oids = (int[]) group.browse(oid);
                if (oids[0] == 0) // upgrade to TAKEN
                    group.getNextID(oid);
                oids[0] ++;
            }
            // count for dependents
            if (group.getType(nid) == XQueue.CELL_EMPTY) {
                group.reserve(nid);
                // init the array of count for collectibles and dependents
                group.add(new int[] {0, 1}, nid);
            }
            else { // just increase the count of dependencies
                oids = (int[]) group.browse(nid);
                if (oids[1] == 0) // upgrade to TAKEN
                    group.getNextID(nid);
                oids[1] ++;
            }
        }

        while ((oid = group.getNextID()) >= 0) { // clean up OCCUPIED oids
            oids = (int[]) group.browse(oid);
            browser.reset();
            if (oids[0] == 0) { // the outlink has no collectibles
                while ((mid = browser.next()) >= 0) { // check dependents
                    state = msgList.getMetaData(mid);
                    if (oid == (int) state[MSG_CID]) { // found a dependent
                        list.remove(mid);
                        oids[1] --;
                        nid = (int) state[MSG_OID];
                        nids = (int[]) group.browse(nid);
                        nids[0] --;
                        if (nids[0] == 0 && oid != nid) { // update nid status
                            if (nids[1] <= 0)
                                group.remove(nid);
                            else if (group.getType(nid) == XQueue.CELL_TAKEN)
                                group.putback(nid);
                        }
                    }
                }
            }
            else { // the outlink has no dependents
                while ((mid = browser.next()) >= 0) { // check collectibles
                    state = msgList.getMetaData(mid);
                    if (oid == (int) state[MSG_OID]) { // found a collectible
                        list.remove(mid);
                        oids[0] --;
                        nid = (int) state[MSG_CID];
                        nids = (int[]) group.browse(nid);
                        nids[1] --;
                        if (nids[1] == 0 && oid != nid) { // update nid status
                            if (nids[0] <= 0)
                                group.remove(nid);
                            else if (group.getType(nid) == XQueue.CELL_TAKEN)
                                group.putback(nid);
                        }
                    }
                }
            }
            group.remove(oid);
        }

        m = 0;
        n = list.size();
        if (n <= 0) // nothing to collect
            return 0;

        if (inDetail) {
            strBuf = new StringBuffer();
            browser.reset();
            while ((i = browser.next()) >= 0) {
                state = msgList.getMetaData(i);
                strBuf.append(" " + i + ":" + state[MSG_OID] +"/"+
                    state[MSG_CID]);
            }
            if (strBuf.length() > 0)
                new Event(Event.DEBUG, name + " COLL0: found " + n +
                    " collectibles inter-linked\n\t" +strBuf.toString()).send();
        }
        // clear group to store inter-chained outLinks
        group.clear();
        nids = new int[n];
        k = 0;
        // start traceing at the first available one
        browser.reset();
        mid = browser.next();
        if (mid >= 0) {
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            // retrieve the next oid from MSG_CID
            nid = (int) state[MSG_CID];
            // add the first oid to group so that oid is OCCUPIED
            group.reserve(oid);
            group.add(new int[] {mid}, oid);
            list.remove(mid);
            k ++;
            if (inDetail)
                strBuf = new StringBuffer(" " + mid + "/" + oid + ":" +
                   state[MSG_BID] + "/" + nid);
        }
        else {
            nid = -1;
        }
        while (mid >= 0) { // trace circles and collect them in turns
            while (group.reserve(nid) >= 0) {//keep tracing if next oid is empty
                browser.reset();
                while ((cid = browser.next()) >= 0) { // scan the list of cid
                    state = msgList.getMetaData(cid);
                    if (nid == (int) state[MSG_OID]) { // found the next one
                        group.add(new int[] {cid}, nid);
                        list.remove(cid);
                        k ++;
                        if (inDetail)
                            strBuf.append(" " + cid + "/" + nid + ":" +
                                state[MSG_BID] + "/" + state[MSG_CID]);
                        nid = (int) state[MSG_CID];
                        break;
                    }
                }
                if (cid < 0) { // no circle found
                    group.cancel(nid);
                    break;
                }
                if (nid == oid) // circle closed
                    break;
            }

            if (inDetail && strBuf.length() > 0)
                new Event(Event.DEBUG, name + " COLL1: traced " + k +
                    " collectbales and ended at " + nid + "," +
                    group.getType(nid) + " " + list.size() + "\n\t" +
                    strBuf.toString()).send();

            if (group.getType(nid) == XQueue.CELL_OCCUPIED) { // circle closed
                Message msg = null;
                n = group.queryIDs(nids, XQueue.CELL_OCCUPIED);
                if (inDetail)
                    strBuf = new StringBuffer("collecting:");
                oid = nid;
                // cache the first collectible of the circle
                mid = ((int[]) group.takeback(oid))[0];
                state = msgList.getMetaData(mid);
                id = (int) state[MSG_BID];
                if (n > 1) {
                    Object[] asset = (Object[]) assetList.get(oid);
                    XQueue out = (XQueue) asset[ASSET_XQ];
                    // cache the msg
                    msg = (Message) in.browse(mid);
                    state0 = assetList.getMetaData(oid);
                    state0[OUT_SIZE] --;
                    state0[OUT_COUNT] ++;
                    // remove the mid to empty the cell at oid/id
                    msgList.remove(mid);
                    // cache the metadata
                    state0 = state;
                }
                for (i=n-1; i>0; i--) { // backward from the end
                    nid = nids[i];
                    if (nid == oid) // beginning of circle
                        break;
                    cid = ((int[]) group.takeback(nid))[0];
                    state = msgList.getMetaData(cid);
                    // save the original id
                    j = (int) state[MSG_BID];
                    cells.collect(-1L, cid);
                    if (collect(in, cid, id, null) < 0) // rollback
                        cells.take(cid);
                    id = j;
                    if (inDetail)
                        strBuf.append(" " + cid + "/" + nid + ":" + id);
                }
                if (n > 1) { // collect the cached msg of mid
                    String key = nid + "/" + id;
                    // save the original id
                    j = (int) state0[MSG_BID];
                    state0[MSG_OID] = nid;
                    state0[MSG_BID] = id;
                    state0[MSG_CID] = -1;
                    msgList.add(key, state0, key, mid);
                    cells.collect(-1L, mid);
                    if (collect(in, mid, id, msg) < 0) // rollback
                        cells.take(mid);
                    id = j;
                }
                else {
                    cells.collect(-1L, mid);
                    if (collect(in, mid, -1, null) < 0) // rollback
                        cells.take(mid);
                }
                if (inDetail) {
                    strBuf.append(" " + mid + "/" + oid + ":" + id);
                    k = n - group.depth();
                    new Event(Event.DEBUG, name + " COLL2: collected " + k +
                        " msg with " + list.size() + " left\n\t" +
                        strBuf.toString()).send();
                }

                if ((k = group.depth()) > 0) { // group not empty yet
                    m += n - k;
                    oid = nids[k-1];
                    mid = ((int[]) group.browse(oid))[0];
                    state = msgList.getMetaData(mid);
                    nid = (int) state[MSG_CID];
                    k = 0;
                }
                else { // init group since it is empty
                    m += n;
                    browser.reset();
                    mid = browser.next();
                    if (mid >= 0) {
                        state = msgList.getMetaData(mid);
                        oid = (int) state[MSG_OID];
                        id = (int) state[MSG_BID];
                        // retrieve the next oid from MSG_CID
                        nid = (int) state[MSG_CID];
                        // add the first oid to group so that oid is OCCUPIED
                        group.reserve(oid);
                        group.add(new int[] {mid, id}, oid);
                        list.remove(mid);
                        k = 1;
                    }
                }
            }
            else { // no circle found
                if (inDetail && group.depth() > 0) {
                    strBuf = new StringBuffer();
                    browser = group.browser();
                    while ((i = browser.next()) >= 0) {
                        cid = ((int[]) group.browse(i))[0];
                        state = msgList.getMetaData(cid);
                        strBuf.append(" " + cid +":"+ i + "/" + state[MSG_CID]);
                    }
                    new Event(Event.DEBUG, name + " COLL3: discarded " +
                        group.depth() + " non-circular collectibles and with " +
                        list.size() + " left\n\t" + strBuf.toString()).send();
                }
                group.clear();
                browser = list.browser();
                mid = browser.next();
                if (mid >= 0) {
                    state = msgList.getMetaData(mid);
                    oid = (int) state[MSG_OID];
                    id = (int) state[MSG_BID];
                    // retrieve the next oid from MSG_CID
                    nid = (int) state[MSG_CID];
                    // add the first oid to group so that oid is OCCUPIED
                    group.reserve(oid);
                    group.add(new int[] {mid, id}, oid);
                    list.remove(mid);
                    k = 1;
                    if (inDetail)
                        strBuf = new StringBuffer(" "+ mid +":"+ oid +"/"+ nid);
                }
            }
            n = list.size();
            if (n <= 0) { // nothing left
                if (group.depth() > 0 && inDetail)
                    new Event(Event.DEBUG, name + " COLL3: discarded " +
                        group.depth() + " non-circular collectibles and with "+
                        n + " left\n\t" + strBuf.toString()).send();
                break;
            }
        }
        group.clear();
        return m;
    }

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo;
        int id = -1, k, mid, mask, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            k = in.putback(cid);
            new Event(Event.ERR, name + ": asset is null on " +
                assetList.getKey(oid) + " of " + oid + " for " +
                rid + " with msg at " + cid).send();
            try {
                Thread.sleep(500);
            }
            catch (Exception e) {
            }
            return 0;
        }
        out = (XQueue) asset[ASSET_XQ];
        outInfo = assetList.getMetaData(oid);
        len = (int) outInfo[OUT_LENGTH];
        switch (len) {
          case 0:
            shift = 0;
            for (k=0; k<1000; k++) { // reserve an empty cell
                id = out.reserve(-1L);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
          case 1:
            shift = (int) outInfo[OUT_OFFSET];
            for (k=0; k<1000; k++) { // reserve the empty cell
                id = out.reserve(-1L, shift);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
          default:
            shift = (int) outInfo[OUT_OFFSET];
            for (k=0; k<1000; k++) { // reserve an partitioned empty cell
                id = out.reserve(-1L, shift, len);
                if (id >= 0)
                    break;
                mask = in.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0 ||
                    (mask & XQueue.STANDBY) > 0) // disabled or stopped
                    break;
                feedback(in, waitTime);
            }
            break;
        }
        outCapacity = (int) outInfo[OUT_CAPACITY];

        if (id >= 0 && id < outCapacity) { // id-th cell of out reserved
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                // MSG_CID is used to store the oid for next task, set TID to 0
                mid = msgList.add(key, new long[]{-1L, (long) oid, (long) id,
                    (long) rid, 0L, currentTime}, key, cid);
            }
            else if (oid < BOUNDARY) { // id-th cell is just empty, replace it
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                k = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(k);
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name+" passback: " + k + " " + mid +
                        ":" + state[MSG_CID] + " "+key+" "+ruleInfo[RULE_SIZE]+
                        " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                        out.depth() + " " + msgList.size()).send();
                // MSG_CID is used to store the oid for next task, set TID to 0
                mid = msgList.add(key, new long[] {-1L, (long) oid, (long) id,
                    (long) rid, 0L, currentTime}, key, cid);
            }
            else { // it is a response msg to collect
                cells.collect(-1L, mid);
                k = collect(in, mid, -(tid+2), null);
                if (k > 0) { // cell is vacant
                    // MSG_CID is used to store the oid for next task
                    mid = msgList.add(key, new long[]{-1L,(long) oid, (long) id,
                        (long) rid, 0L, currentTime}, key, cid);
                }
                else {
                    if (k < 0) { // failed to collect mid so roll it back
                        // cancel the reservation for collection
                        out.cancel(id);
                        cells.take(mid);
                    }
                    if (tid < 10) { // try to passthru once more
                        k = passthru(currentTime, msg, in, rid, oid, cid,tid+1);
                    }
                    else { // giving up on collect
                        k = in.putback(cid);
                        ruleInfo = ruleList.getMetaData(rid);
                        new Event(Event.WARNING, name + ": XQ is full on " +
                            assetList.getKey(oid) + " of " + oid + " for "+rid+
                            ": " + outInfo[OUT_QDEPTH] +","+ outInfo[OUT_SIZE]+
                            ": " + out.size() + "," + out.depth() + " / " +
                            ruleInfo[RULE_SIZE] + ": " + k + " " + tid).send();
                        k = 0;
                    }
                    return k;
                }
            }
            if (mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = in.putback(cid);
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            k = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = currentTime;
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] ++;
            ruleInfo[RULE_TIME] = currentTime;
            if ((debug & DEBUG_PASS) > 0)
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ":" + cid + " " + key + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
        }
        else { // reservation failed
            k = in.putback(cid);
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                assetList.getKey(oid) + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k + " " + tid).send();
            return 0;
        }
        return 1;
    }

    /**
     * returns the number of done messages removed from in
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        String key;
        int i, k, n, mid, rid, oid, id, l = 0;
        int[] list = new int[129];
        long[] state, outInfo, ruleInfo;
        long t;
        StringBuffer strBuf = null;
        if ((debug & DEBUG_FBAK) > 0)
            strBuf = new StringBuffer();

        k = 0;
        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null)
                continue;
            out = (XQueue) asset[ASSET_XQ];
            id = (int) state[MSG_BID];
            if (oid >= BOUNDARY) { // for collectible messages
                if (collect(in, mid, -1, null) < 0) { // failed to collect
                    list[k++] = mid;
                    if (k >= 128) // list is full
                        break;
                }
                if (milliSec >= 0) // reset waitTime
                    milliSec = 0L;
                continue;
            }
            // for exit messages
            in.remove(mid);
            msgList.remove(mid);
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = t;
            if ((debug & DEBUG_FBAK) > 0)
                strBuf.append("\n\t" + rid + " " + mid + "/" + state[MSG_CID] +
                    " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth() +
                    " " + msgList.size());
            l ++;
            if (milliSec >= 0) // only one collection a time
                break;
        }
        if (l > 0 && (debug & DEBUG_FBAK) > 0)
            new Event(Event.DEBUG, name + " feedback: RID MID?CID OID:ID " +
                "RS OS size|depth ms - " + l + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        if (k > 0) { // process messages failed to be collected
            if (cellList.size() > 0)
                cellList.clear();
            for (i=0; i<k; i++) { // add failed collectibles to cellList
                mid = list[i];
                if (!msgList.existsID(mid))
                    continue;
                cells.take(mid);
                state = msgList.getMetaData(mid);
                key = msgList.getKey(mid);
                if (state[MSG_CID] < 0) // check the next oid stored in MSG_CID
                    continue;
                cellList.add(key, null, key, mid);
            }
            if (cellList.size() > 1) { // try to collect circular collectibles
                k = cellList.size();
                n = collect(in, cellList);
                if (n > 0 && (debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + " collected " + n + "/" + k +
                        " messages with "+cellList.size()+" leftover").send();
            }
            cellList.clear();
        }

        return l;
    }

    /**
     * returns the BOUNDARY which is the lowest id of outlinks for collectibles
     * so that any outlink with either the same id or a higher id is for
     * collectibles. The EXTERNAL_XA bit of all outlinks for collectibles has
     * to be disabled by the container to stop ack propagations downsstream.
     */
    public int getOutLinkBoundary() {
        return BOUNDARY;
    }

    public void close() {
        super.close();
        cellList.clear();
        templateMap.clear();
        substitutionMap.clear();
    }

    protected void finalize() {
        close();
    }
}
