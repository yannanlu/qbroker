package org.qbroker.node;

/* JobNode.java - a MessageNode manages various jobs */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Comparator;
import java.util.Arrays;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.QList;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.common.CollectibleCells;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * JobNode picks up JMS messages as the job requests and job queries from the
 * uplink and processes them according to their content and the pre-defined
 * rulesets. A job request is to start a time consuming job whereas a job
 * query is just to query the status of a running job. If an incoming message
 * is neither a job request nor a job query, JobNode will treat it as a bypass
 * and routes it to the bypass outlink without any delay. When a job request
 * arrives, JobNode creates a job task out of the message and sets the job id
 * and the status to the message. Then it routes the message to the done
 * outlink at once. Meanwhile, JobNode tries to dispatch the new job task to an
 * available worker outlink based on the priority of the job. If there is no
 * enough worker available, the job task will wait until the required worker
 * resource is available. JobNode also periodically monitors all active jobs
 * and sends query events to their worker outlinks. When the query events are
 * collected, They will be escalated to the outlink of notice. Once a job
 * has completed, JobNode will collect the task message from the worker
 * outlink and escalates the final report event to the notice outlink. If an
 * incoming message is a job query, JobNode will forward it to the worker
 * outlink. Once the response is collected, it will be routed to the bypass
 * outlink.
 *<br/><br/>
 * JobNode contains multiple rulesets that categorize the incoming messages
 * into non-overlapped groups.  Each group has its own priority for scheduling
 * and threshold for the maximum number of active jobs. JobNode also supports
 * the dynamic threshold via the array of ActiveTime. By default, all messages
 * in the same group are unique. But if KeyTemplate is defined in the ruleset,
 * messages with the same key are treated as identical. JobNode will make
 * sure only the first one of the identical messages will be scheduled. This
 * way, JobNode is able to manage and schedule jobs for each group of messages.
 * A group can also define its own Heartbeat and DisplayMask, as well as the a
 * bunch of property names. Hearbeat controls how often to query the status on
 * active jobs. DisplayMask determines whether to log the escalation messages.
 * Property names specify what properties to be copied over to the escalation
 * messages.
 *<br/><br/>
 * JobNode always creates an extra ruleset, nohit, for those messages not
 * hitting any pre-defined rulesets. All the nohit messages will be routed to
 * the outlink of bypass.
 *<br/><br/>
 * You are free to choose any names for the outlinks.  But JobNode always
 * assumes the first outlink for done, the second for bypass, and third for
 * notice and the rest for workers.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JobNode extends Node implements Comparator<int[]> {
    private int defaultHeartbeat = 30000;
    private String rcField, sizeField;
    private int[] heartbeat;

    private QList reqList = null;      // pending jobs {cid, rid}
    private AssetList jobList = null;  // active jobs {id, cid, rid, ...}
    private int[] outLinkMap;
    private final static int JOB_ID = 0;
    private final static int JOB_CID = 1;
    private final static int JOB_RID = 2;
    private final static int JOB_SIZE = 3;
    private final static int JOB_TRANS = 4;
    private final static int JOB_COUNT = 5;
    private final static int JOB_RETRY = 6;
    private final static int JOB_TIME = 7;
    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int NOTICE_OUT = 2;
    private final static int OFFSET_OUT = 3;
    private int BOUNDARY = NOTICE_OUT + 1;

    public JobNode(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        long[] ruleInfo, outInfo;
        long tm;
        int i, j, n, id, ruleSize = 512;
        String key;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "schedule";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            defaultHeartbeat = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";
        if ((o = props.get("SizeField")) != null && o instanceof String)
            sizeField = (String) o;
        else
            sizeField = "FileSize";

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{BYPASS_OUT, NOTICE_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, NOTICE_OUT};
        outLinkMap[BYPASS_OUT] = overlap[0];
        outLinkMap[NOTICE_OUT] = overlap[1];

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

        BOUNDARY = outLinkMap[NOTICE_OUT];
        BOUNDARY = (BOUNDARY >= outLinkMap[BYPASS_OUT]) ? BOUNDARY :
            outLinkMap[BYPASS_OUT];
        BOUNDARY ++;
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                " - " + linkName + " " + capacity + " / " + BOUNDARY +
                strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (BOUNDARY >= assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        msgList = new AssetList(name, capacity);
        reqList = new QList(name, capacity);
        jobList = new AssetList(name, assetList.size() - OFFSET_OUT);
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
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = BYPASS_OUT;
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            // set the default threshold to capacity for nohit
            ruleInfo[RULE_MODE] = capacity;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            ruleList.add(key, ruleInfo, rule);
            outInfo = assetList.getMetaData(0);
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

        int hmax = 0;
        int[] hbeat = new int[ruleList.size()];
        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            ruleInfo = ruleList.getMetaData(i);
            hbeat[i] = (int) ruleInfo[RULE_GID];
            if (hbeat[i] <= 0)
                hbeat[i] = defaultHeartbeat;
            if (hbeat[i]> hmax)
                hmax = hbeat[i];
            if ((debug & DEBUG_INIT) == 0)
                continue;
            strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                ruleInfo[RULE_EXTRA] + " " + ruleInfo[RULE_MODE] + " " +
                ruleInfo[RULE_OPTION] + " " + ruleInfo[RULE_TTL]/1000 + " " +
                ruleInfo[RULE_GID]/1000 + " " + ruleInfo[RULE_DMASK] + " - " +
                assetList.getKey((int) ruleInfo[RULE_OID]));
        }
        heartbeat = MonitorUtils.getHeartbeat(hbeat);
        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name+" RuleName: RID Pri Max TWin " +
                "TTL HB DMask" + " - OutName"+ strBuf.toString()).send();
            strBuf = new StringBuffer();
            for (i=0; i<heartbeat.length; i++)
                strBuf.append("\n\t" + i + " " + heartbeat[i]/1000);
            new Event(Event.DEBUG, name + " HEARTBEAT: HID HB - " + hmax +
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
        String key, str, ruleName, preferredOutName = null;
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
        if(preferredOutName !=null && (i=assetList.getID(preferredOutName))!=1){
            preferredOutName = assetList.getKey(outLinkMap[BYPASS_OUT]);
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

        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else {
            ruleInfo[RULE_PID] = TYPE_NONE;

            // store TimeToLive into RULE_TTL field
            if ((o = ph.get("TimeToLive")) != null)
                ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
            else
                ruleInfo[RULE_TTL] = 0;

            // store Heartbeat into RULE_GID field
            if ((o = ph.get("Heartbeat")) != null)
                ruleInfo[RULE_GID] = 1000 * Integer.parseInt((String) o);
            else
                ruleInfo[RULE_GID] = defaultHeartbeat;

            // store Priority into RULE_EXTRA field
            if ((o = ph.get("Priority")) != null)
                ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_EXTRA] = 0;

            // store default threshold into RULE_MODE
            if ((o = ph.get("Threshold")) != null && o instanceof String)
                ruleInfo[RULE_MODE] = Integer.parseInt((String) o);
            if (ruleInfo[RULE_MODE] <= 0)
                ruleInfo[RULE_MODE] = 1;

            if ((o = ph.get("DisplayMask")) != null)
                ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
            else
                ruleInfo[RULE_DMASK] = 0;

            if ((o = ph.get("KeyTemplate")) != null && o instanceof String) {
                rule.put("KeyTemplate", new Template((String) o));
                if((o=ph.get("KeySubstitution")) != null && o instanceof String)
                    rule.put("KeySubstitution",new TextSubstitution((String)o));
            }

            if ((o = ph.get("ActiveTime")) != null && o instanceof List) {
                TimeWindows[] tw;
                int m;
                list = (List) o;
                m = list.size();
                tw = new TimeWindows[m];
                k = 0;
                for (i=0; i<m; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof Map))
                        continue;
                    tw[k++] = new TimeWindows((Map) o);
                }
                rule.put("TimeWindows", tw);
                // store number of time windows into RULE_OPTION
                ruleInfo[RULE_OPTION] = k;
            }
        }
        outInfo = assetList.getMetaData(outLinkMap[BYPASS_OUT]);
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

        return rule;
    }

    /**
     * picks up a message from input queue and evaluate its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String key, msgStr = null, ruleName = null;
        String[] propertyName = null, propertyValue = null;
        long currentTime, previousTime, sessionTime;
        long[] ruleInfo = null, outInfo;
        int[] ruleMap;
        int[][] sorted_rids;
        Browser browser;
        MessageFilter[] filters = null;
        Map rule = null;
        long count = 0;
        int mask, ii, hid, hbeat;
        int i = 0, k, mid, n, size, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0;  // the id of the ruleset
        int oid = -1; // the id of the outlink
        int id = -1;  // the id of the empty cell in pipe
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
            reqList.clear();
            reqList = new QList(name, capacity);
            cells.clear();
            cells = new CollectibleCells(name, capacity);
        }

        // initialize filters
        n = ruleList.size();
        filters = new MessageFilter[n];
        browser = ruleList.browser();
        ruleMap = new int[n];
        sorted_rids = new int[n][3];
        i = 0;
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            filters[i] = (MessageFilter) rule.get("Filter");
            if (filters[i] != null) {
                if ((!ckBody) && filters[i].checkBody())
                    ckBody = true;
            }
            ruleInfo = ruleList.getMetaData(rid);
            sorted_rids[i][0] = rid;
            sorted_rids[i][1] = (int) ruleInfo[RULE_EXTRA];
            sorted_rids[i][2] = i;
            ruleMap[i++] = rid;
        }
        Arrays.sort(sorted_rids, this);

        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0) && !ckBody)
            dspBody = true;

        // update assetList
        n = out.length;
        for (i=0; i<n; i++) {
            Object[] asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

        n = ruleMap.length;
        // set all groups active at startup but the first
        hid = heartbeat.length - 1;
        hbeat = heartbeat[hid];
        currentTime = System.currentTimeMillis();
        previousTime = currentTime;
        sessionTime = currentTime;
        previousRid = -1;
        ii = 0;
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;

            if ((cid = in.getNextCell(waitTime)) >= 0) { // new request
                if ((inMessage = (Message) in.browse(cid)) == null) {
                    in.remove(cid);
                    new Event(Event.WARNING, name + ": null msg from " +
                        in.getName()).send();
                    continue;
                }

                currentTime = System.currentTimeMillis();
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
                    new Event(Event.ERR, str + " failed to apply the filter "+
                        i + ": " + Event.traceStack(e)).send();
                    i = -1;
                }

                if (rid != previousRid) {
                    ruleName = ruleList.getKey(rid);
                    ruleInfo = ruleList.getMetaData(rid);
                    previousRid = rid;
                }

                if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // bypass
                    oid = (int) ruleInfo[RULE_OID];
                    if ((debug & DEBUG_PROP) > 0)
                        new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                            " rid=" + rid + " oid=" + oid).send();

                    if (displayMask > 0) try { // display the message
                        if (dspBody)
                            msgStr = MessageUtils.processBody(inMessage,buffer);
                        new Event(Event.INFO, name + ": " + ruleName +
                            " bypassed msg "+ (count + 1) + ":" +
                            MessageUtils.display(inMessage, msgStr,
                            displayMask, displayPropertyName)).send();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " failed to display msg: " +e.toString()).send();
                    }

                    count += passthru(currentTime, inMessage, in,rid,oid,cid,0);
                }
                else { // new request
                    i = reqList.reserve(cid);
                    if (i >= 0) {
                        reqList.add(new int[]{cid, rid}, i);
                        ruleInfo[RULE_PEND] ++;
                    }
                    else {
                        in.putback(cid);
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " request cache is full: " + i + " " +
                            reqList.size()+ "/" + reqList.getCapacity()).send();
                        try {
                            Thread.sleep(waitTime + waitTime);
                        }
                        catch (Exception e) {
                        }
                    }
                    if ((debug & DEBUG_PROP) > 0)
                        new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                            " rid=" + rid + " i=" + i + " qs=" +
                            reqList.size() + "/" + ruleInfo[RULE_PEND]).send();
                }
                feedback(in, -1L);
                inMessage = null;
            }
            else { // no request
                if (msgList.size() > 0)
                    feedback(in, -1L);
                currentTime = System.currentTimeMillis();
                if (currentTime - previousTime < defaultHeartbeat)
                    continue;
                previousTime = currentTime;
            }

            // reach here only when new request or heartbeat
            if (reqList.size() > 0) { // pending job requests
                i = schedule(currentTime, in, buffer, sorted_rids);
                if (i > 0) { // new jobs scheduled, reset session
                    hid = heartbeat.length - 1;
                    hbeat = heartbeat[hid];
                    currentTime = System.currentTimeMillis();
                    sessionTime = currentTime;
                }
            }

            currentTime = System.currentTimeMillis();
            if (currentTime < sessionTime) { // session not due yet
                continue;
            }

            if (jobList.size() > 0) // active jobs
                query(currentTime, in, hbeat);

            hid ++;
            if (hid >= heartbeat.length) { // reset session
                hid = 0;
                sessionTime += heartbeat[hid];
            }
            else {
                sessionTime += heartbeat[hid] - hbeat;
            }
            hbeat = heartbeat[hid];
            currentTime = System.currentTimeMillis();
            if (currentTime > sessionTime) // reset sessionTime
                sessionTime = currentTime;
        }
    }

    /**
     * schedules each pending job base on its priority and available resources.
     * It sends the first report msg to NOTICE_OUT.
     */
    private int schedule(long currentTime, XQueue in, byte[] buffer,
        int[][] sorted_rids) {
        Message inMessage = null;
        String key, ruleName;
        TextSubstitution sub = null;
        Template temp = null;
        TimeWindows[] tw = null;
        String[] propertyName = null;
        Map rule;
        Browser browser;
        long[] ruleInfo;
        int[] meta;
        int id, cid, rid, oid;
        int i, j, k, m, n, count = 0;
        if (jobList.isFull() || reqList.size() <= 0)
            return count;
        n = sorted_rids.length;
        for (i=0; i<n; i++) {
            rid = sorted_rids[i][0];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_PEND] <= 0) // no jobs pending
                continue;
            rule = (Map) ruleList.get(rid);
            tw = (TimeWindows[]) rule.get("TimeWindows");
            temp = (Template) rule.get("KeyTemplate");
            sub = (TextSubstitution) rule.get("KeySubstitution");
            propertyName = (String[]) rule.get("PropertyName");
            k = (int) ruleInfo[RULE_MODE];
            if (ruleInfo[RULE_OPTION] > 0 && k > 0) { // update threshold
                int[] th;
                m = (int) ruleInfo[RULE_OPTION];
                for (j=0; j<m; j++) {
                    th = tw[j].getCurrentThreshold(currentTime);
                    if (th != null && th.length > 0) {
                        k = th[0] / 1000;
                        break;
                    }
                }
            }
            if (ruleInfo[RULE_SIZE] >= k) // max out
                continue;
            k -= (int) ruleInfo[RULE_SIZE];
            ruleName = ruleList.getKey(rid);
            browser = reqList.browser();
            for (j=0; j<k; j++) {
                if (j > 0)
                    browser.reset();
                while ((id = browser.next()) >= 0) { // scan all pending jobs
                    meta = (int[]) reqList.browse(id);
                    if (rid != meta[1]) // skip if rid is not same
                        continue;
                    cid = meta[0];
                    inMessage = (Message) in.browse(cid);
                    key = null;
                    if (temp != null) try {
                        key = MessageUtils.format(inMessage, buffer, temp);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to retrieve key from msg at "+cid).send();
                        key = null;
                    }
                    if (key == null) // reset to default key
                        key = String.valueOf(cid);
                    else if (sub != null)
                        key = sub.substitute(key);
                    if (jobList.containsKey(key)) {
                        new Event(Event.ERR, name + ": " + ruleName +
                            " existing job for " + key + ": " +
                            cid + "/" + jobList.getID(key)).send();
                        continue;
                    }
                    oid = jobList.add(key,
                        new long[]{-1, cid, rid, 0,0,0,0, currentTime}, key);
                    if (oid < 0) // jobList is full?
                        return count;
                    // adjust oid
                    oid += OFFSET_OUT;

                    if (displayMask > 0) try { // display the message
                        String msgStr = null;
                        if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                            msgStr = MessageUtils.processBody(inMessage,buffer);
                        new Event(Event.INFO, name + ": " + ruleName +
                            " scheduled msg "+ (count + 1) + ":" +
                            MessageUtils.display(inMessage, msgStr,
                            displayMask, displayPropertyName)).send();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " failed to display msg: " +e.toString()).send();
                    }

                    // dispatch the job to the worker
                    m = passthru(currentTime, inMessage, in, rid, oid, cid, -1);
                    if (m > 0) { // job dispatched
                        reqList.getNextID(id);
                        reqList.remove(id);
                        count ++;
                        ruleInfo[RULE_PEND] --;

                        // escalate event
                        TextEvent event = new TextEvent();
                        event.setAttribute("name", key);
                        event.setAttribute("category", ruleName);
                        event.setAttribute("operation", "notice");
                        event.setAttribute("id", String.valueOf(id));
                        event.setAttribute("worker",
                            String.valueOf(oid - OFFSET_OUT));
                        event.setAttribute("status", "SCHEDULED");
                        event.setAttribute("progress", "0");
                        if(propertyName != null && propertyName.length > 0) try{
                            String str;
                            for(int l=0; l<propertyName.length; l++){//copy prop
                                str = MessageUtils.getProperty(propertyName[l],
                                    inMessage);
                                if (str != null && str.length() > 0)
                                    event.setAttribute(propertyName[l], str);
                            }
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName +
                                " failed to copy properties for escalation: "+
                                Event.traceStack(e)).send();
                        }
                        key += " with status of "+ event.getAttribute("status");
                        // flush the first escalation to NOTICE_OUT
                        m = passthru(currentTime, (Message) event, in, rid,
                            NOTICE_OUT, -1, 0);
                        if (m <= 0)
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to flush the first escalation for "+
                                key).send();
                        else if (m > 0 && ruleInfo[RULE_DMASK] > 0)
                            new Event(event.getPriority(), name + ": " +
                                ruleName + " escalated the first event for "+
                                key).send();
                    }
                    else { // failed to dispatch the job, so roll it back
                        new Event(Event.WARNING, name + ": " + ruleName +
                            " failed to dispatch job for " + key).send();
                        jobList.remove(oid - OFFSET_OUT);
                    }
                    if ((debug & DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name+" schedule: cid=" + cid +
                            " rid=" + rid + " oid=" + oid + " " + " k=" + k +
                            " m=" + m + " js=" + jobList.size() + " qs=" +
                            reqList.size() + "/" + ruleInfo[RULE_PEND]).send();
                }
                if (jobList.isFull() || reqList.size() <= 0)
                    return count;
            }
            if (jobList.isFull() || reqList.size() <= 0)
                return count;
        }

        return count;
    }

    /**
     * returns the number of query events sent successfully
     */
    private int query(long currentTime, XQueue in, int hbeat) {
        int i, id, k, n, cid, oid, rid, count = 0;
        Event event;
        Message msg;
        long[] outInfo, ruleInfo, meta;
        String[] propertyName = null;
        String key, str;
        StringBuffer strBuf = null;
        Map rule;
        n = jobList.size();
        if (n <= 0)
            return 0;
        if ((debug & DEBUG_REPT) > 0)
            strBuf = new StringBuffer();

        Browser browser = jobList.browser();
        while ((i = browser.next()) >= 0) { // for each active job
            meta = (long[]) jobList.getMetaData(i);
            rid = (int) meta[JOB_RID];
            ruleInfo = ruleList.getMetaData(rid);
            if ((hbeat % (int) ruleInfo[RULE_GID]) != 0) // report not active
                continue;
            cid = (int) meta[JOB_CID];
            id = (int) meta[JOB_ID];
            oid = i + OFFSET_OUT;
            outInfo = assetList.getMetaData(oid);
            if (outInfo[OUT_SIZE] <= 0) // no job running
                continue;
            key = jobList.getKey(cid);
            if (ruleInfo[RULE_TTL] > 0 &&
                currentTime - meta[JOB_TIME] >= ruleInfo[RULE_TTL]) {//timed out
                str = "abort";
                new Event(Event.WARNING, name + " job of " + key + " in "+
                    ruleList.getKey(rid) + " seems running away at " + oid +
                    ":" + meta[JOB_ID] + " with " + meta[JOB_RETRY]).send();
                if (id >= 0) { // previous query not collected yet
                    XQueue out=(XQueue)((Object[])assetList.get(oid))[ASSET_XQ];
                    if (out.remove(id) >= 0)
                        continue;
                    // not taken yet
                    out.takeback(id);
                    meta[JOB_ID] = -1;
                }
            }
            else {
                str = "query";
                if (id >= 0) { //previous query not collected yet
                    meta[JOB_RETRY] ++;
                    if ((meta[JOB_RETRY] % 4) == 0)
                        new Event(Event.WARNING, name + " job of " + key +
                            " in " + ruleList.getKey(rid) + " seems stuck at " +
                            oid + ":" + meta[JOB_ID] + " with " +
                            meta[JOB_RETRY]).send();
                    continue;
                }
            }
            if ((debug & DEBUG_REPT) > 0)
                strBuf.append("\n\t" + cid + " " + rid + " " + oid + ":" + id +
                " " + outInfo[OUT_SIZE]+ " " + outInfo[OUT_EXTRA]+ " " +
                msgList.size() + " " + reqList.size() + " " + jobList.size());

            event = new ObjectEvent();
            event.setPriority(Event.INFO);
            event.removeAttribute("priority");
            event.setAttribute("name", key);
            event.setAttribute("category", ruleList.getKey(rid));
            event.setAttribute("operation", str);
            event.setAttribute("id", String.valueOf(id));
            event.setAttribute("worker", String.valueOf(oid - OFFSET_OUT));
            rule = (Map) ruleList.get(rid);
            propertyName = (String[]) rule.get("PropertyName");
            msg = (Message) in.browse(cid);
            if (propertyName != null) try { // copy properties
                for (int j=0; j<propertyName.length; j++) {
                    str = MessageUtils.getProperty(propertyName[j], msg);
                    if (str != null && str.length() > 0)
                        event.setAttribute(propertyName[j], str);
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                    " failed to copy properties: "+Event.traceStack(e)).send();
            }

            // dispatch query event to the worker
            k = passthru(currentTime, (Message) event, in, rid, oid, -1, -1);
            if (k > 0) {
                count ++;
                outInfo[OUT_EXTRA] ++;
                meta[JOB_RETRY] = 0;
                // cache the request in jobList
                jobList.set(i, event);
            }
            else
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to passthru a query event to " + oid).send();
        }
        if ((debug & DEBUG_REPT) > 0 && strBuf.length() > 0)
            new Event(Event.DEBUG, name + " query: CID RID OID:ID " +
                "OS EX ms qs js - " + count + " msgs sent for query" +
                strBuf.toString()).send();

        return count;
    }

    /**
     * It passes the message from an input XQueue over to an output XQ.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo = null;
        int id = -1, k, mid, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            if (cid >= 0) // incoming msg
                k = (tid >= 0) ? in.putback(cid) : -2;
            else
                k = -1;
            new Event(Event.ERR, name + ": asset is null on " +
                assetList.getKey(oid) + " of " + oid + " for " +
                rid + " with msg at " + cid + "," + k).send();
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
            id = out.reserve(waitTime);
            break;
          case 1:
            shift = (int) outInfo[OUT_OFFSET];
            id = out.reserve(waitTime, shift);
            break;
          default:
            shift = (int) outInfo[OUT_OFFSET];
            id = out.reserve(waitTime, shift, len);
            break;
        }
        outCapacity = (int) outInfo[OUT_CAPACITY];

        if (id >= 0 && id < outCapacity) { // id-th cell of out reserved
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                        currentTime}, key, cid);
                else if ((state=jobList.getMetaData(oid-OFFSET_OUT)) != null) {
                    // for job query
                    if (state[JOB_ID] >= 0) { // previous query still active
                        mid = -4;
                    }
                    else { // save id to JOB_TID for tracking the queries
                        state[JOB_ID] = id;
                    }
                }
                else
                    mid = -1;
            }
            else if (oid == BYPASS_OUT) { // id-th cell has just been empty now
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
                mid = msgList.add(key, new long[] {cid, oid, id, rid,
                    tid, currentTime}, key, cid);
            }
            else if (tid < 0) { // for job request or job query
                // previous job is done, let feedback to collect it
                mid = -3;
            }
            else { // something wrong?
                mid = -2;
            }
            if (cid >= 0 && mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = (tid >= 0) ? in.putback(cid) : -2;
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size() +","+k).send();
                return 0;
            }
            if (cid >= 0 || tid < 0) // incoming msg or query event
                k = out.add(msg, id, cbw);
            else // generated msg
                k = out.add(msg, id);
            if (cid >= 0) {
                outInfo[OUT_SIZE] ++;
                if ((outInfo[OUT_STATUS] & XQueue.KEEP_RUNNING) > 0)
                    outInfo[OUT_TIME] = currentTime;
                ruleInfo = ruleList.getMetaData(rid);
                ruleInfo[RULE_SIZE] ++;
                ruleInfo[RULE_TIME] = currentTime;
            }
            else if (tid >= 0) { // for notification
                outInfo[OUT_COUNT] ++;
                outInfo[OUT_TIME] = currentTime;
            }
            if ((debug & DEBUG_PASS) > 0) {
                if (cid < 0 && tid < 0)
                    ruleInfo = ruleList.getMetaData(rid);
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ":" + cid + " " + key + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
            }
        }
        else { // reservation failed
            if (cid >= 0) // incoming msg
                k = (tid >= 0) ? in.putback(cid) : -2;
            else // generated msg
                k = -1;
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                assetList.getKey(oid) + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k).send();
            return 0;
        }
        return 1;
    }

    /**
     * It returns the number of done messages removed from the input XQueue.
     * There is no wait and it tries to collect all cells regardless what
     * milliSec is. If a collected msg is from one of the worker outlinks,
     * the escalation event will be flushed to NOTICE_OUT accordingly.
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        int k, mid, rid, oid, id, count = 0;
        long[] state, outInfo, ruleInfo;
        long t;
        StringBuffer strBuf = null;
        if ((debug & DEBUG_FBAK) > 0)
            strBuf = new StringBuffer();

        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            if (state == null) // job is done?
                continue;
            oid = (int) state[MSG_OID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null)
                continue;
            out = (XQueue) asset[ASSET_XQ];
            id = (int) state[MSG_BID];
            rid = (int) state[MSG_RID];
            outInfo = assetList.getMetaData(oid);
            ruleInfo = ruleList.getMetaData(rid);
            if (oid == BYPASS_OUT) { // for bypass only
                in.remove(mid);
                msgList.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                if ((outInfo[OUT_STATUS] & XQueue.KEEP_RUNNING) > 0)
                    outInfo[OUT_TIME] = t;
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = t;
                if ((debug & DEBUG_FBAK) > 0)
                    strBuf.append("\n\t" + rid +" "+ mid + "/" + state[MSG_CID]+
                        " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                        outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth()+
                        " " + msgList.size());
                count ++;
            }
            else if (out.getCellStatus(id) != XQueue.CELL_EMPTY) { // query
                TextEvent event;
                String key = jobList.getKey(oid - OFFSET_OUT);
                long[] meta = jobList.getMetaData(oid - OFFSET_OUT);
                if (meta == null)
                    continue;
                id = (int) meta[JOB_ID];
                if (id < 0)
                    continue;
                meta[JOB_COUNT] ++;
                meta[JOB_ID] = -1;
                outInfo[OUT_DEQ] ++;
                outInfo[OUT_EXTRA] --;
                try {
                    Event ev = (Event) jobList.get(oid - OFFSET_OUT);
                    if (meta[JOB_SIZE] <= 0)
                        meta[JOB_SIZE] =
                            Long.parseLong(ev.getAttribute(sizeField));
                    event = TextEvent.toTextEvent(ev);
                    key += " with progress of " +
                        event.getAttribute("progress") + "%";
                    k = passthru(t, (Message)event, in, rid, NOTICE_OUT, -1, 0);
                    if (k <= 0)
                        new Event(Event.ERR, name + ": " + ruleList.getKey(rid)+
                            " failed to escalate a msg for " + key).send();
                    else if (ruleInfo[RULE_DMASK] > 0)
                        new Event(event.getPriority(), name + ": " +
                            ruleList.getKey(rid) + " escalated a msg for " +
                            key).send();
                }
                catch (Exception e) {
                    event = null;
                    new Event(Event.WARNING, name +" failed to escalate: "+
                        e.toString()).send();
                }
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + " collect: " + mid + " "+
                        rid + " " + id + " " + meta[JOB_COUNT] + " " +
                        outInfo[OUT_EXTRA]+ " " + outInfo[OUT_DEQ]+ " " +
                        out.size() + "|" + out.depth() + " " +
                        msgList.size() + " " + reqList.size() + " " +
                        jobList.size()).send();
            }
            else { // job terminated
                String key = msgList.getKey(mid);
                String str = null;
                String[] propertyName;
                Map rule;
                TextEvent event;
                Message msg = (Message) in.browse(mid);
                try {
                    str = MessageUtils.getProperty(rcField, msg);
                }
                catch (Exception e) {
                    str = null;
                }
                // orginal request is removed without passthru
                in.remove(mid);
                msgList.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                if ((outInfo[OUT_STATUS] & XQueue.KEEP_RUNNING) > 0)
                    outInfo[OUT_TIME] = t;
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = t;
                jobList.remove(oid - OFFSET_OUT);
                if ((debug & DEBUG_FBAK) > 0)
                    strBuf.append("\n\t" + rid + " " + mid + "/" + mid +
                        " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                        outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth()+
                        " " + msgList.size());
                count ++;

                // final escalation
                event = new TextEvent();
                event.setAttribute("name", key);
                event.setAttribute("category", ruleList.getKey(rid));
                event.setAttribute("operation", "notice");
                event.setAttribute("id", String.valueOf(id));
                event.setAttribute("worker", String.valueOf(oid - OFFSET_OUT));
                if ("0".equals(str)) {
                    event.setAttribute("status", "FINISHED");
                    event.setAttribute("progress", "100");
                }
                else {
                    event.setAttribute("status", "FAILED");
                    event.setPriority(Event.ERR);
                }
                if (str != null)
                    event.setAttribute(rcField, str);
                rule = (Map) ruleList.get(rid);
                propertyName = (String[]) rule.get("PropertyName");
                if (propertyName != null && propertyName.length > 0) try {
                    for (int j=0; j<propertyName.length; j++) { // copy props
                        str = MessageUtils.getProperty(propertyName[j], msg);
                        if (str != null && str.length() > 0)
                            event.setAttribute(propertyName[j], str);
                    }
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleList.getKey(rid)+
                        " failed to copy properties for final escalation: "+
                        Event.traceStack(e)).send();
                }
                key += " with status of " + event.getAttribute("status");
                k = passthru(t, (Message) event, in, rid, NOTICE_OUT, -1, 0);
                if (k <= 0)
                    new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                        " failed on final escalation for " + key).send();
                else if (ruleInfo[RULE_DMASK] > 0)
                    new Event(event.getPriority(), name + ": " +
                        ruleList.getKey(rid) + " escalated a msg for " +
                        key).send();
            }
            if (milliSec >= 0) // one collection a time
                break;
        }
        if (count > 0 && (debug & DEBUG_FBAK) > 0)
            new Event(Event.DEBUG, name + " feedback: RID MID/CID OID:ID " +
                "RS OS size|depth ms - " + count + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        return count;
    }

    /**
     * It marks the cell in cells with key and id.
     */
    public void callback(String key, String id) {
        int mid;
        if (key == null || id == null || key.length() <= 0 || id.length() <= 0)
            return;
        int oid = assetList.getID(key);
        if (oid == BYPASS_OUT) // for bypass
            mid = msgList.getID(oid + "/" + id);
        else if (oid >= OFFSET_OUT) { // for a worker
            long[] meta = jobList.getMetaData(oid - OFFSET_OUT);
            if (meta != null && meta.length > JOB_CID)
                mid = (int) meta[JOB_CID];
            else
                mid = -1;
        }
        else
            mid = -1;
        if (mid >= 0) { // mark the mid is ready to be collected
            cells.take(mid);
            if ((debug & DEBUG_FBAK) > 0)
                new Event(Event.DEBUG, name +": "+ key + " called back on "+
                    mid + " with " + oid + "/" + id).send();
        }
        else if (oid < OFFSET_OUT)
            new Event(Event.ERR, name +": "+ key+" failed to callback on "+
                oid + ":" + id).send();
    }

    /**
     * cleans up MetaData for all XQs and messages
     */
    public void resetMetaData(XQueue in, XQueue[] out) {
        Object[] asset;
        XQueue xq;
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

        browser = assetList.browser();
        while ((i = browser.next()) >= 0) {
            state = assetList.getMetaData(i);
            state[OUT_SIZE] = 0;
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            state = ruleList.getMetaData(i);
            state[RULE_SIZE] = 0;
        }
    }

    /** lists all propagating messages and pending jobs */
    public Map<String, String> listPendings(XQueue xq, int type) {
        Message msg;
        Map<String, String> h, ph;
        StringBuffer strBuf;
        String key, qname, str, text;
        int[] list, info;
        int i, id, cid, k, n;
        long tm;
        boolean hasSummary;

        if (xq == null || !linkName.equals(xq.getName()))
            return null;

        h = super.listPendings(xq, type);
        if (reqList.size() <= 0 || h == null)
            return h;
        else if (h.size() <= 0) { // empty
            if (xq.size() <= xq.depth())
                return h;
            strBuf = new StringBuffer();
            k = 0;
            ph = new HashMap<String, String>();
        }
        else { // try to count k
            if (xq.size() <= xq.depth())
                return new HashMap<String, String>();
            ph = new HashMap<String, String>();
            strBuf = new StringBuffer();
            if (type == Utils.RESULT_XML) {
               strBuf.append((String) h.remove("MSG"));
               n = 0;
               while ((i = strBuf.indexOf("<CellID>", n)) >= 0) {
                   n = i+8;
                   i = strBuf.indexOf("</", n);
                   if (i > n)
                       ph.put(strBuf.substring(n, i), null);;
                   n = i;
               }
               k = ph.size();
            }
            else if (type == Utils.RESULT_JSON) {
               strBuf.append((String) h.remove("MSG"));
               n = 0;
               while ((i = strBuf.indexOf(", \"CellID\":", n)) >= 0) {
                   n = i+11;
                   i = strBuf.indexOf(", ", n);
                   if (i > n)
                       ph.put(strBuf.substring(n, i), null);;
                   n = i;
               }
               k = ph.size();
               n = strBuf.length();
               if (k > 0 && n > 2) { // trim the strBuf
                   strBuf.deleteCharAt(n-1);
                   strBuf.deleteCharAt(0);
               }
            }
            else {
                k = h.size();
            }
        }

        hasSummary = (displayPropertyName != null &&
            displayPropertyName.length > 0);

        n = reqList.getCapacity();
        list = new int[n];
        n = reqList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) { // scan the list
            cid = list[i];
            if (ph.containsKey(String.valueOf(cid))) // duplicate
                continue;
            if (xq.getCellStatus(cid) != XQueue.CELL_TAKEN)
                continue;
            msg = (Message) xq.browse(cid);
            info = (int[]) reqList.browse(cid);
            if (msg == null || info == null)
                continue;
            try {
                tm = msg.getJMSTimestamp();
            }
            catch (Exception e) {
                tm = 0;
            }
            text = Event.dateFormat(new Date(tm));
            if (!hasSummary)
                str = "";
            else try {
                str = MessageUtils.display(msg, null, MessageUtils.SHOW_NOTHING,
                    displayPropertyName);
                if (str == null)
                    str = "";
            }
            catch (Exception e) {
                str = e.toString();
            }

            key = ruleList.getKey(info[1]);
            if (key == null)
                key = "-";

            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + k + "</ID>");
                strBuf.append("<CellID>" + cid + "</CellID>");
                strBuf.append("<Status>PENDING</Status>");
                strBuf.append("<Rule>" + key + "</Rule>");
                strBuf.append("<OutLink>-</OutLink>");
                strBuf.append("<Time>" + Utils.escapeXML(text) + "</Time>");
                strBuf.append("<Summary>" +Utils.escapeXML(str)+ "</Summary>");
                strBuf.append("</Record>");
            }
            else if (type == Utils.RESULT_JSON) { // for json
                if (k > 0)
                    strBuf.append(", {");
                else
                    strBuf.append("{");
                strBuf.append("\"ID\":" + k);
                strBuf.append(", \"CellID\":" + cid);
                strBuf.append(", \"Status\":\"PENDING\"");
                strBuf.append(", \"Rule\":\"" + key + "\"");
                strBuf.append(", \"OutLink\":\"-\"");
                strBuf.append(", \"Time\":\"" + Utils.escapeJSON(text) + "\"");
                strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) + "\"");
                strBuf.append("}");
            }
            else { // for text
                h.put(linkName + "_" + k, cid + " PENDING " + key +
                    " - " + text + " " + str);
            }
            k ++;
        }

        if (k == 0) // no message found
            return h;
        else if (type == Utils.RESULT_XML)
            h.put("MSG", strBuf.toString());
        else if (type == Utils.RESULT_JSON)
            h.put("MSG", "[" + strBuf.toString() + "]");

        return h;
    }

    /**
     * returns -(BOUNDARY+1) to have the container enable ack regardlessly on
     * the outlinks starting from BOUNDARY and beyond
     */
    public int getOutLinkBoundary() {
        return -BOUNDARY-1;
    }

    public void close() {
        Browser browser;
        Map rule;
        int rid;
        setStatus(NODE_CLOSED);
        msgList.clear();
        assetList.clear();
        reqList.clear();
        jobList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null)
                rule.clear();
        }
        ruleList.clear();
    }

    public int compare(int[] a, int[] b) {
        int i, j;
        i = a[1];
        j = b[1];
        if (i > j)
            return -1;
        else if (i < j)
            return 1;
        else {
            i = a[2];
            j = b[2];
            if (i > j)
                return 1;
            else if (i < j)
                return -1;
            else
                return 0;
        }
    }

    protected void finalize() {
        close();
    }
}
