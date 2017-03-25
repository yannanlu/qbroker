package org.qbroker.node;

/* DuplicateNode.java - a MessageNode duplicating JMS messages */

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
import javax.jms.MessageFormatException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * DuplicateNode duplicates incoming JMS messages to various selected
 * destinations.  It has two types of outlinks, primary as of the first outlink
 * and non-primary as of rest of outlinks.  All the incoming messages always
 * go out thru the primary outlink.  Their duplicates will be routed to their
 * selected non-primay outlinks.
 *<br/><br/>
 * DuplicateNode may also contain a number of predefined rulesets.  These
 * rulesets categorize messages into non-overlapping groups.  Therefore, each
 * rule defines a unique message group.  If a ruleset has the preferredOutLink
 * defined and it is same as the primary (the first) outlink, DuplicateNode
 * will route the messages of the group to the primay outlink without any
 * duplicate actions as a bypass.  Any other bypass will be forced to the
 * primary outlink since the messages going out of non-primary outlinks are
 * not collectible. Otherwise, the original message will be duplicated into
 * copies for delivery. The total number of duplicated messages are tracked
 * by RULE_PEND of their rulesets.
 *<br/><br/>
 * A ruleset may define a list of the outlinks as the selected non-primary
 * outlinks for the duplicated messages.  If any selected outlink is either
 * the primary or not existing, DuplicateNode will ignore it with a warning.
 * If the selected outlinks are not defined or empty, DuplicateNode will
 * assign the default list that contains all non-primary outlinks.  But if
 * PreferredOutLink is defined in the ruleset, the ruleset will be treated as
 * a bypass ruleset.  For those messages falling off all defined rulesets,
 * DuplicateNode always creates an extra ruleset, nohit, to handle them.
 * Nohit ruleset is always a bypass rule to the primay outlink.
 *<br/><br/>
 * NB. There is a transaction control on the primary (first) outlink.
 * The XAMode can be defined on the node level or on the rule level.
 * The XAMode on the rule level will overwrite the one on the node level.
 * If the XAMode of the node is on, all non-primary outlinks will wait for
 * the transaction completed on the primary outlink before their propagation
 * unless it is set off on certain rules.  Therefore, it is possible to loss
 * messages on the non-primary outlinks, since secondary outlinks have no
 * transaction support.  However, if the XAMode of the node is off, all the
 * non-primary outlinks will propagate with no wait.  Therefore, there is a
 * chance that the messages either double fed or lost on non-primary outlinks.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DuplicateNode extends Node {
    private int NOHIT_OUT = 0;
    private long[] OID_MASK;

    public DuplicateNode(Map props) {
        super(props);
        Object o;
        List list;
        String key;
        Browser browser;
        Map<String, Object> rule;
        long[] outInfo, ruleInfo;
        long tm;
        int i, j, n, id, option, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "duplicate";

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        assetList = NodeUtils.initOutLinks(tm, capacity, n, OUT_ORULE,
            name, list);

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));

        n = assetList.size();
        if (n > 60)
            n = 60;
        OID_MASK = new long[n+1];
        long mask = 1;
        for (i=0; i<=n; i++) {
            OID_MASK[i] = mask;
            mask += mask;
        }

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
                    "," + outInfo[OUT_LENGTH] + " " + outInfo[OUT_ORULE]);
        }

        if ((debug & DEBUG_INIT) > 0) {
            new Event(Event.DEBUG, name + " LinkName: OID Capacity Partition " +
                "Option - "+linkName +" "+ capacity + strBuf.toString()).send();
            strBuf = new StringBuffer();
        }

        if (assetList.size() <= 0)
            throw(new IllegalArgumentException(name+": OutLink is empty"));

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
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            ruleInfo[RULE_MODE] = xaMode;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            rule.put("PropertyName", displayPropertyName);
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

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_EXTRA] + ruleInfo[RULE_GID] + " " +
                    ruleInfo[RULE_OPTION] + ruleInfo[RULE_DMASK] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID MODE NCOPY OMASK OPTION DMASK OutName" +
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
        long[] outInfo;
        int[] oids;
        int i, j, k, n, id, m;

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
        if (preferredOutName != null &&
           !preferredOutName.equals(assetList.getKey(NOHIT_OUT))) {
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

        if ((o = ph.get("ResetOption")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = RESET_NONE;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store xaMode into RULE_MODE
        ruleInfo[RULE_MODE] = xaMode;
        if ((o = ph.get("XAMode")) != null) // overwrite xaMode
            ruleInfo[RULE_MODE] = Integer.parseInt((String) o);

        if (preferredOutName != null) { // bypass
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("SelectedOutLink")) != null &&
            o instanceof List) { // for selected duplicates
            long mask = 0;
            list = (List) o;
            k = list.size();
            oids = new int[k];
            m = 0;
            for (i=0; i<k; i++) {
                str = (String) list.get(i);
                if (str == null || (id = assetList.getID(str)) < 0) {
                    new Event(Event.WARNING, name + ": SelectedOutLink" +
                        "[" + i + "]=" + str + " for "+ ruleName +
                        " not defined, skipped it").send();
                    continue;
                }
                oids[m++] = id;
                if (id <= 60)
                    mask += OID_MASK[id];
            }
            if (m <= 0) { // no selected outlinks defined, use default
                m = assetList.size() - 1;
                oids = new int[m];
                for (i=1; i<=m; i++) {
                    oids[i-1] = i;
                }
                if (m < 60)
                    mask = OID_MASK[m+1] - 1 - OID_MASK[0];
                else
                    mask = 0;
            }
            // store number of duplicated copies into RULE_EXTRA
            ruleInfo[RULE_EXTRA] = m;
            // store the oid mask into RULE_GID
            ruleInfo[RULE_GID] = mask;
            rule.put("SelectedOutLink", oids);
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = TYPE_NONE;
        }
        else { // for default duplicates
            m = assetList.size() - 1;
            // store number of duplicated copies into RULE_EXTRA
            ruleInfo[RULE_EXTRA] = m;
            // store the oid mask into RULE_GID
            if (m < 60)
                ruleInfo[RULE_GID] = OID_MASK[m+1] - 1 - OID_MASK[0];
            else
                ruleInfo[RULE_GID] = 0;
            oids = new int[m];
            for (i=1; i<=m; i++) {
                oids[i-1] = i;
            }
            rule.put("SelectedOutLink", oids);
            ruleInfo[RULE_OID] = NOHIT_OUT;
            ruleInfo[RULE_PID] = TYPE_NONE;
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
     * picks up a message from input queue and duplicate it to all
     * output XQ
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null, message, msg;
        long currentTime, wt;
        String msgStr = null, ruleName = null;
        Object[] asset;
        Map rule;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap, oids = null;
        long count = 0;
        int mask, ii, sz, dspBody;
        int i = 0, n, previousRid = -1;
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
            currentTime = System.currentTimeMillis();

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
                oids = (int[]) rule.get("SelectedOutLink");
                previousRid = rid;
            }
            oid = (int) ruleInfo[RULE_OID];

            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagated: cid=" + cid +
                    " rid=" + rid + " oid=" + oid + " dup=" +
                    ruleInfo[RULE_EXTRA]).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                if ((ruleInfo[RULE_DMASK] & dspBody) > 0 && !ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                new Event(Event.INFO, name+": "+ ruleName + " duplicated msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, msgStr,
                    (int) ruleInfo[RULE_DMASK], propertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + " failed to display " +
                    "msg for " + ruleName + ": " +e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try { // post format
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

                filter.format(inMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to format the msg: "+ Event.traceStack(e)).send();
            }

            if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // bypass
                count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            }
            else if (ruleInfo[RULE_MODE] == 1) { // dup msg later
                count += passthru(currentTime, inMessage, in, rid, oid, cid, 1);
            }
            else { // no wait on the primary, so dup msg at once
                message = MessageUtils.duplicate(inMessage, buffer);
                count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
                copy(currentTime, message, in, rid, (int) ruleInfo[RULE_EXTRA],
                    oids, buffer);
            }
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * returns the number of the copied messages pushed out
     */
    private int copy(long currentTime, Message message, XQueue in, int rid,
        int n, int[] oids, byte[] buffer) {
        Message msg;
        int i, k, count, oid;
        String ruleName;
        long[] ruleInfo;
        if (message == null || oids == null || n <= 0 || oids.length < n)
            return 0;
        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        count = 0;
        for (i=0; i<n; i++) {
            oid = oids[i];
            if (i == n - 1) // last one
                msg = message;
            else try {
                msg = MessageUtils.duplicate(message, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to duplicate msg to " + assetList.getKey(oid) +
                    ": " + Event.traceStack(e)).send();
                continue;
            }
            k = passthru(currentTime, msg, in, rid, oid, -1, 0);
            if (k <= 0)
                new Event(Event.ERR, name + ": " + ruleName + 
                    " failed to passthru copied msg to " +
                    assetList.getKey(oid) + " " + k).send();
            else {
                count ++;
                ruleInfo[RULE_PEND] ++;
            }
        }

        return count;
    }

    /**
     * It passes the message from the input XQueue over to an output XQueue and
     * returns 1 upon success or 0 otherwise. In case of cid < 0, the message
     * has nothing to do with the input XQueue. Nothing will be added to
     * msgList for tracking. No update on stats of ruleset either.
     * If tid is 1, it will cache the duplicated msg into msgList for
     * later retrieval.
     */
    protected int passthru(long currentTime, Message msg, XQueue in,
        int rid, int oid, int cid, int tid) {
        Message message = null; // for message cache
        Object[] asset = null;
        XQueue out;
        long[] state, outInfo, ruleInfo = null;
        byte[] buffer = new byte[4096];
        int id = -1, k, mid, mask, len, shift, outCapacity;
        asset = (Object[]) assetList.get(oid);
        if (asset == null) {
            if (cid >= 0) // incoming msg
                k = in.putback(cid);
            else
                k = -1;
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
            String key;
            if (cid < 0 || tid != 1) // use tid=1 to indicate duplication
                message = msg;
            else try { // duplicate first msg for cache
                ruleInfo = ruleList.getMetaData(rid);
                message = MessageUtils.duplicate(msg, buffer);
            }
            catch (Exception e) {
                out.cancel(id);
                k = in.putback(cid);
                new Event(Event.WARNING, name + ": " + ruleList.getKey(rid) +
                    " failed to duplicate msg at " + cid + ": " +
                    Event.traceStack(e)).send();
                return 0;
            }
            key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[]{cid, oid, id, rid,
                        tid, currentTime}, message, cid);
                else // generated msg
                    mid = -1;
            }
            else { // id-th cell has just been empty now, replace it
                Message mg = null;
                cells.collect(-1L, mid);
                state = msgList.getMetaData(mid);
                k = (int) state[MSG_RID];
                ruleInfo = ruleList.getMetaData(k);
                if (ruleInfo[RULE_MODE] == 1) // retrieve cached message
                    mg = (Message) msgList.get(mid);
                msgList.remove(mid);
                in.remove(mid);
                outInfo[OUT_SIZE] --;
                outInfo[OUT_COUNT] ++;
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if (mg != null) { // duplicated the cached msg once collected
                    Map rule = (Map) ruleList.get(rid);
                    copy(currentTime, mg, in, k, (int) ruleInfo[RULE_EXTRA],
                        (int[]) rule.get("SelectedOutLink"), buffer);
                }
                if ((debug & DEBUG_FBAK) > 0)
                    new Event(Event.DEBUG, name+" passback: " + k + " " + mid +
                        ":" + state[MSG_CID] + " "+key+" "+ruleInfo[RULE_SIZE]+
                        " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                        out.depth() + " " + msgList.size()).send();
                if (cid >= 0) // incoming msg
                    mid = msgList.add(key, new long[] {cid, oid, id, rid,
                        tid, currentTime}, message, cid);
                else // generated msg
                    mid = -1;
            }
            if (cid >= 0 && mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = in.putback(cid);
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            if (cid >= 0) { // incoming msg
                k = out.add(msg, id, cbw);
                ruleInfo = ruleList.getMetaData(rid);
                ruleInfo[RULE_SIZE] ++;
                ruleInfo[RULE_TIME] = currentTime;
                outInfo[OUT_SIZE] ++;
                if (outInfo[OUT_STATUS] == NODE_RUNNING)
                    outInfo[OUT_TIME] = currentTime;
            }
            else { // generated msg
                k = out.add(msg, id);
                outInfo[OUT_COUNT] ++;
                outInfo[OUT_TIME] = currentTime;
            }
            if ((debug & DEBUG_PASS) > 0) {
                if (cid < 0)
                    ruleInfo = ruleList.getMetaData(rid);
                new Event(Event.DEBUG, name+" passthru: " + rid + " " +
                    mid + ":" + cid + " " + key + " " + ruleInfo[RULE_SIZE] +
                    " " + outInfo[OUT_SIZE] + " " + out.size() + ":" +
                    out.depth() + " " + msgList.size()).send();
            }
        }
        else { // reservation failed
            if (cid >= 0) // incoming msg
                k = in.putback(cid);
            else // generated msg
                k = -1;
            ruleInfo = ruleList.getMetaData(rid);
            new Event(Event.WARNING, name + ": XQ is full on " +
                out.getName() + " of " + oid + " for " + rid +
                ": " + outInfo[OUT_QDEPTH] + "," + outInfo[OUT_SIZE] +
                ": " + out.size() + "," + out.depth() + " / " +
                ruleInfo[RULE_SIZE] + ": " + k).send();
            return 0;
        }
        return 1;
    }

    /**
     * It returns the number of done messages removed from in.
     * If milliSec < 0, there is no wait and it tries to collect all cells.
     * Otherwise, it just tries to collect the first collectible cell.
     * If ruleInfo[RULE_MODE] is 1, it will retrieve the duplicated msg
     * from msgList and copies it to their destinations.
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        byte[] buffer = new byte[4096];
        Message msg;
        int mid, rid, oid, id, l = 0;
        long[] state, outInfo, ruleInfo;
        long t;
        StringBuffer strBuf = null;
        if ((debug & DEBUG_FBAK) > 0)
            strBuf = new StringBuffer();

        t = System.currentTimeMillis();
        while ((mid = cells.collect(milliSec)) >= 0) {
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null)
                continue;
            out = (XQueue) asset[ASSET_XQ];
            id = (int) state[MSG_BID];
            in.remove(mid);
            rid = (int) state[MSG_RID];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_MODE] == 1) // retrieve the cached msg
                msg = (Message) msgList.get(mid);
            else
                msg = null;
            msgList.remove(mid);
            outInfo = assetList.getMetaData(oid);
            outInfo[OUT_SIZE] --;
            outInfo[OUT_COUNT] ++;
            if (outInfo[OUT_STATUS] == NODE_RUNNING)
                outInfo[OUT_TIME] = t;
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = t;
            if ((debug & DEBUG_FBAK) > 0)
                strBuf.append("\n\t" + rid + " " + mid + "/" + state[MSG_CID] +
                    " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth() +
                    " " + msgList.size());
            if (msg != null) { // duplicate the cached msg once collected
                Map rule = (Map) ruleList.get(rid);
                copy(t, msg, in, rid, (int) ruleInfo[RULE_EXTRA],
                    (int[]) rule.get("SelectedOutLink"), buffer);
            }
            l ++;
            if (milliSec >= 0) // only one collection a time
                break;
        }
        if (l > 0 && (debug & DEBUG_FBAK) > 0)
            new Event(Event.DEBUG, name + " feedback: RID MID/CID OID:ID " +
                "RS OS size|depth ms - " + l + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        return l;
    }
}
