package org.qbroker.node;

/* EventCorrelator.java - a MessageNode correlating Events */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.QList;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.CollectibleCells;
import org.qbroker.monitor.ConfigList;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.EventSelector;
import org.qbroker.event.EventSummary;
import org.qbroker.event.EventMerger;
import org.qbroker.event.Event;

/**
 * EventCorrelator converts incoming JMS messages into JMSEvents and correlates
 * them according to their content and the predefined rulesets.  All the events
 * are correlated into non-overlapped groups based on sessions.  EventCorrelator
 * then creates a summary event for each of the groups.  The summary event
 * will be routed to the outlink of done. All the incoming events will be
 * routed to one of three outlinks: nohit for those events do not belong to
 * any rulesets, failure for the events failed in the correlation process and
 * bypass for those events participated in correlations. However, the outlink
 * of done can be shared with other outlinks. Since EventCorrelator
 * does not consume any events, any in-coming events has to find a way out via
 * one of the three outlinks.
 *<br><br>
 * EventCorrelator contains a number of predefined rulesets.  These rulesets
 * are used to categorize events into groups.  It defines an EventFilter for
 * selecting events and an EventMerger for building a summary event on the
 * events of the group.  An event may belong to multiple groups initially.  The
 * correlation process will decide its final ownership based on the size of the
 * groups.  The group with the largest size will own all the events in the
 * group exclusively.  In case there is a tie between two groups, the group
 * with the lowest rule ID will be the winner.  Therefore each ruleset defines
 * a unique event group.  The ruleset also defines a summary list for the
 * merger to generate a new event and to attach the summary of all the events
 * in the group. All the summary events will get formatted if the post
 * formatter is defined in their rulesets.
 *<br><br>
 * EventCorrelator always creates two extra rulesets.   The first one is nohit
 * ruleset for those events without any matches.  The other is candidate
 * ruleset for all incoming events hitting at least one matches.  Since there
 * is no way to figure out the final groupships of candidate events before the
 * correlation, they will be put into candidate group for tracking.  Please
 * remember, candidate events may not necessarily contribute to any summary
 * events.  The summary events will be counted by their own rulesets.  Their
 * property displaying and resetting are mutual-exclusively determined by the
 * DisplayMask and StringProperty of their own rulesets.  If DisplayMask
 * is not defined in a rule or it is set to 0 or -1 (default), its
 * StringProperty will be used to reset the string properties on the summary
 * events.  Otherwise, its StringProperty will only be used to display the
 * details of summary events.  On the node level, DisplayMask and StringProperty
 * control the displaying result of all candidate events.  If the DisplayMask
 * of a ruleset is set to -1, that rule will inherit the DisplayMask and the
 * StringProperty from the node for display control on the rule level.
 *<br><br>
 * It is OK to have a bypass ruleset without any EventMerger.  Any ruleset
 * with PreferredOutLink defined is called bypass ruleset.  If a bypass ruleset
 * matches an event,  the event will be routed to the specified outlink
 * directly, bypassing rest of the correlation process.  It is acting like
 * a jump out of the loop.
 *<br><br>
 * You are free to choose any names for the four fixed outlinks.  But
 * EventCorrelator always assumes the first outlink for done, the second for
 * bypass, the third for failure and the last for nohit.  Any two or more
 * outlinks can share the same outlink name.  It means these outlinks are
 * sharing the same output channel.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class EventCorrelator extends Node {
    private int heartbeat = 60000;
    private int sessionTimeout = 300000;
    private int sessionSize = 64;
    private QList groupList = null;    // groups for correlation
    private int[] outLinkMap;

    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int GROUP_DISABLED = -2;
    private final static int GROUP_NONE = -1;

    public EventCorrelator(Map props) {
        super(props);
        Object o;
        List list;
        Browser browser;
        Map<String, Object> rule;
        Map ph;
        String key;
        long[] outInfo, ruleInfo;
        long tm;
        int i, j, n, ruleSize = 512;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "correlate";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        if ((o = props.get("SessionSize")) != null)
            sessionSize = Integer.parseInt((String) o);

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[BYPASS_OUT] = overlap[0];
        outLinkMap[FAILURE_OUT] = overlap[1];
        outLinkMap[NOHIT_OUT] = overlap[2];

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
        i = (i >= outLinkMap[BYPASS_OUT]) ? i : outLinkMap[BYPASS_OUT];
        if (++i > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        cfgList = new AssetList(name, 64);
        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        groupList = new QList(name, ruleSize);
        cells = new CollectibleCells(name, capacity);

        if ((o = props.get("Ruleset")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();
        n = list.size();

        try { // init rulesets
            String str;
            ConfigList cfg;
            int k, m;
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

            // for candidate
            key = "candidate";
            ruleInfo = new long[RULE_TIME + 1];
            for (i=0; i<=RULE_TIME; i++)
                ruleInfo[i] = 0;
            ruleInfo[RULE_STATUS] = NODE_RUNNING;
            ruleInfo[RULE_TIME] = tm;
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_NONE;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            ruleList.add(key, ruleInfo, rule);
            outInfo = assetList.getMetaData(outLinkMap[BYPASS_OUT]);
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
                ph = (Map) o;
                if((o = ph.get("RuleType")) == null || !(o instanceof String) ||
                    !"ConfigList".equals((String) o)) {
                    ruleInfo = new long[RULE_TIME+1];
                    rule = initRuleset(tm, ph, ruleInfo);
                    if(rule != null && (key=(String) rule.get("Name")) != null){
                        if(ruleList.add(key, ruleInfo, rule) < 0)//failed to add
                            new Event(Event.ERR, name + ": ruleset " + i + ", "+
                                key + ", failed to be added").send();
                    }
                    else
                        new Event(Event.ERR, name + ": ruleset " + i +
                            " failed to be initialized").send();
                    continue;
                }
                try { // for external rules via ConfigList
                    cfg = new ConfigList(ph);
                }
                catch (Exception ex) {
                    new Event(Event.ERR, name + ": ConfigList " + i +
                        " failed to be initialized").send();
                    continue;
                }
                key = cfg.getName();
                cfg.setDataField(name);
                cfg.loadList();
                k = cfg.getSize();
                m = cfgList.add(key, new long[]{k, 0}, cfg);
                if (m < 0) {
                    new Event(Event.ERR, name + ": ConfigList " + key +
                        " failed to be added to the list").send();
                    cfg.close();
                    continue;
                }
                for (j=0; j<k; j++) { // init all external rulesets
                    str = cfg.getItem(j);
                    ph = cfg.getProps(str);
                    key = cfg.getKey(j);
                    ruleInfo = new long[RULE_TIME+1];
                    try {
                        rule = initRuleset(tm, ph, ruleInfo);
                    }
                    catch (Exception ex) {
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+" failed to init external rule "+key+
                            " at " + j + ": " + Event.traceStack(ex)).send();
                        continue;
                    }
                    if (rule != null && rule.size() > 0) {
                        if(ruleList.add(key, ruleInfo, rule) < 0)//failed to add
                            new Event(Event.ERR, name + ": ConfigList " +
                                cfg.getName()+ " failed to add external rule " +
                                key + " at " + j).send();
                    }
                    else
                        new Event(Event.ERR, name + ": ConfigList " +
                            cfg.getName()+ " failed to init external rule "+
                            key + " at " + j).send();
                }
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_EXTRA] + " " +
                    ruleInfo[RULE_DMASK] + " " + ruleInfo[RULE_OPTION] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+
                " RuleName: RID PID EXTRA DMASK OPTION - OutName" +
                "OutName" + strBuf.toString()).send();
        }
    }

    public int updateParameters(Map props) {
        Object o;
        int i, n;

        n = super.updateParameters(props);
        if ((o = props.get("Heartbeat")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i > 0 && i != heartbeat) {
                heartbeat = i;
                n++;
            }
        }
        if ((o = props.get("SessionTimeout")) != null) {
            i = 1000 * Integer.parseInt((String) o);
            if (i >= 0 && i != sessionTimeout) {
                sessionTimeout = i;
                n++;
            }
        }
        if ((o = props.get("SessionSize")) != null) {
            i = Integer.parseInt((String) o);
            if (i >= 0 && i != sessionSize) {
                sessionSize = i;
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
        if(preferredOutName !=null && !assetList.containsKey(preferredOutName)){
            preferredOutName = assetList.getKey(outLinkMap[NOHIT_OUT]);
            new Event(Event.WARNING, name + ": OutLink for " +
                ruleName + " not well defined, use the default: "+
                preferredOutName).send();
        }

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        // displayMask of ruleset stored in RULE_DMASK
        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store correlation mask into RULE_OPTION
        if ((o = ph.get("CorrelationMask")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = ruleInfo[RULE_DMASK];

        // store min event count in RULE_EXTRA
        if ((o = ph.get("MinimumEventCount")) != null)
            ruleInfo[RULE_EXTRA] = Integer.parseInt((String) o);

        rule.put("Filter", new EventSelector(ph)); 
        if (preferredOutName != null) { // for bypass
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
        }
        else { // for EventSummary
            // init post formatter for summary events only
            if ((o = ph.get("FormatterArgument")) != null && o instanceof List){
                Map<String, Object> hmap = new HashMap<String, Object>();
                hmap.put("Name", ruleName);
                hmap.put("FormatterArgument", o);
                hmap.put("ResetOption", "0");
                rule.put("Formatter", new MessageFilter(hmap));
                hmap.clear();
            }
            ruleInfo[RULE_PID] = TYPE_ACTION;
            rule.put("Merger", new EventSummary(ph));
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;


        // for StringProperty
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
     * It correlates the events into groups identified by their groupID and
     * returns total number of events in the groups
     *<br><br>
     * NB.  It is not MT-Safe due to global ruleInfo, filter, etc
     */
    private int correlate(long currentTime, int[] ruleMap,
        EventSelector[] filter, List<Event> eventList) {
        int ruleCount, size;
        int i, j, k, l, n, id, gid, rid, count, maxCount, position;
        int[] eids, nids, groupIDs;
        long[] ruleInfo;
        int[][] eventIDs;
        Event[] eventGroup;
        Event event;

        n = eventList.size();
        nids = new int[n];

        count = 0;
        ruleCount = 0;
        size = ruleMap.length;
        groupIDs = new int[size+1];
        eventIDs = new int[size+1][];
        // mark each event and match with all occurrences
        for (i=0; i<size; i++) {
            groupIDs[i] = GROUP_NONE;
            count = 0;
            for (j=0; j<n; j++) {
                event = eventList.get(j);
                if (event.getGroupID() == GROUP_DISABLED) // bypass all disabled
                    continue;
                if (filter[i].evaluate(currentTime, event))
                    nids[count++] = j;
            }
            rid = ruleMap[i];
            ruleInfo = ruleList.getMetaData(rid);
            if (count >= ruleInfo[RULE_EXTRA]) { // enforce minimum rule
                ruleInfo[RULE_SIZE] = count;
                eids = new int[count];
                for (j=0; j<count; j++)
                    eids[j] = nids[j];
                eventIDs[ruleCount] = eids;
                groupIDs[ruleCount++] = rid;
            }
            else
                ruleInfo[RULE_SIZE] = 0;
        }

        if ((debug & DEBUG_UPDT) > 0)
            new Event(Event.INFO, name + " correlate: groups=" + ruleCount +
                " gid=" +((ruleCount > 0) ? groupIDs[ruleCount-1] : -1)).send();

        while (ruleCount > 0) {
            count = 0;
            maxCount = 0;
            gid = -1;
            position = ruleCount;
            for (i=0; i<ruleCount; i++) { // find gid with the most count
                rid = groupIDs[i];
                ruleInfo = ruleList.getMetaData(rid);
                count = (int) ruleInfo[RULE_SIZE];
                if (count > maxCount) {
                    maxCount = count;
                    gid = rid;
                    position = i;
                }
            }

            if (gid < 0 || maxCount <= 0) { // leftover are empty
                ruleCount = 0;
                break;
            }

            eids = eventIDs[position];
            for (i=position+1; i<ruleCount; i++) { // delete gid from gids
                groupIDs[i-1] = groupIDs[i];
                eventIDs[i-1] = eventIDs[i];
            }
            ruleCount --;

            ruleInfo = ruleList.getMetaData(gid);
            if (maxCount < ruleInfo[RULE_EXTRA]) { // enforce MIN rule
                ruleInfo[RULE_SIZE] = 0;
                continue;
            }

            if ((debug & DEBUG_UPDT) > 0) {
                new Event(Event.INFO, name + " correlate: maxCount=" + maxCount+
                    " gid=" + gid + " eventCount="+ruleInfo[RULE_SIZE]).send();
            }

            eventGroup = new Event[maxCount];
            for (j=0; j<maxCount; j++) { // add events to the eventGroup
                i = eids[j];
                event = eventList.get(i);
                event.setGroupID(gid);
                event.setAttribute("gid", ruleList.getKey(gid));
                eventGroup[j] = event;
                for (l=0; l<ruleCount; l++) { // remove the event from eids
                    rid = groupIDs[l];
                    ruleInfo = ruleList.getMetaData(rid);
                    if (ruleInfo[RULE_SIZE] <= 0)
                        continue;
                    nids = eventIDs[l];
                    for (k=0; k<nids.length; k++) { // linear search
                        if (nids[k] == i) {
                            nids[k] = -1;
                            ruleInfo[RULE_SIZE] --;
                            break;
                        }
                        else if (nids[k] > i || k >= i) {
                            break;
                        }
                    }
                }
            }
            if (maxCount > 0) {
                i = groupList.reserve(gid);
                groupList.add(eventGroup, gid);
            }

            for (i=0; i<ruleCount; i++) { // update gids and eventIDs
                rid = groupIDs[i];
                ruleInfo = ruleList.getMetaData(rid);
                count = (int) ruleInfo[RULE_SIZE];
                if (count > 0) { // update eventIDs
                    nids = eventIDs[i];
                    if (nids.length <= count) {
                        ruleInfo[RULE_SIZE] = nids.length;
                        continue;
                    }
                    eids = new int[count];
                    k = 0;
                    for (j=0; j<nids.length; j++) {
                        if (nids[j] >= 0)
                            eids[k++] = nids[j];
                    }
                    if (k > 0) {
                        eventIDs[i] = eids;
                        ruleInfo[RULE_SIZE] = k;
                        continue;
                    }
                }
                // delete gid from gids
                ruleInfo[RULE_SIZE] = 0;
                for (j=i+1; j<ruleCount; j++) {
                    groupIDs[j-1] = groupIDs[j];
                    eventIDs[j-1] = eventIDs[j];
                }
                i --;
                ruleCount --;
            }
        }

        count = 0;
        for (i=0; i<size; i++) {
            rid = ruleMap[i];
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_SIZE] > 0)
                count ++;
        }

        if ((debug & DEBUG_UPDT) > 0)
            new Event(Event.INFO, name+" correlate: groupCount="+ count).send();

        return count;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        Event event = null;
        String msgStr = null, ruleName = null;
        EventSelector[] filter = null;
        String[] propertyName = null;
        Object[] asset;
        Browser browser;
        Map rule;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long currentTime, st, previousTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, m, n, size = 0, previousRid, dspBody;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
        int oid = 0; // the id of the output queue
        List<Event> eventList = new ArrayList<Event>();
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

        // initialize patterns
        m = ruleList.size() - 2;
        filter = new EventSelector[m];
        browser = ruleList.browser();
        ruleMap = new int[m];
        i = 0;
        while ((rid = browser.next()) >= 0) {
            if (rid <= 1)
                continue;
            rule = (Map) ruleList.get(rid);
            filter[i] = (EventSelector) rule.get("Filter");
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

        previousTime = 0L;
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
            if (currentTime - previousTime >= sessionTimeout) {
                i = correlate(currentTime, ruleMap, filter, eventList);
                size = flush(currentTime, in, out[0], 0, eventList, buffer);
                if (size > 0)
                    new Event(Event.INFO, name + ": flushed " +
                        size + " msgs due to overtime").send();
                size = 0;
                eventList.clear();
                groupList.clear();
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

            event = null;
            if (inMessage instanceof JMSEvent) {
                event = (Event) inMessage;
            }
            else try { // copy the message into event
                event = (Event) MessageUtils.duplicate(inMessage, buffer);
            }
            catch (JMSException e) {
                new Event(Event.ERR, name + " failed to copy msg: " +
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to copy msg: " +
                    Event.traceStack(e)).send();
            }

            i = NOHIT_OUT;
            if (event == null) { // for failures
                i = FAILURE_OUT;
            }
            else if (event instanceof TextEvent) { // for TextEvent
                if (((TextEvent) event).getLogMode() != Event.LOG_JMS) try {
                    // not from toEvent, so set attribute of "text"
                    event.setPriority(event.getPriority());
                    event.setAttribute("text", ((TextEvent) event).getText());
                    ((TextEvent) event).setLogMode(Event.LOG_JMS);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " failed to reset event: " +
                        Event.traceStack(e)).send();
                    i = FAILURE_OUT;
                }
            }

            rid = 0;
            if (i != FAILURE_OUT) { // evaluate the event with all rulesets
                int j;
                for (j=0; j<m; j++) {
                    if (filter[j].evaluate(currentTime, event))
                        break;
                }
                rid = (j < m) ? ruleMap[j] : 0;
            }
            ruleInfo = ruleList.getMetaData(rid);

            if (rid <= 1) { // nohit or failure
                oid = outLinkMap[i];
                rid = 0;
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
            }
            else if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // bypass
                oid = (int) ruleInfo[RULE_OID];
                i = -1;
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
            }
            else { // candidate for correlations
                i = BYPASS_OUT;
                eventList.add(event);
                size ++;
                oid = outLinkMap[i];
                ruleName = ruleList.getKey(rid);
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();

                if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                    rule = (Map) ruleList.get(rid);
                    propertyName = (String[]) rule.get("PropertyName");
                    msgStr = null;
                    if ((ruleInfo[RULE_DMASK] & dspBody) > 0)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    new Event(Event.INFO, name +": "+ ruleName + " caught msg "+
                        (count+1) + ":"+ MessageUtils.display(inMessage, msgStr,
                        (int) ruleInfo[RULE_DMASK], propertyName)).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to display msg: " + e.toString()).send();
                }

                // reset to candidate ruleset
                rid = 1;
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            if (size >= sessionSize) { // correlation
                i = size - sessionSize;
                currentTime = System.currentTimeMillis();
                i = correlate(currentTime, ruleMap, filter, eventList);
                size = flush(currentTime, in, out[0], 0, eventList, buffer);
                if (size > 0)
                    new Event(Event.INFO, name + ": flushed " +
                        size + " msgs due to oversize: " +i).send();
                size = 0;
                eventList.clear();
                groupList.clear();
                previousTime = currentTime;
            }
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * displays and sets properties on the summary events according to
     * their rulesets and puts them to the specified output link before
     * returning the number of events flushed.
     */
    private int flush(long currentTime, XQueue in, XQueue out, int oid,
        List<Event> eventList, byte[] buffer) {
        int i, n, cid, rid, dmask, count = 0, dspBody;
        Map rule;
        EventMerger merger;
        MessageFilter filter;
        String[] pn;
        long[] ruleInfo;
        String ruleName;
        Event event;
        Event[] eventGroup;
        n = eventList.size();
        if (n <= 0)
            return 0;

        dspBody = MessageUtils.SHOW_BODY + MessageUtils.SHOW_SIZE;
        count = 0;
        for (i=0; i<n; i++) { // log and merge
            event = eventList.get(i);
            rid = event.getGroupID();
            if (rid < 0)
                continue;
            ruleInfo = ruleList.getMetaData(rid);
            if (ruleInfo[RULE_SIZE] <= 0)
                continue;
            eventGroup = (Event[]) groupList.browse(rid);
            if (eventGroup == null || eventGroup.length <= 0)
                continue;
            groupList.takeback(rid);
            ruleName = ruleList.getKey(rid);
            rule = (Map) ruleList.get(rid);
            pn = (String[]) rule.get("PropertyName");
            merger = (EventMerger) rule.get("Merger");
            filter = (MessageFilter) rule.get("Formatter");
            try {
                event = merger.merge(currentTime, eventGroup);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to merge: " + Event.traceStack(e)).send();
                ruleInfo[RULE_SIZE] = 0;
                continue;
            }

            if ((debug & DEBUG_COLL) > 0)
                new Event(Event.DEBUG, name + " flush: rid=" +
                    rid + " "+ oid +"/"+(count+1)).send();

            // retrieve displayMask from RULE_OPTION
            dmask = (int) ruleInfo[RULE_OPTION];
            if (dmask != 0) try {  // display the message
                String msgStr = null;
                if((dmask & dspBody) > 0)
                    msgStr = ((TextEvent) event).getText();
                new Event(Event.INFO, name + ": " + ruleName + " correlated " +
                    (count+1) + ":" + MessageUtils.display((TextEvent) event,
                    msgStr, dmask, pn)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            if (filter != null && filter.hasFormatter()) try { // post format
                filter.format((TextEvent) event, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to format the msg: " + Event.traceStack(e)).send();
            }

            // flush the summary event
            if(passthru(currentTime, (JMSEvent)event, in, rid, oid, -1, 0) > 0){
                count ++;
                ruleInfo[RULE_SIZE] = 0;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
            }
            else
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to flush a summary event of " + i).send();
        }

        return count;
    }

    public void close() {
        super.close();
        groupList.clear();
    }

    protected void finalize() {
        close();
    }
}
