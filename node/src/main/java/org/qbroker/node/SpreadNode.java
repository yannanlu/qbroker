package org.qbroker.node;

/* SpreadNode.java - a MessageNode spreading data in JMS messages */

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
import org.qbroker.common.QList;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.CollectibleCells;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.TextEvent;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * SpreadNode retrieves data from JMS messages and spreads them according to
 * the predefined rulesets.  It filters them into three outlinks: bypass
 * for all the spread messages, nohit for those messages do not belong to
 * any rulesets, failure for the messages failed at the spread operations.
 * SpreadNode will create a set of new messages as the result of the spread
 * operations for each spread message.  These new messages go to the outlink
 * of done.  Also all the new messages will be in TextMessage.  Any incoming
 * messages will go out via one of the three other outlinks.
 *<br/><br/>
 * SpreadNode contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each ruleset
 * defines a unique message group.  Ruleset also defines the MaxDuration and
 * the Interval for spread operations.  Each group maintains its own data cache
 * and the state. If any data expires, SpreadNode will create a new TextMessage
 * loaded with the data.  The new messages will be flushed out.  The flushed
 * messages can be displayed via node based DisplayMask and StringProperty.
 * The number of the data points in cache is tracked by RULE_PEND field.
 * SpreadNode always creates an extra ruleset, nohit, for all messages not
 * hitting any patterns.
 *<br/><br/>
 * SpreadNode also supports checkpointing of the internal data cache via the
 * container. In order to enable checkpointing, CheckpointTimeout must be defined
 * explicitly with a positive number in seconds for the node. On the container
 * level, CheckpointDir and SAXParser must be defined, too.
 *<br/><br/>
 * You are free to choose any names for the four fixed outlinks.  But
 * SpreadNode always assumes the first outlink for done, the second for bypass,
 * the third for failure and the last for nohit.  Any two or more outlinks can
 * share the same outlink name.  It means these outlinks are sharing the same
 * output channel.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SpreadNode extends Node {
    private int heartbeat = DEFAULT_HEARTBEAT;

    private String reportName = null;
    private QList cacheList = null;    // active caches with rid
    private int[] outLinkMap;
    private Map<String, Object> templateMap, substitutionMap, dateFormatMap;

    private final static int DEFAULT_HEARTBEAT = 60000;
    private final static int RESULT_OUT = 0;
    private final static int BYPASS_OUT = 1;
    private final static int FAILURE_OUT = 2;
    private final static int NOHIT_OUT = 3;
    private final static int ONSET_OUT = 4;
    private final static int FIELD_DATA = 0;
    private final static int FIELD_DURATION = 1;
    private final static int FIELD_COUNT = 2;
    private final static int DATA_LENGTH = 0;
    private final static int DATA_DELTA = 1;
    private final static int DATA_SIZE = 2;
    private final static int DATA_HEAD = 3;
    private final static int DATA_TAIL = 4;
    private final static int DATA_COUNT = 5;
    private final static int DATA_CTIME = 6;
    private final static int DATA_OTIME = 7;
    private final static int DATA_TIME = 8;
    private final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    public SpreadNode(Map props) {
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
            operation = "spread";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;
        n = list.size();

        tm = System.currentTimeMillis();

        int[] overlap = new int[]{FAILURE_OUT, NOHIT_OUT};
        assetList = NodeUtils.initFixedOutLinks(tm, capacity, n, overlap,
            name, list);
        outLinkMap = new int[]{RESULT_OUT, BYPASS_OUT, FAILURE_OUT, NOHIT_OUT};
        outLinkMap[FAILURE_OUT] = overlap[0];
        outLinkMap[NOHIT_OUT] = overlap[1];

        if (assetList == null)
            throw(new IllegalArgumentException(name +
                ": failed to init OutLinks"));
        if (overlap[0] < BYPASS_OUT || overlap[1] < BYPASS_OUT)
            throw(new IllegalArgumentException(name + ": bad overlap outlink "+
                overlap[0] + " or " + overlap[1]));

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
        dateFormatMap = new HashMap<String, Object>();

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        cacheList = new QList(name, ruleSize);
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
            ruleInfo[RULE_OID] = outLinkMap[NOHIT_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
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
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_TTL]/1000 + " " +
                    ruleInfo[RULE_OPTION] + " - " +
                    assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name+" RuleName: RID PID TTL Option - " +
                "OutName"+ strBuf.toString()).send();
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
        else if (heartbeat != DEFAULT_HEARTBEAT) {
            heartbeat = DEFAULT_HEARTBEAT;
            n++;
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
        int i, k, n, id, interval;

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
        if(preferredOutName !=null && (i=assetList.getID(preferredOutName))<=0){
            preferredOutName = assetList.getKey(outLinkMap[NOHIT_OUT]);
            new Event(Event.WARNING, name + ": OutLink for " + ruleName +
                " not well defined, use the default: "+preferredOutName).send();
        }

        rule.put("Filter", new MessageFilter(ph));
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String)
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);

        // store scale in RULE_OPTION
        if ((o = ph.get("Scale")) != null && o instanceof String)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);

        // MaxDuration is stored in RULE_TTL
        if ((o = ph.get("MaxDuration")) != null && o instanceof String)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);

        if ((o = ph.get("Interval")) != null && o instanceof String)
            interval = 1000 * Integer.parseInt((String) o);
        else
            interval = 0;

        if (preferredOutName != null) {
            ruleInfo[RULE_OID] = assetList.getID(preferredOutName);
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        else if ((o = ph.get("SpreadData")) != null &&
            o instanceof List) { // for spread ruleset
            Map map;
            long[] metadata;
            String[][] dataField;
            double[][] data;
            int[][] dataCount;
            int m = (int) (ruleInfo[RULE_TTL] / interval);
            list = (List) o;
            k = list.size();
            data = new double[k][m];
            dataCount = new int[k][m];
            dataField = new String[k][FIELD_COUNT + 1];
            metadata = new long[DATA_TIME + 1];
            for (i=0; i<k; i++) {
                map = (Map) list.get(i);
                if (map == null)
                    map = new HashMap();
                str = (String) map.get("DataField");
                if (str == null)
                    dataField[i][FIELD_DATA] = null;
                else if ((key = MessageUtils.getPropertyID(str)) != null)
                    dataField[i][FIELD_DATA] = key;
                else
                    dataField[i][FIELD_DATA] = new String(str);
                str = (String) map.get("DurationField");
                if (str == null)
                    dataField[i][FIELD_DURATION] = null;
                else if ((key = MessageUtils.getPropertyID(str)) != null)
                    dataField[i][FIELD_DURATION] = key;
                else
                    dataField[i][FIELD_DURATION] = new String(str);
                str = (String) map.get("CountField");
                if (str == null)
                    dataField[i][FIELD_COUNT] = null;
                else if ((key = MessageUtils.getPropertyID(str)) != null)
                    dataField[i][FIELD_COUNT] = key;
                else
                    dataField[i][FIELD_COUNT] = new String(str);
                for (int j=0; j<m; j++) {
                    data[i][j] = 0.0;
                    dataCount[i][j] = 0;
                }
            }

            for (i=0; i<=DATA_TIME; i++)
                metadata[i] = 0;
            metadata[DATA_DELTA] = interval;
            metadata[DATA_LENGTH] = m;
            metadata[DATA_SIZE] = k;
            rule.put("DataField", dataField);
            rule.put("Data", data);
            rule.put("DataCount", dataCount);
            rule.put("MetaData", metadata);

            if ((o = ph.get("TimeTemplate")) != null && o instanceof String) {
                str = (String) o;
                if (!templateMap.containsKey(str))
                    templateMap.put(str, new Template(str));
                rule.put("Template", templateMap.get(str));
                if ((o = ph.get("TimeSubstitution")) != null &&
                    o instanceof String) {
                    str = (String) o;
                    if (!substitutionMap.containsKey(str))
                        substitutionMap.put(str, new TextSubstitution(str));
                    rule.put("Substitution", substitutionMap.get(str));
                }
            }
            else
                throw new IllegalArgumentException(name + ": TimeTemplate in " +
                     ruleName + " is not well defined");

            if ((o = ph.get("TimePattern")) != null && o instanceof String) {
                str = (String) o;
                if (!dateFormatMap.containsKey(str))
                    dateFormatMap.put(str, new SimpleDateFormat(str));
                rule.put("DateFormat", dateFormatMap.get(str));
            }

            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_SPREAD;
        }
        else { // bypass
            ruleInfo[RULE_OID] = outLinkMap[BYPASS_OUT];
            ruleInfo[RULE_PID] = TYPE_BYPASS;
        }
        outInfo = assetList.getMetaData((int) ruleInfo[RULE_OID]);
        outInfo[OUT_NRULE] ++;
        outInfo[OUT_ORULE] ++;

        // StringProperties control what to copied over
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
     * For a givan data y and a positive duration, it uniformly spreads the
     * data into the given data array based on the interval.  Upon success,
     * it returns the incremental of count of cached data points.  Or it
     * returns -1 to indicate failure.
     *<br/><br/>
     * tail is latest data point<br/>
     * head + 1 is oldest data point
     */
    private int spread(long tm, XQueue in, int oid, int rid, long[] metadata,
        long duration, double y, double[] data, int[] dataCount) {
        int i, j, k, m, n, count, delta, tail, len, c;
        long t0, t1, tt;
        boolean inDetail = ((debug & DEBUG_REPT) > 0);
        StringBuffer strBuf = null;
        if (metadata == null || metadata.length <= DATA_TIME)
            return -1;
        delta = (int) metadata[DATA_DELTA];
        if (delta <= 0 || duration <= 0)
            return -1;

        if (inDetail)
            strBuf = new StringBuffer();

        t1 = metadata[DATA_TIME];
        len = (int) metadata[DATA_LENGTH];
        t0 = t1 - delta * len;
        if (tm < t0) // data point is too old and outside the range
            return 0;
        count = (int) metadata[DATA_COUNT];
        c = (count < 0) ? 0 : count;

        if (inDetail)
            strBuf.append("\n\t" + delta + "/" + len + " " + t1 + "/" +
                (t1 - t0) + " " + Event.dateFormat(new Date(tm)) + " - " +
                y + "/" + duration + "=" + (y/duration));

        if (tm > t1) { // data point is new
            if (count <= 0) { // initialize metadata
                t0 = tm % delta;
                t1 = (t0 == 0) ? tm : tm - t0 + delta;
                metadata[DATA_TIME] = t1;
                t0 = t1 - delta * len;
                metadata[DATA_TAIL] = len - 1;
                metadata[DATA_HEAD] = 0;
                // add the metadata to cacheList
                cacheList.reserve(rid);
                cacheList.add(metadata, rid);
            }
            else {
                k = 0;
                while (tm > t1) {
                    t1 += delta;
                    k ++;
                }
                metadata[DATA_TIME] = t1;
                t0 = t1 - delta * len;
                if (k > len) { // overflow at both ends
                    m = flush(System.currentTimeMillis(), in, oid, rid,
                        count, metadata);
                    metadata[DATA_TAIL] = len - 1;
                    metadata[DATA_HEAD] = 0;
                    count = 0;
                    c = 0;
                    // add the metadata to cacheList
                    cacheList.reserve(rid);
                    cacheList.add(metadata, rid);
                }
                else if (count + k > len) { // overflow at lower end
                    m = flush(System.currentTimeMillis(), in, oid, rid,
                        count + k - len, metadata);
                    tail = ((int) metadata[DATA_TAIL] + k) % len;
                    metadata[DATA_HEAD] = tail;
                    metadata[DATA_TAIL] = tail;
                    count += k - m;
                    c -= m;
                }
                else { // no overflow
                    count += k;
                    tail = ((int) metadata[DATA_TAIL] + k) % len;
                    metadata[DATA_TAIL] = tail;
                }
            }
        }
        // data point is within the current range
        t1 -= delta;
        k = 0;
        while (t1 >= tm) {
            t1 -= delta;
            k ++;
        }
        t1 += delta;

        tt = metadata[DATA_OTIME];
        tail = (int) metadata[DATA_TAIL];
        k = (tail - k + len) % len;
        if (duration <= delta) {
            n = 1;
        }
        else {
            n = (int) (duration / delta);
            if (duration > n * delta)
                n ++;
        }
        if (n <= 0)
            return -1;

        y /= duration;
        m = 0;
        j = k;
        for (i=n-1; i>0; i--) {
            data[j] += y;
            dataCount[j] ++;
            if (inDetail && (debug & DEBUG_UPDT) > 0)
                strBuf.append("\n\t" + m + ": " + j + " " + data[j] + " " +
                    Event.dateFormat(new Date(t1)));
            t1 -= delta;
            m ++;
            j = (j - 1 + len) % len;
            if (j == tail)
                break;
        }
        if (n == 1) { // for short duration
            data[j] += duration * y / delta;
            dataCount[j] ++;
            if (inDetail && (debug & DEBUG_UPDT) > 0)
                strBuf.append("\n\t" + m + ": " + j + " " + data[j] + " " +
                    Event.dateFormat(new Date(t1)));
            m ++;
            j = (j - 1 + len) % len;
            if (count <= 0) { // initialization
                metadata[DATA_COUNT] = m;
                metadata[DATA_HEAD] = j;
                metadata[DATA_OTIME] = metadata[DATA_TIME] - (m-1) * delta;
                metadata[DATA_CTIME] = System.currentTimeMillis() +
                    (len - m + 1) * delta;
            }
            else if (j == tail) { // full house
                metadata[DATA_HEAD] = j;
                metadata[DATA_COUNT] = len;
                metadata[DATA_OTIME] = metadata[DATA_TIME] - (len-1)*delta;
                if (tt > metadata[DATA_OTIME])
                    metadata[DATA_CTIME] = System.currentTimeMillis() + delta;
            }
            else if ((i = (len + tail - j) % len) > count) { // beyound
                metadata[DATA_COUNT] = i;
                metadata[DATA_HEAD] = j;
                metadata[DATA_OTIME] = metadata[DATA_TIME] - (i-1) * delta;
                if (tt > metadata[DATA_OTIME])
                    metadata[DATA_CTIME] = System.currentTimeMillis() +
                        (len - i + 1) * delta;
            }
            else {
                metadata[DATA_COUNT] = count;
            }
        }
        else if (i == 0) { // special treatment for the first data point
            if (j != tail) {
                data[j] += (duration % delta) * y / delta;
                dataCount[j] ++;
                if (inDetail && (debug & DEBUG_UPDT) > 0)
                    strBuf.append("\n\t" + m + ": " + j + " " + data[j] +
                        " " + Event.dateFormat(new Date(t1)));
                m ++;
                j = (j - 1 + len) % len;
                if (count <= 0) { // initialization
                    metadata[DATA_COUNT] = m;
                    metadata[DATA_HEAD] = j;
                    metadata[DATA_OTIME] = metadata[DATA_TIME] - (m-1) * delta;
                    metadata[DATA_CTIME] = System.currentTimeMillis() +
                        (len - m + 1) * delta;
                }
                else if (j == tail) { // full house
                    metadata[DATA_HEAD] = j;
                    metadata[DATA_COUNT] = len;
                    metadata[DATA_OTIME] = metadata[DATA_TIME] - (len-1)*delta;
                    if (tt > metadata[DATA_OTIME])
                        metadata[DATA_CTIME] = System.currentTimeMillis()+delta;
                }
                else if ((i = (len + tail - j) % len) > count) { // beyound
                    metadata[DATA_COUNT] = i;
                    metadata[DATA_HEAD] = j;
                    metadata[DATA_OTIME] = metadata[DATA_TIME] - (i-1) * delta;
                    if (tt > metadata[DATA_OTIME])
                        metadata[DATA_CTIME] = System.currentTimeMillis() +
                            (len - i + 1) * delta;
                }
                else {
                    metadata[DATA_COUNT] = count;
                }
            }
            else {
                metadata[DATA_HEAD] = j;
                metadata[DATA_OTIME] = metadata[DATA_TIME] - (len-1) * delta;
                if (count <= 0)
                    metadata[DATA_CTIME] = System.currentTimeMillis() + delta;
                else if (tt > metadata[DATA_OTIME])
                    metadata[DATA_CTIME] = System.currentTimeMillis() + delta;
                metadata[DATA_COUNT] = len;
            }
        }
        else { // truncated beyound lower bound
            metadata[DATA_HEAD] = j;
            metadata[DATA_OTIME] = metadata[DATA_TIME] - (len-1) * delta;
            if (count <= 0)
                metadata[DATA_CTIME] = System.currentTimeMillis() + delta;
            else if (tt > metadata[DATA_OTIME])
                metadata[DATA_CTIME] = System.currentTimeMillis() + delta;
            metadata[DATA_COUNT] = len;
        }
        if (inDetail && strBuf.length() > 0)
            new Event(Event.DEBUG, name + " " + ruleList.getKey(rid) + ": " +
                k + ":" + j + " " + m + "/" + n + "/" + metadata[DATA_COUNT] +
                " " + Event.dateFormat(new Date(metadata[DATA_OTIME])) +
                " " + Event.dateFormat(new Date(metadata[DATA_TIME])) +
                strBuf.toString()).send();

        return (int) (metadata[DATA_COUNT] - c);
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    @SuppressWarnings("unchecked")
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, ruleName = null;
        Object[] asset;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        String[] propertyName = null;
        long[] outInfo, ruleInfo = null, metadata = null;
        int[] ruleMap, options = null;
        double[][] data = null;
        int[][] dataCount = null;
        String[][] dataField = null;
        DateFormat dateFormat = null;
        Template template = null;
        TextSubstitution sub = null;
        long currentTime, previousTime, wt;
        long count = 0;
        int mask, ii, sz;
        int i = 0, n, size = 0, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
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
        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0) && !ckBody)
            dspBody = true;

        // update assetList
        n = out.length;
        for (i=0; i<n; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }
        outInfo = assetList.getMetaData(0);

        n = ruleMap.length;
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
            if (currentTime - previousTime >= heartbeat) {
                if (cacheList.size() > 0) {
                    size = update(currentTime, in, RESULT_OUT);
                    if (size > 0)
                        currentTime = System.currentTimeMillis();
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
                if (ruleInfo[RULE_PID] == TYPE_SPREAD) {
                    data = (double[][]) rule.get("Data");
                    dataCount = (int[][]) rule.get("DataCount");
                    dataField = (String[][]) rule.get("DataField");
                    metadata = (long[]) rule.get("MetaData");
                    dateFormat = (DateFormat) rule.get("DateFormat");
                    template = (Template) rule.get("Template");
                    sub = (TextSubstitution) rule.get("Substitution");
                    propertyName = (String[]) rule.get("PropertyName");
                }
                previousRid = rid;
            }

            if (i < 0) // failed to apply filters
                oid = outLinkMap[FAILURE_OUT];
            else if (rid == 0) {
                i = NOHIT_OUT;
                oid = outLinkMap[i];
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
            }
            else if (ruleInfo[RULE_PID] == TYPE_BYPASS) { // bypass
                oid = (int) ruleInfo[RULE_OID];
                i = -1;
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
            }
            else try { // spread engine
                String key, str = null;
                long duration, tm;
                double y;
                int j, k, m, mm, scale;

                // make a copy on selected properties from the 1st msg
                if (metadata[DATA_COUNT] <= 0 && propertyName != null) {
                    m = propertyName.length;
                    String[] propertyValue = new String[m];
                    rule.put("PropertyValue", propertyValue);
                    for (i=0; i<propertyName.length; i++) {
                        key = propertyName[i];
                        propertyValue[i] =
                            MessageUtils.getProperty(key, inMessage);
                    }
                }

                key = MessageUtils.format(inMessage, buffer, template);
                if (sub != null && key != null)
                    key = sub.substitute(key);
                if (dateFormat != null)
                    tm = dateFormat.parse(key).getTime();
                else
                    tm = Long.parseLong(key);

                scale = (int) ruleInfo[RULE_OPTION];
                k = (int) metadata[DATA_SIZE];
                m = 0;
                for (i=0; i<k; i++) {
                    key = dataField[i][FIELD_DATA];
                    str = MessageUtils.getProperty(key, inMessage);
                    if (str == null)
                        continue;
                    y = Double.parseDouble(str);
                    if (y == 0.0) // bypass 0.0
                        continue;
                    key = dataField[i][FIELD_DURATION];
                    str = MessageUtils.getProperty(key, inMessage);
                    if (str == null)
                        continue;
                    duration = (long) (Double.parseDouble(str) * scale);
                    mm = spread(tm, in, RESULT_OUT, rid, metadata, duration,
                        y, data[i], dataCount[i]);
                    if (mm > 0) {
                        ruleInfo[RULE_PEND] += mm;
                        ruleInfo[RULE_TIME] = tm;
                        m += mm;
                    }
                    else if (mm < 0)
                        new Event(Event.ERR, name + " " + ruleName +
                            ": failed to spread data point of " + key + ": (" +
                            duration + ", " + y + ")").send();
                }

                i = BYPASS_OUT;
                oid = outLinkMap[i];

                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i +
                        " count=" + m + ":" + metadata[DATA_COUNT]).send();
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
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name+" propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + " i=" + i).send();
                new Event(Event.ERR, str + " failed to escalate msg: "+
                    Event.traceStack(e)).send();
            }

            count += passthru(currentTime, inMessage, in, rid, oid, cid, 0);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /**
     * loads the given number of data set into JMS messages and flushes them
     * to the outLink.  It returns the number of messages flushed.
     */
    private int flush(long currentTime, XQueue in, int oid, int rid,
        int n, long[] metadata){
        int i, j, k, m, cid, delta, head, len, scale, count = 0;
        long tm;
        String[] propertyName, propertyValue;
        String[][] dataField;
        double[][] data;
        int[][] dataCount;
        long[] ruleInfo;
        DateFormat dateFormat;
        Template template;
        Message inMessage;
        Map rule;
        String ruleName, key;
        count = (int) metadata[DATA_COUNT];
        if (count <= 0)
            return 0;

        ruleName = ruleList.getKey(rid);
        ruleInfo = ruleList.getMetaData(rid);
        rule = (Map) ruleList.get(rid);
        propertyName = (String[]) rule.get("PropertyName");
        propertyValue = (String[]) rule.get("PropertyValue");
        data = (double[][]) rule.get("Data");
        dataCount = (int[][]) rule.get("DataCount");
        dataField = (String[][]) rule.get("DataField");
        dateFormat = (DateFormat) rule.get("DateFormat");
        template = (Template) rule.get("Template");
        if (template.numberOfFields() > 0)
            key = template.getAllFields()[0];
        else
            key = template.copyText();

        head = (int) metadata[DATA_HEAD];
        k = (int) metadata[DATA_SIZE];
        len = (int) metadata[DATA_LENGTH];
        tm = metadata[DATA_OTIME];
        delta = (int) metadata[DATA_DELTA];
        scale = (int) ruleInfo[RULE_OPTION];
        count = 0;
        for (j=0; j<n; j++) {
            m = (head + 1 + j) % len;
            cid = -1;
            inMessage = new TextEvent();
            try {
                inMessage.setJMSTimestamp(tm);
                if (dateFormat != null) // set time key
                    MessageUtils.setProperty(key,
                        dateFormat.format(new Date(tm)), inMessage);
                else
                    MessageUtils.setProperty(key, String.valueOf(tm),inMessage);
                for (i=0; i<k; i++) { // set the data
                    MessageUtils.setProperty(dataField[i][FIELD_DATA],
                        String.valueOf(data[i][m] * scale), inMessage);
                    if (dataField[i][FIELD_COUNT] != null)
                        MessageUtils.setProperty(dataField[i][FIELD_COUNT],
                            String.valueOf(dataCount[i][m]), inMessage);
                    data[i][m] = 0.0;
                    dataCount[i][m] = 0;
                }
            }
            catch (Exception e) {
                for (i=0; i<k; i++) { // reset the data
                    data[i][m] = 0.0;
                    dataCount[i][m] = 0;
                }
                new Event(Event.ERR, name + ": " + ruleName +
                    " failed to set data on msg: " + e.toString()).send();
            }

            if (propertyName != null && propertyValue != null) try{
                int ic = -1;
                for (i=0; i<propertyName.length; i++) {
                    if (MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], inMessage) != 0)
                        ic = i;
                }
                if (ic >= 0)
                    new Event(Event.WARNING, name + ": " + ruleName +
                        " failed to set property of " +
                        propertyName[ic]).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to set properties: " +
                    Event.traceStack(e)).send();
            }

            if (displayMask > 0) try {  // display the message
                new Event(Event.INFO, name + ": " + ruleName +
                    " flushed spread msg " + (count + 1) + ":" +
                    MessageUtils.display(inMessage, null, displayMask,
                    displayPropertyName)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to display msg: " + e.toString()).send();
            }

            cid = passthru(currentTime, inMessage, in, rid, oid, -1, 0);
            if (cid > 0) {
                count ++;
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_EXTRA] ++;
                ruleInfo[RULE_TIME] = tm;
            }
            else
                new Event(Event.WARNING, name + ": " + ruleName +
                    " failed to flush the msg at " +
                    Event.dateFormat(new Date(tm))).send();
            tm += delta;
            metadata[DATA_OTIME] = tm;
            metadata[DATA_COUNT] --;
            metadata[DATA_HEAD] ++;
            metadata[DATA_HEAD] %= len;
            metadata[DATA_CTIME] = currentTime + delta;
        }
        if (metadata[DATA_COUNT] <= 0) {
            cacheList.getNextID(rid);
            cacheList.remove(rid);
        }

        return count;
    }

    /**
     * It monitors the cache mtime so that the cached data will be flushed
     * regularly.  It returns total number of msgs flushed
     */
    private int update(long currentTime, XQueue in, int oid) {
        StringBuffer strBuf = null;
        Browser browser;
        long[] metadata;
        Object o;
        long dt;
        int i, rid, n, size, count = 0;
        n = cacheList.size();
        if (n <= 0)
            return 0;
        int[] list = new int[n];

        n = cacheList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            rid = list[i];
            metadata = (long[]) cacheList.browse(rid);
            if (metadata[DATA_COUNT] <= 0) {
                cacheList.getNextID(rid);
                cacheList.remove(rid);
                continue;
            }
            dt = currentTime - metadata[DATA_CTIME];
            if (dt >= 0) { // check expiration
                size = flush(currentTime, in, oid, rid, 1, metadata);
                count += size;
                if ((debug & DEBUG_COLL) > 0)
                    new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                        " flushed " +size+ " msgs due to timeout: "+dt).send();
            }
        }

        return count;
    }

    /** returns the checkpoint map for current state */
    @SuppressWarnings("unchecked")
    public Map<String, Object> checkpoint() {
        Map<String, Object> h, chkpt = new HashMap<String, Object>();
        Map rule;
        List<Object> list, pl;
        double[][] data;
        int[][] dataCount;
        String[][] dataField;
        long[] metadata;
        String[] propertyName, propertyValue;
        String key;
        int i, j, rid, n;
        n = 0;
        if (chkptTimeout <= 0) // checkpointing disabled
            return chkpt;
        Browser browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            metadata = ruleList.getMetaData(rid);
            if (rule == null || metadata[RULE_PID] != TYPE_SPREAD)
                continue;
            metadata = (long[]) rule.get("MetaData");
            n += (int) metadata[DATA_COUNT];
            key = ruleList.getKey(rid);
            data = (double[][]) rule.get("Data");
            dataCount = (int[][]) rule.get("DataCount");
            dataField = (String[][]) rule.get("DataField");
            propertyName = (String[]) rule.get("PropertyName");
            propertyValue = (String[]) rule.get("PropertyValue");
            h = new HashMap<String, Object>();
            chkpt.put(key, h);
            list = new ArrayList<Object>();
            for (i=0; i<metadata.length; i++)
                list.add(String.valueOf(metadata[i]));
            h.put("Metadata", list);
            h = new HashMap<String, Object>();
            for (i=0; i<propertyName.length; i++)
                h.put(propertyName[i], propertyValue[i]);
            rule.put("StringProperty", h);
            list = new ArrayList<Object>();
            for (i=0; i<dataField.length; i++) {
                h = new HashMap<String, Object>();
                key = dataField[i][FIELD_DATA];
                list.add(h);
                pl = new ArrayList<Object>();
                h.put(key, pl);
                for (j=0;j<data[i].length; j++) {
                    h = new HashMap<String, Object>();
                    h.put("Data", String.valueOf(data[i][j]));
                    h.put("Count", String.valueOf(dataCount[i][j]));
                    pl.add(h);
                }
            }
            rule.put("SpreadData", list);
        }
        if (chkpt.size() > 0) { // add name and time for checkpoint
            chkpt.put("Name", name);
            chkpt.put("Total", String.valueOf(n));
            chkpt.put("CheckpointTime",
                String.valueOf(System.currentTimeMillis()));
            new Event(Event.INFO, name + " checkpointed " + n +
                " data points").send();
        }
        return chkpt;
    }

    /** restores the state from checkpoint map */
    @SuppressWarnings("unchecked")
    public void restore(Map<String, Object> chkpt) {
        Map h, rule;
        Object o;
        Browser browser;
        List list, pl;
        double[][] data;
        int[][] dataCount;
        String[][] dataField;
        long[] metadata, ruleInfo;
        String[] propertyName, propertyValue;
        String key;
        long tm;
        int i, j, rid, n;
        if (chkpt == null || chkpt.size() <= 0 || chkptTimeout <= 0)
            return;
        o = chkpt.get("CheckpointTime");
        if (o != null && o instanceof String)
            tm = Long.parseLong((String) o);
        else
            tm = 0;
        if(System.currentTimeMillis() - tm >= chkptTimeout)//checkpoint timedout
            return;
        o = chkpt.get("Total");
        if (o != null && o instanceof String) { // check Total
            if ((i = Integer.parseInt((String) o)) <= 0)
                return;
        }
        n = 0;
        browser = ruleList.browser();
        while ((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            ruleInfo = ruleList.getMetaData(rid);
            if (rule == null || ruleInfo[RULE_PID] != TYPE_SPREAD)
                continue;
            metadata = (long[]) rule.get("MetaData");
            key = ruleList.getKey(rid);
            if ((o = chkpt.get(key)) == null || !(o instanceof Map))
                continue;
            data = (double[][]) rule.get("Data");
            dataCount = (int[][]) rule.get("DataCount");
            dataField = (String[][]) rule.get("DataField");
            propertyName = (String[]) rule.get("PropertyName");
            propertyValue = new String[propertyName.length];
            rule.put("PropertyValue", propertyValue);
            rule = (Map) o;
            if ((o = rule.get("Metadata")) == null || !(o instanceof List))
                continue;
            list = (List) o;
            i = Integer.parseInt((String) list.get(DATA_COUNT));
            if (i <= 0) // no data cached so skip the rule
                continue;
            i = Integer.parseInt((String) list.get(DATA_SIZE));
            if (metadata[DATA_SIZE] != i) // schema changed so skip the rule
                continue;
            i = Integer.parseInt((String) list.get(DATA_LENGTH));
            if (metadata[DATA_LENGTH] != i) // length changed so skip the rule
                continue;
            i = Integer.parseInt((String) list.get(DATA_DELTA));
            if (metadata[DATA_DELTA] != i) // interval changed so skip the rule
                continue;
            if ((o=rule.get("SpreadData")) == null || !(o instanceof List))
                continue;
            for (i=0; i<metadata.length; i++)
                metadata[i] = Long.parseLong((String) list.get(i));
            ruleInfo[RULE_PEND] += metadata[DATA_COUNT];
            n += (int) ruleInfo[RULE_PEND];
            h = (Map) rule.get("StringProperty");
            if (h != null && h.size() > 0) { // restore copied properties
                for (i=0; i<propertyName.length; i++)
                    propertyValue[i] = (String) h.get(propertyName[i]);
            }
            list = (List) o;
            for (i=0; i<dataField.length; i++) {
                if ((o = list.get(i)) == null || !(o instanceof Map))
                    continue;
                h = (Map) o;
                key = dataField[i][FIELD_DATA];
                if ((o = h.get(key)) == null || !(o instanceof List)) 
                    continue;
                pl = (List) o;
                for (j=0; j<data[i].length; j++) {
                    if ((o = pl.get(j)) == null || !(o instanceof Map))
                        continue;
                    h = (Map) o;
                    key = (String) h.get("Data");
                    data[i][j] = Double.parseDouble(key);
                    key = (String) h.get("Count");
                    dataCount[i][j] = Integer.parseInt(key);
                }
            }
            // add the metadata to cacheList
            cacheList.reserve(rid);
            cacheList.add(metadata, rid);
        }
        if (n > 0)
            new Event(Event.INFO, name + " restored " + n +
                " data points from checkpoint at " +
                Event.dateFormat(new Date(tm))).send();
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

    public void close() {
        super.close();
        cacheList.clear();
        templateMap.clear();
        substitutionMap.clear();
        dateFormatMap.clear();
    }

    protected void finalize() {
        close();
    }
}
