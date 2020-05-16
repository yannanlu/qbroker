package org.qbroker.node;

/* ServiceNode.java - a MessageNode providing service on JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.SocketException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QList;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.CollectibleCells;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.MessageStream;
import org.qbroker.node.MessageNode;
import org.qbroker.node.Node;
import org.qbroker.node.NodeUtils;
import org.qbroker.event.Event;

/**
 * ServiceNode listens to an input XQ for JMS ObjectMessages with a connected 
 * socket as a request to establish a connection.  The incoming message is also
 * called a handshake message. It contains the information about the request,
 * the client and the connection.  ServiceNode matches the properties against
 * the predefined rulesets to determine which ruleset to handle the connection
 * request.  ServiceNode extracts the URI from the handshake message and looks
 * it up in the cache for an XQueue as the transmit queue established for the
 * connection.  If there is an XQueue in the cache, it means that the service
 * is ready. Hence ServiceNode will just check out a thread to handle the
 * connection on the XQueue.  Otherwise, ServiceNode will generate a new
 * request with a newly created XQueue for either a new consumer or a new
 * producer to serve on the connection. The request will be sent to the right
 * outlink according to the ruleset.
 *<br><br>
 * ServiceNode has two types of outlinks, position-fixed and non-fixed.
 * There are three position-fixed outlinks: producer for all requests to
 * a ReceiverPool, consumer for all requests to a PersisterPool, nohit for
 * all requests not covered by any rulesets.  The non-fixed outlinks are for
 * the dynamic destinations or sources.
 *<br><br>
 * ServiceNode also contains a number of predefined rulesets.  These rulesets
 * categorize messages into non-overlapping groups.  Therefore, each ruleset
 * defines a unique message group.  The ruleset also specifies the way to
 * construct the URI and properties for the new Persisters or receivers.  For
 * those messages falling off all defined rulesets, ServiceNode always creates
 * an extra ruleset, nohit, to gather stats on them.  Currently, all the nohit
 * messages will not be routed to the nohit outlink.  Instead, they will be
 * removed at once by ServiceNode. The stats of pending messages will be
 * tracked by RULE_PEND for each ruleset.
 *<br><br>
 * If the XQueue does not exist and the ruleset is up to the consumer pool,
 * ServiceNode will create a new ObjectMessage as the request for a new
 * persister thread.  It will also create an XQueue and puts the XQueue and
 * the URI into the request before sending it to consumer outlink.  Once the
 * persister is instantiated, ServiceNode will collect the response from the
 * PersisterPool.  The response contains the persister thread for delivering
 * the messages to the specific destination.  ServiceNode caches the thread
 * and the id of the XQueue using the URI as the key.  Then it starts the
 * proxy thread to pipe all messages from the socket to the XQueue. This
 * way, ServiceNode is able to deliver JMS messages to arbitrary destinations
 * according to their URIs and the predefined rulesets dynamically.
 *<br><br>
 * If the XQueue does not exist and the ruleset is up to the producer pool,
 * ServiceNode will create a new ObjectMessage as the request for a new
 * receiver thread.  It will also create an XQueue and puts the XQueue and
 * the URI into the request before sending it to producer outlink.  Once the
 * receiver is instantiated, ServiceNode will collect the response from the
 * ReceiverPool.  The response contains the receiver thread for picking up
 * the messages from the specific source.  ServiceNode caches the thread
 * and the id of the XQueue using the URI as the key.  Then it starts the
 * proxy thread to pipe all messages from the XQueue to the socket.  This
 * way, ServiceNode is able to pick up JMS messages from arbitrary sources
 * according to their URIs and the predefined rulesets dynamically.
 *<br><br>
 * URI is used to identify destinations or sources.  In order to construct the
 * URI string for an arbitrary destination or source, each ruleset has to
 * define a template via URITemplate.  Optinally, URISubstitution may be
 * defined also for a simple substitution.  With them, ServiceNode will be able
 * to retrieve the URI string from the handshake messages. A ruleset may also
 * contain a hash of DefaultProperty as static or dynamic properties for new
 * persisters or new receivers. If the URI string is not in the cache,
 * ServiceNode will resolve all the dynamic variables in the DefaultProperty
 * from the handshake message first. Then it adds the DefaultProperty to the
 * new ObjectMessage for the either pools to create a new consumer or a new
 * producer.  If any of the operations fails, the handshake message will be
 * removed and the socket will be closed as a failure.
 *<br><br>
 * For each new destination/source, ServiceNode uses the same Object message as
 * the request containing the URI and the XQueue.  The request is sent to either
 * PersisterPool/ReceiverPool via the corresponding outlink.  Then ServiceNode
 * frequently checks the response for each outstanding requests.  The response
 * is supposed to have the status and the persister/receiver thread for the new
 * destination/source.  ServiceNode will use the thread to monitor its status.
 * If the response has no such thread, ServiceNode will remove the incoming
 * message and closes the socket. It will also remove the XQueue and the URI
 * from the cache. The samething will happen if the request for a new persister
 * or receiver times out.  MaxRetry is used to control when to timeout the
 * request on the pool.  It also controls the timeout on a dead persister or
 * receiver thread.
 *<br><br>
 * ServiceNode also maintains an active set of XQueues as the transmit queues
 * for all destinations and sources.  Behind each XQueue, there is at least
 * one persister or receiver thread processing the messages.  The messages
 * may be stuck in the XQueue until they are processed, as long as the persister
 * or receiver is not stopped.  However, if there is no message in the queue,
 * ServiceNode will mark it idle.  All the transmit queues are monitored
 * frequently in sessions.  If one of them has been idle for over MaxIdleTime,
 * its queue will be stopped. Its persister or receiver thread and the transmit
 * queue will be removed from the cache.  The associated socket will be closed
 * too.
 *<br><br>
 * You are free to choose any names for the three fixed outlinks.  But
 * ServiceNode always assumes the first outlink for the producer, the second
 * for consumer and the third is for nohit. The rest of the outlinks are
 * on-demand outlinks for dynamic destinations or sources. The name of the
 * first outlink has to be unique on the node.
 *<br><br>
 * Currently, there is no support for NOHIT outlink yet.  But it may be added
 * in the future to support existing static XQueues.  If a ruleset is nohit
 * and the XQueue does not exist, ServiceNode will send an ObjectMessage as
 * the request to the nohit outlink.  The nohit outlink should always point to
 * the escalation queue.  So the manager of the apps should reply with an
 * static XQueue with partition info.  Then ServiceNode starts the proxy
 * thread to handle the connection.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ServiceNode extends Node {
    private int maxRetry = 2;
    private long delta, pauseTime = 6000L, standbyTime = 60000L;
    private int maxConnection, heartbeat;

    private QList pendList = null;     // list of msgs pending for establishment
    private ThreadPool thPool;
    private boolean takebackEnabled = false;

    private final static int ASSET_REQ = ASSET_THR + 1;
    private final static int OP_NONE = 0;
    private final static int OP_ACQUIRE = 1;
    private final static int OP_RESPOND = 2;
    private final static int OP_PROVIDE = 3;
    private final static int OP_PUBLISH = 4;
    private final static int FAILURE_OUT = -1;
    private final static int PRODUCER_OUT = 0;
    private final static int CONSUMER_OUT = 1;
    private final static int NOHIT_OUT = 2;

    public ServiceNode(Map props) {
        super(props);
        Object o;
        List list;
        long tm;
        int i, j, k, n, id, ruleSize = 512;
        String key, saxParser = null;
        Browser browser;
        Map<String, Object> rule;
        long[] outInfo, ruleInfo;
        StringBuffer strBuf = new StringBuffer();

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "proxy";
        if ((o = props.get("MaxNumberRule")) != null)
            ruleSize = Integer.parseInt((String) o);
        if (ruleSize <= 0)
            ruleSize = 512;
        if ((o = props.get("MaxNumberConnection")) != null)
            maxConnection = Integer.parseInt((String) o);
        else
            maxConnection = 256;
        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);
        if (maxRetry <= 0)
            maxRetry = 2;
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        else
            heartbeat = 60000;
        if (heartbeat <= 0)
            heartbeat = 60000;

        delta = waitTime / maxRetry;
        if (delta <= 0)
            delta = 10L;

        if ((o = props.get("TakeBackEnabled")) != null &&
            "true".equalsIgnoreCase((String) o))
            takebackEnabled = true;

        if ((o = props.get("OutLink")) == null || !(o instanceof List))
            throw(new IllegalArgumentException(name +
                ": OutLink is not well defined"));

        list = (List) o;

        tm = System.currentTimeMillis();

        assetList = NodeUtils.initFixedOutLinks(tm, capacity, maxConnection,
            new int[0], name, list);

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

        i = NOHIT_OUT;
        if (++i > assetList.size())
            throw(new IllegalArgumentException(name+": missing some OutLinks"));

        msgList = new AssetList(name, capacity);
        ruleList = new AssetList(name, ruleSize);
        pendList = new QList(name, capacity);
        thPool = new ThreadPool(name+"/proxy", 1, maxConnection, this, "proxy",
            new Class[]{Map.class});
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
            throw(new IllegalArgumentException(name + ": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        if ((debug & DEBUG_INIT) > 0) {
            browser = ruleList.browser();
            while ((i = browser.next()) >= 0) {
                ruleInfo = ruleList.getMetaData(i);
                strBuf.append("\n\t" + ruleList.getKey(i) + ": " + i + " " +
                    ruleInfo[RULE_PID] + " " + ruleInfo[RULE_OPTION] + " " +
                    ruleInfo[RULE_TTL]/1000 + " " + ruleInfo[RULE_MODE] + " " +
                    ruleInfo[RULE_GID] + " " + ruleInfo[RULE_EXTRA] +
                    " - " + assetList.getKey((int) ruleInfo[RULE_OID]));
            }
            new Event(Event.DEBUG, name +
                " RuleName: RID PID OPTION TTL MODE Retry Client - OutName"+
                strBuf.toString()).send();
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
        if ((o = props.get("MaxRetry")) != null) {
            i = Integer.parseInt((String) o);
            if (i > 0 && i != maxRetry) {
                maxRetry = i;
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
        Iterator iter;
        List list;
        String key, str, ruleName;
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

        hmap = new HashMap<String, Object>();
        hmap.put("Name", ruleName);
        if ((o = ph.get("JMSPropertyGroup")) != null)
            hmap.put("JMSPropertyGroup", o);
        if ((o = ph.get("XJMSPropertyGroup")) != null)
            hmap.put("XJMSPropertyGroup", o);
        rule.put("Filter", new MessageFilter(hmap));
        hmap.clear();
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_STATUS] = NODE_RUNNING;
        ruleInfo[RULE_TIME] = tm;

        if ((o = ph.get("DisplayMask")) != null && o instanceof String) {
            ruleInfo[RULE_DMASK] = Integer.parseInt((String) o);
            rule.put("DisplayMask", o);
        }
        else
            ruleInfo[RULE_DMASK] = displayMask;

        // store Capacity into RULE_OPTION field
        if ((o = ph.get("Capacity")) != null)
            ruleInfo[RULE_OPTION] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_OPTION] = capacity;

        // store MaxIdleTime into RULE_TTL field
        if ((o = ph.get("MaxIdleTime")) != null)
            ruleInfo[RULE_TTL] = 1000 * Integer.parseInt((String) o);
        else
            ruleInfo[RULE_TTL] = 0;

        // store MaxRetry into RULE_GID field
        if ((o = ph.get("MaxRetry")) != null)
            ruleInfo[RULE_GID] = Integer.parseInt((String) o);
        else
            ruleInfo[RULE_GID] = maxRetry;
        if (ruleInfo[RULE_GID] <= 0)
            ruleInfo[RULE_GID] = maxRetry;

        if ((o = ph.get("Operation")) != null)
            str = (String) o;
        else
            str = "";

        // store Operation into RULE_MODE field
        if ("acquire".equals(str)) {
            ruleInfo[RULE_MODE] = OP_ACQUIRE;
            ruleInfo[RULE_OID] = CONSUMER_OUT;
        }
        else if ("respond".equals(str)) {
            ruleInfo[RULE_MODE] = OP_RESPOND;
            ruleInfo[RULE_OID] = CONSUMER_OUT;
        }
        else if ("provide".equals(str)) {
            ruleInfo[RULE_MODE] = OP_PROVIDE;
            ruleInfo[RULE_OID] = PRODUCER_OUT;
        }
        else if ("publish".equals(str)) {
            ruleInfo[RULE_MODE] = OP_PUBLISH;
            ruleInfo[RULE_OID] = PRODUCER_OUT;
        }
        else {
            ruleInfo[RULE_MODE] = OP_NONE;
            ruleInfo[RULE_OID] = NOHIT_OUT;
        }
        ruleInfo[RULE_PID] = TYPE_SERVICE;

        // for max number of clients sharing the same xq
        if ((o = ph.get("MaxClient")) != null &&
            Integer.parseInt((String) o) > 0)
            k = Integer.parseInt((String) o);
        else
            k = (int) ruleInfo[RULE_OPTION];
        // store MaxClient into RULE_EXTRA field
        ruleInfo[RULE_EXTRA] = k;

        if (ruleInfo[RULE_MODE] == OP_PUBLISH) {
            k = 1;
            if ((o = ph.get("MaxPublisher")) != null)
                k = Integer.parseInt((String) o);
            if (k <= 0)
                k = 1;
            rule.put("Publisher", new ThreadPool(name+"/"+ruleName, 0, k, this,
                "doPublish",  new Class[]{XQueue.class, QList.class,
                Template.class}));

            // for publishing template
            if((o = ph.get("TemplateFile")) != null && ((String) o).length() >0)
                rule.put("Template", new Template(new File((String) o)));
            else if ((o = ph.get("Template")) != null &&
                ((String) o).length() > 0)
                rule.put("Template", new Template((String) o));
        }
        else
            rule.put("MessageStream", new MessageStream(ph));


        if ((o = ph.get("URITemplate")) != null && o instanceof String) {
            rule.put("URITemplate", new Template((String) o));
            if ((o = ph.get("URISubstitution")) != null && o instanceof String)
                rule.put("URISubstitution",new TextSubstitution((String)o));
        }
        else
            throw(new IllegalArgumentException(name + " " + ruleName +
                ": URITemplate not well defined"));

        // default properties
        if ((o = ph.get("DefaultProperty")) != null && o instanceof Map) {
            Template temp = new Template(JSON2Map.toJSON((Map) o));
            if (temp.size() <= 0) // not a template
                rule.put("DefaultProperty", Utils.cloneProperties((Map) o));
            else
                rule.put("DefaultProperty", temp);
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
     * It removes the rule from the ruleList and returns the rule id upon
     * success. It is not MT-Safe.
     */
    public int removeRule(String key, XQueue in) {
        int id = ruleList.getID(key);
        if (id == 0) // can not remove the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long[] ruleInfo = ruleList.getMetaData(id);
            if (ruleInfo != null && ruleInfo[RULE_SIZE] > 0) // check integrity
                throw(new IllegalStateException(name+": "+key+" is busy with "+
                    ruleInfo[RULE_SIZE] + " outstangding msgs"));
            Map h = (Map) ruleList.remove(id);
            if (h != null) {
                ThreadPool pool = (ThreadPool) h.remove("Publisher");
                if (pool != null)
                    pool.close();
                MessageFilter filter = (MessageFilter) h.remove("Filter");
                if (filter != null)
                    filter.clear();
                h.clear();
            }
            return id;
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.removeRule(key, in);
        }
        return -1;
    }

    /**
     * It replaces the existing rule of the key and returns its id upon success.
     * It is not MT-Safe.
     */
    public int replaceRule(String key, Map ph, XQueue in) {
        int id = ruleList.getID(key);
        if (id == 0) // can not replace the default rule
            return -1;
        else if (id > 0) { // for a normal rule
            if (ph == null || ph.size() <= 0)
                throw(new IllegalArgumentException("Empty property for rule"));
            if (!key.equals((String) ph.get("Name"))) {
                new Event(Event.ERR, name + ": name not match for rule " + key +
                    ": " + (String) ph.get("Name")).send();
                return -1;
            }
            if (getStatus() == NODE_RUNNING)
                throw(new IllegalStateException(name + " is in running state"));
            long tm = System.currentTimeMillis();
            long[] meta = new long[RULE_TIME+1];
            long[] ruleInfo = ruleList.getMetaData(id);
            Map rule = initRuleset(tm, ph, meta);
            if (rule != null && rule.containsKey("Name")) {
                StringBuffer strBuf = ((debug & DEBUG_DIFF) <= 0) ? null :
                    new StringBuffer();
                Map h = (Map) ruleList.set(id, rule);
                if (h != null) {
                    ThreadPool pool = (ThreadPool) h.remove("Publisher");
                    if (pool != null)
                        pool.close();
                    MessageFilter filter = (MessageFilter) h.remove("Filter");
                    if (filter != null)
                        filter.clear();
                    h.clear();
                }
                for (int i=0; i<RULE_TIME; i++) { // update metadata
                    switch (i) {
                      case RULE_SIZE:
                        break;
                      default:
                        ruleInfo[i] = meta[i];
                    }
                    if ((debug & DEBUG_DIFF) > 0)
                        strBuf.append(" " + ruleInfo[i]);
                }
                if ((debug & DEBUG_DIFF) > 0)
                    new Event(Event.DEBUG, name + "/" + key + " ruleInfo:" +
                        strBuf).send();
                return id;
            }
            else
                new Event(Event.ERR, name + " failed to init rule "+key).send();
        }
        else if (cfgList != null && cfgList.containsKey(key)) {
            return super.replaceRule(key, ph, in);
        }
        return -1;
    }

    /**
     * picks up a message from input queue and evaluates its content to
     * decide which output queue to propagate
     */
    @SuppressWarnings("unchecked")
    public void propagate(XQueue in, XQueue[] out) throws JMSException {
        Message inMessage = null;
        String msgStr = null, uriStr, ip, ruleName = null;
        Socket sock = null;
        Object[] asset;
        Object o;
        Map rule = null, client;
        XQueue pool;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null;
        MessageStream ms = null;
        Template template = null;
        TextSubstitution sub = null;
        long currentTime, previousTime;
        long[] outInfo, ruleInfo = null;
        int[] ruleMap;
        long count = 0;
        int mask, ii, sz;
        int i = 0, min = 0, n, previousRid;
        int cid = -1; // the cell id of the message in input queue
        int rid = 0; // the id of the ruleset
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
        if (n > NOHIT_OUT) for (i=0; i<=NOHIT_OUT; i++) {
            asset = (Object[]) assetList.get(i);
            asset[ASSET_XQ] = out[i];
            outInfo = assetList.getMetaData(i);
            if (outInfo[OUT_CAPACITY] != out[i].getCapacity())
                outInfo[OUT_CAPACITY] = out[i].getCapacity();
        }

        n = ruleMap.length;
        currentTime = System.currentTimeMillis();
        previousTime = currentTime;
        previousRid = -1;
        ii = 0;
        sz = msgList.size();
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((cid = in.getNextCell(waitTime)) < 0) {
                if (++ii >= 10) {
                    feedback(in, -1L);
                    sz =  msgList.size();
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
                monitor(currentTime, in, assetList);
                previousTime = currentTime;
            }
            if (cid < 0)
                continue;

            if ((inMessage = (Message) in.browse(cid)) == null) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": null msg from " +
                    in.getName()).send();
                continue;
            }
            if (!(inMessage instanceof ObjectMessage)) {
                in.remove(cid);
                new Event(Event.WARNING, name + ": not an object msg from " +
                    in.getName()).send();
                continue;
            }
            client = (Map) ((ObjectMessage) inMessage).getObject();
            if (client != null) {
                sock = (Socket) client.get("Socket");
                ip = (String) client.get("ClientIP");
            }
            else {
                in.remove(cid);
                new Event(Event.WARNING, name + ": bad msg object").send();
                continue;
            }

            if (sock == null) { // socket closed so clean up the request
                in.remove(cid);
                outInfo = (long[]) client.get("MetaData");
                ip = (String) client.get("ClientIP");
                client.clear();
                cleanup(cid, (int) outInfo[MSG_RID], (int) outInfo[MSG_OID]);
                new Event(Event.INFO, name + ": closed socket " + cid +
                    " for client " + ip).send();
                continue;
            }

            filter = null;
            rid = 0;
            i = 0;
            try {
                for (i=1; i<n; i++) {
                    if (filters[i].evaluate(inMessage, null)) {
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
                template = (Template) rule.get("URITemplate");
                sub = (TextSubstitution) rule.get("URISubstitution");
                if (ruleInfo[RULE_MODE] != OP_PUBLISH)
                    ms = (MessageStream) rule.get("MessageStream");
                previousRid = rid;
            }

            uriStr = MessageUtils.format(inMessage, buffer, template);
            if (sub != null)
                uriStr = sub.substitute(uriStr);

            oid = (int) ruleInfo[RULE_OID];
            new Event(Event.DEBUG, name + ": " + ruleName +
                " got a msg for "+ oid + ": " + uriStr).send();
            if (i < 0) { // failed to apply filters
                oid = FAILURE_OUT;
                filter = null;
            }
            else if(uriStr == null || uriStr.length() <= 0 || oid == NOHIT_OUT){
                oid = FAILURE_OUT;
                filter = null;
            }
            else if ((oid = assetList.getID(uriStr)) >= 0) { // existing queue
                int tid;
                asset = (Object[]) assetList.get(oid);
                if (asset == null || asset.length <= ASSET_REQ) {
                    new Event(Event.ERR, name + ": " + ruleName +
                        " got a bad asset of "+ oid + ": " + uriStr).send();
                    oid = FAILURE_OUT;
                }
                else if (asset[ASSET_THR] != null) { // thread is ready
                    Thread thr;
                    long[] state;
                    state = new long [] {cid, oid, 0, rid, 0, currentTime};
                    if (ruleInfo[RULE_MODE] == OP_PUBLISH) { // add sub
                        QList subList = (QList) asset[ASSET_REQ];
                        int k = reserveSubscriber(waitTime, subList);
                        if (k < 0) { // failed to reserve on subList
                            new Event(Event.ERR, name + ": " + ruleName +
                                " failed to reserve on sublist of "+ oid + ": "+
                                uriStr).send();
                            oid = FAILURE_OUT;
                        }
                        else {
                            addSubscriber(new IndexedXQueue(uriStr + "_" + k,
                                (int) ruleInfo[RULE_OPTION]), k, subList);
                            client.put("XQueue", subList.browse(k));
                            state[MSG_BID] = k;
                        }
                    }
                    else {
                        client.put("XQueue", asset[ASSET_XQ]);
                        client.put("MessageStream", ms);
                    }
                    if (oid != FAILURE_OUT &&
                        (thr = thPool.checkout(5*waitTime)) != null) { //success
                        tid = thPool.getId(thr);
                        state[MSG_TID] = tid;
                        client.put("MetaData", state);
                        client.put("InLink", in);
                        thPool.assign(new Object[] {client}, tid);
                        ruleInfo[RULE_SIZE] ++;
                        ruleInfo[RULE_TIME] = currentTime;
                        outInfo = assetList.getMetaData(oid);
                        outInfo[OUT_TIME] = currentTime;
                        // increase number of clients to OUT_QIPPS
                        outInfo[OUT_QIPPS] ++;
                        if (displayMask > 0) try { // display the message
                            new Event(Event.INFO,name + ": " + ruleName +
                                ((ruleInfo[RULE_MODE] == OP_PUBLISH) ?
                                " added msg "+ (count + 1) + " to sub list:" :
                                " attached msg "+ (count + 1) + " to xq:") +
                                MessageUtils.display(inMessage, uriStr,
                                displayMask, displayPropertyName)).send();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName +
                                " failed to display msg: "+e.toString()).send();
                        }
                        continue;
                    }
                    else if (oid != FAILURE_OUT) { //failed to checkout a thread
                        new Event(Event.ERR, name + ": " + ruleName +
                            " failed to checkout a thread for "+ oid + ": " +
                            uriStr).send();
                        oid = FAILURE_OUT;
                    }
                    // failed
                }
                else { // thread not ready yet so add client to pending list
                    // add the cid of the client to pending list
                    i = pendList.reserve(cid);
                    if (i >= 0) { // added
                        pendList.add(uriStr, cid);
                        ruleInfo[RULE_PEND] ++;
                        ruleInfo[RULE_SIZE] ++;
                        ruleInfo[RULE_TIME] = currentTime;
                        outInfo = assetList.getMetaData(oid);
                        outInfo[OUT_TIME] = currentTime;
                        // increase number of pendings to OUT_SIZE
                        outInfo[OUT_SIZE] ++;
                        if (displayMask > 0) try { // display the message
                            new Event(Event.INFO,name + ": " + ruleName +
                                " added msg " + (count + 1) +" to pendng list:"+
                                MessageUtils.display(inMessage, uriStr,
                                displayMask, displayPropertyName)).send();
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, name + ": " + ruleName +
                                " failed to display msg: "+e.toString()).send();
                        }
                        continue;
                    }
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to reserve on sublist of "+ oid + ": " +
                        uriStr).send();
                    oid = FAILURE_OUT;
                }
            }
            else if ((oid = assetList.add(uriStr, new long[OUT_QTIME + 1],
                new Object[ASSET_REQ + 1])) >= 0) { // new asset for 1st request
                outInfo = assetList.getMetaData(oid);
                for (i=0; i<=OUT_QTIME; i++)
                    outInfo[i] = 0;
                outInfo[OUT_TIME] = currentTime;
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_READY;
                outInfo[OUT_CAPACITY] = ruleInfo[RULE_OPTION];
                // store the default MaxIdleTime into OUT_ORULE
                outInfo[OUT_ORULE] = ruleInfo[RULE_TTL];
                // store rid into OUT_MODE
                outInfo[OUT_MODE] = rid;
                // store cid into OUT_EXTRA
                outInfo[OUT_EXTRA] = cid;
                asset = (Object[]) assetList.get(oid);
                // store socket in URI field and remove socket from request
                asset[ASSET_URI] = client.remove("Socket");
                asset[ASSET_THR] = null;
                asset[ASSET_REQ] = null;
                XQueue xq = new IndexedXQueue(ruleName + "_" + oid,
                    (int) outInfo[OUT_CAPACITY]);
                asset[ASSET_XQ] = xq;
                client.put("XQueue", xq);

                if ((o = rule.get("DefaultProperty")) == null)
                    ; // doing nothing
                else if (o instanceof Map) // default properties
                    client.put("Properties", (Map) o);
                else if (o instanceof Template) try { // template
                    String str;
                    str = MessageUtils.format(inMessage, buffer, (Template) o);
                    StringReader sin = new StringReader(str);
                    o = JSON2Map.parse(sin);
                    client.put("Properties", (Map) o);
                    sin.close();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name +": " + ruleName +
                        " failed to rebuild properties for " + uriStr +
                        ": " + Event.traceStack(e)).send();
                }
                if ((debug & DEBUG_INIT) > 0 &&
                    (o = client.get("Properties")) != null)
                    new Event(Event.DEBUG, name + ": " + ruleName +
                        " created the client for " + uriStr +
                        ": " + JSON2Map.toJSON((Map) o)).send();

                ruleInfo[RULE_PEND] ++;
                ruleInfo[RULE_TIME] = currentTime;
                // increase number of pendings to OUT_SIZE
                outInfo[OUT_SIZE] ++;
                // add the cid of the client to pending list
                i = pendList.reserve(cid);
                pendList.add(uriStr, cid);
                // reset the URI
                ((ObjectEvent) inMessage).setAttribute("URI", uriStr);
            }
            if (oid < 0) { // failure or nohit so close socket now
                socketClose(sock);
                client.clear();
                in.remove(cid);
                // update stats for NOHIT
                outInfo = assetList.getMetaData(NOHIT_OUT);
                outInfo[OUT_COUNT] ++;
                outInfo[OUT_TIME] = currentTime;
                ruleInfo = ruleList.getMetaData(0);
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = currentTime;
                if ((debug & DEBUG_PROP) > 0)
                    new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                        " rid=" + rid + " oid=" + oid + "/" + assetList.size() +
                        ((uriStr != null) ? ": " + uriStr : "")).send();
                if (ruleInfo[RULE_PID] == TYPE_SERVICE)
                    new Event(Event.ERR, name + ": " + ruleName +
                        " failed to add a new asset for " + uriStr).send();
                continue;
            }

            // get OID for the request
            i = (int) ruleInfo[RULE_OID];
            if ((debug & DEBUG_PROP) > 0)
                new Event(Event.DEBUG, name + " propagate: cid=" + cid +
                    " rid=" + rid + " oid=" + i + "/" + assetList.size() +
                    ((uriStr != null) ? ": " + uriStr : "")).send();

            if (ruleInfo[RULE_DMASK] > 0) try { // display the message
                new Event(Event.INFO,name + ": " + ruleName + " delivered msg "+
                    (count + 1) + ":" + MessageUtils.display(inMessage, uriStr,
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

            count += passthru(currentTime, inMessage, in, rid, i, cid, oid);
            feedback(in, -1L);
            sz = msgList.size();
            inMessage = null;
        }
    }

    /** cleans up the list and assetList */
    private int cleanup(int cid, int rid, int oid) {
        String key;
        XQueue xq = null;
        List list;
        Object[] asset = null;
        long[] outInfo, ruleInfo;
        ruleInfo = ruleList.getMetaData(rid);
        outInfo = assetList.getMetaData(oid);
        asset = (Object[]) assetList.get(oid);
        if (outInfo != null)
            outInfo[OUT_QIPPS] --;
        ruleInfo[RULE_SIZE] --;
        ruleInfo[RULE_COUNT] ++;
        if (asset == null) // asset is gone
            return 0;
        else if (ruleInfo[RULE_MODE] == OP_PUBLISH) { // update QIPPS 
            int n = 0;
            QList subList = null;
            subList = (QList) asset[ASSET_REQ];
            if (subList != null) {
                subList.getNextID(cid);
                xq = (XQueue) subList.remove(cid);
                n = subList.size();
            }
            if (xq != null) {
                MessageUtils.stopRunning(xq);
                xq.clear();
            }
            if (outInfo != null)
                outInfo[OUT_QIPPS] = n;
            if (n <= 0) { // no subscribers left, clean up publisher xq
                xq = (XQueue) asset[ASSET_XQ];
                if (xq != null) {
                    MessageUtils.stopRunning(xq);
                    xq.clear();
                }
                asset[ASSET_XQ] = null;
                asset[ASSET_URI] = null;
                asset[ASSET_THR] = null;
                asset[ASSET_REQ] = null;
                assetList.remove(oid);
            }
            return n;
        }
        else { // for non-subscribers
            asset[ASSET_XQ] = null;
            asset[ASSET_URI] = null;
            asset[ASSET_THR] = null;
            asset[ASSET_REQ] = null;
            assetList.remove(oid);
            return 0;
        } 
    }

    /**
     * It continuously reads byte stream from the socket and package
     * them into JMS Messages.  It adds the messages to an XQueue as
     * output.  The messages flow downstream and they will be consumed
     * eventually.  It acts like a proxy for JMS messages.
     */
    @SuppressWarnings("unchecked")
    public void proxy(Map client) {
        QList list = null; 
        CollectibleCells rcells = null;
        DelimitedBuffer sBuf = null;
        MessageStream ms = (MessageStream) client.get("MessageStream");
        Socket sock = (Socket) client.get("Socket");
        XQueue xq = (XQueue) client.get("XQueue");
        XQueue in = (XQueue) client.get("InLink");
        InputStream ins = null;
        long[] state = (long[]) client.get("MetaData");
        long[] ruleInfo;
        byte[] buff = new byte[5];
        String ip, linkName;
        int cid, sid, tid, oid, mode, rid;
        int i = 0, n = 0;
        cid = (int) state[MSG_CID];
        rid = (int) state[MSG_RID];
        tid = (int) state[MSG_TID];
        oid = (int) state[MSG_OID];
        sid = (int) state[MSG_BID];
        ruleInfo = ruleList.getMetaData(rid);
        mode = (int) ruleInfo[RULE_MODE];
        ip = sock.getInetAddress().getHostAddress();
        client.put("ClientIP", ip);
        linkName = xq.getName();

        if (mode == OP_RESPOND) {
            list = new QList(String.valueOf(cid), capacity);
            rcells = new CollectibleCells(String.valueOf(cid), capacity);
            try {
                sBuf = new DelimitedBuffer(bufferSize, ms.getOffhead(),
                    ms.getSotBytes(), ms.getOfftail(), ms.getEotBytes());
            }
            catch (Exception e) {
                socketClose(sock);
                new Event(Event.ERR, linkName + " failed to create buffer: "+
                    Event.traceStack(e)).send();
                return;
            }
            catch (Error e) {
                socketClose(sock);
                new Event(Event.ERR, linkName + " failed to create buffer: " +
                    Event.traceStack(e)).send();
                Event.flush(e);
            }
        }
        else if (mode == OP_PUBLISH) {
            Object[] asset = (Object[]) assetList.get(oid);
            list = (QList) asset[ASSET_REQ];
        }

        try {
            switch (mode) {
              case OP_ACQUIRE:
                ms.read(sock.getInputStream(), xq);
                break;
              case OP_RESPOND:
                ms.respond(sock.getInputStream(), xq, sock.getOutputStream(),
                    list, sBuf, rcells);
                break;
              case OP_PROVIDE:
                while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
                    i = 0;
                    try {
                        ms.write(xq, sock.getOutputStream());
                    }
                    catch (TimeoutException ex) { // idle for too long
                        i = -1;
                        if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, linkName + " for " + ip +
                                ": " + ex.getMessage()).send();
                    }
                    if (i < 0 && xq.size() <= 0) try { // test socket via read
                        sock.setSoTimeout(1000);
                        ins = sock.getInputStream();
                        i = 0;
                        if (ins != null) try {
                            i = ins.read(buff, 0, 1);
                        }
                        catch (InterruptedIOException ee) {
                        }
                        if (i < 0) { // client has shutdown socket
                            new Event(Event.INFO, linkName+" socket has been "+
                                "shutdown from " + ip).send();
                            break;
                        }
                    }
                    catch (Exception ex) {
                        new Event(Event.WARNING, linkName + " failed to test "+
                            " socket from " + ip + ": " + ex.toString()).send();
                        break;
                    }
                }
                break;
              case OP_PUBLISH:
                while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
                    i = 0;
                    try {
                        write(xq, sock.getOutputStream(), ip,
                            (int) ruleInfo[RULE_TTL]);
                    }
                    catch (TimeoutException ex) { // idle for too long
                        i = -1;
                        if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, linkName + " for " + ip +
                                ": " + ex.getMessage()).send();
                    }
                    if (i < 0 && xq.size() <= 0) try { // test socket via read
                        sock.setSoTimeout(1000);
                        ins = sock.getInputStream();
                        i = 0;
                        if (ins != null) try {
                            i = ins.read(buff, 0, 1);
                        }
                        catch (InterruptedIOException ee) {
                        }
                        if (i < 0) { // client has shutdown socket
                            new Event(Event.INFO, linkName+" socket has been "+
                                "shutdown from " + ip).send();
                            break;
                        }
                    }
                    catch (Exception ex) {
                        new Event(Event.WARNING, linkName + " failed to test "+
                            "socket from " + ip + ": " + ex.toString()).send();
                        break;
                    }
                }
                break;
              default:
                break;
            }
        }
        catch (SocketException e) {
            new Event(Event.WARNING, linkName + " " + name +
                " failed to proxy: " + Event.traceStack(e)).send();
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + name +
                " failed to proxy: " + Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName + " " + name + " failed to proxy: ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + name +
                " failed in to proxy: " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            MessageUtils.stopRunning(xq);
            socketClose(sock);
            xq.clear();
            state[MSG_CID] = -1;
            // remove the socket from the request for notification
            client.remove("Socket");
            // notify the primary thread to remove the request at cid
            in.putback(cid);
            if (mode == OP_RESPOND) {
                list.clear();
                rcells.clear();
                sBuf.reset();
            }
            else if (mode == OP_PUBLISH) {
                removeSubscriber(sid, list);
            }
            new Event(Event.ERR, linkName + " " + name +
                " failed in to proxy: " + Event.traceStack(e)).send();
            Event.flush(e);
        }

        MessageUtils.stopRunning(xq);
        socketClose(sock);
        xq.clear();
        i = 0;
        if (mode == OP_RESPOND) {
            list.clear();
            rcells.clear();
            sBuf.reset();
        }
        else if (mode == OP_PUBLISH) {
            removeSubscriber(sid, list);
            i = list.size();
        }
        state[MSG_CID] = -1;
        // remove the socket from the request for notification
        client.remove("Socket");
        // notify the primary thread to remove the request at cid
        in.putback(cid);
        n = thPool.getCount() - 1;
        if (mode == OP_PUBLISH) { // for publisher
            new Event(Event.INFO, name + ": " + linkName +
                " disconnected from client "+ cid + " at " + ip + " with " +
                i + " subscribers and " + n + " clients left").send();
        }
        else { // for non-publisher
            new Event(Event.INFO, name + ": " + linkName +
                " disconnected from client "+ cid + " at " + ip + " with " +
                n + " clients left").send();
        }
    }

    /** It keeps publishing until its end of life */
    public void doPublish(XQueue xq, QList subList, Template template) {
        Browser browser;
        int count = 0, id;
        String linkName = xq.getName();

        for (;;) {
            while (keepRunning(xq)) { // publish session
                try {
                    count += publish(xq, subList, template);
                }
                catch (Exception e) {
                    new Event(Event.ERR, linkName + " " + name + ": " +
                        Event.traceStack(e)).send();
                }
                catch (Error e) {
                    MessageUtils.stopRunning(xq);
                    browser = subList.browser();
                    while ((id = browser.next()) >= 0) {
                        XQueue q = (XQueue) subList.browse(id); 
                        if (q != null)
                            MessageUtils.stopRunning(q);
                    }
                    new Event(Event.ERR, linkName + " " + name + ": " +
                        Event.traceStack(e)).send();
                    Event.flush(e);
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                try {
                    Thread.sleep(pauseTime);
                }
                catch (InterruptedException e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                try {
                    Thread.sleep(standbyTime);
                }
                catch (InterruptedException e) {
                }
            }
            if (isStopped(xq))
                break;
        }
        MessageUtils.stopRunning(xq);
        browser = subList.browser();
        while ((id = browser.next()) >= 0) {
            XQueue q = (XQueue) subList.browse(id); 
            if (q != null)
                MessageUtils.stopRunning(q);
        }

        new Event(Event.INFO, name+": publisher stopped for "+linkName).send();
    }

    /**
     * It gets JMS Messages from the input XQueue and publishes the content
     * of messages to all the XQueues for active subscribers continuously.
     * At the other end of each output XQueue, a thread will pick up the
     * message content and write it to the socket.  It returns number of
     * messages published.
     */
    private int publish(XQueue xq, QList subList, Template template) {
        Message message;
        Object[] asset;
        QList retryList;
        int k, n, sid, count = 0;
        int[] oids;
        String dt, msgStr;
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        long currentTime;
        byte[] buffer = new byte[bufferSize];

        asset = (Object[]) assetList.get(subList.getName());
        if (asset == null || asset.length <= ASSET_REQ ||
            subList == null || subList.size() <= 0) {
            MessageUtils.stopRunning(xq);
            return 0;
        }
        retryList = new QList(Thread.currentThread().getName(),
            subList.getCapacity());

        k = 0;
        currentTime = System.currentTimeMillis();
        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                continue;
            }

            message = (Message) xq.browse(sid);
            if (message == null) { // msg is supposed not to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            dt = null;
            try {
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt = Event.dateFormat(new Date(message.getJMSTimestamp()));
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to get JMS property for "+
                    name + ": " + Event.traceStack(e)).send();
            }

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(message, buffer, template);
                else
                    msgStr = MessageUtils.processBody(message, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to format msg: " +
                    Event.traceStack(e)).send();
                if (acked) try {
                    message.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to format msg: " +
                    Event.traceStack(e)).send();
                if (acked) try {
                    message.acknowledge();
                }
                catch (Exception ex) {
                }
                Event.flush(e);
            }

            if (acked) try {
                message.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg: " +
                    str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg: " +
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg: " +
                    e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);

            oids = (int[]) asset[ASSET_URI];
            if (oids == null || oids.length <= 0) // all sockets closed?
                break;
            n = oids.length;
            k = assign(maxRetry, msgStr, oids, retryList, subList);
            count ++;
        }
        if (displayMask != 0)
            new Event(Event.INFO, name+": published " + count + " msgs").send();
        return count;
    }

    /**
     * It assigns the msgStr into all active outLinks defined in oids and
     * returns number of outLinks assigned to.
     */
    private int assign(int retry, String msgStr, int[] oids,
        QList retryList, QList subList) {
        XQueue xq;
        int[] list;
        int i, k = 0, n, oid, sid;
        if (oids == null || msgStr == null || msgStr.length() <= 0)
            return -1;
        if ((n = oids.length) <= 0)
            return k;
        retryList.clear();
        for (i=0; i<n; i++) {
            oid = oids[i];
            xq = (XQueue) subList.browse(oid);
            if (xq == null) {
                continue;
            }
            if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                continue;
            sid = xq.reserve(-1L);
            if (sid >= 0) {
                xq.add(msgStr, sid);
                k ++;
                continue;
            }
            // save the xq for retry
            retryList.reserve(oid);
            retryList.add(xq, oid);
        }
        n = retryList.depth();
        if (n <= 0) // no retry needed
            return k;
        list = new int[n];
        for (int j=1; j<=retry; j++) { // for retries
            n = retryList.queryIDs(list, XQueue.CELL_OCCUPIED);
            for (i=0; i<n; i++) {
                oid = list[i];
                xq = (XQueue) retryList.browse(oid);
                if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0) {
                    retryList.getNextID(oid);
                    retryList.remove(oid);
                    continue;
                }
                sid = xq.reserve(j*delta);
                if (sid >= 0) {
                    xq.add(msgStr, sid);
                    retryList.getNextID(oid);
                    retryList.remove(oid);
                    k ++;
                    continue;
                }
            }
            n = retryList.depth();
            if (n <= 0)
                return k;
        }
        n = retryList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            oid = list[i];
            xq = (XQueue) retryList.browse(oid);
            if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0) {
                retryList.getNextID(oid);
                retryList.remove(oid);
                continue;
            }
            sid = xq.reserve(waitTime);
            if (sid >= 0) {
                xq.add(msgStr, sid);
                k ++;
            }
            else {
                sid = xq.getNextCell(5L);
                xq.remove(sid);
                new Event(Event.WARNING, xq.getName() + ": overran at " + sid +
                    " with " + xq.depth() + "/" + xq.getCapacity()).send();
                sid = xq.reserve(5L, sid);
                xq.add(msgStr, sid);
                k ++;
            }
            retryList.getNextID(oid);
            retryList.remove(oid);
        }
        return k;
    }

    /**
     * It gets a String from the input XQueue and write all bytes to the
     * OutputStream continuously.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like write,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * If maxIdleTime > 0 and it is idle for too long, TimeoutException will
     * be thrown; 
     *<br><br>
     * This method is MT-Safe.
     */
    private void write(XQueue xq, OutputStream out, String u, int maxIdleTime)
        throws IOException, TimeoutException {
        int count = 0;
        int sid = -1;
        long idleTime, currentTime;
        String msgStr = null;
        boolean checkIdle = (maxIdleTime > 0);

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            for (int i=0; i<10; i++) {
                if ((sid = xq.getNextCell(waitTime)) >= 0)
                    break;
            }
            if (sid < 0) {
                if (checkIdle) {
                    if (currentTime - idleTime >= maxIdleTime)
                        throw(new TimeoutException("idle too long"));
                    else
                        currentTime = System.currentTimeMillis();
                }
                continue;
            }
            idleTime = currentTime;

            msgStr = (String) xq.browse(sid);
            if (msgStr == null) { // empty payload
                xq.remove(sid);
                new Event(Event.WARNING, "dropped an empty payload from " +
                    xq.getName()).send();
                continue;
            }

            if (msgStr.length() > 0) try {
                out.write(msgStr.getBytes());
                count ++;
            }
            catch (IOException e) {
                xq.remove(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.remove(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.remove(sid);
                Event.flush(e);
            }

            xq.remove(sid);
        }
        if (displayMask != 0)
            new Event(Event.INFO, xq.getName() + ": wrote " + count +
                " msgs to " + u).send();
    }

    /**
     * monitors all output destinations by checking their transmit queue depth
     * and the status of the persister threads, and updates their state info.
     * If any queue is stuck for too long or its thread is not RUNNING,
     * it will take actions to migrate all the stuck messages to the failure
     * outLink and remove the destination.  It returns the total number of
     * removed persisters.
     */
    private int monitor(long currentTime, XQueue in, AssetList assetList) {
        StringBuffer strBuf = null;
        String uriStr, key, str;
        XQueue xq;
        Thread thr;
        Object[] asset;
        long[] outInfo, ruleInfo;
        long st;
        boolean isPersister = false;
        int i, k, l, n, mask, oid, mode, retry, rid, maxIdleTime;
        int NOT_STOPPED = XQueue.KEEP_RUNNING|XQueue.STANDBY|XQueue.PAUSE;

        n = assetList.size();
        if (n <= NOHIT_OUT + 1)
            return 0;

        int[] list = new int[n];
        n = assetList.queryIDs(list);

        if ((debug & DEBUG_UPDT) > 0)
            strBuf = new StringBuffer();

        k = 0;
        for (i=0; i<n; i++) {
            oid = list[i];
            if (oid <= NOHIT_OUT) // leave fixed outLinks alone
                continue;
            asset = (Object[]) assetList.get(oid);
            if (asset == null || asset.length <= ASSET_REQ)
                continue;
            uriStr = assetList.getKey(oid);
            xq = (XQueue) asset[ASSET_XQ];
            thr = (Thread) asset[ASSET_THR];
            outInfo = assetList.getMetaData(oid);
            rid = (int) outInfo[OUT_MODE];
            ruleInfo = ruleList.getMetaData(rid);
            // retrieve rid from RULE_MODE
            mode = (int) ruleInfo[RULE_MODE];
            // retrieve maxRetry from RULE_GID
            retry = (int) ruleInfo[RULE_GID];
            str = getServiceType(mode) + " ";
            if (thr == null) { // obj is not instantiated yet
                if ((int) outInfo[OUT_NRULE] < retry) {
                    outInfo[OUT_NRULE] ++;
                    continue;
                }
                else { // pending for too long so stop all clients on oid
                    // retrieve cid from OUT_EXTRA
                    int cid = (int) outInfo[OUT_EXTRA];
                    new Event(Event.WARNING, name + ": " + str + oid +
                        " is not instantiated yet on " + xq.getName() +
                        " with " + outInfo[OUT_SIZE] + " " + xq.depth() + ":" +
                        xq.size() + " " + msgList.size()).send();
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_STOPPED;
                    k = stopPendingClients(currentTime, in, cid, rid, oid);
                    continue;
                }
            }
            else if (!thr.isAlive()) { // thread is not running
                new Event(Event.WARNING, name + ": thread of " + str +
                    oid + " is stopped on " + uriStr + " with " +
                    outInfo[OUT_SIZE] + " " + xq.depth() + ":" +
                    xq.size() + " " + msgList.size()).send();
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_STOPPED;
                if (ruleInfo[RULE_OID] != NOHIT_OUT) // for dynamic xq
                    MessageUtils.stopRunning(xq);
            }
            outInfo[OUT_NRULE] = 0;
            outInfo[OUT_DEQ] = xq.reset();
            outInfo[OUT_QDEPTH] = xq.size();
            // increase total into OUT_COUNT
            outInfo[OUT_COUNT] += outInfo[OUT_DEQ];

            isPersister = (mode == OP_RESPOND || mode == OP_ACQUIRE);
            mask = xq.getGlobalMask();
            if ((mask & NOT_STOPPED) == 0) { // xq stopped
                new Event(Event.INFO, name + ": " + str + oid +
                    " is removed due to the stopped XQ " + xq.getName() +
                    " and gone with " + xq.size() + " msgs").send();
                if (ruleInfo[RULE_OID] != NOHIT_OUT) // for dynamic xq
                    xq.clear();
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_CLOSED;
                outInfo[OUT_DEQ] = 0;
                k ++;
                continue;
            }
            else if ((mask & XQueue.STANDBY) > 0) {
                if ((int) outInfo[OUT_STATUS] != NODE_STANDBY) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_STANDBY;
                }
                continue;
            }
            else if ((mask & XQueue.PAUSE) > 0) {
                if ((int) outInfo[OUT_STATUS] != NODE_PAUSE) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_PAUSE;
                }
                continue;
            }

            // retrieve MaxIdleTime from OUT_ORULE
            maxIdleTime = (int) outInfo[OUT_ORULE];
            switch ((int) outInfo[OUT_STATUS]) {
              case NODE_DISABLED:
                if (outInfo[OUT_QDEPTH] > 0L || outInfo[OUT_DEQ] > 0L) {
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                    outInfo[OUT_QTIME] = currentTime;
                }
                else { // expired
                    new Event(Event.INFO, name + ": " + str + oid +
                        " is removed due to its expiration and " +
                        "gone with " + xq.size() + " msgs").send();
                    if (ruleInfo[RULE_OID] != NOHIT_OUT) { // for dynamic xq
                        MessageUtils.stopRunning(xq);
                        xq.clear();
                    }
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_CLOSED;
                    k ++;
                }
                break;
              case NODE_STOPPED:
                outInfo[OUT_STATUS] = NODE_RUNNING;
                outInfo[OUT_QTIME] = currentTime;
                break;
              case NODE_RUNNING:
                if (outInfo[OUT_DEQ] > 0L) { // flowing OK
                    outInfo[OUT_QTIME] = currentTime;
                    break;
                }

                // stuck or idle
                st = currentTime - outInfo[OUT_QTIME];
                if (isPersister) { // for persister
                    if (outInfo[OUT_QDEPTH] > 0L) { // stuck
                        if (st >= heartbeat) {
                            outInfo[OUT_QTIME] = currentTime;
                            outInfo[OUT_STATUS] = NODE_RETRYING;
                        }
                    }
                    else if (maxIdleTime > 0 && st >= maxIdleTime) { // idle
                        outInfo[OUT_QTIME] = currentTime;
                        outInfo[OUT_STATUS] = NODE_DISABLED;
                        if ((debug & DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name + ": " + str + oid +
                                " is disabled with " + outInfo[OUT_QDEPTH]+" "+
                                xq.depth() + ":" + xq.size()).send();
                    }
                }
                else if (outInfo[OUT_QDEPTH] > 0L) { // stuck for receiver
                    if (st >= heartbeat) {
                        outInfo[OUT_QTIME] = currentTime;
                        outInfo[OUT_STATUS] = NODE_RETRYING;
                    }
                }
                break;
              case NODE_RETRYING:
                if (outInfo[OUT_DEQ] > 0L) {
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                }
                else if (isPersister && outInfo[OUT_QDEPTH] == 0) { // for pstr
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                }
                else if (!isPersister && outInfo[OUT_QDEPTH] > 0) { // for rcvr
                    outInfo[OUT_QTIME] = currentTime;
                    outInfo[OUT_STATUS] = NODE_RUNNING;
                }
                break;
              case NODE_PAUSE:
              case NODE_STANDBY:
                outInfo[OUT_QTIME] = currentTime;
                outInfo[OUT_STATUS] = NODE_RUNNING;
                break;
              case NODE_READY:
              default:
                break;
            }
        }

        return k;
    }

    /**
     * stops the all pending clients and removes their requests from the input
     * XQueue.
     */
    private int stopPendingClients(long tm, XQueue in, int cid, int rid,
        int oid) {
        ObjectMessage msg;
        Object[] asset = null;
        Object o;
        XQueue xq;
        Socket sock;
        Map client;
        String uriStr, str;
        long[] outInfo, ruleInfo;
        int mid, i, n = 0;
        if (oid <= NOHIT_OUT) // leave fixed outLinks alone
            return 0;
        asset = (Object[]) assetList.get(oid);
        if (asset == null || asset.length <= ASSET_REQ)
            return 0;
        uriStr = assetList.getKey(oid);
        xq = (XQueue) asset[ASSET_XQ];
        outInfo = assetList.getMetaData(oid);
        ruleInfo = ruleList.getMetaData(rid);
        o = asset[ASSET_URI];
        if (o != null && o instanceof Socket) { // client is still pending
            int[] list;
            // retrieve cid from OUT_EXTRA
            cid = (int) outInfo[OUT_EXTRA];
            sock = (Socket) o;
            socketClose(sock);
            msg = (ObjectMessage) in.browse(cid);
            in.remove(cid);
            msgList.remove(cid);
            ruleInfo[RULE_PEND] --;
            ruleInfo[RULE_SIZE] --;
            ruleInfo[RULE_COUNT] ++;
            ruleInfo[RULE_TIME] = tm;
            try {
                client = (Map) msg.getObject();
                client.clear();
            }
            catch (Exception e) {
            }
            n = pendList.size();
            list = new int[n];
            n = pendList.queryIDs(list, XQueue.CELL_OCCUPIED);
            for (i=0; i<n; i++) {
                mid = list[i];
                str = (String) pendList.browse(mid);
                if (!uriStr.equals(str))
                    continue;
                pendList.takeback(mid);
                if (mid == cid)
                    continue;
                msg = (ObjectMessage) in.browse(mid);
                in.remove(mid);
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_SIZE] --;
                ruleInfo[RULE_COUNT] ++;
                ruleInfo[RULE_TIME] = tm;
                if (msg == null)
                    continue;
                else try {
                    client = (Map) msg.getObject();
                }
                catch (Exception e) {
                    client = null;
                }
                if (client == null)
                    continue;
                sock = (Socket) client.get("Socket");
                socketClose(sock);
                client.clear();
            }
            assetList.remove(oid);
            str = getServiceType((int) ruleInfo[RULE_MODE]) + " ";
            new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                " removed " + str + oid + " due to pending for too long on " +
                xq.getName() + " and gone with " + n + " requests").send();
            if (ruleInfo[RULE_OID] != NOHIT_OUT) {
                MessageUtils.stopRunning(xq);
                xq.clear();
            }
            asset[ASSET_XQ] = null;
            asset[ASSET_URI] = null;
            asset[ASSET_THR] = null;
            asset[ASSET_REQ] = null;
        }
        return n;
    }

    /** returns the string of the service type */
    private static String getServiceType(int mode) {
        String str;
        switch (mode) {
          case OP_ACQUIRE:
          case OP_RESPOND:
            str = "receiver";
            break;
          case OP_PROVIDE:
          case OP_PUBLISH:
            str = "persister";
            break;
          default:
            str = "object";
            break;
        }
        return str;
    }

    /** removes the id-th XQueue from the subList and rebuilds oidList */
    private synchronized XQueue removeSubscriber(int id, QList subList) {
        Object[] asset;
        XQueue xq;
        int n;
        int[] oidList;
        subList.getNextID(id);
        xq = (XQueue) subList.browse(id);
        subList.remove(id);
        n = subList.depth();
        oidList = new int[n];
        n = subList.queryIDs(oidList, XQueue.CELL_OCCUPIED);
        asset = (Object[]) assetList.get(subList.getName());
        asset[ASSET_URI] = oidList;
        notifyAll();
        return xq;
    }

    /** reserves a cell on the subList and returns the id */
    private synchronized int reserveSubscriber(long milliSec, QList subList) {
        int i = subList.reserve();
        if (i < 0) {
            try {
                wait(milliSec);
            }
            catch (Exception e) {
            }
            i = subList.reserve();
        }
        return i;
    }

    /** adds a subscriber to the subList and rebuilds oidList */
    private synchronized int addSubscriber(XQueue xq, int id, QList subList) {
        Object[] asset;
        int n;
        int[] oidList;
        n = subList.add(xq, id);
        if (n < 0)
            return -1;
        n = subList.depth();
        oidList = new int[n];
        n = subList.queryIDs(oidList, XQueue.CELL_OCCUPIED);
        asset = (Object[]) assetList.get(subList.getName());
        asset[ASSET_URI] = oidList;
        return n;
    }

    /**
     * collects the persister/receiver thread of mid from out and returns
     * number of threads started upon success or 0 otherwise.
     */
    @SuppressWarnings("unchecked")
    private int collect(long tm, XQueue in, int mid, XQueue out) {
        ObjectMessage msg;
        Map client, rule;
        QList subList = null;
        Object[] asset;
        Object o;
        XQueue xq;
        String uriStr, str;
        StringBuffer strBuf = null;
        long[] state, outInfo, ruleInfo;
        int i, id, cid, oid, pid, rid, tid, n, k = 0, maxClient = 1;
        if (mid < 0 || mid >= capacity) // bad arguments
            return 0;
        state = (long[]) msgList.getMetaData(mid);
        if (state == null || (pid = (int) state[MSG_OID]) >= NOHIT_OUT)
            return 0;
        id = (int) state[MSG_BID];
        rid = (int) state[MSG_RID];
        cid = (int) state[MSG_CID];
        oid = (int) state[MSG_TID];

        uriStr = (String) msgList.get(mid);
        msgList.remove(mid);

        // update stats for pid
        state = assetList.getMetaData(pid);
        state[OUT_SIZE] --;
        state[OUT_COUNT] ++;
        state[OUT_TIME] = tm;
        ruleInfo = ruleList.getMetaData(rid);
        ruleInfo[RULE_PEND] --;
        ruleInfo[RULE_TIME] = tm;

        msg = (ObjectMessage) in.browse(mid);
        outInfo = assetList.getMetaData(oid);
        // update pending size for oid
        outInfo[OUT_SIZE] --;
        if (msg == null) // asset already trashed
            return 0;

        if ((debug & DEBUG_COLL) > 0)
            strBuf = new StringBuffer();
        rule = (Map) ruleList.get(rid);
        asset = (Object[]) assetList.get(oid);
        xq = (XQueue) asset[ASSET_XQ];

        maxClient = (int) ruleInfo[RULE_EXTRA];

        if ((debug & DEBUG_COLL) > 0)
            new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                " collecting " + pid + " " + oid + " " + rid + " " +
                mid + ": " + uriStr).send();

        client = null;
        if (msg != null) try {
            client = (Map) msg.getObject();
        }
        catch (Exception e) {
            client = new HashMap();
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " collected empty msg for " + uriStr).send();
        }
        if (client.containsKey("MaxIdleTime") && // overwrite the default
            (ruleInfo[RULE_MODE] == OP_RESPOND ||
            ruleInfo[RULE_MODE] == OP_ACQUIRE))
            outInfo[OUT_ORULE] =
                1000 * Integer.parseInt((String) client.get("MaxIdleTime"));

        asset[ASSET_THR] = client.remove("Thread");
        if (asset[ASSET_THR] != null) { // start proxy thread on the 1st msg
            Thread thr = thPool.checkout(waitTime);
            tid = thPool.getId(thr);
            // put socket back to request
            client.put("Socket", asset[ASSET_URI]);
            asset[ASSET_URI] = null;
            MessageUtils.resumeRunning(xq);
            outInfo[OUT_STATUS] = NODE_RUNNING;
            outInfo[OUT_QTIME] = tm;
            outInfo[OUT_TIME] = tm;
            outInfo[OUT_QIPPS] ++;
            state = new long[] {mid, oid, 0, rid, tid, tm};
            client.put("MetaData", state);
            client.put("InLink", in);
            if (ruleInfo[RULE_MODE] == OP_PUBLISH) { // replace xq
                subList = new QList(uriStr, maxClient);
                asset[ASSET_REQ] = subList;
                k = subList.reserve();
                subList.add(new IndexedXQueue(uriStr + "_" + k,
                    xq.getCapacity()), k);
                client.put("XQueue", subList.browse(k));
                state[MSG_BID] = k;
                asset[ASSET_URI] = new int[]{k};
            }
            else if (ruleInfo[RULE_MODE] == OP_RESPOND) {
                client.put("XQueue", xq);
                client.put("MessageStream", rule.get("MessageStream"));
            }
            else {
                client.put("XQueue", xq);
                client.put("MessageStream", rule.get("MessageStream"));
            }
            thPool.assign(new Object[] {client}, tid);
            if ((debug & DEBUG_COLL) > 0) {
                str = getServiceType((int) ruleInfo[RULE_MODE]) + " ";
                new Event(Event.DEBUG, name + ": " + ruleList.getKey(rid) +
                    " collected " + str + " of "+ oid + " for "+ uriStr).send();
            }
        }
        else { // failed to establish consumer or producer
            String ip = socketClose((Socket) asset[ASSET_URI]);
            new Event(Event.INFO, name + ": closed socket " + mid + " on " +
                ip).send();
            asset[ASSET_URI] = null;
            client.clear();
            in.remove(cid);
            // reset cid to -1 on OUT_EXTRA
            outInfo[OUT_EXTRA] = -1;
            str = getServiceType((int) ruleInfo[RULE_MODE]) + " ";
            new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                " failed to collect " + str + " for "+ uriStr).send();
            if (outInfo[OUT_SIZE] <= 0) { // no one is in pending state
                outInfo[OUT_STATUS] = NODE_STOPPED;
                outInfo[OUT_TIME] = tm;
                asset[ASSET_XQ] = null;
                assetList.remove(oid);
                MessageUtils.stopRunning(xq);
                xq.clear();
                return 1;
            }
        }

        // process pending list
        n = pendList.size();
        int[] list = new int[n];
        n = pendList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            tid = list[i];
            str = (String) pendList.browse(tid);
            if (!uriStr.equals(str))
                continue;
            msg = (ObjectMessage) in.browse(tid);
            try {
                client = (Map) msg.getObject();
            }
            catch (Exception e) {
                continue;
            }
            if (asset[ASSET_THR] == null) { // retry on the 1st pending msg
                // reset the URI
                ((ObjectEvent) msg).setAttribute("URI", uriStr);
                asset[ASSET_URI] = client.remove("Socket");
                client.put("XQueue", xq);
                // reset cid to that of next pending msg on OUT_EXTRA
                outInfo[OUT_EXTRA] = tid;
                k = msgList.add(pid+"/"+id, new long[]{tid, pid, id, rid, oid,
                    tm}, pendList.takeback(tid), tid);
                out.add(msg, id);
                state = assetList.getMetaData(pid);
                state[OUT_SIZE] ++;
                state[OUT_TIME] = tm;
                return 0;
            }
            else if (client != null) { // start thread for pending msg
                Thread thr;
                thr = thPool.checkout(waitTime);
                tid = thPool.getId(thr);
                state = new long[] {id, oid, 0, rid, tid, tm};
                client.put("MetaData", state);
                client.put("InLink", in);
                pendList.takeback(id);
                if (ruleInfo[RULE_MODE] == OP_PUBLISH) { // replace xq
                    k = subList.reserve();
                    subList.add(new IndexedXQueue(uriStr + "_" + k,
                        xq.getCapacity()), k);
                    client.put("XQueue", subList.browse(k));
                    state[MSG_BID] = k;
                }
                else if (ruleInfo[RULE_MODE] == OP_RESPOND) {
                    client.put("XQueue", xq);
                    client.put("MessageStream", rule.get("MessageStream"));
                    client.put("RequestList", asset[ASSET_REQ]);
                }
                else {
                    client.put("XQueue", xq);
                    client.put("MessageStream", rule.get("MessageStream"));
                }
                outInfo[OUT_SIZE] --;
                outInfo[OUT_QIPPS] ++;
                ruleInfo[RULE_PEND] --;
                ruleInfo[RULE_TIME] = tm;
                thPool.assign(new Object[] {client}, tid);
                if ((debug & DEBUG_COLL) > 0)
                    strBuf.append("\n\t" + id + " " + rid + " " + tid + " " +
                        outInfo[OUT_STATUS] + " " + (outInfo[OUT_ORULE]/1000) +
                        " " + uriStr);
            }
        }

        if (ruleInfo[RULE_MODE] == OP_PUBLISH) { // start up publisher
            n = subList.size();
            if (n > 1) { // update oid list
                list = new int[n];
                n = subList.queryIDs(list, XQueue.CELL_OCCUPIED);
                asset[ASSET_URI] = list;
            }
            ThreadPool publisher = (ThreadPool) rule.get("Publisher");
            Template template = (Template) rule.get("Template");
            Thread pub = publisher.checkout(waitTime);
            if (pub != null) {
                id = publisher.getId(pub);
                subList = (QList) asset[ASSET_REQ];
                publisher.assign(new Object[] {xq, subList, template}, id);
                new Event(Event.INFO, name + ": " + ruleList.getKey(rid) +
                    " started the publisher thread " + pub.getName() + " " +
                    publisher.getCount() + "/" + publisher.getSize()).send();
            }
            else {
                new Event(Event.ERR, name + ": " + ruleList.getKey(rid) +
                    " failed to spawn the publisher thread " +
                    publisher.getCount() + "/" + publisher.getSize()).send();
                MessageUtils.stopRunning(xq);
                Browser browser = subList.browser();
                while ((id = browser.next()) >= 0) {
                    XQueue q = (XQueue) subList.browse(id); 
                    if (q != null)
                        MessageUtils.stopRunning(q);
                }
            }
        }
        return 1;
    }

    /**
     * passes the message from an input XQ over to an output XQ as the
     * request for a new persister or receiver.  The last argument, tid, is
     * the outLink id of the new persister or receiver.
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
            new Event(Event.ERR, name+": asset is null on " + oid + "/" +
                rid + " for msg at " + cid).send();
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
            for (k=0; k<1000; k++) { // reserve any empty cell
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

        if (id >= 0 && id < outCapacity) { // id-th cell of out is reserved
            String key = oid + "/" + id;
            mid = msgList.getID(key);
            if (mid < 0) { // id-th cell was empty before, add new entry to it
                mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                    currentTime}, pendList.takeback(cid), cid);
            }
            else { // id-th cell is just empty, collect it
                cells.collect(-1L, mid);
                k = collect(currentTime, in, mid, out);
                if (k > 0) {
                    mid = msgList.add(key, new long[]{cid, oid, id, rid, tid,
                        currentTime}, pendList.takeback(cid), cid);
                }
                else { // failed, need more work here!!
                    mid = -1;
                    pendList.takeback(cid);
                }
            }
            if (mid < 0) { // failed to add state info to msgList
                out.cancel(id);
                k = in.putback(cid);
                assetList.remove(tid);
                new Event(Event.ERR, name + ": failed to add to MSGLIST at " +
                    mid + ":" + cid + " with " + key + " for " +
                    ruleList.getKey(rid) + ": " + msgList.size()).send();
                return 0;
            }
            k = out.add(msg, id, cbw);
            outInfo[OUT_SIZE] ++;
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
            pendList.takeback(cid);
            assetList.remove(tid);
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
     * returns number of done messages removed from in
     */
    protected int feedback(XQueue in, long milliSec) {
        Object[] asset;
        XQueue out;
        int i, k, n, mid, rid, tid, oid, id, l = 0;
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
            outInfo = assetList.getMetaData(oid);
            rid = (int) state[MSG_RID];
            k = collect(t, in, mid, out);
            ruleInfo = ruleList.getMetaData(rid);
            if ((debug & DEBUG_FBAK) > 0)
                strBuf.append("\n\t" + rid + " " + mid + "/" + state[MSG_CID] +
                    " " + oid + ":" + id + " " + ruleInfo[RULE_SIZE] + " " +
                    outInfo[OUT_SIZE]+ " " + out.size() + "|" + out.depth() +
                    " " + msgList.size() + ", " + k);
            l ++;
            if (milliSec >= 0) // one collection a time
                break;
        }
        if (l > 0 && (debug & DEBUG_FBAK) > 0)
            new Event(Event.DEBUG, name + " feedback: RID MID/CID OID:ID " +
                "RS OS size|depth ms - " + l + " msgs fed back to " +
                in.getName() + " with " + in.size() + ":" + in.depth() +
                strBuf.toString()).send();

        return l;
    }

    /**
     * It marks the cell in cells with key and id.
     */
    public void callback(String key, String id) {
        int mid;
        if (key == null || id == null || key.length() <= 0 || id.length() <= 0)
            return;
        int oid = assetList.getID(key);
        if (oid < 0 && key.matches("[-\\w]+_\\d+")) { // dynamic outlink
            if ((oid = key.lastIndexOf("_")) > 0)
                mid = msgList.getID(key.substring(oid+1) + "/" + id);
            else
                mid = -1;
        }
        else
            mid = msgList.getID(oid + "/" + id);
        if (mid >= 0) { // mark the mid is ready to be collected
            cells.take(mid);
            if ((debug & DEBUG_FBAK) > 0)
                new Event(Event.DEBUG, name +": "+ key + " called back on "+
                    mid + " with " + oid + "/" + id).send();
        }
        else
            new Event(Event.ERR, name +": "+ key+" failed to callback on "+
                oid + ":" + id).send();
    }

    private String socketClose(Socket sock) {
        String ip = null;
        if (sock != null) {
            ip = sock.getInetAddress().getHostAddress();
            try {
                OutputStream os = sock.getOutputStream();
                if (os != null)
                    os.flush();
            }
            catch (Exception e) {
            }
            try {
                sock.shutdownInput();
            }
            catch (Exception e) {
            }
            try {
                sock.shutdownOutput();
            }
            catch (Exception e) {
            }
            try {
                sock.close();
            }
            catch (Exception e) {
            }
            sock = null;
        }
        return ip;
    }

    private boolean keepRunning(XQueue xq) {
        return ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
    }

    private boolean isStopped(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        return (!((xq.getGlobalMask() & mask) > 0));
    }

    /**
     * stops all dynamic XQs and cleans up associated assets and pstrs or recvs
     */
    private void stopAll() {
        int i, k, n;
        Object[] asset;
        XQueue xq;
        int[] list;
        n = assetList.size();
        list = new int[n];
        n = assetList.queryIDs(list);
        for (i=0; i<n; i++) {
            k = list[i];
            asset = (Object[]) assetList.get(k);
            if (asset == null || asset.length <= ASSET_REQ)
                continue;
            xq = (XQueue) asset[ASSET_XQ];
            if (xq != null) {
                MessageUtils.stopRunning(xq);
                xq.clear();
            }
            assetList.remove(k);
            asset[ASSET_XQ] = null;
            asset[ASSET_URI] = null;
            asset[ASSET_THR] = null;
            asset[ASSET_REQ] = null;
        }
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
        int i, j, k, n, mid, oid, id;

        feedback(in, -1L);
        n = msgList.size();
        list = new int[n];
        n = msgList.queryIDs(list);
        for (i=0; i<n; i++) {
            mid = list[i];
            state = msgList.getMetaData(mid);
            oid = (int) state[MSG_OID];
            if (oid > NOHIT_OUT) // only reset fixed oids
               continue;
            id = (int) state[MSG_BID];
            asset = (Object[]) assetList.get(oid);
            if (asset == null || asset.length <= ASSET_REQ) {
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
            state[OUT_QIPPS] = 0;
            state[OUT_SIZE] = 0;
            state[OUT_DEQ] = 0;
        }

        browser = ruleList.browser();
        while ((i = browser.next()) >= 0) {
            state = ruleList.getMetaData(i);
            state[RULE_SIZE] = 0;
        }
        stopAll();
    }

    /** always returns true since it supports dynamic XQs */
    public boolean internalXQSupported() {
        return true;
    }

    protected boolean takebackEnabled() {
        return takebackEnabled;
    }

    public void close() {
        Map rule;
        Browser browser;
        ThreadPool pool;
        int rid;
        stopAll();
        thPool.close();
        pendList.clear();
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null) {
                pool = (ThreadPool) rule.get("Publisher");
                if (pool != null)
                    pool.close();
            }
        }
        super.close();
    }

    protected void finalize() {
        close();
    }
}
