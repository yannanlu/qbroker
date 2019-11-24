package org.qbroker.persister;

/* MessageTerminator.java - a persister terminating JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DataSet;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * MessageTerminator is similar to MessageEvaluator. It also contains a bunch
 * of rulesets for evaluating messages. But which rule to invoked is determined
 * by the match process rather than the value of the given property. Besides
 * modifying the messages, such as setting their return code, a ruleset may
 * acknowledge messages according to their cell ids. So MessageTerminator is
 * used to terminate message channels.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageTerminator extends Persister {
    private String msgID = null;
    private int bufferSize = 4096;
    private int debug = 0;
    private int displayMask = 0;
    private int maxMsgLength = 4194304;
    private long sessionTime;

    private String rcField, orcField;
    private AssetList ruleList;

    private int retryCount;
    private final static int SUCCESS = 0;
    private final static int FAILURE = 1;
    private final static int EXCEPTION = -1;
    private final static int RULE_TIME = 12;

    public MessageTerminator(Map props) {
        super(props);
        Object o;
        List list;
        Map<String, Object> rule;
        long[] ruleInfo;
        String key;
        long tm = System.currentTimeMillis();
        int i = 0, n, ruleSize;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        if (operation == null)
            operation = "evaluate";
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberRules")) != null)
            ruleSize = Integer.parseInt((String) o);
        else
            ruleSize = 512;

        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("OriginalRC")) != null && o instanceof String)
            orcField = (String) o;
        else
            orcField = "OriginalRC";

        ruleList = new AssetList(uri, ruleSize);

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
            ruleInfo[RULE_TIME] = tm;
            rule = new HashMap<String, Object>();
            rule.put("Name", key);
            ruleList.add(key, ruleInfo, rule);

            for (i=0; i<n; i++) { // for defined rules
                o = list.get(i);
                if (o instanceof String) {
                    o = props.get((String) o);
                    if (o == null || !(o instanceof Map)) {
                        new Event(Event.ERR, uri + ": ruleset " + i + ", " +
                            (String)list.get(i)+", is not well defined").send();
                        continue;
                    }
                }
                ruleInfo = new long[RULE_TIME+1];
                rule = initRuleset(tm, (Map) o, ruleInfo);
                if (rule != null && (key = (String) rule.get("Name")) != null) {
                    if (ruleList.add(key, ruleInfo, rule) < 0) // new rule added
                        new Event(Event.ERR, uri + ": ruleset " + i + ", " +
                            key + ", failed to be added").send();
                }
                else
                    new Event(Event.ERR, uri + ": ruleset " + i +
                        " failed to be initialized").send();
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri + ": failed to init rule "+
                i + ": " + Event.traceStack(e)));
        }

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    /**
     * It initializes a new ruleset with the ruleInfo and returns the rule upon
     * success.  Otherwise, it throws an exception or returns null.
     */
    protected Map<String, Object> initRuleset(long tm, Map ph, long[] ruleInfo){
        Object o;
        Map<String, Object> rule;
        Iterator iter;
        String key, ruleName;
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

        rule.put("Filter", new MessageFilter(ph));
        if ((o = rule.get("Filter")) == null)
            throw(new IllegalArgumentException(ruleName +
                ": Filter is not well defined"));

        for (i=0; i<=RULE_TIME; i++)
            ruleInfo[i] = 0;

        ruleInfo[RULE_TIME] = tm;

        // for String properties
        if ((o = ph.get("StringProperty")) != null && o instanceof Map) {
            iter = ((Map) o).keySet().iterator();
            k = ((Map) o).size();
            String[] pn = new String[k];
            String[] pv = new String[k];
            k = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if ((pn[k] = MessageUtils.getPropertyID(key)) == null)
                    pn[k] = key;
                pv[k] = (String) ((Map) o).get(key);
                k ++;
            }
            rule.put("PropertyName", pn);
            rule.put("PropertyValue", pv);
        }

        return rule;
    }

    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(PSTR_READY, PSTR_RUNNING);

        for (;;) {
            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                terminate(xq);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == PSTR_PAUSE) {
                if (status > PSTR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > PSTR_PAUSE)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                status == PSTR_STANDBY) {
                if (status > PSTR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > PSTR_STANDBY)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
            if (status == PSTR_READY) {
                setStatus(PSTR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * It picks up a message from input queue and evaluates its content to
     * decide what return code to set.  The evaluation may modify the content.
     */
    public void terminate(XQueue in) {
        Message outMessage;
        Map rule = null;
        Browser browser;
        MessageFilter[] filters = null;
        MessageFilter filter = null;
        String[] propertyName = null, propertyValue = null;
        String key, str, msgStr = null, ruleName = null;
        int[] ruleMap;
        long currentTime;
        long count = 0;
        long[] ruleInfo;
        Object o = null;
        int i = 0, n, rid, mask, option, dmask, previousRid;
        int sid = -1; // the cell id of the message in input queue
        String okRC = String.valueOf(MessageUtils.RC_OK);
        byte[] buffer = new byte[bufferSize];
        boolean ckBody = false;

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

        previousRid = -1;
        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = in.getNextCell(waitTime)) < 0) {
                continue;
            }

            if ((outMessage = (Message) in.browse(sid)) == null) {
                in.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    in.getName()).send();
                continue;
            }

            currentTime = System.currentTimeMillis();
            msgStr = null;
            filter = null;
            rid = 0;
            i = 0;
            try {
                if (ckBody)
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                for (i=1; i<n; i++) {
                    if (filters[i].evaluate(outMessage, msgStr)) {
                        rid = ruleMap[i];
                        filter = filters[i];
                        break;
                    }
                }
            }
            catch (Exception e) {
                str = uri;
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
                propertyValue = (String[]) rule.get("PropertyValue");
                previousRid = rid;
            }

            str = null;
            // copy the original RC and set the default RC
            try {
                str = MessageUtils.getProperty(rcField, outMessage);
                MessageUtils.setProperty(orcField, str, outMessage);
                MessageUtils.setProperty(rcField, okRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(orcField, str, outMessage);
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                catch (Exception ex) {
                    in.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+in.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                in.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    in.getName()).send();
                outMessage = null;
                continue;
            }

            if (propertyName != null && propertyValue != null) try {
                int ic = -1;
                for (i=0; i<propertyName.length; i++) {
                    if (MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], outMessage) != 0)
                        ic = i;
                }
                if (ic >= 0)
                    new Event(Event.WARNING, uri + ": " + ruleName +
                        " failed to set property of "+ propertyName[ic]).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, uri + ": " + ruleName +
                    " failed to set properties: " + Event.traceStack(e)).send();
            }

            if (displayMask > 0) try { // display the message
                if((displayMask & MessageUtils.SHOW_BODY) > 0 && msgStr == null)
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                new Event(Event.INFO, uri +": "+ ruleName + " terminated msg "+
                    (count+1) + ":" + MessageUtils.display(outMessage, msgStr,
                    displayMask, null)).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, uri + ": " + ruleName +
                    " failed to display msg: " +e.toString()).send();
            }
            count ++;

            in.remove(sid);
            outMessage = null;
        }
    }

    public String getRuleName(int i) {
        return ruleList.getKey(i);
    }

    public int getNumberOfRules() {
        return ruleList.size();
    }

    public void close() {
        int rid;
        Map rule;
        Browser browser;
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        browser = ruleList.browser();
        while((rid = browser.next()) >= 0) {
            rule = (Map) ruleList.get(rid);
            if (rule != null)
                rule.clear();
        }
        ruleList.clear();
    }

    protected void finalize() {
        close();
    }
}
