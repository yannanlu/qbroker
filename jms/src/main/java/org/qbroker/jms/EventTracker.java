package org.qbroker.jms;

/* EventTracker.java - an EventEscalation to keep tracking events */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.DisabledException;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.Aggregation;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventSelector;
import org.qbroker.event.EventEscalation;
import org.qbroker.event.Event;

/**
 * EventTracker aggregates incoming events and keeps tracking their state
 * according to the predefined policies and the content of the aggregated
 * events.  In case of the state changes up to the escalation point, it
 * generates a new event as the escalation and returns it.  EventTracker
 * maitains the session for all aggregated events.  The session will expire
 * if it reaches the expiration time, or reset by certain incoming events.
 * If the Type is set to MessageTracker, it will return a TextEvent as a
 * JMS Message.
 *<br/><br/>
 * EventTracker caches the aggregated events based on their keys and the
 * shared TimeToLive.  If the cached time for the key exceeds the TTL, it
 * will be expired and a new event will be generated for aggregation.
 *<br/><br/>
 * EventTracker implements EventEscalation. It provides a way for us to
 * correlate events across the time line.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventTracker implements EventEscalation {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private Template template, keyTemp = null;
    private TextSubstitution keySub = null;
    private Aggregation aggr;
    private QuickCache cache = null;   // cache of the aggregated msgs
    private EventSelector selector;
    private String[] copiedProperty;
    private String hostname, program, pid, resetField = null;
    private long ttl, minSpan = 0;
    private boolean isJMSEvent = false;
    private int statusOffset, initStatus;
    private int maxRetry, maxPage, tolerance, repeatPeriod, serialNumber;
    private int checkpointExpiration, escalationOrder = ORDER_NONE;
    private final static String statusText[] = {"Exception",
        "Exception in blackout", "Disabled", "Blackout",
        "Normal", "Occurred"};

    public EventTracker(Map props) {
        Object o;
        int n;
        List list;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "EventTracker";
        if ("MessageTracker".equals(type))
            isJMSEvent = true;

        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");

        if ((o = props.get("Description")) != null)
            description = MonitorUtils.substitute((String) o, template);
        else
            description = "keep tracking events";

        if ((o = props.get("TimeToLive")) != null && o instanceof String)
            ttl = 1000 * Long.parseLong((String) o);
        else
            ttl = 0;

        if ((o = props.get("CopiedProperty")) != null && o instanceof List) {
            String key;
            List<String> pl = new ArrayList<String>();
            for (Object ky : (List) o) {
                if (!(ky instanceof String))
                    continue;
                key = (String) ky; 
                if (key.length() > 0)
                    pl.add(key);
            }   
            copiedProperty = pl.toArray(new String[pl.size()]);
        }
        else
            copiedProperty = new String[0];

        if ((o = props.get("EscalationOrder")) != null) {
            if ("first".equals(((String) o).toLowerCase()))
                escalationOrder = ORDER_FIRST;
            else if ("last".equals(((String) o).toLowerCase()))
                escalationOrder = ORDER_LAST;
        }

        if ((o = props.get("EscalationPattern")) != null) {
            Map<String, Object> ph = new HashMap<String, Object>();
            ph.put("EventPattern", o);
            selector = new EventSelector(ph);
        }
        else
            throw(new IllegalArgumentException("EscalationPattern is not " +
                "defined for " + name));

        if ((o = props.get("MinTimeSpan")) != null)
            minSpan = 1000 * Long.parseLong((String) o);
        if (minSpan < 0)
            minSpan = 0;

        if ((o = props.get("ResetField")) != null)
            resetField = (String) o;

        if ((o = props.get("KeyTemplate")) != null && o instanceof String)
            keyTemp = new Template((String) o);

        if ((o = props.get("KeySubstitution")) != null &&
            o instanceof String)
            keySub = new TextSubstitution((String)o);

        if ((o = props.get("Aggregation")) == null || !(o instanceof List))
            throw(new IllegalArgumentException("Aggregation is not defined"));
        list = (List) o;

        aggr = new Aggregation(name, list);

        cache = new QuickCache(name, QuickCache.META_DEFAULT, 0, 0);

        if ((o = props.get("Tolerance")) == null ||
            (tolerance = Integer.parseInt((String) o)) < 0)
            tolerance = 0;

        if ((o = props.get("MaxRetry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) < 0)
            maxRetry = 2;

        if ((o = props.get("MaxPage")) == null ||
            (maxPage = Integer.parseInt((String) o)) < 0)
            maxPage = 2;

        if ((o = props.get("QuietPeriod")) == null ||
            (n = Integer.parseInt((String) o)) < 0)
            n = 0;

        repeatPeriod = maxRetry + maxPage + n;

        if ((o = props.get("CheckpointExpiration")) == null ||
            (checkpointExpiration = 1000*Integer.parseInt((String) o)) <= 0)
            checkpointExpiration = 480000;

        serialNumber = 0;
        initStatus = TimeWindows.ELATE;
        statusOffset = 0 - TimeWindows.EXCEPTION;
        hostname = Event.getHostName();
        program = Event.getProgramName();
        pid = String.valueOf(Event.getPID());
    }

    public Event escalate(int status, long currentTime, Event event) {
        int level = 0, escalationCount = 0, previousStatus = initStatus;
        int[] meta = null;
        Object o;
        TextEvent msg = null;
        byte[] buffer = new byte[4096];
        StringBuffer strBuf = new StringBuffer();
        String key = null;
        if (event == null)
            return null;

        if (!isJMSEvent)
            key = EventUtils.format(event, keyTemp);
        else try {
            key = MessageUtils.format((JMSEvent) event, buffer, keyTemp);
        }
        catch (Exception e) {
            key = null;
            status = (status == TimeWindows.BLACKOUT) ?
                TimeWindows.BLACKEXCEPTION : TimeWindows.EXCEPTION;
        }
        if (keySub != null && key != null)
            key = keySub.substitute(key);

        if (key != null) {
            int i = -1;
            String str = null;
            if (resetField != null)
                str = event.getAttribute(resetField);
            if (str != null && str.length() > 0) { // reset state
                cache.expire(key, currentTime);
                return null;
            }
            msg = (TextEvent) cache.get(key);
            if (msg == null) { // initialize the first one
                msg = new TextEvent();
                msg.setTimestamp(event.getTimestamp());
                if (!isJMSEvent)
                    i = aggr.initialize(currentTime, event, msg);
                else try {
                    i = aggr.initialize(currentTime, (JMSEvent) event,
                        (Message) msg);
                }
                catch (Exception e) {
                    i = -1;
                    msg = null;
                    new Event(Event.ERR, name + ": failed to init on " + key +
                        "\n"+Event.traceStack(e)).send();
                }
                if (i == 0) {
                    meta = new int[]{1, 0, 0, initStatus};
                    cache.insert(key, currentTime, (int) ttl, meta, msg);
                    escalationCount = meta[2];
                    previousStatus = meta[3];
                }
            }
            else if (!isJMSEvent) { // initialized already for event
                meta = cache.getMetaData(key);
                long tm = msg.getTimestamp();
                long te = event.getTimestamp();
                if (minSpan > 0 && te - tm <  minSpan)
                    return null;
                else // aggregate
                    i = aggr.aggregate(currentTime, event, msg);
                if (i == 0) { // update internal count for the key
                    meta[0] ++;
                    meta[1] += (int) (te - tm);
                    escalationCount = meta[2];
                    previousStatus = meta[3];
                    msg.setTimestamp(te);
                }
            }
            else try { // initialized already for jms
                meta = cache.getMetaData(key);
                long tm = msg.getTimestamp();
                long te = event.getTimestamp();
                if (minSpan > 0 && te - tm < minSpan)
                    return null;
                else // aggregate
                    i = aggr.aggregate(currentTime, (JMSEvent) event,
                        (Message) msg);
                if (i == 0) { // update internal count for the key
                    meta[0] ++;
                    meta[1] += (int) (te - tm);
                    escalationCount = meta[2];
                    previousStatus = meta[3];
                    msg.setTimestamp(te);
                }
            }
            catch (Exception e) {
                i = -1;
                new Event(Event.ERR, name + ": failed to aggregate on " +
                    key + "\n"+Event.traceStack(e)).send();
            }
        }

        if (msg != null) // aggregation is succeeded
            serialNumber ++;
        else if (key != null) // key is not null
            status = (status == TimeWindows.BLACKOUT) ?
                TimeWindows.BLACKEXCEPTION : TimeWindows.EXCEPTION;

        // check the status
        switch (status) {
          case TimeWindows.BLACKOUT: // blackout
          case TimeWindows.NORMAL: // normal
            if (status == TimeWindows.NORMAL && previousStatus != status) {
                escalationCount = 0;
            }
            level = Event.INFO;
            escalationCount ++;

            if (selector.evaluate(currentTime, msg)) { // pattern hit
                if (status != TimeWindows.BLACKOUT)
                    level = Event.ERR;
                strBuf.append(name + " got pattern hit");
            }
            else if (status == TimeWindows.BLACKOUT)
                strBuf.append(name + " is blacked out");
            else
                strBuf.append(name + " got pattern hit");
            break;
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                escalationCount = 0;
                if (meta != null) // update persisted state
                    meta[2] = escalationCount;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                escalationCount = 0;
                strBuf.append(name + " has been disabled");
            }
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
          case TimeWindows.EXCEPTION: // exception
            return null;
          default:
            return null;
        }
        if (meta != null) { // update persisted state
            meta[2] = escalationCount;
            meta[3] = status;
            if (escalationCount == 1 && level != Event.INFO) // reset offset
                meta[1] = 0;
        }

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            count = escalationCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // errors
            count = escalationCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                break;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          default:
            if (escalationCount > 1)
                return null;
            break;
        }

        Event ev;
        if (isJMSEvent) {
            ev = new org.qbroker.jms.TextEvent(strBuf.toString());
            ev.setPriority(level);
            ev.removeAttribute("priority");
        }
        else
            ev = new Event(level, strBuf.toString());

        long te = event.getTimestamp();
        ev.setTimestamp(te);
        ev.setAttribute("name", name);
        ev.setAttribute("site", site);
        ev.setAttribute("category", category);
        ev.setAttribute("type", type);
        ev.setAttribute("description", description);
        ev.setAttribute("date", Event.dateFormat(new Date(te)));
        ev.setAttribute("status", statusText[status + statusOffset]);
        ev.setAttribute("EscalationKey", key);
        ev.setAttribute("escalationCount", String.valueOf(escalationCount));
        if (meta != null && level != Event.INFO)
            ev.setAttribute("firstOccurred",
                Event.dateFormat(new Date(te - meta[1])));

        for (int i=0; i<copiedProperty.length; i++) {
            if ((o = msg.getAttribute(copiedProperty[i])) == null)
                continue;
            if ("text".equals(copiedProperty[i]))
                ev.setAttribute("originalText", (String) o);
            else if ("hostname".equals(copiedProperty[i]))
                ev.setAttribute("originalHost", (String) o);
            else
                ev.setAttribute(copiedProperty[i], (String) o);
        }

        // make sure the internal attributes set
        ev.setAttribute("program", program);
        ev.setAttribute("hostname", hostname);
        ev.setAttribute("pid", pid);

        return ev;
    }

    public int resetSession(long currentTime) {
        String[] keys;
        keys = cache.disfragment(currentTime);
        cache.setStatus(cache.getStatus(), currentTime);
        if (keys != null)
            return keys.length;
        else
            return 0;
    }

    public long getSessionTime() {
        return cache.getMTime();
    }

    public int getSessionSize() {
        return cache.size();
    }

    public String getName() {
        return name;
    }

    public int getEscalationOrder() {
        return escalationOrder;
    }

    public void clear() {
        cache.clear();
        aggr.clear();
        if (template != null)
            template.clear();
        template = null;
        if (keyTemp != null)
            keyTemp.clear();
        keyTemp = null;
        if (keySub != null)
            keySub.clear();
        keySub = null;
        if (selector != null)
            selector.clear();
        selector = null;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        chkpt.put("Name", name);
        chkpt.put("CheckpointTime", String.valueOf(System.currentTimeMillis()));
        chkpt.put("SerialNumber", String.valueOf(serialNumber));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long dt, pTimestamp;
        int eCount, pValue, pStatus, sNumber, pPriority;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            dt = System.currentTimeMillis() - Long.parseLong((String) o);
            if (dt >= checkpointExpiration)
                return;
        }
        else
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        serialNumber = sNumber;
    }
}
