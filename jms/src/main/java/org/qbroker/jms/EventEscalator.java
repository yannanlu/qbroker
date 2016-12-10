package org.qbroker.jms;

/* EventEscalator.java - an EventEscalation to respond upon events */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.DisabledException;
import org.qbroker.common.Template;
import org.qbroker.common.DataSet;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.EventEscalation;
import org.qbroker.event.Event;

/**
 * EventEscalator maintains a session for selected events to keep tracking
 * a specific number as the indicator of the state change across the time
 * line.  The number being tracked can be either the sum of the number
 * specified by TrackedField of each event within the session, or
 * the total number of the events in the session if TrackedField is
 * not defined.  The method of escalate() will calculate the number
 * based on the incoming event and the session cache.  If the number falls
 * into the predefined EscalationRange, a new event will be generated as the
 * escalation.  If Type is set to MessageEscalator, the returned event
 * will be a TextEvent as a JMS Message.
 *<br/><br/>
 * EventEscalator implements EventEscalation. It provides a way to
 * correlate the same sort of events across the time line.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventEscalator implements EventEscalation {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private Template template;
    private boolean isJMSEvent = false;
    private int escalationOrder = ORDER_NONE;
    private int maxRetry, maxPage, tolerance, repeatPeriod, serialNumber;
    private int previousStatus, escalationCount, checkpointTimeout;
    private int sessionSize, sessionTimeout = 0, statusOffset;
    private long sessionTime, trackedNumber;
    private String[] copiedProperty;
    private String hostname, program, pid, trackedField;
    private DataSet dataSet;
    private final static String statusText[] = {"Exception",
        "Exception in blackout", "Disabled", "Blackout",
        "Normal", "Occurred"};

    public EventEscalator(Map props) {
        Object o;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "EventEscalator";
        if ("MessageEscalator".equals(type))
            isJMSEvent = true;

        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");

        if ((o = props.get("Description")) != null)
            description = MonitorUtils.substitute((String) o, template);
        else
            description = "respond to an event";

        List<String> list = new ArrayList<String>();
        if ((o = props.get("EscalationRange")) != null)
            list.add((String) o);
        else // default
            list.add("[1,)");
        dataSet = new DataSet(list);

        if ((o = props.get("TrackedField")) != null)
            trackedField = (String) o;
        else
            trackedField = null;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        if (sessionTimeout < 0)
            sessionTimeout = 0;

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

        if ((o = props.get("CheckpointTimeout")) == null ||
            (checkpointTimeout = 1000*Integer.parseInt((String) o)) < 0)
            checkpointTimeout = 480000;

        serialNumber = 0;
        escalationCount = 0;
        trackedNumber = 0;
        sessionSize = 0;
        sessionTime = System.currentTimeMillis() - sessionTimeout;
        previousStatus = TimeWindows.EXCEPTION;
        statusOffset = 0 - TimeWindows.EXCEPTION;
        hostname = Event.getHostName();
        program = Event.getProgramName();
        pid = String.valueOf(Event.getPID());
    }

    public Event escalate(int status, long currentTime, Event event) {
        int level = 0;
        Object o;
        StringBuffer strBuf = new StringBuffer();
        if (event == null)
            throw(new IllegalArgumentException("null event for " + name));

        serialNumber ++;

        if (sessionTimeout == 0) // no session
            resetSession(currentTime);
        else if (currentTime - sessionTime >= sessionTimeout)
            resetSession(currentTime);

        // check the status
        switch (status) {
          case TimeWindows.BLACKOUT: // blackout
          case TimeWindows.NORMAL: // normal
            if (status == TimeWindows.NORMAL && previousStatus != status) {
                resetSession(currentTime);
            }
            level = Event.INFO;
            escalationCount ++;

            if (trackedField != null) { // tracking on the sum
                if ((o = event.getAttribute(trackedField)) != null)
                    trackedNumber += Long.parseLong((String) o);
            }
            else { // tracking on the count
                trackedNumber = escalationCount;
            }

            if (dataSet.contains(trackedNumber)) { // out of the normal range
                if (status != TimeWindows.BLACKOUT)
                    level = Event.ERR;
                strBuf.append(name + " is out of normal range");
            }
            else if (status == TimeWindows.BLACKOUT)
                strBuf.append(name + " is blacked out");
            else
                strBuf.append(name + " is in the normal range");
            break;
          case TimeWindows.DISABLED:
          default:
            if (previousStatus == status) { // always disabled
                escalationCount = 0;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                resetSession(currentTime);
                strBuf.append(name + " has been disabled");
            }
            break;
        }
        previousStatus = status;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            sessionSize = 1;
            count = escalationCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // errors
            sessionSize = 1;
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
            ev = new TextEvent(strBuf.toString());
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
        ev.setAttribute("escalationCount", String.valueOf(escalationCount));

        for (int i=0; i<copiedProperty.length; i++) {
            if ((o = event.getAttribute(copiedProperty[i])) == null)
                continue;
            if ("text".equals(copiedProperty[i]))
                ev.setAttribute("originalText", (String) o);
            else if ("hostname".equals(copiedProperty[i]))
                ev.setAttribute("originalHost", (String) o);
            else
                ev.setAttribute(copiedProperty[i], (String) o);
        }
        if (trackedField != null)
            ev.setAttribute(trackedField, String.valueOf(trackedNumber));

        // make sure the internal attributes set
        ev.setAttribute("program", program);
        ev.setAttribute("hostname", hostname);
        ev.setAttribute("pid", pid);

        return ev;
    }

    public int resetSession(long currentTime) {
        int s = sessionSize;
        sessionTime = currentTime;
        escalationCount = 0;
        trackedNumber = 0;
        sessionSize = 0;
        return s;
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public int getSessionSize() {
        return sessionSize;
    }

    public String getName() {
        return name;
    }

    public int getEscalationOrder() {
        return escalationOrder;
    }

    public void clear() {
        resetSession(System.currentTimeMillis());
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        chkpt.put("Name", name);
        chkpt.put("CheckpointTime", String.valueOf(System.currentTimeMillis()));
        chkpt.put("SerialNumber", String.valueOf(serialNumber));
        chkpt.put("EscalationCount", String.valueOf(escalationCount));
        chkpt.put("PreviousStatus", String.valueOf(previousStatus));
        chkpt.put("TrackedNumber", String.valueOf(trackedNumber));
        chkpt.put("SessionTime", String.valueOf(sessionTime));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct, sTime, tNumber;
        int eCount, pStatus, sNumber, pPriority;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            ct = Long.parseLong((String) o);
            if (ct <= System.currentTimeMillis() - checkpointTimeout)
                return;
        }
        else
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("SessionTime")) != null)
            sTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("TrackedNumber")) != null)
            tNumber = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("EscalationCount")) != null)
            eCount = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        escalationCount = eCount;
        previousStatus = pStatus;
        trackedNumber = tNumber;
        sessionTime = sTime;
        serialNumber = sNumber;
    }
}
