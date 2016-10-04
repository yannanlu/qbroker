package org.qbroker.persister;

/* EventPersister.java - a generic persister for Events */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.monitor.MonitorAction;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.persister.Persister;
import org.qbroker.event.EventAction;
import org.qbroker.event.Event;

/**
 * EventPersister listens to an XQueue and receives ObjectMessages
 * from it.  The incomint message is supposed to contain a task Map.
 * EventPersister completes the task and returns the message with updates.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventPersister extends Persister {
    private String rcField = "ReturnCode", scriptField = null;
    private int retryCount;
    private int type;
    private int maxNumberMsg;
    private Template template = null;
    private long sessionTime;
    private final static int EVENT_SUB = 1;
    private final static int EVENT_PUB= 2;
    private final static int EVENT_ACT = 3;

    public EventPersister(Map props) {
        super(props);
        String scheme = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException("invalid uri: " + uri));
        }

        scheme = u.getScheme();

        if (!"event".equals(scheme))
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "invoke";

        if (!"invoke".equals(operation))
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
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
                eventOperation(xq, baseTime);

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
     * real implementation of persist() with exception handling and retry
     */
    private int eventOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            invoke(xq);
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(PSTR_STOPPED);
            return -1;
        }
        catch (Error e) {
            setStatus(PSTR_STOPPED);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            Event.flush(e);
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == PSTR_RUNNING &&
            (mask & XQueue.STANDBY) == 0) // job is done
            setStatus(PSTR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    /**
     * takes an event-action task from the queue and invokes the action upon
     * the event
     */
    @SuppressWarnings("unchecked")
    private void invoke(XQueue xq) {
        int sid, n, mask;
        long currentTime = 0L, count = 0;
        Event event;
        ObjectMessage outMessage;
        Object o;
        EventAction eAction;
        MonitorAction mAction;
        Map task = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

            currentTime = System.currentTimeMillis();
            o = xq.browse(sid);
            if (o == null || !(o instanceof ObjectMessage)) {
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a non-object msg from " +
                    xq.getName()).send();
                continue;
            }
            outMessage = (ObjectMessage) o;

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    continue;
                }
            }
            catch (Exception e) {
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                continue;
            }

            try {
                o = outMessage.getObject();
            }
            catch (JMSException e) {
                o = null;
            }

            if (o == null || !(o instanceof Map)) {
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null or bad msg from " +
                    xq.getName()).send();
                continue;
            }
            else
                task = (Map) o;

            o = task.get("Action");
            if (o instanceof EventAction) {
                String line = "";
                event = (Event) task.get("Event");
                eAction = (EventAction) o;

                try {
                    eAction.invokeAction(currentTime, event);
                    if (displayMask > 0) try {
                        line = MessageUtils.display((JMSEvent) event,
                            event.getAttribute("Text"), displayMask,
                            propertyName);
                    }
                    catch (Exception ex) {
                        line = ex.toString();
                    }
                    new Event(Event.INFO, uri + " " + eAction.getName() +
                        " invoked action on event from " + linkName +
                        " with (" + line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, uri+ " " + eAction.getName() +
                        " failed: " + Event.traceStack(e)).send();
                }
            }
            else if (o instanceof MonitorAction) {
                String line = "";
                Event ev = null;
                event = (Event) task.get("Event");
                mAction = (MonitorAction) o;
                task.remove("Action");

                try {
                    ev = mAction.performAction(0, currentTime, task);
                    if (displayMask > 0) try {
                        line = MessageUtils.display((JMSEvent) event,
                            event.getAttribute("Text"), displayMask,
                            propertyName);
                    }
                    catch (Exception ex) {
                        line = ex.toString();
                    }
                    new Event(Event.INFO, uri + " " + mAction.getName() +
                        " performed action on event from " + linkName +
                        " with (" + line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, uri+ " " + mAction.getName() +
                        " failed: " + Event.traceStack(e)).send();
                    xq.remove(sid);
                    continue;
                }
                if (ev != null) // put new event back to replace task
                    task.put("Escalation", ev);
            }
            else {
                new Event(Event.ERR, "got an unsupported action from " +
                    xq.getName()).send();
            }
            xq.remove(sid);
            outMessage = null;
            count ++;
        }
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
    }

    protected void finalize() {
        close();
    }
}
