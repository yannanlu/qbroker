package org.qbroker.persister;

/* MonitorPersister.java - a generic persister for Occurrences */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorAction;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * MonitorPersister listens to an XQueue and receives ObjectMessages
 * from it.  The incomint message is supposed to contain a task Map.
 * MonitorPersister completes the task and returns the message with
 * updates.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MonitorPersister extends Persister {
    private String rcField = "status";
    private int retryCount;
    private int type;
    private int maxNumberMsg;
    private Template template = null;
    private long sessionTime;

    public MonitorPersister(Map props) {
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

        if (!"mon".equals(scheme))
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
                monitorOperation(xq, baseTime);

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
    private int monitorOperation(XQueue xq, int baseTime) {
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
     * It takes a monitor task from the queue and invokes the detection and
     * action
     */
    @SuppressWarnings("unchecked")
    private void invoke(XQueue xq) {
        int sid, n, skippingStatus = -1, myStatus = -1;
        int mask;
        long currentTime = 0L, sampleTime, count = 0;
        ObjectMessage outMessage;
        Object o;
        MonitorReport report;
        MonitorAction action;
        Exception ex;
        Event ev = null;
        TimeWindows tw;
        Map<String, Object> latest = null;
        Map task;
        String reqRC = String.valueOf(TimeWindows.BLACKEXCEPTION);

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

            o = xq.browse(sid);
            if (o == null || !(o instanceof ObjectMessage)) {
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a non-object msg from " +
                    xq.getName()).send();
                continue;
            }
            outMessage = (ObjectMessage) o;

            currentTime = System.currentTimeMillis();
            // setting default RC
            try {
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, reqRC, outMessage);
                }
                catch (Exception ee) {
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
                new Event(Event.WARNING, "dropped an empty msg from " +
                    xq.getName()).send();
                continue;
            }
            else
                task = (Map) o;

            // for skipped status
            myStatus = TimeWindows.BLACKOUT;
            report = (MonitorReport) task.get("Report");
            action = (MonitorAction) task.get("Action");
            tw = (TimeWindows) task.get("TimeWindow");

            ex = null;
            try {
                latest = report.generateReport(currentTime);
                skippingStatus = report.getSkippingStatus();
                if (displayMask > 0)
                    new Event(Event.INFO, uri + " generated report on " +
                        action.getName() + " from " + linkName + ": " +
                        skippingStatus).send();
            }
            catch (Exception e) {
                latest = new HashMap<String, Object>();
                myStatus = TimeWindows.EXCEPTION;
                ex = e;
            }

            try {
                MessageUtils.setProperty(rcField, String.valueOf(myStatus),
                    outMessage);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set skipping status on " +
                    "msg from " + xq.getName()).send();
            }

            if (myStatus != TimeWindows.EXCEPTION) { // report is OK
                if (skippingStatus == MonitorReport.DISABLED) { // disabled
                    myStatus = TimeWindows.DISABLED;
                    if (tw.check(currentTime, 0L) == TimeWindows.EXPIRED)
                        myStatus = TimeWindows.EXPIRED;
                }
                else if (skippingStatus == MonitorReport.SKIPPED) { //skipped
                    if (tw.check(currentTime, 0L) == TimeWindows.EXPIRED)
                        myStatus = TimeWindows.EXPIRED;
                    else
                        xq.remove(sid);
                    continue;
                }
                else { // no skip
                    if (latest != null && (o = latest.get("SampleTime")) != null
                        && o instanceof long[] && ((long[]) o).length > 0)
                        sampleTime = ((long[]) o)[0];
                    else
                        sampleTime = 0L;
                    myStatus = tw.check(currentTime, sampleTime);
                    if (myStatus != TimeWindows.EXPIRED &&
                        skippingStatus == MonitorReport.EXCEPTION) {
                        if (myStatus == TimeWindows.BLACKOUT)
                            myStatus = TimeWindows.BLACKEXCEPTION;
                        else
                            myStatus = TimeWindows.EXCEPTION;
                    }
                }
            }
            else { // exception
                myStatus = tw.check(currentTime, 0L);
                if (myStatus == TimeWindows.BLACKOUT)
                    myStatus = TimeWindows.BLACKEXCEPTION;
                else if (myStatus != TimeWindows.EXPIRED) {
                    if (ex != null)
                        latest.put("Exception", ex);
                    myStatus = TimeWindows.EXCEPTION;
                }
            }

            // setting final RC
            try {
                MessageUtils.setProperty(rcField, String.valueOf(myStatus),
                    outMessage);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set skipping status on " +
                    "msg from " + xq.getName()).send();
            }

            // action
            if (myStatus != TimeWindows.EXPIRED) try {
                ev = action.performAction(myStatus, currentTime, latest);
            }
            catch (Exception e) {
                ev = null;
                new Event(Event.ERR, "Exception in: " + action.getName() +
                    ": " + Event.traceStack(e)).send();
            }

            if (ev != null) { // escalation
                task.put("Event", ev);
                ev = null;
            }
            xq.remove(sid);
            outMessage = null;
            count ++;
            latest.clear();
        }
    }

    public void close() {
        setStatus(PSTR_CLOSED);

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
