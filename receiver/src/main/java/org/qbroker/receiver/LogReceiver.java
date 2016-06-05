package org.qbroker.receiver;

/* LogReceiver.java - a Messagereceiver receiving JMS messages from a log */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import javax.jms.JMSException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageLogger;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * LogReceiver listens to a log file and fetches all new entries of it.
 * It appends the new log entries as JMS Messages to an XQueue.
 * LogReceiver supports flow control and allows object control from its owner.
 * It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class LogReceiver extends Receiver {
    private MessageLogger msgLog = null;
    private int retryCount;
    private int initDelay;
    private long sessionTime;
    private List[] dependencyGroup = null;

    public LogReceiver(Map props) {
        super(props);
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        if ((o = props.get("DependencyGroup")) != null && o instanceof List) {
            dependencyGroup = MonitorUtils.getDependencies((List) o);
        }

        msgLog = new MessageLogger(props);
        operation = msgLog.getOperation();
        if (!"fetch".equals(operation))
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));

        if(dependencyGroup == null && (o = props.get("SaveReference")) != null){
            if (uri.startsWith("log://") && "false".equals((String) o)) {
                List<Object> list = new ArrayList<Object>();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("URI", "file:" + uri.substring(4));
                map.put("Name", "file:" + uri.substring(4));
                map.put("Type", "FileTester");
                list.add(map);
                map = new HashMap<String, Object>();
                map.put("Dependency", list);
                list = new ArrayList<Object>();
                list.add(map);
                dependencyGroup = MonitorUtils.getDependencies(list);
            }
        }

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        if ((o = props.get("InitDelay")) == null ||
            (initDelay = 1000 * Integer.parseInt((String) o)) <= 0)
            initDelay = 0;

        retryCount = 0;
        sessionTime = 0L;
    }

    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        if (initDelay > 0) try { // initial delay at startup
            Thread.sleep(initDelay);
            initDelay = 0;
        }
        catch (Exception e) {
            initDelay = 0;
        }
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        for (;;) {
            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                logOperation(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // pause temporarily
                    if (status == RCVR_READY) // for confirmation
                        setStatus(RCVR_DISABLED);
                    else if (status == RCVR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > RCVR_RETRYING && status < RCVR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == RCVR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == RCVR_PAUSE) {
                if (status > RCVR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > RCVR_PAUSE)
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
                status == RCVR_STANDBY) {
                if (status > RCVR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > RCVR_STANDBY)
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

            if (isStopped(xq) || status >= RCVR_STOPPED)
                break;
            if (status == RCVR_READY) {
                setStatus(RCVR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    private int logOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            msgLog.fetch(xq);
        }
        catch (Exception e) {
            Exception ex = null;
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            String str = linkName + " " + uri + ": ";
            if (e instanceof JMSException)
                ex = ((JMSException) e).getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                long currentTime;
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }
                currentTime = System.currentTimeMillis();
                if (dependencyGroup != null) {
                    if (MonitorUtils.checkDependencies(currentTime,
                        dependencyGroup, uri) == MonitorReport.NOSKIP) {
                        resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                        sessionTime = currentTime;
                        new Event(Event.INFO, uri + " retried " + --retryCount+
                            " times on " + linkName).send();
                        return retryCount;
                    }
                    if (i <= maxRetry)
                        new Event(Event.INFO, uri + " skipped " +
                            (retryCount-1) + " retries on " + linkName).send();
                }
                else if (i <= maxRetry) {
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = currentTime;
                    new Event(Event.INFO, uri + " retried " + --retryCount +
                        " times on " + linkName).send();
                    return retryCount;
                }
            }
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    public void close() {
        setStatus(RCVR_CLOSED);
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (msgLog != null)
            msgLog = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
