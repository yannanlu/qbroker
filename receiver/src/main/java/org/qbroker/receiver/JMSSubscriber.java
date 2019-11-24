package org.qbroker.receiver;

/* JMSSubscrier.java - a JMS producer for subscribing JMS topics */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.lang.reflect.InvocationTargetException;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TopicConnector;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * JMSSubscriber subscribes a JMS Topic and receives JMS messages
 * from it.  It puts the JMS Messages to an XQueue as the output.
 * JMSSubscriber supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br><br>
 * Due to the serial access of subscription for IBM MQSeries,
 * you can specify ActionSynchronized = true to keep creation, reaction
 * as serial processes.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JMSSubscriber extends Receiver {
    private TopicConnector tConn = null;
    private String tName;
    private int retryCount;
    private long sessionTime;
    private boolean isSynchronous, isConnected = false, isDaemon = false;

    private final static int SUB_CLOSE = 0;
    private final static int SUB_REOPEN = 1;
    private final static int SUB_RECONNECT = 2;

    @SuppressWarnings("unchecked")
    public JMSSubscriber(Map props) {
        super(props);
        Object o;
        String className = null;

        if (uri == null || uri.length() <= 0) { // default uri
            String hostName = (String) props.get("HostName");
            if (hostName != null && hostName.length() > 0) {
                uri = "wmq://" + hostName;
                if (props.get("Port") != null)
                    uri += ":" + (String) props.get("Port");
            }
            else
                uri = "wmq://";
            if (props.get("QueueManager") != null)
                uri += "/" + props.get("QueueManager");
            else
                uri += "/";
            props.put("URI", uri);
        }

        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        else
            xaMode = TopicConnector.XA_CLIENT;

        isDaemon = (mode > 0);

        if ((o = props.get("ActionSynchronized")) != null &&
            "true".equals((String) o))
            isSynchronous = true;
        else
            isSynchronous = false;

        tName = (String) props.get("TopicName");

        if ((o = props.get("JMSClassName")) != null) {
            className = (String) o;
            if (className.length() == 0)
                throw(new IllegalArgumentException("JMSClassName is empty"));
        }
        else if (uri.startsWith("wmq://"))
            className = "org.qbroker.wmq.TConnector";
        else
            className = "org.qbroker.jms.TConnector";

        String str = "failed to instantiate " + className + " on " + uri + ": ";
        try {
            Class<?> cls = Class.forName(className);
            java.lang.reflect.Constructor con =
                cls.getConstructor(new Class[]{Map.class});
            if (isSynchronous)
                tConn = syncCreate(con, props);
            else
                tConn = (TopicConnector) con.newInstance(new Object[]{props});
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            if (ex == null)
                throw(new IllegalArgumentException(str + Event.traceStack(e)));
            else if (ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.toString() + "\n";
            }
            else {
                str += e.toString() + " with Target exception:\n";
            }
            throw(new IllegalArgumentException(str + Event.traceStack(ex)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }

        operation = tConn.getOperation();
        if (!"sub".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }

        isConnected = true;
        new Event(Event.INFO, tName + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    /**
     * implementation of receive() with flow control
     * individual object control will be provided by the owner
     */
    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        for (;;) {
            if (!isConnected && status != RCVR_DISABLED) {
                if (isSynchronous)
                    str = syncAction(tConn, SUB_RECONNECT);
                else
                    str = tConn.reconnect();
                isConnected = (str == null);
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                topicOperation(xq, baseTime);

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
                    new Event(Event.INFO, tName + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == RCVR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                if (isConnected && Utils.getOutstandingSize(xq, partition[0],
                    partition[1]) <= 0) // safe to disconnect
                    disconnect();
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
                if (isConnected && xaMode == 0) // disconnect anyway
                    disconnect();
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
                new Event(Event.INFO, tName + " restarted on "+linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        if (isDaemon) // disconnect only if it is a daemon
            disconnect();
        new Event(Event.INFO, tName + " stopped on " + linkName).send();
    }

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int topicOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (!isConnected) {
                sessionTime = System.currentTimeMillis();
                retryCount = tolerance;
                throw(new JMSException(uri + ": connection is down"));
            }
            else
                tConn.sub(xq);
        }
        catch (JMSException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            String str = linkName + " " + tName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            if (status != RCVR_RETRYING) // no need to retry
                return 0;

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (retryCount <= tolerance) {
                if (retryCount > 1) {
                    try {
                        Thread.sleep(baseTime * (retryCount - 1));
                    }
                    catch (Exception e1) {
                    }
                }
                if (isSynchronous) {
                   str = syncAction(tConn, SUB_REOPEN);
                   i = (str == null) ? 0 : -1;
                }
                else
                   i = tConn.tReopen();
                if (i == 0) {
                    new Event(Event.INFO, tName + " reopened to " + operation +
                        " on " + linkName).send();
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return retryCount;
                }
                retryCount ++;
                if (!keepRunning(xq) || status != RCVR_RETRYING)
                    break;
            }
            isConnected = false;
            if (tolerance > 0) {
                new Event(Event.ERR, linkName + ": failed to open " + tName +
                    " after " + tolerance + " retries").send();
            }
            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount++ - tolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > tolerance + 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

                if (isSynchronous)
                   str = syncAction(tConn, SUB_RECONNECT);
                else
                   str = tConn.reconnect();
                if (str == null) {
                    new Event(Event.INFO, tName + " reconnected on " +linkName+
                        " after " + i + " retries: " + str).send();
                    isConnected = true;
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
                if (i == 1 || i == maxRetry)
                    new Event(Event.ERR, linkName + ": failed to connect on "+
                        tName + " after " + i + " retries: " + str).send();
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + tName + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private static synchronized TopicConnector syncCreate(
        java.lang.reflect.Constructor con, Map props)
        throws Exception {
        return (TopicConnector) con.newInstance(new Object[] {props});
    }

    private static synchronized String syncAction(TopicConnector conn,
        int action) {
        String str = null;
        if (conn == null)
            return "connector is null";
        switch (action) {
          case SUB_REOPEN:
            if (conn.tReopen() != 0)
                str = "failed to open topic";
            break;
          case SUB_RECONNECT:
            str = conn.reconnect();
            break;
          case SUB_CLOSE:
            try {
                conn.close();
            }
            catch (Exception e) {
            }
            break;
          default:
            break;
        }
        return str;
    }

    private void disconnect() {
        isConnected = false;
        if (tConn == null)
            return;

        if (isSynchronous)
            syncAction(tConn, SUB_CLOSE);
        else
            tConn.close();
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, tName + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (tConn != null)
            tConn = null;
    }

    protected void finalize() {
        close();
    }
}
