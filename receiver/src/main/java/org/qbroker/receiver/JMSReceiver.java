package org.qbroker.receiver;

/* JMSReceiver.java - a JMS producer for receiving JMS messages */

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.lang.reflect.InvocationTargetException;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.QueueSender;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.QueueConnector;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * JMSReceiver listens to a JMS Queue and receives JMS messages
 * from it.  It puts the JMS Messages to an XQueue as the output.
 * JMSReceiver supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JMSReceiver extends Receiver {
    private QueueConnector qConn = null;
    private List[] dependencyGroup = null;
    private String qName;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false, isDaemon = false;

    @SuppressWarnings("unchecked")
    public JMSReceiver(Map props) {
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
            xaMode = QueueConnector.XA_CLIENT;

        isDaemon = (mode > 0);

        qName = (String) props.get("QueueName");

        if ((o = props.get("JMSClassName")) != null) {
            className = (String) o;
            if (className.length() == 0)
                throw(new IllegalArgumentException("JMSClassName is empty"));
        }
        else if (uri.startsWith("wmq://"))
            className = "org.qbroker.wmq.QConnector";
        else
            className = "org.qbroker.jms.QConnector";

        String str = "failed to instantiate " + className + " on " + uri + ": ";
        try {
            Class<?> cls = Class.forName(className);
            java.lang.reflect.Constructor con =
                cls.getConstructor(new Class[]{Map.class});
            qConn = (QueueConnector) con.newInstance(new Object[]{props});
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

        operation = qConn.getOperation();
        if (!"browse".equals(operation) && !"get".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }
        else if ("browse".equals(operation)) {
            if((o = props.get("DependencyGroup")) != null && o instanceof List){
                dependencyGroup = MonitorUtils.getDependencies((List) o);
                MonitorUtils.checkDependencies(System.currentTimeMillis(),
                    dependencyGroup, uri);
            }
        }

        isConnected = true;
        new Event(Event.INFO, qName + " opened and ready to " + operation +
            " on " + linkName ).send();

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
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        // reconnect if it is not connected
        if (!isConnected && status != RCVR_DISABLED) {
            str = qConn.reconnect();
            isConnected = (str == null);
        }

        for (;;) {
            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                queueOperation(xq, baseTime);

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
                    new Event(Event.INFO, qName + " is " + // state changed
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
            if (!isConnected && status != RCVR_DISABLED) {
                str = qConn.reconnect();
                isConnected = (str == null);
            }
            if (status == RCVR_READY) {
                setStatus(RCVR_RUNNING);
                new Event(Event.INFO, qName + " restarted on "+linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        if (isDaemon) // disconnect only if it is a daemon
            disconnect();
        new Event(Event.INFO, qName + " stopped on " + linkName).send();
    }

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int queueOperation(XQueue xq, int baseTime) {
        int i = 0, mask;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (!isConnected) {
                sessionTime = System.currentTimeMillis();
                retryCount = tolerance;
                throw(new JMSException(uri + ": connection is down"));
            }
            else if ("get".equals(operation))
                qConn.get(xq);
            else {
                qConn.browse(xq);
                if (dependencyGroup != null) { // repeated browse
                    sessionTime = System.currentTimeMillis();
                    long previousTime = sessionTime + baseTime;
                    while(((mask=xq.getGlobalMask()) & XQueue.KEEP_RUNNING)>0){
                        if ((mask & XQueue.PAUSE) > 0 || // paused temporarily
                            status != RCVR_RUNNING)      // status changed
                            break;
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception ex) {
                        }
                        sessionTime = System.currentTimeMillis();
                        if (previousTime <= sessionTime) { // time is up
                            previousTime = sessionTime + baseTime;
                            i = MonitorUtils.checkDependencies(sessionTime,
                                dependencyGroup, uri);
                            if (i == MonitorReport.NOSKIP)
                                qConn.browse(xq);
                            else if (i == MonitorReport.EXCEPTION)
                                new Event(Event.WARNING, uri +
                                    ": dependencies failed with " + i).send();
                        }
                    }
                }
            }
        }
        catch (JMSException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            String str = linkName + " " + qName + ": ";
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
                if (qConn.qReopen() == 0) {
                    new Event(Event.INFO, qName + " reopened to " + operation +
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
                new Event(Event.ERR, linkName + ": failed to open " +qName+
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

                if ((str = qConn.reconnect()) == null) {
                    new Event(Event.INFO, qName + " reconnected on " +linkName+
                        " after " + i + " retries").send();
                    isConnected = true;
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
                if (i == 1 || i == maxRetry)
                    new Event(Event.ERR, linkName + ": failed to connect on " +
                        qName + " after " + i + " retries: " + str).send();
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + qName + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    /** sends the message to the default queue */
    public synchronized void send(Message msg) throws JMSException {
        QueueSender qs;
        long ttl;
        if (msg == null)
            return;
        ttl = msg.getJMSExpiration();
        if (ttl > 0) { // recover ttl first
            ttl -= System.currentTimeMillis();
            if (ttl <= 0) // reset ttl to default
                ttl = 1000L;
        }
        if ((qs = qConn.getQueueSender()) != null)
            qs.send(msg, msg.getJMSDeliveryMode(), msg.getJMSPriority(), ttl);
        else
            throw(new JMSException(qName + ": failed to get qSender"));
    }

    /** sends the message with the text and properties to the default queue */
    public void send(String text, int d, int p, long ttl, String mtype)
        throws JMSException {
        TextMessage msg = (TextMessage) qConn.createMessage(1);
        if (text == null)
            text = "";
        if (msg != null) {
            if (d == 1)
                msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            else if (d == 2)
                msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            if (p >= 0 && p <= 9)
                msg.setJMSPriority(p);
            if (mtype != null)
                msg.setJMSType(mtype);
            msg.setText(text);
            if (ttl > 0)
                msg.setJMSExpiration(System.currentTimeMillis() + ttl);
            try {
                send(msg);
            }
            catch (JMSException e) {
                if (status == RCVR_RUNNING) // raise async exception
                    qConn.onException(e);
                throw(e);
            }
        }
        else
            throw(new JMSException(qName + ": failed to create msg"));
    }

    private void disconnect() {
        isConnected = false;
        if (qConn != null)
            qConn.close();
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, qName + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (qConn != null)
            qConn = null;
    }

    protected void finalize() {
        close();
    }
}
