package org.qbroker.receiver;

/* RMQReceiver.java - a RabbitMQ receiver for receiving messages */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.QueueSender;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.RMQMessenger;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * RMQReceiver listens to a RabbitMQ queue and receives messages from it.
 * It converts the messages into JMS Messages and puts them to an XQueue
 * as the output. RMQReceiver supports flow control and allows object control
 * from its owner.  It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RMQReceiver extends Receiver {
    private RMQMessenger rmq = null;
    private String qName;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false, isDaemon = false;

    public RMQReceiver(Map props) {
        super(props);
        Object o;
        String className = null;

        if (uri == null) {
            String hostName = (String) props.get("HostName");
            if (hostName == null || hostName.length() <= 0)
                hostName = "localhost";
            uri = "amqp://" + hostName;
            if (props.get("Port") != null)
                uri += ":" + (String) props.get("Port");
            if ((o = props.get("VirtualHost")) != null)
                uri += (String) o;
            else
                uri += "/";
        }

        isDaemon = (mode > 0);

        String str = "failed to instantiate " + uri + ":";
        try {
            rmq = new RMQMessenger(props);
        }
        catch (IOException e) {
            throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }

        qName = rmq.getDestination();
        operation = rmq.getOperation();
        if (!"get".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation + " for " + uri));
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
            rmq.reconnect();
            isConnected = true;
        }

        for (;;) {
            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                rmqOperation(xq, baseTime);

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
                rmq.reconnect();
                isConnected = true;
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
    private int rmqOperation(XQueue xq, int baseTime) {
        int i = 0, mask;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            rmq.get(xq);
        }
        catch (IOException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            String str = linkName + " " + qName + ": ";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            if (status != RCVR_RETRYING) // no need to retry
                return 0;

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

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

                if (rmq.reconnect() == null) {
                    new Event(Event.INFO, qName + " reconnected on " +linkName+
                        " after " + i + " retries").send();
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    isConnected = true;
                    return --retryCount;
                }
                if (i == 1 || i == maxRetry)
                    new Event(Event.ERR, linkName + ": failed to connect on " +
                        qName + " after " + i + " retries").send();
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

    private void disconnect() {
        isConnected = false;
        if (rmq != null) try {
            rmq.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, linkName + ": failed to close " +
                qName + ": " + Event.traceStack(e)).send();
        }
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, qName + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (rmq != null)
            rmq = null;
    }

    protected void finalize() {
        close();
    }
}
