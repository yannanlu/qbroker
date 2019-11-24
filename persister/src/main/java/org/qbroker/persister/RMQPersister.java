package org.qbroker.persister;

/* RMQPersister.java - a JMS persister publishing messages to a RabbitMQ queue*/

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.RMQMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * RMQPersister listens to an XQueue and receives JMS messages from it.
 * It publishes the messages with certain routing keys to a RabbitMQ exchange
 * as the output. RMQPersister supports flow control and allows object control
 * from its owner.  It is fault tolerant with retry and idle options.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class RMQPersister extends Persister {
    private RMQMessenger rmq = null;
    private String qName, msgID;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false;

    public RMQPersister(Map props) {
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
        if (!"pub".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation + " for " + uri));
        }

        isConnected = true;
        new Event(Event.INFO, qName + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    /**
     * implementation of persist() with flow control
     * individual object control will be provided by the owner
     */
    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;
        boolean checkIdle = true;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(PSTR_READY, PSTR_RUNNING);

        for (;;) {
            if (!isConnected && status != PSTR_DISABLED)
                rmq.reconnect();

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                rmqOperation(xq, baseTime);
                checkIdle = true;

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                        checkIdle = false;
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, qName + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                if (isConnected)
                    disconnect();
                if (checkIdle && xq.size() > 0) { // not idle any more
                    resetStatus(PSTR_DISABLED, PSTR_READY);
                    break;
                }
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
                new Event(Event.INFO, qName+" restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        disconnect();
        new Event(Event.INFO, qName + " stopped on " + linkName).send();
    }

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int rmqOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            rmq.pub(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + qName +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (IOException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            String str = linkName + " " + qName + ": ";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            if (status != PSTR_RETRYING) // no need to retry
                return 0;

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == PSTR_RETRYING) {
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
                    new Event(Event.INFO, qName + " reconnected on " + linkName+
                        " after " + i + " retries").send();
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
                if (i == 1 || i == maxRetry)
                    new Event(Event.ERR, linkName + ": failed to connect on "+
                        qName + " after " + i + " retries").send();
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + qName + ": " +
                Event.traceStack(e)).send();
            setStatus(PSTR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == PSTR_RUNNING &&
            (mask & XQueue.STANDBY) == 0) // job is done
            setStatus(PSTR_STOPPED);
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
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, qName + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        disconnect();
        if (rmq != null)
            rmq = null;
    }

    protected void finalize() {
        close();
    }
}
