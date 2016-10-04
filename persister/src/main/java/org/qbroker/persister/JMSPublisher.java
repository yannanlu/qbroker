package org.qbroker.persister;

/* JMSPublisher.java - a JMS consumer */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TopicConnector;
import org.qbroker.jms.MessageInputStream;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * JMSPublisher listens to an XQueue and receives JMS messages
 * from it.  It publishes the JMS Messages to a JMS Topic as the output.
 * JMSPublisher supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMSPublisher extends Persister {
    private TopicConnector tConn = null;
    private String tName, msgID;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false;
    private boolean isSplitting = false;

    @SuppressWarnings("unchecked")
    public JMSPublisher(Map props) {
        super(props);
        Object o;
        String className = null;

        if (uri == null) {
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
        if (!"pub".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }

        if (props.containsKey("EOTBytes") || props.containsKey("SOTBytes")) {
            operation = "split";
            isSplitting = true;
        }

        isConnected = true;
        new Event(Event.INFO, tName+" opened and ready to " + operation +
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
            if (!isConnected && status != PSTR_DISABLED) {
                str = tConn.reconnect();
                isConnected = (str == null);
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                topicOperation(xq, baseTime);
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
                    new Event(Event.INFO, tName + " is " + // state changed
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
                new Event(Event.INFO, tName+" restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        disconnect();
        new Event(Event.INFO, tName + " stopped on " + linkName).send();
    }

    /**
     * real implementation of persist() with exception handling and retry
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
            else if (isSplitting)
                split(xq);
            else
                tConn.pub(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + tName +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (JMSException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            String str = linkName + " " + tName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            if (status != PSTR_RETRYING) // no need to retry
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
                if (tConn.tReopen() == 0) {
                    new Event(Event.INFO, tName + " reopened to " + operation +
                        " on " + linkName).send();
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return retryCount;
                }
                retryCount ++;
                if (!keepRunning(xq) || status != PSTR_RETRYING)
                    break;
            }
            isConnected = false;
            if (tolerance > 0) {
                new Event(Event.ERR, linkName + ": failed to open " + tName +
                    " after " + tolerance + " retries").send();
            }
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

                if ((str = tConn.reconnect()) == null) {
                    new Event(Event.INFO, tName + " reconnected on " + linkName+
                        " after " + i + " retries").send();
                    isConnected = true;
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
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

    /**
     * splits the content of the incoming message and publishes them on topic
     */
    private int split(XQueue xq) throws JMSException {
        int sid, count = 0, mask;
        Message message;
        InputStream bin = null;
        String id;

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // disabled temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                continue;
            }
            message = (Message) xq.browse(sid);
            if (message == null) { // msg is supposed not to be null
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": got a null message " +
                    "from " + xq.getName()).send();
                continue;
            }

            id = message.getJMSMessageID();
            try {
                if (message instanceof TextMessage) {
                    bin = new ByteArrayInputStream(
                        ((TextMessage) message).getText().getBytes());
                    tConn.pub(bin);
                }
                else if (message instanceof BytesMessage) {
                    bin = new MessageInputStream((BytesMessage) message);
                    tConn.pub(bin);
                }
                else {
                    tConn.getTopicPublisher().publish(message);
                }
            }
            catch (JMSException e) {
                xq.putback(sid);
                try {
                    if (bin != null)
                        bin.close();
                }
                catch (Exception ex) {
                }
                throw(e);
            }
            catch (IOException e) {
                new Event(Event.WARNING, uri + ": failed to split "+
                    " message of " + id + ": " + e.toString()).send();
            }
            msgID = id;
            xq.remove(sid);
            try {
                if (bin != null)
                    bin.close();
            }
            catch (Exception ex) {
            }
            bin = null;
            count ++;
            message = null;
        }
        return count;
    }

    private void disconnect() {
        isConnected = false;
        if (tConn != null)
            tConn.close();
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, tName + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        disconnect();
        if (tConn != null)
            tConn = null;
    }

    protected void finalize() {
        close();
    }
}
