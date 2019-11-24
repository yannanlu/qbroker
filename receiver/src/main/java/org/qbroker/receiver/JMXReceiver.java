package org.qbroker.receiver;

/* JMXReceiver.java - a receiver for listening on JMX notifications */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import javax.management.JMException;
import org.qbroker.common.Connector;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.jms.MessageUtils;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * JMXReceiver listens on JMX notifications from a JMX service and puts
 * notification content to JMS messages.  Those messages will be
 * sent into an XQueue as the output.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JMXReceiver extends Receiver {
    private Connector conn = null;
    private java.lang.reflect.Method method = null;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false;

    public JMXReceiver(Map props) {
        super(props);
        String scheme = null;
        String className = null;
        Class<?> cls;
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

        if ("tcp".equals(scheme)) {
            className = "org.qbroker.sonicmq.SonicMQMessenger";
            if (operation == null || operation.length() <= 0)
                operation = "listen";
        }
        else if ((o = props.get("PluginClassName")) == null) {
            throw(new IllegalArgumentException("no plugin defined for: " +uri));        }
        else if (operation == null || operation.length() <= 0) {
            throw(new IllegalArgumentException("no operation defined for: " +
                uri));
        }
        else {
            className = (String) o;
        }

        try {
            cls = Class.forName(className);
            method = cls.getMethod(operation, new Class[]{XQueue.class});
        }
        catch (Exception e) {
            String str = "failed to get method of '" + operation +
                "' on " + className + " for " + uri + ":";
            throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }

        try {
            java.lang.reflect.Constructor con;
            con = cls.getConstructor(new Class[]{Map.class});
            conn = (Connector) con.newInstance(new Object[]{props});
        }
        catch (Exception e) {
            String str = "failed to instantiate " + className + " for " +
                uri + ":";
            throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

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

        for (;;) {
            if (!isConnected && status != RCVR_DISABLED) {
                conn.reconnect();
                isConnected = true;
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                performOperation(xq, baseTime);

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
                if (isConnected && Utils.getOutstandingSize(xq, partition[0],
                    partition[1]) <= 0) // safe to disconnect
                    disconnect();
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
                if (isConnected) // disconnect anyway
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
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        disconnect();
        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int performOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            method.invoke(conn, new Object[]{xq});
        }
        catch (java.lang.reflect.InvocationTargetException e) { // error
            Throwable t = e.getTargetException();
            if (t == null) { // unexpected failure
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(e)).send();
                setStatus(RCVR_STOPPED);
                return -1;
            }
            else if (t instanceof JMException) { // JMX failures
                resetStatus(RCVR_RUNNING, RCVR_RETRYING);
                String str = linkName + " " + uri + ": ";
                new Event(Event.ERR, str + Event.traceStack(t)).send();
                if (status != RCVR_RETRYING) // no need to retry
                    return 0;

                if (System.currentTimeMillis() - sessionTime > timeout)
                    retryCount = 1;
                else
                    retryCount ++;

                while (keepRunning(xq) && status == RCVR_RETRYING) {
                    i = retryCount++;
                    if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                        i = (i - 1) % repeatPeriod + 1;
                    if (retryCount > 2) try {
                        Thread.sleep(standbyTime);
                    }
                    catch (Exception e1) {
                    }

                    if (i > maxRetry)
                        continue;

                    if ((str = conn.reconnect()) != null) {
                        new Event(Event.ERR, linkName+": failed to reconnect "+
                            uri + " after " + i + " retries: " + str).send();
                        continue;
                    }
                    new Event(Event.INFO, uri + " reconnected on " + linkName+
                        " after " + i + " retries").send();
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
                sessionTime = System.currentTimeMillis();
                return retryCount;
            }
            else if (t instanceof JMSException) { // message failures
                if (System.currentTimeMillis() - sessionTime > timeout)
                    retryCount = 1;
                else
                    retryCount ++;
                String str = linkName + " " + uri + ": ";
                Exception ex = ((JMSException) t).getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex + "\n";
                i = retryCount;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 1) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }
                if (i == 1 || i == maxRetry)
                    new Event(Event.ERR, str + Event.traceStack(t)).send();
                sessionTime = System.currentTimeMillis();
                return retryCount;
            }
            else { // unknown failures
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(t)).send();
                setStatus(RCVR_STOPPED);
                return -1;
            }
        }
        catch (Exception e) { // unknown failures
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.STANDBY) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private void disconnect() {
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        isConnected = false;
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (method != null)
            method = null;
        if (conn != null)
            conn = null;
    }

    protected void finalize() {
        close();
    }
}
