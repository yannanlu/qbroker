package org.qbroker.persister;

/* JMXPersister.java - a JMX persister */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.management.JMException;
import org.qbroker.common.Connector;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageInputStream;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * JMXPersister listens to an XQueue and receives JMS messages from it.
 * It converts each JMS Message into a JMX request and sends the request
 * to the specified JMX Service.  Once it gets the response back, the content
 * of the response will be put into the message body as the response.
 *<br/><br/>
 * Currently, it only supports PCF, IMQ, QMF and JMX requests by default. 
 * It also supports customized plugins via PluginClassName and Operation.
 * The plugin is supposed to implement org.qbroker.common.Connector and the
 * name of the method to be invoked must be same as the value of Operation.
 * JMXPersister only knows how to handle TimeoutException, JMSException and
 * JMException. TimeoutException is for hibernate, JMSException for failures
 * on message operations, JMException for failures related to the remote
 * datasource, such as network issue, service not available, etc. Therefore,
 * if JMException is caught, JMXPersister will keep retrying until either the
 * failure is resolved or the flow is paused.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMXPersister extends Persister {
    private Connector conn = null;
    private String msgID;
    private int retryCount;
    private long sessionTime;
    private java.lang.reflect.Method method = null;
    private boolean isConnected = false;

    @SuppressWarnings("unchecked")
    public JMXPersister(Map props) {
        super(props);
        Class<?> cls;
        Object o;
        URI u;
        String scheme = null, className = null;

        if (uri == null) { // default for pcf
            String hostName = (String) props.get("HostName");
            if (hostName != null && hostName.length() > 0) {
                uri = "pcf://" + hostName;
                if (props.get("Port") != null)
                    uri += ":" + (String) props.get("Port");
            }
            else
                uri = "pcf://";
            if (props.get("QueueManager") != null)
                uri += "/" + props.get("QueueManager");
            else
                uri += "/";
            props.put("URI", uri);
        }

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException("invalid uri: " + uri));
        }

        scheme = u.getScheme();

        if ("pcf".equals(scheme)) {
            className = "org.qbroker.wmq.PCFRequester";
            if (operation == null || operation.length() <= 0)
                operation = "display";
        }
        else if ("service".equals(scheme) || "imq".equals(scheme)) {
            className = "org.qbroker.jms.JMXMessenger";
            if (operation == null || operation.length() <= 0)
                operation = "display";
        }
        else if ("amqp".equals(scheme)) {
            className = "org.qbroker.jms.QMFMessenger";
            if (operation == null || operation.length() <= 0)
                operation = "display";
        }
        else if ("tcp".equals(scheme)) {
            className = "org.qbroker.sonicmq.SonicMQMessenger";
            if (operation == null || operation.length() <= 0)
                operation = "display";
        }
        else if ((o = props.get("PluginClassName")) == null) {
            throw(new IllegalArgumentException("no plugin defined for: " +uri));
        }
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
                if (conn != null)
                    conn.reconnect();
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                if (conn != null)
                    performOperation(xq, baseTime);
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
                    new Event(Event.INFO, uri + " is " + // state changed
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
                new Event(Event.INFO, uri+" restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        disconnect();
        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * real implementation of persist() with exception handling and retry
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
                setStatus(PSTR_STOPPED);
                return -1;
            }
            else if (t instanceof TimeoutException) { // hibernate
                resetStatus(PSTR_RUNNING, PSTR_DISABLED);
                new Event(Event.INFO, linkName + " hibernated on " +
                    uri + ": " + t.getMessage()).send();
                return 0;
            }
            else if (t instanceof JMException) { // JMX failures
                resetStatus(PSTR_RUNNING, PSTR_RETRYING);
                String str = linkName + " " + uri + ": ";
                new Event(Event.ERR, str + Event.traceStack(t)).send();
                if (status != PSTR_RETRYING) // no need to retry
                    return 0;

                if (System.currentTimeMillis() - sessionTime > timeout)
                    retryCount = 1;
                else
                    retryCount ++;

                while (keepRunning(xq) && status == PSTR_RETRYING) {
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
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
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
                setStatus(PSTR_STOPPED);
                return -1;
            }
        }
        catch (Exception e) { // unknown failures
            new Event(Event.ERR, linkName + " " + uri + ": " +
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
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        isConnected = false;
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
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
