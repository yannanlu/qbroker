package org.qbroker.receiver;

/* JDBCReceiver.java - a JDBC receiver for JMS messages */

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.sql.SQLException;
import javax.jms.JMSException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JDBCMessenger;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * JDBCReceiver monitors a JDBC data source and retrieves the records
 * from it via a query.  It puts the JMS Messages to an XQueue as the output.
 * JDBCReceiver supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JDBCReceiver extends Receiver {
    private JDBCMessenger db = null;
    private List[] dependencyGroup = null;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false;

    public JDBCReceiver(Map props) {
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
        if ("jdbc".equals(scheme)) {
            try {
                db = new JDBCMessenger(props);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(
                    "failed to create jdbc connection: "+ Event.traceStack(e)));
            }
            operation = db.getOperation();
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if ((o = props.get("DependencyGroup")) != null && o instanceof List) {
            dependencyGroup = MonitorUtils.getDependencies((List) o);
            MonitorUtils.checkDependencies(System.currentTimeMillis(),
                dependencyGroup, uri);
        }

        if (!"select".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
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
                db.reconnect();
                isConnected = true;
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                dbOperation(xq, baseTime);
                if (dependencyGroup == null)
                    setStatus(RCVR_STOPPED);

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
    private int dbOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        String str;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            db.select(xq);
            if (dependencyGroup != null) {
                currentTime = System.currentTimeMillis();
                previousTime = currentTime + baseTime;
                while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0){
                    if ((mask & XQueue.PAUSE) > 0 || // paused temporarily
                        status != RCVR_RUNNING)      // status changed
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception ex) {
                    }
                    currentTime = System.currentTimeMillis();
                    if (previousTime <= currentTime) { // time is up
                        previousTime = currentTime + baseTime;
                        i = MonitorUtils.checkDependencies(currentTime,
                            dependencyGroup, uri);
                        if (i == MonitorReport.NOSKIP)
                            db.select(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (SQLException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

                if ((str = db.reconnect()) != null) {
                    new Event(Event.ERR, linkName + ": failed to reconnect "+
                        uri + " after " + i + " retries: " + str).send();
                    continue;
                }
                new Event(Event.INFO, uri + " reconnected on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                sessionTime = System.currentTimeMillis();
                isConnected = true;
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (JMSException e) {
            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;
            str = linkName + " " + uri + ": ";
            Exception ex = e.getLinkedException();
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
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

                if ((str = db.reconnect()) != null) {
                    new Event(Event.ERR, linkName + ": failed to reconnect "+
                        uri + " after " + i + " retries: " + str).send();
                    continue;
                }
                new Event(Event.INFO, uri + " reconnected on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                sessionTime = System.currentTimeMillis();
                isConnected = true;
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
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
        if (db != null) try {
            db.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, linkName + ": failed to " +
                "close "+uri+": "+Event.traceStack(e)).send();
        }
    }

    public void close() {
        setStatus(RCVR_CLOSED);
        disconnect();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (db != null)
            db = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
