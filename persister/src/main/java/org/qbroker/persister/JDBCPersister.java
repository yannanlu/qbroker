package org.qbroker.persister;

/* JDBCPersister.java - a JDBC persister for JMS messages */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.sql.SQLException;
import javax.jms.JMSException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JDBCMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * JDBCPersister listens to an XQueue and receives JMS Messages
 * from it.  Either it updates the JDBC data source based on the JMS Message,
 * or it queries for data records from the JDBC data source according to
 * the content of the messages and puts back the messages with the data content
 * to their body. JDBCPersister supports flow control and allows object control
 * from its owner.  It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JDBCPersister extends Persister {
    private JDBCMessenger db = null;
    private int retryCount;
    private long sessionTime;
    private boolean isConnected = false;

    public JDBCPersister(Map props) {
        super(props);
        String scheme = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("uri not defined"));

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
                throw(new IllegalArgumentException("failed to create jdbc " +
                    "connection: " + Event.traceStack(e)));
            }
            operation = db.getOperation();
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if (!"update".equals(operation) && !"query".equals(operation) &&
            !"list".equals(operation)) {
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
                db.reconnect();

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                dbOperation(xq, baseTime);
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
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
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
    private int dbOperation(XQueue xq, int baseTime) {
        String str;
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("update".equals(operation))
                db.update(xq);
            else if ("query".equals(operation))
                db.query(xq);
            else
                db.list(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + uri +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (SQLException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == PSTR_RETRYING) {
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
                resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                sessionTime = System.currentTimeMillis();
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
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == PSTR_RETRYING) {
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
                resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                sessionTime = System.currentTimeMillis();
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
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
        if (db != null) try {
            db.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, linkName + ": failed to " +
                "close "+uri+": "+Event.traceStack(e)).send();
        }
    }

    public void close() {
        setStatus(PSTR_CLOSED);
        disconnect();
        if (db != null)
            db = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
