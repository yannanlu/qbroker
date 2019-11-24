package org.qbroker.persister;

/* DocumentPersister.java - a document persister for JMS messages */

import java.util.Map;
import java.util.Date;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.NonSQLException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MongoDBMessenger;
import org.qbroker.jms.RiakMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * DocumentPersister listens to an XQueue and receives JMS Messages
 * from it.  Either it stores the JMS Messages on a remote server as documents,
 * or it downloads documents from a remote server according to the content of
 * the messages and puts the content back to the messages in their body.
 * DocumentPersister supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br><br>
 * Currently, it only supports MongoDB and Riak.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DocumentPersister extends Persister {
    private MongoDBMessenger mongo = null;
    private RiakMessenger riak = null;
    private String rcField = "ReturnCode", scriptField = null;
    private int type;
    private int retryCount, maxNumberMsg;
    private Template template = null;
    private long sessionTime;
    private boolean isConnected = false;
    private final static int DOC_MONGO = 1;
    private final static int DOC_RIAK = 2;

    public DocumentPersister(Map props) {
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

        if ("mongodb".equals(scheme)) {
            type = DOC_MONGO;
            try {
                mongo = new MongoDBMessenger(props);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to create mongodb "+
                    "connection: "+ Event.traceStack(e)));
            }
            operation = mongo.getOperation();
        }
        else if ("riak".equals(scheme)) {
            type = DOC_RIAK;
            try {
                riak = new RiakMessenger(props);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to create riak "+
                    "connection: "+ Event.traceStack(e)));
            }
            operation = riak.getOperation();
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if (type == DOC_RIAK && !"srore".equals(operation) &&
            !"download".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }
        else if (!"find".equals(operation) && !"update".equals(operation) &&
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
            if (!isConnected && status != PSTR_DISABLED) {
                switch (type) {
                  case DOC_MONGO:
                    mongo.reconnect();
                    break;
                  case DOC_RIAK:
                    riak.reconnect();
                    break;
                  default:
                    break;
                }
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                switch (type) {
                  case DOC_MONGO:
                    mongoOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case DOC_RIAK:
                    riakOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  default:
                    setStatus(PSTR_STOPPED);
                    break;
                }

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
    private int mongoOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("find".equals(operation))
                mongo.find(xq);
            else if ("update".equals(operation))
                mongo.update(xq);
            else
                mongo.list(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + uri +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (NonSQLException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            String str;
            Exception ex = e.getLinkedException();
            if (ex != null)
                str = e.toString() + "\n" + Event.traceStack(ex);
            else
                str = Event.traceStack(e);
            new Event(Event.ERR, linkName + " " + uri + ": " + str).send();

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

                if ((str = mongo.reconnect()) != null) {
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
        catch (Exception e) {
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

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int riakOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("download".equals(operation))
                riak.download(xq);
            else
                riak.store(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + uri +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (NonSQLException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            String str;
            Exception ex = e.getLinkedException();
            if (ex != null)
                str = e.toString() + "\n" + Event.traceStack(ex);
            else
                str = Event.traceStack(e);
            new Event(Event.ERR, linkName + " " + uri + ": " + str).send();

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

                if ((str = riak.reconnect()) != null) {
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
            String str = linkName + " " + uri + ": ";
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
        isConnected = false;
        switch (type) {
          case DOC_MONGO:
            if (mongo != null) try {
                mongo.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case DOC_RIAK:
            if (riak != null) try {
                riak.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          default:
            break;
        }
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        disconnect();
        if (mongo != null)
            mongo = null;
        if (riak != null)
            riak = null;
    }

    protected void finalize() {
        close();
    }
}
