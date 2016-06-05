package org.qbroker.receiver;

/* DocumentReceiver.java - a document receiver for JMS messages */

import java.util.Map;
import java.util.List;
import java.util.Date;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.RunCommand;
import org.qbroker.common.PatternFilenameFilter;
import org.qbroker.common.NonSQLException;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MongoDBMessenger;
import org.qbroker.jms.RiakMessenger;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * DocumentReceiver monitors a file or page on a server and retrieves the
 * document from it.  The retrieved document will be packed into a
 * message which will be put to an XQueue as the output. DocumentReceiver
 * supports flow control and allows object control from its owner.  It is
 * fault tolerant with retry and idle options.
 *<br/><br/>
 * Currently, it only supports MongoDB and Riak.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DocumentReceiver extends Receiver {
    private MongoDBMessenger mongo = null;
    private RiakMessenger riak = null;
    private List[] dependencyGroup = null;
    private int type;
    private int retryCount;
    private long sessionTime, startTime = 0L;
    private boolean isConnected = false;
    private final static int DOC_MONGO = 1;
    private final static int DOC_RIAK = 2;

    public DocumentReceiver(Map props) {
        super(props);
        String scheme = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("uri is not defined"));

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
                    throw(new IllegalArgumentException("failed to create " +
                        "mongodb connection: "+ Event.traceStack(e)));
            }
            operation = mongo.getOperation();
        }
        else if ("riak".equals(scheme)) {
            type = DOC_RIAK;
            try {
                riak = new RiakMessenger(props);
            }
            catch (Exception e) {
                    throw(new IllegalArgumentException("failed to create " +
                        "riak connection: "+ Event.traceStack(e)));
            }
            operation = riak.getOperation();
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if ((o = props.get("DependencyGroup")) != null && o instanceof List) {
            dependencyGroup = MonitorUtils.getDependencies((List) o);
            MonitorUtils.checkDependencies(System.currentTimeMillis(),
                dependencyGroup, uri);
        }

        if ((type == DOC_MONGO && !"findone".equals(operation)) ||
            (type == DOC_RIAK && !"fetch".equals(operation))) {
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
                isConnected = true;
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                switch (type) {
                  case DOC_MONGO:
                    mongoOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case DOC_RIAK:
                    riakOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  default:
                    setStatus(RCVR_STOPPED);
                    break;
                }

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
    private int mongoOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            mongo.findone(xq);
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
                            mongo.findone(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (NonSQLException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
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

                if ((str = mongo.reconnect()) != null) {
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

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int riakOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            riak.fetch(xq);
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
                            riak.fetch(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (NonSQLException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
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

                if ((str = riak.reconnect()) != null) {
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
        setStatus(RCVR_CLOSED);
        disconnect();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (mongo != null)
            mongo = null;
        if (riak != null)
            riak = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
