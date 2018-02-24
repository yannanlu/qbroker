package org.qbroker.persister;

/* AWSRequester.java - an AWS Requester for JMS messages */

import java.util.Map;
import java.io.IOException;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.Service;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.AWSMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * FilePersister listens to an XQueue and receives JMS Messages
 * from it.  Either it stores the JMS Messages on a remote server as files,
 * or it downloads files from a remote server according to the content of
 * the messages and puts back the messages with the downloaded content in
 * their body.  FilePersister supports flow control and allows object control
 * from its owner.  It is fault tolerant with retry and idle options.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class AWSRequester extends Persister {
    private AWSMessenger aws = null;
    private int retryCount, maxNumberMsg;
    private Template template = null;
    private long sessionTime;
    private boolean isConnected = false;

    public AWSRequester(Map props) {
        super(props);
        try {
            aws = new AWSMessenger(props);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to create aws client: "+
                Event.traceStack(e)));
        }
        operation = getOperation();

        if (!"display".equals(operation)) {
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
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
            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                awsOperation(xq, baseTime);
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

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int awsOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("display".equals(operation))
                aws.display(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + uri +
                ": " + e.getMessage()).send();
            return 0;
        }
        catch (IOException e) {
            String str;
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
                if (i <= maxRetry) {
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            Exception ex = null;
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            String str = linkName + " " + uri + ": ";
            if (e instanceof JMSException)
                ex = ((JMSException) e).getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();

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
                if (i <= maxRetry) {
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
            }
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == PSTR_RUNNING &&
            (mask & XQueue.STANDBY) == 0) // job is done
            setStatus(PSTR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        if (aws != null) {
            aws.close();
            aws = null;
        }
    }

    protected void finalize() {
        close();
    }
}
