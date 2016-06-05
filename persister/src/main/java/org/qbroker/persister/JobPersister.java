package org.qbroker.persister;

/* JobPersister.java - a job persister with JMS messages */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.FTPMessenger;
import org.qbroker.jms.FTPSMessenger;
import org.qbroker.jms.SFTPMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * JobPersister listens to an XQueue and receives JMS Messages as the job
 * requests. There are two types job requests. The first one is the init
 * request with complete job information. The other is the request on the
 * current job such as a query of the progress data or a command to abort
 * the job, etc. Once the persister gets a job init message, it will initialize
 * the job and starts to process it. Usually, it takes a while to process a
 * job request. While the job is running, the persister is also listening to
 * the XQueue for messages. The most common messages are the queries for
 * progress data on the job. In this case, the persister will load the progress
 * data to the message and removes it from the queue. If the request is to
 * abort the running job, the persister will abort the job and resets the
 * return code. The message will be removed from the queue. In case the message
 * is another job init request while a job is running, the persister will just
 * reset the error code on the message and removes it from the queue. Once
 * the job is done, the job init message will be acknowledged and removed
 * from the queue.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JobPersister extends Persister {
    private FTPMessenger ftp = null;
    private FTPSMessenger ftps = null;
    private SFTPMessenger sftp = null;
    private String rcField = "ReturnCode", scriptField = null;
    private int type;
    private int retryCount, maxNumberMsg, maxIdleTime = 0;
    private Template template = null;
    private long sessionTime, sessionTimeout;
    private boolean isConnected = false;
    private final static int FILE_FILE = 1;
    private final static int FILE_FTP = 2;
    private final static int FILE_FTPS = 3;
    private final static int FILE_SFTP = 4;
    private final static int FILE_HTTP = 5;
    private final static int FILE_NNTP = 6;
    private final static int FILE_SCRIPT = 7;

    public JobPersister(Map props) {
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

        if ("ftp".equals(scheme)) {
            type = FILE_FTP;
            try {
                ftp = new FTPMessenger(props);
            }
            catch (Exception e) {
         throw(new IllegalArgumentException("failed to create ftp connection: "+
                    Event.traceStack(e)));
            }
            operation = ftp.getOperation();
        }
        else if ("ftps".equals(scheme)) {
            type = FILE_FTPS;
            try {
                ftps = new FTPSMessenger(props);
            }
            catch (Exception e) {
        throw(new IllegalArgumentException("failed to create ftps connection: "+
                    Event.traceStack(e)));
            }
            operation = ftp.getOperation();
        }
        else if ("sftp".equals(scheme)) {
            type = FILE_SFTP;
            try {
                sftp = new SFTPMessenger(props);
            }
            catch (Exception e) {
        throw(new IllegalArgumentException("failed to create sftp connection: "+
                    Event.traceStack(e)));
            }
            operation = sftp.getOperation();
        }
        else if ("script".equals(scheme)) {
            type = FILE_SCRIPT;
            if ((o = props.get("RCField")) != null)
                rcField = (String) o;
            if ((o = props.get("ScriptField")) != null)
                scriptField = (String) o;
            if ((o = props.get("Operation")) != null)
                operation = (String) o;
            if ((o = props.get("DisplayMask")) != null)
                displayMask = Integer.parseInt((String) o);
            else
                displayMask = 0;
            if ((o = props.get("MaxIdleTime")) != null) {
                maxIdleTime = 1000 * Integer.parseInt((String) o);
                if (maxIdleTime < 0)
                    maxIdleTime = 0;
            }
            if ((o = props.get("MaxNumberMsg")) != null)
                maxNumberMsg = Integer.parseInt((String) o);
            else
                maxNumberMsg = 0;
            if ((o = props.get("SessionTimeout")) != null)
                sessionTimeout = 1000 * Integer.parseInt((String) o);
            else
                sessionTimeout = 300000;
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if (!"upload".equals(operation) && !"test".equals(operation)) {
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
                  case FILE_FTP:
                    ftp.reconnect();
                    break;
                  case FILE_FTPS:
                    ftps.reconnect();
                    break;
                  case FILE_SFTP:
                    sftp.reconnect();
                    break;
                  default:
                    break;
                }
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                switch (type) {
                  case FILE_FTP:
                    ftpOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case FILE_FTPS:
                    ftpsOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case FILE_SFTP:
                    sftpOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case FILE_SCRIPT:
                    scriptOperation(xq, baseTime);
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
    private int ftpOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            ftp.upload(xq);
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

                if (i > maxRetry)
                    continue;

                if ((str = ftp.reconnect()) != null) {
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

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int ftpsOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            ftps.upload(xq);
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

                if (i > maxRetry)
                    continue;

                if ((str = ftps.reconnect()) != null) {
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

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int sftpOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            sftp.upload(xq);
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

                if (i > maxRetry)
                    continue;

                if ((str = sftp.reconnect()) != null) {
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

    private int scriptOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            test(xq);
        }
        catch (TimeoutException e) { // hibernate
            resetStatus(PSTR_RUNNING, PSTR_DISABLED);
            new Event(Event.INFO, linkName + " hibernated on " + uri +
                ": " + e.getMessage()).send();
            return 0;
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

    /**
     * simulates long running jobs via sleep certain seconds. It will listen
     * to the request with two commands supported, query and abort.
     */
    private void test(XQueue xq) throws TimeoutException, JMSException {
        Message inMessage = null;
        Object o;
        int i, m, n, mask;
        int sid = -1, cid = -1;
        int dmask = MessageUtils.SHOW_DATE;
        int actionCount = 0;
        long currentTime, st, idleTime, baseTime, duration = 0, count = 0;
        String msgStr;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String badRC = String.valueOf(MessageUtils.RC_BADDATA);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        byte[] buffer = new byte[8192];

        dmask ^= displayMask;
        dmask &= displayMask;

        m = 0;
        n = 0;
        currentTime = System.currentTimeMillis();
        baseTime = currentTime;
        idleTime = currentTime;
        st = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0)
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0 && cid < 0) {
                if (m++ == 0) {
                    st = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - st >= sessionTimeout) {
                        st = currentTime;
                    }
                    m = 0;
                }
                if (checkIdle) {
                    if (n++ == 0) {
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }

            n = 0;
            m = 0;
            currentTime = System.currentTimeMillis();

            if (sid >= 0) // new request
                o =  xq.browse(sid);
            else // no request
                o = null;

            if (o == null) { // either got a new request or no request
                if (sid >= 0) { // request is not supposed to be null
                    xq.remove(sid);
                    new Event(Event.WARNING, "dropped a null msg from " +
                        xq.getName()).send();
                    continue;
                }
            }
            else if (o instanceof ObjectEvent) { // request on current job
                Event event = (Event) o;
                actionCount ++;
                event.setAttribute("actionCount", String.valueOf(actionCount));
                event.setAttribute("duration", String.valueOf(duration/1000));
                event.setAttribute(rcField, "0");
                if (cid >= 0) { // active job
                    String str = "0";
                    long tm = currentTime - baseTime;
                    if (duration > 0)
                        str = String.valueOf((int) (tm * 100 / duration));
                    event.setAttribute("progress", str);
                    if ("abort".equals(event.getAttribute("operation"))) {
                        event.setAttribute("status", "ABORTED");
                        event.setPriority(Event.WARNING);
                        if (isWriteable) try {
                            MessageUtils.setProperty(rcField, reqRC, inMessage);
                        }
                        catch (Exception e) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "aborted to test process for "+
                            duration).send();
                        if (acked) try {
                            inMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        catch (Error ex) {
                            xq.remove(cid);
                            Event.flush(ex);
                        }
                        xq.remove(cid);
                        inMessage = null;
                        cid = -1;
                        continue;
                    }
                    else if (actionCount > 1)
                        event.setAttribute("status", "RUNNING");
                    else
                        event.setAttribute("status", "STARTED");
                    xq.remove(sid);
                    str = String.valueOf(tm/1000);
                    if (isWriteable) try {
                        MessageUtils.setProperty("TestTime", str, inMessage);
                    }
                    catch (Exception e) {
                    }
                }
                else { // no active transfer job yet
                    event.setAttribute("progress", "0");
                    event.setAttribute("status", "NOTFOUND");
                    xq.remove(sid);
                    continue;
                }
            }
            else if (cid >= 0) { // active job
                Message msg = (Message) o;

                // setting default RC
                if (isWriteable) try {
                    MessageUtils.setProperty(rcField, uriRC, msg);
                }
                catch (MessageNotWriteableException e) {
                    try {
                        MessageUtils.resetProperties(msg);
                        MessageUtils.setProperty(rcField, uriRC, msg);
                    }
                    catch (Exception ee) {
                        new Event(Event.WARNING,
                           "failed to set RC on msg from "+xq.getName()).send();
                        if (acked) try {
                            msg.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        catch (Error ex) {
                            xq.remove(sid);
                            Event.flush(ex);
                        }
                    }
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to set RC on msg " +
                        "with active job from " + xq.getName()).send();
                    if (acked) try {
                        msg.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                }
                xq.remove(sid);
                count ++;
                new Event(Event.WARNING, "active job for " + uri +
                    " and dropped the incoming init msg from " +
                    xq.getName()).send();
            }
            else { // new request to init transfer job
                inMessage = (Message) o;
                cid = sid;
                baseTime = currentTime;
                actionCount = 0;

                // setting default RC
                if (isWriteable) try {
                    MessageUtils.setProperty(rcField, msgRC, inMessage);
                }
                catch (MessageNotWriteableException e) {
                    try {
                        MessageUtils.resetProperties(inMessage);
                        MessageUtils.setProperty(rcField, msgRC, inMessage);
                    }
                    catch (Exception ex) {
                        new Event(Event.WARNING,
                           "failed to set RC on msg from "+xq.getName()).send();
                        if (acked) try {
                            inMessage.acknowledge();
                        }
                        catch (Exception ee) {
                        }
                        catch (Error ee) {
                            xq.remove(cid);
                            Event.flush(ee);
                        }
                        xq.remove(cid);
                        inMessage = null;
                        cid = -1;
                        continue;
                    }
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to set RC on msg from " +
                        xq.getName()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(cid);
                        Event.flush(ex);
                    }
                    xq.remove(cid);
                    inMessage = null;
                    cid = -1;
                    continue;
                }

                try {
                    msgStr = MessageUtils.getProperty(scriptField, inMessage);
                    if (isWriteable)
                        MessageUtils.setProperty(rcField, badRC, inMessage);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to get properties on " +
                        "msg from " + xq.getName() + ": " +e.toString()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(cid);
                        Event.flush(ex);
                    }
                    xq.remove(cid);
                    inMessage = null;
                    cid = -1;
                    continue;
                }

                try {
                    duration = 1000 * Long.parseLong(msgStr);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to parse duration on " +
                        "msg from " + xq.getName() + ": " +e.toString()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(cid);
                        Event.flush(ex);
                    }
                    xq.remove(cid);
                    inMessage = null;
                    cid = -1;
                    continue;
                }

                if (duration <= 0) { // bad request
                    new Event(Event.WARNING, "bad duration in msg from " +
                        xq.getName()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(cid);
                        Event.flush(ex);
                    }
                    xq.remove(cid);
                    inMessage = null;
                    cid = -1;
                    continue;
                }
            }

            // process job
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.STANDBY) == 0) try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }

            currentTime = System.currentTimeMillis();
            if (currentTime - baseTime < duration) // job not done yet
                continue;

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after job done on "+
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after job done on "+
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(cid);
                new Event(Event.ERR,"failed to ack msg after job done on "+
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }

            xq.remove(cid);

            if (displayMask > 0) try {
                String line = MessageUtils.display(inMessage, null,
                    displayMask, propertyName);
                new Event(Event.INFO, "tested a msg with duration of " +
                    duration + " ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "tested a msg with duration of " +
                    duration).send();
            }
            count ++;
            inMessage = null;
            cid = -1;
            duration = 0;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "tested " +count+ " msgs on " + uri).send();
    }

    private void disconnect() {
        isConnected = false;
        switch (type) {
          case FILE_FTP:
            if (ftp != null) try {
                ftp.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case FILE_FTPS:
            if (ftps != null) try {
                ftps.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case FILE_SFTP:
            if (sftp != null) try {
                sftp.close();
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
        setStatus(PSTR_CLOSED);
        disconnect();
        if (ftp != null)
            ftp = null;
        if (ftps != null)
            ftps = null;
        if (sftp != null)
            sftp = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
