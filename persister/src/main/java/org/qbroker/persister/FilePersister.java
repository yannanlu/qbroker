package org.qbroker.persister;

/* FilePersister.java - a file persister for JMS messages */

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
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageLogger;
import org.qbroker.jms.FTPMessenger;
import org.qbroker.jms.FTPSMessenger;
import org.qbroker.jms.SFTPMessenger;
import org.qbroker.jms.HTTPMessenger;
import org.qbroker.jms.NNTPMessenger;
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

public class FilePersister extends Persister {
    private FTPMessenger ftp = null;
    private FTPSMessenger ftps = null;
    private SFTPMessenger sftp = null;
    private HTTPMessenger http = null;
    private NNTPMessenger nntp = null;
    private MessageLogger msgLog = null;
    private String rcField = "ReturnCode", scriptField = null;
    private int type;
    private int retryCount, maxNumberMsg;
    private Template template = null;
    private long sessionTime;
    private boolean isConnected = false;
    private final static int FILE_FILE = 1;
    private final static int FILE_FTP = 2;
    private final static int FILE_FTPS = 3;
    private final static int FILE_SFTP = 4;
    private final static int FILE_HTTP = 5;
    private final static int FILE_NNTP = 6;
    private final static int FILE_SCRIPT = 7;

    public FilePersister(Map props) {
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

        if ("file".equals(scheme)) {
            type = FILE_FILE;
            msgLog = new MessageLogger(props);
            operation = msgLog.getOperation();
        }
        else if ("ftp".equals(scheme)) {
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
            operation = ftps.getOperation();
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
        else if ("nntp".equals(scheme)) {
            type = FILE_NNTP;
            try {
                nntp = new NNTPMessenger(props);
            }
            catch (Exception e) {
        throw(new IllegalArgumentException("failed to create nntp connection: "+
                    Event.traceStack(e)));
            }
            operation = nntp.getOperation();
        }
        else if ("http".equals(scheme) || "https".equals(scheme)) {
            type = FILE_HTTP;
            try {
                http = new HTTPMessenger(props);
            }
            catch (Exception e) {
        throw(new IllegalArgumentException("failed to create http connection: "+
                    Event.traceStack(e)));
            }
            operation = http.getOperation();
        }
        else if ("script".equals(scheme)) {
            type = FILE_SCRIPT;
            if ((o = props.get("RCField")) != null)
                rcField = (String) o;
            if ((o = props.get("ScriptField")) != null)
                scriptField = (String) o;
            if ((o = props.get("Operation")) != null)
                operation = (String) o;
            if ((o = props.get("Template")) != null) {
                String str = (String) o;
                template = new Template(str);
            }
            if ((o = props.get("DisplayMask")) != null)
                displayMask = Integer.parseInt((String) o);
            else
                displayMask = 0;
            if ((o = props.get("MaxNumberMsg")) != null)
                maxNumberMsg = Integer.parseInt((String) o);
            else
                maxNumberMsg = 0;
            if ((o = props.get("StringProperty")) != null && o instanceof Map) {
                String key, value, cellID;
                Template temp = new Template("##CellID##");
                Iterator iter = ((Map) o).keySet().iterator();
                int n = ((Map) o).size();
                propertyName = new String[n];
                cellID = (String) props.get("CellID");
                if (cellID == null || cellID.length() <= 0)
                    cellID = "0";
                n = 0;
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if ((propertyName[n]=MessageUtils.getPropertyID(key))==null)
                        propertyName[n] = key;
                    n ++;
                }
            }
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if (!"store".equals(operation) && !"download".equals(operation) &&
            !"post".equals(operation) && !"retrieve".equals(operation) &&
            !"list".equals(operation) && !"execute".equals(operation) &&
            !"query".equals(operation) && !"copy".equals(operation) &&
            !"fulfill".equals(operation)) {
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
                  case FILE_NNTP:
                    nntp.reconnect();
                    break;
                  case FILE_HTTP:
                    http.reconnect();
                    break;
                  case FILE_FILE:
                  case FILE_SCRIPT:
                  default:
                    break;
                }
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                switch (type) {
                  case FILE_FILE:
                    fileOperation(xq, baseTime);
                    checkIdle = false;
                    break;
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
                  case FILE_HTTP:
                    httpOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case FILE_NNTP:
                    nntpOperation(xq, baseTime);
                    checkIdle = true;
                    break;
                  case FILE_SCRIPT:
                    scriptOperation(xq, baseTime);
                    checkIdle = false;
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

    private int fileOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("store".equals(operation))
                msgLog.append(xq);
            else
                msgLog.download(xq);
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

    private int httpOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("store".equals(operation))
                http.store(xq);
            else if ("download".equals(operation))
                http.download(xq);
            else
                http.fulfill(xq);
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

                if ((str = http.reconnect()) != null) {
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
     * real implementation of persist() with exception handling and retry
     */
    private int ftpOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("store".equals(operation))
                ftp.store(xq);
            else if ("list".equals(operation))
                ftp.list(xq);
            else if ("copy".equals(operation))
                ftp.copy(xq);
            else
                ftp.download(xq);
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
            if ("store".equals(operation))
                ftps.store(xq);
            else if ("list".equals(operation))
                ftps.list(xq);
            else if ("copy".equals(operation))
                ftps.copy(xq);
            else
                ftps.download(xq);
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
            if ("store".equals(operation))
                sftp.store(xq);
            else if ("list".equals(operation))
                sftp.list(xq);
            else if ("copy".equals(operation))
                sftp.copy(xq);
            else
                sftp.download(xq);
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

    /**
     * real implementation of persist() with exception handling and retry
     */
    private int nntpOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("post".equals(operation))
                nntp.post(xq);
            else if ("list".equals(operation))
                nntp.list(xq);
            else
                nntp.retrieve(xq);
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

                if ((str = nntp.reconnect()) != null) {
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
            execute(xq);
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
     * It gets a JMS Message from the XQueue and executes the script retrieved
     * from it continuously.  If XAMode has XA_CLIENT bit enabled, the incoming
     * message will be required to be writeable.  In this case, the method
     * will set a String property of the message specified by the RCField with
     * the return code, indicating the status of the execution on the script.
     * The requester is required to check the value of the property once it
     * gets the message back.  If the return code is 0, the execution is
     * successful.
     */
    public void execute(XQueue xq) throws JMSException {
        Message outMessage;
        byte[] buffer = new byte[4096];
        long currentTime, tm;
        long count = 0;
        int sid = -1;
        int size = 0, mask;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String output, script;
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        currentTime = System.currentTimeMillis();
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0)
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                continue;
            }

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (acked) try {
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    catch (Error ee) {
                        xq.remove(sid);
                        Event.flush(ee);
                    }
                    xq.remove(sid);
                    continue;
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                if (acked) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                continue;
            }

            script = null;
            tm = 0L;
            try {
                if (isWriteable)
                    script = MessageUtils.getProperty(scriptField, outMessage);
                if (script == null) {
                    if (template != null)
                        script = MessageUtils.format(outMessage, buffer,
                            template);
                    else
                        script = MessageUtils.processBody(outMessage, buffer);
                }
                if (isWriteable)
                    MessageUtils.setProperty(rcField, reqRC, outMessage);
                tm = outMessage.getJMSExpiration();
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm <= 0L) // default timeout
                tm = timeout;
            else if (tm > currentTime) // override timeout
                tm -= currentTime;
            else { // timeout already
                tm -= currentTime;
                new Event(Event.ERR, uri + ": msg already timed out with " +
                    tm + "ms: " + script).send();
                if (acked) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                continue;
            }
            if (script == null || script.length() <= 0) {
                new Event(Event.ERR, uri + ": failed to get " +
                    "script from msg").send();
                if (acked) try {
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                continue;
            }

            try {
                output = RunCommand.exec(script, (int) tm);
            }
            catch (Exception e) {
                if (e instanceof TimeoutException)
                    new Event(Event.ERR, uri + ": script timed out on " + tm +
                        "ms: " + Event.traceStack(e)).send();
                else
                    new Event(Event.ERR, uri + ": script failed with: " +
                        Event.traceStack(e)).send();
                if (acked) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                continue;
            }

            if (output == null)
                output = "";

            size = output.length();
            try {
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(output);
                    if (isWriteable)
                        MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(output.getBytes());
                    if (isWriteable)
                        MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else if (scriptField != null) {
                    MessageUtils.setProperty(scriptField, output, outMessage);
                    if (isWriteable)
                        MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else {
                    if (isWriteable) try {
                        MessageUtils.setProperty(rcField, msgRC, outMessage);
                    }
                    catch (Exception ex) {
                    }
                    if (acked) try {
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    continue;
                }
                if (isWriteable)
                    outMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                if (acked) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with output " +
                    " from " + script +": " + Event.traceStack(e)).send();
                continue;
            }

            if (acked) try {
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after update on "+
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after update on "+
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after update on "+
                    uri + ": " + Event.traceStack(e)).send();
                Event.flush(e);
            }

            xq.remove(sid);

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, script,
                    displayMask, propertyName);
                new Event(Event.INFO, "executed script with output of " +
                    size + " bytes for a msg ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "executed script with output of " +
                    size + " bytes for a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0)
            new Event(Event.INFO, "executed " + count + " scripts").send();
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
          case FILE_NNTP:
            if (nntp != null) try {
                nntp.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case FILE_HTTP:
            if (http != null) try {
                http.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case FILE_FILE:
          default:
            break;
        }
    }

    public void close() {
        setStatus(PSTR_CLOSED);
        disconnect();
        if (msgLog != null)
            msgLog = null;
        if (ftp != null)
            ftp = null;
        if (ftps != null)
            ftps = null;
        if (sftp != null)
            sftp = null;
        if (nntp != null)
            nntp = null;
        if (http != null)
            http = null;

        new Event(Event.INFO, uri + " closed on " + linkName).send();
    }
}
