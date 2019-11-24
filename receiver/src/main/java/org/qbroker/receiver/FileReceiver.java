package org.qbroker.receiver;

/* FileReceiver.java - a file receiver for JMS messages */

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.RunCommand;
import org.qbroker.common.PatternFilenameFilter;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageStream;
import org.qbroker.jms.FTPMessenger;
import org.qbroker.jms.FTPSMessenger;
import org.qbroker.jms.SFTPMessenger;
import org.qbroker.jms.HTTPMessenger;
import org.qbroker.jms.NNTPMessenger;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * FileReceiver monitors a file or page on a server and retrieves the file
 * from it.  It can also run a script and packs the output to the message.
 * The JMS Messages will be put to an XQueue as the output. Currently,
 * FileReceiver supports various types of data sources, such as file, log, ftp,
 * ftps, http, https, sftp and shell script. FileReceiver also supports flow
 * control and allows object control from its owner.  It is fault tolerant with
 * retry and idle options.
 *<br><br>
 * In case of the script, please make sure to redirect the STDERR to STDOUT
 * or close the STDERR, since FileReceiver only reads from its STDOUT.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class FileReceiver extends Receiver {
    private FTPMessenger ftp = null;
    private FTPSMessenger ftps = null;
    private SFTPMessenger sftp = null;
    private HTTPMessenger http = null;
    private NNTPMessenger nntp = null;
    private Process proc = null;
    private MessageStream ms = null;
    private File inputFile = null;
    private File referenceFile = null;
    private FilenameFilter filter = null;
    private InputStream in = null;
    private List[] dependencyGroup = null;
    private String[] cmdArray = null;
    private int type;
    private int retryCount;
    private long sessionTime, startTime = 0L;
    private boolean isConnected = false;
    private final static int FILE_FILE = 1;
    private final static int FILE_FTP = 2;
    private final static int FILE_FTPS = 3;
    private final static int FILE_SFTP = 4;
    private final static int FILE_HTTP = 5;
    private final static int FILE_NNTP = 6;
    private final static int FILE_SCRIPT = 7;

    public FileReceiver(Map props) {
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
        if ("file".equals(scheme)) {
            type = FILE_FILE;
            String filename = (String) props.get("InputFile");
            if (filename == null && u.getPath() != null)
                filename = u.getPath();

            inputFile = new File(filename);
            if (!inputFile.isDirectory()) {
                try {
                    in = (InputStream) new FileInputStream(inputFile);
                }
                catch (IOException e) {
                    throw(new IllegalArgumentException(inputFile + ": " +
                        Event.traceStack(e)));
                }
            }
            else if ((o = props.get("Pattern")) != null) {
                filter = new PatternFilenameFilter((String) o);
                if ((o = props.get("ReferenceFile")) != null)
                    referenceFile = new File((String) o);

                // load startTime
                if (referenceFile != null && referenceFile.exists()) {
                    String str = null;
                    byte[] buffer = new byte[1024];
                    int n, len = 0;
                    try {
                        FileInputStream is = new FileInputStream(referenceFile);
                        while ((n = is.read(buffer, len, 1024 - len)) >= 0) {
                            len += n;
                            if (len >= 1024)
                                break;
                        }
                        is.close();
                        str = new String(buffer, 0, len);
                    }
                    catch (IOException e) {
                    }
                    if (str != null && str.length() > 0) { // load startTime
                        n = str.indexOf(" ");
                        startTime = Long.parseLong(str.substring(0, n));
                    }
                }
                else
                    startTime = System.currentTimeMillis();
            }
            else
                throw(new IllegalArgumentException("no pattern defined"));

            ms = new MessageStream(props);
            operation = ms.getOperation();
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
            String script = (String) props.get("Script");
            if (script == null)
                throw(new IllegalArgumentException("Script not defined"));
            cmdArray = RunCommand.parseCmd(script);
            ms = new MessageStream(props);
            operation = ms.getOperation();
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if ((o = props.get("DependencyGroup")) != null && o instanceof List) {
            dependencyGroup = MonitorUtils.getDependencies((List) o);
            MonitorUtils.checkDependencies(System.currentTimeMillis(),
                dependencyGroup, uri);
        }

        if (!("retrieve".equals(operation) || "fetch".equals(operation))) {
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
                  case FILE_FILE:
                    if (filter == null) try {
                        in = (InputStream) new FileInputStream(inputFile);
                    }
                    catch (Exception e) {
                    }
                    break;
                  case FILE_HTTP:
                    http.reconnect();
                    break;
                  case FILE_SCRIPT:
                  default:
                    break;
                }
                isConnected = true;
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                switch (type) {
                  case FILE_FILE:
                    fileOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case FILE_FTP:
                    ftpOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case FILE_FTPS:
                    ftpsOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case FILE_SFTP:
                    sftpOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case FILE_HTTP:
                    httpOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case FILE_NNTP:
                    nntpOperation(xq, baseTime);
                    break;
                  case FILE_SCRIPT:
                    scriptOperation(xq, baseTime);
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

    private int fileOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (filter == null)
                ms.read(in, xq);
            else
                pickup(xq);
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
                        if (i == MonitorReport.NOSKIP) { // updated
                            if (filter == null) {
                                try {
                                    in.close();
                                }
                                catch (IOException ex) {
                                }
                                in=(InputStream) new FileInputStream(inputFile);
                                ms.read(in, xq);
                            }
                            else
                                pickup(xq);
                        }
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
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
                catch (Exception ex) {
                }

                if (i > maxRetry)
                    continue;

                if (in != null) try {
                    in.close();
                }
                catch (IOException ex) {
                }
                if (filter == null) try {
                    in = (InputStream) new FileInputStream(inputFile);
                }
                catch (IOException ex) {
                    new Event(Event.ERR, linkName + ": failed to open " + uri +
                        " after " +i+ " retries: "+Event.traceStack(ex)).send();
                    continue;
                }
                new Event(Event.INFO, uri + " reopened on " + linkName +
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
            String str = uri + ": ";
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

    private void pickup(XQueue xq) throws JMSException, IOException {
        int i, n;
        long tm;
        String key;
        File[] list;
        if (xq == null || filter == null)
            return;
        list = inputFile.listFiles(filter);
        n = list.length;
        for (i=0; i<n; i++) {
            if (list[i] == null)
                continue;
            tm = list[i].lastModified();
            if (tm <= startTime)
                continue;
            key = list[i].getName();
            try {
                in = (InputStream) new FileInputStream(list[i]);
            }
            catch (Exception e) {
                new Event(Event.ERR, linkName + ": failed to open " + key +
                    ": " + Event.traceStack(e)).send();
                continue;
            }
            
            ms.read(in, xq);
            startTime = tm;
            if (referenceFile != null) try {
                FileWriter out = new FileWriter(referenceFile);
                out.write(tm + " " + list[i].getName() + "\n");
                out.flush();
                out.close();
            }
            catch (Exception e) {
                new Event(Event.ERR,linkName+" failed to write state info to "+
                     referenceFile.getPath() + " for " + key + ": " +
                     Event.traceStack(e)).send();
            }
            try {
                in.close();
            }
            catch (Exception e) {
            }
        }
    }

    private int httpOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            http.retrieve(xq);
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
                        if (i == MonitorReport.NOSKIP) // updated
                            http.retrieve(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
            String str;
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

                if ((str = http.reconnect()) != null) {
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
        catch (Exception e) {
            Exception ex = null;
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
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

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }
                if (i <= maxRetry) {
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                    sessionTime = System.currentTimeMillis();
                    return --retryCount;
                }
            }
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
    private int ftpOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            ftp.retrieve(xq);
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
                            ftp.retrieve(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
            String str;
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

                if ((str = ftp.reconnect()) != null) {
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
    private int ftpsOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            ftps.retrieve(xq);
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
                            ftps.retrieve(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
            String str;
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

                if ((str = ftps.reconnect()) != null) {
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
    private int sftpOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            sftp.retrieve(xq);
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
                            sftp.retrieve(xq);
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
            String str;
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

                if ((str = sftp.reconnect()) != null) {
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
    private int nntpOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if ("fetch".equals(operation))
                nntp.fetch(xq);
        }
        catch (IOException e) {
            String str;
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

                if ((str = nntp.reconnect()) != null) {
                    new Event(Event.ERR, linkName + ": failed to reconnect "+
                        uri + " after " + i + " retries: "  + str).send();
                    continue;
                }
                new Event(Event.INFO, uri + " reconnected on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
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
            setStatus(RCVR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private int scriptOperation(XQueue xq, int baseTime) {
        int i = 0, mask;
        long currentTime, previousTime;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (proc == null) {
                proc = Runtime.getRuntime().exec(cmdArray, null);
                in = proc.getInputStream();
            }

            ms.read(in, xq);
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
                        if (i == MonitorReport.NOSKIP) { // updated
                            try {
                                in.close();
                                if (proc != null)
                                    proc.destroy();
                                proc = null;
                            }
                            catch (Exception ex) {
                                if (proc != null)
                                    proc.destroy();
                                proc = null;
                            }
                            proc = Runtime.getRuntime().exec(cmdArray, null);
                            in = proc.getInputStream();
                            ms.read(in, xq);
                        }
                        else if (i == MonitorReport.EXCEPTION)
                            new Event(Event.WARNING, uri +
                                ": dependencies failed with " + i).send();
                    }
                }
            }
        }
        catch (IOException e) {
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
                catch (Exception ex) {
                }

                if (i > maxRetry)
                    continue;

                try {
                    in.close();
                    if (proc != null)
                        proc.destroy();
                    proc = null;
                }
                catch (Exception ex) {
                    if (proc != null)
                        proc.destroy();
                    proc = null;
                }
                new Event(Event.INFO, uri + " reopened on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
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
            String str = uri + ": ";
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
            }
            break;
          case FILE_FILE:
            if (in != null) try {
                in.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + ": failed to " +
                    "close "+uri+": "+Event.traceStack(e)).send();
            }
            break;
          case FILE_SCRIPT:
            if (proc != null) try {
                proc.destroy();
            }
            catch (Exception e) {
            }
            if (in != null) try {
                in.close();
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
          default:
            break;
        }
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (in != null)
            in = null;
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
    }

    protected void finalize() {
        close();
    }
}
