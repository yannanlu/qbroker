package org.qbroker.jms;

/* LogSlicer.java - a slicer truncating a growing logfile periodically */

import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.MalformedPatternException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.qbroker.common.DisabledException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.XQueue;
import org.qbroker.common.Utils;
import org.qbroker.net.ClientSocket;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.event.Event;

/**
 * LogSlicer slices a growing logfile into pieces dynamically
 * so that they can be havestered.
 *<br><br>
 * LogSlicer continuously monitors the size of the logfile and renames
 * the file into a new name with a sequential id if either the session
 * times out and the file size is none zero, or the size reaches the
 * threshold before the timeout.
 *<br><br>
 * It can be used to rotate logs as long as it is safe to truncate the
 * logs.  Some applications open the logfile to append and then close it
 * immediately.  Other applications keep the logfile open all the times.
 * It is safe to truncate the logfile in the former case.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class LogSlicer {
    private String operation = "slice";
    private String uri, host;
    private Socket sock = null;
    private InputStream in;
    private OutputStream out;
    private boolean isConnected = false, autoClose = false;
    private byte[][] queryRequest;
    private byte[][] newRequest;
    private byte[][] connRequest;
    private byte[][] quitRequest;
    private int heartbeat, port, textMode = 1;
    private long waitTime = 500L;
    private int timeout, sessionTimeout, xaMode, capacity;
    private String hostField, fileField, sizeField, storeField;
    private String logName, filename, stagingDir = null;
    private long maxFileSize = 4194304 * 1024;
    private byte[] buffer = new byte[0];
    private int[] partition;
    private int bufferSize, displayMask, threshold;
    private java.lang.reflect.Method ackMethod = null;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private Pattern ftpPattern = null;
    private Pattern ftpOrPattern = null;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private final static int LOG_SIZE = 0;
    private final static int LOG_TIME = 1;
    private final static int LOG_ID = 2;
    public final static int NOTCONNECTED = -1;
    public final static int CMDOK = 0;
    public final static int PROTOCOLERROR = 1;
    public final static int CMDFAILED = 2;
    public final static int FILENOTFOUND = 3;
    public final static int CLIENTERROR = 4;
    public final static int SERVERERROR = 5;
    public final static int READFAILED = 6;
    public final static int WRITEFAILED = 7;
    public final static int CONNTIMEOUT = 8;
    public final static int CONNREFUSED = 9;

    public LogSlicer(Map props) {
        Object o;
        URI u;
        int i;
        String username, password;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MessageUtils.substitute((String) o);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"ftp".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: "+u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 21;

        if ((host = u.getHost()) == null || host.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((logName = u.getPath()) == null || logName.length() == 0)
            throw(new IllegalArgumentException("no path specified in URI"));

        if ((i = logName.lastIndexOf("/")) >= 0)
            filename = logName.substring(i+1);
        else
            filename = logName;

        if ((o = props.get("Username")) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        else {
            username = (String) o;

            password = null;
            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null)
                throw(new IllegalArgumentException("Password is not defined"));
        }

        if ((o = props.get("AutoClose")) != null &&
            "true".equals(((String) o).toLowerCase()))
            autoClose = true;
        else
            autoClose = false;
        if ((o = props.get("Operation")) != null)
            operation = (String) o;

        if ((o = props.get("PropertyMap")) != null && o instanceof Map) {
            hostField = (String) ((Map) o).get("HostName");
            fileField = (String) ((Map) o).get("FileName");
            sizeField = (String) ((Map) o).get("FileSize");
            storeField = (String) ((Map) o).get("LocalStore");
        }
        if (hostField == null || hostField.length() == 0)
            hostField = "HostName";
        if (fileField == null || fileField.length() == 0)
            fileField = "FileName";
        if (sizeField == null || sizeField.length() == 0)
            sizeField = "FileSize";
        if (storeField == null || storeField.length() == 0)
            storeField = "LocalStore";

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value, cellID;
            Template temp = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            cellID = (String) props.get("CellID");
            if (cellID == null || cellID.length() <= 0)
                cellID = "0";
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if ((propertyName[n] = MessageUtils.getPropertyID(key)) == null)
                    propertyName[n] = key;
                if (value != null && value.length() > 0) {
                    propertyValue[n] = temp.substitute("CellID", cellID, value);
                }
                n ++;
            }
        }

        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else if ((o = props.get("CellID")) != null) {
            partition = new int[2];
            partition[0] = Integer.parseInt((String) o);
            partition[1] = 1;
        }
        else {
            partition = new int[2];
            partition[0] = 0;
            partition[1] = 0;
        }
        if (props.get("MaxFileSize") != null) {
            maxFileSize = Long.parseLong((String) props.get("MaxFileSize"));
        }
        if ((o = props.get("StagingDir")) != null && ((String)o).length() > 0)
            stagingDir = (String) o + System.getProperty("file.separator");

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        else
            heartbeat = 30000;

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000*Integer.parseInt((String) o);
        else
            sessionTimeout = 60000;

        if ((o = props.get("WaitTime")) != null) {
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 500L;
        }

        if (props.get("Threshold") != null)
            threshold = Integer.parseInt((String) props.get("Threshold"));
        else
            threshold = 1;

        if (props.get("TextMode") != null)
            textMode = Integer.parseInt((String) props.get("TextMode"));
        else
            textMode = 1;

        if (props.get("XAMode") != null)
            xaMode = Integer.parseInt((String) props.get("XAMode"));
        else
            xaMode = 0;

        if (props.get("DisplayMask") != null)
            displayMask = Integer.parseInt((String) props.get("DisplayMask"));
        else
            displayMask = 0;

        if (props.get("Capacity") != null)
            capacity = Integer.parseInt((String) props.get("Capacity"));
        else
            capacity = 0;

        if ((xaMode & MessageUtils.XA_CLIENT) > 0) {
            Class<?> cls = this.getClass();
            try {
                ackMethod = cls.getMethod("acknowledge",
                    new Class[] {long[].class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("found no ack method"));
            }
        }

        connRequest = new byte[3][];
        queryRequest = new byte[2][];
        newRequest = new byte[2][];
        quitRequest = new byte[1][];
        connRequest[0] = new String("USER " + username + "\r\n").getBytes();
        connRequest[1] = new String("PASS " + password + "\r\n").getBytes();
        connRequest[2] = new String("TYPE I\r\n").getBytes();
        queryRequest[0] = new String("SIZE " + logName + "\r\n").getBytes();
        queryRequest[1] = new String("MDTM " + logName + ".0\r\n").getBytes();
        newRequest[0] = new String("RNFR " + logName + "\r\n").getBytes();
        newRequest[1] = new String("RNTO " + logName + ".0\r\n").getBytes();
        quitRequest[0] = new String("QUIT" + "\n").getBytes();

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            pattern = pc.compile("(^220 .+\\r\\n|\\n220 .+\\r\\n)");
            ftpPattern = pc.compile("^(\\d\\d\\d) (.+)\\r$");
            ftpOrPattern = pc.compile("\\n(\\d\\d\\d) (.+)\\r$");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("BufferSize")) == null ||
            (bufferSize = Integer.parseInt((String) o)) <= 0)
            bufferSize = 4096;

        buffer = new byte[bufferSize];
    }

    /**
     * connect to the remote FTP server via a client socket
     * and orthenticate via username/password
     */
    private synchronized int connect() {
        long timeStart;
        int i, totalBytes, bytesRead, timeLeft, retCode, wt;
        int[] responseCode = new int[3];
        String[] responseText = new String[3];
        String response;
        StringBuffer strBuf = new StringBuffer();
        boolean hasNew = false;

        totalBytes = 0;
        wt = (int) waitTime;
        if (sock != null)
            disconnect();

        timeStart = System.currentTimeMillis();
        try {
            sock = ClientSocket.connect(host, port, timeout);
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to connect on " + uri +
                ": " + NOTCONNECTED + " " + e.toString()).send();
            return NOTCONNECTED;
        }
        timeLeft = timeout - (int) (System.currentTimeMillis() - timeStart);
        if (sock == null) {
            if (timeLeft <= 0) {
                new Event(Event.WARNING, "connection failed on " + uri +
                    ": " + CONNTIMEOUT + " timeout").send();
                return CONNTIMEOUT;
            }
            else {
                new Event(Event.WARNING, "connection failed on " + uri +
                    ": " + CONNREFUSED + " refused").send();
                return CONNREFUSED;
            }
        }
        if (timeLeft <= 0) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + CONNTIMEOUT + " timeout").send();
            disconnect();
            return CONNTIMEOUT;
        }
        try {
            sock.setSoTimeout(timeout);
            in = sock.getInputStream();
            out = sock.getOutputStream();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to set socket on " + uri +
                ": " + NOTCONNECTED + " " + e.toString()).send();
            disconnect();
            return NOTCONNECTED;
        }
        if (in == null || out == null) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + NOTCONNECTED + " null IO Streams").send();
            disconnect();
            return NOTCONNECTED;
        }

        try {
            do { // read everything until EOF or pattern hit or timeout
                bytesRead = 0;
                hasNew = false;
                if (sock != null) try {
                    sock.setSoTimeout(wt);
                }
                catch (Exception ex) {
                }
                try {
                    while ((bytesRead = in.read(buffer, 0, bufferSize)) > 0) {
                        strBuf.append(new String(buffer, 0, bytesRead));
                        totalBytes += bytesRead;
                        hasNew = true;
                        for (i=bytesRead-1; i>=0; i--) // check newline
                            if (buffer[i] == '\n')
                                break;
                        if (i >= 0) // found the newline
                            break;
                    }
                }
                catch (InterruptedIOException ex) { // timeout or interrupt
                }
                if (hasNew && pm.contains(strBuf.toString(), pattern))
                    break;
                timeLeft = timeout-(int) (System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
        }
        catch (IOException e) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + READFAILED + " " + e.toString()).send();
            disconnect();
            return READFAILED;
        }
        if (timeLeft <= 0 || bytesRead < 0) {
            retCode = (bytesRead < 0) ? SERVERERROR : CONNTIMEOUT;
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + retCode + " " + timeLeft + ":" + bytesRead).send();
            disconnect();
            return retCode;
        }

        isConnected = true;
        retCode = send(connRequest, responseCode, responseText);
        if (retCode == CMDOK && responseCode[1] == 230)
            isConnected = true;
        else {
            disconnect();
            if (retCode == CMDOK)
                retCode = CMDFAILED;
            new Event(Event.ERR, "login failed for " + uri +
                ": " + retCode + " " + responseText[1]).send();
        }

        return retCode;
    }

    public void reconnect() {
        disconnect();
        connect();
    }

    private synchronized int send(byte[][] request, int[] responseCode,
        String[] responseText) {
        int i, j, totalBytes, bytesRead, timeLeft = 1, wt;
        String response;
        StringBuffer strBuf = new StringBuffer();
        long timeStart;
        boolean hasNew = false;

        if (!isConnected)
            return NOTCONNECTED;

        totalBytes = 0;
        wt = (int) waitTime;
        for (i=0; i<request.length; i++) {
            responseText[i] = null;
            responseCode[i] = -1;
        }

        timeStart = System.currentTimeMillis();
        for (i=0; i<request.length; i++) {
            strBuf.delete(0, strBuf.length());
            try {
                out.write(request[i]);
            }
            catch (IOException e) {
                responseCode[request.length-1] = WRITEFAILED;
                responseText[request.length-1] = e.toString();
                close();
                return WRITEFAILED;
            }

            try {
                do { // read everything until EOF or pattern hit or timeout
                    bytesRead = 0;
                    hasNew = false;
                    if (sock != null) try {
                        sock.setSoTimeout(wt);
                    }
                    catch (Exception ex) {
                    }
                    try {
                        while((bytesRead = in.read(buffer, 0, bufferSize)) > 0){
                            strBuf.append(new String(buffer,0,bytesRead));
                            totalBytes += bytesRead;
                            hasNew = true;
                            for (j=bytesRead-1; j>=0; j--) // check newline
                                if (buffer[j] == '\n')
                                    break;
                            if (j >= 0) // found the newline
                                break;

                        }
                    }
                    catch (InterruptedIOException ex) { // timeout or interrupt
                    }
                    if (hasNew &&
                        (pm.contains(strBuf.toString(), ftpPattern) ||
                        pm.contains(strBuf.toString(), ftpOrPattern)))
                        break;
                    timeLeft = timeout-(int) (System.currentTimeMillis()-
                        timeStart);
                } while (bytesRead >= 0 && timeLeft > 0);
            }
            catch (Exception e) {
                responseCode[request.length-1] = READFAILED;
                responseText[request.length-1] = e.toString();
                disconnect();
                return READFAILED;
            }
            if (timeLeft <= 0 || bytesRead < 0) {
                disconnect();
                return ((bytesRead < 0) ? SERVERERROR : READFAILED);
            }
            response = strBuf.toString();
            MatchResult mr = pm.getMatch();
            responseCode[i] = Integer.parseInt(mr.group(1));
            responseText[i] = mr.group(2);
            if (responseCode[i] >= 530 && responseCode[i] < 550) {
                disconnect();
                return SERVERERROR;
            }
            else if (responseCode[i] >= 400 && responseCode[i] < 500) {
                disconnect();
                return CLIENTERROR;
            }
        }

        return CMDOK;
    }

    /**
     * It sends the requests to the remove server to check the log size
     * frequently.  If the log size is large enough, it renames the log file
     * to a unique name and creates a JMS message with the full path of
     * the new log file.  Then it puts the message to the XQueue as a
     * download request.  The new message supports acknowledgement to
     * delete the new log file.
     */
    public void slice(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        int sid = -1, cid, mask = 0;
        int i, k = 0, l, n, retCode, retry = 0;
        long st = 0, length, currentTime, count = 0;
        long size, mtime, loopTime, sessionTime;
        int[] responseCode = new int[3];
        String[] responseText = new String[3];
        String line, msgStr;
        StringBuffer strBuf;
        boolean xa = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

        switch (len) {
          case 0:
            do { // reserve an empty cell
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            do { // reserve the empty cell
                sid = xq.reserve(waitTime, shift);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            do { // reserve a partitioned empty cell
                sid = xq.reserve(waitTime, shift, len);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
        }

        if (sid >= 0) {
            if (textMode == 0)
                outMessage = new BytesEvent();
            else
                outMessage = new TextEvent();
            outMessage.clearBody();
            isText = (outMessage instanceof TextMessage);
        }
        else if ((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0) {
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return;
        }
        else {
            return;
        }

        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        loopTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            // check the size and timestamp
            queryRequest[1]=new String("MDTM " + logName + "." + sid +
                "\r\n").getBytes();
            retCode = send(queryRequest, responseCode, responseText);
            size = -1L;
            if (retCode == CMDOK) { // query is OK
                retry = 0;
                if (responseCode[0] == 213) // got size
                    size = Long.parseLong(responseText[0]);
            }
            else { // query failed
                if (retCode == SERVERERROR || retCode == CLIENTERROR ||
                    retry > 2) {
                    xq.cancel(sid);
                    throw(new IOException("query failed on " + uri + ": " +
                        retCode + " " + responseText[1]));
                }
                else {
                    retry ++;
                    reconnect();
                    sessionTime = System.currentTimeMillis();
                }
            }
            currentTime = System.currentTimeMillis();
            if (size >= threshold || (size > 0 && // need to rename the log
                currentTime >= sessionTime + sessionTimeout)) {
                newRequest[1] = new String("RNTO " + logName + "." + sid +
                    "\r\n").getBytes();
                retCode = send(newRequest, responseCode, responseText);
                if (retCode == CMDOK) { // rename is done
                    if (propertyName != null && propertyValue != null) {
                        for (i=0; i<propertyName.length; i++)
                            MessageUtils.setProperty(propertyName[i],
                                propertyValue[i], outMessage);
                    }
                    outMessage.setStringProperty(fileField, logName +"."+ sid);
                    outMessage.setStringProperty(hostField, host);
                   outMessage.setStringProperty(sizeField,String.valueOf(size));
                    if (stagingDir != null && size >= maxFileSize)
                        outMessage.setStringProperty(storeField, stagingDir +
                            filename + "." + sid);
                    if (xa)
                        ((JMSEvent) outMessage).setAckObject(this, ackMethod,
                            new long[] {(long) size, currentTime, (long) sid});
                    outMessage.setJMSTimestamp(currentTime);
                    cid = xq.add(outMessage, sid);

                    if (cid > 0) {
                        count ++;
                        if (displayMask > 0)
                            new Event(Event.INFO, "sliced new log of " +
                                logName + "." + sid + " from " + uri).send();
                    }
                    else {
                        xq.cancel(sid);
                        new Event(Event.ERR, uri + " failed to add a msg at " +
                            sid + " to " + xq.getName()).send();
                        // need a way to rollback
                    }
                }
                else { // rename failed
                    xq.cancel(sid);
                    if (retCode != NOTCONNECTED) {
                        throw(new IOException("rename failed on " + uri +
                            ": " + retCode + " " + responseText[1]));
                    }
                    else {
                        reconnect();
                        sessionTime = System.currentTimeMillis();
                    }
                }

                sid = -1;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0) switch (len) {
                  case 0:
                    do { // reserve an empty cell
                        sid = xq.reserve(waitTime);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                  case 1:
                    do { // reserve the empty cell
                        sid = xq.reserve(waitTime, shift);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                  default:
                    do { // reserve a partitioned empty cell
                        sid = xq.reserve(waitTime, shift, len);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                }

                if (sid >= 0) {
                    if (isText)
                        outMessage = new TextEvent();
                    else
                        outMessage = new BytesEvent();
                    outMessage.clearBody();
                }
                else {
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0)
                        new Event(Event.ERR, "failed to reserve a cell on "+
                            xq.getName() + " for " + uri).send();
                    else if (displayMask != 0)
                        new Event(Event.INFO, "sliced " + count + " logs from "+
                            uri).send();
                    return;
                }
            }
            currentTime = System.currentTimeMillis();
            if (currentTime >= sessionTime + sessionTimeout) {
                sessionTime = currentTime;
                if (autoClose)
                    reconnect();
            }

            if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) == 0)
                break;
            else if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            if (heartbeat > 0 && heartbeat > currentTime - loopTime) {
                st = heartbeat + loopTime - currentTime;
                try {
                    Thread.sleep(st);
                }
                catch (InterruptedException e) {
                }
            }
            loopTime = currentTime;
        }
        if (sid >= 0) {
            xq.cancel(sid);
        }
        if (displayMask != 0)
            new Event(Event.INFO, "sliced " + count + " logs from " +
                uri).send();
    }

    private synchronized void disconnect() {
        isConnected = false;
        if (out != null) try {
            out.write(quitRequest[0]);
        }
        catch (Exception ex) {
        }
        close();
    }

    public synchronized void acknowledge(long[] state) throws JMSException {
        int id, retCode;
        byte[][] deleteRequest = new byte[1][];
        int[] responseCode = new int[1];
        String[] responseText = new String[1];

        if (state == null)
            return;
        if (!isConnected)
            connect();

        id = (int) state[LOG_ID];
        deleteRequest[0] = new String("DELE "+logName+"."+id+"\n").getBytes();
        retCode = send(deleteRequest, responseCode, responseText);
        if (retCode != CMDOK) { // failed so retry
            String str = responseText[0];
            reconnect();
            retCode = send(deleteRequest, responseCode, responseText);
            if (retCode != CMDOK) { // failed once more
                disconnect();
                throw(new JMSException("failed to acknowledge msg (" +
                    state[LOG_SIZE] + " " + state[LOG_TIME] + " " +
                    state[LOG_ID] + "): " + str));
            }
        }
    }

    public void close() {
        if (out != null) {
            try {
                out.close();
            }
            catch (Exception e) {
            }
            out = null;
        }
        if (in != null) {
            try {
                in.close();
            }
            catch (Exception e) {
            }
            in = null;
        }
        if (sock != null) {
            try {
                sock.close();
            }
            catch (Exception e) {
            }
            sock = null;
        }
    }
}
