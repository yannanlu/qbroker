package org.qbroker.jms;

/* SFTPMessenger.java - an SFTP messenger for JMS messages */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Map;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.SFTPConnector;
import org.qbroker.jms.MessageOutputStream;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * SFTPMessenger connects to a SSH server and initializes one of the operations,
 * such as retrieve, download and store.  It carries out the operation with
 * JMS Messages.
 *<br><br>
 * There are seven methods, retrieve(), download(), list(), store(), copy(),
 * upload() and pickup().  The first method, retrieve(), is used to retrieve
 * files from SSH servers. The method of download() is used for synchronous
 * requests.  The method of store() is used for asynchronous storing content
 * to remote SSH servers. The method of copy() is for copying a file from
 * one SSH server to anther. Both upload() and pickup() are for large file
 * transfers with JobPersister.
 *<br><br>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT goal.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SFTPMessenger extends SFTPConnector {
    private String msgID = null;
    private int bufferSize = 4096;
    private Template template = null;

    private String path;
    private String hostField;
    private String fileField;
    private String sizeField;
    private String storeField;
    private String rcField;
    private int sessionTimeout = 300000;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;
    private int pathType = PATH_NONE;

    private String correlationID = null;
    private String operation = "store";
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int sleepTime = 0;

    private int mode = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int retry = 2;

    private int offtail = 0;
    private int offhead = 0;
    private StringBuffer textBuffer = null;
    private String stagingDir = null;
    private long maxFileSize = 4194304 * 1024;
    private byte[] sotBytes = new byte[0];
    private byte[] eotBytes = new byte[0];
    private int sotPosition = 0;
    private int eotPosition = 0;
    private int boundary = 0;
    private int[] partition;

    private String[] propertyName = null;
    private String[] propertyValue = null;

    private boolean check_body = false;
    private boolean check_jms = false;
    private boolean verifyDirectory = false;
    private final static int PATH_NONE = 0;
    private final static int PATH_STATIC = 1;
    private final static int PATH_DYNAMIC = 2;
    private final static int MS_SOT = 1;
    private final static int MS_EOT = 2;

    /** Creates new SFTPMessenger */
    public SFTPMessenger(Map props) throws JSchException {
        super(props);

        Object o;
        URI u;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((path = u.getPath()) == null || path.length() == 0)
            path = null;
        else
            pathType |= PATH_STATIC;

        if ((o = props.get("PropertyMap")) != null && o instanceof Map) {
            hostField = (String) ((Map) o).get("HostName");
            fileField = (String) ((Map) o).get("FileName");
            sizeField = (String) ((Map) o).get("FileSize");
            storeField = (String) ((Map) o).get("LocalStore");
            pathType |= PATH_DYNAMIC;
        }
        if (hostField == null || hostField.length() == 0)
            hostField = "HostName";
        if (fileField == null || fileField.length() == 0)
            fileField = "FileName";
        if (sizeField == null || sizeField.length() == 0)
            sizeField = "FileSize";
        if (storeField == null || storeField.length() == 0)
            storeField = "LocalStore";

        if ((o = props.get("RCField")) != null)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("VerifyDirectory")) != null &&
            "true".equals(((String) o).toLowerCase()))
            verifyDirectory = true;

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        else
            sessionTimeout = 300000;

        if ((o = props.get("Retry")) == null ||
            (retry = Integer.parseInt((String) o)) < 0)
            retry = 2;

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if ((o = props.get("Operation")) != null)
            operation = (String) props.get("Operation");
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
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        if ((o = props.get("Offhead")) != null)
            offhead = Integer.parseInt((String) o);
        if (offhead < 0)
            offhead = 0;
        if ((o = props.get("Offtail")) != null)
            offtail = Integer.parseInt((String) o);
        if (offtail < 0)
            offtail = 0;
        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberMessage")) != null)
            maxNumberMsg = Integer.parseInt((String) o);
        if ((o = props.get("MaxIdleTime")) != null) {
            maxIdleTime = 1000 * Integer.parseInt((String) o);
            if (maxIdleTime < 0)
                maxIdleTime = 0;
        }
        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        if ((o = props.get("WaitTime")) != null) {
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 500L;
        }
        if ((o = props.get("SleepTime")) != null)
            sleepTime= Integer.parseInt((String) o);

        boundary = 0;
        if ((o = props.get("EOTBytes")) != null) {
            eotBytes = MessageUtils.hexString2Bytes((String) o);
            if (eotBytes != null && eotBytes.length > 0)
                boundary += MS_EOT;
        }
        if ((o = props.get("SOTBytes")) != null) {
            sotBytes = MessageUtils.hexString2Bytes((String) o);
            if (sotBytes != null && sotBytes.length > 0)
                boundary += MS_SOT;
        }
        if ((o = props.get("MaxFileSize")) != null)
            maxFileSize =Long.parseLong((String) o);
        if ((o = props.get("StagingDir")) != null && ((String)o).length() > 0)
            stagingDir = (String) o + System.getProperty("file.separator");

        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0))
            check_body = true;

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

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * retrieve a file from sftp server and put it to an XQ
     */
    public void retrieve(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        String reply = null;
        StringBuffer strBuf = new StringBuffer();
        int i, sid = -1, cid, mask = 0;
        long count = 0;
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

        if (path == null)
            throw(new IOException("path is null: " + uri));

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
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return;
        }
        else {
            return;
        }

        if (! isConnected()) try {
            connect();
        }
        catch (Exception e) {
            xq.cancel(sid);
            new Event(Event.ERR, "failed to connect to " + uri +
                " for " + xq.getName()).send();
            reconnect();
        }
        catch (Error e) {
            xq.cancel(sid);
            Event.flush(e);
        }
        for (i=0; i<=retry; i++) {
            strBuf = new StringBuffer();
            try {
                reply = sftpGet(strBuf, path);
            }
            catch (Exception e) {
                reply = "" + e.toString();
            }
            catch (Error e) {
                xq.cancel(sid);
                new Event(Event.ERR, "failed to retrieve " + path + " from " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            if (reply == null)
                break;
            if (i == 0)
                new Event(Event.WARNING, "failed to retrieve " + path+" from "+
                    uri + ": " + reply).send();
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.PAUSE) > 0) { // temporarily paused
                xq.cancel(sid);
                new Event(Event.WARNING, "aborted to retrieve " + path+" from "+
                    uri + " due to being disabled").send();
                return;
            }
            try {
                Thread.sleep(receiveTime);
            }
            catch (InterruptedException e) {
            }
            if (i < retry && reconnect() != null)
                break;
        }
        if (reply != null) {
            xq.cancel(sid);
            throw(new IOException("failed to retrieve " + path + " from " +
                uri +" with " + i + " retries: " + reply));
        }

        try {
            if (propertyName != null && propertyValue != null) {
                outMessage.clearProperties();
                for (i=0; i<propertyName.length; i++)
                    MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], outMessage);
            }

            if (isText)
                ((TextMessage) outMessage).setText(strBuf.toString());
            else
            ((BytesMessage)outMessage).writeBytes(strBuf.toString().getBytes());

            outMessage.setJMSTimestamp(System.currentTimeMillis());
        }
        catch (JMSException e) {
            xq.cancel(sid);
            new Event(Event.WARNING, "failed to load msg with " + path +
                " retrieved from " + uri + ": " + Event.traceStack(e)).send();
            return;
        }

        cid = xq.add(outMessage, sid);
        if (cid >= 0) {
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, null);
                new Event(Event.INFO, "retrieved " + path + " of " +
                    strBuf.length()+ " bytes from " + uri + " to a msg (" +
                    line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "retrieved " + path + " of " +
                    strBuf.length()+ " bytes from " + uri + " to a msg").send();
            }
            count ++;
        }
        else {
            xq.cancel(sid);
        }
    }

    /**
     * It gets a JMS message from xq that contains a filename to be downloaded
     * and downloads the file via sftp.  If LocalStore is defined in the
     * message, the downloaded file will be stored into the local filename.
     * Otherwise, the content of the file will be put into the message body.
     * The message will be removed from the XQueue so that the requester at
     * the other end of the XQueue can easily process the content either out of
     * the local filename or out of the message.
     *<br><br>
     * Since the download operation relies on a request, this method will  
     * intercept the request and processes it. The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the download is successful.  Otherwise, the
     * download operation is failed. No matter what happens, it will not
     * acknowledge the message.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void download(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        MessageOutputStream out = null;
        BytesBuffer msgBuf = null;
        String filename, reply=null;
        String localFilename;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isLocal = false;
        boolean isBytes = false;
        boolean isSleepy = (sleepTime > 0);
        long currentTime, sessionTime, idleTime, size;
        long count = 0, stm = 10;
        int sid = -1;
        int i, m, n, mask;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            sessionTime = System.currentTimeMillis();

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is supposed not to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            filename = null;
            localFilename = null;
            try {
                filename = MessageUtils.getProperty(fileField, outMessage);
                if (filename == null) {
                    if (path == null) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "default path is null for " +
                            uri).send();
                        outMessage = null;
                        continue;
                    }
                    filename = path;
                }
                localFilename = MessageUtils.getProperty(storeField,
                    outMessage);
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get filename for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                Event.flush(e);
            }

            if (localFilename != null && localFilename.length() > 0) {
                isLocal = true;
                // make sure local dir exists
                if (verifyDirectory &&
                    (reply = getParent(localFilename)) != null) {
                    File dir = new File(reply);
                    if (!dir.exists()) {
                        for (i=0; i<=retry; i++) {
                            if (dir.exists())
                                break;
                            dir.mkdirs();
                        }
                    }
                    if (!dir.exists())
                        new Event(Event.WARNING, "failed to create dir " +
                            reply + " for " + localFilename).send();
                }
                for (i=0; i<=retry; i++) {
                    try {
                        reply = sftpGet(new File(localFilename), filename);
                    }
                    catch (Exception e) {
                        reply = "" + e.toString();
                    }
                    catch (Error e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "failed to download " + filename +
                            " from " + uri + ": " + e.toString()).send();
                        Event.flush(e);
                    }
                    if (reply == null)
                        break;
                    if (i == 0)
                        new Event(Event.WARNING, "failed to download " +
                            filename + " from " + uri + ": " + reply).send();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                        xq.putback(sid);
                        new Event(Event.WARNING, "aborted to download " +
                            filename + " from " + uri +
                            " due to being disabled").send();
                        return;
                    }
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                    if (i < retry && reconnect() != null)
                        break;
                }
            }
            else { // store the content to msg body
                isLocal = true;
                isBytes = (outMessage instanceof BytesMessage);
                if (isBytes)
                    out = new MessageOutputStream((BytesMessage) outMessage);
                else
                    msgBuf = new BytesBuffer();
                for (i=0; i<=retry; i++) {
                    try {
                        if (isBytes)
                            reply = sftpGet(out, filename);
                        else
                            reply = sftpGet(msgBuf, filename);
                    }
                    catch (Exception e) {
                        reply = "" + e.toString();
                    }
                    catch (Error e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "failed to download " + filename +
                            " from " + uri + ": " + e.toString()).send();
                        if (isBytes) {
                            out.close();
                            out = null;
                        }
                        else {
                            msgBuf.close();
                            msgBuf = null;
                        }
                        Event.flush(e);
                    }
                    if (reply == null)
                        break;
                    if (i == 0)
                        new Event(Event.WARNING, "failed to download " +
                            filename + " from " + uri + ": " + reply).send();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                        xq.putback(sid);
                        new Event(Event.WARNING, "aborted to download " +
                            filename + " from " + uri +
                            " due to being disabled").send();
                        if (isBytes) {
                            out.close();
                            out = null;
                        }
                        else {
                            msgBuf.close();
                            msgBuf = null;
                        }
                        return;
                    }
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                    if (i < retry && reconnect() != null)
                        break;
                    if (!isBytes)
                        msgBuf.reset();
                    else try {
                        outMessage.clearBody();
                    }
                    catch (JMSException e) {
                        new Event(Event.ERR, "failed to clear bod for " +
                            filename + ": " + e.toString()).send();
                        break;
                    }
                }
            }
            if (reply != null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to download " +
                    filename + " from " + uri + ": " + reply).send();
                if (!isLocal) {
                    if (isBytes) {
                        out.close();
                        out = null;
                    }
                    else {
                        msgBuf.close();
                        msgBuf = null;
                    }
                }
                throw(new IOException("failed to download " + filename +
                    " from " + uri + " with " + i + " retries: " + reply));
            }

            try {
                if (isLocal)
                    size = new File(localFilename).length();
                else if (isBytes) {
                    size = ((BytesMessage) outMessage).getBodyLength();
                    out.close();
                    out = null;
                }
                else if (outMessage instanceof TextMessage) {
                    size = msgBuf.getCount();
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(msgBuf.toString());
                    msgBuf.close();
                    msgBuf = null;
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    msgBuf.close();
                    msgBuf = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with " +
                    filename + " downloaded from " + uri +": " +
                    Event.traceStack(e)).send();
                outMessage = null;
                if (!isLocal) {
                    if (isBytes) {
                        if (out != null)
                            out.close();
                        out = null;
                    }
                    else {
                        if (msgBuf != null)
                            msgBuf.close();
                        msgBuf = null;
                    }
                }
                continue;
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, null,
                    displayMask, propertyName);
                new Event(Event.INFO, "downloaded " + filename + " of " +
                    size + " bytes from " + uri + " with a msg ("+ line +
                    " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "downloaded " + filename + " of " +
                    size + " bytes from " + uri + " with a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO,"downloaded "+count+" files from "+uri).send();
    }

    /**
     * It gets a JMS message from the uplink as the file transfer request that
     * is either an init request for the file transfer or a query request for
     * the progress of the file transfer. In case of the init file transfer
     * request, it initializes the transfer job and starts to transfer the
     * file. Meanwhile, it continues to listen to the xq for query request
     * or other commands on the transfer job. For each query request, it
     * loads the progress data and the status to the message and removes
     * the message from the uplink so that the other end is able to collect it.
     * When the transfer job completes, it cleans up the transfer job and
     * acknowledges the init message before removing it from the uplink. In case
     * that the incoming request is to abort the current transfer job, it will
     * stop the transfer process and cleans up the job. The init message will
     * be acknowledged and removed from the uplink.
     *<br><br>
     * Since the upload operation relies on requests, the method will intercept
     * the requests and processes them.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the upload is successful.  Otherwise, the message
     * body will contain the failure reason out of upload operation.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */

    public void upload(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage = null;
        Object o;
        File localFile = null;
        InputStream in = null;
        OutputStream out = null;
        int i, m, n, mask;
        int sid = -1, cid = -1;
        int actionCount = 0;
        int dmask = MessageUtils.SHOW_DATE;
        long count = 0, stm = 10;
        long currentTime, sessionTime, idleTime, size = 0, len, transferred = 0;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null;
        String filename = null, reply=null, localFilename;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String ioRC = String.valueOf(MessageUtils.RC_IOERROR);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String badRC = String.valueOf(MessageUtils.RC_BADDATA);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0 && cid < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            currentTime = System.currentTimeMillis();
            n = 0;
            m = 0;

            if (sid >= 0) // new request
                o = xq.browse(sid);
            else // no request
                o = null;

            if (o == null) { // either got a new request or no request
                if (sid >= 0) { // request is not supposed to be null
                    xq.remove(sid);
                    new Event(Event.WARNING, "dropped a null request from " +
                        xq.getName()).send();
                    continue;
                }
            }
            else if (o instanceof ObjectEvent) { // request on current job
                String str = "0";
                Event event = (Event) o;
                actionCount ++;
                event.setAttribute(sizeField, String.valueOf(size));
                if (size > 0)
                    str = String.valueOf((int) (transferred * 100 / size));
                event.setAttribute("progress", str);
                event.setAttribute("actionCount", String.valueOf(actionCount));
                event.setAttribute(rcField, "0");
                if (in != null && out != null) { // active transfer job
                    if ("abort".equals(event.getAttribute("operation"))) {
                        event.setAttribute("status", "ABORTED");
                        event.setPriority(Event.WARNING);
                        if (isWriteable) try {
                            MessageUtils.setProperty(rcField, reqRC, inMessage);
                        }
                        catch (Exception e) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "aborted to upload file to "+
                            uri + " for " + filename).send();
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
                        if (out != null) try {
                            out.close();
                        }
                        catch (Exception ex) {
                        }
                        out = null;
                        if (in != null) try {
                            in.close();
                        }
                        catch (Exception ex) {
                        }
                        in = null;
                        throw(new IOException("aborted to upload file to " +
                            uri + " for " + filename));
                    }
                    else if (actionCount > 1)
                        event.setAttribute("status", "RUNNING");
                    else
                        event.setAttribute("status", "STARTED");
                    xq.remove(sid);
                    str = String.valueOf(((long)(transferred/10000.0))/100.0);
                    if (isWriteable) try {
                        MessageUtils.setProperty(sizeField, str, inMessage);
                    }
                    catch (Exception e) {
                    }
                }
                else { // no active transfer job yet
                    event.setAttribute("status", "NOTFOUND");
                    xq.remove(sid);
                    continue;
                }
            }
            else if (in != null || out != null) { // a duplicat job request
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
                        "during active transfer from " + xq.getName()).send();
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
                new Event(Event.WARNING, "active transfer to " + uri +
                    " and dropped the incoming init msg from " +
                    xq.getName()).send();
            }
            else { // new request to init transfer job
                inMessage = (Message) o;
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
                    catch (Exception ee) {
                        new Event(Event.WARNING,
                           "failed to set RC on msg from "+xq.getName()).send();
                        if (acked) try {
                            inMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        catch (Error ex) {
                            xq.remove(sid);
                            Event.flush(ex);
                        }
                        xq.remove(sid);
                        inMessage = null;
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
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    continue;
                }

                cid = sid;
                filename = null;
                localFilename = null;
                try {
                    filename = MessageUtils.getProperty(fileField, inMessage);
                   localFilename=MessageUtils.getProperty(storeField,inMessage);
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

                if (filename == null)
                    filename = path;

                localFile = null;
                if (localFilename != null && localFilename.length() > 0)
                    localFile = new File(localFilename);
                if (localFile == null || !localFile.exists() ||
                    !localFile.canRead() || !localFile.isFile()) {
                    new Event(Event.ERR, uri + ": failed to open local file " +
                        localFilename).send();
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

                transferred = 0;
                try {
                    size = localFile.length();
                    in = new FileInputStream(localFile);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to open stream for "+
                        localFile + ": " + Event.traceStack(e)).send();
                }

                if (! isConnected()) try {
                    connect();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.putback(cid);
                    Event.flush(e);
                }

                reply = null;
                // make sure dir exist on the server
                if(verifyDirectory && (reply = getParent(filename)) != null)try{
                    reply = createDirectory(reply, retry);
                    if (reply != null)
                        new Event(Event.WARNING,"failed to create dir "+
                            reply + " for " + filename).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING,"failed to create dir for "+
                        filename + ": " + Event.traceStack(e)).send();
                }

                if (isWriteable) try {
                    MessageUtils.setProperty(rcField, ioRC, inMessage);
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, "failed to set RC_IOERROR before"+
                        " uploading on " + uri + " for " + filename).send();
                }

                try {
                    out = storeFileStream(filename);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to open stream for "+
                        filename + ": " + Event.traceStack(e)).send();
                    reply = e.toString();
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
                    out = null;
                    if (in != null) try {
                        in.close();
                    }
                    catch (Exception ex) {
                    }
                    in = null;
                    continue;
                }
                if (reply != null) {
                    new Event(Event.ERR, "failed to open stream for " +
                        filename + " with: " + reply).send();
                }
            }

            // copy stream
            len = 0;
            reply = null;
            while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
                if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                    new Event(Event.WARNING, "aborted to copy stream on " +
                        filename + " due to being disabled").send();
                    xq.putback(cid);
                    inMessage = null;
                    cid = -1;
                    if (in != null) try {
                        in.close();
                    }
                    catch (Exception ex) {
                    }
                    in = null;
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception ex) {
                    }
                    out = null;
                    return;
                }
                try {
                    len = Utils.copyStream(in, out, buffer, retry);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to copy stream on " +
                        filename + ": " + e.toString()).send();
                    reply = e.toString();
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
                    if (in != null) try {
                        in.close();
                    }
                    catch (Exception ex) {
                    }
                    in = null;
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception ex) {
                    }
                    out = null;
                    break;
                }
                catch (Error e) {
                    xq.remove(cid);
                    new Event(Event.ERR, "failed to store " +
                        filename + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (len >= 0) {
                    transferred += len;
                    if (transferred >= size)
                        break;
                    else if (xq.depth() > 0) // for new request
                        break;
                }
                else
                    break;
            }
            if (reply != null) {
                new Event(Event.ERR, "failed to copy stream for " +
                    filename + " with: " + reply).send();
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
                if (in != null) try {
                    in.close();
                }
                catch (Exception ex) {
                }
                in = null;
                if (out != null) try {
                    out.close();
                }
                catch (Exception ex) {
                }
                out = null;
                throw(new IOException("failed to upload to " + uri + " on " +
                    filename + " with only " + transferred + "/" + size +
                    " transferred: " + reply));
            }
            else if (len >= 0) // not end of stream yet
                continue;

            if (in != null) try {
                in.close();
            }
            catch (Exception e) {
            }
            in = null;
            if (out != null) try {
                out.close();
            }
            catch (Exception e) {
            }
            out = null;
            
            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for "+ filename + ": " + str +Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for "+ filename + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(cid);
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for " + filename + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt = Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "uploading on " + uri + " for " + filename).send();
            }
            xq.remove(cid);
            cid = -1;
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, null, dmask,
                    propertyName);
                reply = size + " bytes to "; 
                if ((displayMask & MessageUtils.SHOW_DATE) > 0) {
                    new Event(Event.INFO, "uploaded " + filename + " of " +
                        reply + uri + " with a msg ( Date: " + dt +
                        line + " )").send();
                }
                else { // no date
                    new Event(Event.INFO, "uploaded " + filename + " of " +
                        reply + uri + " with a msg (" +
                        line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, "uploaded " + filename + " of " +
                    size + " bytes to " + uri + " with a msg").send();
            }

            inMessage = null;
            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "uploaded " +count+ " msgs to " + uri).send();
    }

    /**
     * It gets a JMS message from the uplink as the file transfer request that
     * is either an init request for the file transfer or a query request for
     * the progress of the file transfer. In case of the init file transfer
     * request, it initializes the transfer job and starts to transfer the
     * file. Meanwhile, it continues to listen to the xq for query request
     * or other commands on the transfer job. For each query request, it
     * loads the progress data and the status to the message and removes
     * the message from the uplink so that the other end is able to collect it.
     * When the transfer job completes, it cleans up the transfer job and
     * acknowledges the init message before removing it from the uplink. In case
     * that the incoming request is to abort the current transfer job, it will
     * stop the transfer process and cleans up the job. The init message will
     * be acknowledged and removed from the uplink.
     *<br><br>
     * Since the pickup operation relies on requests, the method will intercept
     * the requests and processes them.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the pickup is successful.  Otherwise, the message
     * body will contain the failure reason out of upload operation.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */

    public void pickup(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage = null;
        Object o;
        File localFile = null;
        InputStream in = null;
        OutputStream out = null;
        int i, m, n, mask;
        int sid = -1, cid = -1;
        int actionCount = 0;
        int dmask = MessageUtils.SHOW_DATE;
        long count = 0, stm = 10;
        long currentTime, sessionTime, idleTime, size = 0, len, transferred = 0;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null;
        String filename = null, reply=null, localFilename;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String ioRC = String.valueOf(MessageUtils.RC_IOERROR);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String badRC = String.valueOf(MessageUtils.RC_BADDATA);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0 && cid < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            currentTime = System.currentTimeMillis();
            n = 0;
            m = 0;

            if (sid >= 0) // new request
                o = xq.browse(sid);
            else // no request
                o = null;

            if (o == null) { // either got a new request or no request
                if (sid >= 0) { // request is not supposed to be null
                    xq.remove(sid);
                    new Event(Event.WARNING, "dropped a null request from " +
                        xq.getName()).send();
                    continue;
                }
            }
            else if (o instanceof ObjectEvent) { // request on current job
                String str = "0";
                Event event = (Event) o;
                actionCount ++;
                event.setAttribute(sizeField, String.valueOf(size));
                if (size > 0)
                    str = String.valueOf((int) (transferred * 100 / size));
                event.setAttribute("progress", str);
                event.setAttribute("actionCount", String.valueOf(actionCount));
                event.setAttribute(rcField, "0");
                if (in != null && out != null) { // active transfer job
                    if ("abort".equals(event.getAttribute("operation"))) {
                        event.setAttribute("status", "ABORTED");
                        event.setPriority(Event.WARNING);
                        if (isWriteable) try {
                            MessageUtils.setProperty(rcField, reqRC, inMessage);
                        }
                        catch (Exception e) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "aborted to pickup file to "+
                            uri + " for " + filename).send();
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
                        if (out != null) try {
                            out.close();
                        }
                        catch (Exception ex) {
                        }
                        out = null;
                        if (in != null) try {
                            in.close();
                        }
                        catch (Exception ex) {
                        }
                        in = null;
                        throw(new IOException("aborted to pickup file to " +
                            uri + " for " + filename));
                    }
                    else if (actionCount > 1)
                        event.setAttribute("status", "RUNNING");
                    else
                        event.setAttribute("status", "STARTED");
                    xq.remove(sid);
                    str = String.valueOf(((long)(transferred/10000.0))/100.0);
                    if (isWriteable) try {
                        MessageUtils.setProperty(sizeField, str, inMessage);
                    }
                    catch (Exception e) {
                    }
                }
                else { // no active transfer job yet
                    event.setAttribute("status", "NOTFOUND");
                    xq.remove(sid);
                    continue;
                }
            }
            else if (in != null || out != null) { // a duplicate job request
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
                        "during active transfer from " + xq.getName()).send();
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
                new Event(Event.WARNING, "active transfer to " + uri +
                    " and dropped the incoming init msg from " +
                    xq.getName()).send();
            }
            else { // new request to init transfer job
                inMessage = (Message) o;
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
                    catch (Exception ee) {
                        new Event(Event.WARNING,
                           "failed to set RC on msg from "+xq.getName()).send();
                        if (acked) try {
                            inMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        catch (Error ex) {
                            xq.remove(sid);
                            Event.flush(ex);
                        }
                        xq.remove(sid);
                        inMessage = null;
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
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    continue;
                }

                cid = sid;
                filename = null;
                localFilename = null;
                try {
                    filename = MessageUtils.getProperty(fileField, inMessage);
                   localFilename=MessageUtils.getProperty(storeField,inMessage);
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

                if (filename == null)
                    filename = path;

                localFile = null;
                if (localFilename != null && localFilename.length() > 0)
                    localFile = new File(localFilename);
                else {
                    new Event(Event.ERR, uri +
                        ": local file is not defined").send();
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

                // make sure dir exist
                if (verifyDirectory &&
                    (reply = getParent(localFilename)) != null) {
                    File dir = new File(reply);
                    if (!dir.exists()) {
                        for (i=0; i<=retry; i++) {
                            if (dir.exists())
                                break;
                            dir.mkdirs();
                        }
                    }
                    if (!dir.exists())
                        new Event(Event.WARNING, "failed to create dir " +
                            reply + " for " + localFilename).send();
                }

                transferred = 0;
                try {
                    out = new FileOutputStream(localFile);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to open stream for "+
                        localFile + ": " + Event.traceStack(e)).send();
                    xq.remove(cid);
                    inMessage = null;
                    cid = -1;
                    continue;
                }

                if (! isConnected()) try {
                    connect();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.putback(cid);
                    Event.flush(e);
                }

                if (isWriteable) try {
                    MessageUtils.setProperty(rcField, ioRC, inMessage);
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, "failed to set RC_IOERROR before"+
                        " uploading on " + uri + " for " + filename).send();
                }

                reply = null;
                try {
                    size = getSize(filename);
                    in = retrieveFileStream(filename);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to open stream for "+
                        filename + ": " + Event.traceStack(e)).send();
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
                    out = null;
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception ex) {
                    }
                    out = null;
                    reply = e.toString();
                    continue;
                }
                if (reply != null) {
                    new Event(Event.ERR, "failed to open stream for " +
                        filename + " with: " + reply).send();
                }
            }

            // copy stream
            len = 0;
            reply = null;
            while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
                if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                    new Event(Event.WARNING, "aborted to copy stream on " +
                        filename + " due to being disabled").send();
                    xq.putback(cid);
                    inMessage = null;
                    cid = -1;
                    if (in != null) try {
                        in.close();
                    }
                    catch (Exception ex) {
                    }
                    in = null;
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception ex) {
                    }
                    out = null;
                    return;
                }
                try {
                    len = Utils.copyStream(in, out, buffer, retry);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to copy stream on " +
                        filename + ": " + e.toString()).send();
                    reply = e.toString();
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
                    if (in != null) try {
                        in.close();
                    }
                    catch (Exception ex) {
                    }
                    in = null;
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception ex) {
                    }
                    out = null;
                    break;
                }
                catch (Error e) {
                    xq.remove(cid);
                    new Event(Event.ERR, "failed to store " +
                        filename + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (len >= 0) {
                    transferred += len;
                    if (transferred >= size)
                        break;
                    else if (xq.depth() > 0) // for new request
                        break;
                }
                else
                    break;
            }
            if (reply != null) {
                new Event(Event.ERR, "failed to copy stream for " +
                    filename + " with: " + reply).send();
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
                if (in != null) try {
                    in.close();
                }
                catch (Exception ex) {
                }
                in = null;
                if (out != null) try {
                    out.close();
                }
                catch (Exception ex) {
                }
                out = null;
                throw(new IOException("failed to upload to " + uri + " on " +
                    filename + " with only " + transferred + "/" + size +
                    " transferred: " + reply));
            }
            else if (len >= 0) // not end of stream yet
                continue;

            if (in != null) try {
                in.close();
            }
            catch (Exception e) {
            }
            in = null;
            if (out != null) try {
                out.close();
            }
            catch (Exception e) {
            }
            out = null;
            
            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after pickup on "+ uri +
                    " for "+ filename + ": " + str +Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after pickup on "+ uri +
                    " for "+ filename + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(cid);
                new Event(Event.ERR,"failed to ack msg after pickup on "+ uri +
                    " for " + filename + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt = Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "picking up on " + uri + " for " + filename).send();
            }
            xq.remove(cid);
            cid = -1;
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, null, dmask,
                    propertyName);
                reply = size + " bytes to "; 
                if ((displayMask & MessageUtils.SHOW_DATE) > 0) {
                    new Event(Event.INFO, "picked up " + filename + " of " +
                        reply + uri + " with a msg ( Date: " + dt +
                        line + " )").send();
                }
                else { // no date
                    new Event(Event.INFO, "picked up " + filename + " of " +
                        reply + uri + " with a msg (" +
                        line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, "picked up " + filename + " of " +
                    size + " bytes to " + uri + " with a msg").send();
            }

            inMessage = null;
            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO,"picked up " +count+ " msgs to " + uri).send();
    }

    /**
     * It gets a JMS message from xq that contains a dirname for listing files
     * in it and gets the list via sftp.  It puts the list of files into the
     * message body one name per line and sends it back.  The requester at the
     * other end of the XQueue can easily read the list out of the message.
     *<br><br>
     * Since the list operation relies on a request, this method will  
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the list is successful.  Otherwise, the
     * message body will not contain the content out of the list operation.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void list(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        String[] list;
        StringBuffer strBuf = new StringBuffer();
        String dirname;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, sessionTime, idleTime, size;
        long count = 0, stm = 10;
        int sid = -1;
        int i, k, m, n, mask;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            sessionTime = System.currentTimeMillis();

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is supposed not to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            dirname = null;
            try {
                dirname = MessageUtils.getProperty(fileField, outMessage);
                if (dirname == null) {
                    if (path == null) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "default path is null for " +
                            uri).send();
                        outMessage = null;
                        continue;
                    }
                    dirname = path;
                }
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get dirname for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                Event.flush(e);
            }

            list = null;
            for (i=0; i<=retry; i++) {
                try {
                    list = sftpList(dirname, FTP_FILE);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to list " + dirname +
                        " on " + uri + ": " + e.toString()).send();
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to list " + dirname + " on " +
                        uri + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (list != null)
                    break;
                if (i == 0)
                    new Event(Event.WARNING, "failed to list " + dirname +
                        " on " + uri).send();
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    xq.putback(sid);
                    new Event(Event.WARNING, "aborted to list " + dirname +
                        " on " + uri + " due to being disabled").send();
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
                if (i < retry && reconnect() != null)
                    break;
            }
            if (list == null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to list " + dirname));
            }

            k = list.length;
            strBuf.delete(0, strBuf.length());
            for (i=0; i<k; i++) {
                if (list[i] != null)
                    strBuf.append(list[i] + "\n");
                list[i] = null;
            }
            list = null;
            try {
                size = (long) strBuf.length();
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with list of " +
                    dirname + " from " + uri +": " +Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, propertyName);
                new Event(Event.INFO, "listed " + dirname + " of " + k +
                    " files from " + uri + " with a msg ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "listed " + dirname + " of " + k +
                    " files from " + uri + " with a msg").send();
            }
            count ++;
            outMessage = null;
            strBuf.delete(0, strBuf.length());

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "listed "+count+" folders from "+uri).send();
    }

    /**
     * It gets a JMS Message from the XQueue and stores its body into a remote
     * file via sftp continuously.
     *<br><br>
     * It will always try to retrieve the URL from the field specified by
     * FieldName property.  If the message does not contain the URL, the method
     * will use the default URL defined by URI property.
     *<br><br>
     * It also supports LocalStore feature so that it will read the content
     * from the local file and stores the file to the remote filesystem.
     * If the local file is a directory, it will transfer all the files
     * and subdirectories to remote filesystem.  In this case, there is no
     * guarantee on the file transfer.
     *<br><br>
     * The store operation also supports the requests.  If the XAMode has
     * enabled the XA_CLIENT bit, it will treat the message as a request from
     * the uplink.  In this case, the incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is supposed to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the operation is successful.  Otherwise, the
     * request will be dropped due to failures.  On the other hand, if the
     * XA_CLIENT bit has been disabled by XAMode, the incoming message will
     * not be modified.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br><br>
     * If the XQueue has enabled the EXTERNAL_XA bit, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void store(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage;
        MessageInputStream in = null;
        int i, m, n, mask;
        int sid = -1;
        int totalCount = 0;
        int dmask = MessageUtils.SHOW_DATE;
        long count = 0, stm = 10;
        long currentTime, sessionTime, idleTime, size = 0;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isDirectory = false;
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isBytes = false;
        boolean isSleepy = (sleepTime > 0);
        String dt = null;
        String msgStr = null;
        String filename, reply=null, localFilename;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            currentTime = System.currentTimeMillis();
            n = 0;
            m = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, uriRC, inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, uriRC, inMessage);
                }
                catch (Exception ee) {
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
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
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            filename = null;
            localFilename = null;
            try {
                filename = MessageUtils.getProperty(fileField, inMessage);
                localFilename=MessageUtils.getProperty(storeField, inMessage);
                if (isWriteable)
                    MessageUtils.setProperty(rcField, jmsRC, inMessage);
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
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            if (filename == null)
                filename = path;

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            msgStr = null;
            isDirectory = false;
            if (localFilename == null || localFilename.length() <= 0) {
                // content is in msg
                try {
                    if (template != null)
                        msgStr = MessageUtils.format(inMessage,buffer,template);
                    else
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                }
                catch (JMSException e) {
                }

                if (msgStr == null || filename == null) {
                    new Event(Event.WARNING, uri + "(" + filename + ")" +
                        ": unknown msg type").send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception e) {
                    }
                    catch (Error e) {
                        xq.remove(sid);
                        Event.flush(e);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    continue;
                }

                // make sure dir exists on the serrver
                if (verifyDirectory &&
                    (reply = getParent(filename)) != null) try {
                    reply = createDirectory(reply, retry);
                    if (reply != null)
                        new Event(Event.WARNING, "failed to create dir " +
                            reply + " for " + filename).send();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to create dir for " +
                         filename + ": " + Event.traceStack(e)).send();
                }
                catch (Error e) {
                    xq.putback(sid);
                    new Event(Event.ERR, "failed to create dir for " +
                        filename + ": " + e.toString()).send();
                    Event.flush(e);
                }
                isBytes = (inMessage instanceof BytesMessage);
                if (isBytes)
                    in = new MessageInputStream((BytesMessage) inMessage);
                reply = null;
                size = msgStr.length();
                for (i=0; i<=retry; i++) {
                    try {
                        if (isBytes)
                            reply = sftpPut(in, filename);
                        else
                            reply = sftpPut(msgStr, filename);
                    }
                    catch (Exception e) {
                        reply = "" + e.toString();
                    }
                    catch (Error e) {
                        xq.putback(sid);
                        new Event(Event.ERR, "failed to store " + filename +
                            " on " + uri + ": " + e.toString()).send();
                        if (isBytes)
                            in.close();
                        Event.flush(e);
                    }
                    if (reply == null)
                        break;
                    if (i == 0)
                        new Event(Event.WARNING, "failed to store " +
                            filename + ": " + reply).send();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                        xq.putback(sid);
                        new Event(Event.WARNING, "aborted to store " +
                            filename + " on " + uri +
                            " due to being disabled").send();
                        if (isBytes)
                            in.close();
                        return;
                    }
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                    if (i < retry && reconnect() != null)
                        break;
                    if (isBytes) try {
                        ((BytesMessage) inMessage).reset();
                    }
                    catch (JMSException e) {
                        new Event(Event.ERR, "failed to reset msg for " +
                            filename + ": " + e.toString()).send();
                        break;
                    }
                }
                if (isBytes)
                    in.close();
            }
            else try { // content is in local file
                File localFile = new File(localFilename);
                if (!localFile.exists() || !localFile.canRead()) {
                    new Event(Event.ERR, uri+": failed to open local file "+
                        localFilename).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception e) {
                    }
                    catch (Error e) {
                        xq.remove(sid);
                        Event.flush(e);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    continue;
                }
                reply = null;
                if (localFile.isDirectory()) {
                    long[] state;
                    isDirectory = true;
                    state  = putAllFiles(localFile, filename, retry);
                    if (state != null && state.length >= 3) {
                        size = state[0];
                        totalCount = (int) state[1];
                    }
                    else {
                        size = 0;
                        totalCount = 0;
                        new Event(Event.WARNING, "illegal arguments for '" +
                            localFile + "' or '" + filename).send();
                    }
                }
                else { // assume it is a file
                    size = localFile.length();
                    // make sure dir exist on the server
                    if (verifyDirectory &&
                        (reply = getParent(filename)) != null) try {
                        reply = createDirectory(reply, retry);
                        if (reply != null)
                            new Event(Event.WARNING,"failed to create dir "+
                                reply + " for " + filename).send();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING,"failed to create dir for "+
                             filename + ": " + Event.traceStack(e)).send();
                    }
                    reply = putAFile(localFile, filename, retry);
                }
            }
            catch (Error e) {
                xq.putback(sid);
                new Event(Event.ERR, "failed to store " +
                    filename + ": " + e.toString()).send();
                Event.flush(e);
            }
            if (reply != null) {
                xq.putback(sid);
                throw(new IOException("failed to store " + filename + " on " +
                    uri + " with " + retry + " retries: " + reply));
            }

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for "+ filename + ": " + str +Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for " + filename + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after store on "+ uri +
                    " for " + filename + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt = Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "store on " + uri + " for " + filename).send();
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if (isDirectory)
                    reply = size + " out of " + totalCount + " files to "; 
                else
                    reply = size + " bytes to "; 
                if ((displayMask & MessageUtils.SHOW_DATE) > 0) {
                    new Event(Event.INFO, "stored " + filename + " of " +
                        reply + uri + " with a msg ( Date: " + dt +
                        line + " )").send();
                }
                else { // no date
                    new Event(Event.INFO, "stored " + filename + " of " +
                        reply + uri + " with a msg (" + line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, "stored " + filename + " of " +
                    size + " bytes to " + uri + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "stored " + count + " msgs to " + uri).send();
    }

    /**
     * It gets a JMS Message from the XQueue and retrieves the property map in
     * JSON from its body. It copies the source file defined in FileName to the
     * remote file in LocalStore via sftp.
     *<br><br>
     * It will always try to retrieve the URL from the field specified by
     * FileName property.  If the message does not contain the URL, the method
     * will use the default URL defined by URI property.  The LocalStore
     * specifies the target filename and its property map is required to be
     * stored in message body.
     *<br><br>
     * The copy operation also supports the requests.  If the XAMode has
     * enabled the XA_CLIENT bit, it will treat the message as a request from
     * the uplink.  In this case, the incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is supposed to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the operation is successful.  Otherwise, the
     * request will be dropped due to failures.  On the other hand, if the
     * XA_CLIENT bit has been disabled by XAMode, the incoming message will
     * not be modified.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br><br>
     * If the XQueue has enabled the EXTERNAL_XA bit, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void copy(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage;
        SFTPConnector target = null;
        int i, m, n, mask;
        int sid = -1;
        int totalCount = 0;
        int dmask = MessageUtils.SHOW_DATE;
        long count = 0, stm = 10;
        long currentTime, sessionTime, idleTime, size = 0;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isDirectory = false;
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null;
        String msgStr = null, rt;
        String filename, reply=null, targetFilename;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
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
            currentTime = System.currentTimeMillis();
            n = 0;
            m = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, uriRC, inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, uriRC, inMessage);
                }
                catch (Exception ee) {
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
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
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            filename = null;
            targetFilename = null;
            msgStr = null;
            try {
                filename = MessageUtils.getProperty(fileField, inMessage);
                targetFilename = MessageUtils.getProperty(storeField,inMessage);
                msgStr = MessageUtils.processBody(inMessage, buffer);
                if (isWriteable)
                    MessageUtils.setProperty(rcField, jmsRC, inMessage);
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
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            if (filename == null) // use the default
                filename = path;

            if (msgStr == null || msgStr.length() <= 0) {
                new Event(Event.ERR, uri +
                    ": remote target is not defined").send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            target = null;
            try { // instantiate the target from JSON content
                StringReader ins = new StringReader(msgStr);
                Map ph = (Map) JSON2Map.parse(ins);
                ins.close();
                for (i=0; i<=retry; i++) {
                    try {
                        target = new SFTPConnector(ph);
                    }
                    catch (Exception e) {
                        if (i == 0)
                            new Event(Event.WARNING,"failed to instantiate " +
                                "target for: " + targetFilename + ": " +
                                e.toString()).send();
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.STANDBY) > 0) {
                            xq.putback(sid);
                            new Event(Event.WARNING, uri +
                                ": aborted to instantiate the target " +
                                targetFilename+" due to being disabled").send();
                            return;
                        }
                        try {
                            Thread.sleep(1000);
                        }
                        catch (Exception ex) {
                        }
                    }
                }
                ph.clear();
            }
            catch (Exception e) {
                new Event(Event.ERR, uri +
                    ": failed to open remote target: " + targetFilename).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (target != null) try {
                    target.close();
                }
                catch (Exception ex) {
                }
                target = null;
                inMessage = null;
                continue;
            }
            catch (Error e) {
                new Event(Event.ERR, uri +
                    ": failed to open remote target: " + targetFilename).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (target != null) try {
                    target.close();
                }
                catch (Exception ex) {
                }
                target = null;
                Event.flush(e);
            }

            reply = null;
            // make sure dir exist on the target server
            if (target == null) {
                new Event(Event.ERR, uri +
                    ": failed to open remote target: " + targetFilename).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }
            else if (verifyDirectory &&
                (reply = target.getParent(targetFilename)) != null) try {
                reply = target.createDirectory(reply, retry);
                if (reply != null)
                    new Event(Event.WARNING, "failed to create dir "+
                        reply + " for " + targetFilename).send();
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to create dir for "+
                     targetFilename + ": " + Event.traceStack(e)).send();
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
            }
            catch (Error e) {
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (target != null) try {
                    target.close();
                }
                catch (Exception ex) {
                }
                target = null;
                Event.flush(e);
            }

            size = 0;
            for (i=0; i<=retry; i++) {
                try {
                    size = getSize(filename);
                }
                catch (Exception e) {
                    if (i == 0)
                        new Event(Event.WARNING, "failed to get size of " +
                            filename + ": " + e.toString()).send();
                    reconnect();
                    continue;
                }
                break;
            }

            reply = null;
            try { // copy stream
                OutputStream out = target.storeFileStream(targetFilename);
                reply = sftpGet(out, filename);
                if (out != null) try {
                    out.close();
                }
                catch (Exception ex) {
                }
                out = null;
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to copy " + filename +
                    " to " + targetFilename + ": " + e.toString()).send();
                if (target != null) try {
                    target.close();
                }
                catch (Exception ex) {
                }
                target = null;
                reply = Event.traceStack(e);
            }
            catch (Error e) {
                new Event(Event.ERR, "failed to copy " + filename +
                    " to " + targetFilename + ": " + e.toString()).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (target != null) try {
                    target.close();
                }
                catch (Exception ex) {
                }
                target = null;
                Event.flush(e);
            }
            if (target != null) try { // try to close the target
                target.close();
            }
            catch (Exception e) {
            }
            target = null;
            if (reply != null) {
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to copy " + filename +
                    " to " + targetFilename + ": " + reply));
            }

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after copy on "+ uri +
                    " for "+ filename + ": " + str +Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after copy on "+ uri +
                    " for " + filename + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after copy on "+ uri +
                    " for " + filename + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt = Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "copy on " + uri + " for " + targetFilename).send();
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if (isDirectory)
                    reply = size + " out of " + totalCount + " files to "; 
                else
                    reply = size + " bytes to "; 
                if ((displayMask & MessageUtils.SHOW_DATE) > 0) {
                    new Event(Event.INFO, "copied " + targetFilename + " of " +
                        reply + uri + " with a msg ( Date: " + dt +
                        line + " )").send();
                }
                else { // no date
                    new Event(Event.INFO, "copied " + targetFilename + " of " +
                        reply + uri + " with a msg (" + line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, "copied " + filename + " of " +
                    size + " bytes to " + uri + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "copied " + count + " msgs to " + uri).send();
    }

    /**
     * It puts a local file to the server and stores it with the filename.
     */
    private String putAFile(File localFile, String filename, int retry) {
        int i;
        String reply = null;
        for (i=0; i<=retry; i++) {
            try {
                reply = sftpPut(localFile, filename);
            }
            catch (Exception e) {
                reply = "" + e.toString();
            }
            if (reply == null)
                return null;
            try {
                Thread.sleep(receiveTime);
            }
            catch (InterruptedException e) {
            }
            if (i < retry && reconnect() != null)
                break;
        }
        return reply;
    }

    /**
     * It puts all the files in the local dir and files in all subdirs to
     * the remote dir with retry support. In case there is an error with
     * a file, it only loggs without a stop.  If the error is on a local or
     * remote directory, it will skip the files in the directory and log
     * an error.  Both local dir and remote dir are required to be the
     * absolute path.  Upon success, it returns a long array with three
     * numbers: number of files done, number of files to put, total bytes moved.
     * Otherwise, it returns null to indicate illegal arguments.
     */
    private long[] putAllFiles(File localDir, String remoteDir, int retry) {
        int count, i, n, totalCount;
        long size;
        long[] state;
        char fs;
        String baseDir, reply = null;
        String[] list;
        File file;
        if (localDir == null || remoteDir == null || remoteDir.length() <= 0)
            return null;
        baseDir = localDir.getPath();
        if (!localDir.isDirectory() || !localDir.canRead()) {
            new Event(Event.ERR, "failed to open local directory " + baseDir +
                " and aborted to store files to " + remoteDir).send();
            return new long[]{0L, 0L, 0L};
        }
        baseDir += baseDir.charAt(0);
        list = localDir.list();
        n = list.length;
        fs = remoteDir.charAt(0);
        if (fs != '/' && fs != '\\')
            return null;
        try {
            reply = createDirectory(remoteDir, retry);
        }
        catch (Exception e) {
            reply = "" + e.toString();
        }
        if (reply != null) {
            new Event(Event.ERR, "failed to create remote directory '" +
                remoteDir +"' and aborted to store files from "+baseDir).send();
            return new long[]{0L, (long) n, 0L};
        }
        totalCount = 0;
        count = 0;
        size = 0;
        for (i=0; i<n; i++) {
            if (list[i] == null || list[i].length() <= 0)
                continue;
            file = new File(baseDir + list[i]);
            if (file == null)
                continue;
            if (!file.isDirectory()) {
                size += file.length();
                totalCount ++;
                reply = putAFile(file, remoteDir+fs+list[i], retry); 
                if (reply != null) {
                    new Event(Event.ERR, "failed to store file '" + reply +
                        "' to " + remoteDir + fs + list[i]).send();
                }
                else {
                    new Event(Event.INFO, "successfully stored the file " +
                        file.getPath() + " of " + file.length() + " bytes to "+
                        remoteDir).send();
                    count ++;
                }
                continue;
            }
            state = putAllFiles(file, remoteDir+fs+list[i], retry);
            if (state != null && state.length >= 3) {
                size += state[2];
                totalCount += (int) state[1];
                count += (int) state[0];
            }
            else {
                new Event(Event.WARNING, "illegal arguments for '" +
                    file.getPath() + "' or '"+remoteDir+fs+list[i]+"'").send();
            }
        }
        return new long[]{(long) count, (long) totalCount, size};
    }

    public String getOperation() {
        return operation;
    }
}
