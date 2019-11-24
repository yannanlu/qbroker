package org.qbroker.jms;

/* MessageLogger.java - a message logger */

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
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
import javax.jms.Topic;
import javax.jms.Queue;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.NewlogFetcher;
import org.qbroker.common.DirectoryTree;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * MessageLogger handles common logs as JMS Messages.
 *<br><br>
 * There are three methods, fetch(), download() and append().  The former is
 * NOT MT-Safe.  During the fetch operation, the message acknowledgement and
 * session transaction are supported via explicitly persisting reference.
 * The method of download() is used for synchronous requests.  The method of
 * append() is used for asynchronous logging or storing.
 *<br><br>
 * For fetch(), there are 4 different transaction mode determined by XAMode.
 * For XAMode = 1 or 3, the client based transcation is enabled so that the
 * commit is done by the persister when it completes to persist the message.
 * If XAMode = 2, the session commit is enabled so that the commit will be done
 * when the read operation finishes and the log file is closed.  The session
 * can be forced to termination if MaxNumberMessage is larger than 0 and the
 * total number of processed logs and dropped logs exceeds it. For XAMode = 0,
 * the auto commit is enabled.  In this case, the persistence of the state info
 * is determined by SaveReference. By default, SaveReference is set to be true.
 * It means the state info will be flushed to the disk on every processed log
 * entry. If SaveReference is set to false, otherwise, the state info will not
 * saved to the reference file.
 *<br><br>
 * For charSet and encoding, you can overwrite the default encoding by
 * starting JVM with "-Dfile.encoding=UTF8 your_class".
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageLogger {
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int sleepTime = 0;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int textMode = 1;
    private int xaMode = 0;
    private int mode = 0;
    private int logSize = 1;
    private int maxNumberMsg = 0;
    private SimpleDateFormat dateFormat = null;
    private Template template = null;
    private boolean append = true, oldLogFile = false;
    private boolean verifyDirectory = false;
    private java.lang.reflect.Method ackMethod = null;
    private MessageFilter filter = null;

    private long offset, position, timestamp;
    private long maxMsgLength = 4194304;
    private int bufferSize = 4096;
    private NewlogFetcher newlog;
    private String logfile;
    private String rcField, fieldName;
    private String operation = "append";
    private String uri;
    private int[] partition;
    private int retry = 2;
    private final static int LOG_POS = 0;
    private final static int LOG_TIME = 1;
    private final static int LOG_OFFSET = 2;

    public MessageLogger(Map props) {
        Object o;
        URI u;
        String s;

        if (props.get("Operation") != null) {
            operation = (String) props.get("Operation");
        }

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((s = u.getScheme()) == null || "log".equals(s) ||
            ("file".equals(s) && ("download".equals(operation) ||
            "store".equals(operation)))) {
            logfile = u.getPath();
            if (logfile == null || logfile.length() == 0)
                throw(new IllegalArgumentException("URI has no path: " + uri));
        }
        else {
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        }

        if (props.get("XAMode") != null) {
            xaMode = Integer.parseInt((String) props.get("XAMode"));
        }
        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((fieldName = (String) props.get("FieldName")) == null ||
            fieldName.length() == 0)
            fieldName = null;
        else if ("JMSType".equals(fieldName))
            fieldName = MessageUtils.getPropertyID(fieldName);

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ("store".equals(operation) && "file".equals(s)) {
            append = false;
            if ((o = props.get("VerifyDirectory")) != null &&
                "true".equals(((String) o).toLowerCase()))
                verifyDirectory = true;
        }

        if ("fetch".equals(operation)) {
            Map<String, Object> h = new HashMap<String, Object>(); 
            if ((o = props.get("LogSize")) == null ||
                (logSize = Integer.parseInt((String) o)) < 0)
                logSize = 1;
            if (logSize > 500)
                logSize = 500;

            if ((o=props.get("PatternGroup"))==null || !(o instanceof List))
             throw(new IllegalArgumentException("PatternGroup is not defined"));

            try {
                Map<String, Object> hmap = new HashMap<String, Object>();

                hmap.put("Name", uri);
                if ((o = props.get("PatternGroup")) != null)
                    hmap.put("PatternGroup", o);
                if ((o = props.get("XPatternGroup")) != null)
                    hmap.put("XPatternGroup", o);
                filter = new MessageFilter(hmap);
                hmap.clear();
            }
            catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed: "+e.getMessage()));
            }

            h.put("LogFile", logfile);
            if ((o = props.get("OldLogFile")) != null) {
                oldLogFile = true;
                h.put("OldLogFile", o);
            }
            else
                oldLogFile = false;
            h.put("ReferenceFile", props.get("ReferenceFile"));
            h.put("TimePattern", props.get("TimePattern"));
            h.put("OrTimePattern", props.get("OrTimePattern"));
            h.put("PerlPattern", props.get("PerlPattern"));
            h.put("MaxNumberLogs", props.get("MaxNumberLogs"));
            h.put("MaxScannedLogs", props.get("MaxScannedLogs"));
            h.put("MaxLogLength", props.get("MaxLogLength"));
            h.put("Debug", props.get("Debug"));

            if ((xaMode & MessageUtils.XA_CLIENT) > 0) { // for XAMode 1 or 3
                Class<?> cls = this.getClass();
                h.put("SaveReference", "false");
                try {
                    ackMethod = cls.getMethod("acknowledge",
                        new Class[] {long[].class});
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("found no ack method"));
                }
            }
            else if ((xaMode & MessageUtils.XA_COMMIT) > 0) // for XAMode 2
                h.put("SaveReference", "false");
            else if ((o =  props.get("SaveReference")) != null) // for XAMode 0
                h.put("SaveReference", o);

            newlog = new NewlogFetcher(h);
            h.clear();
        }

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
        if (props.get("Retry") == null ||
            (retry = Integer.parseInt((String) props.get("Retry"))) < 0)
            retry = 2;

        if (props.get("Mode") != null &&
            "daemon".equals((String) props.get("Mode"))) {
            mode = 1;
        }
        if (props.get("TextMode") != null) {
            textMode = Integer.parseInt((String) props.get("TextMode"));
        }
        if (props.get("DisplayMask") != null) {
            displayMask = Integer.parseInt((String) props.get("DisplayMask"));
        }
        if ((o = props.get("MaxNumberMessage")) != null) {
            maxNumberMsg = Integer.parseInt((String) o);
        }
        if (props.get("WaitTime") != null) {
            waitTime = Long.parseLong((String) props.get("WaitTime"));
            if (waitTime <= 0L)
                waitTime = 500L;
        }
        if (props.get("ReceiveTime") != null) {
            receiveTime= Integer.parseInt((String) props.get("ReceiveTime"));
        }
        if (props.get("SleepTime") != null) {
            sleepTime= Integer.parseInt((String) props.get("SleepTime"));
        }
        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
        }
        if (props.get("MaxMsgLength") != null) {
            maxMsgLength = Long.parseLong((String) props.get("MaxMsgLength"));
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It fetches new log entries from the logfile and packages them
     * into JMS Messages and add the messages to an XQueue.
     * It has the acknowledge support or session transcation support if
     * the XAMode is enabled.  With the session transaction, the application
     * has to call commit() explicitly to persist the reference.
     *<br><br>
     * For those nohit log entries, it will commit them anyway.  This may
     * break the client transaction control.  So please make sure to use
     * the correct pattern to catch all entries.
     *<br><br>
     * This method is NOT MT-Safe.
     */
    public void fetch(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        int sid = -1, cid, mask = 0;
        int i, k = 0, l, n = 0, sessionCount = 0;
        long t, st = 0, pos, length, count = 0, stm = 10;
        String line, msgStr;
        StringBuffer strBuf;
        boolean isSleepy = (sleepTime > 0);
        boolean xa = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean xb = ((xaMode & MessageUtils.XA_COMMIT) > 0);
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

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

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            try {
                pos = newlog.locateNewlog();
            }
            catch (IOException e) {
                xq.cancel(sid);
                throw(new IOException("failed to locate new log: " +
                    Event.traceStack(e)));
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new IOException("failed to locate new entries: " +
                    Event.traceStack(e)));
            }

            if (oldLogFile && (n = newlog.logBuffer.size()) > 0) {
                new Event(Event.NOTICE, "log rotated with " + n +
                    " new entries").send();
                l = 0;
                for (i=0; i<n; i++) { // process all old log entries
                    msgStr = (String) newlog.logBuffer.get(i);
                    if (filter.evaluate(outMessage, msgStr)) {
                        if (isText)
                            ((TextMessage) outMessage).setText(msgStr);
                        else
                       ((BytesMessage)outMessage).writeBytes(msgStr.getBytes());

                        if (propertyName != null && propertyValue != null) {
                            outMessage.clearProperties();
                            for (int j=0; j<propertyName.length; j++)
                                MessageUtils.setProperty(propertyName[j],
                                    propertyValue[j], outMessage);
                        }

                        if ((st = newlog.getTimestamp(msgStr)) >= 0)
                            outMessage.setJMSTimestamp(st);

                        cid = xq.add(outMessage, sid);

                        if (cid >= 0) {
                            count ++;
                            l ++;
                            if (displayMask > 0) try {
                                new Event(Event.INFO, "fetched an entry from " +
                                    newlog.getPath() + " into a msg (" +
                                    MessageUtils.display(outMessage, msgStr,
                                    displayMask, null) + " )").send();
                            }
                            catch (Exception e) {
                                new Event(Event.INFO, "fetched an entry from " +
                                    newlog.getPath() + " into a msg").send();
                            }
                        }
                        else {
                            xq.cancel(sid);
                            new Event(Event.WARNING, "XQ is full: " +
                                xq.size() + "/" + xq.depth() + " " + sid +
                                "/" + count).send();
                            i --;
                        }

                        sid = -1;
                        switch (len) {
                          case 0:
                            do { // reserve an empty cell
                                sid = xq.reserve(waitTime);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0);
                            break;
                          case 1:
                            do { // reserve the empty cell
                                sid = xq.reserve(waitTime, shift);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0);
                            break;
                          default:
                            do { // reserve a partitioned empty cell
                                sid = xq.reserve(waitTime, shift, len);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0);
                            break;
                        }

                        if (sid >= 0) {
                            if (!isText)
                                outMessage = new BytesEvent();
                            else
                                outMessage = new TextEvent();
                        }
                        else {
                            newlog.close();
                            if (displayMask != 0)
                                new Event(Event.INFO, xq.getName() +
                                    ": fetched " + count + " log entries from "+
                                    newlog.getPath()).send();
                            return;
                        }
                    }
                }
                new Event(Event.NOTICE, "fetched " + l +
                    " log entries from the rotated logfile").send();
            }

            n = 0;
            k ++;
            length = 0;
            strBuf = new StringBuffer();
            while ((line = newlog.getLine()) != null) {
                l = line.length();
                if ((t = newlog.getTimestamp(line)) >= 0) { // valid log
                    if (n > 0) { // matching the patterns
                        msgStr = strBuf.toString();
                        strBuf = new StringBuffer();
                        if (filter.evaluate(outMessage, msgStr)) {
                            if (isText)
                                ((TextMessage) outMessage).setText(msgStr);
                            else
                      ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());

                            if (propertyName != null && propertyValue != null) {
                                outMessage.clearProperties();
                                for (int j=0; j<propertyName.length; j++)
                                    MessageUtils.setProperty(propertyName[j],
                                        propertyValue[j], outMessage);
                            }

                            outMessage.setJMSTimestamp(st);
                            newlog.updateReference(pos, st, length);
                            if (xa) // client commit
                                ((JMSEvent) outMessage).setAckObject(this,
                                    ackMethod, new long[] {newlog.getPosition(),
                                    newlog.getTimestamp(), newlog.getOffset()});
                            else if (!xb) try { // auto commit
                                newlog.saveReference();
                            }
                            catch (Exception e) {
                                new Event(Event.WARNING,
                                    "auto acknowledge failed: " +
                                    Event.traceStack(e)).send();
                            }

                            cid = xq.add(outMessage, sid);

                            if (cid >= 0) {
                                count ++;
                                sessionCount ++;
                                if (displayMask > 0) try {
                                  new Event(Event.INFO,"fetched an entry from "+
                                        newlog.getPath() + " into a msg (" +
                                        MessageUtils.display(outMessage, msgStr,
                                        displayMask, null) + " )").send();
                                }
                                catch (Exception e) {
                                  new Event(Event.INFO,"fetched an entry from "+
                                        newlog.getPath() +" into a msg").send();
                                }
                                if (isSleepy) { // slow down a while
                                    long tm = System.currentTimeMillis() +
                                        sleepTime;
                                    do {
                                        mask = xq.getGlobalMask();
                                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                            (mask & XQueue.PAUSE) > 0) // paused
                                            break;
                                        else try {
                                            Thread.sleep(stm);
                                        }
                                        catch (InterruptedException e) {
                                        }
                                    } while (tm > System.currentTimeMillis());
                                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                        (mask & XQueue.PAUSE) > 0) // paused
                                        break;
                                }
                            }
                            else {
                                xq.cancel(sid);
                                new Event(Event.WARNING, "XQ is full: " +
                                    xq.size() + "/" + xq.depth() + " " + sid +
                                    "/" + count).send();
                                newlog.seek(pos);
                                strBuf = new StringBuffer(msgStr);
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
                                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                    (mask & XQueue.PAUSE) == 0);
                                break;
                              case 1:
                                do { // reserve the empty cell
                                    sid = xq.reserve(waitTime, shift);
                                    if (sid >= 0)
                                        break;
                                    mask = xq.getGlobalMask();
                                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                    (mask & XQueue.PAUSE) == 0);
                                break;
                              default:
                                do { // reserve a partitioned empty cell
                                    sid = xq.reserve(waitTime, shift, len);
                                    if (sid >= 0)
                                        break;
                                    mask = xq.getGlobalMask();
                                } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                    (mask & XQueue.PAUSE) == 0);
                                break;
                            }

                            if (sid >= 0) { // a cell is reserved
                                if (!isText)
                                    outMessage = new BytesEvent();
                                else
                                    outMessage = new TextEvent();
                            }
                            else { // failed to reserve a cell
                                newlog.close();
                                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                    (mask & XQueue.PAUSE) == 0)
                                    new Event(Event.ERR,
                                        "failed to reserve a cell on " +
                                        xq.getName() + " for " + uri).send();
                                else if (displayMask != 0)
                                    new Event(Event.INFO, xq.getName() +
                                        ": fetched " + count +
                                        " log entries from "+
                                        newlog.getPath()).send();
                                if (!xa && xb && sessionCount > 0) try {
                                    // session commit
                                    sessionCount = 0;
                                    newlog.saveReference(newlog.getPosition(),
                                        newlog.getTimestamp(),
                                        newlog.getOffset());
                                }
                                catch (Exception e) {
                                    new Event(Event.WARNING,
                                        "session commit failed: " +
                                        Event.traceStack(e)).send();
                                }
                                return;
                            }
                            if (cid < 0) // XQ full so retry
                                continue;
                        }
                        else try { // commit on nohit
                            newlog.updateReference(pos, st, length);
                            sessionCount ++;
                            if (!xa && !xb) // auto commit
                                newlog.saveReference();
                            else if (xa && xq.size() <= 0) // save reference
                                newlog.saveReference(newlog.getPosition(),
                                    newlog.getTimestamp(), newlog.getOffset());
                        }
                        catch (Exception e) {
                        }
                    }
                    st = t;
                    if (maxNumberMsg > 0 && sessionCount >= maxNumberMsg) {
                        n = 0;
                        newlog.seek(pos);
                        // end of the session
                        break;
                    }
                    strBuf.append(line);
                    pos += length;
                    length = l;
                    n = 1;
                }
                else if (n > 0 && n < logSize) {
                    strBuf.append(line);
                    length += l;
                    n ++;
                }
                else if (n == 0) {
                    pos += l;
                    newlog.updateReference(l);
                }
                else {
                    length += l;
                }
            }

            if (sid >= 0 && n > 0) { // leftover
                if (n < logSize) { // there may be lines leftover
                    t = receiveTime / (logSize - n);
                    if (t <= 0)
                        t = 10;
                    do {
                        try {
                            Thread.sleep(t);
                        }
                        catch (InterruptedException e) {
                        }
                        n ++;
                        if ((line = newlog.getLine()) != null) {
                            l = line.length();
                            if (newlog.getTimestamp(line) >= 0) // valid log
                                break;
                            strBuf.append(line); 
                            length += l;
                        }
                    } while (n < logSize);
                }
                msgStr = strBuf.toString();
                if (filter.evaluate(outMessage, msgStr)) {
                    if (isText)
                        ((TextMessage) outMessage).setText(msgStr);
                    else
                      ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());

                    if (propertyName != null && propertyValue != null) {
                        outMessage.clearProperties();
                        for (int j=0; j<propertyName.length; j++)
                            MessageUtils.setProperty(propertyName[j],
                                propertyValue[j], outMessage);
                    }

                    outMessage.setJMSTimestamp(st);
                    newlog.updateReference(pos, st, length);
                    if (xa) // client commit
                        ((JMSEvent) outMessage).setAckObject(this,
                            ackMethod, new long[] {newlog.getPosition(),
                            newlog.getTimestamp(), newlog.getOffset()});
                    else if (!xb) try { // auto commit
                        newlog.saveReference();
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, "auto acknowledge failed: " +
                            Event.traceStack(e)).send();
                    }

                    cid = xq.add(outMessage, sid);

                    if (cid >= 0) {
                        count ++;
                        sessionCount ++;
                        if (displayMask > 0) try {
                            new Event(Event.INFO, "fetched an entry from " +
                                newlog.getPath() + " into a msg (" +
                                MessageUtils.display(outMessage, msgStr,
                                displayMask, null) + " )").send();
                        }
                        catch (Exception e) {
                            new Event(Event.INFO, "fetched an entry from " +
                                newlog.getPath() + " into a msg").send();
                        }
                        if (isSleepy) { // slow down a while
                            long tm = System.currentTimeMillis() + sleepTime;
                            do {
                                mask = xq.getGlobalMask();
                                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                    (mask & XQueue.PAUSE) > 0) // paused
                                    break;
                                else try {
                                    Thread.sleep(stm);
                                }
                                catch (InterruptedException e) {
                                }
                            } while (tm > System.currentTimeMillis());
                            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) > 0) // paused
                                break;
                        }
                    }
                    else {
                        xq.cancel(sid);
                        new Event(Event.WARNING, "XQ is full: " + xq.size() +
                            "/" + xq.depth() + " " + sid + "/" + count).send();
                        newlog.seek(pos);
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
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                      case 1:
                        do { // reserve the empty cell
                            sid = xq.reserve(waitTime, shift);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                      default:
                        do { // reserve a partitioned empty cell
                            sid = xq.reserve(waitTime, shift, len);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                    }

                    if (sid >= 0) { // a cell is reserved
                        if (!isText)
                            outMessage = new BytesEvent();
                        else
                            outMessage = new TextEvent();
                    }
                    else { // failed to reserve a cell
                        newlog.close();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0)
                            new Event(Event.ERR,
                                "failed to reserve a cell on " +
                                xq.getName() + " for " + uri).send();
                        else if (displayMask != 0)
                            new Event(Event.INFO, xq.getName() +
                                ": fetched " + count + " log entries from "+
                                newlog.getPath()).send();
                        if (!xa && xb && sessionCount > 0) try {//session commit
                            sessionCount = 0;
                            newlog.saveReference(newlog.getPosition(),
                                newlog.getTimestamp(), newlog.getOffset());
                        }
                        catch (Exception e) {
                            new Event(Event.WARNING, "session commit failed: " +
                                Event.traceStack(e)).send();
                        }
                        return;
                    }
                }
                else try { // commit on nohit
                    newlog.updateReference(pos, st, length);
                    sessionCount ++;
                    if (!xa && !xb) // auto commit
                        newlog.saveReference();
                    else if (xa && xq.size() <= 0) // save reference
                        newlog.saveReference(newlog.getPosition(),
                            newlog.getTimestamp(), newlog.getOffset());
                }
                catch (Exception e) {
                }
            }
            newlog.close();
            if (!xa && xb && sessionCount > 0) try { // session commit
                sessionCount = 0;
                newlog.saveReference(newlog.getPosition(),
                    newlog.getTimestamp(), newlog.getOffset());
            }
            catch (Exception e) {
                new Event(Event.WARNING, "session commit failed: " +
                    Event.traceStack(e)).send();
            }
            if(maxNumberMsg > 0 && sessionCount >= maxNumberMsg) //session reset
                sessionCount = 0;
            if (n == 0 && receiveTime > 0) {
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0) try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }
        }

        if (sid >= 0 && xq != null) {
            xq.cancel(sid);
        }

        if (!xa && xb && sessionCount > 0) try { // session commit
            newlog.saveReference(newlog.getPosition(),
                newlog.getTimestamp(), newlog.getOffset());
        }
        catch (Exception e) {
            new Event(Event.WARNING, "session commit failed: " +
                Event.traceStack(e)).send();
        }

        if (displayMask != 0)
            new Event(Event.INFO, xq.getName() + ": fetched " + count +
                " log entries from " + newlog.getPath()).send();
    }

    /**
     * It gets a JMS message from xq that contains a filename to be downloaded
     * and downloads the local file.  It puts the content of the file into
     * the message body and sends it back.  The requester at the other end
     * of the xq can easily read the content out of the message.
     *<br><br>
     * Since the download operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the download is successful.  Otherwise, the
     * message body will not contain the content out of download operation.
     *<br><br>
     * This method will not acknowledge messages.  It is up to the requester to
     * do that.
     */
    public void download(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        int i, sid = -1, mask;
        long count = 0, stm = 10;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String filename, reply = null;
        boolean isSleepy = (sleepTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        StringBuffer strBuf = null;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is not supposed to be null
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
            try {
                filename = MessageUtils.getProperty(fieldName, outMessage);
                if (filename == null) {
                    if (logfile == null)
                        throw(new JMSException("logfile is null"));
                    filename = logfile;
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
                new Event(Event.WARNING, "failed to get filename for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            for (i=0; i<retry; i++) {
                strBuf = new StringBuffer();
                try {
                    reply = localGet(strBuf, filename);
                }
                catch (IOException e) {
                    reply = "" + e.getMessage();
                }
                if (reply == null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) // standby temporarily
                    break;
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }

            if (reply != null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0)
                    break;
                else
                    throw(new IOException("failed to download " + filename +
                        " with " + retry + "retries: " + reply));
            }

            try {
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                    MessageUtils.setProperty(rcField, okRC, outMessage);
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
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with result" +
                    " downloaded from " + filename).send();
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
                new Event(Event.INFO, "downloaded " + filename + " of " +
                    strBuf.length() + " bytes from " + uri +
                    " with a msg ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "downloaded " + filename + " of " +
                    strBuf.length() + " bytes from " + uri +
                    " with a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
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
            new Event(Event.INFO, xq.getName() + ": downloaded " + count +
                " files").send();
    }

    private String localGet(StringBuffer strBuf, String filename)
        throws IOException {
        FileInputStream in = null;
        int bytesRead = 0;
        byte[] buffer = new byte[bufferSize];

        if (strBuf == null)
            return "strBuf is null";

        File file = new File(filename);
        if (file.exists() && file.isDirectory()) { // directory content
            DirectoryTree dir = new DirectoryTree(fieldName, filename, 0);
            strBuf.append(dir.getTreeMap());
            dir.clear();
            return null;
        }
        in = new FileInputStream(filename);

        while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
            if (bytesRead == 0) {
                try {
                    Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                }
                continue;
            }

            strBuf.append(new String(buffer, 0, bytesRead));
        }
        try {
            in.close();
        }
        catch (Exception e) {
        }

        return null;
    }

    /**
     * It gets a JMS Message from an XQueue and append the formatted content
     * to the logfile continuously, or store the content to a file.
     *<br><br>
     * The store or append operation also supports requests.  If the XAMode has
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
     * If the XQueue has enabled the EXTERNAL_XA, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void append(XQueue xq) throws IOException, JMSException {
        Message inMessage;
        long count = 0, stm = 10;
        int sid = -1, mask;
        int dmask = MessageUtils.SHOW_DATE;
        String filename, msgStr = null, opStr = ((append)?"appended":"stored");
        String dt = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        byte[] buffer = new byte[bufferSize];
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

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
            try {
                filename = MessageUtils.getProperty(fieldName, inMessage);
                if (isWriteable)
                    MessageUtils.setProperty(rcField, jmsRC, inMessage);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to get filename for " +
                    uri + ": " + e.toString()).send();
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
                filename = logfile;

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(inMessage,buffer,template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null) {
                new Event(Event.WARNING, uri + ": unknown msg type").send();
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
            if ((!append) && verifyDirectory) { // make sure dir exists
                String folder = Utils.getParent(filename);
                if (folder != null) { // full path
                    File dir = new File(folder);
                    if (!dir.exists()) {
                        for (int i=0; i<=retry; i++) {
                            if (dir.exists())
                                break;
                            dir.mkdirs();
                        }
                    }
                    if (!dir.exists())
                        new Event(Event.WARNING, "failed to create dir " +
                            folder + " for " + filename).send();
                }
            }

            try { // will write 0 byte to the file if msg is empty
                FileOutputStream out
                    = new FileOutputStream(filename, append);
                if (inMessage instanceof BytesMessage) {
                    ((BytesMessage) inMessage).reset();
                    MessageUtils.copyBytes((BytesMessage) inMessage,buffer,out);
                }
                else
                    out.write(msgStr.getBytes());
                out.close();
            }
            catch (IOException e) {
                xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after "+ opStr +
                    " on " + filename +": "+Event.traceStack(e)).send();
            }
            catch (Error e) {
                new Event(Event.ERR, "failed to ack msg after "+ opStr +
                    " on " + filename +": "+Event.traceStack(e)).send();
                xq.remove(sid);
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
                inMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    opStr + " on " + filename).send();
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, opStr + " " + msgStr.length() +
                        " bytes to " + filename + " with a msg ( Date: " +
                        dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, opStr + " " + msgStr.length() +
                        " bytes to " + filename + " with a msg (" +
                        line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, opStr + " " + msgStr.length() +
                    " bytes to " + filename + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
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
            new Event(Event.INFO, xq.getName() + ": " + opStr + " " + count +
                " msgs").send();
    }

    public void rollback() throws JMSException {
    }

    public synchronized void commit() throws JMSException {
        if (newlog == null)
            return;
        try {
            newlog.saveReference(newlog.getPosition(), newlog.getTimestamp(),
                newlog.getOffset());
        }
        catch (Exception e) {
            throw(new JMSException("failed to commit session: " +
                Event.traceStack(e)));
        }
    }

    public synchronized void acknowledge(long[] state) throws JMSException {
        if (state == null || newlog == null || state[LOG_TIME] < timestamp)
            return;

        try {
            if (state[LOG_TIME] > timestamp) {
                newlog.saveReference(state[LOG_POS], state[LOG_TIME],
                    state[LOG_OFFSET]);
                position = state[LOG_POS];
                timestamp = state[LOG_TIME];
                offset = state[LOG_OFFSET];
            }
            else if (state[LOG_POS]==position && state[LOG_OFFSET]>offset) {
                // same timestamp with larger offset
                newlog.saveReference(state[LOG_POS], state[LOG_TIME],
                    state[LOG_OFFSET]);
                offset = state[LOG_OFFSET];
            }
            else if (state[LOG_POS] != position) { // log rotated
                newlog.saveReference(state[LOG_POS], state[LOG_TIME],
                    state[LOG_OFFSET]);
                position = state[LOG_POS];
                offset = state[LOG_OFFSET];
            }
        }
        catch (IOException e) {
            throw(new JMSException("failed to acknowledge msg (" +
                state[LOG_POS] + " " + state[LOG_TIME] + " " +
                state[LOG_OFFSET] + "): " + Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new JMSException("failed to acknowledge msg '" +
                state[LOG_POS] + " " + state[LOG_TIME] + " " +
                state[LOG_OFFSET] + "': " + Event.traceStack(e)));
        }
    }

    public String getOperation() {
        return operation;
    }
}
