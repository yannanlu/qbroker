package org.qbroker.jms;

/* MessageStream.java - a byte stream for JMS messages */

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.text.DateFormat;
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
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.XQueue;
import org.qbroker.common.QList;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.Browser;
import org.qbroker.common.Utils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * MessageStream handles Stream IO on JMS Messages.  The Stream has to be
 * established before using this object.
 *<br><br>
 * There are four methods, read(), write(), request() and respond() on Stream.
 * The method of read() is MT-Safe.  If read() gets the end of stream,
 * it will stop and return.  The method of write() is for asynchronous
 * output whereas request() is for synchronous requests. For testing purpose,
 * MessageStream supports two methods, echo() and collect().
 *<br><br>
 * MessageStream reacts on EOF differently according to the Mode.  If its value
 * is set to command mode, read() just exits when an EOF is read.  For Linux
 * comm port,  the timeout implementation returns an EOF.  Therefore, setting
 * the value to ignore mode will have most of read operation to ignore the EOF
 * and continue to read.  If the value is set to daemon, read() will throw an
 * IOException whenever it reads an EOF to indicate end of stream unexpectedly.
 *<br><br>
 * For charSet and encoding, you can overwrite the default encoding by
 * starting JVM with "-Dfile.encoding=UTF8 your_class".
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageStream {
    private String uri;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int sleepTime = 0, maxIdleTime = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int mode = 0;
    private int capacity, maxNumberMsg = 0;
    private int[] partition;
    private DateFormat dateFormat = null;
    private Template template = null;
    private StringBuffer msgBuf;
    private String rcField, operation = "read";
    private Map parserProps = null;
    private java.lang.reflect.Method ackMethod = null;

    private Perl5Matcher pm = null;
    private Pattern pattern;

    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 5000; // for timeout in ms
    private int bufferSize = 4096;
    private int boundary = 0;
    private int offhead = 0;
    private int offtail = 0;
    private byte[] sotBytes = new byte[0];
    private byte[] eotBytes = new byte[0];
    private final static int MS_SOT = DelimitedBuffer.MS_SOT;
    private final static int MS_EOT = DelimitedBuffer.MS_EOT;

    public MessageStream(Map props) {
        Object o;

        if ((o = props.get("URI")) != null || (o = props.get("Name")) != null)
            uri = (String) o;
        else
            uri = "stream";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

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

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((o = props.get("Mode")) != null) {
            if ("daemon".equals(((String) o).toLowerCase()))
                mode = 1;
            else if ("ignore".equals(((String) o).toLowerCase()))
                mode = -1;
        }
        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;
        if ((o = props.get("Operation")) != null)
            operation = (String) props.get("Operation");
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
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

        if ("respond".equals(operation) || "request".equals(operation)) {
            Map ph = (Map) props.get("Parser");
            if (ph != null && (o = ph.get("ClassName")) != null) {
                parserProps = Utils.cloneProperties(ph);
                MessageUtils.getPlugins(ph, "ParserArgument", "parse",
                    new String[] {"java.lang.String"}, null, uri);
            }
            if ("respond".equals(operation)) try {
                Class<?> cls;
                cls = Class.forName("org.qbroker.common.CollectibleCells");
                ackMethod = cls.getMethod("acknowledge",
                    new Class[]{long[].class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to find ack method:"+
                    e.toString()));
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

        if ((o = props.get("Offhead")) != null)
            offhead = Integer.parseInt((String) o);
        if (offhead < 0)
            offhead = 0;
        if ((o = props.get("Offtail")) != null)
            offtail = Integer.parseInt((String) o);
        if (offtail < 0)
            offtail = 0;
        boundary = 0;
        if ((o = props.get("SOTBytes")) != null) {
            sotBytes = MessageUtils.hexString2Bytes((String) o);
            if (sotBytes != null && sotBytes.length > 0)
                boundary += MS_SOT;
        }
        if ((o = props.get("EOTBytes")) != null) {
            eotBytes = MessageUtils.hexString2Bytes((String) o);
            if (eotBytes != null && eotBytes.length > 0)
                boundary += MS_EOT;
        }

        if ("request".equals(operation) && eotBytes.length <= 0 &&
            sotBytes.length <= 0) { // check pattern for termination
            o = props.get("Pattern");
            if (o instanceof String && ((String) o).length() > 0) try {
                Perl5Compiler pc = new Perl5Compiler();
                pm = new Perl5Matcher();
                pattern = pc.compile((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(uri + ": " + e.toString()));
            }
        }

        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength = Long.parseLong((String) o);

        if (boundary == 0 && "request".equals(operation)) { // default
            eotBytes = MessageUtils.hexString2Bytes("0x0a");
            boundary += MS_EOT;
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It reads the byte stream from an InputStream, packages them
     * into JMS Messages and adds the messages to an XQueue.
     * It supports flexible but static delimiters at either
     * or both ends.  It also has the blind trim support at both ends.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like read,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * This is MT-Safe as long as the threads share the same offhead,
     * offtail, sotBytes and eotBytes.
     */
    public long read(InputStream in, XQueue xq) throws IOException,
        JMSException {
        return read(in, xq, null, null);
    }

    /**
     * It reads the byte stream from an InputStream, packages them into
     * JMS Messages and sets the properties before adding the messages to
     * an XQueue.  It supports flexible but static delimiters at either
     * or both ends.  It also has the blind trim support at both ends.
     * At the end, it returns number of messages read from input stream.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like read,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * The class variable controls the reaction on EOF.  If the value is set to
     * command mode, it just exits when an EOF is read.  For Linux comm port,  
     * the timeout implementation returns EOF.  Therefore, setting the value
     * to ignore mode will have the method to ignore EOF and continue to read.  
     * If the value is set to daemon, it will throw IOException whenever it
     * reads an EOF to indicate connection reset by peer.
     *<br><br>
     * This is MT-Safe as long as the threads share the same offhead,
     * offtail, sotBytes and eotBytes.
     */
    public long read(InputStream in, XQueue xq, String[] pNames,
        String[] pValues) throws IOException, JMSException {
        Message outMessage;
        int sid = -1, cid, mask = 0;
        int position = 0;
        int offset = 0;
        int bytesRead = 0;
        int totalBytes = 0;
        int retry = 0, len = 0, rawLen = 0, skip;
        int dmask = MessageUtils.SHOW_BODY | MessageUtils.SHOW_SIZE;
        long count = 0, stm = 10;
        DelimitedBuffer sBuf = null;
        byte[] buffer;
        byte[] rawBuf;
        StringBuffer textBuffer = null;
        boolean isSleepy = (sleepTime > 0);
        boolean isNewMsg = false;
        boolean isText;
        boolean ignoreEOF = false;
        boolean isDaemon = false;
        int shift = partition[0];
        int length = partition[1];

        try {
            sBuf = new DelimitedBuffer(bufferSize, offhead, sotBytes,
                offtail, eotBytes);
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to created buffer for read: " +
                Event.traceStack(e)).send();
            return 0;
        }
        catch (Error e) {
            new Event(Event.ERR, uri + " failed to created buffer for read: " +
                Event.traceStack(e)).send();
            Event.flush(e);
        }

        buffer = sBuf.getBuffer();
        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        // hack for RXTX driver and Rocketport comm port on Linux
        if (mode < 0) // for Linux comm port to ignore EOF
            ignoreEOF = true;
        else if (mode > 0) // need to react on end of stream
            isDaemon = true;

        if (pNames == null || pNames.length <= 0) {
            pNames = propertyName;
            pValues = propertyValue;
        }

        dmask ^= displayMask;
        dmask &= displayMask;
        switch (length) {
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
                sid = xq.reserve(waitTime, shift, length);
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
            isText = (outMessage instanceof TextMessage);
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "faileded to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return count;
        }
        else {
            return count;
        }

        if ((boundary & MS_SOT) == 0) { // SOT not defined
            isNewMsg = true;
            if (isText)
                textBuffer = new StringBuffer();
        }

        /*
         * rawBuf stores the last rawLen bytes on each read from the stream.
         * The bytes before the stored bytes will be put into message buffer.
         * This way, the bytes at tail can be trimed before message is sent
         * len: current byte count of rawBuf
         * skip: current byte count to skip
         */
        rawLen = ((boundary & MS_EOT) == 0) ? sotBytes.length+offtail : offtail;
        rawBuf = new byte[rawLen];
        skip = offhead;

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // paused temporarily
                xq.cancel(sid);
                sid = -1;
                break;
            }
            try {
                bytesRead = in.read(buffer, 0, bufferSize);
            }
            catch (InterruptedIOException e) { // timeout for socket
                if (mode == 0 && maxNumberMsg == 1)
                    // special case for single msg read
                    break;
                else
                    continue;
            }
            catch (IOException e) {
                xq.cancel(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new RuntimeException(e.toString()));
            }
            catch (Error e) {
                xq.cancel(sid);
                Event.flush(e);
            }
            if (bytesRead <= 0) { // timeout or EOF
                if (bytesRead == 0 || ignoreEOF) // timeout for comm port
                    continue;
                else if (isDaemon) { // end of stream
                    xq.cancel(sid);
                    throw(new IOException("end of stream"));
                }
                else // EOF
                    break;
            }
            offset = 0;
            do { // process all bytes read out of in
                position = sBuf.scan(offset, bytesRead);

                if (position > 0) { // found SOT
                    int k = position - offset - sotBytes.length;
                    totalBytes += k;
                    if ((boundary & MS_EOT) == 0 && skip > 0) {
                        // still have bytes to skip
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if ((boundary & MS_EOT) == 0 && // no eotBytes defined
                        isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (isText && len > 0)
                                textBuffer.append(new String(rawBuf,0,len));
                            else if (len > 0)
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, len);

                            // fill in the first k-rawLen bytes to msg buf
                            if (isText)
                                textBuffer.append(new String(buffer, offset,
                                    k - rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(buffer,
                                    offset, k - rawLen);
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            if (isText)
                                textBuffer.append(new String(rawBuf, 0,
                                    k+len-rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, k+len-rawLen);
                        }
                        len = 0;
                        totalBytes -= rawLen;
                        if (totalBytes <= 0 && count == 0) {
                            xq.cancel(sid);
                        }
                        else {
                            if (pNames != null && pValues != null) {
                                outMessage.clearProperties();
                                for (int i=0; i<pNames.length; i++)
                                   MessageUtils.setProperty(pNames[i],
                                       pValues[i], outMessage);
                            }
                            if (isText) {
                                ((TextMessage) outMessage).setText(
                                    textBuffer.toString());
                                textBuffer = null;
                            }

                            outMessage.setJMSTimestamp(
                                System.currentTimeMillis());

                            cid = xq.add(outMessage, sid);
                            if (cid > 0) {
                                if (displayMask > 0) try {
                                    String line = MessageUtils.display(
                                        outMessage, "", dmask, null);
                                    new Event(Event.INFO, "read " +
                                        totalBytes + " bytes from " + uri +
                                        " into a msg ("+ line+ " )").send();
                                }
                                catch (Exception e) {
                                    new Event(Event.INFO, "read " +
                                        totalBytes + " bytes from " + uri +
                                        " into a msg").send();
                                }
                                count ++;
                                if (maxNumberMsg > 0 && count>=maxNumberMsg)
                                    return count;
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
                                new Event(Event.WARNING, xq.getName() +
                                    " is full and lost " + totalBytes +
                                    " bytes read from " + uri).send();
                            }
                        }

                        sid = -1;
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0) switch (length) {
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
                                sid = xq.reserve(waitTime, shift, length);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0);
                            break;
                        }

                        if (sid >= 0) {
                            if (isText)
                                outMessage = new TextEvent();
                            else
                                outMessage = new BytesEvent();
                        }
                        else {
                            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0)
                                new Event(Event.ERR,
                                    "failed to reserve a cell on "+
                                    xq.getName()+ " for " + uri).send();
                            else if (displayMask != 0 && maxNumberMsg != 1)
                                new Event(Event.INFO, "read " + count +
                                    " msgs from " + uri).send();
                            return count;
                        }
                    }
                    else if (isNewMsg) { // missing EOT or msg too large
                        outMessage.clearBody();
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " + totalBytes +
                                " bytes out of " + uri).send();
                    }

                    // act as if it read all SOT bytes
                    skip = offhead;
                    k = sotBytes.length;
                    totalBytes = sotBytes.length;
                    offset = 0;
                    if (skip > 0) { // need to skip bytes
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all SOT bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (k > rawLen) { // read more bytes than rawBuf
                        // fill in the first k-rawLen bytes to msg buf
                        if (isText)
                            textBuffer = new StringBuffer(
                                new String(sotBytes, offset, k - rawLen));
                        else
                            ((BytesMessage) outMessage).writeBytes(sotBytes,
                                offset, k - rawLen);

                        // pre-store the last rawLen bytes to rawBuf
                        for (int i=1; i<=rawLen; i++)
                            rawBuf[rawLen-i] = sotBytes[sotBytes.length-i];
                        len = rawLen;
                    }
                    else { // not enough to fill up rawBuf
                        len = k;
                        for (int i=0; i<len; i++)
                            rawBuf[i] = sotBytes[offset+i];
                        if (isText)
                            textBuffer = new StringBuffer();
                    }

                    isNewMsg = true;
                    offset = position;
                }
                else if (position < 0) { // found EOT
                    int k;
                    position = -position;
                    k = position - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (isText && len > 0)
                                textBuffer.append(new String(rawBuf,0,len));
                            else if (len > 0)
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, len);

                            // fill in the first k-rawLen bytes to msg buf
                            if (isText)
                                textBuffer.append(new String(buffer, offset,
                                    k - rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(buffer,
                                    offset, k - rawLen);
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            if (isText)
                                textBuffer.append(new String(rawBuf, 0,
                                    k + len - rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, k + len - rawLen);
                        }
                        // reset current byte count of rawBuf
                        len = 0;
                        totalBytes -= rawLen;
                        if (pNames != null && pValues != null) {
                            outMessage.clearProperties();
                            for (int i=0; i<pNames.length; i++)
                               MessageUtils.setProperty(pNames[i], pValues[i],
                                   outMessage);
                        }
                        if (isText) {
                            ((TextMessage) outMessage).setText(
                                textBuffer.toString());
                            textBuffer = null;
                        }

                        outMessage.setJMSTimestamp(System.currentTimeMillis());

                        cid = xq.add(outMessage, sid);
                        if (cid > 0) {
                            if (displayMask > 0) try {
                                String line = MessageUtils.display(
                                    outMessage, "", dmask, null);
                                new Event(Event.INFO, "read " +
                                    totalBytes + " bytes from " + uri +
                                    " into a msg ("+ line+ " )").send();
                            }
                            catch (Exception e) {
                                new Event(Event.INFO, "read " +
                                    totalBytes + " bytes from " + uri +
                                    " into a msg").send();
                            }
                            count ++;
                            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                                return count;
                            if (isSleepy) { // slow down a while
                                long tm = System.currentTimeMillis()+sleepTime;
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
                            new Event(Event.WARNING, xq.getName() +
                                " is full and lost " + totalBytes +
                                " bytes read from " + uri).send();
                        }
                    }
                    else { // garbage at beginning or msg too large
                        xq.cancel(sid);
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " +
                                totalBytes + " bytes out of " + uri).send();
                    }
                    totalBytes = 0;
                    skip = offhead;
                    len = 0;
                    offset = position;
                    if ((boundary & MS_SOT) == 0) {
                        isNewMsg = true;
                        if (isText)
                            textBuffer = new StringBuffer();
                    }
                    else
                        isNewMsg = false;

                    sid = -1;
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0) switch (length) {
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
                            sid = xq.reserve(waitTime, shift, length);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                    }

                    if (sid >= 0) {
                        if (isText)
                            outMessage = new TextEvent();
                        else
                            outMessage = new BytesEvent();
                    }
                    else {
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0)
                            new Event(Event.ERR,
                                "failed to reserve a cell on "+
                                xq.getName() + " for " + uri).send();
                        else if (displayMask != 0 && maxNumberMsg != 1)
                            new Event(Event.INFO, "read " + count +
                                " msgs from " + uri).send();
                        return count;
                    }
                }
                else if (isNewMsg) { // read k bytes with no SOT nor EOT
                    int k = bytesRead - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped and done
                            skip -= k;
                            k = 0;
                            offset = bytesRead;
                            continue;
                        }
                    }
                    if (totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer 
                            if (isText && len > 0)
                                textBuffer.append(new String(rawBuf,0,len));
                            else if (len > 0)
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, len);

                            // fill in the first k-rawLen bytes to msg buf
                            if (isText)
                                textBuffer.append(new String(buffer, offset,
                                    k - rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(buffer,
                                    offset, k - rawLen);

                            // pre-store the last rawLen bytes to rawBuf
                            for (int i=1; i<=rawLen; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            if (isText)
                                textBuffer.append(new String(rawBuf, 0,
                                    k + len - rawLen));
                            else
                                ((BytesMessage) outMessage).writeBytes(rawBuf,
                                    0, k + len - rawLen);

                            // shift rawLen-k bytes to beginning of rawBuf
                            for (int i=0; i<rawLen-k; i++)
                                rawBuf[i] = rawBuf[k+len-rawLen+i];

                            // pre-store all k bytes to the end of rawBuf
                            for (int i=1; i<=k; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else { // not enough to fill up rawBuf
                            for (int i=0; i<k; i++)
                                rawBuf[len+i] = buffer[offset+i];
                            len += k;
                        }
                    }
                    offset = bytesRead;
                }
                else {
                    offset = bytesRead;
                }
            } while (offset < bytesRead &&
                (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
            if ((mask & XQueue.KEEP_RUNNING) == 0 || (mask & XQueue.PAUSE) > 0)
                break;
        }
        if(sid >= 0 && boundary <= MS_SOT && bytesRead == -1 && totalBytes > 0){
            if (totalBytes - rawLen <= maxMsgLength) {
                if (pNames != null && pValues != null) {
                    outMessage.clearProperties();
                    for (int i=0; i<pNames.length; i++)
                       MessageUtils.setProperty(pNames[i],
                           pValues[i], outMessage);
                }
                if (isText)
                    ((TextMessage) outMessage).setText(textBuffer.toString());

                outMessage.setJMSTimestamp(System.currentTimeMillis());

                cid = xq.add(outMessage, sid);
                if (cid > 0) {
                    if (displayMask > 0) try {
                        String line = MessageUtils.display(outMessage,
                            "", dmask, null);
                        new Event(Event.INFO, "read " + totalBytes +
                            " bytes from " + uri + " into a msg (" +
                            line + " )").send();
                    }
                    catch (Exception e) {
                        new Event(Event.INFO, "read " + totalBytes +
                            " bytes from " + uri + " into a msg").send();
                    }
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.WARNING, xq.getName()+" is full and lost "+
                        totalBytes + " bytes read from " + uri).send();
                }
            }
            else {
                xq.cancel(sid);
                new Event(Event.INFO, "discarded " + totalBytes +
                    " bytes out of " + uri).send();
            }
        }
        else if (sid >= 0) {
            xq.cancel(sid);
            sid = -1;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "read " + count + " msgs from " + uri).send();
        return count;
    }

    /**
     * It collects the responses from the XQueue and writes the responses to
     * the OutputStream if there is any.  Upon success, it returns number
     * of responded messages.
     */
    private int feedback(XQueue xq, OutputStream out, QList msgList,
        CollectibleCells cells, Template temp, byte[] buffer) {
        int id, k = 0;
        Message msg;
        String msgStr;

        while ((id = cells.collect(waitTime)) >= 0) {
            msg = (Message) msgList.takeback(id);
            msgStr = send(out, msg, temp, buffer);
            k ++;
            if (displayMask > 0) try {
                String line = MessageUtils.display(msg, msgStr,
                    displayMask, null);
                new Event(Event.INFO, "responded on a request from " +
                    uri + " with a msg (" +line+ " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "responded on a request from " +
                    uri + " with a msg").send();
            }
        }
        return k;
    }

    private String send(OutputStream out, Message message, Template temp,
        byte[] buffer) {
        int length;
        String msgStr = null;

        if (message == null || out == null)
            return null;
        try {
            if (temp != null)
                msgStr = MessageUtils.format(message, buffer, temp);
            else
                msgStr = MessageUtils.processBody(message, buffer);
        }
        catch (JMSException e) {
            return null;
        }

        if (msgStr == null)
            return null;

        length = msgStr.length();
        if (length > 0) try {
            out.write(msgStr.getBytes(), 0, length);
            out.flush();
        }
        catch (IOException e) {
            new Event(Event.WARNING, uri + " failed to send response").send();
        }
        return msgStr;
    }

    /**
     * It reads the byte stream from an InputStream, packages them into
     * JMS Messages and adds the messages to an XQueue as the request.  Then
     * it continually monitors the XQueue for the reply messages.  If it finds
     * one, the message will be sent back to the OutputStream associated with
     * the InputStream as the response.  Since there is no blocking between
     * the request and the response, it is possible for the thread to process
     * two requests before any response sent back.  In order for the requester
     * to tell a response is corresponding to which request, each request
     * message should contains an ID even though it is unknown to this method.
     * So the downstream message flow is not supposed to modify or remove that
     * ID.  It supports flexible but static delimiters at either or both ends.
     * It also has the blind trim support at both ends.
     *<br><br>
     * The msgList is a dedicated instance of QList for tracking the
     * collectible responses. The sBuf is also a dedicated buffer with
     * delimiter supports. The last argument is a dedicate instance of
     * CollectibleCells for reverse propagations.  Its capacity should be same
     * as the xq.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like read,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * To support re-entrance, it depends on msgList and sBuf for state info.
     * Please make sure sBuf has the same properties as the instance.
     * This is MT-Safe as long as the threads share the same offhead, offtail,
     * sotBytes and eotBytes.
     */
    public long respond(InputStream in, XQueue xq, OutputStream out,
        QList msgList, DelimitedBuffer sBuf, CollectibleCells cells)
        throws IOException, JMSException {
        Message outMessage;
        Template temp = null;
        java.lang.reflect.Method parse = null;
        Object parser = null;
        int sid = -1, cid;
        int position = 0;
        int offset = 0;
        int bytesRead = 0;
        int totalBytes = 0;
        int retry = 0, len = 0, rawLen = 0, skip, mask = 0;
        long count = 0;
        StringBuffer textBuffer = null;
        String msgStr;
        byte[] buffer;
        byte[] buf = new byte[bufferSize];
        byte[] rawBuf;
        boolean isNewMsg = false;
        boolean isText;
        boolean ignoreEOF = false;
        int shift = partition[0];
        int length = partition[1];

        if (msgList == null) {
            new Event(Event.ERR, uri + " msg list is null").send();
            return count;
        }
        else if (msgList.getCapacity() != xq.getCapacity()) {
            new Event(Event.ERR, uri + " msg list has wrong lenth: " +
                msgList.getCapacity() +"/"+ xq.getCapacity()).send();
            return count;
        }
        if (sBuf == null) {
            new Event(Event.ERR, uri + " dynamic buffer is null").send();
            return count;
        }
        else if (sBuf.getBoundary() != boundary) {
            new Event(Event.ERR, uri + " dynamic buffer has wrong params: " +
                sBuf.getBoundary() +"/"+ boundary).send();
            return count;
        }
        if (cells == null || ackMethod == null) {
            new Event(Event.ERR, uri+": cells is null or no ack method").send();
            return count;
        }
        else if (cells.getCapacity() < shift + length) {
            new Event(Event.ERR, uri + ": cells " + cells.getName() +
              " is too short: "+cells.getCapacity()+"/"+(shift+length)).send();
            return count;
        }

        // hack for RXTX driver and Rocketport comm port on Linux
        if (mode < 0) // for Linux comm port to ignore EOF
            ignoreEOF = true;

        buffer = sBuf.getBuffer();
        if (template != null) // each thread has its own template instance
            temp = new Template(template.copyText());
        if (parserProps != null) { // parser defined
            Object[] o = MessageUtils.getPlugins(parserProps, "ParserArgument",
                "parse", new String[] {"java.lang.String"}, null, xq.getName());
            parse = (java.lang.reflect.Method) o[0];
            parser = o[1];
        }

        switch (length) {
          case 0:
            do {
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            do {
                sid = xq.reserve(waitTime, shift);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            do {
                sid = xq.reserve(waitTime, shift, length);
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
            isText = (outMessage instanceof TextMessage);
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " + xq.getName()+
                " for " + uri).send();
            return count;
        }
        else {
            if (displayMask != 0 && maxNumberMsg != 1)
                new Event(Event.INFO, "responded " + count + " msgs from " +
                    uri).send();
            return count;
        }

        if ((boundary & MS_SOT) == 0) { // SOT not defined
            isNewMsg = true;
            textBuffer = new StringBuffer();
        }

        /*
         * rawBuf stores the last rawLen bytes on each read from the stream.
         * The bytes before the stored bytes will be put into message buffer.
         * This way, the bytes at tail can be trimed before message is sent
         * len: current byte count of rawBuf
         * skip: current byte count to skip
         */
        rawLen = ((boundary & MS_EOT) == 0) ? sotBytes.length+offtail : offtail;
        rawBuf = new byte[rawLen];
        skip = offhead;

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // standby temporarily
                xq.cancel(sid);
                if (msgList.depth() > 0)
                    count += feedback(xq, out, msgList, cells, temp, buf);
                break;
            }
            try {
                bytesRead = in.read(buffer, 0, bufferSize);
            }
            catch (InterruptedIOException e) { // timeout for socket
                if (msgList.depth() > 0)
                    count += feedback(xq, out, msgList, cells, temp, buf);
                continue;
            }
            catch (IOException e) {
                xq.cancel(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new RuntimeException(e.toString()));
            }
            catch (Error e) {
                xq.cancel(sid);
                Event.flush(e);
            }
            if (bytesRead <= 0) { // timeout or end of stream
                if (msgList.depth() > 0)
                    count += feedback(xq, out, msgList, cells, temp, buf);
                if (bytesRead == 0 || ignoreEOF) // timeout for comm port
                    continue;
                else // end of stream
                    break;
            }
            offset = 0;
            do { // process all bytes read out of in
                position = sBuf.scan(offset, bytesRead);

                if (position > 0) { // found SOT
                    int k = position - offset - sotBytes.length;
                    totalBytes += k;
                    if ((boundary & MS_EOT) == 0 && skip > 0) {
                        // still have bytes to skip
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if ((boundary & MS_EOT) == 0 && // no eotBytes defined
                        isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                textBuffer.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            textBuffer.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            textBuffer.append(new String(rawBuf, 0,
                                k+len-rawLen));
                        }
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = textBuffer.toString();
                        textBuffer = null;
                        if (totalBytes <= 0 && count == 0) {
                            xq.cancel(sid);
                            sid = -1;
                        }
                        else try {
                            if (propertyName!=null && propertyValue!=null) {
                                outMessage.clearProperties();
                                for (int i=0; i<propertyName.length; i++)
                                   MessageUtils.setProperty(propertyName[i],
                                       propertyValue[i], outMessage);
                            }
                            if (parse != null) {
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                            }
                            else if (isText) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes());
                            }

                            outMessage.setJMSTimestamp(
                                System.currentTimeMillis());
                            ((JMSEvent) outMessage).setAckObject(cells,
                                ackMethod, new long[]{sid});
                        }
                        catch (JMSException e) {
                            xq.cancel(sid);
                            throw(e);
                        }
                        catch (Exception e) {
                            xq.cancel(sid);
                            throw(new RuntimeException(e.toString()));
                        }
                        catch (Error e) {
                            xq.cancel(sid);
                            Event.flush(e);
                        }

                        if (sid >= 0) { // message is ready
                            msgList.reserve(sid);
                            msgList.add(outMessage, sid);
                            cid = xq.add(outMessage, sid);
                            if (cid <= 0) {
                                msgList.takeback(sid);
                                xq.cancel(sid);
                                new Event(Event.ERR, xq.getName() +
                                    " is full and lost " + totalBytes +
                                    " bytes read from " + uri).send();
                            }
                        }

                        count += feedback(xq, out, msgList, cells, temp, buf);

                        sid = -1;
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0) switch (length) {
                          case 0:
                            do {
                                sid = xq.reserve(waitTime);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0);
                            break;
                          case 1:
                            do {
                                sid = xq.reserve(waitTime, shift);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0);
                            break;
                          default:
                            do {
                                sid = xq.reserve(waitTime, shift, length);
                                if (sid >= 0)
                                    break;
                                mask = xq.getGlobalMask();
                            } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0);
                            break;
                        }

                        if (msgList.depth() > 0)
                            count += feedback(xq, out, msgList, cells,temp,buf);

                        if (sid >= 0) {
                            if (msgList.getNextID(sid) >= 0) { //need to respond
                                cells.collect(-1L, sid);
                                Message msg = (Message) msgList.browse(sid);
                                String line = send(out, msg, temp, buf);
                                if (displayMask > 0) try {
                                    line = MessageUtils.display(msg, line,
                                        displayMask, null);
                                    new Event(Event.INFO, "responded on a "+
                                        "request from "+ uri + " with a msg (" +
                                        line + " )").send();
                                }
                                catch (Exception e) {
                                    new Event(Event.INFO,
                                        "respnded on a request from " + uri +
                                        " with a msg").send();
                                }
                                msgList.remove(sid);
                                count ++;
                            }
                            if (isText)
                                outMessage = new TextEvent();
                            else
                                outMessage = new BytesEvent();
                        }
                        else {
                            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                                (mask & XQueue.PAUSE) == 0)
                                new Event(Event.ERR,
                                    "failed to reserve a cell on " +
                                    xq.getName() + " for " + uri).send();
                            else if (displayMask != 0 && maxNumberMsg != 1)
                                new Event(Event.INFO, "responded " + count +
                                    " msgs from " + uri).send();
                            return count;
                        }
                    }
                    else if (isNewMsg) { // missing EOT or msg too large
                        outMessage.clearBody();
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " + totalBytes +
                                " bytes out of " + uri).send();
                    }

                    // act as if it read all SOT bytes
                    skip = offhead;
                    k = sotBytes.length;
                    totalBytes = sotBytes.length;
                    offset = 0;
                    if (skip > 0) { // need to skip bytes
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all SOT bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (k > rawLen) { // read more bytes than rawBuf
                        // fill in the first k-rawLen bytes to msg buf
                        textBuffer = new StringBuffer(
                            new String(sotBytes, offset, k - rawLen));

                        // pre-store the last rawLen bytes to rawBuf
                        for (int i=1; i<=rawLen; i++)
                            rawBuf[rawLen-i] = sotBytes[sotBytes.length-i];
                        len = rawLen;
                    }
                    else { // not enough to fill up rawBuf
                        len = k;
                        for (int i=0; i<len; i++)
                            rawBuf[i] = sotBytes[offset+i];
                        textBuffer = new StringBuffer();
                    }

                    isNewMsg = true;
                    offset = position;
                }
                else if (position < 0) { // found EOT
                    int k;
                    position = -position;
                    k = position - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                textBuffer.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            textBuffer.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            textBuffer.append(new String(rawBuf, 0,
                                k + len - rawLen));
                        }
                        // reset current byte count of rawBuf
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = textBuffer.toString();
                        textBuffer = null;
                        try {
                            if (propertyName!=null && propertyValue!=null) {
                                outMessage.clearProperties();
                                for (int i=0; i<propertyName.length; i++)
                                   MessageUtils.setProperty(propertyName[i],
                                        propertyValue[i], outMessage);
                            }
                            if (parse != null) {
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                            }
                            else if (isText) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes());
                            }

                            outMessage.setJMSTimestamp(
                                System.currentTimeMillis());
                            ((JMSEvent) outMessage).setAckObject(cells,
                                ackMethod, new long[]{sid});
                        }
                        catch (JMSException e) {
                            xq.cancel(sid);
                            throw(e);
                        }
                        catch (Exception e) {
                            xq.cancel(sid);
                            throw(new RuntimeException(e.toString()));
                        }
                        catch (Error e) {
                            xq.cancel(sid);
                            Event.flush(e);
                        }

                        msgList.reserve(sid);
                        msgList.add(outMessage, sid);
                        cid = xq.add(outMessage, sid);
                        if (cid <= 0) {
                            msgList.takeback(sid);
                            xq.cancel(sid);
                            new Event(Event.WARNING, xq.getName() +
                                " is full and lost " + totalBytes +
                                " bytes read from " + uri).send();
                        }

                        count += feedback(xq, out, msgList, cells, temp, buf);
                    }
                    else { // garbage at beginning or msg too large
                        xq.cancel(sid);
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " + totalBytes +
                                " bytes out of " + uri).send();
                    }
                    totalBytes = 0;
                    skip = offhead;
                    len = 0;
                    offset = position;
                    if ((boundary & MS_SOT) == 0) {
                        isNewMsg = true;
                        textBuffer = new StringBuffer();
                    }
                    else
                        isNewMsg = false;

                    sid = -1;
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0) switch (length) {
                      case 0:
                        do {
                            sid = xq.reserve(waitTime);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                      case 1:
                        do {
                            sid = xq.reserve(waitTime, shift);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                      default:
                        do {
                            sid = xq.reserve(waitTime, shift, length);
                            if (sid >= 0)
                                break;
                            mask = xq.getGlobalMask();
                        } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0);
                        break;
                    }

                    if (msgList.depth() > 0)
                        count += feedback(xq, out, msgList, cells, temp, buf);

                    if (sid >= 0) {
                        if (msgList.getNextID(sid) >= 0) { // need to respond
                            cells.collect(-1L, sid);
                            Message msg = (Message) msgList.browse(sid);
                            String line = send(out, msg, temp, buf);
                            if (displayMask > 0) try {
                                line = MessageUtils.display(msg, line,
                                    displayMask, null);
                                new Event(Event.INFO, "responded on a " +
                                    "request from "+ uri + " with a msg (" +
                                    line + " )").send();
                            }
                            catch (Exception e) {
                                new Event(Event.INFO,
                                    "respnded on a request from " + uri +
                                    " with a msg").send();
                            }
                            msgList.remove(sid);
                            count ++;
                        }
                        if (isText)
                            outMessage = new TextEvent();
                        else
                            outMessage = new BytesEvent();
                    }
                    else {
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) == 0)
                            new Event(Event.ERR, "failed to reserve a cell on "+
                                xq.getName() + " for " + uri).send();
                        else if (displayMask != 0 && maxNumberMsg != 1)
                            new Event(Event.INFO, "responded " + count +
                                " msgs from " + uri).send();
                        return count;
                    }
                }
                else if (isNewMsg) { // read k bytes with no SOT nor EOT
                    int k = bytesRead - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped and done
                            skip -= k;
                            k = 0;
                            offset = bytesRead;
                            continue;
                        }
                    }
                    if (totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer 
                            if (len > 0)
                                textBuffer.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            textBuffer.append(new String(buffer, offset,
                                k - rawLen));

                            // pre-store the last rawLen bytes to rawBuf
                            for (int i=1; i<=rawLen; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            textBuffer.append(new String(rawBuf, 0,
                                k + len - rawLen));

                            // shift rawLen-k bytes to beginning of rawBuf
                            for (int i=0; i<rawLen-k; i++)
                                rawBuf[i] = rawBuf[k+len-rawLen+i];

                            // pre-store all k bytes to the end of rawBuf
                            for (int i=1; i<=k; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else { // not enough to fill up rawBuf
                            for (int i=0; i<k; i++)
                                rawBuf[len+i] = buffer[offset+i];
                            len += k;
                        }
                    }
                    offset = bytesRead;
                }
                else {
                    offset = bytesRead;
                }
                if (totalBytes > bufferSize && msgList.depth() > 0)
                    count += feedback(xq, out, msgList, cells, temp, buf);
            } while (offset < bytesRead &&
                (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
            if ((mask & XQueue.KEEP_RUNNING) == 0 || (mask & XQueue.PAUSE) > 0)
                break;
        }
        if(sid >= 0 && boundary <= MS_SOT && bytesRead == -1 && totalBytes > 0){
            msgStr = textBuffer.toString();
            textBuffer = null;
            if (totalBytes - rawLen <= maxMsgLength) {
                try {
                    if (propertyName != null && propertyValue != null) {
                        outMessage.clearProperties();
                        for (int i=0; i<propertyName.length; i++)
                            MessageUtils.setProperty(propertyName[i],
                                propertyValue[i], outMessage);
                    }
                    if (parse != null) {
                        Throwable t = MessageUtils.parse(parse, parser, msgStr,
                            outMessage);
                        if (t != null) {
                            if (t instanceof Exception)
                                throw((Exception) t);
                            else
                                throw((Error) t);
                        }
                    }
                    else if (isText) {
                        ((TextMessage) outMessage).setText(msgStr);
                    }
                    else {
                       ((BytesMessage)outMessage).writeBytes(msgStr.getBytes());
                    }

                    outMessage.setJMSTimestamp(System.currentTimeMillis());
                    ((JMSEvent) outMessage).setAckObject(cells, ackMethod,
                        new long[]{sid});
                }
                catch (JMSException e) {
                    xq.cancel(sid);
                    throw(e);
                }
                catch (Exception e) {
                    xq.cancel(sid);
                    throw(new RuntimeException(e.toString()));
                }
                catch (Error e) {
                    xq.cancel(sid);
                    Event.flush(e);
                }

                msgList.reserve(sid);
                msgList.add(outMessage, sid);
                cid = xq.add(outMessage, sid);
                if (cid <= 0) {
                    msgList.takeback(sid);
                    xq.cancel(sid);
                    new Event(Event.WARNING, xq.getName()+" is full and lost "+
                        totalBytes + " bytes read from " + uri).send();
                }
                count += feedback(xq, out, msgList, cells, temp, buf);
            }
            else {
                xq.cancel(sid);
                new Event(Event.INFO, "discarded " + totalBytes +
                    " bytes out of " + uri).send();
            }
        }
        else if (sid >= 0) {
            xq.cancel(sid);
            sid = -1;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "responded " + count + " msgs from " +
                uri).send();
        return count;
    }

    /**
     * It scans the each occupied msg via id in msgList to check if it is
     * expired or not.  If it is expired, the msg will be removed from the
     * both msgList and the xqueue. It returns the number of msgs expired
     * and removed from the list. The expiration is set by the requester.
     * If it is not set, the default is to never get expired.
     */
    private int expire(QList msgList, XQueue xq) {
        Message msg;
        Browser browser;
        int k, sid;
        long t, currentTime;
        boolean ack;

        ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        browser = msgList.browser();
        currentTime = System.currentTimeMillis();
        k = 0;
        while ((sid = browser.next()) >= 0) {
            msg = (Message) xq.browse(sid);
            if (msg == null)
                continue;
            else try {
                t = msg.getJMSExpiration();
            }
            catch (Exception e) {
                t = 0L;
            }
            if (t > 0L && t <= currentTime) { // msg expired
                k ++;
                if (ack) try { // try to ack the msg
                    msg.acknowledge();
                }
                catch (Exception ex) {
                }
                msgList.remove(sid);
                xq.remove(sid);
                if (displayMask > 0) try {
                    new Event(Event.INFO, "expired a request at " + sid +
                        " to " + uri + " (" + MessageUtils.display(msg, null,
                        displayMask, null) + " )").send();
                }
                catch (Exception e) {
                }
            }
        }

        return k;
    }

    /**
     * It cleans up the wrong message and puts it back to msgList.
     */
    private void recover(int id, long tm, Message message, QList msgList) {
        if (id < 0 || message == null || msgList == null)
            return;
        try {
            message.clearProperties();
            message.clearBody();
            message.setJMSTimestamp(tm);
            MessageUtils.setProperty(rcField, "0", message);
        }
        catch (Exception e) {
        }
        msgList.putback(id);
    }

    /**
     * It reads the byte stream from the InputStream, packs them into the
     * response buffer.  Then it calls the parser to parse the text and
     * retrieves the SID from the rcField of the response.  The method uses
     * the SID to identify the corresponding request and loads the response
     * into the request message before removing it from the XQueue. It returns
     * 1 upon success to indicate one response received, or 0 otherwise.
     *<br><br>
     * If parser is defined, it will assume that each outgoing messages are
     * tagged with sid on the rcField.  The response is supposed to have the
     * same tag for the pending message.
     */
    private int receive(InputStream in, XQueue xq, QList msgList,
        DelimitedBuffer sBuf, Object parser, java.lang.reflect.Method parse)
        throws IOException, JMSException {
        Message outMessage;
        int sid = -1, cid;
        int position = 0;
        int offset = 0;
        int bytesRead = 0;
        int totalBytes = 0;
        int count = 0, retry = 0, len = 0, rawLen = 0, skip, leftover;
        byte[] buffer;
        byte[] rawBuf;
        String msgStr = null, line = null;
        long tm;
        boolean isNewMsg = false;
        boolean isText, ignoreEOF = false;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        leftover = sBuf.getLeftover();
        buffer = sBuf.getBuffer();

        // hack for RXTX driver and Rocketport comm port on Linux
        if (mode < 0) // for Linux comm port to ignore EOF
            ignoreEOF = true;

        if ((boundary & MS_SOT) == 0) // SOT not defined
            isNewMsg = true;

        /*
         * rawBuf stores the last rawLen bytes on each read from the stream.
         * The bytes before the stored bytes will be put into message buffer.
         * This way, the bytes at tail can be trimed before message is sent
         * len: current byte count of rawBuf
         * skip: current byte count to skip
         */
        rawLen = ((boundary & MS_EOT) == 0) ? sotBytes.length+offtail : offtail;
        rawBuf = new byte[rawLen];
        skip = offhead;

        if (leftover > 0) { // process leftover in the buffer first
            offset = 0;
            bytesRead = leftover;
            do { // process all bytes read out of in
                position = sBuf.scan(offset, bytesRead);

                if (position > 0) { // found SOT
                    int k = position - offset - sotBytes.length;
                    totalBytes += k;
                    if ((boundary & MS_EOT) == 0 && skip > 0) {
                        // still have bytes to skip
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if ((boundary & MS_EOT) == 0 && // no eotBytes defined
                        isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                    k+len-rawLen));
                        }
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = msgBuf.toString();
                        msgBuf = new StringBuffer();
                        if (totalBytes <= 0 && count == 0) {
                            break;
                        }
                        else if ((sid = msgList.getNextID()) < 0) {
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }
                       else if((outMessage=(Message)msgList.browse(sid))==null){
                            msgList.remove(sid);
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }

                        // got a response back for the next pending msg
                        try {
                            if (parse != null) { // parse the response
                                tm = outMessage.getJMSTimestamp();
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    new Event(Event.ERR, "failed to parse "+
                                        totalBytes + " bytes from " + uri +
                                        ": " + msgStr).send();
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                                // retrieve the tag to compare with sid
                                line = MessageUtils.getProperty(rcField,
                                    outMessage);
                                if (!(String.valueOf(sid).equals(line))) {
                                    // response is not for sid, put it back
                                    recover(sid, tm, outMessage, msgList);
                                    cid = Integer.parseInt(line);
                                    sid = msgList.getNextID(cid);
                                    // retrieve the right pending message
                                    outMessage = (Message) msgList.browse(sid);
                                    t = MessageUtils.parse(parse, parser,
                                        msgStr, outMessage);
                                    if (t != null) {
                                        if (t instanceof Exception)
                                            throw((Exception) t);
                                        else
                                            throw((Error) t);
                                    }
                                }
                            }
                            else if (outMessage instanceof TextMessage) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes(), 0, totalBytes);
                            }
                            if (propertyName!=null && propertyValue!=null) {
                                for (int i=0; i<propertyName.length; i++)
                                   MessageUtils.setProperty(propertyName[i],
                                       propertyValue[i], outMessage);
                            }
                            if (displayMask > 0)
                                line = MessageUtils.display(outMessage, msgStr,
                                    displayMask, null);

                         outMessage.setJMSTimestamp(System.currentTimeMillis());
                        }
                        catch (JMSException e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Exception e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Error e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                e.toString()).send();
                            Event.flush(e);
                        }
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        msgList.remove(sid);
                        xq.remove(sid);
                        if (displayMask > 0)
                            new Event(Event.INFO, "requested " + totalBytes +
                                " bytes from " + uri + " (" +line+ " )").send();
                        return 1;
                    }
                    else if (isNewMsg) { // missing EOT or msg too large
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " +
                                totalBytes + " bytes out of " + uri).send();
                    }

                    // act as if it read all SOT bytes
                    msgBuf = new StringBuffer();
                    skip = offhead;
                    k = sotBytes.length;
                    totalBytes = sotBytes.length;
                    offset = 0;
                    if (skip > 0) { // need to skip bytes
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all SOT bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (k > rawLen) { // read more bytes than rawBuf
                        // fill in the first k-rawLen bytes to msg buf
                        msgBuf.append(new String(sotBytes, offset, k - rawLen));

                        // pre-store the last rawLen bytes to rawBuf
                        for (int i=1; i<=rawLen; i++)
                            rawBuf[rawLen-i] = sotBytes[sotBytes.length-i];
                        len = rawLen;
                    }
                    else { // not enough to fill up rawBuf
                        len = k;
                        for (int i=0; i<len; i++)
                            rawBuf[i] = sotBytes[offset+i];
                    }

                    isNewMsg = true;
                    offset = position;
                }
                else if (position < 0) { // found EOT
                    int k;
                    position = -position;
                    k = position - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                k + len - rawLen));
                        }
                        // reset current byte count of rawBuf
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = msgBuf.toString();
                        msgBuf = new StringBuffer();
                        if (totalBytes <= 0 && count == 0) {
                            break;
                        }
                        else if ((sid = msgList.getNextID()) < 0) {
                            new Event(Event.ERR, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }
                       else if((outMessage=(Message)msgList.browse(sid))==null){
                            msgList.remove(sid);
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }

                        // got a response back for the next pending msg
                        try {
                            if (parse != null) {  // parse the response
                                tm = outMessage.getJMSTimestamp();
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    new Event(Event.ERR, "failed to parse "+
                                        totalBytes + " bytes from " + uri +
                                        ": " + msgStr).send();
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                                // retrieve the tag to compare with sid
                                line = MessageUtils.getProperty(rcField,
                                    outMessage);
                                if (!(String.valueOf(sid).equals(line))) {
                                    // response is not for sid, put it back
                                    recover(sid, tm, outMessage, msgList);
                                    cid = Integer.parseInt(line);
                                    sid = msgList.getNextID(cid);
                                    // retrieve the right pending message
                                    outMessage = (Message) msgList.browse(sid);
                                    t = MessageUtils.parse(parse, parser,
                                        msgStr, outMessage);
                                    if (t != null) {
                                        if (t instanceof Exception)
                                            throw((Exception) t);
                                        else
                                            throw((Error) t);
                                    }
                                }
                            }
                            else if (outMessage instanceof TextMessage) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes(), 0, totalBytes);
                            }
                            if (propertyName != null && propertyValue != null) {
                                for (int i=0; i<propertyName.length; i++)
                                    MessageUtils.setProperty(propertyName[i],
                                        propertyValue[i], outMessage);
                            }
                            if (displayMask > 0)
                                line = MessageUtils.display(outMessage, msgStr,
                                    displayMask, null);

                         outMessage.setJMSTimestamp(System.currentTimeMillis());

                        }
                        catch (JMSException e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Exception e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Error e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                e.toString()).send();
                            Event.flush(e);
                        }
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        msgList.remove(sid);
                        xq.remove(sid);
                        if (displayMask > 0)
                            new Event(Event.INFO, "requested " + totalBytes +
                                " bytes from " + uri + " (" +line+ " )").send();
                        offset = position;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 1;
                    }  
                    else { // garbage at beginning or msg too large
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " +
                                totalBytes + " bytes from " + uri).send();
                        offset = position;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 0;
                    }
                }
                else if (isNewMsg) { // read k bytes with no SOT nor EOT
                    int k = bytesRead - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped and done
                            skip -= k;
                            k = 0;
                            offset = bytesRead;
                            continue;
                        }
                    }
                    if (totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer 
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));

                            // pre-store the last rawLen bytes to rawBuf
                            for (int i=1; i<=rawLen; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                k + len - rawLen));

                            // shift rawLen-k bytes to beginning of rawBuf
                            for (int i=0; i<rawLen-k; i++)
                                rawBuf[i] = rawBuf[k+len-rawLen+i];

                            // pre-store all k bytes to the end of rawBuf
                            for (int i=1; i<=k; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else { // not enough to fill up rawBuf
                            for (int i=0; i<k; i++)
                                rawBuf[len+i] = buffer[offset+i];
                            len += k;
                        }
                    }
                    offset = bytesRead;
                }
                else {
                    offset = bytesRead;
                }
            } while (offset < bytesRead &&
                (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
        }
        leftover = 0;
        sBuf.setLeftover(leftover);
        offset = 0;
        bytesRead = 0;

        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            try {
                bytesRead = in.read(buffer, 0, bufferSize);
            }
            catch (InterruptedIOException e) {
                bytesRead = 0;
            }
            catch (IOException e) {
                throw(e);
            }
            catch (Exception e) {
                throw(new RuntimeException(e.toString()));
            }
            catch (Error e) {
                Event.flush(e);
            }

            if (bytesRead == 0)
                break;
            else if (bytesRead < 0) { // timeout or end of stream
                if (ignoreEOF) // timeout for Linux comm port
                    break;
                else // end of stream on socket is not expected
                    throw(new IOException("end of stream"));
            }

            offset = 0;
            do { // process all bytes read out of in
                position = sBuf.scan(offset, bytesRead);

                if (position > 0) { // found SOT
                    int k = position - offset - sotBytes.length;
                    totalBytes += k;
                    if ((boundary & MS_EOT) == 0 && skip > 0) {
                        // still have bytes to skip
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if ((boundary & MS_EOT) == 0 && // no eotBytes defined
                        isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                    k+len-rawLen));
                        }
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = msgBuf.toString();
                        msgBuf = new StringBuffer();
                        if (totalBytes <= 0 && count == 0) {
                            break;
                        }
                        else if ((sid = msgList.getNextID()) < 0) {
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }
                       else if((outMessage=(Message)msgList.browse(sid))==null){
                            msgList.remove(sid);
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }

                        // got a response back for the next pending msg
                        try {
                            if (parse != null) { // parse the response
                                tm = outMessage.getJMSTimestamp();
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    new Event(Event.ERR, "failed to parse "+
                                        totalBytes + " bytes from " + uri +
                                        ": " + msgStr).send();
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                                // retrieve the tag to compare with sid
                                line = MessageUtils.getProperty(rcField,
                                    outMessage);
                                if (!(String.valueOf(sid).equals(line))) {
                                    // response is not for sid, put it back
                                    recover(sid, tm, outMessage, msgList);
                                    cid = Integer.parseInt(line);
                                    sid = msgList.getNextID(cid);
                                    // retrieve the right pending message
                                    outMessage = (Message) msgList.browse(sid);
                                    t = MessageUtils.parse(parse, parser,
                                        msgStr, outMessage);
                                    if (t != null) {
                                        if (t instanceof Exception)
                                            throw((Exception) t);
                                        else
                                            throw((Error) t);
                                    }
                                }
                            }
                            else if (outMessage instanceof TextMessage) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes(), 0, totalBytes);
                            }
                            if (propertyName!=null && propertyValue!=null) {
                                for (int i=0; i<propertyName.length; i++)
                                   MessageUtils.setProperty(propertyName[i],
                                       propertyValue[i], outMessage);
                            }
                            if (displayMask > 0)
                                line = MessageUtils.display(outMessage, msgStr,
                                    displayMask, null);

                         outMessage.setJMSTimestamp(System.currentTimeMillis());
                        }
                        catch (JMSException e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Exception e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Error e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                e.toString()).send();
                            Event.flush(e);
                        }
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        msgList.remove(sid);
                        xq.remove(sid);
                        if (displayMask > 0)
                            new Event(Event.INFO, "requested " + totalBytes +
                                " bytes from " + uri + " (" +line+ " )").send();
                        offset = position - sotBytes.length;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 1;
                    }
                    else if (isNewMsg) { // missing EOT or msg too large
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " +
                                totalBytes + " bytes out of " + uri).send();
                        msgBuf = new StringBuffer();
                        offset = position - sotBytes.length;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 0;
                    }

                    // act as if it read all SOT bytes
                    msgBuf = new StringBuffer();
                    skip = offhead;
                    k = sotBytes.length;
                    totalBytes = sotBytes.length;
                    offset = 0;
                    if (skip > 0) { // need to skip bytes
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all SOT bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (k > rawLen) { // read more bytes than rawBuf
                        // fill in the first k-rawLen bytes to msg buf
                        msgBuf.append(new String(sotBytes, offset, k - rawLen));

                        // pre-store the last rawLen bytes to rawBuf
                        for (int i=1; i<=rawLen; i++)
                            rawBuf[rawLen-i] = sotBytes[sotBytes.length-i];
                        len = rawLen;
                    }
                    else { // not enough to fill up rawBuf
                        len = k;
                        for (int i=0; i<len; i++)
                            rawBuf[i] = sotBytes[offset+i];
                    }

                    isNewMsg = true;
                    offset = position;
                }
                else if (position < 0) { // found EOT
                    int k;
                    position = -position;
                    k = position - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped
                            skip -= k;
                            k = 0;
                        }
                    }
                    if (isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                k + len - rawLen));
                        }
                        // reset current byte count of rawBuf
                        len = 0;
                        totalBytes -= rawLen;
                        msgStr = msgBuf.toString();
                        msgBuf = new StringBuffer();
                        if (totalBytes <= 0 && count == 0) {
                            break;
                        }
                        else if ((sid = msgList.getNextID()) < 0) {
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }
                       else if((outMessage=(Message)msgList.browse(sid))==null){
                            msgList.remove(sid);
                            new Event(Event.WARNING, "dropped " + totalBytes +
                                " bytes from " + uri + ": " + msgStr).send();
                            break;
                        }

                        // got a response back for the next pending msg
                        try {
                            if (parse != null) { // parse the response
                                tm = outMessage.getJMSTimestamp();
                                Throwable t = MessageUtils.parse(parse, parser,
                                    msgStr, outMessage);
                                if (t != null) {
                                    new Event(Event.ERR, "failed to parse "+
                                        totalBytes + " bytes from " + uri +
                                        ": " + msgStr).send();
                                    if (t instanceof Exception)
                                        throw((Exception) t);
                                    else
                                        throw((Error) t);
                                }
                                // retrieve the tag to compare with sid
                                line = MessageUtils.getProperty(rcField,
                                    outMessage);
                                if (!(String.valueOf(sid).equals(line))) {
                                    // response is not for sid, put it back
                                    recover(sid, tm, outMessage, msgList);
                                    cid = Integer.parseInt(line);
                                    sid = msgList.getNextID(cid);
                                    // retrieve the right pending message
                                    outMessage = (Message) msgList.browse(sid);
                                    t = MessageUtils.parse(parse, parser,
                                        msgStr, outMessage);
                                    if (t != null) {
                                        if (t instanceof Exception)
                                            throw((Exception) t);
                                        else
                                            throw((Error) t);
                                    }
                                }
                            }
                            else if (outMessage instanceof TextMessage) {
                                ((TextMessage) outMessage).setText(msgStr);
                            }
                            else {
                                ((BytesMessage) outMessage).writeBytes(
                                    msgStr.getBytes(), 0, totalBytes);
                            }
                            if (propertyName != null && propertyValue != null) {
                                for (int i=0; i<propertyName.length; i++)
                                    MessageUtils.setProperty(propertyName[i],
                                        propertyValue[i], outMessage);
                            }
                            if (displayMask > 0)
                                line = MessageUtils.display(outMessage, msgStr,
                                    displayMask, null);

                         outMessage.setJMSTimestamp(System.currentTimeMillis());
                        }
                        catch (JMSException e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Exception e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                Event.traceStack(e)).send();
                            break;
                        }
                        catch (Error e) {
                            msgList.putback(sid);
                            new Event(Event.WARNING, "failed to load msg with "+
                                "bytes received from " + uri + ": "+
                                e.toString()).send();
                            Event.flush(e);
                        }
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        msgList.remove(sid);
                        xq.remove(sid);
                        if (displayMask > 0)
                            new Event(Event.INFO, "requested " + totalBytes +
                                " bytes from " + uri + " (" +line+ " )").send();
                        offset = position;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 1;
                    }  
                    else { // garbage at beginning or msg too large
                        if (totalBytes > 0)
                            new Event(Event.INFO, "discarded " +
                                totalBytes + " bytes out of " + uri).send();
                        msgBuf = new StringBuffer();
                        offset = position;
                        leftover = bytesRead - offset;
                        sBuf.setLeftover(leftover);
                        for (int i=0; i<leftover; i++)
                            buffer[i] = buffer[offset+i];
                        return 0;
                    }
                }
                else if (isNewMsg) { // read k bytes with no SOT nor EOT
                    int k = bytesRead - offset;
                    totalBytes += k;
                    if (skip > 0) { // still have bytes to skip at beginning
                        if (k > skip) { // some bytes left after skip
                            k -= skip;
                            offset += skip;
                            totalBytes -= offhead;
                            skip = 0;
                        }
                        else { // all read bytes skipped and done
                            skip -= k;
                            k = 0;
                            offset = bytesRead;
                            continue;
                        }
                    }
                    if (totalBytes - rawLen <= maxMsgLength) {
                        if (k > rawLen) { // read more bytes than rawBuf
                            // move existing bytes of rawBuf to msg buffer 
                            if (len > 0)
                                msgBuf.append(new String(rawBuf,0,len));

                            // fill in the first k-rawLen bytes to msg buf
                            msgBuf.append(new String(buffer, offset,
                                k - rawLen));

                            // pre-store the last rawLen bytes to rawBuf
                            for (int i=1; i<=rawLen; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else if (k + len > rawLen) { // not many but enough
                            // move the extra bytes of rawBuf to msg buffer 
                            msgBuf.append(new String(rawBuf, 0,
                                k + len - rawLen));

                            // shift rawLen-k bytes to beginning of rawBuf
                            for (int i=0; i<rawLen-k; i++)
                                rawBuf[i] = rawBuf[k+len-rawLen+i];

                            // pre-store all k bytes to the end of rawBuf
                            for (int i=1; i<=k; i++)
                                rawBuf[rawLen-i] = buffer[bytesRead-i];
                            len = rawLen;
                        }
                        else { // not enough to fill up rawBuf
                            for (int i=0; i<k; i++)
                                rawBuf[len+i] = buffer[offset+i];
                            len += k;
                        }
                    }
                    offset = bytesRead;
                }
                else {
                    offset = bytesRead;
                }
            } while (offset < bytesRead &&
                (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
        }
        return 0;
    }

    /**
     * It listens on an XQueue for incoming JMS messages and writes all
     * bytes to an OutputStream as the requests.  Meanwhile, it reads the
     * byte stream from the InputStream associated with the OutputStream,
     * loads them into the request messages and removes them from the XQueue
     * as the responses.  In order to track the incoming responses, it will
     * add the SID to the rcField of each outgoing message.  Whoever receives
     * the request should ensure the response message also contains the same
     * SID.  Hence this method will be able to retrieve the SID and
     * to associate the response with the request.  It also supports flexible
     * but static delimiters at either or both ends.  It also has the blind
     * trim support at both ends.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like read,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * Since the request operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the request is successful.  This method will not
     * acknowledge messages.  It is up to the requester to do that.
     *<br><br>
     * To support multi-plexing, it depends on msgList and sBuf to track the
     * pending requests and the state info. It does not support re-entrance.
     */
    public long request(InputStream in, XQueue xq, OutputStream out,
        QList msgList, DelimitedBuffer sBuf) throws IOException, JMSException,
        TimeoutException {
        Message inMessage;
        java.lang.reflect.Method parse = null;
        Object parser = null;
        long currentTime, idleTime, count = 0;
        int sid = -1, m = 0, n, mask;
        String msgStr = null;
        byte[] buffer = new byte[bufferSize];
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        msgBuf = new StringBuffer();

        if (msgList == null) {
            new Event(Event.ERR, uri + " msg list is null").send();
            return count;
        }
        else if (msgList.getCapacity() != xq.getCapacity()) {
            new Event(Event.ERR, uri + " msg list has wrong lenth: " +
                msgList.getCapacity() +"/"+ xq.getCapacity()).send();
            return count;
        }
        if (sBuf == null) {
            new Event(Event.ERR, uri + " dynamic buffer is null").send();
            return count;
        }
        else if (sBuf.getBoundary() != boundary) {
            new Event(Event.ERR, uri + " dynamic buffer has wrong params: " +
                sBuf.getBoundary() +"/"+ boundary).send();
            return count;
        }

        if (msgList.size() > 0) { // re-entry due to exception
            sBuf.reset();
            while ((sid = msgList.getNextID()) >= 0) { // clean up pending reqs
                msgList.remove(sid);
                inMessage = (Message) xq.browse(sid);
                if (inMessage != null && ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
            }
        }

        if (parserProps != null) { // parser defined
            Object[] o = MessageUtils.getPlugins(parserProps, "ParserArgument",
                "parse", new String[] {"java.lang.String"}, null, xq.getName());
            parse = (java.lang.reflect.Method) o[0];
            parser = o[1];
        }

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                count += receive(in, xq, msgList, sBuf, parser, parse);
                expire(msgList, xq);
                break;
            }

            if ((sid = xq.getNextCell(waitTime)) < 0) { // no incoming request
                count += receive(in, xq, msgList, sBuf, parser, parse);
                if (m++ > 5)
                    m = expire(msgList, xq);
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10 && msgList.size() == 0) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                count += receive(in, xq, msgList, sBuf, parser, parse);
                if (m++ > 5)
                    m = expire(msgList, xq);
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, "-1", inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, "-1", inMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        inMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                        "failed to set RC on msg from "+xq.getName()).send();
                    count += receive(in, xq, msgList, sBuf, parser, parse);
                    if (m++ > 5)
                        m = expire(msgList, xq);
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                count += receive(in, xq, msgList, sBuf, parser, parse);
                if (m++ > 5)
                    m = expire(msgList, xq);
                continue;
            }

            msgStr = null;
            try {
                if (parse != null) // to tag the outgoing request
                    MessageUtils.setProperty(rcField, String.valueOf(sid),
                        inMessage);

                if (template != null)
                    msgStr = MessageUtils.format(inMessage, buffer, template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null || msgStr.length() <= 0) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "empty request for " + uri).send();
                count += receive(in, xq, msgList, sBuf, parser, parse);
                if (m++ > 5)
                    m = expire(msgList, xq);
                continue;
            }
            else try { // try to write request out
                out.write(msgStr.getBytes());
                out.flush();
            }
            catch (IOException e) {
                xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                throw(new RuntimeException(e.toString()));
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            msgList.reserve(sid);
            msgList.add(inMessage, sid);
            try { // clear message for response
                inMessage.clearProperties();
                inMessage.clearBody();
                MessageUtils.setProperty(rcField, "0", inMessage);
                inMessage.setJMSTimestamp(System.currentTimeMillis());
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "request to " + uri).send();
            }

            count += receive(in, xq, msgList, sBuf, parser, parse);
            if (m++ > 5)
                m = expire(msgList, xq);
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO,"requested " +count+ " msgs to " + uri).send();
        return count;
    }

    /**
     * It listens on an XQueue for an incoming JMS messages and writes all
     * bytes to an OutputStream as the request.  After the write operation,
     * it reads the byte stream from the InputStream associated with the
     * OutputStream until the pattern is hit.  Then it loads all the bytes
     * into the request message and removes it from the XQueue as the
     * response.  Both write and read operations block the message flow.
     * Therefore, there is no need to track SID for each messages.
     * It supports flexible terminate patterns with timeout.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like read,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * Since the request operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the request is successful.  This method will not
     * acknowledge messages.  It is up to the requester to do that.
     *<br><br>
     * It is NOT MT-safe.
     */
    public long request(InputStream in, XQueue xq, OutputStream out)
        throws IOException, TimeoutException, JMSException {
        Message inMessage;
        java.lang.reflect.Method parse = null;
        Object parser = null;
        long currentTime, idleTime, count = 0;
        int sid = -1, k, n, mask;
        String msgStr = null;
        byte[] buffer = new byte[bufferSize];
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        if (parserProps != null) { // parser defined
            Object[] o = MessageUtils.getPlugins(parserProps, "ParserArgument",
                "parse", new String[] {"java.lang.String"}, null, xq.getName());
            parse = (java.lang.reflect.Method) o[0];
            parser = o[1];
        }

        msgBuf = new StringBuffer();

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;

            if ((sid = xq.getNextCell(waitTime)) < 0) { // no incoming request
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, "-1", inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, "-1", inMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        inMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    inMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                inMessage = null;
                continue;
            }

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(inMessage, buffer, template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null || msgStr.length() <= 0) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "empty request for " + uri).send();
                inMessage = null;
                continue;
            }
            else try { // try to write out the request
                out.write(msgStr.getBytes());
                out.flush();
            }
            catch (IOException e) {
                xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                throw(new RuntimeException(e.toString()));
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            k = receive(receiveTime, in, msgBuf, buffer);

            if (k <= 0) {
                xq.putback(sid);
                new Event(Event.ERR,"failed to get response from "+ uri).send();
                continue;
            }
            else try { // clear message for response
                if (parse != null) {
                    Throwable t = MessageUtils.parse(parse, parser,
                        msgBuf.toString(), inMessage);
                    if (t != null) {
                        if (t instanceof Exception)
                            throw((Exception) t);
                        else
                            throw((Error) t);
                    }
                }
                else if (inMessage instanceof TextMessage) {
                    inMessage.clearBody();
                    ((TextMessage) inMessage).setText(msgBuf.toString());
                }
                else if (inMessage instanceof BytesMessage) {
                    inMessage.clearBody();
                    ((BytesMessage) inMessage).writeBytes(
                        msgBuf.toString().getBytes());
                }
                else {
                    if (ack) try { // try to ack the msg
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "msg type not supported for "+
                        uri).send();
                    inMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, "0", inMessage);
                inMessage.setJMSTimestamp(System.currentTimeMillis());
                count ++;
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to set content after " +
                    "request to " + uri + ": " + str +
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to set content after " +
                    "request to " + uri + ": " +
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to parse response after "+
                    "receiving from " + uri + ": " +
                    Event.traceStack(e)).send();
                Event.flush(e);
            }

            if (ack) try { // try to ack the msg
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after receive from " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after receive from " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after receive from " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) { // display the response message
                new Event(Event.INFO, "sent a request of " + msgStr.length() +
                    " bytes to " + uri + " and got the response: " +
                    msgBuf.toString()).send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO,"requested " +count+ " msgs to " + uri).send();
        return count;
    }

    /**
     * It reads everything from the InputStream until either EOF, or pattern
     * hit or timeout.  Once the pattern is hit, it packs all bytes into the
     * response buffer.  It returns total number of bytes received upon success,
     * or 0 for timeout.  Otherwise, it returns -1 to indicate failure. 
     */
    private int receive(int timeout, InputStream in, StringBuffer strBuf,
        byte[] buffer) {
        long timeStart;
        int i, k, n, totalBytes, bytesRead, bufferSize, timeLeft;
        boolean hasNew = false;

        if (buffer == null || buffer.length <= 0 || strBuf == null || in ==null)
            return -1;
        bufferSize = buffer.length;

        strBuf.setLength(0);
        timeStart = System.currentTimeMillis();
        timeLeft = timeout;
        totalBytes = 0;
        k = 0;
        try {
            do { // read everything until EOF or pattern hit or timeout
                bytesRead = 0;
                hasNew = false;
                i = -1;
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
                if (hasNew && pm.contains(strBuf.substring(k), pattern))
                    break;
                if (i >= 0)
                    k = totalBytes;
                else try {
                    Thread.sleep(5);
                }
                catch (Exception ex) {
                }
                timeLeft = timeout-(int)(System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
        }
        catch (IOException e) {
            return -1;
        }
        if (timeLeft <= 0)
            totalBytes = 0;
        return totalBytes;
    }

    /**
     * It gets JMS Messages from an XQueue and write all bytes to the
     * OutputStream continuously.
     *<br><br>
     * Most of the IOExceptions from the stream IO operations, like write,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     * It also throws JMSException from the operations regarding to the message
     * itself, like get/set JMS properties.  This does not require to reset
     * the stream at all.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br><br>
     * It will not modify the original messages.  If the XQueue has enabled
     * the EXTERNAL_XA, it will also acknowledge every messages.  This method
     * is MT-Safe.
     */
    public long write(XQueue xq, OutputStream out) throws IOException,
        TimeoutException, JMSException {
        Message inMessage;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        long currentTime, idleTime, count = 0, stm = 10;
        int n, sid = -1, mask;
        int dmask = MessageUtils.SHOW_DATE;
        String dt = null, msgStr = null;
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idle too long"));
                    }
                }
                continue;
            }
            n = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

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
                new Event(Event.WARNING,"unknown msg type for "+uri).send();
                if (ack) try {
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
            if (msgStr.length() > 0) {
                try {
                    if (sotBytes.length > 0)
                        out.write(sotBytes);
                    out.write(msgStr.getBytes());
                    if (eotBytes.length > 0)
                        out.write(eotBytes);
                    out.flush();
                }
                catch (IOException e) {
                    xq.putback(sid);
                    throw(e);
                }
                catch (Exception e) {
                    xq.putback(sid);
                    throw(new RuntimeException(e.toString()));
                }
                catch (Error e) {
                    xq.putback(sid);
                    Event.flush(e);
                }
            }

            if (ack) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after write to "+
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after write to "+
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after write to "+
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt= Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "wrote " + msgStr.length() +
                        " bytes to " + uri + " with a msg ( Date: " + dt +
                        line + " )").send();
                else // no date
                    new Event(Event.INFO, "wrote " + msgStr.length() +
                        " bytes to " + uri + " with a msg (" +
                        line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "wrote " + msgStr.length() +
                    " bytes to " + uri + " with a msg").send();
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
            new Event(Event.INFO, "wrote " + count + " msgs to " + uri).send();
        return count;
    }

    /** collects the response for a request for testing purpose */
    public void collect(InputStream in, XQueue xq, int timeout)
        throws JMSException {
        int i, m, count = 0, bytesRead, size = 0, sid = -1;
        StringBuffer strBuf = new StringBuffer();
        byte[] buffer = new byte[bufferSize];
        int shift =  partition[0];
        int len = partition[1];

        try { // read the content of the request
            while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                if (bytesRead > 0)
                    strBuf.append(new String(buffer, 0, bytesRead));
                size += bytesRead;
            }
        }
        catch (IOException e) {
            new Event(Event.ERR, "failed to read bytes from input stream: " +
                e.toString()).send();
            return;
        }

        TextMessage outMessage = new TextEvent(strBuf.toString());
        if (propertyName != null && propertyValue != null) { // set properties
            for (i=0; i<propertyName.length; i++)
                MessageUtils.setProperty(propertyName[i],
                    propertyValue[i], outMessage);
        }

        switch (len) {
          case 0:
            for (i=0; i<100; i++) { // reserve an empty cell
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
            }
            break;
          case 1:
            for (i=0; i<100; i++) { // reserve the empty cell
                sid = xq.reserve(waitTime, shift);
                if (sid >= 0)
                    break;
            }
            break;
          default:
            for (i=0; i<100; i++) { // reserve a partitioned empty cell
                sid = xq.reserve(waitTime, shift, len);
                if (sid >= 0)
                    break;
            }
            break;
        }
        if (sid < 0) {
            new Event(Event.ERR, "failed to reserve a cell").send();
            return;
        }

        i = xq.add(outMessage, sid);
        if (timeout > 0)
            m = timeout / 500 + 1;
        else
            m = 100;

        for (i=0; i<m; i++) {
            if (xq.collect(500L, sid) < 0)
                continue;
            String msgStr = outMessage.getText();
            if (displayMask > 0) { // display the message
                new Event(Event.INFO, "collected " + msgStr.length() +
                    " bytes with a response (" + MessageUtils.display(
                    outMessage, msgStr, displayMask, null) + " )").send();
            }
            else
                new Event(Event.INFO, "collected " + msgStr.length() +
                    " bytes with a response").send();
            return;
        }
        new Event(Event.ERR, "request timed out at " + timeout).send();
    }

    /** echoes the message back for testing purpose */
    public void echo(XQueue xq) throws JMSException {
        Message inMessage;
        int sid = -1, mask;
        long count = 0;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;
            else {
                inMessage = (Message) xq.browse(sid);
                if (inMessage == null) { // msg is not supposed to be null
                    xq.remove(sid);
                    new Event(Event.WARNING, "dropped a null msg from " +
                        xq.getName()).send();
                    continue;
                }
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (JMSException e) {
                    String str = "";
                    Exception ee = e.getLinkedException();
                    if (ee != null)
                        str += "Linked exception: " + ee.getMessage()+ "\n";
                    new Event(Event.ERR, "failed to ack msg at echo on "+
                        uri + ": " + str + Event.traceStack(e)).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to ack msg at echo on " +
                        uri + ": " + Event.traceStack(e)).send();
                }
                catch (Error e) {
                    xq.remove(sid);
                    new Event(Event.ERR,"failed to ack msg at echo on  "+
                        uri + ": " + e.toString()).send();
                    Event.flush(e);
                }

                xq.remove(sid);
                count ++;
                inMessage = null;
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "echoed " + count + " msgs to " + uri).send();
    }

    public String getOperation() {
        return operation;
    }

    public byte[] getSotBytes() {
        int n = sotBytes.length;
        byte[] s = new byte[n];
        for (int i=0; i<n; i++)
            s[i] = sotBytes[i];
        return s;
    }

    public byte[] getEotBytes() {
        int n = eotBytes.length;
        byte[] e = new byte[n];
        for (int i=0; i<n; i++)
            e[i] = eotBytes[i];
        return e;
    }

    public int getOfftail() {
        return offtail;
    }

    public int getOffhead() {
        return offhead;
    }
}
