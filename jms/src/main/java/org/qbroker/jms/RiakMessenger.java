package org.qbroker.jms;

/* RiakMessenger.java - a Riak DataStore messenger for JMS messages */

import java.util.Date;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.Utils;
import org.qbroker.common.XML2Map;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.NonSQLException;
import org.qbroker.net.RiakConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * RiakMessenger connects to a Riak database specified by URI and
 * initializes the operations of fetch, store and download with JMS Messages on
 * various data buckets.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RiakMessenger extends RiakConnector {
    private int timeout;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String[] metaPropertyName = null;
    private String[] metaPropertyValue = null;
    private int displayMask = 0;
    private int capacity, maxRetry, sleepTime = 0;
    private int resultType = Utils.RESULT_JSON;
    private int xaMode = 0;
    private int textMode = 1;
    private int mode = 0;
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private SimpleDateFormat dateFormat = null;
    private boolean hasMeta = false;

    private int[] partition;
    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private String rcField, resultField, bucketField, keyField;
    private String operation = "fetch", defaultBucketName = null;
    private String defaultKeyName = null;

    public RiakMessenger(Map props) {
        super(props);
        Object o;

        if ((o = props.get("BucketField")) != null)
            bucketField = (String) o;
        else
            bucketField = "Bucket";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("ResultField")) != null && o instanceof String)
            resultField = (String) o;
        else
            resultField = "QueryResult";

        if ((o = props.get("KeyField")) != null && o instanceof String)
            keyField = (String) o;
        else
            keyField = "RIAK";

        if ((o = props.get("DefaultBucketName")) != null && o instanceof String)
            defaultBucketName = (String) o;

        if ((o = props.get("DefaultKeyName")) != null && o instanceof String)
            defaultKeyName = (String) o;

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;

        if ((o = props.get("Retry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) <= 0)
            maxRetry = 3;

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

        if ((o = props.get("MetaProperty")) != null && o instanceof Map) {
            String key;
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            metaPropertyName = new String[n];
            metaPropertyValue = new String[n];
            for (int i=0; i<n; i++)
                metaPropertyName[i] = null;
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                metaPropertyName[n] = key;
                key = (String) ((Map) o).get(key);
                if (key == null || key.length() <= 0)
                    continue;
                metaPropertyValue[n] = MessageUtils.getPropertyID(key);
                if (metaPropertyValue[n] == null)
                    metaPropertyValue[n] = key;
                n ++;
            }
            if (n > 0)
                hasMeta = true;
        }

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
        if ((o = props.get("ResultType")) != null)
            resultType = Integer.parseInt((String) o);
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

        if ("fetch".equals(operation) && (defaultBucketName == null ||
            defaultKeyName == null))
            throw(new IllegalArgumentException("DefaultBucketName or " +
                "DefaultKeyName is not defined"));
    }

    /**
     * It fetches content with the default key from the Riak service and
     * loads content into a message before sending it to the XQ.
     */
    public void fetch(XQueue xq) throws NonSQLException, JMSException {
        Message inMessage;
        IRiakObject o;
        String msgStr, bName = defaultBucketName, keyName = defaultKeyName;
        long count = 0;
        int sid = -1, cid, mask = 0;
        int shift = partition[0];
        int len = partition[1];

        if (! isConnected) try {
            reconnect();
        }
        catch (Exception e) {
        }
        catch (Error e) {
            xq.cancel(sid);
            Event.flush(e);
        }

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

        if (sid < 0) {
            if ((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0)
                new Event(Event.ERR, "failed to reserve a cell on " +
                    xq.getName() + " for " + uri).send();
            return;
        }

        msgStr = null;
        o = null;
        try {
            o = fetch(bName, keyName);
            if (o != null)
                msgStr = o.getValueAsString();
        }
        catch (RiakException e) {
            xq.cancel(sid);
            throw(new NonSQLException(e, e.toString()));
        }
        catch (Exception e) {
            xq.cancel(sid);
            throw(new RuntimeException(e.toString()));
        }
        catch (Error e) {
            xq.cancel(sid);
            Event.flush(e);
        }

        if (o == null || msgStr == null) { // failed
            xq.cancel(sid);
            new Event(Event.ERR, "failed to fetch content for " +
                bName +"/"+ keyName + " from " + uri).send();
            return;
        }
        else try { // got content
            if (textMode > 0)
                inMessage = new TextEvent(msgStr);
            else {
                inMessage = new BytesEvent();
                ((BytesMessage) inMessage).writeBytes(msgStr.getBytes());
            }

            if (hasMeta && metaPropertyName != null) {
                int i, ic = 0;
                String value;
                for (i=0; i<metaPropertyName.length; i++) {
                    if (metaPropertyName[i] == null)
                        continue;
                    if ("ContentType".equals(metaPropertyName[i]))
                        value = o.getContentType();
                    else if ("LastModified".equals(metaPropertyName[i]))
                        value = Event.dateFormat(o.getLastModified());
                    else {
                        Set s = o.getBinIndex(metaPropertyName[i]);
                        if (s != null && !s.isEmpty())
                            value = (String) s.iterator().next();
                        else
                            value = null;
                    }
                    if (value == null || value.length() <= 0)
                        continue;
                    ic = MessageUtils.setProperty(metaPropertyValue[i],
                        value, inMessage);
                    if (ic != 0)
                        break;
                }
                if (ic != 0)
                    new Event(Event.WARNING, "failed to set property "+
                        "of: " + metaPropertyValue[i] + " for " +
                        bName + "/" + keyName).send();
            }

            if (propertyName != null && propertyValue != null) {
                int i, ic = 0;
                for (i=0; i<propertyName.length; i++) {
                    ic = MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], inMessage);
                    if (ic != 0)
                        break;
                }
                if (ic != 0)
                    new Event(Event.WARNING, "failed to set property "+
                        "of: " + propertyName[i] + " for " +
                        bName + "/" + keyName).send();
            }
        }
        catch (Exception e) {
            xq.cancel(sid);
            new Event(Event.ERR, "failed to load content for " +
                bName +"/"+ keyName +": "+ Event.traceStack(e)).send();
            return;
        }

        cid = xq.add(inMessage, sid);
        if (cid > 0) {
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, "fetched a msg from " + bName +
                    "/" + keyName + " with (" +  line + " )").send();
            }
            catch(Exception e) {
            }
            count ++;
        }
        else {
            xq.cancel(sid);
            new Event(Event.ERR, xq.getName() +
                ": failed to add a msg from " + bName + "/" + keyName).send();
        }
    }

    /**
     * It gets a JMS Message from the XQueue and stores its content into a
     * bucket with the given key.
     *<br/><br/>
     * It always tries to retrieve the bucket name from the field specified
     * by BucketField and the key name from the field specified by the
     * KeyField.  If the message does not contain the bucket or key, the method
     * will use the default values.
     *<br/><br/>
     * If the XQueue has enabled the EXTERNAL_XA bit, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void store(XQueue xq) throws TimeoutException, NonSQLException,
        JMSException {
        Message outMessage;
        Map<String, String> meta = null;
        String msgStr, keyName, bName, str;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, count = 0;
        int mask;
        int sid = -1;
        int n, size = 0;
        int dmask = MessageUtils.SHOW_DATE;
        byte[] buffer = new byte[bufferSize];
        dmask ^= displayMask;
        dmask &= displayMask;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
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
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            currentTime = System.currentTimeMillis();
            bName = null;
            keyName = null;
            msgStr = null;
            try {
                bName = MessageUtils.getProperty(bucketField, outMessage);
                keyName = MessageUtils.getProperty(keyField, outMessage);
                msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (bName == null || bName.length() <= 0)
                bName = defaultBucketName;
            if (keyName == null || keyName.length() <= 0)
                keyName = defaultKeyName;

            if (msgStr == null || msgStr.length() <= 0) {
                new Event(Event.ERR, uri +
                    ": failed to get content from msg").send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }

            if (hasMeta && metaPropertyName != null) try { // meta properties
                meta = new HashMap<String, String>();
                for (int i=0; i<metaPropertyName.length; i++) {
                     if (metaPropertyName[i] == null)
                         continue;
                     str = outMessage.getStringProperty(metaPropertyValue[i]);
                     if (str == null || str.length() <= 0)
                         continue;
                     meta.put(metaPropertyName[i], str);
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to get meta property for " +
                    bName + "/" + keyName + ": " + e.toString()).send();
            }

            str = null;
            try {
                str = store(bName, keyName, msgStr, meta);
            }
            catch (RiakException e) {
                xq.putback(sid);
                new Event(Event.ERR, "failed to store a msg to " + bName +
                    "/" + keyName + ": " + e.toString()).send();
                throw(new NonSQLException(e, "failed to store a msg to " +
                    bName + "/" + keyName + " for " + uri));
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to store a msg to " + bName +
                    "/" + keyName + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }
            catch (Error e) {
                xq.putback(sid);
                new Event(Event.ERR, "failed to store msg to " +
                    bName + "/" + keyName + ": " + e.toString()).send();
                Event.flush(e);
            }

            if (str == null) { // store failed
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to store a msg to " + bName +
                    "/" + keyName).send();
                outMessage = null;
                continue;
            }

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, xq.getName() + " stored a msg to "+bName+
                     "/" + keyName + " with ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " stored a msg to "+bName+
                    "/" + keyName).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after storing on " +
                    bName+"/"+keyName +": "+ str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after storing on " +
                    bName + "/" + keyName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after storing on " +
                    bName + "/" + keyName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "stored " + count + " msgs").send();
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * bucket name and the key name from each message.  Then it downloads the
     * content from the Riak server and loads the content to the message body.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br/><br/>
     * The original message will not be modified.  If the XQueue has enabled
     * the EXTERNAL_XA bit, it will also acknowledge the messages also.
     */
    public void download(XQueue xq) throws TimeoutException, NonSQLException,
        JMSException {
        Message outMessage;
        IRiakObject o;
        long currentTime, tm, idleTime;
        long count = 0;
        int i, k, sid = -1, cid, rid, n, mask;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String dt = null, str = null, msgStr = null, bName=null, keyName = null;
        byte[] buffer = new byte[bufferSize];

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
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
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;
            currentTime = System.currentTimeMillis();

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

            tm = 0L;
            bName = null;
            keyName = null;
            try {
                tm = outMessage.getJMSExpiration();
                bName = MessageUtils.getProperty(bucketField, outMessage);
                keyName = MessageUtils.getProperty(keyField, outMessage);
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (JMSException e) {
            }

            if (bName == null || bName.length() <= 0)
                bName = defaultBucketName;
            if (keyName == null || keyName.length() <= 0)
                keyName = defaultKeyName;

            o = null;
            msgStr = null;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                try {
                    o = fetch(bName, keyName);
                    if (o != null)
                        msgStr = o.getValueAsString();
                }
                catch (RiakException e) {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) {
                        new Event(Event.ERR, "failed to download " + bName +
                            "/" + keyName + " from " + uri + ": " +
                            Event.traceStack(e)).send();
                        xq.putback(sid);
                        return;
                    }
                    if (k <= 0)
                        new Event(Event.ERR, "failed to download " + bName +
                            "/" + keyName + " from " + uri + ": " +
                            Event.traceStack(e)).send();
                    else if (k + 1 >= maxRetry) { // give up
                        xq.putback(sid);
                        throw(new NonSQLException(e, e.toString()));
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    o = null;
                    msgStr = null;
                    continue;
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to download " + bName + "/" +
                        keyName + " from " + uri + ": " +
                        Event.traceStack(e)).send();
                    o = null;
                    msgStr = null;
                }
                break;
            }

            if (o == null) { // key is not available or failed to download
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "empty key at " + bName + "/" +
                    keyName + " from " + uri).send();
                outMessage = null;
                continue;
            }
            else if (msgStr == null || msgStr.length() <= 0) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "empty key at " + bName + "/" +
                    keyName + " from " + uri).send();
                outMessage = null;
                continue;
            }
            else try { // store content into message
                outMessage.clearBody();
                if (outMessage instanceof TextMessage)
                    ((TextMessage) outMessage).setText(msgStr);
                else
                    ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());
                MessageUtils.setProperty(rcField, okRC, outMessage);
                if (hasMeta && metaPropertyName != null) {
                    String value;
                    for (i=0; i<metaPropertyName.length; i++) {
                        if (metaPropertyName[i] == null)
                            continue;
                        if ("ContentType".equals(metaPropertyName[i]))
                            value = o.getContentType();
                        else if ("LastModified".equals(metaPropertyName[i]))
                            value = Event.dateFormat(o.getLastModified());
                        else {
                            Set s = o.getBinIndex(metaPropertyName[i]);
                            if (s != null && !s.isEmpty())
                                value = (String) s.iterator().next();
                            else
                                value = null;
                        }
                        if (value == null || value.length() <= 0)
                            continue;
                        MessageUtils.setProperty(metaPropertyValue[i],
                            value, outMessage);
                    }
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to set message body for "+ bName+
                   "/" + keyName + ": " + Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, xq.getName() + " downloaded a msg from "+
                     bName + "/" + keyName + " with ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " downloaded a msg from "+
                    bName + "/" + keyName).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after downloading on " +
                    bName + "/" + keyName + ": " + str +
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after downloading on " +
                    bName + "/" + keyName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after downloading on " +
                    bName + "/" + keyName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (sleepTime > 0) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "downloaded " + count + " msgs").send();
    }

    public String getOperation() {
        return operation;
    }
}
