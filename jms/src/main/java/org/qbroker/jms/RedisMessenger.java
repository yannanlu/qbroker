package org.qbroker.jms;

/* RedisMessenger.java - a Redis DataStore messenger for JMS messages */

import java.io.File;
import java.io.StringReader;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.Map;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import org.qbroker.common.Connector;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.Utils;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.TimeoutException;
import org.qbroker.net.JedisConnector;
import org.qbroker.net.RedisConnector;
import org.qbroker.jms.MessageOutputStream;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * RedisMessenger connects to a Redis data store specified by URI and
 * initializes the operations of get and put, etc with JMS Messages on
 * various redis operations.
 *<br/><br/>
 * The following methods, get(), put(), sub(), pub(), query() and update() are
 * supported for the Redis operations. The method of get() gets all the items
 * from a Redis list with the given name. The method of put() puts content of
 * a message to a Redis list. The method of sub() subscribes to a Redis channel 
 * with the given name and gets the published messages. The method of pub()
 * publishes the content of a message to a Redis channel. The method of query()
 * is extracts the Redis command from the message and executes it to qeury data.
 * The method of update() extracts the Redis command from the message and
 * executes the command to update the Redis dataset.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RedisMessenger implements Connector {
    private String uri;
    private JedisConnector jedis = null;
    private RedisConnector redis = null;
    private int timeout;
    private Template template;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int capacity, maxRetry, sleepTime = 0;
    private int resultType = Utils.RESULT_JSON;
    private int xaMode = 0;
    private int textMode = 1;
    private int mode = 0;
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private SimpleDateFormat dateFormat = null;
    private boolean isConnected = false;

    private int[] partition;
    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private String queryField, rcField, resultField, keyField;
    private String operation = "query", qName = null;

    public RedisMessenger(Map props) {
        Object o;
        
        if ((o = props.get("URI")) != null && o instanceof String)
            uri = (String) o;
        else
            throw(new IllegalArgumentException("URI is not defined"));

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("ResultField")) != null && o instanceof String)
            resultField = (String) o;
        else
            resultField = "QueryResult";

        if ((o = props.get("QueryField")) != null && o instanceof String)
            queryField = (String) o;
        else
            queryField = "Query";

        if ((o = props.get("KeyField")) != null && o instanceof String)
            keyField = (String) o;
        else
            keyField = "REDIS";

        if ((o = props.get("DefaultKeyName")) != null && o instanceof String)
            qName = (String) o;

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

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

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

        if (qName == null && ("get".equals(operation) ||
            "put".equals(operation) || "pub".equals(operation) ||
            "sub".equals(operation)))
            throw(new IllegalArgumentException("DefaulKeyName is required"));

        if ("sub".equals(operation)) {
            redis = new RedisConnector(props);
            redis.subscribe(qName);
        }
        else
            jedis = new JedisConnector(props);
        isConnected = true;
    }

    /**
     * It gets items from a Redis List specified by qName and puts
     * them to the XQ.
     */
    public void get(XQueue xq) throws IOException {
        Message inMessage;
        String msgStr;
        long count = 0;
        int k, n, sid = -1, cid, mask = 0;
        boolean isText = (textMode > 0);
        boolean isSleepy = (sleepTime > 0);
        int shift = partition[0];
        int len = partition[1];

        if (qName == null)
            throw(new IOException("qName is null for " + uri));

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            switch (len) {
              case 0:
                sid = xq.reserve(waitTime);
                break;
              case 1:
                sid = xq.reserve(waitTime, shift);
                break;
              default:
                sid = xq.reserve(waitTime, shift, len);
                break;
            }
            if (sid < 0)
                continue;

            msgStr = null;
            try {
                msgStr = jedis.redisBlpop(1, qName);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new IOException(e.toString()));
            }
            catch (Error e) {
                xq.cancel(sid);
                Event.flush(e);
            }

            if (msgStr == null || msgStr.length() <= 0) { // noting is available
                xq.cancel(sid);
                continue;
            }
            else { // got something
                if (isText)
                    inMessage = new TextEvent(msgStr);
                else try {
                    inMessage = new BytesEvent();
                    ((BytesMessage) inMessage).writeBytes(msgStr.getBytes());
                }
                catch (Exception e) {
                    inMessage = null;
                }

                if (propertyName != null && propertyValue != null) {
                    int i=0, ic = 0;
                    try {
                        MessageUtils.resetProperties(inMessage);
                        for (i=0; i<propertyName.length; i++) {
                            ic = MessageUtils.setProperty(propertyName[i],
                                propertyValue[i], inMessage);
                            if (ic != 0)
                                break;
                        }
                        if (ic != 0)
                            new Event(Event.WARNING, "failed to set property "+
                                "of: "+propertyName[i]+ " for " + qName).send();
                    }
                    catch (JMSException e) {
                        new Event(Event.WARNING, "failed to set property of: "+
                            propertyName[i] + " for " + qName + ": " +
                            Event.traceStack(e)).send();
                    }
                }

                cid = xq.add(inMessage, sid);
                if (cid > 0) {
                    if (displayMask > 0) try { // display the message
                        String line = MessageUtils.display(inMessage, msgStr,
                            displayMask, propertyName);
                        new Event(Event.INFO, "got a msg from " + qName +
                            " with (" +  line + " )").send();
                    }
                    catch(Exception e) {
                    }
                    count ++;
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + qName).send();
                }

                if (maxNumberMsg > 0 && count >= maxNumberMsg)
                    break;

                if (isSleepy) { // slow down a while
                    long tm = System.currentTimeMillis() + sleepTime;
                    do {
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) > 0) // temporarily disabled
                            break;
                        else try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (tm > System.currentTimeMillis());
                }
            }
        }

        if (displayMask != 0)
            new Event(Event.INFO, "got " +count+ " msgs from " + qName).send();
    }

    /**
     * It subscribes to a channel specified by qName and puts messages
     * to the XQ.
     */
    public void sub(XQueue xq) throws IOException {
        Message inMessage;
        String msgStr, str;
        BytesBuffer msgBuf = new BytesBuffer();
        OutputStream out = null;
        long count = 0;
        int k, n, sid = -1, cid, mask = 0;
        boolean isText = (textMode > 0);
        boolean isSleepy = (sleepTime > 0);
        int shift = partition[0];
        int len = partition[1];

        if (qName == null)
            throw(new IOException("qName is null for " + uri));

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            switch (len) {
              case 0:
                sid = xq.reserve(waitTime);
                break;
              case 1:
                sid = xq.reserve(waitTime, shift);
                break;
              default:
                sid = xq.reserve(waitTime, shift, len);
                break;
            }
            if (sid < 0)
                continue;

            msgStr = null;
            try {
                msgStr = redis.nextMessage(1, msgBuf);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new IOException(e.toString()));
            }
            catch (Error e) {
                xq.cancel(sid);
                Event.flush(e);
            }

            if (msgStr == null) { // noting is available
                xq.cancel(sid);
                continue;
            }
            else { // got something
                if (isText)
                    inMessage = new TextEvent(msgBuf.toString());
                else try {
                    inMessage = new BytesEvent();
                    out = new MessageOutputStream((BytesMessage) inMessage);
                    msgBuf.writeTo(out);
                }
                catch (Exception e) {
                    inMessage = null;
                }
                msgBuf.reset();

                if (propertyName != null && propertyValue != null) {
                    int i=0, ic = 0;
                    try {
                        MessageUtils.resetProperties(inMessage);
                        for (i=0; i<propertyName.length; i++) {
                            ic = MessageUtils.setProperty(propertyName[i],
                                propertyValue[i], inMessage);
                            if (ic != 0)
                                break;
                        }
                        if (ic != 0)
                            new Event(Event.WARNING, "failed to set property "+
                                "of: "+propertyName[i]+ " for " + qName).send();
                    }
                    catch (JMSException e) {
                        new Event(Event.WARNING, "failed to set property of: "+
                            propertyName[i] + " for " + qName + ": " +
                            Event.traceStack(e)).send();
                    }
                }

                cid = xq.add(inMessage, sid);
                if (cid > 0) {
                    if (displayMask > 0) try { // display the message
                        String line = MessageUtils.display(inMessage, msgStr,
                            displayMask, propertyName);
                        new Event(Event.INFO, "got a msg from " + qName +
                            " with (" +  line + " )").send();
                    }
                    catch(Exception e) {
                    }
                    count ++;
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + qName).send();
                }

                if (maxNumberMsg > 0 && count >= maxNumberMsg)
                    break;

                if (isSleepy) { // slow down a while
                    long tm = System.currentTimeMillis() + sleepTime;
                    do {
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) > 0) // temporarily disabled
                            break;
                        else try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (tm > System.currentTimeMillis());
                }
            }
        }

        if (displayMask != 0)
            new Event(Event.INFO, "got " +count+ " msgs from " + qName).send();
    }

    public void put(XQueue xq) throws TimeoutException, IOException {
        Message outMessage;
        String msgStr, keyName;
        boolean xa = ((xaMode & MessageUtils.XA_COMMIT) > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, l, count = 0;
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
            keyName = null;
            msgStr = null;
            try {
                keyName = MessageUtils.getProperty(keyField, outMessage);
                msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (keyName == null || keyName.length() <= 0)
                keyName = qName;

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

            try {
                l = jedis.redisRpush(keyName, msgStr);
            }
            catch (Exception e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to put an item to " + keyName +
                    ": " + e.toString()).send();
                outMessage = null;
                continue;
            }
            if (l < 0) { // push failed
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to put an item to " +keyName+
                    " for " + uri));
            }

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, xq.getName() + " put a msg to "+ keyName +
                     " with ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " put a msg to " +keyName+
                    " with " + l).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after putting on " +
                    keyName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after putting on " +
                    keyName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after putting on " +
                    keyName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
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
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "put " + count + " items").send();
    }

    public void pub(XQueue xq) throws TimeoutException, IOException {
        Message outMessage;
        String msgStr, keyName;
        boolean xa = ((xaMode & MessageUtils.XA_COMMIT) > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, l, count = 0;
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
            keyName = null;
            msgStr = null;
            try {
                keyName = MessageUtils.getProperty(keyField, outMessage);
                msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (keyName == null || keyName.length() <= 0)
                keyName = qName;

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

            try {
                l = jedis.redisPublish(keyName, msgStr);
            }
            catch (Exception e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to publish an item to " + keyName +
                    ": " + e.toString()).send();
                outMessage = null;
                continue;
            }
            if (l < 0) { // push failed
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to publish an item to " +keyName+
                    " for " + uri));
            }

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, xq.getName() + " published a msg to "+
                     keyName + " with ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " publish a msg to " +
                    keyName+ " with " + l).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after publishing on " +
                    keyName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after publishing on " +
                    keyName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after publishing on " +
                    keyName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
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
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "published " + count + " items").send();
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * Redis query command from each message. It executes the Redis command to
     * query the dataset from the Redis server. Based on the result type, the
     * query result will be formatted into the customized content. The formatted
     * content will be put into the message body before the message is removed
     * from the XQueue.  The requester will be able to get the result content
     * out of the message.
     *<br/><br/>
     * Since the query operation relies on the request, this method will
     * intercept the request and processes it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the query is successful.  Otherwise, the
     * message body will not contain the content out of query operation.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void query(XQueue xq) throws IOException, TimeoutException {
        Message outMessage;
        Object o;
        long currentTime, tm, idleTime;
        long count = 0;
        int retry = 0, rc = 0, heartbeat = 600000, ttl = 7200000;
        int i, k, sid = -1, cid, rid, n, mask;
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String[] cmd = null;
        String dt = null, str = null, msgStr = null, keyName = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
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
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }

            tm = 0L;
            msgStr = null;
            try {
                tm = outMessage.getJMSExpiration();
                msgStr = MessageUtils.getProperty(queryField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }
            if (msgStr == null || msgStr.length() <= 0) {
                new Event(Event.ERR, uri +
                    ": failed to get Redis command from msg").send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }

            cmd = jedis.parse(msgStr);
            if (cmd == null || cmd.length <= 1) {
                keyName = null;
            }
            else
                keyName = cmd[1];

            if (cmd.length > 1 && "keys".equalsIgnoreCase(cmd[0])) try {
                StringBuffer strBuf;
                String[] keys = jedis.list(cmd[1]);
                keyName = cmd[1];
                rc = keys.length;
                strBuf = getResult(resultType, keys);
                if (outMessage instanceof TextMessage) {
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    try {
                        MessageUtils.setProperty(rcField, msgRC, outMessage);
                    }
                    catch (Exception ex) {
                    }
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    outMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    outMessage);
                if (displayMask > 0) {
                    String line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
                    new Event(Event.INFO, xq.getName() + " queried "+ rc +
                        " keys and set them into a msg ("+ line + " )").send();
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to list keys for " + uri +
                    ": " + Event.traceStack(e)).send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to list keys for " + uri));
            }

            o = null;
            try {
                o = jedis.query(cmd);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to run " + msgStr + " for " + uri +
                    ": " + Event.traceStack(e)).send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to run " +msgStr+ " for " + uri));
            }

            if (o == null) {
            }
            else try { // got data
                StringBuffer strBuf;
                if (outMessage instanceof TextMessage) {
                    if (o instanceof Collection) {
                        rc = ((Collection) o).size();
                        strBuf = getResult(resultType, (Collection) o);
                    }
                    else if (o instanceof Map) {
                        rc = ((Map) o).size();
                        strBuf = getResult(resultType, (Map) o);
                    }
                    else {
                        rc = 0;
                        strBuf = new StringBuffer();
                    }
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to load msg with results " +
                    "queried on " + keyName +": "+Event.traceStack(e)).send();
                try {
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                }
                catch (Exception ex) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }

            try {
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    outMessage);
                if (displayMask > 0) {
                    String line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
                    new Event(Event.INFO, xq.getName() + " queried "+ rc +
                        " items on "+ keyName + " and set them into a msg ("+
                        line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " queried " + rc +
                    " items on "+ keyName +" and set them into a msg").send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
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
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries").send();
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * Redis update command from each message.  After the compiling and
     * setting data on the command, it executes the Redis command to update
     * the dataset on the Redis server.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br/><br/>
     * The original message will not be modified.  If the XQueue has enabled
     * the EXTERNAL_XA bit, it will also acknowledge the messages also.
     */
    public void update(XQueue xq) throws IOException, TimeoutException {
        Message outMessage;
        Object o;
        long currentTime, tm, idleTime;
        long count = 0;
        int retry = 0, heartbeat = 600000, ttl = 7200000;
        int i, k, sid = -1, cid, rid, n, mask;
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        String[] cmd = null;
        String dt = null, str = null, msgStr = null, keyName = null;
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

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            tm = 0L;
            msgStr = null;
            try {
                tm = outMessage.getJMSExpiration();
                msgStr = MessageUtils.getProperty(queryField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                }
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }
            if (msgStr == null || msgStr.length() <= 0) {
                new Event(Event.ERR, uri +
                    ": failed to get Redis command from msg").send();
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }

            cmd = jedis.parse(msgStr);
            if (cmd == null || cmd.length <= 1) {
                keyName = null;
            }
            else
                keyName = cmd[1];

            o = null;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                try {
                    o = jedis.update(cmd);
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to run " + msgStr + " for " +
                        uri + ": " + Event.traceStack(e)).send();
                    continue;
                }
                break;
            }
            if (o == null) { // update failed
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to run " +msgStr+ " for " + uri));
            }

            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage, msgStr,
                    displayMask, propertyName);
                new Event(Event.INFO, xq.getName() + " updated "+ keyName +
                     " with " + o.toString() + " from the msg ("+
                     line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " updated " + keyName +
                    " with " + o.toString() + " from the msg").send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after updating on " +
                    keyName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after updating on " +
                    keyName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after updating on " +
                    keyName + ": " + e.toString()).send();
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
            new Event(Event.INFO, "completed " + count + " updates").send();
    }

    public StringBuffer getResult(int type, Map map) {
        if (map == null)
            return null;
        Object[] list = map.keySet().toArray();
        Object o;
        int i, j, k, n;
        String key;
        StringBuffer strBuf = new StringBuffer();
        k = list.length;
        if ((type & Utils.RESULT_JSON) > 0) {
            strBuf.append("[");
            for (i=0; i<k; i++) {
                o = list[i];
                if (o == null)
                    continue;
                key = (String) o;
                if (i > 0)
                    strBuf.append("," + Utils.RS);
                strBuf.append("{\"Key\": ");
                strBuf.append("\"" + Utils.escapeJSON(key) + "\"");
                strBuf.append(", \"Value\": ");
                strBuf.append("\"" + Utils.escapeJSON((String) map.get(key)) +
                    "\"");
                strBuf.append("}");
            }
            strBuf.append("]" + Utils.RS);
        }
        else if ((type & Utils.RESULT_XML) > 0) {
            for (i=0; i<k; i++) {
                o = list[i];
                if (o == null)
                    continue;
                key = (String) o;
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<Key>");
                strBuf.append(Utils.escapeXML(key));
                strBuf.append("</Key>");
                strBuf.append("<Value>");
                strBuf.append(Utils.escapeXML((String) map.get(key)));
                strBuf.append("</Value>");
                strBuf.append("</Record>" + Utils.RS);
            }
        }

        return strBuf;
    }

    public StringBuffer getResult(int type, Collection collection) {
        if (collection == null)
            return null;
        Object[] list = collection.toArray();
        Object o;
        int i, j, k, n;
        double z;
        String key;
        StringBuffer strBuf = new StringBuffer();
        k = list.length;
        if (k == 0)
            return strBuf;
        if (collection instanceof List) { // for list
            if ((type & Utils.RESULT_JSON) > 0) {
                strBuf.append("[");
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = (String) o;
                    if (i > 0)
                        strBuf.append("," + Utils.RS);
                    strBuf.append("{\"Index\": " + i);
                    strBuf.append(", \"Item\": \"" + Utils.escapeJSON(key) +
                        "\"");
                    strBuf.append("}");
                }
                strBuf.append("]" + Utils.RS);
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = (String) o;
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Index>" + i + "</Index>");
                    strBuf.append("<Item>");
                    strBuf.append(Utils.escapeXML(key));
                    strBuf.append("</Item>");
                    strBuf.append("</Record>" + Utils.RS);
                }
            }
        }
        else if (list[0] instanceof Tuple) { // for sorted with scores
            if ((type & Utils.RESULT_JSON) > 0) {
                strBuf.append("[");
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = ((Tuple) o).getElement();
                    z = ((Tuple) o).getScore();
                    if (i > 0)
                        strBuf.append("," + Utils.RS);
                    strBuf.append("{\"Member\": ");
                    strBuf.append("\"" + Utils.escapeJSON(key) + "\"");
                    strBuf.append(", \"Score\": \"" + z + "\"");
                    strBuf.append("}");
                }
                strBuf.append("]" + Utils.RS);
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = ((Tuple) o).getElement();
                    z = ((Tuple) o).getScore();
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Member>");
                    strBuf.append(Utils.escapeXML(key));
                    strBuf.append("</Member>");
                    strBuf.append("<Score>" + z + "</Score>");
                    strBuf.append("</Record>" + Utils.RS);
                }
            }
        }
        else if (list[0] instanceof String) { // for string
            if ((type & Utils.RESULT_JSON) > 0) {
                strBuf.append("[");
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = (String) o;
                    if (i > 0)
                        strBuf.append("," + Utils.RS);
                    strBuf.append("{\"Member\": ");
                    strBuf.append("\"" + Utils.escapeJSON(key) + "\"");
                    strBuf.append("}");
                }
                strBuf.append("]" + Utils.RS);
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                for (i=0; i<k; i++) {
                    o = list[i];
                    if (o == null)
                        continue;
                    key = (String) o;
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Member>");
                    strBuf.append(Utils.escapeXML(key));
                    strBuf.append("</Member>");
                    strBuf.append("</Record>" + Utils.RS);
                }
            }
        }

        return strBuf;
    }

    public StringBuffer getResult(int type, String[] list) {
        if (list == null)
            return null;
        int i, j, k, n;
        String keyName;
        StringBuffer strBuf = new StringBuffer();
        k = list.length;
        if ((type & Utils.RESULT_JSON) > 0) {
            strBuf.append("[");
            for (i=0; i<k; i++) {
                keyName = list[i];
                if (keyName == null || keyName.length() <= 0)
                    continue;
                j = jedis.getType(keyName);
                n = jedis.count(j, keyName);
                if (i > 0)
                    strBuf.append("," + Utils.RS);
                strBuf.append("{\"KeyName\": ");
                strBuf.append("\"" + Utils.escapeJSON(keyName) + "\"");
                strBuf.append(", \"Type\": \"" + JedisConnector.Type[j] + "\"");
                strBuf.append(", \"Count\": " + n);
                strBuf.append("}");
            }
            strBuf.append("]" + Utils.RS);
        }
        else if ((type & Utils.RESULT_XML) > 0) {
            for (i=0; i<k; i++) {
                keyName = list[i];
                if (keyName == null || keyName.length() <= 0)
                    continue;
                j = jedis.getType(keyName);
                n = jedis.count(j, keyName);
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<KeyName>");
                strBuf.append(Utils.escapeXML(keyName));
                strBuf.append("</KeyName>");
                strBuf.append("<Type>" + JedisConnector.Type[j] + "</Type>");
                strBuf.append("<Count>" + n + "</Count>");
                strBuf.append("</Record>" + Utils.RS);
            }
        }

        return strBuf;
    }

    /** reconnects and returns null upon sucess or error msg otherwise */
    public String reconnect() {
        isConnected = false;
        if (jedis != null) {
            String str = jedis.reconnect();
            if (str == null)
                isConnected = true;
            return str;
        }
        else if (redis != null) {
            redis.unsubscribe(qName);
            String str = redis.reconnect();
            if (str == null) {
                isConnected = true;
                redis.subscribe(qName);
            }
            return str;
        }
        else
            return "it is closed";
    }

    public void close() {
        isConnected = false;
        if (jedis != null)
            jedis.close();
        if (redis != null)
            redis.close();
    }

    public String getOperation() {
        return operation;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }
}
