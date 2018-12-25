package org.qbroker.jms;

/* RMQMessenger.java - a RabbitMQ messenger for JMS messages */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.net.RMQConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * RMQMessenger connects to a RabbitMQ server and initializes one of the
 * operations, such as get and pub.  It carries out the operation with support
 * of JMS Messages.
 *<br/><br/>
 * There are two methods, get() and put(). The first method, get(), is used to
 * get messages from a queue on the RabbitMQ server.  The method of pub() is
 * to publish messages with certain routing keys to the RabbitMQ server.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RMQMessenger extends RMQConnector {
    private String uri, msgID = null;
    private int bufferSize = 4096;
    private Template template = null;

    private String rcField, dest;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int sleepTime = 0;

    private int textMode = 1;
    private int xaMode = 0;
    private int retry = 3;
    private int overwrite = 0;
    private int persistence = 1;
    private int priority = 0;
    private long expiry = 0;

    private java.lang.reflect.Method ackMethod = null;
    private int[] partition;

    private String[] propertyName = null;
    private String[] propertyValue = null;

    private boolean check_body = false;
    private boolean check_jms = false;
    private boolean isDaemon = false;

    /** Creates new RMQMessenger */
    public RMQMessenger(Map props) throws IOException {
        super(props);
        Object o;

        uri = super.getURI();
        dest = super.getDestination();
        if ((o = props.get("RCField")) != null)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("Retry")) == null ||
            (retry = Integer.parseInt((String) o)) <= 0)
            retry = 3;

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            isDaemon = true;
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
        if ((o = props.get("Template")) != null)
            template = new Template((String) o);
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

        if ((o = props.get("Persistence")) != null) {
            persistence = Integer.parseInt((String) o);
            if (persistence < 1 || persistence > 2)
                persistence = 1;
            overwrite += 1;
        }
        if ((o = props.get("Priority")) != null) {
            priority = Integer.parseInt((String) o);
            if (priority < 0 || priority >= 10)
                priority = 0;
            overwrite += 2;
        }
        if ((o = props.get("Expiry")) != null) {
            expiry = Long.parseLong((String) o);
            overwrite += 4;
        }

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

        if ("get".equals(operation)) {
            if ((xaMode & MessageUtils.XA_CLIENT) > 0) { // for XAMode 1 or 3
                Class<?> cls = this.getClass();
                try {
                    ackMethod = cls.getMethod("acknowledge",
                        new Class[] {long[].class});
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("found no ack method"));
                }
            }
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * retrieves messages from the RabbitMQ server and puts them to the XQ
     */
    public void get(XQueue xq) throws IOException, JMSException {
        Message inMessage;
        QueueingConsumer consumer = null;
        QueueingConsumer.Delivery delivery;
        String line = null;
        byte[] buf = null;
        int  sid = -1, cid, mask = 0;
        long count = 0, stm = 10, deliveryTag;
        boolean isText = (textMode == 1);
        boolean isSleepy = (sleepTime > 0);
        boolean ack = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        int shift = partition[0];
        int len = partition[1];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        consumer = getConsumer(!ack);

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

            try {
                delivery = consumer.nextDelivery(receiveTime);
            }
            catch (InterruptedException e) {
                delivery = null;
            }
            catch (Exception e) {
                xq.cancel(sid);
                new Event(Event.ERR, dest + ": failed to receive msg: " +
                    e.toString()).send();
                throw(new IOException(e.toString()));
            }

            if (delivery == null) {
                xq.cancel(sid);
                if (isDaemon) { // daemon mode
                    continue;
                }
                else {
                    break;
                }
            }
            buf = delivery.getBody();
            if (buf == null)
                buf = new byte[]{};
            deliveryTag = delivery.getEnvelope().getDeliveryTag();

            if (overwrite < 0) {
                if (isText)
                    inMessage = new TextEvent(new String(buf));
                else {
                    inMessage = new BytesEvent();
                    ((BytesMessage) inMessage).writeBytes(buf);
                }
            }
            else try {
                inMessage = convert(delivery, buf, isText);
            }
            catch (JMSException e) {
                xq.cancel(sid);
                new Event(Event.ERR, dest + ": failed to convert msg: " +
                    Event.traceStack(e)).send();
                super.ack(deliveryTag);
                continue;
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
                            "of: "+propertyName[i]+ " for " + dest).send();
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, "failed to set property of: "+
                        propertyName[i] + " for " + dest + ": " +
                        Event.traceStack(e)).send();
                }
            }

            if (displayMask > 0) try {
                line = MessageUtils.display(inMessage, null,
                    displayMask, null);
            }
            catch (Exception e) {
                line = "";
            }

            if (overwrite > 0) {
                switch (overwrite) {
                  case 1:
                    inMessage.setJMSDeliveryMode(persistence);
                    break;
                  case 2:
                    inMessage.setJMSPriority(priority);
                    break;
                  case 3:
                    inMessage.setJMSDeliveryMode(persistence);
                    inMessage.setJMSPriority(priority);
                    break;
                  case 4:
                    inMessage.setJMSExpiration(expiry +
                        inMessage.getJMSTimestamp());
                    break;
                  case 5:
                    inMessage.setJMSDeliveryMode(persistence);
                    inMessage.setJMSExpiration(expiry +
                        inMessage.getJMSTimestamp());
                    break;
                  case 6:
                    inMessage.setJMSPriority(priority);
                    inMessage.setJMSExpiration(expiry +
                        inMessage.getJMSTimestamp());
                    break;
                  case 7:
                    inMessage.setJMSDeliveryMode(persistence);
                    inMessage.setJMSPriority(priority);
                    inMessage.setJMSExpiration(expiry +
                        inMessage.getJMSTimestamp());
                    break;
                  default:
                }
            }
            if (ack) { // client ack
                ((JMSEvent) inMessage).setAckObject(this, ackMethod,
                    new long[] {deliveryTag});
            }
            cid = xq.add(inMessage, sid);

            if (cid > 0) {
                if (displayMask > 0) // display the message
                    new Event(Event.INFO, "got a msg from " + dest +
                        " with (" + line + " )").send();
                count ++;
            }
            else {
                xq.cancel(sid);
                new Event(Event.ERR, xq.getName() +
                    ": failed to add a msg from " + dest).send();
                break;
            }

            if (maxNumberMsg > 0 && count >= maxNumberMsg) break;

            if (isSleepy) { // slow down a while
                long tm = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
            }
        }

        if (displayMask != 0)
            new Event(Event.INFO, "got " + count + " msgs from "+ dest).send();
    }

    /**
     * It gets JMS messages from the XQueue and publishes them with certain
     * routing keys to the RabbitMQ server.
     */
    public void pub(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message message;
        JMSException ex = null;
        AMQP.BasicProperties props = null;
        String msgStr;
        String dt = null, id = null, key;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        boolean hasTemplate = (template != null);
        long currentTime, idleTime, ttl, count = 0, stm = 10;
        byte[] buffer = new byte[4096];
        int mask;
        int sid = -1;
        int n;
        int dmask = MessageUtils.SHOW_DATE;
        dmask ^= displayMask;
        dmask &= displayMask;
        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dest = getDestination();
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
                        n = 0;
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            message = (Message) xq.browse(sid);
            if (message == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            dt = null;
            ttl = -1;
            key = null;
            try {
                if (hasTemplate)
                    key = MessageUtils.format(message, buffer, template);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt = Event.dateFormat(new Date(message.getJMSTimestamp()));
                id = message.getJMSMessageID();
                ttl = message.getJMSExpiration();
            }
            catch (JMSException e) {
                if (ttl < 0)
                    ttl = 0;
                id = null;
                new Event(Event.WARNING, "failed to get JMS property for "+
                    dest + ": " + Event.traceStack(e)).send();
            }

            if (key == null) // use the default key
                key = dest;
            if (ttl > 0) { // recover ttl first
                ttl -= System.currentTimeMillis();
                if (ttl <= 0) // reset ttl to default
                    ttl = 1000L;
            }
            try {
                if (count == 0 && msgID != null) { // for retries
                    if (!msgID.equals(id)) {
                        props = getBasicProperties(message);
                        if (message instanceof BytesMessage) {
                            BytesBuffer bs = new BytesBuffer();
                            ((BytesMessage) message).reset();
                            MessageUtils.copyBytes((BytesMessage) message,
                                buffer, bs);
                            pub(key, props, bs.toByteArray());
                        }
                        else {
                            msgStr = MessageUtils.processBody(message, buffer);
                            pub(key, props, msgStr.getBytes());
                        }
                    }
                    else { // skipping the resend
                        new Event(Event.NOTICE, "msg of " + msgID +
                            " has already been delivered to " + key).send();
                    }
                }
                else {
                    props = getBasicProperties(message);
                    if (message instanceof BytesMessage) {
                        BytesBuffer bs = new BytesBuffer();
                        ((BytesMessage) message).reset();
                        MessageUtils.copyBytes((BytesMessage)message,buffer,bs);
                        pub(key, props, bs.toByteArray());
                    }
                    else {
                        msgStr = MessageUtils.processBody(message, buffer);
                        pub(key, props, msgStr.getBytes());
                    }
                }
            }
            catch (JMSException e) {
                new Event(Event.ERR, "Invalid JMS Destination for "+
                    dest + ": " + Event.traceStack(e)).send();
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
            msgID = id;
            if (ack) {
                try {
                    message.acknowledge();
                }
                catch (JMSException e) {
                    String str = "";
                    Exception ee = e.getLinkedException();
                    if (ee != null)
                        str += "Linked exception: " + ee.getMessage()+ "\n";
                    new Event(Event.ERR, "failed to ack msg after pub to " +
                        key + ": " + str + Event.traceStack(e)).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to ack msg after pub to " +
                        key + ": " + Event.traceStack(e)).send();
                }
                catch (Error e) {
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to ack msg after put to " +
                        key + ": " + e.toString()).send();
                    Event.flush(e);
                }
                xq.remove(sid);
            }
            else
                xq.remove(sid);

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(message, null, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "pub a msg to " + key +
                        " with ( Date: " + dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "pub a msg to " + key +
                        " with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "pub a msg to " + key).send();
            }
            count ++;
            message = null;

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
            new Event(Event.INFO, "pub " + count + " msgs to " + uri).send();
    }

    /**
     * converts a Delivery into a JMSEvent in the specific type and returns
     * the new message where type 0 is for BytesMessage, 1 for TextMessage,
     * 2 for MapMessage and 3 for ObjectMessage.  In case of failure, it
     * may just return null without exception.
     */
    public static Message convert(QueueingConsumer.Delivery msg, byte[] body,
        boolean isText) throws JMSException {
        Object o;
        Map map;
        Message message = null;
        Envelope envelope = null;
        AMQP.BasicProperties props = null;
        String key, value;
        String msgStr = "";

        if (msg == null)
            return null;

        if (isText) { // for TextMessage
            message = new TextEvent();
            if (body == null)
                ((TextMessage) message).setText(new String(msg.getBody()));
            else
                ((TextMessage) message).setText(new String(body));
        }
        else { // for BytesMessage
            message = new BytesEvent();
            if (body == null)
                ((BytesMessage) message).writeBytes(msg.getBody());
            else
                ((BytesMessage) message).writeBytes(body);
        }
        envelope = msg.getEnvelope();
        props = msg.getProperties();

        if ((o = props.getPriority()) != null)
            message.setJMSPriority(((Integer) o).intValue());
        else
            message.setJMSPriority(0);
        if ((o = props.getDeliveryMode()) != null)
            message.setJMSDeliveryMode(((Integer) o).intValue());
        if ((o = props.getTimestamp()) != null)
            message.setJMSTimestamp(((Date) o).getTime());
        if ((o = props.getMessageId()) != null)
            message.setJMSMessageID((String) o);
        if ((o = props.getCorrelationId()) != null)
            message.setJMSCorrelationID((String) o);
        if ((o = props.getType()) != null)
            message.setJMSType((String) o);
        if ((o = props.getReplyTo()) != null)
            message.setJMSReplyTo(new EventDestination((String) o));
        if (envelope != null) {
            message.setJMSRedelivered(envelope.isRedeliver());
            message.setStringProperty("RMQ_DeliveryTag",
                String.valueOf(envelope.getDeliveryTag()));
            key = envelope.getExchange();
            if (key != null && key.length() > 0)
                message.setStringProperty("RMQ_Exchange", key);
            key = envelope.getRoutingKey();
            if (key != null && key.length() > 0)
                message.setStringProperty("RMQ_RoutingKey", key);
        }

        if ((o = props.getContentType()) != null)
            message.setStringProperty("RMQ_ContentType", (String) o);
        if ((o = props.getContentEncoding()) != null)
            message.setStringProperty("RMQ_ContentEncoding", (String) o);
        if ((o = props.getUserId()) != null)
            message.setStringProperty("RMQ_UserId", (String) o);
        if ((o = props.getAppId()) != null)
            message.setStringProperty("RMQ_AppId", (String) o);
        if ((o = props.getClusterId()) != null)
            message.setStringProperty("RMQ_ClusterId", (String) o);
        if ((o = props.getClassName()) != null)
            message.setStringProperty("RMQ_ClassName", (String) o);
        message.setStringProperty("RMQ_ClassId",
            String.valueOf(props.getClassId()));

        map = (Map) props.getHeaders();
        if (map != null && map.size() > 0) {
            Iterator iter = map.keySet().iterator();
            while (iter.hasNext()) {
                o = iter.next();
                if (o == null || !(o instanceof String))
                    continue;
                key = (String) o;
                o = map.get(key);
                if (o == null)
                    continue;
                else if (o instanceof byte[])
                    message.setStringProperty(key, new String((byte[]) o));
                else if (o instanceof String)
                    message.setStringProperty(key, (String) o);
                else
                    message.setObjectProperty(key, o);
            }
        }

        return message;
    }

    public static AMQP.BasicProperties getBasicProperties(Message msg)
        throws JMSException {
        Map<String, Object> map;
        Enumeration propNames;
        String key;
        AMQP.BasicProperties.Builder ph;
        if (msg == null)
            return null;

        ph = new AMQP.BasicProperties.Builder();
        key = msg.getJMSType(); 
        if (key != null && key.length() > 0)
            ph.type(key);
        key = msg.getJMSMessageID(); 
        if (key != null && key.length() > 0)
            ph.messageId(key);
        key = msg.getJMSCorrelationID(); 
        if (key != null && key.length() > 0)
            ph.correlationId(key);
        ph.priority(new Integer(msg.getJMSPriority()));
        ph.deliveryMode(new Integer(msg.getJMSDeliveryMode()));
        key = msg.getStringProperty("RMQ_ContentType"); 
        if (key != null && key.length() > 0)
            ph.contentType(key);
        key = msg.getStringProperty("RMQ_ContentEncoding"); 
        if (key != null && key.length() > 0)
            ph.contentEncoding(key);
        key = msg.getStringProperty("RMQ_UserId"); 
        if (key != null && key.length() > 0)
            ph.userId(key);
        key = msg.getStringProperty("RMQ_AppId"); 
        if (key != null && key.length() > 0)
            ph.appId(key);
        key = msg.getStringProperty("RMQ_ClusterId"); 
        if (key != null && key.length() > 0)
            ph.clusterId(key);

        map = new HashMap<String, Object>();
        propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS") || key.startsWith("RMQ"))
                continue;
            map.put(key, msg.getObjectProperty(key));
        }
        if (map.size() > 0)
            ph.headers(map);

        return ph.build();
    }

    public static boolean isRMQMessage(Message msg) throws JMSException {
        return msg.propertyExists("RMQ_ClassId");
    }

    /** acknowledges the RabbitMQ msgs only */
    public void acknowledge(long[] state) throws JMSException {
        if (state != null && state.length > 0) try {
            super.ack(state[0]);
        }
        catch (Exception e) {
            throw(new JMSException(e.toString()));
        }
    }

    public String getOperation() {
        return operation;
    }
}
