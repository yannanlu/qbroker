package org.qbroker.jms;

/* JMSTConnector.java - an abstract class implements TopicConnector */

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Date;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.InvalidDestinationException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.jms.QueueSender;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.jms.TopicConnector;
import org.qbroker.event.Event;

/**
 * JMSTConnector connects to a JMS topic and initializes one of the operations,
 * such as sub, pub.
 *<br/><br/>
 * For durable subscriptions, you have to specify SubscriptionID.  On the other
 * hand, if SubscriptionID is defined, it is assumed to be durable subscription.
 *<br/><br/>
 * For pub operations, Overwrite mask controls whether to reset the persistence,
 * the priority, and the expiration time on each incoming message. 0 means no
 * overwrite. In this case, the persistence, the priority, and the time-to-live
 * will be recovered from each incoming message for send action. 1 is to reset
 * the persistence to a specific value. 2 is to reset the priority to a
 * certain value. 4 is to reset the expiration time with a given time-to-live.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public abstract class JMSTConnector implements TopicConnector {
    protected TopicConnection tConnection = null;
    protected TopicSession tSession = null, bSession = null;
    protected TopicPublisher tPublisher = null, bPublisher = null;
    protected TopicSubscriber tSubscriber = null, bSubscriber = null;
    protected Topic topic = null;

    protected int bufferSize = 4096;
    protected String connectionFactoryName;
    protected String tName = null;  // either JNDI name or real name
    protected String username = null;
    protected String password = null;

    protected String clientID = null;
    protected String subscriptionID = null;
    protected String msgID = null;
    protected int[] partition;
    protected int displayMask = 0;

    protected long expiry = 0;
    protected int persistence = Message.DEFAULT_DELIVERY_MODE;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected int overwrite = 0;
    protected int mode = 0;

    protected String[] propertyName = null;
    protected String[] propertyValue = null;
    protected String msgSelector = null;

    protected String operation = "pub";
    protected int maxMsgLength = 4194304;
    protected int maxNumberMsg = 0;
    protected int maxIdleTime = 0;
    protected long startTime = -1;
    protected long endTime = -1;
    protected long waitTime = 500L;
    protected int receiveTime = 1000;
    protected int sleepTime = 0;
    protected int textMode = 1;
    protected int xaMode = XA_CLIENT;
    protected int cellID = 0;
    protected int batchSize = 1;
    protected int boundary = 0;
    protected int offhead = 0;
    protected int offtail = 0;
    protected byte[] sotBytes = new byte[0];
    protected byte[] eotBytes = new byte[0];
    protected final static int MS_SOT = DelimitedBuffer.MS_SOT;
    protected final static int MS_EOT = DelimitedBuffer.MS_EOT;

    protected boolean isPhysical = false;
    protected boolean enable_exlsnr = false;
    protected boolean isDurable = false;
    protected boolean check_body = false;
    protected boolean resetOption = false;
    protected boolean withFlowControlDisabled = false;

    /** Initializes the JMS components (TopicConnectionFactory,
     * TopicConnection, TopicPublisher, BytesMessage) for use
     * later in the application
     * @throws JMSException occurs on any JMS Error
     */
    protected abstract TopicConnectionFactory initialize() throws JMSException;

    protected void connect() throws JMSException {
        TopicConnectionFactory factory = initialize();
        if (username != null)
            tConnection = factory.createTopicConnection(username, password);
        else
            tConnection = factory.createTopicConnection();

        if (!operation.equals("pub") && clientID != null)
            tConnection.setClientID(clientID);
        if (enable_exlsnr) // register the exception listener on connection
            tConnection.setExceptionListener(this);
        tConnection.start();

        if (operation.equals("pub")) {
            boolean xa = ((xaMode & XA_COMMIT) > 0) ? true : false;
            tSession = tConnection.createTopicSession(xa,
                TopicSession.AUTO_ACKNOWLEDGE);

            if (withFlowControlDisabled) // for SonicMQ only
                MessageUtils.disableFlowControl(tSession);

            if (topic == null && isPhysical)
                topic = tSession.createTopic(tName);
        }
        else {
            int ack;
            if ((xaMode & XA_CLIENT) != 0)
                ack = TopicSession.CLIENT_ACKNOWLEDGE;
            else
                ack = TopicSession.AUTO_ACKNOWLEDGE;

            tSession = tConnection.createTopicSession(false, ack);

            if (topic == null && isPhysical)
                topic = tSession.createTopic(tName);

            if (operation.equals("DeregSub") && isDurable) {
                tSession.unsubscribe(subscriptionID);
                return;
            }
            else if (operation.equals("RegSub")) {
                if (isDurable)
                    tSession.createDurableSubscriber(topic, subscriptionID);
                else
                    tSession.createSubscriber(topic);
                return;
            }
        }

        tOpen();
    }

    /** opens the topic resources */
    public void tOpen() throws JMSException {
        if (tSession == null)
           throw(new JMSException("tSession for "+tName+" is not initialized"));

        if (operation.equals("pub")) {
            tPublisher = tSession.createPublisher(topic);
        }
        else if (isDurable) {
            if (msgSelector != null)
                tSubscriber = tSession.createDurableSubscriber(topic,
                    subscriptionID, msgSelector, true);
            else
                tSubscriber = tSession.createDurableSubscriber(topic,
                    subscriptionID);
        }
        else {
            if (msgSelector != null)
                tSubscriber = tSession.createSubscriber(topic,msgSelector,true);
            else
                tSubscriber = tSession.createSubscriber(topic);
        }
    }

    /** reopens the topic */
    public int tReopen() {
        try {
            tClose();
        }
        catch (JMSException e) {
            String str = tName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.WARNING, str + Event.traceStack(e)).send();
        }
        if (tConnection == null || tSession == null)
            return -1;
        try {
            tOpen();
            return 0;
        }
        catch (JMSException e) {
            String str = tName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.WARNING, str + Event.traceStack(e)).send();
            try {
                tClose();
            }
            catch (JMSException ee) {
            }
        }
        return -1;
    }

    /** closes the JMS topic resources */
    public synchronized void tClose() throws JMSException {
        if (tPublisher != null) {
            tPublisher.close();
            tPublisher = null;
        }
        if (tSubscriber != null) {
            tSubscriber.close();
            tSubscriber = null;
        }
    }

    /** reconnects to the JMS topic and returns null upon success */
    public String reconnect() {
        String str = tName + ": ";
        close();
        try {
            connect();
        }
        catch (JMSException e) {
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            str += Event.traceStack(e);
            try {
                close();
            }
            catch (Exception ee) {
            }
            return str;
        }
        catch (Exception e) {
            str += Event.traceStack(e);
            try {
                close();
            }
            catch (Exception ee) {
            }
            return str;
        }
        return null;
    }

    /**
     * It subscribes a JMS topic and sends the JMS messages into a output
     * channel that may be a JMS destination, an OutputStream or an XQueue.
     * It supports the message acknowledgement at the consumer end.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties.  In this case, it still tries to
     * send the message to the output channel and logs the errors.  It
     * does not catch the JMSException or IOException from the message
     * transport operations, like subscribe and send.  Such Exception will be
     * thrown so that the caller can handle it either to reset the connection
     * or to do something else.
     */
    public void sub(Object out) throws IOException, JMSException {
        Message inMessage = null;
        boolean clientXA = ((xaMode & XA_CLIENT) != 0);
        boolean isDaemon = (mode > 0);
        boolean isSleepy = (sleepTime > 0);
        String msgStr = null;
        String line = null;
        long count = 0, stm = 10;
        int type = 0;
        int sid = -1, cid;
        byte[] buffer = new byte[bufferSize];
        XQueue xq = null;
        int shift = partition[0];
        int len = partition[1];

        if (tSubscriber == null)
        throw(new JMSException("tSubscriber for "+tName+" is not initialized"));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        if (out == null) {
            type = 0;
        }
        else if (out instanceof QueueSender) {
            type = TC_QUEUE;
        }
        else if (out instanceof TopicPublisher) {
            type = TC_TOPIC;
        }
        else if (out instanceof OutputStream) {
            type = TC_STREAM;
        }
        else if (out instanceof XQueue) {
            type = TC_XQ;
            xq = (XQueue) out;
            if (clientXA && (xq.getGlobalMask() & XQueue.EXTERNAL_XA) == 0)
                clientXA = false;
            else
                clientXA = true;
        }
        else {
            type = -1;
        }

        if (type == TC_XQ) {
            int mask;
            boolean resetReplyTo = false;
            Queue replyTo = null;
            if (propertyName != null && propertyName.length > 0) { // replyTo
                line = String.valueOf(MessageUtils.SHOW_REPLY);
                if (propertyName[0].equals(line)) {
                    resetReplyTo = true;
                    if (propertyValue[0] != null && propertyValue[0].length()>0)
                        replyTo = createQueue(propertyValue[0]);
                }
            }
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
                    inMessage = (Message) tSubscriber.receive(receiveTime);
                }
                catch (JMSException e) {
                    xq.cancel(sid);
                    throw(e);
                }
                catch (Exception e) {
                    xq.cancel(sid);
                    throw(new RuntimeException(e.getMessage()));
                }
                catch (Error e) {
                    xq.cancel(sid);
                    Event.flush(e);
                }

                if (inMessage == null) {
                    xq.cancel(sid);
                    if (isDaemon) { // daemon mode
                        continue;
                    }
                    else {
                        break;
                    }
                }

                if (check_body) try {
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        new Event(Event.WARNING, tName +
                            ": unknown msg type").send();
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, tName + ": failed on body: " +
                        Event.traceStack(e)).send();
                }

                if (propertyName != null && propertyValue != null) {
                    int i=0, ic = 0;
                    try {
                        if (resetOption)
                            MessageUtils.resetProperties(inMessage);
                            MessageUtils.resetProperties(inMessage);
                        if (resetReplyTo) {
                            ic = MessageUtils.resetReplyTo(replyTo, inMessage);
                            i ++;
                        }
                        for (; i<propertyName.length; i++) {
                            ic = MessageUtils.setProperty(propertyName[i],
                                propertyValue[i], inMessage);
                            if (ic != 0)
                                break;
                        }
                        if (ic != 0)
                            new Event(Event.WARNING, "failed to set property "+
                                "of: "+propertyName[i]+ " for " + tName).send();
                    }
                    catch (JMSException e) {
                        new Event(Event.WARNING, "failed to set property of: "+
                            propertyName[i] + " for " + tName + ": " +
                            Event.traceStack(e)).send();
                    }
                }

                if (displayMask > 0) try {
                    line = MessageUtils.display(inMessage, msgStr,
                        displayMask, null);
                }
                catch (Exception e) {
                    line = "";
                }

                cid = xq.add(inMessage, sid);

                if (cid > 0) {
                    if (displayMask > 0) // display the message
                        new Event(Event.INFO, "sub a msg from " + tName +
                            " with (" + line + " )").send();
                    count ++;
                    if (!clientXA) try { // out has no EXTERNAL_XA set
                        inMessage.acknowledge();
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, xq.getName() +
                            ": failed to ack a msg from " + tName + ": " +
                            e.toString()).send();
                    }
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + tName).send();
                    break;
                }
                inMessage = null;

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
        }
        else {
            long ttl = 0;
            while (true) {
                inMessage = (Message) tSubscriber.receive(receiveTime);

                if (inMessage == null) {
                    if (isDaemon) { // daemon mode
                        continue;
                    }
                    else {
                        break;
                    }
                }

                if (check_body) try {
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        new Event(Event.WARNING, tName +
                            ": unknown msg type").send();
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, tName + ": failed on body: " +
                        Event.traceStack(e)).send();
                }

                if (displayMask > 0) try {
                    line = MessageUtils.display(inMessage, msgStr,
                        displayMask, propertyName);
                }
                catch (Exception e) {
                    line = "";
                }

                switch (type) {
                  case TC_QUEUE:
                    ttl = inMessage.getJMSExpiration();
                    if (ttl > 0) { // recover ttl first
                        ttl -= System.currentTimeMillis();
                        if (ttl <= 0) // reset ttl to default
                            ttl = 1000L;
                    }
                    switch (overwrite) {
                      case 0:
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), ttl);
                        break;
                      case 1:
                        ((QueueSender) out).send(inMessage, persistence,
                            inMessage.getJMSPriority(), ttl);
                        break;
                      case 2:
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(), priority, ttl);
                        break;
                      case 3:
                        ((QueueSender) out).send(inMessage, persistence,
                            priority, ttl);
                        break;
                      case 4:
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), expiry);
                        break;
                      case 5:
                        ((QueueSender) out).send(inMessage, persistence,
                            inMessage.getJMSPriority(), expiry);
                        break;
                      case 6:
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(), priority, expiry);
                        break;
                      case 7:
                        ((QueueSender) out).send(inMessage, persistence,
                            priority, expiry);
                        break;
                      default:
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), ttl);
                        break;
                    }
                    break;
                  case TC_TOPIC:
                    ttl = inMessage.getJMSExpiration();
                    if (ttl > 0) { // recover ttl first
                        ttl -= System.currentTimeMillis();
                        if (ttl <= 0) // reset ttl to default
                            ttl = 1000L;
                    }
                    switch (overwrite) {
                      case 0:
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), ttl);
                        break;
                      case 1:
                        ((TopicPublisher) out).publish(inMessage, persistence,
                            inMessage.getJMSPriority(), ttl);
                        break;
                      case 2:
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(), priority, ttl);
                        break;
                      case 3:
                        ((TopicPublisher) out).publish(inMessage, persistence,
                            priority, ttl);
                        break;
                      case 4:
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), expiry);
                        break;
                      case 5:
                        ((TopicPublisher) out).publish(inMessage, persistence,
                            inMessage.getJMSPriority(), expiry);
                        break;
                      case 6:
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(), priority, expiry);
                        break;
                      case 7:
                        ((TopicPublisher) out).publish(inMessage, persistence,
                            priority, expiry);
                        break;
                      default:
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(), ttl);
                        break;
                    }
                    break;
                  case TC_STREAM:
                    if (inMessage instanceof BytesMessage) {
                        MessageUtils.copyBytes((BytesMessage) inMessage, buffer,
                            (OutputStream) out);
                    }
                    else if (msgStr.length() > 0)
                        ((OutputStream) out).write(msgStr.getBytes());
                    break;
                  default:
                    break;
                }
                if (clientXA) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }

                if (displayMask > 0) // display the message
                    new Event(Event.INFO, "sub a msg from " + tName +
                        " with (" + line + " )").send();
                count ++;
                inMessage = null;

                if (maxNumberMsg > 0 && count >= maxNumberMsg) break;

                if (isSleepy) try { // slow down for a while
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "sub " + count + " msgs from "+ tName).send();
    }

    /**
     * read bytes from the InputStream and publish them into a JMS topic as
     * JMS messages.  It supports flexable but static delimiters at either
     * or both ends.  It also has the blind trim support at both ends.
     *<br/><br/>
     * This is MT-Safe as long as the threads share the same offhead,
     * offtail, sotBytes and eotBytes.
     */
    public void pub(InputStream in) throws IOException, JMSException {
        Message outMessage;
        int position = 0;
        int offset = 0;
        int bytesRead = 0;
        int totalBytes = 0;
        int retry = 0, len = 0, rawLen = 0, skip;
        long count = 0;
        DelimitedBuffer sBuf;
        byte[] buffer;
        byte[] rawBuf;
        StringBuffer textBuffer = null;
        boolean isNewMsg = false;
        boolean isSleepy = (sleepTime > 0);

        if (tPublisher == null)
         throw(new JMSException("tPublisher for "+tName+" is not initialized"));

        sBuf = new DelimitedBuffer(bufferSize, offhead, sotBytes,
            offtail, eotBytes);
        buffer = sBuf.getBuffer();

        if (textMode > 0)
            outMessage = tSession.createTextMessage();
        else
            outMessage = tSession.createBytesMessage();
        if (propertyName != null && propertyValue != null) {
            for (int i=0; i<propertyName.length; i++)
                MessageUtils.setProperty(propertyName[i], propertyValue[i],
                    outMessage);
        }

        if ((boundary & MS_SOT) == 0) { // SOT not defined
            isNewMsg = true;
            if (textMode > 0)
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

        for (;;) {
            if (in.available() == 0) {
                if (bytesRead < 0) // end of stream
                    break;
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                }
                if (retry > 2)
                    continue;
                retry ++;
            }
            while ((bytesRead = in.read(buffer, 0, bufferSize)) > 0) {
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
                        if ((boundary & MS_EOT) == 0 && //no eotBytes defined
                            isNewMsg && totalBytes - rawLen <= maxMsgLength) {
                            if (k > rawLen) { // read more bytes than rawBuf
                                // move existing bytes of rawBuf to msg buffer
                                if (textMode > 0 && len > 0)
                                    textBuffer.append(new String(rawBuf,0,len));
                                else if (len > 0)
                                  ((BytesMessage) outMessage).writeBytes(rawBuf,
                                        0, len);

                                // fill in the first k-rawLen bytes to msg buf
                                if (textMode > 0)
                                    textBuffer.append(new String(buffer, offset,
                                        k - rawLen));
                                else
                                  ((BytesMessage) outMessage).writeBytes(buffer,
                                        offset, k - rawLen);
                            }
                            else if (k + len > rawLen) { // not many but enough
                                // move the extra bytes of rawBuf to msg buffer
                                if (textMode > 0)
                                    textBuffer.append(new String(rawBuf, 0,
                                        k+len-rawLen));
                                else
                                  ((BytesMessage) outMessage).writeBytes(rawBuf,
                                        0, k+len-rawLen);
                            }
                            len = 0;
                            totalBytes -= rawLen;
                            if (textMode > 0) {
                                ((TextMessage) outMessage).setText(
                                    textBuffer.toString());
                                textBuffer = null;
                            } 
                            if (totalBytes > 0 || count > 0) {
                                if (overwrite == 0)
                                    tPublisher.publish(outMessage);
                                else
                                    tPublisher.publish(outMessage, persistence,
                                        priority, expiry);
                                count ++;
                                if (displayMask > 0)
                                    new Event(Event.INFO, "pub a msg of " +
                                        totalBytes + " bytes to "+tName).send();
                                if (isSleepy) try {
                                    Thread.sleep(sleepTime);
                                }
                                catch (InterruptedException e) {
                                }
                            }
                        }
                        else if (isNewMsg) { // missing EOT or msg too large
                            if (totalBytes > 0)
                                new Event(Event.INFO, "discarded " +
                                    totalBytes + " bytes to " + tName).send();
                        }
                        outMessage.clearBody();

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
                            if (textMode > 0)
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
                            if (textMode > 0)
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
                                if (textMode > 0 && len > 0)
                                    textBuffer.append(new String(rawBuf,0,len));
                                else if (len > 0)
                                  ((BytesMessage) outMessage).writeBytes(rawBuf,
                                        0, len);

                                // fill in the first k-rawLen bytes to msg buf
                                if (textMode > 0)
                                    textBuffer.append(new String(buffer, offset,
                                        k - rawLen));
                                else
                                  ((BytesMessage) outMessage).writeBytes(buffer,
                                        offset, k - rawLen);
                            }
                            else if (k + len > rawLen) { // not many but enough
                                // move the extra bytes of rawBuf to msg buffer
                                if (textMode > 0)
                                    textBuffer.append(new String(rawBuf, 0,
                                        k + len - rawLen));
                                else
                                  ((BytesMessage) outMessage).writeBytes(rawBuf,
                                        0, k + len - rawLen);
                            }
                            // reset current byte count of rawBuf
                            len = 0;
                            totalBytes -= rawLen;
                            if (textMode > 0) {
                                ((TextMessage) outMessage).setText(
                                    textBuffer.toString());
                                textBuffer = null;
                            }
                            if (overwrite == 0)
                                tPublisher.publish(outMessage);
                            else
                                tPublisher.publish(outMessage, persistence,
                                    priority, expiry);
                            count ++;
                            if (displayMask > 0)
                                new Event(Event.INFO, "pub a msg of " +
                                    totalBytes + " bytes to " + tName).send();
                            if (isSleepy) try {
                                Thread.sleep(sleepTime);
                            }
                            catch (InterruptedException e) {
                            }
                        }
                        else { // garbage at beginning or msg too large
                            if (totalBytes > 0)
                                new Event(Event.INFO, "discarded " +
                                    totalBytes + " bytes to " + tName).send();
                        }
                        outMessage.clearBody();
                        totalBytes = 0;
                        skip = offhead;
                        len = 0;
                        offset = position;
                        if ((boundary & MS_SOT) == 0) {
                            isNewMsg = true;
                            if (textMode > 0)
                                textBuffer = new StringBuffer();
                        }
                        else
                            isNewMsg = false;
                    }
                    else if (isNewMsg) {
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
                        if (totalBytes - rawLen <= maxMsgLength){
                            if (k > rawLen) { // read more bytes than rawBuf
                                // move existing bytes of rawBuf to msg buffer
                                if (textMode > 0 && len > 0)
                                    textBuffer.append(new String(rawBuf,0,len));
                                else if (len > 0)
                                  ((BytesMessage) outMessage).writeBytes(rawBuf,
                                        0, len);

                                // fill in the first k-rawLen bytes to msg buf
                                if (textMode > 0)
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
                                if (textMode > 0)
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
                } while (offset < bytesRead);
            }
        }
        if (boundary <= MS_SOT && bytesRead == -1 && totalBytes > 0) {
            if (totalBytes - rawLen <= maxMsgLength) {
                if (textMode > 0) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(textBuffer.toString());
                }
                if (overwrite == 0)
                    tPublisher.publish(outMessage);
                else
                    tPublisher.publish(outMessage, persistence,
                        priority, expiry);
                count ++;
                if (displayMask > 0)
                    new Event(Event.INFO, "pub a msg of " + totalBytes +
                        " bytes to " + tName).send();
            }
            else {
                new Event(Event.INFO, "discarded " + totalBytes +
                    " bytes to " + tName).send();
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "pub " + count + " msgs to " + tName).send();
    }

    /**
     * It gets JMS messages from the XQueue and publishes them on a given
     * JMS topic. It supports both transaction in the delivery session and
     * the message acknowledgement at the source.  In the publish operation,
     * the persistence, the priority, and the expiration time of each message
     * may get reset according to the overwrite mask. If overwrite mask is 0,
     * the persistence, the priority, and the time-to-live will be recovered
     * from each message for publish operation. Otherwise, certain
     * configuration values will be used. In this case, the overwrite mask
     * of 1 is to reset the persistence to a specific value. 2 is to reset
     * the priority to a certain value. 4 is to reset the expiration time
     * with a given time-to-live.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties or acknowlegement. In this case,
     * it removes the message from the XQueue and logs the errors.  It
     * does not catch the JMSException from the message transport operations,
     * like publish or commit.  Such JMSException will be thrown so that
     * the caller can handle it either to reset the connection or to do
     * something else.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void pub(XQueue xq) throws TimeoutException, JMSException {
        Message message;
        JMSException ex = null;
        String dt = null, id = null;
        boolean xa = ((xaMode & XA_COMMIT) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, ttl, count = 0, stm = 10;
        int mask;
        int sid = -1;
        int n, size = 0;
        int[] batch = null;
        int dmask = MessageUtils.SHOW_DATE;
        dmask ^= displayMask;
        dmask &= displayMask;
        if (xa)
            batch = new int[batchSize];

        if (tPublisher == null)
         throw(new JMSException("tPublisher for "+tName+" is not initialized"));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                if (size > 0) {
                    ex = commit(xq, batch, size);
                    size = 0;
                    if (ex != null) {
                        msgID = null;
                        new Event(Event.ERR, "failed to commit " + size +
                            " msgs from " + xq.getName() + ": " +
                            ex.toString()).send();
                    }
                }
                break;
            }
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (size > 0) {
                    ex = commit(xq, batch, size);
                    size = 0;
                    if (ex != null) {
                        msgID = null;
                        throw(ex);
                    }
                }
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
                if (size > 0) {
                    ex = commit(xq, batch, size);
                    size = 0;
                    if (ex != null) {
                        msgID = null;
                        throw(new JMSException("failed to commit " + size +
                            " msgs from " + xq.getName() + ": "+ex.toString()));
                    }
                }
                continue;
            }

            dt = null;
            ttl = -1;
            try {
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
                    tName + ": " + Event.traceStack(e)).send();
            }

            if (ttl > 0) { // recover ttl first
                ttl -= System.currentTimeMillis();
                if (ttl <= 0) // reset ttl to default
                    ttl = 1000L;
            }
            try {
                if (count == 0 && msgID != null) { // for retries
                    if (! msgID.equals(id)) switch (overwrite) {
                      case 0:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        tPublisher.publish(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(), priority, ttl);
                        break;
                      case 3:
                        tPublisher.publish(message, persistence, priority, ttl);
                        break;
                      case 4:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        tPublisher.publish(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(), priority, expiry);
                        break;
                      case 7:
                        tPublisher.publish(message, persistence, priority,
                            expiry);
                        break;
                      default:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                }
                else {
                    switch (overwrite) {
                      case 0:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        tPublisher.publish(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(), priority, ttl);
                        break;
                      case 3:
                        tPublisher.publish(message, persistence, priority, ttl);
                        break;
                      case 4:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        tPublisher.publish(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(), priority, expiry);
                        break;
                      case 7:
                        tPublisher.publish(message, persistence, priority,
                            expiry);
                        break;
                      default:
                        tPublisher.publish(message,message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                }
            }
            catch (InvalidDestinationException e) {
                new Event(Event.WARNING, "Invalid JMS Destination for "+
                    tName + ": " + Event.traceStack(e)).send();
            }
            catch (JMSException e) {
                if (xa) { // XA_COMMIT
                    batch[size++] = sid;
                    rollback(xq, batch, size);
                }
                else
                    xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                if (xa) { // XA_COMMIT
                    batch[size++] = sid;
                    rollback(xq, batch, size);
                }
                else
                    xq.putback(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                if (xa) { // XA_COMMIT
                    batch[size++] = sid;
                    rollback(xq, batch, size);
                }
                else
                    xq.putback(sid);
                Event.flush(e);
            }
            msgID = id;
            if (xa) { // XA_COMMIT
                batch[size++] = sid;
                if (size >= batchSize) {
                    ex = commit(xq, batch, size);
                    size = 0;
                    if (ex != null) {
                        msgID = null;
                        throw(ex);
                    }
                }
            }
            else if ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) {
                try {
                    message.acknowledge();
                }
                catch (JMSException e) {
                    String str = "";
                    Exception ee = e.getLinkedException();
                    if (ee != null)
                        str += "Linked exception: " + ee.getMessage()+ "\n";
                    new Event(Event.ERR, "failed to ack msg after pub on " +
                        tName + ": " + str + Event.traceStack(e)).send();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to ack msg after put to " +
                        tName + ": " + Event.traceStack(e)).send();
                }
                catch (Error e) {
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to ack msg after put to " +
                        tName + ": " + e.toString()).send();
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
                    new Event(Event.INFO, "pub a msg to " + tName +
                        " with ( Date: " + dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "pub a msg to " + tName +
                        " with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "pub a msg to " + tName).send();
            }
            count ++;
            message = null;

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
        if (displayMask != 0)
            new Event(Event.INFO, "pub " + count + " msgs to " + tName).send();
    }

    protected JMSException commit(XQueue xq, int[] batch, int size){
        int i;
        JMSException ex = null;
        try {
            tSession.commit();
        }
        catch (JMSException e) {
            rollback(xq, batch, size);
            return e;
        }
        catch (Exception e) {
            rollback(xq, batch, size);
            return new JMSException(e.getMessage());
        }
        catch (Error e) {
            rollback(xq, batch, size);
            Event.flush(e);
        }
        if ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0) {
            Message msg;
            for (i=size-1; i>=0; i--) {
                msg = (Message) xq.browse(batch[i]);
                try {
                    msg.acknowledge();
                }
                catch (JMSException e) {
                    if (ex == null) {
                        String str = "";
                        Exception ee = e.getLinkedException();
                        if (ee != null)
                            str += "Linked exception: " + ee.getMessage()+ "\n";
                        new Event(Event.ERR, "failed to ack msg after commit " +
                            "on " + tName +": "+str+Event.traceStack(e)).send();
                        ex = e;
                    }
                }
                catch (Exception e) {
                    if (ex == null) {
                        new Event(Event.ERR, "failed to ack msg after commit " +
                            "to " + tName + ": " + Event.traceStack(e)).send();
                        ex = new JMSException(e.getMessage());
                    }
                }
                catch (Error e) {
                    for (i=0; i<size; i++) {
                        xq.remove(batch[i]);
                    }
                    Event.flush(e);
                }
            }
        }
        for (i=0; i<size; i++) {
            xq.remove(batch[i]);
        }
        return null;
    }

    protected void rollback(XQueue xq, int[] batch, int size) {
        int i;
        try {
            tSession.rollback();
        }
        catch (JMSException e) {
            String str = "";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.getMessage() + "\n";
            new Event(Event.ERR, "failed to rollback " + size + " msgs on " +
                tName + ": " + str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to rollback " + size + " msgs from " +
                tName + ": " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            new Event(Event.ERR, "failed to rollback " + size + " msgs from " +
                tName + ": " + e.toString()).send();
            for (i=0; i<size; i++) {
                xq.putback(batch[i]);
            }
            Event.flush(e);
        }
        for (i=0; i<size; i++) {
            xq.putback(batch[i]);
        }
    }

    public String getOperation() {
        return operation;
    }

    /** returns an on-demand publisher in a separate session */
    public TopicPublisher getTopicPublisher() {
        if (bPublisher != null) // return the cached one
            return bPublisher;
        else try {
            Topic t;
            if (bSession != null) try {
                bSession.close();
            }
            catch (Exception ex) {
            }
            bSession = tConnection.createTopicSession(false,
                TopicSession.AUTO_ACKNOWLEDGE);
            t = bSession.createTopic(topic.getTopicName());
            bPublisher = bSession.createPublisher(t);
            return bPublisher;
        }
        catch (Exception e) {
            return null;
        }
    }

    /** returns an on-demand non-durable subscriber in a separate session */
    public TopicSubscriber getTopicSubscriber() {
        if (bSubscriber != null) // return the cached one
            return bSubscriber;
        else try {
            Topic t;
            if (bSession != null) try {
                bSession.close();
            }
            catch (Exception ex) {
            }
            bSession = tConnection.createTopicSession(false,
                TopicSession.AUTO_ACKNOWLEDGE);
            t = bSession.createTopic(topic.getTopicName());
            if (msgSelector != null)
                bSubscriber = tSession.createSubscriber(t, msgSelector, true);
            else
                bSubscriber = tSession.createSubscriber(t);
            return bSubscriber;
        }
        catch (Exception e) {
            return null;
        }
    }

    public Message createMessage(int type) throws JMSException {
        if (type > 0)
            return tSession.createTextMessage();
        else
            return tSession.createBytesMessage();
    }

    public Queue createQueue(String queueName) throws JMSException {
        return tSession.createQueue(queueName);
    }

    public void onException(JMSException e) {
        String str = "ExceptionListener on " + tName + ": ";
        Exception ex = e.getLinkedException();
        if (ex != null)
            str += "Linked exception: " + ex.toString() + "\n";
        if (tConnection != null) try {
            tClose();
        }
        catch (Exception ee) {
        }
        new Event(Event.ERR, str + Event.traceStack(e)).send();
    }

    public void close() {
        try {
            tClose();
        }
        catch (Exception e) {
        }
        if (tSession != null) try {
            tSession.close();
            tSession = null;
        }
        catch (Exception e) {
            tSession = null;
        }
        bPublisher = null;
        bSubscriber = null;
        if (bSession != null) try {
            bSession.close();
            bSession = null;
        }
        catch (Exception e) {
            bSession = null;
        }
        if (tConnection != null) try {
            tConnection.close();
        }
        catch (Exception e) {
        }
        tConnection = null;
    }
}
