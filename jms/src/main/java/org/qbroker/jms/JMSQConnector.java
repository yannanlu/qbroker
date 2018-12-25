package org.qbroker.jms;

/* JMSQConnector.java - an abstract class implements QueueConnector */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.InvalidDestinationException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueReceiver;
import javax.jms.QueueBrowser;
import javax.jms.QueueSession;
import javax.jms.TopicSubscriber;
import javax.jms.TopicPublisher;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.common.Utils;
import org.qbroker.common.QuickCache;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.QueueConnector;
import org.qbroker.event.Event;

/**
 * JMSQConnector is an abstract class partially implements QueueConnector. It
 * has only one abstract methods, initialize() that is vendor specific. It
 * creates a QueueConnectionFactory for all the operations. JMSQConnector has
 * implemented all the public methods of QueueConnector. But they can be
 * overriden if it is neccessary. Most important methods are connect(), get(),
 * put(), browse(), query(), request(), and reply(). It supports repeated
 * browsing on a queue with auto increment of JMSTimestamp.  But it requires
 * Mode is set to daemon and SequentialSearch is on.  The application is
 * supposed to reset the browser every time with null string as the only
 * argument.  If ReferenceFile is defined, the state info will be persisted
 * to the file for restart.
 *<br/><br/>
 * For put operations, Overwrite mask controls whether to reset the persistence,
 * the priority, and the expiration time on each incoming message. 0 means no
 * overwrite. In this case, the persistence, the priority, and the time-to-live
 * will be recovered from each incoming message for send action. 1 is to reset
 * the persistence to a specific value. 2 is to reset the priority to a
 * certain value. 4 is to reset the expiration time with a given time-to-live.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public abstract class JMSQConnector implements QueueConnector {
    protected QueueConnection qConnection = null;
    protected QueueSession qSession = null, bSession = null;

    protected QueueReceiver qReceiver = null, bReceiver = null;
    protected QueueBrowser qBrowser = null, bBrowser = null;
    protected QueueSender qSender = null, bSender = null;

    protected Queue queue = null;
    protected String msgID = null;
    protected long timestamp = -1;
    protected int bufferSize = 4096;

    protected String qName = null; // either JNDI name or real name
    protected File referenceFile = null;
    protected String username = null;
    protected String password = null;

    protected int[] partition;
    protected int displayMask = 0;
    protected long expiry = 0;
    protected int persistence = Message.DEFAULT_DELIVERY_MODE;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected int resultType = Utils.RESULT_TEXT;
    protected int overwrite = 0;
    protected int mode = 0;
    protected int sequentialSearch = 0;

    protected String[] propertyName = null;
    protected String[] propertyValue = null;
    protected String[] respPropertyName = null;
    protected String msgSelector = null;
    protected String operation = "browse";
    protected String msField = null, rcField = null, resultField = null;
    protected Template template = null;
    protected QuickCache cache = null;
    protected Msg2Text msg2Text = null;
    protected long startTime = -1;
    protected long endTime = -1;
    protected long waitTime = 500L;
    protected int maxIdleTime = 0;
    protected int maxNumberMsg = 0;
    protected int maxMsgLength = 4194304;
    protected int sleepTime = 0;
    protected int receiveTime = 1000;
    protected int timeout = 10000;
    protected int textMode = 1;
    protected int xaMode = XA_CLIENT;
    protected int batchSize = 1;
    protected int boundary = 0;
    protected int offhead = 0;
    protected int offtail = 0;
    protected byte[] sotBytes = new byte[0];
    protected byte[] eotBytes = new byte[0];
    protected final static int MS_SOT = DelimitedBuffer.MS_SOT;
    protected final static int MS_EOT = DelimitedBuffer.MS_EOT;

    protected MessageFilter[] msgFilters = null;
    protected boolean enable_exlsnr = false;
    protected boolean isPhysical = false;
    protected boolean resetOption = false;
    protected boolean withFlowControlDisabled = false;

    /** initializes the QueueConnectionFactory and returns it */
    protected abstract QueueConnectionFactory initialize() throws JMSException;

    /** establishes the queue connection to the JMS service */
    protected void connect() throws JMSException {
        QueueConnectionFactory factory = initialize();
        if (username != null)
            qConnection = factory.createQueueConnection(username, password);
        else
            qConnection = factory.createQueueConnection();

        if (enable_exlsnr) // register the exception listener on connection
            qConnection.setExceptionListener(this);
        qConnection.start();

        if (operation.equals("get")) {
            int ack;
            if ((xaMode & XA_CLIENT) != 0)
                ack = QueueSession.CLIENT_ACKNOWLEDGE;
            else
                ack = QueueSession.AUTO_ACKNOWLEDGE;
            qSession = qConnection.createQueueSession(false, ack);
        }
        else if (operation.equals("put")) {
            boolean xa = ((xaMode & XA_COMMIT) > 0) ? true : false;
            qSession = qConnection.createQueueSession(xa,
                QueueSession.AUTO_ACKNOWLEDGE);

            if (withFlowControlDisabled) // for SonicMQ only
                MessageUtils.disableFlowControl(qSession);
        }
        else if (operation.equals("request")) {
            qSession = qConnection.createQueueSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);

            if (withFlowControlDisabled) // for SonicMQ only
                MessageUtils.disableFlowControl(qSession);
        }
        else {
            qSession = qConnection.createQueueSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        }

        if (queue == null && isPhysical)
            queue = qSession.createQueue(qName);

        qOpen();
    }

    /** opens the queue resources */
    public void qOpen() throws JMSException {
        int i;
        if (qSession == null)
           throw(new JMSException("qSession for "+qName+" is not initialized"));

        if ("get".equals(operation)) {
            if (msgSelector != null)
                qReceiver = qSession.createReceiver(queue, msgSelector);
            else
                qReceiver = qSession.createReceiver(queue);
        }
        else if ("put".equals(operation) || "request".equals(operation)) {
            qSender = qSession.createSender(queue);
        }
        else if ("reply".equals(operation)) { // unidentitied qSender
            qSender = qSession.createSender(null);
        }
        else {
            if (msgSelector != null) {
                qBrowser = qSession.createBrowser(queue, msgSelector);
            }
            else {
                qBrowser = qSession.createBrowser(queue);
            }
        }
    }

    /**
     * closes the current qBrowser and creates a new qBrowser on the queue
     * with the given physical queue name and the message selector
     */
    protected int resetQueueBrowser(String name, String mSelector) {
        Queue q;
        int ic = 0;
        if (qBrowser != null) try {
            qBrowser.close();
        }
        catch (Exception e) {
        }

        if (name == null || name.length() <= 0) // use the default queue
            q = queue;
        else try { // create the queue for the name 
            q = qSession.createQueue(name);
        }
        catch (JMSException e) {
            String str = "failed to create queue on " + name + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            return -1;
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to create queue for " + name +
                ": " + Event.traceStack(e)).send();
            return -1;
        }

        try {
            if (mSelector != null)
                qBrowser = qSession.createBrowser(q, mSelector);
            else if (mode > 0 && timestamp >= 0 && msgID != null) {
                // for repeated browse
                mSelector = msgSelector;
                if (sequentialSearch > 0) { // update the msgSelector
                    int i;
                    startTime = timestamp;
                    if (mSelector == null)
                        mSelector = "JMSTimestamp >= " + startTime;
                    else if ((i = mSelector.indexOf("JMSTimestamp")) > 0)
                        mSelector = mSelector.substring(0, i) + " AND " +
                            "JMSTimestamp >= " + startTime;
                    else if (i == 0)
                        mSelector = "JMSTimestamp >= " + startTime;
                    else
                        mSelector += "AND JMSTimestamp >= " + startTime;
                }
                qBrowser = qSession.createBrowser(q, mSelector);
            }
            else if (msgSelector != null)
                qBrowser = qSession.createBrowser(q, msgSelector);
            else
                qBrowser = qSession.createBrowser(q);
        }
        catch (JMSException e) {
            if (name == null || name.length() <= 0)
                name = qName;
            String str = "failed to create qBrowser on " + name + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
            ic = -1;
        }
        return ic;
    }

    /** reopens the queue */
    public int qReopen() {
        try {
            qClose();
        }
        catch (JMSException e) {
            String str = qName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.WARNING, str + Event.traceStack(e)).send();
        }
        if (qConnection == null || qSession == null)
            return -1;
        try {
            qOpen();
            return 0;
        }
        catch (JMSException e) {
            String str = qName + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.WARNING, str + Event.traceStack(e)).send();
            try {
                qClose();
            }
            catch (JMSException ee) {
            }
        }
        return -1;
    }

    /** closes the JMS queue resources */
    public synchronized void qClose() throws JMSException {
        if (qSender != null) {
            qSender.close();
            qSender = null;
        }
        if (qReceiver != null) {
            qReceiver.close();
            qReceiver = null;
        }
        if (qBrowser != null) {
            qBrowser.close();
            qBrowser = null;
        }
    }

    /** reconnects to the JMS queue and returns null if success */
    public String reconnect() {
        String str = qName + ": ";
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
     * It browses JMS messages from the JMS queue and sends them into a output
     * channel that may be a JMS destination, an OutputStream or an XQueue.
     */
    public void browse(Object out) throws IOException, JMSException {
        Message inMessage;
        String msgStr = null;
        String line = null;
        Enumeration msgQ;
        long count = 0, stm = 10;
        int type = 0;
        int sid = -1;
        boolean isSleepy = (sleepTime > 0), hasFilter = false, ckBody = false;
        byte[] buffer = new byte[bufferSize];

        if (qBrowser == null)
           throw(new JMSException("qBrowser for "+qName+" is not initialized"));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        if (msgFilters != null && msgFilters.length > 0) {
            hasFilter = true;
            if ((displayMask & MessageUtils.SHOW_BODY) > 0)
                ckBody = true;
            else for (int i=0; i<msgFilters.length; i++) {
                if (msgFilters[i].checkBody()) {
                    ckBody = true;
                    break;
                }
            }
        }
        else if ((displayMask & MessageUtils.SHOW_BODY) > 0)
            ckBody = true;

        if (timestamp >= 0) // for repeated browse
            resetQueueBrowser(null, null);
        msgQ = qBrowser.getEnumeration();

        if (out == null) {
            type = 0;
        }
        else if (out instanceof QueueSender) {
            type = QC_QUEUE;
        }

        else if (out instanceof TopicPublisher) {
            type = QC_TOPIC;
        }
        else if (out instanceof OutputStream) {
            type = QC_STREAM;
        }
        else if (out instanceof XQueue) {
            int mask = ((XQueue) out).getGlobalMask() & (~XQueue.EXTERNAL_XA);
            ((XQueue) out).setGlobalMask(mask);
            type = QC_XQ;
        }
        else {
            type = -1;
        }
        if (out != null && out instanceof XQueue) {
            XQueue xq = (XQueue) out;
            String id = null;
            long ts = -1;
            int mask = 0;
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
            if (sequentialSearch > 0 && timestamp > 0) // reset startTime
                startTime = timestamp;
            while (msgQ.hasMoreElements()) {
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) == 0)
                    break;
                if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                    break;
                if ((sid = xq.reserve(waitTime)) < 0)
                    continue;

                inMessage = (Message) msgQ.nextElement();
                if (sequentialSearch > 0) { // start sequential search
                    id = inMessage.getJMSMessageID();
                    ts = inMessage.getJMSTimestamp();
                    if (startTime > 0) {
                        if (ts < startTime || (ts == startTime &&
                            id.equals(msgID))) {
                            xq.cancel(sid);
                            continue;
                        }
                    }
                    else if (endTime > 0 && ts > endTime) {
                        xq.cancel(sid);
                        break;
                    }
                    // turn off startTime
                    startTime = 0;
                    if (ts >= timestamp) { // update state info
                        msgID = id;
                        timestamp = ts;
                    }
                }

                if (hasFilter) { // apply filters
                    int j;
                    if (ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    else
                        msgStr = null;

                    for (j=0; j<msgFilters.length; j++) {
                        if (msgFilters[j].evaluate(inMessage, msgStr))
                            break;
                    }

                    if (j >= msgFilters.length) { // no hit
                        xq.cancel(sid);
                        continue;
                    }
                }
                else if (ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                else
                    msgStr = null;

                if (propertyName != null && propertyValue != null) {
                    int i=0, ic = 0;
                    try {
                        if (resetOption)
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
                                "of: "+propertyName[i]+ " for " + qName).send();
                    }
                    catch (JMSException e) {
                        new Event(Event.WARNING, "failed to set property of: "+
                            propertyName[i] + " for " + qName + ": " +
                            Event.traceStack(e)).send();
                    }
                }

                if (displayMask > 0) try {
                    line = MessageUtils.display(inMessage, msgStr,
                        displayMask, propertyName);
                }
                catch (Exception e) {
                    line = "";
                }

                sid = xq.add(inMessage, sid);

                if (sid > 0) {
                    if (displayMask > 0) // display the message
                        new Event(Event.INFO, (count+1) +":" + line).send();
                    if (sequentialSearch > 0) {
                        if (ts >= timestamp) try { // enforce non-decrease rule
                            msgID = id;
                            timestamp = ts;
                            saveReference();
                        }
                        catch (Exception e) {
                            new Event(Event.ERR, "failed to save reference to "+
                              referenceFile.getName()+": "+e.toString()).send();
                        }
                    }
                    count ++;
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + qName).send();
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
        }
        else {
            long ttl = 0;
            while (msgQ.hasMoreElements()) {
                inMessage = (Message) msgQ.nextElement();
                if (sequentialSearch > 0) { // start sequential search
                    long t = inMessage.getJMSTimestamp();
                    if (startTime > 0 && t < startTime) {
                        continue;
                    }
                    else if (endTime > 0 && t > endTime) {
                        break;
                    }
                }

                if (hasFilter) { // apply filters
                    int j;
                    if (ckBody)
                        msgStr = MessageUtils.processBody(inMessage, buffer);
                    else
                        msgStr = null;

                    for (j=0; j<msgFilters.length; j++) {
                        if (msgFilters[j].evaluate(inMessage, msgStr))
                            break;
                    }

                    if (j >= msgFilters.length) // no hit
                        continue;
                }
                else if (ckBody)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                else
                    msgStr = null;

                if (displayMask > 0) try {
                    line = MessageUtils.display(inMessage, msgStr,
                        displayMask, propertyName);
                }
                catch (Exception e) {
                    line = "";
                }

                switch (type) {
                  case QC_QUEUE:
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
                        ((QueueSender) out).send(inMessage,
                            persistence, priority, ttl);
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
                  case QC_TOPIC:
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
                  case QC_STREAM:
                    if (inMessage instanceof BytesMessage) {
                        MessageUtils.copyBytes((BytesMessage) inMessage, buffer,
                            (OutputStream) out);
                    }
                    else {
                        if (msgStr == null)
                            msgStr = MessageUtils.processBody(inMessage,buffer);
                        if (msgStr != null && msgStr.length() > 0)
                            ((OutputStream) out).write(msgStr.getBytes());
                    }
                    break;
                  default:
                    break;
                }
                if (displayMask > 0) // display the message
                    new Event(Event.INFO, (count+1) +":" + line).send();
                count ++;

                if (maxNumberMsg > 0 && count >= maxNumberMsg) break;

                if (isSleepy) try { // slow down a while
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }
        if (mode == 0) // for non-repeated browse
            qBrowser.close();
        if (displayMask != 0)
            new Event(Event.INFO, "browsed "+count+" msgs from "+qName).send();
    }

    /**
     * flushes the current reference info to a given file for tracking the
     * browser state if the ReferenceFile is defined.
     */
    protected void saveReference() throws IOException {
        if (referenceFile != null) try {
            FileWriter out = new FileWriter(referenceFile);
            out.write(timestamp + " " + msgID + "\n");
            out.flush();
            out.close();
        }
        catch (Exception e) {
            throw(new IOException(e.toString()));
        }
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * query statement from each message. The query statement contains a list
     * of column names, a physical queue name and an optional message selector.
     * After creation of a new queue browser, it browses the messages from the
     * queue.  Each message will be formatted into a structured text according
     * to the column list and the ResultType. The content of each message will
     * be appended into the body of the incoming message as the response.
     * Please always to set a reasonable MaxNumberMsg to limit maximum number
     * of messages for the query.
     *<br/><br/>
     * It also supports the dynamic content filter and the formatter on queried
     * messages. The filter and the formatter are defined via a JSON text with
     * Ruleset defined as a list. The JSON text is stored in the body of the
     * request message. If the filter is defined, it will be used to select
     * certain messages based on the content and the header. If the formatter
     * is defined, it will be used to format the messages.
     */
    public void query(XQueue xq) throws JMSException, TimeoutException {
        Message outMessage, msg;
        String msgStr = null;
        String line = null, key;
        Enumeration msgQ;
        Msg2Text msg2Text = null;
        int i, k, n, mask;
        int sid = -1;
        long currentTime, idleTime, tm, count = 0;
        int heartbeat = 600000, ttl = 7200000;
        StringBuffer strBuf;
        MessageFilter[] filters = null;
        boolean ckBody = false, hasFilter = false;
        boolean checkIdle = (maxIdleTime > 0), isJMSType = false; 
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
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
            if ((mask & XQueue.STANDBY) > 0) //standby temporarily
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
                        if (currentTime - cache.getMTime() >= heartbeat) {
                            cache.disfragment(currentTime);
                            cache.setStatus(cache.getStatus(), currentTime);
                        }
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

            if (!(outMessage instanceof TextMessage) &&
                !(outMessage instanceof BytesMessage)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "unsupported msg type from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            msgStr = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                msgStr = MessageUtils.getProperty(msField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    if (template != null)
                        msgStr = MessageUtils.format(outMessage, buffer,
                            template);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            // get queue name
            key = MessageUtils.getQueue(msgStr);
            if (key == null || key.length() <= 0) { // no queue defined
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "bad query from " + xq.getName() +
                    ": " + msgStr).send();
                outMessage = null;
                continue;
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, xq.getName()+": message expired at " +
                    Event.dateFormat(new Date(tm)) + " for " + key).send();
                outMessage = null;
                continue;
            }

            // get template string and its formatter
            line = toKey(MessageUtils.getColumns(msgStr));
            if (cache.containsKey(line)) { // existing formatter
                msg2Text = (Msg2Text) cache.get(line);
            }
            else {
                Map<String, Object> h = new HashMap<String, Object>();
                h.put("Name", qName);
                h.put("Template", line);

                if ((resultType & Utils.RESULT_XML) > 0)
                    h.put("BaseTag", "Record");

                msg2Text = new Msg2Text(h);
                cache.insert(line, currentTime, 0, null, msg2Text);
            }

            // get message selector
            line = MessageUtils.getSelector(msgStr);

            // get dynamic content filters
            msgStr = null;
            try { // retrieve dynamic content filters
                msgStr = MessageUtils.processBody(outMessage, buffer);
                if (msgStr == null || msgStr.length() <= 0)
                    filters = null;
                else if (msgStr.indexOf("Ruleset") < 0)
                    filters = null;
                else if (cache.containsKey(msgStr) &&
                    !cache.isExpired(msgStr, currentTime))
                    filters = (MessageFilter[]) cache.get(msgStr, currentTime);
                else { // new or expired
                    Map ph;
                    StringReader ins = new StringReader(msgStr);
                    ph = (Map) JSON2Map.parse(ins);
                    ins.close();
                    if (currentTime - cache.getMTime() >= heartbeat) {
                        cache.disfragment(currentTime);
                        cache.setStatus(cache.getStatus(), currentTime);
                    }
                    filters = MessageFilter.initFilters(ph);
                    if (filters != null && filters.length > 0)
                        cache.insert(msgStr, currentTime, ttl, null, filters);
                    else
                        filters = null;
                }
            }
            catch (Exception e) {
                filters = null;
                new Event(Event.WARNING, xq.getName() +
                    ": failed to retrieve content filters for " + key + ": " +
                    msgStr + ": " + Event.traceStack(e)).send();
            }

            // set checkers for dynamic content filters
            if (filters != null && filters.length > 0) {
                ckBody = false;
                hasFilter = true;
                for (i=0; i<filters.length; i++) {
                    if (filters[i].checkBody()) {
                        ckBody = true;
                        break;
                    }
                }
            }
            else {
                ckBody = false;
                hasFilter = false;
            }

            if (resetQueueBrowser(key, line) != 0) { // reset MessageSelector
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, xq.getName() +
                    ": failed to reset qBrowser for " + key + " with " +
                    line).send();
                outMessage = null;
                continue;
            }

            k = 0;
            strBuf = new StringBuffer();
            try {
                msgQ = qBrowser.getEnumeration();
                while (msgQ.hasMoreElements()) {
                    if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                        break;
                    msg = (Message) msgQ.nextElement();

                    if (msg == null)
                        break;

                    if (hasFilter) { // apply filters
                        if (ckBody)
                            line = MessageUtils.processBody(msg, buffer);
                        else
                            line = null;

                        for (i=0; i<filters.length; i++) {
                            if (filters[i].evaluate(msg, line)) {
                                if (filters[i].hasFormatter())
                                    filters[i].format(msg, buffer);
                                break;
                            }
                        }

                        if (i >= filters.length) // no hit
                            continue;
                    }

                    line = msg2Text.format(resultType, msg);
                    if (line != null && line.length() > 0) {
                        if ((resultType & Utils.RESULT_XML) > 0)
                            strBuf.append(line + Utils.RS);
                        else if ((resultType & Utils.RESULT_JSON) > 0) {
                            if (strBuf.length() > 0)
                                strBuf.append(",");
                            strBuf.append(line + Utils.RS);
                        }
                        else
                            strBuf.append(line + Utils.RS);
                        k ++;
                    }
                    else
                        new Event(Event.WARNING, xq.getName() +
                            ": failed to format msg from " + key).send();
                    if (maxNumberMsg > 0 && k >= maxNumberMsg) break;
                }

                if ((resultType & Utils.RESULT_XML) > 0) {
                    strBuf.insert(0, "<Result>" + Utils.RS);
                    strBuf.append("</Result>");
                }
                else if ((resultType & Utils.RESULT_JSON) > 0) {
                    strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
                    strBuf.append("]}");
                }

                outMessage.clearBody();
                if (outMessage instanceof TextMessage)
                    ((TextMessage) outMessage).setText(strBuf.toString());
                else {
                    line = strBuf.toString();
                    ((BytesMessage) outMessage).writeBytes(line.getBytes());
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(k),
                    outMessage);
                if (displayMask > 0)
                    line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
            }
            catch (JMSException e) {
                String str = key;
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                new Event(Event.ERR, str + " failed to set property " +
                    Event.traceStack(e)).send();
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
                    qName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    qName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    qName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) // display the message
                new Event(Event.INFO, (count+1) +":" + line).send();
            count ++;
            outMessage = null;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries from " +
                xq.getName()).send();
    }

    protected static String toKey(String[] keys) {
        StringBuffer strBuf;
        if (keys == null || keys.length <= 0)
            return null;
        strBuf = new StringBuffer();
        for (int i=0; i<keys.length; i++) {
            if (keys[i] == null || keys[i].length() <= 0)
                continue;
            if (strBuf.length() > 0)
                strBuf.append(" ");
            strBuf.append("##" + keys[i] + "##");
        }
        if (strBuf.length() <= 0)
            return null;
        else
            return strBuf.toString();
    }

    /**
     * It gets JMS messages from the JMS queue and sends them into a output
     * channel that may be a JMS destination, an OutputStream or an XQueue.
     * It supports the message acknowledgement at the consumer end.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties.  In this case, it still tries to
     * send the message to the output channel and logs the errors.  It
     * does not catch the JMSException or IOException from the message
     * transport operations, like receive and send.  Such Exception will be
     * thrown so that the caller can handle it either to reset the connection
     * or to do something else.
     */
    public void get(Object out) throws IOException, JMSException {
        Message inMessage = null;
        String msgStr = null;
        String line = null;
        boolean clientXA = ((xaMode & XA_CLIENT) != 0);
        boolean isDaemon = (mode > 0);
        boolean isSleepy = (sleepTime > 0);
        boolean showBody = ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0);
        long count = 0, stm = 10;
        int type = 0;
        int sid = -1, cid;
        byte[] buffer = new byte[bufferSize];
        int shift = partition[0];
        int len = partition[1];
        XQueue xq = null;

        if (qReceiver == null)
          throw(new JMSException("qReceiver for "+qName+" is not initialized"));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        if (out == null) {
            type = 0;
        }
        else if (out instanceof QueueSender) {
            type = QC_QUEUE;
        }
        else if (out instanceof TopicPublisher) {
            type = QC_TOPIC;
        }
        else if (out instanceof OutputStream) {
            type = QC_STREAM;
        }
        else if (out instanceof XQueue) {
            type = QC_XQ;
            xq = (XQueue) out;
            if (clientXA && (xq.getGlobalMask() & XQueue.EXTERNAL_XA) == 0)
                clientXA = false;
            else
                clientXA = true;
        }
        else {
            type = -1;
        }

        if (type == QC_XQ) {
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
                    inMessage = (Message) qReceiver.receive(receiveTime);
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

                if (showBody) try {
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        new Event(Event.WARNING, qName +
                            ": unknown msg type").send();
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, qName + ": failed on body: " +
                        Event.traceStack(e)).send();
                }

                if (propertyName != null && propertyValue != null) {
                    int i=0, ic = 0;
                    try {
                        if (resetOption)
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
                                "of: "+propertyName[i]+ " for " + qName).send();
                    }
                    catch (JMSException e) {
                        new Event(Event.WARNING, "failed to set property of: "+
                            propertyName[i] + " for " + qName + ": " +
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
                        new Event(Event.INFO, "got a msg from " + qName +
                            " with (" + line + " )").send();
                    count ++;
                    if (!clientXA) try { // xq has no EXTERNAL_XA set
                        inMessage.acknowledge();
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, xq.getName() +
                            ": failed to ack a msg from " + qName + ": " +
                            e.toString()).send();
                    }
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + qName).send();
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
                inMessage = (Message) qReceiver.receive(receiveTime);

                if (inMessage == null) {
                    if (isDaemon) { // daemon mode
                        continue;
                    }
                    else {
                        break;
                    }
                }

                if (showBody) try {
                    msgStr = MessageUtils.processBody(inMessage, buffer);
                    if (msgStr == null)
                        new Event(Event.WARNING, qName +
                            ": unknown msg type").send();
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, qName + ": failed on body: " +
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
                  case QC_QUEUE:
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
                  case QC_TOPIC:
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
                        ((TopicPublisher) out).publish(inMessage,
                            persistence, inMessage.getJMSPriority(), ttl);
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
                  case QC_STREAM:
                    if (msgStr.length() > 0)
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
                    new Event(Event.INFO, "got a msg from " + qName +
                        " with (" + line + " )").send(); 
                count ++;
                inMessage = null;

                if (maxNumberMsg > 0 && count >= maxNumberMsg) break;

                if (isSleepy) try { // slow down a while
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "got " + count + " msgs from "+qName).send();
    }

    /**
     * It reads bytes from the InputStream and put them into a JMS queue as
     * JMS messages.  It supports flexable but static delimiters at either
     * or both ends.  It also has the blind trim support at both ends.
     *<br/><br/>
     * This is MT-Safe as long as the threads share the same offhead,
     * offtail, sotBytes and eotBytes.
     */
    public void put(InputStream in) throws IOException, JMSException {
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

        if (qSender == null)
            throw(new JMSException("qSender for "+qName+" is not initialized"));

        if (textMode > 0)
            outMessage = qSession.createTextMessage();
        else
            outMessage = qSession.createBytesMessage();

        sBuf = new DelimitedBuffer(bufferSize, offhead, sotBytes,
            offtail, eotBytes);
        buffer = sBuf.getBuffer();

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
                        if ((boundary & MS_EOT) == 0 && // no eotBytes defined
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
                                    qSender.send(outMessage);
                                else
                                    qSender.send(outMessage, persistence,
                                        priority, expiry);
                                count ++;
                                if (displayMask > 0)
                                    new Event(Event.INFO, "put a msg of " +
                                        totalBytes + " bytes to "+qName).send();
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
                                    totalBytes + " bytes to " + qName).send();
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
                                qSender.send(outMessage);
                            else
                                qSender.send(outMessage, persistence,
                                    priority, expiry);
                            count ++;
                            if (displayMask > 0)
                                new Event(Event.INFO, "put a msg of " +
                                    totalBytes + " bytes to " + qName).send();
                            if (isSleepy) try {
                                Thread.sleep(sleepTime);
                            }
                            catch (InterruptedException e) {
                            }
                        }
                        else { // garbage at beginning or msg too large
                            if (totalBytes > 0)
                                new Event(Event.INFO, "discarded " +
                                    totalBytes + " bytes to " + qName).send();
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
                    qSender.send(outMessage);
                else
                    qSender.send(outMessage, persistence, priority, expiry);
                count ++;
                if (displayMask > 0)
                    new Event(Event.INFO, "put a msg of " +
                        totalBytes + " bytes to " + qName).send();
            }
            else {
                new Event(Event.INFO, "discarded " + totalBytes +
                    " bytes to " + qName).send();
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "put " + count + " msgs to " + qName).send();
    }

    /**
     * It gets JMS messages from the XQueue and puts them into a JMS queue.
     * It supports both transaction in the delivery session and the message
     * acknowledgement at the source. In the send operation, the persistence,
     * the priority, and the expiration time of each message may get reset
     * according to the overwrite mask. If overwrite mask is 0, the persistence,
     * the priority, and the time-to-live will be recovered from each message
     * for send operation. Otherwise, certain configuration values will be used.
     * In this case, the overwrite mask of 1 is to reset the persistence to a
     * specific value. 2 is to reset the priority to a certain value. 4 is to
     * reset the expiration time with a given time-to-live.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties or acknowlegement. In this case,
     * it removes the message from the XQueue and logs the errors.  It
     * does not catch the JMSException from the message transport operations,
     * like send or commit.  Such JMSException will be thrown so that
     * the caller can handle it either to reset the connection or to do
     * something else.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime. 
     * it is up to the caller to handle this exception.
     */
    public void put(XQueue xq) throws TimeoutException, JMSException {
        Message message;
        JMSException ex = null;
        String dt = null, id = null;
        boolean xa = ((xaMode & XA_COMMIT) > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
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

        if (qSender == null)
            throw(new JMSException("qSender for "+qName+" is not initialized"));

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
                            " msgs from " + xq.getName() +": "+ ex.toString()));
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
                    qName + ": " + Event.traceStack(e)).send();
            }

            if (ttl > 0) { // recover ttl first
                ttl -= System.currentTimeMillis();
                if (ttl <= 0) // reset ttl to default
                    ttl = 1000L;
            }
            try {
                if (count == 0 && msgID != null) { // for retries
                    if (!msgID.equals(id)) switch (overwrite) {
                      case 0:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, ttl);
                        break;
                      case 3:
                        qSender.send(message, persistence, priority, ttl);
                        break;
                      case 4:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, expiry);
                        break;
                      case 7:
                        qSender.send(message, persistence, priority, expiry);
                        break;
                      default:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                    else { // skipping the resend
                        new Event(Event.NOTICE, "msg of " + msgID +
                            " has already been delivered to " + qName).send();
                    }
                }
                else {
                    switch (overwrite) {
                      case 0:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, ttl);
                        break;
                      case 3:
                        qSender.send(message, persistence, priority, ttl);
                        break;
                      case 4:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, expiry);
                        break;
                      case 7:
                        qSender.send(message, persistence, priority, expiry);
                        break;
                      default:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                }
            }
            catch (InvalidDestinationException e) {
                new Event(Event.WARNING, "Invalid JMS Destination for "+
                    qName + ": " + Event.traceStack(e)).send();
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
            else if (ack)
                ackMessage(sid, xq, message);
            else
                xq.remove(sid);

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(message, null, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "put a msg to " + qName +
                        " with ( Date: " + dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "put a msg to " + qName +
                        " with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "put a msg to " + qName).send();
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
            new Event(Event.INFO, "put " + count + " msgs to " + qName).send();
    }

    /**
     * It gets JMS messages from the XQueue and puts them into a JMS queue as
     * requests. Then it receives response messages one by one and copies the
     * message body from the response to the request message and reset its
     * rcField with a success code before removing it from the XQueue.
     <br/><br/>
     * It supports the message acknowledgement at the source. In the send
     * operation, the persistence, the priority, and the expiration time of
     * each message may get reset according to the overwrite mask. If overwrite
     * mask is 0, the persistence, the priority, and the time-to-live will be
     * recovered from each message for send operation. Otherwise, certain
     * configuration values will be used. In this case, the overwrite mask of
     * 1 is to reset the persistence to a specific value. 2 is to reset the
     * priority to a certain value. 4 is to reset the expiration time with a
     * given time-to-live.
     *<br/><br/>
     * For each sent request, it tries to receive the response message from
     * the given response queue without message selectors. If the response
     * queue name is null, a temporary queue will be created for each of the
     * request. Once the response is received, the message body will be copied
     * into the request message as the response. The rcField of the message
     * will be reset to the value of "0". In case of timeout on the receive
     * process, the expRC will be set to indicate the failure. In case of
     * failure, the error text will be set to the request body.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties or acknowlegement. In this case,
     * it removes the message from the XQueue and logs the errors.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime. 
     * It is up to the caller to handle this exception.
     */
    public void request(XQueue xq, String responseQueueName)
        throws TimeoutException, JMSException {
        Message message;
        Queue rq = null;
        JMSException ex = null;
        String dt = null, id = null, msgStr = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        boolean isTempQ = (responseQueueName == null);
        boolean withResp = (respPropertyName != null);
        long currentTime, idleTime, ttl, count = 0, stm = 10;
        int mask;
        int sid = -1;
        int n;
        int dmask = MessageUtils.SHOW_DATE;
        byte[] buffer = new byte[bufferSize];
        dmask ^= displayMask;
        dmask &= displayMask;

        if (qSender == null)
            throw(new JMSException("qSender for "+qName+" is not initialized"));

        if (!isTempQ) { // for permanent response queue
            rq = qSession.createQueue(responseQueueName);
            qReceiver = qSession.createReceiver(rq);
        }

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                break;
            }
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

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, msgRC, message);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(message);
                    MessageUtils.setProperty(rcField, msgRC, message);
                }
                catch (Exception ee) {
                    new Event(Event.ERR,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (ack)
                        ackMessage(sid, xq, message);
                    else
                        xq.remove(sid);
                    message = null;
                    continue;
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to set RC on request from " +
                    xq.getName()).send();
                if (ack)
                    ackMessage(sid, xq, message);
                else
                    xq.remove(sid);
                message = null;
                continue;
            }

            if (isTempQ) try { // init temp queue and the receiver
                rq = qSession.createTemporaryQueue();
                qReceiver = qSession.createReceiver(rq);
            }
            catch (JMSException e) { // session may be expired
                xq.putback(sid);
                cleanupTemporaryQueue((TemporaryQueue) rq);
                rq = null;
                qReceiver = null;
                throw(e);
            }
            catch (Throwable t) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to create TempQ: " + 
                    Event.traceStack(t)).send();
                cleanupTemporaryQueue((TemporaryQueue) rq);
                rq = null;
                qReceiver = null;
                if (t instanceof Exception)
                    continue;
                else
                    throw((Error) t);
            }

            dt = null;
            ttl = -1;
            try {
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt = Event.dateFormat(new Date(message.getJMSTimestamp()));
                id = message.getJMSMessageID();
                ttl = message.getJMSExpiration();
                message.setJMSReplyTo(rq);
                MessageUtils.setProperty(rcField, reqRC, message);
            }
            catch (JMSException e) {
                if (ttl < 0)
                    ttl = 0;
                id = null;
                new Event(Event.WARNING, "failed to get JMS property for "+
                    qName + ": " + Event.traceStack(e)).send();
            }

            if (ttl > 0) { // recover ttl first
                ttl -= System.currentTimeMillis();
                if (ttl <= 0) // reset ttl to default
                    ttl = 1000L;
            }

            msgStr = null;
            try {
                if (count == 0 && msgID != null) { // for retries
                    if (!msgID.equals(id)) switch (overwrite) {
                      case 0:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, ttl);
                        break;
                      case 3:
                        qSender.send(message, persistence, priority, ttl);
                        break;
                      case 4:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, expiry);
                        break;
                      case 7:
                        qSender.send(message, persistence, priority, expiry);
                        break;
                      default:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                    else { // skipping the resend
                        new Event(Event.NOTICE, "msg of " + msgID +
                            " has already been delivered to " + qName).send();
                    }
                }
                else {
                    switch (overwrite) {
                      case 0:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                      case 1:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), ttl);
                        break;
                      case 2:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, ttl);
                        break;
                      case 3:
                        qSender.send(message, persistence, priority, ttl);
                        break;
                      case 4:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), expiry);
                        break;
                      case 5:
                        qSender.send(message, persistence,
                            message.getJMSPriority(), expiry);
                        break;
                      case 6:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            priority, expiry);
                        break;
                      case 7:
                        qSender.send(message, persistence, priority, expiry);
                        break;
                      default:
                        qSender.send(message, message.getJMSDeliveryMode(),
                            message.getJMSPriority(), ttl);
                        break;
                    }
                }
            }
            catch (InvalidDestinationException e) {
                msgStr = "Invalid JMS Destination for "+
                    qName + ": " + Event.traceStack(e);
                new Event(Event.WARNING, msgStr).send();
            }
            catch (JMSException e) {
                xq.putback(sid);
                if (isTempQ) { // clean up tempQ
                    cleanupTemporaryQueue((TemporaryQueue) rq);
                    rq = null;
                    qReceiver = null;
                }
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                if (isTempQ) { // clean up tempQ
                    cleanupTemporaryQueue((TemporaryQueue) rq);
                    rq = null;
                    qReceiver = null;
                }
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.putback(sid);
                if (isTempQ) { // clean up tempQ
                    cleanupTemporaryQueue((TemporaryQueue) rq);
                    rq = null;
                    qReceiver = null;
                }
                Event.flush(e);
            }

            if (msgStr == null) { // request sent out, wait for response
                Message msg = null;
                mask = xq.getGlobalMask();
                ttl = timeout + System.currentTimeMillis();
                do {
                    if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                        break;
                    try {
                        msg = (Message) qReceiver.receive(receiveTime);
                    }
                    catch (Exception e) {
                        msg = null;
                        msgStr = "failed to get response for request to "+
                            qName + ": " + Event.traceStack(e);
                    }
                    if (msg != null) // got response back
                        break;
                    if (System.currentTimeMillis() > ttl) // timeout
                        break;
                }
                while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0);

                if (msgStr != null) // exception
                    new Event(Event.ERR, msgStr).send();
                else if (msg == null) { // timed out or standby
                    if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                        msgStr = "aborted waiting on response for request to " +
                            qName;
                    else
                        msgStr = "response timed out for request to "+ qName;
                    new Event(Event.ERR, msgStr).send();
                    try {
                        MessageUtils.setProperty(rcField, expRC, message);
                    }
                    catch (JMSException e) {
                        new Event(Event.ERR,
                            "failed to set RC of timeout on request for "+
                            qName + ": " + Event.traceStack(e)).send();
                    }
                }
                else try { // copy response
                    message.clearBody();
                    msgStr = MessageUtils.processBody(msg, buffer);
                    if (message instanceof TextMessage)
                        ((TextMessage) message).setText(msgStr);
                    else
                        ((BytesMessage) message).writeBytes(msgStr.getBytes());

                    if (withResp) { // copy properties over
                        if (respPropertyName.length <= 0) {//copy all properties
                            Enumeration propNames = msg.getPropertyNames();
                            while (propNames.hasMoreElements()) {
                                String key = (String) propNames.nextElement();
                                if (key == null || key.length() <= 0)
                                    continue;
                                if (key.startsWith("JMS"))
                                    continue;
                                Object obj = msg.getObjectProperty(key);
                                if (obj != null)
                                    message.setObjectProperty(key, obj);
                            }
                        }
                        else for (String key : respPropertyName) {
                            String str = MessageUtils.getProperty(key, msg);
                            if (str != null && str.length() > 0)
                                MessageUtils.setProperty(key, str, msg);
                        }
                    }
                    MessageUtils.setProperty(rcField, okRC, message);
                }
                catch (JMSException e) {
                    new Event(Event.ERR, "failed to load response for " +
                        qName + ": " + Event.traceStack(e)).send();
                }
                msg = null;
            }

            msgID = id;
            if (ack)
                ackMessage(sid, xq, message);
            else
                xq.remove(sid);

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(message, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "requested a msg to " + qName +
                        " with ( Date: " + dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "requested a msg to " + qName +
                        " with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "requested a msg to " + qName).send();
            }
            count ++;
            message = null;
            msgStr = null;

            if (isTempQ) { // clean up tempQ
                cleanupTemporaryQueue((TemporaryQueue) rq);
                rq = null;
                qReceiver = null;
            }

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
            new Event(Event.INFO, "requested " + count + " msgs to " +
                qName).send();
    }

    /**
     * It gets JMS messages from the XQueue and sends them to their ReplyTo
     * queues. If it fails to get the ReplyTo queue, just sents the message
     * to the default destination. It supports the message acknowledgement
     * at the source.
     *<br/><br/>
     * It catches JMSException in the operations regarding to the message
     * itself, like get/set JMS properties or acknowlegement. In this case,
     * it removes the message from the XQueue and logs the errors.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime. 
     * It is up to the caller to handle this exception.
     */
    public void reply(XQueue xq) throws TimeoutException, JMSException {
        Message message;
        Queue rq = null;
        String dt = null, msgStr = null, rqName = null;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, count = 0, stm = 10;
        int mask;
        int sid = -1;
        int n;
        int dmask = MessageUtils.SHOW_DATE;
        byte[] buffer = new byte[bufferSize];
        dmask ^= displayMask;
        dmask &= displayMask;

        if (qSender == null)
            throw(new JMSException("qSender for "+qName+" is not initialized"));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // standby temporarily
                break;
            }
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
            try {
                rq = (Queue) message.getJMSReplyTo();
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt = Event.dateFormat(new Date(message.getJMSTimestamp()));
            }
            catch (Exception e) {
                rq = null;
                new Event(Event.ERR, "failed to get ReplyTo from msg of "+
                    xq.getName() + ": " + Event.traceStack(e)).send();
            }

            if (rq == null) // use default queue
                rq = queue;
            rqName = rq.getQueueName();

            try {
                qSender.send(rq, message);
            }
            catch (InvalidDestinationException e) {
                new Event(Event.ERR, "Invalid JMS Destination for " +
                    rqName + ": " + Event.traceStack(e)).send();
            }
            catch (JMSException e) { // session may be expired
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

            if (ack)
                ackMessage(sid, xq, message);
            else
                xq.remove(sid);

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(message, null, dmask,
                    propertyName);
                new Event(Event.INFO, "replied a msg to " + rqName +
                    " with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "replied a msg to " + rqName).send();
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
            new Event(Event.INFO, "replied " + count + " msgs for " +
                qName).send();
    }

    private void ackMessage(int sid, XQueue xq, Message message) {
        try {
            message.acknowledge();
        }
        catch (JMSException e) {
            String str = "";
            Exception ee = e.getLinkedException();
            if (ee != null)
                str += "Linked exception: " + ee.getMessage()+ "\n";
            new Event(Event.ERR, "failed to ack msg after request to " +
                qName + ": " + str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to ack msg after request to " +
                qName + ": " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            xq.remove(sid);
            new Event(Event.ERR, "failed to ack msg after request to " +
                qName + ": " + e.toString()).send();
            Event.flush(e);
        }
        xq.remove(sid);
    }

    private void cleanupTemporaryQueue(TemporaryQueue rq) {
        if (qReceiver != null) try {
            qReceiver.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to close temp qreceiver: " +
                Event.traceStack(e)).send();
        }
        if (rq != null) try {
            rq.delete();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to delete temp queue: " +
                Event.traceStack(e)).send();
        }
    }

    /** commits messages sent in the batch and returns exception upon failure */
    protected JMSException commit(XQueue xq, int[] batch, int size){
        int i;
        JMSException ex = null;
        try {
            qSession.commit();
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
                if (msg != null) try {
                    msg.acknowledge();
                }
                catch (JMSException e) {
                    if (ex == null) {
                        String str = "";
                        Exception ee = e.getLinkedException();
                        if (ee != null)
                            str += "Linked exception: " + ee.getMessage()+ "\n";
                        new Event(Event.ERR, "failed to ack msg after commit " +
                            "to " + qName +": "+str+Event.traceStack(e)).send();
                        ex = e;
                    }
                }
                catch (Exception e) {
                    if (ex == null) {
                        new Event(Event.ERR, "failed to ack msg after commit " +
                            "to " + qName + ": " + Event.traceStack(e)).send();
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

    /** rolls back the messages sent in the batch  */
    protected void rollback(XQueue xq, int[] batch, int size) {
        int i;
        try {
            qSession.rollback();
        }
        catch (JMSException e) {
            String str = "";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.getMessage() + "\n";
            new Event(Event.ERR, "failed to rollback " + size + " msgs from " +
                qName + ": " + str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to rollback " + size + " msgs from " +
                qName + ": " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            new Event(Event.ERR, "failed to rollback " + size + " msgs from " +
                qName + ": " + e.toString()).send();
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

    /** returns an on-demand browser in a separate session */
    public QueueBrowser getQueueBrowser() {
        if (bBrowser != null)
            return bBrowser;
        else try {
            Queue q;
            if (bSession != null) try {
                bSession.close();
            }
            catch (Exception ex) {
            }
            bSession = qConnection.createQueueSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
            q = bSession.createQueue(queue.getQueueName());
            if (msgSelector != null)
                bBrowser = qSession.createBrowser(q, msgSelector);
            else
                bBrowser = qSession.createBrowser(q);
            return bBrowser;
        }
        catch (Exception e) {
            return null;
        }
    }

    /** returns an on-demand receiver in a separate session */
    public QueueReceiver getQueueReceiver() {
        if (bReceiver != null) // return the cache
            return bReceiver;
        else try {
            Queue q;
            if (bSession != null) try {
                bSession.close();
            }
            catch (Exception ex) {
            }
            bSession = qConnection.createQueueSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
            q = bSession.createQueue(queue.getQueueName());
            if (msgSelector != null)
                bReceiver = bSession.createReceiver(q, msgSelector);
            else
                bReceiver = bSession.createReceiver(q);
            return bReceiver;
        }
        catch (Exception e) {
            return null;
        }
    }

    /** returns an on-demand sender in a separate session */
    public QueueSender getQueueSender() {
        if (bSender != null) // return the cache
            return bSender;
        else try { // create a new one in a separate session
            Queue q;
            if (bSession != null) try {
                bSession.close();
            }
            catch (Exception ex) {
            }
            bSession = qConnection.createQueueSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
            q = bSession.createQueue(queue.getQueueName());
            bSender = bSession.createSender(q);
            return bSender;
        }
        catch (Exception e) {
            return null;
        }
    }

    public Message createMessage(int type) throws JMSException {
        if (type > 0)
            return qSession.createTextMessage();
        else
            return qSession.createBytesMessage();
    }

    public Queue createQueue(String queueName) throws JMSException {
        return qSession.createQueue(queueName);
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return qSession.createTemporaryQueue();
    }

    public void onException(JMSException e) {
        String str = "ExceptionListener on " + qName + ": ";
        Exception ex = e.getLinkedException();
        if (ex != null)
            str += "Linked exception: " + ex.toString() + "\n";
        if (qConnection != null) try {
            qClose();
        }
        catch (Exception ee) {
        }
        new Event(Event.ERR, str + Event.traceStack(e)).send();
    }

    public void close() {
        if (cache != null)
            cache.clear();
        try {
            qClose();
        }
        catch (Exception e) {
        }
        if (qSession != null) try {
            qSession.close();
            qSession = null;
        }
        catch (Exception e) {
            qSession = null;
        }
        bSender = null;
        bBrowser = null;
        bReceiver = null;
        if (bSession != null) try {
            bSession.close();
            bSession = null;
        }
        catch (Exception e) {
            bSession = null;
        }
        if (qConnection != null) try {
            qConnection.close();
        }
        catch (Exception e) {
        }
        qConnection = null;
    }
}
