package org.qbroker.wmq;

/* TConnector.java - a connector to a MQSeries topic */

import com.ibm.mq.jms.MQTopicConnectionFactory;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQTopic;
import com.ibm.mq.jms.JMSC;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
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
import javax.jms.InvalidDestinationException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.jms.QueueSender;
import javax.jms.QueueReceiver;
import javax.jms.QueueBrowser;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.TopicConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.BytesEvent;
import org.qbroker.wmq.QConnector;
import org.qbroker.event.Event;

/**
 * TConnector connects to a topic and initializes one of the operations,
 * such as sub, pub, RegSub or DeregSub.
 *<br/><br/>
 * It supports both MQ Broker (V1) and MQSI Broker (V2) for Publication and
 * Subscription.  You can choose the version by BrokerVersion.  Due to IBM's
 * implementation on publish, the API for publish has some kind of transaction
 * controls.  Within every 25 messages it publishes, the first message has
 * the reply field enabled and the API expects the reply from the broker.
 * This means the publish is no longer asynchronous.  In a distributed
 * environment, if the broker is down the messages will stay in the queue.
 * But the API will wait for the reply and hold the flow.  If the broker is
 * not available at the publish client startup, the client will fail.
 * There is a parameter to set so that the transaction size is 0.
 *<br/><br/>
 * There are two ways to get around this problem.  The first one is to set up
 * a generic publish message flow in MQSI Broker.  The flow will pick up the
 * topic string from certain JMS property and add the publish header with
 * the topic.  All the messages will be sent to the Publish Node.  This way,
 * the client just needs to use point-to-point to publish the messages.
 * The other way is to implement the message flow into publish method.
 * We have implemented a simple Publication and Subscription via
 * point-to-point.  In the implementation, the pub/sub header is added to
 * each messages.  In case of sub, it only supports QueueStore.  You can
 * choose this implementation by setting BrokerVersion to 0.  If you want to
 * use this implementation, please remember to specify the full path of each
 * JMS properties in the message selector.  For example, if you reference a
 * property called "FileName", you have to use "Root.MQRFH2.usr.FileName".
 *<br/><br/>
 * Here is the detail about the Pub/Sub support with point-to-point:
 *<br/><br/>
 * (1) always assume the subscription queue is set up already.<br/>
 * (2) browse SYSTEM.JMS.ADMIN.QUEUE for the clientID and subscriptionID.<br/>
 * (3) if found the entry, compare the topic and the subscription queue.<br/>
 * (4) if they are same, use get() rather than sub() to receive one message.<br/>
 * (5) if either topic or queue is not same, unsub old topic and sub new topic
 *      via broker and continue to get the message.<br/>
 * (6) if there is no match entry, sub the topic via broker and continue
 *     to get the messages.<br/>
 *<br/>
 * This is NOT MT-Safe due to IBM's implementation on sessions and conn.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TConnector implements TopicConnector {
    private TopicConnection tConnection = null;
    protected TopicSession tSession = null;
    private TopicPublisher tPublisher = null;
    private TopicSubscriber tSubscriber = null;
    private Topic topic = null;

    private QConnector qConn = null;
    private QueueReceiver qReceiver = null;
    private QueueBrowser qBrowser = null;
    private QueueSender qSender = null;

    private int bufferSize = 4096;
    private int port = 1414;
    private String uri;
    private String hostName;
    private String channelName;
    private String qmgrName;
    private String subQmgr = null;
    private String qName;
    private String tName;

    private String clientID = null;
    private String subscriptionID = null;
    private String msgID = null;
    private String contentFilter = null;
    private String brokerControlQueue = "SYSTEM.BROKER.CONTROL.QUEUE";
    private int brokerVersion = 2;
    private int regOption = 0;
    private int storeOption = 0;
    private String pubOption = null;
    private int[] partition;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private long expiry = 0;
    private int persistence = 0;
    private int priority = -3;
    private int targetClient = 0;
    private int mode = 0;
    private int sequentialSearch = 0;

    private String correlationID = null;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String msgSelector = null;
    private String operation = "pub";
    private String securityExit = null;
    private String securityData = null;
    private String username = null;
    private String password = null;

    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private long startTime = -1;
    private long endTime = -1;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int sleepTime = 0;
    private int textMode = 1;
    private int xaMode = XA_CLIENT;
    private int cellID = 0;
    private int batchSize = 1;

    private int nameValueCCSID = 1208;
    private int charSetID = 1208;
    private int encoding = 273;
    private byte subLock = 0x20;
    private int maxMsgLength = 4194304;
    private int boundary = 0;
    private int offhead = 0;
    private int offtail = 0;
    private byte[] sotBytes = new byte[0];
    private byte[] eotBytes = new byte[0];
    private final static int MS_SOT = DelimitedBuffer.MS_SOT;
    private final static int MS_EOT = DelimitedBuffer.MS_EOT;

    private boolean enable_exlsnr = false;
    private boolean isDurable = false;
    private boolean check_body = false;
    private boolean resetOption = false;

    /** Creates new TConnector */
    public TConnector(Map props) throws JMSException {
        boolean ignoreInitFailure = false;
        Object o;

        Template template = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");

        hostName = (String) props.get("HostName");
        qmgrName = (String) props.get("QueueManager");
        uri = (String) props.get("URI");
        if (hostName != null && hostName.length() > 0) {
            uri = "wmq://" + hostName;
            if ((o = props.get("Port")) != null)
                port = Integer.parseInt((String) o);
            uri += ":" + port;
        }
        else if (qmgrName != null && qmgrName.length() > 0)
            uri = "wmq:///" + qmgrName;
        else if (uri == null || uri.length() <= 6) // for default qmgr on bind
            uri = "wmq:///";
        else try {
            URI u = new URI(uri);
            hostName = u.getHost();
            if (hostName == null || hostName.length() <= 0) {
                qmgrName = u.getPath();
                if (qmgrName != null && qmgrName.length() > 1)
                    qmgrName = qmgrName.substring(1);
                else // for default qmgr on bind mode
                    qmgrName = null;
            }
            else if (u.getPort() > 0)
                port = u.getPort();
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        o = (String) props.get("QueueName");
        qName = MonitorUtils.substitute((String) o, template);
        tName = (String) props.get("TopicName");
        channelName = (String) props.get("ChannelName");
        msgSelector = (String) props.get("MessageSelector");
        if ((o = props.get("CellID")) != null)
            cellID = Integer.parseInt((String) o);
        if ((o = props.get("Port")) != null)
            port = Integer.parseInt((String) o);

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value;
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if((propertyName[n] = MessageUtils.getPropertyID(key)) == null){
                    propertyName[n] = key;
                    resetOption = true;
                }
                if (value != null && value.length() > 0) {
                    propertyValue[n] = template.substitute("CellID",
                        String.valueOf(cellID), value);
                }
                else if ("JMSReplyTo".equals(key)) {
                    propertyValue[n] = "";
                }
                if ("JMSReplyTo".equals(key) && n > 0) { // swap with the first
                    key = propertyName[0];
                    propertyName[0] = propertyName[n];
                    propertyName[n] = key;
                    value = propertyValue[0];
                    propertyValue[0] = propertyValue[n];
                    propertyValue[n] = value;
                }
                if ("JMSCorrelationID".equals(key))
                    correlationID = propertyValue[n];
                n ++;
            }
        }

        if ((o = props.get("SecurityExit")) != null)
            securityExit = (String) o;
        if ((o = props.get("SecurityData")) != null)
            securityData = (String) o;
        if ((o = props.get("Username")) != null)
            username = (String) o;
        if ((o = props.get("Password")) != null)
            password = (String) o;
        if ((o = props.get("BrokerControlQueue")) != null)
            brokerControlQueue = (String) o;
        if ((o = props.get("BrokerVersion")) != null)
            brokerVersion = Integer.parseInt((String) o);
        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if((o=props.get("SequentialSearch")) != null && "on".equals((String)o))
            sequentialSearch = 1;
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
        if (!operation.equals("pub")) {
            o = props.get("ClientID");
            clientID = template.substitute("CellID",
                String.valueOf(cellID), (String) o);
            o = props.get("SubscriptionID");
            subscriptionID = template.substitute("CellID",
                String.valueOf(cellID), (String) o);
            if ((o = props.get("StoreOption")) != null)
                storeOption = Integer.parseInt((String) o);
            if (brokerVersion == 0) {
                // JMS API will prefix "Root.MQRFH2.usr." to each of the user
                // properties referenced in MessageSelector.  So we have to
                // explicitly specify them in the MessageSelector if
                // BrokerVersion is set to 0
                contentFilter = msgSelector;
                if (contentFilter != null) // rebuild the MessageSelector
                    msgSelector = Utils.doSearchReplace("Root.MQRFH2.usr.",
                        "", msgSelector);

                subQmgr = (String) props.get("RealSubQmgr");
                if ((subQmgr == null && ! qName.endsWith("*")) ||
                    (subQmgr != null && correlationID != null))
                    regOption = 1;
            }
        }
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("BatchSize")) != null)
            batchSize = Integer.parseInt((String) o);
        if (batchSize <= 0)
            batchSize = 1;
        if ((o = props.get("TargetClient")) != null)
            targetClient = Integer.parseInt((String) o);
        if ((o = props.get("Persistence")) != null)
            persistence = Integer.parseInt((String) o);
        if ((o = props.get("Priority")) != null)
            priority = Integer.parseInt((String) o);
        if ((o = props.get("Expiry")) != null)
            expiry = Long.parseLong((String) o);
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
        if ((o = props.get("IgnoreInitFailure")) != null &&
            "true".equalsIgnoreCase((String) o))
            ignoreInitFailure = true;

        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);
        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0))
            check_body = true;

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");

        Date myDate = null;
        if ((o = props.get("StartDate")) != null) {
            myDate = dateFormat.parse((String) o, new ParsePosition(0));
            if (myDate != null) {
                startTime = myDate.getTime();
                if (msgSelector != null) {
                    msgSelector += " AND JMSTimestamp >= " +startTime;
                }
                else {
                    msgSelector = "JMSTimestamp >= " + startTime;
                }
            }
            myDate = null;
        }
        if ((o = props.get("EndDate")) != null) {
            myDate = dateFormat.parse((String) o, new ParsePosition(0));
            if (myDate != null) {
                endTime = myDate.getTime();
                if (msgSelector != null) {
                    msgSelector += " AND JMSTimestamp <= " + endTime;
                }
                else {
                    msgSelector = "JMSTimestamp <= " + endTime;
                }
            }
        }

        if ((o = props.get("EnableExceptionListener")) != null &&
            "true".equalsIgnoreCase((String) o))
            enable_exlsnr = true;

        try {
            if (brokerVersion != 0)
                connect();
            else
                connect_v0();
        }
        catch (JMSException e) {
            String str = tName + ": ";
            Exception ee = e.getLinkedException();
            if (ee != null)
                str += "Linked exception: " + ee.toString() + "\n";
            if (ignoreInitFailure)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            else
                throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }
    }

    /** Initializes the JMS components (TopicConnectionFactory,
     * TopicConnection, TopicPublisher, BytesMessage) for use
     * later in the application
     * @throws JMSException occurs on any JMS Error
     */
    private TopicConnectionFactory initialize() throws JMSException {
        String durableQPrefix = "SYSTEM.JMS.D.";

        // get an MQ-specific factory and set its props
        MQTopicConnectionFactory mqFactory = new MQTopicConnectionFactory();
        if (hostName != null) {
            mqFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
            mqFactory.setHostName(hostName);
            if (port > 0) {
                mqFactory.setPort(port);
            }
            else {
                mqFactory.setPort(1414);
            }
            if (channelName != null) {
                mqFactory.setChannel(channelName);
            }
            else {
                mqFactory.setChannel("SYSTEM.DEF.SVRCONN");
            }
            mqFactory.setCCSID(charSetID);
            if (securityExit != null) {
                mqFactory.setSecurityExit(securityExit);
                mqFactory.setSecurityExitInit(securityData);
            }
        }
        else {
            mqFactory.setTransportType(JMSC.MQJMS_TP_BINDINGS_MQ);
            if (qmgrName != null)
                mqFactory.setBrokerQueueManager(qmgrName);
        }
        mqFactory.setBrokerControlQueue(brokerControlQueue);

        if (brokerVersion == 2) {
            mqFactory.setBrokerVersion(JMSC.MQJMS_BROKER_V2);
        }
        else {
            mqFactory.setBrokerVersion(JMSC.MQJMS_BROKER_V1);
        }

        if (operation.equals("pub")) {
            if (qName != null) {
                mqFactory.setBrokerPubQueue(qName);
            }
            else {
                mqFactory.setBrokerPubQueue("SYSTEM.BROKER.DEFAULT.STREAM");
            }
        }
        else {
            // IBM JMS 5.21 and MQSeries 5.2 + CSD04 require to set QueueStore
            // Otherwise, it uses MigrateStore and you get double subscriptions
            if (storeOption == 0)
                mqFactory.setSubscriptionStore(JMSC.MQJMS_SUBSTORE_QUEUE);
            else
                mqFactory.setSubscriptionStore(JMSC.MQJMS_SUBSTORE_BROKER);

            if (qName != null && !qName.startsWith(durableQPrefix))
                mqFactory.setBrokerSubQueue(qName);
        }

        //return a casted generic JMS factory for connect
        return (TopicConnectionFactory) mqFactory;
    }

    protected void connect() throws JMSException {
        TopicConnectionFactory factory = initialize();
        String durableQPrefix = "SYSTEM.JMS.D.";

        if (hostName != null && username != null)
            tConnection = factory.createTopicConnection(username, password);
        else
            tConnection = factory.createTopicConnection();

        if (!operation.equals("pub"))
            tConnection.setClientID(clientID);
        if (enable_exlsnr) // register the exception listener on connection
            tConnection.setExceptionListener(this);
        tConnection.start();

        if (operation.equals("pub")) {
            boolean xa = ((xaMode & XA_COMMIT) > 0) ? true : false;
            tSession = tConnection.createTopicSession(xa,
                TopicSession.AUTO_ACKNOWLEDGE);

            StringBuffer tURL = new StringBuffer("topic://");
            tURL.append(tName);
            tURL.append("?targetClient=");
            if (targetClient != 0) {
                tURL.append(1);
            }
            else {
                tURL.append(0);
            }
            if (this.expiry > 0) {
                tURL.append("&expiry=");
                tURL.append(expiry);
            }
            if (priority >= -2 && priority <= 9) {
                tURL.append("&priority=");
                tURL.append(priority);
            }
            if (this.persistence != 0) {
                tURL.append("&persistence=");
                tURL.append(persistence);
            }

            topic = tSession.createTopic(tURL.toString());
        }
        else {
            int ack;
            if ((xaMode & XA_CLIENT) != 0)
                ack = TopicSession.CLIENT_ACKNOWLEDGE;
            else
                ack = TopicSession.AUTO_ACKNOWLEDGE;

            tSession = tConnection.createTopicSession(false, ack);

            topic = tSession.createTopic("topic://" + tName);
            if (qName.startsWith(durableQPrefix)) {
                // durable queue
                ((MQTopic) topic).setBrokerDurSubQueue(qName);
                if (brokerVersion == 2)
                    ((MQTopic) topic).setBrokerVersion(JMSC.MQJMS_BROKER_V2);

                if (operation.equals("DeregSub")) {
                    tSession.unsubscribe(subscriptionID);
                    return;
                }
                else if (operation.equals("RegSub")) {
                    tSession.createDurableSubscriber(topic, subscriptionID);
                    return;
                }
            }
            else if (operation.equals("DeregSub")) {
                tSession.unsubscribe(subscriptionID);
                return;
            }
            else if (operation.equals("RegSub")) {
                tSession.createSubscriber(topic);
                return;
            }
        }
        tOpen();
    }

    public void tOpen() throws JMSException {
        String durableQPrefix = "SYSTEM.JMS.D.";

        if (tSession == null)
           throw(new JMSException("tSession for "+tName+" is not initialized"));

        if (operation.equals("pub")) {
            tPublisher = tSession.createPublisher(topic);
        }
        else if (qName.startsWith(durableQPrefix)) {
            if (msgSelector != null)
                tSubscriber = tSession.createDurableSubscriber(topic,
                    subscriptionID, msgSelector, true);
            else
                tSubscriber = tSession.createDurableSubscriber(topic,
                    subscriptionID);
        }
        else {
            if (msgSelector != null)
                tSubscriber =tSession.createSubscriber(topic,msgSelector,true);
            else
                tSubscriber = tSession.createSubscriber(topic);
        }
    }

    public int tReopen() {
        int retCode = -1;
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
        if (brokerVersion != 0) {
            if (tConnection == null || tSession == null)
                return -1;
            try {
                tOpen();
                retCode = 0;
            }
            catch (JMSException e) {
                String str = tName + ": ";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex + "\n";
                new Event(Event.WARNING, str + Event.traceStack(e)).send();
                retCode = -1;
            }
        }
        else if (qConn == null) {
            return -1;
        }
        else if (qConn.qReopen() == 0) {
            qSender = qConn.getQueueSender();
            qReceiver = qConn.getQueueReceiver();
            retCode = 0;
        }
        else {
            retCode = -1;
        }

        if (retCode != 0) {
            try {
                tClose();
            }
            catch (JMSException e) {
            }
        }
        return retCode;
    }

    public void tClose() throws JMSException {
        if (tPublisher != null) {
            tPublisher.close();
            tPublisher = null;
        }
        if (tSubscriber != null) {
            tSubscriber.close();
            tSubscriber = null;
        }
        if (qConn != null)
            qConn.qClose();
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

    public String reconnect() {
        String str = tName + ": ";
        close();
        if (brokerVersion != 0) {
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
        }
        else if ((str = qConn.reconnect()) == null) {
            qSender = qConn.getQueueSender();
            qReceiver = qConn.getQueueReceiver();
        }
        else {
            return str;
        }
        return null;
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
        if (tSession != null) try {
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
        if (qConn != null) try {
            qConn.close();
        }
        catch (Exception e) {
        }
        if (tConnection != null) try {
            tConnection.close();
        }
        catch (Exception e) {
        }
        tConnection = null;
    }

    /**
     * It subscribes a JMS topic and sends the JMS messages into a output
     * channel that may be a JMS destination, an OutputStream or an XQueue.
     * It supports the message acknowledgement at the source.
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
        boolean clientXA = true;
        boolean isDaemon = (mode > 0);
        boolean isSleepy = (sleepTime > 0);
        String msgStr = null;
        String line = null;
        int count = 0;
        int type = 0;
        int sid = -1, cid;
        byte[] buffer = new byte[bufferSize];
        XQueue xq = null;
        int shift = partition[0];
        int len = partition[1];

        if (brokerVersion == 0) {
            qConn.get(out);
            return;
        }

        if (tSubscriber == null)
        throw(new JMSException("tSubscriber for "+tName+" is not initialized"));

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
            if ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0)
                clientXA = true;
            else
                clientXA = false;
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
                    if (!clientXA) // out has no EXTERNAL_XA set
                        inMessage.acknowledge();
                }
                else {
                    xq.cancel(sid);
                    new Event(Event.ERR, xq.getName() +
                        ": failed to add a msg from " + tName).send();
                    inMessage.acknowledge();
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
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (tm > System.currentTimeMillis());
                }
            }
        }
        else {
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
                    if (priority == -2) // use msg props
                        ((QueueSender) out).send(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(),
                            inMessage.getJMSExpiration());
                    else
                        ((QueueSender) out).send(inMessage);
                    break;
                  case TC_TOPIC:
                    if (priority == -2) // use msg props
                        ((TopicPublisher) out).publish(inMessage,
                            inMessage.getJMSDeliveryMode(),
                            inMessage.getJMSPriority(),
                            inMessage.getJMSExpiration());
                    else
                        ((TopicPublisher) out).publish(inMessage);
                    break;
                  case TC_STREAM:
                    if (msgStr.length() > 0)
                        ((OutputStream) out).write(msgStr.getBytes());
                    break;
                  default:
                    break;
                }
                if ((xaMode & XA_CLIENT) > 0)
                    inMessage.acknowledge();

                if (displayMask > 0) // display the message
                    new Event(Event.INFO, "sub a msg from " + tName +
                        " with (" + line + " )").send();
                count ++;

                if (maxNumberMsg > 0 && count >= maxNumberMsg) break;

                if (isSleepy) try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "sub " + count + " msgs from "+tName).send();
    }

    /**
     * It reads bytes from the InputStream and publish them into a JMS topic as
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
        int count = 0, retry = 0, len = 0, rawLen = 0, skip;
        DelimitedBuffer sBuf;
        byte[] buffer;
        byte[] rawBuf;
        byte[] psHeader = new byte[0];
        StringBuffer textBuffer = null;
        boolean isNewMsg = false;
        boolean isSleepy = (sleepTime > 0);

        if (brokerVersion == 0) {
            if (qSender == null)
            throw(new JMSException("qSender for "+tName+" is not initialized"));
            psHeader = psHeader_v2("publish", tName, propertyName,
                 propertyValue);
            outMessage = qConn.createMessage(0);
        }
        else {
            if (tPublisher == null)
         throw(new JMSException("tPublisher for "+tName+" is not initialized"));
            outMessage = createMessage(textMode);
            if (propertyName != null && propertyValue != null) {
                for (int i=0; i<propertyName.length; i++)
                    MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], outMessage);
            }
        }

        sBuf = new DelimitedBuffer(bufferSize, offhead, sotBytes,
            offtail, eotBytes);
        buffer = sBuf.getBuffer();

        if ((boundary & MS_SOT) == 0) { // SOT not defined
            isNewMsg = true;
            if (textMode > 0)
                textBuffer = new StringBuffer();
            else if (brokerVersion == 0)
                ((BytesMessage) outMessage).writeBytes(psHeader,
                    0, psHeader.length);
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
                                if (brokerVersion == 0)
                                    textBuffer.insert(0, new String(psHeader));
                                ((TextMessage) outMessage).setText(
                                    textBuffer.toString());
                                textBuffer = null;
                            }
                            if (totalBytes > 0 || count > 0) {
                                if (brokerVersion == 0)
                                    qSender.send(outMessage);
                                else
                                    tPublisher.publish(outMessage);
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
                        if (brokerVersion == 0 && textMode <= 0)
                            ((BytesMessage) outMessage).writeBytes(psHeader,
                                0, psHeader.length);

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
                        else {
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
                            len = 0;
                            totalBytes -= rawLen;
                            if (textMode > 0) {
                                if (brokerVersion == 0)
                                    textBuffer.insert(0, new String(psHeader));
                                ((TextMessage) outMessage).setText(
                                    textBuffer.toString());
                                textBuffer = null;
                            }
                            if (brokerVersion == 0)
                                qSender.send(outMessage);
                            else
                                tPublisher.publish(outMessage);
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
                            else if (brokerVersion == 0)
                                ((BytesMessage) outMessage).writeBytes(psHeader,
                                    0, psHeader.length);
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
                    if (brokerVersion == 0)
                        textBuffer.insert(0, new String(psHeader));
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(textBuffer.toString());
                }
                if (brokerVersion == 0)
                    qSender.send(outMessage);
                else
                    tPublisher.publish(outMessage);
                count ++;
                if (displayMask > 0)
                    new Event(Event.INFO, "pub a msg of " +
                        totalBytes + " bytes to " + tName).send();
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
     * It gets JMS messages from the XQueue and publishes them on a JMS topic. 
     * It supports both transaction in the delivery session and the
     * message acknowledgement at the source.
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
        long currentTime, idleTime;
        int count = 0, mask;
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

        n = 0;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (size > 0) {
                    ex = commit(xq, batch, size);
                    size = 0;
                    if (ex != null)
                        throw(ex);
                }
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

            message = (Message) xq.browse(sid);
            if (message == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                if (size > 0) {
                    ex = commit(xq, batch, size);
                    if (ex != null)
                       throw(new JMSException("failed to commit " + size +
                            xq.getName() + ": " + ex));
                }
                continue;
            }

            dt = null;
            try {
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt=Event.dateFormat(new Date(message.getJMSTimestamp()));
                id = message.getJMSMessageID();
            }
            catch (JMSException e) {
                id = null;
                new Event(Event.WARNING, "failed to get JMS property for "+
                    tName + ": " + Event.traceStack(e)).send();
            }

            try {
                if (count == 0 && msgID != null) { // for retries
                    if (! msgID.equals(id)) {
                        if (brokerVersion == 0)
                            publish_v0(tName, message);
                        else if (priority == -2) // use msg props
                            tPublisher.publish(message,
                                message.getJMSDeliveryMode(),
                                message.getJMSPriority(),
                                message.getJMSExpiration());
                        else
                            tPublisher.publish(message);
                    }
                }
                else {
                    if (brokerVersion == 0)
                        publish_v0(tName, message);
                    else if (priority == -2) // use msg props
                        tPublisher.publish(message,
                            message.getJMSDeliveryMode(),
                            message.getJMSPriority(),
                            message.getJMSExpiration());
                    else
                        tPublisher.publish(message);
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
                    new Event(Event.ERR, "failed to ack msg after pub on " +
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
        if (displayMask != 0)
            new Event(Event.INFO, "pub " + count + " msgs to " + tName).send();
    }

    private JMSException commit(XQueue xq, int[] batch, int size){
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

    private void rollback(XQueue xq, int[] batch, int size) {
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

    public TopicPublisher getTopicPublisher() {
        return tPublisher;
    }

    public TopicSubscriber getTopicSubscriber() {
        return tSubscriber;
    }

    /** Initializes the JMS components to support MQSI V2 PUB/SUB
     * by using point-to-point method and returns QueueConnector
     * for use later in the application
     */
    private void connect_v0() throws JMSException {
        Map<String, Object> props = new HashMap<String, Object>();
        Message inMessage;
        int isSubscribed;
        int isMultipleQueue = 0;

        props.put("HostName", hostName);
        props.put("QueueManager", qmgrName);
        props.put("ChannelManager", channelName);
        props.put("Port", String.valueOf(port));
        props.put("TargetClient", "1");
        props.put("Persistence", String.valueOf(persistence));
        props.put("Expiry", String.valueOf(expiry));
        props.put("Priority", String.valueOf(priority));
        props.put("TextMode", String.valueOf(textMode));
        props.put("Operation", "put");
        if ("pub".equals(operation)) {
            if (qName != null)
                props.put("QueueName", qName);
            else
                props.put("QueueName", "SYSTEM.BROKER.DEFAULT.STREAM");
        }
        else { // for sub, RegSub and DeregSub
            props.put("QueueName", brokerControlQueue);
        }

        qConn = new QConnector(props);
        qSender = qConn.getQueueSender();

        if ("pub".equals(operation)) {
            if (correlationID != null)
                msgID = correlationID;
            return;
        }
        else if (subQmgr != null) { // directly connect to real broker
            if (qName.endsWith("*"))
                isMultipleQueue = 1;
            if (operation.equals("RegSub")) {
                if (msgID == null && correlationID != null) {
                    msgID = correlationID;
                }
                put_v2("RegSub", tName, null, null, 0);
                qSender.close();
                new Event(Event.INFO, "topic: " + tName + " registeded").send();
            }
            else if (operation.equals("DeregSub")) {
                if (msgID == null && correlationID != null) {
                    msgID = correlationID;
                }
                put_v2("DeregSub", tName, null, null, 0);
                qSender.close();
                new Event(Event.INFO, "topic: "+tName+ " deregisteded").send();
            }
            subQmgr = null;
            return;
        }

        // only regular sub, RegSub or DeregSub reach here
        isSubscribed = check_subscription();

        if (isSubscribed == 0) { // subscribe the topic
            if (operation.equals("DeregSub")) {
                if (msgID == null && correlationID != null) {
                    msgID = correlationID;
                }
                put_v2("DeregSub", tName, null, null, 0);
                qSender.close();
                new Event(Event.INFO, "topic: "+tName+ " deregisteded").send();
                return;
            }
            add_JMS_ADMIN_entry_v2();
            put_v2("RegSub", tName, null, null, 0);
            new Event(Event.INFO, "topic: " + tName + " subscribed").send();
            qSender.close();
        }
        else if (isSubscribed < 0) {
            // topic or subQ differs from the existing ones
            throw(new MessageFormatException("topic or subQ differs from" +
                "the existing ones"));
        }
        else if (operation.equals("DeregSub")) { // subscribed
            put_v2("DeregSub", tName, null, null, 0);
            qSender.close();
            String q = "SYSTEM.JMS.ADMIN.QUEUE";
            qReceiver = qConn.getQueueReceiver(q, "JMSMessageID = '"+msgID+"'");
            inMessage = qReceiver.receive(3000);
            qReceiver.close();
            return;
        }
        else if (operation.equals("RegSub")) { // already subscribed
            new Event(Event.INFO, "subscription: " +tName+ " confirmed").send();
            return;
        }
        else { // already subscribed, no need to do anything here
            new Event(Event.INFO, "subscription: " +tName+ " confirmed").send();
        }
        qConn.close();

        if (isMultipleQueue == 0) {
            if (msgSelector != null) {
                msgSelector += " AND JMSCorrelationID = '" + msgID + "'";
            }
            else {
                msgSelector = "JMSCorrelationID = '" + msgID + "'";
            }
        }

        props.put("MessageSelector", msgSelector);
        props.put("QueueName", qName);
        props.put("Operation", "get");
        props.put("XAMode", String.valueOf(xaMode));
        props.put("DisplayMask", String.valueOf(displayMask));
        if (mode != 0)
            props.put("Mode", "daemon");
        qConn = new QConnector(props);
        qReceiver = qConn.getQueueReceiver();
    }

    /**  check the subscription existence by browsing SYSTEM.JMS.ADMIN.QUEUE
     *  It returns 1 for subscribed, 0 for not subscribed and -1 for subscibed
     *  but not the same.  In case of existence, it also set msgID.
     */
    public int check_subscription() throws JMSException {
        Message inMessage;
        StringBuffer strBuf;
        String originalTopicName;
        String originalSubQName;
        String originalFilter;
        int isSubscribed = 0;
        int bytesRead = 0;
        int count = 0;
        byte[] buffer = new byte[bufferSize];
        String id_string = clientID + ":" + subscriptionID;
        String q = "queue:///SYSTEM.JMS.ADMIN.QUEUE?targetClient=1";
        QueueBrowser admQBrowser = qConn.getQueueBrowser(q, null);
        Enumeration msgQ = admQBrowser.getEnumeration();
        String header_str = "MQJMS_PS_ADMIN_ENTRY";
        String header_v2_str = "MQJMS_PS_SUBENTRY_v2";

        msgID = null;
        while (msgQ.hasMoreElements()) {
            count ++;
            inMessage = (Message) msgQ.nextElement();

            if (inMessage instanceof TextMessage) {
                strBuf = new StringBuffer(((TextMessage) inMessage).getText());
            }
            else if (inMessage instanceof BytesMessage) {
                int bufSize;
                strBuf = new StringBuffer();
                bufSize = header_str.length();
                bytesRead=((BytesMessage) inMessage).readBytes(buffer, bufSize);
                String header = new String(buffer, 0, bytesRead);
                if ((!header_str.equals(header)) &&
                    (!header_v2_str.equals(header))) {
                    new Event(Event.WARNING, tName +
                        ": unknown message format").send();
                    continue;
                }

                // get length of the id field
                bufSize = ((BytesMessage) inMessage).readInt();
                bytesRead=((BytesMessage) inMessage).readBytes(buffer, bufSize);
                if (id_string.equals(new String(buffer, 0, bytesRead))) {
                    int i;
                    new Event(Event.INFO, tName+": found the same ids").send();
                    // get length of the topic field
                    bufSize = ((BytesMessage) inMessage).readInt();
                 bytesRead=((BytesMessage) inMessage).readBytes(buffer,bufSize);
                    originalTopicName = new String(buffer, 0, bytesRead);
                    // get length of the sub queue field
                    bufSize = ((BytesMessage) inMessage).readInt();
                 bytesRead=((BytesMessage) inMessage).readBytes(buffer,bufSize);
                    while (bytesRead > 0) {
                        if (buffer[bytesRead - 1] == (byte) ' ') bytesRead --;
                        else break;
                    }
                    originalSubQName = new String(buffer, 0, bytesRead);
                    // get length of the filter field
                    bufSize = ((BytesMessage) inMessage).readInt();
                    if (bufSize > 0) {
                 bytesRead=((BytesMessage) inMessage).readBytes(buffer,bufSize);
                        originalFilter = new String(buffer, 0, bytesRead);
                    }
                    msgID = inMessage.getJMSMessageID();
                    bytesRead =((BytesMessage) inMessage).readBytes(buffer,2);
                    if (header_v2_str.equals(header)) {
                       bytesRead=((BytesMessage) inMessage).readBytes(buffer,4);
                        subLock = buffer[3];
                    }
                    if (qName.endsWith("*")) {
                        if (tName.equals(originalTopicName)) {
                            new Event(Event.INFO, tName +
                                ": found the same topic").send();
                            qName = originalSubQName;
                            isSubscribed = 1;
                        }
                        else {
                            isSubscribed = -1;
                            new Event(Event.INFO, tName +
                                ": topic is different").send();
                        }
                    }
                    else {
                        if (tName.equals(originalTopicName) &&
                            qName.equals(originalSubQName)) {
                            new Event(Event.INFO, tName +
                                ": found the same topic and queue").send();
                            isSubscribed = 1;
                        }
                        else {
                            isSubscribed = -1;
                            new Event(Event.INFO, tName +
                                ": topic or queue is different").send();
                        }
                    }
                    break;
                }
                else {
                    continue;
                }
            }
            else {
                new Event(Event.INFO, tName +
                    ": unknown message type").send();
            }
        }
        // close browser first
        admQBrowser.close();
        return isSubscribed;
    }

    /**  To support MQSI V2 broker, this method uses qSender to put the message
     *   with the bare V2 header that contains MQSI V2 PUB command.  Therefore,
     *   there is no need to set up a convert flow for publish since the header
     *   is already in RFH2.
     */
    public void publish_v0(String topicName, Message msg) throws JMSException {
        BytesMessage pubMessage;
        int totalBytes = 0;
        int bytesRead = 0;
        byte[] buffer = new byte[bufferSize];
        StringBuffer psc = new StringBuffer("<psc><Command>Publish</Command>");
        StringBuffer mcd = new StringBuffer("<mcd><Msd>jms_text</Msd></mcd>  ");
        StringBuffer jms = new StringBuffer("<jms><Dst>topic://");
        StringBuffer usr = new StringBuffer("<usr>");
        StringBuffer map = new StringBuffer("<map>");
        char[] paddings = {' ', ' ', ' ', ' '};
        String format = "MQSTR   ";
        String header_id = "RFH ";

        // psc folder
        psc.append("<PubOpt>");
        if (pubOption != null) {
            psc.append(pubOption);
        }
        else {
            psc.append("None");
        }
        psc.append("</PubOpt>");

        // jms folder
        jms.append(tName);
        jms.append("</Dst><Tms>");
        jms.append(String.valueOf(msg.getJMSTimestamp()));
        jms.append("</Tms>");
        if (persistence != 0) {
            jms.append("<Dlv>");
            jms.append(String.valueOf(persistence));
            jms.append("</Dlv>");
        }
        jms.append("</jms>");
        bytesRead = jms.length() % 4;
        if (bytesRead > 0) {
            jms.append(paddings, 0, 4 - bytesRead);
        }

        // usr folder
        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            String name, value;
            StringBuffer strBuf = new StringBuffer();

            name = (String) propNames.nextElement();
            if (name.equals("null")) continue;
            value = msg.getStringProperty(name);
            usr.append("<" + name + ">");
            usr.append(value);
            usr.append("</" + name + ">");
        }
        if (usr.length() > 5) {
            usr.append("</usr>");
            bytesRead = usr.length() % 4;
            if (bytesRead > 0) {
                usr.append(paddings, 0, 4 - bytesRead);
            }
        }
        psc.append("<Topic>" + topicName + "</Topic>");
        psc.append("</psc>");
        bytesRead = psc.length() % 4;
        if (bytesRead > 0) {
            psc.append(paddings, 0, 4 - bytesRead);
        }

        pubMessage = (BytesMessage) qConn.createMessage(0);

        bytesRead = header_id.length();
        pubMessage.writeBytes(header_id.getBytes(), 0, bytesRead);

        pubMessage.writeInt(2);
        totalBytes = psc.length() + mcd.length() + 44;
        totalBytes += jms.length() + 4;
        if (usr.length() > 5)
            totalBytes += usr.length() + 4;
        pubMessage.writeInt(totalBytes);
        pubMessage.writeInt(encoding);
        pubMessage.writeInt(charSetID);
        pubMessage.writeBytes(format.getBytes(), 0, 8);
        pubMessage.writeInt(0);
        pubMessage.writeInt(nameValueCCSID);

        bytesRead = mcd.length();
        pubMessage.writeInt(bytesRead);
        pubMessage.writeBytes(mcd.toString().getBytes(), 0, bytesRead);

        bytesRead = psc.length();
        pubMessage.writeInt(bytesRead);
        pubMessage.writeBytes(psc.toString().getBytes(), 0, bytesRead);

        String name, value;
        StringBuffer strBuf = new StringBuffer();

        bytesRead = jms.length();
        pubMessage.writeInt(bytesRead);
        pubMessage.writeBytes(jms.toString().getBytes(), 0, bytesRead);

        if (usr.length() > 5) {
            bytesRead = usr.length();
            pubMessage.writeInt(bytesRead);
            pubMessage.writeBytes(usr.toString().getBytes(), 0, bytesRead);
        }

        if (msg instanceof TextMessage) {
            bytesRead = ((TextMessage) msg).getText().length();
            pubMessage.writeBytes(((TextMessage) msg).getText().getBytes(),
                0, bytesRead);
        }
        else if (msg instanceof BytesMessage) {
            totalBytes = 0;
            while ((bytesRead =
                ((BytesMessage) msg).readBytes(buffer, bufferSize)) >= 0) {
                if (bytesRead > 0)
                    pubMessage.writeBytes(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
        }
        else if (msg instanceof MapMessage) {
            Enumeration mapNames;
            mapNames = ((MapMessage) msg).getMapNames();

            while (mapNames.hasMoreElements()) {
                name = (String) mapNames.nextElement();
                if (name.equals("null")) continue;
                value = ((MapMessage) msg).getString(name);
                map.append("<" + name + ">");
                map.append(value);
                map.append("</" + name + ">");
            }
            if (map.length() > 5) {
                map.append("</map>");
                bytesRead = map.length() % 4;
                if (bytesRead > 0) {
                    map.append(paddings, 0, 4 - bytesRead);
                }
                bytesRead = map.length();
                pubMessage.writeInt(bytesRead);
                pubMessage.writeBytes(map.toString().getBytes(), 0, bytesRead);
            }
        }
        pubMessage.setStringProperty("JMS_IBM_Format", "MQHRF2");

        if (priority == -2) // use msg props
            qSender.send(pubMessage, msg.getJMSDeliveryMode(),
                msg.getJMSPriority(), msg.getJMSExpiration());
        else
            qSender.send(pubMessage); //send the message
    }

    private byte[] psHeader_v2(String psCommand, String topicName,
        String[] pName, String[] pValue) throws JMSException {
        int totalBytes = 0;
        int bytesRead = 0;
        BytesMessage rawMsg = new BytesEvent();
        byte[] buffer;
        StringBuffer psc = new StringBuffer("<psc><Command>" + psCommand +
            "</Command>");
        StringBuffer mcd = new StringBuffer("<mcd><Msd>jms_text</Msd></mcd>  ");
        StringBuffer jms = new StringBuffer("<jms><Dst>topic://");
        StringBuffer map = new StringBuffer("<map>");
        StringBuffer usr = null;
        char [] paddings = {' ', ' ', ' ', ' '};
        String format = "MQSTR   ";
        String header_id = "RFH ";

        if ("Publish".equals(psCommand)) {
            // psc folder
            psc.append("<PubOpt>");
            if (pubOption != null) {
                psc.append(pubOption);
            }
            else {
                psc.append("None");
            }
            psc.append("</PubOpt>");

            // jms folder
            jms.append(tName);
            jms.append("</Dst><Tms>");
            jms.append(String.valueOf(System.currentTimeMillis()));
            jms.append("</Tms>");
            if (persistence != 0) {
                jms.append("<Dlv>");
                jms.append(String.valueOf(persistence));
                jms.append("</Dlv>");
            }
            jms.append("</jms>");

            bytesRead = jms.length() % 4;
            if (bytesRead > 0) {
                jms.append(paddings, 0, 4 - bytesRead);
            }

            // usr folder
            if (pName != null && pValue != null) {
                usr = new StringBuffer("<usr>");
                for (int i=0; i<pName.length; i++) {
                    usr.append("<" + pName[i] +">");
                    usr.append(pValue[i]);
                    usr.append("</" + pName[i] +">");
                }
                if (usr.length() > 5)
                    usr.append("</usr>");
            }

            if (usr != null && usr.length() > 5) {
                bytesRead = usr.length() % 4;
                if (bytesRead > 0) {
                    usr.append(paddings, 0, 4 - bytesRead);
                }
            }
        }
        else {
            if (regOption != 0 && ("RegSub".equals(psCommand) ||
                "DeregSub".equals(psCommand))) {
                psc.append("<RegOpt>" + regOption + "</RegOpt>");
            }
            if (contentFilter != null) {
                psc.append("<Filter>" + contentFilter + "</Filter>");
            }
            if (subQmgr != null)
                psc.append("<QMgrName>" + subQmgr + "</QMgrName>");
            psc.append("<QName>" + qName + "</QName>");
        }
        psc.append("<Topic>" + topicName + "</Topic>");
        psc.append("</psc>");
        bytesRead = psc.length() % 4;
        if (bytesRead > 0) {
            psc.append(paddings, 0, 4 - bytesRead);
        }

        rawMsg.clearBody();
        bytesRead = header_id.length();
        rawMsg.writeBytes(header_id.getBytes(), 0, bytesRead);

        rawMsg.writeInt(2);
        totalBytes = psc.length() + mcd.length() + 44;
        if (psCommand.equals("Publish")) {
            totalBytes += jms.length() + 4;
            if (usr != null && usr.length() > 5)
                totalBytes += usr.length() + 4;
        }
        rawMsg.writeInt(totalBytes);
        rawMsg.writeInt(encoding);
        rawMsg.writeInt(charSetID);
        rawMsg.writeBytes(format.getBytes(), 0, 8);
        rawMsg.writeInt(0);
        rawMsg.writeInt(nameValueCCSID);

        bytesRead = mcd.length();
        rawMsg.writeInt(bytesRead);
        rawMsg.writeBytes(mcd.toString().getBytes(), 0, bytesRead);

        bytesRead = psc.length();
        rawMsg.writeInt(bytesRead);
        rawMsg.writeBytes(psc.toString().getBytes(), 0, bytesRead);

        if ("Publish".equals(psCommand)) {
            String name, value;
            StringBuffer strBuf = new StringBuffer();

            bytesRead = jms.length();
            rawMsg.writeInt(bytesRead);
            rawMsg.writeBytes(jms.toString().getBytes(), 0, bytesRead);

            if (usr != null && usr.length() > 5) {
                bytesRead = usr.length();
                rawMsg.writeInt(bytesRead);
                rawMsg.writeBytes(usr.toString().getBytes(), 0, bytesRead);
            }
        }

        rawMsg.reset();
        buffer = new byte[totalBytes];
        while ((bytesRead = rawMsg.readBytes(buffer, totalBytes)) > 0) {
            totalBytes -= bytesRead;
            if (totalBytes <= 0)
                break;
        }
        return buffer;
    }

    /**
     * To support MQSI V2 broker, this method uses qSender to put the message
     * with the bare V2 header that contains MQSI V2 Pub/Sub command.
     * Therefore, there is no need to set up a convert flow for publish
     * since the header is already in RFH2.
     */
    private void put_v2(String psCommand, String topicName, StringBuffer usr,
        byte[] msgBody, int length) throws JMSException {
        BytesMessage pubMessage;
        int totalBytes = 0;
        int bytesRead = 0;
        StringBuffer mcd;
        StringBuffer psc = new StringBuffer("<psc><Command>" + psCommand +
            "</Command>");
        StringBuffer jms = new StringBuffer("<jms><Dst>topic://");
        StringBuffer map = new StringBuffer("<map>");
        char [] paddings = {' ', ' ', ' ', ' '};
        String format = "MQSTR   ";
        String header_id = "RFH ";

        if (textMode > 0)
            mcd = new StringBuffer("<mcd><Msd>jms_text</Msd></mcd>  ");
        else
            mcd = new StringBuffer("<mcd><Msd>jms_bytes</Msd></mcd> ");

        if ("Publish".equals(psCommand)) {
            // psc folder
            psc.append("<PubOpt>");
            if (pubOption != null) {
                psc.append(pubOption);
            }
            else {
                psc.append("None");
            }
            psc.append("</PubOpt>");

            // jms folder
            jms.append(tName);
            jms.append("</Dst><Tms>");
            jms.append(String.valueOf(System.currentTimeMillis()));
            jms.append("</Tms>");
            if (persistence != 0) {
                jms.append("<Dlv>");
                jms.append(String.valueOf(persistence));
                jms.append("</Dlv>");
            }
            jms.append("</jms>");

            bytesRead = jms.length() % 4;
            if (bytesRead > 0) {
                jms.append(paddings, 0, 4 - bytesRead);
            }

            // usr folder
            if (usr != null && usr.length() > 5) {
                bytesRead = usr.length() % 4;
                if (bytesRead > 0) {
                    usr.append(paddings, 0, 4 - bytesRead);
                }
            }
        }
        else {
            if (regOption != 0 && ("RegSub".equals(psCommand) ||
                "DeregSub".equals(psCommand))) {
                psc.append("<RegOpt>" + regOption + "</RegOpt>");
            }
            if (contentFilter != null) {
                psc.append("<Filter>" + contentFilter + "</Filter>");
            }
            if (subQmgr != null)
                psc.append("<QMgrName>" + subQmgr + "</QMgrName>");
            psc.append("<QName>" + qName + "</QName>");
        }
        psc.append("<Topic>" + topicName + "</Topic>");
        psc.append("</psc>");
        bytesRead = psc.length() % 4;
        if (bytesRead > 0) {
            psc.append(paddings, 0, 4 - bytesRead);
        }

        pubMessage = (BytesMessage) qConn.createMessage(0);

        bytesRead = header_id.length();
        pubMessage.writeBytes(header_id.getBytes(), 0, bytesRead);

        pubMessage.writeInt(2);
        totalBytes = psc.length() + mcd.length() + 44;
        if (psCommand.equals("Publish")) {
            totalBytes += jms.length() + 4;
            if (usr != null && usr.length() > 5)
                totalBytes += usr.length() + 4;
        }
        pubMessage.writeInt(totalBytes);
        pubMessage.writeInt(encoding);
        pubMessage.writeInt(charSetID);
        pubMessage.writeBytes(format.getBytes(), 0, 8);
        pubMessage.writeInt(0);
        pubMessage.writeInt(nameValueCCSID);

        bytesRead = mcd.length();
        pubMessage.writeInt(bytesRead);
        pubMessage.writeBytes(mcd.toString().getBytes(), 0, bytesRead);

        bytesRead = psc.length();
        pubMessage.writeInt(bytesRead);
        pubMessage.writeBytes(psc.toString().getBytes(), 0, bytesRead);

        if ("Publish".equals(psCommand)) {
            String name, value;
            StringBuffer strBuf = new StringBuffer();

            bytesRead = jms.length();
            pubMessage.writeInt(bytesRead);
            pubMessage.writeBytes(jms.toString().getBytes(), 0, bytesRead);

            if (usr != null && usr.length() > 5) {
                bytesRead = usr.length();
                pubMessage.writeInt(bytesRead);
                pubMessage.writeBytes(usr.toString().getBytes(), 0, bytesRead);
            }

            if (msgBody != null && length > 0)
                pubMessage.writeBytes(msgBody, 0, length);
        }
        pubMessage.setStringProperty("JMS_IBM_Format", "MQHRF2");
        if (msgID != null && !"Publish".equals(psCommand)) {
            pubMessage.setJMSCorrelationID(msgID);
        }

        qSender.send(pubMessage); //send the message
    }

    private void add_JMS_ADMIN_entry() throws JMSException {
        int totalBytes = 0;
        int bytesRead = 0;
        String header_str = "MQJMS_PS_ADMIN_ENTRY";
        byte[] footer = {0, 0, 0, 0, 0, 0x6E};
        byte[] paddings = {0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                           0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                           0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                           0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                           0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                           0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20};
        String q = "queue:///SYSTEM.JMS.ADMIN.QUEUE?targetClient=1";
        QueueSender admQSender = qConn.getQueueSender(q);
        BytesMessage admMessage = (BytesMessage) qConn.createMessage(0);

        bytesRead = header_str.length();
        admMessage.writeBytes(header_str.getBytes(), 0, bytesRead);
        totalBytes += bytesRead;

        bytesRead = clientID.length() + subscriptionID.length() + 1;
        admMessage.writeInt(bytesRead);
        admMessage.writeBytes(clientID.getBytes(), 0,
            clientID.length());
        admMessage.writeByte((byte) ':');
        admMessage.writeBytes(subscriptionID.getBytes(), 0,
            subscriptionID.length());
        totalBytes += bytesRead;

        bytesRead = tName.length();
        admMessage.writeInt(bytesRead);
        admMessage.writeBytes(tName.getBytes(), 0, bytesRead);
        totalBytes += bytesRead;

        bytesRead = qName.length();
        totalBytes += bytesRead;
        if (bytesRead < 48) {
            admMessage.writeInt(48);
            admMessage.writeBytes(qName.getBytes(), 0, bytesRead);
            bytesRead = 48 - bytesRead;
            admMessage.writeBytes(paddings, 0, bytesRead);
            totalBytes += bytesRead;
        }
        else {
            admMessage.writeInt(bytesRead);
            admMessage.writeBytes(qName.getBytes(), 0, bytesRead);
        }
        bytesRead = 6;
        admMessage.writeBytes(footer, 0, bytesRead);
        totalBytes += bytesRead;

        admQSender.send(admMessage); //send the message
        msgID = admMessage.getJMSMessageID();
        admQSender.close();
    }

    private void add_JMS_ADMIN_entry_v2() throws JMSException {
        int totalBytes = 0;
        int bytesRead = 0;
        String header_str = "MQJMS_PS_SUBENTRY_v2";
        byte[] footer = {0, 0x6E, 0, 0x79, 0, 0x69};
        byte[] footer1 = {0, 0x79, 0, 0x79, 0, 0x75};
        String q = "queue:///SYSTEM.JMS.ADMIN.QUEUE?targetClient=1";
        QueueSender admQSender = qConn.getQueueSender(q);
        BytesMessage admMessage = (BytesMessage) qConn.createMessage(0);

        admMessage.clearBody();

        bytesRead = header_str.length();
        admMessage.writeBytes(header_str.getBytes(), 0, bytesRead);
        totalBytes += bytesRead;

        bytesRead = clientID.length() + subscriptionID.length() + 1;
        admMessage.writeInt(bytesRead);
        admMessage.writeBytes(clientID.getBytes(), 0,
            clientID.length());
        admMessage.writeByte((byte) ':');
        admMessage.writeBytes(subscriptionID.getBytes(), 0,
            subscriptionID.length());
        totalBytes += bytesRead;

        bytesRead = tName.length();
        admMessage.writeInt(bytesRead);
        admMessage.writeBytes(tName.getBytes(), 0, bytesRead);
        totalBytes += bytesRead;

        bytesRead = qName.length();
        totalBytes += bytesRead;
        admMessage.writeInt(bytesRead);
        admMessage.writeBytes(qName.getBytes(), 0, bytesRead);
        if (contentFilter != null && contentFilter.length() > 0) {
            bytesRead = contentFilter.length();
            admMessage.writeInt(bytesRead);
            totalBytes += bytesRead;
            bytesRead = footer1.length;
            admMessage.writeBytes(footer1, 0, bytesRead);
        }
        else {
            admMessage.writeInt(0);
            totalBytes += 4;
            bytesRead = footer.length;
            admMessage.writeBytes(footer, 0, bytesRead);
        }
        totalBytes += bytesRead;

        admQSender.send(admMessage); //send the message
        msgID = admMessage.getJMSMessageID();
        admQSender.close();
    }
}
