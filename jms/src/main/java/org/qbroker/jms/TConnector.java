package org.qbroker.jms;

/* TConnector.java - a connector for generic JMS topics */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.InvalidDestinationException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.jms.JMSTConnector;
import org.qbroker.event.Event;

/**
 * TConnector connects to a JMS topic and initializes one of the operations,
 * such as sub, pub, DeregSub and RegSub.
 *<br/><br/>
 * For durable subscriptions, you have to specify SubscriptionID.  On the other
 * hand, if SubscriptionID is defined, it is assumed to be durable subscription.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TConnector extends JMSTConnector {
    private SimpleDateFormat dateFormat = null;
    private Hashtable<String, String> env;

    /** Creates new TConnector */
    public TConnector(Map props) throws JMSException {
        Object o;
        String str, uri;
        boolean ignoreInitFailure = false;
        env = new Hashtable<String, String>();

        Template template = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
        if ((o = props.get("ContextFactory")) != null) {
            str = (String) o;
            env.put(Context.INITIAL_CONTEXT_FACTORY, str);
        }
        else
           throw(new IllegalArgumentException("ContextFactory is not defined"));

        if ((o = props.get("URI")) != null)
            uri = (String) o;
        else
            throw(new IllegalArgumentException("URI is not defined"));

        env.put(Context.PROVIDER_URL, uri);

        if("com.sun.enterprise.naming.SerialInitContextFactory".equals(str))try{
            int port;
            URI u = new URI(uri);
            if ("iiop".equals(u.getScheme())) { // hack for Glassfish V3
                str = u.getHost();
                port = u.getPort();
                if (str == null || str.length() <= 0)
                    str = "localhost";
                env.put("org.omg.CORBA.ORBInitialHost", str);
                System.setProperty("org.omg.CORBA.ORBInitialHost", str);
                str = (port > 0) ? String.valueOf(port) : "3700";
                env.put("org.omg.CORBA.ORBInitialPort", str);
                System.setProperty("org.omg.CORBA.ORBInitialPort", str);
            }
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        // for URL PKG support
        if ((o = props.get("URLPkgs")) != null)
            env.put(Context.URL_PKG_PREFIXES, (String) o);

        // for STATE FACTORIES support
        if ((o = props.get("StateFactories")) != null)
            env.put(Context.STATE_FACTORIES, (String) o);

        if ((o = props.get("Principal")) != null)
            env.put(Context.SECURITY_PRINCIPAL, (String) o);
        if ((o = props.get("Credentials")) != null)
            env.put(Context.SECURITY_CREDENTIALS, (String) o);

        if ((o = props.get("Username")) != null)
            username = (String) o;
        if ((o = props.get("Password")) != null)
            password = (String) o;

        connectionFactoryName = (String) props.get("ConnectionFactoryName");
        if(connectionFactoryName == null || connectionFactoryName.length() <= 0)
            throw(new IllegalArgumentException("TCF name not well defined"));
        tName = (String) props.get("TopicName"); // JNDI name for the topic
        if(tName == null || tName.length() <= 0)
            throw(new IllegalArgumentException("T name not well defined"));
        msgSelector = (String) props.get("MessageSelector");

        if ((o = props.get("IsPhysical")) != null && "true".equals((String) o))
            isPhysical = true;
        else
            isPhysical = false;

        if ((o = props.get("CellID")) != null)
            cellID = Integer.parseInt((String) o);

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
                n ++;
            }
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
        if (!operation.equals("pub")) {
            o = props.get("ClientID");
            clientID = template.substitute("CellID",
                String.valueOf(cellID), (String) o);
            o = props.get("SubscriptionID");
            subscriptionID = template.substitute("CellID",
                String.valueOf(cellID), (String) o);
            if (subscriptionID != null)
                isDurable = true;
        }
        else { // for pub only
            if ((o = props.get("WithFlowControlDisabled")) != null &&
                "true".equalsIgnoreCase((String) o)) // for SonicMQ only
                withFlowControlDisabled = true;
        }

        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("BatchSize")) != null)
            batchSize = Integer.parseInt((String) o);
        if (batchSize <= 0)
            batchSize = 1;
        if ((o = props.get("Persistence")) != null) {
            persistence = Integer.parseInt((String) o);
            if (persistence == 1)
                persistence = DeliveryMode.NON_PERSISTENT;
            else if (persistence == 2)
                persistence = DeliveryMode.PERSISTENT;
            overwrite += 1;
        }
        if ((o = props.get("Expiry")) != null) {
            expiry = Long.parseLong((String) o);
            overwrite += 4;
        }
        if ((o = props.get("Priority")) != null) {
            priority = Integer.parseInt((String) o);
            if (priority == -2)
                overwrite = -1;
            else
                overwrite += 2;
        }
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
            connect();
        }
        catch (JMSException e) {
            str =  tName + ": ";
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
    protected TopicConnectionFactory initialize() throws JMSException {
        Context ctx;
        TopicConnectionFactory factory;
        try {
            ctx = new InitialContext(env);
        }
        catch (NamingException e) {
            ctx = null;
            throw(new JMSException("failed to get ctx: " + e));
        }

        try {
            factory = (TopicConnectionFactory)ctx.lookup(connectionFactoryName);
        }
        catch (NamingException e) {
            try {
                Thread.sleep(500L);
            }
            catch (Exception ex) {
            }
            try { // retry
                ctx = null;
                ctx = new InitialContext(env);
              factory=(TopicConnectionFactory)ctx.lookup(connectionFactoryName);
            }
            catch (NamingException ex) {
                factory = null;
                throw(new JMSException(
                    "failed to lookup TopicConnnectionFactory '"+
                    connectionFactoryName + "': " + e.toString()));
            }
        }

        if (!isPhysical) try {
            topic = (Topic) ctx.lookup(tName);
        }
        catch (NamingException e) {
            try {
                Thread.sleep(500L);
            }
            catch (Exception ex) {
            }
            try { // retry
                topic = (Topic) ctx.lookup(tName);
            }
            catch (NamingException ex) {
                topic = null;
                throw(new JMSException("failed to lookup topic '" + tName +
                    "': " + e.toString()));
            }
        }

        return factory;
    }
}
