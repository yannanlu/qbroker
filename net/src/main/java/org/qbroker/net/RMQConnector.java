package org.qbroker.net;

/* RMQConnector.java - a connector to a RabbitMQ queue */

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeoutException;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import org.qbroker.common.TraceStackThread;
import org.qbroker.common.Connector;
import org.qbroker.common.Utils;

/**
 * RMQConnector connects to an exchange on a RabbitMQ server and initializes
 * the channel, the queue and the bindings for operations, such as check, get
 * and pub.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RMQConnector implements Connector {
    protected String username = null;
    protected String password = null;
    protected String operation = "check";
    protected boolean isDurable = false;
    protected boolean isAuto = false;
    protected boolean isPassive = true;
    public static final int RMQ_DIRECT = 0;
    public static final int RMQ_FANOUT = 1;
    public static final int RMQ_TOPIC = 2;
    public static final int RMQ_HEADERS = 3;

    private Connection conn = null;
    private Channel channel = null;
    private QueueingConsumer consumer = null;
    private String uri;
    private String exchangeName = "";
    private String routingKey = "";
    private String qName = "";
    private String dest = null;
    private String hostName;
    private String virtualHost = "/";
    private int port = 5672;
    private int type = RMQ_DIRECT;
    private String[] keyList = null;
    private boolean isConnected = false;

    /** Creates new RMQConnector */
    public RMQConnector(Map props) {
        Object o;
        String str;

        hostName = (String) props.get("HostName");
        uri = (String) props.get("URI");
        if (hostName != null && hostName.length() > 0) {
            uri = "amqp://" + hostName;
            if ((o = props.get("Port")) != null)
                port = Integer.parseInt((String) o);
            uri += ":" + port;
            if ((o = props.get("VirtualHost")) != null)
                uri += (String) o;
            else
                uri += "/";
        }
        else try {
            URI u = new URI(uri);
            hostName = u.getHost();
            if (hostName == null || hostName.length() <= 0)
                hostName = "localhost";
            if (u.getPort() > 0)
                port = u.getPort();
            str = u.getPath();
            if (str != null && str.length() > 0)
                virtualHost = str;
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(uri + ": " + e.toString()));
        }

        if ((o = props.get("VirtualHost")) != null)
            virtualHost = (String) o;
        if ((o = props.get("QueueName")) != null)
            qName = (String) o;
        if ((o = props.get("ExchangeName")) != null)
            exchangeName = (String) o;
        if ((o = props.get("RoutingKey")) != null) {
            if (o instanceof ArrayList) {
                int i, k, n;
                ArrayList list = (ArrayList) o;
                k = 0;
                n = list.size();
                for (i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    if ("".equals((String) o))
                        continue;
                    k ++;
                }
                keyList = new String[k];
                for (i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    if ("".equals((String) o))
                        continue;
                    keyList[k ++] = (String) o;
                }
                if (k > 0)
                    routingKey = keyList[0];
            }
            else
                routingKey = (String) o;
        }
        if ((o = props.get("ExchangeType")) != null) {
            str = (String) o;
            if ("direct".equals(str))
                type = RMQ_DIRECT;
            else if ("fanout".equals(str))
                type = RMQ_FANOUT;
            else if ("topic".equals(str))
                type = RMQ_TOPIC;
            else if ("headers".equals(str))
                type = RMQ_HEADERS;
            else
                throw(new IllegalArgumentException(uri +
                    "Type not supported: "+str));
        }

        if ((o = props.get("IsDurable")) != null &&
            "true".equalsIgnoreCase((String) o))
            isDurable = true;
        if ((o = props.get("AudoDelete")) != null &&
            "true".equalsIgnoreCase((String) o))
            isAuto = true;
        if ((o = props.get("IsPassive")) != null &&
            "false".equalsIgnoreCase((String) o))
            isPassive = false;
        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
        }

        if ((o = props.get("Operation")) != null)
            operation = (String) o;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) try {
            connect();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri + ": " +
                TraceStackThread.traceStack(e)));
        }
    }

    /** Initializes the ConnectionFactory and sets the basic parameters */
    private ConnectionFactory initialize() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(username);
        factory.setUsername(password);
        factory.setVirtualHost(virtualHost);
        factory.setHost(hostName);
        factory.setPort(port);

        // return the factory
        return factory;
    }

    private void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = initialize();
        conn = factory.newConnection();
        channel = conn.createChannel();

        if ("check".equals(operation)) {
            isConnected = true;
            return;
        }

        switch (type) {
          case RMQ_DIRECT:
            if (exchangeName.length() > 0) { // dedicated exchange
                if (!isPassive || !exchangeExists(exchangeName))
                    channel.exchangeDeclare(exchangeName, "direct", isDurable,
                        isAuto, null);
                if (qName.length() > 0) {
                    if (!isPassive || !queueExists(qName))
                        channel.queueDeclare(qName,isDurable,false,isAuto,null);
                }
                else
                    qName = channel.queueDeclare().getQueue();
                channel.queueBind(qName, exchangeName, routingKey);
            }
            else if (qName.length() > 0) { // default exchange for a queue
                if (!isPassive || !queueExists(qName))
                    channel.queueDeclare(qName, isDurable, false, isAuto, null);
                if ("pub".equals(operation))
                    routingKey = qName;
            }
            else {
                qName = channel.queueDeclare().getQueue();
                channel.queueBind(qName, exchangeName, routingKey);
            }
            dest = ("get".equals(operation)) ? qName : routingKey;
            break;
          case RMQ_FANOUT:
            if (exchangeName.length() <= 0)
                throw(new IOException("empty exchange of fanout for: " + uri));
            if (!isPassive || !exchangeExists(exchangeName))
                channel.exchangeDeclare(exchangeName, "fanout", isDurable,
                    isAuto, null);
            if ("get".equals(operation)) { // for listen
                if (qName.length() > 0) {
                    if (!isPassive || !queueExists(qName))
                        channel.queueDeclare(qName,isDurable,false,isAuto,null);
                }
                else
                    qName = channel.queueDeclare().getQueue();
                channel.queueBind(qName, exchangeName, "");
                dest = qName;
            }
            else
                dest = routingKey;
            break;
          case RMQ_TOPIC:
            if (exchangeName.length() <= 0)
                throw(new IOException("empty exchange of topic for: " + uri));
            if (!isPassive || !exchangeExists(exchangeName))
                channel.exchangeDeclare(exchangeName, "topic", isDurable,
                    isAuto, null);
            if ("get".equals(operation)) { // for sub
                if (qName.length() > 0) {
                    if (!isPassive || !queueExists(qName))
                        channel.queueDeclare(qName,isDurable,false,isAuto,null);
                }
                else
                    qName = channel.queueDeclare().getQueue();
                if (keyList != null && keyList.length > 0) {
                    for (int i=0; i<keyList.length; i++)
                        channel.queueBind(qName, exchangeName, keyList[i]);
                }
                else
                    channel.queueBind(qName, exchangeName, routingKey);
                dest = qName;
            }
            else
                dest = routingKey;
            break;
          case RMQ_HEADERS:
            break;
          default:
        }
        isConnected = true;
    }

    public byte[] get(long timeout, Map<String, String> props)
        throws IOException {
        QueueingConsumer consumer = getConsumer(true);
        QueueingConsumer.Delivery delivery = null;
        try {
            delivery = consumer.nextDelivery(timeout);
        }
        catch (InterruptedException e) {
        }
        if (delivery != null) {
            if (props != null) {
                props.clear();
                Map<String, Object> map = delivery.getProperties().getHeaders();
                if (map != null && map.size() > 0) {
                    Object o;
                    for (String key : map.keySet()) {
                        o = map.get(key);
                        if (o == null)
                            continue;
                        else if (o instanceof byte[])
                            props.put(key, new String((byte[]) o));
                        else if (o instanceof String)
                            props.put(key, (String) o);
                        else
                            props.put(key, o.toString());
                    }
                }
            }
            return delivery.getBody();
        }
        else
            return null;
    }

    /**
     * It waits for a message up to timeout in ms. Upon received a message,
     * it returns byte array of the message body and fills in the property map
     * with RabbitMQ properties. If timeout is 0 or less, there is no wait.
     * If there is no message received, it returns null.
     */
    public byte[] get(boolean autoAck, long timeout, Map<String, String> props)
        throws IOException {
        GetResponse r = channel.basicGet(qName, autoAck);
        if (r == null && timeout > 0) {
            long tm = System.currentTimeMillis() + timeout;
            do {
                try {
                    Thread.sleep((timeout >= 20) ? 20 : timeout);
                }
                catch (InterruptedException e) {
                }
                r = channel.basicGet(qName, autoAck);
                if (r != null)
                    break; 
                timeout = tm - System.currentTimeMillis(); 
            } while (timeout > 0);
        }
        if (r != null) {
            if (props != null) {
                props.clear();
                Map<String, Object> map = r.getProps().getHeaders();
                if (map != null && map.size() > 0) {
                    Object o;
                    for (String key : map.keySet()) {
                        o = map.get(key);
                        if (o == null)
                            continue;
                        else if (o instanceof byte[])
                            props.put(key, new String((byte[]) o));
                        else if (o instanceof String)
                            props.put(key, (String) o);
                        else
                            props.put(key, o.toString());
                    }
                }
            }
            return r.getBody();
        }
        else
            return null;
    }

    public void pub(String key, AMQP.BasicProperties props, byte[] buff)
        throws IOException {
        channel.basicPublish(exchangeName, key, props, buff);
        try {
            channel.waitForConfirms();
        }
        catch (InterruptedException e) {
        }
    }

    public void pub(String key, AMQP.BasicProperties props, String msgStr)
        throws IOException {
        channel.confirmSelect();
        channel.basicPublish(exchangeName, key, props, msgStr.getBytes());
        try {
            channel.waitForConfirms();
        }
        catch (InterruptedException e) {
        }
    }

    public boolean exchangeExists(String name) {
        try {
            channel.exchangeDeclarePassive(name);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean queueExists(String name) {
        try {
            channel.queueDeclarePassive(name);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    public String exchangeDeclare(String name, String type, boolean duable) {
        try {
            channel.exchangeDeclare(name, type, duable);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String exchangeDelete(String name) {
        try {
            channel.exchangeDelete(name);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String exchangeBind(String target, String name, String key) {
        try {
            channel.exchangeBind(target, name, key);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String exchangeUnbind(String target, String name, String key) {
        try {
            channel.exchangeUnbind(target, name, key);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String queueBind(String queue, String name, String key) {
        try {
            channel.queueBind(queue, name, key);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String queueUnbind(String queue, String name, String key) {
        try {
            channel.queueUnbind(queue, name, key);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String queueDeclare(String name, boolean durable) {
        try {
            channel.queueDeclare(name, durable, false, false, null);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public String queueDelete(String name) {
        try {
            channel.queueDelete(name);
        }
        catch (Exception e) {
            Throwable t = e.getCause();
            return (t != null) ? t.toString() : e.toString();
        }
        return null;
    }

    public QueueingConsumer getConsumer(boolean ack) throws IOException {
        if (consumer != null)
            return consumer;
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(qName, ack, consumer);
        return consumer;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** It reconnects and returns null or error message upon failure */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (IOException e) {
            return TraceStackThread.traceStack(e);
        }
        catch (TimeoutException e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    /** acknowledges the RabbitMQ msgs only */
    public void ack(long deliveryTag) throws IOException {
        channel.basicAck(deliveryTag, false);
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public String getExchange() {
        return exchangeName;
    }

    public String getDestination() {
        return dest;
    }

    public int getExchangeType() {
        return type;
    }

    public void close() {
        if (consumer != null)
            consumer = null;
        if (channel != null) try {
            channel.close();
        }
        catch (Exception e) {
        }
        channel = null;
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        conn = null;
        isConnected = false;
    }

    public static void main(String args[]) {
        HashMap<String, Object> props;
        final int ACTION_NONE = 0;
        final int ACTION_GET = 1;
        final int ACTION_PUB = 2;
        final int ACTION_CHK = 3;
        final int ACTION_ADD = 4;
        final int ACTION_DEL = 5;
        final int ACTION_BIN = 6;
        final int ACTION_UNB = 7;
        int i, timeout = 0, action = ACTION_NONE;
        String uri = null, str = null, msg = null, type = "direct";
        String eName = null, qName = null, key = null;
        boolean isDurable = false;
        GetResponse r = null;
        RMQConnector conn = null;
        SimpleDateFormat dateFormat;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zz");
        props = new HashMap<String, Object>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                if (i == args.length-1)
                    msg = args[i];
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'a':
                str = args[++i];
                if ("get".equalsIgnoreCase(str))
                    action = ACTION_GET;
                else if ("pub".equalsIgnoreCase(str))
                    action = ACTION_PUB;
                else if ("check".equalsIgnoreCase(str))
                    action = ACTION_CHK;
                else if ("add".equalsIgnoreCase(str))
                    action = ACTION_ADD;
                else if ("delete".equalsIgnoreCase(str))
                    action = ACTION_DEL;
                else if ("bind".equalsIgnoreCase(str))
                    action = ACTION_BIN;
                else if ("unbind".equalsIgnoreCase(str))
                    action = ACTION_UNB;
                else
                    action = ACTION_NONE;
                if (action > ACTION_NONE && action < ACTION_CHK)
                    props.put("Operation", str);
                else
                    props.put("Operation", "check");
                break;
              case 'u':
                if (i+1 < args.length)
                    uri = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'p':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              case 'b':
                if (i+1 < args.length)
                    props.put("BufferSize", args[++i]);
                break;
              case 'T':
                if (i+1 < args.length) {
                    type = args[++i];
                    props.put("ExchangeType", type);
                }
                break;
              case 'D':
                if (i+1 < args.length) {
                    str = args[++i];
                    props.put("IsDurable", str);
                    if ("true".equalsIgnoreCase(str))
                        isDurable = true;
                }
                break;
              case 'P':
                if (i+1 < args.length)
                    props.put("IsPassive", args[++i]);
                break;
              case 'A':
                if (i+1 < args.length)
                    props.put("AutoDelete", args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = 1000 * Integer.parseInt(args[++i]);
                break;
              case 'q':
                if (i+1 < args.length) {
                    qName = args[++i];
                    props.put("QueueName", qName);
                }
                break;
              case 'e':
                if (i+1 < args.length) {
                    eName = args[++i];
                    props.put("ExchangeName", eName);
                }
                break;
              case 'k':
                if (i+1 < args.length) {
                    key = args[++i];
                    props.put("RoutingKey", key);
                }
                break;
              default:
            }
        }

        if (uri == null) {
            printUsage();
            System.exit(0);
        }
        else
            props.put("URI", uri);

        try {
            byte[] buf;
            Map<String, String> map = new HashMap<String, String>();
            conn = new RMQConnector(props);
            str = null;
            switch (action) {
              case ACTION_GET:
                buf = conn.get(true, (long) timeout, map);
                if (buf != null) {
                    System.out.println("got a msg: " + new String(buf));
                    if (map.size() > 0) {
                        for (String s : map.keySet()) {
                            System.out.println(s + ": " + map.get(s));
                        }
                    }
                }
                else
                    System.out.println("no message is available");
                break;
              case ACTION_PUB:
                if (msg != null && msg.length() > 0) {
                    if (qName != null)
                        key = qName;
                    else if (key == null)
                        key = "";
                    conn.pub(key, new AMQP.BasicProperties.Builder()
                        .contentType("text/plain").deliveryMode(new Integer(2))
                        .priority(new Integer(1)).userId("guest")
                        .build(), msg);
                    System.out.println("published a msg to " + key);
                }
                else
                    str = "msg is empty";
                break;
              case ACTION_CHK:
                if (eName != null) {
                    if (conn.exchangeExists(eName))
                        System.out.println("exchange: " + eName + " exists");
                    else
                        System.out.println("no exchange for " + eName);
                }
                else if (qName != null && qName.length() > 0) {
                    if (conn.queueExists(qName))
                        System.out.println("queue: " + qName + " exists");
                    else
                        System.out.println("no queue for " + qName);
                }
                break;
              case ACTION_ADD:
                if (eName != null) {
                    str = conn.exchangeDeclare(eName, type, isDurable);
                    if (str == null)
                        System.out.println("exchange: " + eName + " created");
                }
                else if (qName != null && qName.length() > 0) {
                    str = conn.queueDeclare(qName, isDurable);
                    if (str == null)
                        System.out.println("queue: " + qName + " created");
                }
                break;
              case ACTION_DEL:
                if (eName != null) {
                    str = conn.exchangeDelete(eName);
                    if (str == null)
                        System.out.println("exchange: " + eName + " deleted");
                }
                else if (qName != null && qName.length() > 0) {
                    str = conn.queueDelete(qName);
                    if (str == null)
                        System.out.println("queue: " + qName + " deleted");
                }
                break;
              case ACTION_BIN:
                if (eName != null && qName != null && key != null) {
                    str = conn.queueBind(qName, eName, key);
                    if (str == null)
                        System.out.println("queue: " + qName + " bound to " + 
                            eName + " on the key of " + key);
                }
                else if (eName != null && msg != null && key != null) {
                    str = conn.exchangeBind(msg, eName, key);
                    if (str == null)
                        System.out.println("exchange: " + msg + " bound to "+
                            eName + " on the key of " + key);
                }
                else
                    str = "one of [exchange, queue or msg, key] is empty";
                break;
              case ACTION_UNB:
                if (eName != null && qName != null && key != null) {
                    str = conn.queueUnbind(qName, eName, key);
                    if (str == null)
                        System.out.println("queue: " + qName + " unbound to " + 
                            eName + " on the key of " + key);
                }
                else if (eName != null && msg != null && key != null) {
                    str = conn.exchangeUnbind(msg, eName, key);
                    if (str == null)
                        System.out.println("exchange: " + msg + " unbound to "+
                            eName + " on the key of " + key);
                }
                else
                    str = "one of [exchange, queue or msg, key] is empty";
                break;
              default:
                str = "not supported yet";
            }
            if (str != null)
                System.out.println("Error: " + str);
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (conn != null) try {
                conn.close();
            }
            catch (Exception ex) {
            }
        }
    }

    private static void printUsage() {
        System.out.println("RMQConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("RMQConnector: a RabbitMQ client to get or put msgs");
        System.out.println("Usage: java org.qbroker.net.RMQConnector -u uri -t timeout -n username -p password -q qname -a action [message]");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of get, pub, add, delete, bind, unbind (default: check)");
        System.out.println("  -b: buffer size in byte");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -q: queue name");
        System.out.println("  -e: exchange name");
        System.out.println("  -k: routing key");
        System.out.println("  -T: exchange type (default: direct)");
        System.out.println("  -D: durable or not (default: false)");
        System.out.println("  -P: passive or not (default: true)");
        System.out.println("  -A: auto delete or not (default: false)");
        System.out.println("  -t: timeout in sec (default: 60)");
    }
}
