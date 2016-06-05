package org.qbroker.net;

/* QMFRequester.java - a generic QMF requester */

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;
import java.nio.ByteBuffer;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MapMessage;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import org.apache.qpid.transport.codec.BBDecoder;
import org.qbroker.common.Requester;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;
import org.qbroker.common.WrapperException;
import org.qbroker.net.JMXRequester;

/**
 * QMFRequester connects to a generic QMF server for QMF requests. Currently,
 * it supports the methods of list, getValues and getResponse for QPid only.
 *<br/><br/>
 * It requires qpid-client.jar, qpid-common.jar, slf4-api.jar, slf4-log4j12,jar
 * and Java 6 to compile.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QMFRequester implements Requester {
    protected String uri;
    private Properties env = null;
    private Connection connection = null;
    private Session session = null;
    private Destination queue = null;
    private Destination replyQ = null;
    private MessageProducer sender = null;
    private MessageConsumer receiver = null;
    private String connectionFactoryName;
    private String qName;
    private long timeout = 10000;
    private String username = null;
    private String password = null;
    private boolean isConnected = false;

    public QMFRequester(Map props) {
        Object o;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;
        if(!uri.startsWith("amqp:"))
            throw(new IllegalArgumentException("Bad service URL: " + uri));

        if ((o = props.get("Timeout")) != null)
            timeout = Long.parseLong((String) o);
        if (timeout <= 0)
            timeout = 10000;

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null)
                password = (String) o;
        }

        qName = "broker";
        connectionFactoryName = "host";
        env = new Properties();
        env.setProperty("java.naming.factory.initial",
            "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        env.setProperty("connectionfactory.host", uri);
        env.setProperty("destination.broker", "qmf.default.direct/broker");

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) try {
            connect();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to initialize " +
                "QMF connector for " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }
    }

    /** returns names of the all objects for the given class on the server */
    public String[] list(String target) throws JMSException {
        final MapMessage msg;

        if (receiver == null || !isConnected)
            throw(new JMSException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empry target for " + uri));
        else try {
            final Map<String, String> schemaId = new HashMap<String, String>();
            schemaId.put("_class_name", target);
            msg = session.createMapMessage();
            msg.setJMSReplyTo(replyQ);
            msg.setStringProperty("x-amqp-0-10.app-id", "qmf2");
            msg.setStringProperty("qmf.opcode", "_query_request");
            msg.setString("_what", "OBJECT");
            msg.setObject("_schema_id", schemaId);
        }
        catch (Exception e) {
            throw(new JMSException("failed to build QMF request msg for " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }

        // Query object names
        try {
            final List response = request(msg, timeout);
            if (response != null) {
                int n = response.size();
                final String[]  keys = new String[n];
                n = 0;
                for (Object o : response) {
                    Map ph = (Map) ((Map) o).get("_object_id");
                    keys[n++] = new String((byte[]) ph.get("_object_name"),
                        "UTF8");
                }
                return keys;
            }
            else
                return null;
        }
        catch (JMSException e) {
            throw(new JMSException("failed to list QMF objects on " + target +
                " from " + uri + ": " + TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            throw(new RuntimeException("unexpected failure on response from " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns a list of all objects for the given class on the server */
    public List<Map> listAll(String target, String... attrs)
        throws JMSException {
        final MapMessage msg;

        if (receiver == null || !isConnected)
            throw(new JMSException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empry target for " + uri));
        else try {
            final Map<String, String> schemaId = new HashMap<String, String>();
            schemaId.put("_class_name", target);
            msg = session.createMapMessage();
            msg.setJMSReplyTo(replyQ);
            msg.setStringProperty("x-amqp-0-10.app-id", "qmf2");
            msg.setStringProperty("qmf.opcode", "_query_request");
            msg.setString("_what", "OBJECT");
            msg.setObject("_schema_id", schemaId);
        }
        catch (Exception e) {
            throw(new JMSException("failed to build QMF request msg for " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }

        // Query object names
        try {
            final List response = request(msg, timeout);
            if (response != null) {
                final List<Map> list = new ArrayList<Map>();
                if (attrs.length <= 0) {
                    for (Object o : response) {
                        list.add((Map) ((Map) o).get("_values"));
                    }
                }
                else {
                    for (Object o : response) {
                        Map map = (Map) ((Map) o).get("_values");
                        HashMap<String, Object> ph =
                            new HashMap<String, Object>();
                        for (String attr: attrs) {
                            ph.put(attr, map.get(attr));
                        }
                        list.add(ph);
                    }
                }
                return list;
            }
            else
                return null;
        }
        catch (JMSException e) {
            throw(new JMSException("failed to list QMF objects for " + target +
                " from " + uri + ": " + TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            throw(new RuntimeException("unexpected failure on response from " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns a map with the info for the target */
    public Map<String, List> getInfo(String target) throws JMSException {
        final MapMessage msg;

        if (receiver == null || !isConnected)
            throw(new JMSException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empry target for " + uri));
        else try {
            final Map<String, String> objectId = new HashMap<String, String>();
            objectId.put("_object_name", target);
            msg = session.createMapMessage();
            msg.setJMSReplyTo(replyQ);
            msg.setStringProperty("x-amqp-0-10.app-id", "qmf2");
            msg.setStringProperty("qmf.opcode", "_query_request");
            msg.setString("_what", "OBJECT");
            msg.setObject("_object_id", objectId);
        }
        catch (Exception e) {
            throw(new JMSException("failed to build QMF request msg for " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }

        // send the request
        try {
            final List response = request(msg, timeout);
            if (response == null) {
                return null;
            }
            else if (response.size() <= 0) {
                return new HashMap<String, List>();
            }
            else {
                List<Map> list;
                Map<String, List> ph = new HashMap<String, List>();
                Map<String, String> h;
                final Map map = (Map) ((Map) response.get(0)).get("_values");
                if (map != null && map.size() > 0) {
                    list = new ArrayList<Map>();
                    for (Object o: map.keySet().toArray()) {
                        h = new HashMap<String, String>();
                        h.put("name", (String) o);
                        h.put("type", map.get(o).getClass().getName());
                        h.put("description", "");
                        list.add(h);
                    }
                    ph.put("Attribute", list);
                }
                return ph;
            }
        }
        catch (JMSException e) {
            throw(new JMSException("failed to get info on " + target +
                " from "+ uri + ": " + TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            throw(new RuntimeException("unexpected failure on response from " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the value of the attribute from the target */
    public Object getValue(String target, String attr) throws JMSException {
        final MapMessage msg;

        if (receiver == null || !isConnected)
            throw(new JMSException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empry target for " + uri));
        else if (attr == null || attr.length() <= 0)
            throw(new IllegalArgumentException("empry attribute on " +
                target + " for " + uri));
        else try {
            final Map<String, String> objectId = new HashMap<String, String>();
            objectId.put("_object_name", target);
            msg = session.createMapMessage();
            msg.setJMSReplyTo(replyQ);
            msg.setStringProperty("x-amqp-0-10.app-id", "qmf2");
            msg.setStringProperty("qmf.opcode", "_query_request");
            msg.setString("_what", "OBJECT");
            msg.setObject("_object_id", objectId);
        }
        catch (Exception e) {
            throw(new JMSException("failed to build QMF request msg for " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }

        // request attributes for the object
        try {
            final List response = request(msg, timeout);
            if (response == null) {
                return null;
            }
            else if (response.size() <= 0) {
                return null;
            }
            else {
                final Map map = (Map) ((Map) response.get(0)).get("_values");
                return map.get(attr);
            }
        }
        catch (JMSException e) {
            throw(new JMSException("failed to get value on " + target +
                " from "+ uri + ": " + TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            throw(new RuntimeException("unexpected failure on response from " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the key-value map for the attributes from the target */
    public Map<String, Object> getValues(String target, String[] attrs)
        throws JMSException {
        final MapMessage msg;

        if (receiver == null || !isConnected)
            throw(new JMSException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empry target for " + uri));
        else if (attrs == null || attrs.length <= 0)
            throw(new IllegalArgumentException("empry attributes on " +
                target + " for " + uri));
        else try {
            final Map<String, String> objectId = new HashMap<String, String>();
            objectId.put("_object_name", target);
            msg = session.createMapMessage();
            msg.setJMSReplyTo(replyQ);
            msg.setStringProperty("x-amqp-0-10.app-id", "qmf2");
            msg.setStringProperty("qmf.opcode", "_query_request");
            msg.setString("_what", "OBJECT");
            msg.setObject("_object_id", objectId);
        }
        catch (Exception e) {
            throw(new JMSException("failed to build QMF request msg for " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }


        // request attributes for the object
        try {
            final List response = request(msg, timeout);
            if (response == null) {
                return null;
            }
            else if (response.size() <= 0) {
                return new HashMap<String, Object>();
            }
            else {
                final Map map = (Map) ((Map) response.get(0)).get("_values");
                Map<String, Object> ph = new HashMap<String, Object>();
                for (String key : attrs) {
                    ph.put(key, map.get(key));
                }
                return ph;
            }
        }
        catch (JMSException e) {
            throw(new JMSException("failed to get attribute list on " +
                target + " from " + uri +": "+ TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            throw(new RuntimeException("unexpected failure on response from " +
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }


    /**
     * This takes a QMF query command and converts it into a QMF request.
     * It sends the request to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection.
     *<br/></br/>
     * The QMF command is supposed to be as follows:
     *</br>
     * DISPLAY Target attr0:attr1:attr2
     *<br/></br/>
     * In case of a fatal failure, it disconnects first and then throws a
     * WrapperException with cause set to the vendor specific exception or IO
     * related exception. Therefore please make sure to get the cause exception
     * from it and handles the cause exception with care. For other failures,
     * it throws an IllegalArgumentException. If the disconnect flag is set to
     * true, it disconnects right before it throws the exception.
     */
    public int getResponse(String qmfCmd, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        String target, attrs, line = null;
        String[] keys;
        int k = -1;

        if (qmfCmd == null || qmfCmd.length() <= 0)
            throw(new IllegalArgumentException("empty request for " + uri));

        target = JMXRequester.getTarget(qmfCmd);
        attrs = JMXRequester.getAttributes(qmfCmd);
        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("bad request of " + qmfCmd +
                " for " + uri));

        if (strBuf == null)
            throw(new IllegalArgumentException("response buffer is null on " +
                target + " for " + uri));

        if (!isConnected) {
            String str;
            if ((str = reconnect()) != null) {
                throw(new WrapperException("QMF connection failed on " +uri+
                    ": " + str));
            }
        }

        if ("*".equals(target))
            target = "queue";

        if (target.indexOf(':') > 0) // for a query on a specific object
            keys = null;
        else if (attrs != null && attrs.length() > 0) // for a list on a class
            keys = null;
        else try { // for a list on a class
            keys = list(target);
            if (keys == null)
                keys = new String[0];
        }
        catch (JMSException e) { // fatal err converted to JMException
            close();
            throw(new WrapperException("failed to get the list on " + target +
                " from " + uri, e));
        }
        catch (Exception e) {
            if (autoDisconn)
                close();
            throw(new IllegalArgumentException("failed to get list on " +
                target + " from " + uri +": "+ TraceStackThread.traceStack(e)));
        }

        if ((k = strBuf.length()) > 0)
            strBuf.delete(0, k);

        if (keys != null) { // for a list of names
            k = 0;
            for (String key : keys) {
                line = "\"" + Utils.escapeJSON(key) + "\"";
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append(line + Utils.RS);
            }
        }
        else { // for a list or a specific object
            List<Map> list;
            String[] a;
            if (attrs != null && attrs.length() > 0) { // for a list
                a = (attrs.indexOf(':') < 0) ? new String[]{attrs} :
                    Utils.split(":", attrs);
            }
            else try { // for a specific object
                a = getAllAttributes(target);
            }
            catch (JMSException e) {
                close();
                throw(new WrapperException("failed to get info on " + target +
                   " from " + uri, e));
            }
            catch (Exception e) {
                if (autoDisconn)
                    close();
                throw(new IllegalArgumentException("failed to get info on " +
                    target + " from " + uri + ": " +
                    TraceStackThread.traceStack(e)));
            }

            try {
                list = listAll(target, a);
            }
            catch (JMSException e) {
                close();
                throw(new WrapperException("failed to query on " + target +
                    " from " + uri, e));
            }
            catch (Exception e) {
                if (autoDisconn)
                    close();
                throw(new IllegalArgumentException("failed to query on " +
                    target + " from " + uri + ": " +
                    TraceStackThread.traceStack(e)));
            }
            k = 0;
            for (Map map : list) {
                if (map.size() <= 0)
                    continue;
                line = Utils.toJSON(map);
                if (line != null && line.length() > 0) {
                    if (k++ > 0)
                        strBuf.append(",");
                    strBuf.append(line + Utils.RS);
                }
            }
        }

        strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
        strBuf.append("]}");
        if (autoDisconn)
            close();
        return k;
    }

    /** returns names of all attributes for the target */
    public String[] getAllAttributes(String target) throws JMSException {
        int i, n;
        Object o;
        Map map;
        List list;
        String[] keys;

        map = getInfo(target);
        if (map != null && (o = map.get("Attribute")) != null)
            list = (List) o;
        else
            list = new ArrayList();
        n = list.size();
        keys = new String[n];
        for (i=0; i<n; i++) {
            o = list.get(i);
            keys[i] = (String) ((Map) o).get("name");
        }
        return keys;
    }

    private void connect() throws JMSException {
        isConnected = false;
        final ConnectionFactory factory = initialize();
        if (username != null)
            connection = factory.createConnection(username, password);
        else
            connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        replyQ = session.createQueue("qmf.default.direct/" +
            UUID.randomUUID() + "; {node: {type: topic}}");
        sender = session.createProducer(queue);
        receiver = session.createConsumer(replyQ);
        isConnected = true;
    }

    protected ConnectionFactory initialize() throws JMSException {
        final Context ctx;
        ConnectionFactory factory;
        try {
            ctx = new InitialContext(env);
        }
        catch (NamingException e) {
            throw(new JMSException("failed to get ctx on " + uri + ": "+
                TraceStackThread.traceStack(e)));
        }

        try {
            factory = (ConnectionFactory) ctx.lookup(connectionFactoryName);
        }
        catch (NamingException e) {
            factory = null;
            throw(new JMSException("failed to lookup ConnnectionFactory '" +
                connectionFactoryName + "' on " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }

        try {
            queue = (Destination) ctx.lookup(qName);
        }
        catch (NamingException e) {
            queue = null;
            throw(new JMSException("failed to lookup queue '" + qName +
                "' on " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        return factory;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** returns null if reconnected or error msg otherwise */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    public void close() {
        if (sender != null) try {
            sender.close();
        }
        catch (Exception e) {
        }
        sender = null;
        if (receiver != null) try {
            receiver.close();
        }
        catch (Exception e) {
        }
        receiver = null;
        if (session != null) try {
            session.close();
        }
        catch (Exception e) {
        }
        session = null;
        if (connection != null) try {
            connection.close();
        }
        catch (Exception e) {
        }
        connection = null;
        isConnected = false;
    }

    /** returns null if timed out */
    private List request(MapMessage msg, long timeout) throws JMSException {
        sender.send(msg);
        Message response = receiver.receive(timeout);
        if (response == null) // timed out
            return null;
        if (response instanceof BytesMessage) {
            return decode((BytesMessage) response);
        }
        else if (response instanceof MapMessage) {
            throw(new JMSException("Request failed: " + response.toString()));
        }
        else {
            throw(new IllegalArgumentException("Received unexpected response: "+
                response.toString()));
        }
    }

    private static List decode(BytesMessage msg) throws JMSException {
        //only handles responses up to 2^31-1 bytes long
        byte[] data = new byte[(int) msg.getBodyLength()];
        msg.readBytes(data);
        BBDecoder decoder = new BBDecoder();
        decoder.init(ByteBuffer.wrap(data));
        return decoder.readList();
    }

    public static void main(String[] args) {
        String url = null;
        String username = null;
        String password = null;
        String target = null;
        String type = null;
        String attr = null;
        String[] keys;
        QMFRequester qmf = null;
        Map<String, Object> ph = new HashMap<String, Object>();
        int i, n = 0;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'u':
                if (i+1 < args.length)
                    url = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    username = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    password = args[++i];
                break;
              case 'c':
                if (i+1 < args.length)
                    type = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    target = args[++i];
                break;
              case 'a':
                if (i+1 < args.length)
                    attr = args[++i];
                break;
              default:
            }
        }

        if (type == null)
            type = "queue";

        ph.put("URI", url);
        if (username != null) {
            ph.put("Username", username);
            ph.put("Password", password);
        }

        try {
            qmf = new QMFRequester(ph);
            if (target == null || target.length() <= 0) { // list
                keys = qmf.list(type);
                n = keys.length;
                for (i=0; i<n; i++)
                    System.out.println(i + ": " + keys[i]);
            }
            else if (attr == null || attr.length() <= 0) { // for info
                List list;
                Map<String, List> map = qmf.getInfo(target);
                if ((list = map.get("Attribute")) != null) {
                    Object o;
                    n = list.size();
                    if (n > 0)
                        System.out.println("Attribute: Name, Type, Desc");
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        System.out.println(i + ": "+((Map) o).get("name") +
                            ", " + ((Map) o).get("type") +
                            ", " + ((Map) o).get("description"));
                    }
                }
            }
            else if ((n = attr.indexOf(':')) > 0) { // for multiple attributes
                Object o;
                List<String> list = new ArrayList<String>();
                i = 0;
                do {
                    list.add(attr.substring(i, n));
                    i = n + 1;
                } while ((n = attr.indexOf(':', i)) > i);
                list.add(attr.substring(i));
                n = list.size();
                keys = list.toArray(new String[n]);
                ph = qmf.getValues(target, keys);
                for (i=0; i<n; i++) {
                    o = ph.get(keys[i]);
                    if (o != null)
                        System.out.println(keys[i] + ": " +
                            ((o instanceof byte[]) ? new String((byte[]) o) :
                            o.toString()));
                }
            }
            else { // for a single attribute
                Object o = qmf.getValue(target, attr);
                if (o != null)
                    System.out.println(attr + ": " + ((o instanceof byte[]) ?
                        new String((byte[]) o) : o.toString()));
            }
            if (qmf != null)
                qmf.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (qmf != null)
                qmf.close();
        }
    }

    private static void printUsage() {
        System.out.println("QMFRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("QMFRequester: list all objects on a QMF Server");
        System.out.println("Usage: java org.qbroker.net.QMFRequester -u url -n username -p password -t target -a attibutes");
        System.out.println("  -?: print this message");
        System.out.println("  u: service url");
        System.out.println("  n: username");
        System.out.println("  p: password");
        System.out.println("  c: class");
        System.out.println("  t: target");
        System.out.println("  a: attributes delimited via ':'");
    }
}
