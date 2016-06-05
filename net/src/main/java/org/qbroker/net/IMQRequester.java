package org.qbroker.net;

/* IMQRequester.java - a JMX requester for Sun OpenMQ */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.management.remote.JMXConnector;
import com.sun.messaging.AdminConnectionFactory;
import com.sun.messaging.AdminConnectionConfiguration;
import org.qbroker.common.TraceStackThread;

/**
 * IMQRequester connects to the admin service of a Sun OpenMQ MBean Server
 * for JMX requests.
 *<br/><br/>
 * It requires imq.jar, imqjmx.jar and Java 5 to compile.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class IMQRequester extends org.qbroker.net.JMXRequester {
    private int port = 7676;
    private String hostname;
    private String username;
    private String password;

    public final static String[] imqAttrs = {
        "Name",
        "Type",
        "StateLabel",
        "NumMsgsIn",
        "NumMsgsOut",
        "NumMsgs",
        "NumConsumers",
        "NumProducers",
        "NumMsgsPendingAcks",
        "NumMsgsHeldInTransaction"
    };

    /** Creates new IMQRequester */
    public IMQRequester(Map props) {
        super();
        Object o;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            URI u = new URI(uri);
            if (!"imq".equals(u.getScheme()))
                throw(new IllegalArgumentException("unsupported scheme: "+uri));
            hostname = u.getHost();
            if (hostname == null || hostname.length() <= 0)
                throw(new IllegalArgumentException("Bad uri: " + uri));
            else if (u.getPort() > 0)
                port = u.getPort();
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("Username")) != null)
            username = (String) o;
        if ((o = props.get("Password")) != null)
            password = (String) o;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) try {
            connect();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to initialize " +
                "JMX connector for " + uri + ": " + e.toString()));
        }
    }

    protected void connect() throws IOException {
        jmxc = null;
        isConnected = false;
        try {
            AdminConnectionFactory acf = new AdminConnectionFactory();
            acf.setProperty(AdminConnectionConfiguration.imqAddress,
                hostname + ":" + port);
            jmxc = acf.createConnection(username, password);
        }
        catch (Exception e) {
            throw(new IOException("failed to connect to " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }
        if (jmxc != null)
            isConnected = true;
    }

    public static void main(String[] args) {
        String url = null;
        String username = null;
        String password = null;
        String target = null;
        String attr = null;
        String[] keys;
        IMQRequester jmx = null;
        Map<String, Object> ph;
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

        ph = new HashMap<String, Object>();
        ph.put("URI", url);
        if (username != null) {
            ph.put("Username", username);
            ph.put("Password", password);
        }

        try {
            jmx = new IMQRequester(ph);
            if (target == null || target.length() <= 0 ||
                target.indexOf('*') >= 0 || target.indexOf('?') >= 0) { // list
                keys = jmx.list(target);
                n = keys.length;
                for (i=0; i<n; i++)
                    System.out.println(i + ": " + keys[i]);
            }
            else if (attr == null || attr.length() <= 0) { // for info
                Object o;
                List list;
                Map<String, List> pl = jmx.getInfo(target);
                if ((o = ph.get("Attribute")) != null) {
                    list = (List) o;
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
                if ((o = ph.get("Operation")) != null) {
                    list = (List) o;
                    n = list.size();
                    if (n > 0)
                        System.out.println("Operation: Name, Type, Desc");
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        System.out.println(i + ": "+((Map) o).get("name") +
                            ", " + ((Map) o).get("type") +
                            ", " + ((Map) o).get("description"));
                    }
                }
                if ((o = ph.get("Notification")) != null) {
                    list = (List) o;
                    n = list.size();
                    if (n > 0)
                        System.out.println("Notification: Name, Type, Desc");
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
                keys = new String[n];
                for (i=0; i<n; i++)
                    keys[i] = (String) list.get(i);
                ph = jmx.getValues(target, keys);
                for (i=0; i<n; i++) {
                    o = ph.get(keys[i]);
                    if (o != null)
                        System.out.println(keys[i] + ": " + o.toString());
                }
            }
            else { // for a single attribute
                Object o = jmx.getValue(target, attr);
                if (o != null)
                    System.out.println(attr + ": " + o.toString());
            }
            jmx.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (jmx != null) try {
                jmx.close();
            }
            catch (Exception ex) {
            }
        }
    }

    private static void printUsage() {
        System.out.println("IMQRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("IMQRequester: list all MBeans on an IMQ Server");
        System.out.println("Usage: java org.qbroker.net.IMQRequester -u url -n username -p password -t target -a attributes");
        System.out.println("  -?: print this message");
        System.out.println("  u: imq url");
        System.out.println("  n: username");
        System.out.println("  p: password");
        System.out.println("  t: target");
        System.out.println("  a: attributes delimited via ':'");
    }
}
