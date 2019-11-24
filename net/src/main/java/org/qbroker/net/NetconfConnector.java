package org.qbroker.net;

/* NetconfConnector.java - a NetConf connector for routers */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.w3c.dom.Document;
import com.tailf.jnc.JNCException;
import com.tailf.jnc.NetconfSession;
import com.tailf.jnc.SSHConnection;
import com.tailf.jnc.SSHSession;
import com.tailf.jnc.NodeSet;
import com.tailf.jnc.XMLParser;
import com.tailf.jnc.Element;
import org.qbroker.common.Utils;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * NetconfConnector connects to a router via ssh and provides the following
 * methods for read-only operations: getXML() and getTunnelNames().
 *<br><br>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class NetconfConnector implements Connector {
    protected String uri;

    private SSHConnection conn = null;
    private NetconfSession session = null;
    private String hostname;
    private String username;
    private String password;
    private int port = 22;
    private int timeout = 60000;
    private int bufferSize = 4096;
    private boolean isConnected = false;

    /** Creates new NetconfConnector */
    public NetconfConnector(Map props) throws Exception {
        Object o;
        URI u;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"netconf".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 830;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((o = props.get("Username")) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        else {
            username = (String) o;

            password = null;
            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null)
                throw(new IllegalArgumentException("Password is not defined"));
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o))
            connect();
    }

    protected void connect() throws IOException {
        try {
            conn = new SSHConnection(hostname, port, timeout);
            conn.authenticateWithPassword(username, password);
            SSHSession ssh = new SSHSession(conn);
            session = new NetconfSession(ssh);
            isConnected = true;
        }
        catch (JNCException e) {
            throw(new IOException(e.toString()));
        }
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
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (session != null) try {
            session.closeSession();
        }
        catch (Exception e) {
        }
        session = null;
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        conn = null;
        isConnected = false;
    }

    /** returns all the names for tunnels with the given xpath.  */
    public String[] getTunnelNames(String path) throws IOException {
        if (path == null || path.length() <= 0)
            return null;

        try {
            NodeSet tunnelSet = session.get(path);

            if (tunnelSet.size() == 0) {
                return new String[0];
            }
            else {
                String xmlString = tunnelSet.toXMLString();
                XMLParser parser = new XMLParser();
                Element xml = parser.parse(xmlString);
                NodeSet nodes = xml.get("interface/Tunnel");
                ArrayList<String> list = new ArrayList<String>();

                for (Element node : nodes) {
                    list.add((String) node.getFirst("name").value);
                }
                return list.toArray(new String[list.size()]);
            }
        }
        catch (JNCException e) {
            throw(new IOException(e.toString()));
        }
    }

    /**
     * returns xml for running configuration and device state information
     * with the given path or null otherwise.
     */
    protected String getXML(String path) throws IOException {
        if (path == null || path.length() <= 0)
            return null;

        try {
            NodeSet set = session.get(path);
            if (set.size() == 0)
                return "";
            else
                return set.toXMLString();
        }
        catch (JNCException e) {
            throw(new IOException(e.toString()));
        }
    }

    public static void main(String args[]) {
        Map<String, Object> props;
        URI u;
        int i, timeout = 60, category = 0;
        String uri = null, str = null, path = null;
        String[] list;
        long size, mtime;
        NetconfConnector conn = null;
        SimpleDateFormat dateFormat;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zz");
        props = new HashMap<String, Object>();
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
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'x':
                if (i+1 < args.length)
                    path = args[++i];
                break;
              case 'c':
                if (i+1 < args.length)
                    category = Integer.parseInt(args[++i]);
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
        props.put("SOTimeout", String.valueOf(timeout));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(uri + ": " + e.toString()));
        }

        if (path == null || path.length() == 0)
            throw(new IllegalArgumentException("xpath not defined: " + uri));

        try {
            conn = new NetconfConnector(props);
            if (category == 1) { // for xml
                str = conn.getXML(path);
                if (str == null || str.length() <= 0)
                    System.out.println("failed to query xml for " + path);
                else
                    System.out.println(str);
            }
            else { // for tunnels
                list = conn.getTunnelNames(path);
                if (list == null)
                    System.out.println("failed to query tunnels for " + path);
                else {
                    System.out.println("found " + list.length + " tunnels");
                    for (i=0; i<list.length; i++)
                        System.out.println("tunnel " + i + ": " + list[i]);
                }
            }
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
        System.out.println("NetconfConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("NetconfConnector: a Netconf client to get info from a router");
        System.out.println("Usage: java org.qbroker.net.NetconfConnector -u uri -t timeout -n username -p password -x xpath");
        System.out.println("  -?: print this message");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -x: xpath");
        System.out.println("  -c: category, 0 for tunnel, 1 for xml");
        System.out.println("  -t: timeout in sec (default: 60)");
    }
}
