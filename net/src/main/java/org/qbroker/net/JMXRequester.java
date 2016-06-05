package org.qbroker.net;

/* JMXRequester.java - a generic JMX requester */

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.io.IOException;
import javax.management.ObjectName;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanInfo;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.JMException;
import org.qbroker.common.Utils;
import org.qbroker.common.Requester;
import org.qbroker.common.TraceStackThread;
import org.qbroker.common.WrapperException;

/**
 * JMXRequester connects to a generic JMX server for JMX requests. Currently,
 * it supports the methods of list, getValues and getResponse.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JMXRequester implements Requester {
    protected String uri;
    protected JMXConnector jmxc = null;
    protected boolean isConnected = false;
    private String username = null;
    private String password = null;

    /** Creates new JMXRequester */
    public JMXRequester() {
    }

    public JMXRequester(Map props) {
        Object o;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;
        if(!uri.startsWith("service:"))
            throw(new IllegalArgumentException("Bad service URL: " + uri));

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

    /** returns names of the all MBeans for the given pattern on the server */
    public String[] list(String target) throws JMException {
        ObjectName objName;
        String[] keys = null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else if (target == null || target.length() <= 0)
            objName = null;
        else try {
                objName = new ObjectName(target);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("bad formed target '" +
                target + "' for " + uri + ": " + e.toString()));
        }

        try {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Query MBean names
            Set names = mbsc.queryNames(objName, null);
            int n = names.size();
            keys = new String[n];
            n = 0;
            for (Object obj : names) {
                keys[n++] = ((ObjectName) obj).toString();
            }
        }
        catch (Exception e) {
            throw(new JMException("failed to list MBeans for " + target +
                " from " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        return keys;
    }

    /** returns a hashmap with MBeanInfo for the target */
    public Map<String, List> getInfo(String target) throws JMException {
        ObjectName objName;
        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("empty target  for " +
                uri));

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            objName = new ObjectName(target);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("bad formed target '" +
                target + "' for " + uri + ": " + e.toString()));
        }

        try {
            MBeanInfo mbInfo;
            Map<String, List> ph = new HashMap<String, List>();
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // get MBean info
            mbInfo = mbsc.getMBeanInfo(objName);
            if (mbInfo != null) {
                List<Map> list;
                Map<String, String> h;
                MBeanAttributeInfo[] aList = mbInfo.getAttributes();
                if (aList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<aList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", aList[i].getName());
                        h.put("type", aList[i].getType());
                        h.put("description", aList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Attribute", list);
                }
                MBeanOperationInfo[] oList = mbInfo.getOperations();
                if (oList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<oList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", oList[i].getName());
                        h.put("type", oList[i].getReturnType());
                        h.put("description", oList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Operation", list);
                }
                MBeanNotificationInfo[] nList = mbInfo.getNotifications();
                if (nList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<nList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", nList[i].getName());
                        h.put("type", nList[i].getNotifTypes()[0]);
                        h.put("description", nList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Notification", list);
                }
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get info for " + target+" from "+
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the value of the attribute from the target */
    public Object getValue(String target, String attr) throws JMException {
        ObjectName objName;
        if (target == null || target.length() <= 0 || attr == null ||
            attr.length() <= 0)
            throw(new IllegalArgumentException("empty target or attr for " +
                uri));

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            objName = new ObjectName(target);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("bad formed target '" +
                target + "' for " + uri + ": " + e.toString()));
        }

        try {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Display MBean attributes 
            return mbsc.getAttribute(objName, attr);
        }
        catch (Exception e) {
            throw(new JMException("failed to get value for " + target+" from "+
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the value of the attribute from the objName */
    public Object getValue(ObjectName objName, String attr) throws JMException {
        if (objName == null || attr == null || attr.length() <= 0)
            throw(new IllegalArgumentException("null objName or attr for " +
                uri));

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Display MBean attributes 
            return mbsc.getAttribute(objName, attr);
        }
        catch (Exception e) {
            throw(new JMException("failed to get value for "+objName.toString()+
                " form " + uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the key-value map for the attributes from the target */
    public Map<String, Object> getValues(String target, String[] attrs)
        throws JMException {
        ObjectName objName;
        if (target == null || target.length() <= 0 || attrs == null ||
            attrs.length <= 0)
            throw(new IllegalArgumentException("empty target or attr for " +
                uri));

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            objName = new ObjectName(target);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("bad formed target '" +
                target + "' for " + uri + ": " + e.toString()));
        }

        try {
            AttributeList list;
            Attribute attr;
            Map<String, Object> ph = new HashMap<String, Object>();
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Display MBean attributes 
            list = mbsc.getAttributes(objName, attrs);
            int n = list.size();
            for (int i=0; i<n; i++) {
                attr = (Attribute) list.get(i);
                ph.put(attrs[i], attr.getValue());
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get attribute list for " +
                target + " form " + uri +": "+TraceStackThread.traceStack(e)));
        }
    }

    /** returns the key-value map for the attributes from the objName */
    public Map<String, Object> getValues(ObjectName objName, String[] attrs)
        throws JMException {
        if (objName == null || attrs == null || attrs.length <= 0)
            throw(new IllegalArgumentException("null objName or attr for " +
                uri));

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            AttributeList list;
            Attribute attr;
            Map<String, Object> ph = new HashMap<String, Object>();
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Display MBean attributes 
            list = mbsc.getAttributes(objName, attrs);
            int n = list.size();
            for (int i=0; i<n; i++) {
                attr = (Attribute) list.get(i);
                ph.put(attrs[i], attr.getValue());
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get attribute list for " +
                objName.toString() + " from " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }
    }

    /**
     * This takes a JMX query command and converts it into a JMX request.
     * It sends the request to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection.
     *<br/></br/>
     * The JMX request is supposed to be as follows: 
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
    public int getResponse(String jmxCmd, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        String target, attrs, line = null;
        int k = -1;
        boolean isPattern = false;

        if (jmxCmd == null || jmxCmd.length() <= 0)
            throw(new IllegalArgumentException("empty request for " + uri));

        target = getTarget(jmxCmd);
        attrs = getAttributes(jmxCmd);
        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("bad request of " + jmxCmd +
                " for " + uri));

        if (strBuf == null)
            throw(new IllegalArgumentException("response buffer is null on " +
                target + " for " + uri));

        if (!isConnected) {
            String str;
            if ((str = reconnect()) != null) {
                throw(new WrapperException("JMX connection failed on " +
                    uri + ": " + str));
            }
        }

        String[] keys = null;
        if (target.indexOf('*') < 0 && target.indexOf('?') < 0) { // for a query
            keys = new String[]{target};
            isPattern = false;
        }
        else try { // for a list with pattern
            keys = list(target);
            if (keys == null)
                keys = new String[0];
            isPattern = true;
        }
        catch (JMException e) {
            close();
            throw(new WrapperException("failed to list on " + target +
                " from " + uri, e));
        }
        catch (Exception e) {
            if (autoDisconn)
                close();
            throw(new IllegalArgumentException(
               "failed to process list request on " + target + " from " +
               uri + ": " + TraceStackThread.traceStack(e)));
        }

        if ((k = strBuf.length()) > 0)
            strBuf.delete(0, k);

        if (attrs == null && attrs.length() <= 0 && isPattern) { // for a list
            k = 0;
            for (String key : keys) {
                line = "\"" + Utils.escapeJSON(key) + "\"";
                if (k++ > 0)
                    strBuf.append(",");
                strBuf.append(line + Utils.RS);
            }
        }
        else { // for a list or a specific MBean
            String[] a;
            if (attrs != null && attrs.length() > 0) { // for a list
                a = (attrs.indexOf(':') < 0) ? new String[]{attrs} :
                    Utils.split(":", attrs);
            }
            else try { // for a specific MBean
                a = getAllAttributes(target);
            }
            catch (JMException e) {
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

            k = 0;
            try {
                for (String key : keys) {
                    Map map = getValues(key, a);
                    line = Utils.toJSON(map);
                    if (line != null && line.length() > 0) {
                        if (k++ > 0)
                            strBuf.append(",");
                        strBuf.append(line + Utils.RS);
                    }
                }
            }
            catch (JMException e) {
                close();
                throw(new WrapperException("failed to query on " + keys[k] +
                    " from " + uri, e));
            }
            catch (Exception e) {
                if (autoDisconn)
                    close();
                throw(new IllegalArgumentException("failed to query on " +
                    keys[k] + " from " + uri + ": " +
                    TraceStackThread.traceStack(e)));
            }
        }

        strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
        strBuf.append("]}");
        if (autoDisconn)
            close();
        return k;
    }

    protected void connect() throws IOException {
        jmxc = null;
        isConnected = false;
        Map<String, Object> env = new HashMap<String, Object>();
        String[] credentials = new String[] {username, password};
        env.put("jmx.remote.credentials", credentials);

        // Create an RMI connector client
        try {
            JMXServiceURL url = new JMXServiceURL(uri);
            jmxc = JMXConnectorFactory.connect(url, env);
        }
        catch (Exception e) {
            throw(new IOException(TraceStackThread.traceStack(e)));
        }
        if (jmxc != null)
            isConnected = true;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public String getURI() {
        return uri;
    }

    /** returns null if reconnected or error msg otherwise */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (Exception e) {
            return e.toString();
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (jmxc != null) try {
            jmxc.close();
        }
        catch (Exception e) {
        }
        jmxc = null;
        isConnected = false;
    }

    /** returns the name of the target in the JMX command */
    public static String getTarget(String cmd) {
        int i, j, n;
        char c;
        String str;
        if (cmd == null || cmd.length() <= 7)
            return null;
        i = 0;
        n = cmd.length();
        while ((c = cmd.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = cmd.substring(i, i+8).toUpperCase();
        if ("DISPLAY ".equals(str)) { // for display
            i += 8;
            str = cmd.substring(i).trim();
            j = str.indexOf(' ');
            if (j > 0)
                return str.substring(0, j).trim();
            else if (str.length() > 0)
                return str;
        }
        return null;
    }

    /** returns the attributes for the target in the JMX command */
    public static String getAttributes(String cmd) {
        int i, j, n;
        char c;
        String str;
        if (cmd == null || cmd.length() <= 7)
            return null;
        i = 0;
        n = cmd.length();
        while ((c = cmd.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = cmd.substring(i, i+8).toUpperCase();
        if ("DISPLAY ".equals(str)) { // for display
            i += 8;
            str = cmd.substring(i).trim();
            j =  str.indexOf(" ");
            if (j > 0) {
                str = str.substring(j).trim();
                if (str.length() > 0)
                    return str;
            }
        }
        return null;
    }

    /** returns names of all attributes for the target */
    public String[] getAllAttributes(String target) throws JMException {
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

    public static void main(String[] args) {
        String url = null;
        String username = null;
        String password = null;
        String target = null;
        String attr = null;
        String[] keys;
        JMXRequester jmx = null;
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
            jmx = new JMXRequester(ph);
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
                if ((list = pl.get("Attribute")) != null) {
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
                if ((list = pl.get("Operation")) != null) {
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
                if ((list = pl.get("Notification")) != null) {
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
            if (jmx != null)
                jmx.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (jmx != null)
                jmx.close();
        }
    }

    private static void printUsage() {
        System.out.println("JMXRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("JMXRequester: list all MBeans on a JMX Server");
        System.out.println("Usage: java org.qbroker.net.JMXRequester -u url -n username -p password -t target -a attributes");
        System.out.println("  -?: print this message");
        System.out.println("  u: service url");
        System.out.println("  n: username");
        System.out.println("  p: password");
        System.out.println("  t: target");
        System.out.println("  a: attributes delimited via ':'");
    }
}
