package org.qbroker.net;

/* CQLConnector.java - a Cassandra connector for data of keyspaces */

import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Date;
import java.text.SimpleDateFormat;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.exceptions.DriverException;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * CQLConnector connects to a Cassandra cluster and provides some basic CQL
 * operation methods: select(), and execute().
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class CQLConnector implements Connector {
    protected String hostname;
    protected String keyspace = null;
    protected String username = null;
    protected String password = null;
    protected String uri;
    protected int port = 9042;
    protected int timeout = 120000;
    protected boolean isConnected = false;
    public static final int ACTION_LIST = 0;
    public static final int ACTION_COUNT = 1;
    public static final int ACTION_SELECT = 2;
    public static final int ACTION_UPDATE = 3;
    public static final int ACTION_DELETE = 4;
    public static final String FS = " | ";
    public static final String RS = "\n";
    private static final DataType CINT = DataType.cint();
    private static final DataType CLONG = DataType.bigint();
    private static final DataType CFLOAT = DataType.cfloat();
    private static final DataType CDOUBLE = DataType.cdouble();
    private static final DataType CUUID = DataType.uuid();
    private static final DataType CTEXT = DataType.text();
    private static final DataType CDATE = DataType.timestamp();

    protected Session session = null;
    private Cluster cluster = null;
    private int bufferSize = 4096;

    /** Creates new CQLConnector */
    public CQLConnector(Map props) {
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

        if (!"jdbc".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        try {
            u = new URI(uri.substring(5));
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"cassandra".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported subscheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 9042;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((keyspace = u.getPath()) == null || keyspace.length() == 0)
            throw(new IllegalArgumentException("no keyspace specified in URI"));
        keyspace = keyspace.substring(1);

        if ((o = props.get("Username")) == null)
            username = null;
        else
            username = (String) o;

        if ((o = props.get("Password")) == null)
            password = null;
        else
            password = (String) o;

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        if (timeout <= 0)
            timeout = 120000;

        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
            if (bufferSize <= 0)
                bufferSize = 4096;
        }

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) {
            String err = reconnect();
            if (err != null)
                throw(new IllegalArgumentException("failed to connect to " +
                    uri + ": " + err));
        }
    }

    /** it connects to a Cassandra cluster and establishes the session */
    private void connect() throws DriverException {
        cluster = Cluster.builder()
            .addContactPoint(hostname)
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withLoadBalancingPolicy(
                new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
            .withPort(port)
            .build();
        if (keyspace != null && keyspace.length() > 0)
            session = cluster.connect(keyspace);
        else
            session = cluster.connect();
        if (session != null)
            isConnected = true;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** It reconnects and returns null or error message upon failure */
    public String reconnect() {
        try {
            close();
        }
        catch (Exception e) {
        }
        try {
            connect();
        }
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    /** returns a string buffer with info on all columns for a specific table */
    public StringBuffer list(String table, int[] rc) throws DriverException {
        if (table == null || table.length() <= 0)
            return null;
        if (rc == null || rc.length < 2)
            rc = new int[2];

        final Metadata mdata = cluster.getMetadata();
        final KeyspaceMetadata kdata = mdata.getKeyspace(keyspace);
        final TableMetadata tdata = kdata.getTable(table);
        StringBuffer strBuf = getResult(tdata.getColumns(), rc);
        if (strBuf != null && rc[0] > 0) {
            int[] rb = new int[2];
            strBuf.append("\n\nPartitionKey:\n" +
                getResult(tdata.getPartitionKey(), rb).toString());
            StringBuffer sb = getResult(tdata.getClusteringColumns(), rb);
            if (sb != null && rb[0] > 0)
                strBuf.append("\n\nClusteringKey:\n" + sb.toString());
        }
        return strBuf;
    }

    /** returns selected results in string buffer for the query */
    public StringBuffer select(int rt, String cqlStr, int[] rc)
        throws DriverException {
        if (cqlStr == null || cqlStr.length() <= 0)
            return null;
        if (rc == null || rc.length < 2)
            rc = new int[2];

        final PreparedStatement ps = session.prepare(cqlStr);
        final ResultSet rset = session.execute(new BoundStatement(ps));
        return getResult(rt, rset, rc);
    }

    /** returns null for success or error msg for failure otherwise */
    public String update(String cqlStr) {
        if (cqlStr == null || cqlStr.length() <= 0)
            return "empty query";

        try {
            final PreparedStatement ps = session.prepare(cqlStr);
            session.execute(new BoundStatement(ps));
        }
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    /** closes and shutdown the Cassandra cluster */
    public void close() {
        if (isConnected)
            isConnected = false;
        if (session != null) try {
            session.close();
            session = null;
        }
        catch (Exception e) {
        }
        if (cluster != null)  try {
            cluster.close();
            cluster = null;
        }
        catch (Exception e) {
        }
    }

    /**
     * It gets all the rows from the ReseulSet and packs them into a
     * StringBuffer according to the ResultType.  The int array of rc
     * should have at least two numbers.  The first is for the number of
     * rows.  The second is for the number of columns.
     *<br/>
     * If any returned string is null, it will be reset into an empty string.
     */
    public static StringBuffer getResult(int type, ResultSet rset, int[] rc)
        throws DriverException {
        ColumnDefinitions cinfo;
        StringBuffer strBuf = new StringBuffer();
        String key, str;
        DataType t;
        int i, n;
        if (rset == null)
            return null;

        rc[0] = 0;
        rc[1] = 0;
        if ((type & 4) > 0) {
            StringBuffer line;
            for (Row row : rset) {
                line = new StringBuffer();
                line.append("<Record type=\"ARRAY\">");
                cinfo = row.getColumnDefinitions();
                n = cinfo.size();
                if (n <= 0)
                    continue;
                for (i=0; i<n; i++) {
                    t = cinfo.getType(i);
                    if (t.equals(CUUID))
                        str = row.getUUID(i).toString();
                    else if (t.equals(CINT))
                        str = String.valueOf(row.getInt(i));
                    else if (t.equals(CLONG))
                        str = String.valueOf(row.getLong(i));
                    else if (t.equals(CFLOAT))
                        str = String.valueOf(row.getFloat(i));
                    else if (t.equals(CDOUBLE))
                        str = String.valueOf(row.getDouble(i));
                    else if (t.equals(CDATE))
                        str = row.getDate(i).toString();
                    else
                        str = row.getString(i);
                    key = cinfo.getName(i);
                    line.append("<" + key + ">");
                    if (str != null)
                        line.append(escapeXML(str));
                    line.append("</" + key + ">");
                }
                line.append("</Record>");
                strBuf.append(line.toString() + RS);
                rc[0] ++;
            }
        }
        else if ((type & 8) > 0) {
            StringBuffer line;
            strBuf.append("[");
            for (Row row : rset) {
                line = new StringBuffer();
                cinfo = row.getColumnDefinitions();
                n = cinfo.size();
                if (n <= 0)
                    continue;
                if (rc[0] > 0)
                    line.append(",");
                line.append(RS + "{");
                for (i=0; i<n; i++) {
                    t = cinfo.getType(i);
                    if (t.equals(CUUID))
                        str = row.getUUID(i).toString();
                    else if (t.equals(CINT))
                        str = String.valueOf(row.getInt(i));
                    else if (t.equals(CLONG))
                        str = String.valueOf(row.getLong(i));
                    else if (t.equals(CFLOAT))
                        str = String.valueOf(row.getFloat(i));
                    else if (t.equals(CDOUBLE))
                        str = String.valueOf(row.getDouble(i));
                    else if (t.equals(CDATE))
                        str = row.getDate(i).toString();
                    else
                        str = row.getString(i);
                    key = cinfo.getName(i);
                    if (i > 0)
                        line.append(",");
                    line.append("\"" + key + "\":");
                    if (str == null)
                        line.append("null");
                    else
                        line.append("\"" + escapeJSON(str) + "\"");
                }
                line.append("}");
                strBuf.append(line.toString());
                rc[0] ++;
            }
            strBuf.append(RS + "]");
        }
        else {
            StringBuffer line;
            for (Row row : rset) {
                line = new StringBuffer();
                cinfo = row.getColumnDefinitions();
                n = cinfo.size();
                if (n <= 0)
                    continue;
                for (i=0; i<n; i++) {
                    t = cinfo.getType(i);
                    if (t.equals(CUUID))
                        str = row.getUUID(i).toString();
                    else if (t.equals(CINT))
                        str = String.valueOf(row.getInt(i));
                    else if (t.equals(CLONG))
                        str = String.valueOf(row.getLong(i));
                    else if (t.equals(CFLOAT))
                        str = String.valueOf(row.getFloat(i));
                    else if (t.equals(CDOUBLE))
                        str = String.valueOf(row.getDouble(i));
                    else if (t.equals(CDATE))
                        str = row.getDate(i).toString();
                    else
                        str = row.getString(i);
                    if (i > 0)
                        line.append(FS);
                    if (str != null)
                        line.append(str);
                }
                strBuf.append(line.toString() + RS);
                rc[0] ++;
            }
        }

        return strBuf;
    }

    /**
     * It gets all the rows from the list of ColumnMetadata and packs them into
     * a StringBuffer according to the ResultType.  The int array of rc
     * should have at least two numbers.  The first is for the number of
     * rows.  The second is for the number of columns.
     *<br/>
     * If any returned string is null, it will be reset into an empty string.
     */
    public static StringBuffer getResult(List<ColumnMetadata> list, int[] rc)
        throws DriverException {
        StringBuffer strBuf = new StringBuffer();
        String key, str;
        if (list == null)
            return null;

        rc[0] = 0;
        rc[1] = 0;
        for (ColumnMetadata data : list) {
            key = data.getName();
            str = data.getType().getName().toString();
            if (rc[0] > 0)
                strBuf.append("," + RS);
            strBuf.append(key + " " + str);
            rc[0] ++;
        }

        return strBuf;
    }

    public static String escapeJSON(String text) {
        int i;
        String[] escaped = new String[] {"\\\\", "\\n", "\\r", "\\\"", "\\b",
            "\\f", "\\t"};
        String[] unescaped = new String[] {"\\", "\n", "\r", "\"", "\b", "\f",
            "\t"};
        if (text == null || text.length() <= 0)
            return text;
        for (i=0; i<escaped.length; i++)
            text = doSearchReplace(unescaped[i], escaped[i], text);
        return text;
    }

    public static String escapeXML(String text) {
        int i;
        String[] escaped = new String[] {"&amp;", "&lt;", "&gt;", "&quot;",
            "&apos;"};
        String[] unescaped = new String[] {"&", "<", ">", "\"", "'"};
        if (text == null || text.length() <= 0)
            return text;
        for (i=0; i<escaped.length; i++)
            text = doSearchReplace(unescaped[i], escaped[i], text);
        return text;
    }

    public static String doSearchReplace(String s, String r, String text) {
        int len, i, j, k, d;

        if (s == null || (len = s.length()) == 0 || (i  = text.indexOf(s)) < 0)
            return text;

        len = s.length();
        d = r.length() - len;
        j = i;
        k = i;
        StringBuffer buffer = new StringBuffer(text);
        while (i >= j) {
            k += i - j;
            buffer.replace(k, k+len, r);
            k += d;
            j = i;
            i = text.indexOf(s, j+len);
        }
        return buffer.toString();
    }

    /** tests Cassandra basic operations */
    public static void main(String args[]) {
        HashMap<String, Object> props;
        URI u;
        int i, timeout = 60, action = ACTION_SELECT, rt = 1;
        String keyspaceName = null, uri = null, str = null, cqlStr = null;
        String table = null;
        StringBuffer strBuf;
        String[] list;
        int[] rc = new int[2];
        long size, mtime;
        CQLConnector conn = null;
        SimpleDateFormat dateFormat;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
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
              case 'a':
                str = args[++i];
                if ("count".equalsIgnoreCase(str))
                    action = ACTION_COUNT;
                else if ("select".equalsIgnoreCase(str))
                    action = ACTION_SELECT;
                else if ("update".equalsIgnoreCase(str))
                    action = ACTION_UPDATE;
                else if ("delete".equalsIgnoreCase(str))
                    action = ACTION_DELETE;
                else if ("list".equalsIgnoreCase(str))
                    action = ACTION_LIST;
                else
                    action = ACTION_LIST;
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
              case 'q':
                if (i+1 < args.length)
                    cqlStr = args[++i];
                break;
              case 'r':
                str = args[++i];
                if ("json".equalsIgnoreCase(str))
                    rt = 8;
                else if ("xml".equalsIgnoreCase(str))
                    rt = 4;
                else
                    rt = 1;
                break;
              case 'T':
                if (i+1 < args.length)
                    table = args[++i];
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

        if (action != ACTION_LIST && cqlStr == null)
            throw(new IllegalArgumentException("query is not specified"));
        else if (action == ACTION_LIST && table == null)
            throw(new IllegalArgumentException("table is not specified"));

        try {
            conn = new CQLConnector(props);
            str = null;
            switch (action) {
              case ACTION_LIST:
                strBuf = conn.list(table, rc);
                if (strBuf != null)
                    System.out.println(table + " has " + rc[0] + " columns:\n"+
                        strBuf.toString());
                else
                    str = "list failed";
                break;
              case ACTION_SELECT:
                strBuf = conn.select(rt, cqlStr, rc);
                if (strBuf != null)
                    System.out.println("selected " + rc[0] + " rows:\n" +
                        strBuf.toString());
                else
                    str = "query failed";
                break;
              case ACTION_UPDATE:
                str = conn.update(cqlStr);
                if (str == null)
                    System.out.println("update succeded");
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
        System.out.println("CQLConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("CQLConnector: a Cassandra client");
        System.out.println("Usage: java org.qbroker.net.CQLConnector -u uri -t timeout -q query -r result_type -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of list, select, update, delete (default: list)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -q: CQL query statement");
        System.out.println("  -r: result type");
        System.out.println("  -T: table name to list columns");
    }
}
