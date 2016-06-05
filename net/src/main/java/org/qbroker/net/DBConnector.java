package org.qbroker.net;

/* DBConnector.java - a JDBC Database connector */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.SQLException;
import org.qbroker.common.Connector;
import org.qbroker.common.SQLUtils;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;

/**
 * DBConnector connects to a database specified by URI and initializes the
 * operations of select and update, etc, for SQL statements.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DBConnector implements Connector {
    protected String uri;
    protected int resultType = Utils.RESULT_TEXT;
    protected int sqlExecTimeout;
    private int timeout;
    private String username;
    private String password;
    private Connection conn = null;
    private boolean isConnected = false;

    public DBConnector(Map props) {
        Object o;
        String dbDriver;
        
        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        if ((o = props.get("DBDriver")) != null)
            dbDriver = (String) o;
        else try {
            String ssp;
            URI u = new URI(uri);
            ssp = u.getSchemeSpecificPart();
            if (ssp == null)
                throw(new IllegalArgumentException("uri is not well defined"));
            else if (ssp.startsWith("oracle"))
                dbDriver = "oracle.jdbc.driver.OracleDriver";
            else if (ssp.startsWith("mysql"))
                dbDriver = "com.mysql.jdbc.Driver";
            else if (ssp.startsWith("postgresql"))
                dbDriver = "postgresql.Driver";
            else if (ssp.startsWith("microsoft"))
                dbDriver = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
            else if (ssp.startsWith("db2"))
                dbDriver = "com.ibm.db2.jcc.DB2Driver";
            else
                throw(new IllegalArgumentException("DBDriver is not defined"));
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (dbDriver == null || dbDriver.length() <= 0)
            throw(new IllegalArgumentException("DBDriver is not defined"));

        if ((o = props.get("Username")) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        username = (String) o;

        if ((o = props.get("Password")) == null)
            throw(new IllegalArgumentException("Password is not defined"));
        password = (String) o;

        if ((o = props.get("ResultType")) != null)
            resultType = Integer.parseInt((String) o);

        if ((o = props.get("DBTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 30000;
        
        /* This is the timeout for how long the sql query takes to execute.
         * It is in seconds, not milliseconds like the one above.
         */
        if ((o = props.get("SQLExecTimeout")) != null)
            sqlExecTimeout = Integer.parseInt((String) o);
        else
            sqlExecTimeout = -1;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) try {
            Class.forName(dbDriver);
            DriverManager.setLoginTimeout(timeout);
            conn = DriverManager.getConnection(uri, username, password);
            isConnected = true;
        }
        catch (ClassNotFoundException e) {
            throw(new IllegalArgumentException("class not found: " +
                TraceStackThread.traceStack(e)));
        }
        catch (SQLException e) {
            throw(new IllegalArgumentException("SQLException: " +
                TraceStackThread.traceStack(e)));
        }
    }

    /**
     * It assumes the SQL statement querying on a single column of BLOB.
     * It executes the query and copies the bytes stream into the given out.
     * Upon success, it returns the number of bytes copied.
     */
    public int copyStream(String sqlStr, OutputStream out) throws IOException {
        long size = -1;
        PreparedStatement ps = null;
        ResultSet rset = null;

        if (sqlStr == null || sqlStr.length() <= 0 || out == null)
            return -1;

        try {
            byte[] buffer = new byte[8192];
            ps = conn.prepareStatement(sqlStr,ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
            if (sqlExecTimeout >= 0)
                ps.setQueryTimeout(sqlExecTimeout);
            rset = ps.executeQuery(); 
            rset.next();
            size = Utils.copyStream(rset.getBinaryStream(1), out, buffer, 0);
            rset.close();
            rset = null;
            ps.close();
            ps = null;
        }
        catch (SQLException e) {
            if (rset != null) try {
                rset.close();
            }
            catch (Exception ex) {
            }
            if (ps != null) try {
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(new IOException("failed to execute the query: " +
                TraceStackThread.traceStack(e)));
        }
        catch (IOException e) {
            if (rset != null) try {
                rset.close();
            }
            catch (Exception ex) {
            }
            if (ps != null) try {
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(e);
        }
        catch (Exception e) {
            if (rset != null) try {
                rset.close();
            }
            catch (Exception ex) {
            }
            if (ps != null) try {
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(new IOException("failed to execute the query: " +
                TraceStackThread.traceStack(e)));
        }

        return (int) size;
    }

    /**
     * It assumes the SQL statement updating on a single column of BLOB.
     * It executes the query and copies the bytes stream from the given in.
     * Upon success, it returns the number of bytes copied.
     */
    public int copyStream(int len, InputStream in, String sqlStr)
        throws IOException {
        int rc = -1;
        PreparedStatement ps = null;

        if (sqlStr == null || sqlStr.length() <= 0 || in == null)
            return -1;

        try {
            ps = conn.prepareStatement(sqlStr,ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
            if (sqlExecTimeout >= 0)
                ps.setQueryTimeout(sqlExecTimeout);
            ps.setBinaryStream(1, in, len);
            rc = ps.executeUpdate();
            ps.close();
            ps = null;
        }
        catch (SQLException e) {
            if (ps != null) try {
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(new IOException("failed to execute the query: " +
                TraceStackThread.traceStack(e)));
        }
        catch (Exception e) {
            if (ps != null) try {
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(new IOException("failed to execute the query: " +
                TraceStackThread.traceStack(e)));
        }

        return rc;
    }

    /** returns selected result in string buffer for the query */
    public StringBuffer select(String sqlStr, int[] rc) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rset = null;
        StringBuffer strBuf = null;

        if (sqlStr == null || sqlStr.length() <= 0)
            return null;
        if (rc == null || rc.length < 2)
            rc = new int[2];

        ps = conn.prepareStatement(sqlStr, ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        if (sqlExecTimeout >= 0)
            ps.setQueryTimeout(sqlExecTimeout);

        rset = ps.executeQuery();

        try {
            strBuf = SQLUtils.getResult(resultType, rset, rc);
        }
        catch (IOException e) {
            try {
                rset.close();
                ps.close();
            }
            catch (Exception ex) {
            }
            throw(new SQLException(TraceStackThread.traceStack(e)));
        }

        rset.close();
        ps.close();

        return strBuf;
    }

    /** returns the number of rows changed for update, insert and delete only */
    public int executeQuery(String sqlStr) throws SQLException {
        PreparedStatement ps = null;
        int n = 0;

        if (sqlStr == null || sqlStr.length() <= 0)
            return -1;

        ps = conn.prepareStatement(sqlStr);
        if (sqlExecTimeout >= 0)
            ps.setQueryTimeout(sqlExecTimeout);

        n = ps.executeUpdate();
        ps.close();

        return n;
    }

    /** returns a string buffer with info on all objects for the types */
    public StringBuffer list(String[] types, int[] rc) throws SQLException {
        ResultSet rset = null;
        DatabaseMetaData dbm = null;
        StringBuffer strBuf = null;

        if (types == null || types.length <= 0 || (types.length == 1 &&
            (types[0] == null || types[0].length() <= 0)))
            types = new String[]{"TABLE"};

        dbm = conn.getMetaData();
        rset = dbm.getTables(null, null, "%", types);

        try {
            strBuf = SQLUtils.getResult(resultType, rset, rc);
        }
        catch (IOException e) {
            try {
                rset.close();
            }
            catch (Exception ex) {
            }
            throw(new SQLException(TraceStackThread.traceStack(e)));
        }
        rset.close();

        return strBuf;
    }

    /** returns the list of all objects for the specific types */
    public List<Object> list(String pattern, String[] types)
        throws SQLException {
        ResultSet rset = null;
        DatabaseMetaData dbm = null;
        List<Object> list = new ArrayList<Object>();
        int n = -1;

        if (pattern == null || pattern.length() <= 0)
            pattern = "%";

        if (types == null || types.length <= 0 || (types.length == 1 &&
            (types[0] == null || types[0].length() <= 0)))
            types = new String[]{"TABLE"};

        dbm = conn.getMetaData();
        rset = dbm.getTables(null, null, pattern, types);

        try {
            n = SQLUtils.getResult(Utils.RESULT_XML, rset, list);
        }
        catch (IOException e) {
            try {
                rset.close();
            }
            catch (Exception ex) {
            }
            throw(new SQLException(TraceStackThread.traceStack(e)));
        }
        rset.close();

        return list;
    }

    /** returns a string buffer with info on all columns for a specific table */
    public StringBuffer list(String table, int[] rc) throws SQLException {
        ResultSet rset = null;
        DatabaseMetaData dbm = null;
        StringBuffer strBuf = null;

        if (table == null || table.length() <= 0)
            return null;

        dbm = conn.getMetaData();
        rset = dbm.getColumns(null, null, table, "%");

        try {
            strBuf = SQLUtils.getResult(resultType, rset, rc);
        }
        catch (IOException e) {
            try {
                rset.close();
            }
            catch (Exception ex) {
            }
            throw(new SQLException(TraceStackThread.traceStack(e)));
        }
        rset.close();

        return strBuf;
    }

    private void connect() throws SQLException {
        isConnected = false;
        conn = DriverManager.getConnection(uri, username, password);
        isConnected = true;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** reconnects to the DB and returns null on success or err msg otherwise */
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

    public void commit() throws SQLException {
        conn.commit();
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void setAutoCommit(boolean ac) throws SQLException {
        conn.setAutoCommit(ac);
    }

    /** returns the prepared statement */
    public PreparedStatement prepareStatement(String str) throws SQLException {
        return conn.prepareStatement(str);
    }

    public void close() {
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        conn = null;
        isConnected = false;
    }

    public static void main(String args[]) {
        Map<String, Object> props;
        int i, timeout = 60;
        int[] rc = new int[]{-1, 0};
        String action = "list", uri = null, str = null, sqlStr = null;
        String table = null;
        DBConnector conn = null;
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
              case 'r':
                str = args[++i];
                if ("json".equalsIgnoreCase(str))
                    props.put("ResultType", String.valueOf(Utils.RESULT_JSON));
                else if ("xml".equalsIgnoreCase(str))
                    props.put("ResultType", String.valueOf(Utils.RESULT_XML));
                else
                    props.put("ResultType", String.valueOf(Utils.RESULT_TEXT));
                break;
              case 'a':
                action = args[++i];
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
              case 'd':
                if (i+1 < args.length)
                    props.put("DBDriver", args[++i]);
                break;
              case 'q':
                if (i+1 < args.length)
                    sqlStr = args[++i];
                break;
              case 'T':
                if (i+1 < args.length)
                    table = args[++i];
                break;
              default:
            }
        }

        if (sqlStr == null && !"list".equalsIgnoreCase(action)) {
            System.out.println("SQL Query is not defined");
            System.exit(0);
        }

        if (uri == null) {
            printUsage();
            System.exit(0);
        }
        else
            props.put("URI", uri);

        try {
            StringBuffer strBuf = null;
            conn = new DBConnector(props);
            if ("list".equalsIgnoreCase(action) && table == null) {
                strBuf = conn.list(new String[]{"TABLE"}, rc);
                if (strBuf == null)
                    str = action + " failed";
                else if (rc[0] <= 0)
                    str = "no tables found";
                else
                    str = action + "ed " + rc[0] + " tables:\n" +
                        strBuf.toString();
            }
            else if ("list".equalsIgnoreCase(action)) {
                strBuf = conn.list(table, rc);
                if (strBuf == null)
                    str = action + " failed";
                else if (rc[0] <= 0)
                    str = "no such table: " + table;
                else
                    str = table + " has " + rc[0] + " columns:\n" +
                        strBuf.toString();
            }
            else if ("select".equalsIgnoreCase(action)) {
                strBuf = conn.select(sqlStr, rc);
                if (strBuf == null)
                    str = action + " failed";
                else
                    str = action + "ed " + rc[0] + " rows:\n" +
                        strBuf.toString();
            }
            else {
                i = conn.executeQuery(sqlStr);
                if (i < 0)
                    str = action + " failed: " + rc[0];
                else
                    str = action + "d " + i + " rows";
            }
            System.out.println(str);
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
        System.out.println("DBConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("DBConnector: a JDBC client for DB operations");
        System.out.println("Usage: java org.qbroker.net.DBConnector -u uri -t timeout -n username -p password -d driver -a action -r TEXT -q query");
        System.out.println("  -?: print this message");
        System.out.println("  -r: set result type (default: text)");
        System.out.println("  -a: action (default: select)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -d: DB driver name (optional)");
        System.out.println("  -q: SQL query statement");
        System.out.println("  -T: table name to list columns");
    }
}
