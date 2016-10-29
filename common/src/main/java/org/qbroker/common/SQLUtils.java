package org.qbroker.common;

/* SQLUtils.java - utilities for JDBC SQL operations */

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Clob;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;

/**
 * SQLUtils contains a bunch of constants and static methods on SQL operations
 * for common uses
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SQLUtils {
    public static final int SQL_NULL = 0;
    public static final int SQL_BYTE = 1;
    public static final int SQL_BYTES = 2;
    public static final int SQL_STRING = 3;
    public static final int SQL_INT = 4;
    public static final int SQL_LONG = 5;
    public static final int SQL_FLOAT = 6;
    public static final int SQL_DOUBLE = 7;
    public static final int SQL_DATE = 8;
    public static final int SQL_CLOB = 9;
    public static final int SQL_BLOB = 10;
    public static final int SQL_TIME = 11;
    public static final int SQL_TIMESTAMP = 12;
    public static final int DB_NONE = 0;
    public static final int DB_ORACLE = 1;
    public static final int DB_DB2 = 2;
    public static final int DB_SYBASE = 3;
    public static final int DB_MSSQL = 4;
    public static final int DB_TERADATA = 5;
    public static final int DB_POSTGRES = 6;
    public static final int DB_MYSQL = 7;
    protected static final int MAXLENGTH = Integer.MAX_VALUE;
    private static final Map<String, String> typeMap;
    private static final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public SQLUtils() {
    }

    static {
        typeMap = new HashMap<String, String>();
        typeMap.put("NULL", String.valueOf(SQL_NULL));
        typeMap.put("BYTE", String.valueOf(SQL_BYTE));
        typeMap.put("BYTES", String.valueOf(SQL_BYTES));
        typeMap.put("STRING", String.valueOf(SQL_STRING));
        typeMap.put("INT", String.valueOf(SQL_INT));
        typeMap.put("LONG", String.valueOf(SQL_LONG));
        typeMap.put("FLOAT", String.valueOf(SQL_FLOAT));
        typeMap.put("DOUBLE", String.valueOf(SQL_DOUBLE));
        typeMap.put("DATE", String.valueOf(SQL_DATE));
        typeMap.put("CLOB", String.valueOf(SQL_CLOB));
        typeMap.put("BLOB", String.valueOf(SQL_BLOB));
        typeMap.put("TIME", String.valueOf(SQL_TIME));
        typeMap.put("TIMESTAMP", String.valueOf(SQL_TIMESTAMP));
    }

    /**
     * It parses the SQL text to look for any named parameters.  A named
     * paramenter is supposed to be like %%Name%% or %%Type:Name%% where
     * Name is the parameter name and Type is its data type.  If there is
     * no Type defined, SQL_STRING will be used as the default.  All the
     * parameters will be transformed into "?" and be stored into a String
     * array according to their original order appearing in the SQL text.
     * The array will be put into the Map in the key of Sequence. Their
     * data types will be stored in an int array and be put into the Map
     * in the key of Type.  The transformed SQL text will be returned.
     *<br/><br/>
     * In case there is no named parameters defined, the original SQL text
     * will be returned with no changes to the Map.
     */
    @SuppressWarnings("unchecked")
    public static String getParamInfo(String sqlText, Map map) {
        Template temp = new Template(sqlText, "%%[^%]+%%");
        String[] list = temp.getSequence();
        int[] type = new int[list.length];
        String str;
        if (list.length > 0) {
            Object o;
            int i, j;
            for (i=0; i<list.length; i++) {
                j = list[i].indexOf(":");
                if (j < 0) {
                    type[i] = SQL_STRING;
                    continue;
                }
                str = list[i].substring(0, j);
                o = typeMap.get(str.toUpperCase());
                if (o == null)
                    o = "0";

                list[i] = list[i].substring(j+1);
                type[i] = Integer.parseInt((String) o);
            }
            map.put("Sequence", list);
            map.put("Type", type);
            list = temp.getAllFields();
            str = new String(sqlText);
            for (i=0; i<list.length; i++)
                str = temp.substitute(null, list[i], "?", str);
        }
        else {
            str = sqlText;
        }
        return str;
    }

    /**
     * It translates the data in String into the right data type and set
     * the value according to its index on the PreparedStatement.
     *<br/>
     * Due to MT-safety issue with SimpleDateFormat, please provide your own
     * instance of SimpleDateFormat.
     */
    public static void setParameter(int id, int type, PreparedStatement stmt,
        String data, SimpleDateFormat df) throws SQLException {
        switch (type) {
          case SQL_BYTE:
            stmt.setByte(id, data.getBytes()[0]);
            break;
          case SQL_BYTES:
            stmt.setBytes(id, data.getBytes());
            break;
          case SQL_INT:
            stmt.setInt(id, Integer.parseInt(data));
            break;
          case SQL_LONG:
            stmt.setLong(id, Long.parseLong(data));
            break;
          case SQL_DOUBLE:
            stmt.setDouble(id, Double.parseDouble(data));
            break;
          case SQL_FLOAT:
            stmt.setFloat(id, Float.parseFloat(data));
            break;
          case SQL_STRING:
            stmt.setString(id, data);
            break;
          case SQL_DATE:
            if (df == null) // default
                df = dateFormat;
            stmt.setDate(id,
                new Date(df.parse(data, new ParsePosition(0)).getTime()));
            break;
          case SQL_TIME:
            if (df == null) // default
                df = dateFormat;
            stmt.setTime(id,
                new Time(df.parse(data, new ParsePosition(0)).getTime()));
            break;
          case SQL_TIMESTAMP:
            if (df == null) // default
                df = dateFormat;
            stmt.setTimestamp(id,
                new Timestamp(df.parse(data, new ParsePosition(0)).getTime()));
            break;
          case SQL_BLOB:
          case SQL_CLOB:
          default:
            break;
        }
    }

    /** It is not MT-safe due to sharing the default dateFormat */
    public static void setParameter(int id, int type, PreparedStatement stmt,
        String data) throws SQLException {
        setParameter(id, type, stmt, data, dateFormat);
    }

    /**
     * It gets all the records from the ReseulSet and packs them into a
     * StringBuffer according to the ResultType.  The int array of rc
     * should have at least two numbers.  The first is for the number of
     * records.  The second is for the number of columns.
     *<br/>
     * If any returned string is null, it will be reset into an empty string.
     */
    public static StringBuffer getResult(int type, ResultSet rset, int[] rc)
        throws SQLException, IOException {
        StringBuffer strBuf = new StringBuffer();
        String str;
        int i, n;
        if (rset == null)
            return null;

        ResultSetMetaData rsmd = rset.getMetaData();
        n = rsmd.getColumnCount();
        rc[0] = 0;
        rc[1] = n;
        String[] keys = new String[n]; 
        int[] dataType = new int[n];
        for (i=1; i<=n; i++) {
            dataType[i-1] = rsmd.getColumnType(i);
            str = rsmd.getColumnLabel(i);
            keys[i-1] = (str != null && str.length() > 0) ?
                str : rsmd.getColumnName(i);
        }
        if ((type & Utils.RESULT_XML) > 0) {
            StringBuffer line;
            while (rset.next()) {
                line = new StringBuffer();
                line.append("<Record type=\"ARRAY\">");
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromCLOB(rset.getClob(i), sb);
                        str = sb.toString();
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromBLOB(rset.getBlob(i), sb);
                        str = sb.toString();
                    }
                    else {
                        str = rset.getString(i);
                    }
                    line.append("<" + keys[i-1] + ">");
                    if (str != null)
                        line.append(Utils.escapeXML(str));
                    line.append("</" + keys[i-1] + ">");
                }
                line.append("</Record>");
                strBuf.append(line.toString() + Utils.RS);
                rc[0] ++;
            }
        }
        else if ((type & Utils.RESULT_JSON) > 0) {
            StringBuffer line;
            strBuf.append("[");
            while (rset.next()) {
                line = new StringBuffer();
                if (rc[0] > 0)
                    line.append(",");
                line.append("{");
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromCLOB(rset.getClob(i), sb);
                        str = sb.toString();
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromBLOB(rset.getBlob(i), sb);
                        str = sb.toString();
                    }
                    else {
                        str = rset.getString(i);
                    }
                    if (i > 1)
                        line.append(",");
                    line.append("\"" + keys[i-1] + "\":");
                    if (str == null)
                        line.append("null");
                    else
                        line.append("\"" + Utils.escapeJSON(str) + "\"");
                }
                line.append("}");
                strBuf.append(line.toString() + Utils.RS);
                rc[0] ++;
            }
            strBuf.append("]");
        }
        else {
            StringBuffer line;
            while (rset.next()) {
                line = new StringBuffer(); 
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromCLOB(rset.getClob(i), sb);
                        str = sb.toString();
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromBLOB(rset.getBlob(i), sb);
                        str = sb.toString();
                    }
                    else {
                        str = rset.getString(i);
                    }
                    if (i > 1)
                        line.append(Utils.FS);
                    if (str != null)
                        line.append(str);
                }
                strBuf.append(line.toString() + Utils.RS);
                rc[0] ++;
            }
        }

        return strBuf;
    }

    /**
     * It gets all the records from the ReseulSet and packs them into an
     * List according to the ResultType.  Each entry of the list is
     * either a Map or an array of String.
     */
    public static int getResult(int type, ResultSet rset, List<Object> list)
        throws SQLException, IOException {
        String[] entry;
        String str;
        int i, n;
        if (rset == null || list == null)
            return -1;
        if (list.size() > 0)
            list.clear();

        ResultSetMetaData rsmd = rset.getMetaData();
        n = rsmd.getColumnCount();
        entry = new String[n]; 
        int[] dataType = new int[n];
        for (i=1; i<=n; i++) {
            dataType[i-1] = rsmd.getColumnType(i);
            str = rsmd.getColumnLabel(i);
            entry[i-1] = (str != null && str.length() > 0) ?
                str : rsmd.getColumnName(i);
        }
        if ((type & Utils.RESULT_XML) > 0 || (type & Utils.RESULT_JSON) > 0) {
            while (rset.next()) {
                Map<String, String> h = new HashMap<String, String>();
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromCLOB(rset.getClob(i), sb);
                        h.put(entry[i-1], sb.toString());
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromBLOB(rset.getBlob(i), sb);
                        h.put(entry[i-1], sb.toString());
                    }
                    else {
                        h.put(entry[i-1], rset.getString(i));
                    }
                }
                list.add(h);
            }
        }
        else {
            while (rset.next()) {
                entry = new String[n]; 
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromCLOB(rset.getClob(i), sb);
                        entry[i-1] = sb.toString();
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        fromBLOB(rset.getBlob(i), sb);
                        entry[i-1] = sb.toString();
                    }
                    else {
                        entry[i-1] = rset.getString(i);
                    }
                }
                list.add(entry);
            }
        }

        return list.size();
    }

    /**
     * It gets all the records from the list and packs them into a
     * StringBuffer according to the ResultType.  Each record is
     * a Map with multiple key-value pairs. All values are strings.
     */
    public static StringBuffer getResult(int type, String[] keys,
        List list) {
        int i, j, k, n;
        StringBuffer strBuf = new StringBuffer();
        String str;
        Map ph;

        if (keys == null || keys.length <= 0 || keys[0] == null ||
            keys[0].length() <= 0)
            return null;

        if (list == null)
            list = new ArrayList();

        n = list.size();
        k = keys.length;
        if ((type & Utils.RESULT_XML) > 0) {
            StringBuffer line;
            for (i=0; i<n; i++) {
                ph = (Map) list.get(i);
                line = new StringBuffer();
                line.append("<Record type=\"ARRAY\">");
                for (j=0; j<k; j++) {
                    str = (String) ph.get(keys[j]);
                    line.append("<" + keys[j] + ">");
                    if (str != null)
                        line.append(Utils.escapeXML(str));
                    line.append("</" + keys[j] + ">");
                }
                line.append("</Record>");
                strBuf.append(line.toString() + Utils.RS);
            }
        }
        else if ((type & Utils.RESULT_JSON) > 0) {
            StringBuffer line;
            strBuf.append("[");
            for (i=0; i<n; i++) {
                ph = (Map) list.get(i);
                line = new StringBuffer();
                if (i > 0)
                    line.append(",");
                line.append("{");
                for (j=0; j<k; j++) {
                    str = (String) ph.get(keys[j]);
                    if (j > 0)
                        line.append(",");
                    line.append("\"" + keys[j] + "\":");
                    if (str == null)
                        line.append("null");
                    else
                        line.append("\"" + Utils.escapeJSON(str) + "\"");
                }
                line.append("}");
                strBuf.append(line.toString() + Utils.RS);
            }
            strBuf.append("]");
        }
        else if ((type & Utils.RESULT_TEXT) > 0) {
            StringBuffer line;
            strBuf.append("[");
            for (i=0; i<n; i++) {
                ph = (Map) list.get(i);
                line = new StringBuffer();
                for (j=0; j<k; j++) {
                    str = (String) ph.get(keys[j]);
                    if (j > 0)
                        line.append(Utils.FS);
                    if (str != null)
                        line.append(str);
                }
                strBuf.append(line.toString() + Utils.RS);
            }
        }
        return strBuf;
    }

    /**
     * It reads bytes from a JDBC BLOB and writes them into the OutputStream.
     * It returns the number of bytes copied over or -1 for the failure.
     */
    public static int fromBLOB(Blob b, OutputStream out)
        throws IOException, SQLException {
        int i, bufferSize = 8192;
        InputStream in = null;
        byte[] buffer;
        if (b == null || out == null)
            return -1;
        buffer = new byte[bufferSize];
        i = -1;
        try {
            in = b.getBinaryStream();
            i = copyBytes(in, out, MAXLENGTH, buffer);
        }
        catch (Exception e) {
            if (in != null) try {
                in.close();
            }
            catch (Exception ex) {
            }
            if (e instanceof IOException)
                throw((IOException) e);
            else
                throw(new IOException("stream copy failed: " + e.toString()));
        }
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        return i;
    }

    /**
     * It reads bytes from the BLOB and writes them into the StringBuffer.
     * It returns the number of bytes read or -1 for the failure.
     */
    public static int fromBLOB (Blob b, StringBuffer strBuf)
        throws IOException, SQLException {
        int totalBytes = 0;
        int i, bufferSize = 8192;
        InputStream in = null;
        byte[] buffer;
        if(b == null || strBuf == null)
            return -1;
        buffer = new byte[bufferSize];
        try {
            in = b.getBinaryStream();
            while ((i = in.read(buffer, 0, bufferSize)) >= 0) {
                if (i > 0) {
                    strBuf.append(new String(buffer, 0, i));
                    totalBytes += i;
                }
            }
        }
        catch (Exception e) {
            if (in != null) try {
                in.close();
            }
            catch (Exception ex) {
            }
            if (e instanceof IOException)
                throw((IOException) e);
            else
                throw(new IOException("stream copy failed: " + e.toString()));
        }
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        return totalBytes;
    }

    /**
     * It reads chars from the CLOB and writes them into the output writer.
     * It returns the number of chars read or -1 for the failure.
     */
    public static int fromCLOB (Clob c, Writer out)
        throws IOException, SQLException {
        int i, bufferSize = 8192;
        Reader in = null;
        char[] buffer;
        if(c == null || out == null)
            return -1;
        buffer = new char[bufferSize];
        try {
            in = c.getCharacterStream();
            i = copyChars(in, out, MAXLENGTH, buffer);
        }
        catch (Exception e) {
            if (in != null) try {
                in.close();
            }
            catch (Exception ex) {
            }
            if (e instanceof IOException)
                throw((IOException) e);
            else
                throw(new IOException("failed to copy chars: " + e.toString()));
        }
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        return i;
    }

    /**
     * It reads chars from the CLOB and writes them into the StringBuffer.
     * It returns the number of chars read or -1 for the failure.
     */
    public static int fromCLOB (Clob c, StringBuffer strBuf)
        throws IOException, SQLException {
        int totalBytes = 0;
        int i, bufferSize = 8192;
        Reader in = null;
        char[] buffer;
        if(c == null || strBuf == null)
            return -1;
        buffer = new char[bufferSize];
        try {
            in = c.getCharacterStream();
            while ((i = in.read(buffer, 0, bufferSize)) >= 0) {
                if (i > 0) {
                    strBuf.append(buffer, 0, i);
                    totalBytes += i;
                }
            }
        }
        catch (Exception e) {
            if (in != null) try {
                in.close();
            }
            catch (Exception ex) {
            }
            if (e instanceof IOException)
                throw((IOException) e);
            else
                throw(new IOException("stream copy failed: " + e.toString()));
        }
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        return totalBytes;
    }

    /**
     * It copies bytes from the InputStream and writes them into the
     * OutputStream.  It returns the number of bytes copied or -1 otherwise.
     */
    protected static int copyBytes(InputStream in, OutputStream out,
        int maxBytes, byte[] buffer) throws IOException {
        int bytesRead = 0;
        int totalBytes = 0;
        if (in == null || out == null || buffer == null || buffer.length <= 0)
            return -1;
        while ((bytesRead = in.read(buffer)) >= 0) {
            if (bytesRead == 0)
                continue;
            else if (totalBytes + bytesRead < maxBytes)
                out.write(buffer, 0, bytesRead);
            else {
                bytesRead = maxBytes - totalBytes;
                out.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
                break;
            }
            totalBytes += bytesRead;
        }
        return totalBytes;
    }

    /**
     * It copies chars from the Reader and writes them into the Writer.
     * It returns the number of chars copied or -1 otherwise.
     */
    protected static int copyChars(Reader in, Writer out, int maxBytes,
        char[] buffer) throws IOException {
        int bytesRead = 0;
        int totalBytes = 0;
        if (in == null || out == null || buffer == null || buffer.length <= 0)
            return -1;
        while ((bytesRead = in.read(buffer)) >= 0) {
            if (bytesRead == 0)
                continue;
            else if (totalBytes + bytesRead < maxBytes)
                out.write(buffer, 0, bytesRead);
            else {
                bytesRead = maxBytes - totalBytes;
                out.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
                break;
            }
            totalBytes += bytesRead;
        }
        return totalBytes;
    }

    /**
     * It parses the SQL query and returns a list of the table names
     * in upper case on which the query is against.  In case of null
     * or empty query, it returns null.
     */
    public static String[] getTables(String query) {
        int i, j, n;
        char c;
        String str;
        if (query == null || query.length() <= 7)
            return null;
        i = 0;
        n = query.length();
        while ((c = query.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') {
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return new String[0];
        str = query.substring(i, i+7).toUpperCase();
        if ("SELECT ".equals(str)) {
            i += 7;
            str = query.substring(i).toUpperCase();
            j = str.indexOf(" FROM ");
            if (j > 0) {
                int k;
                String[] list;
                i = j + 6;
                j = str.indexOf(" WHERE ", i);
                if (j > i)
                    str = str.substring(i, j);
                else if (j < i)
                    str = str.substring(i);
                else // no tables
                    return new String[0];
                n = str.length();
                i = 0;
                k = 0;
                while (i < n) {
                    j = str.indexOf(',', i);
                    k ++;
                    if (j < i)
                        break;
                    i = j + 1;
                }
                list = new String[k];
                i = 0;
                k = 0;
                while (i < n) {
                    j = str.indexOf(',', i);
                    if (j > i) {
                        list[k++] = str.substring(i, j);
                        i = j + 1;
                    }
                    else {
                        list[k++] = str.substring(i);
                        break;
                    }
                }
                for (i=k-1; i>=0; i--) { // trim
                    str = list[i].trim();
                    j = str.indexOf(' ');
                    if (j > 0)
                        list[i] = str.substring(0, j).trim();
                    else
                        list[i] = str;
                }
                return list;
            }
        }
        else if ("UPDATE ".equals(str)) {
            i += 7;
            while (i < n && query.charAt(i) == ' ')
                i ++;
            if (n > i) {
                j = query.indexOf(' ', i);
                if (j > i)
                    return new String[]{query.substring(i, j).toUpperCase()};
            }
        }
        else if ("INSERT ".equals(str)) {
            i += 7;
            while (i < n && query.charAt(i) == ' ')
                i ++;
            i += 4;
            while (i < n && query.charAt(i) == ' ')
                i ++;
            if (n > i) {
                j = query.indexOf(' ', i);
                if (j > i)
                    return new String[]{query.substring(i, j).toUpperCase()};
            }
        }
        else if ("DELETE ".equals(str)) {
            i += 7;
            while (i < n && query.charAt(i) == ' ')
                i ++;
            if (n > i) {
                j = query.indexOf(' ', i);
                if (j > i)
                    str = query.substring(i, j).toUpperCase();
                else
                    str = query.substring(i).toUpperCase();
                if ("FROM".equals(str)) { // standard delete
                    i = j;
                    while (i < n && query.charAt(i) == ' ')
                        i ++;
                    if (n > i) {
                        j = query.indexOf(' ', i);
                        if (j > i)
                            str = query.substring(i, j).toUpperCase();
                        else
                            str = query.substring(i).toUpperCase();
                        return new String[]{str};
                    }
                }
                else { // MySQL delete on a single table
                    return new String[]{str};
                }
            }
        }
        return new String[0];
    }
}
