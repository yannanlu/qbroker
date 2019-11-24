package org.qbroker.jms;

/* JDBCMessenger.java - a Database messenger for JMS messages */

import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.util.Date;
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
import java.sql.Types;
import java.sql.SQLException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QuickCache;
import org.qbroker.common.QList;
import org.qbroker.common.GenericPool;
import org.qbroker.common.SQLUtils;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeoutException;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.DBConnector;
import org.qbroker.jms.MessageInputStream;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.event.Event;

/**
 * JDBCMessenger connects to a database specified by URI and initializes the
 * operation of query and update for SQL statement with JMS Messages.
 *<br><br>
 * There are four methods, select(), query(), update() and list() for the SQL
 * statements.  The method of select() executes a fixed query statement. In
 * case of repeated select operation, DependencyGroup has to be defined. It
 * will be used to determine how often to rerun the select query. If KeyColumn
 * is defined, the SQL statement has to contain a parameter as the start point
 * to filter out old rows.  The rest of methods extract the SQL statements from
 * the incoming messages.  It supports setXXX() with %%TYPE:property%% as
 * the placeholder, where TYPE is one of the followings: INT, LONG, FLOAT,
 * DOUBLE, STRING, DATE, TIME and TIMESTAMP.  If there is no TYPE defined,
 * the default type of STRING will be assumed.  The method of query() is used
 * for synchronous requests, whereas update() is for asynchronous persisting
 * request.  Therefore, if you what to update Database automatically, please
 * use update().  It will keep retry upon database failures until the message
 * expires. The method of list() has no SQL statement. It is to list all tables
 * in a database with dynamic filter support or all columns in a specific table.
 *<br><br>
 * In case of BLOB or LONGVARBINARY data is queried with a BytesMessage, the
 * method of query will only return the first row with the last BLOB or
 * LONGVARBINARY data stored in the message body.  The rest of the data will
 * formatted into the SQLField if there is any.
 *<br><br>
 * In case of query with INSERT action, the method of query() will also
 * check the extra SQL statement stored in the ExtraSQLField of the message.
 * If the statement is defined, it will be executed only if the first statement
 * is successful.  The extra SQL statement can be used to get Auto-Increment
 * id or something else.  The result of the extra statement will be put into
 * the ExtraSQLField as a string for return.  In case of error, "ERROR: "
 * will be prefixed to the result string.
 *<br><br>
 * In case of select, it supports the standard formatter on each of the
 * selected messages.
 *<br><br>
 * For charSet and encoding, you can overwrite the default encoding by
 * starting JVM with "-Dfile.encoding=UTF8 your_class".
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JDBCMessenger extends DBConnector {
    private String sqlQuery;
    private PreparedStatement ps = null;
    private Template template;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int minSize, capacity, maxRetry, sleepTime = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int mode = 0;
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private int maxStatement = 512;
    private SimpleDateFormat dateFormat = null;
    private QuickCache cache; // pools for prepared statements
    private XQueue assetList; // queue for result sets
    private QList paramList;  // list of parameters for prepared statements
    private MessageFilter filter = null;
    private java.lang.reflect.Method ackMethod = null;

    private int[] partition;
    private long sequenceID = 0L;
    private long maxMsgLength = 4194304;
    private long waitTime = 500L, timestamp;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private int offset;
    private String sqlField, extraField, operation = "query";
    private String rcField, resultField, referenceFile = null;
    private String keyField = null, idField = null, msgID = null;
    private String bodyField = null;

    private final static int ASSET_RS = 0;
    private final static int ASSET_KEY = 1;
    private final static int ASSET_POOL = 2;
    private final static int ASSET_PS = 3;
    private final static int ASSET_STATE = 4;
    private final static int ACK_ID = 0;
    private final static int ACK_SID = 1;
    private final static int ACK_TTL = 2;
    private final static int ACK_TIME = 3;

    public JDBCMessenger(Map props) {
        super(props);
        Object o;
        List list;

        if ((o = props.get("SQLField")) != null)
            sqlField = (String) o;
        else
            sqlField = "SQL";

        if ((o = props.get("ExtraSQLField")) != null)
            extraField = (String) o;
        else
            extraField = null;

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("ResultField")) != null && o instanceof String)
            resultField = (String) o;
        else
            resultField = "SQLResult";

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;
        if ((o = props.get("MinSize")) != null)
            minSize = Integer.parseInt((String) o);
        else
            minSize = 1;
        if (minSize > capacity)
            minSize = capacity;

        if ((o = props.get("Retry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) <= 0)
            maxRetry = 3;

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value, cellID;
            Template temp = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            cellID = (String) props.get("CellID");
            if (cellID == null || cellID.length() <= 0)
                cellID = "0";
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if ((propertyName[n] = MessageUtils.getPropertyID(key)) == null)
                    propertyName[n] = key;
                if (value != null && value.length() > 0) {
                    propertyValue[n] = temp.substitute("CellID", cellID, value);
                }
                n ++;
            }
        }

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if (props.get("TextMode") != null)
            textMode = Integer.parseInt((String) props.get("TextMode"));
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
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberStatement")) != null)
            maxStatement = Integer.parseInt((String) o);
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
        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);

        if ((o = props.get("SQLQuery")) != null)
            sqlQuery = (String) o;

        if ((xaMode & MessageUtils.XA_COMMIT) > 0) try {
            setAutoCommit(false);
        }
        catch (SQLException e) {
            throw(new IllegalArgumentException("SQLException: " + e));
        }

        cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 0);
        assetList = new IndexedXQueue(uri, maxStatement);
        paramList = new QList(uri, maxStatement);

        // date format for parameter settings
        if ((o = props.get("TimePattern")) != null && o instanceof String)
            dateFormat = new SimpleDateFormat((String) o);
        else
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        if ((xaMode & MessageUtils.XA_CLIENT) > 0) {
            Class<?> cls = this.getClass();
            try {
                ackMethod = cls.getMethod("acknowledge",
                    new Class[] {long[].class});
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("found no ack method"));
            }
        }

        offset = 1;
        timestamp = 0;
        if ("list".equals(operation) && (resultType & Utils.RESULT_XML) == 0 &&
            (resultType & Utils.RESULT_JSON) == 0)
            throw(new IllegalArgumentException(uri +
                " illegal ResultType for list: "+ resultType));
        else if ("select".equals(operation)) try {
            if ((o = props.get("ReferenceFile")) != null)
                referenceFile = (String) o;

            if (props.containsKey("TimePattern"))
                timestamp = System.currentTimeMillis();

            // load timestamp, offset and msgID
            loadReference();

            if ((o = props.get("KeyColumn")) != null)
                keyField = (String) o;

            if ((o = props.get("IDColumn")) != null)
                idField = (String) o;

            if ((o = props.get("BodyColumn")) != null)
                bodyField = (String) o;

            if ((o = props.get("FormatterArgument")) != null &&
                o instanceof List) { // init formatter
                HashMap<String, Object> hmap = new HashMap<String, Object>();
                hmap.put("Name", uri);
                hmap.put("FormatterArgument", o);
                filter = new MessageFilter(hmap);
                hmap.clear();
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri +
                " failed to load reference: " + e.toString()));
        }
    }

    /**
     * It executes the predfined SQL statement and puts the result to the body
     * of a new message.  The new message will be added to the XQueue as the
     * output.  The result may be a delimited text stored in a TextMessage,
     * a delimited bytes string stored in a BytesMessage, a List with
     * String[] as the member stored in an ObjectMessage, or a ResultSet
     * stored in an ObjectMessage together with the ack method enabled.
     */
    public void select(XQueue xq) throws SQLException, JMSException {
        Message outMessage;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        Object[] asset;
        GenericPool pool = null;
        PreparedStatement ps = null;
        ResultSet rset = null;
        int sid = -1, cid, rid, mask = 0;
        int i, k, n, retry, kid = -1, id = -1, bid = -1;
        long currentTime, count = 0, stm = 10;
        boolean isText = (textMode == 1), isDate = false, checkSkip = false;
        boolean isSleepy = (sleepTime > 0);
        byte[] buffer = new byte[bufferSize];
        String sqlStr, str;
        int shift = partition[0];
        int len = partition[1];
        String[] keys = null;
        int[] dataType = null;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        k = 0;
        sqlStr = sqlQuery;
        pool = getPool(sqlStr);
        if (pool == null) {
            throw(new SQLException(uri + ": failed to get pool for " +
                sqlStr));
        }

        o = null;
        retry = 0;
        do {
            o = pool.checkout(waitTime);
        } while (o == null && ++retry <= maxRetry);

        if (o == null) {
            throw(new SQLException(uri + ": failed to prepare SQL " +
                "statement after " + maxRetry + " retries with " +
                " pool: " + pool.getSize() + " " + pool.getCount()));
        }
        else if (o instanceof Exception) {
            throw(new SQLException(uri +
                ": failed to prepare SQL Statement: "+
                Event.traceStack((Exception) o)));
        }
        else try {
            ps = (PreparedStatement) o;
            if (sqlExecTimeout >= 0)
                ps.setQueryTimeout(sqlExecTimeout);
            if (keyField != null) { // set parameter
                Map h = new HashMap();
                str = SQLUtils.getParamInfo(sqlStr, h);
                if (str != sqlStr) { // parameters defined
                    i = ((int[]) h.get("Type"))[0];
                    if (i == SQLUtils.SQL_DATE || i == SQLUtils.SQL_TIME ||
                        i == SQLUtils.SQL_TIMESTAMP) { // for date or timestamp
                        SQLUtils.setParameter(1, i, ps,
                            dateFormat.format(new Date(timestamp)), dateFormat);
                        isDate = true;
                    }
                    else // not a Date
                        SQLUtils.setParameter(1, i, ps,
                            String.valueOf(timestamp), dateFormat);
                }
                h.clear();
            }
        }
        catch (SQLException e) {
            pool.checkin(ps, pool.POOL_INVALID);
            throw(e);
        }
        catch (Exception e) {
            pool.checkin(ps, pool.POOL_INVALID);
            throw(new SQLException(e.toString()));
        }
        catch (Error e) {
            pool.checkin(ps, pool.POOL_INVALID);
            Event.flush(e);
        }

        rset = null;
        try {
            rset = ps.executeQuery();
        }
        catch (SQLException e) {
            pool.checkin(ps, pool.POOL_INVALID);
            throw(e);
        }
        catch (Exception e) {
            pool.checkin(ps, pool.POOL_INVALID);
            throw(new RuntimeException(e.getMessage()));
        }
        catch (Error e) {
            pool.checkin(ps, pool.POOL_INVALID);
            Event.flush(e);
        }

        try {
            ResultSetMetaData rsmd = rset.getMetaData();
            n = rsmd.getColumnCount();
            dataType = new int[n];
            keys = new String[n];
            if (keyField != null) {
                for (i=1; i<=n; i++) {
                    dataType[i-1] = rsmd.getColumnType(i);
                    str = rsmd.getColumnLabel(i);
                    keys[i-1] = (str != null && str.length() > 0) ?
                        str : rsmd.getColumnName(i);
                    if (keyField.equalsIgnoreCase(keys[i-1]))
                        kid = i;
                    else if (idField.equalsIgnoreCase(keys[i-1]))
                        id = i;
                }
            }
            else if (bodyField != null) {
                for (i=1; i<=n; i++) {
                    dataType[i-1] = rsmd.getColumnType(i);
                    str = rsmd.getColumnLabel(i);
                    keys[i-1] = (str != null && str.length() > 0) ?
                        str : rsmd.getColumnName(i);
                    if (bodyField.equalsIgnoreCase(keys[i-1]))
                        bid = i;
                }
            }
            else {
                for (i=1; i<=n; i++) {
                    dataType[i-1] = rsmd.getColumnType(i);
                    str = rsmd.getColumnLabel(i);
                    keys[i-1] = (str != null && str.length() > 0) ?
                        str : rsmd.getColumnName(i);
                }
            }
        }
        catch (Exception e) {
            try {
                rset.close();
            }
            catch (Exception ex) {
            }
            pool.checkin(ps, pool.POOL_INVALID);
            new Event(Event.ERR, "failed to check columns for " +
                xq.getName() + ": " + Event.traceStack(e)).send();
            throw(new RuntimeException(e.toString()));
        }

        if (isDate) {
            checkSkip = true;
            k = offset;
        }
        while (rset.next()) { // loop thru result set to dispatch msgs
            if (checkSkip) { // skip over the processed records
                str = rset.getString(kid);
                if (str == null)
                    str = "";
                try {
                    Date d = dateFormat.parse(str, new ParsePosition(0));
                    long t = d.getTime();
                    if (t == timestamp) {
                        k--;
                        if (k <= 0)
                            checkSkip = false;
                        continue;
                    }
                    else if (t > timestamp) {
                        checkSkip = false;
                    }
                    else {
                        continue;
                    }
                }
                catch (Exception ex) {
                    new Event(Event.ERR, uri+" failed to parse date at " +
                        kid + ": " + str).send();
                    continue;
                }
            }
            currentTime = System.currentTimeMillis();
            sid = -1;
            switch (len) {
              case 0:
                do { // reserve an empty cell
                    sid = xq.reserve(waitTime);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0);
                break;
              case 1:
                do { // reserve the empty cell
                    sid = xq.reserve(waitTime, shift);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0);
                break;
              default:
                do { // reserve a partitioned empty cell
                    sid = xq.reserve(waitTime, shift, len);
                    if (sid >= 0)
                        break;
                    mask = xq.getGlobalMask();
                } while ((mask & XQueue.KEEP_RUNNING) > 0);
                break;
            }

            if (sid >= 0) {
                if (isText)
                    outMessage = (Message) new TextEvent();
                else
                    outMessage = (Message) new BytesEvent();
                outMessage.clearBody();
            }
            else if((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.PAUSE) == 0) {
                new Event(Event.ERR, "failed to reserve a cell on " +
                    xq.getName() + " for " + uri).send();
                try {
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_OK);
                return;
            }
            else {
                try {
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_OK);
                return;
            }

            try {
                for (i=1; i<=n; i++) {
                    if (dataType[i-1] == Types.CLOB) {
                        StringBuffer sb = new StringBuffer();
                        SQLUtils.fromCLOB(rset.getClob(i), sb);
                        if (i != bid)
                            MessageUtils.setProperty(keys[i-1], sb.toString(),
                                outMessage);
                        else if (isText) // column for body
                            ((TextMessage) outMessage).setText(sb.toString());
                        else // column for body
                            ((BytesMessage) outMessage).writeBytes(
                                sb.toString().getBytes());
                    }
                    else if (dataType[i-1] == Types.BLOB) {
                        StringBuffer sb = new StringBuffer();
                        SQLUtils.fromBLOB(rset.getBlob(i), sb);
                        if (i != bid)
                            MessageUtils.setProperty(keys[i-1], sb.toString(),
                                outMessage);
                        else if (isText) // column for body
                            ((TextMessage) outMessage).setText(sb.toString());
                        else // column for body
                            ((BytesMessage) outMessage).writeBytes(
                                sb.toString().getBytes());
                    }
                    else if (i == kid) { // column for timestamp
                        str = rset.getString(i);
                        if (str == null)
                            str = "";
                        try {
                            long t;
                            if (isDate) {
                                Date d = dateFormat.parse(str,
                                    new ParsePosition(0));
                                t = d.getTime();
                            }
                            else {
                                t = Long.parseLong(str);
                            }
                            if (t > timestamp) {
                                timestamp = t;
                                offset = 1;
                                msgID = rset.getString(id);
                                if (msgID == null)
                                    msgID = "-";
                            }
                            else {
                                offset ++;
                            }
                        }
                        catch (Exception ex) {
                            new Event(Event.ERR, uri+" failed to parse " +
                                ((isDate) ? "date: " : "number: ")+str).send();
                            break;
                        }
                        MessageUtils.setProperty(keys[i-1], str, outMessage);
                    }
                    else if (i == bid) { // column for body
                        str = rset.getString(i);
                        if (str == null)
                            str = "";
                        if (isText)
                            ((TextMessage) outMessage).setText(str);
                        else
                            ((BytesMessage) outMessage).writeBytes(
                                str.getBytes());
                    }
                    else {
                        str = rset.getString(i);
                        if (str == null)
                            str = "";
                        MessageUtils.setProperty(keys[i-1], str, outMessage);
                    }
                }
                if (i <= n) { // failed to parse data so skip the record
                    xq.cancel(sid);
                    continue;
                }

                if (propertyName != null && propertyValue != null) {
                    for (i=0; i<propertyName.length; i++)
                        MessageUtils.setProperty(propertyName[i],
                            propertyValue[i], outMessage);
                }

                outMessage.setJMSTimestamp(currentTime);
            }
            catch (IOException e) {
                xq.cancel(sid);
                try {
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_INVALID);
                throw(new SQLException("failed to load LOB with result " +
                    "queried from " + uri +": " + Event.traceStack(e)));
            }
            catch (SQLException e) {
                xq.cancel(sid);
                try {
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_INVALID);
                throw(e);
            }
            catch (JMSException e) {
                xq.cancel(sid);
                try {
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_OK);
                new Event(Event.WARNING, "failed to load msg with result " +
                    "selected from " + uri + ": " + Event.traceStack(e)).send();
                return;
            }

            if (filter != null && filter.hasFormatter()) try {
                filter.format(outMessage, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to format the msg: "+
                    Event.traceStack(e)).send();
            }

            cid = xq.add(outMessage, sid);
            if (cid >= 0) {
                if (displayMask > 0) try {
                    String line = MessageUtils.display(outMessage,
                        null, displayMask, null);
                    new Event(Event.INFO, "selected a record from " +
                        uri + " into a msg (" + line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.INFO, "selected a record from " +
                        uri + " into a msg").send();
                }
                count ++;
                try {
                    saveReference();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to save reference: " +
                        timestamp + " " + offset + " " + msgID + ": " +
                        e.toString()).send();
                }
                if (maxNumberMsg > 0 && count >= maxNumberMsg)
                    break;

                if (isSleepy) { // slow down a while
                    long tm = System.currentTimeMillis() + sleepTime;
                    do {
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) > 0) // temporarily disabled
                            break;
                        else try {
                            Thread.sleep(stm);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (tm > System.currentTimeMillis());
                }
            }
            else {
                xq.cancel(sid);
            }
        }

        try {
            rset.close();
        }
        catch (Exception e) {
        }
        pool.checkin(ps, pool.POOL_OK);
    }

    /** loads the state parameters from the reference file */
    public void loadReference() throws IOException {
        // load timestamp and msgID
        if (referenceFile != null && new File(referenceFile).exists()) {
            String str = null;
            byte[] buffer = new byte[1024];
            int n, len = 0;
            try {
                FileInputStream in = new FileInputStream(referenceFile);
                while ((n = in.read(buffer, len, 1024 - len)) >= 0) {
                    len += n;
                    if (len >= 1024)
                        break;
                }
                in.close();
                str = new String(buffer, 0, len);
            }
            catch (Exception e) {
                throw(new IOException(uri +
                    " failed to read reference file: " + e.toString()));
            }
            // load timestamp and msgID
            if (str != null && str.length() > 0) try {
                n = str.indexOf(" ");
                timestamp = Long.parseLong(str.substring(0, n));
                str = str.substring(n+1, str.length()-1);
                n = str.indexOf(" ");
                offset = Integer.parseInt(str.substring(0, n));
                msgID = str.substring(n+1);
            }
            catch (Exception e) {
                throw(new IOException(uri +
                    " failed to parse reference data: " + e.toString()));
            }
        }
    }

    /**
     * flushes the current reference info to a given file for tracking the
     * browser state if the ReferenceFile is defined.
     */
    protected void saveReference() throws IOException {
        if (referenceFile != null) try {
            FileWriter out = new FileWriter(referenceFile);
            out.write(timestamp + " " + offset + " " + msgID + "\n");
            out.flush();
            out.close();
        }
        catch (Exception e) {
            throw(new IOException(e.toString()));
        }
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * SQL statement and data from each message.  After compiling and
     * setting data on the statement, it executes the SQL statement to query
     * the database.  Based on the result type, the query result can be
     * formatted into the customized content.  Or the ResultSet of the query
     * can be put into the message before it is removed from the XQueue.  The
     * requester will be able to get the result content or ResultSet out of
     * the message.  In case of non-select SQL statement, it will execute the
     * statement and store the return code into the RCField.
     *<br><br>
     * Since the query operation relies on the request, this method will
     * intercept the request and processes it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the query is successful.  Otherwise, the
     * message body will not contain the content out of query operation.
     *<br><br>
     * If the SQL statement is an INSERT action and its execution is successful,
     * this method will also check the extra SQL statement stored in the
     * ExtraSQLField of the message.  If the statement is defined, it will be
     * executed immediately.  Its result will be put into the same field as the
     * return.  The requester is supposed to check the content in ExtraSQLField.
     * This extra SQL can be used to get Auto-Increment id or something else.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void query(XQueue xq) throws SQLException, TimeoutException {
        Message outMessage;
        PreparedStatement ps = null;
        ResultSet rset = null;
        GenericPool pool = null;
        Object o;
        Object[] asset;
        long currentTime, tm, idleTime, ttl = 0L, count = 0, stm = 10;
        boolean isSelect = true, checkIdle = (maxIdleTime > 0);
        boolean isAutoCommit = ((xaMode & MessageUtils.XA_COMMIT) == 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        int retry = 0;
        int i, k, sid = -1, cid, rid, n, mask;
        int[] rc = new int[2];
        String dt = null, sqlStr = null, msgStr = null, line = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
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

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            rc[1] = 1;
            sqlStr = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                sqlStr = MessageUtils.getProperty(sqlField, outMessage);
                if (sqlStr == null || sqlStr.length() <= 0) {
                    rc[1] = 0;
                    if (template != null)
                        sqlStr = MessageUtils.format(outMessage, buffer,
                            template);
                    else
                        sqlStr = MessageUtils.processBody(outMessage, buffer);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }
            if (sqlStr == null || sqlStr.length() <= 0) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, uri + ": failed to get " +
                    "SQL statement from msg").send();
                outMessage = null;
                continue;
            }

            msgStr = null;
            if (sqlStr.toUpperCase().startsWith("SELECT"))
                isSelect = true;
            else if (sqlStr.toUpperCase().startsWith("WITH"))
                isSelect = true;
            else if (sqlStr.indexOf("%%body%%") < 0) // without body to set
                isSelect = false;
            else if (outMessage instanceof BytesMessage) {
                msgStr = "";
                isSelect = false;
            }
            else try {
                isSelect = false;
                msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, uri + ": failed to get body from msg: " +
                    Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            o = null;
            rid = -1;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                rset = null;
                rc[0] = -1;

                pool = getPool(sqlStr);
                if (pool == null) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new SQLException(uri + ": failed to get PS pool for "+
                        sqlStr));
                }

                o = null;
                retry = 0;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // standby temporarily
                        new Event(Event.WARNING, "aborted to checkout the " +
                            "statement from the pool on "+
                            uri + " due to being disabled").send();
                        xq.putback(sid);
                        return;
                    }
                    o = pool.checkout(waitTime);
                } while (o == null && ++retry <= maxRetry);

                if (o == null) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.DEBUG, k + ": " + sqlStr).send();
                    throw(new SQLException(uri + ": failed to prepare SQL " +
                        "statement after " + maxRetry + " retries with pool: "+
                        pool.getSize() + " " + pool.getCount()));
                }
                else if (o instanceof SQLException) { // retry on SQLException
                    if (k <= 0)
                        new Event(Event.WARNING, uri +
                            ": failed to prepare SQL statement: "+
                            Event.traceStack((SQLException) o)).send();
                    else if (k + 1 >= maxRetry) { // give up on retry
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        throw((SQLException) o);
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    continue;
                }
                else if (o instanceof Exception) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    throw(new RuntimeException(uri + ": failed to prepare SQL "+
                        "statement: "+ Event.traceStack((Exception) o)));
                }

                // set parameters
                try {
                    ps = (PreparedStatement) o;
                    i = setParameters(sqlStr, ps, msgStr, outMessage);
                    if (sqlExecTimeout >= 0)
                        ps.setQueryTimeout(sqlExecTimeout);
                }
                catch (Exception e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    pool.checkin(ps, ((e instanceof SQLException) ?
                        pool.POOL_INVALID : pool.POOL_OK));
                    new Event(Event.ERR, xq.getName() +
                        " failed to set parameters: "+
                        Event.traceStack(e)).send();
                    outMessage = null;
                    msgStr = null;
                    o = null;
                    break;
                }

                o = null;
                rid = -1;
                if ((resultType & Utils.RESULT_SET) > 0) { // for result set
                    for (i=0; i<100; i++) {
                        rid = assetList.reserve(waitTime);
                        if (rid >= 0)
                            break;
                    }

                    if (rid < 0) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        pool.checkin(ps, pool.POOL_OK);
                        new Event(Event.ERR, "failed to reserve ResultSet " +
                            "for " + sqlStr + " on " + uri).send();
                        outMessage = null;
                        break;
                    }
                }

                // execute query
                try {
                    if (isSelect)
                        rset = ps.executeQuery();
                    else
                        rc[0] = ps.executeUpdate();
                }
                catch (SQLException e) {
                    if (rid >= 0)
                        assetList.cancel(rid);
                    pool.checkin(ps, pool.POOL_INVALID);
                    o = e;
                    if (k <= 0)
                        new Event(Event.WARNING, uri +
                            ": failed to execute query: "+ sqlStr + ": " +
                            e.getMessage()).send();
                    else if (k + 1 >= maxRetry) { // give up
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        throw(e);
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    continue;
                }
                catch (Exception e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    if (rid >= 0)
                        assetList.cancel(rid);
                    pool.checkin(ps, pool.POOL_INVALID);
                    throw(new RuntimeException(e.getMessage()));
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    if (rid >= 0)
                        assetList.cancel(rid);
                    pool.checkin(ps, pool.POOL_INVALID);
                    Event.flush(e);
                }
                break;
            }
            msgStr = null;
            if ((isSelect && rset == null) || (rc[0] < 0 && !isSelect)) {
                if (k >= maxRetry) { // failed to query on all retries
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    if (o != null && o instanceof SQLException)
                        throw((SQLException) o);
                    else
                        throw(new RuntimeException(uri + ": query failed"));
                }
                else { // failed to set parameters or to reserve resultset
                    continue;
                }
            }

            currentTime = System.currentTimeMillis();
            if (!isSelect) { // for updates
                pool.checkin(ps, pool.POOL_OK);

                if (extraField != null && // check the extra SQL
                    sqlStr.toUpperCase().startsWith("INSERT")) {
                    String str = null;
                    try {
                        str = MessageUtils.getProperty(extraField, outMessage);
                        str = executeSQLQuery(str);
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, "failed to get extra SQL "+
                            "from " +extraField +": "+ e.toString()).send();
                    }
                    if (str != null) try {
                        MessageUtils.setProperty(extraField, str, outMessage);
                    }
                    catch (Exception e) {
                        new Event(Event.WARNING, "failed to set result to "+
                            extraField +" on "+ uri +": "+ e.toString()).send();
                    }
                }
            }
            else try { // for select
                StringBuffer strBuf;
                if (outMessage instanceof TextMessage) {
                    rc[0] = 0;
                    strBuf = SQLUtils.getResult(resultType, rset, rc);
                    rset.close();
                    pool.checkin(ps, pool.POOL_OK);
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    rc[0] = getResult(resultType, rset, sqlField, buffer,
                        (BytesMessage) outMessage);
                    if (rc[0] < 0) { // no BLOB found
                        rc[0] = 0;
                        strBuf = SQLUtils.getResult(resultType, rset, rc);
                        if ((resultType & Utils.RESULT_XML) > 0) {
                            strBuf.insert(0, "<Result>" + Utils.RS);
                            strBuf.append("</Result>");
                        }
                        else if ((resultType & Utils.RESULT_JSON) > 0) {
                            strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                            strBuf.append("}");
                        }
                        ((BytesMessage) outMessage).writeBytes(
                            strBuf.toString().getBytes());
                    }
                    rset.close();
                    pool.checkin(ps, pool.POOL_OK);
                }
                else if (outMessage instanceof ObjectMessage &&
                    outMessage instanceof JMSEvent) {
                    long[] state;
                    HashMap<String, Object> h = new HashMap<String, Object>();
                    if ((resultType & Utils.RESULT_LIST) > 0) {
                        List<Object> list = new ArrayList<Object>();
                        rc[0] = SQLUtils.getResult(resultType, rset, list);
                        rset.close();
                        h.put("ResultList", list);
                    }
                    else { // for reseult set
                        h.put("ResultSet", rset);
                        state = new long[] {(long) rid, getSequenceID(),
                            ttl, currentTime};
                        asset = new Object[]{rset, sqlStr, pool, ps, state}; 
                        assetList.add(asset, rid);
                        ((JMSEvent) outMessage).setAckObject(this,
                            ackMethod, state);
                        rc[0] = 0;
                    }
                    ((ObjectMessage) outMessage).setObject(h);
                }
                else if (outMessage instanceof ObjectMessage) {
                    HashMap<String, Object> h = new HashMap<String, Object>();
                    List<Object> list = new ArrayList<Object>();
                    rc[0] =  SQLUtils.getResult(resultType, rset, list);
                    if (rid >= 0)
                        assetList.cancel(rid);
                    rset.close();
                    h.put("ResultList", list);
                    ((ObjectMessage) outMessage).setObject(h);
                    pool.checkin(ps, pool.POOL_OK);
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    if (rid >= 0)
                        assetList.cancel(rid);
                    rset.close();
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    pool.checkin(ps, pool.POOL_OK);
                    outMessage = null;
                    continue;
                }
            }
            catch (IOException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (rid >= 0)
                    assetList.cancel(rid);
                try {
                    if (!isAutoCommit)
                        rollback();
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_INVALID);
                throw(new SQLException("failed to load LOB with result " +
                    "queried from " + uri +": " + Event.traceStack(e)));
            }
            catch (SQLException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (rid >= 0)
                    assetList.cancel(rid);
                try {
                    if (!isAutoCommit)
                        rollback();
                    rset.close();
                }
                catch (Exception ex) {
                }
                pool.checkin(ps, pool.POOL_INVALID);
                throw(e);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                if (rid >= 0)
                    assetList.cancel(rid);
                try {
                    if (!isAutoCommit)
                        rollback();
                    rset.close();
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                pool.checkin(ps, pool.POOL_OK);
                new Event(Event.WARNING, "failed to load msg with result " +
                    "queried from " + uri +": "+Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            if (!isAutoCommit) try { // commit in case select for update
                commit();
            }
            catch (SQLException e) {
                new Event(Event.ERR, "failed to commit DB update " +
                    "on " + sqlStr + "\n\t" + Event.traceStack(e)).send();
            }

            try {
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc[0]),
                    outMessage);
                if (displayMask > 0)
                    line = MessageUtils.display(outMessage, sqlStr,
                        displayMask, propertyName);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to set property: " +
                    Event.traceStack(e)).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0)
                new Event(Event.INFO, xq.getName() + " queried "+rc[0]+
                    " records from "+ uri + " into a msg ("+line+ " )").send();
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries").send();
    }

    /**
     * It checks out the SQL statement from the pool for the SQL query,
     * executes the statement and returns the result of SQL_TEXT.  Upon any
     * failures, it returns the error string prefixed with "ERROR: ".
     * For bad argument, it returns null.
     */
    private String executeSQLQuery(String sqlStr) {
        PreparedStatement ps = null;
        ResultSet rset = null;
        GenericPool pool = null;
        Object o;
        String str = null;
        int rc;

        if (sqlStr == null || sqlStr.length() <= 0)
            return null;

        pool = getPool(sqlStr);
        if (pool == null)
            return "ERROR: failed to get pool for " + sqlStr;

        o = null;
        rc = 0;
        do {
            o = pool.checkout(waitTime);
        } while (o == null && ++rc <= maxRetry);

        if (o == null) {
            return "ERROR: failed to prepare SQL statement " + sqlStr +
                " after " + maxRetry + " retries with " +
                " pool: " + pool.getSize() + " " + pool.getCount();
        }
        else if (o instanceof Exception) {
            return "ERROR: failed to prepare SQL statement " + sqlStr +
                ": "+ Event.traceStack((Exception) o);
        }

        ps = (PreparedStatement) o;
        if (sqlExecTimeout >= 0) try {
            ps.setQueryTimeout(sqlExecTimeout);
        }
        catch (Exception e) {
            pool.checkin(ps, ((e instanceof SQLException) ?
                pool.POOL_INVALID : pool.POOL_OK));
            return "ERROR: failed to set timeout: "+ Event.traceStack(e);
        }

        rset = null;
        try {
            if (sqlStr.toUpperCase().startsWith("SELECT"))
                rset = ps.executeQuery();
            else
                rc = ps.executeUpdate();
        }
        catch (Exception e) {
            pool.checkin(ps, pool.POOL_INVALID);
            return "ERROR: failed to execute query " + sqlStr + ": " +
                e.toString();
        }

        if (rset == null) { // for updates
            str = String.valueOf(rc);
            pool.checkin(ps, pool.POOL_OK);
        }
        else try { // for select
            int[] r = new int[2];
            StringBuffer strBuf;
            strBuf = SQLUtils.getResult(Utils.RESULT_TEXT, rset, r);
            rset.close();
            str = strBuf.toString();
            pool.checkin(ps, pool.POOL_OK);
        }
        catch (Exception e) {
            pool.checkin(ps, pool.POOL_INVALID);
            str = "ERROR: " + "failed to get result from " +
                uri +": " + Event.traceStack(e);
        }

        return str;
    }

    private synchronized long getSequenceID() {
        return sequenceID++;
    }

    /**
     * It listens to the XQueue for incoming JMS messages and extracts the
     * SQL statement and data from each message.  After the compiling and
     * setting data on the statement, it executes the SQL statement for
     * the database operation of update, insert or delete.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * It is up to the caller to handle this exception.
     *<br><br>
     * The original message will not be modified.  If the XQueue has enabled
     * the EXTERNAL_XA bit, it will also acknowledge the messages.
     * It is NOT MT-Safe due to the cached SQL statements.
     */
    public void update(XQueue xq) throws SQLException, TimeoutException,
        JMSException {
        Message inMessage;
        PreparedStatement ps = null;
        GenericPool pool = null;
        Object o;
        boolean isAutoCommit = ((xaMode & MessageUtils.XA_COMMIT) == 0);
        boolean isExecute = false, checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, tm, count = 0, stm = 10;
        int retry = 0, rc = -1, mask;
        int n, k, sid = -1;
        int dmask = MessageUtils.SHOW_DATE;
        String dt = null, sqlStr = null, msgStr = null;
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
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

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            sqlStr = null;
            tm = 0L;
            try {
                tm = inMessage.getJMSExpiration();
                sqlStr = MessageUtils.getProperty(sqlField, inMessage);
                if (sqlStr == null) {
                    if (template != null)
                        sqlStr = MessageUtils.format(inMessage,buffer,template);
                    else
                        sqlStr = MessageUtils.processBody(inMessage, buffer);
                }
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }
            if (sqlStr == null || sqlStr.length() <= 0) {
                new Event(Event.ERR, uri + ": failed to get " +
                    "SQL statement from msg").send();
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            if (sqlStr.toUpperCase().startsWith("EXECUTE"))
                isExecute = true;
            else
                isExecute = false;

            msgStr = ""; 
            if (sqlStr.indexOf("%%body%%") < 0) // without body to set
                msgStr = null;
            else if (!(inMessage instanceof BytesMessage)) try {
                msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
                new Event(Event.ERR, uri + ": failed to get body from msg: " +
                    Event.traceStack(e)).send();
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            o = null;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                rc = -1;

                pool = getPool(sqlStr);
                if (pool == null) {
                    xq.putback(sid);
                    throw(new SQLException(uri + ": failed to get pool for " +
                        sqlStr));
                }

                o = null;
                retry = 0;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // standby temporarily
                        new Event(Event.WARNING, "aborted to checkout the " +
                            "statement from the pool on "+
                            uri + " due to being disabled").send();
                        xq.putback(sid);
                        return;
                    }
                    o = pool.checkout(waitTime);
                } while (o == null && ++retry <= maxRetry);

                if (o == null) {
                    xq.putback(sid);
                    throw(new SQLException(uri + ": failed to prepare SQL " +
                        "statement after " + maxRetry + " retries with pool: "+
                        pool.getSize() + " " + pool.getCount()));
                }
                else if (o instanceof SQLException) { // retry on SQLException
                    if (k <= 0)
                        new Event(Event.WARNING, uri +
                            ": failed to prepare SQL statement: "+
                            Event.traceStack((SQLException) o)).send();
                    else if (k + 1 >= maxRetry) { // give up on retry
                        xq.putback(sid);
                        break;
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    continue;
                }
                else if (o instanceof Exception) {
                    xq.putback(sid);
                    throw(new RuntimeException(uri + ": failed to prepare SQL "+
                        "statement: "+ Event.traceStack((Exception) o)));
                }

                // set parameters
                try {
                    ps = (PreparedStatement) o;
                    setParameters(sqlStr, ps, msgStr, inMessage);
                    if (sqlExecTimeout >= 0)
                        ps.setQueryTimeout(sqlExecTimeout);
                }
                catch (Exception e) {
                    pool.checkin(ps, ((e instanceof SQLException) ?
                        pool.POOL_INVALID : pool.POOL_OK));
                    new Event(Event.ERR, xq.getName() +
                        " failed to set SQL parameters: "+
                        Event.traceStack(e)).send();
                    if (ack) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    o = null;
                    break;
                }

                o = null;
                currentTime = System.currentTimeMillis();
                try {
                    if (!isExecute)
                        rc = ps.executeUpdate();
                    else {
                        ps.execute();
                        rc = 0;
                    }
                }
                catch (SQLException e) {
                    pool.checkin(ps, pool.POOL_INVALID);
                    o = e;
                    if (k <= 0)
                        new Event(Event.WARNING, uri +
                            ": failed to execute query: "+ sqlStr + ": " +
                            e.getMessage()).send();
                    else if (k + 1 >= maxRetry)
                        continue;
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    continue;

                }
                catch (Exception e) {
                    xq.putback(sid);
                    pool.checkin(ps, pool.POOL_INVALID);
                    throw(new RuntimeException(e.getMessage()));
                }
                catch (Error e) {
                    xq.putback(sid);
                    pool.checkin(ps, pool.POOL_INVALID);
                    Event.flush(e);
                }
                break;
            }
            if (rc < 0) { // failed
                if (k >= maxRetry) { // failed to query on all retrie
                    xq.putback(sid);
                    if (o != null && o instanceof SQLException)
                        throw((SQLException) o);
                    else
                        throw(new RuntimeException(uri + ": query failed"));
                }
                else { // failed to set parameters
                    continue;
                }
            }
            pool.checkin(ps, pool.POOL_OK);

            if (!isAutoCommit) try {
                commit();
            }
            catch (SQLException e) {
                new Event(Event.ERR, "failed to commit DB update " +
                    "on " + sqlStr + "\n\t" + Event.traceStack(e)).send();
            }

            if (ack) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after update "+
                    "on "+uri+": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after update "+
                    "on " + uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after update " +
                    "on " + uri + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt= Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, sqlStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "updated " + rc + " records on " +
                        uri+" with a msg ( Date: " +dt+ line + " )").send();
                else // no date
                    new Event(Event.INFO, "updated " + rc + " records on " +
                        uri + " with a msg (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "updated " + rc + " records on " +
                    uri + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "updated " + count + " records to " +
               uri).send();
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts a pattern
     * for table names or a specific table name, and content filters from each
     * message.  In case of a pattern, it gets a list of tables in the database
     * and queries the total count of records in each table hit by the filters.
     * If the request has no wildcards, it will be treated as a specific table
     * name. The method will get info for each of the columns. Based on the
     * result type, the query result for tables will be formatted into the
     * customized content with 4 columns, TABLE_NAME, TABLE_TYPE, COUNT, and
     * REMARK. The result will be put into the message before it is removed
     * from the XQueue.  The requester will be able to get the result content
     * out of the message.
     *<br><br>
     * Since the list operation relies on the request, this method will
     * intercept the request and processes it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the query is successful.  Otherwise, the
     * message body will not contain the content out of query operation.
     *<br><br>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    @SuppressWarnings("unchecked")
    public void list(XQueue xq) throws SQLException, TimeoutException {
        Message outMessage;
        PreparedStatement ps = null;
        ResultSet rset = null;
        List list;
        Map ph;
        MessageFilter[] filters = null;
        long currentTime, tm, idleTime, count = 0;
        boolean checkIdle = (maxIdleTime > 0);
        boolean hasFilter = false;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        int retry = 0, mask;
        int i, k, n, sid = -1, heartbeat = 600000, ttl = 7200000;
        String dt = null, sqlStr = null, str, msgStr = null, line = null;
        StringBuffer strBuf;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        byte[] buffer = new byte[bufferSize];

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        n = 0;
                        currentTime = System.currentTimeMillis();
                        if (currentTime - cache.getMTime() >= heartbeat) {
                            cache.disfragment(currentTime);
                            cache.setStatus(cache.getStatus(), currentTime);
                        }
                        if (currentTime >= idleTime + maxIdleTime)
                            throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            if (!(outMessage instanceof TextMessage) &&
                !(outMessage instanceof BytesMessage)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "unsupported msg type from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            tm = 0L;
            sqlStr = null;
            try {
                tm = outMessage.getJMSExpiration();
                sqlStr = MessageUtils.getProperty(sqlField, outMessage);
                MessageUtils.setProperty(rcField, expRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }

            if (sqlStr == null || sqlStr.length() <= 0) // default pattern
                sqlStr = "%";

            // get dynamic content filters
            msgStr = null;
            try { // retrieve dynamic content filters from body
                msgStr = MessageUtils.processBody(outMessage, buffer);
                if (msgStr == null || msgStr.length() <= 0)
                    filters = null;
                else if (msgStr.indexOf("Ruleset") < 0)
                    filters = null;
                else if (cache.containsKey(msgStr) &&
                    !cache.isExpired(msgStr, currentTime))
                    filters = (MessageFilter[]) cache.get(msgStr, currentTime);
                else { // new or expired
                    StringReader ins = new StringReader(msgStr);
                    ph = (Map) JSON2Map.parse(ins);
                    ins.close();
                    if (currentTime - cache.getMTime() >= heartbeat) {
                        cache.disfragment(currentTime);
                        cache.setStatus(cache.getStatus(), currentTime);
                    }
                    filters = MessageFilter.initFilters(ph);
                    if (filters != null && filters.length > 0)
                        cache.insert(msgStr, currentTime, ttl, null, filters);
                    else
                        filters = null;
                }
            }
            catch (Exception e) {
                filters = null;
                new Event(Event.WARNING, xq.getName() +
                    ": failed to retrieve content filters for "+ uri + ": " +
                    msgStr + ": " + Event.traceStack(e)).send();
            }

            // set checkers for dynamic content filters
            if (filters != null && filters.length > 0) {
                hasFilter = true;
            }
            else {
                hasFilter = false;
            }

            strBuf = null;
            if (sqlStr.indexOf("%") >= 0) { // for tables
                list = list(sqlStr, new String[]{"TABLE"});

                // check the list
                if (list == null)
                    list = new ArrayList();

                k = list.size();
                for (i=k-1; i>=0; i--) { // go thru all tables
                    ph = (Map) list.get(i);
                    if (ph == null || ph.size() <= 0) {
                        list.remove(i);
                        continue;
                    }
                    str = (String) ph.get("TABLE_NAME");
                    if (str == null) { // in case of lower case
                        str = (String) ph.remove("table_name");
                        if (str != null && str.length() > 0) {
                            ph.put("TABLE_NAME", str);
                            ph.put("TABLE_TYPE", ph.remove("table_type"));
                            ph.put("TABLE_SCHEM", ph.remove("table_schem"));
                            ph.put("REMARKS", ph.remove("remarks"));
                        }
                    }
                    if (str == null || str.length() <= 0) {
                        list.remove(i);
                        continue;
                    }
                    sqlStr = (String) ph.get("TABLE_SCHEM");
                    if (sqlStr != null && sqlStr.length() > 0)
                        str = sqlStr + "." + str;
                    if (str.startsWith("SYS.HS_PARTITION_COL_")){//for ORA-22812
                        list.remove(i);
                        continue;
                    }
                    if (hasFilter) { // apply filters
                        int j;
                        for (j=0; j<filters.length; j++) {
                            if (filters[j].evaluate(str))
                                break;
                        }

                        if (j >= filters.length) { // no hit
                            list.remove(i);
                            continue;
                        }
                    }
                    sqlStr = "SELECT count(*) FROM " + str;

                    // set parameters
                    try {
                        ps = prepareStatement(sqlStr);
                        if (sqlExecTimeout >= 0)
                            ps.setQueryTimeout(sqlExecTimeout);
                    }
                    catch (Exception e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, xq.getName() +
                            " failed to prepare statement: "+
                            Event.traceStack(e)).send();
                        outMessage = null;
                        break;
                    }

                    // execute query
                    try {
                        rset = ps.executeQuery();
                    }
                    catch (SQLException e) {
                        new Event(Event.WARNING, uri +
                            ": failed to execute query: "+ sqlStr + ": " +
                            e.getMessage()).send();
                        continue;
                    }
                    catch (Exception e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        throw(new RuntimeException(e.getMessage()));
                    }
                    catch (Error e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        Event.flush(e);
                    }
                    if (rset != null) {
                        if (rset.next())
                            ph.put("COUNT", rset.getString(1));
                        try {
                            rset.close();
                        }
                        catch (Exception e) {
                        }
                    }
                    try {
                        ps.close();
                    }
                    catch (Exception e) {
                    }
                }

                currentTime = System.currentTimeMillis();
                k = list.size();
                strBuf = SQLUtils.getResult(resultType,
                    new String[]{"TABLE_NAME", "TABLE_TYPE", "REMARKS","COUNT"},
                    list);
                list.clear();
            }
            else { // for columns
                int[] rc = new int[]{0, 0};
                strBuf = list(sqlStr, rc);
                k = rc[0];
            }
            if (strBuf == null)
                strBuf = new StringBuffer();

            if ((resultType & Utils.RESULT_XML) > 0) {
                strBuf.insert(0, "<Result>" + Utils.RS);
                strBuf.append("</Result>");
            }
            else if ((resultType & Utils.RESULT_JSON) > 0) {
                strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                strBuf.append("}");
            }

            try {
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                try {
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with result " +
                    "queried from " + uri +": "+Event.traceStack(e)).send();
                outMessage = null;
                strBuf.delete(0, strBuf.length());
                continue;
            }
            strBuf.delete(0, strBuf.length());

            try {
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(k),
                    outMessage);
                if (displayMask > 0)
                    line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to set property: " +
                    Event.traceStack(e)).send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0)
                new Event(Event.INFO, xq.getName() + " listed " + k +
                    " tables from "+ uri + " into a msg (" + line +" )").send();
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " lists").send();
    }

    private GenericPool getPool(String key) {
        int i, id, n;
        GenericPool pool = null;
        if (cache.containsKey(key)) {
            pool = (GenericPool) cache.get(key);
        }
        else {
            int[] data;
            Map h = new HashMap();
            String str = SQLUtils.getParamInfo(key, h);
            if (str != key) { // parameters defined
                String[] seq = (String[]) h.get("Sequence");
                int[] lst = (int[]) h.get("Type");
                id = paramList.reserve();
                n = lst.length;
                paramList.add(new Object[] {seq, lst}, id);
            }
            else { // no parameter defined
                n = 0;
                id = -1;
                str = key;
            }
            try {
                pool = new GenericPool(key, minSize, capacity, this,
                    "destroyObject", "prepareStatement", new Object[] {str});
            }
            catch (Exception e) {
                if (id >= 0)
                    paramList.cancel(id);
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    key + ": " + Event.traceStack(e)).send();
                return null;
            }
            catch (NoClassDefFoundError e) {
                if (id >= 0)
                    paramList.cancel(id);
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    key + ": " + Event.traceStack(e)).send();
                return pool;
            }
            catch (UnsupportedClassVersionError e) {
                if (id >= 0)
                    paramList.cancel(id);
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    key + ": " + Event.traceStack(e)).send();
                return pool;
            }
            catch (Error e) {
                if (id >= 0)
                    paramList.cancel(id);
                new Event(Event.ERR, uri +" failed to create a pool for "+
                    key + ": " + Event.traceStack(e)).send();
                Event.flush(e);
            }
            if(str.indexOf(" FOR UPDATE") > 0 || str.indexOf(" for update") > 0)
                data = new int[]{id, n, 1};
            else
                data = new int[]{id, n, 0};
            cache.insert(key, System.currentTimeMillis(), 0, data, pool);
            if (n > 0 && id < 0)
                new Event(Event.WARNING, uri + " param list is full: " + id +
                    "," + n + " " + paramList.size() + "/" + maxStatement +
                    " " + cache.size() + "\n\t" + str).send();
        }
        return pool;
    }

    private Object[] getParamInfo(String key) {
        int[] data = cache.getMetaData(key);
        if (data == null)
            return null;
        else if (data[0] >= 0) {
            return (Object[]) paramList.browse(data[0]);
        }
        else
            return null;
    }

    /** It sets parameters with the data from the message */
    protected int setParameters(String sqlStr, PreparedStatement ps,
        String msgStr, Message msg) throws SQLException, JMSException {
        Object o;
        o = getParamInfo(sqlStr);
        if (o != null && o instanceof Object[]) {
            int i;
            String value;
            String[] list = (String[]) ((Object[]) o)[0];
            int[] type = (int[]) ((Object[]) o)[1];
            if (msg instanceof BytesMessage) { /// for BytesMessages
                if (msgStr == null) for (i=0; i<list.length; i++) {
                    value = MessageUtils.getProperty(list[i], msg);
                    SQLUtils.setParameter(i+1, type[i], ps, value, dateFormat);
                }
                else for (i=0; i<list.length; i++) {
                    if ("body".equals(list[i])) { // for body
                        if (type[i] != SQLUtils.SQL_BLOB)
                            SQLUtils.setParameter(i+1, type[i], ps, msgStr,
                                dateFormat);
                        else try {
                            MessageInputStream in =
                                new MessageInputStream((BytesMessage) msg);
                            ps.setBinaryStream(i+1, (InputStream) in,
                                (int) ((BytesMessage) msg).getBodyLength());
                        }
                        catch (Exception e) {
                            throw(new SQLException("failed to set stream: " +
                                e.toString()));
                        }
                        continue;
                    }
                    value = MessageUtils.getProperty(list[i], msg);
                    SQLUtils.setParameter(i+1, type[i], ps, value, dateFormat);
                }
            }
            else { // for non-BytesMessages
                if (msgStr == null) for (i=0; i<list.length; i++) {
                    value = MessageUtils.getProperty(list[i], msg);
                    SQLUtils.setParameter(i+1, type[i], ps, value, dateFormat);
                }
                else for (i=0; i<list.length; i++) {
                    if ("body".equals(list[i])) { // for body
                        if (type[i] != SQLUtils.SQL_CLOB)
                            SQLUtils.setParameter(i+1, type[i], ps, msgStr,
                                dateFormat);
                        else try {
                            StringReader in = new StringReader(msgStr);
                            ps.setCharacterStream(i+1, (Reader) in,
                                msgStr.length());
                        }
                        catch (Exception e) {
                            throw(new SQLException("failed to set stream: " +
                                e.toString()));
                        }
                        continue;
                    }
                    value = MessageUtils.getProperty(list[i], msg);
                    SQLUtils.setParameter(i+1, type[i], ps, value, dateFormat);
                }
            }
            return i;
        }
        return -1;
    }

    /**
     * It retrieves BLOB data or LONGVARBINARY data as well as other data from
     * the resultset and puts them into the BytesMessage.  Upon success, it
     * returns the number of records retrieved.  Otherwise, it returns -1
     * indicating there is no BLOB data or LONGVARBINARY data found.
     *<br><br>
     * The BLOB data or the LONGVARBINARY data in the last position will be put
     * into the message body.  The rest will be formatted into the field
     * specified by the given key.
     */
    protected static int getResult(int type, ResultSet rset, String key,
        byte[] buffer, BytesMessage msg) throws SQLException, JMSException {
        int i, k, n;
        String str;
        if (rset == null || msg == null)
            return -1;

        ResultSetMetaData rsmd = rset.getMetaData();
        n = rsmd.getColumnCount();
        String[] keys = new String[n];
        int[] dataType = new int[n];
        k = -1;
        for (i=1; i<=n; i++) {
            dataType[i-1] = rsmd.getColumnType(i);
            str = rsmd.getColumnLabel(i);
            keys[i-1] = (str != null && str.length() > 0) ?
                str : rsmd.getColumnName(i);
            if (dataType[i-1] == Types.BLOB) // found a blob
                k = i;
            else if (dataType[i-1] == Types.LONGVARBINARY) { // check the type
                if (k < 0)
                    k = i;
                else if (dataType[k-1] != Types.BLOB) // keep the last id
                    k = i;
            }

        }
        if (k < 0) // no BLOB or LONGVARBINARY found
            return -1;

        if (rset.next()) { // always assuming there is only one record
            StringBuffer line = new StringBuffer();
            if ((type & Utils.RESULT_XML) > 0) {
                line.append("<Record type=\"ARRAY\">");
                for (i=1; i<=n; i++) {
                    if (i == k) { // last blob or longvarbinary
                        MessageUtils.copyBytes(rset.getBinaryStream(i),
                            buffer, msg);
                    }
                    else try {
                        if (dataType[i-1] == Types.CLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromCLOB(rset.getClob(i), sb);
                            str = sb.toString();
                        }
                        else if (dataType[i-1] == Types.BLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromBLOB(rset.getBlob(i), sb);
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
                    catch (IOException e) {
                        throw(new JMSException("failed to read from lob "+
                            keys[i-1] + ": " + Event.traceStack(e)));
                    }
                }
                line.append("</Record>");
            }
            else if ((type & Utils.RESULT_JSON) > 0) {
                line.append("[{");
                for (i=1; i<=n; i++) {
                    if (i == k) { // last blob or longvarbinary
                        MessageUtils.copyBytes(rset.getBinaryStream(i),
                            buffer, msg);
                    }
                    else try {
                        if (dataType[i-1] == Types.CLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromCLOB(rset.getClob(i), sb);
                            str = sb.toString();
                        }
                        else if (dataType[i-1] == Types.BLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromBLOB(rset.getBlob(i), sb);
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
                    catch (IOException e) {
                        throw(new JMSException("failed to read from clob "+
                            keys[i-1] + ": " + Event.traceStack(e)));
                    }
                }
                line.append("}]");
            }
            else {
                for (i=1; i<=n; i++) {
                    if (i == k) { // last blob or longvarbinary
                        MessageUtils.copyBytes(rset.getBinaryStream(i),
                            buffer, msg);
                    }
                    else try {
                        if (dataType[i-1] == Types.CLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromCLOB(rset.getClob(i), sb);
                            str = sb.toString();
                        }
                        else if (dataType[i-1] == Types.BLOB) {
                            StringBuffer sb = new StringBuffer();
                            SQLUtils.fromBLOB(rset.getBlob(i), sb);
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
                    catch (IOException e) {
                        throw(new JMSException("failed to read from clob "+
                            keys[i-1] + ": " + Event.traceStack(e)));
                    }
                }
            }
            MessageUtils.setProperty(key, line.toString(), msg);
            k = 1;
        }

        return k;
    }

    /** reconnects to the DB and returns null on success or err msg otherwise */
    public String reconnect() {
        close();
        String err = super.reconnect();
        if (err != null) {
            return err;
        }
        if ((xaMode & MessageUtils.XA_COMMIT) > 0) try {
            setAutoCommit(false);
        }
        catch (SQLException e) {
            close();
            return "failed to diable AutoCommit: " + e.toString();
        }
        return null;
    }

    /**
     * checks in the prepared statement and removes the asset for the result set
     */
    public void acknowledge(long[] state) throws JMSException {
        int id;
        long sid = -1L;
        Object o;
        Object[] asset;
        GenericPool pool;
        PreparedStatement ps;

        if (state == null)
            return;

        id = (int) state[ACK_ID];
        sid = state[ACK_SID];
        if (assetList.getCellStatus(id) != XQueue.CELL_OCCUPIED)
            return;
        o = assetList.browse(id);
        if (o == null || !(o instanceof Object[]))
            return;
        asset = (Object[]) o;
        state = (long[]) asset[ASSET_STATE];
        if (sid != state[ACK_SID]) // wrong sequenceID
            return;
        ps = (PreparedStatement) asset[ASSET_PS];
        pool = (GenericPool) asset[ASSET_POOL];
        if (pool != null)
            pool.checkin(ps, GenericPool.POOL_CLOSED);
        assetList.getNextCell(id);
        assetList.remove(id);
    }

    public void destroyObject(Object obj) {
        if (obj == null)
            return;
        else if (obj instanceof PreparedStatement) try {
            ps.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to close the statement" +
                e.toString()).send();
        }
    }

    public void close() {
        String[] keys;
        Object o;
        paramList.clear();
        assetList.clear();
        keys = cache.sortedKeys();
        for (int i=0; i<keys.length; i++) {
            if (keys[i] == null)
                continue;
            if ((o = cache.get(keys[i])) == null || !(o instanceof GenericPool))
                continue;
            ((GenericPool) o).close();
        }
        cache.clear();
        super.close();
    }

    public String getOperation() {
        return operation;
    }
}
