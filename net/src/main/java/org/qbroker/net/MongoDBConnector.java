package org.qbroker.net;

/* MongoDBConnector.java - a MongoDB connector for collections */

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.WriteResult;
import com.mongodb.WriteConcern;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.qbroker.common.Utils;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * MongoDBConnector connects to a MongoDB and provides some basic DB operation
 * methods: list(), count(), find(), findone(), insert(), update() and remove().
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MongoDBConnector implements Connector {
    protected String hostname;
    protected String username = null;
    protected String password = null;
    protected String dbName;
    protected String uri;
    protected int port = 27017;
    protected int timeout = 60000;
    protected boolean isConnected = false;
    protected boolean isSlaveOK = false;
    public static final int ACTION_LIST = 0;
    public static final int ACTION_COUNT = 1;
    public static final int ACTION_FIND = 2;
    public static final int ACTION_FINDONE = 3;
    public static final int ACTION_FINDANDMODIFY = 4;
    public static final int ACTION_FINDANDREMOVE = 5;
    public static final int ACTION_INSERT = 6;
    public static final int ACTION_REMOVE = 7;
    public static final int ACTION_UPDATE = 8;
    public static final int ACTION_UPDATEMULTI = 9;
    public static final int ACTION_GROUP = 10;
    public static final int ACTION_MAPREDUCE = 11;
    private static final DBObject emptyQuery = new BasicDBObject();
    private static final HashMap<String, Integer> actionMap =
        new HashMap<String, Integer>();

    private Mongo mongo = null;
    private DB db = null;
    private int bufferSize = 4096;

    static {
        actionMap.put("list", new Integer(ACTION_LIST));
        actionMap.put("count", new Integer(ACTION_COUNT));
        actionMap.put("find", new Integer(ACTION_FIND));
        actionMap.put("findone", new Integer(ACTION_FINDONE));
        actionMap.put("findandmodify", new Integer(ACTION_FINDANDMODIFY));
        actionMap.put("findandremove", new Integer(ACTION_FINDANDREMOVE));
        actionMap.put("insert", new Integer(ACTION_INSERT));
        actionMap.put("remove", new Integer(ACTION_REMOVE));
        actionMap.put("update", new Integer(ACTION_UPDATE));
        actionMap.put("updatemulti", new Integer(ACTION_UPDATEMULTI));
        actionMap.put("group", new Integer(ACTION_GROUP));
        actionMap.put("mapreduce", new Integer(ACTION_MAPREDUCE));
    }

    /** Creates new MongoDBConnector */
    public MongoDBConnector(Map props) {
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

        if (!"mongodb".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 27017;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((dbName = u.getPath()) == null || dbName.length() <= 1)
            throw(new IllegalArgumentException("no dbName specified in URI"));
        dbName = dbName.substring(1);

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

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

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

    /** it connects to a MongoDB server and establishes the DB instance */
    private void connect() throws UnknownHostException, MongoException {
        boolean auth = false;
        mongo = new Mongo(hostname, port);
        if (isSlaveOK)
            mongo.slaveOk();
        db = mongo.getDB(dbName);
        if (username != null && password != null) {
            int n = password.length();
            char[] pw = new char[n];
            password.getChars(0, n, pw, 0);
            auth = db.authenticate(username, pw);
            if (! auth) {
                mongo.close();
                mongo = null;
                db = null;
                isConnected = false;
            }
        }
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
        close();
        try {
            connect();
        }
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    /** parses a JSON text into a DBObject */
    public static DBObject parse(String jsonText) {
        Object o;
        if (jsonText == null || jsonText.length() <= 1)
            return null;
        o = JSON.parse(jsonText);
        if (o != null && o instanceof Map)
            return new BasicDBObject((Map) o);
        else
            return null;
    }

    /** returns the collection or null if name is not well defined */
    public DBCollection getCollection(String collName) {
        if (collName == null || collName.length() <= 0)
            return null;
        return db.getCollection(collName);
    }

    /**
     * It returns DBCursor for all the found docs with the query and the
     * projection or null on failure.
     */
    public DBCursor find(String collName, String query, String projection)
        throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0)
            return null;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return null;
        if (projection == null || (o = parse(projection)) == null) {
            if (query == null)
                return coll.find();
            else if ((o = parse(query)) != null)
                return coll.find(o);
            else
                return null;
        }
        else { // with projection
            DBObject p = (DBObject) o;
            if (query == null)
                return coll.find(emptyQuery, p);
            else if ((o = parse(query)) != null)
                return coll.find(o, p);
            else
                return null;
        }
    }

    /** returns DBCursor for all the found docs or null on failure */
    public DBCursor find(String collName, String query) throws MongoException {
        return find(collName, query, null);
    }

    /**
     * It returns DBObject for the found doc with the query and the projection
     * or null on failure.
     */
    public DBObject findOne(String collName, String query, String projection)
        throws MongoException{
        DBObject o = null;
        if (collName == null || collName.length() <= 0)
            return null;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return null;
        if (projection == null || (o = parse(projection)) == null) {
            if (query == null)
                return coll.findOne();
            else if ((o = parse(query)) != null)
                return coll.findOne(o);
            else
                return null;
        }
        else { // with projection
            DBObject p = (DBObject) o;
            if (query == null)
                return coll.findOne(emptyQuery, p);
            else if ((o = parse(query)) != null)
                return coll.findOne(o, p);
            else
                return null;
        }
    }

    /** returns DBObject for the found docs with the query or null on failure */
    public DBObject findOne(String collName,String query) throws MongoException{
        return findOne(collName, query, null);
    }

    /** returns id of the inserted doc or null on failure */
    public String insert(String collName, String data) throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0 || data == null ||
            data.length() <= 0)
            return null;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return null;

        if ((o = parse(data)) != null) {
            WriteResult r = coll.insert(o, new WriteConcern(0));
            if (r.getError() != null)
                return null;
            else
                return o.get("_id").toString();
        }
        return null;
    }

    /** returns number of the updated doc for the query or -1 on failure */
    public int update(String collName, String query, String data)
        throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0 || data == null ||
            data.length() <= 0 || query == null || query.length() <= 0)
            return -1;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return -1;

        if ((o = parse(query)) != null) {
            WriteResult r = coll.update(o, parse(data));
            int i = r.getN();
            if (i > 0)
                return i;
            else if (r.getError() != null)
                return -1;
            else
                return i;
        }
        return -1;
    }

    /** returns number of updated docs for the query or -1 on failure */
    public int updateMulti(String collName, String query, String data)
        throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0 || data == null ||
            data.length() <= 0 || query == null || query.length() <= 0)
            return -1;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return -1;

        if ((o = parse(query)) != null) {
            WriteResult r = coll.updateMulti(o, parse(data));
            int i = r.getN();
            if (i > 0)
                return i;
            else if (r.getError() != null)
                return -1;
            else
                return i;
        }
        return -1;
    }

    /** returns number of removed docs for the query or -1 on failure */
    public int remove(String collName, String query) throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0 || query == null ||
            query.length() <= 0)
            return -1;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return -1;
        
        if ((o = parse(query)) != null) {
            WriteResult r = coll.remove(o, new WriteConcern(0));
            int i = r.getN();
            if (i > 0)
                return i;
            else if (r.getError() != null)
                return -1;
            else
                return i;
        }
        return -1;
    }

    /** returns the count of the query for the collection or -1 on failure */
    public long count(String collName, String query) throws MongoException {
        DBObject o = null;
        if (collName == null || collName.length() <= 0)
            return -1;
        DBCollection coll = db.getCollection(collName);
        if (coll == null)
            return -1;
        if (query == null)
            return coll.count();
        else if ((o = parse(query)) != null)
            return coll.count(o);
        else
            return -1;
    }

    /** returns an array of collection names for the DB */
    public String[] list() throws MongoException {
        Set colls = db.getCollectionNames();
        int n = colls.size();
        String[] list = new String[n];
        n = 0;
        Iterator iter = colls.iterator();
        while (iter.hasNext()) {
            list[n++] = (String) iter.next();
        }
        return list;
    }

    /** closes the MongoDB connections */
    public void close() {
        if (!isConnected)
            return;
        if (mongo != null) try {
            mongo.close();
        }
        catch (Exception e) {
        }
        mongo = null;
        db = null;
        isConnected = false;
    }

    /** returns the id for the command or -1 if the cmd is not supported */
    public static final int getCommandID(String cmd) {
        Integer i = actionMap.get(cmd);
        if (i != null)
            return i.intValue();
        else
            return -1;
    }

    /** tests MongoDB basic operations */
    public static void main(String args[]) {
        HashMap<String, Object> props;
        URI u;
        int i, timeout = 60, action = ACTION_LIST;
        String collName = null, uri = null, str = null, dbName = null;
        String query = null, data = null;
        String[] list;
        long size, mtime;
        MongoDBConnector conn = null;
        DBObject o = null;
        DBCursor c = null;
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
                action = getCommandID(str);
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
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'c':
                if (i+1 < args.length)
                    collName = args[++i];
                break;
              case 'q':
                if (i+1 < args.length)
                    query = args[++i];
                break;
              case 'd':
                if (i+1 < args.length)
                    data = args[++i];
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

        if ((dbName = u.getPath()) == null || dbName.length() <= 1)
            throw(new IllegalArgumentException("dbName not defined: " + uri));
        dbName = dbName.substring(1);
        
        if (action != ACTION_LIST && collName == null)
            throw(new IllegalArgumentException("collName is not specified"));

        try {
            conn = new MongoDBConnector(props);
            str = null;
            switch (action) {
              case ACTION_LIST:
                list = conn.list();
                if (list != null) {
                    System.out.println(dbName + ": " + list.length);
                    for (i=0; i<list.length; i++)
                        System.out.println(list[i]);
                }
                else
                    str = "failed to list on " + dbName;
                break;
              case ACTION_COUNT:
                size = conn.count(collName, query);
                if (size >= 0)
                    System.out.println(dbName + "." + collName + ": " + size);
                else
                    str = "failed to get count for " + collName;
                break;
              case ACTION_FINDONE:
                o = conn.findOne(collName, query, data);
                if (o != null)
                    System.out.println(dbName + "." + collName + ":\n" +
                        o.toString());
                else
                    str = "failed to find the doc in " + collName;
                break;
              case ACTION_FIND:
                c = conn.find(collName, query, data);
                if (c != null) {
                    System.out.println(dbName +"."+ collName + ": "+ c.size());
                    if (c.size() > 100)
                        c.batchSize(100);
                    while (c.hasNext()) {
                        System.out.println(c.next().toString());
                    }
                    c.close();
                }
                else
                    str = "failed to query on " + collName;
                break;
              case ACTION_INSERT:
                str = conn.insert(collName, data);
                if (str != null) {
                    System.out.println(str + " has been inserted to " +
                        collName);
                    str = null;
                }
                else
                    str = "failed to insert the doc to " + collName;
                break;
              case ACTION_UPDATE:
                i = conn.update(collName, query, data);
                if (i >= 0)
                    System.out.println(i + " doc has been updated in " +
                        collName);
                else
                    str = "failed to update the doc in " + collName + ": " + i;
                break;
              case ACTION_UPDATEMULTI:
                i = conn.updateMulti(collName, query, data);
                if (i >= 0)
                    System.out.println(i + " docs have been updated in " +
                        collName);
                else
                    str = "failed to update docs in " + collName + ": " + i;
                break;
              case ACTION_REMOVE:
                i = conn.remove(collName, query);
                if (i >= 0)
                    System.out.println(i + " docs have been removed from " +
                        collName);
                else
                    str = "failed to remove docs from " + collName + ": " + i;
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
        System.out.println("MongoConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("MongoConnector: a MongoDB client");
        System.out.println("Usage: java org.qbroker.net.MongoDBConnector -u uri -t timeout -c coll_filename -q query -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of list, count, find, findone, insert, update, updatemulti or remove (default: list)");
        System.out.println("  -b: buffer size in byte");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -c: collection name");
        System.out.println("  -q: query");
        System.out.println("  -d: data for insert or update, projection for find");
    }
}
