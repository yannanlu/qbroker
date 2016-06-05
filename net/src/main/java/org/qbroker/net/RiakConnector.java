package org.qbroker.net;

/* RiakConnector.java - a Riak connector for data buckets */

import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Date;
import java.text.SimpleDateFormat;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.RiakException;
import org.qbroker.common.Connector;

/**
 * RiakConnector connects to a Riak service and provides some basic Riak
 * operation methods: list(), fetch(), store(), and delete().
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RiakConnector {
    protected String hostname;
    protected String username = null;
    protected String password = null;
    protected String uri;
    protected int port = 8087;
    protected int timeout = 120000;
    protected boolean isConnected = false;
    public static final int ACTION_LIST = 0;
    public static final int ACTION_COUNT = 1;
    public static final int ACTION_FETCH = 2;
    public static final int ACTION_STORE = 3;
    public static final int ACTION_DELETE = 4;

    protected IRiakClient riakClient = null;
    private int bufferSize = 4096;

    /** Creates new RiakConnector */
    public RiakConnector(Map props) {
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

        if (!"riak".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 8087;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

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

    /** it connects to a Riak server and establishes the client */
    private void connect() throws UnknownHostException, RiakException {
        final Configuration conf = new PBClientConfig.Builder()
            .withConnectionTimeoutMillis(timeout)
            .withHost(hostname)
            .build();
        riakClient = RiakFactory.newClient(conf);
        if (riakClient != null)
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
            return e.toString();
        }
        return null;
    }

    /** returns the Bucket or null if name does not exist in Riak */
    public Bucket fetchBucket(String bucketName) throws RiakException {
        if (bucketName == null || bucketName.length() <= 0)
            return null;
        return riakClient.fetchBucket(bucketName).execute();
    }

    /** returns IRiakObject for the key or null on failure */
    public IRiakObject fetch(String bucketName,String key) throws RiakException{
        if (bucketName == null || bucketName.length() <= 0)
            return null;
        final Bucket bucket = riakClient.fetchBucket(bucketName).execute();
        if (bucket != null)
            return bucket.fetch(key).execute();
        else
            return null;
    }

    /** returns id of the inserted doc or null on failure */
    public String store(String bucketName, String key, String data)
        throws RiakException {
        if (bucketName == null || bucketName.length() <= 0)
            return null;
        if (key == null || data == null)
            return null;
        final Bucket bucket = riakClient.fetchBucket(bucketName).execute();
        if (bucket == null)
            return null;
        bucket.store(key, data).execute();
        return key;
    }

    /** returns id of the inserted doc with meta data or null on failure */
    public String store(String bucketName, String key, String data,
        Map<String, String> meta) throws RiakException {
        if (meta == null || meta.size() <= 0)
            return store(bucketName, key, data);
        if (bucketName == null || bucketName.length() <= 0)
            return null;
        if (key == null || data == null)
            return null;
        final Bucket bucket = riakClient.fetchBucket(bucketName).execute();
        if (bucket == null)
            return null;
        RiakObjectBuilder o = RiakObjectBuilder
            .newBuilder(bucketName, key)
            .withValue(data);
        for (String ky : meta.keySet()) {
            if ("ContentType".equals(ky))
                o.withContentType(meta.get("ContentType"));
            else
                o.addIndex(ky, meta.get("ContentType"));
        }
        bucket.store(o.build()).execute();
        return key;
    }

    /** deletes the key and returns number of keys removed or -1 on failure */
    public int delete(String bucketName, String key) throws RiakException {
        if (bucketName == null || bucketName.length() <= 0 || key == null ||
            key.length() <= 0)
            return -1;
        final Bucket bucket = riakClient.fetchBucket(bucketName).execute();
        if (bucket == null)
            return -1;
        
        Object o = bucket.delete(key).execute();
        return (o != null) ? 1 : 0;
    }

    /** returns an array of all bucket names */
    public String[] list() throws RiakException {
        final Set<String> buckets = riakClient.listBuckets();
        int n = buckets.size();
        String[] list = new String[n];
        n = 0;
        for (String name : buckets) {
            list[n++] = name;
        }
        return list;
    }

    /** closes and shutdown the Riak client */
    public void close() {
        if (!isConnected)
            return;
        isConnected = false;
        if (riakClient != null) {
            riakClient.shutdown();
            riakClient = null;
        }
    }

    /** tests Riak basic operations */
    public static void main(String args[]) {
        HashMap<String, Object> props;
        URI u;
        int i, timeout = 60, action = ACTION_LIST;
        String bucketName = null, uri = null, str = null;
        String key = null, data = null;
        String[] list;
        long size, mtime;
        RiakConnector conn = null;
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
                else if ("fetch".equalsIgnoreCase(str))
                    action = ACTION_FETCH;
                else if ("store".equalsIgnoreCase(str))
                    action = ACTION_STORE;
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
              case 'b':
                if (i+1 < args.length)
                    bucketName = args[++i];
                break;
              case 'k':
                if (i+1 < args.length)
                    key = args[++i];
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

        if (action != ACTION_LIST && bucketName == null)
            throw(new IllegalArgumentException("bucketName is not specified"));

        try {
            IRiakObject o;
            Bucket b;
            conn = new RiakConnector(props);
            str = null;
            switch (action) {
              case ACTION_LIST:
                list = conn.list();
                if (list != null) {
                    for (i=0; i<list.length; i++)
                        System.out.println(list[i]);
                }
                else
                    str = "failed to list buckets";
                break;
              case ACTION_FETCH:
                if (key != null) {
                    o = conn.fetch(bucketName, key);
                    if (o != null)
                        System.out.println(bucketName + "/" + key + ": " +
                            o.getContentType() + "\n" + o.getValueAsString());
                    else
                        str = "failed to fetch the key in " + bucketName;
                }
                else if ((b = conn.fetchBucket(bucketName)) != null) {
                    // list all keys in the bucket
                    for (String ky: b.keys()) {
                        if (ky != null && ky.length() > 0)
                            System.out.println("key: " + ky);
                    }
                }
                else
                    str = "no such bucket: " + bucketName;
                break;
              case ACTION_STORE:
                str = conn.store(bucketName, key, data);
                if (str != null) {
                    System.out.println(str + " has been stored to " +
                        bucketName);
                    str = null;
                }
                else
                    str = "failed to store the key to " + bucketName;
                break;
              case ACTION_DELETE:
                i = conn.delete(bucketName, key);
                if (i >= 0)
                    System.out.println(i + " key have been deleted from " +
                        bucketName);
                else
                    str = "failed to delete key from " + bucketName + ": " + i;
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
        System.out.println("RiakConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("RiakConnector: a Riak client");
        System.out.println("Usage: java org.qbroker.net.RiakConnector -u uri -t timeout -b bucket_name -k key -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of list, fetch, store, delete (default: list)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -b: bucket name");
        System.out.println("  -k: key");
        System.out.println("  -d: data");
    }
}
