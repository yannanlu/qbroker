package org.qbroker.net;

/* JedisConnector.java - a TCP connector to Redis Server via Jedis */

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * JedisConnector connects to a Redis server and provides the following
 * methods for Redis operations: set(), get(), zcadd, rpush, lpop, publish
 * and list(). But it does not support synchronous subscribe.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JedisConnector implements Connector {
    protected String hostname = null;
    protected String uri = null;
    protected String username = null;
    protected String password = null;
    protected int port = 6379;
    protected int timeout = 60000;
    protected boolean isConnected = false;
    public static final int REDIS_NONE = 0;
    public static final int REDIS_STRING = 1;
    public static final int REDIS_LIST = 2;
    public static final int REDIS_HASH = 3;
    public static final int REDIS_SET = 4;
    public static final int REDIS_ZSET = 5;
    public static final int ACTION_GET = 0;
    public static final int ACTION_SET = 1;
    public static final int ACTION_KEYS = 3;
    public static final int ACTION_COUNT = 4;
    public static final int ACTION_SIZE = 5;
    public static final int ACTION_TIME = 6;
    public static final int ACTION_QUERY = 7;
    public static final int ACTION_UPDATE = 8;
    public static final int ACTION_PARSE = 9;
    public static final String[] Type = {"none", "string", "list", "hash",
        "set", "zset"};

    protected Jedis redisClient = null;

    /** Creates new JedisConnector */
    public JedisConnector(Map props) {
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

        if (!"redis".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 6379;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((o = props.get("Username")) != null) {
            username = (String) o;

            if ((o = props.get("Password")) != null)
                password = (String) o;
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) {
            String err = reconnect();
            if (err != null)
                throw(new IllegalArgumentException("failed to connect to " +
                    uri + ": " + err));
        }
    }

    protected void connect() throws JedisException {
        redisClient = new Jedis(hostname, port, timeout);
        redisClient.connect();

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

    public void close() {
        if (!isConnected)
            return;
        if (redisClient != null) try {
            redisClient.disconnect();
            redisClient = null;
        }
        catch (Exception e) {
        }
        isConnected = false;
    }

    /** publishes the string to the channel and returns the number of subs */
    public long redisPublish(String chl, String str) {
        if (chl == null || chl.length() <= 0 || str == null)
            return -1L;
        return redisClient.publish(chl, str).longValue();
    }

    /** appends the string to the list of key and returns its position */
    public long redisRpush(String key, String str) {
        if (key == null || key.length() < 0 || str == null)
            return -1L;
        return redisClient.rpush(key, str).longValue();
    }

    /** prefixes the string to the list of key and returns its position */
    public long redisLpush(String key, String str) {
        if (key == null || key.length() < 0 || str == null)
            return -1L;
        return redisClient.lpush(key, str).longValue();
    }

    /** removes the last item from the list of key and returns it or null */
    public String redisBrpop(int sec, String key) {
        List list = null;
        if (sec < 0)
            sec = 1;
        if (key == null || key.length() <= 0)
            return null;
        try {
            list = redisClient.brpop(sec, new String[]{key});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        if (list == null)
            return null;
        else if (list.size() < 2)
            return null;
        else
            return (String) list.get(1);
    }

    /** removes the first item from the list of key and returns it or null */
    public String redisBlpop(int sec, String key) {
        List list = null;
        if (sec < 0)
            sec = 1;
        if (key == null || key.length() <= 0)
            return null;
        try {
            list = redisClient.blpop(sec, new String[]{key});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        if (list == null)
            return null;
        else if (list.size() < 2)
            return null;
        else
            return (String) list.get(1);
    }

    public String redisGet(String key) {
        return redisClient.get(key);
    }

    public String redisSet(String key, String value) {
        return redisClient.set(key, value);
    }

    public String[] list(String keyPattern) {
        Set s;
        Object[] o;
        int i, n;
        String[] list;
        if (keyPattern == null || keyPattern.length() <= 0)
            keyPattern = "*";
        s = redisClient.keys(keyPattern);
        if (s == null)
            return null;
        n = s.size();
        list = new String[n];
        o = s.toArray();
        for (i=0; i<n; i++)
            list[i] = (String) o[i];
        return list; 
    }

    public int count(int type, String key) {
        int n = 0;
        if (key == null || key.length() <= 0)
            return -1;
        switch (type) {
          case REDIS_HASH:
            n = redisClient.hlen(key).intValue();
            break;
          case REDIS_LIST:
            n = redisClient.llen(key).intValue();
            break;
          case REDIS_SET:
            n = redisClient.scard(key).intValue();
            break;
          case REDIS_ZSET:
            n = redisClient.zcard(key).intValue();
            break;
          case REDIS_STRING:
            n = redisClient.strlen(key).intValue();
            break;
          default:
            n = getType(key);
            if (n > 0)
                n = count(n, key);
        }
        return n;
    }

    public long getTTL(String key) {
        if (key == null || key.length() <= 0)
            return -1;
        return redisClient.ttl(key).longValue();
    }

    public int getType(String key) {
        String str;
        int i;
        if (key == null || key.length() <= 0)
            return -1;
        str = redisClient.type(key);
        if ("list".equals(str))
            i = REDIS_LIST;
        else if ("hash".equals(str))
            i = REDIS_HASH;
        else if ("set".equals(str))
            i = REDIS_SET;
        else if ("zset".equals(str))
            i = REDIS_ZSET;
        else if ("string".equals(str))
            i = REDIS_STRING;
        else
            i = REDIS_NONE;
        return i;
    }

    public long getSize(int id) {
        if (id >= 0)
            redisClient.select(id);
        return redisClient.dbSize().longValue();
    }

    public Object update(String request) {
        if (request == null || request.length() <= 0)
            return null;
        else
            return update(parse(request));
    }

    public Object update(String[] args) {
        int type;
        String cmd, key = null;
        Object o = null;
        if (args == null || args.length < 1)
            return null;
        cmd = args[0].toLowerCase();
        if (args.length > 1)
            key = args[1];
        switch (cmd.charAt(0)) {
          case 'h':
            type = REDIS_HASH;
            if (args.length == 3) {
                if ("hdel".equals(cmd))
                    o = redisClient.hdel(key, args[2]);
            }
            else if (args.length > 3) {
                if ("hset".equals(cmd))
                    o = redisClient.hset(key, args[2], args[3]);
                else if ("hsetnx".equals(cmd))
                    o = redisClient.hsetnx(key, args[2], args[3]);
            }
            break;
          case 'b':
          case 'l':
          case 'r':
            type = REDIS_LIST;
            if (args.length == 2) {
                if ("lpop".equals(cmd))
                    o = redisClient.lpop(key);
                else if ("rpop".equals(cmd))
                    o = redisClient.rpop(key);
            }
            else if (args.length == 3) {
                if ("lpush".equals(cmd))
                    o = redisClient.lpush(key, args[2]);
                else if ("rpush".equals(cmd))
                    o = redisClient.rpush(key, args[2]);
                else if ("blpop".equals(cmd))
                    o = redisBlpop(Integer.parseInt(args[2]), key);
                else if ("brpop".equals(cmd))
                    o = redisBrpop(Integer.parseInt(args[2]), key);
            }
            else if (args.length > 3) {
                if ("lset".equals(cmd))
                    o = redisClient.lset(key, Long.parseLong(args[2]), args[3]);
                else if ("lrem".equals(cmd))
                    o = redisClient.lrem(key, Long.parseLong(args[2]), args[3]);
                else if ("ltrim".equals(cmd))
                    o = redisClient.ltrim(key, Long.parseLong(args[2]),
                        Long.parseLong(args[3]));
            }
            break;
          case 's':
            type = REDIS_SET;
            if ("smembers".equals(cmd) && args.length > 1)
                o = redisClient.smembers(key);
            break;
          case 'z':
            type = REDIS_ZSET;
            if (args.length == 3) {
                if ("zrem".equals(cmd))
                    o = redisClient.zrem(key, args[2]);
            }
            else if (args.length >= 4) {
                if ("zadd".equals(cmd))
                    o = redisClient.zadd(key, Double.parseDouble(args[2]),
                        args[3]);
                else if ("zincrby".equals(cmd))
                    o = redisClient.zincrby(key, Double.parseDouble(args[2]),
                        args[3]);
                else if ("zremrangebyrank".equals(cmd))
                    o = redisClient.zremrangeByRank(key,
                        Integer.parseInt(args[2]),
                        Integer.parseInt(args[3]));
                else if ("zremrangebyscore".equals(cmd))
                    o = redisClient.zremrangeByScore(key,
                        Double.parseDouble(args[2]),
                        Double.parseDouble(args[3]));
            }
            break;
          case 'p':
            type = REDIS_STRING;
            if (args.length == 3) {
                if ("publish".equals(cmd))
                    o = new Long(redisPublish(key, args[2]));
            }
            break;
          default:
            type = REDIS_STRING;
        }
        return o;
    }

    public Object query(String request) {
        if (request == null || request.length() <= 0)
            return null;
        else
            return query(parse(request));
    }

    public Object query(String[] args) {
        int type;
        String cmd, key = null;
        Object o = null;
        if (args == null || args.length < 1)
            return null;
        cmd = args[0].toLowerCase();
        if (args.length > 1)
            key = args[1];
        switch (cmd.charAt(0)) {
          case 'h':
            type = REDIS_HASH;
            if ("hget".equals(cmd) && args.length > 2)
                o = redisClient.hget(key, args[2]);
            else if ("hgetall".equals(cmd))
                o = redisClient.hgetAll(key);
            else if ("hkeys".equals(cmd))
                o = redisClient.hkeys(key);
            else if ("hvals".equals(cmd))
                o = redisClient.hvals(key);
            break;
          case 'l':
            type = REDIS_LIST;
            if ("lindex".equals(cmd) && args.length > 2)
                o = redisClient.lindex(key, Long.parseLong(args[2]));
            else if ("lrange".equals(cmd) && args.length > 3)
                o = redisClient.lrange(key, Long.parseLong(args[2]),
                    Long.parseLong(args[3]));
            break;
          case 's':
            type = REDIS_SET;
            if ("smembers".equals(cmd) && args.length > 1)
                o = redisClient.smembers(key);
            break;
          case 'z':
            type = REDIS_ZSET;
            if (args.length == 4) {
                if ("zrange".equals(cmd))
                    o = redisClient.zrange(key, Integer.parseInt(args[2]),
                        Integer.parseInt(args[3]));
                else if ("zrevrange".equals(cmd))
                    o = redisClient.zrevrange(key, Integer.parseInt(args[2]),
                        Integer.parseInt(args[3]));
                else if ("zrangebyscore".equals(cmd))
                    o = redisClient.zrangeByScore(key,
                        Double.parseDouble(args[2]),
                        Double.parseDouble(args[3]));
                else if ("zrevrangebyscore".equals(cmd))
                    o = redisClient.zrevrangeByScore(key,
                        Double.parseDouble(args[2]),
                        Double.parseDouble(args[3]));
            }
            else if (args.length > 4 && "withscores".equalsIgnoreCase(args[4])){
                if ("zrange".equals(cmd))
                    o = redisClient.zrangeWithScores(key,
                        Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                else if ("zrevrange".equals(cmd))
                    o = redisClient.zrevrangeWithScores(key,
                        Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                else if ("zrangebyscore".equals(cmd))
                    o = redisClient.zrangeByScoreWithScores(key,
                        Double.parseDouble(args[2]),
                        Double.parseDouble(args[3]));
                else if ("zrevrangebyscore".equals(cmd))
                    o = redisClient.zrevrangeByScoreWithScores(key,
                        Double.parseDouble(args[2]),
                        Double.parseDouble(args[3]));
            }
            break;
          default:
            type = REDIS_STRING;
        }
        return o;
    }

    /** parses the Redis request and returns args up to 5 of them */
    public static String[] parse(String request) {
        int i, j, n;
        char c;
        boolean hasQuote = false;
        String str;
        String[] cmd = new String[4];
        if (request == null || request.length() <= 3)
            return null;
        i = 0;
        n = request.length();
        while ((c = request.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 3)
            return null;
        j = request.indexOf(" ", i);
        if (j < i)
            return new String[] {request.substring(i)};
        else
            str = request.substring(i, j);
        cmd[0] = str;
        i = j;
        while ((c = request.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (i >= n)
            return new String[] {str};
        j = request.indexOf(" ", i);
        if (j < i)
            return new String[] {str, request.substring(i)};
        else
            str = request.substring(i, j);
        cmd[1] = str;
        i = j;
        while ((c = request.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (i >= n)
            return new String[] {cmd[0], str};
        else if (c == '"') { // begin of quote
            hasQuote = true;
            for (j=i+1; j<n; j++) { // look for end of quote
                c = request.charAt(j);
                if (c == '"' && request.charAt(j-1) != '\\') // found it
                    break;
            }
        }
        else
            j = request.indexOf(" ", i);
        if (j < i || j >= n)
            return new String[] {cmd[0], str, request.substring(i)};
        else if (hasQuote) {
            str = request.substring(i+1, j);
            hasQuote = false;
            j ++;
            if (j >= n)
                return new String[] {cmd[0], cmd[1], str};
        }
        else
            str = request.substring(i, j);
        cmd[2] = str;
        i = j;
        while ((c = request.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (i >= n)
            return new String[] {cmd[0], cmd[1], str};
        else if (c == '"') { // begin of quote
            hasQuote = true;
            for (j=i+1; j<n; j++) { // look for end of quote
                c = request.charAt(j);
                if (c == '"' && request.charAt(j-1) != '\\') // found it
                    break;
            }
        }
        else
            j = request.indexOf(" ", i);
        if (j < i || j >= n)
            return new String[] {cmd[0], cmd[1], str, request.substring(i)};
        else if (hasQuote) {
            str = request.substring(i+1, j);
            hasQuote = false;
            j ++;
            if (j >= n)
                return new String[] {cmd[0], cmd[1], cmd[2], str};
        }
        else
            str = request.substring(i, j);
        cmd[3] = str;
        i = j;
        while ((c = request.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (i >= n)
            return new String[] {cmd[0], cmd[1], cmd[2], str};
        else
            str = request.substring(i);

        return new String[] {cmd[0], cmd[1], cmd[2], cmd[3], str.trim()};
    }

    public static void main(String args[]) {
        HashMap<String, Object> props;
        Object o;
        URI u;
        int i, timeout = 60, action = ACTION_SIZE;
        String key = null, uri = null, str = null, value = null, cmd = null;
        String[] list;
        long size, mtime;
        JedisConnector conn = null;
        SimpleDateFormat dateFormat;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zz");
        props = new HashMap<String, Object>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                if (i == args.length-1)
                    cmd = args[i];
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'a':
                str = args[++i];
                if ("get".equalsIgnoreCase(str))
                    action = ACTION_GET;
                else if ("set".equalsIgnoreCase(str))
                    action = ACTION_SET;
                else if ("count".equalsIgnoreCase(str))
                    action = ACTION_COUNT;
                else if ("size".equalsIgnoreCase(str))
                    action = ACTION_SIZE;
                else if ("list".equalsIgnoreCase(str))
                    action = ACTION_KEYS;
                else if ("query".equalsIgnoreCase(str))
                    action = ACTION_QUERY;
                else if ("update".equalsIgnoreCase(str))
                    action = ACTION_UPDATE;
                else if ("parse".equalsIgnoreCase(str))
                    action = ACTION_PARSE;
                else
                    action = ACTION_SIZE;
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
              case 'k':
                if (i+1 < args.length)
                    key = args[++i];
                break;
              case 'v':
                if (i+1 < args.length)
                    value = args[++i];
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

        try {
            conn = new JedisConnector(props);
            str = null;
            switch (action) {
              case ACTION_GET:
                if (key != null && key.length() > 0) {
                    str = conn.redisGet(key);
                    System.out.println(key + ": " + str);
                    str = null;
                }
                else
                    str = "key is not defined";
                break;
              case ACTION_SET:
                if (key != null && key.length() > 0) {
                    str = conn.redisSet(key, value);
                    System.out.println(key + ": " + str);
                    str = null;
                }
                else
                    str = "key is not defined";
                break;
              case ACTION_COUNT:
                if (key != null && key.length() > 0) {
                    i = conn.getType(key);
                    size = conn.count(i, key);
                    System.out.println(key + ": " + size);
                }
                else
                    str = "key is not defined";
                break;
              case ACTION_SIZE:
                size = conn.getSize(-1);
                if (size >= 0)
                    System.out.println("size: " + size);
                else
                    str = "failed to get size for " + 0;
                break;
              case ACTION_KEYS:
                list = conn.list(key);
                if (list != null) {
                    System.out.println(0 + ": " + list.length);
                    for (i=0; i<list.length; i++)
                        System.out.println(list[i] + " " +
                            Type[conn.getType(list[i])]);
                }
                else
                    str = "failed to list on " + 0;
                break;
              case ACTION_UPDATE:
                if (cmd != null) {
                    o = conn.update(cmd);
                    if (o != null) {
                        System.out.println(cmd + ": " + o.toString());
                    }
                    else
                        str = "update failed by " + cmd;
                }
                else
                    str = "cmd not defined";
                break;
              case ACTION_PARSE:
                if (cmd != null) {
                    list = parse(cmd);
                    if (list != null && list.length > 0) {
                        System.out.println("cmd: " + cmd);
                        for (i=0; i<list.length; i++)
                            System.out.println(i + ": " + list[i]);
                    }
                    else
                        str = "cmd is empty: " + cmd;
                }
                else
                    str = "cmd not defined";
                break;
              case ACTION_QUERY:
                if (cmd != null) {
                    list = parse(cmd);
                    if (list != null && list.length > 0) {
                        o = conn.query(list);
                        if (o == null)
                            str = "cmd failed: " + cmd;
                        else if (o instanceof List) {
                            List pl = (List) o;
                            i = pl.size();
                            System.out.println("got " + i +
                                " items from cmd: '" + cmd + "'\n");
                            i = 0;
                            for (Object v : pl)
                                System.out.println(i++ + ": " + (String) v);
                        }
                        else if (o instanceof Set) {
                            Set ps = (Set) o;
                            i = ps.size();
                            System.out.println("got " + i +
                                " items from cmd: '" + cmd + "'\n");
                            i = 0;
                            for (Object v : ps)
                                System.out.println(i++ + ": " + (String) v);
                        }
                        else if (o instanceof Map) {
                            Map pm = (Map) o;
                            i = pm.size();
                            System.out.println("got " + i +
                                " items from cmd: '" + cmd + "'\n");
                            for (Object v : pm.keySet())
                                System.out.println((String) v + ": " +
                                    (String) pm.get(v));
                        }
                        else if (o instanceof String) {
                            System.out.println("got 1 item from cmd: '" +
                                cmd + "'\n" + (String) o);
                        }
                        else if (o instanceof Integer) {
                            System.out.println("got 1 item from cmd: '" +
                                cmd + "'\n" + ((Integer) o).intValue());
                        }
                        else {
                            System.out.println("got 1 item from cmd: '" +
                                cmd + "'\n" + o.toString());
                        }
                    }
                    else
                        str = "cmd is empty: " + cmd;
                }
                else
                    str = "cmd not defined";
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
        System.out.println("JedisConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("JedisConnector: a Redis client");
        System.out.println("Usage: java org.qbroker.net.JedisConnector -u uri -t timeout -n username -p password -k key -a action [cmd]");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of size, list, count, set, get, update, parse and query (default: size)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -k: key");
        System.out.println("  -v: value");
    }
}
