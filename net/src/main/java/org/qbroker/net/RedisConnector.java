package org.qbroker.net;

/* RedisRequester.java - a Redis socket connector */

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.Connector;
import org.qbroker.net.ClientSocket;

/**
 * RedisConnector connects to a Redis server and provides the following
 * methods for Redis operations: rpush, lpop, blpop, publish, subscribe,
 * unsubscribe and nextMessage.
 *<br/><br/>
 * This is NOT MT-Safe. Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RedisConnector implements Connector {
    protected String username = null;
    protected String password = null;
    protected String uri = null;
    protected String host = null;
    protected int port;
    protected int timeout = 60000;

    private Socket sock = null;
    private byte[] buffer;
    private int bufferSize = 8192;
    private int totalBytes = 0;
    private int subscriptions = 0;
    private int msec = 500;
    private boolean isConnected = false;

    public final static int REDISTIMEOUT = -1;
    public final static int PROTOCOLERROR = -2;
    public final static int NOTCONNECTED = -3;
    public final static int REDISERROR = -4;
    public final static int SOCKERROR = -5;
    public final static int SERVERERROR = -6;
    public final static int READFAILED = -7;
    public final static int WRITEFAILED = -8;
    public final static int CONNTIMEOUT = -9;
    public final static int CONNREFUSED = -10;
    public final static int REDIS_RPUSH = 0;
    public final static int REDIS_PUBLISH = 1;
    public final static int REDIS_ZADD = 2;
    public final static int REDIS_LPOP = 3;
    public final static int REDIS_BLPOP = 4;
    public final static int REDIS_SUBSCRIBE = 5;
    public final static int REDIS_UNSUBSCRIBE = 6;

    /** Creates new RedisConnector */
    public RedisConnector(Map props) {
        Object o;
        URI u;
        String str;

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

        host = u.getHost();
        if ((port = u.getPort()) <= 0)
            port = 6379;

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null) {
                password = (String) o;
            }
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
        }

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) {
            str = reconnect();
            if (str != null)
                throw(new IllegalArgumentException("failed to connect to " +
                    uri + ": " + str));
        }
        buffer = new byte[bufferSize];
    }

    public long lpop(String key, OutputStream bos) {
        int i;
        if (key == null || key.length() < 0 || bos == null)
            return -1L;
        try {
            i = sendRequest(redisCommand(REDIS_LPOP, key, null), msec);
            if (i > 0)
                i = receive(sock.getInputStream(), i, msec, bos);
        }
        catch (Exception e) {
            i = -1;
        }
        return (long) i;
    }

    public long blpop(String key, int sec, OutputStream bos) {
        int i, ms;
        if (key == null || key.length() < 0 || bos == null)
            return -2L;
        ms = sec * 1000;
        try {
            i = sendRequest(redisCommand(REDIS_BLPOP, key, "", ms), ms + ms);
            if (i > 0)
                i = receive(sock.getInputStream(), i, msec, bos);
        }
        catch (Exception e) {
            i = -3;
        }
        return (long) i;
    }

    public long rpush(String key, String data) {
        int i;
        if (key == null || key.length() < 0 || data == null)
            return -1L;
        try {
            i = sendRequest(redisCommand(REDIS_RPUSH, key, data), msec);
        }
        catch (Exception e) {
            i = -1;
        }
        return (long) i;
    }

    public long publish(String key, String data) {
        int i;
        if (key == null || key.length() < 0 || data == null)
            return -1L;
        try {
            i = sendRequest(redisCommand(REDIS_PUBLISH, key, data), msec);
        }
        catch (Exception e) {
            i = -1;
        }
        return (long) i;
    }

    public synchronized int subscribe(String key) {
        long i;
        if (key == null || key.length() < 0)
            return -1;
        try {
            i = sendRequest(redisCommand(REDIS_SUBSCRIBE, key, ""), msec);
            if (i == 3) {
                InputStream in = sock.getInputStream();
                i = getLength(in, msec);
                getKey(in, (int) i, msec);
                i = getLength(in, msec);
                getKey(in, (int) i, msec);
                i = getLength(in, msec);
                if (i >= 0)
                    subscriptions = (int) i;
            }
        }
        catch (Exception e) {
            i = -1;
        }
        return (int) i;
    }

    public synchronized int unsubscribe(String key) {
        long i;
        if (key == null || key.length() < 0)
            return -1;
        try {
            i = sendRequest(redisCommand(REDIS_UNSUBSCRIBE, key, ""), msec);
            if (i == 3) {
                InputStream in = sock.getInputStream();
                i = getLength(in, msec);
                getKey(in, (int) i, msec);
                i = getLength(in, msec);
                getKey(in, (int) i, msec);
                i = getLength(in, msec);
                if (i >= 0)
                    subscriptions = (int) i;
            }
        }
        catch (Exception e) {
            i = -1;
        }
        return (int) i;
    }

    public String nextMessage(int sec, OutputStream bos) throws IOException {
        long i;
        int ms = sec * 1000;
        String key;
        InputStream in;
        if (subscriptions <= 0)
            throw(new IOException("no subscription"));
        in = sock.getInputStream();
        i = getLength(in, ms);
        if (i == -1) // timeout
            return null;
        else if (i != 3)
            throw(new IOException("failed to read 1st length: " + i));
        i = getLength(in, msec);
        if (i != 7)
            throw(new IOException("failed to read 2nd length: " + i));
        getKey(in, (int) i, msec);
        i = getLength(in, msec);
        if (i <= 0)
            throw(new IOException("failed to read 3rd length: " + i));
        key = getKey(in, (int) i, msec);
        i = getLength(in, msec);
        if (i < 0)
            throw(new IOException("failed to read last length: " + i));
        receive(in, (int) i, msec, bos);
        return key;
    }

    private int sendRequest(byte[] buf, int ms) throws IOException {
        return sendRequest(buf, 0, buf.length, ms);
    }

    private int sendRequest(byte[] buf, int j, int size, int ms)
        throws IOException {
        String str, text;
        long timeStart;
        int c, i, k, bytesRead, timeLeft, ret = -1;
        InputStream in = null;
        OutputStream out = null;

        if (sock == null)
            return NOTCONNECTED;

        timeStart = System.currentTimeMillis();
        timeLeft = (ms > 0) ? ms : timeout;
        try {
            sock.setSoTimeout(timeLeft);
            in = sock.getInputStream();
            out = sock.getOutputStream();
        }
        catch (Exception e) {
            close(out);
            out = null;
            close(in);
            in = null;
            return SOCKERROR;
        }
        if (in == null || out == null) {
            close(out);
            out = null;
            close(in);
            in = null;
            return SOCKERROR;
        }

        try { // write request to server
            out.write(buf, j, size);
            out.flush();
        }
        catch (IOException e) {
            close(out);
            out = null;
            close(in);
            in = null;
            return WRITEFAILED;
        }

        text = null;
        k = totalBytes;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            if (sock != null) try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft -= (int) (System.currentTimeMillis() - timeStart);
                if (timeLeft > 0)
                    continue;
            }
            catch (IOException e) {
                close(out);
                out = null;
                close(in);
                in = null;
                return READFAILED;
            }
            catch (Exception e) {
                close(out);
                out = null;
                close(in);
                in = null;
                return SERVERERROR;
            }
            c = buffer[0];
            for (i=1; i<totalBytes; i++) {
                if (c == '\r' && buffer[i] == '\n')
                    break;
                else
                    c = buffer[i];
            }
            if (i >= totalBytes) {
                k = totalBytes;
            }
            else {
                k = i + 1;
                text = new String(buffer, 1, k-3);
                switch (buffer[0]) {
                  case '+':
                    ret = 0;
                    break;
                  case '-':
                    ret = REDISERROR;
                    break;
                  case ':':
                    ret = Integer.parseInt(text);
                    break;
                  case '$':
                    ret = Integer.parseInt(text);
                    break;
                  case '*':
                    ret = Integer.parseInt(text);
                    break;
                  default:
                }
                totalBytes -= k;
                bytesRead = totalBytes;
                for (i=0; i<bytesRead; i++) // shift left
                    buffer[i] = buffer[i+k];
                k = 0;
                break;
            }
            timeLeft -=  (int) (System.currentTimeMillis() - timeStart);
        } while (bytesRead >= 0 && timeLeft > 0);

        return ret;
    }

    private int receive(InputStream in, int size, int ms, OutputStream bos)
        throws IOException {
        long timeStart;
        int i, k, bytesRead, timeLeft;

        if (bos == null)
            throw(new IOException("output stream is null"));

        if (totalBytes >= size + 2) {
            bos.write(buffer, 0, size);
            for (i=size+2; i<totalBytes; i++)
                buffer[i-size-2] = buffer[i];
            totalBytes -= size + 2;
            size = 0;
            return size;
        }

        if (in == null)
            throw(new IOException("input stream is null"));

        timeStart = System.currentTimeMillis();
        timeLeft = (ms > 0) ? ms : timeout;

        k = totalBytes;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            if (sock != null) try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft -= (int) (System.currentTimeMillis() - timeStart);
                if (timeLeft > 0)
                    continue;
            }
            if (size <= 0) {
                if (totalBytes >= 2) {
                    for (i=2; i<totalBytes; i++)
                        buffer[i-2] = buffer[i];
                    totalBytes -= 2;
                    break;
                }
            }
            else if (totalBytes >= size + 2) {
                bos.write(buffer, 0, size);
                for (i=size+2; i<totalBytes; i++)
                    buffer[i-size-2] = buffer[i];
                totalBytes -= size + 2;
                size = 0;
                break;
            }
            else if (totalBytes >= size) {
                bos.write(buffer, 0, size);
                for (i=size; i<totalBytes; i++)
                    buffer[i-size] = buffer[i];
                totalBytes -= size;
                size = 0;
            }
            else {
                bos.write(buffer, 0, totalBytes);
                size -= totalBytes;
                totalBytes = 0;
            }
            k = totalBytes;
            timeLeft -= (int) (System.currentTimeMillis() - timeStart);
        } while (bytesRead >= 0 && timeLeft > 0);

        return size;
    }

    private long getLength(InputStream in, int ms) throws IOException {
        String text;
        long timeStart;
        int c, i, k, bytesRead, timeLeft;

        if (totalBytes > 0) {
            c = buffer[0];
            for (i=1; i<totalBytes; i++) {
                if (c == '\r' && buffer[i] == '\n')
                    break;
                else
                    c = buffer[i];
            }
            if (i < totalBytes) {
                k = i + 1;
                text = new String(buffer, 1, k-3);
                for (i=k; i<totalBytes; i++)
                    buffer[i-k] = buffer[i];
                totalBytes -= k;
                return Long.parseLong(text);
            }
        }

        if (in == null)
            throw(new IOException("input stream is null"));

        timeStart = System.currentTimeMillis();
        timeLeft = (ms > 0) ? ms : timeout;

        k = totalBytes;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            if (sock != null) try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft -= (int) (System.currentTimeMillis() - timeStart);
                if (timeLeft > 0)
                    continue;
            }
            c = buffer[0];
            for (i=1; i<totalBytes; i++) {
                if (c == '\r' && buffer[i] == '\n')
                    break;
                else
                    c = buffer[i];
            }
            if (i >= totalBytes) {
                k = totalBytes;
            }
            else {
                k = i + 1;
                text = new String(buffer, 1, k-3);
                for (i=k; i<totalBytes; i++)
                    buffer[i-k] = buffer[i];
                totalBytes -= k;
                return Long.parseLong(text);
            }
            timeLeft -= (int) (System.currentTimeMillis() - timeStart);
        } while (bytesRead >= 0 && timeLeft > 0);

        return -1L;
    }

    private String getKey(InputStream in, int size, int ms) throws IOException {
        String key;
        long timeStart;
        int i, k, bytesRead, timeLeft;

        if (totalBytes >= size + 2) {
            key = new String(buffer, 0, size);
            for (i=size+2; i<totalBytes; i++)
                buffer[i-size-2] = buffer[i];
            totalBytes -= size + 2;
            return key;
        }

        if (in == null)
            throw(new IOException("input stream is null"));

        timeStart = System.currentTimeMillis();
        timeLeft = (ms > 0) ? ms : timeout;

        k = totalBytes;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            if (sock != null) try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft -= (int) (System.currentTimeMillis() - timeStart);
                if (timeLeft > 0)
                    continue;
            }
            if (totalBytes >= size + 2) {
                key = new String(buffer, 0, size);
                for (i=size+2; i<totalBytes; i++)
                    buffer[i-size-2] = buffer[i];
                totalBytes -= size + 2;
                size = 0;
                return key;
            }
            else {
                k = totalBytes;
            }
            timeLeft -= (int) (System.currentTimeMillis() - timeStart);
        } while (bytesRead >= 0 && timeLeft > 0);

        return null;
    }

    public static byte[] redisCommand(int id, String key, String data, int n) {
        StringBuffer strBuf;
        if (key == null || key.length() <= 0)
            return null;
        if (data == null)
            data = "";
        String str = String.valueOf(n); 
        strBuf = new StringBuffer();
        switch (id) {
          case REDIS_BLPOP:
            strBuf.append("*3\r\n$5\r\nBLPOP\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            strBuf.append("$" + str.length() + "\r\n" + str + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_ZADD:
            strBuf.append("*4\r\n$4\r\nZADD\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            strBuf.append("$" + str.length() + "\r\n" + str + "\r\n");
            strBuf.append("$" + data.length() + "\r\n" + data + "\r\n");
          default:
        }
        return null;
    }

    public static byte[] redisCommand(int id, String key, String data,
        double d) {
        StringBuffer strBuf;
        if (key == null || key.length() <= 0)
            return null;
        if (data == null)
            data = "";
        String str = String.valueOf(d); 
        strBuf = new StringBuffer();
        switch (id) {
          case REDIS_ZADD:
            strBuf.append("*4\r\n$4\r\nZADD\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            strBuf.append("$" + str.length() + "\r\n" + str + "\r\n");
            strBuf.append("$" + data.length() + "\r\n" + data + "\r\n");
          default:
        }
        return null;
    }

    public static byte[] redisCommand(int id, String key, String data) {
        StringBuffer strBuf;
        String str;
        if (key == null || key.length() <= 0)
            return null;
        if (data == null)
            data = "";
        strBuf = new StringBuffer();
        switch (id) {
          case REDIS_RPUSH:
            strBuf.append("*3\r\n$5\r\nRPUSH\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            strBuf.append("$" + data.length() + "\r\n" + data + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_PUBLISH:
            strBuf.append("*3\r\n$7\r\nPUBLISH\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            strBuf.append("$" + data.length() + "\r\n" + data + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_SUBSCRIBE:
            strBuf.append("*2\r\n$9\r\nSUBSCRIBE\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_UNSUBSCRIBE:
            strBuf.append("*2\r\n$11\r\nUNSUBSCRIBE\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_LPOP:
            strBuf.append("*2\r\n$4\r\nLPOP\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            return strBuf.toString().getBytes();
          case REDIS_ZADD:
            strBuf.append("*4\r\n$4\r\nZADD\r\n");
            strBuf.append("$" + key.length() + "\r\n" + key + "\r\n");
            str = String.valueOf(System.currentTimeMillis());
            strBuf.append("$" + str.length() + "\r\n" + str + "\r\n");
            strBuf.append("$" + data.length() + "\r\n" + data + "\r\n");
            return strBuf.toString().getBytes();
          default:
        }
        return null;
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
        catch (IOException e) {
            return e.toString();
        }
        return null;
    }

    protected void connect() throws IOException {
        int timeLeft;
        long timeStart = System.currentTimeMillis();
        try {
            sock = ClientSocket.connect(host, port, timeout);
        }
        catch (Exception e) {
            throw(new IOException(e.toString()));
        }
        timeLeft = timeout - (int) (System.currentTimeMillis() - timeStart);
        if (sock == null) {
            throw(new IOException((timeLeft <= 0) ? "connection is timed out" :
                "service is not avalalbe"));
        }
        try {
            sock.setSoTimeout(timeout);
        }
        catch (IOException e) {
            close(sock);
            sock = null;
            throw(e);
        }
        catch (Exception e) {
            close(sock);
            sock = null;
            throw(new IOException(e.toString()));
        }
        isConnected = true;
    }

    public void close() {
        subscriptions = 0;
        totalBytes = 0;
        if (sock != null) {
            close(sock);
            sock = null;
        }
        isConnected = false;
    }

    private void close(Object o) {
        if (o != null) try {
            if (o instanceof Socket)
                ((Socket) o).close();
            else if (o instanceof InputStream)
                ((InputStream) o).close();
            else if (o instanceof OutputStream)
                ((OutputStream) o).close();
        }
        catch (Exception e) {
        }
    }

    public static void main(String args[]) {
        Map<String, Object> props;
        int i, n, timeout = 60, action = REDIS_RPUSH;
        String str, key = null, uri = null, data = null;
        StringBuffer strBuf;
        BytesBuffer msgBuf;
        RedisConnector conn = null;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        props = new HashMap<String, Object>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                if (i == args.length-1)
                    data = args[i];
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
              case 'a':
                if ("rpush".equalsIgnoreCase(args[++i]))
                    action = REDIS_RPUSH;
                else if ("pub".equalsIgnoreCase(args[i]))
                    action = REDIS_PUBLISH;
                else if ("publish".equalsIgnoreCase(args[i]))
                    action = REDIS_PUBLISH;
                else if ("lpop".equalsIgnoreCase(args[i]))
                    action = REDIS_LPOP;
                else if ("blpop".equalsIgnoreCase(args[i]))
                    action = REDIS_BLPOP;
                else if ("sub".equalsIgnoreCase(args[i]))
                    action = REDIS_SUBSCRIBE;
                else if ("subscribe".equalsIgnoreCase(args[i]))
                    action = REDIS_SUBSCRIBE;
                else
                    action = REDIS_LPOP;
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
        strBuf = new StringBuffer();
        msgBuf = new BytesBuffer();
        n = 0;

        try {
            conn = new RedisConnector(props);
            switch (action) {
              case REDIS_RPUSH:
                i = (int) conn.rpush(key, data);
                break;
              case REDIS_PUBLISH:
                i = (int) conn.publish(key, data);
                break;
              case REDIS_LPOP:
                i = (int) conn.lpop(key, msgBuf);
                break;
              case REDIS_BLPOP:
                while (true) {
                    i = (int) conn.blpop(key, 1, msgBuf);
                    if (i == -1)
                        continue;
                    else if (i < 0)
                        break;
                    else {
                        n ++;
                        System.out.println("got a msg from " + key);
                        System.out.println(msgBuf.toString());
                        msgBuf.reset();
                        if (n > 10)
                            break;
                    }
                }
                break;
              case REDIS_SUBSCRIBE:
                i = conn.subscribe(key);
                if (i > 0) {
                    while (true) {
                        str = conn.nextMessage(1, msgBuf);
                        if (str != null) {
                            n ++;
                            System.out.println("got a msg from " + str);
                            System.out.println(msgBuf.toString());
                            msgBuf.reset();
                            if (n > 10)
                                break;
                        }
                    }
                    i = conn.unsubscribe(key);
                }
                break;
              default:
            }
            if (i < 0) {
                System.out.println("ErrorCode: " + i + " " + action);
            }
            else if (action == REDIS_LPOP)
                System.out.println(msgBuf.toString());
            else if (action == REDIS_SUBSCRIBE || action == REDIS_BLPOP)
                System.out.println("got " + n + " msgs from " + key);
            else
                System.out.println("Status: " + i);
            conn.close();
        }
        catch (Exception e) {
            if (conn != null)
                conn.close();
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("RedisConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("RedisConnector: a Redis client");
        System.out.println("Usage: java org.qbroker.net.RedisConnector -u uri -t timeout -n username -p password -k key -a action [data]");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of rpush, lpop, blpop, pub, sub (default: lpop)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -k: key");
    }
}
