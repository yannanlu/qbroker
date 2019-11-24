package org.qbroker.net;

/* HTTPClient.java - a wrapper for HttpClient */

import java.net.HttpURLConnection;
import java.io.Reader;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.qbroker.common.Base64Encoder;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.Utils;

/**
 * HTTPClient is a wrapper of HttpClient to implement HTTPConnector.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class HTTPClient implements org.qbroker.common.HTTPConnector {
    protected CloseableHttpClient client = null;
    protected String uri = null;
    private String username = null;
    private String password = null;
    private String host = null;
    private String referer = null;
    private String cookie = null;
    private String proxyHost = null;
    private int port, proxyPort = 1080;
    private int timeout = 60000;
    private byte[] request;
    private int bufferSize = 8192;
    private String authStr = null;
    private boolean isPost = false;
    private boolean isNewVersion = false;
    private boolean isHTTPS = false;
    private boolean trustAll = false;

    /** Creates new HTTPClient */
    public HTTPClient(Map props) {
        Object o;
        URI u;
        String path;
        StringBuffer strBuf = new StringBuffer();
        int i, n;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ("http".equals(u.getScheme()))
            isHTTPS = false;
        else if ("https".equals(u.getScheme())) {
            isHTTPS = true;
            if ((o = props.get("TrustAllCertificates")) != null &&
                "true".equalsIgnoreCase((String) o))
                trustAll = true;
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        host = u.getHost();
        if ((port = u.getPort()) <= 0)
            port = (isHTTPS) ? 443 : 80;

        if ((path = u.getPath()) == null || path.length() == 0)
            path = "/";
        else { // in case of query stuff, rebuild the path + query
            n = uri.indexOf("://" + host);
            if (n > 0)
                path = uri.substring(uri.indexOf("/", n+3));
        }

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null) {
                password = (String) o;
            }
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }

            if (password != null) {
                authStr = username + ":" + password;
                authStr = new String(Base64Encoder.encode(authStr.getBytes()));
                authStr = "Basic " + authStr;
            }
        }

        if ((o = props.get("ProxyHost")) != null) {
            proxyHost = (String) o;
            if ((o = props.get("ProxyPort")) != null)
                proxyPort = Integer.parseInt((String) o);
        }

        if ((o = props.get("HTTPVersion")) != null && "1.1".equals((String) o))
            isNewVersion = true;

        if ((o = props.get("IsPost")) != null &&
            "true".equalsIgnoreCase((String) o)) {
            isPost = true;
            strBuf.append("POST " + path + " HTTP/" +
                ((isNewVersion) ? "1.1" : "1.0") + "\r\n");
        }
        else
            strBuf.append("GET " + path + " HTTP/" +
                ((isNewVersion) ? "1.1" : "1.0") + "\r\n");

        strBuf.append("Host: " + host + "\r\n");
        strBuf.append("User-Agent: " + "QBroker/2.0\r\n");
        strBuf.append("Accept: " + "*/*\r\n");

        if ((o = props.get("Referer")) != null) {
            referer = (String) o;
            strBuf.append("Referer: " + referer + "\r\n");
        }

        if ((o = props.get("Cookie")) != null) {
            cookie = (String) o;
            strBuf.append("Cookie: " + cookie + "\r\n");
        }

        if (authStr != null)
            strBuf.append("Authorization: " + authStr + "\r\n");

        if (isPost) {
           strBuf.append("Content-Type: application/x-www-form-urlencoded\r\n");
        }
        else {
            strBuf.append("\r\n");
        }
        request = new byte[strBuf.length()];
        request = strBuf.toString().getBytes();

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
        }

        reconnect();
    }

    /**
     * It sends a GET request to the HTTP server with the urlStr and the extra
     * request headers stored in the Map. Upon success, it returns
     * the status code of HTTP_OK with the response headers stored in the
     * response buffer and the response content written into the output stream.
     * Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doGet(String urlStr, Map extra, StringBuffer response,
        OutputStream out) throws IOException {
        int status, n = -1;
        if (response == null || out == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        HttpGet httpget = new HttpGet(urlStr);
        CloseableHttpResponse resp = client.execute(httpget);
        status = resp.getStatusLine().getStatusCode();

        if (status < 200 || status >= 300) {
            resp.close();
            n = status;
        }
        else try {
            HttpEntity entity = resp.getEntity();
            if (entity != null)
                n = processEntity(entity, response, out, timeout);
        }
        finally {
            resp.close();
        }
        if (n >= 0)
            return status;
        else
            return -n;
    }

    public int doGet(String urlStr, StringBuffer response, OutputStream out)
        throws IOException {
        return doGet(urlStr, null, response, out);
    }

    /**
     * It sends a GET request to the HTTP server with the urlStr. Upon success,
     * it returns the status code of HTTP_OK with the response content stored
     * in the response buffer. Otherwise, it returns an error code defined in
     * HttpURLConnection with the error message stored in the response buffer.
     */
    public int doGet(String urlStr, StringBuffer response) throws IOException {
        BytesBuffer msgBuf = new BytesBuffer();
        int i = doGet(urlStr, null, response, msgBuf);
        if (i == HttpURLConnection.HTTP_OK) {
            response.delete(0, response.length());
            response.append(msgBuf.toString());
        }
        return i;
    }

    /**
     * It sends a POST request to the HTTP server with the urlStr and the extra
     * request headers, as well as the content stored in the BytesBuffer.
     * Upon success, it returns the status code of HTTP_OK with the response
     * headers loaded to the provided StringBuffer and the response content
     * written into the output stream. Otherwise, it returns an error code
     * defined in HttpURLConnection with the error message stored in the
     * response StringBuffer. The response buffer is used for both input and
     * output. The byte buffer is for input only.
     *<br><br>
     * NB. urlStr is not supposed to contain any query string.
     */
    public int doPost(String urlStr, Map extra, BytesBuffer buf,
        StringBuffer response, OutputStream out) throws IOException {
        int status, n = -1;
        if (buf == null || response == null || out == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        HttpPost httppost = new HttpPost(urlStr);
        ByteArrayEntity reqEntity = new ByteArrayEntity(buf.toByteArray(),
            ContentType.APPLICATION_FORM_URLENCODED);
        httppost.setEntity(reqEntity);
        CloseableHttpResponse resp = client.execute(httppost);
        status = resp.getStatusLine().getStatusCode();

        if (status < 200 || status >= 300) {
            resp.close();
            n = status;
        }
        else try {
            HttpEntity entity = resp.getEntity();
            if (entity != null)
                n = processEntity(entity, response, out, timeout);
        }
        finally {
            resp.close();
        }
        if (n >= 0)
            return HttpURLConnection.HTTP_OK;
        else
            return -n;
    }

    public int doPost(String urlStr, BytesBuffer buf, StringBuffer response,
        OutputStream out) throws IOException {
        return doPost(urlStr, null, buf, response, out);
    }

    /**
     * It sends a POST request to the HTTP server with the urlStr and the
     * request content stored in the byte buffer. Upon success, it returns
     * the status code of HTTP_OK with the response content stored in the
     * response buffer. Otherwise, it returns an error code defined in
     * HttpURLConnection with the error message stored in the response buffer.
     */
    public int doPost(String urlStr, BytesBuffer buf, StringBuffer response)
        throws IOException {
        BytesBuffer msgBuf = new BytesBuffer();
        int i = doPost(urlStr, null, buf, response, msgBuf);
        if (i == HttpURLConnection.HTTP_OK) {
            response.delete(0, response.length());
            response.append(msgBuf.toString());
        }
        return i;
    }

    public int doPost(String urlStr, String line, StringBuffer response)
        throws IOException {
        byte[] b = line.getBytes();
        BytesBuffer buf = new BytesBuffer();
        buf.write(b, 0, b.length);
        return doPost(urlStr, buf, response);
    }

    /**
     * It sends a PUT request to the HTTP server with the urlStr and the extra
     * request headers, as well as the content stored in the BytesBuffer.
     * Upon success, it returns the status code of HTTP_OK with the response
     * headers loaded to the provided StringBuffer and the response content
     * written into the output stream. Otherwise, it returns an error code
     * defined in HttpURLConnection with the error message stored in the
     * response StringBuffer. The response buffer is used for both input and
     * output. The byte buffer is for input only.
     */
    public int doPut(String urlStr, Map extra, BytesBuffer buf,
        StringBuffer response, OutputStream out) throws IOException {
        int status, n = -1;
        if (buf == null || response == null || out == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        HttpPut httpput = new HttpPut(urlStr);
        ByteArrayEntity reqEntity = new ByteArrayEntity(buf.toByteArray(),
            ContentType.APPLICATION_FORM_URLENCODED);
        httpput.setEntity(reqEntity);
        CloseableHttpResponse resp = client.execute(httpput);
        status = resp.getStatusLine().getStatusCode();

        if (status < 200 || status >= 300) {
            resp.close();
            n = status;
        }
        else try {
            HttpEntity entity = resp.getEntity();
            if (entity != null)
                n = processEntity(entity, response, out, timeout);
        }
        finally {
            resp.close();
        }
        if (n >= 0)
            return HttpURLConnection.HTTP_OK;
        else
            return -n;
    }

    public int doPut(String urlStr, BytesBuffer buf, StringBuffer response,
        OutputStream out) throws IOException {
        return doPut(urlStr, null, buf, response, out);
    }

    /**
     * It sends a HEAD request to the HTTP server with the urlStr and loads the
     * response header to the provided StringBuffer. Upon success, it returns
     * the integer of HTTP_OK with the response header stored in the response
     * buffer. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doHead(String urlStr, Map extra, StringBuffer response)
        throws IOException{
        int status = HttpURLConnection.HTTP_UNAVAILABLE;
        if (response == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        HttpHead httphead = new HttpHead(urlStr);
        CloseableHttpResponse resp = client.execute(httphead);
        status = resp.getStatusLine().getStatusCode();
        resp.close();

        return status;
    }

    public int doHead(String urlStr, StringBuffer response) throws IOException{
        return doHead(urlStr, null, response);
    }

    /**
     * It sends a DELETE request to the HTTP server with urlStr and loads the
     * response header to the provided StringBuffer. Upon success, it returns
     * the integer of HTTP_OK with the response header stored in the response
     * buffer. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doDelete(String urlStr, Map extra, StringBuffer response)
        throws IOException{
        int status = HttpURLConnection.HTTP_UNAVAILABLE;
        if (response == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        HttpDelete httpdelete = new HttpDelete(urlStr);
        CloseableHttpResponse resp = client.execute(httpdelete);
        status = resp.getStatusLine().getStatusCode();
        resp.close();

        return status;
    }

    public int doDelete(String urlStr,StringBuffer response) throws IOException{
        return doDelete(urlStr, null, response);
    }

    private int processEntity(HttpEntity entity, StringBuffer extra,
        OutputStream bos, int timeout) throws IOException {
        int k, bytesRead, totalBytes, size, timeLeft;
        long timeEnd, len;
        InputStream in = null;
        byte[] buffer;
        if (entity == null || extra == null || bos == null)
            return -HttpURLConnection.HTTP_BAD_REQUEST;

        totalBytes = extra.length();
        extra.delete(0, totalBytes);
        try {
            in = entity.getContent();
        }
        catch (Exception e) {
            extra.append("failed to get io streams from entity: "+e.toString());
            return -HttpURLConnection.HTTP_UNAVAILABLE;
        }
        buffer = new byte[bufferSize];

        len = entity.getContentLength();
        timeEnd = System.currentTimeMillis() + timeout;
        timeLeft = timeout;
        totalBytes = 0;
        k = 0;
        if (len > 0 || len < 0) do {
            bytesRead = 0;
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
                else { // EOF
                    break;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft = (int) (timeEnd - System.currentTimeMillis());
                if (timeLeft > 0)
                    continue;
            }
            catch (IOException e) {
                try {
                    in.close();
                }
                catch (Exception ex) {
                }
                extra.append("read error at " + totalBytes + "/" + len +
                   ": " + e.toString());
                return -HttpURLConnection.HTTP_PRECON_FAILED;
            }
            catch (Exception e) {
                try {
                    in.close();
                }
                catch (Exception ex) {
                }
                extra.append("read error at " + totalBytes + "/" + len +
                    ": " + e.toString());
                return -HttpURLConnection.HTTP_PRECON_FAILED;
            }
            if (len > 0) { // with content length
                if (bytesRead > 0)
                    bos.write(buffer, k, bytesRead);
                k = 0;
                if (totalBytes >= len) // got all response
                    break;
            }
            else { // read everything until EOF or timeout
                if (bytesRead > 0)
                    bos.write(buffer, k, bytesRead);
                k = 0;
            }
            timeLeft = (int) (timeEnd - System.currentTimeMillis());
        } while (bytesRead >= 0 && timeLeft > 0);
        try {
            in.close();
        }
        catch (Exception ex) {
        }

        return totalBytes;
    }

    public void close() {
        if (client != null) try {
            client.close();
            client = null;
        }
        catch (Exception e) {
            client = null;
        }
    }

    public boolean isPost() {
        return isPost;
    }

    public boolean isHTTPS() {
        return isHTTPS;
    }

    public String getURI() {
        return uri;
    }

    /** returns true since it is connectionless */
    public boolean isConnected() {
        return true;
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

    private void connect() throws IOException {
       RequestConfig cfg;
       if (proxyHost != null && proxyPort > 0) {
           HttpHost proxy = new HttpHost(proxyHost, proxyPort, "http");
           cfg = RequestConfig.custom().setProxy(proxy)
               .setConnectTimeout(timeout)
               .setConnectionRequestTimeout(timeout)
               .setSocketTimeout(timeout).build();
       }
       else {
           cfg = RequestConfig.custom()
               .setConnectTimeout(timeout)
               .setConnectionRequestTimeout(timeout)
               .setSocketTimeout(timeout).build();
       }
       if (!isHTTPS || !trustAll)
           client = HttpClientBuilder.create()
               .setDefaultRequestConfig(cfg).build();
       else try {
           SSLContext sc = SSLContext.getInstance("SSL");
           sc.init(null, new TrustManager[]{new TrustAllManager()}, null);
           client = HttpClientBuilder.create().setSslcontext(sc)
               .setDefaultRequestConfig(cfg).build();
       }
       catch (GeneralSecurityException e) {
           throw(new IOException("failed on SSL initiation: " + e.toString()));
       }
    }

    public static void main(String args[]) {
        Map<String, String> props;
        Map<String, String> headers;
        int i, timeout = 60, action = ACTION_GET;
        String str, query = null, uri = null, type = null, filename = null;
        StringBuffer strBuf;
        BytesBuffer msgBuf;
        HTTPClient client = null;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        props = new HashMap<String, String>();
        headers = new HashMap<String, String>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'T':
                  props.put("TrustAllCertificates", "true");
                break;
              case 'u':
                if (i+1 < args.length)
                    uri = args[++i];
                break;
              case 'a':
                if ("get".equalsIgnoreCase(args[++i]))
                    action = ACTION_GET;
                else if ("put".equalsIgnoreCase(args[i]))
                    action = ACTION_PUT;
                else if ("post".equalsIgnoreCase(args[i]))
                    action = ACTION_POST;
                else if ("delete".equalsIgnoreCase(args[i]))
                    action = ACTION_DELETE;
                else if ("head".equalsIgnoreCase(args[i]))
                    action = ACTION_HEAD;
                else
                    action = ACTION_GET;
                break;
              case 'n':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'p':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              case 'v':
                if (i+1 < args.length)
                    props.put("HTTPVersion", args[++i]);
                break;
              case 'x':
                if (i+1 < args.length)
                    props.put("ProxyHost", args[++i]);
                break;
              case 'P':
                if (i+1 < args.length)
                    props.put("ProxyPort", args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'm':
                if (i+1 < args.length)
                    type = args[++i]; 
                break;
              case 'q':
                if (i+1 < args.length)
                    query = args[++i]; 
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i]; 
                break;
              case 'h':
                if (i+1 < args.length) {
                    int k;
                    str = args[++i]; 
                    if ((k = str.indexOf(":")) > 1)
                        headers.put(str.substring(0, k),
                            str.substring(k+1).trim());
                }
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

        try {
            if (query == null && filename != null) {
                char[] buffer = new char[4096];
                Reader in = new FileReader(filename);
                while ((i = in.read(buffer, 0, 4096)) >= 0) {
                    if (i > 0)
                        strBuf.append(new String(buffer, 0, i));
                }
                in.close();
                i = strBuf.length();
                if (i > 0 && strBuf.charAt(i-1) == '\n') // trim
                    strBuf.deleteCharAt(i-1);
                query = strBuf.toString();
                strBuf = new StringBuffer();
            }
            client = new HTTPClient(props);
            switch (action) {
              case ACTION_GET:
                if (headers.size() > 0)
                    i = client.doGet(uri, headers, strBuf, msgBuf);
                else
                    i = client.doGet(uri, strBuf, msgBuf);
                break;
              case ACTION_PUT:
                if (type != null || headers.size() > 0) {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = client.doPut(uri, headers, payload, strBuf, msgBuf);
                }
                else {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    i = client.doPut(uri, payload, strBuf, msgBuf);
                }
                break;
              case ACTION_DELETE:
                if (type != null || headers.size() > 0) {
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = client.doDelete(uri, headers, strBuf);
                }
                else
                    i = client.doDelete(uri, strBuf);
                break;
              case ACTION_HEAD:
                i = client.doHead(uri, strBuf);
                break;
              case ACTION_POST:
                if (type != null || headers.size() > 0) {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = client.doPost(uri, headers, payload, strBuf, msgBuf);
                }
                else {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    i = client.doPost(uri, payload, strBuf, msgBuf);
                }
                break;
              default:
            }
            if (i != 200) {
                System.out.println("ErrorCode: " + i);
                System.out.println(strBuf.toString());
            }
            else if (action == ACTION_DELETE || action == ACTION_HEAD)
                System.out.print(strBuf.toString());
            else if (action == ACTION_GET && query != null) { // with headers
                System.out.print(msgBuf.toString());
                if ("*".equals(query)) // all headers
                    System.err.println("\n" + strBuf.toString());
                else // a specific header
                    System.err.println("\n" + query + ": " +
                        Utils.getHttpHeader(query, strBuf.toString()));
            }
            else
                System.out.print(msgBuf.toString());
            client.close();
        }
        catch (Exception e) {
            if (client != null)
                client.close();
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("HTTPClient Version 1.0 (written by Yannan Lu)");
        System.out.println("HTTPClient: an HTTP client to get or post web pages");
        System.out.println("Usage: java org.qbroker.net.HTTPClient -u uri -t timeout -a action -q query_string");
        System.out.println("  -?: print this message");
        System.out.println("  -T: trust all certificates");
        System.out.println("  -u: uri");
        System.out.println("  -a: action of get, put, post, delete, head (default: get)");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -v: version (default: 1.0)");
        System.out.println("  -m: mimetype for post or put");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -q: query string or content for post or put; or header name for get");
        System.out.println("  -f: full path to the file containing data for post or put");
        System.out.println("  -x: proxy host");
        System.out.println("  -P: proxy port");
        System.out.println("  -h: request header");
    }
}
