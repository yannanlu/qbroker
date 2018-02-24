package org.qbroker.net;

/* HTTPConnector.java - an HTTP socket connector for content transfers */

import java.net.HttpURLConnection;
import java.io.Reader;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.qbroker.common.Utils;
import org.qbroker.common.Base64Encoder;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;
import org.qbroker.net.ClientSocket;

/**
 * HTTPConnector connects to a web server and provides the following
 * methods of for content transfers: doGet() and doPost().  Somehow, Sun's
 * implementation of HttpURLConnection is lousy.  It sometimes causes the
 * thread stuck.  Its disconnect() is not working either under MT env.
 * Therefore, we have to provide an alternative implementation.
 *<br/><br/>
 * Most of the HTTP 1.1 requirements has been impletemented, including the
 * persistent connection and the chunked response. But it is not fully tested
 * yet. So use it with care for HTTP 1.1 requests.
 *<br/><br/>
 * This is MT-Safe and supports and SSL.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class HTTPConnector implements Connector {
    protected String uri = null;
    private String username = null;
    private String password = null;
    private String host = null;
    private String referer = null;
    private String cookie = null;
    private int port;
    private int timeout = 60000;
    private Socket sock = null;
    private byte[] request;
    private int bufferSize = 8192;
    private String authStr = null;
    private boolean isPost = false;
    private boolean isNewVersion = false;
    private boolean isHTTPS = false;
    private boolean trustAll = false;

    public final static int ACTION_HEAD = 0;
    public final static int ACTION_GET = 1;
    public final static int ACTION_PUT = 2;
    public final static int ACTION_POST = 3;
    public final static int ACTION_DELETE = 4;
    public final static String LINE_SEPARATOR =
        System.getProperty("line.separator");

    /** Creates new HTTPConnector */
    public HTTPConnector(Map props) {
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
    }

    /**
     * It sends an HTTP request to the HTTP server with the urlStr and the extra
     * request headers, as well as the content stored in the BytesBuffer.
     * Upon success, it returns the status code of HTTP_OK with the response
     * headers loaded to the provided StringBuffer and the response content
     * written into the output stream. Otherwise, it returns an error code
     * defined in HttpURLConnection with the error message stored in the
     * response StringBuffer.
     */
    private int request(int action, String urlStr, Map extra, BytesBuffer buf,
        StringBuffer response, OutputStream out) throws IOException {
        String webHost = null, operation = null;
        byte[] webRequest = null;
        int i, webPort = 80;
        boolean overridenReferer = false;
        boolean overridenType = false;
        boolean overridenAuth = false;
        boolean overridenCookie = false;
        if (urlStr == null || response == null || out == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        switch (action) {
          case ACTION_POST:
            operation = "POST";
            break;
          case ACTION_PUT:
            operation = "PUT";
            break;
          case ACTION_GET:
            operation = "GET";
            break;
          case ACTION_HEAD:
            operation = "HEAD";
            break;
          case ACTION_DELETE:
            operation = "DELETE";
            break;
          default:
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }

        response.delete(0, response.length());
        try { // for arbitrary url
            URI u;
            String path;
            u = new URI(urlStr);

            webHost = u.getHost();
            if ((webPort = u.getPort()) <= 0)
                webPort = (isHTTPS) ? 443 : 80;

            if ((path = u.getPath()) == null || path.length() == 0)
                path = "/";
            else { // in case of query stuff, rebuild the path + query
                i = urlStr.indexOf("://" + webHost);
                if (i > 0)
                    path = urlStr.substring(urlStr.indexOf("/", i+3));
            }
            response.append(operation + " " + path + " HTTP/" +
                ((isNewVersion) ? "1.1" : "1.0") + "\r\n");
            response.append("Host: " + webHost + ":" + webPort + "\r\n");
            response.append("User-Agent: " + "QBroker/2.0\r\n");
            if (extra != null && extra.size() > 0) { // copy extra headers
                Iterator iter = extra.keySet().iterator();
                String key, str;
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    if ("ContentLength".equals(key))
                        continue;
                    if ("ContentType".equals(key) && action != ACTION_PUT &&
                        action != ACTION_POST)
                        continue;
                    str = (String) extra.get(key);
                    if (str == null)
                        str = "";
                    response.append(key + ": " + str + "\r\n");
                }
                overridenReferer = extra.containsKey("Referer");
                overridenCookie = extra.containsKey("Cookie");
                overridenAuth = extra.containsKey("Authorization");
                overridenType = extra.containsKey("Content-Type");
                if(!extra.containsKey("Accept") && !extra.containsKey("accept"))
                    response.append("Accept: " + "*/*\r\n");
            }
            else
                response.append("Accept: " + "*/*\r\n");
            if (referer != null && !overridenReferer)
                response.append("Referer: " + referer + "\r\n");
            if (cookie != null && !overridenCookie)
                response.append("Cookie: " + cookie + "\r\n");
            if (authStr != null && !overridenAuth)
                response.append("Authorization: " + authStr + "\r\n");
            if (action == ACTION_PUT || action == ACTION_POST ||
                action == ACTION_DELETE) {
                if (!overridenType)
         response.append("Content-Type: application/x-www-form-urlencoded\r\n");
                if (buf != null)
                    response.append("Content-Length: " + buf.getCount()+"\r\n");
                else
                    response.append("Content-Length: 0\r\n");
            }
            response.append("\r\n");
        }
        catch (URISyntaxException e) {
            response.append("bad url: " + urlStr + ": " +  e.toString());
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }
        catch (Exception e) {
            response.append("failed to prepare http request: " +  e.toString());
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }

        return sendRequest(response, buf, out);
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
        if (response == null || out == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        if (urlStr == null) // for default url
            urlStr = uri;

        return request(ACTION_GET, urlStr, extra, null, response, out);
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
        int i = request(ACTION_GET, urlStr, null, null, response, msgBuf);
        if (i == HttpURLConnection.HTTP_OK) {
            response.delete(0, response.length());
            response.append(msgBuf.toString());
        }
        return i;
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
        if (urlStr == null) // for default url
            urlStr = uri;
        return request(ACTION_PUT, urlStr, extra, buf, response, out);
    }

    public int doPut(String urlStr, BytesBuffer buf, StringBuffer response,
        OutputStream out) throws IOException {
        return doPut(urlStr, null, buf, response, out);
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
     *<br/><br/>
     * NB. urlStr is not supposed to contain any query string.
     */
    public int doPost(String urlStr, Map extra, BytesBuffer buf,
        StringBuffer response, OutputStream out) throws IOException {

        if (urlStr == null) // for default url
            urlStr = uri;

        return request(ACTION_POST, urlStr, extra, buf, response, out);
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
        if (urlStr == null) // for default url
            urlStr = uri;
        int i = request(ACTION_POST, urlStr, null, buf, response, msgBuf);
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
     * It sends a HEAD request to the HTTP server with the urlStr and loads the
     * response header to the provided StringBuffer. Upon success, it returns
     * the integer of HTTP_OK with the response header stored in the response
     * buffer. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doHead(String urlStr, Map extra, StringBuffer response)
        throws IOException{
        BytesBuffer msgBuf = new BytesBuffer();
        return request(ACTION_HEAD, urlStr, null, null, response, msgBuf);
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
        BytesBuffer msgBuf = new BytesBuffer();
        return request(ACTION_DELETE, urlStr, extra, null, response, msgBuf);
    }

    public int doDelete(String urlStr,StringBuffer response) throws IOException{
        return doDelete(urlStr, null, response);
    }

    /**
     * It sends an HTTP request to the HTTP server with the request headers
     * and the content stored in the bytes buffer. Upon success, it returns
     * the status code of HTTP_OK with the response headers loaded to the
     * provided header buffer and the response content written into the output
     * stream. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response header. The header buffer
     * is used as both input and output. The byte buffer is for input only. It
     * can be null to indicate the empty body.
     */
    private int sendRequest(StringBuffer header, BytesBuffer buf,
        OutputStream bos) throws IOException {
        byte[] buffer = new byte[bufferSize];
        String str, text;
        long timeEnd;
        int i, k, httpStatus, bytesRead, timeLeft, totalBytes, len = -1;
        int size = 0;
        boolean isChunked = false, justReconnected = false;
        InputStream in = null;
        OutputStream out = null;

        if (header == null || header.length() <= 0 || bos == null)
            return HttpURLConnection.HTTP_BAD_REQUEST;

        totalBytes = header.length();
        if (sock == null) {
            str = reconnect();
            if (str != null) {
                header.delete(0, totalBytes);
                header.append("connection is not avalalbe for " + uri +
                    ": " + str);
                return HttpURLConnection.HTTP_UNAVAILABLE;
            }
            justReconnected = true;
        }
        timeEnd = System.currentTimeMillis() + timeout;
        timeLeft = timeout;
        if (!justReconnected) try {
            sock.setSoTimeout(timeout);
        }
        catch (Exception e) {
            str = reconnect();
            if (str != null) {
                header.delete(0, totalBytes);
                header.append("reconnection failed for " + uri + ": " + str +
                    " caused by " + e.toString() );
                return HttpURLConnection.HTTP_UNAVAILABLE;
            }
        }
        try {
            in = sock.getInputStream();
            out = sock.getOutputStream();
        }
        catch (Exception e) {
            close(out);
            out = null;
            close(in);
            in = null;
            header.delete(0, totalBytes);
            header.append("got error from socket to " + uri +
                ": " + e.toString());
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }
        if (in == null || out == null) {
            close();
            header.delete(0, totalBytes);
            header.append("failed to get io streams from socket to " + uri);
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }

        try { // write request to server
            out.write(header.toString().getBytes());
            if (buf != null)
                buf.writeTo(out);
            out.flush();
        }
        catch (IOException e) {
            close(out);
            out = null;
            close(in);
            in = null;
            header.delete(0, totalBytes);
            header.append("failed to send request to " + uri +
                ": " + e.toString());
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }

        header.delete(0, totalBytes);
        text = null;
        httpStatus = -1;
        totalBytes = 0;
        k = 0;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            if (sock != null) try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
                close(out);
                out = null;
                close(in);
                in = null;
                header.append("failed to set timeout to " + uri +
                    ": " + ex.toString());
                return HttpURLConnection.HTTP_INTERNAL_ERROR;
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    totalBytes += bytesRead;
                }
                else { // EOF
                    timeLeft = (int) (timeEnd - System.currentTimeMillis());
                    break;
                }
            }
            catch (InterruptedIOException ex) {
                timeLeft = (int) (timeEnd - System.currentTimeMillis());
                if (timeLeft > 0)
                    continue;
            }
            catch (IOException e) {
                close(out);
                out = null;
                close(in);
                in = null;
                header.append("read error at " + totalBytes + "/" + len +
                    ": " + e.toString());
                return HttpURLConnection.HTTP_PRECON_FAILED;
            }
            catch (Exception e) {
                close(out);
                out = null;
                close(in);
                in = null;
                header.append("read error at " + totalBytes + "/" + len +
                    ": " + e.toString());
                return HttpURLConnection.HTTP_PRECON_FAILED;
            }
            if (httpStatus < 0) { // try to parse header
                header.append(new String(buffer, k, bytesRead));
                text = header.toString();
                if ((i = text.indexOf("\r\n\r\n")) > 0) {
                    int j;
                    k = i + 4;
                    text = text.substring(0, k);
                    header.delete(k, totalBytes);
                    totalBytes -= k;
                    bytesRead = totalBytes;
                    for (j=0; j<bytesRead; j++) // shift left
                        buffer[j] = buffer[j+k];
                    k = 0;
                    i = text.indexOf("\r\n");
                    if (i > 5) { // got status line
                        str = text.substring(0, i);
                        if ((!str.startsWith("HTTP")) ||
                            (i = str.indexOf(" ")) < 0 ||
                            (j = str.indexOf(" ", i + 1)) < 0) {
                            header.append("protocol error: wrong protocol");
                            return HttpURLConnection.HTTP_UNAVAILABLE;
                        }
                        try {
                            httpStatus = Integer.parseInt(str.substring(i+1,j));
                        }
                        catch (NumberFormatException e) {
                            header.append("protocol error: " + e.toString());
                            return HttpURLConnection.HTTP_UNAVAILABLE;
                        }
                    }
                    else {
                        header.append("protocol error: bad protocol");
                        return HttpURLConnection.HTTP_UNAVAILABLE;
                    }
                    // got the header
                    if (httpStatus == HttpURLConnection.HTTP_OK) {
                        if ((i = text.indexOf("ncoding: chunked")) > 0) {
                            isChunked = true;
                        }
                        else if ((i = text.indexOf("Content-Length: ")) > 0) {
                            i += 16;
                            j = text.indexOf("\r\n", i);
                            try {
                                len = Integer.parseInt(text.substring(i, j));
                            }
                            catch (NumberFormatException e) {
                                header.append("length error: " + e.toString());
                                return HttpURLConnection.HTTP_UNAVAILABLE;
                            }
                        }
                    }
                    else if ((i = text.indexOf("Content-Length: ")) > 0) {
                        i += 16;
                        j = text.indexOf("\r\n", i);
                        try {
                            len = Integer.parseInt(text.substring(i, j));
                        }
                        catch (NumberFormatException e) {
                        }
                    }
                }
            }
            if (httpStatus == 100) { // continue so reset
                header.delete(0, header.length());
                totalBytes = 0;
                k = 0;
                httpStatus = -1;
            }
            else if (httpStatus == HttpURLConnection.HTTP_OK) { // for content
                if (isChunked) {
                    int j;
                    int n = (bytesRead > 0) ? k + bytesRead : k;
                    if (len > 0) { // leftover for previous chunk
                        if (size < len) {
                            if (size + n >= len + 2) { // previous chunk is done
                                bos.write(buffer, 0, len - size);
                                k = len - size + 2;
                                n -= k;
                                totalBytes -= 2;
                                size = len;
                                for (j=0; j<n; j++) // shift left
                                    buffer[j] = buffer[j+k];
                                k = 0;
                                len = -1;
                            }
                            else if (size + n >= len) { // left over
                                bos.write(buffer, 0, len - size);
                                k = len - size;
                                n -= k;
                                size = len;
                                for (j=0; j<n; j++) // shift left
                                    buffer[j] = buffer[j+k];
                                k = n;
                            }
                            else {
                                bos.write(buffer, 0, n);
                                size += n;
                                n = 0;
                                k = 0;
                            }
                            if (len > 0) { // previous chunk not done yet
                                if (bytesRead < 0) // EOF
                                    break;
                                timeLeft = 
                                    (int)(timeEnd - System.currentTimeMillis());
                                if (timeLeft > 0)
                                    continue;
                                else
                                    break;
                            }
                        }
                        else if (n >= 2) { // leftover
                            totalBytes -= 2;
                            k = 2;
                            n -= k;
                            for (j=0; j<n; j++) // shift left
                                buffer[j] = buffer[j+k];
                            k = 0;
                            len = -1;
                        }
                        else { // waiting for leftover
                            if (bytesRead < 0) // EOF
                                break;
                            timeLeft = 
                                (int) (timeEnd - System.currentTimeMillis());
                            if (timeLeft > 0)
                                continue;
                            else
                                break;
                        }
                    }
                    else if (len == 0) { // end of previous chunk
                        str = new String(buffer, 0, n);
                        if ((i = str.indexOf("\r\n\r\n")) > 0) { //end of footer
                            totalBytes -= n;
                            k = 0;
                            break;
                        }
                        else if (str.indexOf("\r\n") == 0) { // empty footer
                            totalBytes -= n;
                            k = 0;
                            break;
                        }
                        else { // footer is not done yet
                            if (bytesRead < 0) { // EOF
                                totalBytes -= n;
                                break;
                            }
                            timeLeft = 
                                (int) (timeEnd - System.currentTimeMillis());
                            if (timeLeft > 0)
                                continue;
                            else {
                                totalBytes -= n;
                                break;
                            }
                        }
                    }

                    i = -1;
                    len = -1;
                    while (n > k) {
                        for (j=0; j<n; j++) {
                            if (buffer[j] == (byte) 13) {
                                i = j;
                                break;
                            }
                        }
                        if (i < 0) { // chunk is not done yet
                            k = n;
                            break;
                        }
                        else if (i+1 >= n || buffer[i+1] != (byte) 10) {
                            k = n;
                            break;
                        }
                        else try {
                            str = new String(buffer, 0, i);
                            len = Integer.parseInt(str, 16);
                        }
                        catch (NumberFormatException e) {
                            header.append("length error: " + e.toString());
                            return HttpURLConnection.HTTP_UNAVAILABLE;
                        }
                        if (len > 0) { // got the length of a chunk
                            if (n >= i + len + 4) { // got a chuck
                                bos.write(buffer, i+2, len);
                                k = i + len + 4;
                                n -= k;
                                totalBytes -= i + 4;
                                for (j=0; j<n; j++) // shift left
                                    buffer[j] = buffer[j+k];
                                k = 0; 
                                i = -1;
                                len = -1;
                            }
                            else if (n >= i + len + 2) { // leftover
                                bos.write(buffer, i+2, len);
                                k = i + len + 2;
                                n -= k;
                                totalBytes -= i + 2;
                                size = len;
                                for (j=0; j<n; j++) // shift left
                                    buffer[j] = buffer[j+k];
                                k = n; 
                                i = -1;
                                break;
                            }
                            else { // not a complete chunk yet
                                n -= i + 2;
                                bos.write(buffer, i+2, n);
                                totalBytes -= i + 2;
                                size = n;
                                k = 0;
                                i = -1;
                                break;
                            }
                        }
                        else if (len == 0) { // end chunk
                            k = i + 2;
                            totalBytes -= k;
                            n -= k;
                            for (j=0; j<n; j++) // shift left
                                buffer[j] = buffer[j+k];
                            k = 0;
                            str = new String(buffer, 0, n);
                            if ((i = str.indexOf("\r\n\r\n")) > 0) { // footer
                                totalBytes -= n;
                                break;
                            }
                            else if (str.indexOf("\r\n") == 0) { // empty footer
                                totalBytes -= n;
                                break;
                            }
                            else { // footer is not done yet
                                if (bytesRead < 0) { // EOF
                                    totalBytes -= n;
                                    break;
                                }
                                timeLeft = 
                                    (int)(timeEnd - System.currentTimeMillis());
                                if (timeLeft > 0)
                                    continue;
                                else {
                                    totalBytes -= n;
                                    break;
                                }
                            }
                        }
                        else { // bad chunk
                            header.append("length error: " + len);
                            return HttpURLConnection.HTTP_UNAVAILABLE;
                        }
                    }
                    if (len == 0)
                        break;
                }
                else if (len > 0) { // with content length
                    if (bytesRead > 0)
                        bos.write(buffer, k, bytesRead);
                    k = 0;
                    if (totalBytes >= len) // got all response
                         break;
                }
                else if (len == 0)
                    break;
                else { // read everything until EOF or timeout
                    if (bytesRead > 0)
                        bos.write(buffer, k, bytesRead);
                    k = 0;
                }
            }
            else if (httpStatus > 0) { // error
                if (len > 0) { // with content length
                    if (bytesRead > 0)
                        bos.write(buffer, k, bytesRead);
                    k = 0;
                    if (totalBytes >= len) // got all response
                         break;
                }
                else
                    break;
            }
            timeLeft = (int) (timeEnd - System.currentTimeMillis());
        } while (bytesRead >= 0 && timeLeft > 0);

        if (bytesRead < 0 || // EOF
            (text != null && text.indexOf("Connection: close\r\n") > 0)) {
            close(out);
            out = null;
            close(in);
            in = null;
            close();
        }

        if (httpStatus < 0) {
            if (timeLeft <= 0) {
                header.append("read timed out: " + timeLeft + " " + totalBytes +
                    "/" + bytesRead);
                return HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
            }
            else {
                header.append("read failed: " + timeLeft + " " + totalBytes +
                    "/" + bytesRead);
                return HttpURLConnection.HTTP_UNAVAILABLE;
            }
        }
        else if (httpStatus == HttpURLConnection.HTTP_MOVED_TEMP ||
            httpStatus == HttpURLConnection.HTTP_MOVED_PERM) { // redirect
            k = text.indexOf(httpStatus + " ");
            i = text.indexOf("\r\n");
            header.append(text.substring(k+4, i));
        }
        else if (httpStatus != HttpURLConnection.HTTP_OK) {
            k = text.indexOf(httpStatus + " ");
            i = text.indexOf("\r\n");
            header.append(text.substring(k+4, i));
        }

        return httpStatus;
    }

    public boolean isNewVersion() {
        return isNewVersion;
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
        int timeLeft, type;
        long timeEnd = System.currentTimeMillis() + timeout;
        if (isHTTPS)
            type = (trustAll) ? ClientSocket.TYPE_SSL : ClientSocket.TYPE_HTTPS;
        else
            type = ClientSocket.TYPE_TCP;
        try {
            sock = ClientSocket.connect(host, port, timeout, type);
        }
        catch (Exception e) {
            throw(new IOException(TraceStackThread.traceStack(e)));
        }
        timeLeft = (int) (timeEnd - System.currentTimeMillis());
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
    }

    public void close() {
        if (sock != null) {
            close(sock);
            sock = null;
        }
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

    /**
     * It returns the value of the header for the key or null if there is
     * no such key in the headers.
     */
    public static String getHeader(String key, String headers) {
        int i, j, k;
        if (headers == null || headers.length() <= 0 || key == null ||
            key.length() <= 0)
            return null;
        if ((i = headers.indexOf(key + ": ")) < 0)
            return null;
        else if (i == 0) {
           k = key.length() + 2;
           if ((j = headers.indexOf("\r\n", i + k)) >= i + k)
               return headers.substring(i+k, j);
           else
               return null;
        }
        else if ((i = headers.indexOf("\n" + key + ": ")) > 0) {
           i ++;
           k = key.length() + 2;
           if ((j = headers.indexOf("\r\n", i + k)) >= i + k)
               return headers.substring(i+k, j);
           else
               return null;
        }
        else
            return null;
    }

    /**
     * It returns dirname of the filename or null if it is a relative path.
     */
    public static String getParent(String filename) {
        char fs;
        int i, k;
        if (filename == null || (k = filename.length()) <= 0) // bad argument
            return null;
        fs = filename.charAt(0);
        if (fs != '/' && fs != '\\') // no file separator found
            return null;
        if ((i = filename.lastIndexOf(fs, k-1)) >= 0)
            return filename.substring(0, i);
        else
            return null;
    }

    public static void main(String args[]) {
        Map<String, String> props;
        Map<String, String> headers;
        int i, timeout = 60, action = ACTION_GET;
        String str, query = null, uri = null, type = null, filename = null;
        StringBuffer strBuf;
        BytesBuffer msgBuf;
        HTTPConnector conn = null;
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
            conn = new HTTPConnector(props);
            switch (action) {
              case ACTION_GET:
                if (headers.size() > 0)
                    i = conn.doGet(uri, headers, strBuf, msgBuf);
                else
                    i = conn.doGet(uri, strBuf, msgBuf);
                break;
              case ACTION_PUT:
                if (type != null || headers.size() > 0) {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = conn.doPut(uri, headers, payload, strBuf, msgBuf);
                }
                else {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    i = conn.doPut(uri, payload, strBuf, msgBuf);
                }
                break;
              case ACTION_DELETE:
                if (type != null || headers.size() > 0) {
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = conn.doDelete(uri, headers, strBuf);
                }
                else
                    i = conn.doDelete(uri, strBuf);
                break;
              case ACTION_HEAD:
                i = conn.doHead(uri, strBuf);
                break;
              case ACTION_POST:
                if (type != null || headers.size() > 0) {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    if (type != null)
                        headers.put("Content-Type", type);
                    i = conn.doPost(uri, headers, payload, strBuf, msgBuf);
                }
                else {
                    BytesBuffer payload = new BytesBuffer();
                    payload.write(query.getBytes(), 0, query.length());
                    i = conn.doPost(uri, payload, strBuf, msgBuf);
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
                        getHeader(query, strBuf.toString()));
            }
            else
                System.out.print(msgBuf.toString());
            conn.close();
        }
        catch (Exception e) {
            if (conn != null)
                conn.close();
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("HTTPConnector Version 2.0 (written by Yannan Lu)");
        System.out.println("HTTPConnector: an HTTP client to get or post web pages");
        System.out.println("Usage: java org.qbroker.net.HTTPConnector -u uri -t timeout -a action -q query_string");
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
        System.out.println("  -h: request header");
    }
}
