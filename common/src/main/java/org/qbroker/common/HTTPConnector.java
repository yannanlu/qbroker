package org.qbroker.common;

/* HTTPConnector.java - an Interface for operations on an HTTP connection */

import java.util.Map;
import java.io.OutputStream;
import java.io.IOException;

public interface HTTPConnector {
    /**
     * It sends a GET request to the HTTP server with the urlStr and the extra
     * request headers stored in the Map. Upon success, it returns
     * the status code of HTTP_OK with the response headers stored in the
     * response buffer and the response content written into the output stream.
     * Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doGet(String urlStr, Map extra, StringBuffer response,
        OutputStream out) throws IOException;

    public int doGet(String urlStr, StringBuffer response, OutputStream out)
        throws IOException;

    /**
     * It sends a GET request to the HTTP server with the urlStr. Upon success,
     * it returns the status code of HTTP_OK with the response content stored
     * in the response buffer. Otherwise, it returns an error code defined in
     * HttpURLConnection with the error message stored in the response buffer.
     */
    public int doGet(String urlStr, StringBuffer response) throws IOException;

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
        StringBuffer response, OutputStream out) throws IOException;

    public int doPost(String urlStr, BytesBuffer buf, StringBuffer response,
        OutputStream out) throws IOException;

    /**
     * It sends a POST request to the HTTP server with the urlStr and the
     * request content stored in the byte buffer. Upon success, it returns
     * the status code of HTTP_OK with the response content stored in the
     * response buffer. Otherwise, it returns an error code defined in
     * HttpURLConnection with the error message stored in the response buffer.
     */
    public int doPost(String urlStr, BytesBuffer buf, StringBuffer response)
        throws IOException;

    public int doPost(String urlStr, String line, StringBuffer response)
        throws IOException;

    public int doPost(String urlStr, Map extra, BytesBuffer buf,
        StringBuffer response) throws IOException;

    public int doPost(String urlStr, Map extra, String line,
        StringBuffer response) throws IOException;

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
        StringBuffer response, OutputStream out) throws IOException;

    public int doPut(String urlStr, BytesBuffer buf, StringBuffer response,
        OutputStream out) throws IOException;

    /**
     * It sends a HEAD request to the HTTP server with the urlStr and loads the
     * response header to the provided StringBuffer. Upon success, it returns
     * the integer of HTTP_OK with the response header stored in the response
     * buffer. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doHead(String urlStr, Map extra, StringBuffer response)
        throws IOException;

    public int doHead(String urlStr, StringBuffer response) throws IOException;

    /**
     * It sends a DELETE request to the HTTP server with urlStr and loads the
     * response header to the provided StringBuffer. Upon success, it returns
     * the integer of HTTP_OK with the response header stored in the response
     * buffer. Otherwise, it returns an error code defined in HttpURLConnection
     * with the error message stored in the response buffer.
     */
    public int doDelete(String urlStr, Map extra, StringBuffer response)
        throws IOException;

    public int doDelete(String urlStr,StringBuffer response) throws IOException;

    public boolean isPost();

    public boolean isHTTPS();

    /** reconnects and returns null upon success or error msg otherwise */
    public String reconnect();
    public boolean isConnected();
    public String getURI();
    public void close();

    public final static int ACTION_HEAD = 0;
    public final static int ACTION_GET = 1;
    public final static int ACTION_PUT = 2;
    public final static int ACTION_POST = 3;
    public final static int ACTION_DELETE = 4;
}
