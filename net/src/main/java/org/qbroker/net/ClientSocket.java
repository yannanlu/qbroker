package org.qbroker.net;

/* ClientSocket.java - a client TCP socket with timeout support */

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLHandshakeException;
import org.qbroker.common.Utils;

/**
 * ClientSocket opens a TCP socket with timeout protections.  The only
 * static method, connect(), returns the socket object if the open
 * is successful before it is timed out.  If type is set to TYPE_HTTPS,
 * the socket will be on SSL that verifies the certificate. If type is set
 * to TYPE_SSL, the socket will be on SSL that trusts all certificates.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ClientSocket {
    public static final int TYPE_TCP = 0;
    public static final int TYPE_HTTP = 1;
    public static final int TYPE_HTTPS = 2;
    public static final int TYPE_SSL = 3;

    public static Socket connect(String host, int port, int timeout, int type) {
        return connect(host, port, timeout, type, null, 0);
    }

    public static Socket connect(String host, int port, int timeout) {
        return connect(host, port, timeout, TYPE_TCP, null, 0);
    }

    public static Socket connect(String host, int port, int timeout, int type,
        String proxyHost, int proxyPort) {
        Socket socket;
        InetAddress address;
        SocketAddress saddr = null;

        if (timeout < 0)
            throw(new IllegalArgumentException(host + ":" + port +
                ": negative timeout: " + timeout));

        if (host != null && host.length() > 0) {
            if (port <= 0 || port >= 65536)
                throw(new IllegalArgumentException("port is illegal: " + port));
            try {
                address = InetAddress.getByName(host);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("unknown host: " + host));
            }
            saddr = new InetSocketAddress(address, port);
        }
        else
            throw(new IllegalArgumentException("bad host: " + host));

        if (proxyHost != null && proxyHost.length() > 0) { // with proxy
            if (port <= 0 || port >= 65536)
                throw(new IllegalArgumentException("proxyport is illegal: " +
                    proxyPort));
            try {
                address = InetAddress.getByName(proxyHost);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("unknown proxy host: " +
                    proxyHost));
            }
            if (type == TYPE_TCP) { // for Type.SOCKS
                Proxy proxy = new Proxy(Proxy.Type.SOCKS,
                    new InetSocketAddress(address, proxyPort));

                try {
                    socket = new Socket(proxy);
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("failed to init proxy: "+
                        proxyHost +":"+ proxyPort +": "+ Utils.traceStack(e)));
                }
            }
            else { // for Type.HTTP
                try {
                    socket = connect(proxyHost, proxyPort, timeout);
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("failed to connect to " +
                        proxyHost +":"+ proxyPort +": "+ Utils.traceStack(e)));
                }
                String request = "CONNECT " + host + ":" + port + " HTTP/1.0\n"+
                    "User-Agent: Java Socket 1.0\r\n\r\n";
                int ic = doHTTPConnect(socket, request, timeout);
                if (ic != HttpURLConnection.HTTP_OK)
                    throw(new IllegalArgumentException("failed to init proxy" +
                        " of " + proxyHost + ":" + proxyPort + ": " + ic));
                try {
                    if (type == TYPE_HTTPS) {
                       SSLSocketFactory sf;
                       sf = (SSLSocketFactory) SSLSocketFactory.getDefault();
                       return sf.createSocket(socket, host, port, true);
                    }
                    else if (type == TYPE_SSL) {
                        SSLContext sc = SSLContext.getInstance("SSL");
                        sc.init(null, new TrustManager[]{new TrustAllManager()},
                            null); 
                        return sc.getSocketFactory().createSocket(socket,
                            host, port, true);
                    }
                    else
                        return socket;
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("failed to create SSL "+
                        "socket for " + host + ":" + port + ": " +
                        Utils.traceStack(e)));
                }
            }
        }
        else try { // no proxy
            if (type == TYPE_TCP)
                socket = new Socket(Proxy.NO_PROXY);
            else if (type == TYPE_HTTPS)
                socket = SSLSocketFactory.getDefault().createSocket();
            else if (type == TYPE_SSL) { // trust all by default
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[]{new TrustAllManager()}, null); 
                socket = sc.getSocketFactory().createSocket();
            }
            else // for TYPE_HTTP
                socket = new Socket(Proxy.NO_PROXY);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init socket: " +
                Utils.traceStack(e)));
        }

        try {
            socket.connect(saddr, timeout);
        }
        catch (SSLHandshakeException e) {
            throw(new IllegalArgumentException("failed on SSL operation to " +
                host + ":" + port + ": " + Utils.traceStack(e)));
        }
        catch (IOException e) {
            throw(new IllegalArgumentException("timed out on connect to " +
                host + ":" + port + ": " + Utils.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to connect to " +
                host + ":" + port + ": " + Utils.traceStack(e)));
        }

        return socket;
    }

    private static int doHTTPConnect(Socket sock, String request, int timeout) {
        long timeEnd;
        int i, k, bufferSize, bytesRead, timeLeft;
        String response;
        byte[] buffer;
        InputStream in = null;
        OutputStream out = null;

        bufferSize = 4096;
        buffer = new byte[bufferSize];
        timeEnd = System.currentTimeMillis() + timeout;
        timeLeft = timeout;
        try {
            in = sock.getInputStream();
            out = sock.getOutputStream();
        }
        catch (Exception e) {
            in = null;
            out = null;
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }
        if (in == null || out == null) {
            in = null;
            out = null;
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }

        try {
            out.write(request.getBytes());
            out.flush();
        }
        catch (IOException e) {
            in = null;
            out = null;
            return HttpURLConnection.HTTP_INTERNAL_ERROR;
        }

        k = 0;
        do { // read everything until EOF or timeout
            bytesRead = 0;
            try {
                sock.setSoTimeout(500);
            }
            catch (Exception ex) {
                in = null;
                out = null;
                return HttpURLConnection.HTTP_INTERNAL_ERROR;
            }
            try {
                if ((bytesRead = in.read(buffer, k, bufferSize - k)) > 0) {
                    k += bytesRead;
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
                out = null;
                in = null;
                return HttpURLConnection.HTTP_PRECON_FAILED;
            }
            catch (Exception e) {
                out = null;
                in = null;
                return HttpURLConnection.HTTP_PRECON_FAILED;
            }
            if (k > 4 && buffer[k - 4] == '\r') { // check end of response
                if (buffer[k-3] == '\n' && buffer[k-2] == '\r' &&
                    buffer[k-1] == '\n') // got all the response
                    break;
            }
            timeLeft = (int) (timeEnd - System.currentTimeMillis());
        } while (bytesRead >= 0 && timeLeft > 0);
        in = null;
        out = null;

        if (timeLeft <= 0)
            return HttpURLConnection.HTTP_GATEWAY_TIMEOUT;

        response = new String(buffer, 0, k);
        i = response.indexOf("\r\n");
        if (i <= 0 || !response.startsWith("HTTP/"))
            return HttpURLConnection.HTTP_NOT_IMPLEMENTED;

        if (response.lastIndexOf("200 OK", i) > 0)
            return HttpURLConnection.HTTP_OK;

System.out.print(response);
        return HttpURLConnection.HTTP_BAD_GATEWAY;
    }
}
