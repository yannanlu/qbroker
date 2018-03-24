package org.qbroker.net;

/* ClientSocket.java - a client TCP socket with timeout support */

import java.io.IOException;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
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
    public static final int TYPE_HTTPS = 1;
    public static final int TYPE_SSL = 2;

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

        if (proxyHost != null && proxyHost.length() > 0 && type == TYPE_TCP) {
            Proxy proxy;
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
            proxy = new Proxy(Proxy.Type.SOCKS,
                new InetSocketAddress(address, proxyPort));

            try {
                socket = new Socket(proxy);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to init proxy to " +
                    proxyHost + ":" + proxyPort + ": " + Utils.traceStack(e)));
            }
        }
        else try {
            if (type == TYPE_TCP)
                socket = new Socket(Proxy.NO_PROXY);
            else if (type == TYPE_HTTPS)
                socket = SSLSocketFactory.getDefault().createSocket();
            else { // trust all by default
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[]{new TrustAllManager()}, null); 
                socket = sc.getSocketFactory().createSocket();
            }
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
}
