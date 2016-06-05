package org.qbroker.net;

/* ClientSocket.java - a client TCP socket with timeout support */

import java.io.IOException;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLHandshakeException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeoutException;

/**
 * ClientSocket opens a TCP socket with timeout protections.  The
 * static method, connect(), returns the socket object if the open
 * is successful before it is timed out.  If type is set to TYPE_HTTPS,
 * the socket will be on SSL that verifies the certificate. If type is set
 * to TYPE_SSL, the socket will be on SSL that trusts all certificates.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ClientSocket implements Runnable {
    private String uri = null;
    private String host = null;
    private int port = 0;
    private int type = 0;
    private InetAddress address = null;
    private Socket socket = null;

    public static final int TYPE_TCP = 0;
    public static final int TYPE_HTTPS = 1;
    public static final int TYPE_SSL = 2;

    public static Socket connect(String host, int port, int timeout, int type) {
        if (timeout < 0)
            throw(new IllegalArgumentException(host + ":" + port +
                ": negative timeout: " + timeout));
        int timeLeft = timeout;
        ClientSocket client = new ClientSocket(host, port, type);
        Thread t = new Thread(client);
        t.start();
        long timeStart = System.currentTimeMillis();
        for (;;) {
            try {
                t.join(timeLeft);
                if (t.isAlive()) try { // in case of timedout, interrupt thread
                    t.interrupt();
                    Thread.sleep(2000);
                    client.close();
                    GenericLogger.log(Event.ERR, Utils.traceStack(
                        new TimeoutException("Socket to " + host + ":" + port +
                            " is closed due to timeout: " + timeout)));
                }
                catch (Exception ex) {
                    client.close();
                    GenericLogger.log(Event.ERR, Utils.traceStack(
                        new TimeoutException("Socket to " + host + ":" + port +
                            " is closed due to timeout: " + timeout)));
                }
            }
            catch (InterruptedException e) {
                timeLeft = timeout-(int)(System.currentTimeMillis()-timeStart);
                if (timeLeft > 0)
                    continue;
                else if (t.isAlive()) try {
                    t.interrupt();
                    client.close();
                    GenericLogger.log(Event.ERR, Utils.traceStack(
                        new TimeoutException("Socket to " + host + ":" + port +
                            " is closed due to timeout: " + timeout)));
                }
                catch (Exception ex) {
                    client.close();
                    GenericLogger.log(Event.ERR, Utils.traceStack(
                        new TimeoutException("Socket to " + host + ":" + port +
                            " is closed due to timeout: " + timeout)));
                }
            }
            break;
        }
        return client.getSocket();
    }

    public static Socket connect(String host, int port, int timeout) {
        return connect(host, port, timeout, TYPE_TCP);
    }

    public ClientSocket(String host, int port, int type) {
        int i;

        this.socket = null;
        this.type = type;

        if (host != null && host.length() > 0) {
            this.host = host;

            if (port > 0 && port < 65536)
                this.port = port;
            else
                throw(new IllegalArgumentException("port is illegal: " + port));
            try {
                address = InetAddress.getByName(this.host);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("unknown host: " +host));
            }
            if ((i = this.host.indexOf('.')) > 0 &&
                !this.host.equals(address.getHostAddress()))
                this.host = this.host.substring(0, i);
        }
        else
            throw(new IllegalArgumentException("bad host: " +host));

        this.uri = (type == TYPE_HTTPS || type == TYPE_SSL) ? "https://" +
            host + ":" + port : host + ":" + port;
    }

    public ClientSocket(String host, int port) {
        this(host, port, TYPE_TCP);
    }

    public void run() {
        int i;
        try {
            if (type == TYPE_TCP)
                socket = new Socket(address, port);
            else if (type == TYPE_HTTPS)
                socket=SSLSocketFactory.getDefault().createSocket(address,port);
            else { // trust all by default
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[]{new TrustAllManager()}, null); 
                socket = sc.getSocketFactory().createSocket(address,port);
            }
        }
        catch (SocketException e) {
            GenericLogger.log(Event.ERR, "Connection to " + uri +
                " is interrupted: " + Utils.traceStack(e));
            close();
        }
        catch (IOException e) {
            GenericLogger.log(Event.ERR, "Socket to " + uri +
                " is interrupted: " + Utils.traceStack(e));
            close();
        }
        catch (Exception e) {
            GenericLogger.log(Event.ERR, "Socket to " + uri +
                " failed to init: " + Utils.traceStack(e));
            close();
        }
    }

    private Socket getSocket() {
        return socket;
    }

    public synchronized void close() {
        if (socket == null)
            return;
        try {
            socket.shutdownOutput();
        }
        catch (Exception e) {
            GenericLogger.log(Event.DEBUG, "Failed to shutdown output on " +
                uri + ": " + Utils.traceStack(e));
        }
        try {
            socket.shutdownInput();
        }
        catch (Exception e) {
            GenericLogger.log(Event.DEBUG, "Failed to shutdown input on " +
                uri + ": " + Utils.traceStack(e));
        }
        try {
            socket.close();
        }
        catch (Exception e) {
            GenericLogger.log(Event.DEBUG, "Failed to close socket on " +
                uri + ": " + Utils.traceStack(e));
        }
        socket = null;
    }

    protected void finalize() {
        if (socket != null) try {
            socket.close();
        }
        catch (Exception e) {
        }
        socket = null;
    }
}
