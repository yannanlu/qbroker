package org.qbroker.net;

/* UDPSocket.java  - a Datagram Socket */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.XQueue;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * UDPSocket connects an UDP socket for sending and/or receiving Datagram
 * packets.  Due to its connectionless feature, you can use the same socket
 * to send and receive packets as long as the outgoing packets having the
 * destination IP address and the prot.  Please do not use it to transfer
 * the data more than 8K, since there is no implementation on handling data
 * longer than the buffer size.
 *<br/><br/>
 * Please also be careful on the differences of implementations and Java
 * versions.  On Java 1.4, the send() method has implemented ICMP ping
 * if connect() is used.  Therefore, you will get PortUnreachableException
 * if you try to send packets to a connected destination where there is
 * no one listening to it.  On Java 1.3, there is no such correlation by
 * calling connect().  To establish a connectionless UDPSocket, you can
 * pass hostname as null and set up the socket as a server socket.  This
 * will bypass connect() in the initialization.  Set address and port
 * to each outgoing packets for sending.
 *<br/><br/>
 * Be careful also on the timeout.  If it is set to zero, the block on receive
 * is forever.  This may end up dead lock.
 *<br/><br/>
 * This is NOT MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class UDPSocket {
    private DatagramSocket socket = null;
    private DatagramPacket inPacket, packet;
    private String serverName = null, hostname = null;
    private InetAddress serverAddress = null, address = null;
    private int serverPort = 0, port;
    private int timeout = 1000;
    private int bufferSize = 8192;
    private int waitTime = 500, sleepTime = 0;

    // for client mode only
    public UDPSocket(String hostname, int port) {
        this(null, -1, hostname, port, 8192, 0);
    }

    // for server mode only
    public UDPSocket(String serverName, int serverPort, int bufferSize) {
        this(serverName, serverPort, null, -1, bufferSize, 0);
    }

    // generic case
    public UDPSocket(String serverName, int serverPort, String hostname,
        int port, int bufferSize, int timeout) {
        int i;

        if (bufferSize > 65535)
            this.bufferSize = 65535;
        else if (bufferSize < 0)
            this.bufferSize = 8192;

        if (timeout >= 0)
            this.timeout = timeout;
        else
            this.timeout = 1000;

        // for server mode
        if (serverPort >= 0 && serverPort < 65536)
            this.serverPort = serverPort;
        else
            this.serverPort = -1;

        if (serverName != null && serverName.length() > 0) {
            this.serverName = serverName;

            try {
                serverAddress = InetAddress.getByName(this.serverName);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("unknown server: " +
                    serverName));
            }
            if ((i = this.serverName.indexOf('.')) > 0 &&
                !this.serverName.equals(serverAddress.getHostAddress()))
                this.serverName = this.serverName.substring(0, i);
        }
        else // localhost
            serverAddress = null;

        if (hostname != null && hostname.length() > 0) {
            this.hostname = hostname;

            if (port > 0 && port < 65536)
                this.port = port;
            else
                throw(new IllegalArgumentException("port is illegal: " + port));
            try {
                address = InetAddress.getByName(this.hostname);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("unknown host: " +hostname));
            }
            if ((i = this.hostname.indexOf('.')) > 0 &&
                !this.hostname.equals(address.getHostAddress()))
                this.hostname = this.hostname.substring(0, i);
        }
        else // server mode
            address = null;

        try {
             connect();
        }
        catch (SocketException e) {
            if (address != null)
                GenericLogger.log(Event.WARNING,"failed to open UDP socket to "+
                    this.hostname+":"+port+": " + Utils.traceStack(e));
            else // server mode
            throw(new IllegalArgumentException("failed to open UDP socket to "+
                    this.hostname + ":" + port + ": " + Utils.traceStack(e)));
            close();
        }

        if (address != null) {
            packet = new DatagramPacket(new byte[bufferSize], 0, bufferSize,
                address, this.port);
        }
        inPacket = new DatagramPacket(new byte[bufferSize], bufferSize);
    }

    private void connect() throws SocketException {
        if (serverAddress != null && serverPort > 0)
            socket = new DatagramSocket(serverPort, serverAddress);
        else if (serverPort > 0)
            socket = new DatagramSocket(serverPort);
        else {
            socket = new DatagramSocket();
            if (serverPort >= 0)
                serverPort = socket.getLocalPort();
        }
        if (address != null)
            socket.connect(address, port);
        socket.setSoTimeout(timeout);
    }

    public int reconnect() {
        close();
        try {
            connect();
            return 0;
        }
        catch (SocketException e) {
            if (address != null)
                GenericLogger.log(Event.WARNING,"failed to open UDP socket to "+
                    hostname + ":" + port + ": " + Utils.traceStack(e));
            else
                GenericLogger.log(Event.WARNING, "failed to open UDP socket: " +
                    Utils.traceStack(e));
            close();
        }
        return -1;
    }

    public void close() {
        if (socket != null) try {
            socket.disconnect();
            socket.close();
        }
        catch (Exception e) {
        }
        socket = null;
    }

    public boolean isConnected() {
        if (socket != null) {
            try {
                socket.getSoTimeout();
            }
            catch (SocketException e) {
                return false;
            }
            catch (Exception e) {
                return false;
            }
            return true;
        }
        return false;
    }

    public InetAddress getLocalAddress() {
        if (socket != null)
            return socket.getLocalAddress();
        else
            return null;
    }

    public int getLocalPort() {
        if (socket != null)
            return socket.getLocalPort();
        else
            return -1;
    }

    public int getTimeout() {
        return timeout;
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    public void setTimeout(int timeout) {
        if (timeout >= 0 && isConnected()) try {
            socket.setSoTimeout(timeout);
        }
        catch (Exception e) {
        }
    }

    public void setSleepTime(int ms) {
        if (ms >= 0)
            sleepTime = ms;
    }

    public void setWaitTime(int ms) {
        if (ms >= 0)
            waitTime = ms;
    }

    public void send(DatagramPacket p) throws IOException {
        if (socket == null)
            throw(new NullPointerException("socket is null"));
        if (p == null)
            throw(new IllegalArgumentException("packet is null"));

        socket.send(p);
    }

    public void send(byte[] buffer, int offset, int length) throws IOException {
        if (socket == null)
            throw(new NullPointerException("socket is null"));
        if (address == null)
            throw(new IOException("destination address is null"));
        if (buffer == null)
            throw(new IllegalArgumentException("buffer is null"));

        if (packet == null)
            packet = new DatagramPacket(buffer, offset, length, address, port);
        else
            packet.setData(buffer, offset, length);

        socket.send(packet);
    }

    public void send(String text) throws IOException {
        if (socket == null)
            throw(new IOException("socket is null"));
        if (address == null)
            throw(new IOException("destination address is null"));

        if (text == null)
            throw(new IllegalArgumentException("text is null"));

        if (packet == null)
            packet = new DatagramPacket(text.getBytes(), 0, text.length(),
                address, port);
        else
            packet.setData(text.getBytes(), 0, text.length());

        socket.send(packet);
    }

    public void send(XQueue xq) throws IOException {
        int sid;
        int count = 0;
        DatagramPacket p;

        if (socket == null)
            throw(new IOException("socket is null"));
        if (xq == null)
            throw(new IllegalArgumentException("xq is null"));

        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((sid = xq.getNextCell(waitTime)) < 0)
                continue;

            p = (DatagramPacket) xq.browse(sid);
            if (p == null) { // packet is supposed not to be null
                xq.remove(sid);
                GenericLogger.log(Event.WARNING, "null packet from " +
                    xq.getName());
                continue;
            }
            try {
                socket.send(p);
            }
            catch (IOException e) {
                xq.putback(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.putback(sid);
                throw(new IOException(Utils.traceStack(e)));
            }
            xq.remove(sid);
            count ++;
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
        }
    }

    public void receive(DatagramPacket p) throws IOException {
        if (socket == null)
            throw(new NullPointerException("socket is null"));
        if (p == null)
            throw(new IllegalArgumentException("packet is null"));

        socket.receive(p);
    }

    /**
     *  get a used packet from XQ first and send the received packet to XQ
     *  if the used packet is null, create a new packet for use
     */
    public void receive(XQueue xq) throws IOException {
        int sid, cid;
        int count = 0;
        DatagramPacket p;

        if (socket == null)
            throw(new NullPointerException("socket is null"));
        if (xq == null)
            throw(new IllegalArgumentException("xq is null"));

        do {
            sid = xq.reserve(waitTime);
        } while (sid < 0 && (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);

        if ((p = (DatagramPacket) xq.browse(sid)) == null)
            p = new DatagramPacket(new byte[bufferSize], bufferSize);

        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            try {
                socket.receive(p);
            }
            catch (InterruptedIOException e) {
                continue;
            }
            catch (IOException e) {
                xq.cancel(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.cancel(sid);
                throw(new IOException(Utils.traceStack(e)));
            }
            cid = xq.add(p, sid);
            if (cid >= 0) {
                count ++;
            }
            else {
                xq.cancel(sid);
                GenericLogger.log(Event.WARNING, "XQ is full and lost packet: "+
                     new String(p.getData(), 0, p.getLength()));
            }
            do {
                sid = xq.reserve(waitTime);
            } while (sid < 0 && (xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);

            if ((p = (DatagramPacket) xq.browse(sid)) == null)
                p = new DatagramPacket(new byte[bufferSize], bufferSize);
        }
    }

    public String receive() throws IOException {
        if (socket == null)
            return null;
        if (inPacket == null)
            inPacket = new DatagramPacket(new byte[bufferSize], bufferSize);

        try {
            socket.receive(inPacket);
        }
        catch (InterruptedIOException e) {
            return "";
        }

        byte[] buffer = inPacket.getData();
        StringBuffer strBuf =
            new StringBuffer(inPacket.getAddress().getHostAddress());
        strBuf.append(":" + inPacket.getPort());
        strBuf.append(" " + new String(buffer, 0, inPacket.getLength()));

        // reset to the buffer length
        inPacket.setLength(buffer.length);

        return strBuf.toString();
    }

    public static void main(String args[]) {
        int i, number = 1, timeout = 5000, serverPort = -1, port = -1;
        int bufferSize = 8192, sleepTime = 2000, wait = 0;
        String msg = null, hostname = null, serverHost = null;
        String str;
        UDPSocket socket = null;
        DatagramPacket packet;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                if (i == args.length-1)
                    msg = args[i];
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'h':
                if (i + 1 < args.length)
                    serverHost = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    serverPort = Integer.parseInt(args[++i]);
                break;
              case 'H':
                if (i + 1 < args.length)
                    hostname = args[++i];
                break;
              case 'P':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    bufferSize = Integer.parseInt(args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = 1000 * Integer.parseInt(args[++i]);
                break;
              case 'T':
                if (i+1 < args.length)
                    sleepTime = 1000 * Integer.parseInt(args[++i]);
                break;
              case 'w':
                if (i+1 < args.length)
                    wait = Integer.parseInt(args[++i]);
                break;
              case 'n':
                if (i+1 < args.length)
                    number = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        packet = new DatagramPacket(new byte[bufferSize], bufferSize);
        try {
            if (hostname == null && msg == null) { // server mode
                socket = new UDPSocket(null, serverPort, bufferSize);
                if (timeout >= 0)
                    socket.setTimeout(timeout);
                for (i=0; i<number; i++) {
                    msg = socket.receive();
                    if (msg == null)
                        socket.reconnect();
                    else if (msg.length() == 0)
                        System.out.println("timed out in " + (timeout/1000) +
                            " sec");
                    else {
                        System.out.println(msg);
                    }
                    if (sleepTime > 0) try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                    }
                }
            }
            else if (hostname == null && msg != null) { // server mode
                byte[] buffer;
                socket = new UDPSocket(null, serverPort, bufferSize);
                if (timeout >= 0)
                    socket.setTimeout(timeout);
                for (i=0; i<number; i++) {
                    try {
                        socket.receive(packet);
                    }
                    catch (NullPointerException ex) {
                        socket.reconnect();
                        continue;
                    }
                    catch (InterruptedIOException ex) {
                        System.out.println("timed out in " + (timeout/1000) +
                            " sec");
                        continue;
                    }
                    buffer = packet.getData();
                    str = new String(buffer, 0, packet.getLength());
                    System.out.println(packet.getAddress().getHostAddress() +
                        ":" + packet.getPort() + ": " + str);
                    str = msg + " at " + i + ": " + str;
                    packet.setData(str.getBytes(), 0, str.length());
                    socket.send(packet);
                    packet.setData(buffer, 0, buffer.length);
                    if (sleepTime > 0) try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                    }
                }
            }
            else {
                socket = new UDPSocket(hostname, port);
                if (timeout >= 0)
                    socket.setTimeout(timeout);
                for (i=0; i<number; i++) {
                    long timestamp = System.currentTimeMillis();
                    StringBuffer strBuf = new StringBuffer();
                    strBuf.append(Utils.dateFormat(new Date(timestamp)));
                    strBuf.append(" " + i + ": " + msg);
                    socket.send(strBuf.toString());
                    if (wait > 0) {
                        str = socket.receive();
                        if (str == null) {
                            socket.reconnect();
                            continue;
                        }
                        else if (str.length() == 0) {
                            System.out.println("timed out in "+ (timeout/1000) +
                                " sec");
                            continue;
                        }
                        else
                            System.out.println(str);
                    }

                    if (sleepTime > 0) try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                    }
                }
            }
            socket.close();
        } catch (Exception e) {
            if (socket != null)
                socket.close();
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("UDPSocket Version 1.0 (written by Yannan Lu)");
        System.out.println("UDPSocket: an UDPSocket for sending or receiving Datagram packets");
        System.out.println("Usage: java org.qbroker.net.UDPSocket -h hostname -p port -t timeout -n number [message]");
        System.out.println("  -?: print this message");
        System.out.println("  -h: server hostname or IP (for receive)");
        System.out.println("  -p: server port (for receive)");
        System.out.println("  -H: destination hostname or IP (for send)");
        System.out.println("  -P: remote port (for send)");
        System.out.println("  -s: buffer size for receive (default: 8192)");
        System.out.println("  -t: timeout for receive in sec (default: 5)");
        System.out.println("  -T: sleepTime for send in sec (default: 2)");
        System.out.println("  -w: wait for reply (0: no, 1: yes; default: 0)");
        System.out.println("  -n: number of operations (default: 1)");
        System.out.println("  msg: empty for receive)");
    }
}
