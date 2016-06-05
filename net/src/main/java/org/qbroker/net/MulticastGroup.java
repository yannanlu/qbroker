package org.qbroker.net;

/* MulticastGroup.java - a Multicast socket */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.XQueue;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * MulticastGroup connects to a multicast socket for publishing or receiving
 * Datagram packets.  It supports binding to a given network interface under
 * Java 1.4.
 *<br/><br/>
 * This is NOT MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MulticastGroup {
    private MulticastSocket socket = null;
    private DatagramPacket inPacket, packet;
    private String group, inf = null;
    private InetAddress address = null;
    private NetworkInterface netInf = null;
    private int port;
    private int bufferSize = 8192;
    private int timeout = 1000;
    private int ttl = 1;
    private int waitTime = 500, sleepTime = 0;

    public MulticastGroup(String group, int port) {
        this(group, port, 8192, -1, 1, null);
    }

    public MulticastGroup(String group, int port, int bufferSize) {
        this(group, port, bufferSize, -1, 1, null);
    }

    public MulticastGroup(String group, int port, int bufferSize,
        int timeout, int ttl, String inf) {
        int i;
        if (port > 0 && port < 65536)
            this.port = port;
        else
            throw(new IllegalArgumentException("port is illegal: " + port));

        if (bufferSize > 65535)
            this.bufferSize = 65535;
        else if (bufferSize < 0)
            this.bufferSize = 8192;

        if (timeout >= 0)
            this.timeout = timeout;
        else
            this.timeout = 1000;

        if (ttl >= 0 && ttl <= 255)
            this.ttl = ttl;
        else
            this.ttl = 1;

        if (group != null && group.length() > 0) {
            this.group = group;
            try {
                address = InetAddress.getByName(this.group);
            }
            catch (UnknownHostException e) {
                throw(new IllegalArgumentException("illegal group: " + group));
            }
            if (!address.isMulticastAddress())
              throw(new IllegalArgumentException("not a multicast IP: "+group));
        }
        else {
            throw(new IllegalArgumentException("group not defined"));
        }

        if (inf != null && inf.length() > 0) {
            if (inf.indexOf(".") > 0)
                this.inf = inf;
            else try {
                netInf = NetworkInterface.getByName(inf);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("bad interface: " + inf));
            }
        }

        try {
             connect();
        }
        catch (IOException e) {
            close();
            throw(new IllegalArgumentException("failed to join group " +
                    this.group + ": " + Utils.traceStack(e)));
        }

        packet = new DatagramPacket(new byte[bufferSize], 0, bufferSize,
            address, this.port);
        inPacket = new DatagramPacket(new byte[bufferSize], bufferSize);
    }

    private void connect() throws IOException {
        socket = new MulticastSocket(port);
        if (ttl >= 0 && ttl <= 255)
            socket.setTimeToLive(ttl);
        if (timeout >= 0)
            socket.setSoTimeout(timeout);
        if (netInf != null) {
            try {
                socket.setNetworkInterface(netInf);
            }
            catch (Exception e) {
                throw new IOException("failed to set interface on : "+
                    Utils.traceStack(e));
            }
        }
        else if (inf != null) try {
            socket.setInterface(InetAddress.getByName(inf));
        }
        catch (Exception e) {
            throw new IOException("failed to set interface on " + inf + ": "+
                Utils.traceStack(e));
        }
        socket.joinGroup(address);
    }

    public int reconnect() {
        close();
        try {
            connect();
            return 0;
        }
        catch (IOException e) {
            GenericLogger.log(Event.WARNING, "failed to join group " +
                group + ": " + Utils.traceStack(e));
            close();
        }
        return -1;
    }

    public void close() {
        if (socket != null) {
            try {
                socket.leaveGroup(address);
            }
            catch (IOException e) {
                GenericLogger.log(Event.WARNING, "failed to leave group " +
                    group + ": " + Utils.traceStack(e));
            }
            try {
                socket.close();
            }
            catch (Exception e) {
            }
            socket = null;
        }
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

    public InetAddress getInetAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getTimeout() {
        return timeout;
    }

    public InetAddress getInterface() throws SocketException {
        return socket.getInterface();
    }

    public NetworkInterface getNetworkInterface() throws SocketException {
        return socket.getNetworkInterface();
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

    public void setTimeToLive(int tll) {
        if (ttl >= 0 && ttl < 256 &&
            isConnected()) try {
            socket.setTimeToLive(ttl);
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
        int i, number = 1, timeout = 5000, ttl = 1, port = -1;
        int bufferSize = 8192, sleepTime = 2000, wait = 0;
        String msg = null, group = null, inf = null;
        String str;
        MulticastGroup socket = null;
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
              case 'g':
                if (i + 1 < args.length)
                    group = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 'l':
                if (i+1 < args.length)
                    ttl = Integer.parseInt(args[++i]);
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
              case 'i':
                if (i+1 < args.length)
                    inf = args[++i];
                break;
              default:
            }
        }

        packet = new DatagramPacket(new byte[bufferSize], bufferSize);
        if (inf != null && inf.length() > 0)
            socket = new MulticastGroup(group, port, bufferSize, timeout,
                ttl, inf);
        else
            socket = new MulticastGroup(group, port);
        if (timeout >= 0)
            socket.setTimeout(timeout);
        if (ttl >= 0)
            socket.setTimeToLive(ttl);
        try {
            if (inf != null)
                System.out.println("interface: " +
                    socket.getNetworkInterface().getName());
            else
                System.out.println("interface: " +
                    socket.getInterface().getHostAddress());

            if (msg == null) {
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
            else if (msg != null && ttl < 0) {
                byte[] buffer;
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
        System.out.println("MulticastGroup Version 1.0 (written by Yannan Lu)");
        System.out.println("MulticastGroup: a MulticastSocket for sending or receiving Datagram packets");
        System.out.println("Usage: java org.qbroker.net.MulticastGroup -g group -p port -t timeout -n number [message]");
        System.out.println("  -?: print this message");
        System.out.println("  -g: multicast group IP");
        System.out.println("  -p: port");
        System.out.println("  -l: ttl for send (default: 1)");
        System.out.println("  -s: buffer size for receive (default: 8192)");
        System.out.println("  -t: timeout for receive in sec (default: 5)");
        System.out.println("  -T: sleepTime for send in sec (default: 2)");
        System.out.println("  -w: wait for reply (0: no, 1: yes; default: 0)");
        System.out.println("  -n: number of operations (default: 1)");
        System.out.println("  -i: IP of the interface to be bound");
        System.out.println("  msg: empty for receive)");
    }
}
