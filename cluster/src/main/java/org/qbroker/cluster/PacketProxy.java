package org.qbroker.cluster;

/* PacketProxy.java - a proxy forwarding UDP packets */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QuickCache;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.Template;
import org.qbroker.common.TimeoutException;
import org.qbroker.net.UDPSocket;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.event.Event;

/**
 * PacketProxy is a proxy forwarding UDP packets to existing destinations.
 * As a utility, it can ack like an observer for a cluster.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PacketProxy implements Runnable {
    private String name;
    private UDPSocket socket = null;
    private InetAddress address;
    private int port;
    private IndexedXQueue root = null;
    private int timeout, waitTime, heartbeat, sessionTimeout;
    private long sleepTime = 1000L;
    private AssetList nodeList;
    private String myURI;
    private int debug, maxGroupSize = 32, bufferSize = 8192;
    public final static int DEBUG_NONE = 0;
    public final static int DEBUG_INIT = 1;
    public final static int DEBUG_EXAM = 2;
    public final static int DEBUG_CHECK = 4;
    public final static int DEBUG_RELAY = 8;
    public final static int DEBUG_CLOSE = 16;
    public final static int DEBUG_HBEAT = 32;
    public final static int DEBUG_ALL = 2 * DEBUG_HBEAT - 1;

    public PacketProxy(Map props) {
        Template template;
        Object o;
        URI u;
        String uri, host;

        template = new Template("##hostname##, ##HOSTNAME##");
        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        else if (o instanceof String)
            uri = MonitorUtils.substitute((String) o, template);
        else if (o instanceof Map) {
            o = ((Map) o).get(Event.getHostName());
            if (o == null || !(o instanceof String))
                throw(new IllegalArgumentException("URI is not well defined"));
            else
                uri = MonitorUtils.substitute((String) o, template);
        }
        else
            throw(new IllegalArgumentException("URI is not well defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"udp".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: "+u.getScheme()));

        if ((port = u.getPort()) <= 0)
            throw(new IllegalArgumentException("port not defined: " + uri));

        if ((host = u.getHost()) == null || host.length() == 0)
            throw(new IllegalArgumentException("no hostname specified in URL"));

        try {
            address = InetAddress.getByName(host);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException("unknown host: " + host));
        }

        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);

        host = address.getHostAddress();
        socket = new UDPSocket(host, port, null, -1, bufferSize, 0);
        myURI = address.getHostAddress() + ":" + port;

        if ((o = props.get("Timeout")) != null)
            timeout = Integer.parseInt((String) o);
        else
            timeout = 1000;

        if (timeout > 0) // socket receive() blocks if timeout = 0
            socket.setTimeout(timeout);

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);
        else
            heartbeat = 10000;

        if ((o = props.get("WaitTime")) != null)
            waitTime = Integer.parseInt((String) o);
        else
            waitTime = 100;

        if (sleepTime <= 0L)
            sleepTime = 1000L;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = DEBUG_NONE;

        nodeList = new AssetList(myURI, maxGroupSize);

        root = new IndexedXQueue(name, 64);

        sessionTimeout = 3 * heartbeat;
    }

    public void run() {
        int exceptionCount = 0;
        long loopTime = System.currentTimeMillis();
        String threadName = Thread.currentThread().getName();
        String uri = threadName;

        XQueue xq = root;

        if ("close".equals(threadName)) {
            close();
            new Event(Event.INFO,  name + ": " + myURI + " closed").send();
            return;
        }

        if ("udp_socket".equals(threadName)) {
            uri = myURI;
            new Event(Event.INFO, name + ": " + uri + " started").send();
            while (keepRunning(xq) && socket != null) {
                try {
                    socket.receive(xq);
                }
                catch (Exception e) {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - loopTime >= sessionTimeout) {
                        exceptionCount = 0;
                        loopTime = currentTime;
                    }
                    exceptionCount ++;
                    if (exceptionCount <= 2)
                        new Event(Event.ERR, name + ": failed to receive on " +
                            uri + ": " + Event.traceStack(e)).send();
                }
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                }
            }
            try { // graceful time to allow QUIT packet going out
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
            }
            socket.close();
        }

        new Event(Event.INFO, name + ": " + uri + " stopped").send();
    }

    public void close() {
        stopRunning(root);
        if (socket != null)
            socket.close();
    }

    private boolean keepRunning(XQueue xq) {
        if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0)
            return true;
        else
            return false;
    }

    private void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    /** sends the packet to all destinations */
    private void send(DatagramPacket packet) {
        if (packet == null)
            return;
        try {
            socket.send(packet);
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": failed to send packet: " +
                Event.traceStack(e)).send();
        }
    }

    /**
     * It forwards each incoming packet to rest destinations
     */
    private void proxy() {
        Object o;
        String uri;
        XQueue xq;
        DatagramPacket packet;
        long currentTime, sessionTime;
        int i, k = -1, cid = -1;
        byte[] buffer = null;
        Browser browser = nodeList.browser();

        xq = root;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;

        while (keepRunning(xq)) {
            if ((cid = xq.getNextCell(waitTime)) < 0) {
                currentTime += waitTime;
                if (currentTime - sessionTime < heartbeat)
                    continue;

                currentTime = System.currentTimeMillis();
                sessionTime = currentTime;
                if (nodeList.size() > 0) // check idle destinations
                    check(currentTime);

                continue;
            }

            currentTime = System.currentTimeMillis();
            if ((o = xq.browse(cid)) == null) { // null packet
                xq.remove(cid);
                new Event(Event.WARNING, name + ": null packet from "+
                    xq.getName()).send();
                continue;
            }
            else if (o instanceof DatagramPacket) { // packet
                packet = (DatagramPacket) o;
                display(0, DEBUG_EXAM, packet);
                uri = packet.getAddress().getHostAddress() + ":" +
                    packet.getPort();
                buffer = packet.getData();
                if (!nodeList.containsKey(uri)) {
                    k = nodeList.add(uri, new long[]{currentTime}, packet);
                }
                else {
                    long[] info;
                    k = nodeList.getID(uri);
                    info = nodeList.getMetaData(k);
                    info[0] = currentTime;
                }
            }

            // reset object to null for a new packet next turn
            xq.remove(cid);

            browser.reset();
            while ((i = browser.next()) >= 0) { // forward the packet to rest
                if (i == k)
                    continue;
                packet = (DatagramPacket) nodeList.get(i);
                packet.setData(buffer, 0, buffer.length);
                send(packet);
                display(1, DEBUG_CHECK, packet);
            }
            if (!keepRunning(xq))
                 break;
            if (currentTime - sessionTime < heartbeat)
                continue;

            currentTime = System.currentTimeMillis();
            sessionTime = currentTime;
            if (nodeList.size() > 0) // check idle destinations
                check(currentTime);
        }
    }

    private int check(long currentTime) {
        int i, n;
        int[] list;
        long[] info = null;
        StringBuffer strBuf = null;
        Browser browser = nodeList.browser();
        if ((debug & DEBUG_HBEAT) > 0)
            strBuf = new StringBuffer();

        n = nodeList.size();
        list= new int[n];
        n = 0;
        while ((i = browser.next()) >= 0) {
           info = nodeList.getMetaData(i);
           if (currentTime - info[0] >= sessionTimeout)
               list[n++] = i;
           if ((debug & DEBUG_HBEAT) > 0) {
               DatagramPacket packet = (DatagramPacket) nodeList.get(i);
               info = nodeList.getMetaData(i);
               strBuf.append("\n\t" + i +": "+
                   packet.getAddress().getHostAddress() + ":" +
                   packet.getPort() +" "+ Event.dateFormat(new Date(info[0])));
           }
        }

        for (i=0; i<n; i++) // remove those idled node
            nodeList.remove(list[i]);

        if ((debug & DEBUG_HBEAT) > 0)
            new Event(Event.DEBUG, name + ": " + n + "/" + nodeList.size() +
                strBuf.toString()).send();

        return n;
    }

    /**
     * It starts the cluster node and supervises its state.  With the
     * communication XQueue defined, the cluster node will escalate or relay
     * events to/from external caller via the XQueue.  The cluster node will
     * keep running until its shutdown.
     */
    public void start() {
        Thread s, thr = null;
        s = new Thread(this, "udp_socket");
        s.setPriority(Thread.NORM_PRIORITY);
        s.setDaemon(true);
        s.start();

        proxy();

        stopRunning(root);

        if (s.isAlive()) {
            s.interrupt();
            try {
                s.join(5000);
            }
            catch (Exception e) {
                s.interrupt();
            }
        }

        if (nodeList != null && nodeList.size() > 0)
            nodeList.clear();
        if (socket != null)
            socket.close();
        new Event(Event.INFO, name + ": " + myURI + " terminated").send();
    }

    /**
     * It displays packets for troubleshooting and debugging.<br/>
     * option: 0 - displaying incoming packets with tag of a space<br/>
     * option: 1 - displaying outgoing packets with tag of a colon<br/>
     * option: 2 - displaying discarded incoming packets with tag of a bam<br/>
     * location: - debug mask for a specified section
     */
    private void display(int option, int location, DatagramPacket packet) {
        if (packet == null)
            return;
        if ((debug & location) > 0) {
            if (option == 1 && location == DEBUG_HBEAT)
                new Event(Event.DEBUG, name + ": " +
                    new String(packet.getData(), 0, packet.getLength()) + " " +
                    packet.getAddress().getHostAddress() + ":" +
                    packet.getPort()).send();
            else if (option == 2)
                new Event(Event.DEBUG, name + "! " +
                    new String(packet.getData(), 0, packet.getLength()) + " " +
                    root.depth() + "/" + root.size() + " " +
                    root.getGlobalMask()).send();
            else
                new Event(Event.DEBUG, name + ((option==0) ? "  " : ": ") +
                    new String(packet.getData(), 0, packet.getLength())).send();
        }
    }

    public static void main(String args[]) {
        Thread c;
        int i, timeout = 30, port = -1, heartbeat = 10;
        int bufferSize = 8192, waitTime = 100, debug = 0;
        String host = null, logDir = null;
        PacketProxy node;
        Map<String, Object> props;

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'L':
                if (i+1 < args.length)
                    logDir = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 'd':
                if (i+1 < args.length)
                    debug = Integer.parseInt(args[++i]);
                break;
              case 'h':
                if (i+1 < args.length)
                    heartbeat = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    bufferSize = Integer.parseInt(args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'w':
                if (i+1 < args.length)
                    waitTime = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (logDir != null)
            Event.setLogDir(logDir);

        props = new HashMap<String, Object>();
        props.put("URI", "udp://##hostname##:" + port);
        props.put("Name", "Proxy");
        props.put("BufferSize", String.valueOf(bufferSize));
        props.put("Debug", String.valueOf(debug));
        props.put("WaitTime", String.valueOf(waitTime));
        props.put("Heartbeat", String.valueOf(heartbeat));

        try {
            node = new PacketProxy(props);
            c = new Thread(node, "close");
            Runtime.getRuntime().addShutdownHook(c);

            node.start();

            node.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("PacketProxy Version 1.0 (written by Yannan Lu)");
        System.out.println("PacketProxy: a UDP packet proxy");
        System.out.println("Usage: java org.qbroker.cluster.PacketProxy -u hostname -p port");
        System.out.println("  -?: print this message");
        System.out.println("  -p: port");
        System.out.println("  -d: debug mode (default: 0)");
        System.out.println("  -h: heartbeat in sec (default: 10)");
        System.out.println("  -s: buffer size for receive (default: 8192)");
        System.out.println("  -w: waitTime for XQueue in ms (default: 100)");
        System.out.println("  -L: log directory");
    }
}
