package org.qbroker.cluster;

/* UnicastNode.java - a cluster node of a Unicast Cluster */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Arrays;
import java.text.SimpleDateFormat;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import javax.jms.TextMessage;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QuickCache;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.Template;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Utils;
import org.qbroker.net.UDPSocket;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.receiver.Receiver;
import org.qbroker.receiver.StreamReceiver;
import org.qbroker.receiver.PacketReceiver;
import org.qbroker.receiver.JMSReceiver;
import org.qbroker.cluster.ClusterNode;
import org.qbroker.cluster.PacketNodeBean;
import org.qbroker.event.Event;

/**
 * UnicastNode is a cluster node of a Unicast cluster.  The cluster
 * contains only two UnicastNodes communicating with each other via
 * DatagramPackets.  One of the nodes is the manager that supervises
 * the cluster.  The other node is the worker or the monitor
 * reporting to the manager and monitoring its behavior closely.  In
 * case that the manager quits or the monitor misses certain number of
 * heartbeats from the manager, the monitor will take over the role of
 * manager and resumes to work.  The monitor can leave the cluster anytime.
 * If the last node leaves the cluster, the cluster goes down. Due to the
 * feature of Unicast, it supports a cluster accros networks.
 *<br><br>
 * UnicastNode also supports an extra path for communications between nodes
 * if ExtraReceiver is defined.  With ExtraReceiver, each outgoing packet will
 * be duplicated on its content.  Then it will be sent out via the extra
 * communication path.  Currently, ExtraReceiver only supports three of the
 * schemes, comm, wmq or udp.  In case of comm, an RS232 connection is required
 * as the direct connection between both nodes.  In case of wmq, a WebSphere MQ
 * is required for both nodes to share and access.  In case of udp, it requires
 * a separate observer to forward packets for the secondary communication path.
 * The URI of ExtraReceiver specifies how to access the extra receiver.
 *<br><br>
 * UnicastNode is supposed to run at backgroud.  It is designed to be a black
 * automaton box.  It will automatically maitain the state of the cluster.
 * But if the escalation outLink is specified at startup, it will also send
 * the state information and relay the data.  The application on the node can
 * intercept the events from the node and send data to any other nodes via
 * the API of relay().
 *<br><br>
 * A UnicastNode may be in one of the following states:<br>
 * NODE_NEW:      just starting up, not joining the cluster yet<br>
 * NODE_RUNNING:  joined in the cluster and running OK<br>
 * NODE_STANDBY:  joined in the cluster but still in transient state<br>
 * NODE_TIMEOUT:  missed a fixed number of heartbeats from the node<br>
 * NODE_DOWN:     no longer active<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public class UnicastNode implements ClusterNode, Runnable {
    private String name;
    private UDPSocket socket = null;
    private InetAddress address;
    private int port;
    private IndexedXQueue root = null;
    private XQueue out = null;
    private Receiver extraRcvr = null;
    private PacketNodeBean myBean;
    private Event event = null;
    private int timeout, waitTime, heartbeat, sessionTimeout, ttl;
    private int outBegin, outLen, rcvrType = RCVR_NONE;
    private long sleepTime = 1000L;
    private AssetList nodeList;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private String masterURI, myURI, groupURI, extraURI = null;
    private int debug, previousStatus, repeatPeriod;
    private long sequenceNumber = 0L, sequenceId = 0L;
    private int maxRetry, maxGroupSize, groupSize, bufferSize = 8192;
    private boolean isInitialized, hasExtra = false;

    public UnicastNode(Map props) {
        Template template;
        Object o;
        URI u;
        String uri, host;

        template = new Template("##hostname##, ##HOSTNAME##");
        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = MonitorUtils.substitute((String) o, template);
        groupURI = name;

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

        host = address.getHostAddress();
        socket = new UDPSocket(host, port, null, -1, bufferSize, 0);
        myURI = address.getHostAddress() + ":" + port;

        host = null;
        if ((o = props.get("Member")) != null && o instanceof List) {
            List list = (List) o;
            int i, n = list.size();
            host = Event.getHostName();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                if (host.equalsIgnoreCase((String) o))
                    continue;
                host = (String) o;
                break;
            }
            if (i >= n) // not found
                host = null;
        }
        else
            throw(new IllegalArgumentException("Member not defined: " + name));

        if (host == null || host.length() <= 0)
            throw(new IllegalArgumentException("Patener not defined: " + name));

        try {
            address = InetAddress.getByName(host);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException("unknown host: " + host));
        }

        if ((o = props.get("Timeout")) != null)
            timeout = Integer.parseInt((String) o);
        else
            timeout = 1000;

        if (timeout > 0) // socket receive() blocks if timeout = 0
            socket.setTimeout(timeout);

        if ((o = props.get("WaitTime")) != null)
            waitTime = Integer.parseInt((String) o);
        else
            waitTime = 100;

        // interval for the heartbeat
        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000*Integer.parseInt((String) o);
        else
            heartbeat = 10000;

        // timeout for the session of joining group
        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000*Integer.parseInt((String) o);
        else
            sessionTimeout = 3 * heartbeat;

        if (sessionTimeout <= heartbeat)
            sessionTimeout = 3 * heartbeat;

        if ((o = props.get("SleepTime")) != null)
            sleepTime = 1000*Long.parseLong((String) o);

        if (sleepTime <= 0L)
            sleepTime = 1000L;

        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = DEBUG_NONE;

        // retry number of sessions before to give up
        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);
        else
            maxRetry = 3;

        maxGroupSize = 2;

        repeatPeriod = 3600000 / sessionTimeout;

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            pattern = pc.compile("^(\\d+) (-?\\d+) (\\d+) (.*)");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        nodeList = new AssetList(myURI, maxGroupSize);

        if ((o = props.get("ExtraReceiver")) != null && o instanceof Map) try {
            Map<String, Object> map = Utils.cloneProperties((Map) o);
            String device;

            if ((o = map.get("Name")) == null)
                throw(new IllegalArgumentException("Name is not defined"));
            device = MonitorUtils.substitute((String) o, template);

            map.put("Name", device);

            if ((o = map.get("URI")) == null)
                throw(new IllegalArgumentException("URI is not defined"));
            else if (o instanceof String)
                extraURI = MonitorUtils.substitute((String) o, template);
            else if (o instanceof Map) {
                o = ((Map) o).get(Event.getHostName());
                if (o == null || !(o instanceof String))
                 throw(new IllegalArgumentException("URI is not well defined"));
                else
                    extraURI = MonitorUtils.substitute((String) o, template);
            }
            else
                throw(new IllegalArgumentException("URI is not well defined"));

            map.put("URI", extraURI);

            u = new URI(extraURI);

            if ("comm".equals(u.getScheme())) { // serialport
                device = u.getPath();
                extraURI = Event.getHostName() + ":" + device;

                map.put("Operation", "read");
                map.put("Mode", "daemon");
                map.put("XMode", "0");
                map.put("TextMode", "1");
                map.put("Partition", "0,0");
                map.put("EOTBytes", "0x0a");
                map.put("Offtail", "1");
                extraRcvr = new StreamReceiver(map);
                rcvrType = RCVR_COMM;
            }
            else if ("udp".equals(u.getScheme())) { // unicast
                InetAddress a = InetAddress.getByName(u.getHost());
                extraURI = a.getHostAddress() + ":" + u.getPort();
                extraRcvr = null;
                rcvrType = RCVR_UDP;
            }
            else if ("wmq".equals(u.getScheme())) { // jms
                map.put("Operation", "get");
                map.put("Mode", "daemon");
                map.put("XMode", "0");
                map.put("TextMode", "1");
                map.put("Partition", "0,0");

                o = map.get("MessageSelector");
                if (o == null) {
                    new Event(Event.WARNING, name + ": MessageSelector is not "+
                        "defined for ExtraReceiver").send();
                }
                else if (o instanceof String) {
                    map.put("MessageSelector",
                        MonitorUtils.substitute((String) o, template));
                }
                else if (o instanceof Map) {
                    o = ((Map) o).get(Event.getHostName());
                    if (o == null || !(o instanceof String))
                        new Event(Event.ERR, name + ": MessageSelector is not "+
                            "well defined for ExtraReceiver").send();
                    else
                        map.put("MessageSelector",
                            MonitorUtils.substitute((String) o, template));
                }
                else
                    new Event(Event.ERR, name + ": MessageSelector is not "+
                        "well defined for ExtraReceiver").send();

                extraRcvr = new JMSReceiver(map);
                extraURI = u.getHost() + ":" + u.getPort();
                rcvrType = RCVR_JMS;
            }
            else { // not supported
                extraRcvr = null;
                new Event(Event.ERR, name + ": scheme not supported"
                    + " for ExtraReceiver: " + u.getScheme()).send();
            }
        }
        catch (Exception e) {
            extraRcvr = null;
            rcvrType = RCVR_NONE;
            new Event(Event.ERR, name + ": ExtraReceiver failed to be "+
                "instantiated: " + Event.traceStack(e)).send();
        }
        if (rcvrType != RCVR_NONE) { // define root XQ
            hasExtra = true;
            if (16 * maxGroupSize <= 64)
                root = new IndexedXQueue(name, 64);
            else
                root = new IndexedXQueue(name, 16 * maxGroupSize);
        }
        else {
            if (8 * maxGroupSize <= 32)
                root = new IndexedXQueue(name, 32);
            else
                root = new IndexedXQueue(name, 8 * maxGroupSize);
        }

        masterURI = null;
        groupSize = 0;
        previousStatus = NODE_TIMEOUT;
        isInitialized = false;
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
        else if ("extra_rcvr".equals(threadName)) { // for extra rcvr
            uri = extraURI;
            new Event(Event.INFO, name + ": " + uri + " started").send();
            while (keepRunning(xq) && hasExtra) {
                try {
                    extraRcvr.receive(xq, 0);
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
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
            }
            extraRcvr.close();
        }

        new Event(Event.INFO, name + ": " + uri + " stopped").send();
    }

    public void close() {
        stopRunning(root);
        if (myBean != null && myBean.getStatus() != NODE_DOWN) {
            myBean.setStatus(NODE_DOWN);
            DatagramPacket packet;
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(System.currentTimeMillis() + " " + getNextSid());
            strBuf.append(" " + PacketNodeBean.MCG_QUIT);
            strBuf.append(" " + myBean.getRole());
            strBuf.append(" " + NODE_DOWN);
            strBuf.append(" " + myURI + " " + groupSize);
            packet = new DatagramPacket(strBuf.toString().getBytes(),
                0, strBuf.length(), address, port);
            myBean.setPacket(packet);
            send(packet);
            display(1, DEBUG_CLOSE, packet);
        }
        if (socket != null)
            socket.close();
        if (extraRcvr != null)
            extraRcvr.close();
    }

    private boolean keepRunning(XQueue xq) {
        if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0)
            return true;
        else
            return false;
    }

    private boolean isStopped(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        if ((xq.getGlobalMask() & mask) > 0)
            return false;
        else
            return true;
    }

    private void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    private void resumeRunning(XQueue xq) {
        int mask = XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.KEEP_RUNNING);
    }

    private void pause(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.PAUSE);
    }

    private void standby(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.STANDBY);
    }

    /**
     * It initializes the sockets and joins the group.
     */
    private void initialize() {
        Object o;
        String uri;
        XQueue xq;
        DatagramPacket packet;
        PacketNodeBean pBean;
        Browser browser;
        long[] hbInfo;
        long tm, currentTime, loopTime, sessionTime;
        int i, count, status, type, retry, cid;
        boolean isJMS = false;
        StringBuffer strBuf;

        sequenceNumber = 0L;
        sequenceId = 0L;
        groupSize = 0;
        count = 0;
        myBean = null;
        masterURI = null;
        if (nodeList.size() > 0) { // cleanup first
            browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                pBean = (PacketNodeBean) nodeList.get(i);
                if (pBean != null)
                    pBean.clear();
            }
            nodeList.clear();
            browser = null;
        }
        currentTime = System.currentTimeMillis();
        type = PacketNodeBean.MCG_HSHAKE;
        status = NODE_NEW;
        strBuf = new StringBuffer(currentTime + " " + getNextSid());
        strBuf.append(" " + type);
        strBuf.append(" -1 " + status);
        strBuf.append(" " + myURI + " " + groupSize);
        packet = new DatagramPacket(strBuf.toString().getBytes(),
            0, strBuf.length(), address, port);
        myBean = new PacketNodeBean();
        myBean.setPacket(packet);
        if (hasExtra) {
            myBean.setExtraURI(extraURI);
            if (rcvrType == RCVR_UDP) { // set extra unicast addess
                InetAddress a;
                if ((i = extraURI.indexOf(":")) > 0) try {
                    a = InetAddress.getByName(extraURI.substring(0, i));
                    i = Integer.parseInt(extraURI.substring(i+1));
                    myBean.setExtraAddress(a);
                    myBean.setExtraPort(i);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to get UDP address " +
                        "for " + extraURI).send();
                }
            }
        }
        send(packet);
        display(1, DEBUG_INIT, packet);
        hbInfo = new long[HB_ETIME+1];
        hbInfo[HB_SIZE] = (hasExtra) ? 2 : 1;
        hbInfo[HB_TYPE] = rcvrType;
        hbInfo[HB_PCOUNT] = 0;
        hbInfo[HB_ECOUNT] = 0;
        hbInfo[HB_PTIME] = currentTime;
        hbInfo[HB_ETIME] = currentTime;
        nodeList.add(myURI, hbInfo, myBean);

        xq = root;
        retry = 0;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        loopTime = currentTime;
        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((cid = xq.getNextCell(waitTime)) < 0) {
                currentTime += waitTime;
                if (currentTime - loopTime < heartbeat)
                    continue;

                currentTime = System.currentTimeMillis();
                loopTime = currentTime;
                if (currentTime - sessionTime < sessionTimeout) { // heartbeat
                    status = myBean.getStatus();
                    if (nodeList.size() >= maxGroupSize) // all on board
                        sessionTime = currentTime;
                    else if (status == NODE_NEW) { // HSHAKE
                        strBuf = new StringBuffer(currentTime + " " +
                            getNextSid());
                        strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                        strBuf.append(" -1 " + status);
                        strBuf.append(" " + myURI + " " + groupSize);
                        packet= new DatagramPacket(strBuf.toString().getBytes(),
                            0, strBuf.length(), myBean.getAddress(),
                            myBean.getPort());
                        myBean.setPacket(packet);
                        send(packet);
                        display(1, DEBUG_INIT, packet);
                        continue;
                    }
                    else // wait for more to come
                        continue;
                }

                currentTime = System.currentTimeMillis();
                sessionTime = currentTime;
                retry ++;
                new Event(Event.INFO, name + ": session timed out for " +
                    retry + " times on " + myURI).send();

                status = myBean.getStatus();
                packet = escalate(currentTime, status, retry);
                send(packet);
                display(1, DEBUG_INIT, packet);

                status = myBean.getStatus();
                if (status == NODE_RUNNING) {
                    isInitialized = true;
                    break;
                }
                if (retry > maxRetry)
                    break;
                else
                    continue;
            }

            if ((o = xq.browse(cid)) == null) { // null packet
                xq.remove(cid);
                new Event(Event.WARNING, name + ": null packet from "+
                    xq.getName()).send();
                currentTime = System.currentTimeMillis();
                if (currentTime - sessionTime < sessionTimeout)
                    continue;
                sessionTime = currentTime;
                retry ++;

                new Event(Event.INFO, name + ": session timed out for " +
                    retry + " times on " + myURI).send();
                packet = escalate(currentTime, myBean.getStatus(), retry);
                send(packet);
                display(1, DEBUG_INIT, packet);

                if (myBean.getStatus() == NODE_RUNNING) {
                    isInitialized = true;
                    break;
                }
                if (retry > maxRetry)
                    break;
                else
                    continue;
            }
            else if (o instanceof DatagramPacket) {
                InetAddress a = null;
                packet = (DatagramPacket) o;
                if ((a = packet.getAddress()) == null) { // relay packet
                    uri = groupURI;
                    isJMS = false;
                }
                else { // primary or extra packet
                    uri = a.getHostAddress() + ":" + packet.getPort();
                    if (rcvrType == RCVR_UDP && !nodeList.containsKey(uri)) {
                        String u = null, str = new String(packet.getData());
                        if ((i = str.indexOf(":")) > 0) { // retrieve uri
                            int j = str.indexOf(" ", i);
                            if (j > i && (i = str.lastIndexOf(" ", i)) > 0)
                                u = str.substring(i+1, j);
                        }
                        if (u == null) { // failed to retrieve uri from payload
                            xq.remove(cid);
                            new Event(Event.WARNING, name +
                                ": failed to retrieve uri from payload: " +
                                str).send();
                            continue;
                        }
                        else if (uri.equals(u)) // primary packet
                            isJMS = false;
                        else { // reset uri for extra packet
                            uri = u;
                            isJMS = true;
                        }
                    }
                    else // primary packet
                        isJMS = false;
                }
            }
            else try { // TextMessage
                String str = ((TextMessage) o).getText();
                packet = new DatagramPacket(str.getBytes(), 0, str.length());
                uri = null;
                if ((i = str.indexOf(":")) > 0) { //retrieve uri from payload
                    int j = str.indexOf(" ", i);
                    if (j > i && (i = str.lastIndexOf(" ", i)) > 0)
                        uri = str.substring(i+1, j);
                }
                if (uri == null) { // failed to retrieve uri from payload
                    xq.remove(cid);
                    new Event(Event.WARNING, name +
                        " failed to retrieve uri from payload: "+str).send();
                    continue;
                }
                isJMS = true;
            }
            catch (Exception e) {
                xq.remove(cid);
                new Event(Event.WARNING,name+" failed to get text from msg: "+
                    Event.traceStack(e)).send();
                continue;
            }

            if (myURI.equals(uri)) { // skip the packet sent by myself
                xq.remove(cid);
                continue;
            }

            if (!nodeList.containsKey(uri)) { // only process HSHAKE packets
                display(0, DEBUG_INIT, packet);
                pBean = new PacketNodeBean();
                if (isJMS) { // make sure address is set for 1st packet
                    InetAddress a = null;
                    int p = 0;
                    if ((i = uri.indexOf(":")) > 0) try {
                        a = InetAddress.getByName(uri.substring(0, i));
                        p = Integer.parseInt(uri.substring(i+1));
                    }
                    catch (Exception e) {
                    }
                    if (a != null && p > 0) { // set address and port
                        packet.setAddress(a);
                        packet.setPort(p);
                    }
                    pBean.setPacket(packet);
                    type = pBean.getPacketType();
                    if (type != PacketNodeBean.MCG_HSHAKE &&
                        type != PacketNodeBean.PTP_HSHAKE) // doing nothing
                        p = 0;
                    else if (rcvrType == RCVR_COMM)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_JMS)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_UDP) {
                        pBean.setExtraURI(extraURI);
                        pBean.setExtraAddress(myBean.getExtraAddress());
                        pBean.setExtraPort(myBean.getExtraPort());
                    }
                    else {
                        new Event(Event.WARNING, name +": failed to get " +
                            "extra uri for " + uri).send();
                    }
                }
                else { // for primary packet
                    pBean.setPacket(packet);
                    type = pBean.getPacketType();
                }
                if (type == PacketNodeBean.MCG_HSHAKE ||
                    type == PacketNodeBean.PTP_HSHAKE) {
                    hbInfo = new long[HB_ETIME+1];
                    hbInfo[HB_SIZE] = (hasExtra) ? 2 : 1;
                    hbInfo[HB_TYPE] = rcvrType;
                    hbInfo[HB_PCOUNT] = 0;
                    hbInfo[HB_ECOUNT] = 0;
                    hbInfo[HB_PTIME] = currentTime;
                    hbInfo[HB_ETIME] = currentTime;
                    i = nodeList.add(uri, hbInfo, pBean);
                    if (i < 0) { // failed to add a newbie
                        int role = pBean.getRole();
                        status = myBean.getStatus();
                        if (uri.compareTo(myURI) > 0) { // notify the node
                            // in case the other side does not know me
                            strBuf = new StringBuffer(currentTime + " " +
                                getNextSid());
                            strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                            strBuf.append(" " + myBean.getRole());
                            strBuf.append(" " + status);
                            strBuf.append(" " + myURI + " " + groupSize);
                            packet.setData(strBuf.toString().getBytes(),
                                0, strBuf.length());
                            send(packet);
                            display(1, DEBUG_INIT, packet);
                            strBuf = new StringBuffer(currentTime + " " +
                                getNextSid());
                            strBuf.append(" " + PacketNodeBean.PTP_UPDATE);
                            strBuf.append(" " +myBean.getRole()+ " " + status);
                            strBuf.append(" " + myURI);
                            strBuf.append(" " + ACTION_DEL + " " + role);
                            status = NODE_DOWN;
                            strBuf.append(" " + status + " " + uri);
                            packet.setData(strBuf.toString().getBytes(),
                                0, strBuf.length());
                            send(packet);
                            display(1, DEBUG_INIT, packet);
                        }
                        pBean.clear();
                        xq.remove(cid);
                        new Event(Event.WARNING, name + ": node of " +
                            role + " failed to be added: "+ uri).send();
                        if (status == NODE_DOWN)
                            continue;
                        else // leaving the group
                            break;
                    }
                }
                else { // illegal packet
                    xq.remove(cid);
                    pBean.clear();
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime < sessionTimeout)
                        continue;
                    sessionTime = currentTime;
                    retry ++;
                    new Event(Event.INFO, name + ": session timed out for " +
                        retry + " times on " + myURI).send();
                    packet = escalate(currentTime, myBean.getStatus(), retry);
                    send(packet);
                    display(1, DEBUG_INIT, packet);

                    if (myBean.getStatus() == NODE_RUNNING) {
                        new Event(Event.INFO, name + " " + myURI +
                            ": the master of group of " + groupSize + " is: " +
                            masterURI).send();
                        isInitialized = true;
                        break;
                    }
                    if (retry > maxRetry)
                        break;
                    else
                        continue;
                }
            }
            else { // packet from existing node
                pBean = (PacketNodeBean) nodeList.get(uri);
                hbInfo = nodeList.getMetaData(uri);
                if (!isJMS) // primary packet
                    hbInfo[HB_PTIME] = currentTime;
                else if (pBean.getExtraURI() != null) // already set
                    hbInfo[HB_ETIME] = currentTime;
                else { // set extra address and port
                    hbInfo[HB_ETIME] = currentTime;
                    if (rcvrType == RCVR_COMM)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_JMS)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_UDP) {
                        pBean.setExtraURI(extraURI);
                        pBean.setExtraAddress(myBean.getExtraAddress());
                        pBean.setExtraPort(myBean.getExtraPort());
                    }
                    else {
                        new Event(Event.WARNING, name +": failed to get " +
                            "extra uri for " + uri).send();
                    }
                }
                if (pBean.setPacket(packet) <= 0) { // already handled
                    xq.remove(cid);
                    display(2, DEBUG_HBEAT + DEBUG_HBEAT, packet);
                    continue;
                }
                type = pBean.getPacketType();
                display(0, DEBUG_INIT, packet);
            }

            // reset object to null for a new packet next turn
            xq.remove(cid);

            if (type == PacketNodeBean.PTP_HBEAT ||
                type == PacketNodeBean.MCG_HBEAT) { // skip HBEAT packets
                currentTime = System.currentTimeMillis();
                if (currentTime - sessionTime < sessionTimeout)
                    continue;
                sessionTime = currentTime;
                retry ++;
                new Event(Event.INFO, name + ": session timed out for " +
                    retry + " times on " + myURI).send();
                status = myBean.getStatus();
                packet = escalate(currentTime, status, retry);
                send(packet);
                display(1, DEBUG_INIT, packet);

                if (myBean.getStatus() == NODE_RUNNING) {
                    new Event(Event.INFO, name + " " + myURI +
                        ": the master of group of " + groupSize + " is: " +
                        masterURI).send();
                    isInitialized = true;
                    break;
                }
                continue;
            }

            currentTime = System.currentTimeMillis();
            packet = respond(currentTime, type, pBean);
            send(packet);
            display(1, DEBUG_INIT, packet);

            status = myBean.getStatus();
            if (status == NODE_RUNNING) { // done
                new Event(Event.INFO, name + " " + myURI +
                    ": the master of group of " + groupSize + " is: " +
                    masterURI).send();
                isInitialized = true;
                break;
            }
            else if (status == NODE_STANDBY && status != previousStatus) {
                // first HSHAKE from master
                sessionTime = currentTime;
                previousStatus = status;
            }
            else if (pBean.getRole() == 0 && packet != null &&
                myBean.getPacketType() == PacketNodeBean.MCG_HSHAKE &&
                myBean.getPacketInfo(PacketNodeBean.MCG_HSHAKE,
                PacketNodeBean.PKT_COUNT) == retry + 2) {
                sessionTime = currentTime;
            }
        }

        if (!isInitialized) {
            new Event(Event.WARNING, name + ": initialization failed on: " +
                myURI).send();
            if (out != null) { // notify the apps
                event = new Event(Event.WARNING, myURI +
                    " failed to join the group");
                event.setAttribute("status", String.valueOf(NODE_NEW));
                event.setAttribute("role", "-1");
                event.setAttribute("size", String.valueOf(groupSize));
                event.setAttribute("uri", myURI);
                event.setAttribute("operation", "remove");
                event.setAttribute("action", String.valueOf(ACTION_DEL));
                passthru(currentTime, event, out);
                event = null;
            }
            close();
        }
    }

    /** sends the packet to all destinations */
    private void send(DatagramPacket packet) {
        byte[] buffer;
        if (packet == null)
            return;
        buffer = packet.getData();
        try {
            socket.send(packet);
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": failed to send packet: " +
                Event.traceStack(e)).send();
        }
        packet.setData(buffer, 0, buffer.length);
        if (hasExtra) try {
            int i = 0;
            DatagramPacket p = null;
            switch (rcvrType) { // try to send to the extra destination
              case RCVR_COMM:
                i = ((StreamReceiver) extraRcvr).send(new String(buffer)+"\n");
                if (i < 0)
                    new Event(Event.WARNING, name + 
                        ": failed to send packet to " +extraURI+ ": "+i).send();
                break;
              case RCVR_UDP:
                p = new DatagramPacket(buffer, 0, buffer.length,
                    myBean.getExtraAddress(), myBean.getExtraPort());
                socket.send(p);
                break;
              case RCVR_JMS:
                ((JMSReceiver) extraRcvr).send(new String(buffer),
                    1, -1, 1000L, Event.getHostName());
                break;
              default:
                break;
            }
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": failed to send extra packet: " +
                Event.traceStack(e)).send();
        }
    }

    /**
     * It is the API for application to send data to the node or group
     * asynchronously.  It returns a non-negative number for success,
     * or -1 otherwise.
     */
    public int relay(Event data, int waitTime) {
        int cid = -1, myRole, myStatus;
        DatagramPacket packet;
        StringBuffer strBuf;
        String uri;
        if (data == null)
            return -5;
        myRole = myBean.getRole();
        myStatus = myBean.getStatus();
        if (myRole < 0 || myStatus != NODE_RUNNING)
            return -4;
        if (myRole > 0) { // send data to the master directly
            PacketNodeBean pBean = (PacketNodeBean) nodeList.get(masterURI);
            if (masterURI == null || pBean == null) {
                new Event(Event.ERR, name + ": failed to relay the data " +
                    "to master: " + masterURI).send();
                return -2;
            }
            strBuf = new StringBuffer(data.getTimestamp() + " "+getNextSid());
            strBuf.append(" " + PacketNodeBean.PTP_DATA + " ");
            strBuf.append(myRole + " " + myStatus);
            strBuf.append(" " + myURI + " " + data.getAttribute("text"));
            packet = new DatagramPacket(strBuf.toString().getBytes(),
                0, strBuf.length(), pBean.getAddress(), pBean.getPort());
            send(packet);
            display(1, DEBUG_EXAM, packet);
            return 0;
        }
        else if ("failover".equals(data.getAttribute("operation"))) { //failover
            uri = data.getAttribute("master");
            if (uri == null) { // nominate an active node as the new master
                Browser browser;
                PacketNodeBean pBean;
                int i, n = 0;
                browser = nodeList.browser();
                while ((i = browser.next()) >= 0) { // search for first uri
                    pBean = (PacketNodeBean) nodeList.get(i);
                    if (pBean != null && !myURI.equals(nodeList.getKey(i)) &&
                        pBean.getStatus() == NODE_RUNNING) {
                        uri = nodeList.getKey(i);
                        break;
                    }
                }
            }
            if (uri == null || !nodeList.containsKey(uri)) {
                new Event(Event.ERR, name + ": failover aborted since " +
                    "no active node is available").send();
                return -3;
            }

            strBuf = new StringBuffer(data.getTimestamp() + " " +getNextSid());
            strBuf.append(" " + PacketNodeBean.MCG_QUIT + " ");
            strBuf.append(myRole + " " + NODE_STANDBY);
            strBuf.append(" " + myURI + " " + uri);
        }
        else { // data
            strBuf = new StringBuffer(data.getTimestamp() + " "+getNextSid());
            strBuf.append(" " + PacketNodeBean.MCG_DATA + " ");
            strBuf.append(myRole + " " + myStatus);
            strBuf.append(" " + myURI + " " + data.getAttribute("text"));
        }
        packet = new DatagramPacket(strBuf.toString().getBytes(),
            0, strBuf.length());
        if (waitTime <= 0)
            waitTime = this.waitTime;

        for (int i=0; i<3; i++) { // three retries
            cid = root.reserve(waitTime);
            if (cid >= 0) {
                cid = root.add(packet, cid);
                break;
            }
        }

        return cid;
    }

    /**
     * It examines each incoming packet and takes actions upon
     * everyone of them.
     */
    private void examine() {
        Object o;
        String uri;
        XQueue xq;
        DatagramPacket packet;
        PacketNodeBean pBean;
        long[] hbInfo;
        long loopTime, tm, currentTime, sessionTime;
        int i, ic, m, status, type, retry, cid;
        boolean isJMS = false;
        StringBuffer strBuf;
        type = PacketNodeBean.PTP_HBEAT;

        xq = root;
        retry = 0;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        loopTime = sessionTime;
        status = myBean.getStatus();
        if (status == NODE_RUNNING) { // send first heartbeat
            packet = escalate(currentTime, status, 0);
            send(packet);
            display(1, DEBUG_HBEAT, packet);
        }

        if (out != null) { // notify the apps
            event = new Event(Event.NOTICE, myURI + " joined the group");
            event.setAttribute("status", String.valueOf(status));
            event.setAttribute("role", String.valueOf(myBean.getRole()));
            event.setAttribute("size", String.valueOf(groupSize));
            event.setAttribute("uri", myURI);
            event.setAttribute("operation", "create");
            event.setAttribute("action", String.valueOf(ACTION_ADD));
            passthru(currentTime, event, out);
            event = null;
        }

        ic = 0;
        m = sessionTimeout / heartbeat;
        while (keepRunning(xq)) {
            if ((cid = xq.getNextCell(waitTime)) < 0) {
                currentTime += waitTime;
                if (currentTime - loopTime < heartbeat)
                    continue;

                currentTime = System.currentTimeMillis();
                loopTime = currentTime;
                status = myBean.getStatus();
                if (status == NODE_RUNNING) { // send heartbeat
                    packet = escalate(currentTime, status, 0);
                    send(packet);
                    display(1, DEBUG_HBEAT, packet);
                }

                if (++ic >= m) { // session timed out
                    ic = 0;
                    if (myBean.getRole() == 0) { // for master
                        if (groupSize > 1)
                            check(currentTime, 0, status);
                    }
                    else if (hasExtra) { // for non-master
                        monitor(currentTime, 0, masterURI,
                            nodeList.getMetaData(masterURI));
                    }
                }

                if (currentTime - sessionTime < sessionTimeout)
                    continue;

                sessionTime = currentTime;
                if (myBean.getRole() == 0) // for master only
                    continue;

                retry ++;
                new Event(Event.INFO, name + ": session timed out for " +
                    retry + " times on " + myURI).send();
                status = myBean.getStatus();
                packet = escalate(currentTime, status, retry);
                send(packet);
                display(1, DEBUG_EXAM, packet);
                if (out != null && event != null) { // notify the apps
                    passthru(currentTime, event, out);
                    event = null;
                }
                continue;
            }

            if ((o = xq.browse(cid)) == null) { // null packet
                xq.remove(cid);
                new Event(Event.WARNING, name + ": null packet from "+
                    xq.getName()).send();
                currentTime = System.currentTimeMillis();
                if (currentTime - loopTime < heartbeat)
                    continue;
                loopTime = currentTime;
                status = myBean.getStatus();
                if (status == NODE_RUNNING) { // heartbeat
                    packet = escalate(currentTime, status, 0);
                    send(packet);
                    display(1, DEBUG_HBEAT, packet);
                }
                continue;
            }
            else if (o instanceof DatagramPacket) { // packet
                InetAddress a = null;
                packet = (DatagramPacket) o;
                if ((a = packet.getAddress()) == null) { // relay packet
                    uri = groupURI;
                    isJMS = false;
                }
                else { // primary or extra packet
                    uri = a.getHostAddress() + ":" + packet.getPort();
                    if (rcvrType == RCVR_UDP && !nodeList.containsKey(uri)) {
                        String u = null, str = new String(packet.getData());
                        if ((i = str.indexOf(":")) > 0) { // retrieve uri
                            int j = str.indexOf(" ", i);
                            if (j > i && (i = str.lastIndexOf(" ", i)) > 0)
                                u = str.substring(i+1, j);
                        }
                        if (u == null) { // failed to retrieve uri from payload
                            xq.remove(cid);
                            new Event(Event.WARNING, name +
                                ": failed to retrieve uri from payload: " +
                                str).send();
                            continue;
                        }
                        else if (uri.equals(u)) // primary packet
                            isJMS = false;
                        else { // reset uri for the extra packet
                            uri = u;
                            isJMS = true;
                        }
                    }
                    else // primary packet
                        isJMS = false;
                }
            }
            else try { // TextMessage
                String str = ((TextMessage) o).getText();
                packet = new DatagramPacket(str.getBytes(), 0, str.length());
                uri = null;
                if ((i = str.indexOf(":")) > 0) { //retrieve uri from payload
                    int j = str.indexOf(" ", i);
                    if (j > i && (i = str.lastIndexOf(" ", i)) > 0)
                        uri = str.substring(i+1, j);
                }
                if (uri == null) { // failed to retrieve uri from payload
                    xq.remove(cid);
                    new Event(Event.WARNING, name +
                        " failed to retrieve uri from payload: "+str).send();
                    continue;
                }
                isJMS = true;
            }
            catch (Exception e) {
                xq.remove(cid);
                new Event(Event.WARNING,name+" failed to get text from msg: "+
                    Event.traceStack(e)).send();
                continue;
            }

            if (myURI.equals(uri)) { // skip packet sent by myself
                xq.remove(cid);
                continue;
            }

            if (!nodeList.containsKey(uri)) { // HSHAKE or relay packet
                int role;
                if (groupURI.equals(uri)) { // outgoing packet to relay
                    display(0, DEBUG_EXAM, packet);
                    xq.remove(cid);
                    status = myBean.getStatus();
                    role = myBean.getRole();
                    if (status == NODE_RUNNING) {
                        myBean.setPacket(packet);
                        type = myBean.getPacketType();
                        if (type == PacketNodeBean.MCG_QUIT) { // master resigns
                            status = myBean.getStatus();
                            masterURI = myBean.getContent(type);
                            pBean = (PacketNodeBean) nodeList.get(masterURI);
                            if (pBean != null) { // swap the role for new master
                                i = pBean.getRole();
                                pBean.setRole(role);
                                role = i;
                                myBean.setRole(role);
                                new Event(Event.INFO, name +
                                    ": I am resigned and nominated " +
                                    "the new master: " + masterURI).send();
                            }
                            else { // roll back changes
                                new Event(Event.ERR, name +
                                    ": failover aborted on "+masterURI).send();
                                myBean.setStatus(NODE_RUNNING);
                                myBean.setRole(role);
                                masterURI = myURI;
                                continue;
                            }
                            loopTime = System.currentTimeMillis();
                            sessionTime = loopTime;
                            retry = 0;
                        }
                        send(packet);
                        display(1, DEBUG_RELAY, packet);
                    }
                    currentTime = System.currentTimeMillis();
                    if (currentTime - loopTime < heartbeat)
                        continue;
                    loopTime = currentTime;
                    if (role == 0 && status == NODE_RUNNING) {
                        packet = escalate(currentTime, status, 0);
                        send(packet);
                        display(1, DEBUG_HBEAT, packet);
                    }
                    continue;
                }
                pBean = new PacketNodeBean();
                if (isJMS) { // make sure address is set for 1st packet
                    InetAddress a = null;
                    int p = 0;
                    if ((i = uri.indexOf(":")) > 0) try {
                        a = InetAddress.getByName(uri.substring(0, i));
                        p = Integer.parseInt(uri.substring(i+1));
                    }
                    catch (Exception e) {
                    }
                    if (a != null && p > 0) { // set address and port
                        packet.setAddress(a);
                        packet.setPort(p);
                    }
                    pBean.setPacket(packet);
                    type = pBean.getPacketType();
                    if (type != PacketNodeBean.MCG_HSHAKE &&
                        type != PacketNodeBean.PTP_HSHAKE) // doing nothing
                        p = 0;
                    else if (rcvrType == RCVR_COMM)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_JMS)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_UDP) {
                        pBean.setExtraURI(extraURI);
                        pBean.setExtraAddress(myBean.getExtraAddress());
                        pBean.setExtraPort(myBean.getExtraPort());
                    }
                    else {
                        new Event(Event.WARNING, name +": failed to get " +
                            "extra uri for " + uri).send();
                    }
                }
                else { // for primary packet
                    pBean.setPacket(packet);
                    type = pBean.getPacketType();
                }
                if (type == PacketNodeBean.MCG_HSHAKE ||
                    type == PacketNodeBean.PTP_HSHAKE) { // HSHAKE packet
                    display(0, DEBUG_EXAM, packet);
                    hbInfo = new long[HB_ETIME+1];
                    hbInfo[HB_SIZE] = (hasExtra) ? 2 : 1;
                    hbInfo[HB_TYPE] = rcvrType;
                    hbInfo[HB_PCOUNT] = 0;
                    hbInfo[HB_ECOUNT] = 0;
                    hbInfo[HB_PTIME] = currentTime;
                    hbInfo[HB_ETIME] = currentTime;
                    i = nodeList.add(uri, hbInfo, pBean);
                    if (i < 0) { // failed to add a newbie
                        role = pBean.getRole();
                        status = myBean.getStatus();
                        if (myBean.getRole() == 0) { // I am the master
                            // in case the other side does not know me
                            strBuf = new StringBuffer(currentTime + " " +
                                getNextSid());
                            strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                            strBuf.append(" " + myBean.getRole());
                            strBuf.append(" " + status);
                            strBuf.append(" " + myURI + " " + groupSize);
                            packet.setData(strBuf.toString().getBytes(),
                                0, strBuf.length());
                            send(packet);
                            display(1, DEBUG_EXAM, packet);
                            strBuf = new StringBuffer(currentTime + " " +
                                getNextSid());
                            strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                            strBuf.append(" " + 0 + " " + status);
                            strBuf.append(" " + myURI);
                            strBuf.append(" " + ACTION_DEL + " " + role);
                            status = NODE_DOWN;
                            strBuf.append(" " + status + " " + uri);
                            packet.setData(strBuf.toString().getBytes(),
                                0, strBuf.length());
                            send(packet);
                            display(1, DEBUG_EXAM, packet);
                        }
                        pBean.clear();
                        xq.remove(cid);
                        new Event(Event.WARNING, name + ": node of " +
                            role + " failed to be added: "+ uri).send();
                        continue;
                    }
                }
                else { // illegal packet
                    if (myBean.getRole()==0 && type==PacketNodeBean.MCG_HBEAT) {
                        display(0, DEBUG_ALL, packet);
                        // there are more than one master
                        if (sequenceNumber < pBean.getSid() ||
                            (sequenceNumber == pBean.getSid() &&
                            myURI.compareTo(uri) > 0)) { // I am out of group 
                            new Event(Event.WARNING, name + " " + myURI +
                                ": trying to rejoin group of " + uri).send();
                            if (out != null) { // notify the apps for leaving
                                event = new Event(Event.NOTICE, myURI +
                                    " is trying to rejoin the group");
                                event.setAttribute("status",
                                    String.valueOf(NODE_NEW));
                                event.setAttribute("role", "-1");
                                event.setAttribute("size", "0");
                                event.setAttribute("uri", myURI);
                                event.setAttribute("operation", "update");
                                event.setAttribute("action",
                                    String.valueOf(ACTION_MOD));
                                passthru(currentTime, event, out);
                                event = null;
                            }
                            retry = 0;
                            previousStatus = NODE_TIMEOUT;
                            isInitialized = false;
                            initialize();

                            currentTime = System.currentTimeMillis();
                            status = myBean.getStatus();
                            // send first heartbeat after rejoining the group
                            if (status == NODE_RUNNING) {
                                packet = escalate(currentTime, status, 0);
                                send(packet);
                                display(1, DEBUG_HBEAT, packet);
                            }
                            else {
                                new Event(Event.WARNING, name + " " + myURI +
                                    ": failed to rejoin group of "+uri).send();
                                continue;
                            }

                            if (out != null) { // notify the apps for joining
                                event = new Event(Event.NOTICE, myURI +
                                    " rejoined the group");
                                event.setAttribute("status",
                                    String.valueOf(status));
                                event.setAttribute("role",
                                    String.valueOf(myBean.getRole()));
                                event.setAttribute("size",
                                    String.valueOf(groupSize));
                                event.setAttribute("uri", myURI);
                                event.setAttribute("operation", "create");
                                event.setAttribute("action",
                                    String.valueOf(ACTION_ADD));
                                passthru(currentTime, event, out);
                                event = null;
                            }

                            continue;
                        }
                        new Event(Event.ERR, name + " " + myURI +
                            ": more than one master of " + uri + ": " +
                            packet.getData()).send();
                    }
                    else if (type == PacketNodeBean.MCG_HBEAT ||
                        type == PacketNodeBean.PTP_HBEAT) // late heartbeat
                        display(2, DEBUG_HBEAT, packet);
                    else // lost packet
                        display(2, DEBUG_EXAM, packet);

                    // reset object to null for a new packet next turn
                    xq.remove(cid);
                    pBean.clear();
                    currentTime = System.currentTimeMillis();
                    if (currentTime - loopTime < heartbeat)
                        continue;
                    loopTime = currentTime;
                    status = myBean.getStatus();
                    if (status == NODE_RUNNING) { // heartbeat
                        packet = escalate(currentTime, status, 0);
                        send(packet);
                        display(1, DEBUG_HBEAT, packet);
                    }
                    continue;
                }
            }
            else { // packet from existing node
                pBean =(PacketNodeBean) nodeList.get(uri);
                hbInfo = nodeList.getMetaData(uri);
                if (!isJMS)
                    hbInfo[HB_PTIME] = currentTime;
                else if (pBean.getExtraURI() != null)
                    hbInfo[HB_ETIME] = currentTime;
                else { // set extra address and port
                    hbInfo[HB_ETIME] = currentTime;
                    if (rcvrType == RCVR_COMM)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_JMS)
                        pBean.setExtraURI(extraURI);
                    else if (rcvrType == RCVR_UDP) {
                        pBean.setExtraURI(extraURI);
                        pBean.setExtraAddress(myBean.getExtraAddress());
                        pBean.setExtraPort(myBean.getExtraPort());
                    }
                    else {
                        new Event(Event.WARNING, name +": failed to get " +
                            "extra uri for " + uri).send();
                    }
                }
                if (pBean.setPacket(packet) <= 0) { // already handled
                    xq.remove(cid);
                    display(2, DEBUG_HBEAT + DEBUG_HBEAT, packet);
                    continue;
                }
                type = pBean.getPacketType();
                if (type == PacketNodeBean.MCG_HBEAT ||
                    type == PacketNodeBean.PTP_HBEAT) // heartbeat
                    display(0, DEBUG_HBEAT, packet);
                else
                    display(0, DEBUG_EXAM, packet);
            }

            // reset object to null for a new packet next turn
            xq.remove(cid);

            if (type == PacketNodeBean.PTP_HBEAT) { // HBEAT from non-masters
                currentTime = System.currentTimeMillis();
                if (currentTime - loopTime < heartbeat)
                    continue;
                loopTime = currentTime;
                status = myBean.getStatus();
                if (status == NODE_RUNNING) { // heartbeat
                    packet = escalate(currentTime, status, 0);
                    send(packet);
                    display(1, DEBUG_HBEAT, packet);
                }

                if (++ic >= m) { // session timed out
                    ic = 0;
                    if (groupSize > 1)
                        check(currentTime, 0, status);
                }
                if (currentTime - sessionTime < sessionTimeout)
                    continue;
                sessionTime = currentTime;
                continue;
            }
            else if (type == PacketNodeBean.MCG_HBEAT) { // HBEAT from master
                currentTime = System.currentTimeMillis();
                sessionTime = currentTime;
                if (currentTime - loopTime < heartbeat)
                    continue;
                loopTime = currentTime;
                status = myBean.getStatus();
                if (status == NODE_RUNNING) { // heartbeat
                    packet = escalate(currentTime, status, 0);
                    send(packet);
                    display(1, DEBUG_HBEAT, packet);
                    if (retry > 0) // reset the retry count
                        retry = 0;
                }
                if (++ic >= m) { // session timed out
                    ic = 0;
                    if (hasExtra) { // for non-master
                        monitor(currentTime, 0, masterURI,
                            nodeList.getMetaData(masterURI));
                    }
                }
                continue;
            }

            currentTime = System.currentTimeMillis();
            packet = respond(currentTime, type, pBean);
            send(packet);
            display(1, DEBUG_EXAM, packet);
            if (out != null && event != null) { // notify the apps
                passthru(currentTime, event, out);
                event = null;
            }
            if (!keepRunning(xq))
                 break;

            status = myBean.getStatus();
            if (status == NODE_RUNNING && // resume running and heartbeating
                (currentTime-loopTime >= heartbeat || previousStatus !=status)){
                currentTime = System.currentTimeMillis();
                loopTime = currentTime;
                if (previousStatus != status) { // resume running
                    sessionTime = currentTime;
                    retry = 0;
                }
                packet = escalate(currentTime, status, 0);
                send(packet);
                display(1, DEBUG_HBEAT, packet);
                previousStatus = status;
            }
            else if (status != previousStatus) {
                sessionTime = System.currentTimeMillis();
                previousStatus = status;
            }

            if (currentTime - sessionTime >= sessionTimeout) { // timeout
                currentTime = System.currentTimeMillis();
                status = myBean.getStatus();
                sessionTime = currentTime;
                if (myBean.getRole() == 0) { // for master
                    if (groupSize > 1)
                        check(currentTime, 0, status);
                    continue;
                }
                else if (hasExtra) { // for non-master
                    monitor(currentTime, 0, masterURI,
                        nodeList.getMetaData(masterURI));
                }

                retry ++;
                new Event(Event.INFO, name + ": session timed out for: " +
                    retry + " times on " + myURI).send();
                status = myBean.getStatus();
                packet = escalate(currentTime, status, retry);
                send(packet);
                display(1, DEBUG_EXAM, packet);
                if (out != null && event != null) { // notify the apps
                    passthru(currentTime, event, out);
                    event = null;
                }
            }
        }
    }

    /**
     * It displays packets for troubleshooting and debugging.<br>
     * option: 0 - displaying incoming packets with tag of a space<br>
     * option: 1 - displaying outgoing packets with tag of a colon<br>
     * option: 2 - displaying discarded incoming packets with tag of a bam<br>
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

    /**
     * It resets the refCount of all nodes into zero.
     */
    private void resetAllRefCount() {
        int i;
        PacketNodeBean pBean;
        Browser browser = nodeList.browser();

        while ((i = browser.next()) >= 0) {
            pBean = (PacketNodeBean) nodeList.get(i);
            if (pBean != null)
                pBean.setRefCount(0);
        }
    }

    /**
     * returns the sum of the refCount on all active nodes.
     */
    private int sumRefCount() {
        int i, n, status;
        PacketNodeBean pBean;
        Browser browser = nodeList.browser();

        n = 0;
        while ((i = browser.next()) >= 0) {
            pBean = (PacketNodeBean) nodeList.get(i);
            if (pBean == null)
                continue;
            status = pBean.getStatus();
            if (status == NODE_RUNNING || status == NODE_STANDBY) {
                n += pBean.getRefCount();
            }
        }
        return n;
    }

    /**
     * returns the number of active nodes, ie, the current groupSize.
     */
    private int countActiveNodes() {
        int i, n, status;
        PacketNodeBean pBean;
        Browser browser = nodeList.browser();

        n = 0;
        while ((i = browser.next()) >= 0) {
            pBean = (PacketNodeBean) nodeList.get(i);
            if (pBean == null)
                continue;
            status = pBean.getStatus();
            if (status == NODE_RUNNING || status == NODE_STANDBY)
                n ++;
        }
        return n;
    }

    /**
     * returns the first recycled role or the current groupSize.
     */
    private int getFirstRecycledRole() {
        int i, n, r;
        int[] role;
        PacketNodeBean pBean;
        Browser browser = nodeList.browser();
        if (myBean.getRole() != 0 || groupSize == 0)
            return -1;

        role = new int[groupSize];

        n = 0;
        while ((i = browser.next()) >= 0) {
            pBean = (PacketNodeBean) nodeList.get(i);
            if (pBean == null)
                continue;
            if ((r = pBean.getRole()) >= 0)
                role[n++] = r;
        }

        if (n == 0)
            return -1;

        for (i=n; i<groupSize; i++)
            role[i] = groupSize;

        Arrays.sort(role);

        for (i=0; i<groupSize; i++)
            if (role[i] > i)
                break;

        return i;
    }

    /**
     * returns the most popular URI among all the nodes for election.
     */
    private String getFavoriteURI() {
        int i;
        String str, uri = null;
        Browser browser = nodeList.browser();

        if (groupSize > 0) {
            PacketNodeBean pBean;
            int n = 0, status;
            while ((i = browser.next()) >= 0) { // search for lowest uri
                pBean = (PacketNodeBean) nodeList.get(i);
                if (pBean == null)
                    continue;
                status = pBean.getStatus();
                if (status == NODE_RUNNING || status == NODE_STANDBY) {
                    if (n > pBean.getRefCount())
                        continue;
                    n = pBean.getRefCount();
                    str = nodeList.getKey(i);
                    if (uri == null)
                        uri = str;
                    else if (uri.compareTo(str) > 0)
                        uri = str;
                }
            }
        }
        else {
            uri = myURI;
            while ((i = browser.next()) >= 0) { // search for lowest uri
                str = nodeList.getKey(i);
                if (uri.compareTo(str) > 0)
                    uri = str;
            }
        }

        return uri;
    }

    /**
     * It monitors timestamps on both receivers and updates the heartbeat info.
     * If any one of them is late,  it logs a warning.  It returns number of
     * late receivers. 
     */
    private int monitor(long currentTime, int role, String uri,
        long[] hbInfo) {
        String str;
        int i, k, pcount, ecount;
        if (role < 0 || hbInfo == null || hbInfo.length <= HB_ETIME)
            return -1;

        i = 0;
        pcount = (int) hbInfo[HB_PCOUNT];
        if (currentTime - hbInfo[HB_PTIME] >= sessionTimeout)
            hbInfo[HB_PCOUNT] ++;
        else if (hbInfo[HB_PCOUNT] > 0)
            hbInfo[HB_PCOUNT] = 0;

        ecount = (int) hbInfo[HB_ECOUNT];
        if (currentTime - hbInfo[HB_ETIME] >= sessionTimeout)
            hbInfo[HB_ECOUNT] ++;
        else if (hbInfo[HB_ECOUNT] > 0)
            hbInfo[HB_ECOUNT] = 0;

        if (hbInfo[HB_PCOUNT] > 0) {
            str = "missed heartbeats from node of " + role;
            if (hbInfo[HB_ECOUNT] > 0) {
                str += " on both receivers: " + uri;
                k = (hbInfo[HB_PCOUNT] < hbInfo[HB_ECOUNT]) ?
                    (int) (hbInfo[HB_PCOUNT] % repeatPeriod) :
                    (int) (hbInfo[HB_ECOUNT] % repeatPeriod);
                i = 2;
            }
            else {
                str += " on primary: " + uri;
                k = (int) (hbInfo[HB_PCOUNT] % repeatPeriod);
                i = 1;
            }
            if (k >= maxRetry)
                return i;
            else if (k > 1)
                new Event(Event.ERR, name + ": " + str).send();
            else
                new Event(Event.WARNING, name + ": " + str).send();
        }
        else if (hbInfo[HB_ECOUNT] > 0) {
            str = "missed heartbeats from node of " + role +
                " on secondary: " + extraURI;
            i = 1;
            k = (int) (hbInfo[HB_ECOUNT] % repeatPeriod);
            if (k >= maxRetry)
                return i;
            if (i > 1)
                new Event(Event.ERR, name + ": " + str).send();
            else
                new Event(Event.WARNING, name + ": " + str).send();
        }
        else if (pcount > 0 && ecount > 0) {
            str = "recovered heartbeats from node of " + role +
                " on both receivers: " + uri;
            new Event(Event.INFO, name + ": " + str).send();
        }
        else if (pcount > 0) {
            str = "recovered heartbeats from node of " + role +
                " on primary: " + uri;
            new Event(Event.INFO, name + ": " + str).send();
        }
        else if (ecount > 0) {
            str = "recovered heartbeats from node of " + role +
                " on secondary: " + extraURI;
            new Event(Event.INFO, name + ": " + str).send();
        }
        return i;
    }

    /**
     * The master checks heartbeat of all nodes to see any one is late.  If it
     * finds one, it notifies first and then removes the node from the group.
     */
    private int check(long currentTime, int myRole, int myStatus) {
        String uri = null;
        StringBuffer strBuf;
        DatagramPacket packet;
        PacketNodeBean pBean;
        Browser browser;
        int i, len, n = 0, status, role;
        long tm;

        if (groupSize <= 1 || myRole != 0)
            return 0;

        browser = nodeList.browser();
        n = 0;
        while ((i = browser.next()) >= 0) { // search for all nodes
            pBean = (PacketNodeBean) nodeList.get(i);
            if (pBean == null)
                continue;
            role = pBean.getRole();
            if (role <= 0) // skip non-active nodes or master
                continue;

            status = pBean.getStatus();
            uri = nodeList.getKey(i);
            tm = pBean.getPacketInfo(PacketNodeBean.PTP_HBEAT,
                PacketNodeBean.PKT_TIME);

            if (hasExtra) // check both receivers
                monitor(currentTime, role, uri, nodeList.getMetaData(i));

            switch (status) {
              case NODE_RUNNING:
                if (currentTime - tm < sessionTimeout)
                    break;
                status = NODE_TIMEOUT;
                pBean.setStatus(status);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_NOTICE);
                strBuf.append(" 0 " + myStatus + " " + myURI);
                strBuf.append(" " + ACTION_CHK);
                strBuf.append(" " + (int) (currentTime - tm));
                strBuf.append(" " + status + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                send(packet);
                display(1, DEBUG_CHECK, packet);
                n ++;
                new Event(Event.WARNING, name + ": node of " + role +
                    " timed out at " + uri).send();
                break;
              case NODE_STANDBY:
                break;
              case NODE_TIMEOUT:
                if (currentTime - tm < sessionTimeout + sessionTimeout) {
                    strBuf = new StringBuffer(currentTime + " " + getNextSid());
                    strBuf.append(" " + PacketNodeBean.PTP_NOTICE);
                    strBuf.append(" 0 " + myStatus + " " + myURI);
                    strBuf.append(" " + ACTION_CHK);
                    strBuf.append(" " + (int) (currentTime - tm));
                    strBuf.append(" " + status + " " + uri);
                    packet = new DatagramPacket(strBuf.toString().getBytes(),
                        0,strBuf.length(), pBean.getAddress(), pBean.getPort());
                    send(packet);
                    n ++;
                    display(1, DEBUG_CHECK, packet);
                    break;
                }
                status = NODE_DOWN;
                pBean.setStatus(status);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" 0 " + myStatus + " " + myURI);
                strBuf.append(" " + ACTION_DEL);
                strBuf.append(" " + role);
                strBuf.append(" " + status + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                send(packet);
                display(1, DEBUG_CHECK, packet);
                nodeList.remove(uri);
                pBean.clear();
                pBean = null;
                // removing a node which used to be active
                groupSize --;
                n ++;
                new Event(Event.WARNING, name + ": node of " + role +
                    " removed due to timeout: " + uri).send();
                if (out != null) { // notify the apps
                    event = new Event(Event.NOTICE, uri + " timed out");
                    event.setAttribute("status", String.valueOf(status));
                    event.setAttribute("role", String.valueOf(role));
                    event.setAttribute("size", String.valueOf(groupSize));
                    event.setAttribute("uri", uri);
                    event.setAttribute("operation", "remove");
                    event.setAttribute("action", String.valueOf(ACTION_DEL));
                    passthru(currentTime, event, out);
                    event = null;
                }
                break;
              default:
            }
        }

        return n;
    }

    /**
     * It initiates an election process for the new master and casts the vote,
     * or declares itself as the new master if it is the only one in the group.
     *<br><br>
     * NB. a node can only vote once in the same election
     */
    private DatagramPacket vote(long currentTime, String u) {
        DatagramPacket packet = null;
        PacketNodeBean pBean;
        StringBuffer strBuf;
        int myRole, myStatus;
        String uri;

        // master is gone
        myRole = myBean.getRole();
        myStatus = myBean.getStatus();
        strBuf = new StringBuffer(currentTime + " " + getNextSid());
        if (groupSize > 1) {
            myStatus = NODE_STANDBY;
            // cast the first vote
            resetAllRefCount();
            uri = getFavoriteURI();
            pBean = (PacketNodeBean) nodeList.get(uri);
            pBean.setRefCount(1);
            if (u != null && uri.equals(u)) {
                pBean.setRefCount(2);
            }
            else if (u != null) {
                pBean = (PacketNodeBean) nodeList.get(uri);
                pBean.setRefCount(1);
            }
            strBuf.append(" " + PacketNodeBean.MCG_NOTICE);
            strBuf.append(" " + myRole + " " + myStatus);
            strBuf.append(" " + myURI);
            strBuf.append(" " + ACTION_VOTE + " 0 " + groupSize);
            strBuf.append(" " + uri);
        }
        else { // taking over at once if I am the only member
            masterURI = myURI;
            myStatus = NODE_RUNNING;
            strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
            strBuf.append(" " + myRole + " " + myStatus);
            strBuf.append(" " + myURI);
            strBuf.append(" " + ACTION_MOD + " 0 " + myStatus);
            strBuf.append(" " + myURI);
            previousStatus = NODE_STANDBY;
            myRole = 0;
            new Event(Event.INFO, name + ": I am the new master of group " +
                groupSize + ": " + myURI).send();
            if (out != null) { // notify the apps
                event = new Event(Event.NOTICE, myURI+" is the new master");
                event.setAttribute("status", String.valueOf(myStatus));
                event.setAttribute("role", String.valueOf(myRole));
                event.setAttribute("size", String.valueOf(groupSize));
                event.setAttribute("uri", myURI);
                event.setAttribute("operation", "create");
                event.setAttribute("action", String.valueOf(ACTION_ADD));
            }
        }
        packet = new DatagramPacket(strBuf.toString().getBytes(),
            0, strBuf.length(), myBean.getAddress(), myBean.getPort());
        myBean.setPacket(packet);
        myBean.setStatus(myStatus);
        myBean.setRole(myRole);

        return packet;
    }

    /**
     * It returns a heartbeat packet or a packet as the reaction to the
     * timeout incident.  It also updates all the relevent beans and node info.
     * The returned packet may be null meaning nothing need to send.
     */
    private DatagramPacket escalate(long currentTime, int myStatus, int retry) {
        DatagramPacket packet = null;
        PacketNodeBean pBean;
        StringBuffer strBuf;
        int i, role, status, type, myRole;
        long tm;
        String data, uri;
        byte[] buffer;

        myRole = myBean.getRole();
        if (retry == 0) { // hearbeat
            if (myRole == 0) {
                pBean = myBean;
                type = PacketNodeBean.MCG_HBEAT;
            }
            else {
                pBean = (PacketNodeBean) nodeList.get(masterURI);
                type = PacketNodeBean.PTP_HBEAT;
                if (pBean == null)
                    return null;
            }
            strBuf = new StringBuffer(currentTime + " ");
            strBuf.append(++sequenceNumber + " " + type);
            strBuf.append(" " + myRole + " " + myStatus);
            strBuf.append(" " + myURI + " " + groupSize);
            packet = new DatagramPacket(strBuf.toString().getBytes(),
                0, strBuf.length(), pBean.getAddress(), pBean.getPort());
            if (type == PacketNodeBean.MCG_HBEAT)
                myBean.setPacket(packet);
            else
                pBean.storePacket(packet, type);
            return packet;
        }

        // all new or only myself active at retry > 0, ie, timeout
        if (groupSize == 0 || groupSize == 1 &&
            (myStatus == NODE_STANDBY || myStatus == NODE_RUNNING)) {
            uri = getFavoriteURI();
            if (myURI.equals(uri)) { // I am the new master
                masterURI = myURI;
                if (groupSize == 0)
                    groupSize ++;
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                myStatus = NODE_RUNNING;
                myBean.setStatus(myStatus);
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_MOD + " 0 " + myStatus);
                strBuf.append(" " + myURI);
                previousStatus = NODE_STANDBY;
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                myBean.setRole(0);
                sequenceNumber = 0L;
                myRole = myBean.getRole();
                if (!isInitialized)
                    isInitialized = true;
                new Event(Event.INFO, name + ": I am the new master of group " +
                    groupSize + ": " + myURI).send();
                if (out != null) { // notify the apps
                    event = new Event(Event.NOTICE, myURI+" is the new master");
                    event.setAttribute("status", String.valueOf(myStatus));
                    event.setAttribute("role", String.valueOf(myRole));
                    event.setAttribute("size", String.valueOf(groupSize));
                    event.setAttribute("uri", myURI);
                    event.setAttribute("operation", "create");
                    event.setAttribute("action", String.valueOf(ACTION_ADD));
                }
            }
            else if (myRole < 0) { // still new
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI + " " + groupSize);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
            }
        }
        else if (masterURI == null) { // groupSize > 0 but no master yet
            packet = myBean.getPacket();
/*
            buffer = packet.getData();
            i = String.valueOf(currentTime).length() + 1;
            buffer[i] ++;
*/
        }
        else if (myRole > 0) { // have master but still standby
            myStatus = myBean.getStatus();
            pBean = (PacketNodeBean) nodeList.get(masterURI);
            if (pBean == null)
                return null;
            status = pBean.getStatus();

            if (myStatus == NODE_RUNNING && status == NODE_RUNNING) {
                // master timeout
                status = NODE_TIMEOUT;
                pBean.setStatus(status);
                resetAllRefCount();
                pBean.setRefCount(1);
                tm = pBean.getPacketInfo(PacketNodeBean.MCG_HBEAT,
                    PacketNodeBean.PKT_TIME);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_NOTICE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_CHK + " " + (currentTime - tm));
                strBuf.append(" " + status + " " + masterURI);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                myBean.setStatus(NODE_STANDBY);
                new Event(Event.WARNING, name + ": the master '" + masterURI +
                    "' timed out so switched to STANDBY now on " +myURI).send();
            }
            else if (myStatus == NODE_STANDBY && status == NODE_TIMEOUT) {
                // check timeout
                if (hasExtra && rcvrType == RCVR_JMS && // check observer
                    (i = extraRcvr.getStatus()) != Receiver.RCVR_RUNNING) {
                    if (((retry - 1) % maxRetry) == 0)
                        new Event(Event.WARNING, name + ": the master '" +
                            masterURI + "' timed out but observer is still in "+
                            i + " on " + myURI).send();
                    return null;
                }
                // removing the master which used to be active
                groupSize --;
                pBean =(PacketNodeBean) nodeList.get(masterURI);
                nodeList.remove(masterURI);
                if (pBean != null)
                    pBean.clear();
                pBean = null;
                new Event(Event.WARNING,name+": removed the master '"+masterURI+
                    "' and initiated new election by " + myURI).send();
                masterURI = null;
                // start election at once
                packet = vote(currentTime, null);
            }
            else if (myStatus == NODE_STANDBY && status == NODE_RUNNING) {
                // master is alive
                myBean.setStatus(NODE_RUNNING);
            }
        }

        return packet;
    }

    /**
     * It returns a packet as the response to the incoming packet.
     * It also updates all the relevent beans and node info.
     * The returned packet may be null meaning no need to reply.
     */
    private DatagramPacket respond(long currentTime, int type,
        PacketNodeBean pBean) {
        DatagramPacket packet = null;
        int action, x, y, n, role, status, myRole, myStatus;
        long t, tm, sid;
        String data, uri, u;
        StringBuffer strBuf;
        if (pBean == null || type == PacketNodeBean.MCG_HBEAT ||
            type == PacketNodeBean.PTP_HBEAT)
            return null;

        uri = pBean.getUri();
        if (myURI.equals(uri)) // no need to respond to itself
            return null;

        tm = pBean.getTimestamp();
        sid = pBean.getSid();
        role = pBean.getRole();
        status = pBean.getStatus();
        data = pBean.getData();
        myRole = myBean.getRole();
        myStatus = myBean.getStatus();

        switch (type) {
          case PacketNodeBean.MCG_QUIT:
            // signing off from an existing node
            if (status == NODE_DOWN) {
                if (role >= 0) // for an active node
                    groupSize --;
                nodeList.remove(uri);
                pBean.clear();
                pBean = null;
                groupSize = countActiveNodes();
                new Event(Event.INFO, name + ": node of " + role +
                    " removed due to exit: " + uri + " at " + groupSize).send();
                if (myRole == 0 && out != null) { // notify the apps
                    event = new Event(Event.NOTICE, uri + " left the group");
                    event.setAttribute("status", String.valueOf(status));
                    event.setAttribute("role", String.valueOf(role));
                    event.setAttribute("size", String.valueOf(groupSize));
                    event.setAttribute("uri", uri);
                    event.setAttribute("operation", "remove");
                    event.setAttribute("action", String.valueOf(ACTION_DEL));
                    passthru(currentTime, event, out);
                    event = null;
                }
                if (role != 0 || masterURI == null)
                    break;
                // master signing off
                masterURI = null;
                packet = vote(currentTime, null);
            }
            else if (role == 0 && uri.equals(masterURI) &&
                status == NODE_STANDBY && nodeList.containsKey(data) &&
                myStatus == NODE_RUNNING) {
                // master nominates a new master and wants to swap roles
                masterURI = data;
                PacketNodeBean mBean = (PacketNodeBean) nodeList.get(data);
                role = mBean.getRole();
                mBean.setRole(0);
                pBean.setRole(role);
                mBean.setStatus(status);
                if (!myURI.equals(masterURI)) {
                    myStatus = status;
                    myBean.setStatus(myStatus);
                    new Event(Event.INFO, name + ": " + uri +
                        " resigned and nominated the new master: " +
                        masterURI).send();
                    break;
                }
                // I am nominated as the new master
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_MOD + " 0 " + myStatus);
                strBuf.append(" " + myURI);
                previousStatus = NODE_STANDBY;
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                myBean.setRole(0);
                sequenceNumber = 0L;
                myRole = myBean.getRole();

                new Event(Event.INFO, name + ": I am appointed as the new" +
                    " master of group " + groupSize + ": " + myURI).send();

                if (out != null) { // notify the apps
                    event = new Event(Event.NOTICE, myURI +
                        " is appointed as master");
                    event.setAttribute("status", String.valueOf(myStatus));
                    event.setAttribute("role", String.valueOf(myRole));
                    event.setAttribute("size", String.valueOf(groupSize));
                    event.setAttribute("uri", myURI);
                    event.setAttribute("operation", "update");
                    event.setAttribute("action", String.valueOf(ACTION_MOD));
                }
            }
            break;
          case PacketNodeBean.MCG_HSHAKE:
            // signing on from a newbie
            if (status == NODE_NEW) { // newbie
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_HSHAKE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                if (myRole == 0 && role < 0) { // assign new role
                    role = getFirstRecycledRole();
                    strBuf.append(" " + role + " " + uri);
                    pBean.setRole(role);
                    if (pBean.getRefCount() == 0) { // add to group only once
                        String ip = address.getHostAddress();
                        groupSize ++;
                        pBean.setRefCount(1);
                        status = NODE_STANDBY;
                        pBean.setStatus(status);
                        // check partener's address
                        if (!ip.equals(pBean.getAddress().getHostAddress())) {
                            StringBuffer sb;
                            long[] hbInfo = (long[])nodeList.getMetaData(myURI);
                            address = pBean.getAddress();
                            nodeList.remove(myURI);
                            myBean.clear();
                            // rebuild the packet
                            sb = new StringBuffer(currentTime+" "+getNextSid());
                            sb.append(" " + PacketNodeBean.MCG_HSHAKE);
                            sb.append(" " + myRole);
                            sb.append(" " + myStatus);
                            sb.append(" " + myURI + " " + groupSize);
                            packet =new DatagramPacket(sb.toString().getBytes(),
                                0, sb.length(), address, port);
                            myBean = new PacketNodeBean();
                            // reset the packet
                            myBean.setPacket(packet);
                            nodeList.add(myURI, hbInfo, myBean);
                            new Event(Event.NOTICE, name + ": partener's IP" +
                                " has changed: " + uri).send();
                        }
                        new Event(Event.INFO, name + ": node of " + role +
                            " has joined in group "+groupSize+": "+uri).send();
                        if (out != null) { // notify the apps
                            event = new Event(Event.NOTICE, uri +
                                " joined the group");
                            event.setAttribute("status",String.valueOf(status));
                            event.setAttribute("role", String.valueOf(role));
                            event.setAttribute("size",
                                String.valueOf(groupSize));
                            event.setAttribute("uri", uri);
                            event.setAttribute("operation", "create");
                            event.setAttribute("action",
                                String.valueOf(ACTION_ADD));
                        }
                    }
                }
                else if (myRole == 0) // role already assigned
                    strBuf.append(" " + role + " " + groupSize);
                else if (myRole > 0)
                    strBuf.append(" " + groupSize + " " + masterURI);
                else if (groupSize == 0 && nodeList.size() >= maxGroupSize) {
                    String ip = address.getHostAddress();
                    // in case the other side does not know me
                    strBuf = new StringBuffer(currentTime + " " + getNextSid());
                    strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                    strBuf.append(" " + myRole);
                    strBuf.append(" " + myStatus);
                    strBuf.append(" " + myURI + " " + groupSize);
                    packet = new DatagramPacket(strBuf.toString().getBytes(),
                        0, strBuf.length(), pBean.getAddress(), port);
                    if (!ip.equals(pBean.getAddress().getHostAddress())) {
                        long[] hbInfo = (long[]) nodeList.getMetaData(myURI);
                        address = pBean.getAddress();
                        nodeList.remove(myURI);
                        myBean.clear();
                        myBean = new PacketNodeBean();
                        // reset the packet
                        myBean.setPacket(packet);
                        nodeList.add(myURI, hbInfo, myBean);
                        new Event(Event.NOTICE, name + ": partener's IP" +
                            " has changed: " + uri).send();
                    }
                    send(packet);
                    packet = escalate(currentTime, myBean.getStatus(), 1);
                    break;
                }
                else // I am newbie too, but it is not full house yet
                    strBuf.append(" -1 " + myURI);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.PTP_HSHAKE);
            }
            break;
          case PacketNodeBean.PTP_HSHAKE:
            // welcome from master or existing members
            if (role == 0 && status == NODE_RUNNING) { // master
                if (masterURI == null) {
                    masterURI = uri;
                    groupSize ++;
                }
                if (pm.contains(data, pattern)) {
                    MatchResult mr = pm.getMatch();
                    myRole = Integer.parseInt(mr.group(3));
                }
                else {
                    new Event(Event.WARNING, name + " " + myURI +
                        ": malformed data from " + uri + ": " + data).send();
                    break;
                }
                myStatus = NODE_STANDBY;
                myBean.setStatus(myStatus);
                myBean.setRole(myRole);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_CONFRM);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                strBuf.append(" " + type + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.MCG_CONFRM);
            }
            else if (role > 0 &&
                (status == NODE_RUNNING || status == NODE_STANDBY)) {
                // welcome from an existing non-master node, no response
                groupSize = countActiveNodes();
            }
            // welcome from an existing newbie, no response either
            break;
          case PacketNodeBean.PTP_CONFRM:
            if (pm.contains(data, pattern)) {
                MatchResult mr = pm.getMatch();
                t = Long.parseLong(mr.group(1));
                x = Integer.parseInt(mr.group(2));
                y = Integer.parseInt(mr.group(3));
                u = mr.group(4);
            }
            else {
                new Event(Event.WARNING, name + " " + myURI +
                    ": malformed data from " + uri + ": " + data).send();
                break;
            }

            if (myRole == 0 && status == NODE_STANDBY &&
                y == PacketNodeBean.PTP_HSHAKE) { // confirmation for master
                status = NODE_RUNNING;
                pBean.setStatus(status);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_ADD + " " + role);
                strBuf.append(" " + status + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
            }
            else if (myRole == 0 && status == NODE_RUNNING) {
                // update state
            }
            else if (myRole != 0 && status == NODE_RUNNING &&
                y == PacketNodeBean.MCG_NOTICE && myURI.equals(u)) {
                // master timeout has not been confirmed
                n = pBean.getRefCount() + 1;
                pBean.setRefCount(n);
            }
            else if (myRole != 0 &&
                (status == NODE_RUNNING || status == NODE_STANDBY) &&
                y == PacketNodeBean.MCG_NOTICE && u.equals(masterURI)) {
                // master timeout has been confirmed
                pBean = (PacketNodeBean) nodeList.get(masterURI);
                if (pBean == null)
                    break;
                n = pBean.getRefCount() + 1;
                pBean.setRefCount(n);
                if (n < groupSize - 1)
                    break;
                groupSize --;
                nodeList.remove(masterURI);
                pBean.clear();
                pBean = null;
                new Event(Event.INFO, name + ": removed the master '"+masterURI
                    + "' after the confirmation and initiated new election by "
                    + myURI).send();
                // master is removed due to timeout
                masterURI = null;
                packet = vote(currentTime, null);
            }
            break;
          case PacketNodeBean.PTP_NOTICE:
            if (role == 0 && myRole > 0) { // master notifies me on my timeout
                if (myStatus == NODE_STANDBY)
                    myBean.setStatus(NODE_RUNNING);
                packet = escalate(currentTime, myStatus, 0);
            }
            break;
          case PacketNodeBean.MCG_NOTICE:
          case PacketNodeBean.PTP_UPDATE:
          case PacketNodeBean.MCG_UPDATE:
            if (pm.contains(data, pattern)) {
                MatchResult mr = pm.getMatch();
                action = (int) Long.parseLong(mr.group(1));
                x = (int) Long.parseLong(mr.group(2));
                y = (int) Long.parseLong(mr.group(3));
                u = mr.group(4);
            }
            else {
                new Event(Event.WARNING, name + " " + myURI +
                    ": malformed data from " + uri + ": " + data).send();
                break;
            }
            packet = evaluate(currentTime, type, action, x, y, u, pBean);
            break;
          case PacketNodeBean.PTP_DATA:
          case PacketNodeBean.MCG_DATA:
            // data reload from master
            if (out != null && myStatus == NODE_RUNNING) {
                event = new Event(Event.INFO, data);
                event.setTimestamp(tm);
                event.setAttribute("status", String.valueOf(status));
                event.setAttribute("role", String.valueOf(role));
                event.setAttribute("size", String.valueOf(groupSize));
                event.setAttribute("uri", uri);
                event.setAttribute("operation", "reload");
                event.setAttribute("sid", String.valueOf(sid));
            }
            break;
          case PacketNodeBean.MCG_HBEAT:
          case PacketNodeBean.PTP_HBEAT:
          default:
        }

        return packet;
    }

    /**
     * It evaluates the parsed result on the body part and returns
     * a packet if it needs to respond.  It also updates all the relevent
     * beans and node info.  If there is no need to respond, it returns null.
     */
    private DatagramPacket evaluate(long currentTime, int type,
        int action, int x, int y, String u, PacketNodeBean pBean) {
        DatagramPacket packet = null;
        StringBuffer strBuf;
        String uri;
        int n, role, status, myRole, myStatus;
        long t, tm, sid;

        if (pBean == null || type == PacketNodeBean.MCG_HBEAT ||
            type == PacketNodeBean.PTP_HBEAT)
            return null;
        uri = pBean.getUri();
        if (myURI.equals(uri)) // no need to respond to itself
            return null;

        tm = pBean.getTimestamp();
        sid = pBean.getSid();
        role = pBean.getRole();
        status = pBean.getStatus();
        myRole = myBean.getRole();
        myStatus = myBean.getStatus();

        switch (action) {
          case ACTION_MOD:
            if (role > 0 && uri.equals(u) && x == 0 && myRole > 0 &&
                (status == NODE_STANDBY||status == NODE_RUNNING)) {
                // elected new master
                if (role == myRole)
                    myStatus = NODE_RUNNING;
                else
                    myStatus = status;
                myBean.setStatus(myStatus);
                pBean.setRole(x);
                if (masterURI == null)
                    masterURI = uri;
                pBean.setStatus(y);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_CONFRM);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                strBuf.append(" " + type + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.MCG_CONFRM);
                if (role != myRole)
                    new Event(Event.INFO, name +": nominated master sworn in: "+
                        masterURI).send();
                else { // I used to be the master, notify apps my new role
                    new Event(Event.INFO, name + ": new master swapped its " +
                        "role with me: " + masterURI).send();
                    if (out != null) { // notify the apps
                        event = new Event(Event.NOTICE, myURI +
                            " swapped the role");
                        event.setAttribute("status", String.valueOf(myStatus));
                        event.setAttribute("role", String.valueOf(myRole));
                        event.setAttribute("size", String.valueOf(groupSize));
                        event.setAttribute("uri", myURI);
                        event.setAttribute("operation", "update");
                        event.setAttribute("action",String.valueOf(ACTION_MOD));
                    }
                }
            }
            else if (role < 0 && uri.equals(u) && myRole > 0 &&
                status == NODE_STANDBY) { // self-declared new master
                myStatus = status;
                myBean.setStatus(myStatus);
                pBean.setRole(x);
                pBean.setStatus(y);
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_CONFRM);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                strBuf.append(" " + type + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.MCG_CONFRM);
            }
            else if (myRole < 0 && x == 0 && y == NODE_RUNNING) {
                if (role > 0 && masterURI == null) { // changed to new master
                    nodeList.remove(uri);
                    pBean.clear();
                    pBean = null;
                    groupSize --;
                }
                else {
                    pBean.setRole(x);
                    pBean.setStatus(y);
                }
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_HSHAKE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI + " " + groupSize);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
            }
            break;
          case ACTION_ADD:
            // update from master or monitor
            if (role == 0 && status == NODE_RUNNING) {
                PacketNodeBean mBean = (PacketNodeBean) nodeList.get(u);
                mBean.setRole(x);
                mBean.setStatus(y);
                groupSize = countActiveNodes();
                if (myRole == x)
                    myStatus = y;
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_CONFRM);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                strBuf.append(" " + type + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.MCG_CONFRM);
            }
            break;
          case ACTION_CHK:
            // missing heartbeat of master
            if (myRole == 0 && myURI.equals(u) && status == NODE_STANDBY &&
                myStatus == NODE_RUNNING) { // over rule
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_OVR + " " + role);
                strBuf.append(" " + myStatus + " " + groupURI);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                new Event(Event.INFO, name + ": overruled the change request "+
                    "on master's timeout initiated by " + uri).send();
            }
            else if (myRole > 0 && !uri.equals(u) && u.equals(masterURI) &&
                (status == NODE_RUNNING || status == NODE_STANDBY)) {
                // check heartbeat
                PacketNodeBean mBean = (PacketNodeBean) nodeList.get(u);
                t = tm - x; // recover the lastest timestamp of hbeat
                t = mBean.getPacketInfo(PacketNodeBean.MCG_HBEAT,
                    PacketNodeBean.PKT_TIME) - t;
                if ((int) t < heartbeat + heartbeat) { // comfirmed
                    myBean.setStatus(NODE_STANDBY);
                    mBean.setStatus(y);
                    uri = u;
                }

                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                strBuf.append(" " + PacketNodeBean.PTP_CONFRM);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + tm + " " + sid);
                strBuf.append(" " + type + " " + uri);
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), pBean.getAddress(), pBean.getPort());
                pBean.storePacket(packet, PacketNodeBean.MCG_CONFRM);
            }
            break;
          case ACTION_VOTE:
            // participate election process and count the votes
            if (masterURI != null && !masterURI.equals(u) && role != 0 &&
                groupSize == y+1) { // remove master first
                groupSize --;
                pBean = (PacketNodeBean) nodeList.get(masterURI);
                nodeList.remove(masterURI);
                if (pBean != null)
                    pBean.clear();
                pBean = null;
                new Event(Event.INFO, name + ": removed the master '"+masterURI
                    + "' and participating new election as " + myURI).send();
                masterURI = null;
                packet = vote(currentTime, u);
            }

            if (masterURI == null &&
                (myStatus == NODE_RUNNING || myStatus == NODE_STANDBY)) {
                pBean = (PacketNodeBean) nodeList.get(u);
                pBean.setRefCount(pBean.getRefCount()+1);
                n = myBean.getRefCount();
                if (n + n <= groupSize) { // not enough votes for me
                    uri = getFavoriteURI();
                    pBean = (PacketNodeBean) nodeList.get(uri);
                    n = pBean.getRefCount();
                    if (n + n <= groupSize) // not enough votes yet
                        break;
                    if (!myURI.equals(uri)) // votes is enough, but not me
                        break;
                }

                // I am the elected new master
                masterURI = myURI;
                myRole = myBean.getRole();
                strBuf = new StringBuffer(currentTime + " " + getNextSid());
                myStatus = NODE_RUNNING;
                myBean.setStatus(myStatus);
                strBuf.append(" " + PacketNodeBean.MCG_UPDATE);
                strBuf.append(" " + myRole + " " + myStatus);
                strBuf.append(" " + myURI);
                strBuf.append(" " + ACTION_MOD + " 0 " + myStatus);
                strBuf.append(" " + myURI);
                previousStatus = NODE_STANDBY;
                packet = new DatagramPacket(strBuf.toString().getBytes(),
                    0, strBuf.length(), myBean.getAddress(), myBean.getPort());
                myBean.setPacket(packet);
                myBean.setRole(0);
                sequenceNumber = 0L;
                myRole = myBean.getRole();
                if (!isInitialized)
                    isInitialized = true;
                new Event(Event.INFO, name+": I am elected as master of group "+
                    groupSize + ": " + myURI).send();
                if (out != null) { // notify the apps
                    event = new Event(Event.NOTICE, myURI +
                        " is elected as master");
                    event.setAttribute("status", String.valueOf(myStatus));
                    event.setAttribute("role", String.valueOf(myRole));
                    event.setAttribute("size", String.valueOf(groupSize));
                    event.setAttribute("uri", myURI);
                    event.setAttribute("operation", "create");
                    event.setAttribute("action", String.valueOf(ACTION_ADD));
                }
            }
            break;
          case ACTION_DEL:
            if (role == 0 && !uri.equals(u) && nodeList.containsKey(u) &&
                y == NODE_DOWN) {
                pBean = (PacketNodeBean) nodeList.get(u);
                status = pBean.getStatus();
                if (status != NODE_NEW && status != NODE_DOWN)
                    groupSize --;
                nodeList.remove(u);
                pBean.clear();
                pBean = null;
                new Event(Event.INFO, name + ": removed the node '" + u +
                    "' from " + myURI).send();
                if ((x == myRole && x >= 0) || myURI.equals(u)) {
                    new Event(Event.INFO, name + ": " + myURI +
                        " is shutdown at request of master: " +
                        ((masterURI == null) ? uri : masterURI)).send();
                    if (out != null) { // notify the apps
                        event = new Event(Event.WARNING, myURI +
                            " is shutdown at request of master: " + masterURI);
                        event.setAttribute("status", String.valueOf(y));
                        event.setAttribute("role", String.valueOf(x));
                        event.setAttribute("size", String.valueOf(groupSize));
                        event.setAttribute("uri", myURI);
                        event.setAttribute("operation", "remove");
                        event.setAttribute("action",String.valueOf(ACTION_DEL));
                        passthru(currentTime, event, out);
                        event = null;
                    }
                    close();
                }
            }
            else if (role < 0 && nodeList.containsKey(u) && y == NODE_DOWN) {
                pBean = (PacketNodeBean) nodeList.get(u);
                status = pBean.getStatus();
                if (status != NODE_NEW && status != NODE_DOWN)
                    groupSize --;
                nodeList.remove(u);
                pBean.clear();
                pBean = null;
                new Event(Event.INFO, name + ": removed the node '" + u +
                    "' on " + myURI).send();
                if (myURI.equals(u)) { // it is me to leave group
                    new Event(Event.INFO, name + ": " + myURI +
                        " is shutdown at request of " + uri).send();
                    if (out != null) { // notify the apps
                        event = new Event(Event.WARNING, myURI +
                            " is shutdown at request of " + uri);
                        event.setAttribute("status", String.valueOf(y));
                        event.setAttribute("role", String.valueOf(x));
                        event.setAttribute("size", String.valueOf(groupSize));
                        event.setAttribute("uri", myURI);
                        event.setAttribute("operation", "remove");
                        event.setAttribute("action",String.valueOf(ACTION_DEL));
                        passthru(currentTime, event, out);
                        event = null;
                    }
                    close();
                }
            }
            break;
          case ACTION_OVR:
            if (role == 0 && myRole > 0 && groupURI.equals(u) &&
                status == y && y == NODE_RUNNING) {
                if (nodeList.containsKey(uri) && uri.equals(masterURI)) {
                    pBean.setStatus(y);
                }
                if (myStatus == NODE_STANDBY) {
                    myBean.setStatus(NODE_RUNNING);
                    new Event(Event.INFO, name + ": master is not timeout " +
                        "since the change got overruled by " + uri).send();
                }
            }
            break;
          case ACTION_IGR:
          default:
            break;
        }

        return packet;
    }

    /**
     * It sends an Event to the application via the output XQ.  Currently
     * INFO is for DATA transfers, NOTICE for state changes and
     * WANRING for failures.  Application is supposed to use relay()
     * to send events to a node or the entire cluster.
     */
    private int passthru(long currentTime, Event event, XQueue out) {
        int cid, priority;
        cid = -1;
        if (event == null || out == null)
            return cid;
        priority = event.getPriority();
        switch (priority) {
          case Event.WARNING:
          case Event.NOTICE:
            if (outLen > 0) {
                cid = out.reserve(waitTime, outBegin, outLen);
                if (cid >= 0) {
                    cid = out.add(event, cid);
                }
                else if (out.depth() > 0) {
                    cid = out.getNextCell(waitTime*3, outBegin, outLen);
                    if (cid >= 0)
                        out.remove(cid);
                    cid = out.reserve(waitTime);
                    cid = out.add(event, cid);
                }
            }
            else {
                cid = out.reserve(waitTime);
                if (cid >= 0) {
                    cid = out.add(event, cid);
                }
                else if (out.depth() > 0) {
                    cid = out.getNextCell(waitTime);
                    if (cid >= 0)
                        out.remove( cid);
                    cid = out.reserve(waitTime);
                    cid = out.add(event, cid);
                }
            }
            break;
          case Event.INFO:
            if (outLen > 0) {
                for (int i=0; i<3; i++) {
                    cid = out.reserve(waitTime, outBegin, outLen);
                    if (cid >= 0) {
                         cid = out.add(event, cid);
                         break;
                    }
                }
            }
            else {
                for (int i=0; i<3; i++) {
                    cid = out.reserve(waitTime);
                    if (cid >= 0) {
                         cid = out.add(event, cid);
                         break;
                    }
                }
            }
            break;
          default:
            if (outLen > 0) {
                do {
                    cid = out.reserve(waitTime, outBegin, outLen);
                } while (cid < 0 &&
                    (out.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
                if (cid >= 0)
                    cid = out.add(event, cid);
            }
            else {
                do {
                    cid = out.reserve(waitTime);
                } while (cid < 0 &&
                    (out.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
                if (cid >= 0)
                    cid = out.add(event, cid);
            }
            break;
        }

        return cid;
    }

    /**
     * returns next sid for sending non-HBEAT packet
     */
    private synchronized long getNextSid() {
        return ++sequenceId;
    }

    public boolean isMaster() {
        if (myBean != null)
            return (myBean.getRole() == 0);
        else
            return false;
    }

    public int getRole() {
        if (myBean != null)
            return myBean.getRole();
        else
            return -1;
    }

    public int getStatus() {
        if (myBean != null)
            return myBean.getStatus();
        else
            return -1;
    }

    public int getTimeout() {
        return sessionTimeout;
    }

    public String getURI() {
        return myURI;
    }

    public int getSize() {
        return groupSize;
    }

    public int getDebug() {
        return debug;
    }

    public void setDebug(int debug) {
        if (debug >= DEBUG_NONE && debug <= DEBUG_ALL)
            this.debug = debug;
    }

    /** It starts the cluster node and supervises its state. */
    public void start() {
        start((XQueue) null, 0, 0);
    }

    /**
     * It starts the cluster node and supervises its state.  With the
     * communication XQueue defined, the cluster node will escalate or relay
     * events to/from external caller via the XQueue.  The cluster node will
     * keep running until its shutdown.
     */
    public void start(XQueue out, int begin, int len) {
        Thread s, thr = null;
        s = new Thread(this, "udp_socket");
        s.setPriority(Thread.NORM_PRIORITY);
        s.setDaemon(true);
        s.start();
        if (hasExtra && rcvrType != RCVR_UDP) {
            thr = new Thread(this, "extra_rcvr");
            thr.setPriority(Thread.NORM_PRIORITY);
            thr.setDaemon(true);
            thr.start();
        }

        if (out != null) { // set outLink for escalations
            this.out = out;
            outBegin = begin;
            outLen = len;
        }

        if (!isInitialized)
            initialize();

        examine();

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

        if (thr != null && thr.isAlive()) { // receiver thread
            thr.interrupt();
            try {
                thr.join(5000);
            }
            catch (Exception e) {
                thr.interrupt();
            }
        }

        if (nodeList != null && nodeList.size() > 0) {
            int i;
            PacketNodeBean pBean;
            Browser browser = nodeList.browser();

            while ((i = browser.next()) >= 0) {
                pBean = (PacketNodeBean) nodeList.get(i);
                if (pBean != null)
                    pBean.clear();
            }
            nodeList.clear();
        }
        if (socket != null)
            socket.close();
        if (extraRcvr != null)
            extraRcvr.close();
        groupSize = 0;
        myBean = null;
        masterURI = null;
        previousStatus = NODE_TIMEOUT;
        isInitialized = false;
        new Event(Event.INFO, name + ": " + myURI + " terminated").send();
    }

    public static void main(String args[]) {
        Thread c;
        int i, retry = 3, timeout = 30, port = -1;
        int bufferSize = 8192, sleepTime = 2, waitTime = 100;
        int heartbeat = 10, debug = 0;
        String group = null, logDir = null, queue = null, ms = null;
        UnicastNode node;
        Map<String, Object> props, extra = new HashMap<String, Object>();
        List<String> list = new ArrayList<String>();

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
              case 'g':
                if (i + 1 < args.length)
                    group = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 'm':
                if (i + 1 < args.length)
                    retry = Integer.parseInt(args[++i]);
                break;
              case 'd':
                if (i+1 < args.length)
                    debug = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    bufferSize = Integer.parseInt(args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'h':
                if (i+1 < args.length)
                    heartbeat = Integer.parseInt(args[++i]);
                break;
              case 'w':
                if (i+1 < args.length)
                    waitTime = Integer.parseInt(args[++i]);
                break;
              case 'u':
                if (i + 1 < args.length)
                    extra.put("URI", args[++i]);
                break;
              case 'q':
                if (i + 1 < args.length)
                    queue = args[++i];
                break;
              case 'f':
                if (i + 1 < args.length)
                    ms = args[++i];
                break;
              default:
            }
        }
        if (group != null && group.length() > 0)
            list.add(group);

        if (logDir != null)
            Event.setLogDir(logDir);

        props = new HashMap<String, Object>();
        props.put("URI", "udp://##hostname##:" + port);
        props.put("Name", "PGroup");
        props.put("Heartbeat", String.valueOf(heartbeat));
        props.put("BufferSize", String.valueOf(bufferSize));
        props.put("Debug", String.valueOf(debug));
        props.put("MaxRetry", String.valueOf(retry));
        props.put("WaitTime", String.valueOf(waitTime));
        props.put("SleepTime", String.valueOf(sleepTime));
        props.put("SessionTimeout", String.valueOf(timeout));
        props.put("Member", list);
        if (extra.size() > 0) {
            extra.put("Name", "Observer");
            if (queue != null && queue.length() > 0)
                extra.put("QueueName", queue);
            if (ms != null && ms.length() > 0)
                extra.put("MessageSelector", ms);
            props.put("ExtraReceiver", extra);
        }

        try {
            node = new UnicastNode(props);
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
        System.out.println("UnicastNode Version 2.0 (written by Yannan Lu)");
        System.out.println("UnicastNode: a UnicastNode for clustering");
        System.out.println("Usage: java org.qbroker.cluster.UnicastNode -g hostname -p port");
        System.out.println("  -?: print this message");
        System.out.println("  -g: IP address for the parterner");
        System.out.println("  -p: port");
        System.out.println("  -d: debug mode (default: 0)");
        System.out.println("  -h: heartbeat in sec (default: 10)");
        System.out.println("  -s: buffer size for receive (default: 8192)");
        System.out.println("  -t: sessionTimeout in sec (default: 30)");
        System.out.println("  -w: waitTime for XQueue in ms (default: 100)");
        System.out.println("  -u: uri of the extra receiver");
        System.out.println("  -q: queue name for the extra receiver of wmq");
        System.out.println("  -f: message selector for the extra receiver of wmq");
        System.out.println("  -L: log directory");
    }
}
