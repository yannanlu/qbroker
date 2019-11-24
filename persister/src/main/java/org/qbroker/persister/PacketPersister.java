package org.qbroker.persister;

/* PacketPersister.java - a persister sending JMS messages to a UDP port */

import java.util.Map;
import java.util.Date;
import java.io.IOException;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.UnknownHostException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.net.UDPSocket;
import org.qbroker.net.MulticastGroup;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessagePacket;
import org.qbroker.jms.SNMPMessenger;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * PacketPersister listens to an XQueue for JMSMessages, packages the
 * content into DatagramPackets and sends them to a UDP socket.
 * PacketPersister supports flow control
 * and allows object control from its owner.  It is fault tolerant with
 * retry and idle options.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class PacketPersister extends Persister {
    private MessagePacket mp = null;
    private SNMPMessenger snmp = null;
    private UDPSocket socket = null;
    private MulticastGroup group = null;
    private InetAddress address;
    private String hostIP, inf;
    private int retryCount, soTimeout, port;
    private int bufferSize, ttl;
    private long sessionTime;
    private boolean isConnected = false, isMulticast = false;
    private boolean doInquire = false, isSNMP = false;

    public PacketPersister(Map props) {
        super(props);
        String scheme = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"udp".equals(u.getScheme()) && !"snmp".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: "+u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 0;

        if ((hostIP = u.getHost()) == null || hostIP.length() == 0)
            throw(new IllegalArgumentException("no Host/IP specified in URL"));

        try {
            address = InetAddress.getByName(hostIP);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException("unknown host: " + hostIP));
        }

        ttl = 1;
        if (address.isMulticastAddress()) {
            isMulticast = true;
            if ((o = props.get("Interface")) != null && o instanceof String)
                inf = (String) o;
            else if (o != null && o instanceof Map)
                inf = (String) ((Map) o).get(Event.getHostName());
            else
                inf = null;
            if (port <= 0)
                throw(new IllegalArgumentException("port not defined: " + uri));

            if ((o = props.get("TimeToLive")) != null)
                ttl = Integer.parseInt((String) o);
        }

        if ((o = props.get("SOTimeout")) != null)
            soTimeout = 1000 * Integer.parseInt((String) o);
        else
            soTimeout = 1000;

        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);

        if (isMulticast) {
            group = new MulticastGroup(hostIP, port, bufferSize, soTimeout,
               ttl, inf);
        }
        else if (!"snmp".equals(u.getScheme())) {
            socket = new UDPSocket(null, port, bufferSize);
            if (soTimeout > 0)
                socket.setTimeout(soTimeout);
        }
        else try {
            isSNMP = true;
            snmp = new SNMPMessenger(props);
            operation = snmp.getOperation();
        }
        catch (Exception e) {
          throw(new IllegalArgumentException("failed to create SNMPMessenger: "+
                Event.traceStack(e)));
        }

        if (!isSNMP) try {
            mp = new MessagePacket(props);
            operation = mp.getOperation();
        }
        catch (Exception e) {
          throw(new IllegalArgumentException("failed to create MessagePacket: "+
                Event.traceStack(e)));
        }

        if (!"talk".equals(operation) && !"inquire".equals(operation)
            && !(isSNMP && "send".equals(operation))) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }

        if ("inquire".equals(operation))
            doInquire = true;

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(PSTR_READY, PSTR_RUNNING);

        for (;;) {
            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                socketOperation(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == PSTR_PAUSE) {
                if (status > PSTR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > PSTR_PAUSE)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                status == PSTR_STANDBY) {
                if (status > PSTR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > PSTR_STANDBY)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
            if (status == PSTR_READY) {
                setStatus(PSTR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        disconnect();
        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    private int socketOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (isMulticast) {
                if (doInquire)
                    mp.inquire(xq, group.getSocket());
                else
                    mp.talk(xq, group.getSocket());
            }
            else if (!isSNMP) { // non-snmp
                if (doInquire)
                    mp.inquire(xq, socket.getSocket());
                else
                    mp.talk(xq, socket.getSocket());
            }
            else { // for snmp
                if (doInquire)
                    snmp.inquire(xq);
                else
                    snmp.send(xq);
            }
        }
        catch (IOException e) {
            resetStatus(PSTR_RUNNING, PSTR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == PSTR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

//                socketReconnect();
                if (!isConnected) {
                    new Event(Event.ERR, linkName + ": failed to connect "+
                        uri + " after " + i + " retries").send();
                    continue;
                }
                new Event(Event.INFO, uri + " reconnected on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                sessionTime = System.currentTimeMillis();
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (JMSException e) {
            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;
            String str = linkName + " " + uri + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            i = retryCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                i = (i - 1) % repeatPeriod + 1;
            if (retryCount > 1) try {
                Thread.sleep(standbyTime*(i-1));
            }
            catch (Exception e1) {
            }
            if (i == 1 || i == maxRetry)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(PSTR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == PSTR_RUNNING &&
            (mask & XQueue.STANDBY) == 0) // job is done
            setStatus(PSTR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private void socketReconnect() {
        if (isMulticast) {
        }
        else {
        }
    }

    private void disconnect() {
        isConnected = false;
        try {
            if (isMulticast) {
                if (group != null)
                    group.close();
            }
            else if (!isSNMP) {
                if (socket != null)
                    socket.close();
            }
            else {
                if (snmp != null)
                    snmp.close();
            }
        }
        catch (Exception e) {
            new Event(Event.WARNING, linkName + ": failed to close " +
                uri + ": " + Event.traceStack(e)).send();
        }
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        disconnect();
        if (group != null)
            group = null;
        if (socket != null)
            socket = null;
        if (snmp != null)
            snmp = null;
    }

    protected void finalize() {
        close();
    }
}
