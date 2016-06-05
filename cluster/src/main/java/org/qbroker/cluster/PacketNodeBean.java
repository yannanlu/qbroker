package org.qbroker.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Date;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.cluster.ClusterNode;
import org.qbroker.event.Event;

/**
 * PacketNodeBean is a collection of properties for tracking the
 * state of a given source of UPD packets
 *<br/><br/>
 * A PacketNodeBean has bunch of node properties, like name,
 * role, address, port, hearbeat, status, latest timestamp,
 * sequence ID, packet type, packet count, packet info and
 * the first packet.  It is used for tracking the state of the node.
 * It can also be used for non UPD schemes. 
 *<br/><br/>
 * There are 3 getters and setters for extra uri, address and port,
 * respectively.  They are used for storing extra data for the node.
 *<br/><br/>
 * It is NOT MT-Safe. So use it with care.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PacketNodeBean implements java.io.Serializable {
    private String name;
    private String uri, extraURI = null;
    private long sid;
    private int role;
    private int heartbeat = 10000;
    private int bufferSize = 8192;
    private int port = 0, extraPort = 0;

    private InetAddress address = null, extraAddress = null;
    private DatagramPacket packet;

    private int status;
    private int packetType;
    private int refCount = 0;
    private long count = 0L;
    private long timestamp = 0L;
    private long latestTime = 0L;
    private String data;

    private int previousStatus;
    private int previousType;
    private long previousTime = 0L;
    private String[] content;
    private long[][] packetInfo;
    private boolean initialized = false;
    private Perl5Matcher pm = null;
    private Pattern pattern = null;

    public final static int MCG_HBEAT = 0;
    public final static int MCG_QUIT = 2;
    public final static int MCG_HSHAKE = 4;
    public final static int MCG_NOTICE = 6;
    public final static int MCG_UPDATE = 8;
    public final static int MCG_CONFRM = 10;
    public final static int MCG_QUERY = 12;
    public final static int MCG_DATA = 14;
    public final static int PTP_HBEAT = 1;
    public final static int PTP_QUIT = 3;
    public final static int PTP_HSHAKE = 5;
    public final static int PTP_NOTICE = 7;
    public final static int PTP_UPDATE = 9;
    public final static int PTP_CONFRM = 11;
    public final static int PTP_QUERY = 13;
    public final static int PTP_DATA = 15;
    public final static int PKT_TIME = 0;
    public final static int PKT_SID = 1;
    public final static int PKT_ROLE = 2;
    public final static int PKT_STATUS = 3;
    public final static int PKT_ACK = 4;
    public final static int PKT_COUNT = 5;

    public void clear() {
        packet = null;
        packetInfo = null;
        content = null;
        pm = null;
        address = null;
        initialized = false;
    }

    public void setPattern(String pattern) {
        if (pattern == null)
            return;
        try {
            Perl5Compiler pc = new Perl5Compiler();
            if (pm == null)
                pm = new Perl5Matcher();

            this.pattern = pc.compile(pattern);
        }
        catch (MalformedPatternException e) {
            throw(new IllegalArgumentException(e.toString()));
        }
    }

    /**
     * It stores the header and body of the packet to packetInfo as the given
     * type for public access.  There is no update on the node properties, like
     * status, packetType, timestamp, etc.
     */
    public void storePacket(DatagramPacket packet, int type) {
        if (packet == null || type < 0 || type > PTP_DATA)
            return;

        String text = new String(packet.getData(), 0, packet.getLength());
        if (pm.contains(text, pattern)) {
            MatchResult mr = pm.getMatch();
            packetInfo[type][PKT_TIME] = Long.parseLong(mr.group(1));
            packetInfo[type][PKT_SID] = Long.parseLong(mr.group(2));
            packetInfo[type][PKT_ROLE] = (long) Integer.parseInt(mr.group(4));
            packetInfo[type][PKT_STATUS] = (long) Integer.parseInt(mr.group(5));
            packetInfo[type][PKT_ACK] = 0L;
            content[type] = mr.group(7);
        }
    }

    /**
     * It parses the header of packet and updates properties.  Then it stores
     * the body into packetInfo for public access.  The class member of packet
     * is kind of final.  It will be initialized at the first set.
     * After that it is read-only.  But its content can be changed.
     * The update will be controlled by the sid and timestamp.  If it is a new
     * packet, the packetInfo and metadata will be updated.  In this case,
     * it returns 1 to indicate success.  Otherwise, 0 will be returned for
     * valid packet or -1 for parsing failure.
     */
    public int setPacket(DatagramPacket packet) {
        if (packet == null)
            return -1;

        if (!initialized) {
            if (pm == null)
          setPattern("^(\\d+) (\\d+) (-?\\d+) (-?\\d+) (-?\\d+) ([^ ]+) ?(.*)");

            address = packet.getAddress();
            port = packet.getPort();
            status = ClusterNode.NODE_NEW;
            if (address != null)
                uri = address.getHostAddress() + ":" + port;
            if (name == null)
                name = uri;
            content = new String[PTP_DATA + 1];
            packetInfo = new long[PTP_DATA + 1][PKT_COUNT + 1];
            for (int i=0; i<=PTP_DATA; i++) {
                content[i] = null;
                packetInfo[i] =
                    new long[] {0L, 0L, -1L, ClusterNode.NODE_NEW, 0L, 0L};
            }
            // set the packet as the final update
            this.packet = packet;
            initialized = true;
        }

        String text = new String(packet.getData(), 0, packet.getLength());
        if (pm.contains(text, pattern)) {
            MatchResult mr = pm.getMatch();
            previousTime = timestamp;
            timestamp = Long.parseLong(mr.group(1));
            sid = Long.parseLong(mr.group(2));
            previousType = packetType;
            packetType = Integer.parseInt(mr.group(3));
            packetInfo[packetType][PKT_COUNT] ++;
            role = Integer.parseInt(mr.group(4));
            previousStatus = status;
            status = Integer.parseInt(mr.group(5));
            if (address == null && uri == null) { // for generic packet support
                uri = mr.group(6);
                if (name == null)
                    name = uri;
            }
            data = mr.group(7);
            if (timestamp > latestTime)
                latestTime = timestamp;
            count ++;
            if (sid > packetInfo[packetType][PKT_SID] ||
                timestamp > packetInfo[packetType][PKT_TIME]) {
                packetInfo[packetType][PKT_TIME] = timestamp;
                packetInfo[packetType][PKT_SID] = sid;
                packetInfo[packetType][PKT_ROLE] = (long) role;
                packetInfo[packetType][PKT_STATUS] = (long) status;
                content[packetType] = data;
                return 1;
            }
            return 0;
        }
        return -1;
    }

    public DatagramPacket getPacket() {
        return packet;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String getContent(int type) {
        return content[type];
    }

    public long getPacketInfo(int type, int i) {
        return packetInfo[type][i];
    }

    public int getPacketType() {
        return packetType;
    }

    public long getSid() {
        return sid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLatestTime() {
        return latestTime;
    }

    public long getCount() {
        return count;
    }

    public int getRefCount() {
        return refCount;
    }

    public void setRefCount(int refCount) {
        this.refCount = refCount;
    }

    public String getName() {
        return name;
    }

    public String getUri() {
        return uri;
    }

    public int getRole() {
        return role;
    }

    public void setRole(int role) {
        this.role = role;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        if (status >= ClusterNode.NODE_NEW && status <= ClusterNode.NODE_DOWN)
            this.status = status;
    }

    public String getData() {
        return data;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public String getExtraURI() {
        return extraURI;
    }

    public void setExtraURI(String u) {
        this.extraURI = u;
    }

    public InetAddress getExtraAddress() {
        return extraAddress;
    }

    public void setExtraAddress (InetAddress a) {
        if (a != null)
            this.extraAddress = a;
    }

    public int getExtraPort() {
        return extraPort;
    }

    public void setExtraPort(int port) {
        if (port > 0 && port < 65536)
            this.extraPort = port;
    }
}
