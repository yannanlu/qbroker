package org.qbroker.net;

/* SNMPConnector.java - an SNMP connector for sending queries and traps */

import java.io.IOException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Snmp;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.TransportMapping;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Counter32;
import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.Null;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * SNMPConnector connects to an SNMP agent or manager and provides the
 * following methods of for SNMP operations: snmpGet(), snmpSet(), sendTrap(),
 * snmpNotify() and receive().
 *<br/><br/>
 * This is NOT MT-Safe.  It requires Java 1.4 or above due to SNMP4J.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SNMPConnector implements Connector {
    protected String hostname = null;
    protected String hostIP = null;
    protected InetAddress agentAddress = null;
    protected String community = "public";
    protected String agentName = null;
    protected String username = null;
    protected String password = null;
    protected String uri = null;
    protected String operation = "notify";
    protected int port = 161;
    protected int timeout = 2000;
    protected int retry = 2;
    protected long startTime;
    protected Snmp snmp;
    protected TransportMapping transport;
    protected CommunityTarget comTarget;
    protected OID enterpriseOID = null;
    private int defaultVersion = 1;
    private boolean isListening = false;
    public final static char INTEGER32 = 'i'; 
    public final static char UNSIGN32 = 'u'; 
    public final static char COUNTER32 = 'c'; 
    public final static char STRING = 's'; 
    public final static char HEXSTRING = 'x'; 
    public final static char DECIMALSTRING = 'd'; 
    public final static char NULL = 'n'; 
    public final static char TIMETICKS = 't'; 
    public final static char ADDRESS = 'a'; 
    public final static char BITSTRING = 'b'; 
    public final static char OIDSTRING = 'o'; 

    /** creates new SNMPConnector */
    public SNMPConnector(Map props) {
        Object o;
        URI u;
        int i;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"snmp".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 161;

        if ((agentName = u.getHost()) == null || agentName.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        try {
            agentAddress = InetAddress.getByName(agentName);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException("unknown host: "+agentName));
        }
        if ((i = agentName.indexOf('.')) > 0 &&
            !agentName.equals(agentAddress.getHostAddress()))
            agentName = agentName.substring(0, i);

        try {
            InetAddress ipAddress = InetAddress.getLocalHost();
            hostIP = ipAddress.getHostAddress();
            hostname = ipAddress.getCanonicalHostName();
            if (hostname == null || hostname.length() == 0)
                hostname = hostIP;
            else {
                i = hostname.indexOf(".");
                if (i > 0)
                    hostname = hostname.substring(0, i);
            }
        }
        catch (Exception e) {
            hostname = "127.0.0.1";
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 2000;

        if ((o = props.get("EnterpriseOID")) != null)
            enterpriseOID = new OID((String) o);
        else
            enterpriseOID = new OID("1.3.6.1.4.1.4103.3.0");

        if ((o = props.get("Operation")) != null)
            operation = (String) o;

        if ((o = props.get("Community")) != null)
            community = (String) o;

        if ((o = props.get("Version")) != null &&
            "2c".equalsIgnoreCase((String) o))
            defaultVersion = 2;
        else if (o != null && "3".equalsIgnoreCase((String) o))
            defaultVersion = 3;
        else
            defaultVersion = 1;

        try {
            UdpAddress targetAddress = new UdpAddress(agentAddress, port);
            if ("listen".equalsIgnoreCase(operation)) {
                transport =
                    new SimpleUdpTransportMapping(targetAddress);
               ((SimpleUdpTransportMapping)transport).setSocketTimeout(timeout);
                snmp = new Snmp(transport);
                snmp.addCommandResponder((CommandResponder) transport);
            }
            else if ("notify".equalsIgnoreCase(operation)) {
                transport = new DefaultUdpTransportMapping();
                snmp = new Snmp(transport);
            }
            else {
                transport = new DefaultUdpTransportMapping();
                snmp = new Snmp(transport);
                transport.listen();
            }
            isListening = transport.isListening();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to initialize " +
                "SNMP objects: " + e.toString()));
        }
        comTarget = getTarget(defaultVersion, retry, timeout, community,
            agentAddress.getHostAddress() + "/" + port);

        startTime = System.currentTimeMillis();
    }

    public CommunityTarget getTarget(int version, int retry, int timeout,
        String community, String address) {
        CommunityTarget comTarget = new CommunityTarget();
        if (community != null)
            comTarget.setCommunity(new OctetString(community));
        else
            comTarget.setCommunity(new OctetString("public"));
        if (version == 2)
            comTarget.setVersion(SnmpConstants.version2c);
        else if (version == 3)
            comTarget.setVersion(SnmpConstants.version3);
        else
            comTarget.setVersion(SnmpConstants.version1);
        comTarget.setAddress(new UdpAddress(address));
        comTarget.setRetries(retry);
        comTarget.setTimeout(timeout);
        return comTarget;
    }

    /** returns a string of "oid = value" for the community and the address */
    public String snmpInquire(int version, String community, String address,
        String oid) throws IOException {
        CommunityTarget target = getTarget(version, retry, timeout,
            community, address);
        return snmpGet(target, oid);
    }

    /** returns a string of "oid = value" or an error message for failure */
    public String snmpGet(CommunityTarget target, String oid)
        throws IOException {
        String str = null;
        Variable variable;

        PDU pdu = new PDU();
        ResponseEvent response;
        pdu.add(new VariableBinding(new OID(oid)));
        pdu.setType(PDU.GET);
        if (target != null)
            response = snmp.get(pdu, target);
        else
            response = snmp.get(pdu, comTarget);
        if (response != null) {
            pdu = response.getResponse();
            str = pdu.getErrorStatusText();
            if ("Success".equalsIgnoreCase(str)) {
                variable = ((VariableBinding)
                    pdu.getVariableBindings().firstElement()).getVariable();
                str = oid + " = " + variable.getSyntaxString() + ": " +
                    variable.toString();
            }
        }
        else
            str = "timedout";

        return str;
    }

    public String snmpSet(String oid, int value) throws IOException {
        return snmpSet(null, new String[]{oid, "i", String.valueOf(value)});
    }

    /**
     * It sets a value on oid and returns a string of null upon success or
     * an error message for failure.
     */
    public String snmpSet(int version, String community, String address,
        String[] data) throws IOException {
        CommunityTarget target = getTarget(version, retry, timeout,
            community, address);
        return snmpSet(target, data);
    }

    /**
     * It sets a value on oid and returns a string of null upon success or
     * an error message for failure.
     */
    public String snmpSet(CommunityTarget target, String[] data)
        throws IOException {
        int k;
        long l;
        String str = null;

        if (data == null || data.length < 3 || data[0] == null ||
            data[0].length() <= 0 || data[1] == null || data[1].length() <= 0)
            return "data error";
        PDU pdu = new PDU();
        ResponseEvent response;
        switch (data[1].charAt(0)) {
          case 'o':
            pdu.add(new VariableBinding(new OID(data[0]), new OID(data[2])));
            break;
          case 'i':
            k = Integer.parseInt(data[2]);
            pdu.add(new VariableBinding(new OID(data[0]), new Integer32(k)));
            break;
          case 'u':
            k = Integer.parseInt(data[2]);
            pdu.add(new VariableBinding(new OID(data[0]),
                new UnsignedInteger32(k)));
            break;
          case 't':
            l = Long.parseLong(data[2]);
            pdu.add(new VariableBinding(new OID(data[0]), new TimeTicks(l)));
            break;
          case 'c':
            k = Integer.parseInt(data[2]);
            pdu.add(new VariableBinding(new OID(data[0]), new Counter32(k)));
            break;
          case 'n':
            pdu.add(new VariableBinding(new OID(data[0]), new Null()));
            break;
          case 'a':
            pdu.add(new VariableBinding(new OID(data[0]),
                new IpAddress(data[2])));
            break;
          case 's':
          case 'x':
          case 'd':
          case 'b':
          default:
            pdu.add(new VariableBinding(new OID(data[0]),
                new OctetString(data[2])));
            break;
        }
        pdu.setType(PDU.SET);
        if (target != null)
            response = snmp.set(pdu, target);
        else
            response = snmp.set(pdu, comTarget);
        if (response != null) {
            PDU pduResponse = response.getResponse();
            str = pduResponse.getErrorStatusText();
            if ("success".equalsIgnoreCase(str))
                str = null;
        }
        else
            str = "timeout";

        return str;
    }

    /** sends out a trap of multiple oids to the address */
    public void snmpTrap(int version, String community, String address,
        String ip,int generic,int specific,String[][] data) throws IOException{
        CommunityTarget target = getTarget(version, retry, timeout,
            community, address);
        sendTrap(target, ip, generic, specific, data); 
    }

    /** sends out a trap for multiple oids */
    public void sendTrap(CommunityTarget target, String ip, int generic,
        int specific, String[][] data) throws IOException {
        if (comTarget.getVersion() != SnmpConstants.version1)
            throw(new IOException("wrong version"));
        if (ip == null || ip.length() <= 0)
            ip = hostIP;
        PDUv1 pdu = new PDUv1();
        pdu.setType(PDU.V1TRAP);
        pdu.setEnterprise(enterpriseOID);
        pdu.setGenericTrap(generic);
        pdu.setSpecificTrap(specific);
        pdu.setTimestamp((System.currentTimeMillis() - startTime) / 10);
        pdu.setAgentAddress(new IpAddress(ip));
        if (data != null && data.length > 0) {
            int i, k;
            long l;
            for (i=0; i<data.length; i++) {
                if (data[i] == null || data[i].length < 3)
                    continue;
                if (data[i][0] == null || data[i][0].length() <= 0)
                    continue;
                if (data[i][1] == null || data[i][1].length() <= 0)
                    continue;
                switch (data[i][1].charAt(0)) {
                    case 'o':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new OID(data[i][2])));
                      break;
                    case 'i':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Integer32(k)));
                      break;
                    case 'u':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new UnsignedInteger32(k)));
                      break;
                    case 't':
                      l = Long.parseLong(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new TimeTicks(l)));
                      break;
                    case 'c':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Counter32(k)));
                      break;
                    case 'n':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Null()));
                      break;
                    case 'a':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new IpAddress(data[i][2])));
                      break;
                    case 's':
                    case 'x':
                    case 'd':
                    case 'b':
                    default:
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new OctetString(data[i][2])));
                      break;
                }
            }
        }
        if (target != null)
            snmp.trap(pdu, target);
        else
            snmp.trap(pdu, comTarget);
    }

    /** sends a notification of multiple oids to the address */
    public void snmpNotify(int version, String community, String address,
        String[][] data) throws IOException {
        CommunityTarget target = getTarget(version, retry, timeout,
            community, address);
        snmpNotify(target, data);
    }

    /** sends a notification for multiple oids */
    public void snmpNotify(CommunityTarget target, String[][] data)
        throws IOException {
        if (comTarget.getVersion() == SnmpConstants.version1)
            throw(new IOException("wrong version"));
        PDU pdu = new PDU();
        pdu.setType(PDU.NOTIFICATION);
        pdu.add(new VariableBinding(SnmpConstants.sysUpTime,
            new TimeTicks((System.currentTimeMillis() - startTime) / 10)));
        pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, enterpriseOID));
        if (data != null && data.length > 0) {
            int i, k;
            long l;
            for (i=0; i<data.length; i++) {
                if (data[i] == null || data[i].length < 3)
                    continue;
                if (data[i][0] == null || data[i][0].length() <= 0)
                    continue;
                if (data[i][1] == null || data[i][1].length() <= 0)
                    continue;
                switch (data[i][1].charAt(0)) {
                    case 'o':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new OID(data[i][2])));
                      break;
                    case 'i':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Integer32(k)));
                      break;
                    case 'u':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new UnsignedInteger32(k)));
                      break;
                    case 't':
                      l = Long.parseLong(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new TimeTicks(l)));
                      break;
                    case 'c':
                      k = Integer.parseInt(data[i][2]);
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Counter32(k)));
                      break;
                    case 'n':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new Null()));
                      break;
                    case 'a':
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new IpAddress(data[i][2])));
                      break;
                    case 's':
                    case 'x':
                    case 'd':
                    case 'b':
                    default:
                      pdu.add(new VariableBinding(new OID(data[i][0]),
                          new OctetString(data[i][2])));
                      break;
                }
            }
        }
        if (target != null)
            snmp.notify(pdu, target);
        else
            snmp.notify(pdu, comTarget);
    }

    public String[] receive() throws IOException {
        return receive(null);
    }

    /** receives an SNMP packet and returns the content in array of strings */
    public String[] receive(DatagramPacket p) throws IOException {
        if (!(transport instanceof SimpleUdpTransportMapping))
            throw(new IllegalArgumentException("wrong transport"));

        if (p == null)
            p = new DatagramPacket(new byte[8192], 8192);
        return ((SimpleUdpTransportMapping) transport).receive(p);
    }

    public int getDefaultVersion() {
        return defaultVersion;
    }

    public boolean isListening() {
        return isListening;
    }

    public String getURI() {
        return uri;
    }

    /** returns true since it is connectionless  */
    public boolean isConnected() {
        return true;
    }

    /** reinitializes session */
    public String reconnect() {
        close();
        try {
            UdpAddress targetAddress = new UdpAddress(agentAddress, port);
            if ("listen".equalsIgnoreCase(operation)) {
                transport =
                    new SimpleUdpTransportMapping(targetAddress);
               ((SimpleUdpTransportMapping)transport).setSocketTimeout(timeout);
                snmp = new Snmp(transport);
                snmp.addCommandResponder((CommandResponder) transport);
            }
            else if ("notify".equalsIgnoreCase(operation)) {
                transport = new DefaultUdpTransportMapping();
                snmp = new Snmp(transport);
            }
            else {
                transport = new DefaultUdpTransportMapping();
                snmp = new Snmp(transport);
                transport.listen();
            }
            isListening = transport.isListening();
        }
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    /** closes all transports and resources */
    public void close() {
         isListening = false;
         if (snmp != null) try {
             snmp.close();
         }
         catch (Exception e) {
         }
         snmp = null;
    }

    public static void main(String args[]) {
        int i, number = 1, timeout = 2, port = 161;
        int bufferSize = 8192, sleepTime = 2000, wait = 0, version = 1;
        String msg = null, hostname = null, str, oid = null;
        String[] content;
        SNMPConnector conn = null;
        Map<String, Object> props = new HashMap<String, Object>();
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
                    hostname = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    bufferSize = Integer.parseInt(args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
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
              case 'o':
                if (i + 1 < args.length)
                    oid = args[++i];
                break;
              case 'v':
                if (i+1 < args.length)
                    version = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        try {
            if (oid == null && msg == null) { // server mode
                props.put("URI", "snmp://"+ "0.0.0.0:" + port);
                props.put("Operation", "listen");
                props.put("SOTimeout", String.valueOf(timeout));
                conn = new SNMPConnector(props);
                for (i=0; i<number; i++) {
                    content = conn.receive();
                    if (content.length == 0)
                        System.out.println("timed out in " + timeout + " sec");
                    else {
                        for (int j=0; j<content.length; j++) {
                            if (j == 0)
                                System.out.print("from: ");
                            else if (j == 1)
                                System.out.print("type: ");
                            System.out.println(content[j]);
                        }
                    }
                    if (sleepTime > 0) try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                    }
                }
            }
            else if (msg != null) {
                props.put("URI", "snmp://"+ hostname + ":" + port);
                props.put("Operation", "notify");
                if (version == 2)
                    props.put("Version", "2c");
                else
                    props.put("Version", "1");
                conn = new SNMPConnector(props);
                if (version == 2)
                    conn.snmpNotify(null, new String[][]{{oid, "s", msg}});
                else
                    conn.sendTrap(null,null,6,1,new String[][]{{oid,"s",msg}});
            }
            else {
                props.put("URI", "snmp://"+ hostname + ":" + port);
                props.put("Operation", "inquire");
                props.put("SOTimeout", String.valueOf(timeout));
                if (version == 2)
                    props.put("Version", "2c");
                else
                    props.put("Version", "1");
                conn = new SNMPConnector(props);
                str = conn.snmpGet(null, oid);
                if (str != null)
                    System.out.println(str);
            }
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (conn != null) try {
                conn.close();
            }
            catch (Exception ex) {
            }
        }
    }

    private static void printUsage() {
        System.out.println("SNMPConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("SNMPConnector: an SNMP connector for sending or receiving SNMP messages");
        System.out.println("Usage: java org.qbroker.net.SNMPConnetor -h hostname -p port -t timeout -o OID [message]");
        System.out.println("  -?: print this message");
        System.out.println("  -h: hostname or IP for send or receive");
        System.out.println("  -p: port for send or receive");
        System.out.println("  -s: buffer size for receive (default: 8192)");
        System.out.println("  -t: timeout for receive in sec (default: 2)");
        System.out.println("  -T: sleepTime for send in sec (default: 2)");
        System.out.println("  -w: wait for reply (0: no, 1: yes; default: 0)");
        System.out.println("  -n: number of operations (default: 1)");
        System.out.println("  -o: OID for inquire (default for receive)");
        System.out.println("  -v: version of SNMP (default: 1)");
        System.out.println("  msg: empty for receive)");
    }
}
