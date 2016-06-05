package org.qbroker.net;

/* SimpleUdpTransportMapping.java - simple UDP transport for SNMP */

import java.util.Vector;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.SNMP4JSettings;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/**
 * <code>SimpleUdpTransportMapping</code> extends the SNMP4J Default
 * UDP transport mapping and implements CommandResponder for listening on
 * the inbound socket.  It has a method of receive() which returns an array
 * of strings with first two members of type and address.  It should be used
 * only if the listener is not running.  It requires Java 1.4 or above.
 *<br/>
 * @author Yannanlu@yahoo.com
 */

public class SimpleUdpTransportMapping extends DefaultUdpTransportMapping
    implements CommandResponder {
    private String[] content = null;
    private final static String[] TIMEOUT = new String[0];
    private final static String[] TRAPID_TEXT = new String[] {
        "Cold Start Trap", "Warm Start Trap", "Link Down Trap", "Link Up Trap",
        "Authentication Failure Trap", "EGP Neighbor Loss Trap",
        "Enterprise Specific Trap", "Unknown Type Trap"
    };

    public SimpleUdpTransportMapping() throws IOException {
        super(new UdpAddress(InetAddress.getLocalHost(), 0));
    }

    public SimpleUdpTransportMapping(UdpAddress udpAddress) throws IOException {
        super(udpAddress);
    }

    /**
     * It is the default callback method for processing incoming request,
     * report and notification PDU.  It just resets the content with the
     * data from the received PDU.  It should be added as the CommandResponder
     * to SNMP session.
     */
    public void processPdu(CommandResponderEvent e) {
        PDU command;
        Vector list;
        VariableBinding vb;
        Variable variable;
        String str, key;
        int i, j, k, n;
        content = null;
        if (e == null)
            return;
        command = e.getPDU();
        if (command != null) {
            list = command.getVariableBindings();
            n = list.size();
            key = PDU.getTypeString(command.getType());
            k = ("V1TRAP".equals(key)) ? 4 : 2;
            content = new String[n + k];
            content[0] = e.getPeerAddress().toString();
            content[1] = key;
            if (k > 2) { // V1TRAP
                long tm = ((PDUv1) command).getTimestamp() * 10;
                content[2] = "UpTime = Timeticks: " + tm;
                str = ((PDUv1) command).getEnterprise().toString();
                i = ((PDUv1) command).getGenericTrap();
                if (i < 0 || i > 6)
                    i = 7;
                j = ((PDUv1) command).getSpecificTrap();
                content[3] = str + " = STRING: " + TRAPID_TEXT[i] +
                    " (" + j + ")";
            }
            for (i=0; i<n; i++) {
                vb = (VariableBinding) list.elementAt(i);
                if (vb != null) {
                    key = vb.getOid().toString();
                    variable = vb.getVariable();
                }
                else {
                    content[i+k] = null;
                    continue;
                }
                if (variable != null) {
                    str = variable.getSyntaxString();
                    content[i+k] = key + " = " + str +": "+variable.toString();
                }
                else {
                    content[i+k] = key + " = NULLOBJ: null";
                }
            }
            if (!"RESPONSE".equals(content[0]))
                e.setProcessed(true);
        }
    }

    /**
     * It receives an SNMP packet and invokes the callback method to process
     * it. Upon success, it returns an array of strings whose first two
     * items are the peer address and the type of PDU.  The rest of the
     * array are the content in the form of "OID = value".  In case of
     * timeout, it returns an empty string array.  In case of failure, it
     * throws IOException or returns null.
     */
    public String[] receive(DatagramPacket packet) throws IOException {
        int t;
        if (socket == null)
            throw(new IOException("socket is null"));
        if (packet == null)
            throw(new IOException("packet is null"));
        if (isListening())
            throw(new IOException("listener is running"));
        try {
            if ((t = getSocketTimeout()) > 0)
                socket.setSoTimeout(t);
            socket.receive(packet);
        }
        catch (InterruptedIOException e) {
            return TIMEOUT;
        }
        catch (IOException e) {
            throw(e);
        }
        catch (Exception e) {
            throw(new RuntimeException(e.toString()));
        }
        catch (Error e) {
            throw(e);
        }
        content = null;
        try {
            fireProcessMessage(new UdpAddress(packet.getAddress(),
                packet.getPort()), ByteBuffer.wrap(packet.getData()));
        }
        catch (Exception e) {
            throw(new RuntimeException(e.toString()));
        }
        catch (Error e) {
            throw(e);
        }
        if (content != null) // content is set by processPdu()
           return content;
        else
           return TIMEOUT;
    }
}
