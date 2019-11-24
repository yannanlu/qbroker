package org.qbroker.common;

/**
 * UniqueID generates unique and monotonously increaing IDs.
 * It supports three different uniqueness scopes, JVM wide,
 * machine wide and globe wide.
 *<br><br>
 * The unique ID is a 16 byte HexString on globe scope, 12
 * byte HexString on machine scope and 10 byte HexString
 * on JVM scope.  For a full length HexString, the first
 * 8 bytes are for timestamp, the next 2 byes for
 * sequence number and next 2 byes for pid of JVM and
 * the last 4 bytes for the hostid of the machine.
 * Currently, hostid is not implemented yet.
 *<br><br>
 * Since Win2K does not support hostid, you should use the
 * IP address to identify the machine.  Please remember, there may
 * be overlap between some hostid and ip.  Therefore, please use ip
 * across entire cluster if there is any Win2K node.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class UniqueID {
    private int scope;
    private int pid;
    private int hostid;
    private int sequenceID = 0;
    private long timestamp;

    private String pidStr;
    private String hostidStr;

    private final static int PID;
    private final static int HID;
    public final static int SCOPE_JVM = 0;
    public final static int SCOPE_MACHINE = 1;
    public final static int SCOPE_GLOBE = 2;
    public final static int SCOPE_UNIVERSE = 3;

    public UniqueID (int scope) {
        if (scope >= SCOPE_JVM && scope <= SCOPE_UNIVERSE)
            this.scope = scope;
        else
            throw(new IllegalArgumentException("illegal scope: " + scope));

        if (scope == SCOPE_UNIVERSE) {
            hostid = HID;
            hostidStr = Integer.toHexString(hostid);
        }
        else if (scope == SCOPE_GLOBE) {
            byte[] buffer;
            try {
                buffer = java.net.InetAddress.getLocalHost().getAddress();
            }
            catch (java.net.UnknownHostException e) {
                buffer = new byte[] {0, 0, 0, 0};
            }
            hostidStr = toString(buffer);
        }
        else
            hostidStr = "00000000";

        switch (hostidStr.length()) {
          case 0:
            hostidStr = "00000000";
            break;
          case 1:
            hostidStr = "0000000" + hostidStr;
            break;
          case 2:
            hostidStr = "000000" + hostidStr;
            break;
          case 3:
            hostidStr = "00000" + hostidStr;
            break;
          case 4:
            hostidStr = "0000" + hostidStr;
            break;
          case 5:
            hostidStr = "000" + hostidStr;
            break;
          case 6:
            hostidStr = "00" + hostidStr;
            break;
          case 7:
            hostidStr = "0" + hostidStr;
            break;
          default:
            hostidStr = hostidStr.substring(0, 8);
        }

        if (scope >= SCOPE_MACHINE)
            pid = PID;
        else
            pid = 0;

        pidStr = Integer.toHexString(pid & 0xFFFF);
        switch (pidStr.length()) {
          case 0:
            pidStr = "0000";
            break;
          case 1:
            pidStr = "000" + pidStr;
            break;
          case 2:
            pidStr = "00" + pidStr;
            break;
          case 3:
            pidStr = "0" + pidStr;
            break;
          default:
        }

        timestamp = System.currentTimeMillis();
        sequenceID = 0;
    }

    public synchronized String getNext() {
        long t = System.currentTimeMillis();
        if (t > timestamp) {
            timestamp = t;
            sequenceID = 0;
        }
        else
            sequenceID ++;

        StringBuffer strBuf = new StringBuffer(Long.toHexString(timestamp));
        String str = Integer.toHexString(sequenceID & 0xFFFF);
        switch (str.length()) {
          case 0:
            strBuf.append("0000");
            break;
          case 1:
            strBuf.append("000" + str);
            break;
          case 2:
            strBuf.append("00" + str);
            break;
          case 3:
            strBuf.append("0" + str);
            break;
          default:
            strBuf.append(str);
        }

        if (scope == SCOPE_JVM)
            return strBuf.toString();
        else if (scope == SCOPE_MACHINE) {
            strBuf.append(pidStr);
            return strBuf.toString();
        }
        strBuf.append(pidStr);
        strBuf.append(hostidStr);

        return strBuf.toString();
    }

    public static byte[] toBytes(String uid) {
        int n;
        byte[] b;
        if (uid == null)
            return null;
        n = uid.length();
        if (n%2 == 0) {
            n /= 2;
            b = new byte[n];
            for (int i=0; i<n; i++)
                b[i] = Byte.parseByte(uid.substring(i+i, i+i+2));
        }
        else {
            n /= 2;
            b = new byte[n+1];
            b[0] = Byte.parseByte(uid.substring(0, 1));
            for (int i=1; i<=n; i++)
                b[i] = Byte.parseByte(uid.substring(i+i-1, i+i+1));
        }
        return b;
    }

    public static String toString(byte[] b) {
        StringBuffer strBuf = new StringBuffer();
        if (b == null)
            return null;
        for (int i=0; i<b.length; i++) {
            int x = (int) (b[i] & 0xFF);
            if (x < 0x10)
                strBuf.append("0" + Integer.toString(x, 16));
            else
                strBuf.append(Integer.toString(x, 16));
        }
        return strBuf.toString();
    }

    static {
        String str =
            java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        PID = Integer.parseInt(str.split("@")[0]);
        HID = 0;
    }
}
