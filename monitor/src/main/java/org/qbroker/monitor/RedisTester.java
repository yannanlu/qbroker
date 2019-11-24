package org.qbroker.monitor;

/* RedisTester.java - a tester to get info from a Redis server */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.net.ClientSocket;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * RedisTester connects to a Redis server and sends a request of "info" for
 * status check.
 *<br><br>
 * It provides a test method, testRedis() for the service.  This method
 * throws exceptions if the internal error occurs.   Otherwise it returns an
 * integer indicating the status of the MessageBus.  If the return code
 * is not zero, the response will be stored in the last responseText.
 *<br><br>
 * Status     Code         Description<br>
 *   -1     TESTFAILED     unexpected system errors<br>
 *   0      TESTOK         MessageBus is OK<br>
 *   1      PROTOCOLERROR  protocol error, probably wrong port<br>
 *   2      CMDFAILED      MessageBus command failed<br>
 *   3      CHLNOTFOUND    channel not found on server<br>
 *   4      CLIENTERROR    client error<br>
 *   5      SERVERERROR    MessageBus internal error<br>
 *   6      READFAILED     failed to read from server<br>
 *   7      WRITEFAILED    failed to write to server<br>
 *   8      CONNTIMEOUT    connection timeout, probably server is busy<br>
 *   9      CONNREFUSED    connection refused, probably server is down<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public class RedisTester extends Report {
    private String uri, host;
    private byte[][] request;
    private String[] responseText;
    private int port;
    private int timeout;
    private byte[] buffer = new byte[0];
    private int bufferSize;
    private Pattern pattern = null;
    public final static int TESTFAILED = -1;
    public final static int TESTOK = 0;
    public final static int PROTOCOLERROR = 1;
    public final static int CMDFAILED = 2;
    public final static int CHLNOTFOUND = 3;
    public final static int CLIENTERROR = 4;
    public final static int SERVERERROR = 5;
    public final static int READFAILED = 6;
    public final static int WRITEFAILED = 7;
    public final static int CONNTIMEOUT = 8;
    public final static int CONNREFUSED = 9;

    public RedisTester(Map props) {
        super(props);
        Object o;
        URI u;
        String user, password;
        int n;

        if (type == null)
            type = "RedisMonitor";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"tcp".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 6379;

        if ((host = u.getHost()) == null || host.length() == 0)
            throw(new IllegalArgumentException("no host specified in URL"));

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        request = new byte[2][];
        request[0] = new String("info" + "\r\n").getBytes();
        request[1] = new String("quit" + "\r\n").getBytes();
        responseText = new String[request.length];

        try {
            Perl5Compiler pc = new Perl5Compiler();

            pattern = pc.compile("\r\n\r\n");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("BufferSize")) == null ||
            (bufferSize = Integer.parseInt((String) o)) <= 0)
            bufferSize = 4096;

        buffer = new byte[bufferSize];
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int returnCode = -1;

        report.clear();
        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                return report;
            }
            else {
                skip = NOSKIP;
                serialNumber ++;
            }
        }
        else {
            skip = NOSKIP;
            serialNumber ++;
        }

        if (dependencyGroup != null) { // check dependency
            skip = MonitorUtils.checkDependencies(currentTime, dependencyGroup,
                name);
            if (skip != NOSKIP) {
                if (skip == EXCEPTION) {
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                    return report;
                }
                else if (skip == SKIPPED)
                    return report;
                else if (!disabledWithReport)
                    return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        report.put("ResponseText", responseText);

        returnCode = testRedis();

        report.put("ReturnCode", String.valueOf(returnCode));
        if ((disableMode > 0 && returnCode != TESTOK) ||
            (disableMode < 0 && returnCode == TESTOK))
            skip = DISABLED;

        return report;
    }

    private int testRedis() {
        long timeStart;
        int i, n, len, totalBytes, bytesRead, timeLeft, waitTime, byteCount;
        String response;
        StringBuffer strBuf = new StringBuffer();
        Socket s = null;
        InputStream in = null;
        OutputStream out = null;

        byteCount = -1;
        totalBytes = 0;
        waitTime = 500;
        bytesRead = 0;
        n = 0;

        for (i=0; i<request.length; i++) {
            responseText[i] = null;
        }

        timeStart = System.currentTimeMillis();
        try {
            s = ClientSocket.connect(host, port, timeout);
        }
        catch (Exception e) {
            return CONNREFUSED;
        }
        timeLeft = timeout - (int) (System.currentTimeMillis() - timeStart);
        if (s == null) {
            if (timeLeft <= 0)
                return CONNTIMEOUT;
            else
                return CONNREFUSED;
        }
        if (timeLeft <= 0) {
            close(s);
            s = null;
            return CONNTIMEOUT;
        }
        try {
            in = s.getInputStream();
            out = s.getOutputStream();
        }
        catch (Exception e) {
            close(out);
            close(in);
            close(s);
            s = null;
            out = null;
            in = null;
            return TESTFAILED;
        }
        if (in == null || out == null) {
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return TESTFAILED;
        }

        timeStart = System.currentTimeMillis();
        strBuf.delete(0, strBuf.length());
        totalBytes = 0;
        n = 0;
        try {
            out.write(request[0]);
            out.flush();
        }
        catch (IOException e) {
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return WRITEFAILED;
        }

        timeLeft = timeout-(int) (System.currentTimeMillis()-timeStart);
        try {
            do { // read everything until EOF or pattern hit or timeout
                bytesRead = 0;
                if (in.available() <= 0) try {
                    Thread.sleep(20);
                }
                catch (Exception ex) {
                }
                if (s != null) try {
                    n --;
                    s.setSoTimeout(waitTime);
                }
                catch (Exception ex) {
                }
                try {
                    while((bytesRead = in.read(buffer, 0, bufferSize)) > 0){
                        strBuf.append(new String(buffer,0,bytesRead));
                        totalBytes += bytesRead;
                        if (byteCount < 0 && (i = strBuf.indexOf("\r\n")) > 0) {
                            if (strBuf.charAt(0) == '$')
                                byteCount =
                                    Integer.parseInt(strBuf.substring(1, i));
                            else if (strBuf.charAt(0) == '-') // error
                                throw(new IOException(strBuf.toString()));
                        }
                        else if (totalBytes >= byteCount)
                            break;
                        if (in.available() <= 0)
                            break;
                    }
                }
                catch (InterruptedIOException ex) {
                }
                if (byteCount > 0 && totalBytes >= byteCount)
                    break;
                timeLeft = timeout-(int) (System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
        }
        catch (Exception e) {
            try {
                out.write(request[request.length-1]);
                out.flush();
            }
            catch (Exception ex) {
            }
            if (strBuf.length() > 0)
                responseText[request.length-1] = strBuf.toString();
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return READFAILED;
        }
        if (i > 0) {
            strBuf.delete(0, i+2);
            len = strBuf.length();
            strBuf.delete(len-2, len);
            response = strBuf.toString();
            responseText[0] = response;
        }
        else
            responseText[0] = strBuf.toString();
        try {
            out.write(request[request.length-1]);
            out.flush();
        }
        catch (Exception ex) {
        }
        close(out);
        close(in);
        close(s);
        s = null;
        in = null;
        out = null;
        return TESTOK;
    }

    private void close(Object o) {
        if (o != null) {
            if (o instanceof Socket) { 
                try {
                    ((Socket) o).close();
                }
                catch (Exception e) {
                }
            }
            else if (o instanceof InputStream) {
                try {
                    ((InputStream) o).close();
                }
                catch (Exception e) {
                }
            }
            else if (o instanceof OutputStream) {
                try {
                    ((OutputStream) o).close();
                }
                catch (Exception e) {
                }
            }
        }
    }

    public static void main(String args[]) {
        Map<String, Object> props, r;
        int i, timeout = 60;
        String str, uri = null;
        RedisTester conn = null;
        if (args.length == 0) {
            System.exit(0);
        }
        props = new HashMap<String, Object>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                System.exit(0);
                break;
              case 'u':
                if (i+1 < args.length)
                    uri = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (uri == null) {
            System.exit(0);
        }
        else
            props.put("URI", uri);
        props.put("Timeout", String.valueOf(timeout));
        props.put("Name", "Test");

        try {
            conn = new RedisTester(props);
            r = conn.generateReport(System.currentTimeMillis());
            str = (String) r.get("ReturnCode");
            if (str != null)
                i = Integer.parseInt(str);
            else
                i = -1;
            if (i != 0)
                System.out.print("ErrorCode: " + i + " ");
            else {
                String[] response = (String[]) r.get("ResponseText");
                System.out.println(response[0]);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
