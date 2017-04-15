package org.qbroker.monitor;

/* FTPTester.java - a monitor to test access to an ftp server  */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.net.ClientSocket;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * FTPTester tests an ftp server to see if it hangs or it is down
 *<br/><br/>
 * It provides a test method, testFTPD() for ftp servers.  This method
 * throws exceptions if the internal error occurs.   Otherwise it returns an
 * integer indicating the status of the ftp server.  If the return code
 * is not zero, the response will be stored in the last responseText or
 * the one where failure occurs.
 *<br/><br/>
 * Status     Code         Description<br/>
 *   -1     TESTFAILED     unexpected system errors<br/>
 *   0      TESTOK         server is OK<br/>
 *   1      PROTOCOLERROR  protocol error, probably wrong port<br/>
 *   2      CMDFAILED      ftp command failed<br/>
 *   3      FILENOTFOUND   file not found on server<br/>
 *   4      CLIENTERROR    client error, such as permissin issues<br/>
 *   5      SERVERERROR    ftp server internal error<br/>
 *   6      READFAILED     failed to read from server<br/>
 *   7      WRITEFAILED    failed to write to server<br/>
 *   8      CONNTIMEOUT    connection timeout, probably server is busy<br/>
 *   9      CONNREFUSED    connection refused, probably server is down<br/>
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FTPTester extends Report {
    private String uri, host;
    private byte[][] request;
    private String[] responseText;
    private int[] responseCode;
    private int port;
    private int timeout;
    private byte[] buffer = new byte[0];
    private int bufferSize;
    private Pattern ftpPattern = null;
    private Pattern ftpOrPattern = null;
    private Pattern pattern = null;
    public final static int TESTFAILED = -1;
    public final static int TESTOK = 0;
    public final static int PROTOCOLERROR = 1;
    public final static int CMDFAILED = 2;
    public final static int FILENOTFOUND = 3;
    public final static int CLIENTERROR = 4;
    public final static int SERVERERROR = 5;
    public final static int READFAILED = 6;
    public final static int WRITEFAILED = 7;
    public final static int CONNTIMEOUT = 8;
    public final static int CONNREFUSED = 9;

    public FTPTester(Map props) {
        super(props);
        Object o;
        List list;
        URI u;
        String user, password;

        if (type == null)
            type = "FTPTester";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"ftp".equals(u.getScheme()))
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 21;

        if ((host = u.getHost()) == null || host.length() == 0)
            throw(new IllegalArgumentException("no host specified in URL"));

        if ((o = props.get("User")) == null)
            throw(new IllegalArgumentException("User is not defined"));
        user = (String) o;

        if ((o = props.get("Password")) == null)
            throw(new IllegalArgumentException("Password is not defined"));
        password = (String) o;

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("Request")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();

        request = new byte[list.size() + 3][];
        request[0] = new String("USER " + user + "\r\n").getBytes();
        request[1] = new String("PASS " + password + "\r\n").getBytes();
        for (int i=0; i<list.size(); i++)
            request[i+2] = ((String) list.get(i)).getBytes();
        request[request.length-1] = new String("QUIT" + "\r\n").getBytes();
        responseText = new String[request.length];
        responseCode = new int[request.length];

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            pattern = pc.compile("(^220 .+\\r\\n|\\n220 .+\\r\\n)");
            ftpPattern = pc.compile("^(\\d\\d\\d) (.+)\\r$");
            ftpOrPattern = pc.compile("\\n(\\d\\d\\d) (.+)\\r$");
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
        report.put("ResponseCode", responseCode);

        returnCode = testFTPD();

        report.put("ReturnCode", String.valueOf(returnCode));
        if ((disableMode > 0 && returnCode != TESTOK) ||
            (disableMode < 0 && returnCode == TESTOK))
            skip = DISABLED;

        return report;
    }

    private int testFTPD() {
        long timeStart;
        int i, j, n, totalBytes, bytesRead, timeLeft, waitTime;
        String response;
        StringBuffer strBuf = new StringBuffer();
        boolean hasNew = false;
        Socket s = null;
        InputStream in = null;
        OutputStream out = null;

        totalBytes = 0;
        waitTime = 500;
        bytesRead = 0;
        n = 0;

        for (i=0; i<request.length; i++) {
            responseText[i] = null;
            responseCode[i] = -1;
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
        try {
            do { // read everything until EOF or pattern hit or timeout
                bytesRead = 0;
                hasNew = false;
                if (s != null) try {
                    n ++;
                    s.setSoTimeout(waitTime);
                }
                catch (Exception ex) {
                }
                try {
                    while ((bytesRead = in.read(buffer, 0, bufferSize)) > 0) {
                        strBuf.append(new String(buffer, 0, bytesRead));
                        totalBytes += bytesRead;
                        hasNew = true;
                        for (i=bytesRead-1; i>=0; i--) // check newline
                            if (buffer[i] == '\n')
                                break;
                        if (i >= 0) // found the newline
                            break;
                    }
                }
                catch (InterruptedIOException ex) { // timeout or interrupt
                }
                if (hasNew && pm.contains(strBuf.toString(), pattern))
                    break;
                timeLeft = timeout-(int) (System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
        }
        catch (IOException e) {
            try {
                out.write(request[request.length-1]);
                out.flush();
            }
            catch (IOException ex) {
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
        if (timeLeft <= 0 || bytesRead < 0) {
            if (strBuf.length() > 0)
                responseText[request.length-1] = strBuf.toString();
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return ((bytesRead < 0) ? SERVERERROR : CONNTIMEOUT);
        }

        timeStart = System.currentTimeMillis();
        for (i=0; i<request.length; i++) {
            strBuf.delete(0, strBuf.length());
            totalBytes = 0;
            n = 0;
            try {
                out.write(request[i]);
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
                    hasNew = false;
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
                            hasNew = true;
                            for (j=bytesRead-1; j>=0; j--) // check newline
                                if (buffer[j] == '\n')
                                    break;
                            if (j >= 0) // found the newline
                                break;
                        }
                    }
                    catch (InterruptedIOException ex) {
                    }
                    if (hasNew &&
                        (pm.contains(strBuf.toString(), ftpPattern) ||
                        pm.contains(strBuf.toString(), ftpOrPattern)))
                        break;
                    timeLeft = timeout-(int) (System.currentTimeMillis()-
                        timeStart);
                } while (bytesRead >= 0 && timeLeft > 0);
            }
            catch (Exception e) {
                if (i != request.length - 1) try {
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
            if (timeLeft <= 0 || (i != request.length - 1 && bytesRead < 0)) {
                if (strBuf.length() > 0)
                    responseText[request.length-1] = strBuf.toString();
                close(out);
                close(in);
                close(s);
                s = null;
                in = null;
                out = null;
                return ((timeLeft <= 0) ? READFAILED : SERVERERROR);
            }
            response = strBuf.toString();
            MatchResult mr = pm.getMatch();
            responseCode[i] = Integer.parseInt(mr.group(1));
            responseText[i] = mr.group(2);
            if (responseCode[i] >= 500) {
                close(out);
                close(in);
                close(s);
                s = null;
                in = null;
                out = null;
                return SERVERERROR;
            }
            else if (responseCode[i] >= 400) {
                close(out);
                close(in);
                close(s);
                s = null;
                in = null;
                out = null;
                return CLIENTERROR;
            }
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

    // update i-th request byte string
    public String updateRequest(int i, String request) {
        String original;
        int size = this.request.length;
        if (i < 0 || i >= size || request == null || request.length() < 5)
            return null;
        original = new String(this.request[i]);
        this.request[i] = request.getBytes();
        return original;
    }
}
