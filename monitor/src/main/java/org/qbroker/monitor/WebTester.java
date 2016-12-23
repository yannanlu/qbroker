package org.qbroker.monitor;

/* WebTester.java - a monitor checking socket */

import java.util.Map;
import java.util.Date;
import java.util.Properties;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
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
import org.qbroker.common.Base64Encoder;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.net.ClientSocket;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;

/**
 * WebTester sends requests via a client socket and gets response back.
 * It supports protocols of http, https and generic tcp sockets.
 *<br/><br/>
 * It provides a test method, testHTTPD() for web servers.  This method
 * throws exceptions if the internal error occurs.   Otherwise it returns an
 * integer indicating the status of the web server.
 *<br/><br/>
 * Status     Code         Description<br/>
 *   0      TESTOK         server is OK<br/>
 *   1      PROTOCOLERROR  protocol error, probably wrong port<br/>
 *   2      PATTERNNOHIT   pattern not matched, wrong page or server error<br/>
 *   3      NOTMULTIPART   not a multipart<br/>
 *   4      CLIENTERROR    client error, such as page not found, broken link<br/>
 *   5      SERVERERROR    web server internal error<br/>
 *   6      READFAILED     failure in read, probably server is busy<br/>
 *   7      WRITEFAILED    failure in write, probably server is busy<br/>
 *   8      CONNTIMEOUT    connection timeout, probably server is busy<br/>
 *   9      CONNREFUSED    connection refused, probably server is down<br/>
 *exception TESTFAILED     unexpected system errors<br/>
 *<br/><br/>
 * One of the client error is 401 error, Not authorized.  It will happen when
 * a password protected page is requested.  In order to get the page, the
 * 64Based encoded authentication string should be specified in the argument
 * hash.  For security reason, we do not use the original string.  To get the
 * auth string, try to use the following tool:
 *<br/><br/>
 * wget -d --http-user=UID --http-passwd=PASSWORD password_protected_page
 *<br/><br/>
 * In the output, search for the line with "Authorization:", and copy down
 * the encrypted auth string, i.e., the last string of the line.
 *<br/><br/>
 * The other method, generateReport(long), returns a report in a Map
 * that contains the response as a String and status code defined above
 * except exceptions.  It catches all exceptions and returns -1 indicating
 * internal error.  Therefore, generateReport(long) will not throw any
 * exceptions.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class WebTester extends Report {
    private String uri, host;
    private byte[] request;
    private String response;
    private int port;
    private boolean isHTTP, isPost = false, isHead = false;
    private int debug = 0;
    private int totalBytes, timeout, socketType;
    private byte[] buffer = new byte[0];
    private int maxBytes, responseTime;
    private int bufferSize;
    private SimpleDateFormat dateFormat;
    private Pattern httpPattern = null;
    private Pattern pattern = null;
    private String data, cookie = null;
    public final static int TESTFAILED = -1;
    public final static int TESTOK = 0;
    public final static int PROTOCOLERROR = 1;
    public final static int PATTERNNOHIT = 2;
    public final static int NOTMULTIPART = 3;
    public final static int CLIENTERROR = 4;
    public final static int SERVERERROR = 5;
    public final static int READFAILED = 6;
    public final static int WRITEFAILED = 7;
    public final static int CONNTIMEOUT = 8;
    public final static int CONNREFUSED = 9;

    public WebTester(Map props) {
        super(props);
        Object o;
        URI u;
        String path;
        int n;
        StringBuffer strBuf = new StringBuffer();

        if (type == null)
            type = "WebTester";

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.select(o);
        uri = MonitorUtils.substitute(uri, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        socketType = ClientSocket.TYPE_TCP;
        if ("http".equals(u.getScheme()))
            isHTTP = true;
        else if ("https".equals(u.getScheme())) {
            isHTTP = true;
            socketType = ClientSocket.TYPE_HTTPS;
        }
        else if ("tcp".equals(u.getScheme()))
            isHTTP = false;
        else
            throw(new IllegalArgumentException("wrong scheme: " +
                u.getScheme()));

        host = u.getHost();

        if ((port = u.getPort()) <= 0)
            port = (socketType == ClientSocket.TYPE_HTTPS) ? 443 : 80;

        if ((path = u.getPath()) == null || path.length() == 0)
            path = "/";
        else { // in case of query stuff, rebuild the path + query
            n = uri.indexOf("://" + host);
            if (n > 0)
                path = uri.substring(uri.indexOf("/", n+3));
        }

        // check time dependency on path
        n = initTimeTemplate(path);
        if (n > 0)
            path = updateContent(System.currentTimeMillis());

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        try {
            String ps;
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Pattern")) != null)
                ps = MonitorUtils.substitute((String) o, template);
            else
                ps = "200 OK";
            pattern = pc.compile(ps);
            httpPattern = pc.compile("^(HTTP)\\/[^ ]* (\\d+) ");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (isHTTP) { // normal http test
            if ((o = props.get("Request")) != null)
                strBuf.append((String) o + "\r\n");
            else if ((o = props.get("Operation")) != null) {
                String str = (String) o;
                if ("HEAD".equals(str))
                    strBuf.append("HEAD " + path + " HTTP/1.0\r\n");
                else if ("POST".equals(str))
                    strBuf.append("POST " + path + " HTTP/1.0\r\n");
                else
                    strBuf.append("GET " + path + " HTTP/1.0\r\n");
            }
            else
                strBuf.append("GET " + path + " HTTP/1.0\r\n");
            if ("POST ".equals(strBuf.substring(0, 5))) {
                isPost = true;
                isHead = false;
            }
            else if ("HEAD ".equals(strBuf.substring(0, 5))) {
                isPost = false;
                isHead = true;
            }
            else {
                isPost = false;
                isHead = false;
            }
            strBuf.append("Host: " + host + "\r\n");
            strBuf.append("User-Agent: " + "MonitorAgent/1.0\r\n");

            if ((o = props.get("Referer")) != null)
                strBuf.append("Referer: " + (String) o + "\r\n");
            if ((o = props.get("Cookie")) != null) {
                if (isPost)
                    cookie = (String) o;
                else
                    strBuf.append("Cookie: " + (String) o + "\r\n");
            }
            if ((o = props.get("EncryptedAuthorization")) != null)
                strBuf.append("Authorization: Basic " + (String) o + "\r\n");
            else if ((o = props.get("AuthString")) != null)
                strBuf.append("Authorization: " + (String) o + "\r\n");
            else if ((o = props.get("Username")) != null) {
                String str = (String) o;
                if ((o = props.get("Password")) != null) {
                    str += ":" + (String) o;
                    str = new String(Base64Encoder.encode(str.getBytes()));
                    strBuf.append("Authorization: Basic " + str + "\r\n");
                }
            }

            if ((o = props.get("Connection")) != null)
                strBuf.append("Connection: " + (String) o + "\r\n");

            if (isPost) {
                if ((o = props.get("Data")) != null)
                    data = (String) o;
            }
            else {
                strBuf.append("\r\n");
            }
            request = new byte[strBuf.length()];
            request = strBuf.toString().getBytes();
        }
        else if ((o = props.get("Request")) != null) // generic tcp test
            request = new String((String) o + "\r\n").getBytes();
        else if ((o = props.get("BinaryRequest")) != null) // generic tcp test
            request = Utils.hexString2Bytes((String) o); 
        else // readonly tcp test
            request = null;

        if ((o = props.get("MaxBytes")) == null ||
            (maxBytes = Integer.parseInt((String) o)) < 0)
            maxBytes = 512;

        if ((o = props.get("BufferSize")) == null ||
            (bufferSize = Integer.parseInt((String) o)) <= 0)
            bufferSize = 4096;

        buffer = new byte[bufferSize];
        totalBytes = 0;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int returnCode = -1;

        report.clear();
        response = null;
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
                if (skip == EXCEPTION)
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        // update the time-depenendent path
        if (timeFormat.length > 0 && isHTTP && !isPost) {
            int i;
            String str = new String(request);
            if ((i = str.indexOf("\r\n")) > 0) { // replace path
                String path = updateContent(currentTime);
                path = "GET " + path + " HTTP/1.0" + str.substring(i);
                request = path.getBytes();
            }
        }

        try {
            returnCode = testHTTPD();
        }
        catch (Exception e) {
            if (response != null && debug != 0)
                System.out.print(name + ": " + response);
            throw(new IOException("testHTTP: " + Event.traceStack(e)));
        }

        if (statsLogger != null) {
            String stats = Event.dateFormat(new Date(currentTime)) +
                " " + name + " " + returnCode + " " + responseTime;
            report.put("Stats", stats);
            try {
                statsLogger.log(stats);
            }
            catch (Exception e) {
            }
        }

        report.put("ReturnCode", String.valueOf(returnCode));
        report.put("Response", response);
        if ((disableMode > 0 && returnCode != TESTOK) ||
            (disableMode < 0 && returnCode == TESTOK))
            skip = DISABLED;

        return report;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public void setData(String data) {
        this.data = data;
    }

    /**
     * return HTML content
     */
    public String getContent() {
        int n;
        if (response != null && (n = response.indexOf("\r\n\r\n")) > 0)
            return (isHead)?response.substring(0, n):response.substring(n+4);
        else
            return "";
    }

    private int testHTTPD() {
        StringBuffer strBuf;
        long timeStart;
        int i, httpStatus, bytesRead, timeLeft;
        byte[] webRequest;
        boolean hasNew = false;
        Socket s = null;
        InputStream in = null;
        OutputStream out = null;

        if (isPost) {
            if (data == null)
                data = "";

            strBuf = new StringBuffer();
            if (cookie != null)
                strBuf.append("Cookie: " + cookie + "\r\n");
            strBuf.append("Content-Length: " + data.length() + "\r\n");
           strBuf.append("Content-Type: application/x-www-form-urlencoded\r\n");
            strBuf.append("\r\n" + data);
            webRequest = new byte[request.length + strBuf.length()];
            System.arraycopy(request, 0, webRequest, 0, request.length);
            System.arraycopy(strBuf.toString().getBytes(), 0,
                webRequest, request.length, strBuf.length());
        }
        else {
            webRequest = request;
        }

        strBuf = new StringBuffer();
        totalBytes = 0;
        responseTime = timeout;
        timeStart = System.currentTimeMillis();
        try {
            s = ClientSocket.connect(host, port, timeout, socketType);
        }
        catch (Exception e) {
            new Event(Event.WARNING, name + ": failed to connect: "+
                        e.toString()).send();
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
            s.setSoTimeout(timeLeft);
            in = s.getInputStream();
            out = s.getOutputStream();
        }
        catch (Exception e) {
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
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

        if (webRequest != null) { // for readonly
            try {
                out.write(webRequest);
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
        }

        try {
            for (i=0; i<300; i++) {
                if (in.available() > 0)
                    break;
                try {
                    Thread.sleep(5);
                }
                catch (Exception ex) {
                }
            }
            do { // read everything until EOF or timeout
                bytesRead = 0;
                hasNew = false;
                if (in.available() <= 0) try {
                    Thread.sleep(20);
                }
                catch (Exception ex) {
                }
                if (s != null) try {
                    s.setSoTimeout(500);
                }
                catch (Exception ex) {
                }
                try {
                    while ((bytesRead = in.read(buffer, 0, bufferSize)) > 0) {
                        strBuf.append(new String(buffer, 0, bytesRead));
                        totalBytes += bytesRead;
                        hasNew = true;
                        if (in.available() <= 0)
                            break;
                        if (maxBytes > 0 && totalBytes >= maxBytes)
                            break;
                    }
                }
                catch (InterruptedIOException ex) {
                }
                if (maxBytes > 0 && totalBytes >= maxBytes)
                    break;
                timeLeft = timeout-(int) (System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
        }
        catch (IOException e) {
            if (debug != 0)
                new Event(Event.DEBUG, name + ": read failed: " + e.toString() +
                    "\n" + strBuf).send();
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return READFAILED;
        }
        catch (Exception e) {
            if (debug != 0)
                new Event(Event.DEBUG, name + ": read failed: " + e.toString() +
                    "\n" + strBuf).send();
            close(out);
            close(in);
            close(s);
            s = null;
            in = null;
            out = null;
            return READFAILED;
        }
        close(out);
        close(in);
        close(s);
        s = null;
        in = null;
        out = null;

        responseTime = (int) (System.currentTimeMillis() - timeStart);
        response = strBuf.toString();

        if (!isHTTP) { // for generic tcp test like aggie
            if (!pm.contains(response, pattern)) {
                if (debug != 0)
                    System.out.print(name + ": " + response);
                return PATTERNNOHIT;
            }
            return TESTOK;
        }

        i = response.indexOf("\n");
        if (i <= 0) {
            if (debug != 0)
                System.out.print(name + ": " + response);
            return ((timeLeft <= 0L) ? READFAILED : PROTOCOLERROR);
        }

        if (pm.contains(response.substring(0, i), httpPattern)) {
            MatchResult mr = pm.getMatch();
            if ((! "HTTP".equals(mr.group(1))) || mr.group(2) == null) {
                if (debug != 0)
                    System.out.print(name + ": " + response);
                return PROTOCOLERROR;
            }
            try {
                httpStatus = Integer.parseInt(mr.group(2));
            }
            catch (NumberFormatException e) {
                if (debug != 0)
                    System.out.print(name + ": NumberFormatException: " +
                        mr.group(2) + "\n" + response);
                return PROTOCOLERROR;
            }
            if (httpStatus >= 500) {
                if (debug != 0)
                    System.out.print(name + ": " + response);
                return SERVERERROR;
            }
            else if (httpStatus >= 400) {
                if (debug != 0)
                    System.out.print(name + ": " + response);
                return CLIENTERROR;
            }
            else if (!pm.contains(response, pattern)) {
                if (debug != 0)
                    System.out.print(name + ": " + response);
                return PATTERNNOHIT;
            }
            return TESTOK;
        }
        if (debug != 0)
            System.out.print(name + ": " + response);
        return PROTOCOLERROR;
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

    public static void main(String[] args) {
        String filename = null;
        MonitorReport report = null;
        int i, n = 0;

        if (args.length <= 1) {
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
              case 'I':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
           java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            report = new WebTester(ph);
            Map r = report.generateReport(0L);
            String str = (String) r.get("ReturnCode");
            if (str != null)
                System.out.println(str + ": " + r.get("Response"));
            else
                System.out.println("failed to get the response");
            if (report != null)
                report.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (report != null)
                report.destroy();
        }
    }

    private static void printUsage() {
        System.out.println("WebTester Version 1.0 (written by Yannan Lu)");
        System.out.println("WebTester: monitor a web site via http");
        System.out.println("Usage: java org.qbroker.monitor.WebTester -I cfg.json");
    }
}
