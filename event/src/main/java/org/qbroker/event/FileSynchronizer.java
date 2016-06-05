package org.qbroker.event;

/* FileSynchronizer.java - an action keeping a file in sync with its source */

import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;
import org.qbroker.net.FTPConnector;
import org.qbroker.net.SFTPConnector;
import org.qbroker.net.HTTPConnector;
import org.qbroker.event.Event;

/**
 * FileSynchronizer monitors a source file on its last modified time and
 * keeps its target file in sync with the source.
 *<br/><br/>
 * Currently, it only supports the data source of http, ftp and file.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FileSynchronizer implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private Template template;
    private String uri, configDir, authString;
    private File targetFile = null;
    private HTTPConnector httpConn = null;
    private FTPConnector ftpConn = null;
    private SFTPConnector sftpConn = null;
    private File file;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private int sourceType, debug = 0;
    private long serialNumber;
    private static final int SOURCE_FILE = 0;
    private static final int SOURCE_HTTP = 1;
    private static final int SOURCE_FTP = 2;
    private static final int SOURCE_SFTP = 3;
    private static final int SOURCE_SCP = 4;
    private static final int SOURCE_JDBC = 5;
    private static final int SOURCE_JMS = 6;

    public FileSynchronizer(Map props) {
        Object o;
        URI u;
        int n;
        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "FileSynchronizer";

        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");

        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "sync the changes on a file";

        if ((o = (String) props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = EventUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        authString = null;
        if ("http".equals(u.getScheme()) || "https".equals(u.getScheme())) {
            sourceType = SOURCE_HTTP;
            Map<String, Object> h = Utils.cloneProperties(props);

            h.put("URI", uri);
            h.remove("Site");
            h.remove("Type");
            h.remove("Category");
            h.remove("Priority");
            if ((o = h.remove("Timeout")) != null)
                h.put("SOTimeout", o);
            httpConn = new HTTPConnector(h);
        }
        else if ("ftp".equals(u.getScheme())) {
            sourceType = SOURCE_FTP;
            try {
                file = new File(Utils.decode(u.getPath()));
            }
            catch(Exception e) {
                throw(new IllegalArgumentException("failed to decode: "+uri));
            }
            try {
                ftpConn = new FTPConnector(props);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to create ftp connection to " + uri +
                    " : "+ Event.traceStack(e)));
            }
            try {
                ftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if ("sftp".equals(u.getScheme())) {
            sourceType = SOURCE_SFTP;
            try {
                file = new File(Utils.decode(u.getPath()));
            }
            catch(Exception e) {
                throw(new IllegalArgumentException("failed to decode: "+uri));
            }
            try {
                sftpConn = new SFTPConnector(props);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to create ftp connection to " + uri +
                    " : "+ Event.traceStack(e)));
            }
            try {
                sftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if ("file".equals(u.getScheme())) {
            String fileName = u.getPath();
            sourceType = SOURCE_FILE;
            if (fileName == null || fileName.length() == 0)
                throw(new IllegalArgumentException("URI has no path: "+uri));
            try {
                file = new File(Utils.decode(u.getPath()));
            }
            catch(Exception e) {
                throw(new IllegalArgumentException("failed to decode: "+uri));
            }
        }
        else
            throw(new IllegalArgumentException(name +
                " unsupported scheme: " + u.getScheme()));

        o = EventUtils.substitute((String) props.get("TargetFile"), template);
        if (o != null)
            targetFile = new File((String) o);
        else
            throw(new IllegalArgumentException("TargetFile not defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Priority")) != null)
                pattern = pc.compile((String) o);
            else
                pattern = null;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        serialNumber = 0;
    }

    public void invokeAction(long currentTime, Event event) {
        String content = null;
        byte[] buffer = new byte[4096];
        long size = 0L;
        int returnCode = -1, bytesRead = 0, bufferSize = 4096;
        if (event == null)
            return;

        String priorityName = (String) event.getAttribute("priority");
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;

        // check returnCode on the uri
        if (sourceType == SOURCE_HTTP) {
            StringBuffer strBuf = new StringBuffer();
            try {
                httpConn.reconnect();
                returnCode = httpConn.doGet(null, strBuf);
                httpConn.close();
            }
            catch (Exception e) {
                httpConn.close();
                new Event(Event.ERR, name + " failed to get content from " +
                    uri + ": " + Event.traceStack(e)).send();
                return;
            }

            if (returnCode == HttpURLConnection.HTTP_OK) {
                content = strBuf.toString();
                if (content == null || content.length() <= 0) {
                    new Event(Event.ERR, name + " empty content from " +
                        uri).send();
                }
                else try {
                    FileOutputStream fout = new FileOutputStream(targetFile);
                    fout.write(content.getBytes());
                    fout.close();
                    size = content.length();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + " failed to write to " +
                        targetFile.getPath() + ": " + e.toString()).send();
                }
            }
            else {
                new Event(Event.ERR, name + " failed get content from " +
                    uri + ": " + returnCode).send();
            }
        }
        else if (sourceType == SOURCE_FTP) { // remote file
            String rc = ftpConn.reconnect();
            if (rc != null) { // reconnect failed
                new Event(Event.ERR, name + " failed to connect to " +
                    uri + ": " + rc).send();
                returnCode = -1;
            }
            else try {
                content = ftpConn.ftpGet(targetFile, file.getPath());
                if (content != null) { // failed to get file
                    new Event(Event.ERR, name + " failed to get " +
                        file.getPath() + ": " + content).send();
                }
                else
                    size = targetFile.length();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to download " +
                    file.getPath() + ": " + e.toString()).send();
            }
            try {
                ftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if (sourceType == SOURCE_SFTP) { // remote file via sftp
            String rc = sftpConn.reconnect();
            if (rc != null) { // reconnect failed
                new Event(Event.ERR, name + " failed to connect to " +
                    uri + ": " + rc).send();
                returnCode = -1;
            }
            else try {
                content = sftpConn.sftpGet(targetFile, file.getPath());
                if (content != null) { // failed to get file
                    new Event(Event.ERR, name + " failed to get " +
                        file.getPath() + ": " + content).send();
                }
                else
                    size = targetFile.length();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to download " +
                    file.getPath() + ": " + e.toString()).send();
            }
            try {
                sftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if (sourceType == SOURCE_FILE) { // local file
            if (!file.exists() || !file.canRead()) {
                new Event(Event.ERR, name + " failed open file: " +
                    uri).send();
            }
            else try {
                FileInputStream fin = new FileInputStream(file);
                FileOutputStream fout = new FileOutputStream(targetFile);
                while ((bytesRead = fin.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        fout.write(buffer, 0, bytesRead);
                    size += bytesRead;
                }
                fout.close();
                fin.close();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to write to " +
                    targetFile.getPath() + ": " + e.toString()).send();
            }
        }
        else {
            new Event(Event.ERR, name + " scheme not supported for " +
                uri).send();
        }
        if (debug > 0 && size > 0)
            new Event(Event.DEBUG, name + ": " + targetFile.getPath() +
                " got updated with " + size + " bytes").send();
    }

    public String getName() {
        return name;
    }
}
