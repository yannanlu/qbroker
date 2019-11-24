package org.qbroker.monitor;

/* FileTester.java - a monitor to check the file's existence */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Calendar;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.PatternFilenameFilter;
import org.qbroker.common.DataSet;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.net.FTPConnector;
import org.qbroker.net.SFTPConnector;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Report;
import org.qbroker.monitor.FTPTester;

/**
 * FileTester is a generic tester on a file or a directory.
 * Use FileTester to test accessibility of a given file. In case of directory,
 * FileTester can check if it is empty or not with support of pattern, mtime
 * and size.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class FileTester extends Report {
    private File file;
    private FilenameFilter filter = null;
    private boolean isRemote;
    private DataSet ageRange = null, sizeRange = null;
    private String uri, baseURI, fileName;
    private String hostname, username, password;
    private FTPTester ftpTester = null;
    private FTPConnector ftpConn = null;
    private SFTPConnector sftpConn = null;
    private int timeout, setPassiveMode, ftpStatusOffset;
    private final static String ftpStatusText[] = {"Exception",
        "Test OK", "Protocol error", "Commond failed",
        "File not found", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Server is down"};

    public FileTester(Map props) {
        super(props);
        Object o;
        URI u;
        String s;
        int n;

        if (type == null)
            type = "FileTester";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        ftpTester = null;
        ftpConn = null;
        sftpConn = null;
        setPassiveMode = 0;
        fileName = u.getPath();
        if (fileName == null || fileName.length() == 0)
            throw(new IllegalArgumentException("URI has no path: " + uri));

        try {
            fileName = Utils.decode(fileName);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to decode: "+fileName));
        }

        // check time dependency on fileName
        n = initTimeTemplate(fileName);
        if (n > 0)
            fileName = updateContent(System.currentTimeMillis());

        if ((s = u.getScheme()) == null || "file".equals(s)) {
            isRemote = false;
            ftpConn = null;
            sftpConn = null;
            file = new File(fileName);
            if (file.isDirectory() && (o = props.get("Pattern")) != null)
                filter = new PatternFilenameFilter((String) o);
            if ((o = props.get("SizeRange")) != null &&
                o instanceof List && ((List) o).size() > 0)
                sizeRange = new DataSet((List) o);
            if ((o = props.get("AgeRange")) != null &&
                o instanceof List && ((List) o).size() > 0)
                ageRange = new DataSet((List) o);
        }
        else if ("sftp".equals(s)) {
            isRemote = true;
            file = null;
            ftpConn = null;
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("SOTimeout", props.get("Timeout"));

            try {
                sftpConn = new SFTPConnector(h);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to init sftp: " +
                    e.toString()));
            }
        }
        else if ("ftp".equals(s) && props.get("SetPassiveMode") == null) {
            isRemote = true;
            file = null;
            sftpConn = null;
            List<String> request = new ArrayList<String>();
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("Name", name);
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("User", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("Timeout", (String) props.get("Timeout"));
            h.put("Step", "1");
            request.add("TYPE I\r\n");
            request.add("SIZE " + fileName + "\r\n");
            h.put("Request", request);
            ftpTester = new FTPTester(h);
        }
        else if ("ftp".equals(s) && (o = props.get("SetPassiveMode")) != null) {
            isRemote = true;
            file = null;
            sftpConn = null;
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("SOTimeout", props.get("Timeout"));
            h.put("SetPassiveMode", props.get("SetPassiveMode"));

            try {
                ftpConn = new FTPConnector(h);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to init ftp: " +
                    e.toString()));
            }
        }
        else {
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        }
        n = uri.indexOf(u.getPath());
        baseURI = (n > 0) ? uri.substring(0, n) : "";

        ftpStatusOffset = 0 - FTPTester.TESTFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long size = -1L, mtime = -1L;
        int returnCode;

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

        if (timeFormat.length > 0) { // time-depenendent fileName
            fileName = updateContent(currentTime);
            uri = baseURI + fileName;
            if (isRemote && setPassiveMode == 0 && sftpConn == null) {
                ftpTester.updateRequest(3, "SIZE " + fileName + "\n");
            }
            else if (!isRemote) {
                file = new File(fileName);
            }
        }
        report.put("URI", uri);

        if (isRemote && sftpConn != null) { // for sftp
            sftpConn.reconnect();
            try {
                size = sftpConn.getSize(fileName);
            }
            catch (Exception e) {
                throw(new IOException("sftp failed on " + uri + ": " +
                    e.toString()));
            }
            if ((disableMode > 0 && size < 0) || (disableMode < 0 && size >= 0))
                skip = DISABLED;
            try {
                sftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if (isRemote && setPassiveMode == 0) { // for ftpTester
            int i;
            int [] responseCode;
            String [] responseText;
            Map r = ftpTester.generateReport(currentTime);
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
                responseCode = (int []) r.get("ResponseCode");
                responseText = (String []) r.get("ResponseText");
                if (returnCode == FTPTester.TESTOK) {
                    size = (long) Integer.parseInt(responseText[3]);
                }
                else if (returnCode >= FTPTester.CMDFAILED &&
                    returnCode <= FTPTester.SERVERERROR) {
                    for (i=responseCode.length-1; i>=0; i--) {
                        if (responseCode[i] > 0 &&
                            responseText[i] != null)
                            break;
                    }
                    throw(new IOException("ftp error:(" + i + ") " +
                        responseCode[i] + " " + responseText[i]));
                }
                else {
                    throw(new IOException("ftp failed: " +
                        ftpStatusText[returnCode + ftpStatusOffset]));
                }
                if ((disableMode > 0 && returnCode != FTPTester.TESTOK) ||
                    (disableMode < 0 && returnCode == FTPTester.TESTOK))
                    skip = DISABLED;
            }
            else {
                throw(new IOException("ftp failed on " + uri));
            }
        }
        else if (isRemote && setPassiveMode != 0) { // for ftp
            ftpConn.reconnect();
            size = ftpConn.getSize(fileName);
            if ((disableMode > 0 && size < 0) || (disableMode < 0 && size >= 0))
                skip = DISABLED;
            try {
                ftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if (filter != null) { // for pattern match on files in a folder
            File[] list = file.listFiles(filter);
            if (list != null && list.length > 0) { // folder not empty
                int i, k = list.length;
                size = 0;
                if (sizeRange != null && ageRange != null) {
                    for (i=0; i<k; i++) {
                        if (list[i] != null && list[i].exists() &&
                            list[i].canRead() &&
                            sizeRange.contains(list[i].length())) {
                            mtime = (currentTime - list[i].lastModified())/1000;
                            if (mtime < 0)
                                mtime = 0;
                            if (ageRange.contains(mtime))
                                size ++;
                        }
                    }
                }
                else if (sizeRange != null) {
                    for (i=0; i<k; i++) {
                        if (list[i] != null && list[i].exists() &&
                            list[i].canRead() &&
                            sizeRange.contains(list[i].length()))
                            size ++;
                    }
                }
                else if (ageRange != null) {
                    for (i=0; i<k; i++) {
                        if (list[i] != null && list[i].exists() &&
                            list[i].canRead()) {
                            mtime = (currentTime - list[i].lastModified())/1000;
                            if (mtime < 0)
                                mtime = 0;
                            if (ageRange.contains(mtime))
                                size ++;
                        }
                    }
                }
                else {
                    size = list.length;
                }
                // check empty or not
                if (size > 0) { // not empty
                    if (disableMode < 0)
                        skip = DISABLED;
                }
                else if (disableMode > 0) { // empty
                    skip = DISABLED;
                }
                else if (disableMode == 0) {
                    throw(new IOException(uri + " not found"));
                }
            }
            else if (disableMode > 0) // empty folder or not exist 
                skip = DISABLED;
            else if (disableMode == 0)
                throw(new IOException(uri + " not found"));
        }
        else if (file.exists()) {
            size = file.length();
            if (disableMode < 0)
                skip = DISABLED;
        }
        else if (disableMode > 0) {
            skip = DISABLED;
        }
        else if (disableMode == 0) {
            throw(new IOException(uri + " not found"));
        }
        report.put("Size", String.valueOf(size));

        return report;
    }

    private void close() {
        if (ftpConn != null) {
            try {
                ftpConn.close();
            }
            catch (Exception e) {
            }
        }
        if (sftpConn != null) {
            try {
                sftpConn.close();
            }
            catch (Exception e) {
            }
        }
    }

    public void destroy() {
        super.destroy();
        if (ftpTester != null) {
            ftpTester.destroy();
            ftpTester = null;
        }
        close();
        if (ftpConn != null) {
            ftpConn = null;
        }
        if (sftpConn != null) {
            sftpConn = null;
        }
    }

    protected void finalize() {
        destroy();
    }

    /** lists all files in a folder selected by a pattern, mtime or size */
    public static void main(String args[]) {
        int i, ic = -1, jc = -1, k;
        long currentTime, mtime = -1, size = -1;
        String dirName = null, pattern = null;
        File file = null;
        File list[];
        DataSet ageRange = null, sizeRange = null;
        FilenameFilter filter = null;

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2)
                continue;
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'd':
                dirName = args[i+1];
                break;
              case 'A':
                mtime = Long.parseLong(args[i+1]);
                ic = 1;
                break;
              case 'a':
                mtime = Long.parseLong(args[i+1]);
                ic = 0;
                break;
              case 'S':
                size = Long.parseLong(args[i+1]);
                jc = 1;
                break;
              case 's':
                size = Long.parseLong(args[i+1]);
                jc = 0;
                break;
              case 'p':
                pattern = args[i+1];
                break;
              default:
                break;
            }
        }

        if (dirName == null) {
            printUsage();
            System.exit(0);
        }
        file = new File(dirName);
        if (!file.exists() || !file.isDirectory() || !file.canRead())
            throw new IllegalArgumentException("Illegal directory: " + dirName);

        if (pattern != null && pattern.length() > 0)
            filter = new PatternFilenameFilter(pattern);

        if (mtime >= 0L) {
            List<String> pl = new ArrayList<String>();
            if (ic > 0)
                pl.add("(," + mtime + "]");
            else
                pl.add("[" + mtime + ",)");
            ageRange = new DataSet(pl);
        }
        if (size >= 0L) {
            List<String> pl = new ArrayList<String>();
            if (jc > 0)
                pl.add("(," + size + "]");
            else
                pl.add("[" + size + ",)");
            sizeRange = new DataSet(pl);
        }

        list = (filter != null) ? file.listFiles(filter) : file.listFiles();
        currentTime = System.currentTimeMillis();
        if (list != null && list.length > 0) {
            k = list.length;
            if (sizeRange != null && ageRange != null) {
                for (i=0; i<k; i++) {
                    if (list[i] != null && list[i].exists() &&
                        list[i].canRead() &&
                        sizeRange.contains(list[i].length())) {
                        mtime = (currentTime - list[i].lastModified()) / 1000L;
                        if (mtime < 0L)
                            mtime = 0L;
                        if (ageRange.contains(mtime))
                            System.out.println(list[i].getPath());
                    }
                }
            }
            else if (sizeRange != null) {
                for (i=0; i<k; i++) {
                    if(list[i] != null && list[i].exists() &&
                        list[i].canRead() &&
                        sizeRange.contains(list[i].length()))
                        System.out.println(list[i].getPath());
                }
            }
            else if(ageRange != null) {
                for (i=0; i<k; i++) {
                    if (list[i] != null && list[i].exists() &&
                        list[i].canRead()) {
                        mtime = (currentTime - list[i].lastModified()) / 1000L;
                        if (mtime < 0L)
                            mtime = 0L;
                        if (ageRange.contains(mtime))
                            System.out.println(list[i].getPath());
                    }
                }
            }
            else {
                for(i=0; i<k; i++) {
                    if (list[i] != null && list[i].exists() &&
                        list[i].canRead())
                        System.out.println(list[i].getPath());
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("FileTester Version 2.0 (written by Yannan Lu)");
        System.out.println("FileTester: a utility to list files in a folder");
        System.out.println("Usage: java org.qbroker.monitor.FileTester -d dirName [-a|-s]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -o: option");
        System.out.println("  -s: minimum size in bytes");
        System.out.println("  -S: maximum size in bytes");
        System.out.println("  -a: minimum time in seconds");
        System.out.println("  -A: maximum time in seconds");
        System.out.println("  -p: pattern to select files");
    }
}
