package org.qbroker.net;

/* FTPConnector.java - an FTP connector for file transfers */

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.MalformedServerReplyException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * FTPConnector connects to an FTP server and provides the following
 * methods for file transfers: ftpGet(), ftpPut() and ftpList().
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FTPConnector implements Connector {
    protected String uri;
    public static final int FTP_ALL = 0;
    public static final int FTP_FILE = 1;
    public static final int FTP_DIRECTORY = 2;
    public static final int FTP_LINK = 4;
    public static final int FTP_UNKNOWN = 8;
    public static final int ACTION_PWD = 0;
    public static final int ACTION_CWD = 1;
    public static final int ACTION_MKD = 2;
    public static final int ACTION_RMD = 3;
    public static final int ACTION_LIST = 4;
    public static final int ACTION_GET = 5;
    public static final int ACTION_PUT = 6;
    public static final int ACTION_DEL = 7;
    public static final int ACTION_SIZE = 8;
    public static final int ACTION_TIME = 9;

    private FTPClient ftpClient = null;
    private String hostname;
    private String username;
    private String password;
    private int port = 21;
    private int timeout = 60000;
    private int bufferSize = 4096;
    private boolean isConnected = false;
    private boolean setPassiveMode = true;
    private boolean asciiType = false;

    /** Creates new FTPConnector */
    public FTPConnector(Map props) throws IOException {
        Object o;
        URI u;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"ftp".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 21;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((o = props.get("Username")) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        username = (String) o;

        if ((o = props.get("Password")) == null)
            throw(new IllegalArgumentException("Password is not defined"));
        password = (String) o;

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if (props.get("SetPassiveMode") != null &&
            "false".equals((String) props.get("SetPassiveMode")))
            setPassiveMode = false;

        if(props.get("FileType") != null && "ascii".equalsIgnoreCase((String)o))
            asciiType = true;

        ftpClient = new FTPClient();
        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
            if (bufferSize > 0)
                ftpClient.setBufferSize(bufferSize);
            else
                bufferSize = 4096;
        }
        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o))
            connect();
    }

    protected void connect() throws IOException {
        int replyCode;
        isConnected = false;
        ftpClient.setDefaultTimeout(timeout + timeout);
        ftpClient.connect(hostname, port);
        ftpClient.setSoTimeout(timeout);

        replyCode = ftpClient.getReplyCode();
        if (! FTPReply.isPositiveCompletion (replyCode))
            throw new IOException ("unable to connect to " + hostname + ": " +
                ftpClient.getReplyString());

        if (! ftpClient.login(username, password))
            throw new IOException ("login failed for " + username + '@' +
                hostname + ": " + ftpClient.getReplyString());

        if (!asciiType) { // set binary type
            if (! ftpClient.setFileType(FTP.BINARY_FILE_TYPE))
                throw new IOException ("unable to set binary type on " +
                    hostname + ": " + ftpClient.getReplyString());
        }

        if (setPassiveMode)
            ftpClient.enterLocalPassiveMode();
        isConnected = true;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** It reconnects and returns null or error message upon failure */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (IOException e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (ftpClient != null) try {
            ftpClient.logout();
            if (ftpClient.isConnected())
                ftpClient.disconnect();
        }
        catch (Exception e) {
        }
        isConnected = false;
    }

    public String ftpGet(File localFile, String filename) throws IOException {
        OutputStream out = null;

        if (localFile == null || (localFile.exists() && !localFile.canWrite()))
            return "bad localFile: " + localFile;

        try {
            out = new FileOutputStream(localFile);
        }
        catch (Exception e) {
            try {
                if (out != null)
                    out.close();
            }
            catch (Exception ex) {
            }
            return e.toString();
        }

        if (! ftpClient.retrieveFile(filename, out)) {
            try {
                out.close();
            }
            catch (Exception e) {
            }
            return "" + ftpClient.getReplyString();
        }
        try {
            out.close();
        }
        catch (Exception e) {
        }

        return null;
    }

    public String ftpGet(StringBuffer strBuf, String filename)
        throws IOException {
        InputStream in = null;
        int replyCode;
        int bytesRead = 0;
        byte[] buffer = new byte[bufferSize];

        if (strBuf == null)
            return "strBuf is null";

        strBuf.setLength(0);
        in = ftpClient.retrieveFileStream(filename);
        replyCode = ftpClient.getReplyCode();
        if (replyCode != 150) {
            try {
                in.close();
            }
            catch (Exception e) {
            }
            return "" + ftpClient.getReplyString();
        }
        while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
            if (bytesRead == 0) {
                try {
                    Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                }
                continue;
            }

            strBuf.append(new String(buffer, 0, bytesRead));
        }
        try {
            in.close();
        }
        catch (Exception e) {
        }

        // Must call completePendingCommand() to finish command.
        if (!ftpClient.completePendingCommand()) {
            return "transfer failed on " + filename;
        }
        return null;
    }

    public String ftpGet(OutputStream out, String filename) throws IOException {
        if (out == null)
            return "null outputStream";

        if (! ftpClient.retrieveFile(filename, out)) {
            return "" + ftpClient.getReplyString();
        }

        return null;
    }

    public String ftpPut(File localFile, String filename) throws IOException {
        InputStream in = null;

        if (localFile == null || !localFile.exists() || !localFile.canRead())
            return "bad localFile: " + localFile;

        try {
            in = new FileInputStream(localFile);
        }
        catch (Exception e) {
            try {
                if (in != null)
                    in.close();
            }
            catch (Exception ex) {
            }
            return e.toString();
        }

        if (! ftpClient.storeFile(filename, in)) {
            try {
                in.close();
            }
            catch (Exception e) {
            }
            return "" + ftpClient.getReplyString();
        }
        try {
            in.close();
        }
        catch (Exception e) {
        }

        return null;
    }

    public String ftpPut(String payload, String filename) throws IOException {
        OutputStream out = null;
        int replyCode;

        if (payload == null)
            return "payload is null";

        out = ftpClient.storeFileStream(filename);
        replyCode = ftpClient.getReplyCode();
        if (replyCode != 150) {
            try {
                out.close();
            }
            catch (Exception e) {
            }
            return "" + ftpClient.getReplyString();
        }
        out.write(payload.getBytes());
        try {
            out.close();
        }
        catch (Exception e) {
        }

        // Must call completePendingCommand() to finish command.
        if (!ftpClient.completePendingCommand()) {
            return "transfer failed on " + filename;
        }
        return null;
    }

    public String ftpPut(InputStream in, String filename) throws IOException {
        if (in == null)
            return "null inputStream";

        if (! ftpClient.storeFile(filename, in)) {
            return "" + ftpClient.getReplyString();
        }

        return null;
    }

    /**
     * It returns an array of strings for requested files of type.  In case of
     * failure, it returns null.  Due to individual parsing errors, the array
     * may contain null members if type is FTP_ALL.  In this case, caller
     * should check each entry for null.  For other types, any null member
     * will be skipped.  The names in the array do not contain the full path.
     */
    public String[] ftpList(String dirname, int type) throws IOException {
        FTPFile[] flist;
        String[] list;
        int i, n;
        if (dirname == null || dirname.length() <= 0)
            flist = ftpClient.listFiles();
        else
            flist = ftpClient.listFiles(dirname);
        if (flist == null)
            return null;
        n = flist.length;
        if (type <= 0) {
            list = new String[n];
            for (i=0; i<n; i++) {
                if (flist[i] != null)
                    list[i] = flist[i].getName();
                else
                    list[i] = null;
            }
        }
        else {
            int k = 0;
            int[] mark = new int[n];
            for (i=0; i<n; i++) {
                mark[i] = 0;
                if (flist[i] == null)
                    continue;
                else if (flist[i].isFile())
                    mark[i] = ((type & FTP_FILE) > 0) ? 1 : 0;
                else if (flist[i].isDirectory())
                    mark[i] = ((type & FTP_DIRECTORY) > 0) ? 1 : 0;
                else if (flist[i].isSymbolicLink())
                    mark[i] = ((type & FTP_LINK) > 0) ? 1 : 0;
                else
                    mark[i] = ((type & FTP_UNKNOWN) > 0) ? 1 : 0;
                k += mark[i];
            }
            list = new String[k];
            if (k > 0) {
                k = 0;
                for (i=0; i<n; i++) {
                    if (mark[i] > 0)
                        list[k ++] = flist[i].getName();
                }
            }
        }
        return list;
    }

    public String ftpPwd() throws IOException {
        return ftpClient.printWorkingDirectory();
    }

    public boolean ftpCwd(String pathname) throws IOException {
        return ftpClient.changeWorkingDirectory(pathname);
    }

    public boolean ftpMkd(String pathname) throws IOException {
        return ftpClient.makeDirectory(pathname);
    }

    public boolean ftpRmd(String pathname) throws IOException {
        return ftpClient.removeDirectory(pathname);
    }

    public boolean ftpDelete(String pathname) throws IOException {
        return ftpClient.deleteFile(pathname);
    }

    public boolean ftpNoOp() throws IOException {
        return ftpClient.sendNoOp();
    }

    public boolean completePendingCommand() throws IOException {
        return ftpClient.completePendingCommand();
    }

    public int getReplyCode() throws IOException {
        return ftpClient.getReplyCode();
    }

    public String getReplyString() throws IOException {
        return ftpClient.getReplyString();
    }

    /**
     * It checks the existence of the directory tree on the server and creates
     * the missing ones if necessary.  It returns null if successful or
     * the failed directory name otherwise.  It requires the absolute path
     * for dirname.
     */
    public String createDirectory(String dirname, int retry) throws IOException{
        char fs;
        String dir = null;
        IOException ex = null;
        boolean isDone = false;
        int i, j, k;
        if (dirname == null || (k = dirname.length()) <= 0) // bad argument
            return null;
        // verify the dir
        for (j=0; j<=retry; j++) {
            try {
                isDone = ftpClient.changeWorkingDirectory(dirname);
            }
            catch (FTPConnectionClosedException e) {
                reconnect();
                continue;
            }
            catch (SocketException e) {
                reconnect();
                continue;
            }
            catch (IOException e) {
                if (j > 0)
                    ex = e;
                else
                    reconnect();
                continue;
            }
            if (isDone)
                return null;
            else {
                ex = null;
                break;
            }
        }
        if (ex != null)
            throw ex;
        fs = dirname.charAt(0);
        if (fs != '/' && fs != '\\') // no file separator found
            return null;
        // try to find which subdir does not exist
        while ((i = dirname.lastIndexOf(fs, k-1)) > 0) {
            k = i;
            dir = dirname.substring(0, i);
            try {
                isDone = ftpClient.changeWorkingDirectory(dir);
            }
            catch (FTPConnectionClosedException e) {
                reconnect();
            }
            catch (SocketException e) {
                reconnect();
            }
            catch (IOException e) {
                throw e;
            }
            if (isDone)
                break;
        }
        // try to make dirs
        while ((i = dirname.indexOf(fs, k+1)) > 0) {
            dir = dirname.substring(0, i); 
            for (j=0; j<=retry; j++) {
                try {
                    isDone = ftpClient.makeDirectory(dir);
                }
                catch (FTPConnectionClosedException e) {
                    reconnect();
                    continue;
                }
                catch (SocketException e) {
                    reconnect();
                    continue;
                }
                catch (IOException e) {
                }
                if (isDone)
                    break;
            }
            if (j > retry) // failed to create dir
                return dir;
            k = i;
        }
        dir = dirname;
        for (j=0; j<=retry; j++) {
            try {
                isDone = ftpClient.makeDirectory(dir);
            }
            catch (FTPConnectionClosedException e) {
                reconnect();
                continue;
            }
            catch (SocketException e) {
                reconnect();
                continue;
            }
            catch (IOException e) {
            }
            if (isDone)
                return null;
        }
        return dir;
    }

    /**
     * It returns dirname of the filename or null if it is a relative path.
     */
    public static String getParent(String filename) {
        char fs;
        int i, k;
        if (filename == null || (k = filename.length()) <= 0) // bad argument
            return null;
        fs = filename.charAt(0);
        if (fs != '/' && fs != '\\') // no file separator found
            return null;
        if ((i = filename.lastIndexOf(fs, k-1)) >= 0)
            return filename.substring(0, i);
        else
            return null;
    }

    public FTPClient getFTPClient() {
        return ftpClient;
    }

    public InputStream retrieveFileStream(String remote) throws IOException {
        if (isConnected && ftpClient != null)
            return ftpClient.retrieveFileStream(remote);
        else
            return null;
    }

    public OutputStream storeFileStream(String remote) throws IOException {
        if (isConnected && ftpClient != null)
            return ftpClient.storeFileStream(remote);
        else
            return null;
    }

    /** returns the file size or -1 in case of failure */
    public long getSize(String remote) throws IOException {
        if (isConnected && ftpClient != null) {
            FTPFile[] ffile = ftpClient.listFiles(remote);
            ftpClient.getReplyCode();
            if (ffile != null && ffile.length > 0)
                return ffile[0].getSize();
        }
        return -1;
    }

    /** returns the mtime or -1 in case of failure */
    public long getTimestamp(String remote) throws IOException {
        if (isConnected && ftpClient != null) {
            FTPFile[] ffile = ftpClient.listFiles(remote);
            ftpClient.getReplyCode();
            if (ffile != null && ffile.length > 0)
                return ffile[0].getTimestamp().getTimeInMillis();
        }
        return -1;
    }

    public static void main(String args[]) {
        Map<String, Object> props;
        URI u;
        int i, timeout = 60, action = ACTION_PWD;
        String filename = null, uri = null, str = null, path = null;
        String[] list;
        long size, mtime;
        FTPConnector conn = null;
        SimpleDateFormat dateFormat;
        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zz");
        props = new HashMap<String, Object>();
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'A':
                props.put("SetPassiveMode", "false");
                break;
              case 'T':
                props.put("FileType", "ascii");
                break;
              case 'a':
                str = args[++i];
                if ("get".equalsIgnoreCase(str))
                    action = ACTION_GET;
                else if ("put".equalsIgnoreCase(str))
                    action = ACTION_PUT;
                else if ("del".equalsIgnoreCase(str))
                    action = ACTION_DEL;
                else if ("size".equalsIgnoreCase(str))
                    action = ACTION_SIZE;
                else if ("time".equalsIgnoreCase(str))
                    action = ACTION_TIME;
                else if ("pwd".equalsIgnoreCase(str))
                    action = ACTION_PWD;
                else if ("cwd".equalsIgnoreCase(str))
                    action = ACTION_CWD;
                else if ("mkd".equalsIgnoreCase(str))
                    action = ACTION_MKD;
                else if ("rmd".equalsIgnoreCase(str))
                    action = ACTION_RMD;
                else if ("list".equalsIgnoreCase(str))
                    action = ACTION_LIST;
                else
                    action = ACTION_PWD;
                break;
              case 'u':
                if (i+1 < args.length)
                    uri = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'p':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              case 'b':
                if (i+1 < args.length)
                    props.put("BufferSize", args[++i]);
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        if (uri == null) {
            printUsage();
            System.exit(0);
        }
        else
            props.put("URI", uri);
        props.put("SOTimeout", String.valueOf(timeout));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(uri + ": " + e.toString()));
        }

        if ((path = u.getPath()) == null || path.length() == 0)
            throw(new IllegalArgumentException("path not defined: " + uri));

        try {
            File file;
            conn = new FTPConnector(props);
            str = null;
            switch (action) {
              case ACTION_GET:
                if (filename != null && filename.length() > 0) {
                    file = new File(filename);
                    str = conn.ftpGet(file, path);
                }
                else
                    str = "local file is not defined";
                break;
              case ACTION_PUT:
                if (filename != null && filename.length() > 0) {
                    file = new File(filename);
                    str = conn.ftpPut(file, path);
                }
                else
                    str = "local file is not defined";
                break;
              case ACTION_SIZE:
                size = conn.getSize(path);
                if (size >= 0)
                    System.out.println(path + ": " + size);
                else
                    str = "failed to get size for " + path;
                break;
              case ACTION_TIME:
                mtime = conn.getTimestamp(path);
                if (mtime >= 0)
                    System.out.println(path + ": " +
                        dateFormat.format(new Date(mtime)));
                else
                    str = "failed to get mtime for " + path;
                break;
              case ACTION_DEL:
                if (conn.ftpDelete(path))
                    System.out.println(path + " is deleted");
                else
                    str = "failed to delete the file: " + path;
                break;
              case ACTION_PWD:
                str = conn.ftpPwd();
                if (str != null) {
                    System.out.println("working directory: " + str);
                    str = null;
                }
                else
                    str = "failed to print the working directory";
                break;
              case ACTION_CWD:
                if (conn.ftpCwd(path))
                    System.out.println("current directory: " + path);
                else
                    str = "failed to change to " + path;
                break;
              case ACTION_MKD:
                str = conn.createDirectory(path, 1);
                if (str == null)
                    System.out.println(path + " is created");
                else
                    str = "failed to create the directory: " + str;
                break;
              case ACTION_RMD:
                if (conn.ftpRmd(path))
                    System.out.println(path + " is removed");
                else
                    str = "failed to remove the directory: " + path;
                break;
              case ACTION_LIST:
                list = conn.ftpList(path, FTP_ALL);
                if (list != null) {
                    System.out.println(path + ": " + list.length);
                    for (i=0; i<list.length; i++)
                        System.out.println(list[i]);
                }
                else
                    str = "failed to list on " + path;
                break;
              default:
                str = "not supported yet";
            }
            if (str != null)
                System.out.println("Error: " + str);
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
        System.out.println("FTPConnector Version 2.0 (written by Yannan Lu)");
        System.out.println("FTPConnector: an FTP client to get or put files");
        System.out.println("Usage: java org.qbroker.net.FTPConnector -u uri -t timeout -n username -p password -f local_filename -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -A: set active mode (default: passive)");
        System.out.println("  -T: set ascii type (default: binary)");
        System.out.println("  -a: action of list, get, put, del, size, time, pwd, cwd, mkd, rmd (default: pwd)");
        System.out.println("  -b: buffer size in byte");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -f: local filename");
    }
}
