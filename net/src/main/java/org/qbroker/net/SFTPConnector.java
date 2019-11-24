package org.qbroker.net;

/* SFTPConnector.java - a SFTP connector for file transfers */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.Date;
import java.text.SimpleDateFormat;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.qbroker.common.Utils;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * SFTPConnector connects to a SSH server and provides the following
 * methods for file transfers: sftpGet(), sftpPut() and sftpList().
 *<br><br>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SFTPConnector implements Connector {
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

    private Session session = null;
    private ChannelSftp sftp = null;
    private String hostname;
    private String username;
    private String password;
    private int port = 22;
    private int timeout = 60000;
    private int bufferSize = 4096;
    private boolean isConnected = false;

    /** Creates new SFTPConnector */
    public SFTPConnector(Map props) throws JSchException {
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

        if (!"sftp".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 22;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((o = props.get("Username")) == null)
            throw(new IllegalArgumentException("Username is not defined"));
        else {
            username = (String) o;

            password = null;
            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null)
                throw(new IllegalArgumentException("Password is not defined"));
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o))
            connect();
    }

    protected void connect() throws JSchException {
        Channel channel;
        session = new JSch().getSession(username, hostname, port);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setTimeout(timeout);
        session.setPassword(password);
        session.connect();

        channel = session.openChannel("sftp");
        channel.connect();

        sftp = (ChannelSftp) channel; 
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
        catch (Exception e) {
            return TraceStackThread.traceStack(e);
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (sftp != null) try {
            sftp.exit();
        }
        catch (Exception e) {
        }
        sftp = null;
        if (session != null) try {
            session.disconnect();
        }
        catch (Exception e) {
        }
        session = null;
        isConnected = false;
    }

    public String sftpGet(File localFile, String filename) {
        String str = null;
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

        str = sftpGet(out, filename);

        try {
            out.close();
        }
        catch (Exception e) {
        }

        return str;
    }

    public String sftpGet(StringBuffer strBuf, String filename) {
        int bytesRead;
        String str = null;
        InputStream in = null;
        byte[] buffer = new byte[bufferSize];

        if (strBuf == null)
            return "strBuf is null";

        strBuf.setLength(0);
        try {
            in = sftp.get(filename);
        }
        catch (Exception e) {
            return e.toString();
        }

        try {
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
        }
        catch (Exception e) {
            str = e.toString();
        }

        try {
            in.close();
        }
        catch (Exception e) {
        }

        return str;
    }

    public String sftpGet(OutputStream out, String filename)  {
        if (out == null)
            return "null outputStream";

        try {
            sftp.get(filename, out);
        }
        catch (Exception e) {
            return e.toString();
        }

        return null;
    }

    public String sftpPut(File localFile, String filename) {
        String str = null;
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

        str = sftpPut(in, filename);

        try {
            in.close();
        }
        catch (Exception e) {
        }

        return str;
    }

    public String sftpPut(String payload, String filename) {
        String str = null;
        InputStream in = null;

        if (payload == null)
            return "payload is null";

        try {
            in = new ByteArrayInputStream(payload.getBytes());
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

        str = sftpPut(in, filename);

        try {
            in.close();
        }
        catch (Exception e) {
        }

        return str;
    }

    public String sftpPut(InputStream in, String filename) {
        if (in == null)
            return "null inputStream";

        try {
            sftp.put(in, filename);
        }
        catch (Exception e) {
            return "" + e.toString();
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
    public String[] sftpList(String dirname, int type) throws SftpException {
        Object o;
        Vector flist;
        String[] list;
        int i, n;
        if (dirname == null || dirname.length() <= 0)
            flist = sftp.ls("*");
        else
            flist = sftp.ls(dirname);
        if (flist == null)
            return null;
        n = flist.size();
        if (type <= 0) {
            list = new String[n];
            for (i=0; i<n; i++) {
                o = flist.get(i);
                if (o != null)
                    list[i] =
                        ((com.jcraft.jsch.ChannelSftp.LsEntry) o).getFilename();
                else
                    list[i] = null;
            }
        }
        else {
            SftpATTRS attr;
            int k = 0, mark;
            String[] temp = new String[n];
            for (i=0; i<n; i++) {
                mark = 0;
                o = flist.get(i);
                if (o == null)
                    continue;
                attr = ((com.jcraft.jsch.ChannelSftp.LsEntry) o).getAttrs();
                if (attr.isDir())
                    mark = ((type & FTP_DIRECTORY) > 0) ? 1 : 0;
                else if (attr.isLink())
                    mark = ((type & FTP_LINK) > 0) ? 1 : 0;
                else
                    mark = ((type & FTP_FILE) > 0) ? 1 : 0;
                if (mark > 0)
                    temp[k++] =
                        ((com.jcraft.jsch.ChannelSftp.LsEntry) o).getFilename();
            }
            list = new String[k];
            for (i=0; i<k; i++) {
                list[i] = temp[i];
            }
        }
        return list;
    }

    /**
     * It checks the existence of the directory tree on the server and creates
     * the missing ones if necessary.  It returns null if successful or
     * the failed directory name otherwise.  It requires the absolute path
     * for dirname.
     */
    public String createDirectory(String dirname, int retry)
        throws SftpException {
        char fs;
        String dir = null;
        SftpException ex = null;
        boolean isDone = false;
        int i, j, k;
        if (dirname == null || (k = dirname.length()) <= 0) // bad argument
            return null;
        // verify the dir
        for (j=0; j<=retry; j++) {
            try {
                sftp.cd(dirname);
            }
            catch (SftpException e) {
                if (e.id == 2) {
                    ex = null;
                    break;
                }
                if (j > 0 || j == retry)
                    ex = e;
                else
                    reconnect();
                continue;
            }
            // dir exits
            return null;
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
                sftp.cd(dir);
            }
            catch (SftpException e) {
                if (e.id == 2)
                    continue;
                else
                    throw(e);
            }
            break;
        }
        // try to make dirs
        while ((i = dirname.indexOf(fs, k+1)) > 0) {
            dir = dirname.substring(0, i); 
            for (j=0; j<=retry; j++) {
                try {
                    sftp.mkdir(dir);
                }
                catch (SftpException e) {
                    reconnect();
                    continue;
                }
                break;
            }
            if (j > retry) // failed to create dir
                return dir;
            k = i;
        }
        dir = dirname;
        for (j=0; j<=retry; j++) {
            try {
                sftp.mkdir(dir);
            }
            catch (SftpException e) {
                reconnect();
                continue;
            }
            return null;
        }
        return dir;
    }

    /**
     * It returns dirname of the filename or null if it is a relative path.
     */
    protected static String getParent(String filename) {
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

    public ChannelSftp getChannelSftp() {
        return sftp;
    }

    public InputStream retrieveFileStream(String remote) throws SftpException {
        if (isConnected && sftp != null)
            return sftp.get(remote);
        else
            return null;
    }

    public OutputStream storeFileStream(String remote) throws SftpException {
        if (isConnected && sftp != null)
            return sftp.put(remote);
        else
            return null;
    }

    public boolean sftpDelete(String pathname) throws SftpException {
        sftp.rm(pathname);
        return true;
    }

    public String sftpPwd() throws SftpException {
        return sftp.pwd();
    }

    public boolean sftpCwd(String pathname) throws SftpException {
        sftp.cd(pathname);
        return true;
    }

    public boolean sftpMkd(String pathname) throws SftpException {
        sftp.mkdir(pathname);
        return true;
    }

    public boolean sftpRmd(String pathname) throws SftpException {
        sftp.rmdir(pathname);
        return true;
    }

    /** returns the file size or -1 in case of failure */
    public long getSize(String remote) throws SftpException {
        if (isConnected && sftp != null) {
            SftpATTRS attr = sftp.stat(remote);
            if (attr != null)
                return attr.getSize();
        }
        return -1;
    }

    /** returns the mtime in millis or -1 in case of failure */
    public long getTimestamp(String remote) throws SftpException {
        if (isConnected && sftp != null) {
            SftpATTRS attr = sftp.stat(remote);
            if (attr != null)
                return 1000L * attr.getMTime();
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
        SFTPConnector conn = null;
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
            conn = new SFTPConnector(props);
            str = null;
            switch (action) {
              case ACTION_GET:
                if (filename != null && filename.length() > 0) {
                    file = new File(filename);
                    str = conn.sftpGet(file, path);
                }
                else
                    str = "local file is not defined";
                break;
              case ACTION_PUT:
                if (filename != null && filename.length() > 0) {
                    file = new File(filename);
                    str = conn.sftpPut(file, path);
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
                if (conn.sftpDelete(path))
                    System.out.println(path + " is deleted");
                else
                    str = "failed to delete the file: " + path;
                break;
              case ACTION_PWD:
                str = conn.sftpPwd();
                if (str != null) {
                    System.out.println("working directory: " + str);
                    str = null;
                }
                else
                    str = "failed to print the working directory";
                break;
              case ACTION_CWD:
                if (conn.sftpCwd(path))
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
                if (conn.sftpRmd(path))
                    System.out.println(path + " is removed");
                else
                    str = "failed to remove the directory: " + path;
                break;
              case ACTION_LIST:
                list = conn.sftpList(path, FTP_ALL);
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
        System.out.println("SFTPConnector Version 2.0 (written by Yannan Lu)");
        System.out.println("SFTPConnector: an SFTP client to get or put files");
        System.out.println("Usage: java org.qbroker.net.SFTPConnector -u uri -t timeout -n username -p password -f local_filename -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action (default: pwd)");
        System.out.println("  -u: uri");
        System.out.println("  -n: username");
        System.out.println("  -p: password");
        System.out.println("  -t: timeout in sec (default: 60)");
        System.out.println("  -f: local filename");
    }
}
