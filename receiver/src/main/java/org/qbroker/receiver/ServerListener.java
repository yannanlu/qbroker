package org.qbroker.receiver;

/* ServerListener.java - a Socket listener for JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.FailedLoginException;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.Template;
import org.qbroker.jaas.SimpleCallbackHandler;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageStream;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventParser;
import org.qbroker.event.Event;

/**
 * ServerListener listens to a ServerSocket and accepts socket connections
 * from it.  Once there is a new connection, it will check out a thread
 * to handle the authentication on the socket.  The thread reads the byte
 * stream, converts them into an ObjectEvent and invokes authentcation method
 * on it.  If successful, it sends a response message back to the socket.
 * Before the thread exits, it sets the socket to the event and adds the
 * message to the output XQueue.  ServerListener supports flow control and
 * allows object control from its owner.  It is fault tolerant with retry
 * and idle options.  The maximum number of client connections is determined
 * by both Capacity and Partition of the XQueue.  Backlog is the queue length
 * of the server socket listener.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ServerListener extends Receiver {
    private ServerSocket lsnr = null;
    private MessageStream ms = null;
    private ThreadPool thPool;
    private AssetList reqList = null;//xq for tracking initial negotiations only
    private EventParser parser;
    private DateFormat zonedDateFormat = null;
    private String hostname, jaasLogin = null;
    private SimpleCallbackHandler jaasHandler = null;
    private int retryCount, receiveTime, port;
    private int bufferSize, backlog, maxConnection;
    private long sessionTime;
    private boolean isConnected = false;
    private boolean keepAlive = false;

    public ServerListener(Map props) {
        super(props);
        String scheme = null, path = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException("invalid uri: " + uri));
        }

        if (capacity <= 0)
            capacity = 1;

        maxConnection = (partition[1] == 0)?capacity-partition[0]:partition[1];

        backlog = 1;
        if ((o = props.get("Backlog")) != null)
            backlog = Integer.parseInt((String) o);
        if (backlog <= 0)
            backlog = 1;

        if ((o = props.get("KeepAlive")) != null &&
            "true".equals((String) o))
            keepAlive = true;
        else
            keepAlive = false;

        if (!"accept".equals(operation))
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));

        // timeout is the block time in sec to accept a connection
        if ((o = props.get("Timeout")) != null) {
            timeout = 1000 * Integer.parseInt((String) o);
            if (timeout < 0)
                timeout = 1000;
        }
        else
            timeout = 1000;

        // receiveTime is the socket read timeout in ms
        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        else
            receiveTime = 1000;
        if ((o = props.get("BufferSize")) == null ||
            (bufferSize = Integer.parseInt((String) o)) <= 0)
            bufferSize = 4096;

        if ((o = props.get("JAASLogin")) != null && o instanceof String) {
            jaasLogin = (String) o;
            if ((o = props.get("JAASConfig")) != null && o instanceof String)
                System.setProperty("java.security.auth.login.config",(String)o);
            jaasHandler = new SimpleCallbackHandler();
        }

        scheme = u.getScheme();
        if ("tcp".equals(scheme)) {
            port = u.getPort();
            try {
                lsnr = new ServerSocket(port, backlog);
                if (timeout > 0)
                    lsnr.setSoTimeout(timeout);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to bind socket on " +
                    "port " + port + ": " + Event.traceStack(e)));
            }
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        parser = new EventParser(null);
        reqList = new AssetList(uri, maxConnection);
        thPool = new ThreadPool(uri, 1, maxConnection, this, "negotiate",
            new Class[]{HashMap.class});

        Map<String, Object> ph = new HashMap<String, Object>();
        ph.put("Partition", "0,0");
        ph.put("ReceiveTime", "15000");
        ph.put("MaxNumberMessage", "1");
        ph.put("Mode", "utility");
        ph.put("XAMode", "1");
        ph.put("TextMode", "1");
        ph.put("EOTBytes", "0x0a");
        ph.put("DisplayMask", "0");
        try {
            ms = new MessageStream(ph);
        }
        catch (Exception e) {
            disconnect();
          throw(new IllegalArgumentException("failed to create MessageStream: "+
                Event.traceStack(e)));
        }

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");
        retryCount = 0;
        sessionTime = 0L;
    }

    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        long count = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        for (;;) {
            if (!isConnected && status != RCVR_DISABLED) {
                try {
                    lsnr = new ServerSocket(port, backlog);
                    if (timeout > 0)
                        lsnr.setSoTimeout(timeout);
                    isConnected = true;
                }
                catch (Exception e) {
                }
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                count += accept(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // pause temporarily
                    if (status == RCVR_READY) // for confirmation
                        setStatus(RCVR_DISABLED);
                    else if (status == RCVR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > RCVR_RETRYING && status < RCVR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == RCVR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                if (isConnected && Utils.getOutstandingSize(xq, partition[0],
                    partition[1]) <= 0) // safe to disconnect
                    disconnect();
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
                if (isConnected) // disconnect anyway
                    disconnect();
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == RCVR_PAUSE) {
                if (status > RCVR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > RCVR_PAUSE)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                status == RCVR_STANDBY) {
                if (status > RCVR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > RCVR_STANDBY)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            if (isStopped(xq) || status >= RCVR_STOPPED)
                break;
            if (status == RCVR_READY) {
                setStatus(RCVR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        disconnect();
        new Event(Event.INFO, uri + " stopped on " + linkName + " with " +
            count + " connections accepted").send();
    }

    /**
     * First it checks out a thread from the pool.  Then it listens on the
     * server socket for new connections.  Once it accepts a new connection, it
     * assigns the task to the pool so that it will be processed by the thread.
     */
    private int accept(XQueue xq, int baseTime) {
        XQueue in;
        Socket sock = null;
        Thread thr;
        HashMap<String, Object> client;
        String ip;
        long tm;
        int cid = -1, tid, len, shift, k, n = 0, mask;

        if (baseTime <= 0)
            baseTime = pauseTime;

        if (!isConnected) {
            try {
                lsnr = new ServerSocket(port, backlog);
                if (timeout > 0)
                    lsnr.setSoTimeout(timeout);
                isConnected = true;
            }
            catch (Exception e) {
            }
        }

        len = partition[1];
        switch (len) {
          case 0:
            shift = 0;
            do { // reserve an empty cell
                cid = xq.reserve(waitTime);
                if (cid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            shift = partition[0];
            do { // reserve the empty cell
                cid = xq.reserve(waitTime, shift);
                if (cid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            shift = partition[0];
            do { // reserve an partitioned empty cell
                cid = xq.reserve(waitTime, shift, len);
                if (cid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
        }

        if (cid < 0) // pasued or stopped
            return 0;

        // listening on the server socket for new connections
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // paused temporarily
                xq.cancel(cid);
                return 0;
            }
            if (reqList.size() > 0) { // check active requests
                long currentTime = System.currentTimeMillis();
                Browser browser = reqList.browser();
                while ((tid = browser.next()) >= 0) {
                    tm = reqList.getMetaData(tid)[0];
                    if (currentTime - tm >= 300000) {
                        in = (XQueue) reqList.remove(tid);
                        if (in != null)
                            MessageUtils.stopRunning(in);
                    }
                }
            }

            try {
                sock = lsnr.accept();
                sock.setSoTimeout(receiveTime);
                if (keepAlive)
                    sock.setKeepAlive(keepAlive);
                break;
            }
            catch (InterruptedIOException e) {
                continue;
            }
            catch (Exception e) {
                resetStatus(RCVR_RUNNING, RCVR_RETRYING);
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(e)).send();
                disconnect();
                retryCount ++;
                if (retryCount > 1) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception ex) {
                }
                try {
                    lsnr = new ServerSocket(port, backlog);
                    if (timeout > 0)
                        lsnr.setSoTimeout(timeout);
                    isConnected = true;
                    resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                }
                catch (Exception ex) {
                }
            }
            catch (Error e) {
                xq.cancel(cid);
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    e.toString()).send();
                Event.flush(e);
            }
        }
        if (sock == null) { // too bad, no connection
            xq.cancel(cid);
            new Event(Event.INFO, uri + " got no connection due to " +
                status + "/" + xq.getGlobalMask() + " on " + linkName).send();
            return 0;
        }

        ip = sock.getInetAddress().getHostAddress();
        thr = thPool.checkout(waitTime);
        if (thr == null) {
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.WARNING, linkName + " " + uri +
                ": failed to check out new connection for " + ip).send();
            return 0;
        }

        // got the connection and assign the task
        tm = System.currentTimeMillis();
        tid = thPool.getId(thr);
        client = new HashMap<String, Object>();
        client.put("Socket", sock);
        client.put("XQueue", xq);
        client.put("Timestamp", new long[] {tm});
        client.put("ClientIP", ip);
        client.put("TID", String.valueOf(tid));
        client.put("CID", String.valueOf(cid));
        if (reqList.getKey(tid) != null) { // not clean up yet
            in = (XQueue) reqList.remove(tid);
            if (in != null)
                MessageUtils.stopRunning(in);
        }
        reqList.add(ip+"_"+tid, new long[]{tm}, new IndexedXQueue(ip, 1), tid);
        n = thPool.assign(new Object[] {client}, tid);
        new Event(Event.INFO, uri + " got a new connection (" + tid + ") on " +
            linkName + " from " + ip + " with " + n +" clients running").send();
        return 1;
    }

    /**
     * It reads the first message from the socket, then packages and evalues
     * the message for negotiations.  It generates an ObjectMessage and copies
     * over the properties of the incoming message.  It puts the socket into
     * the outgoing ObjectMessage and adds the message to the XQueue.  The
     * message flow downstream will pick up the socket and operates on it
     * according to the message properties and the policies.
     *<br>
     * It is MT-Safe.
     */
    public void negotiate(HashMap client) {
        Event event = null;
        Message msg;
        String msgStr;
        Socket sock = (Socket) client.get("Socket");
        XQueue in, xq = (XQueue) client.get("XQueue");
        String ip = (String) client.get("ClientIP");
        int cid = Integer.parseInt((String) client.get("CID"));
        int tid = Integer.parseInt((String) client.get("TID"));
        int n = 0;

        client.remove("XQueue");
        client.remove("CID");
        client.remove("TID");
        in = (XQueue) reqList.get(tid);
        if (in == null || !keepRunning(in)) { 
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                ": failed to retrieve xq").send();
            return;
        }

        // since maxNumberMSg = 1 and mode = 0, ms.read() may time out
        try { // ms.read() is MT-Safe
            n = (int) ms.read(sock.getInputStream(), in);
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + uri +
               " failed to negotiate with client: "+Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName +" "+uri+" failed to negotiate with cleint: ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri +
               " failed to negotiate with client: "+Event.traceStack(e)).send();
        }
        catch (Error e) {
            xq.cancel(cid);
            MessageUtils.stopRunning(in);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
               " failed to negotiate with client: "+Event.traceStack(e)).send();
            Event.flush(e);
        }
        msg = (Message) in.browse(0);
        in.clear();
        MessageUtils.stopRunning(in);
        if (n <= 0 || msg == null) {
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                ": timed out on read").send();
            return;
        }
        // got the first msg
        if (jaasLogin != null) try { // authentication is enabled
            String username, password;
            msgStr = MessageUtils.processBody(msg, new byte[0]);
            event = parser.parse(msgStr);
            username = event.getAttribute("username");
            password = event.getAttribute("password");
            event.removeAttribute("username");
            event.removeAttribute("password");
            if (!login(username, password)) {
                event.setPriority(Event.WARNING);
                event.setAttribute("text", "Authentication failed for " +
                    username);
                msgStr = zonedDateFormat.format(new Date()) + " " +
                    Event.getIPAddress()+" "+EventUtils.collectible(event)+"\n";
                try {
                    OutputStream out = sock.getOutputStream();
                    out.write(msgStr.getBytes(), 0, msgStr.length());
                    out.flush();
                }
                catch (Throwable e) {
                    new Event(Event.ERR, linkName + " " + uri +
                        " failed to send response back: " +
                        Event.traceStack(e)).send();
                }
                event.clearAttributes();
                new Event(Event.WARNING, uri + " authentication failed for " +
                    username + " from "+ ip + " at " + tid + " on " +
                    linkName).send();
                return;
            }
        }
        catch (Throwable e) {
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            return;
        }
        try {
            String value;
            int ic;
            if (jaasLogin == null) {
                msgStr = MessageUtils.processBody(msg, new byte[0]);
                event = parser.parse(msgStr);
            }
            msg = new ObjectEvent();
            msg.setJMSTimestamp(event.getTimestamp());
            msg.setJMSPriority(9-event.getPriority());
            event.removeAttribute("priority");
            event.removeAttribute("text");
            for (String key : event.getAttributeNames()) {
                if (key == null || key.length() <= 0)
                    continue;
                value = event.getAttribute(key);
                if (value == null)
                    continue;
                ic = MessageUtils.setProperty(key, value, msg);
                if (ic != 0) {
                    new Event(Event.WARNING, linkName + " " + uri +
                        ": failed to set property of " +key+ " for "+ip).send();
                }
            }
            ((ObjectEvent) msg).setObject(client);
        }
        catch (Throwable e) {
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            return;
        }

        event.setPriority(Event.NOTICE);
        event.setAttribute("text", "connection OK");
        msgStr = zonedDateFormat.format(new Date()) + " " +
            Event.getIPAddress() + " " + EventUtils.collectible(event) + "\n";
        try {
            OutputStream out = sock.getOutputStream();
            out.write(msgStr.getBytes(), 0, msgStr.length());
            out.flush();
        }
        catch (Throwable e) {
            xq.cancel(cid);
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                " failed to send response back: " + Event.traceStack(e)).send();
            return;
        }
        xq.add(msg, cid);
        event.clearAttributes();

        new Event(Event.INFO, uri + " accepted a connection from "+
            ip + " at " + tid + " on " + linkName).send();
    }

    private String send(OutputStream out, Message message, Template temp,
        byte[] buffer) {
        int length;
        String msgStr = null;

        if (message == null || out == null)
            return null;
        try {
            if (temp != null)
                msgStr = MessageUtils.format(message, buffer, temp);
            else
                msgStr = MessageUtils.processBody(message, buffer);
        }
        catch (JMSException e) {
            return null;
        }

        if (msgStr == null)
            return null;

        length = msgStr.length();
        if (length > 0) try {
            out.write(msgStr.getBytes(), 0, length);
            out.flush();
        }
        catch (IOException e) {
            new Event(Event.WARNING, linkName + " " + uri +
                ": failed to send response").send();
        }
        return msgStr;
    }

    /**
     * It authenticates the username and the password via the JAAS plugin
     * and returns true on success or false on failure.
     */
    private synchronized boolean login(String username, String password) {
        boolean ic = false;
        LoginContext loginCtx = null;
        // moved loginContext here due to Krb5LoginModule not reusable
        try {
            loginCtx = new LoginContext(jaasLogin, jaasHandler);
        }
        catch (LoginException e) {
            new Event(Event.ERR, "failed to create LoginContext for " +
                jaasLogin + ": " + Event.traceStack(e)).send();
            return false;
        }
        jaasHandler.setName(username);
        jaasHandler.setPassword(password);
        try {
            loginCtx.login();
            ic = true;
        }
        catch (FailedLoginException e) {
            jaasHandler.setPassword("");
            ic = false;
        }
        catch (LoginException e) {
            new Event(Event.WARNING, uri + " login failed for " +
                username + ": " + Event.traceStack(e)).send();
        }
        jaasHandler.setPassword("");
        if (ic) try {
            loginCtx.logout();
        }
        catch (Exception e) {
        }
        loginCtx = null;
        return ic;
    }

    private void socketClose(Socket sock) {
        if (sock != null) {
            try {
                OutputStream os = sock.getOutputStream();
                if (os != null)
                    os.flush();
            }
            catch (Exception e) {
            }
            try {
                sock.shutdownInput();
            }
            catch (Exception e) {
            }
            try {
                sock.shutdownOutput();
            }
            catch (Exception e) {
            }
            try {
                sock.close();
            }
            catch (Exception e) {
            }
            sock = null;
        }
    }

    private void disconnect() {
        isConnected = false;
        if (lsnr != null) {
            try {
                lsnr.close();
            }
            catch (Exception e) {
                new Event(Event.WARNING, linkName + " " + uri +
                    ": failed to close lsnr: "+Event.traceStack(e)).send();
            }
        }
    }

    public void close() {
        if (status != RCVR_CLOSED) 
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (lsnr != null)
            lsnr = null;
        if (thPool != null)
            thPool.close();
        if (reqList != null)
            reqList.clear();
    }

    protected void finalize() {
        close();
    }
}
