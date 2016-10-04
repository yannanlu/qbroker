package org.qbroker.receiver;

/* ServerReceiver.java - a receiver receiving JMS messages via a ServerSocket */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
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
import org.qbroker.common.QList;
import org.qbroker.common.Browser;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.TimeWindows;
import org.qbroker.jaas.SimpleCallbackHandler;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageStream;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventParser;
import org.qbroker.event.Event;

/**
 * ServerReceiver listens to a ServerSocket and accepts socket connections
 * from it.  Once there is a new connection, it will check out a thread
 * to handle the message IO on the socket.  The thread reads the byte stream,
 * puts them into messages and adds them to the output XQueue.  ServerReceiver
 * supports flow control and allows object control from its owner.  It is
 * fault tolerant with retry and idle options.  The max number of client
 * connections is determined by both Capacity and Partition of the XQueue.
 * Backlog is the queue length of the server socket listener.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ServerReceiver extends Receiver {
    private ServerSocket lsnr = null;
    private MessageStream ms = null;
    private MessageStream ns = null;
    private ThreadPool thPool;
    private XQueue in = null;        // xq for openning msgs
    private String hostname, jaasLogin = null;
    private SimpleCallbackHandler jaasHandler = null;
    private EventParser parser;
    private DateFormat zonedDateFormat = null;
    private int retryCount, receiveTime, port;
    private int bufferSize, backlog, maxConnection;
    private long sessionTime;
    private boolean isConnected = false;
    private boolean keepAlive = false;

    public ServerReceiver(Map props) {
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

        maxConnection = (partition[1]==0)?capacity-partition[0] : partition[1];

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

        if (!"acquire".equals(operation) && !"respond".equals(operation))
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

        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");
        if ((o = props.get("JAASLogin")) != null && o instanceof String) {
            Map<String, Object> ph;
            jaasLogin = (String) o;
            if ((o = props.get("JAASConfig")) != null && o instanceof String)
                System.setProperty("java.security.auth.login.config",(String)o);
            jaasHandler = new SimpleCallbackHandler();
            parser = new EventParser(null);
            in = new IndexedXQueue(uri, 2);

            ph = new HashMap<String, Object>();
            ph.put("Partition", "0,0");
            ph.put("ReceiveTime", "15000");
            ph.put("MaxNumberMessage", "1");
            ph.put("Mode", "utility");
            ph.put("XAMode", "1");
            ph.put("TextMode", "1");
            ph.put("EOTBytes", "0x0a");
            ph.put("DisplayMask", "0");
            try {
                ns = new MessageStream(ph);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(
                    "failed to create MessageStream for authentication: "+
                    Event.traceStack(e)));
            }
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

        thPool = new ThreadPool(uri, 1, maxConnection, this, operation,
            new Class[]{Map.class});

        try {
            ms = new MessageStream(props);
        }
        catch (Exception e) {
            disconnect();
          throw(new IllegalArgumentException("failed to create MessageStream: "+
                Event.traceStack(e)));
        }

        if ("respond".equals(operation)) { // for tracking responses
            // try to load the class anyway
            new DelimitedBuffer(1, 0, (byte[]) null, 0, (byte[]) null);
        }

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName + " with " + capacity +"/"+ maxConnection).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        long count = 0;
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
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
        Socket sock = null;
        Thread thr;
        Map<String, Object> client;
        String ip;
        int cid, n = 0, mask;

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

        do { // check out a thread first
            thr = thPool.checkout(waitTime);
            if (thr != null)
                break;
            mask = xq.getGlobalMask();
        } while((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0);

        if (thr == null) // paused or stopped
            return 0;

        // listening on the server socket for new connections
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) { // paused temporarily
                thPool.checkin(thr);
                return 0;
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
                thPool.checkin(thr);
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    e.toString()).send();
                Event.flush(e);
            }
        }
        if (sock == null) { // too bad, no connection
            thPool.checkin(thr);
            new Event(Event.INFO, uri + " got no connection due to " +
                status + "/" + xq.getGlobalMask() + " on " + linkName).send();
            return 0;
        }

        // got the connection and check authentications
        ip = sock.getInetAddress().getHostAddress();
        if (jaasLogin != null && ns != null) {
            String msgStr = null;
            Event event = authenticate(sock);
            if (event == null) { // time out or bad msg
                new Event(Event.ERR, linkName + " " + uri +
                    " failed to get the authentication msg from " + ip).send();
                thPool.checkin(thr);
                socketClose(sock);
                return 0;
            }
            msgStr = zonedDateFormat.format(new Date()) + " " +
                Event.getIPAddress() +" "+ EventUtils.collectible(event) + "\n";
            try {
                OutputStream out = sock.getOutputStream();
                out.write(msgStr.getBytes(), 0, msgStr.length());
                out.flush();
            }
            catch (Throwable e) {
                new Event(Event.ERR, linkName + " " + uri +
                    " failed to send authentication response back: " +
                    Event.traceStack(e)).send();
                thPool.checkin(thr);
                socketClose(sock);
                return 0;
            }

            if (event.getPriority() == Event.WARNING) { //authentication failed
                new Event(Event.WARNING, uri + " authentication failed from "+
                    ip + " on " + linkName).send();
                event.clearAttributes();
                thPool.checkin(thr);
                socketClose(sock);
                return 0;
            }
            event.clearAttributes();
        }

        // assign the task
        cid = thPool.getId(thr);
        client = new HashMap<String, Object>();
        client.put("Socket", sock);
        client.put("XQueue", xq);
        client.put("Timestamp", new long[] {System.currentTimeMillis()});
        client.put("ClientIP", ip);
        client.put("CID", String.valueOf(cid));
        n = thPool.assign(new Object[] {client}, cid);
        new Event(Event.INFO, uri + " got a new connection ("+ cid + ") on " +
            linkName + " from " + ip + " with " + n +" clients running").send();
        return 1;
    }

    /**
     * It continuously reads byte stream from the socket and package
     * them into JMS Messages.  It adds the messages to an XQueue as
     * output.  The messages are flowing downstream and they will be
     * consumed eventually.  It acts like a destination of messages.
     */
    public void acquire(Map client) {
        Socket sock = (Socket) client.get("Socket");
        XQueue xq = (XQueue) client.get("XQueue");
        String ip = (String) client.get("ClientIP");
        String cid = (String) client.get("CID");
        long count = 0;
        int n = 0, mask;
        boolean isDisabled = false;

        client.clear();
        try {
            for (;;) { // for disabling support
                isDisabled = false;
                count += ms.read(sock.getInputStream(), xq);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // disabled temporarily
                    isDisabled = true;
                    do { // wait for confirmation
                        if (isStopped(xq))
                            break;
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception ex) {
                        }
                    } while (status == RCVR_READY);
                }

                while (status == RCVR_DISABLED) { // disabled
                    if (isStopped(xq))
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception ex) {
                    }
                }
                if (isStopped(xq) || status >= RCVR_STOPPED || !isDisabled)
                    break;
            }
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to acquire from client: "+ Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName +" "+ uri +" failed to acquire from client: ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to acquire from client: "+ Event.traceStack(e)).send();
        }
        catch (Error e) {
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                " failed to acquire from client: "+ Event.traceStack(e)).send();
            Event.flush(e);
        }
        if (sock != null)
            socketClose(sock);
        n = thPool.getCount() - 1;
        new Event(Event.INFO, uri +" disconnected from client "+ cid + " at " +
            ip + " with " + n + " clients left on " + linkName).send();
    }

    /**
     * It continuously reads byte stream from the socket and package
     * them into JMS Messages.  It adds the messages to an XQueue as
     * requests.  The messages flow downstream and the requests will be
     * fulfilled by some nodes.  In the end, the messages will be
     * collected and sent back to the socket as the responses.  It acts
     * like a service on messages.
     */
    public void respond(Map client) {
        QList msgList;
        DelimitedBuffer sBuf = null;
        CollectibleCells cells = null;
        Socket sock = (Socket) client.get("Socket");
        XQueue xq = (XQueue) client.get("XQueue");
        String ip = (String) client.get("ClientIP");
        String cid = (String) client.get("CID");
        int n = 0, sid, mask;
        long count = 0;
        boolean isDisabled = false;

        client.clear();
        msgList = new QList(cid, capacity);
        cells = new CollectibleCells(cid, capacity);
        try {
            sBuf = new DelimitedBuffer(bufferSize, ms.getOffhead(),
                ms.getSotBytes(), ms.getOfftail(), ms.getEotBytes());
        }
        catch (Exception e) {
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                ": failed to create buffer: " + Event.traceStack(e)).send();
            return;
        }
        catch (Error e) {
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                ": failed to create buffer: " + Event.traceStack(e)).send();
            Event.flush(e);
        }

        try {
            for (;;) { // for disabling support
                isDisabled = false;
                count += ms.respond(sock.getInputStream(), xq,
                    sock.getOutputStream(), msgList, sBuf, cells);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // disabled temporarily
                    isDisabled = true;
                    do { // wait for confirmation
                        if (isStopped(xq))
                            break;
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception ex) {
                        }
                    } while (status == RCVR_READY);
                }

                while (status == RCVR_DISABLED) { // disabled
                    if (isStopped(xq))
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception ex) {
                    }
                }
                if (isStopped(xq) || status >= RCVR_STOPPED || !isDisabled)
                    break;
            }
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to respond to client: " + Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName +" "+ uri + " failed to respond to client: ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to respond to client: " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                " failed to respond to client: " + Event.traceStack(e)).send();
            msgList.clear();
            cells.clear();
            sBuf.reset();
            Event.flush(e);
        }
        if (sock != null)
            socketClose(sock);
        msgList.clear();
        cells.clear();
        sBuf.reset();
        n = thPool.getCount() - 1;
        new Event(Event.INFO, uri +" disconnected from client "+ cid + " at " +
            ip + " with " + n + " clients left on " + linkName).send();
    }

    /**
     * It reads the openning msg from the socket and tries to authenticate the
     * username and the password in the msg via the JAAS plugin. It retrurns
     * null for time-out or a bad message. Otherwise, an event as the response
     * msg is returned with either Event.NOTICE for success or Event.WARNING
     * for failure.
     */
    private Event authenticate(Socket sock) {
        Event event = null;
        Message msg;
        String msgStr, username, password;
        int n = 0;

        if (in == null || ns == null)
            return null;

        // since maxNumberMSg = 1 and mode = 0, ns.read() may time out
        try { // ns.read() is MT-Safe
            n = (int) ns.read(sock.getInputStream(), in);
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName + " " + uri + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
        }
        msg = (Message) in.browse(0);
        in.clear();
        if (n <= 0 || msg == null) {
            new Event(Event.ERR, linkName + " " + uri +
                ": timed out on read").send();
            return null;
        }

        try {
            msgStr = MessageUtils.processBody(msg, new byte[0]);
            event = parser.parse(msgStr);
        }
        catch (Throwable e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            return null;
        }
        username = event.getAttribute("username");
        password = event.getAttribute("password");
        event.removeAttribute("username");
        event.removeAttribute("password");
        if (login(username, password)) { // authenticated
            event.setPriority(Event.NOTICE);
            event.setAttribute("text", "connection OK");
        }
        else { // failed
            event.setPriority(Event.WARNING);
            event.setAttribute("text", "Authentication failed for " + username);
        }

        return event;
    }

    /**
     * It authenticates the username and the password via the JAAS plugin
     * and returns true on success or false on failure.
     */
    private boolean login(String username, String password) {
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
        if (lsnr != null) try {
            lsnr.close();
        }
        catch (Exception e) {
            new Event(Event.WARNING, linkName + " " + uri +
                ": failed to close lsnr: " + Event.traceStack(e)).send();
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
        if (in != null)
            in.clear();
    }

    protected void finalize() {
        close();
    }
}
