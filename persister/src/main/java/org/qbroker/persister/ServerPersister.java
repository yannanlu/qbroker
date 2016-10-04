package org.qbroker.persister;

/* ServerPersister.java - a persister of JMS messages via a ServerSocket */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.FailedLoginException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QList;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.Template;
import org.qbroker.jaas.SimpleCallbackHandler;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageStream;
import org.qbroker.persister.Persister;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventParser;
import org.qbroker.event.Event;

/**
 * ServerPersister listens to a ServerSocket and accepts the socket
 * connections from it.  Once there is a new connection, it will check
 * out a thread to handle the message IO on the socket.  The thread keeps
 * getting JMS messages from the input XQueue and writes the byte stream
 * to the socket.  Currently, it supports two operations, provide and publish.
 * In case of the provide operation, ServerPersister allows multiple client
 * threads to pick up messages from the same XQueue.  ServerPersister supports
 * flow control and allows object control from its owner.  It is fault tolerant
 * with retry and idle options.  In case of the publish operation, however,
 * ServerPersister allows multiple subscribers to pick up messages published
 * from the same XQueue and each client gets its own copies.  In this case,
 * there is no flow control and buffer overrun may occur.  Therefore,
 * ServerPersister is a JMS message outlet, or a JMS message provider.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ServerPersister extends Persister {
    private ServerSocket lsnr = null;
    private MessageStream ms = null;
    private MessageStream ns = null;
    private Thread pub = null;
    private ThreadPool publisher = null;
    private ThreadPool thPool;
    private QList assetList;     // xq for publish
    private QList xqList;        // xq for retry on publish
    private XQueue in = null;    // xq for openning msgs
    private int[] oidList, list;
    private Template template = null;
    private String hostname, jaasLogin = null;
    private SimpleCallbackHandler jaasHandler = null;
    private EventParser parser;
    private SimpleDateFormat zonedDateFormat = null;
    private int retryCount, receiveTime, port;
    private int bufferSize, maxConnection, backlog;
    private long sessionTime, delta;
    private boolean isConnected = false;
    private boolean isPublish = false;
    private boolean keepAlive = false;

    public ServerPersister(Map props) {
        super(props);
        String scheme = null;
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

        if ((o = props.get("MaxConnection")) != null)
            maxConnection = Integer.parseInt((String) o);
        else
            maxConnection = 64;

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

        if (!("provide".equals(operation) || "publish".equals(operation)))
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

        if (!"provide".equals(operation)) {
            isPublish = true;
            list = new int[maxConnection];
            if ((o = props.get("DisplayMask")) != null)
                displayMask = Integer.parseInt((String) o);

            if ((o = props.get("BufferSize")) != null)
                bufferSize = Integer.parseInt((String) o);
            else
                bufferSize = 4096;

            if ((o = props.get("TemplateFile")) != null &&
                ((String) o).length() > 0)
                template = new Template(new File((String) o));
            else if ((o = props.get("Template")) != null &&
                ((String) o).length()>0)
                template = new Template((String) o);

            if ((o = props.get("StringProperty")) != null && o instanceof Map) {
                Iterator iter = ((Map) o).keySet().iterator();
                int n = ((Map) o).size();
                propertyName = new String[n];
                n = 0;
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    if((propertyName[n]=MessageUtils.getPropertyID(key))==null)
                        propertyName[n] = key;
                    n ++;
                }
            }
            publisher = new ThreadPool("publisher", 1, 1, this, "doPublish",
                new Class[]{XQueue.class});
            xqList = new QList(uri, maxConnection);
            assetList = new QList(uri, maxConnection);
            oidList = new int[0];
        }
        else try {
            ms = new MessageStream(props);
        }
        catch (Exception e) {
            disconnect();
          throw(new IllegalArgumentException("failed to create MessageStream: "+
                Event.traceStack(e)));
        }

        thPool = new ThreadPool("subscriber", 1, maxConnection, this, "provide",
            new Class[] {XQueue.class, Socket.class, String.class});

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName+" with " + capacity + "/" + maxConnection).send();

        delta = waitTime / maxRetry;
        if (delta <= 0)
            delta = 1L;

        retryCount = 0;
        sessionTime = 0L;
    }

    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        long count = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(PSTR_READY, PSTR_RUNNING);

        if (isPublish && pub == null) {
            pub = (Thread) publisher.checkout(waitTime);
            if (pub != null) {
                int cid = publisher.getId(pub);
                publisher.assign(new Object[] {xq}, cid);
                new Event(Event.INFO, uri+ ": started the publisher thread " +
                    pub.getName() + " " + publisher.getCount() +
                    "/" + publisher.getSize()).send();
            }
            else {
                new Event(Event.ERR, uri+ ": failed to spawn the publisher "+
                    "thread " + publisher.getCount() + "/" +
                    publisher.getSize()).send();
                disconnect();
                stopAllAssets();
                resetStatus(PSTR_READY, PSTR_CLOSED);
                return;
            }
        }

        for (;;) {
            if (!isConnected && status != PSTR_DISABLED) {
                try {
                    lsnr = new ServerSocket(port, backlog);
                    if (timeout > 0)
                        lsnr.setSoTimeout(timeout);
                    isConnected = true;
                }
                catch (Exception e) {
                }
            }

            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                count += accept(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                if (isConnected)
                    disconnect();
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == PSTR_PAUSE) {
                if (status > PSTR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > PSTR_PAUSE)
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
                status == PSTR_STANDBY) {
                if (status >= PSTR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > PSTR_STANDBY)
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

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
            if (status == PSTR_READY) {
                setStatus(PSTR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        disconnect();
        stopAllAssets();
        new Event(Event.INFO, uri + " stopped on " + linkName + " with " +
            count + " connections accepted").send();
    }

    /**
     * It listens on a server socket for new connections.  Once it
     * accepts a new connection, it checks out a thread from the
     * pool and assigns the task to the pool so that it will be
     * processed by the thread.
     */
    private int accept(XQueue xq, int baseTime) {
        Socket sock = null;
        Thread thr;
        XQueue in;
        Object[] asset;
        String ip;
        int cid, i = 0, n = 0, mask;

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
        } while((mask & XQueue.KEEP_RUNNING)> 0 && (mask & XQueue.STANDBY)== 0);

        if (thr == null) // disabled or stopped
            return 0;

        // listening on the server socket for new connections
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) { // disabled temporarily
                thPool.checkin(thr);
                return 0;
            }
            try {
                sock = lsnr.accept();
                sock.setSoTimeout(receiveTime);
                if (keepAlive)
                    sock.setKeepAlive(true);
                break;
            }
            catch (InterruptedIOException e) {
                continue;
            }
            catch (Exception e) {
                resetStatus(PSTR_RUNNING, PSTR_RETRYING);
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(e)).send();
                disconnect();
                i = ++retryCount;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
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
                    resetStatus(PSTR_RETRYING, PSTR_RUNNING);
                }
                catch (Exception ex) {
                }
                continue;
            }
            catch (Error e) {
                thPool.checkin(thr);
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    e.toString()).send();
                Event.flush(e);
            }
        }

        if (sock == null) {
            thPool.checkin(thr);
            new Event(Event.INFO, uri+ ": got no connection due to "+
                status + "/" + xq.getGlobalMask()).send();
            return 0;
        }

        // got the connection and invoke authentication
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
        if (isPublish) { // for publish
            if (reserveAsset(waitTime, cid) != cid) {
                thPool.checkin(thr);
                socketClose(sock);
                new Event(Event.WARNING, uri+ ": failed to reserve a new " +
                    "XQueue at " + cid + "/" + assetList.depth() + " for " +
                    ip).send();
                return 0;
            }
            in = new IndexedXQueue(uri +"/"+ cid, capacity);
            addAsset(in, cid);
        }
        else {
            in = xq;
        }
        n = thPool.assign(new Object[] {in, sock, ip}, cid);
        new Event(Event.INFO, uri+ ": got a new connection ("+ cid + ") on " +
            linkName+" from "+ip+ " with " + n +" subscribers running").send();
        return 1;
    }

    /**
     * It gets JMS Messages from an XQueue and writes the content of messages
     * to the socket continuously.  It acts like a source of a byte stream.
     * This method is used to write the content on behalf of a subscriber.
     * In case of publish operation, the xq is the one created for the
     * subscriber. The publisher will copy messages into the xq. For provide
     * operation, the xq is the original source. So the flow control on the
     * xq will impact on this method. For example, if the flow tries to disable
     * the persister, the provide method will return.
     */
    public void provide(XQueue xq, Socket sock, String ip) {
        int n = 0, mask;
        long count = 0;
        boolean isDisabled = false;
        int cid = thPool.getId(Thread.currentThread());

        try {
            if (isPublish) // publish
                count += write(xq, sock.getOutputStream(), ip);
            else for (;;) { // provide
                isDisabled = false;
                count += ms.write(xq, sock.getOutputStream());

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    isDisabled = true;
                    do { // wait for confirmation
                        if (isStopped(xq))
                            break;
                        try {
                            Thread.sleep(waitTime);
                        }
                        catch (Exception ex) {
                        }
                    } while (status == PSTR_READY);
                }

                while (status == PSTR_DISABLED) { // disabled
                    if (isStopped(xq))
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception ex) {
                    }
                }
                if (isStopped(xq) || status >= PSTR_STOPPED || !isDisabled)
                    break;
            }
        }
        catch (IOException e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to publish msgs: " + Event.traceStack(e)).send();
        }
        catch (JMSException e) {
            String str = linkName + " " + uri + " failed to publish msgs: ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            new Event(Event.ERR, str + Event.traceStack(e)).send();
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri +
                " failed to publish msgs: " + Event.traceStack(e)).send();
        }
        catch (Error e) {
            if (isPublish) {
                MessageUtils.stopRunning(xq);
                removeAsset(cid);
            }
            socketClose(sock);
            new Event(Event.ERR, linkName + " " + uri +
                " failed to publish msgs: " + Event.traceStack(e)).send();
            Event.flush(e);
        }
        if (isPublish) { // publish
            MessageUtils.stopRunning(xq);
            removeAsset(cid);
        }
        if (sock != null)
            socketClose(sock);
        n = thPool.getCount() - 1;
        new Event(Event.INFO, linkName + uri +": disconnected from subscriber "+
            cid + " at " + ip + " with " + n + " subscribers left").send();
    }

    /** It keeps publishing until its end of life */
    public void doPublish(XQueue xq) {
        long count = 0;
        int mask;

        for (;;) {
            try {
                count += publish(xq);
            }
            catch (Exception e) {
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                new Event(Event.ERR, linkName + " " + uri + ": " +
                    Event.traceStack(e)).send();
                if (status < PSTR_STOPPED) { // notify the main thread to stop
                    setStatus(PSTR_STOPPED);
                    mask = xq.getGlobalMask();
                    xq.setGlobalMask(mask | XQueue.STANDBY);
                }
                Event.flush(e);
            }

            if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                do { // wait for confirmation
                    if (isStopped(xq))
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception ex) {
                    }
                } while (status == PSTR_READY);
            }

            while (status == PSTR_DISABLED) { // disabled
                if (isStopped(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception ex) {
                }
            }

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
        }
        if (status < PSTR_STOPPED) { // notify the main thread to stop
            setStatus(PSTR_STOPPED);
            mask = xq.getGlobalMask();
            xq.setGlobalMask(mask | XQueue.STANDBY);
        }

        new Event(Event.INFO, linkName + ": publisher stopped for "+uri).send();
    }

    /**
     * It gets JMS Messages from an XQueue and publishes the content of
     * messages to all the XQueues for active subscribers continuously.
     * At the other end of each XQueue, a thread will pick up the message
     * content and write it to the socket.  It returns number of messages
     * published.
     */
    private long publish(XQueue xq) {
        Message message;
        int k, sid, mask;
        String dt, msgStr;
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        long currentTime, count = 0;
        byte[] buffer = new byte[bufferSize];

        k = 0;
        currentTime = System.currentTimeMillis();
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                continue;
            }

            message = (Message) xq.browse(sid);
            if (message == null) { // msg is supposed not to be null
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                xq.remove(sid);
                continue;
            }

            dt = null;
            try {
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                   dt = Event.dateFormat(new Date(message.getJMSTimestamp()));
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to get JMS property for "+
                    uri + ": " + Event.traceStack(e)).send();
            }

            msgStr = null;
            try {
                if (template != null)
                    msgStr = MessageUtils.format(message, buffer, template);
                else
                    msgStr = MessageUtils.processBody(message, buffer);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to format msg: " +
                    Event.traceStack(e)).send();
                if (acked) try {
                    message.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
            }
            catch (Error e) {
                new Event(Event.ERR, "failed to format msg: " +
                    Event.traceStack(e)).send();
                if (acked) try {
                    message.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                Event.flush(e);
            }

            if (acked) try {
                message.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg: " +
                    str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg: " +
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                new Event(Event.ERR, "failed to ack msg: " +
                    e.toString()).send();
                xq.remove(sid);
                Event.flush(e);
            }
            xq.remove(sid);

            k = assign(maxRetry, msgStr, getAssetIDs());

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(message, msgStr,
                    displayMask, propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "published a msg to " + k +
                        " subscribers with ( Date: " + dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "published a msg to " + k +
                        " subscribers with (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "published a msg to " + k +
                    " subscribers").send();
            }
            count ++;
            message = null;
        }
        if (displayMask != 0)
            new Event(Event.INFO, uri+": published " + count + " msgs").send();
        return count;
    }

    /**
     * It assigns the msgStr into all active outLinks defined in oids and
     * returns number of outLinks assigned to.
     */
    private int assign(int retry, String msgStr, int[] oids) {
        XQueue xq;
        int i, k = 0, n, oid, sid;
        if (oids == null || msgStr == null || msgStr.length() <= 0)
            return -1;
        n = oids.length;
        if (n <= 0)
            return k;
        xqList.clear();
        for (i=0; i<n; i++) {
            oid = oids[i];
            xq = (XQueue) assetList.browse(oid);
            if (xq == null) {
                removeAsset(oid);
                continue;
            }
            if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                continue;
            sid = xq.reserve(-1L);
            if (sid >= 0) {
                xq.add(msgStr, sid);
                k ++;
                continue;
            }
            // save the xq for retry
            xqList.reserve(oid);
            xqList.add(xq, oid);
        }
        n = xqList.depth();
        if (n <= 0) // no retry needed
            return k;
        for (int j=1; j<=retry; j++) { // for retries
            n = xqList.queryIDs(list, XQueue.CELL_OCCUPIED);
            for (i=0; i<n; i++) {
                oid = list[i];
                xq = (XQueue) xqList.browse(oid);
                if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0) {
                    xqList.getNextID(oid);
                    xqList.remove(oid);
                    continue;
                }
                sid = xq.reserve(j*delta);
                if (sid >= 0) {
                    xq.add(msgStr, sid);
                    xqList.getNextID(oid);
                    xqList.remove(oid);
                    k ++;
                    continue;
                }
            }
            n = xqList.depth();
            if (n <= 0)
                return k;
        }
        n = xqList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            oid = list[i];
            xq = (XQueue) xqList.browse(oid);
            if ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) == 0) {
                xqList.getNextID(oid);
                xqList.remove(oid);
                continue;
            }
            sid = xq.reserve(waitTime);
            if (sid >= 0) {
                xq.add(msgStr, sid);
                k ++;
            }
            else {
                sid = xq.getNextCell(5L);
                xq.remove(sid);
                new Event(Event.WARNING, xq.getName() + ": overran at " + sid +
                    " with " + xq.depth() + "/" + xq.getCapacity()).send();
                sid = xq.reserve(5L, sid);
                xq.add(msgStr, sid);
                k ++;
            }
            xqList.getNextID(oid);
            xqList.remove(oid);
        }
        return k;
    }

    /**
     * It gets a String from the XQueue and write all bytes to the
     * OutputStream continuously.
     *<br/><br/>
     * Most of the IOExceptions from the stream IO operations, like write,
     * will be thrown to notify the caller.  The caller is supposed to catch
     * the exception, either to reset the stream or to do other stuff.
     *<br/><br/>
     * This method is MT-Safe.
     */
    private long write(XQueue xq,OutputStream out,String u) throws IOException {
        long count = 0;
        int sid = -1, mask;
        String msgStr = null;

        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                continue;
            }

            msgStr = (String) xq.browse(sid);
            if (msgStr == null) { // empty payload
                xq.remove(sid);
                new Event(Event.WARNING, "dropped an empty payload from " +
                    xq.getName()).send();
                continue;
            }

            if (msgStr.length() > 0) try {
                out.write(msgStr.getBytes());
                count ++;
            }
            catch (IOException e) {
                xq.remove(sid);
                throw(e);
            }
            catch (Exception e) {
                xq.remove(sid);
                throw(new RuntimeException(e.getMessage()));
            }
            catch (Error e) {
                xq.remove(sid);
                Event.flush(e);
            }

            xq.remove(sid);
            msgStr = null;
        }
        if (displayMask != 0)
            new Event(Event.INFO, xq.getName() + ": wrote " + count +
                " msgs to " + u).send();
        return count;
    }

    /** returns the current oidList */
    private synchronized int[] getAssetIDs() {
        return oidList;
    }

    /** adds the object to assetList and rebuilds oidList */
    private synchronized int addAsset(Object o, int id) {
        int n;
        int[] tmpList;
        n = assetList.add(o, id);
        if (n < 0)
            return -1;
        n = assetList.depth();
        if (!isPublish)
            return n;
        tmpList = new int[n];
        n = assetList.queryIDs(tmpList, XQueue.CELL_OCCUPIED);
        oidList = tmpList;
        return n;
    }

    /** removes the object from assetList and rebuilds oidList */
    private synchronized Object removeAsset(int id) {
        Object o;
        int n;
        int[] tmpList;
        assetList.getNextID(id);
        o = assetList.browse(id);
        assetList.remove(id);
        n = assetList.depth();
        tmpList = new int[n];
        n = assetList.queryIDs(tmpList, XQueue.CELL_OCCUPIED);
        oidList = tmpList;
        notifyAll();
        return o;
    }

    /** reserves the id-th cell on assetList and returns the id */
    private synchronized int reserveAsset(long milliSec, int id) {
        int i = assetList.reserve(id);
        if (i < 0) {
            try {
                wait(milliSec);
            }
            catch (Exception e) {
            }
            i = assetList.reserve(id);
        }
        return i;
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
            new Event(Event.WARNING, linkName + ": failed to " +
                "close "+uri+": "+Event.traceStack(e)).send();
        }
    }

    private void stopAllAssets() {
        int i, n, oid;
        XQueue xq;
        if (!isPublish)
            return;
        n = assetList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
            oid = list[i];
            if (oid < 0)
                continue;
            xq = (XQueue) assetList.browse(oid);
            if (xq == null) {
                removeAsset(oid);
                continue;
            }
            MessageUtils.stopRunning(xq);
        }
        assetList.clear();
    }

    public void close() {
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        disconnect();
        stopAllAssets();
        if (publisher != null)
            publisher.close();
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
