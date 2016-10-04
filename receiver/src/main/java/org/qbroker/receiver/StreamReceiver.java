package org.qbroker.receiver;

/* StreamReceiver.java - a receiver receiving JMS messages from a stream */

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.SerialPortDevice;
import org.qbroker.common.QList;
import org.qbroker.common.DelimitedBuffer;
import org.qbroker.common.CollectibleCells;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.net.ClientSocket;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageStream;
import org.qbroker.receiver.Receiver;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventParser;

/**
 * StreamReceiver listens to an InputStream and receives byte stream
 * from it.  It puts the JMS Messages to an XQueue as the output.
 * StreamReceiver supports flow control and allows object control from
 * its owner.  It is fault tolerant with retry and idle options.
 *<br/><br/>
 * In case that Destination is defined and scheme is tcp, StreamReceiver
 * will send a request to the server to open the destination after the
 * connection and waits for the response.  The request is supposed to contain
 * all the properties about the connection, the destination and the operation.
 * After the connection is established and the destination is opened,
 * StreamReceiver will read JMS messages out of the InputStream for the
 * normal operation.  In this case, SOTimeout is used for connection timeout
 * while ReceiveTime is used to timeout read().
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class StreamReceiver extends Receiver {
    private MessageStream ms = null;
    private InputStream in = null;
    private Socket sock = null;
    private SerialPortDevice comm = null;
    private QList msgList = null;
    private CollectibleCells cells = null;
    private DelimitedBuffer sBuf = null;
    private File inputFile = null;
    private OutputStream out;
    private List[] dependencyGroup = null;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private EventParser parser = null;
    private Event openningMsg = null;
    private DateFormat zonedDateFormat = null;
    private String hostname;
    private byte[][] connRequest = null;
    private byte[][] quitRequest = null;
    private String[] errorResponse = null;
    private int retryCount, receiveTime, port;
    private int type, bufferSize = 0;
    private int soTimeout = 60000;
    private long sessionTime;
    private boolean keepAlive = false, isConnected = false;
    private boolean doResponse = false, withGreeting = false;
    public final static int NOTCONNECTED = -1;
    public final static int CMDOK = 0;
    public final static int DATAERROR = 1;
    public final static int CMDFAILED = 2;
    public final static int ERRORFOUND = 3;
    public final static int CLIENTERROR = 4;
    public final static int SERVERERROR = 5;
    public final static int READFAILED = 6;
    public final static int WRITEFAILED = 7;
    public final static int CONNTIMEOUT = 8;
    public final static int CONNREFUSED = 9;
    private final static int STREAM_PORT = 1;
    private final static int STREAM_FILE = 2;
    private final static int STREAM_SOCK = 3;
    private final static int STREAM_PIPE = 4;

    @SuppressWarnings("unchecked")
    public StreamReceiver(Map props) {
        super(props);
        String scheme = null, path = null;
        URI u;
        Object o;

        if (uri == null || uri.length() <= 0) { // default uri
            uri = "comm://" + (String) props.get("Device");
            props.put("URI", uri);
        }

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException("invalid uri: " + uri));
        }

        try {
            ms = new MessageStream(props);
        }
        catch (Exception e) {
          throw(new IllegalArgumentException("failed to create MessageStream: "+
                Event.traceStack(e)));
        }

        operation = ms.getOperation();
        if (!"read".equals(operation) && !"response".equals(operation) &&
            !"collect".equals(operation)) {
            disconnect();
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));
        }

        scheme = u.getScheme();
        path = u.getPath();
        if ("comm".equals(scheme)) {
            int baudRate = 9600;
            int dataBits = 8;
            String stopBits = "1";
            String parity = "None";
            String device = null;
            String flowControl = "None";

            type = STREAM_PORT;
            device = (String) props.get("Device");
            if (props.get("BaudRate") != null)
                baudRate = Integer.parseInt((String) props.get("BaudRate"));
            if (props.get("DataBits") != null)
                dataBits = Integer.parseInt((String) props.get("DataBits"));
            if (props.get("StopBits") != null)
                stopBits = (String) props.get("StopBits");
            if (props.get("Parity") != null)
                parity = (String) props.get("Parity");
            if (props.get("FlowControl") != null)
                flowControl = (String) props.get("FlowControl");
            if (device == null || device.length() == 0) {
                if (path == null || path.length() == 0)
                   throw(new IllegalArgumentException("URI has no path: "+uri));
                device = path;
            }
            if ("respond".equals(operation)) {
                doResponse = true;
            }
            if ((o = props.get("ReceiveTime")) != null)
                receiveTime = Integer.parseInt((String) o);
            else
                receiveTime = 1000;

            try {
                comm = new SerialPortDevice(device, baudRate, dataBits,
                    stopBits, parity, flowControl, "read-write", receiveTime);
            }
            catch (IOException e) {
                throw(new IllegalArgumentException(device + ": " +
                    Event.traceStack(e)));
            }

            if (comm == null) {
                new Event(Event.ERR, linkName + ": " + Event.traceStack(
                    new IOException("failed to open "+ device))).send();
                throw(new IllegalArgumentException("failed to open: "+ device));
            }
        }
        else if ("file".equals(scheme)) {
            String filename = (String) props.get("InputFile");
            if (filename == null || filename.length() == 0) {
                if (uri.equals("file://-"))
                    path = "-";
                else if (path == null || path.length() == 0)
                   throw(new IllegalArgumentException("URI has no path: "+uri));
                filename = path;
            }
            if ("-".equals(filename)) {
                in = System.in;
                if ("response".equals(operation)) {
                    doResponse = true;
                }
            }
            else try {
                inputFile = new File(filename);
                in = (InputStream) new FileInputStream(inputFile);
            }
            catch (IOException e) {
                throw(new IllegalArgumentException(inputFile + ": " +
                    Event.traceStack(e)));
            }
            if ((o = props.get("DependencyGroup")) != null &&
                o instanceof List) {
                dependencyGroup = MonitorUtils.getDependencies((List) o);
                MonitorUtils.checkDependencies(System.currentTimeMillis(),
                    dependencyGroup, uri);
            }

            type = STREAM_FILE;
        }
        else if ("tcp".equals(scheme)) {
            int i;
            String patternStr = null;
            port = u.getPort();
            hostname = u.getHost();
            if ((o = props.get("ReceiveTime")) != null)
                receiveTime = Integer.parseInt((String) o);
            else
                receiveTime = 1000;
            if ((o = props.get("SOTimeout")) != null)
                soTimeout = 1000 * Integer.parseInt((String) o);
            if (soTimeout < 0)
                soTimeout = 60000;
            if ((o = props.get("KeepAlive")) != null &&
                "true".equals((String) o))
                keepAlive = true;
            else
                keepAlive = false;
            if ((o = props.get("BufferSize")) == null ||
                (bufferSize = Integer.parseInt((String) o)) <= 0)
                bufferSize = 4096;

            if ((o = props.get("WithGreeting")) != null &&
                "true".equals((String) o))
                withGreeting = true;

            zonedDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");
            if ((o = props.get("DestinationProperty")) != null &&
                o instanceof Map) { // connection to SocketListener
                String text, key;
                Map h = (Map) o;
                openningMsg = new TextEvent();
                text = (String) h.get("URI");
                openningMsg.setAttribute("uri", text);
                openningMsg.setPriority(openningMsg.getPriority());
                text = (String) h.get("Operation");
                openningMsg.setAttribute("operation", text);
                if ((o = props.get("Username")) != null)
                    openningMsg.setAttribute("username", (String) o);
                if ((o = props.get("Password")) != null)
                    openningMsg.setAttribute("password", (String) o);
                Iterator iter = h.keySet().iterator();
                while (iter.hasNext()) { // copy properties over
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    if ("URI".equals(key) || "Operation".equals(key))
                        continue;
                    if ((o = h.get(key)) == null || !(o instanceof String) ||
                        ((String) o).length() <= 0)
                        continue;
                    openningMsg.setAttribute(key, (String) o);
                }
                parser = new EventParser(null);
                connRequest = new byte[1][];
                quitRequest = new byte[0][];
                errorResponse = new String[0];
                patternStr = "\n";
            }
            else if ((o = props.get("Destination")) != null &&
                o instanceof String) { // connection to SocketListener
                String text;
                openningMsg = new TextEvent();
                openningMsg.setAttribute("uri", (String) o);
                openningMsg.setPriority(openningMsg.getPriority());
                openningMsg.setAttribute("operation", operation);
                if ((o = props.get("Username")) != null)
                    openningMsg.setAttribute("username", (String) o);
                if ((o = props.get("Password")) != null)
                    openningMsg.setAttribute("password", (String) o);
                if ((o = props.get("StringProperty")) != null &&
                    o instanceof Map) { // set properties
                    String key;
                    Map h = (Map) o;
                    Iterator iter = h.keySet().iterator();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        if (key == null || key.length() <= 0)
                            continue;
                        if ("uri".equals(key) || "operation".equals(key))
                            continue;
                        if ((o = h.get(key)) == null || !(o instanceof String)||
                            ((String) o).length() <= 0)
                            continue;
                        openningMsg.setAttribute(key, (String) o);
                    }
                }
                parser = new EventParser(null);
                connRequest = new byte[1][];
                quitRequest = new byte[0][];
                errorResponse = new String[0];
                patternStr = "\n";
            }
            else if ((o = props.get("ConnectionRequest")) != null &&
                o instanceof List) { // connection to arbitrary service
                List list = (List) o;
                connRequest = new byte[list.size()][];
                for (i=0; i<connRequest.length; i++)
                    connRequest[i] = ((String) list.get(i) + "\n").getBytes();
                if ((o = props.get("Pattern")) != null)
                    patternStr = (String) o;

                if ((o = props.get("ErrorResponse")) != null &&
                    o instanceof List)
                    list = (List) o;
                else
                    list = new ArrayList();
                errorResponse = new String[list.size()];
                for (i=0; i<errorResponse.length; i++)
                    errorResponse[i] = (String) list.get(i);

                if ((o = props.get("CloseRequest")) != null &&
                    o instanceof List)
                    list = (List) o;
                else
                    list = new ArrayList();
                quitRequest = new byte[list.size()][];
                for (i=0; i<quitRequest.length; i++)
                    quitRequest[i] = ((String) list.get(i) + "\n").getBytes();
            }
            else {
                connRequest = new byte[0][];
                if ((o = props.get("Pattern")) != null)
                    patternStr = (String) o;
            }

            if (connRequest.length > 0 && patternStr == null)
                throw(new IllegalArgumentException("Pattern not defined " +
                    "for connections"));
            else if (patternStr != null) try {
                Perl5Compiler pc = new Perl5Compiler();
                pm = new Perl5Matcher();

                pattern = pc.compile(patternStr);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(e.toString()));
            }

            i = CMDOK;
            if (connRequest.length == 0) {
                try {
                    sock = ClientSocket.connect(hostname, port, soTimeout);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to connect on " + uri +
                        ": " + e.toString()).send();
                    sock = null;
                }
                if (sock != null) try {
                    if (receiveTime > 0)
                        sock.setSoTimeout(receiveTime);
                    if (keepAlive)
                        sock.setKeepAlive(true);
                }
                catch (Exception e) {
                    new Event(Event.WARNING, "failed to set socket on " + uri +
                        ": " + e.toString()).send();
                    socketClose();
                }
            }
            else
                i = socketConnect();
            if (sock == null) {
                new Event(Event.ERR, linkName + " "+uri+ ": "+Event.traceStack(
                    new IOException("socket timedout"))).send();
              throw(new IllegalArgumentException("failed to connect to: "+uri));
            }

            type = STREAM_SOCK;
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        if (doResponse) {
            msgList = new QList(uri, capacity);
            cells = new CollectibleCells(uri, capacity);
            sBuf = new DelimitedBuffer(bufferSize, ms.getOffhead(),
                ms.getSotBytes(), ms.getOfftail(), ms.getEotBytes());
        }

        isConnected = true;
        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        if (doResponse) { 
            msgList.clear();
            cells.clear();
            if (capacity != msgList.getCapacity()) {
                msgList = new QList(uri, capacity);
                cells = new CollectibleCells(uri, capacity);
            }
            sBuf.reset();
        }

        for (;;) {
            if (!isConnected && status != RCVR_DISABLED) {
                switch (type) {
                  case STREAM_PORT:
                    try {
                        comm.portOpen();
                    }
                    catch (IOException e) {
                    }
                    break;
                  case STREAM_FILE:
                    if (inputFile != null) try {
                        in = (InputStream) new FileInputStream(inputFile);
                    }
                    catch (IOException e) {
                    }
                    break;
                  case STREAM_SOCK:
                    socketReconnect();
                    break;
                  default:
                    break;
                }
                isConnected = true;
            }

            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                switch (type) {
                  case STREAM_PORT:
                    portOperation(xq, baseTime);
                    break;
                  case STREAM_FILE:
                    fileOperation(xq, baseTime);
                    if (dependencyGroup == null)
                        setStatus(RCVR_STOPPED);
                    break;
                  case STREAM_SOCK:
                    socketOperation(xq, baseTime);
                    break;
                  default:
                    setStatus(RCVR_STOPPED);
                    break;
                }

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

        if (doResponse) { // clean up
            msgList.clear();
            sBuf.reset();
        }

        disconnect();
        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int portOperation(XQueue xq, int baseTime) {
        int i = 0;
        String device = comm.getPortName();

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (doResponse)
                ms.respond(comm.getInputStream(), xq, comm.getOutputStream(),
                    msgList, sBuf, cells);
            else
                ms.read(comm.getInputStream(), xq);
        }
        catch (IOException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            new Event(Event.ERR, linkName + " " + device + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

                try {
                    comm.close();
                }
                catch (IOException ex) {
                }
                try {
                    comm.portOpen();
                }
                catch (IOException ee) {
                    new Event(Event.ERR, linkName + ": " +
                        Event.traceStack(new IOException("failed to open " +
                        device + " after " + i + " retries"))).send();
                    continue;
                }
                new Event(Event.INFO, device + " reopened on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                sessionTime = System.currentTimeMillis();
                isConnected = true;
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (JMSException e) {
            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;
            String str = linkName + " " + device + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            i = retryCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                i = (i - 1) % repeatPeriod + 1;
            if (retryCount > 1) try {
                Thread.sleep(standbyTime);
            }
            catch (Exception e1) {
            }
            if (i == 1 || i == maxRetry)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + device + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        catch (Error e) {
            disconnect();
            new Event(Event.ERR, linkName + " " + device + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            Event.flush(e);
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private int fileOperation(XQueue xq, int baseTime) {
        int i = 0;
        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (doResponse) { 
                ms.respond(in, xq, System.out, msgList, sBuf, cells);
                return 0;
            }
            else if ("collect".equals(operation)) { // for collect
                ms.collect(in, xq, soTimeout);
                return 0;
            }
            ms.read(in, xq);
            if (dependencyGroup != null && inputFile != null) {
                while (keepRunning(xq) && status == RCVR_RUNNING) {
                    try {
                        Thread.sleep(baseTime);
                    }
                    catch (Exception ex) {
                    }
                    i=MonitorUtils.checkDependencies(System.currentTimeMillis(),
                        dependencyGroup, uri);
                    if (i == MonitorReport.NOSKIP) { // updated
                        try {
                            in.close();
                        }
                        catch (IOException ex) {
                        }
                        in = (InputStream) new FileInputStream(inputFile);
                        ms.read(in, xq);
                    }
                }
            }
        }
        catch (IOException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            if (inputFile == null)
                return 0;

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception ex) {
                }

                if (i > maxRetry)
                    continue;

                try {
                    in.close();
                }
                catch (IOException ex) {
                }
                try {
                    in = (InputStream) new FileInputStream(inputFile);
                }
                catch (IOException ex) {
                    new Event(Event.ERR, linkName + ": failed to open " + uri+
                        " at " + i + " retries: "+Event.traceStack(ex)).send();
                    continue;
                }
                new Event(Event.INFO, uri + " reopened on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                sessionTime = System.currentTimeMillis();
                isConnected = true;
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (JMSException e) {
            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;
            String str = linkName + " " + uri + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            i = retryCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                i = (i - 1) % repeatPeriod + 1;
            if (retryCount > 1) try {
                Thread.sleep(standbyTime);
            }
            catch (Exception e1) {
            }
            if (i == 1 || i == maxRetry)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private int socketOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            ms.read(sock.getInputStream(), xq);
        }
        catch (IOException e) {
            resetStatus(RCVR_RUNNING, RCVR_RETRYING);
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();

            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;

            while (keepRunning(xq) && status == RCVR_RETRYING) {
                i = retryCount ++;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    i = (i - 1) % repeatPeriod + 1;
                if (retryCount > 2) try {
                    Thread.sleep(standbyTime);
                }
                catch (Exception e1) {
                }

                if (i > maxRetry)
                    continue;

                socketReconnect();
                if (!isConnected) {
                    new Event(Event.ERR, linkName + ": failed to connect "+
                        uri + " after " + i + " retries").send();
                    continue;
                }
                new Event(Event.INFO, uri + " reconnected on " + linkName +
                    " after " + i + " retries").send();
                resetStatus(RCVR_RETRYING, RCVR_RUNNING);
                sessionTime = System.currentTimeMillis();
                isConnected = true;
                return --retryCount;
            }
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (JMSException e) {
            if (System.currentTimeMillis() - sessionTime > timeout)
                retryCount = 1;
            else
                retryCount ++;
            String str = linkName + " " + uri + ": ";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex + "\n";
            i = retryCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                i = (i - 1) % repeatPeriod + 1;
            if (retryCount > 1) try {
                Thread.sleep(standbyTime);
            }
            catch (Exception e1) {
            }
            if (i == 1 || i == maxRetry)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            sessionTime = System.currentTimeMillis();
            return retryCount;
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        catch (Error e) {
            disconnect();
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            Event.flush(e);
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    /**
     * connects to the remote socket via a client socket
     * and authenticates via connRequest
     */
    private int socketConnect() {
        long timeStart;
        int i, n, totalBytes, bytesRead, timeLeft, retCode, wt;
        String[] responseText = new String[connRequest.length];
        String response;
        StringBuffer strBuf = new StringBuffer();
        boolean hasNew = false;
        byte[] buffer = new byte[bufferSize];

        n = connRequest.length;
        totalBytes = 0;
        wt = (int) waitTime;
        if (sock != null)
            socketClose();

        timeStart = System.currentTimeMillis();
        if (openningMsg != null) {
            String text;
            openningMsg.setTimestamp(timeStart);
            text = zonedDateFormat.format(new Date()) + " " +
                Event.getIPAddress()+ " " +
                EventUtils.collectible(openningMsg) + "\n";
            connRequest[0] = text.getBytes();
            n = 1;
        }
        try {
            sock = ClientSocket.connect(hostname, port, soTimeout);
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to connect on " + uri +
                ": " + NOTCONNECTED + " " + e.toString()).send();
            socketClose();
            return NOTCONNECTED;
        }
        timeLeft = soTimeout - (int) (System.currentTimeMillis() - timeStart);
        if (sock == null) {
            if (timeLeft <= 0) {
                new Event(Event.WARNING, "connection failed on " + uri +
                    ": " + CONNTIMEOUT + " timeout").send();
                return CONNTIMEOUT;
            }
            else {
                new Event(Event.WARNING, "connection failed on " + uri +
                    ": " + CONNREFUSED + " refused").send();
                return CONNREFUSED;
            }
        }
        if (timeLeft <= 0) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + CONNTIMEOUT + " timeout").send();
            socketClose();
            return CONNTIMEOUT;
        }
        try {
            if (receiveTime > 0)
                sock.setSoTimeout(receiveTime);
            if (keepAlive)
                sock.setKeepAlive(true);
            in = sock.getInputStream();
            out = sock.getOutputStream();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to set socket on " + uri +
                ": " + NOTCONNECTED + " " + e.toString()).send();
            socketClose();
            return NOTCONNECTED;
        }
        if (in == null || out == null) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + NOTCONNECTED + " null IO Streams").send();
            socketClose();
            return NOTCONNECTED;
        }

        if (withGreeting) try { // try to read greeting text
            do { // read everything until EOF or pattern hit or timeout
                bytesRead = 0;
                hasNew = false;
                if (sock != null) try {
                    sock.setSoTimeout(wt);
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
                if (status != RCVR_RUNNING && status != RCVR_RETRYING) {
                    socketClose();
                    return CLIENTERROR;
                }
                try {
                    Thread.sleep(5);
                }
                catch (Exception ex) {
                }
                timeLeft= soTimeout-(int)(System.currentTimeMillis()-timeStart);
            } while (bytesRead >= 0 && timeLeft > 0);
            if (timeLeft <= 0 || bytesRead < 0) {
                retCode = (bytesRead < 0) ? SERVERERROR : CONNTIMEOUT;
                new Event(Event.WARNING, "connection failed on " + uri +
                    ": " + retCode + " " + timeLeft + ":" + bytesRead).send();
                socketClose();
                return retCode;
            }
        }
        catch (IOException e) {
            new Event(Event.WARNING, "connection failed on " + uri +
                ": " + READFAILED + " " + e.toString()).send();
            socketClose();
            return READFAILED;
        }

        isConnected = true;
        retCode = send(connRequest, responseText);
        if (retCode != CMDOK) {
            socketClose();
            if (retCode == CMDOK)
                retCode = CMDFAILED;
            new Event(Event.ERR, "login failed for " + uri +
                ": " + responseText[n-1]).send();
            return retCode;
        }
        else if (openningMsg != null) {
            Event event = parser.parse(responseText[0]);
            if (event == null) {
                socketClose();
                retCode = CLIENTERROR;
                new Event(Event.ERR, "protocol error for " + uri +
                    ": " + responseText[n-1]).send();
                return retCode;
            }
            else if (event.getPriority() != Event.NOTICE) {
                socketClose();
                retCode = CMDFAILED;
                new Event(Event.ERR, "failed to open " + uri +
                    ": " + event.getAttribute("text")).send();
                return retCode;
            }
            else
                isConnected = true;
        }
        else
            isConnected = true;

        // SoTimeout has to be set again, otherwise read() will block
        try {
            if (receiveTime > 0)
                sock.setSoTimeout(receiveTime);
            if (keepAlive)
                sock.setKeepAlive(true);
        }
        catch (Exception e) {
            isConnected = false;
            new Event(Event.WARNING, "failed to set timeout on " + uri +
                ": " + e.toString()).send();
            socketClose();
            return CMDFAILED;
        }

        return retCode;
    }

    private void socketClose() {
        isConnected = false;
        if (sock != null) {
            if (out != null) try {
                if (quitRequest != null && quitRequest.length > 0)
                    out.write(quitRequest[0]);
                out.flush();
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

    private void socketReconnect() {
        socketClose();
        if (connRequest.length == 0) {
            try {
                sock = ClientSocket.connect(hostname, port, soTimeout);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to connect on " + uri +
                    ": " + e.toString()).send();
                sock = null;
            }
            if (sock != null) try {
                if (receiveTime > 0)
                    sock.setSoTimeout(receiveTime);
                if (keepAlive)
                    sock.setKeepAlive(true);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set socket on " + uri +
                    ": " + e.toString()).send();
                socketClose();
            }
            if (sock != null)
                isConnected = true;
        }
        else
            socketConnect();
    }

    /**
     * writes the text to the OutputStream of the existing connection
     * and returns 0 on success or none zero otherwise
     */
    public synchronized int send(String text) {
        int length;
        if (text == null || text.length() <= 0)
            return DATAERROR;
        if (!isConnected)
            return NOTCONNECTED;
        length = text.length();
        if (length > 0) try {
            switch (type) {
              case STREAM_PORT:
                if (comm == null)
                    return NOTCONNECTED;
                comm.getOutputStream().write(text.getBytes(), 0, length);
                comm.getOutputStream().flush();
                return CMDOK;
              case STREAM_SOCK:
                if (sock == null)
                    return NOTCONNECTED;
                sock.getOutputStream().write(text.getBytes(), 0, length);
                sock.getOutputStream().flush();
                return CMDOK;
              case STREAM_FILE:
              default:
                return CLIENTERROR;
            }
        }
        catch (IOException e) {
            return WRITEFAILED;
        }
        catch (Exception e) {
            return SERVERERROR;
        }
        return DATAERROR;
    }

    /**
     * extracts the content from the message and writes it to the
     * OutputStream of the existing connection and returns 0 on success
     * or none zero otherwise
     */
    public int send(Message message) {
        byte[] buff = new byte[4096];
        String msgStr = null;

        if (message == null)
            return DATAERROR;
        try {
            msgStr = MessageUtils.processBody(message, buff);
        }
        catch (JMSException e) {
            return DATAERROR; 
        }

        if (msgStr == null)
            return DATAERROR;

        return send(msgStr);
    }

    /**
     * writes the connection requests to the socket and reads the replies from
     * the socket while it keeps checking the corresponding replies.  It returns
     * 0 if success or none zero otherwise.
     */
    private int send(byte[][] request, String[] responseText) {
        int i, j, totalBytes, bytesRead, timeLeft = 1, wt;
        String response;
        StringBuffer strBuf = new StringBuffer();
        long timeStart;
        boolean hasNew = false;
        byte[] buffer = new byte[bufferSize];

        if (!isConnected)
            return NOTCONNECTED;

        totalBytes = 0;
        wt = (int) waitTime;
        for (i=0; i<request.length; i++) {
            responseText[i] = null;
        }

        timeStart = System.currentTimeMillis();
        for (i=0; i<request.length; i++) {
            strBuf.delete(0, strBuf.length());
            try {
                out.write(request[i]);
                out.flush();
            }
            catch (IOException e) {
                responseText[request.length-1] = e.toString();
                socketClose();
                return WRITEFAILED;
            }

            try {
                do { // read everything until EOF or pattern hit or timeout
                    bytesRead = 0;
                    hasNew = false;
                    if (sock != null) try {
                        sock.setSoTimeout(wt);
                    }
                    catch (Exception ex) {
                    }
                    try {
                        while((bytesRead = in.read(buffer, 0, bufferSize)) > 0){
                            strBuf.append(new String(buffer, 0, bytesRead));
                            totalBytes += bytesRead;
                            hasNew = true;
                            for (j=bytesRead-1; j>=0; j--) // check newline
                                if (buffer[j] == '\n')
                                    break;
                            if (j >= 0) // found the newline
                                break;
                        }
                    }
                    catch (InterruptedIOException ex) { // timeout or interrupt
                    }
                    if (hasNew && pm.contains(strBuf.toString(), pattern))
                        break;
                    if (status != RCVR_RUNNING && status != RCVR_RETRYING) {
                        socketClose();
                        return CLIENTERROR;
                    }
                    try {
                        Thread.sleep(5);
                    }
                    catch (Exception ex) {
                    }
                    timeLeft = soTimeout - (int) (System.currentTimeMillis() -
                        timeStart);
                } while (bytesRead >= 0 && timeLeft > 0);
            }
            catch (Exception e) {
                responseText[request.length-1] = e.toString();
                socketClose();
                return READFAILED;
            }
            if (timeLeft <= 0 || bytesRead < 0) {
                socketClose();
                return ((bytesRead < 0) ? SERVERERROR : READFAILED);
            }
            response = strBuf.toString();
            responseText[i] = response;
            for (j=0; j<errorResponse.length; ++j) {
                if (response.indexOf(errorResponse[j]) >= 0) {
                    responseText[request.length-1] = response;
                    socketClose();
                    return ERRORFOUND;
                }
            }
        }

        return CMDOK;
    }

    private void disconnect() {
        isConnected = false;
        switch (type) {
          case STREAM_PORT:
            if (comm != null) {
                String device = comm.getPortName();
                try {
                    comm.close();
                }
                catch (Exception e) {
                }
            }
            break;
          case STREAM_FILE:
            if (inputFile != null && in != null) {
                try {
                    in.close();
                }
                catch (Exception e) {
                }
            }
            break;
          case STREAM_SOCK:
            if (sock != null)
                socketClose();
            break;
          default:
            break;
        }
    }

    public void close() {
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
        disconnect();
        if (dependencyGroup != null) { // clear dependencies
            MonitorUtils.clearDependencies(dependencyGroup);
            dependencyGroup = null;
        }
        if (comm != null)
            comm = null;
        if (in != null)
            in = null;
        if (sock != null)
            sock = null;

        if (doResponse) {
            msgList.clear();
            cells.clear();
            sBuf.reset();
        }
    }

    protected void finalize() {
        close();
    }
}
