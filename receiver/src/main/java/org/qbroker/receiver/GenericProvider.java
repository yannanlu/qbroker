package org.qbroker.receiver;

/* GenericProvider.java - a provider of JMS messages via various receiver */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.ThreadPool;
import org.qbroker.common.DataSet;
import org.qbroker.common.Provider;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.WrapperException;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * GenericProvider provides JMS messages via various Receivers.
 *<br>
 * @author yannanlu@yahoo.com
 */
public class GenericProvider implements Provider, Service {
    private String uri;
    private String name;
    private SimpleDateFormat zonedDateFormat;
    private ThreadPool pool;
    private Thread thr = null;
    private XQueue xq;
    private MessageReceiver receiver = null;
    private Template template = null;
    private Object object = null;
    private java.lang.reflect.Method method = null;
    private long count = 0;
    private int debug = 0, reqTimeout = 2000;
    private boolean isConnected = false;

    public GenericProvider(Map props) {
        String str;
        Object o;
        URI u = null;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));
        uri = (String) o;

        if ((o = props.get("RequestTimeout")) != null) {
            reqTimeout = 1000 * Integer.parseInt((String) o);
            if (reqTimeout <= 0)
                reqTimeout = 2000;
        }

        o = MessageUtils.initNode(props, name);
        if (o == null) {
            throw(new IllegalArgumentException(name +
                " failed to instanciate receiver with null"));
        }
        else if (o instanceof MessageReceiver) {
            receiver = (MessageReceiver) o;
            new Event(Event.INFO, name + " is instantiated").send();
        }
        else if (o instanceof InvocationTargetException) {
            InvocationTargetException e = (InvocationTargetException) o;
            Throwable ex = e.getTargetException();
            if (ex == null) {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate receiver: "+
                    Event.traceStack(e)));
            }
            else if (ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate receiver: " +ee.toString()+" "+
                        Event.traceStack(ex)));
                else
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate receiver: " +
                        Event.traceStack(ex)));
            }
            else {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate receiver: " +
                    Event.traceStack(ex)));
            }
        }
        else if (o instanceof Exception) {
            Exception e = (Exception) o;
            throw(new IllegalArgumentException(name +
                " failed to instanciate receiver: " + Event.traceStack(e)));
        }
        else if (o instanceof Error) {
            Error e = (Error) o;
            new Event(Event.ERR, name + " failed to instantiate receiver: " +
                e.toString()).send();
            Event.flush(e);
        }
        else {
            throw(new IllegalArgumentException(name +
                " failed to instanciate receiver: "+ o.toString()));
        }

        if ((o = props.get("Template")) != null && o instanceof String)
            template = new Template((String) o);
        else if (o != null && o instanceof Map &&
            ((Map) o).containsKey("ClassName")) { // plugin of formatter
            Object[] asset;
            asset = MessageUtils.getPlugins(props, "Template", "format",
                new String[]{"javax.jms.Message"}, "close", name);
            object = asset[1];
            method = (java.lang.reflect.Method) asset[0];
        }
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        xq = new IndexedXQueue("xq_" + name, 1);
        pool = new ThreadPool("pool_" + name, 1, 1, receiver, "receive",
            new Class[] {XQueue.class, int.class});
        thr = pool.checkout(500L);
        pool.assign(new Object[] {xq, new Integer(0)}, 0);
    }

    /**
     * It gets the data provided by the receiver and fills result in the given
     * strBuf. It returns 1 for success, 0 for no message, and -1 for failure.
     * In case of failure, the error detail will be store in stfBuf.
     */
    public int getResult(StringBuffer strBuf, boolean autoDisconn) {
        Message msg;
        byte[] buffer = new byte[4096];
        if (!isConnected)
            reconnect();
        msg = getMessage(reqTimeout);
        if (msg == null) {
            if (autoDisconn)
                stop();
            return 0;
        }
        else try {
            String text;
            if (template != null)
                text = MessageUtils.format(msg, buffer, template);
            else if (method == null)
                text = MessageUtils.processBody(msg, buffer);
            else // with plugin
                text = (String) method.invoke(object, new Object[] {msg});
            strBuf.append(text);
            if (autoDisconn)
                stop();
            return 1;
        }
        catch (Exception e) {
            strBuf.append("failed to format message: " + e.toString());
            if (autoDisconn)
                stop();
            return -1;
        }
    }

    /**
     * It gets a message via the receiver and copies the result into the
     * provided empty event. It returns 1 for success, 0 for timed out and
     * -1 for failure. The method is MT-Safe.
     *<br><br>
     * The provided event is required to be a JMSEvent. If you want to get
     * JMS messages provided by original vendors, please use the method of
     * getMessage() directly.
     */
    public int doRequest(org.qbroker.common.Event event, int timeout) {
        byte[] buffer = new byte[4096];
        if (timeout <= 0)
            timeout = reqTimeout;

        if (event == null)
            return -1;
        else if (!(event instanceof JMSEvent))
            return -1;
        else {
            Message msg = getMessage(timeout);
            if (msg == null) // timed out
                return 0;
            else try {
                MessageUtils.copy(msg, (JMSEvent) event, buffer);
                return 1;
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to copy message: " +
                    Event.traceStack(e)).send();
                return -1;
            }
        }
    }

    /** returns the next available message received by the receiver or null */
    public Message getMessage(int timeout) {
        int sid;
        if (timeout <= 0)
            timeout = reqTimeout;

        if ((sid = xq.getNextCell(timeout)) >= 0) {
            Message msg = (Message) xq.browse(sid);
            xq.remove(sid);
            count ++;
            return msg;
        }
        else // no message available
            return null;
    }

    public String reconnect() {
        stop();
        xq.clear();
        start();
        return null;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public String getURI() {
        return uri;
    }

    public XQueue getXQueue() {
        return xq;
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return receiver.getOperation();
    }

    public int getStatus() {
        return receiver.getStatus();
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public void start() {
        receiver.setStatus(Receiver.RCVR_READY);
        MessageUtils.resumeRunning(xq);
        thr = pool.checkout(500L);
        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        isConnected = true;
    }

    public void stop() {
        receiver.setStatus(Receiver.RCVR_STOPPED);
        MessageUtils.stopRunning(xq);
        if (thr.isAlive())
            thr.interrupt();
        pool.checkin(thr);
        isConnected = false;
    }

    public void close() {
        stop();
        receiver.close();
        pool.close();
        xq.clear();
    }

    public static void main(String[] args) {
        byte[] buffer = new byte[4096];
        String filename = null, operation = null;
        GenericProvider provider = null;
        int max = 0, sleepTime = 0, timeout = 0;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (int i=0; i<args.length; i++) {
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
              case 'n':
                if (i+1 < args.length)
                    max = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    sleepTime = Integer.parseInt(args[++i]);
                break;
              case 'o':
                if (i+1 < args.length)
                    operation = args[++i];
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
            int n, i = 0;
            StringBuffer strBuf = new StringBuffer();
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            if (operation != null && operation.length() > 0)
                ph.put("Operation", operation);
            if (timeout > 0)
                ph.put("RequestTimeout", String.valueOf(timeout));
            provider = new GenericProvider(ph);
            if (sleepTime > 0) try { // wait for a while
                Thread.sleep(sleepTime * 1000L);
            }
            catch (Exception e) {
            }
            do {
                n = provider.getResult(strBuf, false);
                if (n > 0)
                    System.out.println("Got a message:\n\t"+strBuf.toString());
                else if (n < 0)
                    System.out.println("Failure:\n\t" + strBuf.toString());
                else {
                    System.out.println("No message is avaiable any more");
                    break;
                }
                strBuf.delete(0, strBuf.length());
            } while (++i < max || max == 0);
            if (provider != null)
                provider.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (provider != null)
                provider.close();
        }
        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("GenericProvider Version 1.0 (written by Yannan Lu)");
        System.out.println("GenericProvider: receive messages via a given receiver");
        System.out.println("Usage: java org.qbroker.receiver.GenericProvider -I cfg.json [-n 1]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -n: max number of messages to get");
        System.out.println("  -s: seconds to sleep before getting results");
        System.out.println("  -o: operation of the receiver");
        System.out.println("  -t: timeout in second");
    }
}
