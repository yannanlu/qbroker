package org.qbroker.persister;

/* GenericRequester.java - a requester of JMS messages via various persister */

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
import org.qbroker.common.Requester;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.WrapperException;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * GenericRequester implements the API of Requester for various Persisters.
 *<br>
 * @author yannanlu@yahoo.com
 */
public class GenericRequester implements Requester, Service {
    private String uri;
    private String name;
    private String dataField, rcField, resultField;
    private SimpleDateFormat zonedDateFormat;
    private ThreadPool pool;
    private Thread thr;
    private XQueue xq;
    private MessagePersister persister = null;
    private Map<String, Object> msgMap;
    private DocumentBuilder builder = null;
    private XPathExpression xpe = null;
    private Template temp = null;
    private TextSubstitution tsub = null;
    private Perl5Matcher pm = null;
    Pattern patternLF = null;
    private Pattern[][] aPatternGroup = null;
    private Pattern[][] xPatternGroup = null;
    private int dataType = Utils.RESULT_JSON;
    private int reqTimeout = 10000, debug = 0;
    boolean isConnected = false;
    boolean isCommand = false;

    public GenericRequester(Map props, XQueue xq) {
        String str;
        Object o;
        URI u = null;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(name + " failed to parse uri: "+
                e.toString()));
        }

        if ((o = props.get("RequestCommand")) != null) // for command
            isCommand = true;

        if ((o = props.get("RequestTemplate")) != null)
            temp = new Template((String) o);

        if ((o = props.get("RequestTimeout")) != null) {
            reqTimeout = 1000 * Integer.parseInt((String) o);
            if (reqTimeout <= 0)
                reqTimeout = 10000;
        }

        msgMap = new HashMap<String, Object>();
        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value;
            Iterator iter = ((Map) o).keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if ((str = MessageUtils.getPropertyID(key)) != null)
                    key = str;
                if (value != null && value.length() > 0) {
                    Template tmp = new Template(value);
                    if (tmp.size() > 0)
                        msgMap.put(key, tmp);
                    else
                        msgMap.put(key, value);
                }
            }
        }

        dataField = (String) props.get("DataField");
        if (dataField == null)
            throw(new IllegalArgumentException(name +
                ": DataField is not defined"));

        if ((o = props.get("RCField")) != null)
            rcField = (String) o;
        else
            rcField = null;

        if ((o = props.get("ResultType")) != null) {
            dataType = Integer.parseInt((String) o);
            if (dataType == Utils.RESULT_XML) { // xpath
                if((o = props.get("XPath")) == null || ((String) o).length()<=0)
                    throw(new IllegalArgumentException(name +
                        ": XPath is not defined"));
                try {
                    XPath xpath = XPathFactory.newInstance().newXPath();
                    xpe = xpath.compile((String) o);
                    builder = Utils.getDocBuilder();
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(name +
                        " failed to get XPath: " + e.toString()));
                }
            }
            else if (dataType == Utils.RESULT_TEXT) try { // text
                Perl5Compiler pc = new Perl5Compiler();
                patternLF = pc.compile("\\n");
                aPatternGroup = MonitorUtils.getPatterns("PatternGroup",
                    props, pc);
                xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",
                    props, pc);

                if ((o = props.get("ResultSubstitution")) != null)
                    tsub = new TextSubstitution((String) o);
                pm = new Perl5Matcher();
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    ": failed to compile pattern: " + e.toString()));
            }
        }
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        if (xq != null && xq.getCapacity() > 0)
            this.xq = xq;
        initialize(props);
    }

    public GenericRequester(Map props) {
        this(props, null);
    }

    public void initialize(Map props) {
        Object o = MessageUtils.initNode(props, name);
        if (o == null) {
            throw(new IllegalArgumentException(name +
                " failed to instanciate persister with null"));
        }
        else if (o instanceof MessagePersister) {
            persister = (MessagePersister) o;
            new Event(Event.INFO, name + " is instantiated").send();
        }
        else if (o instanceof InvocationTargetException) {
            InvocationTargetException e = (InvocationTargetException) o;
            Throwable ex = e.getTargetException();
            if (ex == null) {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate persister: "+
                    Event.traceStack(e)));
            }
            else if (ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate persister: " +ee.toString()+" "+
                        Event.traceStack(ex)));
                else
                    throw(new IllegalArgumentException(name +
                        " failed to instanciate persister: " +
                        Event.traceStack(ex)));
            }
            else {
                throw(new IllegalArgumentException(name +
                    " failed to instanciate persister: " +
                    Event.traceStack(ex)));
            }
        }
        else if (o instanceof Exception) {
            Exception e = (Exception) o;
            throw(new IllegalArgumentException(name +
                " failed to instanciate persister: " + Event.traceStack(e)));
        }
        else if (o instanceof Error) {
            Error e = (Error) o;
            new Event(Event.ERR, name + " failed to instantiate persister: " +
                e.toString()).send();
            Event.flush(e);
        }
        else {
            throw(new IllegalArgumentException(name +
                " failed to instanciate persister: "+ o.toString()));
        }

        if (xq == null)
            xq = new IndexedXQueue("xq_" + name, 1);
        pool = new ThreadPool("pool_" + name, 1, 1, persister, "persist",
            new Class[] {XQueue.class, int.class});
        thr = pool.checkout(500L);
        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        isConnected = true;
    }

    /**
     * This takes either an ad hoc message body or a generic query command as a
     * request. First it converts the request into a JMS message. It sends the
     * request message to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection alive.
     *<br><br>
     * By default, the request is not assumed to be a query command. If
     * RequestCommand is defined, the request is assumed as a query command.
     * A query command is supposed to be as follows:
     *<br>
     * ACTION Target Attributes
     *<br><br>
     * In case of a fatal failure, it disconnects first and then throws a
     * WrapperException with cause set to the vendor specific exception or IO
     * related exception. Therefore please make sure to get the cause exception
     * from it and handles the cause exception with care. For other failures,
     * it throws an IllegalArgumentException. If the disconnect flag is set to
     * true, it disconnects right before it throws the exception.
     */
    public int getResponse(String request, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        int k;
        String str, target, operation, attrs;
        TextEvent event = null;

        if (request == null || request.length() <= 0)
            throw(new IllegalArgumentException(name + ": empty request"));

        if (strBuf == null)
            throw(new IllegalArgumentException(name +
                ": response buffer is null"));

        try {
            if (isCommand) { // for query command
                event = (TextEvent) generateMessage(request);
            }
            else { // for raw request
                event = new TextEvent(request);
                event.setJMSPriority(9-Event.INFO);
            }
        }
        catch (Exception e) {
        }
        if (event == null)
            throw(new IllegalArgumentException(name + ": bad request '" +
                request + "'"));

        if (msgMap.size() > 0) try {
            for (String key : msgMap.keySet()) {
                event.setAttribute(key, (String) msgMap.get(key));
            }
        }
        catch (Exception e) {
            throw(new WrapperException(name + " failed to set properties: " +
                e.toString()));
        }

        if (!isConnected)
            reconnect();

        k = doRequest(event, reqTimeout);

        if (k == 0) { // timed out
            if (autoDisconn)
                stop();
            throw(new WrapperException(name + ": request timed out from "+uri));
        }
        else if (k < 0) { // failed
            stop();
            throw(new WrapperException(name + " failed to add request at 0 /" +
                xq.size() + " " + xq.depth()));
        }
        else if (autoDisconn) {
            stop();
        }

        if (rcField != null) { // rcField is expected
            str = event.getAttribute(rcField);
            if (str != null)
                k = Integer.parseInt(str);
            else
                k = -1;
            if (k != 0) // request failed with non-zero
                return (k > 0) ? -k : -2;
        }
        if (!"body".equals(dataField))
            str = event.getAttribute(dataField);
        else try {
            str = event.getText();
        }
        catch (Exception e) {
            throw(new WrapperException(name + " failed to retrieve body: "+
                e.toString()));
        }

        if ((k = strBuf.length()) > 0)
            strBuf.delete(0, k);
        switch (dataType) { // convert result into JSON
          case Utils.RESULT_JSON:
            strBuf.append(str);
            k = 1;
            break;
          case Utils.RESULT_XML:
            k = xml2JSON(str, strBuf);
            break;
          case Utils.RESULT_TEXT:
            k = text2JSON(str, strBuf);
            break;
          default:
            k = -1;
        }
            
        return k;
    }

    public int doRequest(org.qbroker.common.Event event, int timeout) {
        int id, cid, k;
        if (!(event instanceof TextEvent))
            return -1;
        id = xq.reserve(500L);
        cid = xq.add(event, id);
        if (id < 0 || cid <= 0) // failed to reserve or add the msg
            return -1;

        k = timeout / 500 + 1;
        cid = -1;
        for (int i=0; i<=k; i++) { // wait for response
            cid = xq.collect(500L, id);
            if (cid >= 0) // got response
                return 1;
        }
        return 0;
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

    /** returns the expected dataType of responses */
    public int getDataType() {
        return dataType;
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return persister.getOperation();
    }

    public int getStatus() {
        return persister.getStatus();
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public void start() {
        persister.setStatus(Persister.PSTR_READY);
        MessageUtils.resumeRunning(xq);
        thr = pool.checkout(500L);
        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        isConnected = true;
    }

    public void stop() {
        persister.setStatus(Persister.PSTR_STOPPED);
        MessageUtils.stopRunning(xq);
        if (thr.isAlive())
            thr.interrupt();
        pool.checkin(thr);
        isConnected = false;
    }

    public void close() {
        stop();
        persister.close();
        pool.close();
        xq.clear();
    }

    /** returns a Message created from the request and formatters */
    private Message generateMessage(String request) throws JMSException {
        int k;
        String operation, target, attrs, queryStr;
        if (request == null || request.length() <= 0)
            return null;

        String[] keys = request.split("\\s+");
        operation = keys[0];
        target = keys[1];
        k = request.indexOf(" " + target);
        if (k > 0 && k + 1 + target.length() > request.length())
            attrs = request.substring(k+2+target.length()).trim();
        else
            attrs = "";

        Event ev = new Event(Event.INFO);
        ev.setAttribute("type", operation.toLowerCase());
        ev.setAttribute("category", target);
        ev.setAttribute("name", attrs);
        ev.setAttribute("status", "Normal");
        queryStr = Event.getIPAddress() + " " + EventUtils.collectible(ev);

        TextEvent event = new TextEvent();
        event.setJMSPriority(9-Event.INFO);
        event.setText(zonedDateFormat.format(new Date()) + " " + queryStr);
        if (uri.startsWith("udp")) // udp stuff
            event.setStringProperty("UDP", uri.substring(6));

        return (Message) event;
    }

    /** returns number of items in the List of JSON content at key of List */
    private int text2JSON(String text, StringBuffer strBuf) {
        List dataBlock = new ArrayList();
        List<String> list = new ArrayList<String>();
        Map<String, List> map = new HashMap<String, List>();
        String str = null;
        int k;

        Util.split(dataBlock, pm, patternLF, text);
        if (aPatternGroup.length > 0 || xPatternGroup.length > 0) {
            k = dataBlock.size();
            for (int i=k-1; i>=0; i--) {
                str = (String) dataBlock.get(i);
                if (str == null || str.length() <= 0) {
                    continue;
                }
                if (MonitorUtils.filter(str, aPatternGroup, pm, true) &&
                    !MonitorUtils.filter(str, xPatternGroup, pm, false))
                    continue;
                dataBlock.remove(i);
            }
        }
        k = dataBlock.size();
        for (int i=0; i<k; i++) {
            str = (String) dataBlock.get(i);
            if (tsub != null)
                list.add(tsub.substitute(str));
            else
                list.add(str);
        }
        map.put("Record", list);
        strBuf.append(JSON2Map.toJSON(map));
        dataBlock.clear();
        list.clear();
        map.clear();

        return k;
    }

    /** returns number of items in the List of JSON content at key of List */
    private int xml2JSON(String xml, StringBuffer strBuf) {
        StringReader sr = null;
        Document doc;
        Object o;
        List<String> list = new ArrayList<String>();
        Map<String, List> map = new HashMap<String, List>();
        String str = null;
        int k;

        try {
            sr = new StringReader(xml);
            doc = builder.parse(new InputSource(sr));
            sr.close();
            o = xpe.evaluate(doc, XPathConstants.NODESET);
        }
        catch (Exception e) {
            if (sr != null)
                sr.close();
            throw(new IllegalArgumentException(name +
                " failed to parse XML payload: " + Event.traceStack(e)));
        }
        map.put("List", list);
        if (o != null && o instanceof NodeList) {
            NodeList nl = (NodeList) o;
            k = nl.getLength();
            for (int i=0; i<k; i++) {
                str = Utils.nodeToText(nl.item(i));
                if (str == null || str.length() <= 0)
                    continue;
                list.add(str);
            }
        }
        k = list.size();
        strBuf.append(JSON2Map.toJSON(map));
        list.clear();
        map.clear();

        return k;
    }

    public static void main(String[] args) {
        byte[] buffer = new byte[4096];
        String filename = null, path = null, operation = null;
        int timeout = 0;
        Requester requester = null;

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
                    path = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              case 'o':
                if (i+1 < args.length)
                    operation = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    timeout = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (path == null || filename == null)
            printUsage();
        else try {
            int k, n;
            long tm = System.currentTimeMillis();
            String queryStr = null;
            StringBuffer strBuf = new StringBuffer();
            java.io.FileReader fr = new java.io.FileReader(path);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            if ("-".equals(filename))
                queryStr = Utils.read(System.in, buffer);
            else {
               java.io.FileInputStream fs=new java.io.FileInputStream(filename);
                queryStr = Utils.read(fs, buffer);
                fs.close();
            }

            if (operation != null && operation.length() > 0)
                ph.put("Operation", operation);
            if (timeout > 0)
                ph.put("RequestTimeout", String.valueOf(timeout));
            requester = new GenericRequester(ph);
            n = requester.getResponse(queryStr, strBuf, true);
            if (n >= 0)
                System.out.println(strBuf.toString());
            else
                System.out.println("failed to get response from " +
                    requester.getURI() + ": " +n);

            if (requester != null)
                requester.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (requester != null)
                requester.close();
        }
        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("GenericRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("GenericRequester: send a request via a given persister");
        System.out.println("Usage: java org.qbroker.persister.GenericRequester -I cfg.json -f filename");
        System.out.println("  -?: print this usage page");
        System.out.println("  -f: filename for the content of request");
        System.out.println("  -o: operation of the persister");
        System.out.println("  -t: timeout in second");
    }
}
