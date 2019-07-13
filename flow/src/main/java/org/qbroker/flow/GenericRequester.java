package org.qbroker.flow;

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
import org.qbroker.common.Utils;
import org.qbroker.common.WrapperException;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.persister.MessagePersister;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * GenericRequester implements the API of Requester for various Persisters.
 *<br/>
 * @author yannanlu@yahoo.com
 */
public class GenericRequester implements Requester {
    private String uri;
    private String name;
    private String dataField, rcField, resultField;
    private SimpleDateFormat zonedDateFormat;
    private ThreadPool pool;
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
    private int reqTimeout = 10000;
    boolean isConnected = false;
    boolean isCommand = false;

    public GenericRequester(Map props) {
        String str;
        Object o;
        URI u = null;

        name = (String) props.get("Name");
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

        persister = (MessagePersister) MessageFlow.initNode(props, name);

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
                    if (tmp.numberOfFields() > 0)
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

        xq = new IndexedXQueue("xq_" + name, 2);
        pool = new ThreadPool("pool_" + name, 1, 1, persister, "persist",
            new Class[] {XQueue.class, int.class});
    }

    /**
     * This takes either an ad hoc message body or a generic query command as a
     * request. First it converts the request into a JMS message. It sends the
     * request message to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection alive.
     *<br/></br/>
     * By default, the request is not assumed to be a query command. If
     * RequestCommand is defined, the request is assumed as a query command.
     * A query command is supposed to be as follows:
     *<br/>
     * ACTION Target Attributes
     *<br/></br/>
     * In case of a fatal failure, it disconnects first and then throws a
     * WrapperException with cause set to the vendor specific exception or IO
     * related exception. Therefore please make sure to get the cause exception
     * from it and handles the cause exception with care. For other failures,
     * it throws an IllegalArgumentException. If the disconnect flag is set to
     * true, it disconnects right before it throws the exception.
     */
    public int getResponse(String request, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        int k, id, cid;
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

        str = reconnect();
        if (str != null)
            throw(new WrapperException(name + " failed to reconnect: " + str));
        id = xq.reserve(500L);
        cid = xq.add(event, id);
        if (id < 0 || cid <= 0) { // failed to reserve or add the msg
            MessageUtils.stopRunning(xq);
            isConnected = false;
            throw(new WrapperException(name + " failed to add request at " +
                id + "/" + xq.size() + " " + xq.depth()));
        }

        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        k = reqTimeout / 500 + 1;
        cid = -1;
        for (int i=0; i<=k; i++) {
            cid = xq.collect(500L, id);
            if (cid >= 0)
                break;
        }
        if (autoDisconn) {
            MessageUtils.stopRunning(xq);
            isConnected = false;
        }
        if (cid < 0) {
            throw(new WrapperException(name + ": request timed out from "+uri));
        }
        if (rcField != null) { // rcField is expected
            str = event.getAttribute(rcField);
            if (str != null)
                cid = Integer.parseInt(str);
            else
                cid = -1;
            if (cid != 0) // request failed with non-zero
                return (cid > 0) ? -cid: -2;
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

    public String reconnect() {
        int id;
        if (xq.depth() > 0) { // leftover
            id = xq.getNextCell(500L);
            if (id >= 0)
                xq.remove(id);
        }
        if (xq.size() > 0)
            xq.clear();
        persister.setStatus(MessagePersister.PSTR_READY);
        MessageUtils.resumeRunning(xq);
        Thread thr = pool.checkout(500L);
        if (thr == null) { // failed to check out the thread
            MessageUtils.stopRunning(xq);
            isConnected = false;
            return "failed to checkout a thread from " +
                pool.getName() + ": " + pool.getSize();
        }
        isConnected = true;
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

    public void close() {
        MessageUtils.stopRunning(xq);
        if (pool != null)
            pool.close();
        pool = null;
        if (persister != null)
            persister.close();
        persister = null;
        xq.clear();
        isConnected = false;
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
        String filename = null, path = null;
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
        System.out.println("Usage: java org.qbroker.flow.GenericRequester -I cfg.json -f filename");
    }
}
