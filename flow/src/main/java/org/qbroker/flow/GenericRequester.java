package org.qbroker.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.JMSException;
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
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.persister.MessagePersister;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

public class GenericRequester implements Requester {
    private String uri;
    private String name;
    private String dataField, rcField, resultField;
    private SimpleDateFormat zonedDateFormat;
    private ThreadPool pool;
    private XQueue xq;
    private MessagePersister persister = null;
    private Map<String, Object> msgMap;
    private Template template;
    private Template requestTemp = null;
    int dataType = Utils.RESULT_JSON;
    boolean isConnected = false;
    private final static int OBJ_TCP = 7;
    private final static int OBJ_UDP = 8;

    public GenericRequester(Map props) {
        String str;
        Object o;
        URI u = null;

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        name = (String) props.get("Name");
        persister = (MessagePersister) MessageFlow.initNode(props, name);

        str = "##ACTION## ##Target## ##Attribute## ##Command##";
        template = new Template(str);
        if ((o = props.get("RequestTemplate")) != null)
            requestTemp = new Template((String) o);

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
                    Template temp = new Template(value);
                    if (temp.numberOfFields() > 0)
                        msgMap.put(key, temp);
                    else
                        msgMap.put(key, value);
                }
            }
        }

        dataField = (String) props.get("DataField");
        if ((o = props.get("RCField")) != null)
            rcField = (String) o;
        else
            rcField = null;
        if ((o = props.get("ResultField")) != null)
            resultField = (String) o;
        else
            resultField = null;
        if ((o = props.get("ResultType")) != null)
            dataType = Integer.parseInt((String) o);
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        xq = new IndexedXQueue(name, 2);
        pool = new ThreadPool(name, 1, 1, persister, "persist",
            new Class[] {XQueue.class, int.class});
    }

    /**
     * This takes a generic query command and converts it into a JMS request.
     * It sends the request to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection.
     *<br/></br/>
     * The query command is supposed to be as follows:
     *<br/>
     * ACTION Target Attribute
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
            event = (TextEvent) createMessage(request);
        }
        catch (Exception e) {
        }
        if (event == null)
            throw(new IllegalArgumentException(name + ": bad request '" +
                request + "'"));

        str = reconnect();
        if (str != null)
            throw(new WrapperException(name + ": failed to reconnect: " + str));
        id = xq.reserve(500L);
        cid = xq.add(event, id);
        if (id < 0 || cid <= 0) { // failed to reserve or add the msg
            MessageUtils.stopRunning(xq);
            isConnected = false;
            throw(new WrapperException(name + ": failed to add request at " +
                id + "/" + xq.size() + " " + xq.depth()));
        }

        pool.assign(new Object[] {xq, new Integer(0)}, 0);
        cid = -1;
        for (int i=0; i<100; i++) {
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
        }
        if (resultField != null) { // resultField is expected
            str = event.getAttribute(resultField);
            if (str != null)
                k = Integer.parseInt(str);
            else
                k = 1;
        }
        if ((k = strBuf.length()) > 0)
            strBuf.delete(0, k);
        str = event.getAttribute(dataField);
        switch (dataType) { // convert result into JSON
          case Utils.RESULT_JSON:
            strBuf.append(str);
            break;
          case Utils.RESULT_XML:
            strBuf.append(toJSON(str));
            break;
          case Utils.RESULT_TEXT:
            strBuf.append("{\"Record\":[\"" + str + "\"]}");
            k = 1;
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
    private Message createMessage(String request) throws JMSException {
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

//need work
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

    private String toJSON(String xml) {
        return xml;
    }
}
