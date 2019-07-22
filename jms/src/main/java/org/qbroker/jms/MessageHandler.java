package org.qbroker.jms;

/* MessageHandler.java - a generic hander of HttpServer for JMS messages */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.net.URI;
import java.net.HttpURLConnection;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpHandler;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.SimpleHttpServer;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.EchoService;

/**
 * MessageHandler implements HttpHandler for http servers. It handles HTTP
 * requests and converts them into JMS messages before passing them to the 
 * assigned message service as requests. Once a response message is back,
 * it converts the message to JSON content and sends the content back to the
 * HTTP client as the response. Therefore, MessageHandler is the frontend and
 * the gateway for message services. It is responsible for transformations
 * between HTTP requests/responses and JMS messages.
 *<br/><br/>
 * N.B. The name of the handler is supposed to be same as the substring of
 * the context path starting from the 2nd character (after the beginning "/").
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MessageHandler implements HttpHandler {
    private String name;
    private int timeout = 10000;
    private SimpleDateFormat zonedDateFormat;
    private Service service = null;
    private String restURI = null;
    private String baseURI = null;
    private String headerRegex = null;
    private String[] propertyName, propertyValue;
    private int restURILen = 0;
    private int baseURILen = 0;

    public MessageHandler(Map props) {
        Object o;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("HeaderRegex")) != null)
            headerRegex = (String) o;

        if ((o = props.get("RestURI")) != null) {
            restURI = (String) o;
            restURILen = restURI.length();
        }

        if ((o = props.get("Timeout")) != null) {
            timeout = 1000 * Integer.parseInt((String) o);
            if (timeout <= 0)
                timeout = 10000;
        }

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            n = 0;
            for (Object obj : ((Map) o).keySet()) {
                String key = (String) obj;
                if (key == null || key.length() <= 0)
                    continue;
                propertyName[n] = key;
                propertyValue[n] = (String) ((Map) o).get(key);
                n ++;
            }
        }
        else {
            propertyName = new String[0];
            propertyValue = new String[0];
        }
        zonedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");
        baseURI = "/" + name;
        baseURILen = baseURI.length();
    }

    /** sets the service to fulfill requests */
    public void setService(Service service) {
        if (service == null)
            throw(new IllegalArgumentException(name + ": service is null"));
        this.service = service;
    }

    public void handle(HttpExchange he) throws IOException {
        int rc;
        String method = he.getRequestMethod();
        if (service == null)
            throw(new IOException(name + ": service is not available"));
        if ((service.getDebugMode() & 4096) > 0 &&
            "POST".equals(method)) try { // log posted request
            InputStream in = he.getRequestBody();
            String query = Utils.read(in, new byte[8192]);
            in.close();
            new Event(Event.DEBUG, name + " got posted data:\n" + query).send();
        }
        catch (Exception e) {
        }
        if (method == null || method.length() <= 0)
            throw(new IOException(name + ": method is not well defined"));
        else switch (method.charAt(0)) {
          case 'G':
            if ("GET".equals(method)) {
                rc = doGet(he);
                break;
            }
          case 'P':
            if ("POST".equals(method)) {
                doPost(he);
                break;
            }
            else if ("PUT".equals(method)) {
                rc = doPut(he);
                break;
            }
          case 'D':
            if ("DELETE".equals(method)) {
                rc = doDelete(he);
                break;
            }
          default:
            throw(new IOException(name + " got unsupported method: " + method));
        }
    }

    private int doGet(HttpExchange he) throws IOException {
        String uri = null, text;
        Map<String, String> response = new HashMap<String, String>();

        uri = processRequest(he, response, null);

        if (uri == null)
            return -1;
        else if (uri.endsWith(".jsp")) {
            text = getContent(uri, response);
            if (text != null)
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_OK, text);
            else
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_NOT_FOUND, "bad jsp");
        }
        else if ((text = response.get(uri)) != null)
            SimpleHttpServer.sendResponse(he, HttpURLConnection.HTTP_OK, text);
        else if ((text = response.get("error")) != null)
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_INTERNAL_ERROR, text);
        else
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_NOT_FOUND, "no data");

        return 0;
    }

    public void doPost(HttpExchange he) throws IOException {
        String uri = null, path, str;
        Map<String, String> response = new HashMap<String, String>();
        List<String> list;
        Event event = null;
        Object o;
        int bufferSize = 4096;
        long size = 0;

        URI u = he.getRequestURI();
        path = u.getPath();
        if (path == null)
            path = "";
        else if (path.startsWith(baseURI))
            path = path.substring(baseURILen);

        str = he.getRequestHeaders().getFirst("Content-type");
        if (str == null)
            str = he.getRequestHeaders().getFirst("Content-Type");
        if ("text/xml".equals(str) || // raw xml request
            "application/json".equals(str)) { // or raw json
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            InputStream in = he.getRequestBody();
            if (in == null)
                event = new TextEvent();
            else try {
                StringBuffer strBuf = new StringBuffer();
                while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        strBuf.append(new String(buffer, 0, bytesRead));
                }
                event = new TextEvent(strBuf.toString());
                event.setAttribute("type", str);
            }
            catch (Exception e) {
                String text;
                text = "failed to read raw content: " + Event.traceStack(e);
                new Event(Event.ERR, name + " " + text).send();
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "failed to read raw content");
                return;
            }
            Map<String, List<String>> params =
                SimpleHttpServer.parseGetParameters(he);
            for (String key : params.keySet()) { // copy over parameters
                if (key == null || key.length() <= 0 || "text".equals(key))
                    continue;
                if ((list = params.get(key)) != null) {
                    str = list.get(0);
                    if (str != null)
                        event.setAttribute(key, str);
                }
            }
            params.clear();
        }
        if (event != null && headerRegex != null) { // copy the headers over
            Headers headers = he.getRequestHeaders();
            for (String key : headers.keySet()) {
                if (key == null || key.equals("null") ||
                    key.equalsIgnoreCase("Cookie") ||
                    key.equalsIgnoreCase("Content-Type") ||
                    key.equalsIgnoreCase("Authorization"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = headers.getFirst(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(he, response, event);

        if (uri == null)
            return;
        else if (uri.endsWith(".jsp")) {
            str = getContent(uri, response);
            if (str != null)
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_OK, str);
            else
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_NOT_FOUND, "bad jsp");
        }
        else if ((str = response.get(uri)) != null)
            SimpleHttpServer.sendResponse(he, HttpURLConnection.HTTP_OK, str);
        else if ((str = response.get(uri)) != null)
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_INTERNAL_ERROR, str);
        else
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_NOT_FOUND, "no data");
    }

    private int doPut(HttpExchange he) throws IOException {
        String uri = null, path, str;
        Map<String, String> response = new HashMap<String, String>();
        List<String> list;
        Object o;
        BytesEvent event = null;
        int bufferSize = 4096;
        int bytesRead;
        long size = 0;

        URI u = he.getRequestURI();
        path = u.getPath();
        if (path == null)
            path = "";
        else if (path.startsWith(baseURI))
            path = path.substring(baseURILen);

        byte[] buffer = new byte[bufferSize];
        InputStream in = he.getRequestBody();
        str = he.getRequestHeaders().getFirst("Content-type");
        try {
            event = new BytesEvent();
            if (in != null) {
                while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        event.writeBytes(buffer, 0, bytesRead);
                    size += bytesRead;
                }
                event.reset();
                event.setAttribute("type", str);
                event.setAttribute("method", he.getRequestMethod());
                event.setAttribute("size", String.valueOf(size));
                event.setAttribute("path", path);
            }
        }
        catch (Exception e) {
            String text = "failed to read raw content: " +
                Event.traceStack(e);
            new Event(Event.ERR, name + " " + text).send();
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_INTERNAL_ERROR, text);
            return -1;
        }
        Map<String,List<String>> params=SimpleHttpServer.parseGetParameters(he);
        for (String key : params.keySet()) { // copy over parameters
            if (key == null || key.length() <= 0 || "text".equals(key))
                continue;
            if ((list = params.get(key)) != null) {
                str = list.get(0);
                if (str != null)
                    event.setAttribute(key, str);
            }
        }
        if (headerRegex != null) { // copy the headers over
            Headers headers = he.getRequestHeaders();
            for (String key : headers.keySet()) {
                if (key == null || key.equals("null") ||
                    key.equalsIgnoreCase("Cookie") ||
                    key.equalsIgnoreCase("Content-Type") ||
                    key.equalsIgnoreCase("Authorization"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = headers.getFirst(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(he, response, event);

        if (uri == null)
            return -1;
        else if (uri.endsWith(".jsp")) {
            str = getContent(uri, response);
            if (str != null)
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_OK, str);
            else
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_NOT_FOUND, "bad jsp");
        }
        else if ((str = response.get(uri)) != null)
            SimpleHttpServer.sendResponse(he, HttpURLConnection.HTTP_OK, str);
        else if ((str = response.get("error")) != null)
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_INTERNAL_ERROR, str);
        else
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_NOT_FOUND, "no data");

        return 0;
    }

    private int doDelete(HttpExchange he) throws IOException {
        String text, uri = null;
        Map<String, String> response = new HashMap<String, String>();

        uri = processRequest(he, response, null);

        if (uri == null)
            return -1;
        else if (uri.endsWith(".jsp")) {
            text = getContent(uri, response);
            if (text != null)
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_OK, text);
            else
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_NOT_FOUND, "bad jsp");
        }
        else if ((text = response.get(uri)) != null)
            SimpleHttpServer.sendResponse(he, HttpURLConnection.HTTP_OK, text);
        else if ((text = response.get("error")) != null)
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_INTERNAL_ERROR, text);
        else
            SimpleHttpServer.sendResponse(he,
                HttpURLConnection.HTTP_NOT_FOUND, "no data");

        return 0;
    }

    public static Event getEvent(Map<String,List<String>> params,boolean isJMS){
        Object o;
        List<String> list;
        Event event = null;
        String key = null, username = null, site = null,
            text = null, category = null;
        int priority = -1;
        long timestamp = 0L, expiration = 0L;
        boolean skip = false;

        if ((list = params.get("priority")) != null)
            priority = Event.getPriorityByName(list.get(0));
        if ((list = params.get("summary")) != null)
            text = list.get(0);
        if ((list = params.get("date")) != null) try {
            timestamp = Long.parseLong(list.get(0));
        }
        catch (Exception e) { // bad timestamp for date
            return null;
        }
        if ((list = params.get("NAME")) != null)
            key = list.get(0);
        if ((list = params.get("SITE")) != null)
            site = list.get(0);
        if ((list = params.get("CATEGORY")) != null)
            category = list.get(0);
        if ((list = params.get("username")) != null)
            username = list.get(0);
        if (priority < 0 || text == null || !(key != null || site != null ||
            category != null)) // not an event postable
            return null;
  
        if (text.length() > 0) {
            if (text.indexOf(Event.ESCAPED_QUOTE) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_QUOTE, "\"", text);
            if (text.indexOf(Event.ESCAPED_AMPERSAND) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_AMPERSAND, "&",text);
            if (text.indexOf(Event.ESCAPED_EQUAL) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_EQUAL, "=", text);
            if (text.indexOf("\r") >= 0)
                text = Utils.doSearchReplace("\r", "\n", text);
            if (text.indexOf(Event.ESCAPED_CARRIAGE_RETURN) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_CARRIAGE_RETURN,
                    "\r", text);
        }
        if (!isJMS) {
            event = new Event(priority, text);
        }
        else try {
            event = new TextEvent(text);
            ((TextEvent) event).setJMSPriority(9-priority);
            event.setAttribute("text", text);
        }
        catch (Exception e) { // failed to create a TextEvent
            return null;
        }
        event.setTimestamp(timestamp);
        event.setAttribute("priority", Event.priorityNames[priority]);
        event.setAttribute("date", Event.dateFormat(new Date(timestamp)));
        event.setAttribute("name", key);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("owner", username);
        if ((list = params.get("hostname")) != null)
            event.setAttribute("hostname", list.get(0));
        if ((list = params.get("program")) != null)
            event.setAttribute("program", list.get(0));
        if ((list = params.get("type")) != null)
            event.setAttribute("type", list.get(0));
        if ((list = params.get("pid")) != null)
            event.setAttribute("pid", list.get(0));
        if ((list = params.get("content")) != null)
            text = list.get(0);
        if (text != null && text.length() > 1) {
            Map<String, String> attr = new HashMap<String, String>();
            text = Utils.doSearchReplace(Event.ESCAPED_AMPERSAND, "&", text);
            text = Utils.doSearchReplace(Event.ESCAPED_EQUAL, "=", text);
            text = Utils.doSearchReplace("\r", "\n", text);
            text = Utils.doSearchReplace(Event.ESCAPED_CARRIAGE_RETURN,
                "\r", text);
            Utils.split(Event.ITEM_SEPARATOR, Event.ESCAPED_ITEM_SEPARATOR,
                text, attr);
            for (String ky : attr.keySet()) {
                if (ky == null || ky.length() <= 0)
                    continue;
                event.setAttribute(ky, attr.get(ky));
            }
            if (isJMS) {
                o = event.getAttribute("JMSExpiration");
                event.removeAttribute("JMSExpiration");
            }
            else {
                o = event.getAttribute("EventExpiration");
                event.removeAttribute("EventExpiration");
            }
            if (o != null && o instanceof String) try { // exiration is set
                expiration = Long.parseLong((String) o);
                if (expiration > 0L) { // expiration is positive
                    // recover the original timeout
                    expiration -= timestamp;
                    event.setExpiration(expiration+System.currentTimeMillis());
                }
            }
            catch (Exception e) {
            }
        }
        return event;
    }

    /**
     * With the existing event or the event built from the HTTP request
     * according to the request path, the method sends it to the backend for
     * the response. Once response is back, it packs the response into a
     * data map and saves the map to the request as the attribute with the
     * key of the context path.  Upon success, it returns either the JSP path
     * so that the data will be formatted for output with the given jsp, or
     * the context path for caller to retrieve the data for output. Otherwise,
     * it returns null to indicate failure so that the caller can retrieve
     * the error message.
     */
    @SuppressWarnings("unchecked")
    private String processRequest(HttpExchange he, Map<String, String> response,
        Event event) throws IOException {
        Object o;
        String key = null, uri = null, msg = null, str = null, port = null;
        String action = null, category = null, jsp = null;
        String clientIP, path, method, status, username = null, type = null;
        int l, priority = Event.INFO;
        int i, debug = service.getDebugMode();
        long tm = 0L;
        boolean isJMS = true;          // an event is a JMS event or not
        boolean isCollectible = false; // a non-JMS event is collectible or not
        boolean isStream = false;
        List<String> list;
        Map<String,List<String>> params=SimpleHttpServer.parseGetParameters(he);
        clientIP = he.getRemoteAddress().getAddress().getHostAddress();

        method = he.getRequestMethod();
        if ("POST".equals(method))
            i = SimpleHttpServer.parsePostParameters(he, params);
        URI u = he.getRequestURI();
        path = u.getPath();
        if (path == null)
            path = "";
        else if (path.startsWith(baseURI))
            path = path.substring(baseURILen);
        l = path.length();
        if (event != null) { // JMS event for raw request
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " raw data: " + path).send();
            try { // raw request from POST or PUT
                event.setAttribute("_clientIP", clientIP);
                ((JMSEvent) event).setJMSType(path);
            }
            catch (Exception e) {
            }
        }
        else if (path.startsWith("/collectible") && (l == 12 ||
            path.charAt(12) == '/')) { //JMS event for collectibles
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " collectible: " + path).send();
            isCollectible = true;
            event = getEvent(params, true);
            if (event == null) { // not an event postable
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_BAD_REQUEST,"not an event postable");
                return null;
            }
            if (event.getAttribute("status") == null)
                event.setAttribute("status", "Normal");
            event.setAttribute("hostname", clientIP);
            key = zonedDateFormat.format(new Date()) + " " +
                clientIP + " " + EventUtils.collectible(event);
            event.setBody(key);
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
            category = event.getAttribute("category");
            action = event.getAttribute("operation");
            port = event.getAttribute("port");
        }
        else if (path.startsWith("/event") && (l == 6 ||
            path.charAt(6) == '/')) { // non-JMS event only
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " event: " + path).send();
            isJMS = false;
            event = getEvent(params, false);
            if (event == null) { // not an event postable
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_BAD_REQUEST,"not an event postable");
                return null;
            }
            event.removeAttribute("text");
            if (event.getAttribute("status") == null)
                event.setAttribute("status", "Normal");
            if ((uri = event.getAttribute("URI")) != null && uri.length() > 0) {
                isCollectible = true;
                port = event.getAttribute("Port");
                if (port != null && port.length() <= 0)
                    port = null;
            }
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
            event.setAttribute("hostname", clientIP);
            action = event.getAttribute("operation");
            category = event.getAttribute("category");
        }
        else if (path.startsWith("/jms") && (l == 4 ||
            path.charAt(4) == '/')) { // JMS event without collectibles
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " jms: " + path).send();
            event = getEvent(params, true);
            if (event == null) { // not an event postable
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_BAD_REQUEST,"not an event postable");
                return null;
            }
            event.setAttribute("hostname", clientIP);
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
        }
        else if (path.startsWith("/stream") && (l == 7 ||
            path.charAt(7) == '/')) { //JMS event for download stream
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " stream: " + path).send();
            isStream = true;
            event = getEvent(params, true);
            if (event == null) { // not an event postable
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_BAD_REQUEST,"not an event postable");
                return null;
            }
            event.setAttribute("hostname", clientIP);
        }
        else if (restURILen > 0 && path.startsWith(restURI) && (l==restURILen ||
            path.charAt(restURILen) == '/')) { // REST requests
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " rest: " + path).send();
            if ((list = params.get("text")) != null)
                str = list.get(0);
            else
                str = null;
            if (str != null && str.length() > 0)
                event = new TextEvent(str);
            else
                event = new TextEvent();
            event.setAttribute("_clientIP", clientIP);
            // copy over properties
            for (String ky : params.keySet()) {
                if (ky == null || ky.length() <= 0 || "text".equals(ky))
                    continue;
                if ((list = params.get(ky)) != null) {
                    str = list.get(0);
                    if (str != null)
                        event.setAttribute(ky, str);
                }
            }
            try {
                ((TextEvent) event).setJMSType(path);
            }
            catch (Exception e) {
            }
            if ("STREAM".equals(event.getAttribute("type"))) // download stream
                isStream = true;
        }
        else if ("POST".equals(method) && //xml or json form data
            ("/xml".equals(path) || "/json".equals(path))) {
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " form data: " + path).send();
            // retrieve content from the key of either xml or json
            if ((list = params.get(path.substring(1))) != null)
                str = list.get(0);
            else
                str = "";
            event = new TextEvent(str);
            event.setAttribute("hostname", clientIP);
            event.setAttribute("type",
                (("/xml".equals(path)) ? "text" : "application") + path);
            event.setAttribute("path", path);
        }
        else { // ad hoc form request for test only
            if ((debug & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " ad hoc: " + path).send();
            if ((list = params.get("jsp")) != null) {
                jsp = list.get(0);
                if (jsp != null && jsp.length() <= 0)
                    jsp = null;
            }

            if ((list = params.get("operation")) != null)
                action = list.get(0);
            if (action != null && action.length() <= 0)
                action = "query";

            if ("query".equals(action))
                priority = Event.INFO;
            else if ("stop".equals(action) || "disable".equals(action) ||
                "enable".equals(action) || "failover".equals(action) ||
                "start".equals(action) || "restart".equals(action))
                priority = Event.WARNING;

            if ((list = params.get("Port")) != null) {
                port = list.get(0);
                if (port != null && port.length() <= 0)
                    port = null;
            }

            if ((list = params.get("category")) != null)
                category = list.get(0);
            if (category == null || category.length() <= 0)
                category = "QBROKER";

            if ((list = params.get("type")) != null)
                type = list.get(0);
            if (type == null || type.length() <= 0)
                type = "TEXT";

            if ((list = params.get("URI")) != null) {
                uri = list.get(0);
                if (uri != null && uri.length() <= 0)
                    uri = null;
            }

            if ((list = params.get("name")) != null)
                key = list.get(0);
            else
                key = null;

            if ((list = params.get("view")) != null)
                str = list.get(0);
            else
                str = null;
            if (str != null && str.length() > 0) { // ad hoc jms event
                event = new TextEvent();
                event.setAttribute("name", key);
                event.setAttribute("type", type);
                event.setAttribute("view", str);
                event.setAttribute("category", category);
                event.setAttribute("operation", action);
                event.setAttribute("hostname", clientIP);
                if ((list = params.get("service")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("service", str);
                }
                if ((list = params.get("asset")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("asset", str);
                }
                if ((list = params.get("site_name")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("site_name", str);
                }
                if ((list = params.get("short_name")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("short_name", str);
                }
                if ((list = params.get("FileName")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("FileName", str);
                }
                if (uri != null) {
                    event.setAttribute("URI", uri);
                    if (port != null)
                        event.setAttribute("Port", port);
                }
                if ((list = params.get("Expiry")) != null) try {
                    tm = Long.parseLong(list.get(0));
                    if (tm > 0) // set expiration
                        event.setExpiration(tm + event.getTimestamp());
                }
                catch (Exception e) {
                }
            }
            else if (key != null && key.length() > 0) { // non-JMS event
                isJMS = false;
                event = new Event(priority);
                event.setAttribute("name", key);
                event.setAttribute("type", type);
                event.setAttribute("category", category);
                event.setAttribute("operation", action);
                event.setAttribute("status", "Normal");
                event.setAttribute("hostname", clientIP);
                if (uri != null) {
                    isCollectible = true;
                    event.setAttribute("URI", uri);
                    if (port != null)
                        event.setAttribute("Port", port);
                }
                if ((list = params.get("short_name")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("short_name", str);
                }
                if ((list = params.get("service")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("service", str);
                }
                if ((list = params.get("asset")) != null) {
                    str = list.get(0);
                    if (str != null && str.length() > 0)
                        event.setAttribute("asset", str);
                }
                if ((list = params.get("Expiry")) != null) try {
                    tm = Long.parseLong(list.get(0));
                    event.setExpiration(tm + event.getTimestamp());
                }
                catch (Exception e) {
                }
            }
            else { // neither name nor view is defined
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_PRECON_FAILED,
                    "neither name nor view is defined");
                return null;
            }
        }

        username = clientIP;
        if (isJMS || isCollectible) { // JMS or non-JMS collectible event
            JMSEvent message;
            tm = 0L;
            if (event instanceof JMSEvent) {
                message = (JMSEvent) event;
                tm = event.getExpiration();
                if (tm > 0L) // recover timeout
                    tm -= System.currentTimeMillis();
                for (i=0; i<propertyName.length; i++)
                    message.setAttribute(propertyName[i], propertyValue[i]);
            }
            else try { // ad hoc request
                key = zonedDateFormat.format(new Date()) + " " +
                    clientIP + " " + EventUtils.collectible(event);
                message = new TextEvent();
                message.setJMSPriority(9 - event.getPriority());
                ((TextEvent) message).setText(key);
                if ((key = event.getAttribute("short_name")) != null)
                    message.setStringProperty("short_name", key);
                if ((key = event.getAttribute("service")) != null)
                    message.setStringProperty("service", key);
                if ((key = event.getAttribute("operation")) != null)
                    message.setStringProperty("operation", key);
                message.setStringProperty("URI", uri);
                if (port != null)
                    message.setStringProperty("Port", port);
                tm = event.getExpiration();
                if (tm > 0L) {
                    message.setJMSExpiration(tm);
                    tm -= System.currentTimeMillis();
                }
                else // set default expiration
                    message.setJMSExpiration(event.getTimestamp() + timeout);
                for (i=0; i<propertyName.length; i++)
                    message.setAttribute(propertyName[i], propertyValue[i]);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " failed to load message: "+
                    Event.traceStack(e)).send();
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "failed to load message");
                return null;
            }

            if (tm <= timeout)
                tm = timeout;

            // send the request and wait until timeout
            i = service.doRequest(message, (int) tm);

            if (i > 0) { // got response back
                for (String ky : ((Event) message).getAttributeNames()) {
                    if (ky == null || ky.length() <= 0)
                        continue;
                    if ("text".equals(ky))
                        continue;
                    response.put(ky, ((Event) message).getAttribute(ky));
                }

                key = null;
                if (message instanceof TextEvent) try { // for body
                    key = ((TextEvent) message).getText();
                }
                catch (Exception e) {
                }
                if (key != null)
                    response.put("text", key);

                if (((Event) message).attributeExists("rc"))
                    str = ((Event) message).getAttribute("rc");
                else
                    str = ((Event) message).getAttribute("ReturnCode");
                if (str != null) // notify client
                    he.getResponseHeaders().set("rc", str);

                str = ((Event) message).getAttribute("jsp");
                if (str != null) // overwrite the presentation jsp
                    jsp = str;
            }
            else { // timed out
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_GATEWAY_TIMEOUT,
                    "request timed out: " + i);
                return null;
            }
        }
        else { // non-JMS event for the current QFlow
            for (i=0; i<propertyName.length; i++)
                event.setAttribute(propertyName[i], propertyValue[i]);
            i = service.doRequest(event, timeout);
            if (i > 0) { // got response back
                Map ph;
                if (event.attributeExists("rc"))
                    str = event.getAttribute("rc");
                else
                    str = event.getAttribute("ReturnCode");
                if (str != null) // notify client
                    he.getResponseHeaders().set("rc", str);
                str = event.getAttribute("operation");
                type = event.getAttribute("type");
                if (!"query".equals(str) || "json".equalsIgnoreCase(type) ||
                    "xml".equalsIgnoreCase(type)) { // no result map to retrieve
                    for (String ky : event.getAttributeNames()) {
                        if (ky == null || ky.length() <= 0)
                            continue;
                        response.put(ky, event.getAttribute(ky));
                    }   
                }
                else if ((ph = (Map) event.getBody()) == null) { // query failed
                    SimpleHttpServer.sendResponse(he,
                        HttpURLConnection.HTTP_NOT_FOUND,
                        event.getAttribute("text"));
                    return null;
                }
                else { // got query result map from event
                    for (Object obj : ph.keySet()) {
                        key = (String) obj;
                        if (key == null || key.length() <= 0)
                            continue;
                        response.put(key, (String) ph.get(key));
                    }
                    ph.clear();
                }   
                str = event.getAttribute("jsp");
                if (str != null) // overwrite the presentation jsp
                    jsp = str;
            }
            else { // timed out
                SimpleHttpServer.sendResponse(he,
                    HttpURLConnection.HTTP_GATEWAY_TIMEOUT,
                    "request timed out: " + i);
                return null;
            }
        }

        if ((debug & Service.DEBUG_REPT) > 0)
            new Event(Event.DEBUG, name + " sent back to " + username +
               " with requested content: "+ response.get("text")).send();

        return (jsp != null) ? jsp : "text";
    }

    private static String getContent(String uri, Map<String, String> response) {
        String value;
        if (uri.charAt(1) == 'g') { // for get
            if (uri.charAt(4) == 'C') { // for child
                value = response.get("text");
                if (value == null)
                    value = "";
                int i = value.indexOf("[{");
                int j = org.qbroker.json.JSON2Map.locate(2, value);
                if (i > 0 && j > i) {
                    return "{\n  \"success\":true,\n  \"data\":" +
                        value.substring(i+1,j+1) + "\n}\n";
                }
                else { // wrong data
                    value =(value.length()>60) ? value.substring(0, 60) : value;
                    return "{\n  \"success\":false,\n" +
                        "  \"errors\":{\"title\":\"text" + "\"},\n" +
                        "  \"errormsg\":\"" +
                        value.replaceAll("\"","'").replaceAll("\n","") +
                        "\"\n}\n";
                }
            }
            else if ("/getJSON.jsp".equals(uri)) {
                return response.get("text");
            }
        }
        else if (uri.charAt(1) == 't') { // for to
            if (uri.charAt(3) == 'R') { // for rc
                if ("/toRCJSON.jsp".equals(uri)) {
                    value = response.get("ReturnCode");
                    if (value == null)
                        value = "";
                    return "{\n  \"success\":true,\n  \"data\":{\n" +
                        "    \"name\":\"" + response.get("name") + "\",\n" +
                        "    \"rc\":\"" + value + "\"\n  }\n}\n";
                }
            }
            else if ("/toJSON.jsp".equals(uri)) {
                StringBuffer strBuf = new StringBuffer();
                for (String key : response.keySet()) {
                    if (key == null || key.length() <= 0)
                        continue;
                    value = response.get(key);
                    if (value == null)
                        value = "";
                    if (strBuf.length() <= 0)
                        strBuf.append("    \"" + key + "\":\"" + value + "\"");
                    else
                        strBuf.append(",\n    \"" + key + "\":\"" + value+"\"");
                }
                return "{\n  \"success\":true,\n  \"data\":{\n" +
                    strBuf.toString() + "\n  }\n}\n";
            }
        }
        response.remove("text");

        return null;
    }

    public static void main(String[] args) {
        byte[] buffer = new byte[4096];
        String path = null, context = "echo";
        EchoService service = null;
        MessageHandler handler = null;
        SimpleHttpServer httpServer = null;
        int debug = 0;

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
              case 'c':
                if (i+1 < args.length)
                    context = args[++i];
                break;
              case 'd':
                if (i+1 < args.length)
                    debug = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (path == null)
            printUsage();
        else try {
            StringBuffer strBuf = new StringBuffer();
            java.io.FileReader fr = new java.io.FileReader(path);
            Map ph = (Map) org.qbroker.json.JSON2Map.parse(fr);
            fr.close();

            if (!context.startsWith("/"))
                context = "/" + context;
            service = new EchoService();
            if (debug != 0)
                service.setDebugMode(debug);
            httpServer = new SimpleHttpServer(ph);
            handler = new MessageHandler(ph);
            handler.setService(service);
            httpServer.addContext(handler, context);
            service.start();
            httpServer.start();
            System.out.println("Server started. Please run the following command to test:");
            System.out.println("Enter Ctrl+C to stop the server");
            httpServer.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (service != null)
            service.stop();
        if (httpServer != null) try {
            httpServer.stop();
        }
        catch (Exception ex) {
        }
    }

    private static void printUsage() {
        System.out.println("MessageHandler Version 1.0 (written by Yannan Lu)");
        System.out.println("MessageHandler: An HttpServer embedded in EchoService for testing");
        System.out.println("Usage: java org.qbroker.jms.MessageHandler -I cfg.json -c context -d debug");
    }
}
