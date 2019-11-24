package org.qbroker.jms;

/* MessageServlet.java - a generic servlet for JMS messages */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.List;
import java.util.Date;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItem;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.FailedLoginException;
import org.qbroker.jaas.SimpleCallbackHandler;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Base64Encoder;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.DBConnector;
import org.qbroker.net.JettyServer;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.EchoService;

/**
 * MessageServlet is a servlet for generic message services. It allows a message
 * service running inside the servlet container as a web application. As a web
 * application, the message service can receive JMS messages directly from web
 * clients and sends messages back on HTTP. Therefore, MessageServlet is the
 * frontend and the gateway for message services.  It is responsible for
 * transformation between the HTTP requests/responses and JMS messages.
 *<br><br>
 * MessageServlet has 6 rulesets referenced by the URL path. The first one is
 * /jms for receiving event postables as the JMS TextEvents. The second is
 * /collectible for receiving event postables from the client and transforming
 * them to the collectible JMS TextEvents. The next is /event for receiving
 * event postables as the non-JMS events. The pre-configured RestURI is for the
 * ruleset to convert simple REST requests to JMS TextEvents. In this ruleset,
 * the path of /json or /xml is for posted JSON or XML data from a client.
 * The path of /stream is for downloading binary content from JDBC datasources, 
 * The rest of paths are for ad hoc HTML form requests. When a web request hits
 * one of the URLs, the corresponding ruleset will be invoked to process the
 * incoming request. MessageServlet supports POST, GET and PUT methods. For
 * POST or PUT, those headers matched with HeaderRegex will be copied into the
 * message. One of examples with HeaderRegex is "^[Xx]-.+$" that matches all
 * HTTP headers of starting with "X-". In case of POST, it also supports file
 * upload or raw content such as xml or json.
 *<br><br>
 * A collectible TextEvent is a TextEvent with its message body set to the
 * collectible format of the original message. It is meant to be sent to remote
 * destinations. In order to get content of the original message, the remote 
 * consumer will have to parse the message body with the EventParser.
 *<br><br>
 * For ad hoc form requests, MessageServlet treats them in two different ways.
 * If the attribute of view is defined in the request and it is not empty, the
 * request will be converted into a TextEvent. Otherwise, if the attribute of
 * name is defined and it is not empty, the request will be converted to an
 * Event. If neither view nor name is defined, the ad hoc request will be
 * dropped as a bad request. Further more, if the attribute of URI is defined
 * and it is not empty, the request Event will be packed into collectible.
 *<br><br>
 * Once an incoming request is transformed into message, MessageServlet will
 * invoke doRequest() of the assigned service to process the message as the
 * request. After the process is done, the message is supposed to have the
 * response loaded. MessageServlet will convert it back to the HTTP response
 * and sends the response back to the HTTP client.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageServlet extends HttpServlet {
    private Service service = null;
    protected String name = null;
    protected String loginURL = "/login.jsp";
    protected String welcomeURL = "/welcome.jsp";
    protected int timeout = 10000;
    protected SimpleDateFormat zonedDateFormat;
    protected DiskFileItemFactory factory = null;
    protected String jaasLogin = null;
    protected String restURI = null;
    protected String headerRegex = null;
    protected SimpleCallbackHandler jaasHandler = null;
    protected String[] propertyName, propertyValue;
    protected MessageDigest md = null;
    protected int restURILen = 0;

    public MessageServlet() { // for servlet container only
    }

    public MessageServlet(Map props) {
        this();
        int i, n;
        Object o;
        String jaasConfig = null;

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

        if ((o = props.get("JAASLogin")) != null) {
            jaasLogin = (String) o;
            if ((o = props.get("JAASConfig")) != null)
                jaasConfig = (String) o;
        }

        if (jaasLogin != null) { // initialize JAAS LoginContext
            if (jaasConfig != null)
               System.setProperty("java.security.auth.login.config",jaasConfig);
            jaasHandler = new SimpleCallbackHandler();
        }

        // initialize MD5 
        if (jaasHandler != null) try {
            md = MessageDigest.getInstance("MD5");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init MD5: " +
                e.toString()));
        }

        factory = new DiskFileItemFactory();

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            Iterator iter = ((Map) o).keySet().iterator();
            n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            n = 0;
            while (iter.hasNext()) {
                String key = (String) iter.next();
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
    }

    /** sets the service to fulfill requests */
    public void setService(Service service) {
        if (service == null)
            throw(new IllegalArgumentException("service is null"));
        this.service = service;
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
        String uri = null;
        Object o;

        uri = processRequest(request, response, null);

        if (uri == null)
            return;
        else if (uri.endsWith(".jsp")) {
            String text = getContent(uri, request, response);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().println((String) o);
        }
    }

    public void doPost(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null, path;
        Object o;
        Iterator iter;
        Event event = null;
        String str;
        int bufferSize = 4096;
        long size = 0;
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);

        path = request.getPathInfo();
        if (path == null)
            path = "";

        str = request.getContentType();
        if ("text/xml".equals(str) || // raw xml request
            "application/json".equals(str)) { // or raw json
            byte[] buffer = new byte[bufferSize];
            String key;
            int bytesRead;
            InputStream in = request.getInputStream();
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
                new Event(Event.ERR, getServletName() +
                    " failed to read raw content: " +
                    Event.traceStack(e)).send();
                response.setContentType("text/plain");
                response.getWriter().println("failed to read raw content: " +
                    Event.traceStack(e));
                return;
            }
            Map props = request.getParameterMap();
            if (props == null)
                props = new HashMap();
            iter = props.keySet().iterator();
            while (iter.hasNext()) { // copy over properties
                key = (String) iter.next();
                if (key == null || key.length() <= 0 || "text".equals(key))
                    continue;
                if ((o = props.get(key)) != null) {
                    str = ((String[]) o)[0];
                    if (str != null)
                        event.setAttribute(key, str);
                }
            }
        }
        else if (isMultipart) try { // fileUpload
            String key, type = null, mimetype = null;
            ServletFileUpload upload = new ServletFileUpload(factory);
            List items = upload.parseRequest(request);
            if (items != null&& items.size() > 0) {
                long len = 0;
                InputStream in = null;
                event = new TextEvent();
                byte[] buffer = new byte[bufferSize];
                iter = items.iterator();
                while (iter.hasNext()) {
                    FileItem item = (FileItem) iter.next();
                    if (item.isFormField()) {
                        key = item.getFieldName();
                        event.setAttribute(key, item.getString());
                    }
                    else {
                        event.setAttribute("name", item.getName());
                        mimetype = item.getContentType();
                        len = item.getSize();
                        event.setAttribute("size", String.valueOf(len));
                        in = item.getInputStream();
                    }
                }
                type = event.getAttribute("type");
                if (type == null || type.length() <= 0) { // reset type
                    if (mimetype != null && mimetype.length() > 0)
                        type = mimetype;
                    else // default
                        type = "text/plain";
                    event.setAttribute("type", type);
                }
                if (!path.startsWith("/stream")) { // for regular file upload
                    int bytesRead;
                    StringBuffer strBuf = new StringBuffer();
                    while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                        if (bytesRead > 0)
                            strBuf.append(new String(buffer, 0, bytesRead));
                    }
                    ((TextEvent) event).setText(strBuf.toString());
                    ((TextEvent) event).setJMSPriority(9-Event.INFO);
                }
                else { // file upload for stream operations
                    int i, rc = -1;
                    Map<String, Object> ph;
                    DBConnector conn = null;
                    long tm = event.getExpiration();
                    if (tm > 0L) // recover timeout
                        tm -= System.currentTimeMillis();
                    if (jaasLogin != null) { // enforce JAAS Login
                        HttpSession session = request.getSession(true);
                        if (session.isNew()) { // not login yet
                            response.setContentType("text/plain");
                            response.getWriter().println("please login first");
                            return;
                        }
                        key = (String) session.getAttribute("username");
                        event.setAttribute("login", key);
                    }
                    key = event.getAttribute("name");
                    for (i=0; i<propertyName.length; i++)
                        event.setAttribute(propertyName[i], propertyValue[i]);
                    i = service.doRequest(event, (int) tm);
                    if (event.attributeExists("rc"))
                        str = event.getAttribute("rc");
                    else
                        str = event.getAttribute("ReturnCode");
                    if (str != null && !"0".equals(str)) { // failed
                        response.sendError(response.SC_NOT_FOUND);
                    }
                    else try { // try to send payload back
                        type = event.getAttribute("type");
                        uri = event.getAttribute("uri");
                        ph = new HashMap<String, Object>();
                        ph.put("URI", uri);
                        str = event.getAttribute("Username");
                        if (str != null && str.length() > 0) {
                            ph.put("Username", str);
                            str = event.getAttribute("Password");
                            if (str != null && str.length() > 0)
                                ph.put("Password", str);
                            else
                                ph.put("EncryptedPassword",
                                    event.getAttribute("EncryptedPassword"));
                        }
                        conn = new DBConnector(ph);
                        str = ((TextEvent) event).getText();
                        rc = conn.copyStream((int) len, in, str);
                        try {
                            conn.close();
                        }
                        catch (Exception ex) {
                        }
                        key += " has been uploaded with "+rc+" record updated";
                        if ((service.getDebugMode() & service.DEBUG_TRAN) > 0)
                            new Event(Event.DEBUG, getServletName() +
                                ": " + key).send();
                        key += ". You may need to refresh to see the change.";
                        response.getWriter().println(key);
                    }
                    catch (Exception ee) {
                        if (conn != null) try {
                            conn.close();
                        }
                        catch (Exception ex) {
                        }
                        new Event(Event.ERR, getServletName() +
                           " failed to upload " + len + " bytes for " +
                           event.getAttribute("name") + ": " +
                           Event.traceStack(ee)).send();
                        response.sendError(response.SC_INTERNAL_SERVER_ERROR);
                    }
                    return;
                }
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, getServletName() + " failed to upload file: " +
                Event.traceStack(e)).send();
            response.setContentType("text/plain");
            response.getWriter().println("file upload failed: " +
                Event.traceStack(e));
            return;
        }
        if (event != null && headerRegex != null) { // copy the headers over
            String key;
            Enumeration headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                key = (String) headerNames.nextElement();
                if (key == null || key.equals("null") ||
                    key.equalsIgnoreCase("Cookie") ||
                    key.equalsIgnoreCase("Content-Type") ||
                    key.equalsIgnoreCase("Authorization"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = request.getHeader(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(request, response, event);

        if (uri == null)
            return;
        else if (uri.endsWith(".jsp")) {
            String text = getContent(uri, request, response);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().println((String) o);
        }
    }

    public void doPut(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null, path, key, str;
        Object o;
        Iterator iter;
        BytesEvent event = null;
        int bufferSize = 4096;
        int bytesRead;
        long size = 0;

        path = request.getPathInfo();
        if (path == null)
            path = "";

        byte[] buffer = new byte[bufferSize];
        InputStream in = request.getInputStream();
        try {
            event = new BytesEvent();
            if (in != null) {
                while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        event.writeBytes(buffer, 0, bytesRead);
                    size += bytesRead;
                }
                event.reset();
                event.setAttribute("type", request.getContentType());
                event.setAttribute("method", request.getMethod());
                event.setAttribute("size", String.valueOf(size));
                event.setAttribute("path", path);
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, getServletName() +
                " failed to read raw content: " +
                Event.traceStack(e)).send();
            response.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT);
            response.setContentType("text/plain");
            response.getWriter().println("failed to read raw content: " +
                Event.traceStack(e));
            return;
        }
        Map props = request.getParameterMap();
        if (props == null)
            props = new HashMap();
        iter = props.keySet().iterator();
        while (iter.hasNext()) { // copy over properties
            key = (String) iter.next();
            if (key == null || key.length() <= 0 || "text".equals(key))
                continue;
            if ((o = props.get(key)) != null) {
                str = ((String[]) o)[0];
                if (str != null)
                    event.setAttribute(key, str);
            }
        }
        if (headerRegex != null) { // copy the headers over
            Enumeration headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                key = (String) headerNames.nextElement();
                if (key == null || key.equals("null") ||
                    key.equalsIgnoreCase("Cookie") ||
                    key.equalsIgnoreCase("Content-Type") ||
                    key.equalsIgnoreCase("Authorization"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = request.getHeader(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(request, response, event);

        if (uri == null)
            return;
        else if (uri.endsWith(".jsp")) {
            String text = getContent(uri, request, response);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            response.getWriter().println((String) o);
        }
    }

    public void doDelete(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null;
        Object o;

        uri = processRequest(request, response, null);

        if (uri == null)
            return;
        else if (uri.endsWith(".jsp")) {
            String text = getContent(uri, request, response);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().println((String) o);
        }
    }

    public static Event getEvent(Map props, boolean isJMS) {
        Object o;
        Event event = null;
        String key = null, username = null, site = null,
            text = null, category = null;
        int priority = -1;
        long timestamp = 0L, expiration = 0L;
        boolean skip = false;

        if ((o = props.get("priority")) != null)
            priority = Event.getPriorityByName(((String[]) o)[0]);
        if ((o = props.get("summary")) != null)
            text = ((String[]) o)[0];
        if ((o = props.get("date")) != null) try {
            timestamp = Long.parseLong(((String[]) o)[0]);
        }
        catch (Exception e) { // bad timestamp for date
            return null;
        }
        if ((o = props.get("NAME")) != null)
            key = ((String[]) o)[0];
        if ((o = props.get("SITE")) != null)
            site = ((String[]) o)[0];
        if ((o = props.get("CATEGORY")) != null)
            category = ((String[]) o)[0];
        if ((o = props.get("username")) != null)
            username = ((String[]) o)[0];
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
        if ((o = props.get("hostname")) != null)
            event.setAttribute("hostname", ((String[]) o)[0]);
        if ((o = props.get("program")) != null)
            event.setAttribute("program", ((String[]) o)[0]);
        if ((o = props.get("type")) != null)
            event.setAttribute("type", ((String[]) o)[0]);
        if ((o = props.get("pid")) != null)
            event.setAttribute("pid", ((String[]) o)[0]);
        if ((o = props.get("content")) != null)
            text = ((String[]) o)[0];
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
                if (ky != null && ky.length() > 0)
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
     * key of the context path. Upon success, it returns either the JSP path
     * so that the data will be formatted for output with the given jsp, or
     * the context path for caller to retrieve the data for output. Otherwise,
     * it returns null to indicate failure so that the caller can retrieve
     * the error message.
     */
    @SuppressWarnings("unchecked")
    protected String processRequest(HttpServletRequest request,
        HttpServletResponse response, Event event)
        throws ServletException, IOException {
        Object o;
        String key = null, uri = null, msg = null, str = null, port = null;
        String target = null, action = null, category = null, jsp = null;
        String clientIP, path, sessionId, status, username = null, type = null;
        int l, priority = Event.INFO;
        long tm = 0L;
        boolean isJMS = true;          // an event is a JMS event or not
        boolean isCollectible = false; // a non-JMS event is collectible or not
        boolean isFileUpload = false;
        boolean isStream = false;
        Map props = request.getParameterMap();
        if (name == null)
            name = getServletName();
        path = request.getPathInfo();
        clientIP = request.getRemoteAddr();
        if (jaasLogin != null) { // enforce JAAS Login
            HttpSession session = request.getSession(true);
            if (session.isNew() || // new session
                (status = (String) session.getAttribute("status")) == null) {
                if (session.getAttribute("url") == null) {
                    o = request.getHeader("Referer");
                    if (o != null && ((String) o).indexOf(loginURL) < 0)
                        session.setAttribute("url", (String) o);
                }
                session.setMaxInactiveInterval(28800);
                session.setAttribute("status", "NEW");
                status = "NEW";
            }
            else if ("/logout".equals(path)) try { // logging out
                username = (String) session.getAttribute("username");
                session.invalidate();
                new Event(Event.INFO, name +": "+ username +" logged out from "+
                    clientIP).send();
                response.setContentType("text/plain");
              response.getWriter().println("Thanks,your session is terminated");
                return null;
            }
            catch (Exception e) {
                response.setContentType("text/plain");
                response.getWriter().println("Sorry, " + e.toString());
                return null;
            }

            if ("NEW".equals(status)) { // not logged in yet
                TextEvent message;
                o = props.get("username");
                username = (o != null) ? ((String[]) o)[0] : null;
                o = props.get("password");
                String password = (o != null) ? ((String[]) o)[0] : null;
                if (username == null || password == null)
                    return loginURL;
                if (!login(username, password)) // failed on login
                    return loginURL;
                // the user has logged in
                response.setHeader("login", username);
                session.setAttribute("status", "ACTIVE");
                session.setAttribute("username", username);
                session.setAttribute("clientIP", clientIP);
                sessionId = session.getId();
                if (sessionId != null && sessionId.length() > 0) {
                    md.reset();
                    md.update(sessionId.getBytes());
                    sessionId = new String(Base64Encoder.encode(md.digest()));
                }
                new Event(Event.INFO, name +": "+ username +" logged in from "+
                    clientIP + " with the session of " + sessionId).send();
                message = new TextEvent();
                message.setAttribute("name", name);
                message.setAttribute("type", "auth");
                message.setAttribute("view", "session");
                message.setAttribute("category", "user");
                message.setAttribute("operation", "login");
                message.setAttribute("hostname", clientIP);
                message.setAttribute("status", "ACTIVE");
                message.setAttribute("username", username);
                message.setAttribute("sessionid", sessionId);
                message.setAttribute("useragent",
                    request.getHeader("User-Agent"));
                for (int i=0; i<propertyName.length; i++)
                    message.setAttribute(propertyName[i], propertyValue[i]);
                service.doRequest(message, timeout);
                o = props.get("operation");
                if (o == null)
                    return welcomeURL;
                action = ((String[]) o)[0];
                if (action == null || "login".equals(action))
                    return welcomeURL;
            }
            username = (String) session.getAttribute("username");
            response.setHeader("login", username);
        }
        else if (loginURL != null) {
            response.setHeader("login", "no");
        }

        if (path == null)
            path = "";
        l = path.length();
        if (event != null) { // JMS event for file upload or raw request
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " raw data: " + path).send();
            str = request.getContentType();
            if ("POST".equals(request.getMethod()) &&
                !"text/xml".equals(str) && !"application/json".equals(str)) {
                isFileUpload = true;
                if (event.attributeExists("jsp"))
                    jsp = event.getAttribute("jsp");
            }
            else try { // raw request from POST or PUT
                event.setAttribute("_clientIP", clientIP);
                ((JMSEvent) event).setJMSType(path);
            }
            catch (Exception e) {
            }
        }
        else if (path.startsWith("/collectible") && (l == 12 ||
            path.charAt(12) == '/')) { // JMS event for collectibles
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " collectible: " + path).send();
            isCollectible = true;
            event = getEvent(props, true);
            if (event == null) { // not an event postable
                response.sendError(response.SC_BAD_REQUEST);
                request.setAttribute("error", "not an event postable");
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
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " event: " + path).send();
            isJMS = false;
            event = getEvent(props, false);
            if (event == null) { // not an event postable
                response.sendError(response.SC_BAD_REQUEST);
                request.setAttribute("error", "not an event postable");
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
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " jms: " + path).send();
            event = getEvent(props, true);
            if (event == null) { // not an event postable
                response.sendError(response.SC_BAD_REQUEST);
                request.setAttribute("error", "not an event postable");
                return null;
            }
            event.setAttribute("hostname", clientIP);
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
        }
        else if (path.startsWith("/stream") && (l == 7 ||
            path.charAt(7) == '/')) { // JMS event for download stream
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " stream: " + path).send();
            isStream = true;
            event = getEvent(props, true);
            if (event == null) { // not an event postable
                response.sendError(response.SC_BAD_REQUEST);
                request.setAttribute("error", "not an event postable");
                return null;
            }
            event.setAttribute("hostname", clientIP);
        }
        else if (restURILen > 0 && path.startsWith(restURI) && (l==restURILen ||
            path.charAt(restURILen) == '/')) { // REST requests
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " rest: " + path).send();
            if ((o = props.get("text")) != null)
                str = ((String[]) o)[0];
            else
                str = null;
            if (str != null && str.length() > 0)
                event = new TextEvent(str);
            else
                event = new TextEvent();
            event.setAttribute("_clientIP", clientIP);
            // copy over properties
            Iterator iter = props.keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0 || "text".equals(key))
                    continue;
                if ((o = props.get(key)) != null) {
                    str = ((String[]) o)[0];
                    if (str != null)
                        event.setAttribute(key, str);
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
        else if ("POST".equals(request.getMethod()) && // xml or json form data
            ("/xml".equals(path) || "/json".equals(path))) {
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " form data: " + path).send();
            // retrieve content from the key of either xml or json
            if ((o = props.get(path.substring(1))) != null)
                str = ((String[]) o)[0];
            else
                str = "";
            event = new TextEvent(str);
            event.setAttribute("hostname", clientIP);
            event.setAttribute("type",
                (("/xml".equals(path)) ? "text" : "application") + path);
            event.setAttribute("path", path);
        }
        else { // ad hoc form request for test only
            if ((service.getDebugMode() & Service.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " ad hoc: " + path).send();
            if ((o = props.get("jsp")) != null) {
                jsp = ((String[]) o)[0];
                if (jsp != null && jsp.length() <= 0)
                    jsp = null;
            }

            if ((o = props.get("operation")) != null)
                action = ((String[]) o)[0];
            if (action == null || action.length() <= 0)
                action = "query";

            if ("query".equals(action))
                priority = Event.INFO;
            else if ("stop".equals(action) || "disable".equals(action) ||
                "enable".equals(action) || "failover".equals(action) ||
                "start".equals(action) || "restart".equals(action))
                priority = Event.WARNING;

            if ((o = props.get("Port")) != null) {
                port = ((String[]) o)[0];
                if (port != null && port.length() <= 0)
                    port = null;
            }

            if ((o = props.get("category")) != null)
                category = ((String[]) o)[0];
            if (category == null || category.length() <= 0)
                category = "QBROKER";

            if ((o = props.get("type")) != null)
                type = ((String[]) o)[0];
            if (type == null || type.length() <= 0)
                type = "TEXT";

            if ((o = props.get("URI")) != null) {
                uri = ((String[]) o)[0];
                if (uri != null && uri.length() <= 0)
                    uri = null;
            }

            if ((o = props.get("name")) != null)
                key = ((String[]) o)[0];
            else
                key = null;

            if ((o = props.get("view")) != null)
                str = ((String[]) o)[0];
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
                if ((o = props.get("service")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("service", str);
                }
                if ((o = props.get("asset")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("asset", str);
                }
                if ((o = props.get("site_name")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("site_name", str);
                }
                if ((o = props.get("short_name")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("short_name", str);
                }
                if ((o = props.get("FileName")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("FileName", str);
                }
                if (uri != null) {
                    event.setAttribute("URI", uri);
                    if (port != null)
                        event.setAttribute("Port", port);
                }
                if ((o = props.get("Expiry")) != null) try {
                    tm = Long.parseLong(((String[]) o)[0]);
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
                if ((o = props.get("short_name")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("short_name", str);
                }
                if ((o = props.get("service")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("service", str);
                }
                if ((o = props.get("asset")) != null) {
                    str = ((String[]) o)[0];
                    if (str != null && str.length() > 0)
                        event.setAttribute("asset", str);
                }
                if ((o = props.get("Expiry")) != null) try {
                    tm = Long.parseLong(((String[]) o)[0]);
                    event.setExpiration(tm + event.getTimestamp());
                }
                catch (Exception e) {
                }
            }
            else { // neither name nor view is defined
                response.sendError(response.SC_PRECONDITION_FAILED);
                request.setAttribute("error",
                    "neither name nor view is defined");
                return null;
            }
        }

        if (jaasLogin != null)
            event.setAttribute("login", username);
        else
            username = clientIP;
        if (isJMS || isCollectible) { // JMS or non-JMS collectible event
            int i;
            Map<String, Object> ph;
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
                new Event(Event.ERR, name + " failed to load message: " +
                    Event.traceStack(e)).send();
                response.sendError(response.SC_INTERNAL_SERVER_ERROR);
                request.setAttribute("error", "failed to load message");
                return null;
            }

            if (tm <= timeout)
                tm = timeout;

            //send the request and wait until timeout
            i = service.doRequest(message, (int) tm);

            if (i > 0 && isStream) { // for stream on download
                int len = -1;
                DBConnector conn = null;
                if (((Event) message).attributeExists("rc"))
                    str = ((Event) message).getAttribute("rc");
                else
                    str = ((Event) message).getAttribute("ReturnCode");
                if (str != null && !"0".equals(str)) { // failed
                    response.setHeader("rc", str);
                    response.sendError(response.SC_NOT_FOUND);
                }
                else try { // try to send payload back
                    OutputStream out;
                    type = ((Event) message).getAttribute("type");
                    response.setContentType(type);
                    out = response.getOutputStream();
                    uri = ((Event) message).getAttribute("uri");
                    ph = new HashMap<String, Object>();
                    ph.put("URI", uri);
                    str = ((Event) message).getAttribute("Username");
                    if (str != null && str.length() > 0) {
                        ph.put("Username", str);
                        str = ((Event) message).getAttribute("Password");
                        if (str != null && str.length() > 0)
                            ph.put("Password", str);
                        else
                            ph.put("EncryptedPassword",
                            ((Event)message).getAttribute("EncryptedPassword"));
                    }
                    conn = new DBConnector(ph);
                    str = ((TextEvent) message).getText();
                    len = conn.copyStream(str, out);
                    try {
                        conn.close();
                    }
                    catch (Exception ex) {
                    }
                    if ((service.getDebugMode() & service.DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name + " queried " + len +
                            " bytes from SQL: " + str).send();
                }
                catch (Exception e) {
                    if (conn != null) try {
                        conn.close();
                    }
                    catch (Exception ex) {
                    }
                    key = event.getAttribute("name");
                    if (key == null)
                        key = path;
                    new Event(Event.ERR, name + " failed to write " + len +
                        " bytes for " + key + ": "+ Event.traceStack(e)).send();
                    response.setHeader("rc", "-1");
                    response.sendError(response.SC_INTERNAL_SERVER_ERROR);
                }
                return null;
            }
            else if (i > 0) { // got result back
                ph = new HashMap<String, Object>();
                for (String ky : ((Event) message).getAttributeNames()) {
                    if (ky == null || ky.length() <= 0)
                        continue;
                    if ("text".equals(ky))
                        continue;
                    ph.put(ky, ((Event) message).getAttribute(ky));
                }

                key = null;
                if (isFileUpload) { // for upload
                    key = event.getAttribute("name");
                    if (key == null)
                        key = path;
                    key = key + " has been uploaded. " +
                        "Please close the popup and refresh the page";
                }
                else if (message instanceof TextEvent) try { // for body
                    key = ((TextEvent) message).getText();
                }
                catch (Exception e) {
                }
                if (key != null)
                    ph.put("text", key);

                if (((Event) message).attributeExists("rc"))
                    str = ((Event) message).getAttribute("rc");
                else
                    str = ((Event) message).getAttribute("ReturnCode");
                if (str != null) // notify client
                    response.setHeader("rc", str);

                str = ((Event) message).getAttribute("jsp");
                if (str != null) // overwrite the presentation jsp
                    jsp = str;
                else if (jsp == null) { // no presentation
                    str = ((Event) message).getAttribute("type");
                    if (str != null) // overwrite content type
                        type = str;
                    if (type == null || "text".equalsIgnoreCase(type))
                        response.setContentType("text/plain");
                    else if ("json".equalsIgnoreCase(type))
                        response.setContentType("application/json");
                    else if ("xml".equalsIgnoreCase(type))
                        response.setContentType("text/xml");
                    else if ("html".equalsIgnoreCase(type))
                        response.setContentType("text/html");
                    else if (message instanceof TextEvent)
                        response.setContentType("text/plain");
                    else
                        response.setContentType(type);
                }
                // save the data map as the attribute of context
                target = request.getContextPath();
                request.setAttribute(target, ph);
            }
            else { // timed out or failed
                response.sendError(response.SC_GATEWAY_TIMEOUT);
                request.setAttribute("error", "failed to get response: " + i);
                return null;
            }
        }
        else { // non-JMS event for the assigned service
            int i;
            for (i=0; i<propertyName.length; i++)
                event.setAttribute(propertyName[i], propertyValue[i]);
            i = service.doRequest(event, timeout);
            if (i > 0) { // got result back
                Map ph;
                if (event.attributeExists("rc"))
                    str = event.getAttribute("rc");
                else
                    str = event.getAttribute("ReturnCode");
                if (str != null) // notify client
                    response.setHeader("rc", str);
                str = event.getAttribute("operation");
                type = event.getAttribute("type");
                if (!"query".equals(str) || "json".equalsIgnoreCase(type) ||
                    "xml".equalsIgnoreCase(type)) { // no result map to retrieve
                    ph = new HashMap<String, Object>();
                    for (String ky : event.getAttributeNames()) {
                        if (ky == null || ky.length() <= 0)
                            continue;
                        ph.put(ky, event.getAttribute(ky));
                    }
                }
                else if ((ph = (Map) event.getBody()) == null) { // query failed
                    response.sendError(response.SC_NOT_FOUND);
                    request.setAttribute("error", event.getAttribute("text"));
                    return null;
                }
                str = event.getAttribute("jsp");
                if (str != null) // overwrite the presentation jsp
                    jsp = str;
                else if (jsp == null) { // no presentation
                    if (type == null || "text".equalsIgnoreCase(type))
                        response.setContentType("text/plain");
                    else if ("json".equalsIgnoreCase(type))
                        response.setContentType("application/json");
                    else if ("xml".equalsIgnoreCase(type))
                        response.setContentType("text/xml");
                }
                // save the data map as the attribute of context
                target = request.getContextPath();
                request.setAttribute(target, ph);
            }
            else {
                response.sendError(response.SC_GATEWAY_TIMEOUT);
                request.setAttribute("error", "failed to get response: " + i);
                return null;
            }
        }

        if (isCollectible && "query".equals(action)) {
            Map ph = (Map) request.getAttribute(target);
            JSON2Map.flatten(ph);
            if ((service.getDebugMode() & service.DEBUG_REPT) > 0)
                new Event(Event.DEBUG, name + " sent back to " + username +
                    " with requested content: "+ (String)ph.get("text")).send();
        }
        else if ((service.getDebugMode() & service.DEBUG_REPT) > 0) {
            Map ph = (Map) request.getAttribute(target);
            new Event(Event.DEBUG, name + " sent back to " + username +
                " with requested content: " + (String) ph.get("text")).send();
        }

        return (jsp != null) ? jsp : target;
    }

    private String getContent(String uri, HttpServletRequest request,
        HttpServletResponse response) throws IOException {
        if (uri.charAt(1) == 'g') { // for get
            if (uri.charAt(4) == 'C') { // for child
                if ("/getChildJSON.jsp".equals(uri)) {
                    response.setContentType("application/json");
                    return getChild(request, Utils.RESULT_JSON);
                }
                else if ("/getChildXML.jsp".equals(uri)) {
                    response.setContentType("text/xml");
                    return getChild(request, Utils.RESULT_XML);
                }
                else if ("/getChildText.jsp".equals(uri)) {
                    response.setContentType("text/plain");
                    return getChild(request, Utils.RESULT_TEXT);
                }
            }
            else if ("/getJSON.jsp".equals(uri)) {
                response.setContentType("application/json");
                return getMsg(request, Utils.RESULT_JSON);
            }
            else if ("/getXML.jsp".equals(uri)) {
                response.setContentType("text/xml");
                return getMsg(request, Utils.RESULT_XML);
            }
            else if ("/getText.jsp".equals(uri)) {
                response.setContentType("text/plain");
                return getMsg(request, Utils.RESULT_TEXT);
            }
        }
        else if (uri.charAt(1) == 't') { // for to
            if (uri.charAt(3) == 'R') { // for rc
                if ("/toRCJSON.jsp".equals(uri)) {
                    response.setContentType("application/json");
                    return toRC(request, Utils.RESULT_JSON);
                }
                else if ("/toRCXML.jsp".equals(uri)) {
                    response.setContentType("text/xml");
                    return toRC(request, Utils.RESULT_XML);
                }
                else if ("/toRCText.jsp".equals(uri)) {
                    response.setContentType("text/plain");
                    return toRC(request, Utils.RESULT_TEXT);
                }
            }
            else if ("/toJSON.jsp".equals(uri)) {
                response.setContentType("application/json");
                return toMsg(request, Utils.RESULT_JSON);
            }
            else if ("/toXML.jsp".equals(uri)) {
                response.setContentType("text/xml");
                return toMsg(request, Utils.RESULT_XML);
            }
            else if ("/toText.jsp".equals(uri)) {
                response.setContentType("text/plain");
                return toMsg(request, Utils.RESULT_TEXT);
            }
        }
        else if (welcomeURL.equals(uri)) { // for welcome.jsp
            response.sendError(response.SC_OK);
            return null;
        }
        else if (loginURL.equals(uri)) { // for login.jsp
            response.sendError(response.SC_OK);
            return null;
        }
        response.sendError(response.SC_NOT_FOUND);

        return null;
    }

    protected synchronized boolean login(String username, String password) {
        boolean ic = false;
        LoginContext loginCtx = null;
        // moved loginContext here due to Krb5LoginModule not reusable
        try {
            loginCtx = new LoginContext(jaasLogin, jaasHandler);
        }
        catch (LoginException e) {
            new Event(Event.ERR, getServletName() +
                " failed to create LoginContext for " +
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
            new Event(Event.WARNING, getServletName() + ": login failed for " +
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

    private String toMsg(HttpServletRequest req, int type) {
        Map data = null;
        String msg = null, key = req.getContextPath();
        data = (Map) req.getAttribute(key);
        if (data == null || data.size() <= 0) {
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = "{\n  \"success\":false,\n  \"errors\":{\"title\":\"" +
                    key+ "\"},\n  \"errormsg\":\"" +
                    (String) req.getAttribute("error") + "\"\n}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = "<msg><Name>" + key + "</Name><Error>" +
                    (String) req.getAttribute("error") + "</Error></msg>";
            }
            else {
                msg = (String) req.getAttribute("error");
            }
        }
        else { // copy the items over
            Iterator iter = data.keySet().iterator();
            String line;
            StringBuffer strBuf = new StringBuffer();
            if ((type & Utils.RESULT_JSON) > 0) {
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    line = (String) data.get(key);
                    if (line == null)
                        line = "";
                    if (strBuf.length() <= 0)
                        strBuf.append("    \"" + key + "\":\"" + line + "\"");
                    else
                        strBuf.append(",\n    \"" + key + "\":\"" + line +"\"");
                }
                msg = "{\n  \"success\":true,\n  \"data\":{\n" +
                    strBuf.toString()+ "\n  }\n}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    line = (String) data.get(key);
                    if (line == null)
                         line = "";
                    strBuf.append("<" + key + ">" + line + "</" + key + ">\n");
                }
                msg = "<msg>\n" + strBuf.toString() + "</msg>";
            }
            else {
                msg = (String) data.get("text");
            }
        }
        return msg;
    }

    private String toRC(HttpServletRequest req, int type) {
        Map data = null;
        String msg = null, key = req.getContextPath();
        data = (Map) req.getAttribute(key);
        if (data == null || !data.containsKey("name")) {
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = "{\n  \"success\":false,\n  \"errors\":{\"title\":\"" +
                    key+ "\"},\n  " + "\"errormsg\":\"" +
                    (String) req.getAttribute("error") + "\"}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = "<msg><Name>" + key + "</Name><Error>" +
                    (String) req.getAttribute("error") + "</Error></msg>";
            }
            else {
                msg = (String) req.getAttribute("error");
            }
        }
        else { // retrieve the content
            String line = (String) data.get("ReturnCode");
            if (line == null)
                line = "";
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = "{\n  \"success\":true,\n  \"data\":{\n   \"name\":\"" +
                    (String) data.get("name") + "\",\n    \"rc\":\"" + line +
                    "\"}\n}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = "<msg><Name>" + (String) data.get("name") + "</Name>" +
                    "<ReturnCode>" + line + "</ReturnCode></msg>";
            }
            else {
                msg = line;
            }
        }
        return msg;
    }

    private String getMsg(HttpServletRequest req, int type) {
        Map data = null;
        String msg = null, key = req.getContextPath();
        data = (Map) req.getAttribute(key);
        if (data == null || !data.containsKey("text")) {
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = "{\n  \"success\":false,\n  \"errors\":{\"title\":\"" +
                    key+ "\"},\n  " + "\"errormsg\":\"" +
                    (String) req.getAttribute("error") + "\"}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = "<msg><Name>" + key + "</Name><Error>" +
                    (String) req.getAttribute("error") + "</Error></msg>";
            }
            else {
                msg = (String) req.getAttribute("error");
            }
        }
        else { // retrieve the content
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = (String) data.get("text");
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = (String) data.get("text");
                if (msg.indexOf("<?xml") >= 0) { // cut the first line
                  int i = msg.indexOf("?>\n");
                  if (i > 0)
                    msg = msg.substring(i+3);
                }
            }
            else {
                msg = (String) data.get("text");
            }
        }
        return msg;
    }

    private String getChild(HttpServletRequest req, int type) {
        Map data = null;
        String msg = null, key = req.getContextPath();
        data = (Map) req.getAttribute(key);
        if (data == null || !data.containsKey("text")) {
            if ((type & Utils.RESULT_JSON) > 0) {
                msg = "{\n  \"success\":false,\n  \"errors\":{\"title\":\"" +
                    key+ "\"},\n  " + "\"errormsg\":\"" +
                    (String) req.getAttribute("error") + "\"}";
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                msg = "<msg><Name>" + key + "</Name><Error>" +
                    (String) req.getAttribute("error") + "</Error></msg>";
            }
            else {
                msg = (String) req.getAttribute("error");
            }
        }
        else { // retrieve the content
            String line = (String) data.get("text");
            if (line == null)
                line = "";
            if ((type & Utils.RESULT_JSON) > 0) {
                int i = line.indexOf("[{");
                int j = org.qbroker.json.JSON2Map.locate(2, line);
                if (i > 0 && j > i) {
                    msg = "{\n  \"success\":true,\n  \"data\":" +
                        line.substring(i+1,j+1) + "\n}\n";
                }
                else { // wrong data
                    line = (line.length() > 60) ? line.substring(0, 60) : line;
                    msg = "{\"success\":false,\"errors\":{\"title\":\"" + key +
                        "\"}," + "\"errormsg\":\"" +
                        line.replaceAll("\"", "'").replaceAll("\n", "") + "\"}";
                }
            }
            else if ((type & Utils.RESULT_XML) > 0) {
                int i = line.indexOf("<Record type=\"ARRAY\">");
                int j = line.indexOf("</Record>");
                msg = "<msg>" + line.substring(i+21, j) + "</msg>";
            }
            else {
                int i = line.indexOf("\n");
                msg = (i > 0) ? line.substring(0, i) : line;
            }
        }
        return msg;
    }

    public static void main(String[] args) {
        byte[] buffer = new byte[4096];
        String path = null, context = "echo";
        EchoService service = null;
        MessageServlet servlet = null;
        JettyServer jettyServer = null;
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
            if (!context.endsWith("/"))
                context += "/";
            service = new EchoService();
            if (debug != 0)
                service.setDebugMode(debug);
            jettyServer = new JettyServer(ph);
            servlet = new MessageServlet(ph);
            servlet.setService(service);
            jettyServer.addContext(servlet, context + "*");
            service.start();
            jettyServer.start();
            System.out.println("Server started. Please run the following command to test:");
            System.out.println("Enter Ctrl+C to stop the server");
            jettyServer.join();
            if (service != null)
                service.stop();
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ex) {
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            if (service != null)
                service.stop();
            if (jettyServer != null) try {
                jettyServer.stop();
            }
            catch (Exception ex) {
            }
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ex) {
            }
        }
        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("MessageServlet Version 1.0 (written by Yannan Lu)");
        System.out.println("MessageServlet: A JettyServer embedded in EchoService for testing");
        System.out.println("Usage: java org.qbroker.jms.MessageServlet -I cfg.json -c context -d debug");
    }
}
