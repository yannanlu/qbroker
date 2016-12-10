package org.qbroker.flow;

/* QServlet.java - a Servlet for QFlow */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
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
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItem;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.FailedLoginException;
import org.qbroker.jaas.SimpleCallbackHandler;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Base64Encoder;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.DBConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.MapEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.node.NodeUtils;
import org.qbroker.flow.QFlow;
import org.qbroker.event.EventParser;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * QServlet is a servlet of QFlow.  It allows QFlow running
 * inside the servlet container as a web application.  As a web
 * application, QFlow can receive JMS messages directly from web
 * clients and send messages back on HTTP.  Therefore, QServlet is
 * the frontend and one of the reveiver of QFlow.  It is responsible
 * for transformation between the HTTP requests/responses and JMS messages.
 *<br/><br/>
 * QServlet has 5 rulesets represented by the URL path.  They are
 * /jms for non-collectible JMS TextEvents, /collectible for collectible
 * JMS TextEvents, /event for non-JMS events, RestURI for JMS TextEvents
 * of REST requests and other paths for ad hoc form requests.  When the
 * web request hits one of the URLs, the corresponding ruleset will be
 * invoked to process the incoming request.  QServlet supports
 * both POST, GET and PUT methods. In case of POST, it also supports file
 * upload and raw xml or json content. For POST or PUT, those headers matched
 * with HeaderRegex will be copied into the message. One of the examples of
 * HeaderRegex is "^[Xx]-.+$" that matches all HTTP headers of starting with
 * "X-".
 *<br/><br/>
 * A collectible TextEvent is a TextEvent with its message body set to
 * the collectible format of the original message.  It is meant to be sent
 * to remote destinations.  In order to get content of the original
 * message, the consumer will have to parse the message body with the
 * EventParser.
 *<br/><br/>
 * For the ad hoc form requests, QServlet treats them in two
 * different ways.  If the attribute of view is defined in the request
 * and non-empty,  it will be converted into a TextEvent.  Otherwise,
 * if the attribute of name is defined and non-empty, the request will be
 * converted to an Event.  Further more, if the attribute of URI is
 * defined and non-empty, the request will be packed into collectible
 * format.  Otherwise, there is no transformation on the original message. 
 *<br/><br/>
 * Once the incoming requests is transformed into messages, they will be
 * routed to the default receiver XQueue of the QFlow.  After they are
 * processed, the messages will be retrieved as the response.  Those
 * messages will have the requested data.  Next, QServlet loads
 * the messages into the original request and forwards to the presentation
 * JSP.  The presentation JSP is either defined in the incoming request or
 * reset in the returned message.  It is used to retrieve data from the
 * request and render the web page for the client.  If there is no
 * presentation JSP defined, QServlet will just set the content type
 * according to the type attribute of the message and write the content
 * of the message body to the output stream.  The attribute of type can
 * be defined in the incoming request.  It also can be overwritten by
 * the workflow.  This is useful if the content is already a valid
 * web page and you do not want to modify it.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QServlet extends HttpServlet {
    private String cfgDir;
    private String configFile;
    private String name;
    private String loginURL, welcomeURL, baseURI;
    private int timeout = 10000;
    private QFlow qf = null;
    private Map props = new HashMap();
    private Thread manager = null;
    private SimpleDateFormat zonedDateFormat;
    private EventParser parser;
    private DiskFileItemFactory factory = null;
    private String jaasLogin = null;
    private String restURI = null;
    private String headerRegex = null;
    private SimpleCallbackHandler jaasHandler = null;
    private String[] propertyName, propertyValue;
    private MessageDigest md = null;
    private int restURILen = 0;
    private int baseURILen = 0;

    protected final static String statusText[] = {"READY", "RUNNING",
        "RETRYING", "PAUSE", "STANDBY", "DISABLED", "STOPPED", "CLOSED"};
    protected final static String reportStatusText[] = {"Exception",
        "ExceptionInBlackout", "Disabled", "Blackout", "Normal",
        "Occurred", "Late", "VeryLate", "ExtremelyLate"};
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    @SuppressWarnings("unchecked")
    public void init() throws ServletException {
        int i, n;
        Object o;
        List list;
        String loggerName, reportName, jaasConfig;
        ServletConfig config = getServletConfig();
        configFile = config.getInitParameter("ConfigFile");
        loggerName = config.getInitParameter("LoggerName");
        reportName = config.getInitParameter("ReportName");
        jaasConfig = config.getInitParameter("JAASConfig");
        jaasLogin = config.getInitParameter("JAASLogin");
        loginURL = config.getInitParameter("LoginURL");
        welcomeURL = config.getInitParameter("WelcomeURL");
        restURI = config.getInitParameter("RestURI");
        headerRegex = config.getInitParameter("HeaderRegex");
        if (configFile == null)
            throw(new ServletException("ConfigFile not defined"));
        if (restURI != null)
            restURILen = restURI.length();
        if (loggerName == null)
            loggerName = "QFlow";
        if (loginURL == null && jaasLogin != null)
            loginURL = "/login.jsp";
        if (welcomeURL == null)
            welcomeURL = "/welcome.jsp";
        if ((o = config.getInitParameter("Timeout")) != null &&
            o instanceof String)
            timeout = 1000 * Integer.parseInt((String) o);

        if (timeout <= 0)
            timeout = 10000;

        if (System.getProperty("LoggerName") == null)
            System.setProperty("LoggerName", loggerName);

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
            throw(new ServletException("failed to init MD5: " + e.toString()));
        }

        try {
            FileReader fr = new FileReader(configFile);
            props = (Map) JSON2Map.parse(fr);
            fr.close();

            // init Event logging
            String datePattern = null;
            if ((o = props.get("LogDatePattern")) != null)
                datePattern = (String) o;

            if ((o = props.get("LogDir")) != null)
                Event.setLogDir((String) o, datePattern);
            else
                Event.setLogDir("/var/log/qbroker", datePattern);
        }
        catch (Exception e) {
            throw(new ServletException("failed to load configuration from " +
                configFile + ": " + Event.traceStack(e)));
        }

        if ((o = props.get("Name")) == null) {
            new Event(Event.ERR, "Name is not defined in " + configFile).send();
            throw(new ServletException("Name is not defined in " +
                configFile));
        }
        name = (String) o;
        Event.setDefaultName(name);

        if ((o = props.get("ConfigDir")) == null) {
            new Event(Event.ERR, name + " ConfigDir is not defined in " +
                configFile).send();
            throw(new ServletException(name + " ConfigDir is not defined in "+
                configFile));
        }
        cfgDir = (String) o;

        File file = new File(cfgDir);
        if (!file.exists() || !file.isDirectory() || !file.canRead()) {
            new Event(Event.ERR, name + " failed to open " + cfgDir).send();
            throw(new ServletException(name + " failed to open " + cfgDir));
        }

        // ConfigRepository
        n = 0;
        if ((o = props.get("ConfigRepository")) != null && o instanceof String){
            String key = (String) o;
            file = new File(cfgDir + FILE_SEPARATOR + key + ".json");
            if (file.exists() && file.isFile() && file.canRead()) try {
                FileReader fr = new FileReader(file);
                o = JSON2Map.parse(fr);
                fr.close();
                if (o == null || !(o instanceof Map)) {
                    new Event(Event.ERR, "empty property file for "+key).send();
                }
                else {
                    props.put(key, o);
                    n ++;
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to load the config file for "+
                    key + ": " + Event.traceStack(e)).send();
            }
        }

        list = (List) props.get("Reporter");
        if (list == null)
            list = new ArrayList();
        n = MonitorAgent.loadListProperties(list, cfgDir, 0, props);

        list = (List) props.get("Receiver");
        if (list == null)
            list = new ArrayList();
        n = MonitorAgent.loadListProperties(list, cfgDir, 0, props);

        list = (List) props.get("Node");
        if (list == null)
            list = new ArrayList();
        n = MonitorAgent.loadListProperties(list, cfgDir, 0, props);

        // merge 2nd includes only on MessageNodes
        if ((o = props.get("IncludePolicy")) != null && o instanceof Map)
            MonitorAgent.loadIncludedProperties(list, cfgDir, 0, props, (Map)o);

        list = (List) props.get("Persister");
        if (list == null)
            list = new ArrayList();
        n = MonitorAgent.loadListProperties(list, cfgDir, 0, props);

        try {
            qf = new QFlow(props);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " failed to be inintialized: " +
                Event.traceStack(e)).send();
            throw(new ServletException(name+" failed to instantiate QFlow: " +
                Event.traceStack(e)));
        }

        if (reportName != null) {
            Map<String, Object> report = QFlow.getReport(reportName);
            if (report == null || report.size() <= 3) {
                new Event(Event.ERR, name + " empty report: " +
                    reportName).send();
                propertyName = new String[0];
                propertyValue = new String[0];
            }
            else {
                n = report.size();
                propertyName = new String[n-3];
                propertyValue = new String[n-3];
                n = 0;
                for (String key : report.keySet()) {
                    if ("Name".equals(key) || "Status".equals(key) ||
                        "TestTime".equals(key))
                        continue;
                    propertyName[n] = key;
                    propertyValue[n] = (String) report.get(key);
                    n ++;
                }
                if (n == 0) {
                    propertyName = new String[0];
                    propertyValue = new String[0];
                }
            }
        }
        else {
            propertyName = new String[0];
            propertyValue = new String[0];
        }

        baseURI = "/" + getServletName() + "/";
        baseURILen = baseURI.length();
        factory = new DiskFileItemFactory();
        new Event(Event.INFO, name + " inintialized").send();

        parser = new EventParser(null);
        zonedDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        manager = new Thread(qf, "Manager");
        manager.setPriority(Thread.NORM_PRIORITY);
        manager.start();
    }

    public void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null;
        Object o;
        RequestDispatcher rd = null;

        uri = processRequest(request, response, null);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            rd.forward(request, response);
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().print(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().print((String) o);
        }
    }

    public void doPost(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null, path;
        RequestDispatcher rd = null;
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
            if (in != null) try {
                StringBuffer strBuf = new StringBuffer();
                while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        strBuf.append(new String(buffer, 0, bytesRead));
                }
                event = new TextEvent(strBuf.toString());
                event.setAttribute("type", str);
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to read raw content: " +
                    Event.traceStack(e)).send();
                response.setContentType("text/plain");
                response.getWriter().print("failed to read raw content: " +
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
                            response.getWriter().print("please login first");
                            return;
                        }
                        key = (String) session.getAttribute("username");
                        event.setAttribute("login", key);
                    }
                    key = event.getAttribute("name");
                    for (i=0; i<propertyName.length; i++)
                        event.setAttribute(propertyName[i], propertyValue[i]);
                    i = qf.doRequest(event, (int) tm);
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
                        if (str != null || str.length() > 0) {
                            ph.put("Username", str);
                            ph.put("Password", event.getAttribute("Password"));
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
                        if ((qf.getDebugMode() & QFlow.DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name + ": " + key).send();
                        key += ". You may need to refresh to see the change.";
                        response.getWriter().print(key);
                    }
                    catch (Exception ee) {
                        if (conn != null) try {
                            conn.close();
                        }
                        catch (Exception ex) {
                        }
                        new Event(Event.ERR, "failed to upload " + len +
                           " bytes for " + event.getAttribute("name") + ": " +
                           Event.traceStack(ee)).send();
                        response.sendError(response.SC_INTERNAL_SERVER_ERROR);
                    }
                    return;
                }
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, "file upload failed: " +
                Event.traceStack(e)).send();
            response.setContentType("text/plain");
            response.getWriter().print("file upload failed: " +
                Event.traceStack(e));
            return;
        }
        if (event != null && headerRegex != null) { // copy the headers over
            String key;
            Enumeration headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                key = (String) headerNames.nextElement();
                if (key == null || key.equals("null") ||
                    key.equalsIgnoreCase("Content-Type"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = request.getHeader(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(request, response, event);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            rd.forward(request, response);
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().print(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().print((String) o);
        }
    }

    public void doPut(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null, path, key, str;
        RequestDispatcher rd = null;
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
        if (in != null) try {
            event = new BytesEvent();
            while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                if (bytesRead > 0)
                    event.writeBytes(buffer, 0, bytesRead);
                size += bytesRead;
            }
            event.reset();
            event.setAttribute("type", request.getContentType());
            event.setAttribute("method", request.getMethod());
            event.setAttribute("size", String.valueOf(size));
            str = request.getRequestURI();
            int i = str.indexOf(baseURI);
            event.setAttribute("path", str.substring(baseURILen + i - 1));
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to read raw content: " +
                Event.traceStack(e)).send();
            response.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT);
            response.setContentType("text/plain");
            response.getWriter().print("failed to read raw content: " +
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
                    key.equalsIgnoreCase("Content-Type"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = request.getHeader(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        uri = processRequest(request, response, event);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            rd.forward(request, response);
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().print(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            response.getWriter().print((String) o);
        }
    }

    public void doDelete(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null;
        Object o;
        RequestDispatcher rd = null;

        uri = processRequest(request, response, null);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            rd.forward(request, response);
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().print(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().print((String) o);
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
        catch (Exception e) {
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
        if (priority < 0 || text == null ||
            !(key != null || site != null || category != null))
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
        catch (Exception e) {
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
    private String processRequest(HttpServletRequest request,
        HttpServletResponse response, Event event)
        throws ServletException, IOException {
        Object o;
        String key = null, uri = null, msg = null, str = null, port = null;
        String target = null, action = null, category = null, jsp = null;
        String clientIP, path, sessionId, status, username = null, type=null;
        int l, priority = Event.INFO;
        long tm = 0L;
        boolean isJMS = false;        // an event is a JMS event or not
        boolean isCollectible = true; // a non-JMS event is collectible or not
        boolean isFileUpload = false;
        boolean isStream = false;
        Map props = request.getParameterMap();
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
                response.getWriter().print("Thanks,your session is terminated");
                return null;
            }
            catch (Exception e) {
                response.setContentType("text/plain");
                response.getWriter().print("Sorry, " + e.toString());
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
                qf.doRequest(message, timeout);
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
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " with event: " + path).send();
            isJMS = true;
            isCollectible = false;
            str = request.getContentType();
            if ("POST".equals(request.getMethod()) &&
                !"text/xml".equals(str) && !"application/json".equals(str)) {
                isFileUpload = true;
                if (event.attributeExists("jsp"))
                    jsp = event.getAttribute("jsp");
            }
            else try { // raw request from POST or PUT
                isFileUpload = false;
                event.setAttribute("_clientIP", clientIP);
                ((JMSEvent) event).setJMSType(path);
            }
            catch (Exception e) {
            }
        }
        else if (path.startsWith("/collectible") && (l == 12 ||
            path.charAt(13) == '/')) { //JMS event for collectibles
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " collectible: " + path).send();
            isCollectible = true;
            isJMS = true;
            event = getEvent(props, true);
            if (event == null) { // null event
                response.sendError(response.SC_BAD_REQUEST);
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
            path.charAt(7) == '/')) { // non-JMS event only
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " event: " + path).send();
            isJMS = false;
            isCollectible = false;
            event = getEvent(props, false);
            if (event == null) { // null event
                response.sendError(response.SC_BAD_REQUEST);
                return null;
            }
            event.removeAttribute("text");
            if (event.getAttribute("status") == null)
                event.setAttribute("status", "Normal");
            if ((uri = event.getAttribute("URI")) == null || uri.length() <= 0)
                isCollectible = false;
            else {
                isCollectible = true;
                port = event.getAttribute("Port");
                if (port == null || port.length() <= 0)
                    port = null;
            }
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
            event.setAttribute("hostname", clientIP);
            action = event.getAttribute("operation");
            category = event.getAttribute("category");
        }
        else if (path.startsWith("/jms") && (l == 4 ||
            path.charAt(5) == '/')) { // JMS event without collectibles
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " jms: " + path).send();
            isJMS = true;
            isCollectible = false;
            event = getEvent(props, true);
            if (event == null) { // null event
                response.sendError(response.SC_BAD_REQUEST);
                return null;
            }
            event.setAttribute("hostname", clientIP);
            if (event.attributeExists("jsp"))
                jsp = event.getAttribute("jsp");
        }
        else if (path.startsWith("/stream") && (l == 7 ||
            path.charAt(8) == '/')) { //JMS event for stream operations
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " stream: " + path).send();
            isJMS = true;
            isCollectible = false;
            isStream = true;
            event = getEvent(props, true);
            event.setAttribute("hostname", clientIP);
        }
        else if (restURILen > 0 && path.startsWith(restURI) && (l==restURILen ||
            path.charAt(restURILen+1) == '/')) { // REST requests
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " rest: " + path).send();
            Iterator iter = props.keySet().iterator();
            isJMS = true;
            isCollectible = false;
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
            if ("STREAM".equals(event.getAttribute("type"))) // for stream
                isStream = true;
        }
        else if ("POST".equals(request.getMethod()) && // xml or json form data
            ("/xml".equals(path) || "/json".equals(path))) {
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " raw data: " + path).send();
            if ((o = props.get(path.substring(1))) != null)
                str = ((String[]) o)[0];
            else
                str = "";
            isJMS = true;
            isCollectible = false;
            event = new TextEvent(str);
            event.setAttribute("hostname", clientIP);
            event.setAttribute("type",
                (("/xml".equals(path)) ? "text" : "application") + path);
            event.setAttribute("path", path);
        }
        else { // ad hoc form request
            if ((qf.getDebugMode() & QFlow.DEBUG_CTRL) > 0)
                new Event(Event.DEBUG, name + " ad hoc: " + path).send();
            isJMS = false;
            isCollectible = false;
            if ((o = props.get("jsp")) != null)
                jsp = ((String[]) o)[0];
            if (jsp != null && jsp.length() <= 0)
                jsp = null;

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

            if ((o = props.get("Port")) != null)
                port = ((String[]) o)[0];
            if (port == null || port.length() <= 0)
                port = null;

            if ((o = props.get("category")) != null)
                category = ((String[]) o)[0];
            if (category == null || category.length() <= 0)
                category = "QBROKER";

            if ((o = props.get("type")) != null)
                type = ((String[]) o)[0];
            if (type == null || type.length() <= 0)
                type = "TEXT";

            if ((o = props.get("URI")) != null)
                uri = ((String[]) o)[0];
            if (uri == null || uri.length() <= 0)
                uri = null;
            else
                isCollectible = true;

            if ((o = props.get("name")) != null)
                key = ((String[]) o)[0];

            if ((o = props.get("view")) != null)
                str = ((String[]) o)[0];
            if (str != null && str.length() > 0) { // ad hoc jms event
                isJMS = true;
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
                event = new Event(priority);
                event.setAttribute("name", key);
                event.setAttribute("type", type);
                event.setAttribute("category", category);
                event.setAttribute("operation", action);
                event.setAttribute("status", "Normal");
                event.setAttribute("hostname", clientIP);
                if (uri != null) {
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
        }

        if (event != null) {
            int i;
            Map<String, Object> ph = null;
            if (jaasLogin != null)
                event.setAttribute("login", username);
            else
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
                        message.setJMSExpiration(event.getTimestamp() +
                            timeout);
                    for (i=0; i<propertyName.length; i++)
                        message.setAttribute(propertyName[i], propertyValue[i]);
                }
                catch (Exception e) {
                    event.setAttribute("text", "failed to load message: "+
                        Event.traceStack(e));
                    message = null;
                }

                if (tm <= timeout)
                    tm = timeout;
                i = -1;
                if (message != null) { //send the request and wait until timeout
                    i = qf.doRequest(message, (int) tm);
                }

                // for Stream on download
                if (i > 0 && message != null && isStream && !isFileUpload) {
                    int len = -1;
                    DBConnector conn = null;
                    if (((Event) message).attributeExists("rc"))
                        str = ((Event) message).getAttribute("rc");
                    else
                        str = ((Event) message).getAttribute("ReturnCode");
                    if (str != null && !"0".equals(str)) { // failed
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
                        if (str != null || str.length() > 0) {
                            ph.put("Username", str);
                            ph.put("Password",
                                ((Event) message).getAttribute("Password"));
                        }
                        conn = new DBConnector(ph);
                        str = ((TextEvent) message).getText();
                        len = conn.copyStream(str, out);
                        try {
                            conn.close();
                        }
                        catch (Exception ex) {
                        }
                        if ((qf.getDebugMode() & QFlow.DEBUG_UPDT) > 0)
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
                        new Event(Event.ERR, "failed to write " + len +
                            " bytes for " + key + ": " +
                            Event.traceStack(e)).send();
                        response.sendError(response.SC_INTERNAL_SERVER_ERROR);
                    }
                    return null;
                }
                else if (i > 0 && message != null) { // got result back
                    ph = new HashMap<String, Object>();
                    Iterator iter = ((Event) message).getAttributeNames();
                    while (iter.hasNext()) {
                        o = iter.next();
                        if (o == null || !(o instanceof String))
                            continue;
                        key = (String) o;
                        if ("text".equals(key))
                            continue;
                        ph.put(key, ((Event) message).getAttribute(key));
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
                        if (type == null || "text".equals(type.toLowerCase()))
                            response.setContentType("text/plain");
                        else if ("json".equals(type.toLowerCase()))
                            response.setContentType("application/json");
                        else if ("xml".equals(type.toLowerCase()))
                            response.setContentType("text/xml");
                        else if ("html".equals(type.toLowerCase()))
                            response.setContentType("text/html");
                        else if (message instanceof TextEvent)
                            response.setContentType("text/plain");
                        else
                            response.setContentType(type);
                    }
                }
                else {
                    event = new Event(Event.WARNING, "failed to copy message");
                    ph = null;
                }
            }
            else { // non-JMS event for the current QFlow
                for (i=0; i<propertyName.length; i++)
                    event.setAttribute(propertyName[i], propertyValue[i]);
                i = qf.doRequest(event, timeout);
                if (i > 0) {
                    ph = (Map) event.getBody();
                    if (event.attributeExists("rc"))
                        str = event.getAttribute("rc");
                    else
                        str = event.getAttribute("ReturnCode");
                    if (str != null) // notify client
                        response.setHeader("rc", str);
                }
                else
                    ph = null;
            }

            if (ph == null || ph.size() == 0) {
                msg = "ph is empty due to " + i + ": " +
                    event.getAttribute("text");
                request.setAttribute("error", msg);
                target = null;
            }
            else if (isCollectible && "query".equals(action)) {
                JSON2Map.flatten(ph);
                if ((qf.getDebugMode() & QFlow.DEBUG_REPT) > 0)
                    new Event(Event.DEBUG, "sent back to " + username + " with"+
                        " requested content: "+ (String) ph.get("text")).send();
            }
            else if ((qf.getDebugMode() & QFlow.DEBUG_REPT) > 0)
                new Event(Event.DEBUG, "sent back to " + username +
                    " requested content: " + (String) ph.get("text")).send();

            if (ph != null) {// save the data map as an attribute at the context
                target = request.getContextPath();
                request.setAttribute(target, ph);
            }
        }
        else { // no event for the request
            target = null;
            msg = "empty target";
            request.setAttribute("error", msg);
        }

        return (jsp != null) ? jsp : target;
    }

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
            new Event(Event.WARNING, name + " login failed for " +
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

    public void destroy() {
        if (qf != null)
            qf.close();

        super.destroy();
        if (manager != null && manager.isAlive()) try {
            manager.join();
        }
        catch (Exception e) {
        }
        new Event(Event.INFO, name + " terminated").send();
    }
}
