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
import org.qbroker.jms.MessageServlet;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.MapEvent;
import org.qbroker.jms.ObjectEvent;
import org.qbroker.node.NodeUtils;
import org.qbroker.flow.QFlow;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * QServlet is a servlet as an HTTP gateway of QFlow. It hosts a QFlow
 * instance running inside the servlet container as a web application. As a
 * web application, QFlow can receive JMS messages directly from web clients
 * and sends messages back on HTTP via the servlet. Therefore, QServlet is
 * the frontend and one of the reveiver of QFlow.  It is responsible for
 * transformation between the HTTP requests/responses and JMS messages.
 *<br/><br/>
 * QServlet has 6 rulesets referenced by the URL path. The first one is /jms
 * for receiving event postables as the JMS TextEvents. The second is
 * /collectible for receiving event postables from the client and transforming
 * them to the collectible JMS TextEvents. The next is /event for receiving
 * event postables as the non-JMS events. The pre-configured RestURI is to
 * convert simple REST requests to JMS TextEvents. In this ruleset, the path
 * of /json or /xml is for posted JSON or XML data from a client. The path of
 * /stream is for downloading binary content from JDBC datasources, The rest
 * of paths are for ad hoc HTML form requests. When a web request hits one of
 * the URLs, the corresponding ruleset will be invoked to process the incoming
 * request. QServlet supports POST, GET and PUT methods. For POST or PUT, those
 * headers matched with HeaderRegex will be copied into the message. One of
 * examples with HeaderRegex is "^[Xx]-.+$" that matches all HTTP headers of
 * starting with "X-". In case of POST, it also supports file upload or raw
 * content such as xml or json.
 *<br/><br/>
 * A collectible TextEvent is a TextEvent with its message body set to the
 * collectible format of the original message.  It is supposed to be forwarded
 * to remote destinations in the format of text. In order to get content of
 * the original message, the consumer will have to parse the message body with
 * the EventParser.
 *<br/><br/>
 * For ad hoc form requests, MessageServlet treats them in two different ways.
 * If the attribute of view is defined in the request and it is not empty, the
 * request will be converted into a TextEvent. Otherwise, if the attribute of
 * name is defined and it is not empty, the request will be converted to an
 * Event. If neither view nor name is defined, the ad hoc request will be
 * dropped as a bad request. Further more, if the attribute of URI is defined
 * and it is not empty, the request Event will be packed into collectible.
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

public class QServlet extends MessageServlet {
    private String cfgDir;
    private String configFile;
    private String baseURI;
    private QFlow qf = null;
    private Thread manager = null;
    private int baseURILen = 0;

    private final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    @SuppressWarnings("unchecked")
    /** initializes the servlet by the webapps container */
    public void init() throws ServletException {
        int i, n;
        Object o;
        List list;
        Map props;
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

        if ((o = props.get("OpenSSLPlugin")) != null)
            System.setProperty("OpenSSLPlugin", (String) o);

        if ((o = props.get("PluginPasswordFile")) != null)
            System.setProperty("PluginPasswordFile", (String) o);

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

        zonedDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

        manager = new Thread(qf, "Manager");
        manager.setPriority(Thread.NORM_PRIORITY);
        manager.start();
        setService(qf);
    }

    public void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
        String uri = null;
        Object o;
        RequestDispatcher rd = null;

        uri = processRequest(request, response, null);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            if (rd != null)
                rd.forward(request, response);
            else {
               response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                response.getWriter().println("no dispatcher for: " + uri);
                new Event(Event.ERR, "no dispatcher for: " + uri).send();
            }
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
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
            if (in == null) // empty content
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
                new Event(Event.ERR, "failed to read raw content: " +
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
                        if ((qf.getDebugMode() & QFlow.DEBUG_UPDT) > 0)
                            new Event(Event.DEBUG, name + ": " + key).send();
                        key += ". You may need to refresh to see the change.";
                        response.getWriter().println(key);
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
            else { // empty list for file upload
               response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
               response.setContentType("text/plain");
               response.getWriter().println("no file uploaded");
               return;
            }
        }
        catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            new Event(Event.ERR, "file upload failed: " +
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
                    key.equalsIgnoreCase("Content-Type"))
                    continue;
                if (key.matches(headerRegex)) {
                    str = request.getHeader(key);
                    if (str != null && str.length() > 0)
                        event.setAttribute(key, str);
                }
            }
        }
        // other post requests such as forms will have event = null
        uri = processRequest(request, response, event);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            if (rd != null)
                rd.forward(request, response);
            else {
               response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                response.getWriter().println("no dispatcher for: " + uri);
                new Event(Event.ERR, "no dispatcher for: " + uri).send();
            }
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
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
                str = request.getRequestURI();
                int i = str.indexOf(baseURI);
                event.setAttribute("path", str.substring(baseURILen + i - 1));
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, "failed to read raw content: " +
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
            if (rd != null)
                rd.forward(request, response);
            else {
               response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                response.getWriter().println("no dispatcher for: " + uri);
                new Event(Event.ERR, "no dispatcher for: " + uri).send();
            }
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
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
        RequestDispatcher rd = null;

        uri = processRequest(request, response, null);

        if (uri != null && uri.lastIndexOf(".jsp") > 0) {
            rd = request.getRequestDispatcher(uri);
            if (rd != null)
                rd.forward(request, response);
            else {
               response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                response.getWriter().println("no dispatcher for: " + uri);
            }
        }
        else if (uri != null && (o = request.getAttribute(uri)) != null) {
            String text = (String) ((Map) o).get("text");
            request.removeAttribute(uri);
            response.getWriter().println(text);
        }
        else if ((o = request.getAttribute("error")) != null) {
            response.setContentType("text/plain");
            response.getWriter().println((String) o);
        }
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
