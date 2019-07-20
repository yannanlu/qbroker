package org.qbroker.net;

/* JettyServer.java - a wrapper of Jetty server for embedded HTTP servers */

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.qbroker.net.HTTPServer;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;

public class JettyServer extends HttpServlet implements HTTPServer {
    private String name;
    private String username = null;
    private String password = null;
    private Server httpServer;
    private ServletContextHandler context;
    private Map<String, HttpServlet> map = new HashMap<String, HttpServlet>();
    private boolean isHTTPS = false;
    private boolean trustAll = false;

    public JettyServer(Map props) {
        Object o;
        URI u;
        String ksPath = null, ksPassword = null;
        int port;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));

        try {
            u = new URI((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to parse URI: "+
                e.toString()));
        }
        if ("http".equals(u.getScheme()))
            isHTTPS = false;
        else if ("https".equals(u.getScheme())) {
            isHTTPS = true;
            if ((o = props.get("TrustAllCertificates")) != null &&
                "true".equalsIgnoreCase((String) o))
                trustAll = true;
        }
        else
            throw(new IllegalArgumentException(name + " unsupported scheme: " +
                u.getScheme()));

        port = u.getPort();
        if (port <= 0)
            port = (isHTTPS) ? 443 : 80;

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null) {
                password = (String) o;
            }
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + " failed to decrypt "+
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null || password.length() <= 0)
                throw(new IllegalArgumentException(name +
                    ": Password not defined for " + username));
        }

        if (isHTTPS) {
            if ((o = props.get("KeyStoreFile")) != null)
                ksPath = (String) o;
            else
                throw(new IllegalArgumentException(name +
                    " KeyStoreFile is not defined"));

            if ((o = props.get("KeyStorePassword")) != null)
                ksPassword = (String) o;
            else if ((o = props.get("EncryptedKeyStorePassword")) != null) try {
                ksPassword = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + " failed to decrypt "+
                    "EncryptedKeyStorePassword: " + e.toString()));
            }
            if (ksPassword == null || ksPassword.length() <= 0)
                throw(new IllegalArgumentException(name +
                    " KeySTorePassword is not defined"));
            File ks = new File(ksPath);
            if (!ks.exists() && !ks.canRead())
                throw(new IllegalArgumentException(name +
                    " can not open KeyStoreFile to read: " + ksPath));
        }

        context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        if (username == null || password == null) { // without security
            try {
                httpServer = new Server(port);
                httpServer.setHandler(context);
                httpServer.setDumpAfterStart(false);
                httpServer.setDumpBeforeStop(false);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to init Jetty server: "+
                    TraceStackThread.traceStack(e)));
            }
        }
        else try { // for secured loginService
            LoginService loginService = new HashLoginService(name);
            ConstraintSecurityHandler security= new ConstraintSecurityHandler();
            Constraint constraint = new Constraint();
            UserStore us = new UserStore();
            us.addUser(username, Credential.getCredential(password),
                 new String[]{"user", "admin"});
            ((HashLoginService) loginService).setUserStore(us);
            httpServer = new Server(port);
            httpServer.addBean(loginService);
            httpServer.setHandler(security);
            httpServer.setDumpAfterStart(false);
            httpServer.setDumpBeforeStop(false);
            constraint.setName("auth");
            constraint.setAuthenticate(true);
            constraint.setRoles(new String[] { "admin" });
            ConstraintMapping mapping = new ConstraintMapping();
            mapping.setPathSpec("/*");
            mapping.setConstraint(constraint);
            security.setConstraintMappings(Collections.singletonList(mapping));
            security.setAuthenticator(new BasicAuthenticator());
            security.setLoginService(loginService);
            security.setHandler(context);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init Jetty server with security: "+
                TraceStackThread.traceStack(e)));
        }

        if (isHTTPS) try {
            HttpConfiguration http_config = new HttpConfiguration();
            http_config.setSecureScheme("https");
            http_config.setSecurePort(port);

            HttpConfiguration https_config = new HttpConfiguration(http_config);
            https_config.addCustomizer(new SecureRequestCustomizer());

            SslContextFactory sslContextFactory = new SslContextFactory(ksPath);
            sslContextFactory.setKeyStorePassword(ksPassword);

            ServerConnector httpsConnector = new ServerConnector(httpServer, 
		new SslConnectionFactory(sslContextFactory, "http/1.1"),
		new HttpConnectionFactory(https_config));
            httpsConnector.setPort(port);
            httpsConnector.setIdleTimeout(50000);
            httpServer.setConnectors(new Connector[]{httpsConnector});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init Jetty server with SSL: "+
                TraceStackThread.traceStack(e)));
        }
    }

    public void start() throws Exception {
        httpServer.start();
    }

    public void stop() throws Exception {
        for (String key : map.keySet()) {
            HttpServlet servlet = map.get(key);
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception e) {
            }
        }
        httpServer.stop();
    }

    public void join() throws InterruptedException{
        httpServer.join();
    }

    /**
     * It creates the servlet with the given property map and sets the service
     * on the servlet. Then it adds the given servlet at the given path to
     * the context
     */
    public void setService(String path, Map props, Service service) {
        HttpServlet servlet = null;
        java.lang.reflect.Method method = null;
        String className, str = null;
        Object o;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException(name + ": props is empty"));
        else if (service == null)
            throw(new IllegalArgumentException(name + ": service is null"));
        else if ((o = props.get("ClassName")) == null)
            throw(new IllegalArgumentException(name + ": ClassName is null"));
        else try { // instantiate the servlet
            className = (String) o;
            java.lang.reflect.Constructor con;
            str = name + " failed to instantiate servlet of " + className + ":";
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            o = con.newInstance(new Object[]{props});
            if (o instanceof HttpServlet) { // servlet has instantiated
                servlet = (HttpServlet) o;
                context.addServlet(new ServletHolder(servlet), path);
                method = cls.getMethod("setService",new Class[]{Service.class});
            }
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            str += e.toString() + " with Target exception: " + "\n";
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ee) {
            }
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(ex)));
        }
        catch (Exception e) {
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ee) {
            }
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(e)));
        }

        if (servlet == null)
            throw(new IllegalArgumentException(str + " not an HttpServlet"));
        else if (method == null) {
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ee) {
            }
            throw(new IllegalArgumentException(str+" no method of setService"));
        }
        else try { // set the service on the servlet
            str = name + " failed to set servcie of " + service.getName() + ":";
            method.invoke(servlet, new Object[]{service});
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            str += e.toString() + " with Target exception: " + "\n";
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ee) {
            }
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(ex)));
        }
        catch (Exception e) {
            if (servlet != null) try {
                servlet.destroy();
            }
            catch (Exception ee) {
            }
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(e)));
        }

        servlet = map.put(path, servlet);
        if (servlet != null) try { // destroy the old servlet
            servlet.destroy();
        }
        catch (Exception ee) {
        }
    }

    /** adds the given servlet at the given path to the context */
    public void addContext(Object obj, String path) {
        if (path == null || path.length() <= 0)
            throw(new IllegalArgumentException(name + ": path is empty"));
        else if (obj == null || !(obj instanceof HttpServlet))
            throw(new IllegalArgumentException(name + ": not a servlet"));
        else
            context.addServlet(new ServletHolder((HttpServlet) obj), path);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Hello from default servlet</h1>");
    }

    public String getName() {
        return name;
    }

    protected void finalize() {
        try {
            httpServer.stop();
        }
        catch (Exception e) {
        }
    }

    public static void main(String[] args) {
        Map<String, String> props = new HashMap<String, String>();
        JettyServer server = null;
        String path = "/", uri;
        int i, port = 0;
        boolean isHTTPS = false;

        props.put("Name", "testServer");
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 's':
                isHTTPS = true;
                break;
              case 'c':
                if (i+1 < args.length)
                    path = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 'n':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'w':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              case 'f':
                if (i+1 < args.length)
                    props.put("KeyStoreFile", args[++i]);
                break;
              case 'k':
                if (i+1 < args.length)
                    props.put("KeyStorePassword", args[++i]);
                break;
              default:
                break;
            }
        }
        if (!path.startsWith("/"))
            path = "/" + path;
        if (port <= 0)
            port = (isHTTPS) ? 443 : 80;
        if (isHTTPS)
            uri = "https://localhost:" + port;
        else
            uri = "http://localhost:" + port;
        props.put("URI", uri);

        try {
            server = new JettyServer(props);
            server.addContext(server, path);
            server.start();
            System.out.println("Server started. Please run the following command to test:");
            System.out.println("curl " + uri + path);
            System.out.println("Enter Ctrl+C to stop the server");
            server.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JettyServer Version 1.0 (written by Yannan Lu)");
        System.out.println("JettyServer: a jetty server with default servlet");
        System.out.println("Usage: java org.qbroker.net.JettyServer -p port -c context");
        System.out.println("  -?: print this message");
        System.out.println("  -s: https mode");
        System.out.println("  -c: context (default: '/')");
        System.out.println("  -p: port (default: 80 or 443)");
        System.out.println("  -n: username");
        System.out.println("  -w: password");
        System.out.println("  -f: path to keystore file");
        System.out.println("  -k: password for keystore");
    }
}
