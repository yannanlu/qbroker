package org.qbroker.net;

/* JettyServer.java - a wrapper of Jetty server for embedded HTTP servers */

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.qbroker.common.Service;

public class JettyServer extends HttpServlet {
    private String name;
    private Server httpServer;
    private ServletContextHandler context;

    public JettyServer(Map props) {
        Object o;
        URI u;
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
        port = u.getPort();
        if (port <= 0)
            port = 80;
        context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        try {
            httpServer = new Server(port);
            httpServer.setHandler(context);
            httpServer.setDumpAfterStart(false);
            httpServer.setDumpBeforeStop(false);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init Jetty server: "+ e.toString()));
        }
    }

    public void start() throws Exception {
        httpServer.start();
    }

    public void stop() throws Exception {
        httpServer.stop();
    }

    public void join() throws InterruptedException{
        httpServer.join();
    }

    public void addServlet(HttpServlet servlet, String path) {
        context.addServlet(new ServletHolder(servlet), path);
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
            server.addServlet(server, path);
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
    }
}
