package org.qbroker.servlet;

/* JettyServer.java - an embedded HTTP server from Jetty */

import java.util.Map;
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
import org.qbroker.net.HTTPServer;
import org.qbroker.servlet.MsgServlet;

public class JettyServer implements HTTPServer {
    private String name;
    private Server httpServer;
    private MsgServlet servlet;

    public JettyServer(Map props) {
        Object o;
        URI u;
        int port;
        ServletContextHandler context;
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
        context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        try {
            httpServer = (port <= 0) ? new Server(80) : new Server(port);
            httpServer.setHandler(context);
            httpServer.setDumpAfterStart(false);
            httpServer.setDumpBeforeStop(false);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name+" failed to init server: "+
                e.toString()));
        }
        try {
            servlet = new MsgServlet(props);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name+" failed to init servlet: "+
                e.toString()));
        }
        context.addServlet(new ServletHolder(servlet), "/" + name + "/*");
    }

    public void start(Service service) throws Exception {
        if (service == null)
            throw(new IllegalArgumentException(name +": null service"));
        servlet.setService(service);
        httpServer.start();
    }

    public void stop() throws Exception {
        httpServer.stop();
    }

    public void join() throws InterruptedException{
        httpServer.join();
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
        try {
            servlet.destroy();
        }
        catch (Exception e) {
        }
    }
}
