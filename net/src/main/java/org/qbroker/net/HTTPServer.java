package org.qbroker.net;

/* HTTPServer.java - an interface for a generic HTTP server */

import java.util.Map;
import org.qbroker.common.Service;

public interface HTTPServer {
    /**
     * It creates the handler or servlet with the given property map and sets
     * the service on the newly created object. Then it adds the object at the
     * given path to the context
     */
    public void setService(String path, Map props, Service service);
    /** adds the given handler at the given path to the context */
    public void addContext(Object obj, String path);
    /** starts the server with the given service */
    public void start() throws Exception;
    /** stops the server */
    public void stop() throws Exception;
    /** joins the thread of the server */
    public void join() throws InterruptedException ;
    /** returns the name of the server */
    public String getName();
}
