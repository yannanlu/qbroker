package org.qbroker.net;

/* HTTPServer.java - an interface for a generic HTTP server */

import org.qbroker.common.Service;

public interface HTTPServer {
    /** starts the server with the given service */
    public void start(Service service) throws Exception;
    /** stops the server */
    public void stop() throws Exception;
    /** joins the thread of the server */
    public void join() throws InterruptedException ;
    /** returns the name of the server */
    public String getName();
}
