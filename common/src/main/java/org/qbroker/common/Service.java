package org.qbroker.common;

/* Service.java - an Interface for a generic service */

import org.qbroker.common.Event;

/**
 * Service is an interface that processes requests stored in Event and returns
 * the Event back as the response.
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface Service {
    /**
     * process a request in Event, loads the response into the Event and
     * returns 1 for success, 0 for timed out and -1 for failure
     */
    public int doRequest(Event request, int timeout);
    /** returns the string of the operation */
    public String getOperation();
    /** returns the name of the service */
    public String getName();
    /** returns the status of the service */
    public int getStatus();
    /** returns the debug mode of the service */
    public int getDebugMode();
    /** sets the debug mode of the service */
    public void setDebugMode(int debug);
    /** closes all opened resources */
    public void close();

    public final static int SERVICE_READY = 0;
    public final static int SERVICE_RUNNING = 1;
    public final static int SERVICE_RETRYING = 2;
    public final static int SERVICE_PAUSE = 3;
    public final static int SERVICE_STANDBY = 4;
    public final static int SERVICE_DISABLED = 5;
    public final static int SERVICE_STOPPED = 6;
    public final static int SERVICE_CLOSED = 7;
    public final static int DEBUG_NONE = 0;
    public final static int DEBUG_INIT = 1;
    public final static int DEBUG_REPT = 2;
    public final static int DEBUG_DIFF = 4;
    public final static int DEBUG_UPDT = 8;
    public final static int DEBUG_CTRL = 16;
    public final static int DEBUG_LOAD = 32;
    public final static int DEBUG_LOOP = 64;
    public final static int DEBUG_TRAN = 128;
    public final static String statusText[] = {"READY", "RUNNING",
        "RETRYING", "PAUSE", "STANDBY", "DISABLED", "STOPPED", "CLOSED"};
}
