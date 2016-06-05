package org.qbroker.common;

/* Connector.java - an Interface for operations on a generic connection */

public interface Connector {
    /** reconnects and returns null upon success or error msg otherwise */
    public String reconnect();
    public boolean isConnected();
    public String getURI();
    public void close();
}
