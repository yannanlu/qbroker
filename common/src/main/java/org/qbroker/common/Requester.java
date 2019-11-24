package org.qbroker.common;

/* Requester.java - an Interface for genertic requests */

import org.qbroker.common.Connector;
import org.qbroker.common.WrapperException;

/**
 * Requester is an interface that sends the request to a generic service and
 * waits for the response. The response is always a JSON text.
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface Requester extends Connector {
    /**
     * It sends the request to the destination and fills in the response to the
     * proivided string buffer with JSON content. It returns number of items in
     * the response buffer upon success or -1 for minor failure. In case of
     * fatal failures, it throws WrapperException with cause set.
     */
    public int getResponse(String request, StringBuffer responseBuffer,
        boolean autoDisconnect) throws WrapperException;
}
