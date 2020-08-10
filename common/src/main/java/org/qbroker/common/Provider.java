package org.qbroker.common;

/* Provider.java - an Interface for genertic data providers */

import org.qbroker.common.Connector;
import org.qbroker.common.WrapperException;

/**
 * Provider is an interface that gets the result from a generic service.
 * The ressult is always a JSON text.
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface Provider extends Connector {
    /**
     * It gets the result from a data source and fills in the result to the
     * proivided string buffer with JSON content. It returns 1 upon success,
     * 0 for no result or -1 for minor failure. In case of fatal failures,
     * it throws WrapperException with cause set.
     */
    public int getResult(StringBuffer resultBuffer, boolean autoDisconnect)
        throws WrapperException;
}
