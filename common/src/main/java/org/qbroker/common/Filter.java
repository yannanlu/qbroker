package org.qbroker.common;

/**
 * Filter is an Interface for filtering on a generic object with a give pattern
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface Filter<T> {
    /** returns true if the filter matching with the event or false otherwise */
    public boolean evaluate(long currentTime, T obj);
}
