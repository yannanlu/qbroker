package org.qbroker.event;

import org.qbroker.common.Filter;

/**
 * EventFilter is an Interface for filtering event with a give pattern
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface EventFilter extends Filter<org.qbroker.event.Event> {
    /** returns true if the filter matching with the event or false otherwise */
    public boolean evaluate(long currentTime, org.qbroker.event.Event event);
}
