package org.qbroker.event;

/**
 * EventMerger is an Interface for a merger that merges events
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface EventMerger {
    /** merges the set of events into a new event */
    public org.qbroker.event.Event merge(long currentTime,
        org.qbroker.event.Event[] events);
    /** returns the name of the merger */
    public String getName();
    /** closeis and cleans up all the resources */
    public void close();
}
