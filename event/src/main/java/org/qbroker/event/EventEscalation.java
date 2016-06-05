package org.qbroker.event;

/**
 * EventEscalation is an interface for an escalation upon an event.
 *<br/><br/>
 * It is up to the implementation to decide whether to support
 * checkpointing and how to do checkpointing.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface EventEscalation {
    /**
     * It examines the event and returns a new event as the escalation or
     * null if no escalation.
     */
    public Event escalate(int status, long currentTime,
        org.qbroker.event.Event event);
    /** returns the name of the escalation */
    public String getName();
    /**
     * It returns an integer as the escalation order that determines if the
     * escalation event to be processed before the original event or not.
     * If the escalation order is ORDER_FISRT, the escalation event should be
     * processed before the original event.  If it is ORDER_LAST, the
     * escalation event should be processed after.  If it is ORDER_NONE,
     * which one is the first does not matter.
     */
    public int getEscalationOrder();
    /** resets the internal session returns number of items being reset */
    public int resetSession(long currentTime);
    /** returns the start time of current session */
    public long getSessionTime();
    /** returns the size of current session */
    public int getSessionSize();
    /** returns the checkpoint Map for the escalation */
    public java.util.Map<String, Object> checkpoint();
    /** restores the state of the escalation from the checkpoint Map */
    public void restoreFromCheckpoint(java.util.Map<String,Object>checkpointer);
    /** cleans up all internal caches and releases all resources */
    public void clear();

    public final static int ORDER_LAST = -1; 
    public final static int ORDER_NONE = 0; 
    public final static int ORDER_FIRST = 1; 
}
