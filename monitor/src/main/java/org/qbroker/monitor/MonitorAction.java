package org.qbroker.monitor;

/**
 * MonitorAction is an interface for an action upon a report
 *<br/><br/>
 * It is up to the implementation to decide whether to support
 * checkpointing and how to do checkpointing
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface MonitorAction {
    /**
     * examines the report on the monitored entity and reacts to it as an action
     */
    public org.qbroker.event.Event performAction(int status,
        long currentTime, java.util.Map<String, Object> report);
    /** returns the name of the action */
    public String getName();
    /** returns the checkpoint Map for the action */
    public java.util.Map<String, Object> checkpoint();
    /** restores the state of the action from the checkpoint Map */
    public void restoreFromCheckpoint(java.util.Map<String,Object>checkpointer);
}
