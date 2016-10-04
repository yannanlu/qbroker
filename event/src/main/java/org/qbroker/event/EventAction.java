package org.qbroker.event;

/**
 * EventAction is an Interface for an action upon an event
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface EventAction {
    /** invokes the action upon the event */
    public void invokeAction(long currentTime, org.qbroker.event.Event event);
    /** returns the name of the action */
    public String getName();
    /** closes or cleans up all the resources */
    public void close();

    public final static int ACTION_LIST = 0;
    public final static int ACTION_QUERY = 1;
    public final static int ACTION_UPDATE = 2;
    public final static int ACTION_RENAME = 3;
    public final static int ACTION_CREATE = 4;
    public final static int ACTION_REMOVE = 5;
    public final static int ACTION_ENABLE = 6;
    public final static int ACTION_DISABLE = 7;
    public final static int ACTION_STANDBY = 8;
    public final static int ACTION_FAILOVER = 9;
    public final static int ACTION_BALANCE = 10;
    public final static int ACTION_RELOAD = 11;
    public final static int ACTION_DEPLOY = 12;
    public final static int ACTION_UNDEPLOY = 13;
    public final static int ACTION_STOP = 14;
    public final static int ACTION_RESTART = 15;
}
