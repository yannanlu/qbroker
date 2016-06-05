package org.qbroker.common;

/* Event.java - an Interface for a generic event */

import java.util.Iterator;

/**
 * Event is the interfact for a generic event.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface Event {
    /** returns the priority of the event */
    public int getPriority();
    /** sets the priority of the event */
    public void setPriority(int p);
    /** returns the timestamp of the event */
    public long getTimestamp();
    /** sets the timestamp of the event */
    public void setTimestamp(long ts);
    /** returns the expiration of the event */
    public long getExpiration();
    /** sets the expiration of the event */
    public void setExpiration(long ts);
    /** returns the attribute on the key */
    public String getAttribute(String key);
    /** sets the attribute on the key */
    public void setAttribute(String key, String value);
    /** removes the attribute on the key */
    public String removeAttribute(String key);
    /** clears all attributes of the event */
    public void clearAttributes();
    /** returns the iterator for all the keys of attributes */
    public Iterator getAttributeNames();
    /** tests if the attribute exists on the key */
    public boolean attributeExists(String key);

    public final static int EMERG = 0;
    public final static int ALERT = 1;
    public final static int CRIT = 2;
    public final static int ERR = 3;
    public final static int WARNING = 4;
    public final static int NOTICE = 5;
    public final static int INFO = 6;
    public final static int DEBUG = 7;
    public final static int TRACE = 8;
    public final static int NONE = 9;
    public final static String priorityNames[] = {
        "EMERG",
        "ALERT",
        "CRIT",
        "ERR",
        "WARNING",
        "NOTICE",
        "INFO",
        "DEBUG",
        "TRACE",
        "NONE"
    };
}
