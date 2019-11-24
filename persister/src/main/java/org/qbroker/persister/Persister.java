package org.qbroker.persister;

/* Persister.java - an abstract class of MessagePersister */

import java.util.Map;
import java.util.Iterator;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.jms.MessageUtils;
import org.qbroker.persister.MessagePersister;

/**
 * Persister is an abstract class of MessagePersister.
 *<br><br>
 * tolerance:    not used<br>
 * maxRetry:     number of times to reconnect in one period<br>
 * repeatPeriod: quietPeriod + maxRetry, period of repeat operation<br>
 * pauseTime:    sleepTime in millisec for PAUSE state or default baseTime<br>
 * standbyTime:  sleepTime in millisec for STANDBY state or reconnect action<br>
 * timeout:      timeout in millisec to reset retryCount for a new session<br>
 * waitTime:     sleep time in millisec for disable action<br>
 *<br>
 * PSTR_READY    connected and ready to work<br>
 * PSTR_RUNNING  running normally<br>
 * PSTR_RETRYING trying to get back to running state<br>
 * PSTR_PAUSE    pausing for a bit while still connected<br>
 * PSTR_STANDBY  standby for a bit longer while still connected<br>
 * PSTR_DISABLED connection closed but still standby<br>
 * PSTR_STOPPED  completed the job and closed all connections<br>
 * PSTR_CLOSED   closed all connections and ready to destroy object<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public abstract class Persister implements MessagePersister {
    protected String uri = null;
    protected String operation = null;
    protected String linkName = null;
    protected int xaMode = 0;
    protected int capacity = 0;
    protected int status = PSTR_READY;
    protected int repeatPeriod, maxRetry, tolerance;
    protected int pauseTime, standbyTime, timeout;
    protected int displayMask = 0;
    protected String[] propertyName = null;
    protected long waitTime = 500L;
    protected int[] partition = new int[]{0,0};

    public Persister(Map props) {
        Object o;
        int n;

        if ((o = props.get("URI")) != null)
            uri = (String) o;

        if ((o = props.get("LinkName")) != null)
            linkName = (String) o;
        if (linkName == null || linkName.length() <= 0)
            linkName = "root";

        if ((o = props.get("Operation")) != null)
            operation = (String) o;

        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);

        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            Iterator iter = ((Map) o).keySet().iterator();
            n = ((Map) o).size();
            propertyName = new String[n];
            n = 0;
            while (iter.hasNext()) {
                String key = (String) iter.next();
                if ((propertyName[n] = MessageUtils.getPropertyID(key)) == null)
                    propertyName[n] = key;
                n ++;
            }
        }

        if ((o = props.get("WaitTime")) == null ||
            (waitTime = Integer.parseInt((String) o)) <= 0)
            waitTime = 500;

        if ((o = props.get("PauseTime")) == null ||
            (pauseTime = Integer.parseInt((String) o)) <= 0)
            pauseTime = 5000;

        if ((o = props.get("StandbyTime")) == null ||
            (standbyTime = Integer.parseInt((String) o)) <= 0)
            standbyTime = 15000;

        if ((o = props.get("Timeout")) == null ||
            (timeout = Integer.parseInt((String) o)) <= 0)
            timeout = 2000;

        if ((o = props.get("Tolerance")) == null ||
            (tolerance = Integer.parseInt((String) o)) < 0)
            tolerance = 2;

        if ((o = props.get("MaxRetry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) <= 0)
            maxRetry = 1;

        if ((o = props.get("QuietPeriod")) == null ||
            (n = Integer.parseInt((String) o)) < 0)
            n = 4;

        repeatPeriod = maxRetry + n;
    }

    public abstract void persist(XQueue xq, int baseTime);

    public abstract void close();

    protected synchronized void resetStatus(int current, int status) {
        if (this.status == current && status >= PSTR_READY &&
            status <= PSTR_CLOSED)
            this.status = status;
    }

    protected boolean keepRunning(XQueue xq) {
        return ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
    }

    protected boolean isStopped(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        return (!((xq.getGlobalMask() & mask) > 0));
    }

    public String getName() {
        return uri;
    }

    public String getOperation() {
        return operation;
    }

    public String getLinkName() {
        return linkName;
    }

    public int getXAMode() {
        return xaMode;
    }

    public int getCapacity() {
        return capacity;
    }

    public synchronized int getStatus() {
        return status;
    }

    public synchronized void setStatus(int status) {
        if (status >= PSTR_READY && status <= PSTR_CLOSED)
            this.status = status;
    }
}
