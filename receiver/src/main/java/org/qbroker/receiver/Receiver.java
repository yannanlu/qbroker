package org.qbroker.receiver;

/* Receiver.java - an abstract class of MessageReceiver */

import java.util.Map;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeWindows;
import org.qbroker.jms.MessageUtils;
import org.qbroker.receiver.MessageReceiver;

/**
 * Receiver is an abstract class of MessageReceiver.
 *<br/><br/>
 * tolerance:    not used<br/>
 * maxRetry:     number of times to reconnect in one period<br/>
 * repeatPeriod: quietPeriod + maxRetry, period of repeat operation<br/>
 * pauseTime:    sleepTime in millisec for PAUSE state or default baseTime<br/>
 * standbyTime:  sleepTime in millisec for STANDBY state or reconnect action<br/>
 * timeout:      timeout in millisec to reset retryCount for a new session<br/>
 * waitTime:     sleep time in millisec for disable action<br/>
 *<br/>
 * RCVR_READY    connected and ready to work<br/>
 * RCVR_RUNNING  running normally<br/>
 * RCVR_RETRYING trying to get back to running state<br/>
 * RCVR_PAUSE    pausing for a bit while still connected<br/>
 * RCVR_STANDBY  standby for a bit longer while still connected<br/>
 * RCVR_DISABLED connection closed but still standby<br/>
 * RCVR_STOPPED  completed the job and closed all connections<br/>
 * RCVR_CLOSED   closed all connections and ready to destroy object<br/>
 *<br/>
 * @author yannanlu@yahoo.com
 */

public abstract class Receiver implements MessageReceiver {
    protected String uri = null;
    protected String operation = "receive";
    protected String linkName = null;
    protected int mode = 0;
    protected int xaMode = 0;
    protected int capacity = 0;
    protected int status = RCVR_READY;
    protected int repeatPeriod, maxRetry, tolerance;
    protected int pauseTime, standbyTime, timeout;
    protected int displayMask = 0;
    protected long waitTime = 500L;
    protected String[] propertyName = null, propertyValue = null;
    protected int[] partition = new int[]{0,0};

    public Receiver(Map props) {
        Object o;
        int n;

        if ((o = props.get("URI")) != null)
            uri = (String) o;

        if ((o = props.get("LinkName")) != null)
            linkName = (String) o;
        if (linkName == null || linkName.length() <= 0)
            linkName = "root";

        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        else
            xaMode = 0;

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 0;

        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else if ((o = props.get("CellID")) != null) {
            partition[0] = Integer.parseInt((String) o);
            partition[1] = 1;
        }
        else {
            partition[0] = 0;
            partition[1] = 0;
        }

        if ((o = props.get("Operation")) != null)
            operation = (String) o;

        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);

        if ((o = props.get("WaitTime")) == null ||
            (waitTime = Integer.parseInt((String) o)) <= 0)
            waitTime = 500L;

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

    public abstract void receive(XQueue xq, int baseTime);

    public abstract void close();

    protected synchronized void resetStatus(int current, int status) {
        if (this.status == current && status >= RCVR_READY &&
            status <= RCVR_CLOSED)
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

    public int[] getPartition() {
        return new int[]{partition[0], partition[1]};
    }

    public synchronized int getStatus() {
        return status;
    }

    public synchronized void setStatus(int status) {
        if (status >= RCVR_READY && status <= RCVR_CLOSED)
            this.status = status;
    }
}
