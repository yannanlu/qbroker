package org.qbroker.receiver;

/* MessageReceiver.java - an Interface for receiving JMS messages */

import org.qbroker.common.XQueue;
import org.qbroker.common.Service;

/**
 * MessageReceiver is an interface that receives JMS Messages from their
 * originations and puts them to an XQueue as the output.  This XQueue
 * will be used as the input by instances of either MessageNode or
 * MessagePersister.
 *<br><br>
 * MessageReceiver can be used as the input node in a message flow.  It may
 * actively fetch or pick up the messages from a remote origination.
 * It may also scan the local log files for messages.  Or it just creates
 * messages periodically.  Alternatively, it may listen to a network socket
 * for receiving messages passively.  Therefore MessageReceiver is a source
 * of JMS Messages.
 *<br><br>
 * For the transaction support of MessageReceiver, it is up to the
 * implementations.  If the implementation supports the transactions, the
 * property of XAMode should be used to control the transactions.  If XAMode
 * is set to zero, the Auto Acknowledgement should be enabled.  Otherwise,
 * the transaction should be implemented as Client Acknowledgement.  In the
 * mode of Client Acknowledgement, MessageReceiver relies on extrenal
 * resources to acknowledge the messages.  Meanwhile, its output XQueue should
 * have its EXTERNAL_XA bit set so that Client Acknowledgement will be able to
 * propagate to all XQueues downstream.
 *<br><br>
 * At any given time, MessageReceiver is on one of the following 8 different
 * states:
 *<br>
 * RCVR_READY    connected and ready to work<br>
 * RCVR_RUNNING  running normally<br>
 * RCVR_RETRYING trying to reconnect and get back to work<br>
 * RCVR_PAUSE    pausing for a bit while still connected<br>
 * RCVR_STANDBY  standby for a bit longer while still connected<br>
 * RCVR_DISABLED disabled temporarily with all connections closed<br>
 * RCVR_STOPPED  completed the job and closed all connections<br>
 * RCVR_CLOSED   closed all connections and ready to destroy object<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface MessageReceiver {
    /**
     * puts all the received JMS messages to the XQueue as the output.
     * In case of failure, it should keep retry with the sleepTime in
     * milliseconds.
     */
    public void receive(XQueue out, int sleepTime);
    /** returns the string of the operation */
    public String getOperation();
    /** returns the name of the receiver */
    public String getName();
    /** returns the name of the OutLink */
    public String getLinkName();
    /** returns the status of the receiver */
    public int getStatus();
    /** returns the capacity of the OutLink */
    public int getCapacity();
    /** returns the transaction mode of the reciever */
    public int getXAMode();
    /** returns the partiton array of the input XQueue for the reciever */
    public int[] getPartition();
    /** sets the status of the receiver */
    public void setStatus(int status);
    /** closes all opened resources */
    public void close();

    public final static int RCVR_READY = Service.SERVICE_READY;
    public final static int RCVR_RUNNING = Service.SERVICE_RUNNING;
    public final static int RCVR_RETRYING = Service.SERVICE_RETRYING;
    public final static int RCVR_PAUSE = Service.SERVICE_PAUSE;
    public final static int RCVR_STANDBY = Service.SERVICE_STANDBY;
    public final static int RCVR_DISABLED = Service.SERVICE_DISABLED;
    public final static int RCVR_STOPPED = Service.SERVICE_STOPPED;
    public final static int RCVR_CLOSED = Service.SERVICE_CLOSED;
}
