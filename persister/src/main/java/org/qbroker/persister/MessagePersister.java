package org.qbroker.persister;

/* MessagePersister.java - an Interface for persisting JMS messages */

import org.qbroker.common.XQueue;
import org.qbroker.common.Service;

/**
 * MessagePersister is an interface that picks up JMS Messages from an XQueue
 * as the input and persists or delivers them to their destinations.  The
 * XQueue will be used as the output by instances of either MessageNodes or
 * MessageReceivers.
 *<br><br>
 * MessagePersister is used as the output node in a Message Flow. It supports
 * various types destinations, like JMSDestination, logfile, database,
 * serial port, TCP socket, web server, ftp server, ssh server, smtp server,
 * shell script, etc.  Its job is to deliver the messages.
 *<br><br>
 * For the transaction support of MessagePersister, it is up to the
 * implementations.  If the implementation supports the transactions, the
 * property of XAMode should be used to control the transactions.  If XAMode is
 * set to zero, the Auto Commit should be enabled on the delivery.  Otherwise,
 * the transaction should be implemented as Batch Commit.  In case the input
 * XQueue is EXTERNAL_XA enabled, each delivered message has to be acknowledged
 * explicitly.
 *<br><br>
 * At any given time, MessagePersister is on one of the following 8 different
 * states:
 *<br>
 * PSTR_READY    connected and ready to work<br>
 * PSTR_RUNNING  running normally<br>
 * PSTR_RETRYING trying to reconnect and get back to work<br>
 * PSTR_PAUSE    pausing for a bit while still connected<br>
 * PSTR_STANDBY  standby for a bit longer while still connected<br>
 * PSTR_DISABLED disabled temporarily with all connections closed<br>
 * PSTR_STOPPED  completed the job and closed all connections<br>
 * PSTR_CLOSED   closed all connections and ready to destroy object<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface MessagePersister {
    /**
     * gets all the JMS messages from the XQueue and delivers them to
     * their destinations.  In case of failure, it should keep retry with
     * the sleepTime in milliseconds.
     */
    public void persist(XQueue in, int sleepTime);
    /** returns the string of the operation */
    public String getOperation();
    /** returns the name of the persister */
    public String getName();
    /** returns the name of the InLink */
    public String getLinkName();
    /** returns the status of the persister */
    public int getStatus();
    /** returns the capacity of the InLink */
    public int getCapacity();
    /** returns the transaction mode of the persister */
    public int getXAMode();
    /** sets the status of the persister */
    public void setStatus(int status);
    /** closes all opened resources */
    public void close();

    public final static int PSTR_READY = Service.SERVICE_READY;
    public final static int PSTR_RUNNING = Service.SERVICE_RUNNING;
    public final static int PSTR_RETRYING = Service.SERVICE_RETRYING;
    public final static int PSTR_PAUSE = Service.SERVICE_PAUSE;
    public final static int PSTR_STANDBY = Service.SERVICE_STANDBY;
    public final static int PSTR_DISABLED = Service.SERVICE_DISABLED;
    public final static int PSTR_STOPPED = Service.SERVICE_STOPPED;
    public final static int PSTR_CLOSED = Service.SERVICE_CLOSED;
}
