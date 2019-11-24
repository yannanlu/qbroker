package org.qbroker.cluster;

/* ClusterNode.java - an interface for a cluster node */

import org.qbroker.common.XQueue;
import org.qbroker.event.Event;

/**
 * ClusterNode is an automaton node of a cluster.  The cluster may
 * contain many nodes or members communicating with each other via
 * the network.  One of the nodes is the manager that supervises
 * the entire cluster.  The rest of nodes are workers or monitors
 * reporting to the manager and watching its behavior closely.  In
 * case that the manager quits or any node misses certain number of
 * heartbeats from the manager, the rest of the workers will elect
 * a new manager and resume to work.  Any new node can join the
 * cluster anytime as long as it is allowed and there is a room available
 * in the cluster.  Any worker can leave the cluster anytime at its wish.
 * If the last member quits, the cluster goes down.
 *<br><br>
 * ClusterNode is supposed to run at backgroud.  It is required to be an
 * automaton that maintains its own state according to certain predefined
 * rules automatically.  It communicates with outside through an XQueue and
 * APIs via Events.  The application is supposed to listen to the XQueue
 * to monitor the status of the cluster.  To actively control the cluster,
 * application should use relay() API to pass opaque data or instructions.
 * ClusterNode is supposed to be able to intercept the instructions or
 * relay the data to the cluster or other nodes.
 *<br><br>
 * A ClusterNode may be in one of the following states:<br>
 * NODE_NEW:      just starting up, not joining the cluster yet<br>
 * NODE_RUNNING:  joined in the cluster and running OK<br>
 * NODE_STANDBY:  joined in the cluster but still in transient state<br>
 * NODE_TIMEOUT:  missed a fixed number of heartbeats from the node<br>
 * NODE_DOWN:     no longer active<br>
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface ClusterNode {
    public final static int NODE_NEW = 0;
    public final static int NODE_RUNNING = 1;
    public final static int NODE_STANDBY = 2;
    public final static int NODE_TIMEOUT = 3;
    public final static int NODE_DOWN = 4;
    public final static int RCVR_NONE = -1;
    public final static int RCVR_COMM = 0;
    public final static int RCVR_UDP = 1;
    public final static int RCVR_TCP = 2;
    public final static int RCVR_JMS = 3;
    public final static int HB_SIZE = 0;
    public final static int HB_TYPE = 1;
    public final static int HB_PCOUNT = 2;
    public final static int HB_ECOUNT = 3;
    public final static int HB_PTIME = 4;
    public final static int HB_ETIME = 5;
    public final static int ACTION_MOD = 0;
    public final static int ACTION_ADD = 1;
    public final static int ACTION_DEL = 2;
    public final static int ACTION_OVR = 3;
    public final static int ACTION_IGR = 4;
    public final static int ACTION_CHK = 5;
    public final static int ACTION_VOTE = 6;
    public final static int DEBUG_NONE = 0;
    public final static int DEBUG_INIT = 1;
    public final static int DEBUG_EXAM = 2;
    public final static int DEBUG_CHECK = 4;
    public final static int DEBUG_RELAY = 8;
    public final static int DEBUG_CLOSE = 16;
    public final static int DEBUG_HBEAT = 32;
    public final static int DEBUG_ALL = 2 * DEBUG_HBEAT - 1;

    /**
     * It starts the cluster node with partitioned XQueue as the escalation
     * channel, and supervises the cluster.
     */
    public void start(XQueue out, int begin, int len);

    /**
     * It is the API to for application to send data to the cluster
     * asynchronously.  It returns a non-negative number for success,
     * or -1 otherwise.  Event is used to carry opaque data as the
     * escalations or instructions.  The API in turn relays the data
     * to the cluster or the specific node.  Waittime is the
     * millisecond for the API to wait if the communication is busy.
     */
    public int relay(Event data, int waitTime);

    /** closes the node and all resources */
    public void close();

    public boolean isMaster();

    /** returns the role of the node */
    public int getRole();

    /** returns the status of the node */
    public int getStatus();

    /** returns the sessionTimeout of the node */
    public int getTimeout();

    /** returns the URI of the node */
    public String getURI();

    /** returns the size of the cluster */
    public int getSize();

    /** returns the debug mode of the node */
    public int getDebug();

    /** sets the debug mode of the node */
    public void setDebug(int mode);
}
