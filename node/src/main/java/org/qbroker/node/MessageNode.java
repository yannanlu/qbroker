package org.qbroker.node;

/* MessageNode.java - an Interface processing JMS messages */

import java.util.Map;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.Service;

/**
 * MessageNode is an interface that pickes up JMS Messages from the only input
 * XQueue and processes them according to the predefined rules and their
 * content.  Then it puts the processed messages to one of the output XQueues.
 * MessageNode should never consume any messages.  An incoming message has
 * to go out of one of the output XQueues.  However, it is OK for MessageNode
 * to create new Messages as the reaction upon the incoming messages.
 *<br/><br/>
 * The transaction support is determined by the property of XAMode and the
 * EXTERNAL_XA bit of the input XQueue.  If the input XQueue has no EXTERNAL_XA
 * bit set, XAMode can be either 0 as disabled or positive as enabled.
 * Otherwise, XAMode should always be or reset to positive to enable the
 * transction support.  The transaction support is the only way to guarentee 
 * the EXTERNAL_XA feature is fulfilled.
 *<br/><br/>
 * If XAMode is set to 0 and the input XQueue has no EXTERNAL_XA bit set,
 * the incoming message should be removed from the input XQueue once it is
 * put to an output XQueue.  In this case, the de-queue processes are fully
 * separated at MessageNode.  Otherwise, MessageNode should keep track on
 * the status of the message until it is removed from the output XQueue.
 * Only after it is removed from the output XQueue, MessageNode will remove
 * the message from the input XQueue.  So the de-queue processes will
 * propagate in upstream direction.  MessageNode should never acknowledge
 * any messages unless something is really out of control.  It should also
 * enforce the propagation of EXTERNAL_XA bit for all output XQueues.
 *<br/><br/>
 * It is up to developers to decide whether to implement the support of dynamic
 * rules.  If not, all the rules will be static.  They will not be changable on
 * the fly.  The support of checkpointing is not mandatory either.  However,
 * please make sure to document those methods and have them return -1 or null.
 *<br/><br/>
 * At any given time, MessageNode is on one of the following 8 different states:
 *<br/>
 * NODE_READY    all resources opened and ready to work<br/>
 * NODE_RUNNING  running normally<br/>
 * NODE_RETRYING trying to reopen resources and get back to work<br/>
 * NODE_PAUSE    pausing for a bit while still ready to run<br/>
 * NODE_STANDBY  standby for a bit longer while still ready to run<br/>
 * NODE_DISABLED disabled temporarily while still ready to run<br/>
 * NODE_STOPPED  completed the job and closed all resources<br/>
 * NODE_CLOSED   closed all resources and ready to destroy object<br/>
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface MessageNode {
    /**
     * gets the JMS messages from the input XQueue and processes them one by
     * one.  Then it propagets them to the output XQueues according certain
     * rulesets.
     */
    public void propagate(XQueue in, XQueue[] out) throws JMSException;
    /** returns the string of the operation */
    public String getOperation();
    /** returns the name of the node */
    public String getName();
    /** returns the total number of OutLinks */
    public int getNumberOfOutLinks();
    /** returns the BOUNDARY for collectibles */
    public int getOutLinkBoundary();
    /** returns the name of the uplink */
    public String getLinkName();
    /** returns the name of the i-th outlink */
    public String getLinkName(int i);
    /** returns the capacity of the uplink */
    public int getCapacity();
    /** returns the capacity of the i-th outlink */
    public int getCapacity(int i);
    /** returns the transaction mode of the uplink */
    public int getXAMode();
    /** returns the transaction mode of the i-th outlink */
    public int getXAMode(int i);
    /** returns the status of the node */
    public int getStatus();
    /** sets the status of the node */
    public void setStatus(int status);
    /** returns the total number of RuleSets */
    public int getNumberOfRules();
    /** returns the name of the i-th RuleSet */
    public String getRuleName(int i);
    /** returns the displayMask of the node */
    public int getDisplayMask();
    /** sets the displayMask of the node */
    public void setDisplayMask(int mask);
    /** returns the debug mode of the node */
    public int getDebugMode();
    /** sets the debug mode of the node */
    public void setDebugMode(int debug);
    /** gets the display property names of the node */
    public String[] getDisplayProperties();
    /** sets the display property names of the node */
    public void setDisplayProperties(String[] keys);
    /** updates the parameters of the node and returns number of them changed */
    public int updateParameters(Map props);
    /** adds a new rule defined by the property hash and returns the rule id */
    public int addRule(Map props);
    /** removes the rule and returns its id */
    public int removeRule(String ruleName, XQueue in);
    /** replaces the rule with the given property hash and returns its id */
    public int replaceRule(String ruleName, Map props, XQueue in);
    /** renames the rule and returns its id */
    public int renameRule(String oldName, String ruleName);
    /** moves the rule to the end of list and returns its id */
    public int rotateRule(String ruleName);
    /** refreshes the external rules and returns the number of rules updated */
    public int refreshRule(String ruleName, XQueue in);
    /** returns true if the rule exists or false otherwise */
    public boolean containsRule(String ruleName);
    /** returns true if the node has the outlink */
    public boolean containsXQ(String outlinkName);
    /** returns true if the node supports internal XQueues for outlinks */
    public boolean internalXQSupported();
    /** lists all PROPAGATING or PENDING messages of the node in the uplink
     * and returns a Map or null if the uplink is not for the node.
     */
    public Map<String, String> listPendings(XQueue uplink, int type);
    /** lists all NEW messages in the given outlink of the node and returns
     * a Map or null if the outlink is not defined.
     */
    public Map<String, String> list(String outlinkName, int type);
    /** browses the message at the id-th cell from the given outlink and
     * returns the detail text in the given format type
     */
    public String show(String outlinkName, int id, int type);
    /** removes the message at the id-th cell from the given outlink and
     * returns the message upon success
     */
    public Message takeback(String outlinkName, int id);
    /**
     * copies certain type of MetaData from its id-th instance to the buffer
     * and returns the real id of the instance , or -1 on failure.  There are
     * four types of MetaData available:<br/>
     * META_MSG         MetaData of outstanding messages in the uplink<br/>
     * META_OUT         MetaData of output links<br/>
     * META_RULE        MetaData of rule sets<br/>
     * META_XQ          MetaData of XQueues<br/>
     */
    public int getMetaData(int type, int id, long[] data);
    /** cleans up and resets all MetaData for XQs and messages */
    public void resetMetaData(XQueue in, XQueue[] out);
    /** returns the checkpoint hashmap for current state */
    public Map<String, Object> checkpoint();
    /** restores the state from a checkpointed hashmap */
    public void restore(Map<String, Object> chkpt);
    /** closes all opened resources */
    public void close();

    // common constants
    public final static int MSG_CID = 0;     // cell ID of in XQ
    public final static int MSG_OID = 1;     // id of the out XQ
    public final static int MSG_BID = 2;     // cell ID of out XQ
    public final static int MSG_RID = 3;     // id of the ruleset
    public final static int MSG_TID = 4;     // id of the task
    public final static int MSG_TIME = 5;    // timestamp of msg
    public final static int OUT_NRULE = 0;   // no of current rulesets
    public final static int OUT_ORULE = 1;   // no of original rulesets
    public final static int OUT_OFFSET = 2;  // begin position of partition
    public final static int OUT_LENGTH = 3;  // no of partitioned cells
    public final static int OUT_CAPACITY =4; // total no of cells
    public final static int OUT_SIZE = 5;    // no msg still in XQ
    public final static int OUT_COUNT = 6;   // total msg since reset
    public final static int OUT_DEQ = 7;     // de-queue count
    public final static int OUT_STATUS = 8;  // status of XQ
    public final static int OUT_MODE = 9;    // mode of XQ
    public final static int OUT_EXTRA = 10;  // extra info of XQ 
    public final static int OUT_TIME = 11;   // timestamp of update
    public final static int OUT_QDEPTH = 12; // curdepth of queue
    public final static int OUT_QIPPS = 13;  // ipps count of queue
    public final static int OUT_QSTATUS =14; // status of JMS queue
    public final static int OUT_QTIME = 15;  // timestamp of queue query
    public final static int RULE_OID = 0;    // id of current out XQ
    public final static int RULE_PID = 1;    // id of preferred out XQ or type
    public final static int RULE_GID = 2;    // group id of the ruleset
    public final static int RULE_PEND = 3;   // number of msgs in pending
    public final static int RULE_SIZE = 4;   // number of msgs in transient
    public final static int RULE_COUNT = 5;  // number of msgs delivered
    public final static int RULE_STATUS = 6; // status of the ruleset
    public final static int RULE_TTL = 7;    // time-to-live of the ruleset
    public final static int RULE_MODE = 8;   // mode of the ruleset
    public final static int RULE_OPTION = 9; // option of the ruleset
    public final static int RULE_DMASK = 10; // display mask of the ruleset
    public final static int RULE_EXTRA = 11; // extra data of the ruleset
    public final static int RULE_TIME = 12;  // timestamp of update
    public final static int META_MSG = 1;
    public final static int META_OUT = 2;
    public final static int META_MAP = 3;
    public final static int META_RULE = 4;
    public final static int META_XQ = 5;
    public final static int ASSET_XQ = 0;    // XQueue
    public final static int ASSET_URI = 1;   // uri of xq
    public final static int ASSET_THR = 2;   // thread of persister
    public final static int NODE_READY = Service.SERVICE_READY;
    public final static int NODE_RUNNING = Service.SERVICE_RUNNING;
    public final static int NODE_RETRYING = Service.SERVICE_RETRYING;
    public final static int NODE_PAUSE = Service.SERVICE_PAUSE;
    public final static int NODE_STANDBY = Service.SERVICE_STANDBY;
    public final static int NODE_DISABLED = Service.SERVICE_DISABLED;
    public final static int NODE_STOPPED = Service.SERVICE_STOPPED;
    public final static int NODE_CLOSED = Service.SERVICE_CLOSED;
}
