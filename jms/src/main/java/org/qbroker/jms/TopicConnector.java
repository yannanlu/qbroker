package org.qbroker.jms;

import java.io.InputStream;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;

/**
 * TopicConnector is an Interface for JMS Topic and its connections
 *<br/>
 * @author yannanlu@yahoo.com
 */

public interface TopicConnector extends ExceptionListener {
    /** continuously subscribes the JMS messages from the JMS topic and puts
     * them to the output, either a queue, a topic, a stream or an XQueue.
     */
    public void sub(Object out) throws IOException, JMSException;
    /** continuously gets the JMS messages from the XQueue and publishes them
     * with the JMS topic.
     */
    public void pub(XQueue xq) throws TimeoutException, JMSException;
    /** continuously gets the JMS messages from the InputStream and publishes
     * them with the JMS topic.
     */
    public void pub(InputStream in) throws IOException, JMSException;
    /** returns a JMS Message with the initialized JMSSession */
    public Message createMessage(int type) throws JMSException;
    /** returns the queue created for the given name */
    public Queue createQueue(String name) throws JMSException;
    /** returns an on-demand subscriber in a separate session */
    public TopicSubscriber getTopicSubscriber();
    /** returns an on-demand publisher in a separate session */
    public TopicPublisher getTopicPublisher();
    /** returns the string of the operation */
    public String getOperation();
    /** opens the topic for the operation */
    public void tOpen() throws JMSException;
    /** closes the topic */
    public void tClose() throws JMSException;
    /** reopens the topic for the operation */
    public int tReopen();
    /** reconnects to the JMS server for the operation or returns error text */
    public String reconnect();
    /** catches JMSException of Connection asynchronously */
    public void onException(JMSException e);
    /** closes all opened resources */
    public void close();

    public final static int TC_QUEUE = 1;
    public final static int TC_TOPIC = 2;
    public final static int TC_STREAM = 3;
    public final static int TC_XQ = 4;
    public final static int XA_CLIENT = MessageUtils.XA_CLIENT;
    public final static int XA_COMMIT = MessageUtils.XA_COMMIT;
}
