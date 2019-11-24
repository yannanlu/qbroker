package org.qbroker.jms;

import java.io.InputStream;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueReceiver;
import javax.jms.QueueBrowser;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.common.TimeoutException;
import org.qbroker.jms.MessageUtils;

/**
 * QueueConnector is an Interface for JMS Queue and its Connections
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface QueueConnector extends ExceptionListener {
    /** browses the JMS messages from the JMS queue and puts them
     * to the output, either a queue, a topic, a stream or an XQueue.
     */
    public void browse(Object out) throws IOException, JMSException;
    /** continuously gets the JMS messages from the XQueue and extracts
     * message selector from messages.  Then it queries the JMS queue for
     * messages and packs them into the request message body as the response.
     */
    public void query(XQueue xq) throws TimeoutException, JMSException;
    /** continuously gets the JMS messages from the JMS queue and puts them
     * to the output, either a queue, a topic, a stream or an XQueue.
     */
    public void get(Object out) throws IOException, JMSException;
    /** continuously gets the JMS messages from the XQueue and sends them
     * to the JMS queue as requests and waits for responses from given queue.
     */
    public void request(XQueue xq, String rq) throws TimeoutException, JMSException;
    /** continuously gets the JMS messages from the XQueue and sends them
     * to their JMSReplyQ as the responses.
     */
    public void reply(XQueue xq) throws TimeoutException, JMSException;
    /** continuously gets the JMS messages from the InputStream and puts them
     * to the JMS queue.
     */
    public void put(InputStream in) throws IOException, JMSException;
    /** continuously gets the JMS messages from the XQueue and puts them
     * to the JMS queue.
     */
    public void put(XQueue xq) throws TimeoutException, JMSException;
    /** returns a JMS Message with the initialized JMSSession */
    public Message createMessage(int type) throws JMSException;
    /** returns the queue created for the given name */
    public Queue createQueue(String name) throws JMSException;
    /** returns the temporary queue created */
    public Queue createTemporaryQueue() throws JMSException;
    /** returns an on-demand browser in a separate session */
    public QueueBrowser getQueueBrowser();
    /** returns an on-demand receiver in a separate session */
    public QueueReceiver getQueueReceiver();
    /** returns an on-demand sender in a separate session */
    public QueueSender getQueueSender();
    /** returns the string of the operation */
    public String getOperation();
    /** opens the queue for the operation */
    public void qOpen() throws JMSException;
    /** closes the queue */
    public void qClose() throws JMSException;
    /** reopens the queue for the operation */
    public int qReopen();
    /** reconnects to the JMS server for the operation or returns error text */
    public String reconnect();
    /** catches JMSException of Connection asynchronously */
    public void onException(JMSException e);
    /** closes all opened resources */
    public void close();

    public final static int QC_QUEUE = 1;
    public final static int QC_TOPIC = 2;
    public final static int QC_STREAM = 3;
    public final static int QC_XQ = 4;
    public final static int XA_CLIENT = MessageUtils.XA_CLIENT;
    public final static int XA_COMMIT = MessageUtils.XA_COMMIT;
}
