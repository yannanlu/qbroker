package org.qbroker.sonicmq;

/* SonicMQUtils.java - a utility for SonicMQ */

import javax.jms.Session;
import javax.jms.JMSException;
import progress.message.jclient.QueueSession;
import progress.message.jclient.TopicSession;

/**
 * SonicMQUtils provides methods to disable/enable the flow control on a
 * SonicMQ JMS session.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SonicMQUtils {
    public SonicMQUtils() {
    }

    /** disables the flow control on a SonicMQ session */
    public static void disableFlowControl(Session s) throws JMSException {
        if (s == null)
            throw(new IllegalArgumentException("session is null"));

        if (s instanceof QueueSession)
            ((QueueSession) s).setFlowControlDisabled(true);
        else if (s instanceof TopicSession)
            ((TopicSession) s).setFlowControlDisabled(true);
        else
            throw(new JMSException("not a SonicMQ session"));
    }

    /** enables the flow control on a SonicMQ session */
    public static void enableFlowControl(Session s) throws JMSException {
        if (s == null)
            throw(new IllegalArgumentException("session is null"));

        if (s instanceof QueueSession)
            ((QueueSession) s).setFlowControlDisabled(false);
        else if (s instanceof TopicSession)
            ((TopicSession) s).setFlowControlDisabled(false);
        else
            throw(new JMSException("not a SonicMQ session"));
    }
}
