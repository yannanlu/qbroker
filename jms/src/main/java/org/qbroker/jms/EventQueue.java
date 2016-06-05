package org.qbroker.jms;

import javax.jms.Queue;
import javax.jms.JMSException;

/**
 * EventQueue represents JMS Queue for event
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventQueue implements Queue {
    private EventDestination dest;
    private String queueName;

    public EventQueue(String uriString) throws JMSException {
        dest = new EventDestination(uriString);
        if (!"queue".equals(dest.getScheme()))
            throw(new JMSException("not a queue: " + uriString));
        queueName = dest.getPath();
        if (queueName == null || queueName.length() <= 0)
            throw(new JMSException("queue not defined: " + uriString));
    }

    public String getQueueName() {
        return queueName;
    }

    public String toString() {
        return dest.toString();
    }
}
