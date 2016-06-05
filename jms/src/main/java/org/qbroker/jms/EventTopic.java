package org.qbroker.jms;

import javax.jms.Topic;
import javax.jms.JMSException;

/**
 * EventTopic represents JMS Topic for event
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventTopic implements Topic {
    private EventDestination dest;
    private String topicName;

    public EventTopic(String uriString) throws JMSException {
        dest = new EventDestination(uriString);
        if (!"topic".equals(dest.getScheme()))
            throw(new JMSException("not a topic: " + uriString));
        topicName = dest.getPath();
        if (topicName == null || topicName.length() <= 0)
            throw(new JMSException("queue not defined: " + uriString));
    }

    public String getTopicName() {
        return topicName;
    }

    public String toString() {
        return dest.toString();
    }
}
