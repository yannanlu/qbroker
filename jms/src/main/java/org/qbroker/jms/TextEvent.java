package org.qbroker.jms;

import java.util.Iterator;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.event.Event;
import org.qbroker.jms.JMSEvent;

/**
 * TextEvent is an Event that implements JMS TextMessage
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TextEvent extends JMSEvent implements javax.jms.TextMessage {
    public TextEvent() {
        super();
    }

    public TextEvent(String text) {
        this();
        body = text;
    }

    public void setText(String text) throws JMSException {
        body = text;
    }

    public String getText() throws JMSException {
        return (String) body;
    }

    /** for signature of toEvent */
    public int getLogMode() {
        return this.logMode;
    }

    /** for signature of toEvent */
    public void setLogMode(int mode) {
        if (mode >= LOG_LOCAL && mode <= LOG_SYSLOG)
            this.logMode = mode;
    }

    /** returns a TextEvent for the event */
    public static TextEvent toTextEvent(Event event) throws JMSException {
        TextEvent msg = null;
        String key, value;
        Iterator iter;
        if (event == null)
            return null;
        value = event.getAttribute("text");
        if (value == null)
            value = "";
        msg = new TextEvent(value);
        msg.setJMSPriority(9-event.getPriority());
        msg.setJMSTimestamp(event.getTimestamp());
        // set logMode as a signature of toEvent
        msg.logMode = LOG_JMS;
        iter = event.getAttributeNames();
        while (iter.hasNext()) { // COMPACT mode with priority and text
            key = (String) iter.next();
            value = event.getAttribute(key);
            if (value == null)
                continue;
            msg.setAttribute(key, value);
        }
        return msg;
    }
}
