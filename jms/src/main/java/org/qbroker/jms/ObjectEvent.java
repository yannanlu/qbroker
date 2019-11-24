package org.qbroker.jms;

import java.io.Serializable;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.jms.JMSEvent;

/**
 * ObjectEvent is an Event that implements JMS ObjectMessage
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ObjectEvent extends JMSEvent implements javax.jms.ObjectMessage {
    private boolean readonly;
    public ObjectEvent() {
        super();
        body = null;
        readonly = false;
    }

    public void setObject(Serializable object) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        body = object;
    }

    public Serializable getObject() throws JMSException {
        return (Serializable) body;
    }

    public void clearBody() throws JMSException {
        body = null;
        readonly = false;
    }
}
