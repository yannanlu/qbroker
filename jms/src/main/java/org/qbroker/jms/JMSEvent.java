package org.qbroker.jms;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.event.Event;

/**
 * JMSEvent is an Event that implements JMS Message
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JMSEvent extends Event implements javax.jms.Message {
    private String messageID;
    private String correlationID;
    private String messageType;
    private int deliveryCount;
    private Destination destination;
    private Destination replyTo;
    private boolean acknowledged;
    protected boolean writeable;
    private Object ackObject = null;
    private java.lang.reflect.Method ackMethod = null;
    private long[] ackState = null;
    public final static int DEFAULT_DELIVERY_MODE = DeliveryMode.PERSISTENT;
    public final static int DEFAULT_PRIORITY = 4;
    public final static int DEFAULT_TIME_TO_LIVE = -1;
    public final static int TYPE_STRING = 0;
    public final static int TYPE_BOOLEAN = 1;
    public final static int TYPE_BYTE = 2;
    public final static int TYPE_SHORT = 3;
    public final static int TYPE_INTEGER = 4;
    public final static int TYPE_LONG = 5;
    public final static int TYPE_FLOAT = 6;
    public final static int TYPE_DOUBLE = 7;

    public JMSEvent() {
        super(Event.NOTICE);
        attribute.remove("priority");
        writeable = true;
        acknowledged = false;
        deliveryCount = 0;
        deliveryMode = DEFAULT_DELIVERY_MODE;
        messageID = null;
        correlationID = null;
        messageType = null;
        replyTo = null;
        destination = null;
    }

    public void acknowledge() throws JMSException {
        if (!acknowledged) {
            if (ackObject != null && ackMethod != null) {
                try {
                    ackMethod.invoke(ackObject, new Object[] {ackState});
                }
                catch (Exception e) {
                    throw(new JMSException("failed to acknowledge msg: " +
                        Event.traceStack(e)));
                }
            }
            ackObject = null;
            ackMethod = null;
            ackState = null;
            acknowledged = true;
        }
    }

    public void setJMSMessageID(String id) throws JMSException {
        messageID = id;
    }

    public String getJMSMessageID() throws JMSException {
        return messageID;
    }

    public void setJMSCorrelationID(String id) throws JMSException {
        correlationID = id;
    }

    public String getJMSCorrelationID() throws JMSException {
        return correlationID;
    }

    public void setJMSCorrelationIDAsBytes(byte[] id) throws JMSException {
        correlationID = new String(id);
    }

    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return correlationID.getBytes();
    }

    public void setJMSTimestamp(long ts) throws JMSException {
        timestamp = ts;
    }

    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = replyTo;
    }

    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    public void setJMSDestination(Destination dst) throws JMSException {
        this.destination = dst;
    }

    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    public void setJMSDeliveryMode(int d) throws JMSException {
        deliveryMode = d;
    }

    public int getJMSDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    public void setJMSRedelivered(boolean x) throws JMSException {
        if (x && deliveryCount <= 1)
            deliveryCount = 2;
        else if (!x && deliveryCount > 1)
            deliveryCount = 1;
    }

    public boolean getJMSRedelivered() throws JMSException {
        if (deliveryCount <= 1)
            return false;
        else
            return true;
    }

    public void setJMSType(String type) throws JMSException {
        messageType = type;
    }

    public String getJMSType() throws JMSException {
        return messageType;
    }

    public void setJMSExpiration(long ts) throws JMSException {
        expiration = ts;
    }

    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    public void setJMSPriority(int p) throws JMSException {
        if (p >= 0 && p <= 9)
            setPriority(9-p);
        else
            throw(new JMSException("invalid priority: " + p));
        attribute.remove("priority");
    }

    public int getJMSPriority() throws JMSException {
        return (9 - getPriority());
    }

    public void clearBody() throws JMSException {
        body = null;
    }

    public void clearProperties() throws JMSException {
        attribute.clear();
        writeable = true;
    }

    public void setBooleanProperty(String name, boolean x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Boolean(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Boolean)
                attribute.put(name, new Boolean(x));
            else if (o instanceof String)
                attribute.put(name, new Boolean(x).toString());
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public boolean getBooleanProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Boolean)
            return ((Boolean) o).booleanValue();
        else if (!(o instanceof String)) 
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            o = new Boolean((String) o);
            return ((Boolean) o).booleanValue();
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setByteProperty(String name, byte x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));
        
        if (!attribute.containsKey(name))
            attribute.put(name, new Byte(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Byte)
                attribute.put(name, new Byte(x));
            else if (o instanceof String)
                attribute.put(name, Byte.toString(x));
            else if (o instanceof Long)
                attribute.put(name, new Long((long) x));
            else if (o instanceof Integer)
                attribute.put(name, new Integer((int) x));
            else if (o instanceof Short)
                attribute.put(name, new Short((short) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public byte getByteProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Byte)
            return ((Byte) o).byteValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Byte.parseByte((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setShortProperty(String name, short x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Short(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Short)
                attribute.put(name, new Short(x));
            else if (o instanceof String)
                attribute.put(name, Short.toString(x));
            else if (o instanceof Long)
                attribute.put(name, new Long((long) x));
            else if (o instanceof Integer)
                attribute.put(name, new Integer((int) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public short getShortProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Short)
            return ((Short) o).shortValue();
        else if (o instanceof Byte)
            return ((Byte) o).shortValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Short.parseShort((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setIntProperty(String name, int x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Integer(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Integer)
                attribute.put(name, new Integer(x));
            else if (o instanceof String)
                attribute.put(name, String.valueOf(x));
            else if (o instanceof Long)
                attribute.put(name, new Long((long) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public int getIntProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Integer)
            return ((Integer) o).intValue();
        else if (o instanceof Short)
            return ((Short) o).intValue();
        else if (o instanceof Byte)
            return ((Byte) o).intValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Integer.parseInt((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setLongProperty(String name, long x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Long(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Long)
                attribute.put(name, new Long(x));
            else if (o instanceof String)
                attribute.put(name, String.valueOf(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public long getLongProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Long)
            return ((Long) o).longValue();
        else if (o instanceof Integer)
            return ((Integer) o).longValue();
        else if (o instanceof Short)
            return ((Short) o).longValue();
        else if (o instanceof Byte)
            return ((Byte) o).longValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Long.parseLong((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setFloatProperty(String name, float x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Float(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Float)
                attribute.put(name, new Float(x));
            else if (o instanceof String)
                attribute.put(name, String.valueOf(x));
            else if (o instanceof Double)
                attribute.put(name, new Double((double) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public float getFloatProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Float)
            return ((Float) o).floatValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Float.parseFloat((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setDoubleProperty(String name, double x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!attribute.containsKey(name))
            attribute.put(name, new Double(x));
        else {
            Object o = attribute.get(name);
            if (o == null || o instanceof Double)
                attribute.put(name, new Double(x));
            else if (o instanceof String)
                attribute.put(name, String.valueOf(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public double getDoubleProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Double)
            return ((Double) o).doubleValue();
        else if (o instanceof Float)
            return ((Float) o).doubleValue();
        else if (!(o instanceof String))
            throw(new MessageFormatException("invalid type for " + name));
        else try {
            return Double.parseDouble((String) o);
        }
        catch (Exception e) {
            throw(new MessageFormatException("invalid value for " + name));
        }
    }

    public void setStringProperty(String name, String x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));
        attribute.put(name, x);
    }

    public String getStringProperty(String name) throws JMSException {
        Object o;
        o = attribute.get(name);
        if (o != null && !(o instanceof String))
            return o.toString();
        else
            return (String) o;
    }

    public void setObjectProperty(String name, Object x) throws JMSException {
        if (!writeable)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));
// disabled it due to customized object types
//        if (!isValid(x))
//            throw(new MessageFormatException("invalid object"));
        attribute.put(name, x);
    }

    public Object getObjectProperty(String name) throws JMSException {
        return attribute.get(name);
    }

    public Enumeration getPropertyNames() throws JMSException {
       return new org.qbroker.common.Enumerator(attribute.keySet().iterator());
    }

    public boolean propertyExists(String name) throws JMSException {
        return attribute.containsKey(name);
    }

    public void setAckObject(Object obj, java.lang.reflect.Method method,
        long[] state) {
        if (ackMethod == null) {
            ackObject = obj;
            ackMethod = method;
            ackState = state;
        }
    }

    protected boolean isValid(Object obj) {
        if (obj == null)
            return true;
        else if (obj instanceof String)
            return true;
        else if (obj instanceof Integer)
            return true;
        else if (obj instanceof Long)
            return true;
        else if (obj instanceof Boolean)
            return true;
        else if (obj instanceof Double)
            return true;
        else if (obj instanceof Float)
            return true;
        else if (obj instanceof Short)
            return true;
        else if (obj instanceof Byte)
            return true;
        else if (obj instanceof byte[])
            return true;
        else
            return false;
    }

    protected void finalize() {
        super.finalize();
        ackMethod = null;
        ackObject = null;
        ackState = null;
    }
}
