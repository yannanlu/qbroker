package org.qbroker.jms;

import java.util.Enumeration;
import java.util.HashMap;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.jms.JMSEvent;

/**
 * MapEvent is an Event that implements JMS MapMessage
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class MapEvent extends JMSEvent implements javax.jms.MapMessage {
    private final HashMap map;
    private boolean readonly;
    public MapEvent() {
        super();
        map = new HashMap();
        body = map;
        readonly = false;
    }

    public void setBoolean(String name, boolean x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Boolean(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Boolean)
                map.put(name, new Boolean(x));
            else if (o instanceof String)
                map.put(name, new Boolean(x).toString());
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public boolean getBoolean(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setByte(String name, byte x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Byte(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Byte)
                map.put(name, new Byte(x));
            else if (o instanceof String)
                map.put(name, Byte.toString(x));
            else if (o instanceof Short)
                map.put(name, new Short((short) x));
            else if (o instanceof Integer)
                map.put(name, new Integer((int) x));
            else if (o instanceof Long)
                map.put(name, new Long((long) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public byte getByte(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setBytes(String name, byte[] x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value for " + name));

        if (!map.containsKey(name))
            map.put(name, x);
        else {
            Object o = map.get(name);
            if (o == null || o instanceof byte[])
                map.put(name, x);
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public void setBytes(String name, byte[] x, int offset, int length)
        throws JMSException {
        Object o;
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null || offset < 0 || length < 0)
            throw(new JMSException("failed to set value for " + name));

        if (!map.containsKey(name) || (o = map.get(name)) == null ||
            o instanceof byte[]) {
            byte[] y = new byte[length];
            System.arraycopy(x, offset, y, 0, length);
            map.put(name, y);
        }
        else
            throw(new MessageFormatException("invalid type for " + name));
    }

    public byte[] getBytes(String name) throws JMSException {
        Object o;
        o = map.get(name);
        if (o == null)
            return null;
        else if (o instanceof byte[])
            return (byte[]) o;
        else
            throw(new MessageFormatException("invalid type for " + name));
    }

    public void setChar(String name, char x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Character(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Character)
                map.put(name, new Character(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public char getChar(String name) throws JMSException {
        Object o;
        o = map.get(name);
        if (o == null)
            throw(new JMSException("no value for " + name));
        else if (o instanceof Character)
            return ((Character) o).charValue();
        else
            throw(new MessageFormatException("invalid type for " + name));
    }

    public void setShort(String name, short x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Short(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Short)
                map.put(name, new Short(x));
            else if (o instanceof String)
                map.put(name, Short.toString(x));
            else if (o instanceof Integer)
                map.put(name, new Integer((int) x));
            else if (o instanceof Long)
                map.put(name, new Long((long) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public short getShort(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setInt(String name, int x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Integer(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Integer)
                map.put(name, new Integer(x));
            else if (o instanceof String)
                map.put(name, String.valueOf(x));
            else if (o instanceof Long)
                map.put(name, new Long((long) x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public int getInt(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setLong(String name, long x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Long(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Long)
                map.put(name, new Long(x));
            else if (o instanceof String)
                map.put(name, String.valueOf(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public long getLong(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setFloat(String name, float x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Float(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Float)
                map.put(name, new Float(x));
            else if (o instanceof Double)
                map.put(name, new Double((double) x));
            else if (o instanceof String)
                map.put(name, String.valueOf(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public float getFloat(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setDouble(String name, double x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));

        if (!map.containsKey(name))
            map.put(name, new Double(x));
        else {
            Object o = map.get(name);
            if (o == null || o instanceof Double)
                map.put(name, new Double(x));
            else if (o instanceof String)
                map.put(name, String.valueOf(x));
            else
                throw(new MessageFormatException("invalid type for " + name));
        }
    }

    public double getDouble(String name) throws JMSException {
        Object o;
        o = map.get(name);
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

    public void setString(String name, String x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));
        map.put(name, x);
    }

    public String getString(String name) throws JMSException {
        Object o;
        o = map.get(name);
        if (o == null && !(o instanceof String))
            throw(new MessageFormatException("invalid value"));
        else
            return (String) o;
    }

    public void setObject(String name, Object x) throws JMSException {
        if (readonly)
            throw(new MessageNotWriteableException("not writeable"));
        if (name == null)
            throw(new JMSException("failed to set value with name of null"));
        if (!isValid(x) && !(x instanceof byte[]))
            throw(new MessageFormatException("invalid object"));
        map.put(name, x);
    }

    public Object getObject(String name) throws JMSException {
        return map.get(name);
    }

    public Enumeration getMapNames() throws JMSException {
       return new org.qbroker.common.Enumerator(map.keySet().iterator());
    }

    public boolean itemExists(String name) throws JMSException {
        return map.containsKey(name);
    }

    public void clearBody() throws JMSException {
        map.clear();
        readonly = false;
    }

    protected void finalize() {
        super.finalize();
        map.clear();
    }
}
