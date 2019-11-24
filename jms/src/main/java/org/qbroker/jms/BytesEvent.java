package org.qbroker.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageNotReadableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.qbroker.common.Utils;
import org.qbroker.common.BytesBuffer;
import org.qbroker.jms.JMSEvent;

/**
 * BytesEvent is an Event that implements JMS BytesMessage
 *<br>
 * @author yannanlu@yahoo.com
 */

public class BytesEvent extends JMSEvent implements javax.jms.BytesMessage {
    private BytesBuffer bos;
    private ByteArrayInputStream bis;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private long size = 0L;
    private final long MAXLENGTH;
    private boolean readonly;

    public BytesEvent(int maxSize) throws JMSException {
        super();
        if (maxSize > 0) {
            MAXLENGTH = maxSize;
        }
        else
            MAXLENGTH = 4194304;
        try {
            bos = new BytesBuffer();
            oos = new ObjectOutputStream(bos);
        }
        catch (IOException e) {
            throw(new JMSException("failed to init"));
        }
        bis = null;
        ois = null;
        writeable = true;
        readonly = false;
        body = bos;
    }

    public BytesEvent() throws JMSException {
        this(4194304);
    }

    public void writeBytes(byte[] value, int offset, int len)
        throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        if (value == null || offset < 0 || len < 0)
            throw(new JMSException("null buffer or negative numbers"));
        try {
            oos.write(value, offset, len);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public void writeBytes(byte[] value) throws JMSException {
        if (value == null)
            throw(new JMSException("null buffer"));
        writeBytes(value, 0, value.length);
    }

    public void writeByte(byte value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));
        try {
            oos.write((int) value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public int readBytes(byte[] value, int len) throws JMSException {
        int i = 0;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        if (value == null)
            throw(new JMSException("null buffer"));

        try {
            i = ois.read(value, 0, len);
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }
        return i;
    }

    public int readBytes(byte[] value) throws JMSException {
        if (value == null)
            throw(new JMSException("null buffer"));
        return readBytes(value, value.length);
    }

    public byte readByte() throws JMSException {
        byte i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readByte();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }
        return i;
    }

    public int readUnsignedByte() throws JMSException {
        int i = (int) readByte();
        if (i >= 0)
            return i;
        else
            return 256 + i;
    }

    public void writeInt(int value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeInt(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public int readInt() throws JMSException {
        int i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readInt();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeShort(short value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeShort(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public short readShort() throws JMSException {
        short i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readShort();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public int readUnsignedShort() throws JMSException {
        int i = (int) readShort();
        if (i >= 0)
            return i;
        else
            return 65536 + i;
    }

    public void writeChar(char value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeChar(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public char readChar() throws JMSException {
        char i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readChar();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeBoolean(boolean value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeBoolean(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public boolean readBoolean() throws JMSException {
        boolean i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readBoolean();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeLong(long value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeLong(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public long readLong() throws JMSException {
        long i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readLong();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeUTF(String value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeUTF(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public String readUTF() throws JMSException {
        String s;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            s = ois.readUTF();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return s;
    }

    public void writeDouble(double value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeDouble(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public double readDouble() throws JMSException {
        double i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readDouble();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeFloat(float value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeFloat(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public float readFloat() throws JMSException {
        float i;
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        try {
            i = ois.readFloat();
        }
        catch (IOException e) {
            throw(new JMSException("failed to read: " + Utils.traceStack(e)));
        }

        return i;
    }

    public void writeObject(Object value) throws JMSException {
        if (readonly || oos == null)
            throw(new MessageNotWriteableException("not writeable"));

        try {
            oos.writeObject(value);
        }
        catch (IOException e) {
            throw(new JMSException("failed to write: " + Utils.traceStack(e)));
        }
    }

    public synchronized void reset() throws JMSException {
        if (readonly) {
            bis.reset();
            try {
                ois.close();
                ois = new ObjectInputStream(bis);
            }
            catch (IOException e) {
               throw(new JMSException("failed to reset: "+Utils.traceStack(e)));
            }
            return;
        }
        readonly = true;
        try {
            oos.flush();
            oos.reset();
            bos.flush();
            size = bos.getCount();
            bis = new ByteArrayInputStream(bos.toByteArray());
            ois = new ObjectInputStream(bis);
            bos.reset();
        }
        catch (IOException e) {
            throw(new JMSException("failed to reset: " + Utils.traceStack(e)));
        }
        body = bis;
        try {
            oos.close();
            bos.close();
            oos = null;
            bos = null;
        }
        catch (IOException e) {
        }
    }

    public synchronized void clearBody() throws JMSException {
        readonly = false;
        size = 0L;
        try {
            if (ois != null) {
                ois.close();
                ois = null;
            }
            if (bis != null) {
                bis.close();
                bis = null;
            }
            if (oos != null)
                oos.close();
            if (bos != null)
                bos.close();
        }
        catch (Exception e) {
        }
        try {
            bos = new BytesBuffer();
            oos = new ObjectOutputStream(bos);
        }
        catch (IOException e) {
            throw(new JMSException("failed to clear: " + Utils.traceStack(e)));
        }
        body = bos;
    }

    public long getBodyLength() throws JMSException {
        if (!readonly || ois == null)
            throw(new MessageNotReadableException("not readable"));

        return size;
    }

    protected void finalize() {
        super.finalize();
        try {
            if (ois != null) {
                ois.close();
                ois = null;
            }
            if (oos != null) {
                oos.close();
                oos = null;
            }
            if (bis != null) {
                bis.close();
                bis = null;
            }
            if (bos != null) {
                bos.close();
                bos = null;
            }
        }
        catch (Exception e) {
        }
    }
}
