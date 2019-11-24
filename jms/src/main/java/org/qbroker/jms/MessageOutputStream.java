package org.qbroker.jms;

/* MessageOutputStream.java - an OutputStream on a BytesMessage */

import java.io.OutputStream;
import java.io.IOException;
import javax.jms.BytesMessage;

/**
 * MessageOutputStream is an OutputStream on a given JMS BytesMessage.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class MessageOutputStream extends OutputStream {
    private final BytesMessage msg;

    public MessageOutputStream(BytesMessage msg) {

        if (msg != null)
            this.msg = msg;
        else
            throw(new IllegalArgumentException("null BytesMessage"));
        try {
            msg.clearBody();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
    }

    public void write(byte[] buf, int off, int len) throws IOException {
        try {
            msg.writeBytes(buf, off, len);
        }
        catch (Exception e) {
            throw(new IOException("failed to write bytes to msg: " +
                e.toString()));
        }
    }

    public void write(byte[] buf) throws IOException {
        try {
            msg.writeBytes(buf);
        }
        catch (Exception e) {
            throw(new IOException("failed to write bytes to msg: " +
                e.toString()));
        }
    }

    public void write(int b) throws IOException {
        try {
            msg.writeByte((byte) b);
        }
        catch (Exception e) {
            throw(new IOException("failed to write a byte to msg: " +
                e.toString()));
        }
    }

    public void flush() {
    }

    public void close() {
    }
}
