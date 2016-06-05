package org.qbroker.jms;

/* MessageInputStream.java - an InputStream on a BytesMessage */

import java.io.InputStream;
import java.io.IOException;
import javax.jms.BytesMessage;

/**
 * MessageInputStream is an InputStream on a given JMS BytesMessage.
 * So you can use it to read the content of the message.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MessageInputStream extends InputStream {
    private final BytesMessage msg;
    private int bytesAvailable = 0;

    public MessageInputStream(BytesMessage msg) {

        if (msg != null)
            this.msg = msg;
        else
            throw(new IllegalArgumentException("null BytesMessage"));

        try {
            reset();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
    }

    public int read(byte[] buf, int off, int len) throws IOException {
        if (len < off)
            throw(new IOException("length is too small"));
        else if (off > 0) {
            byte[] buffer = new byte[len - off];
            try {
                len = msg.readBytes(buffer);
            }
            catch (Exception e) {
                throw(new IOException("failed to read bytes from msg: " +
                    e.toString()));
            }
            if (len > 0) {
                System.arraycopy(buffer, 0, buf, off, len);
                bytesAvailable -= len;
            }
            return len;
        }
        else try {
            int n = msg.readBytes(buf, len);
            if (n > 0)
                bytesAvailable -= n;
            return n;
        }
        catch (Exception e) {
            throw(new IOException("failed to read bytes from msg: " +
                e.toString()));
        }
    }

    public int read(byte[] buf) throws IOException {
        try {
            int n = msg.readBytes(buf);
            if (n > 0)
                bytesAvailable -= n;
            return n;
        }
        catch (Exception e) {
            throw(new IOException("failed to read bytes from msg: " +
                e.toString()));
        }
    }

    public int read() throws IOException {
        try {
            int i = msg.readByte();
            bytesAvailable --;
            return i;
        }
        catch (Exception e) {
            throw(new IOException("failed to read a byte from msg: " +
                e.toString()));
        }
    }

    public void reset() throws IOException {
        try {
            bytesAvailable = (int) msg.getBodyLength();
            msg.reset();
        }
        catch (Exception e) {
            throw(new IOException("failed to reset msg: " + e.toString()));
        }
    }

    public int available() {
        return bytesAvailable;
    }

    public void mark(int i) {
    }

    public long skip(long n) throws IOException {
        return 0L;
    }

    public boolean markIsSupported() {
        return false;
    }

    public void close() {
        bytesAvailable = -1;
    }
}
