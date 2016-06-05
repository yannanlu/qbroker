package org.qbroker.common;

/* BytesBuffer.java - a wrapper of ByteArrayOutputStream */

import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * BytesBuffer is a byte array with an auto growable size.
 * NB. It is not MT-safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class BytesBuffer extends ByteArrayOutputStream {

    public BytesBuffer(int size) {
        super(size);
    }

    public BytesBuffer() {
        this(4096);
    }

    public int getCount() {
        return count;
    }

    public int delete(int start, int end) {
        if (start < 0 || start >= end || end > count)
            return -1;
        for (int i=end; i<count; i++)
            buf[start + i - end] = buf[i];
        count -= (end - start);
        return end - start;
    }

    public void writeTo(OutputStream out, int off, int len) throws IOException {
        out.write(buf, off, len);
    }

    public String substring(int start) {
        return substring(start, count);
    }

    public String substring(int start, int end) {
        if (end > count)
            end = count;
        if (start < 0)
            start = 0;
        return new String(buf, start, end);
    }

    public int indexOf(byte b) {
        return indexOf(b, 0, count);
    }

    public int indexOf(byte b, int start) {
        return indexOf(b, start, count);
    }

    public int indexOf(byte b, int start, int end) {
        if (start < 0 || start >= end || end > count)
            return -1;
        for (int i=start; i<end; i++)
            if (b == buf[i])
                return i;
        return -1;
    }
}
