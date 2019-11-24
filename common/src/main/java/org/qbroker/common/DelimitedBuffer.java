package org.qbroker.common;

/* DelimitedBuffer.java -  a byte buffer used for parsing stream */

/**
 * DelimitedBuffer is a byte buffer for parsing delimited content.
 * The delimiter can be defined at the beginning as SOT and/or at the
 * end as EOT.  However, EOT and SOT should never share any byte chars.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DelimitedBuffer {
    private byte[] buffer;
    private int bufferSize = 4096;
    private byte[] sotBytes = new byte[0];
    private byte[] eotBytes = new byte[0];
    private int sotPosition = 0;
    private int eotPosition = 0;
    private int offtail = 0;
    private int offhead = 0;
    private int boundary = 0;
    private int leftover = 0;
    public final static int MS_SOT = 1;
    public final static int MS_EOT = 2;

    /**
     * @param size the buffer size
     * @param offhead number of bytes to trim off the delimiter at the beginning
     * @param sot a hexString of the delimiter at the beginning
     * @param offtail number of bytes to trim off the delimiter at the end
     * @param eot a hexString of the delimiter at the end
     */
    public DelimitedBuffer(int size, int offhead, String sot,
        int offtail, String eot) {
        this(size, offhead, Utils.hexString2Bytes(sot), offtail,
            Utils.hexString2Bytes(eot));
    }

    public DelimitedBuffer(int size, int offhead, byte[] sot,
        int offtail, byte[] eot) {

        boundary = 0;
        if (eot != null && eot.length > 0) {
            eotBytes = new byte[eot.length];
            boundary += MS_EOT;
            for (int i=0; i<eot.length; i++)
                eotBytes[i] = eot[i];
        }
        if (sot != null && sot.length > 0) {
            sotBytes = new byte[sot.length];
            boundary += MS_SOT;
            for (int i=0; i<sot.length; i++)
                sotBytes[i] = sot[i];
        }

        if (offhead < 0)
            this.offhead = 0;
        else
            this.offhead = offhead;
        if (offtail < 0)
            this.offtail = 0;
        else
            this.offtail = offtail;

        if (size > 0)
            bufferSize = size; 

        buffer = new byte[bufferSize];
    }

    /**
     * It scans the byte array of the internal buffer for either EOT or
     * SOT strings starting from offset up to its end defined by length.
     * It returns the position immediately after the string.
     * If position is larger than 0, it is end of SOT.  If position is less
     * than 0, it is end of EOT.  Otherwise, it returns 0, nothing found.
     *<br><br>
     * It is ok not to define SOT and/or EOT.  However,
     * EOT and SOT must never share any same byte (no overlap)
     * This method is NOT MT-Safe since it requires the global
     * objects: sotPosition, eotPosition, buffer.
     */
    public int scan(int offset, int length) {
        int i, k;
        byte c;
        switch (boundary) {
          case 3: // both SOT and EOT defined
            for (i=offset; i<length; i++) {
                c = buffer[i];
                if (c == sotBytes[sotPosition]) {
                    eotPosition = 0;
                    k = i;
                    while (++sotPosition < sotBytes.length) {
                        if (++k >= length)
                            break;
                        if (buffer[k] != sotBytes[sotPosition])
                            break;
                    }
                    if (k >= length) { // end of buffer
                        return 0;
                    }
                    else if (sotPosition >= sotBytes.length) {
                        sotPosition = 0;
                        return k+1;
                    }
                    else {
                        sotPosition = 0;
                    }
                }
                else if (c == eotBytes[eotPosition]) {
                    sotPosition = 0;
                    k = i;
                    while (++eotPosition < eotBytes.length) {
                        if (++k >= length)
                            break;
                        if (buffer[k] != eotBytes[eotPosition])
                            break;
                    }
                    if (k >= length) { // end of buffer
                        return 0;
                    }
                    else if (eotPosition >= eotBytes.length) {
                        eotPosition = 0;
                        return -(k+1);
                    }
                    else {
                        eotPosition = 0;
                    }
                }
                else {
                    sotPosition = 0;
                    eotPosition = 0;
                }
            }
            break;
          case MS_SOT: // only SOT defined
            for (i=offset; i<length; i++) {
                c = buffer[i];
                if (c == sotBytes[sotPosition]) {
                    k = i;
                    while (++sotPosition < sotBytes.length) {
                        if (++k >= length)
                            break;
                        if (buffer[k] != sotBytes[sotPosition])
                            break;
                    }
                    if (sotPosition >= sotBytes.length) { // found SOT
                        sotPosition = 0;
                        return k+1;
                    }
                    else if (k < length) { // not at the end of buffer
                        sotPosition = 0;
                    }
                }
                else {
                    sotPosition = 0;
                }
            }
            break;
          case MS_EOT: // only EOT defined
            for (i=offset; i<length; i++) {
                c = buffer[i];
                if (c == eotBytes[eotPosition]) {
                    k = i;
                    while (++eotPosition < eotBytes.length) {
                        if (++k >= length)
                            break;
                        if (buffer[k] != eotBytes[eotPosition])
                            break;
                    }
                    if (k >= length) { // end of buffer
                        return 0;
                    }
                    else if (eotPosition >= eotBytes.length) {
                        eotPosition = 0;
                        return -(k+1);
                    }
                    else {
                        eotPosition = 0;
                    }
                }
                else {
                    eotPosition = 0;
                }
            }
            break;
          case 0: // neither SOT nor EOT defined
          default:
            break;
        }
        return 0;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public byte[] getSotBytes() {
        int n = sotBytes.length;
        byte[] s = new byte[n];
        for (int i=0; i<n; i++)
            s[i] = sotBytes[i];
        return s;
    }

    public byte[] getEotBytes() {
        int n = eotBytes.length;
        byte[] e = new byte[n];
        for (int i=0; i<n; i++)
            e[i] = eotBytes[i];
        return e;
    }

    public int getOfftail() {
        return offtail;
    }

    public int getOffhead() {
        return offhead;
    }

    public int getBoundary() {
        return boundary;
    }

    public int getLeftover() {
        return leftover;
    }

    public void setLeftover(int i) {
        leftover = i;
    }

    public void reset() {
        eotPosition = 0;
        sotPosition = 0;
        leftover = 0;
    }
}
