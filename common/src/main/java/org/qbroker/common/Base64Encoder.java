package org.qbroker.common;

/* Base64Encoder.java - a base64 encoder/decoder with encryption support */

import java.util.Arrays;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.qbroker.common.RandomNumber;

/**
 * Base64Encoder has encryption support.
 *<br>
 * @author yannanlu@yahoo.com
 */
public class Base64Encoder {
    private String name;
    private int key;

    private final static byte[] toBase64 = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
        'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
        'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
        'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
        'w', 'x', 'y', 'z', '0', '1', '2', '3',
        '4', '5', '6', '7', '8', '9', '+', '/'};

    public Base64Encoder(String name, int key) {
        this.name = name;
        this.key = key;
    }

    /**
     * returns encrypted Base64 encoded bytes
     */
    public byte[] encrypt(byte[] input) {
        if (key != 0) {
            RandomNumber rand = new RandomNumber(key);
            int i, k, shift;
            byte[] output = encode(input);
            for (i=0; i<output.length; i++) {
                if (output[i] == '=')
                    break;
                k = fromBase64(output[i]);
                shift = (int) (64.0 * rand.getNext());
                output[i] = toBase64[(k+shift)%64];
            }
            return output;
        }
        else
            return encode(input);
    }

    /**
     * returns decrypted Base64 decoded bytes
     */
    public byte[] decrypt(byte[] input) {
        if (key != 0) {
            RandomNumber rand = new RandomNumber(key);
            int i, k, shift;
            byte[] output = new byte[input.length];
            for (i=0; i<output.length; i++) {
                if (input[i] != '=') {
                    k = fromBase64(input[i]);
                    shift = 64 - (int) (64.0 * rand.getNext());
                    output[i] = toBase64[(k+shift)%64];
                }
                else
                    output[i] = input[i];
            }
            return decode(output);
        }
        else
            return decode(input);
    }

    public static byte[] encode(byte[] input) {
        int i, n, k;
        if (input == null || (n = input.length) <= 0)
            return null;
        k = n / 3;
        if ((n % 3) != 0)
            k ++;

        byte[] output = new byte[4*k];
        k = 0;
        for (i=2; i<n; i+=3) {
            output[k++] = toBase64[(input[i-2] & 0xfc) >> 2];
            output[k++] = toBase64[((input[i-2] & 0x03) << 4) |
                ((input[i-1] & 0xf0) >> 4)];
            output[k++] = toBase64[((input[i-1] & 0x0f) << 2) |
                ((input[i] & 0xc0) >> 6)];
            output[k++] = toBase64[input[i] & 0x3f];
        }

        if (i == n) { // n % 3 = 2
            output[k++] = toBase64[(input[i-2] & 0xfc) >> 2];
            output[k++] = toBase64[((input[i-2] & 0x03) << 4) |
                ((input[i-1] & 0xf0) >> 4)];
            output[k++] = toBase64[((input[i-1] & 0x0f) << 2)];
            output[k++] = '=';
        }
        else if (i == n + 1) { // n % 3 = 1
            output[k++] = toBase64[(input[i-2] & 0xfc) >> 2];
            output[k++] = toBase64[(input[i-2] & 0x03) << 4];
            output[k++] = '=';
            output[k++] = '=';
        }
        return output;
    }

    public static byte[] decode(byte[] input) {
        int i, n, k, m;
        byte a, b, c, d;
        if (input == null || (n = input.length) < 4)
            return null;
        k = n / 4;
        k *= 3;
        m = n - 4;
        if (input[n-2] == '=')
            k -= 2;
        else if (input[n-1] == '=')
            k --;
        else
            m = n;

        byte[] output = new byte[k];
        k = 0;
        for (i=0; i<m; i+=4) {
            a = fromBase64(input[i]);
            b = fromBase64(input[i+1]);
            c = fromBase64(input[i+2]);
            d = fromBase64(input[i+3]);
            output[k++] = (byte) (((a & 0x3f) << 2) | ((b & 0x30) >> 4));
            output[k++] = (byte) (((b & 0x0f) << 4) | ((c & 0x3c) >> 2));
            output[k++] = (byte) (((c & 0x03) << 6) | (d & 0x3f));
        }
        if (input[n-2] == '=') {
            a = fromBase64(input[i]);
            b = fromBase64(input[i+1]);
            output[k++] = (byte) (((a & 0x3f) << 2) | ((b & 0x30) >> 4));
        }
        else if (input[n-1] == '=') {
            a = fromBase64(input[i]);
            b = fromBase64(input[i+1]);
            c = fromBase64(input[i+2]);
            output[k++] = (byte) (((a & 0x3f) << 2) | ((b & 0x30) >> 4));
            output[k++] = (byte) (((b & 0x0f) << 4) | ((c & 0x3c) >> 2));
        }
        return output;
    }

    /**
     * returns encrypted Base64 encoded string with a given prefix of the key
     */
    public static String encrypt(String key, String value)
        throws UnsupportedEncodingException {
        if (value == null || value.length() <= 0)
            return null;
        if (key == null || key.length() <= 0)
            return new String(encode(value.getBytes(StandardCharsets.UTF_8)));
        else {
            int i, k, shift = 0;
            long t = System.currentTimeMillis() / 60000;
            RandomNumber rand = new RandomNumber((int) t);
            for (i=0; i<64; i++)
                shift = (int) (64.0 * rand.getNext());
            key += ":" + value;
            byte[] output = encode(key.getBytes(StandardCharsets.UTF_8));
            for (i=0; i<output.length; i++) {
                if (output[i] == '=')
                    break;
                k = fromBase64(output[i]);
                output[i] = toBase64[(k+shift)%64];
            }
            return new String(output, StandardCharsets.UTF_8);
        }
    }

    /**
     * returns base64 decoded value of an encrypted string with the given prefix
     */
    public static String decrypt(String key, String encrypted)
        throws UnsupportedEncodingException {
        int len;
        if (encrypted == null || encrypted.length() <= 0)
            return null;
        if (key == null || (len = key.length()) <= 0) {
            byte[] output = decode(encrypted.getBytes(StandardCharsets.UTF_8));
            return new String(output, StandardCharsets.UTF_8);
        }
        else {
            int shift;
            len += 1;
            key += ":";
            byte[] input = encrypted.getBytes(StandardCharsets.UTF_8);
            byte[] ref = key.getBytes(StandardCharsets.UTF_8);
            byte[] buffer = new byte[input.length];
            for (shift=0; shift<64; shift++) {
                for (int i=0; i<input.length; i++) {
                    if (input[i] == '=')
                        buffer[i] = input[i];
                    else {
                        int k = fromBase64(input[i]);
                        buffer[i] = toBase64[(k+shift)%64];
                    }
                }
                byte[] output = decode(buffer);
                if (Arrays.equals(ref, Arrays.copyOfRange(output, 0, len)))
                    return new String(output, len, output.length - len);
            }
            throw(new IllegalArgumentException("header is missing"));
        }
    }

    /**
     * verifies the given header on a base64 encoded string and scrambles
     * it with a random shuffling. It returns the scrambled string upon
     * success or the original string otherwise.
     */
    public static String scramble(String header, String encoded)
        throws UnsupportedEncodingException {
        int len;
        if (header == null || (len = header.length()) <= 0)
            throw(new IllegalArgumentException("header is null"));
        if (encoded == null || encoded.length() <= len)
            return encoded;
        else {
            int k, shift = 1;
            long t;
            byte[] input = encoded.getBytes(StandardCharsets.UTF_8);
            byte[] ref = header.getBytes(StandardCharsets.UTF_8);
            byte[] output = decode(input);
            if (output == null || output.length < len)
                return encoded;
            if (!Arrays.equals(ref, Arrays.copyOfRange(output, 0, len)))
                return encoded;
            t = System.currentTimeMillis() / 60000;
            RandomNumber rand = new RandomNumber((int) t);
            for (int i=0; i<64; i++)
                shift = (int) (64.0 * rand.getNext());
            for (int i=0; i<input.length; i++) {
                if (input[i] == '=')
                    break;
                k = fromBase64(input[i]);
                input[i] = toBase64[(k+shift)%64];
            }
            return new String(input, StandardCharsets.UTF_8);
        }
    }

    /**
     * unscrambles a scrambled string and verifies the given header on the
     * end result. It returns the unscrambled string upon success or the
     * scrambled string otherwise.
     */
    public static String unscramble(String header, String scrambled)
        throws UnsupportedEncodingException {
        int len;
        if (header == null || (len = header.length()) <= 0)
            throw(new IllegalArgumentException("header is null"));
        if (scrambled == null || scrambled.length() < len)
            return scrambled;
        else {
            byte[] input = scrambled.getBytes(StandardCharsets.UTF_8);
            byte[] output = new byte[input.length];
            byte[] ref = header.getBytes(StandardCharsets.UTF_8);
            for (int shift=0; shift<64; shift++) {
                for (int i=0; i<input.length; i++) {
                    if (input[i] == '=')
                        output[i] = input[i];
                    else {
                        int k = fromBase64(input[i]);
                        output[i] = toBase64[(k+shift)%64];
                    }
                }
                if (Arrays.equals(ref,Arrays.copyOfRange(decode(output),0,len)))
                    return new String(output, StandardCharsets.UTF_8);
            }
            return scrambled;
        }
    }

    /**
     * returns the position of the byte in the map or throws Exception
     * if the byte is not base64 encoded
     */
    private static byte fromBase64(byte x) {
        if (x >= 'A' && x <= 'Z') {
            return (byte) (0x00 + (x - 'A'));
        }
        else if (x >= 'a' && x <= 'z') {
            return (byte) (0x1a + (x - 'a'));
        }
        else if (x >= '0' && x <= '9') {
            return (byte) (0x34 + (x - '0'));
        }
        else if (x == '+') {
            return 0x3e;
        }
        else if (x == '/') {
            return 0x3f;
        }
        throw(new IllegalArgumentException("byte out of range: " + x));
    }

    public String getName() {
        return name;
    }
}
