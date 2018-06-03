package org.qbroker.common;

/* AES.java - a utility for encryption and decryption with AES-256-CBC */

import java.util.Arrays;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import org.qbroker.common.Base64Encoder;
 
public class AES {
    /**
     * returns a base64 encoded string of the encrypted result with the password
     */
    public static String encrypt(String strToEncrypt, String password)
        throws UnsupportedEncodingException, GeneralSecurityException {
        byte[] result, salt = new byte[8];
        byte[] pass = password.getBytes(StandardCharsets.UTF_8);

        new SecureRandom().nextBytes(salt);
        MessageDigest md = MessageDigest.getInstance("MD5");
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        initCipher(Cipher.ENCRYPT_MODE, cipher, md, pass, salt);
        byte[] encrypted =
            cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8));
        result = new byte[encrypted.length + 16];
        for (int i=0; i<encrypted.length; i++)
            result[i+16] = encrypted[i];
        result[0] = 'S';
        result[1] = 'a';
        result[2] = 'l';
        result[3] = 't';
        result[4] = 'e';
        result[5] = 'd';
        result[6] = '_';
        result[7] = '_';
        for (int i=0; i<8; i++)
            result[i+8] = salt[i];
        return new String(Base64Encoder.encode(result), StandardCharsets.UTF_8);
    }
 
    /**
     * returns decrypted value of an encrypted string with the password or 
     * null upon failure
     */
    public static String decrypt(String strToDecrypt, String password)
        throws UnsupportedEncodingException, GeneralSecurityException {
        byte[] pass = password.getBytes(StandardCharsets.UTF_8);
        byte[] input =
            Base64Encoder.decode(strToDecrypt.getBytes(StandardCharsets.UTF_8));
        byte[] header = Arrays.copyOfRange(input, 0, 8);
        byte[] salt = Arrays.copyOfRange(input, 8, 16);

        if (!Arrays.equals(header,"Salted__".getBytes(StandardCharsets.UTF_8)))
            throw(new IllegalArgumentException("missing header for salt"));
        else {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            initCipher(Cipher.DECRYPT_MODE, cipher, md, pass, salt);
            byte[] output = Arrays.copyOfRange(input, 16, input.length);
            return new String(cipher.doFinal(output), StandardCharsets.UTF_8);
        }
    }

    /**
     * returns decrypted value of an encrypted string with the password load
     * from the password file defined in the system properties
     */
    public static String decrypt(String strToDecrypt) throws IOException,
        GeneralSecurityException {
        String password = null;
        String filename = System.getProperty("PluginPasswordFile");
        if (filename == null)
            throw(new IOException("PluginPasswordFile is not defined"));
        else if ((password = getPassword(filename)) == null)
            throw(new IOException("PluginPasswordFile is empty"));
        else
            return decrypt(strToDecrypt, password);
    }

    private static int initCipher(int mode, Cipher cipher,
        MessageDigest md, byte[] pass, byte[] salt)
        throws InvalidKeyException, InvalidAlgorithmParameterException {
        int k;
        byte[] buffer;
        byte[] key = new byte[32];
        String algorithm = cipher.getAlgorithm();
        md.reset();
        md.update(pass);
        md.update(salt);
        buffer = md.digest();
        k = buffer.length;
        for (int i=0; i<buffer.length; i++)
            key[i] = buffer[i];
        md.reset();
        md.update(buffer);
        md.update(pass);
        md.update(salt);
        buffer = md.digest();
        for (int i=0; i<buffer.length; i++)
            key[i+k] = buffer[i];
        md.reset();
        md.update(buffer);
        md.update(pass);
        md.update(salt);
        buffer = md.digest();
        if ((k = algorithm.indexOf("/")) > 0)
            algorithm = algorithm.substring(0, k);
        cipher.init(mode, new SecretKeySpec(key, algorithm),
            new IvParameterSpec(buffer));
        return 0;
    }

    /** returns the password read from the file or null upon failure */
    public static String getPassword(String filename) throws IOException {
        String str = getContent(filename);
        if (str != null)
            return str.trim();
        else
            return null;
    }

    private static String getContent(String filename) throws IOException {
        byte[] buffer = new byte[4096];
        String str;
        if ("-".equals(filename))
            str = Utils.read(System.in, buffer);
        else {
            FileInputStream fs = new FileInputStream(filename);
            str = Utils.read(fs, buffer);
            fs.close();
        }
        return str;
    }

    public static void main(String[] args) {
        String password = "test";
        String filename = null;
        String infile = null;
        String text = null;
        String action = "encrept";

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'a':
                if (i+1 < args.length)
                    action = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              case 'i':
                if (i+1 < args.length)
                    infile = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    password = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    text = args[++i];
                break;
              default:
            }
        }
     
        try {
            String str;
            if (password == null && filename != null)
                password = getPassword(filename);
            if (password == null || password.length() <= 0)
                System.err.println("password can not be empty");
            if (text == null && infile != null)
                text = getContent(infile);
            if (text == null || text.length() <= 0)
                System.err.println("no content to " + action);
            if ("encrypt".equals(action)) {
                str = AES.encrypt(text, password);
                if (infile != null)
                    System.out.print(str);
                else
                    System.out.println(action + ": " + text + "\n" + str);
            }
            else if ("decrypt".equals(action)) {
                str = AES.decrypt(text, password);
                if (infile != null)
                    System.out.print(str);
                else
                    System.out.println(action + ": " + text + "\n" + str);
            }
            else if ("test".equals(action)) {
                str = Utils.decrypt(text);
                if (infile != null)
                    System.out.print(str);
                else
                    System.out.println(action + ": " + text + "\n" + str);
            }
            else if ("scramble".equals(action)) {
                str = Base64Encoder.scramble("Salted__", text);
                if (infile != null)
                    System.out.print(str);
                else if (!text.equals(str))
                    System.out.println(action + ": " + text + "\n" + str);
                else
                    System.err.println(action + " failed on " + text);
            }
            else if ("unscramble".equals(action)) {
                str = Base64Encoder.unscramble("Salted__", text);
                if (infile != null)
                    System.out.print(str);
                else if (!text.equals(str))
                    System.out.println(action + ": " + text + "\n" + str);
                else
                    System.err.println(action + " failed on " + text);
            }
            else {
                throw(new IllegalArgumentException(action +
                    " not supported"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("AES Version 1.0 (written by Yannan Lu)");
        System.out.println("AES: an OpenSSL-like utility to encrypt or decrypt a text");
        System.out.println("Usage: java org.qbroker.common.AES -p password -t text -a action");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of encrypt, decrypt, scramble and unscramble");
        System.out.println("  -t: text to be processed");
        System.out.println("  -p: password");
        System.out.println("  -f: full path to the password file");
        System.out.println("  -i: full path to the input file");
    }
}
