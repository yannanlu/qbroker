package org.qbroker.aws;

/* AWSSignature.java - AWS signature for REST calls to S3 */

import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.qbroker.common.Base64Encoder;

/**
 * AWSSignature provides a signature for REST calls to S3 service.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class AWSSignature {
    private Mac mac;
    private String awsId, awsKey;

    public AWSSignature(String awsId, String awsKey) {
        String algorithm = "HmacSHA1";
        SecretKeySpec keySpec;

        if (awsId == null || awsId.length() <= 0)
            throw(new IllegalArgumentException("AWS_ID is not defined"));

        this.awsId = awsId;
        if (awsKey == null || awsKey.length() <= 0)
            throw(new IllegalArgumentException("AWS_KEY is not defined"));
        this.awsKey = awsKey;

        keySpec = new SecretKeySpec(awsKey.getBytes(), algorithm);
        try {
            mac = Mac.getInstance(algorithm);
            mac.init(keySpec);
        }
        catch (NoSuchAlgorithmException e) {
            throw(new IllegalArgumentException(algorithm + " not supported: " +
                e.toString()));
        }
        catch (InvalidKeyException e) {
            throw(new IllegalArgumentException(awsKey + " not valid: " +
                e.toString()));
        }
    }

    public byte[] sign(byte[] msg) {
        if (msg == null)
            return null;
        return mac.doFinal(msg);
    }

    public String getAuthString(String method, String bucket, String path,
        String date, String type) {
        String str = method + "\n\n" + ((type == null) ? "" : type) + "\n" +
            date + "\n/" + bucket + ((path == null) ? "/" : path); 
        str = new String(Base64Encoder.encode(sign(str.getBytes())));
        return "AWS " + awsId + ":" + str;
    }

    public String getId() {
        return awsId;
    }

    public String getKey() {
        return awsKey;
    }

    public String getAlgorithm() {
        return mac.getAlgorithm();
    }
}
