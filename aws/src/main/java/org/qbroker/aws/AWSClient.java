package org.qbroker.aws;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.auth.profile.internal.ProfileStaticCredentialsProvider;

import org.qbroker.common.Utils;
import org.qbroker.net.HTTPConnector;

public abstract class AWSClient {
    protected Map<String, BasicProfile> cache = null;
    protected AWSCredentials credentials = null;
    protected String profileName = null;
    protected String defaultRegion = null;
    protected String uri;

    public AWSClient(Map props) {
        Object o;
        String accessKeyId = null, secretAccessKey = null;

        cache = new HashMap<String, BasicProfile>();
        if ((o = props.get("Profile")) != null)
            profileName = (String) o;
        else
            profileName = "default";

        try {
            Map<String, BasicProfile> map =
                new ProfilesConfigFile().getAllBasicProfiles();
            for (String key : map.keySet())
                cache.put(key, map.get(key));
        }
        catch (Exception e) {
        }

        if ((o = props.get("Region")) != null)
            defaultRegion = (String) o;
        else
            defaultRegion = "us-east-1";

        if ((o = props.get("AccessKeyId")) != null) {
            HashMap<String, String> ph = new HashMap<String, String>();
            ph.put("aws_access_key_id", (String) o);
            if ((o = props.get("SecretAccessKey")) != null)
                ph.put("aws_secret_access_key", (String) o);
            else if ((o = props.get("EncryptedSecretAccessKey")) != null) try {
                ph.put("aws_secret_access_key", Utils.decrypt((String) o));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedSecretAccessKey: " + e.toString()));
            }
            if (!ph.containsKey("aws_secret_access_key"))
                throw(new IllegalArgumentException("SecretAccessKey or " +
                    "EncryptedSecretAccessKey not defined"));
            ph.put("region", defaultRegion);
            cache.put(profileName, new BasicProfile(profileName, ph));
        }
        else if (cache.size() <= 0)
            throw(new IllegalArgumentException("no credentials loaded for: " +
                profileName));

        if ((credentials = getCredentials(profileName)) == null)
            throw(new IllegalArgumentException("failed to get profile for: " +
                profileName));
    }

    /**
     * returns the region for the profileName or the default one if not found
     */
    public String getRegion(String profileName) {
        BasicProfile profile = cache.get(profileName);
        if (profile != null)
            return profile.getRegion();
        else
            return defaultRegion;
    }

    /**
     * returns the credentials for the profileName or null if not found
     */
    public AWSCredentials getCredentials(String profileName) {
        BasicProfile profile = cache.get(profileName);
        if (profile != null) {
            ProfileStaticCredentialsProvider provider =
                new ProfileStaticCredentialsProvider(profile);
            return provider.getCredentials();
        }
        else
            return null;
    }

    /**
     * returns the basic profile for the profileName or null if not found
     */
    public BasicProfile getProfile(String profileName) {
        return cache.get(profileName);
    }

    /** returns console url for the temporary credentials or null otherwise */
    public static String getConsoleUrl(String id, String key, String token)
        throws IOException {
        int i, n=0;
        if (id == null || id.length() <= 0 || key == null ||
            key.length() <= 0 || token == null || token.length() <= 0)
            return null;
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("{\"sessionId\":\"" + id);
        strBuf.append("\",\"sessionKey\":\"" + key);
        strBuf.append("\",\"sessionToken\":\"" + token + "\"}");
        String req = "https://signin.aws.amazon.com/federation" +
            "?Action=getSigninToken&Session=" + Utils.encode(strBuf.toString());
        Map<String, String> ph = new HashMap<String, String>();
        ph.put("URI", "https://signin.aws.amazon.com/federation");
        HTTPConnector conn = new HTTPConnector(ph);
        i = conn.doGet(req, strBuf);
        conn.close();
        ph.clear();
        if (i == 200 && (n = strBuf.length()) > 20) {
            strBuf.delete(n-2, n);
            strBuf.delete(0, 16);
            req = "?Action=login&issuer=emory.edu&Destination=";
            req += Utils.encode("https://console.aws.amazon.com/");
            req += "&SigninToken=" + strBuf.toString();
            return "https://signin.aws.amazon.com/federation" + req;
        }
        else
            throw(new IOException("failed to get response with error code: " +
                i + " / " + n));
    }

    public abstract Map getResponse(Map<String, String> req) throws IOException;

    public abstract List getList(Map<String, String> req) throws IOException;

    public void close() {
        if (cache != null) {
            cache.clear();
            cache = null;
        }
        credentials = null;
        profileName = null;
        uri = null;
    }
}
