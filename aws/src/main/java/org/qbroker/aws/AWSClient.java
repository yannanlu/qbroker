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

public abstract class AWSClient {
    protected Map<String, BasicProfile> cache = null;
    protected AWSCredentials credentials = null;
    protected String profileName = null;
    protected String defaultRegion = null;
    protected String uri;

    public AWSClient(Map props) {
        Object o;
        String accessKeyId = null, secretAccessKey = null;

        if ((o = props.get("Profile")) != null)
            profileName = (String) o;
        else
            profileName = "default";

        try {
            cache = new ProfilesConfigFile().getAllBasicProfiles();
        }
        catch (Exception e) {
            cache = new HashMap<String, BasicProfile>();
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
            ph.clear();
        }

        if ((credentials = getCredentials(profileName)) == null)
            throw(new IllegalArgumentException("failed to get profile for: " +
                profileName));
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
