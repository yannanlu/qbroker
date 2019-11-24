package org.qbroker.aws;

/* STSClient.java - an AWS Client for accessing AWS Security Token service */

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.securitytoken.model.FederatedUser;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import com.amazonaws.services.securitytoken.model.AssumedRoleUser;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import org.qbroker.common.Utils;

/**
 * STSClient is an AWSClient for accessing AWS Security Token service.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class STSClient extends AWSClient {
    AWSSecurityTokenService sts;

    public STSClient(Map props) {
        super(props);

        sts = AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(getRegion(profileName))
            .build();
    }

    protected static Map getResponse(AWSSecurityTokenService client,
        Map<String, String> req) throws IOException {
        HashMap<String, String> resp = new HashMap<String, String>();
        Credentials creds;
        int d = 0;
        String role, policy, user, account, str;
        if ((role = req.get("role")) != null &&
            (user = req.get("user")) != null) { // assume role
            if ((str = req.get("duration")) == null)
                d = 3600;
            else try {
                d = Integer.parseInt(str);
            }
            catch (Exception e) {
                d = 3600;
            }
            if (d < 900 || d > 3600)
                d = 3600;
            account = req.get("account");
            if (account == null) {
                GetCallerIdentityRequest request=new GetCallerIdentityRequest();
                account = client.getCallerIdentity(request).getAccount();
            }
            String arn = "arn:aws:iam::" + account + ":role/" + role;
            AssumeRoleRequest request = new AssumeRoleRequest()
                .withDurationSeconds(d)
                .withRoleSessionName(user)
                .withRoleArn(arn);
            creds = client.assumeRole(request).getCredentials();
            resp.put("Action", "assumeRole");
            resp.put("AccessKeyId", creds.getAccessKeyId());
            resp.put("SecretAccessKey", creds.getSecretAccessKey());
            resp.put("SessionToken", creds.getSessionToken());
            resp.put("Expiration", Utils.dateFormat(creds.getExpiration()));
        }
        else if ((user = req.get("user")) != null) { // get federation token
            if ((str = req.get("duration")) == null)
                d = 12 * 3600;
            else try {
                d = Integer.parseInt(str);
            }
            catch (Exception e) {
                d = 12 * 3600;
            }
            if (d < 900)
                d = 12 * 3600;
            GetFederationTokenRequest request = new GetFederationTokenRequest()
                .withName(user)
                .withDurationSeconds(d);
            if ((policy = req.get("policy")) != null)
                request.withPolicy(policy);
            creds = client.getFederationToken(request).getCredentials();
            resp.put("Action", "getFederationToken");
            resp.put("AccessKeyId", creds.getAccessKeyId());
            resp.put("SecretAccessKey", creds.getSecretAccessKey());
            resp.put("SessionToken", creds.getSessionToken());
            resp.put("Expiration", Utils.dateFormat(creds.getExpiration()));
        }
        else { // get caller identity
            GetCallerIdentityRequest request = new GetCallerIdentityRequest();
            GetCallerIdentityResult result = client.getCallerIdentity(request);
            resp.put("UserId", result.getUserId());
            resp.put("Account", result.getAccount());
            resp.put("Arn", result.getArn());
            resp.put("Action", "getCallerIdentity");
        }
        return resp;
    }

    public Map getResponse(Map<String, String> req) throws IOException {
        String profile = req.get("profile"); 
        if (profile == null)
            return getResponse(sts, req);
        else { // dynamic session
            AWSSecurityTokenService client;
            AWSCredentials credentials = getCredentials(profile);
            client = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(getRegion(profile))
                .build();
            return getResponse(client, req);
        }
    }

    public List getList(Map<String, String> req) throws IOException {
        return new ArrayList<String>();
    }

    public void close() {
        super.close();
        sts = null;
    }

    public static void main(String[] args) {
        int i, duration = 3600;
        String profileName = null, policyName = null, account = null;
        Map<String, Object> props = new HashMap<String, Object>();
        Map<String, String> req = new HashMap<String, String>();
        File file = null;
        STSClient sts;
        boolean needUrl = false;
        profileName = System.getProperty("AWS_PROFILE", "default");
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'U':
                needUrl = true;
                break;
              case 'a':
                if (i+1 < args.length)
                    account = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    file = new File(args[++i]);
                break;
              case 'd':
                if (i+1 < args.length)
                    duration = Integer.parseInt(args[++i]);
                break;
              case 'r':
                if (i+1 < args.length)
                    req.put("role", args[++i]);
                break;
              case 'u':
                if (i+1 < args.length)
                    req.put("user", args[++i]);
                break;
              case 'p':
                if (i+1 < args.length)
                  profileName = args[++i];
                break;
              case 'P':
                if (i+1 < args.length)
                  policyName = args[++i];
                break;
              default:
            }
        }
        props.put("Profile", profileName);
        props.put("URI", "sts://amazon.com");
        try {
            sts = new STSClient(props);
            if (req.get("role") != null) {
                Map resp = sts.getResponse(req);
                System.out.println("AccessKeyId\tSecretAccessKey\t" +
                    "SessionToken\tExpiration");
                System.out.println((String) resp.get("AccessKeyId") + "\t" + 
                    (String) resp.get("SecretAccessKey") + "\t" +
                    (String) resp.get("SessionToken") + "\t" +
                    (String) resp.get("Expiration"));
            }
            else if (file != null) {
                StringBuffer strBuf = new StringBuffer();
                char[] buffer = new char[4096];
                Reader in = new FileReader(file);
                while ((i = in.read(buffer, 0, 4096)) >= 0) {
                    if (i > 0)
                        strBuf.append(new String(buffer, 0, i));
                }
                in.close();
                req.put("policy", strBuf.toString());
                Map resp = sts.getResponse(req);
                System.out.println("AccessKeyId\tSecretAccessKey\t" +
                    "SessionToken\tExpiration");
                System.out.println((String) resp.get("AccessKeyId") + "\t" + 
                    (String) resp.get("SecretAccessKey") + "\t" +
                    (String) resp.get("SessionToken") + "\t" +
                    (String) resp.get("Expiration"));
            }
            else if (needUrl) {
                BasicProfile pf = sts.getProfile(profileName);
                if (pf != null && pf.getAwsSessionToken() != null) {
                    String str = getConsoleUrl(pf.getAwsAccessIdKey(),
                        pf.getAwsSecretAccessKey(), pf.getAwsSessionToken());
                    System.out.println(str);
                }
                else
                   System.out.println("No SessionToken found for "+profileName);
            }
            else {
                Map resp = sts.getResponse(req);
                System.out.println("UserId\tAccount\tArn");
                System.out.println((String) resp.get("UserId") + "\t" + 
                    (String) resp.get("Account") + "\t" +
                    (String) resp.get("Arn"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("STSClient Version 1.0 (written by Yannan Lu)");
        System.out.println("STSClient: an AWS STS client");
        System.out.println("Usage: java org.qbroker.aws.STSClient -p profile -u user");
        System.out.println("  -?: print this message");
        System.out.println("  -U: need console url");
        System.out.println("  -u: username");
        System.out.println("  -r: rolename");
        System.out.println("  -f: filename of the policy document");
        System.out.println("  -p: profile name");
    }
}
