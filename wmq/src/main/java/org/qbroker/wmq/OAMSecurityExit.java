package org.qbroker.wmq;

/* OAMSecurityExit.java - a security exit on OAM for MQ V6 client only */

import com.ibm.mq.MQChannelExit;
import com.ibm.mq.MQChannelDefinition;
import com.ibm.mq.MQSecurityExit;
import com.ibm.mq.MQConnectionSecurityParameters;
import com.ibm.mq.MQC;

/**
 * OAMSecurityExit creates an MQCSP object and sets the username and
 * password for the authentication on the server side. The default username
 * and password can be set via the String of SecurityData in the form of
 * username:password.  If the caller does not specify the password, the
 * default username and password will be set to the MQCSP object.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class OAMSecurityExit implements MQSecurityExit {
    private String defaultUserid = null;
    private String defaultPasswd = null;

    public OAMSecurityExit(String data) {
        if (data != null && data.length() > 0) {
             int i = data.indexOf(":");
             if (i >= 0) {
                 defaultUserid = data.substring(0, i);
                 defaultPasswd = data.substring(i+1);
             }
        }
    }

    public byte[] securityExit(MQChannelExit       channelExitParms,
                               MQChannelDefinition channelDefParms,
                               byte[]              agentBuffer) {
        String username, password;
        switch (channelExitParms.exitReason) {
          case MQC.MQXR_INIT:
            break;
          case MQC.MQXR_INIT_SEC:
            break;
          case MQC.MQXR_SEC_PARMS:
            MQConnectionSecurityParameters csp =
                new MQConnectionSecurityParameters();
            csp.setAuthenticationType(MQC.MQCSP_AUTH_USER_ID_AND_PWD);
            username = channelDefParms.remoteUserId.trim();
            password = channelDefParms.remotePassword.trim();
            if (defaultUserid != null && password.length() <= 0) {// set default
                username = defaultUserid;
                password = defaultPasswd;
            }
            csp.setCSPUserId(username);
            csp.setCSPPassword(password);
            channelExitParms.setMQCSP(csp);
            break;
          case MQC.MQXR_TERM:
          default:
            break;
        }
        return agentBuffer;
    }
}
