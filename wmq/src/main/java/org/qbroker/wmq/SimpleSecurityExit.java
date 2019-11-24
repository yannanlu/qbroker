package org.qbroker.wmq;

/* SimpleSecurityExit.java - a channel security exit for client only */

import com.ibm.mq.MQChannelExit;
import com.ibm.mq.MQChannelDefinition;
import com.ibm.mq.MQSecurityExit;
import com.ibm.mq.MQC;

/**
 * SimpleSecurityExit sets the username and password and sends them over
 * for the authentication on the server side.  If the application does not
 * specify the password, it will use the default username and password of the
 * exit, instead.  The default username and password can be defined with
 * the string of username:password via the field of either SCYUSERSATA or
 * SecurityData.  In case there is no security exit enabled on the server side,
 * it will skip the exit and resume the connection as if there is no
 * security exit enabled.  It supports both MQ V5.3 and MQ V6.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class SimpleSecurityExit implements MQSecurityExit {
    private String defaultUserid = null;
    private String defaultPasswd = null;

    public SimpleSecurityExit(String data) {
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
        String username;
        String password;
        byte[] userBytes;
        String authPref = "auth";
        String agentData = null;
        //login prompt from queue manager exit
        String loginPrompt = "login:\0";
        byte[] expRecMsgBytes = null;
        int i, len;

        //determine reason for invocation
        switch (channelExitParms.exitReason) {
          // MQXR_INIT: exit initialization - return MQXCC_OK 
          case MQC.MQXR_INIT:
            channelExitParms.exitResponse = MQC.MQXCC_OK;
            break;                                            

          // MQXR_INIT_SEC: No actions since server initiates the exchange
          case MQC.MQXR_INIT_SEC:
            channelExitParms.exitResponse = MQC.MQXCC_OK;
            break;                                            

          // MQXR_SEC_MSG: receive message from queue manager exit and then
          //  send auth data
          case MQC.MQXR_SEC_MSG:
            if (agentBuffer == null || agentBuffer.length == 0) {
                //if no data sent from queue manager exit, maybe the qmgr
                // does not have security exit enabled, return MQXCC_OK
                channelExitParms.exitResponse = MQC.MQXCC_OK;
            }
            else { // convert expected message to bytes
                try {
                    expRecMsgBytes = loginPrompt.getBytes("UTF8");
                }
                catch (Exception e) {
                }
                //check if message received in agentBuffer matches login prompt
                len = (agentBuffer.length > expRecMsgBytes.length) ?
                    expRecMsgBytes.length : agentBuffer.length;
                for (i=0; i<len; i++) {
                    if (agentBuffer[i] != expRecMsgBytes[i])
                        break;
                }
                if (i >= len) { // perfect match on the login prompt
                    //get the client userid and password
                    username = channelDefParms.remoteUserId.trim();
                    password = channelDefParms.remotePassword.trim();
                    if (defaultUserid != null && password.length() <= 0) {
                        username = defaultUserid;
                        password = defaultPasswd;
                    }
                    //reset agentBuffer and send auth data
                    agentData = new String(authPref+username + ":" + password);
                    //convert to bytes and assign to agentBuffer
                    try {
                        agentBuffer = agentData.getBytes("UTF8"); 
                    }
                    catch (Exception e) {
                        agentBuffer = null;
                    }
                    //return SEND_SEC_MSG to send message in agentBuffer
                    channelExitParms.exitResponse = MQC.MQXCC_SEND_SEC_MSG;
                }
                else {
                    //inbound agentBuffer does not match expected message
                    // so close channel
                    channelExitParms.exitResponse = MQC.MQXCC_SUPPRESS_FUNCTION;
                }
          
                //reset password fields
                password = null;
                username = null;
            }
            break;

          // MQXR_TERM: 
          case MQC.MQXR_TERM:
            //return MQXCC_OK at exit termination               
            channelExitParms.exitResponse = MQC.MQXCC_OK;
            break;

          // MQXR_SEC_PARMS: required by MQ V6
          case 29:
            //return MQXCC_OK
            channelExitParms.exitResponse = MQC.MQXCC_OK;
            break;

          // Unexpected call: Close the connection.
          default:
            channelExitParms.exitResponse = MQC.MQXCC_SUPPRESS_FUNCTION;
            break;
        }
        // Always return the buffer of data passed in.                  
        return agentBuffer;
    }
}
