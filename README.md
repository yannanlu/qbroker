# qbroker

This is a Maven 2 project for QBroker. It has been tested on build with Maven 3.3.3. The following jar files
have to be deployed to the local repository since they are not available in Maven Central:

| name                  |   groupId                  |   artifactId        |   version |
| ---                   |   ---                      |   ---               |   ---     |
| comm.jar              |   javax.comm               |   comm              |   2.0.3   |
| com.ibm.mq.jar        |   com.ibm                  |   com.ibm.mq        |   6.1     |
| com.ibm.mqjms.jar     |   com.ibm                  |   com.ibm.mqjms     |   6.1     |
| com.ibm.mq.pcf.jar    |   com.ibm                  |   com.ibm.mq.pcf    |   6.1     |
| dhbcore.jar           |   com.ibm                  |   dhbcore           |   6.1     |
| mfcontext.jar         |   com.progress.sonic.mq    |   mfcontext         |   7.6.0   |
| sonic_Client.jar      |   com.progress.sonic.mq    |   sonic_Client      |   7.6.0   |
| sonic_Crypto.jar      |   com.progress.sonic.mq    |   sonic_Crypto      |   7.6.0   |
| sonic_XA.jar          |   com.progress.sonic.mq    |   sonic_XA          |   7.6.0   |
| sonic_mgmt_client.jar |   com.progress.sonic.mq    |   sonic_mgmt_client |   7.6.0   |
| mgmt_client.jar       |   com.progress.sonic.mq    |   mgmt_client       |   7.6.0   |

In case you do not need to work with WebSphereMQ and SonicMQ, you can remove the sub modules for them.

## Status

## Description

## Author
Yannan Lu <yannanlu@yahoo.com>
