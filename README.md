# qbroker

This is a Maven 2 project for QBroker, an integration tool for various data sources and stores. It has been tested on build with Maven 3.3.3 by Java 7. The following jar files have to be deployed to the local repository since they are not available in Maven Central:

| name                  |   groupId                  |   artifactId        |   version |
| ---                   |   ---                      |   ---               |   ---     |
| comm.jar              |   javax.comm               |   comm              |   2.0.3   |
| com.ibm.mq.jar        |   com.ibm                  |   com.ibm.mq        |   6.1     |
| com.ibm.mqjms.jar     |   com.ibm                  |   com.ibm.mqjms     |   6.1     |
| com.ibm.mq.pcf.jar    |   com.ibm                  |   com.ibm.mq.pcf    |   6.1     |
| dhbcore.jar           |   com.ibm                  |   dhbcore           |   6.1     |
| mfcontext.jar         |   com.progress.sonic.mq    |   mfcontext         |   8.6     |
| sonic_Client.jar      |   com.progress.sonic.mq    |   sonic_Client      |   8.6     |
| sonic_mgmt_client.jar |   com.progress.sonic.mq    |   sonic_mgmt_client |   8.6     |
| mgmt_client.jar       |   com.progress.sonic.mq    |   mgmt_client       |   8.6     |

In case you do not need to work with WebSphereMQ and SonicMQ, you can remove the sub modules from project and update the corresponding pom.xml. Otherwise, you can contact me for them.

## Status

## Description
To build the project, just run "mvn pacakge". The result of the build is the jar file of qbroker-x.y.z.jar in dist/target. Check [Documentation](https://yannanlu.github.io) for how to install/configure/run/manage QBroker instances.

## Author
Yannan Lu <yannanlu@yahoo.com>

## See Also
* [Documentation](https://yannanlu.github.io)
* [Javadoc](https://yannanlu.github.io/javadoc)
