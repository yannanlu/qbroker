package org.qbroker.flow;

/**
 * QClient.java
 *<br/><br/>
 * QClient subscribs/publishes/registers/deregisters/browses/corpies/gets/
 * moves/puts JMS messages between queues and topics.
 *<br/>
 * @author yannanlu@yahoo.com
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.XQueue;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.QueueConnector;
import org.qbroker.jms.TopicConnector;
import org.qbroker.persister.ReceiverPool;
import org.qbroker.persister.PersisterPool;
import org.qbroker.flow.MessageFlow;
import org.qbroker.event.Event;

/**
 * QCLient browses/copies/gets/moves/puts JMS messages from/to/between queues.
 * The configuration parameters are stored in the property file, Client.json,
 * that list hostname, qmgr_name, q_name, etc.
 *<br/>
 * Usage: java org.qbroker.flow.QClient [-?|-l|-I configFile|...]
 */
@SuppressWarnings("unchecked")
public class QClient {
    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        int i;
        MessageFlow flow = null;
        Map h = null, props = new HashMap();
        List list;
        Object o;
        String uri, operation, configFile = null;
        int numSource, numTarget, show = 0;
        boolean isStream = false, isRequest = false;

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2)
                continue;
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'I':
                if (i+1 < args.length) {
                    configFile = args[i+1];
                }
                break;
              default:
                break;
            }
        }

        try {
            if (configFile != null) {
                FileReader fr = new FileReader(configFile);
                props = (Map) JSON2Map.parse(fr);
                fr.close();
            }

            show = processArgs(args, props);
            if (show == 0) {
                if ((o = props.get("LogDir")) != null) {
                    String logDir = (String) o;
                    if ((o = props.get("LogDatePattern")) != null)
                        Event.setLogDir(logDir, (String) o);
                    else if ((o = props.get("LogMaxFileSize")) != null) {
                        int maxBackups = 1;
                        String maxSize = (String) o;
                        if ((o = props.get("LogMaxBackupIndex")) != null)
                            maxBackups = Integer.parseInt((String) o);
                        Event.setLogDir(logDir, maxSize, maxBackups);
                    }
                    else
                        Event.setLogDir(logDir, null);
                }

                if ((o = props.get("Site")) != null)
                    Event.setDefaultSite((String) o);

                if ((o = props.get("Category")) != null)
                    Event.setDefaultCategory((String) o);

                if ((o = props.get("Name")) != null)
                    Event.setDefaultName((String) o);

                if ((o = props.get("Type")) != null)
                    Event.setDefaultType((String) o);
            }

            if (show > 0) {
                System.out.println(JSON2Map.toJSON(props, "", "\n"));
                System.exit(0);
            }

            if ((o = props.get("Source")) != null) {
                list = (List) o;
                numSource = list.size();
                for (i=0; i<numSource; ++i) {
                    h = (Map) list.get(i);
                    uri = (String) h.get("URI");
                    if (uri == null)
                        uri = "wmq://";
                    operation = (String) h.get("Operation");
                    if (h.get("ClassName") == null) {
                        if ("RegSub".equals(operation) ||
                            "DeregSub".equals(operation) ||
                            "sub".equals(operation)) {
                        h.put("ClassName","org.qbroker.receiver.JMSSubscriber");
                            if ((o = h.get("JMSClassName")) == null &&
                                uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.TConnector");
                            else if (o == null)
                            h.put("JMSClassName","org.qbroker.jms.TConnector");
                        }
                        else if ("browse".equals(operation) ||
                            "get".equals(operation)) {
                          h.put("ClassName","org.qbroker.receiver.JMSReceiver");
                            if ((o = h.get("JMSClassName")) == null &&
                                uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.QConnector");
                            else if (o == null)
                            h.put("JMSClassName","org.qbroker.jms.QConnector");
                        }
                    }
                    else if (h.get("JMSClassName") == null) {
                        if ("RegSub".equals(operation) ||
                            "DeregSub".equals(operation) ||
                            "sub".equals(operation)) {
                            if (uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.TConnector");
                            else
                            h.put("JMSClassName","org.qbroker.jms.TConnector");
                        }
                        else if ("browse".equals(operation) ||
                            "get".equals(operation)) {
                            if (uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.QConnector");
                            else
                            h.put("JMSClassName","org.qbroker.jms.QConnector");
                        }
                    }
                    if (("get".equals(operation) || "sub".equals(operation)) &&
                        (h.containsKey("PatternGroup") ||
                        h.containsKey("XPatternGroup") ||
                        h.containsKey("JMSPropertyGroup") ||
                        h.containsKey("XJMSPropertyGroup")))//no pattern support
                            throw(new IllegalArgumentException(
                                "no pattern support for operation get or sub, "+
                                "please use MessageSelector instead"));
                    h.put("Name", "Source_" + i);
                    h.put("LinkName", "root");
                    h.put("Capacity", String.valueOf(numSource));
                    props.put("Source_" + i, h);
                    list.set(i, "Source_" + i);
                }
                props.put("Receiver", list);
                props.remove("Source");
                if (numSource == 1 && (o = h.get("ClassName")) != null) {
                    String key = (String) o;
                    if (key.indexOf("StreamReceiver") > 0)
                        isStream = true;
                }
            }
            else
                numSource = 0;

            if ((o = props.get("Target")) != null) {
                list = (List) o;
                numTarget = list.size();
                for (i=0; i<numTarget; ++i) {
                    h = (Map) list.get(i);
                    uri = (String) h.get("URI");
                    if (uri == null)
                        uri = "wmq://";
                    operation = (String) h.get("Operation");
                    if (h.get("ClassName") == null) {
                        if ("pub".equals(operation)) {
                        h.put("ClassName","org.qbroker.persister.JMSPublisher");
                            if ((o = h.get("JMSClassName")) == null &&
                                uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.TConnector");
                            else if (o == null)
                            h.put("JMSClassName","org.qbroker.jms.TConnector");
                        }
                        else if ("put".equals(operation)) {
                        h.put("ClassName","org.qbroker.persister.JMSPersister");
                            if ((o = h.get("JMSClassName")) == null &&
                                uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.QConnector");
                            else if (o == null)
                            h.put("JMSClassName","org.qbroker.jms.QConnector");
                        }
                    }
                    else if (h.get("JMSClassName") == null) {
                        if ("pub".equals(operation)) {
                            if (uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.TConnector");
                            else
                            h.put("JMSClassName","org.qbroker.jms.TConnector");
                        }
                        else if ("put".equals(operation)) {
                            if (uri.startsWith("wmq://"))
                             h.put("JMSClassName","org.qbroker.wmq.QConnector");
                            else
                            h.put("JMSClassName","org.qbroker.jms.QConnector");
                        }
                    }
                    h.put("Name", "Target_" + i);
                    h.put("LinkName", "root");
                    h.put("Capacity", String.valueOf(numSource));
                    props.put("Target_" + i, h);
                    list.set(i, "Target_" + i);
                }
                props.put("Persister", list);
                props.remove("Target");
                if (numTarget == 1 && (o = h.get("ClassName")) != null) {
                    String key = (String) o;
                    if (key.indexOf("StreamPersister") < 0)
                        isRequest = true;
                }
            }
            else
                numTarget = 0;

            operation = (String) props.get("Operation");
            String className;
            java.lang.reflect.Constructor con;
            if (operation != null && ("RegSub".equals(operation) ||
                "DeregSub".equals(operation))) {
                h = (Map) props.get("Source_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                TopicConnector tConn = (TopicConnector) con.newInstance(
                    new Object[] {h});
                tConn.close();
                System.exit(0);
            }
            else if ("sub".equals(operation)) {
                h = (Map) props.get("Source_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                TopicConnector tConn = (TopicConnector) con.newInstance(
                    new Object[] {h});
                tConn.sub(null);
                tConn.close();
                System.exit(0);
            }
            else if ("pub".equals(operation) && numSource <= 0) {
                h = (Map) props.get("Target_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                TopicConnector tConn = (TopicConnector) con.newInstance(
                    new Object[] {h});
                tConn.pub(System.in);
                tConn.close();
                System.exit(0);
            }
            else if ("browse".equals(operation)) {
                h = (Map) props.get("Source_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                QueueConnector qConn = (QueueConnector) con.newInstance(
                    new Object[] {h});
                qConn.browse(null);
                qConn.close();
                System.exit(0);
            }
            else if ("get".equals(operation)) {
                h = (Map) props.get("Source_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                QueueConnector qConn = (QueueConnector) con.newInstance(
                    new Object[] {h});
                qConn.get(null);
                qConn.close();
                System.exit(0);
            }
            else if ("put".equals(operation) && numSource <= 0) {
                h = (Map) props.get("Target_0");
                className = (String) h.get("JMSClassName");
                con = Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                QueueConnector qConn = (QueueConnector) con.newInstance(
                    new Object[] {h});
                qConn.put(System.in);
                qConn.close();
                System.exit(0);
            }

            props.put("Name", "Client");
            flow = new MessageFlow(props);
            Thread c = new Thread(flow, "shutdown");
            Runtime.getRuntime().addShutdownHook(c);
            XQueue root = flow.getRootQueue();

            flow.start();

            while (MessageFlow.FLOW_RUNNING == flow.getStatus()) try {
                Thread.sleep(2000);
            }
            catch (Exception e) {
            }

            if (isStream && isRequest && root.size() <= 0 &&
                (o = root.browse(0)) != null) { // for request
                System.out.println("got response:\n" +
                    MessageUtils.processBody((Message) o, new byte[4096]));
            }

            if (c.isAlive()) try {
                c.join();
            }
            catch (Exception e) {
            }

            System.exit(0);
        } catch (JMSException e) {
            Exception ex = e.getLinkedException();
            if (ex != null)
                System.err.println("Linked exception: " + ex);
            e.printStackTrace();
            if (flow != null)
                flow.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (flow != null)
                flow.close();
        }
    }

    public static int processArgs(String args[], Map props) {
        char c;
        List list;
        Map hash;
        Object o;
        String sot = null, eot = null;
        String propertyName=null, propertyValue=null;
        int retCode = 0;
        int removeSource = 0;
        String sBCQ = null, tBCQ = null;
        String storeOption = null, realSubQmgr = null;
        String brokerVersion = null, str;

        if (! props.containsKey("Source"))
            props.put("Source", new ArrayList());

        if (! props.containsKey("Target"))
            props.put("Target", new ArrayList());

        list = (List) props.get("Source");
        if (list.size() == 0)
            list.add(new HashMap());
        Map sProps = (Map) list.get(0);

        list = (List) props.get("Target");
        if (list.size() == 0)
            list.add(new HashMap());
        Map tProps = (Map) list.get(0);

        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2)
                continue;
            c = args[i].charAt(1);
            switch (c) {
              case 'u':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("URI", args[++i]);
                break;
              case 'U':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("URI", args[++i]);
                break;
              case 'f':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("ContextFactory", args[++i]);
                break;
              case 'F':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("ContextFactory", args[++i]);
                break;
              case 't':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("TopicName", args[++i]);
                break;
              case 'T':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("TopicName", args[++i]);
                break;
              case 'i':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("QueueName", args[++i]);
                break;
              case 'o':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("QueueName", args[++i]);
                break;
              case 'a':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("StartDate", args[++i]);
                break;
              case 'b':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("EndDate", args[++i]);
                break;
              case 'h':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("Username", args[++i]);
                break;
              case 'm':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("Password", args[++i]);
                break;
              case 'H':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Username", args[++i]);
                break;
              case 'M':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Password", args[++i]);
                break;
              case 'R':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Credentials", args[++i]);
                realSubQmgr = args[i];
                break;
              case 'O':
                if (i + 1 >= args.length)
                    continue;
                props.put("Operation", args[++i]);
                if (!"RegSub".equals(args[i]) && !"DeregSub".equals(args[i])) 
                    tProps.put("Operation", args[i]);
                break;
              case 's':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("MessageSelector", args[++i]);
                break;
              case 'n':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("MaxNumberMessage", args[++i]);
                break;
              case 'd':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("DisplayMask", args[++i]);
                break;
              case 'D':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("DisplayMask", args[++i]);
                break;
              case 'c':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("ConnectionFactoryName", args[++i]);
                break;
              case 'C':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("ConnectionFactoryName", args[++i]);
                break;
              case 'q':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("Principal", args[++i]);
                sBCQ = args[i];
                break;
              case 'Q':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Principal", args[++i]);
                sBCQ = args[i];
                break;
              case 'w':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("Credentials", args[++i]);
                storeOption = args[i];
                break;
              case 'E':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Expiry", args[++i]);
                break;
              case 'P':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Priority", args[++i]);
                break;
              case 'J':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("Persistence", args[++i]);
                break;
              case 'K':
                if (i + 1 >= args.length)
                    continue;
                tProps.put("TargetClient", args[++i]);
                break;
              case 'W':
                if (i + 1 >= args.length)
                    continue;
                props.put("TextMode", args[++i]);
                break;
              case 'N':
                if (i + 1 >= args.length)
                    continue;
                props.put("NumberThread", args[++i]);
                break;
              case 'p':
                if (i + 1 >= args.length)
                    continue;
                propertyName = args[++i];
                break;
              case 'v':
                if (i + 1 >= args.length)
                    continue;
                propertyValue = args[++i];
                break;
              case 'j':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("ClientID", args[++i]);
                break;
              case 'k':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("SubscriptionID", args[++i]);
                break;
              case 'V':
                if (i + 1 >= args.length)
                    continue;
                sProps.put("BrokerVersion", args[++i]);
                brokerVersion = args[i];
                break;
              case 'g':
                if (i + 1 >= args.length)
                    continue;
                props.put("SleepTime", args[++i]);
                break;
              case 'x':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.get("XPatternGroup") != null) {
                    list = (List) sProps.get("XPatternGroup");
                }
                else {
                    list = new ArrayList();
                    sProps.put("XPatternGroup", list);
                }
                if (list.size() == 0)
                    list.add(new HashMap());
                hash = (Map) list.get(0);
                if ((o=hash.get("Pattern"))==null || !(o instanceof List)){
                    hash.put("Pattern", new ArrayList());
                    o = hash.get("Pattern");
                }
                list = (List) o;
                if (list.size() > 0)
                    list.set(0, args[++i]);
                else
                    list.add(args[++i]);
                break;
              case 'y':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.get("PatternGroup") != null) {
                    list = (List) sProps.get("PatternGroup");
                }
                else {
                    list = new ArrayList();
                    sProps.put("PatternGroup", list);
                }
                if (list.size() == 0)
                    list.add(new HashMap());
                hash = (Map) list.get(0);
                if ((o=hash.get("Pattern"))==null || !(o instanceof List)) {
                    hash.put("Pattern", new ArrayList());
                    o = hash.get("Pattern");
                }
                list = (List) o;
                if (list.size() > 0)
                    list.set(0, args[++i]);
                else
                    list.add(args[++i]);
                break;
              case 'Y':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.get("PatternGroup") != null) {
                    list = (List) sProps.get("PatternGroup");
                }
                else {
                    list = new ArrayList();
                    sProps.put("PatternGroup", list);
                }
                if (list.size() == 0)
                    list.add(new HashMap());
                hash = (Map) list.get(0);
                if ((o=hash.get("Pattern"))==null || !(o instanceof List)) {
                    hash.put("Pattern", new ArrayList());
                    o = hash.get("Pattern");
                }
                list = (List) o;
                if (list.size() > 1)
                    list.set(1, args[++i]);
                else if (list.size() == 1)
                    list.add(args[++i]);
                break;
              case 'z':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.get("PatternGroup") != null) {
                    list = (List) sProps.get("PatternGroup");
                }
                else {
                    list = new ArrayList();
                    sProps.put("PatternGroup", list);
                }
                if (list.size() == 1) {
                    list.add(new HashMap());
                }
                else if (list.size() == 0) {
                    list.add(new HashMap());
                    list.add(new HashMap());
                }
                hash = (Map) list.get(1);
                if ((o=hash.get("Pattern"))==null || !(o instanceof List)) {
                    hash.put("Pattern", new ArrayList());
                    o = hash.get("Pattern");
                }
                list = (List) o;
                if (list.size() > 0)
                    list.set(0, args[++i]);
                else
                    list.add(args[++i]);
                break;
              case 'Z':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.get("PatternGroup") != null) {
                    list = (List) sProps.get("PatternGroup");
                }
                else {
                    list = new ArrayList();
                    sProps.put("PatternGroup", list);
                }
                if (list.size() == 1) {
                    list.add(new HashMap());
                }
                else if (list.size() == 0) {
                    list.add(new HashMap());
                    list.add(new HashMap());
                }
                hash = (Map) list.get(1);
                if ((o=hash.get("Pattern"))==null || !(o instanceof List)) {
                    hash.put("Pattern", new ArrayList());
                    o = hash.get("Pattern");
                }
                list = (List) o;
                if (list.size() > 1)
                    list.set(1, args[++i]);
                else if (list.size() == 1)
                    list.add(args[++i]);
                break;
              case 'X':
                if (i + 1 >= args.length)
                    continue;
                props.put("XAMode", args[++i]);
                sProps.put("XAMode", args[i]);
                tProps.put("XAMode", args[i]);
                break;
              case 'G':
                sProps.put("Mode", "daemon");
                props.put("Mode", "daemon");
                break;
              case 'L':
                if (i + 1 >= args.length)
                    continue;
                props.put("LogDir", args[++i]);
                break;
              case 'S':
                if (i + 1 >= args.length)
                    continue;
                props.put("Capacity", args[++i]);
                break;
              case 'A':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.size() > 0)
                    sProps.put("SOTBytes", args[++i]);
                else
                    sot = args[++i];
                break;
              case 'B':
                if (i + 1 >= args.length)
                    continue;
                if (sProps.size() > 0)
                    sProps.put("EOTBytes", args[++i]);
                else
                    eot = args[++i];
                break;
              case 'e':
                sProps.put("SequentialSearch", "on");
                break;
              case 'r':
                removeSource = 1;
                break;
              case 'l':
                retCode = 1;
                break;
              default:
                break;
            }
        }

        if (sot != null) {
            if (sProps.size() > 0)
                sProps.put("SOTBytes", sot);
            else if (tProps.size() > 0)
                tProps.put("SOTBytes", sot);
        }
        if (eot != null) {
            if (sProps.size() > 0)
                sProps.put("EOTBytes", eot);
            else if (tProps.size() > 0)
                tProps.put("EOTBytes", eot);
        }

        if (sProps.containsKey("PatternGroup")) // ignore removeSource
            removeSource = 0;

        if (sProps.size() > 0 && // source
            (propertyValue != null || "JMSReplyTo".equals(propertyName))) {
            if (sProps.get("StringProperty") == null) {
                sProps.put("StringProperty", new HashMap());
            }
            hash = (Map) sProps.get("StringProperty");
            if (hash.size() <= 0 || !hash.containsKey(propertyName)) {
                hash.put(propertyName, "");
            }
            if (propertyValue != null)
                hash.put(propertyName, propertyValue);
            else
                hash.put(propertyName, "");
        }
        else if (propertyName != null && tProps.size() > 0) { // for target
            if (tProps.get("StringProperty") == null) {
                tProps.put("StringProperty", new HashMap());
            }
            hash = (Map) tProps.get("StringProperty");
            if (hash.size() <= 0 || !hash.containsKey(propertyName)) {
                hash.put(propertyName, "");
            }
        }

        try {
            ReceiverPool.mapURI((String) sProps.get("URI"), sProps);
            PersisterPool.mapURI((String) tProps.get("URI"), tProps);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(Event.traceStack(e)));
        }

        str = (String) sProps.get("URI");
        if (str != null && str.startsWith("wmq://") && // for wmq overload
            !sProps.containsKey("ContextFactory")) { // no jndi lookup
            if (realSubQmgr != null) {
                sProps.put("RealSubQmgr", realSubQmgr);
                tProps.remove("Credentials");
            }
            if (sBCQ != null) {
                sProps.put("BrokerControlQueue", sBCQ);
                sProps.remove("Principal");
            }
            if (storeOption != null) {
                sProps.put("StoreOption", storeOption);
                sProps.remove("Credentials");
            }
        }
        str = (String) tProps.get("URI");
        if (str != null && str.startsWith("wmq://") && // for wmq overload
            !tProps.containsKey("ContextFactory")) { // no jndi lookup
            if (tBCQ != null) {
                tProps.put("BrokerControlQueue", tBCQ);
                tProps.remove("Principal");
            }
        }

        // figure out what operation according to I/O
        String input = (String) sProps.get("QueueName");
        String output = (String) tProps.get("QueueName");
        String iTopic = (String) sProps.get("TopicName");
        String oTopic = (String) tProps.get("TopicName");
        String operation = (String) props.get("Operation");
        if (iTopic != null && oTopic != null) { // repub
            props.put("Operation", "repub");
            sProps.put("Operation", "sub");
            tProps.put("Operation", "pub");
        }
        else if (iTopic != null && output != null) { // sub2q
            tProps.remove("TopicName");
            tProps.remove("BrokerControlQueue");
            props.put("Operation", "sub2q");
            sProps.put("Operation", "sub");
            tProps.put("Operation", "put");
        }
        else if (iTopic != null && tProps.isEmpty()) { // sub only
            tProps.clear();
            if (operation != null) // -O overwrite source's operation
                sProps.put("Operation", operation);
            else {
                sProps.put("Operation", "sub");
                props.put("Operation", "sub");
            }
            if (sProps.get("RealSubQmgr") != null &&
                (sProps.get("BrokerVersion") == null ||
                Integer.parseInt((String) sProps.get("BrokerVersion")) != 0 ||
                !("DeregSub".equals(operation) || "RegSub".equals(operation)))){
                sProps.remove("RealSubQmgr");
            }
        }
        else if (iTopic != null) { // sub to others
            sProps.put("Operation", "sub");
            tProps.remove("TopicName");
            tProps.remove("BrokerControlQueue");
            tProps.remove("QueueName");
            tProps.remove("ChannelName");
            tProps.remove("TargetClient");
            tProps.remove("Expiry");
            tProps.remove("Priority");
            tProps.remove("Persistence");
            tProps.remove("PropertyValue");
            if (operation != null) // -O overwrite target's operation
                tProps.put("Operation", operation);
            else {
                operation = (String) tProps.get("Operation");
                props.put("Operation", operation);
            }
        }
        else if (input != null && oTopic != null) { // qpub
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            tProps.put("Operation", "pub");
            operation =  (String) sProps.get("Operation");
            if (removeSource > 0) {
                props.put("Operation", "qpub");
                sProps.put("Operation", "get");
            }
            else if ("get".equals(operation)) {
                props.put("Operation", "qpub");
                sProps.put("Operation", "get");
            }
            else {
                props.put("Operation", "bpub");
                sProps.put("Operation", "browse");
            }
            if (brokerVersion != null)
                tProps.put("BrokerVersion", brokerVersion);
        }
        else if (sProps.isEmpty() && oTopic != null) { // pub only
            sProps.clear();
            operation =  (String) tProps.get("Operation");
            if (operation != null)
                props.put("Operation", operation);
            else {
                tProps.put("Operation", "pub");
                props.put("Operation", "pub");
            }
            if (props.get("TextMode") != null)
                tProps.put("TextMode", props.get("TextMode"));
            if (brokerVersion != null)
                tProps.put("BrokerVersion", brokerVersion);
        }
        else if (oTopic != null) { // pub from others
            tProps.put("Operation", "pub");
            operation = (String) sProps.get("Operation");
            props.put("Operation", operation);
            if (props.get("TextMode") != null)
                sProps.put("TextMode", props.get("TextMode"));
            if ("select".equals(operation)) { // for JDBC queries
                String sqlQuery = (String) sProps.get("MessageSelector");
                if (sqlQuery != null)
                    sProps.put("SQLQuery", sqlQuery);
            }
            if (sProps.get("SOTBytes") != null && sProps.get("Offhead") == null
                && sProps.containsKey("StartDate")) // copy over Offhead
                sProps.put("Offhead", sProps.get("StartDate"));
            if (sProps.get("EOTBytes") != null && sProps.get("Offtail") == null
                && sProps.containsKey("EndDate")) // copy over Offtail
                sProps.put("Offtail", sProps.get("EndDate"));
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            sProps.remove("QueueName");
            sProps.remove("ChannelName");
            sProps.remove("TargetClient");
            sProps.remove("Expiry");
            sProps.remove("Priority");
            sProps.remove("Persistence");
            sProps.remove("MessageSelector");
            sProps.remove("StartDate");
            sProps.remove("EndDate");
            if (brokerVersion != null)
                tProps.put("BrokerVersion", brokerVersion);
        }
        else if (input != null && output != null) { // q2q
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            tProps.remove("TopicName");
            tProps.remove("BrokerControlQueue");
            if (tProps.get("Operation") == null)
                tProps.put("Operation", "put");
            operation =  (String) sProps.get("Operation");
            if (removeSource > 0) {
                props.put("Operation", "move");
                sProps.put("Operation", "get");
            }
            else if ("get".equals(operation)) {
                props.put("Operation", "move");
            }
            else {
                props.put("Operation", "copy");
                sProps.put("Operation", "browse");
            }
        }
        else if (input != null && tProps.isEmpty()) { // get or browse only
            tProps.clear();
            operation =  (String) sProps.get("Operation");
            if (removeSource > 0) {
                props.put("Operation", "get");
                sProps.put("Operation", "get");
            }
            else if ("get".equals(operation)) {
                props.put("Operation", "get");
            }
            else {
                props.put("Operation", "browse");
                sProps.put("Operation", "browse");
            }
        }
        else if (input != null && "-".equals(input)) { // read to others
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            sProps.remove("QueueName");
            sProps.remove("ChannelName");
            sProps.remove("TargetClient");
            sProps.remove("Expiry");
            sProps.remove("Priority");
            tProps.remove("TopicName");
            tProps.remove("BrokerControlQueue");
            tProps.remove("QueueName");
            tProps.remove("ChannelName");
            tProps.remove("TargetClient");
            tProps.remove("Expiry");
            tProps.remove("Priority");
            tProps.remove("Persistence");
            if (operation != null) // -O overwrite target's operation
                tProps.put("Operation", operation);
            else {
                operation = (String) tProps.get("Operation");
                props.put("Operation", operation);
            }
        }
        else if (input != null) { // get or browse to others
            tProps.remove("TopicName");
            tProps.remove("BrokerControlQueue");
            tProps.remove("QueueName");
            tProps.remove("ChannelName");
            tProps.remove("TargetClient");
            tProps.remove("Expiry");
            tProps.remove("Priority");
            tProps.remove("Persistence");
            if (operation != null) // -O overwrite target's operation
                tProps.put("Operation", operation);
            else {
                operation = (String) tProps.get("Operation");
                props.put("Operation", operation);
            }
            operation =  (String) sProps.get("Operation");
            if (removeSource > 0)
                sProps.put("Operation", "get");
            else if (!"get".equals(operation))
                sProps.put("Operation", "browse");
        }
        else if (sProps.isEmpty() && output != null) { // put only
            sProps.clear();
            props.put("Operation", "put");
            tProps.put("Operation", "put");
            if (props.get("TextMode") != null)
                tProps.put("TextMode", props.get("TextMode"));
        }
        else if (output != null) { // put from others
            tProps.put("Operation", "put");
            operation = (String) tProps.get("Operation");
            props.put("Operation", operation);
            if (props.get("TextMode") != null)
                sProps.put("TextMode", props.get("TextMode"));
            if ("select".equals(operation)) { // for JDBC queries
                String sqlQuery = (String) sProps.get("MessageSelector");
                if (sqlQuery != null)
                    sProps.put("SQLQuery", sqlQuery);
            }
            if (sProps.get("SOTBytes") != null && sProps.get("Offhead") == null
                && sProps.containsKey("StartDate")) // copy over Offhead
                sProps.put("Offhead", sProps.get("StartDate"));
            if (sProps.get("EOTBytes") != null && sProps.get("Offtail") == null
                && sProps.containsKey("EndDate")) // copy over Offtail
                sProps.put("Offtail", sProps.get("EndDate"));
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            sProps.remove("QueueName");
            sProps.remove("ChannelName");
            sProps.remove("MessageSelector");
            sProps.remove("StartDate");
            sProps.remove("EndDate");
        }
        else if (!tProps.isEmpty()) { // non-JMS
            if (operation != null) // -O overwrite target's operation
                tProps.put("Operation", operation);
            else {
                operation = (String) tProps.get("Operation");
                props.put("Operation", operation);
            }
            if (props.get("TextMode") != null && !sProps.isEmpty())
                sProps.put("TextMode", props.get("TextMode"));
            tProps.remove("TopicName");
            tProps.remove("QueueName");
            tProps.remove("ChannelName");
            tProps.remove("BrokerControlQueue");
            tProps.remove("TargetClient");
            tProps.remove("Expiry");
            tProps.remove("Priority");
            tProps.remove("Persistence");
            tProps.remove("PropertyValue");
            operation = (String) sProps.get("Operation");
            if ("select".equals(operation)) { // for JDBC queries
                String sqlQuery = (String) sProps.get("MessageSelector");
                if (sqlQuery != null)
                    sProps.put("SQLQuery", sqlQuery);
            }
            if (sProps.get("SOTBytes") != null && sProps.get("Offhead") == null
                && sProps.containsKey("StartDate")) // copy over Offhead
                sProps.put("Offhead", sProps.get("StartDate"));
            if (sProps.get("EOTBytes") != null && sProps.get("Offtail") == null
                && sProps.containsKey("EndDate")) // copy over Offtail
                sProps.put("Offtail", sProps.get("EndDate"));
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            sProps.remove("QueueName");
            sProps.remove("ChannelName");
            sProps.remove("MessageSelector");
            sProps.remove("StartDate");
            sProps.remove("EndDate");
        }
        else if (!sProps.isEmpty()) { // non-JMS
            operation = (String) sProps.get("Operation");
            props.put("Operation", operation);
            if (props.get("TextMode") != null)
                sProps.put("TextMode", props.get("TextMode"));
            if ("select".equals(operation)) { // for JDBC queries
                String sqlQuery = (String) sProps.get("MessageSelector");
                if (sqlQuery != null)
                    sProps.put("SQLQuery", sqlQuery);
            }
            if (sProps.get("SOTBytes") != null && sProps.get("Offhead") == null
                && sProps.containsKey("StartDate")) // copy over Offhead
                sProps.put("Offhead", sProps.get("StartDate"));
            if (sProps.get("EOTBytes") != null && sProps.get("Offtail") == null
                && sProps.containsKey("EndDate")) // copy over Offtail
                sProps.put("Offtail", sProps.get("EndDate"));
            sProps.remove("TopicName");
            sProps.remove("BrokerControlQueue");
            sProps.remove("QueueName");
            sProps.remove("ChannelName");
            sProps.remove("MessageSelector");
            sProps.remove("StartDate");
            sProps.remove("EndDate");
        }

        String uri = (String) sProps.get("URI");
        if (uri != null && uri.startsWith("wmq://")) { // for wmq
            if (sProps.containsKey("Username") &&
                sProps.containsKey("Password") &&
                !sProps.containsKey("SecurityExit")) {
                sProps.put("SecurityExit","org.qbroker.wmq.SimpleSecurityExit");
            }
        }
        uri = (String) tProps.get("URI");
        if (uri != null && uri.startsWith("wmq://")) { // for wmq
            if (tProps.containsKey("Username") &&
                tProps.containsKey("Password") &&
                !tProps.containsKey("SecurityExit")) {
                tProps.put("SecurityExit","org.qbroker.wmq.SimpleSecurityExit");
            }
        }

        if (!props.containsKey("Mode"))
            props.put("Mode", "command");

        if (tProps.isEmpty())
            props.remove("Target");
        else if (sProps.isEmpty())
            props.remove("Source");

        if (props.containsKey("Source") && !props.containsKey("Target")) {
            operation = (String) sProps.get("Operation");
            if ((o = props.get("SleepTime")) != null)
                sProps.put("SleepTime", o);
            if ("browse".equals(operation) || "get".equals(operation) ||
                "sub".equals(operation) || "DeregSub".equals(operation) ||
                "RegSub".equals(operation)) // JMS stuff
                return retCode;
            tProps.clear();
            tProps.put("URI", "file://-");
            tProps.put("ClassName", "org.qbroker.persister.StreamPersister");
            tProps.put("Operation", "write");
            list = new ArrayList();
            list.add(tProps);
            props.put("Target", list);
            props.put("Operation", operation);
        }
        else if (!props.containsKey("Source") && props.containsKey("Target")) {
            operation = (String) tProps.get("Operation");
            if ((o = props.get("SleepTime")) != null)
                tProps.put("SleepTime", o);
            if ("put".equals(operation) || "pub".equals(operation)) // JMS
                return retCode;
            sProps.clear();
            sProps.put("URI", "file://-");
            sProps.put("ClassName", "org.qbroker.receiver.StreamReceiver");
            sProps.put("Operation", "read");
            if (props.get("TextMode") != null)
                sProps.put("TextMode", props.get("TextMode"));
            list = new ArrayList();
            list.add(sProps);
            props.put("Source", list);
            props.put("Operation", operation);
        }
        else if (((List) props.get("Source")).size() == 1) {
            operation = (String) tProps.get("Operation");
            if ((o = props.get("SleepTime")) != null)
                sProps.put("SleepTime", o);
            if ("echo".equals(operation)) { // server response mode
                uri = (String) sProps.get("URI");
                operation = (String) sProps.get("Operation");
                if (uri.indexOf("udp://") == 0) { // for packet
                    if (!"reply".equals(operation)) // for reply
                        sProps.put("Operation", "reply");
                }
                else if (uri.indexOf("tcp://") == 0) { // for stream
                    if (!"respond".equals(operation)) // for reply
                        sProps.put("Operation", "respond");
                  sProps.put("ClassName","org.qbroker.receiver.ServerReceiver");
                }
            }
        }
        else if ((o = props.get("SleepTime")) != null) {
            sProps.put("SleepTime", o);
        }
        if ((o = props.get("Capacity")) != null) {
            if(sProps.get("Capacity") == null) 
                sProps.put("Capacity", o);
            if(tProps.get("Capacity") == null) 
                tProps.put("Capacity", o);
        }

        return retCode;
    }

    private static void printUsage() {
     System.out.println("JMS QCLient Class Version 3.0 (written by Yannan Lu)");
System.out.println("QClient: pub/sub/browse/copy/get/move/put/read/write JMS messages from/to/between destinations");
        System.out.println("Usage: java org.qbroker.flow.QClient [-?|-l|-I ConfigFile|optional flags as bellow]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -l: list all properties");
        System.out.println("  -r: remove source (ignored if with any of -xyzYZ; default: keep source)");
        System.out.println("  -e: turn on sequential search of JMSTimestamp");
        System.out.println("  -G: run in daemon mode");
        System.out.println("  -S: Capacity of XQueue (default: 1)");
        System.out.println("  -L: LogDir (default: no logging)");
        System.out.println("  -W: TextMode (0: jms_bytes, 1: jms_text, default: 1)");
        System.out.println("  -N: NumberThread (default: 1)");
        System.out.println("  -I: ConfigFile (default: Client.json)");
        System.out.println("  -A: SOTBytes (eg: 0x01)");
        System.out.println("  -B: EOTBytes (eg: 0x0a)");
        System.out.println("  -O: Operation (RegSub/DeregSub/sub/pub/repub/sub2q/qpub/bpub/copy/move/read/write/...)");
        System.out.println("  -X: XAMode (1: Client; 2: Commit; default: 1, client transaction)");
        System.out.println("  -u: URI of Source (wmq, file, tcp, etc, default wmq)");
        System.out.println("  -f: ContextFactory of Source (default depending on scheme)");
        System.out.println("  -i: QueueName of Source (if specified, browse/copy/get/move/sub; otherwise, put/pub/read)");
        System.out.println("  -t: TopicName of Source (for sub/repub/RegSub/DeregSub)");
        System.out.println("  -c: ConnectionFactoryName of Source (for wmq: ChannelName)");
        System.out.println("  -U: URI of Target (wmq, file, tcp, etc, default wmq)");
        System.out.println("  -F: ContextFactory of Target (default depending on scheme)");
        System.out.println("  -o: QueueName of Target (if specified, copy/put/move/pub/sub2q; otherwise, browse/get/sub/write)");
        System.out.println("  -T: TopicName of Target (for pub/repub/qpub/bpub)");
        System.out.println("  -C: ConnectionFactoryName of Target (for wmq: ChannelName)");
        System.out.println("  -E: Expiry (in millisecond, default: 0)");
        System.out.println("  -P: Priority (0-9=priority; -1=QDEF; -2=APP)");
        System.out.println("  -J: Persistence (1=not persistent; 2=persistent; -1=QDEF; -2=APP)");
        System.out.println("  -K: TargetClient (0: JMS; 1: MQ)");
        System.out.println("  -V: BrokerVersion (2: default; 0: workaround, for wmq only)");
        System.out.println("  -j: ClientID (string to id the apps)");
        System.out.println("  -k: SubscriptionID (required for durable sub, RegSub, DeregSub)");
        System.out.println("  -q: Principal of Source or BrokerControlQueue of Source for wmq (default: SYSTEM.BROKER.CONTROL.QUEUE)");
        System.out.println("  -w: Credentials of Source or StoreOption for sub of wmq (0: Queue; 1: Broker; default: 0)");
        System.out.println("  -Q: Principal of Target or BrokerControlQueue of Target for wmq (default: SYSTEM.BROKER.CONTROL.QUEUE)");
        System.out.println("  -R: Credentials of target or RealSubQmgr (valid only if BrokerVersion = 0)");
        System.out.println("  -g: SleepTime (in ms; default: 0)");
        System.out.println("  -s: MessageSelector (eg: JMSMessageID='ID:414D51204645454445524120202020203C76F3500AB17362')");
        System.out.println("  -n: MaxNumberMessage (if negative, count backwards; in case of put, number of copies)");
        System.out.println("  -a: StartDate for JMS (format: yyyy/MM/dd.HH:mm:ss.SSS) or offhead");
        System.out.println("  -b: EndDate for JMS (format: yyyy/MM/dd.HH:mm:ss.SSS) or offtail");
        System.out.println("  -x: XPatternGroup (excluded match for message body)");
        System.out.println("  -y: primary PatternGroup (alternative match for message body)");
        System.out.println("  -Y: primary PatternGroup (AND match for message body)");
        System.out.println("  -z: secondary PatternGroup (alternative match for message body)");
        System.out.println("  -Z: secondary PatternGroup (AND match for message body)");
        System.out.println("  -h: Username of Source (for authentication)");
        System.out.println("  -m: Password of Source (for authentication)");
        System.out.println("  -H: Username of Target (for authentication)");
        System.out.println("  -M: Password of Target (for authentication)");
        System.out.println("  -p: Name of StringProperty (eg: JMSCorrelationID)");
        System.out.println("  -v: Value of StringProperty (if specified, set it to Source msg; eg: ID:414D51204645454445524120202020203C76F3500AB17362)");
        System.out.println("  -d: DisplayMask of Source (0: None; 1:Body; 2:Date; 4:Size; 8:Dst; 16:MID; 32:CID; 64:Props; 128:Expiry; 256:Pri; 512:DMode; 1024:DCount; 2048:TStamp; 4096:MType; 8192:Reply; 16384:Family; 32768:Age)");
        System.out.println("  -D: DisplayMask of Target (0: None; 1:Body; 2:Date; 4:Size; 8:Dst; 16:MID; 32:CID; 64:Props; 128:Expiry; 256:Pri; 512:DMode; 1024:DCount; 2048:TStamp; 4096:MType; 8192:Reply; 16384:Family; 32768:Age)");
    }
}
