package org.qbroker.jms;

/* JMSHealthChecker.java - a monitor testing queue acceptance */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.QueueSender;
import javax.jms.TopicPublisher;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.jms.QueueConnector;
import org.qbroker.jms.TopicConnector;
import org.qbroker.event.Event;

/**
 * HealthCheck checks a JMS destination by putting or publishing a JMS
 * message to it
 *<br><br>
 * It sends or publishes a predefined health-check message to the
 * destination to test its message acceptance.  The health-check
 * message should be harmless to the applications.  Otherwise, you have to
 * disable the auto-commit so that the message will not be committed.
 * If the destination is full or not defined, or not writeable or
 * not available, the health-check will fail.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JMSHealthChecker extends Monitor {
    private Map props = null;
    private String hostName=null, connFactoryName;
    private String qmgrName, qName, tName, uri;
    private QueueConnector jmsQCon = null;
    private TopicConnector jmsTCon = null;
    private TextMessage outMessage = null;
    private boolean isDynamic = false;
    private int previousDstStatus;
    private int operation, port, brokerVersion;
    public final static int DST_UNKNOWN = -2;
    public final static int DST_FAILED = -1;
    public final static int DST_OK = 0;
    public final static int DST_GET = 1;
    public final static int DST_PUT = 2;
    public final static int DST_BROWSE = 3;
    public final static int DST_SUB = 4;
    public final static int DST_PUB = 5;
    public final static int DST_UNSUB = 6;

    @SuppressWarnings("unchecked")
    public JMSHealthChecker(Map props) {
        super(props);
        Object o;
        Map<String, Object> h = new HashMap<String, Object>();
        int n;

        if (type == null)
            type = "JMSHealthChecker";

        if (description == null)
            description = "health-check on a JMS destination";

        if ((o = MonitorUtils.select(props.get("URI"))) != null) {
            URI u = null;
            String path;
            uri = MonitorUtils.substitute((String) o, template);

            try {
                u = new URI(uri);
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException(e.toString()));
            }

            if ("wmq".equals(u.getScheme())) {
                if ((path = u.getPath()) != null && path.length() > 1)
                    qmgrName = path.substring(1);
                else
                    qmgrName = "";
                if ((port = u.getPort()) <= 0)
                    port = 1414;
                hostName = u.getHost();
                if ((o = props.get("ConnectionFactoryName")) != null) {
                    o = MonitorUtils.select(o);
                    connFactoryName =
                        MonitorUtils.substitute((String) o, template);
                }
                else
                    connFactoryName = "SYSTEM.DEF.SVRCONN";
            }
            else {
                o = MonitorUtils.select(props.get("ConnectionFactoryName"));
                connFactoryName = MonitorUtils.substitute((String) o, template);
            }
        }
        else if ((o = MonitorUtils.select(props.get("QueueManager")))!=null){
            qmgrName = MonitorUtils.substitute((String) o, template);
            uri = "wmq:///" + qmgrName;
            connFactoryName = "";
        }
        else {
      throw(new IllegalArgumentException("URI or QueueManager is not defined"));
        }

        brokerVersion = 2;
        if ((o = MonitorUtils.select(props.get("TopicName"))) != null) {
            tName = MonitorUtils.substitute((String) o, template);
            operation = DST_PUB;
            if ((o = props.get("BrokerVersion")) != null)
                brokerVersion = Integer.parseInt((String) o);
        }
        if ((o = MonitorUtils.select(props.get("QueueName"))) != null) {
            qName = MonitorUtils.substitute((String) o, template);
            if (operation != DST_PUB)
                operation = DST_PUT;
        }
        if (operation != DST_PUT && operation != DST_PUB)
            throw(new IllegalArgumentException("Destination is not defined"));

        if (uri.startsWith("wmq://")) { // for wmq
            if (hostName != null) {
                h.put("HostName", hostName);
                h.put("Port", String.valueOf(port));
                h.put("ChannelName", connFactoryName);
            }
            else if (qmgrName.length() > 0) {
                h.put("QueueManager", qmgrName);
            }
            if ((o = props.get("SecurityExit")) != null) {
                h.put("SecurityExit", o);
                if ((o = props.get("SecurityData")) != null)
                    h.put("SecurityData", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedSecurityData")) != null)
                    h.put("EncryptedSecurityData", MonitorUtils.select(o));
            }
            if ((o = props.get("Username")) != null) {
                h.put("Username", MonitorUtils.select(o));
                if ((o = props.get("Password")) != null)
                    h.put("Password", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", MonitorUtils.select(o));
            }
            h.put("DisplayMask", "0");
            h.put("TextMode", "1");
            if (operation == DST_PUB) {
                h.put("TopicName", tName);
                h.put("QueueName", qName);
                h.put("Operation", "pub");
                h.put("BrokerVersion", String.valueOf(brokerVersion));
                try {
                    Class<?> cls = Class.forName("org.qbroker.wmq.TConnector");
                    java.lang.reflect.Constructor con =
                        cls.getConstructor(new Class[]{Map.class});
                    jmsTCon =(TopicConnector) con.newInstance(new Object[]{h});
                    outMessage = (TextMessage) jmsTCon.createMessage(1);
                    jmsTCon.close();
                }
                catch (InvocationTargetException e) {
                    Throwable ex = e.getTargetException();
                    if (ex == null)
                        new Event(Event.ERR, "failed to instantiate " +
                            "TConnector: "+ Event.traceStack(e)).send();
                    else
                        new Event(Event.ERR, "failed to instantiate " +
                            "TConnector: "+ Event.traceStack(ex)).send();
                    this.props = h;
                    outMessage = null;
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to instantiate " +
                        "TopicConnector: " + Event.traceStack(e)).send();
                    this.props = h;
                    outMessage = null;
                }
            }
            else {
                h.put("QueueName", qName);
                h.put("Operation", "put");
                if ((o = props.get("IsPhysical")) != null)
                    h.put("IsPhysical", o);
                try {
                    Class<?> cls = Class.forName("org.qbroker.wmq.QConnector");
                    java.lang.reflect.Constructor con =
                        cls.getConstructor(new Class[]{Map.class});
                    jmsQCon =(QueueConnector) con.newInstance(new Object[]{h});
                    outMessage = (TextMessage) jmsQCon.createMessage(1);
                    jmsQCon.close();
                }
                catch (InvocationTargetException e) {
                    Throwable ex = e.getTargetException();
                    if (ex == null)
                        new Event(Event.ERR, "failed to instantiate " +
                            "QConnector: "+ Event.traceStack(e)).send();
                    else
                        new Event(Event.ERR, "failed to instantiate " +
                            "QConnector: "+ Event.traceStack(ex)).send();
                    this.props = h;
                    outMessage = null;
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to instantiate " +
                        "QueueConnector: " + Event.traceStack(e)).send();
                    this.props = h;
                    outMessage = null;
                }
            }
        }
        else { // for generic JMS
            h.put("URI", uri);
            h.put("ContextFactory",
                MonitorUtils.select(props.get("ContextFactory")));
            if ((o = props.get("IsPhysical")) != null)
                h.put("IsPhysical", o);
            if ((o = props.get("Username")) != null) {
                h.put("Username", MonitorUtils.select(o));
                if ((o = props.get("Password")) != null)
                    h.put("Password", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", MonitorUtils.select(o));
            }
            if ((o = props.get("Principal")) != null) {
                h.put("Principal", MonitorUtils.select(o));
                if ((o = props.get("Credentials")) != null)
                    h.put("Credentials", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedCredentials")) != null)
                    h.put("EncryptedCredentials", MonitorUtils.select(o));
            }
            h.put("ConnectionFactoryName", connFactoryName);
            h.put("DisplayMask", "0");
            h.put("TextMode", "1");
            if (operation == DST_PUB) {
                h.put("TopicName", tName);
                h.put("Operation", "pub");
                try {
                    jmsTCon=(TopicConnector) new org.qbroker.jms.TConnector(h);
                    outMessage = (TextMessage) jmsTCon.createMessage(1);
                    jmsTCon.close();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to instantiate " +
                        "TopicConnector: " + Event.traceStack(e)).send();
                    this.props = h;
                    outMessage = null;
                }
            }
            else {
                h.put("QueueName", qName);
                h.put("Operation", "put");
                try {
                    jmsQCon=(QueueConnector) new org.qbroker.jms.QConnector(h);
                    outMessage = (TextMessage) jmsQCon.createMessage(1);
                    jmsQCon.close();
                }
                catch (Exception e) {
                    new Event(Event.ERR, "failed to instantiate " +
                        "QueueConnector: " + Event.traceStack(e)).send();
                    this.props = h;
                    outMessage = null;
                }
            }
        }

        if ((o = props.get("JMSPropertyGroup")) != null) {
            h = Utils.cloneProperties((Map) o);
            Iterator iter = h.keySet().iterator();
            if (outMessage == null && this.props != null) {
                this.props.put("JMSPropertyGroup", o);
            }
            else try {
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    if (key == null) continue;
                    if ("JMSType".equals(key))
                        outMessage.setJMSType((String) h.get(key));
                    else if ("JMSCorrelationID".equals(key))
                        outMessage.setJMSCorrelationID((String) h.get(key));
                    else if (key.startsWith("JMS"))
                        continue;
                    else
                        outMessage.setStringProperty(key,(String) h.get(key));
                }
            }
            catch (JMSException e) {
                throw(new IllegalArgumentException("failed to set property: "+
                    Event.traceStack(e)));
            }
        }
        if ((o = props.get("MessageBody")) != null) {
            template = new Template((String) o);
            if (template.size() <= 0) {
                isDynamic = false;
                if (outMessage != null) try {
                    outMessage.setText((String) o);
                }
                catch (JMSException e) {
                    throw(new IllegalArgumentException("failed to set body: "+
                        Event.traceStack(e)));
                }
            }
            else {
                isDynamic = true;
            }
        }

        previousDstStatus = DST_UNKNOWN - 1;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        java.lang.reflect.Method pub_v0 = null;
        int dstStatus = DST_OK;
        report.clear();
        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                return report;
            }
            else {
                skip = NOSKIP;
                serialNumber ++;
            }
        }
        else {
            skip = NOSKIP;
            serialNumber ++;
        }

        if (dependencyGroup != null) { // check dependency
            skip = MonitorUtils.checkDependencies(currentTime, dependencyGroup,
                name);
            if (skip != NOSKIP) {
                if (skip == EXCEPTION) {
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                    return report;
                }
                else if (skip == SKIPPED)
                    return report;
                else if (!disabledWithReport)
                    return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        if (props != null && props.size() > 0) {
            Object o;
            if (uri.startsWith("wmq://")) {
                if (operation == DST_PUB) {
                    try {
                        Class<?>cls=Class.forName("org.qbroker.wmq.TConnector");
                        java.lang.reflect.Constructor con =
                            cls.getConstructor(new Class[]{Map.class});
                        jmsTCon =
                           (TopicConnector)con.newInstance(new Object[]{props});
                        outMessage = (TextMessage) jmsTCon.createMessage(1);
                        if (brokerVersion <= 0)
                            pub_v0 = cls.getMethod("publish_v0",
                                new Class[]{String.class, Message.class});
                        jmsTCon.close();
                    }
                    catch (InvocationTargetException e) {
                        Throwable ex = e.getTargetException();
                        if (ex == null)
                            throw(new IOException("failed to instantiate "+
                                "TopicConnector: " + Event.traceStack(e)));
                        else
                            throw(new IOException("failed to instantiate "+
                                "TopicConnector: " + Event.traceStack(e)));
                    }
                    catch (Exception e) {
                        throw(new IOException("failed to instantiate "+
                            "TopicConnector: " + Event.traceStack(e)));
                    }
                }
                else {
                    try {
                        Class<?>cls=Class.forName("org.qbroker.wmq.QConnector");
                        java.lang.reflect.Constructor con =
                            cls.getConstructor(new Class[]{Map.class});
                        jmsQCon =
                           (QueueConnector)con.newInstance(new Object[]{props});
                        outMessage = (TextMessage) jmsQCon.createMessage(1);
                        jmsQCon.close();
                    }
                    catch (InvocationTargetException e) {
                        Throwable ex = e.getTargetException();
                        if (ex == null)
                            throw(new IOException("failed to instantiate "+
                                "QueueConnector: " + Event.traceStack(e)));
                        else
                            throw(new IOException("failed to instantiate "+
                                "QueueConnector: " + Event.traceStack(ex)));
                    }
                    catch (Exception e) {
                        throw(new IOException("failed to instantiate "+
                            "QueueConnector: " + Event.traceStack(e)));
                    }
                }
            }
            else {
                if (operation == DST_PUB) {
                    try {
                        jmsTCon =
                        (TopicConnector) new org.qbroker.jms.TConnector(props);
                        outMessage = (TextMessage) jmsTCon.createMessage(1);
                        jmsTCon.close();
                    }
                    catch (Exception e) {
                        throw(new IOException("failed to instantiate "+
                            "TopicConnector: " + Event.traceStack(e)));
                    }
                }
                else {
                    try {
                        jmsQCon =
                        (QueueConnector) new org.qbroker.jms.QConnector(props);
                        outMessage = (TextMessage) jmsQCon.createMessage(1);
                        jmsQCon.close();
                    }
                    catch (Exception e) {
                        throw(new IOException("failed to instantiate "+
                            "QueueConnector: " + Event.traceStack(e)));
                    }
                }
            }
            if ((o = props.get("JMSPropertyGroup")) != null) {
                Map h = (HashMap) o;
                Iterator iter = h.keySet().iterator();
                try {
                    while (iter.hasNext()) {
                        String key = (String) iter.next();
                        if (key == null) continue;
                        if ("JMSType".equals(key))
                            outMessage.setJMSType((String) h.get(key));
                        else if ("JMSCorrelationID".equals(key))
                            outMessage.setJMSCorrelationID((String) h.get(key));
                        else if (key.startsWith("JMS"))
                            continue;
                        else
                           outMessage.setStringProperty(key,(String)h.get(key));
                    }
                }
                catch (JMSException e) {
                    throw(new IOException("failed to set property: "+
                        Event.traceStack(e)));
                }
            }
            if (!isDynamic) {
                try {
                    outMessage.setText(template.copyText());
                }
                catch (JMSException e) {
                    throw(new IOException("failed to set body: "+
                        Event.traceStack(e)));
                }
            }
            props.clear();
            props = null;
        }

        if (isDynamic) {
            String text = template.copyText();
            if (template.containsField("hostname") ||
                template.containsField("HOSTNAME"))
                text = MonitorUtils.substitute(text, template);

            if (template.containsField("date")) {
                Map<String, String> data = new HashMap<String, String>();
                data.put("date", Event.dateFormat(new Date(currentTime)));
                text = MonitorUtils.substitute(text, template, data);
                data.clear();
            }

            try {
                outMessage.clearBody();
                outMessage.setText(text);
            }
            catch (Exception e) {
                throw(new IOException("failed to update the outmsg: " +
                    Event.traceStack(e)));
            }
        }

        if (operation == DST_PUB) {
            try {
                jmsTCon.reconnect();
                if (brokerVersion > 0) {
                    TopicPublisher tPublisher = jmsTCon.getTopicPublisher();
                    tPublisher.publish(outMessage);
                }
                else if (pub_v0 != null) {
                    pub_v0.invoke(jmsTCon,
                        new Object[]{tName, (Message) outMessage});
                }
                else {
                    jmsTCon.close();
                    throw new IOException("pub_v0 is null for: "+brokerVersion);
                }
                jmsTCon.close();
            }
            catch (JMSException e) {
                dstStatus = DST_FAILED;
            }
            catch (Exception e) {
                throw(new IOException("failed to pub a msg: " +
                    Event.traceStack(e)));
            }
        }
        else {
            String str = jmsQCon.reconnect();
            if (str != null) {
                dstStatus = DST_FAILED;
                new Event(Event.ERR, "failed to reconnect to " + uri +
                    " on " + qName + ": " + str).send();
            }
            else try {
                QueueSender qSender = jmsQCon.getQueueSender();
                qSender.send(outMessage);
                jmsQCon.close();
            }
            catch (JMSException e) {
                dstStatus = DST_FAILED;
            }
            catch (Exception e) {
                throw(new IOException("failed to send a msg: " +
                    Event.traceStack(e)));
            }
        }

        report.put("DstStatus", String.valueOf(dstStatus));

        if ((disableMode > 0 && dstStatus != DST_OK) ||
            (disableMode < 0 && dstStatus == DST_OK))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int dstStatus = DST_UNKNOWN, level = 0;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("DstStatus")) != null && o instanceof String)
            dstStatus = Integer.parseInt((String) o);

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                exceptionCount = 0;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            level = Event.INFO;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
            }
          case TimeWindows.EXCEPTION: // exception
            actionCount = 0;
            if (status == TimeWindows.EXCEPTION) {
                level = Event.WARNING;
                if (previousStatus != status) { // reset count and adjust step
                    exceptionCount = 0;
                    if (step > 0)
                        step = 0;
                }
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            dstStatus = DST_UNKNOWN;
            break;
          case TimeWindows.BLACKOUT: // blackout
            level = Event.INFO;
            if (previousStatus != status) {
                if (normalStep > 0)
                    step = normalStep;
                actionCount = 0;
                exceptionCount = 0;
            }
          default: // normal cases
            level = Event.INFO;
            exceptionCount = 0;
            if (status != TimeWindows.BLACKOUT &&
                previousStatus == TimeWindows.BLACKOUT)
                actionCount = 0;
            actionCount ++;
            if (dstStatus != DST_OK) {
                if (operation == DST_PUB)
                    strBuf.append("Topic: failed to publish a msg to " + tName);
                else
                    strBuf.append("Queue: failed to put a msg to " + qName);
                dstStatus = DST_FAILED;
                if (previousDstStatus != dstStatus)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                }
            }
            else if (previousDstStatus != dstStatus) {
                if (operation == DST_PUB)
                    strBuf.append("Topic: publish is OK");
                else
                    strBuf.append("Queue: put is OK");
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousStatus = status;
        previousDstStatus = dstStatus;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            count = actionCount;
            if (repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxPage) // only page maxPage times
                return null;
            break;
          case Event.ERR: // found errors
            count = actionCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                break;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // exceptions
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
          default:
            if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
        }

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
        }

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("connFactory", connFactoryName);
        if (operation == DST_PUB)
            event.setAttribute("topic", tName);
        else
            event.setAttribute("queue", qName);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        event.setAttribute("actionScript", actionStatus);
        event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousDstStatus", String.valueOf(previousDstStatus));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pDstStatus, pStatus, sNumber;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ActionCount")) != null)
            aCount = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ExceptionCount")) != null)
            eCount = Integer.parseInt((String) o);
        else
            return;

        if ((o = chkpt.get("PreviousDstStatus")) != null)
            pDstStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousDstStatus = pDstStatus;
    }

    public void destroy() {
        super.destroy();
        if (jmsQCon != null) {
            jmsQCon.close();
            jmsQCon = null;
        }
        if (jmsTCon != null) {
            jmsTCon.close();
            jmsTCon = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
