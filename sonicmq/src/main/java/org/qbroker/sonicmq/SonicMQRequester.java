package org.qbroker.sonicmq;

/* SonicMQRequester.java - a JMS/JMX requester for SonicMQ */

import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Comparator;
import java.util.HashSet;
import java.io.IOException;
import javax.management.ObjectName;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanInfo;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.JMException;
import javax.jms.Message;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.jmx.client.JMSConnectorAddress;
import com.sonicsw.mf.jmx.client.ExpressionBasedNotificationFilter;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;
import com.sonicsw.mf.mgmtapi.runtime.ProxyRuntimeException;
import com.sonicsw.mq.common.runtime.IConnectionData;
import com.sonicsw.mq.common.runtime.IDurableSubscriptionData;
import com.sonicsw.mq.common.runtime.ISubscriberData;
import com.sonicsw.mq.common.runtime.IQueueData;
import com.sonicsw.mq.common.runtime.IBrowseToken;
import com.sonicsw.mq.common.runtime.IMessageHeader;
import com.sonicsw.mq.common.runtime.IMessage;
import com.sonicsw.mq.common.runtime.JMSObjectFactory;
import com.sonicsw.mf.common.runtime.INotification;
import com.sonicsw.mf.common.metrics.IMetricInfo;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.common.metrics.IMetricsData;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.MFException;
import org.qbroker.common.Utils;
import org.qbroker.common.Requester;
import org.qbroker.common.GenericNotificationListener;
import org.qbroker.common.TraceStackThread;
import org.qbroker.common.WrapperException;
import org.qbroker.net.JMXRequester;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.Msg2Text;

/**
 * SonicMQRequester connects to a SonicMQ broker for JMX requests. Currently,
 * it supports the method of list, query, browse on durable subscriptions,
 * addListener and removeListener.
 *<br/></br>
 * Since ObjectName does not allow any key containing the char of ':', the
 * replacement of ':' to '.' on subscription_id is assumed for queries as a
 * workaround. It means you have to replace ':' with '.' in the query if the
 * subscription_id is specified and it contains ':'.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SonicMQRequester implements Requester, Comparator<IMetric> {
    protected String uri;
    private boolean isConnected = false;
    private String username = null;
    private String password = null;
    private long timeout = 10000L;
    private JMSConnectorClient jmxc = null;
    private Map<NotificationListener, IBrokerProxy> cache; // for listener
    private Msg2Text msg2Text = null;

    /** Creates new SonicMQRequester */
    public SonicMQRequester(Map props) {
        Object o;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;
        if (!uri.startsWith("tcp://"))
            throw(new IllegalArgumentException("Bad service URL: " + uri));

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
        }

        if ((o = props.get("Timeout")) != null)
            timeout = Long.parseLong((String) o);
        if (timeout <= 0)
            timeout = 10000L;

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o)) try {
            connect();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to initialize " +
                "JMS/JMX connector for " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }

        cache = new HashMap<NotificationListener, IBrokerProxy>();
    }

    /** returns names of the all MBeans for the given pattern on the server */
    public String[] list(String target, String regex) throws JMException {
        String[] keys = null;
        String domain, brokerName, category;
        ObjectName objName = null;

        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("target not defined for "+ uri));
        else try {
            objName = new ObjectName(target);
            domain = objName.getDomain();
            brokerName = objName.getKeyProperty("ID");
            category = objName.getKeyProperty("category");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to parse " + target +
                " for " + uri + ": " + e.toString()));
        }

        if (domain == null || domain.length() <= 0)
            throw(new IllegalArgumentException("domain not defined in "+ target+
                " for " + uri));
        else if (brokerName == null || brokerName.length() <= 0)
            throw(new IllegalArgumentException("ID not defined in " + target +
                " for " + uri));
        else if (category == null || category.length() <= 0)
            throw(new IllegalArgumentException("category not defined in " +
                target + " for " + uri));
        else if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            int n;
            String prefix;
            IBrokerProxy broker = MQProxyFactory.createBrokerProxy(jmxc,
                new ObjectName(domain + ":ID=" + brokerName));
            switch (category.charAt(0)) {
              case 'c':
                prefix = objName.getKeyProperty("user");
                if (prefix != null && (n = prefix.length()) > 0) {
                    if (prefix.endsWith("*"))
                        prefix = prefix.substring(0, n - 1);
                }
                keys = listConnections(broker, prefix, regex);
                break;
              case 'q':
                prefix = objName.getKeyProperty("queue");
                if (prefix != null && (n = prefix.length()) > 0) {
                    if (prefix.endsWith("*"))
                        prefix = prefix.substring(0, n - 1);
                }
                keys = listQueues(broker, prefix, regex);
                break;
              case 's':
                prefix = objName.getKeyProperty("user");
                if (prefix != null && (n = prefix.length()) > 0) {
                    if (prefix.endsWith("*"))
                        prefix = prefix.substring(0, n - 1);
                }
                if (category.endsWith("r"))
                    keys = listSubscribers(broker, prefix, regex);
                else
                    keys = listDurableSubscriptions(broker, prefix, regex);
                break;
              case 'm':
                prefix = objName.getKeyProperty("type");
                if (prefix != null && (n = prefix.length()) > 0) {
                    if (prefix.endsWith("*"))
                        prefix = prefix.substring(0, n - 1);
                }
                keys = listActiveMetrics(broker, prefix);
                break;
              case 'u':
                prefix = objName.getKeyProperty("user");
                if (prefix != null && (n = prefix.length()) > 0) {
                    if (prefix.endsWith("*"))
                        prefix = prefix.substring(0, n - 1);
                }
                keys = listUsersWithDurableSubscriptions(broker, prefix);
                break;
              default:
                throw(new IllegalArgumentException("list not supported on " +
                    target + " for " + uri));
            }
        }
        catch (IllegalArgumentException e) {
            throw(e);
        }
        catch (Exception e) {
            throw(new JMException("failed to list MBeans on " + target +
                " for " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        return keys;
    }

    @SuppressWarnings("unchecked")
    /** returns list of maps for MBeans */
    public List<Map> query(String target, String regex) throws JMException {
        List<Map> list = null;
        String domain, brokerName, category;
        ObjectName objName = null;

        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("target not defined for "+ uri));
        else try {
            objName = new ObjectName(target);
            domain = objName.getDomain();
            brokerName = objName.getKeyProperty("ID");
            category = objName.getKeyProperty("category");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to parse " + target +
                " for " + uri + ": " + e.toString()));
        }

        if (domain == null || domain.length() <= 0)
            throw(new IllegalArgumentException("domain not defined in "+ target+
                " for " + uri));
        else if (brokerName == null || brokerName.length() <= 0)
            throw(new IllegalArgumentException("ID not defined in " + target +
                " for " + uri));
        else if (category == null || category.length() <= 0)
            throw(new IllegalArgumentException("category not defined in " +
                target + " for " + uri));
        else if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            int n;
            String topic, cid, sid, id, prefix;
            IBrokerProxy broker = MQProxyFactory.createBrokerProxy(jmxc,
                new ObjectName(domain + ":ID=" + brokerName));
            switch (category.charAt(0)) {
              case 'c':
                prefix = objName.getKeyProperty("user");
                id = objName.getKeyProperty("connect_id");
                list = queryConnections(broker, prefix, id, regex);
                break;
              case 'q':
                prefix = objName.getKeyProperty("queue");
                id = objName.getKeyProperty("id");
                list = queryQueues(broker, prefix, id, regex);
                break;
              case 's':
                prefix = objName.getKeyProperty("user");
                id = objName.getKeyProperty("connect_id");
                cid = objName.getKeyProperty("client_id");
                sid = objName.getKeyProperty("subscription_id");
                topic = objName.getKeyProperty("topic");
                if (category.endsWith("r"))
                    list = querySubscribers(broker, prefix, id,
                        topic, cid, sid, regex);
                else
                    list = queryDurableSubscriptions(broker, prefix,
                        topic, cid, sid, regex);
                break;
              case 'm':
                prefix = objName.getKeyProperty("type");
                id = objName.getKeyProperty("name");
                if (id == null && "connection".equals(prefix) && // check user
                    (id = objName.getKeyProperty("user")) != null) {
                    Map<String, String> tag = new HashMap<String, String>();
                    topic = objName.getKeyProperty("topic");
                    StringBuffer strBuf = new StringBuffer();
                    if (topic != null) { // for topic connections
                        sid = objName.getKeyProperty("subscription_id");
                        list = querySubscribers(broker, id, null, topic,
                            null, sid, regex);
                        for (Map ph : list) {
                            id = (String) ph.get("connect_id");
                            tag.put(id, (String) ph.get("host"));
                            if (strBuf.length() > 0)
                                strBuf.append("|");
                            strBuf.append(id.replaceAll("\\$", "\\\\\\$"));
                        }
                    }
                    else if (objName.getKeyProperty("queue") != null) {
                        HashSet<String> h = new HashSet<String>();
                        list = querySubscribers(broker, id, null, null,
                            null, null, null);
                        for (Map ph : list) {
                            h.add((String) ph.get("connect_id"));
                        }
                        list.clear();
                        list = queryConnections(broker, id, null, regex);
                        for (Map ph : list) {
                            id = (String) ph.get("connect_id");
                            if (h.contains(id)) // excluding topic connections
                                continue;
                            tag.put(id, (String) ph.get("host"));
                            if (strBuf.length() > 0)
                                strBuf.append("|");
                            strBuf.append(id.replaceAll("\\$", "\\\\\\$"));
                        }
                    }
                    else { // for all connections
                        list = queryConnections(broker, id, null, regex);
                        for (Map ph : list) {
                            id = (String) ph.get("connect_id");
                            tag.put(id, (String) ph.get("host"));
                            if (strBuf.length() > 0)
                                strBuf.append("|");
                            strBuf.append(id.replaceAll("\\$", "\\\\\\$"));
                        }
                    }
                    if (tag.size() <= 0) // no connection found
                        list = new ArrayList<Map>();
                    else { // reset regex
                        regex = "^.*(" + strBuf.toString() + ")$";
                        list = queryActiveMetrics(broker, prefix, null, regex);
                        for (Map ph : list) { // add host as a tag
                            id = (String) ph.get("name");
                            ph.put("host", tag.get(id));
                        }
                    }
                    break;
                }
                list = queryActiveMetrics(broker, prefix, id, regex);
                break;
              case 'u':
                prefix = objName.getKeyProperty("user");
                cid = objName.getKeyProperty("client_id");
                sid = objName.getKeyProperty("subscription_id");
                topic = objName.getKeyProperty("topic");
                list = queryUsersWithDurableSubscriptions(broker, prefix,
                    topic, cid, sid, regex);
                break;
              default:
                throw(new IllegalArgumentException("query not supported on " +
                    target + " for " + uri));
            }
        }
        catch (IllegalArgumentException e) {
            throw(e);
        }
        catch (Exception e) {
            throw(new JMException("failed to query MBeans on " + target +
                " for " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        return list;
    }

    /** returns a list of messages for a durable subscription or null */
    public List<Message> browse(String target, int limit, boolean isLocal)
        throws JMException {
        List list;
        String domain, brokerName, category, sid, topic;
        ObjectName objName = null;

        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("target not defined for "+ uri));
        else try {
            objName = new ObjectName(target);
            domain = objName.getDomain();
            brokerName = objName.getKeyProperty("ID");
            category = objName.getKeyProperty("category");
            sid = objName.getKeyProperty("subscription_id");
            if (sid != null)
                sid = sid.replace('.', ':');
            topic = objName.getKeyProperty("topic");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to parse " + target +
                " for " + uri + ": " + e.toString()));
        }

        if (domain == null || domain.length() <= 0)
            throw(new IllegalArgumentException("domain not defined in "+ target+
                " for " + uri));
        else if (brokerName == null || brokerName.length() <= 0)
            throw(new IllegalArgumentException("ID not defined in " + target +
                " for " + uri));
        else if (!"topic".equals(category))
            throw(new IllegalArgumentException("wrong category for browse in " +
                target + " for " + uri));
        else if (topic == null || topic.length() <= 0)
            throw(new IllegalArgumentException("topic not defined in " + target+
                " for " + uri));
        else if (sid == null || sid.length() <= 0)
            throw(new IllegalArgumentException("subscription_id not defined " +
                "in " + target + " for " + uri));
        else if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            String cid, prefix;
            IBrokerProxy broker = MQProxyFactory.createBrokerProxy(jmxc,
                new ObjectName(domain + ":ID=" + brokerName));
            prefix = objName.getKeyProperty("user");
            cid = objName.getKeyProperty("client_id");
            boolean withClientID = (cid != null && cid.length() > 0);
            if (prefix == null || prefix.length() <= 0) { // without prefix
                list = broker.getUsersWithDurableSubscriptions(null);
            }
            else { // with prefix
                list = broker.getUsersWithDurableSubscriptions(prefix);
            }
            for (Object obj : list) {
                String user = (String) obj;
                List sl = broker.getDurableSubscriptions(user);
                for (Object o : sl) {
                    IDurableSubscriptionData isub =
                        (IDurableSubscriptionData) o;
                    if (!topic.equals(isub.getTopicName()))
                        continue;
                    if (!sid.equals(isub.getSubscriptionName()))
                        continue;
                    if (withClientID && !cid.equals(isub.getClientID()))
                        continue;
                    return browseDurableSubscription(broker,isub,limit,isLocal);
                }
            }
        }
        catch (Exception e) {
            throw(new JMException("failed to browse messages for " + target +
                " on " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        return null;
    }

    public NotificationListener addListener(String target) throws JMException {
        return addListener(target, null, null, null);
    }

    /**
     *  adds a listener with a filter for notifications to the target and
     *  waits for interruptions while listening on the filtered notifications
     */
    public NotificationListener addListener(String target, String className,
        String methodName, Object obj) throws JMException {
        String domain, brokerName, category;
        ObjectName objName = null;

        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("target not defined for "+ uri));
        else try {
            objName = new ObjectName(target);
            domain = objName.getDomain();
            brokerName = objName.getKeyProperty("ID");
            category = objName.getKeyProperty("category");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to parse " + target +
                " for " + uri + ": " + e.toString()));
        }

        if (domain == null || domain.length() <= 0)
            throw(new IllegalArgumentException("domain not defined in "+ target+
                " for " + uri));
        else if (brokerName == null || brokerName.length() <= 0)
            throw(new IllegalArgumentException("ID not defined in " + target +
                " for " + uri));
        else if (!"notification".equals(category))
            throw(new IllegalArgumentException("wrong category for listen in " +
                target + " for " + uri));
        else if (className != null && className.length() > 0 &&
            (methodName == null || methodName.length() <= 0))
            throw(new IllegalArgumentException("no methodName for "+className));
        else if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            ExpressionBasedNotificationFilter filter;
            IBrokerProxy broker = MQProxyFactory.createBrokerProxy(jmxc,
                new ObjectName(domain + ":ID=" + brokerName));
            String filterStr = objName.getKeyProperty("type");
            if (filterStr == null || filterStr.length() <= 0 ||
                "*".equals(filterStr))
                filter = null;
            else { // with filter on notification types
                Hashtable<String, String> tab = objName.getKeyPropertyList();
                filter = new ExpressionBasedNotificationFilter();
                filter.setTypeExpression(filterStr);
                for (String key : tab.keySet()) {
                    if (key.equals("ID") || key.equals("category") ||
                        key.equals("type"))
                        continue;
                    filter.setAttributeExpression(key, tab.get(key));
                }
                tab.clear();
            }

            if (className == null || className.length() <= 0) { // default
                className = "org.qbroker.sonicmq.SonicMQRequester";
                methodName = "displayNotification";
                obj = null;
            }

            NotificationListener listener =
                new GenericNotificationListener(className, methodName);

            broker.addNotificationListener(listener, filter, obj);
            cache.put(listener, broker);
            return listener;
        }
        catch (Exception e) {
            throw(new JMException("failed to add a listener to " + target +
                " on " + uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** removes the listener */
    public void removeListener(NotificationListener listener) {
        IBrokerProxy broker = cache.get(listener);
        if (broker != null)
            broker.removeNotificationListener(listener);
        else
            throw(new IllegalArgumentException("bad listener"));
    }

    /** returns an Map with MBeanInfo for the target */
    public Map<String, List> getInfo(String target) throws JMException {
        if (target == null || target.length() <= 0)
            return null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            ObjectName objName;
            MBeanInfo mbInfo;
            Map<String, List> ph = new HashMap<String, List>();

            objName = new ObjectName(target);

            // get MBean info
            mbInfo = jmxc.getMBeanInfo(objName);
            if (mbInfo != null) {
                List<Map> list;
                Map<String, String> h;
                MBeanAttributeInfo[] aList = mbInfo.getAttributes();
                if (aList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<aList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", aList[i].getName());
                        h.put("type", aList[i].getType());
                        h.put("description", aList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Attribute", list);
                }
                MBeanOperationInfo[] oList = mbInfo.getOperations();
                if (oList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<oList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", oList[i].getName());
                        h.put("type", oList[i].getReturnType());
                        h.put("description", oList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Operation", list);
                }
                MBeanNotificationInfo[] nList = mbInfo.getNotifications();
                if (nList != null) {
                    list = new ArrayList<Map>();
                    for (int i=0; i<nList.length; i++) {
                        h = new HashMap<String, String>();
                        h.put("name", nList[i].getName());
                        h.put("type", nList[i].getNotifTypes()[0]);
                        h.put("description", nList[i].getDescription());
                        list.add(h);
                    }
                    if (list.size() > 0)
                        ph.put("Notification", list);
                }
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get info for " + target+" from "+
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the value of the attribute from the target */
    public Object getValue(String target, String attr) throws JMException {
        if (target == null || target.length() <= 0 || attr == null ||
            attr.length() <= 0)
            return null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            ObjectName objName;

            objName = new ObjectName(target);
            // Display MBean attributes 
            return jmxc.getAttribute(objName, attr);
        }
        catch (Exception e) {
            throw(new JMException("failed to get value for " + target+" from "+
                uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the value of the attribute from the objName */
    public Object getValue(ObjectName objName, String attr) throws JMException {
        if (objName == null || attr == null || attr.length() <= 0)
            return null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            // Display MBean attributes 
            return jmxc.getAttribute(objName, attr);
        }
        catch (Exception e) {
            throw(new JMException("failed to get value for "+objName.toString()+
                " from " + uri + ": " + TraceStackThread.traceStack(e)));
        }
    }

    /** returns the key-value map for the attributes from the target */
    public Map<String, Object> getValues(String target, String[] attrs)
        throws JMException {
        if (target == null || target.length() <= 0 || attrs == null ||
            attrs.length <= 0)
            return null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            int n;
            ObjectName objName;
            AttributeList list;
            Attribute attr;
            Map<String, Object> ph = new HashMap<String, Object>();

            objName = new ObjectName(target);
            // Display MBean attributes 
            list = jmxc.getAttributes(objName, attrs);
            n = list.size();
            for (int i=0; i<n; i++) {
                attr = (Attribute) list.get(i);
                ph.put(attrs[i], attr.getValue());
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get attribute list for " +
                target + " from " + uri +": "+TraceStackThread.traceStack(e)));
        }
    }

    /** returns the key-value map for the attributes from the objName */
    public Map<String, Object> getValues(ObjectName objName, String[] attrs)
        throws JMException {
        if (objName == null || attrs == null || attrs.length <= 0)
            return null;

        if (jmxc == null || !isConnected)
            throw(new JMException("no connection to " + uri));
        else try {
            int n;
            AttributeList list;
            Attribute attr;
            Map<String, Object> ph = new HashMap<String, Object>();

            // Display MBean attributes 
            list = jmxc.getAttributes(objName, attrs);
            n = list.size();
            for (int i=0; i<n; i++) {
                attr = (Attribute) list.get(i);
                ph.put(attrs[i], attr.getValue());
            }
            return ph;
        }
        catch (Exception e) {
            throw(new JMException("failed to get attribute list for " +
                objName.toString() + " from " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }
    }

    /**
     * This takes a JMX query command and converts it into a JMX request.
     * It sends the request to the destination and returns the number of items
     * filled in the given string buffer. The content in the string buffer
     * is a JSON. If the disconnect flag is set to true, it disconnects right
     * before it returns. Otherwise, it will keep the connection.
     *<br/></br/>
     * The JMX command is supposed to be as follows:
     *<br/>
     * DISPLAY Target attr0:attr1:attr2
     *<br/></br/>
     * In case of a fatal failure, it disconnects first and then throws a
     * WrapperException with cause set to the vendor specific exception or IO
     * related exception. Therefore please make sure to get the cause exception
     * from it and handles the cause exception with care. For other failures,
     * it throws an IllegalArgumentException. If the disconnect flag is set to
     * true, it disconnects right before it throws the exception.
     */
    public int getResponse(String jmxCmd, StringBuffer strBuf,
        boolean autoDisconn) throws WrapperException {
        String target, attrs, line = null;
        int k = -1;
        boolean isPattern = false;

        if (jmxCmd == null || jmxCmd.length() <= 0)
            throw(new IllegalArgumentException("empty request for " + uri)); 

        target = JMXRequester.getTarget(jmxCmd);
        attrs = JMXRequester.getAttributes(jmxCmd);
        if (target == null || target.length() <= 0)
            throw(new IllegalArgumentException("bad request of " + jmxCmd +
                " for " + uri)); 

        if (strBuf == null)
            throw(new IllegalArgumentException("response buffer is null on " +
                target + " for " + uri));

        if (!isConnected) {
            String str;
            if ((str = reconnect()) != null) {
                throw(new WrapperException("SonicMQ connection failed on " +
                    uri + ": " + str));
            }
        }

        String[] keys = null;
        if (target.indexOf(",category=") < 0) { // not supported yet
            if (autoDisconn)
                close();
            throw(new IllegalArgumentException("bad target of " + target +
                " for " + uri)); 
        }
        else if (target.indexOf(",category=topic") > 0) { // for a browse
            keys = new String[0];
            isPattern = false;
        }
        else if (target.indexOf('*') < 0) { // for a query
            keys = new String[]{target};
            isPattern = false;
        }
        else try { // for a list with pattern
            keys = list(target, attrs);
            if (keys == null)
                keys = new String[0];
            isPattern = true;
        }
        catch (JMException e) {
            close();
            throw(new WrapperException("failed to list on " + target +
                " from " + uri, e));
        }
        catch (Exception e) {
            if (autoDisconn)
                close();
            throw(new IllegalArgumentException("failed to list on " + target +
               " from " + uri + ": " + TraceStackThread.traceStack(e)));
        }

        if ((k = strBuf.length()) > 0)
            strBuf.delete(0, k);

        if (isPattern) { // for a list
            k = 0;
            for (int i=0; i<keys.length; i++) {
                line = "\"" + Utils.escapeJSON(keys[i]) + "\"";
                if (k > 0)
                    strBuf.append(",");
                strBuf.append(line + Utils.RS);
                k ++;
            }
        }
        else if (keys.length == 0) { // for a browse on the first msg
            List<Message> list;
            int limit = 1;
            if (attrs != null && attrs.length() > 0) try {
                limit = Integer.parseInt(attrs);
            }
            catch (Exception e) {
                limit = 1;
            }
            try {
                list = browse(target, limit, true);
            }
            catch (JMException e) {
                close();
                throw(new WrapperException("failed to query on " + target +
                    " from " + uri, e));
            }
            catch (Exception e) {
                if (autoDisconn)
                    close();
                throw(new IllegalArgumentException("failed to query on " +
                    target + " from " + uri + ": " +
                    TraceStackThread.traceStack(e)));
            }
            if (list == null) { // no durable subscription found
                throw(new IllegalArgumentException("no durable subscription " +
                    "found on " + target + " from " + uri));
            }
            if (msg2Text == null) { // init the default formatter
                HashMap<String, String> ph = new HashMap<String, String>();
                ph.put("Name", uri);
                ph.put("BaseTag", "Record");
                msg2Text = new Msg2Text(ph);
            }
            k = 0;
            for (Message msg : list) {
                line = msg2Text.format(Utils.RESULT_JSON, msg);
                if (line != null && line.length() > 0) {
                    if (k > 0)
                        strBuf.append(",");
                    strBuf.append(line + Utils.RS);
                    k ++;
                }
            }
        }
        else { // for a query
            List<Map> list;
            try {
                if (attrs == null || attrs.length() <= 0) // select all
                    list = query(target, null);
                else if (attrs.indexOf(':') > 0) { // select many
                    attrs = attrs.replaceAll("\\$", "\\\\\\$");
                    attrs = attrs.replace(':', '|');
                    list = query(target, "^.*(" + attrs + ")$");
                }
                else { // select one
                    attrs = attrs.replaceAll("\\$", "\\\\\\$");
                    list = query(target, "^.*" + attrs + "$");
                }
            }
            catch (JMException e) {
                close();
                throw(new WrapperException("failed to query on " + target +
                    " from " + uri, e));
            }
            catch (Exception e) {
                if (autoDisconn)
                    close();
                throw(new IllegalArgumentException("failed to query on " +
                    target + " from " + uri + ": " +
                    TraceStackThread.traceStack(e)));
            }

            k = 0;
            for (Map mp : list) {
                line = Utils.toJSON(mp);
                if (line != null && line.length() > 0) {
                    if (k > 0)
                        strBuf.append(",");
                    strBuf.append(line + Utils.RS);
                    k ++;
                }
            }
        }

        strBuf.insert(0, "{" + Utils.RS + "\"Record\":[");
        strBuf.append("]}");
        if (autoDisconn)
            close();
        return k;
    }

    private void connect() throws IOException {
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put("ConnectionURLs", uri);
        env.put("DefaultUser", username);
        env.put("DefaultPassword", password);

        jmxc = null;
        isConnected = false;
        try {
            jmxc = new JMSConnectorClient();
            JMSConnectorAddress address = new JMSConnectorAddress(env);
            jmxc.connect(address, timeout);
        }
        catch (Exception e) {
            throw(new IOException("failed to initialize " +
                "JMS/JMX connector for " + uri + ": " +
                TraceStackThread.traceStack(e)));
        }
        if (jmxc != null)
            isConnected = true;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** returns null if reconnected or error msg otherwise */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (Exception e) {
            return e.toString();
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (jmxc != null) try {
            for (NotificationListener lsnr : cache.keySet()) {
                IBrokerProxy broker = cache.get(lsnr);
                if (broker != null) try {
                    broker.removeNotificationListener(lsnr);
                }
                catch (Exception ex) {
                }
            }
            jmxc.disconnect();
        }
        catch (Exception e) {
        }
        jmxc = null;
        isConnected = false;
    }

    public int compare(IMetric a, IMetric b) {
        String[] ka = a.getMetricIdentity().getName().split("\\.");
        String[] kb = b.getMetricIdentity().getName().split("\\.");
        int i = ka[0].compareTo(kb[0]);
        if (i == 0) {
            if (ka[0].equals("broker")) // for broker
                i = (ka[1] + ka[2]).compareTo(kb[1] + kb[2]);
            else { // for connections or queues
                i = ka[3].compareTo(kb[3]);
                if (i == 0)
                    i = ka[2].compareTo(kb[2]);
            }
        }
        return i;
    }

    /** returns names of the connections with a given username */
    private String[] listConnections(IBrokerProxy broker, String username,
        String regex) {
        List list;
        List<String> pl = new ArrayList<String>();
        boolean withRegex = (regex != null && regex.length() > 0);
        if (username == null || username.length() <= 0) { // without username
            list = broker.getConnections(null);
        }
        else { // with username
            list = broker.getConnections(username);
        }
        for (Object obj : list) {
            IConnectionData iconn = (IConnectionData) obj;
            if (!iconn.isApplicationConnection())
                continue;
            if (withRegex && !iconn.getHost().matches(regex))
                continue;
            pl.add(iconn.getConnectID());
        }

        return pl.toArray(new String[pl.size()]);
    }

    /** returns a list of maps for the connections with a prefix on usernames */
    private List<Map> queryConnections(IBrokerProxy broker, String username,
        String connectID, String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        boolean withConnectID = (connectID != null && connectID.length() > 0);
        boolean withRegex = (regex != null && regex.length() > 0);
        if (withConnectID)
            withRegex = false;
        if (username == null || username.length() <= 0) { // without username
            list = broker.getConnections(null);
        }
        else { // with username
            list = broker.getConnections(username);
        }
        for (Object obj : list) {
            IConnectionData iconn = (IConnectionData) obj;
            if (!iconn.isApplicationConnection())
                continue;
            String str = iconn.getConnectID();
            if (withConnectID && !connectID.equals(str))
                continue;
            if (withRegex && !iconn.getHost().matches(regex))
                continue;
            Map<String, String> ph = new HashMap<String, String>();
            ph.put("broker", brokerName);
            ph.put("category", "connection");
            ph.put("connect_id", str);
            ph.put("host", iconn.getHost());
            ph.put("user", iconn.getUser());
            pl.add(ph);
        }

        return pl;
    }

    /** returns names of the subscribers with a given prefix on username */
    private String[] listSubscribers(IBrokerProxy broker, String username,
        String regex) {
        List list;
        List<String> pl = new ArrayList<String>();
        boolean withRegex = (regex != null && regex.length() > 0);
        if (username == null || username.length() <= 0) { // without username
            list = broker.getConnections(null);
        }
        else { // with username
            list = broker.getConnections(username);
        }
        for (Object obj : list) {
            IConnectionData iconn = (IConnectionData) obj;
            if (!iconn.isApplicationConnection())
                continue;
            if (withRegex && !iconn.getHost().matches(regex))
                continue;
            String connectID = iconn.getConnectID();
            String user = iconn.getUser();
            List sl = broker.getSubscribers(connectID, user);
            for (Object o : sl) {
                ISubscriberData isub = (ISubscriberData) o;
                String cid = isub.getClientID(); 
                String sid = isub.getSubscriptionName(); 
                pl.add("user=" + user + ",connect_id=" + connectID +
                    ",topic=" + isub.getTopicName() + ((cid == null ||
                    cid.length() <= 0) ? "" : ",client_id=" + cid) +
                    ((sid == null || sid.length() <= 0) ? "" :
                    ",subscription_id=" + sid));
            }
        }

        return pl.toArray(new String[pl.size()]);
    }

    /** returns a list of maps for the subscribers with a prefix on username */
    private List<Map> querySubscribers(IBrokerProxy broker, String username,
        String connectID, String topic, String clientID, String subID,
        String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        boolean withConnectID = (connectID != null && connectID.length() > 0);
        boolean withRegex = (regex != null && regex.length() > 0);
        if (withConnectID)
            withRegex = false;
        boolean withTopic = (topic != null && topic.length() > 0);
        boolean withClientID = (clientID != null && clientID.length() > 0);
        boolean withSubID = (subID != null && subID.length() > 0);
        if (withSubID)
            subID = subID.replace('.', ':');
        if (username == null || username.length() <= 0) { // without username
            list = broker.getConnections(null);
        }
        else { // with username
            list = broker.getConnections(username);
        }
        for (Object obj : list) {
            IConnectionData iconn = (IConnectionData) obj;
            if (!iconn.isApplicationConnection())
                continue;
            String id = iconn.getConnectID();
            if (withConnectID && !connectID.equals(id))
                continue;
            if (withRegex && !iconn.getHost().matches(regex))
                continue;
            List sl = broker.getSubscribers(id, iconn.getUser());
            for (Object o : sl) {
                ISubscriberData isub = (ISubscriberData) o;
                String str = isub.getTopicName();
                if (withTopic && !topic.equals(str))
                    continue;
                String cid = isub.getClientID();
                if (withClientID && !clientID.equals(cid))
                    continue;
                String sid = isub.getSubscriptionName();
                if (withSubID && !subID.equals(sid))
                    continue;
                Map<String, String> ph = new HashMap<String, String>();
                ph.put("broker", brokerName);
                ph.put("category", "subscriber");
                ph.put("connect_id", id);
                ph.put("topic", str);
                if (cid != null && cid.length() > 0)
                    ph.put("client_id", cid);
                if (sid != null && sid.length() > 0)
                    ph.put("subscription_id", sid);
                ph.put("user", iconn.getUser());
                ph.put("host", iconn.getHost());
                ph.put("messagecount", String.valueOf(isub.getMessageCount()));
                ph.put("messagesize", String.valueOf(isub.getMessageSize()));
                ph.put("isDurable", ((isub.isDurable()) ? "true" : "false"));
                pl.add(ph);
            }
        }

        return pl;
    }

    /** returns names of the queues with a given prefix on queue name */
    private String[] listQueues(IBrokerProxy broker, String qPrefix,
        String regex) {
        List list;
        List<String> pl = new ArrayList<String>();
        boolean withRegex = (regex != null && regex.length() > 0);
        if (qPrefix == null || qPrefix.length() <= 0) { // without qPrefix
            list = broker.getQueues(null);
        }
        else { // with qPrefix
            list = broker.getQueues(qPrefix);
        }
        for (Object obj : list) {
            IQueueData iq = (IQueueData) obj;
            if (iq.isTemporaryQueue())
                continue;
            String str = iq.getQueueName();
            if (withRegex && !str.matches(regex))
                continue;
            pl.add(str);
        }

        return pl.toArray(new String[pl.size()]);
    }

    /** returns a list of maps for the queues with a prefix on queue name */
    private List<Map> queryQueues(IBrokerProxy broker, String qPrefix,
        String qName, String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        boolean withQName = (qName != null && qName.length() > 0);
	boolean withRegex = (regex != null && regex.length() > 0);
        if (withQName)
            withRegex = false;
        if (qPrefix == null || qPrefix.length() <= 0) { // without qPrefix
            list = broker.getQueues(null);
        }
        else { // with qPrefix
            list = broker.getQueues(qPrefix);
        }
        for (Object obj : list) {
            IQueueData iq = (IQueueData) obj;
            if (iq.isTemporaryQueue())
                continue;
            String str = iq.getQueueName();
            if (withQName && !qName.equals(str))
                continue;
            if (withRegex && !str.matches(regex))
                continue;
            Map<String, String> ph = new HashMap<String, String>();
            ph.put("broker", brokerName);
            ph.put("category", "queue");
            ph.put("queue", str);
            ph.put("messagecount", String.valueOf(iq.getMessageCount()));
            ph.put("totalmessagesize",String.valueOf(iq.getTotalMessageSize()));
            pl.add(ph);
        }

        return pl;
    }

    /** returns names of durable subscriptions with a given prefix on users */
    private String[] listDurableSubscriptions(IBrokerProxy broker,
        String username, String regex) {
        List list;
        List<String> pl = new ArrayList<String>();
	boolean withRegex = (regex != null && regex.length() > 0);
        if (username == null || username.length() <= 0) { // without username
            list = broker.getUsersWithDurableSubscriptions(null);
        }
        else { // with username
            list = broker.getUsersWithDurableSubscriptions(username);
        }
        for (Object obj : list) {
            String user = (String) obj;
            List sl = broker.getDurableSubscriptions(user);
            for (Object o : sl) {
                IDurableSubscriptionData isub = (IDurableSubscriptionData) o;
                String str = isub.getTopicName();
                if (withRegex && ! str.matches(regex))
                    continue;
                String cid = isub.getClientID(); 
                String sid = isub.getSubscriptionName();
                pl.add("user=" + user + ",topic=" + isub.getTopicName() +
                    ((cid == null || cid.length() <= 0)?"":",client_id="+cid)+
                    ",subscription_id=" + sid);
            }
        }

        return pl.toArray(new String[pl.size()]);
    }

    /** returns a list of maps of durable subscriptions for username */
    private List<Map> queryDurableSubscriptions(IBrokerProxy broker,
        String username, String topic, String clientID, String subID,
        String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        boolean withTopic = (topic != null && topic.length() > 0);
	boolean withRegex = (regex != null && regex.length() > 0);
        if (withTopic)
            withRegex = false;
        boolean withClientID = (clientID != null && clientID.length() > 0);
        boolean withSubID = (subID != null && subID.length() > 0);
        if (withSubID)
            subID = subID.replace('.', ':');
        if (username == null || username.length() <= 0) { // without username
            list = broker.getUsersWithDurableSubscriptions(null);
        }
        else { // with username
            list = broker.getUsersWithDurableSubscriptions(username);
        }
        for (Object obj : list) {
            String user = (String) obj;
            List sl = broker.getDurableSubscriptions(user);
            for (Object o : sl) {
                IDurableSubscriptionData isub = (IDurableSubscriptionData) o;
                String str = isub.getTopicName();
                if (withTopic && !topic.equals(str))
                    continue;
                if (withRegex && !str.matches(regex))
                    continue;
                String cid = isub.getClientID();
                if (withClientID && !clientID.equals(cid))
                    continue;
                String sid = isub.getSubscriptionName();
                if (withSubID && !subID.equals(sid))
                    continue;
                Map<String, String> ph = new HashMap<String, String>();
                ph.put("broker", brokerName);
                ph.put("category", "subscription");
                ph.put("user", user);
                ph.put("topic", str);
                if (cid != null && cid.length() > 0)
                    ph.put("client_id", cid);
                ph.put("subscription_id", sid);
                ph.put("messagecount", String.valueOf(isub.getMessageCount()));
                ph.put("messagesize", String.valueOf(isub.getMessageSize()));
                ph.put("lastconnectedtime",
                    String.valueOf(isub.getLastConnectedTime()));
                pl.add(ph);
            }
        }

        return pl;
    }

    /** returns names of the active metrics with a given type */
    private String[] listActiveMetrics(IBrokerProxy broker, String type) {
        IMetricInfo[] info = broker.getMetricsInfo();
        int n = info.length;
        IMetricIdentity[] ida;
        if (type == null || type.length() <= 0) { // without type
            IMetricIdentity[] ids = new IMetricIdentity[n];
            for (int i=0; i<n; i++) {
                ids[i] = info[i].getMetricIdentity();
            }
            ida = broker.getActiveMetrics(ids);
        }
        else { // with type
            int k = 0;
            ida = new IMetricIdentity[n];
            for (int i=0; i<n; i++) {
                ida[i] = null;
                if (info[i].getMetricIdentity().getName().startsWith(type))
                    ida[k++] = info[i].getMetricIdentity();
            }
            IMetricIdentity[] ids = new IMetricIdentity[k];
            for (int i=0; i<k; i++)
                ids[i] = ida[i];
            ida = broker.getActiveMetrics(ids);
        }
        n = ida.length;
        String[] keys = new String[n];
        for (int i=0; i<n; i++)
            keys[i] = ida[i].getName();

        return keys;
    }

    /** returns a list of maps for actvie metrics with a type and a name */
    private List<Map> queryActiveMetrics(IBrokerProxy broker, String type,
        String name, String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        IMetricInfo[] info = broker.getMetricsInfo();
        int n = info.length;
        IMetricIdentity[] ida;
        boolean withName = (name != null && name.length() > 0);
	boolean withRegex = (regex != null && regex.length() > 0);
        if (withName)
            withRegex = false;
        if (type == null || type.length() <= 0) { // without type
            IMetricIdentity[] ids = new IMetricIdentity[n];
            for (int i=0; i<n; i++) {
                ids[i] = info[i].getMetricIdentity();
            }
            ida = broker.getActiveMetrics(ids);
        }
        else { // with type
            int k = 0;
            ida = new IMetricIdentity[n];
            for (int i=0; i<n; i++) {
                ida[i] = null;
                if (info[i].getMetricIdentity().getName().startsWith(type))
                    ida[k++] = info[i].getMetricIdentity();
            }
            IMetricIdentity[] ids = new IMetricIdentity[k];
            for (int i=0; i<k; i++)
                ids[i] = ida[i];
            ida = broker.getActiveMetrics(ids);
            n = ida.length;
            if (withName && n > 0) {
                k = 0;
                ids = new IMetricIdentity[n];
                for (int i=0; i<n; i++) {
                    if (!ida[i].getName().endsWith(name)) // filtering out
                        continue;
                    ids[k++] = ida[i];
                }
                if (k < n) { // shrunked
                    ida = new IMetricIdentity[k];
                    for (int i=0; i<k; i++)
                        ida[i] = ids[i];
                }
            }
            else if (withRegex && n > 0) {
                k = 0;
                ids = new IMetricIdentity[n];
                for (int i=0; i<n; i++) {
                    if (!ida[i].getName().matches(regex)) // filtering out
                        continue;
                    ids[k++] = ida[i];
                }
                if (k < n) { // shrunked
                    ida = new IMetricIdentity[k];
                    for (int i=0; i<k; i++)
                        ida[i] = ids[i];
                }
            }
        }
        if (ida.length > 0) {
            Map<String, String> ph = null;
            String key = null;
            IMetricsData data = broker.getMetricsData(ida, false);
            String tm = String.valueOf(data.getCurrencyTimestamp());
            IMetric[] metrics = data.getMetrics();
            Arrays.sort(metrics, this);
            for (IMetric me : metrics) {
                String[] keys = me.getMetricIdentity().getName().split("\\.");
                if (keys.length > 4) // skip metrics for SonicMQ
                    continue;
                if (key == null || // first metric
                    (!"broker".equals(keys[0])) && !key.equals(keys[3])) {
                    ph = new HashMap<String, String>();
                    ph.put("broker", brokerName);
                    ph.put("category", "metric");
                    ph.put("type", keys[0]);
                    ph.put("currencytimestamp", tm);
                    pl.add(ph);
                    if ("broker".equals(keys[0])) // for broker
                        key = brokerName;
                    else // for connections or queues
                        key = keys[3];
                    ph.put("name", key);
                }
                ph.put(keys[1] + "_" + keys[2], String.valueOf(me.getValue()));
            }
        }

        return pl;
    }

    /** returns usernames with durable subscriptions with a given prefix */
    private String[] listUsersWithDurableSubscriptions(IBrokerProxy broker,
        String userName) {
        List list;
        if (userName == null || userName.length() <= 0) { // without userName
            list = broker.getUsersWithDurableSubscriptions(null);
        }
        else { // with userName
            list = broker.getUsersWithDurableSubscriptions(userName);
        }
        int n = list.size();
        String[] keys = new String[n];
        for (int i=0; i<n; i++)
            keys[i] = (String) list.get(i);

        return keys;
    }

   /** returns a list of maps of durable subscriptions for users */
    private List<Map> queryUsersWithDurableSubscriptions(IBrokerProxy broker,
        String username, String topic, String clientID, String subID,
        String regex) {
        List list;
        List<Map> pl = new ArrayList<Map>();
        String brokerName = broker.getBrokerName();
        boolean withTopic = (topic != null && topic.length() > 0);
        boolean withClientID = (clientID != null && clientID.length() > 0);
        boolean withSubID = (subID != null && subID.length() > 0);
	boolean withRegex = (regex != null && regex.length() > 0);
        if (withSubID) {
            withRegex = false;
            subID = subID.replace('.', ':');
        }
        if (username == null || username.length() <= 0) { // without username
            list = broker.getUsersWithDurableSubscriptions(null);
        }
        else { // with username
            list = broker.getUsersWithDurableSubscriptions(username);
        }
        for (Object obj : list) {
            String user = (String) obj;
            List sl = broker.getDurableSubscriptions(user);
            for (Object o : sl) {
                IDurableSubscriptionData isub = (IDurableSubscriptionData) o;
                String str = isub.getTopicName();
                if (withTopic && !topic.equals(str))
                    continue;
                String cid = isub.getClientID();
                if (withClientID && !clientID.equals(cid))
                    continue;
                String sid = isub.getSubscriptionName();
                if (withSubID && !subID.equals(sid))
                    continue;
                if (withRegex && !sid.matches(regex))
                    continue;
                Map<String, String> ph = new HashMap<String, String>();
                ph.put("broker", brokerName);
                ph.put("category", "subscription");
                ph.put("user", user);
                ph.put("topic", str);
                if (cid != null && cid.length() > 0)
                    ph.put("client_id", cid);
                ph.put("subscription_id", sid);
                ph.put("messagecount", String.valueOf(isub.getMessageCount()));
                ph.put("messagesize", String.valueOf(isub.getMessageSize()));
                ph.put("lastconnectedtime",
                    String.valueOf(isub.getLastConnectedTime()));
                pl.add(ph);
            }
        }

        return pl;
    }

    /** returns a list of JMS messages browsed from a durable subscription */
    private List<Message> browseDurableSubscription(IBrokerProxy broker,
        IDurableSubscriptionData isub, int limit, boolean isLocal)
        throws MFException {
        List list;
        List<Message> pl = new ArrayList<Message>();
        if (isub.getMessageCount() <= 0)
            return pl;

        IBrowseToken bt = broker.createDurableBrowseTokenFromDSD(isub,
            new Boolean(isLocal));
        list = broker.getBrowseMessages(bt, new Integer(limit));
        for (Object obj : list) {
            IMessageHeader imsgH = (IMessageHeader) obj;
            IMessage imsg = broker.getMessage(imsgH.getToken());
            if (imsg == null)
                continue;
            pl.add(JMSObjectFactory.createJMSMessage(imsg));
        }

        return pl;
    }

    /** displays the notification in details */
    public static void displayNotification(Notification notification) {
        System.out.println(new Date(notification.getTimeStamp()) +
            " notification=" + notification.getType() + ", source=" +
            notification.getSource());
        Map attr = ((INotification) notification).getAttributes();
        for (Object key : attr.keySet()) {
            System.out.println("\t" + (String) key +": " +
                (String) attr.get(key));
        }
    }

    public static Map getAttributes(Notification notification) {
        if (notification != null)
            return ((INotification) notification).getAttributes();
        else
            return null;
    }

    public static void main(String[] args) {
        String url = null;
        String username = null;
        String password = null;
        String target = null;
        String attr = null;
        String[] keys;
        SonicMQRequester jmx = null;
        Map<String, Object> ph;
        int i, n = 0;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'u':
                if (i+1 < args.length)
                    url = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    username = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    password = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    target = args[++i];
                break;
              case 'a':
                if (i+1 < args.length)
                    attr = args[++i];
                break;
              default:
            }
        }

        ph = new HashMap<String, Object>();
        ph.put("URI", url);
        if (username != null) {
            ph.put("Username", username);
            ph.put("Password", password);
        }

        try {
            jmx = new SonicMQRequester(ph);
            if (target == null || target.length() <= 0) { // not bounded
                System.out.println("SonicMQ JMS/JMX supports bounded queries " +
                    "only. Please specify target in the following format:");
                System.out.println("\tdomain.container:ID=component" +
                    "[,category=<CATEGORY>[,key=<VALUE>]]");
                System.out.println("where key is the name of the parameter. "+
                    "The <VALUE> ending with '*' is for list only. The " +
                    "keyword of category is required whereas its value of " +
                    "<CATEGORY> specifies the query space with the following "+
                    "values supported:");
                System.out.println("\tcategory=connection for active " +
                    "connections with selectable items of connect_id or host");
                System.out.println("\tcategory=subscriber for active " + 
                    "subscribers with selectable items of topic or host");
                System.out.println("\tcategory=subscription for durable " +
                    "subscriptions with selectable items of topic");
                System.out.println("\tcategory=queue for non-temporary " + 
                    "queues with selectable items of queue name");
                System.out.println("\tcategory=metric for active metrics " + 
                    "with selectable items of either connect_id or queue name");
                System.out.println("\tcategory=user for users with durable " +
                    "subscriptions");
                System.out.println("\tcategory=topic for browsing up to 1000 " +
                    "messages from the first durable subscription matched " +
                    "with a specific set of topic, subscription_id, cleint_id");
                System.out.println("\tcategory=notification for listening on "+
                    "the JMX notifications");
            }
            else if (target.indexOf(",category=topic") > 0) { // for browse
                List<Message> list;
                if (attr != null)
                    n = Integer.parseInt(attr);
                else
                    n = 1;
                list = jmx.browse(target, n, true);
                if (list != null) {
                    Message msg;
                    n = list.size();
                    System.out.println("browsed "+ n + " messages:");
                    for (i=0; i<n; i++) {
                        msg = (Message) list.get(i);
                        attr = MessageUtils.processBody(msg, new byte[4096]);
                        System.out.println(i + ":" +
                            MessageUtils.display(msg, attr, 4095, null));
                    }
                }
                else
                    System.out.println("no subscription found for " + target);
            }
            else if (target.indexOf(",category=notification") > 0) {//for listen
                NotificationListener lsnr = jmx.addListener(target);
                if (lsnr != null) {
                    System.out.println("listener is added for " + target);
                    System.out.println("Press Ctrl-C to stop listening");
                    try { // wait for interruption
                        Thread.currentThread().join();
                    }
                    catch (InterruptedException e) {
                    }
                    jmx.removeListener(lsnr);
                    System.out.println("listener is removed for " + target);
                }
            }
            else if (target.indexOf('*') >= 0) { // for list
                keys = jmx.list(target, attr);
                n = keys.length;
                for (i=0; i<n; i++)
                    System.out.println(i + ": " + keys[i]);
            }
            else if (target.indexOf(",category=") > 0) {
                // for invoking queries
                List<Map> list;
                String str = null;
                if (attr == null || attr.length() <= 0) // select all
                    list = jmx.query(target, null);
                else if (attr.indexOf(':') > 0) { // select many
                    attr = attr.replaceAll("\\$", "\\\\\\$");
                    attr = attr.replace(':', '|');
                    list = jmx.query(target, "^.*(" + attr + ")$");
                }
                else { // select one
                    attr = attr.replaceAll("\\$", "\\\\\\$");
                    list = jmx.query(target, "^.*" + attr + "$");
                }
                i = 0;
                for (Map mp : list) {
                    str = (String) mp.get("currencytimestamp");
                    if (str != null)
                        str = new Date(Long.parseLong(str)).toString();
                    else
                        str = new Date().toString();
                    System.out.println(str + " " + i++ + ": broker=" +
                        (String) mp.get("broker") + ",category=" +
                        (String) mp.get("category"));
                    Iterator iter = mp.keySet().iterator();
                    while (iter.hasNext()) {
                        Object o;
                        String key = (String) iter.next();
                        if (key.equals("broker") || key.equals("category"))
                            continue;
                        o = mp.get(key);
                        if (o != null)
                            System.out.println("\t"+key+": "+ o.toString());
                    }
                }
            }
            else if (attr == null || attr.length() <= 0) { // for info
                Object o;
                List list;
                Map<String, List> pl = jmx.getInfo(target);
                if ((list = pl.get("Attribute")) != null) {
                    n = list.size();
                    if (n > 0)
                        System.out.println("Attribute: Name, Type, Desc");
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        System.out.println(i + ": "+((Map) o).get("name") +
                            ", " + ((Map) o).get("type") +
                            ", " + ((Map) o).get("description"));
                    }
                }
                if ((list = pl.get("Operation")) != null) {
                    n = list.size();
                    if (n > 0)
                        System.out.println("Operation: Name, Type, Desc");
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        System.out.println(i + ": "+((Map) o).get("name") +
                            ", " + ((Map) o).get("type") +
                            ", " + ((Map) o).get("description"));
                    }
                }
                if ((list = pl.get("Notification")) != null) {
                    n = list.size();
                    if (n > 0)
                        System.out.println("Notification: Name, Type, Desc");
                    for (i=0; i<n; i++) {
                        o = list.get(i);
                        System.out.println(i + ": "+((Map) o).get("name") +
                            ", " + ((Map) o).get("type") +
                            ", " + ((Map) o).get("description"));
                    }
                }
            }
            else if ((n = attr.indexOf(':')) > 0) { // for multiple attributes
                Object o;
                List<String> list = new ArrayList<String>();
                i = 0;
                do {
                    list.add(attr.substring(i, n));
                    i = n + 1;
                } while ((n = attr.indexOf(':', i)) > i);
                list.add(attr.substring(i));
                n = list.size();
                keys = list.toArray(new String[n]);
                ph = jmx.getValues(target, keys);
                for (i=0; i<n; i++) {
                    o = ph.get(keys[i]);
                    if (o != null)
                        System.out.println(keys[i] + ": " + o.toString());
                }
            }
            else { // for a single attribute
                Object o = jmx.getValue(target, attr);
                if (o != null)
                    System.out.println(attr + ": " + o.toString());
            }
            if (jmx != null)
                jmx.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (jmx != null)
                jmx.close();
        }
    }

    private static void printUsage() {
        System.out.println("SonicMQRequester Version 1.0 (written by Yannan Lu)");
        System.out.println("SonicMQRequester: list all MBeans on SonicMQ Broker");
        System.out.println("Usage: java org.qbroker.net.SonicMQRequester -u url -n username -p password -t target -a attributes");
        System.out.println(" -?: print this message");
        System.out.println("  u: service url");
        System.out.println("  n: username");
        System.out.println("  p: password");
        System.out.println("  t: target");
        System.out.println("  a: attributes or items delimited via ':'");
        System.out.println("\nExample: java org.qbroker.net.SonicMQRequester -u tcp://intsonicqa1:2506 -n Administrator -p xxxx");
    }
}
