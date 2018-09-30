package org.qbroker.jms;

/* QConnector.java - a connector for generic JMS queues */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.JMSException;
import javax.jms.DeliveryMode;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Utils;
import org.qbroker.common.QuickCache;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.JMSQConnector;
import org.qbroker.event.Event;

/**
 * QConnector connects to a JMS queue and initializes one of the operations,
 * such as browse, query, get or put. It supports repeated browse on a queue
 * with auto increment of JMSTimestamp.  But it requires Mode is set to daemon
 * and SequentialSearch is on.  The application is supposed to reset the
 * browser every time with null string as the only argument.  If ReferenceFile
 * is defined, the state info will be persisted to the file for restart.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class QConnector extends JMSQConnector {
    private String connectionFactoryName;
    private Hashtable<String, String> env;

    /** Creates new QConnector */
    public QConnector(Map props) throws JMSException {
        Object o;
        String str, uri;
        SimpleDateFormat dateFormat;
        boolean ignoreInitFailure = false;
        env = new Hashtable<String, String>();

        if ((o = props.get("ContextFactory")) != null) {
            str = (String) o;
            env.put(Context.INITIAL_CONTEXT_FACTORY, str);
        }
        else
           throw(new IllegalArgumentException("ContextFactory is not defined"));

        if ((o = props.get("URI")) != null)
            uri = (String) o;
        else
            throw(new IllegalArgumentException("URI is not defined"));

        env.put(Context.PROVIDER_URL, uri);

        if("com.sun.enterprise.naming.SerialInitContextFactory".equals(str))try{
            int port;
            URI u = new URI(uri);
            if ("iiop".equals(u.getScheme())) { // hack for Glassfish V3
                str = u.getHost();
                port = u.getPort();
                if (str == null || str.length() <= 0)
                    str = "localhost";
                env.put("org.omg.CORBA.ORBInitialHost", str);
                System.setProperty("org.omg.CORBA.ORBInitialHost", str);
                str = (port > 0) ? String.valueOf(port) : "3700";
                env.put("org.omg.CORBA.ORBInitialPort", str);
                System.setProperty("org.omg.CORBA.ORBInitialPort", str);
            }
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        // for URL PKG support
        if ((o = props.get("URLPkgs")) != null)
            env.put(Context.URL_PKG_PREFIXES, (String) o);

        // for STATE FACTORIES support
        if ((o = props.get("StateFactories")) != null)
            env.put(Context.STATE_FACTORIES, (String) o);

        if ((o = props.get("Principal")) != null) {
            env.put(Context.SECURITY_PRINCIPAL, (String) o);
            if ((o = props.get("Credentials")) != null)
                env.put(Context.SECURITY_CREDENTIALS, (String) o);
            else if ((o = props.get("EncryptedCredentials")) != null) try {
                env.put(Context.SECURITY_CREDENTIALS, Utils.decrypt((String)o));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedCredentials: " + e.toString()));
            }
        }

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

        connectionFactoryName = (String) props.get("ConnectionFactoryName");
        if(connectionFactoryName == null || connectionFactoryName.length() <= 0)
            throw(new IllegalArgumentException("QCF name not well defined"));
        qName = (String) props.get("QueueName");  // JNDI name for the queue
        if(qName == null || qName.length() <= 0)
            throw(new IllegalArgumentException("Q name not well defined"));
        msgSelector = (String) props.get("MessageSelector");

        if ((o = props.get("IsPhysical")) != null && "true".equals((String) o))
            isPhysical = true;
        else
            isPhysical = false;

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value, cellID;
            Template temp = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            cellID = (String) props.get("CellID");
            if (cellID == null || cellID.length() <= 0)
                cellID = "0";
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if((propertyName[n] = MessageUtils.getPropertyID(key)) == null){
                    propertyName[n] = key;
                    resetOption = true;
                }
                if (value != null && value.length() > 0) {
                    propertyValue[n] = temp.substitute("CellID", cellID, value);
                }
                else if ("JMSReplyTo".equals(key)) {
                    propertyValue[n] = "";
                }
                if ("JMSReplyTo".equals(key) && n > 0) { // swap with the first
                    key = propertyName[0];
                    propertyName[0] = propertyName[n];
                    propertyName[n] = key;
                    value = propertyValue[0];
                    propertyValue[0] = propertyValue[n];
                    propertyValue[n] = value;
                }
                n ++;
            }
        }

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if ((o = props.get("Operation")) != null)
            operation = (String) props.get("Operation");
        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else if ((o = props.get("CellID")) != null) {
            partition = new int[2];
            partition[0] = Integer.parseInt((String) o);
            partition[1] = 1;
        }
        else {
            partition = new int[2];
            partition[0] = 0;
            partition[1] = 0;
        }
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("BatchSize")) != null)
            batchSize = Integer.parseInt((String) o);
        if (batchSize <= 0)
            batchSize = 1;
        if ((o = props.get("Persistence")) != null) {
            persistence = Integer.parseInt((String) o);
            if (persistence == 1)
                persistence = DeliveryMode.NON_PERSISTENT;
            else if (persistence == 2)
                persistence = DeliveryMode.PERSISTENT;
            overwrite += 1;
        }
        if ((o = props.get("Priority")) != null) {
            priority = Integer.parseInt((String) o);
            if (priority == -2)
                overwrite = -1;
            else
                overwrite += 2;
        }
        if ((o = props.get("Expiry")) != null) {
            expiry = Long.parseLong((String) o);
            overwrite += 4;
        }
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        if ((o = props.get("Offhead")) != null)
            offhead = Integer.parseInt((String) o);
        if (offhead < 0)
            offhead = 0;
        if ((o = props.get("Offtail")) != null)
            offtail = Integer.parseInt((String) o);
        if (offtail < 0)
            offtail = 0;
        boundary = 0;
        if ((o = props.get("SOTBytes")) != null) {
            sotBytes = MessageUtils.hexString2Bytes((String) o);
            if (sotBytes != null && sotBytes.length > 0)
                boundary += MS_SOT;
        }
        if ((o = props.get("EOTBytes")) != null) {
            eotBytes = MessageUtils.hexString2Bytes((String) o);
            if (eotBytes != null && eotBytes.length > 0)
                boundary += MS_EOT;
        }

        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberMessage")) != null)
            maxNumberMsg = Integer.parseInt((String) o);
        if ((o = props.get("MaxIdleTime")) != null) {
            maxIdleTime = 1000 * Integer.parseInt((String) o);
            if (maxIdleTime < 0)
                maxIdleTime = 0;
        }
        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        if ((o = props.get("WaitTime")) != null) { 
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 500L;
        }
        if ((o = props.get("SleepTime")) != null)
            sleepTime= Integer.parseInt((String) o);
        if ((o = props.get("IgnoreInitFailure")) != null &&
            "true".equalsIgnoreCase((String) o))
            ignoreInitFailure = true;

        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);

        if ("put".equals(operation)) {
            if ((o = props.get("WithFlowControlDisabled")) != null &&
                "true".equalsIgnoreCase((String) o)) // for SonicMQ only
                withFlowControlDisabled = true;
        }
        else if ("request".equals(operation)) {
            if ((o = props.get("RCField")) != null && o instanceof String)
                rcField = (String) o;
            else
                rcField = "ReturnCode";

            if ((o = props.get("RequestTimeout")) != null) {
                timeout = 1000 * Integer.parseInt((String) o);
                if (timeout <= 0)
                    timeout = 10000;
            }

            if ((o = props.get("ResponseProperty")) != null &&
                o instanceof List) {
                String key;
                List<String> pl = new ArrayList<String>();
                for (Object obj : (List) o) {
                    if (obj == null || !(obj instanceof String))
                        continue;
                    key = (String) obj;
                    if (key.length() <= 0)
                        continue;
                    key = MessageUtils.getPropertyID(key);
                    if (key != null)
                        pl.add(key);
                    else
                        pl.add((String) obj);
                }
                respPropertyName = pl.toArray(new String[pl.size()]);
                pl.clear();
            }
        }
        else if ("query".equals(operation)) {
            String saxParser = null;
            Map<String, String> h = new HashMap<String, String>();

            h.put("Name", qName);
            if ((o = props.get("ResultType")) != null)
                resultType = Integer.parseInt((String) o);

            if ((resultType & Utils.RESULT_XML) > 0)
                h.put("BaseTag", "Record");

            cache = new QuickCache(qName, QuickCache.META_DEFAULT, 0, 0);
            cache.insert("##*##", System.currentTimeMillis(), 0, null,
                new Msg2Text(h));
            h.clear();

            if ((o = props.get("MSField")) != null)
                msField = (String) o;
            else
                msField = "MSQL";

            if ((o = props.get("RCField")) != null && o instanceof String)
                rcField = (String) o;
            else
                rcField = "ReturnCode";

            if ((o = props.get("ResultField")) != null && o instanceof String)
                resultField = (String) o;
            else
                resultField = "MsgCount";

            if((o = props.get("Template")) != null && ((String) o).length() > 0)
                template = new Template((String) o);
        }
        else if ("browse".equals(operation)) try {
            if ((o = props.get("Ruleset")) != null && o instanceof List)
                msgFilters = MessageFilter.initFilters(props);
            else { // for backward compatibility
                Map<String, Object> ph = new HashMap<String, Object>();
                Map<String, Object> h = new HashMap<String, Object>();
                List<Object> pl = new ArrayList<Object>();
                ph.put("Ruleset", pl);
                pl.add(h);
                h.put("Name", qName);
                if ((o = props.get("JMSPropertyGroup")) != null)
                    h.put("JMSPropertyGroup", o);
                if ((o = props.get("XJMSPropertyGroup")) != null)
                    h.put("XJMSPropertyGroup", o);
                if ((o = props.get("PatternGroup")) != null)
                    h.put("PatternGroup", o);
                if ((o = props.get("XPatternGroup")) != null)
                    h.put("XPatternGroup", o);
                msgFilters = MessageFilter.initFilters(ph);
                ph.clear();
                pl.clear();
                h.clear();
            }

            if ((o = props.get("SequentialSearch")) != null &&
                "on".equals((String)o))
                sequentialSearch = 1;

            if ((o = props.get("ReferenceFile")) != null)
                referenceFile = new File((String) o);

            // load startTime and msgID
            if (referenceFile != null && referenceFile.exists()) {
                str = null;
                byte[] buffer = new byte[1024];
                int n, len = 0;
                try {
                    FileInputStream in = new FileInputStream(referenceFile);
                    while ((n = in.read(buffer, len, 1024 - len)) >= 0) {
                        len += n;
                        if (len >= 1024)
                            break;
                    }
                    in.close();
                    str = new String(buffer, 0, len);
                }
                catch (IOException e) {
                }
                if (str != null && str.length() > 0) {//load startTime and msgID
                    n = str.indexOf(" ");
                    timestamp = Long.parseLong(str.substring(0, n));
                    startTime = timestamp;
                    msgID = str.substring(n+1, str.length()-1);
                }
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");

        Date myDate = null;
        if ((o = props.get("EndDate")) != null) {
            myDate = dateFormat.parse((String) o, new ParsePosition(0));
            if (myDate != null) {
                endTime = myDate.getTime();
                if (msgSelector != null) {
                    msgSelector += " AND JMSTimestamp <= " + endTime;
                }
                else {
                    msgSelector = "JMSTimestamp <= " + endTime;
                }
            }
            myDate = null;
        }
        if ((o = props.get("StartDate")) != null) {
            myDate = dateFormat.parse((String) o, new ParsePosition(0));
            if (myDate != null)
                startTime = myDate.getTime();
        }
        if (startTime >= 0) {
            if (msgSelector != null) {
                msgSelector += " AND JMSTimestamp >= " + startTime;
            }
            else {
                msgSelector = "JMSTimestamp >= " + startTime;
            }
        }

        if ((o = props.get("EnableExceptionListener")) != null &&
            "true".equalsIgnoreCase((String) o))
            enable_exlsnr = true;

        try {
            connect();
        }
        catch (JMSException e) {
            str =  qName + ": ";
            Exception ee = e.getLinkedException();
            if (ee != null)
                str += "Linked exception: " + ee.toString() + "\n";
            if (ignoreInitFailure)
                new Event(Event.ERR, str + Event.traceStack(e)).send();
            else
                throw(new IllegalArgumentException(str + Event.traceStack(e)));
        }
    }

    /** Initializes the JMS components (QueueConnectionFactory,
     * QueueConnection) for use later in the application
     * @throws JMSException occurs on any JMS Error
     */
    protected QueueConnectionFactory initialize() throws JMSException {
        Context ctx;
        QueueConnectionFactory factory;
        try {
            ctx = new InitialContext(env);
        }
        catch (NamingException e) {
            ctx = null;
            throw(new JMSException("failed to get ctx: " + e));
        }

        try {
            factory = (QueueConnectionFactory)ctx.lookup(connectionFactoryName);
        }
        catch (NamingException e) {
            try {
                ctx.close();
            }
            catch (NamingException ex) {
            }
            try {
                Thread.sleep(500L);
            }
            catch (Exception ex) {
            }
            ctx = null;
            try { // retry
                ctx = new InitialContext(env);
              factory=(QueueConnectionFactory)ctx.lookup(connectionFactoryName);
            }
            catch (NamingException ex) {
                factory = null;
                try {
                    ctx.close();
                }
                catch (NamingException ee) {
                }
                throw(new JMSException(
                    "failed to lookup QueueConnnectionFactory '"+
                    connectionFactoryName + "': " + e.toString()));
            }
        }

        if (!isPhysical) try {
            queue = (Queue) ctx.lookup(qName);
        }
        catch (NamingException e) {
            try {
                Thread.sleep(500L);
            }
            catch (Exception ex) {
            }
            try { // retry
                queue = (Queue) ctx.lookup(qName);
            }
            catch (NamingException ex) {
                queue = null;
                try {
                    ctx.close();
                }
                catch (NamingException ee) {
                }
                throw(new JMSException("failed to lookup queue '" + qName +
                    "': " + e.toString()));
            }
        }

        try {
            ctx.close();
        }
        catch (NamingException e) {
        }

        return factory;
    }
}
