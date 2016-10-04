package org.qbroker.jms;

/* MessageUtils.java - a wrapper of utilites for JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Date;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.DeliveryMode;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.event.Event;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DataSet;
import org.qbroker.common.Browser;
import org.qbroker.common.XQueue;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;

/**
 * MessageUtils contains a bunch of constants and static methods on JMSMessage
 * for common uses
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class MessageUtils {
    private static Template rptTemplate = null;
    private static MonitorReport defaultReporter = null;
    private static java.lang.reflect.Method enableFlowControl = null;
    private static java.lang.reflect.Method disableFlowControl = null;
    public final static int SHOW_BODY = 1;
    public final static int SHOW_DATE = 2;
    public final static int SHOW_SIZE = 4;
    public final static int SHOW_DST = 8;
    public final static int SHOW_MSGID = 16;
    public final static int SHOW_CORRID = 32;
    public final static int SHOW_PROPS = 64;
    public final static int SHOW_EXPIRY = 128;
    public final static int SHOW_PRIORITY = 256;
    public final static int SHOW_DMODE = 512;
    public final static int SHOW_DCOUNT = 1024;
    public final static int SHOW_TSTAMP = 2048;
    public final static int SHOW_MSGTYPE = 4096;
    public final static int SHOW_REPLY = 8192;
    public final static int SHOW_FAMILY = 16384;
    public final static int SHOW_AGE = 32768;
    public final static int SHOW_NOTHING = 65536;
    public final static int XA_CLIENT = 1;
    public final static int XA_COMMIT = 2;
    /** everything is OK */
    public final static int RC_OK = 0;
    /** unexpected message type */
    public final static int RC_MSGERROR = 1;
    /** required msg property not found */
    public final static int RC_NOTFOUND = 2;
    /** illegal request */
    public final static int RC_REQERROR = 4;
    /** unexpected request data */
    public final static int RC_BADDATA = 8;
    /** JMS failure */
    public final static int RC_JMSERROR = 16;
    /** IO faillure */
    public final static int RC_IOERROR = 32;
    /** Database failure */
    public final static int RC_DBERROR = 64;
    /** message expired */
    public final static int RC_EXPIRED = 128;
    /** request timed out */
    public final static int RC_TIMEOUT = 256;
    /** auth failure */
    public final static int RC_AUTHERROR = 512;
    /** unexpected response */
    public final static int RC_BADRESP = 1024;
    /** unexpected error */
    public final static int RC_UNKNOWN = -1;
    public final static float JAVA_VERSION =
        Float.parseFloat(System.getProperty("java.specification.version"));
    public final static String OS_NAME = System.getProperty("os.name");
    public final static String OS_ARCH = System.getProperty("os.arch");
    public final static String OS_VERSION = System.getProperty("os.version");
    public final static String USER_NAME = System.getProperty("user.name");
    private final static String hostname = Event.getHostName().toLowerCase();
    private final static Map defaultMap = new HashMap();
    private final static Template defaultTemplate =
        new Template("${hostname}, ${HOSTNAME}, ${owner}, ${pid}",
        "\\$\\{[^\\$\\{\\}]+\\}");

    /** default constructor for MessageUtils */
    public MessageUtils() {
    }

    static {
        defaultMap.put("hostname", hostname);
        defaultMap.put("HOSTNAME", hostname.toUpperCase());
        defaultMap.put("owner", USER_NAME);
        defaultMap.put("pid", String.valueOf(Event.getPID()));
    }

    public static String substitute(String input, Template template) {
        return substitute(input, template, null);
    }

    public static String substitute(String input, Template template, Map map) {
        if (input == null)
            return input;

        if (template == null)
            template = defaultTemplate;

        if (map != null)
            return template.substitute(input, map);
        else
            return template.substitute(input, defaultMap);
    }

    public static String substitute(String input) {
        Map report = null;
        MonitorReport reporter = null;

        if (input == null || input.indexOf("${") < 0)
            return input;

        if (defaultReporter == null) { // init the default reporter
            Map<String, Object> ph = new HashMap<String, Object>();
            ph.put("Name", "GlobalProperties");
            ph.put("Type", "ReportQuery");
            ph.put("ReportKey", new ArrayList());
            ph.put("ReportClass", "org.qbroker.flow.QFlow");
            ph.put("Step", "1");
            reporter = MonitorUtils.getNewReport(ph);
            defaultReporter = reporter;
            ph.clear();
        }
        else // use the cached one
            reporter = defaultReporter;

        if (reporter == null)
            return null;
        else try {
            report = reporter.generateReport(System.currentTimeMillis());
        }
        catch (Exception e) { // no report found so take the default
            return substitute(input, defaultTemplate, defaultMap);
        }

        if (report == null || report.size() <= 0) // empty report
            return null;
        else synchronized (reporter) { // got the report
            Template template = null;
            String tempStr = (String) report.get("Template");
            if (tempStr == null || tempStr.indexOf("${") < 0)
                template = defaultTemplate;
            else if (rptTemplate == null) {
                template = new Template(tempStr, "\\$\\{[^\\$\\{\\}]+\\}");
                rptTemplate = template;
            }
            else if (tempStr.equals(rptTemplate.copyText())) // no change
                template = rptTemplate;
            else { // overwrite it
                template = new Template(tempStr, "\\$\\{[^\\$\\{\\}]+\\}");
                rptTemplate = template;
            }
            return substitute(input, template, report);
        }
    }

    /** It returns a Map with fully cloned and substituted properties */
    public static Map<String, Object> substituteProperties(Map props) {
        Object o;
        Map<String, Object> ph;
        String key, str;

        if (props == null)
            return null;

        ph = new HashMap<String, Object>();
        for (Iterator iter = props.keySet().iterator(); iter.hasNext();) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = props.get(key);
            if (o == null)
                ph.put(key, o);
            else if (o instanceof Map)
                ph.put(key, substituteProperties((Map) o));
            else if (o instanceof List)
                ph.put(key, substituteProperties((List) o));
            else if (o instanceof String) {
                str = (String) o;
                if (str.indexOf("${") >= 0)
                    str = substitute(str);
                ph.put(key, str);
            }
            else {
                str = o.toString();
                if (str.indexOf("${") >= 0)
                    str = substitute(str);
                ph.put(key, o.toString());
            }
        }

        return ph;
    }

    /** It returns a List with fully cloned and substituted properties */
    public static List<Object> substituteProperties(List props) {
        Object o;
        List<Object> list;
        String str;
        int i, n;

        if (props == null)
            return null;

        list = new ArrayList<Object>();
        n = props.size();
        for (i=0; i<n; i++) {
            o = props.get(i);
            if (o == null)
                list.add(o);
            else if (o instanceof Map)
                list.add(substituteProperties((Map) o));
            else if (o instanceof List)
                list.add(substituteProperties((List) o));
            else if (o instanceof String) {
                str = (String) o;
                if (str.indexOf("${") >= 0)
                    str = substitute(str);
                list.add(str);
            }
            else {
                str = o.toString();
                if (str.indexOf("${") >= 0)
                    str = substitute(str);
                list.add(str);
            }
        }

        return list;
    }

    /**
     * returns the property ID for a JMSProperty
     */
    public static String getPropertyID(String name) {
        if (name == null || !name.startsWith("JMS"))
            return null;

        if ("JMSType".equals(name))
            return String.valueOf(SHOW_MSGTYPE);
        else if ("JMSDestination".equals(name))
            return String.valueOf(SHOW_DST);
        else if ("JMSDate".equals(name))
            return String.valueOf(SHOW_DATE);
        else if ("JMSAge".equals(name))
            return String.valueOf(SHOW_AGE);
        else if ("JMSMessageID".equals(name))
            return String.valueOf(SHOW_MSGID);
        else if ("JMSCorrelationID".equals(name))
            return String.valueOf(SHOW_CORRID);
        else if ("JMSDeliveryMode".equals(name))
            return String.valueOf(SHOW_DMODE);
        else if ("JMSXDeliveryCount".equals(name))
            return String.valueOf(SHOW_DCOUNT);
        else if ("JMSExpiration".equals(name))
            return String.valueOf(SHOW_EXPIRY);
        else if ("JMSPriority".equals(name))
            return String.valueOf(SHOW_PRIORITY);
        else if ("JMSReplyTo".equals(name))
            return String.valueOf(SHOW_REPLY);
        else if ("JMSTimestamp".equals(name))
            return String.valueOf(SHOW_TSTAMP);
        else if ("JMSFamily".equals(name))
            return String.valueOf(SHOW_FAMILY);
        else
            return "0";
    }

    /**
     * returns the property value of the name from the message
     */
    public static String getProperty(String name, Message msg)
        throws JMSException {
        int id = 0;

        if (msg == null || name == null)
            return null;

        try {
            id = Integer.parseInt(name);
        }
        catch (NumberFormatException e) {
            if (msg.propertyExists(name))
                return msg.getStringProperty(name);
            else
                return null;
        }

        switch (id) {
          case SHOW_DATE:
            return Event.dateFormat(new Date(msg.getJMSTimestamp()));
          case SHOW_AGE:
            return String.valueOf(System.currentTimeMillis() -
                msg.getJMSTimestamp());
          case SHOW_DST:
            return ((msg.getJMSDestination() != null) ?
                msg.getJMSDestination().toString() : "");
          case SHOW_MSGID:
            return msg.getJMSMessageID();
          case SHOW_CORRID:
            return msg.getJMSCorrelationID();
          case SHOW_EXPIRY:
            return String.valueOf(msg.getJMSExpiration());
          case SHOW_PRIORITY:
            return String.valueOf(msg.getJMSPriority());
          case SHOW_DMODE:
            return String.valueOf(msg.getJMSDeliveryMode());
          case SHOW_DCOUNT:
            return (!msg.propertyExists("JMSXDeliveryCount")) ? "" :
                String.valueOf(msg.getIntProperty("JMSXDeliveryCount"));
          case SHOW_TSTAMP:
            return String.valueOf(msg.getJMSTimestamp());
          case SHOW_MSGTYPE:
            return msg.getJMSType();
          case SHOW_REPLY:
            return ((msg.getJMSReplyTo() != null) ?
                msg.getJMSReplyTo().toString() : "");
          case SHOW_FAMILY:
            if (msg instanceof TextMessage)
                return "jms_text";
            else if (msg instanceof BytesMessage)
                return "jms_bytes";
            else if (msg instanceof MapMessage)
                return "jms_map";
            else if (msg instanceof ObjectMessage)
                return "jms_object";
            else if (msg instanceof StreamMessage)
                return "jms_stream";
          default:
            break;
        }
        return null;
    }

    /**
     * sets JMSProperty or String property on the message.  It returns 0 on
     * success, -1 on illegal arguments or JMSException.
     */
    public static int setProperty(String name, String value, Message msg)
        throws JMSException {
        long l;
        char c;
        int id = 0;
        int status = -1;

        if (msg == null || name == null || name.length() <= 0 ||
            "body".equals(name))
            return status;

        c = name.charAt(0);
        if (c != '0' && c != '1' && c != '2' && c != '3' && c != '4' &&
            c != '5' && c != '6' && c != '7' && c != '8' && c != '9') {
            return setProperty(name, JMSEvent.TYPE_STRING, value, msg);
        }
        else try {
            id = Integer.parseInt(name);
        }
        catch (NumberFormatException e) { // not the id
            msg.setStringProperty(name, value);
            return 0;
        }

        status = 0;
        switch (id) {
          case SHOW_DATE:
            try {
                l = Long.parseLong(value);
            }
            catch (NumberFormatException e) {
                return -1;
            }
            msg.setJMSTimestamp(System.currentTimeMillis() - l);
            break;
          case SHOW_DST:
            status = -1;
            break;
          case SHOW_MSGID:
            msg.setJMSMessageID(value);
            break;
          case SHOW_CORRID:
            msg.setJMSCorrelationID(value);
            break;
          case SHOW_EXPIRY:
            if ("0".equals(value))
                msg.setJMSExpiration(0);
            else
                msg.setJMSExpiration(Long.parseLong(value));
            break;
          case SHOW_PRIORITY:
            msg.setJMSPriority(Integer.parseInt(value));
            break;
          case SHOW_DMODE:
            msg.setJMSDeliveryMode(Integer.parseInt(value));
            break;
          case SHOW_DCOUNT:
            status = -1;
            break;
          case SHOW_TSTAMP:
            msg.setJMSTimestamp(Long.parseLong(value));
            break;
          case SHOW_MSGTYPE:
            msg.setJMSType(value);
            break;
          case SHOW_REPLY:
            if (msg instanceof JMSEvent) {
                if (value == null || value.length() <= 0)
                    msg.setJMSReplyTo(null);
                else if (value.indexOf("queue://") == 0)
                    msg.setJMSReplyTo(new EventQueue(value));
                else if (value.indexOf("topic://") == 0)
                    msg.setJMSReplyTo(new EventTopic(value));
                else
                    msg.setJMSReplyTo(new EventQueue(value));
            }
            else if (value == null || value.length() <= 0)
                msg.setJMSReplyTo(null);
            else
                status = -1;
            break;
          default:
            status = -1;
            break;
        }
        return status;
    }

    /**
     * sets the user property of certain type on the message.  It returns 0 on
     * success, -1 on illegal arguments or JMSException.
     */
    public static int setProperty(String name, int type, String value,
        Message msg) throws JMSException {
        char c;
        Object o;
        int status = -1;

        if (msg == null || name == null || name.length() <= 0 ||
            "body".equals(name))
            return status;

        c = name.charAt(0);
        if (c == '1' || c == '2' || c == '3' || c == '4' || c == '5' ||
            c == '6' || c == '7' || c == '8' || c == '9' || c == '0') try {
            status = Integer.parseInt(name);
            return setProperty(name, value, msg);
        }
        catch (NumberFormatException e) { // not the id
        }

        if (msg.propertyExists(name)) {
            o = msg.getObjectProperty(name);
            if (o == null || o instanceof String)
                type = JMSEvent.TYPE_STRING;
            else if (o instanceof Integer)
                type = JMSEvent.TYPE_INTEGER;
            else if (o instanceof Long)
                type = JMSEvent.TYPE_LONG;
            else if (o instanceof Float)
                type = JMSEvent.TYPE_FLOAT;
            else if (o instanceof Double)
                type = JMSEvent.TYPE_DOUBLE;
            else if (o instanceof Short)
                type = JMSEvent.TYPE_SHORT;
            else if (o instanceof Byte)
                type = JMSEvent.TYPE_BYTE;
            else if (o instanceof Boolean)
                type = JMSEvent.TYPE_BOOLEAN;
            else
                type = JMSEvent.TYPE_STRING;
        }
        else if (type < JMSEvent.TYPE_STRING || type > JMSEvent.TYPE_DOUBLE)
            type = JMSEvent.TYPE_STRING;

        try {
            switch (type) {
              case JMSEvent.TYPE_STRING:
                o = value;
                break;
              case JMSEvent.TYPE_INTEGER:
                o = new Integer(value);
                break;
              case JMSEvent.TYPE_LONG:
                o = new Long(value);
                break;
              case JMSEvent.TYPE_FLOAT:
                o = new Float(value);
                break;
              case JMSEvent.TYPE_DOUBLE:
                o = new Double(value);
                break;
              case JMSEvent.TYPE_SHORT:
                o = new Short(value);
                break;
              case JMSEvent.TYPE_BYTE:
                o = new Byte(value);
                break;
              case JMSEvent.TYPE_BOOLEAN:
                o = new Boolean(value);
                break;
              default:
                o = null;
            }
        }
        catch (Exception e) {
            throw(new JMSException(e.toString()));
        }

        msg.setObjectProperty(name, o);

        return 0;
    }

    /**
     * sets the property of a MapMessage.  It returns 0 on
     * success, -1 on illegal arguments or JMSException.
     */
    public static int setMapProperty(String name, int type, String value,
        MapMessage msg) throws JMSException {
        Object o;
        int status = -1;

        if (msg == null || name == null || name.length() <= 0)
            return status;

        if (msg.itemExists(name)) {
            o = msg.getObject(name);
            if (o == null || o instanceof String)
                type = JMSEvent.TYPE_STRING;
            else if (o instanceof Integer)
                type = JMSEvent.TYPE_INTEGER;
            else if (o instanceof Long)
                type = JMSEvent.TYPE_LONG;
            else if (o instanceof Float)
                type = JMSEvent.TYPE_FLOAT;
            else if (o instanceof Double)
                type = JMSEvent.TYPE_DOUBLE;
            else if (o instanceof Short)
                type = JMSEvent.TYPE_SHORT;
            else if (o instanceof Byte)
                type = JMSEvent.TYPE_BYTE;
            else if (o instanceof Boolean)
                type = JMSEvent.TYPE_BOOLEAN;
            else if (o instanceof Character)
                type = JMSEvent.TYPE_BOOLEAN + JMSEvent.TYPE_DOUBLE;
            else if (o instanceof byte[])
                type = JMSEvent.TYPE_BYTE + JMSEvent.TYPE_DOUBLE;
            else
                type = JMSEvent.TYPE_STRING;
        }
        else if (type < JMSEvent.TYPE_STRING || type > JMSEvent.TYPE_DOUBLE
            + JMSEvent.TYPE_BYTE)
            type = JMSEvent.TYPE_STRING;

        try {
            switch (type) {
              case JMSEvent.TYPE_STRING:
                o = value;
                break;
              case JMSEvent.TYPE_INTEGER:
                o = new Integer(value);
                break;
              case JMSEvent.TYPE_LONG:
                o = new Long(value);
                break;
              case JMSEvent.TYPE_FLOAT:
                o = new Float(value);
                break;
              case JMSEvent.TYPE_DOUBLE:
                o = new Double(value);
                break;
              case JMSEvent.TYPE_SHORT:
                o = new Short(value);
                break;
              case JMSEvent.TYPE_BYTE:
                o = new Byte(value);
                break;
              case JMSEvent.TYPE_BOOLEAN:
                o = new Boolean(value);
                break;
              case JMSEvent.TYPE_BOOLEAN+JMSEvent.TYPE_DOUBLE:
                o = new Character(value.charAt(0));
                break;
              case JMSEvent.TYPE_BYTE+JMSEvent.TYPE_DOUBLE:
                o = value.getBytes();
                break;
              default:
                o = null;
            }
        }
        catch (Exception e) {
            throw(new JMSException(e.toString()));
        }

        msg.setObject(name, o);

        return 0;
    }

    /**
     * In case of BytesMessages, you may need to reset them before
     * using this method to process message body.  The previous read
     * will change the starting position of the byte stream.
     *<br/><br/>
     * You need to provide a byte array: buffer[bufferSize] for MT-Safe.
     */
    public static String processBody(Message inMessage, byte[] buffer)
        throws JMSException {
        int bytesRead = 0;
        int bufferSize = buffer.length;
        StringBuffer strBuf;
        String msgStr = "";
        if (inMessage instanceof TextMessage) {
            msgStr = ((TextMessage) inMessage).getText();
        }
        else if (inMessage instanceof BytesMessage) {
            strBuf = new StringBuffer();
            try {
                // reset the msg to read the body
                ((BytesMessage) inMessage).reset();
                while ((bytesRead =
                ((BytesMessage) inMessage).readBytes(buffer, bufferSize)) >= 0){
                    if (bytesRead == 0)
                        return msgStr;

                    strBuf.append(new String(buffer,0,bytesRead,"ISO-8859-1"));
                }
            }
            catch (NullPointerException e) { // hack for the bug in JMS
                if (strBuf.length() > 0)
                    msgStr = strBuf.toString();
                return msgStr;
            }
            catch (UnsupportedEncodingException e) {
                throw(new JMSException(e.toString()));
            }
            msgStr = strBuf.toString();
        }
        else if (inMessage instanceof MapMessage) { // assuming all strings
            Enumeration mapNames;
            Object o;
            mapNames = ((MapMessage) inMessage).getMapNames();
            strBuf = new StringBuffer();

            while (mapNames.hasMoreElements()) {
                String name = (String) mapNames.nextElement();
                if (name == null || name.equals("null"))
                    continue;
                strBuf.append("<" + name + ">");
                o = ((MapMessage) inMessage).getObject(name);
                if (!(o instanceof byte[])) // assuming it is String
                    strBuf.append(o.toString());
                else try {
                    strBuf.append(new String((byte[]) o, "ISO-8859-1"));
                }
                catch (UnsupportedEncodingException e) {
                    throw(new JMSException(e.toString()));
                }
                strBuf.append("</" + name + ">");
                if (strBuf.charAt(strBuf.length()-1) != '\n')
                    strBuf.append('\n');
            }
            msgStr = strBuf.toString();
        }
        else if (inMessage instanceof ObjectMessage) {
            msgStr = "";
        }
        else { // unknown message type
            return null;
        }

        if (msgStr == null) {
            msgStr = "";
        }

        return msgStr;
    }

    /**
     * returns a formatted string of the message with the given template
     */
    public static String format(Message msg, byte[] buffer,
        Template template, Perl5Matcher pm) throws JMSException {
        String key, value, field, text, body = "";
        int i, n;
        if (template == null)
            return processBody(msg, buffer);

        if (template.containsField("body"))
            body = processBody(msg, buffer);

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if ((key = getPropertyID(field)) != null)
                value = getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else if ("body".equals(field))
                value = body;
            else
                value = "";
            if (value == null)
                value = "";
            text = template.substitute(pm, field, value, text);
        }
        return text;
    }

    public static String format(Message msg, byte[] buffer,
        Template template) throws JMSException {
        return format(msg, buffer, template, (Perl5Matcher) null);
    }

    /**
     * returns a formatted string of the MapMessage with the given template
     * and an optional key for accessing to the property of Map.  The value
     * of the Map key will substitute the place holder of ##body##.
     */
    public static String format(MapMessage msg, String mapKey,
        Template template, Perl5Matcher pm) throws JMSException {
        String key, value, field, text, body = "";
        int i, n;
        if (template == null) {
            if (mapKey == null || mapKey.length() <= 0)
                return "";
            else
                return (String) msg.getString(mapKey);
        }

        if (template.containsField("body")) {
            if (mapKey != null && mapKey.length() > 0)
                body = msg.getString(mapKey);
        }

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if ((key = getPropertyID(field)) != null)
                value = getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else if ("body".equals(field))
                value = body;
            else
                value = "";
            if (value == null)
                value = "";
            text = template.substitute(pm, field, value, text);
        }
        return text;
    }

    /**
     * returns a formatted string of the message with the given template
     * and a set of substitutions
     */
    public static String format(Message msg, byte[] buffer,
        Template template, Map subMap) throws JMSException {
        String key, value, field, text, body = "";
        TextSubstitution[] sub;
        Object o;
        int i, n;
        if (subMap == null || subMap.size() <= 0)
            return format(msg, buffer, template);

        if (template == null)
            return processBody(msg, buffer);

        if (template.containsField("body"))
            body = processBody(msg, buffer);

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if ((key = getPropertyID(field)) != null)
                value = getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else if ("body".equals(field))
                value = body;
            else
                value = "";
            if (value == null)
                value = "";
            if ((o = subMap.get(field)) != null &&
                o instanceof TextSubstitution[]) {
                sub = (TextSubstitution[]) o;
                for (int j=0; j<sub.length; j++) {
                    if (sub[j] == null)
                        continue;
                    value = sub[j].substitute(value);
                }
            }
            text = template.substitute(field, value, text);
        }
        return text;
    }

    /**
     * It returns a formatted string of the message with the given template
     * and a repeated template.  It is used to format JMSEvents into events
     * in either postable format or collectible format.  The postable format
     * of an event is a uri query string ready to be posted to a web server.
     * The collectible format of an event is a log string ready to be parsed
     * into an event by EventParser.  But it does not contain the trailing 
     * newline chararcter and the leading timestamp as well as the IP address.
     */
    public static String format(Message msg, byte[] buffer, Template template,
        Template repeat, Map exclude, Perl5Matcher pm) throws JMSException {
        String key, value, field, text, body = "";
        int i, n;
        if (template == null && repeat == null)
            return processBody(msg, buffer);

        if (template.containsField("body"))
            body = processBody(msg, buffer);

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if ((key = getPropertyID(field)) != null)
                value = getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else if ("body".equals(field))
                value = body;
            else
                value = "";
            if (value == null)
                value = "";
            // escaping special chars
            if ("text".equals(field) || "body".equals(field)) {
                if (value.indexOf('&') >= 0)
                    value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND,
                        value);
                if (value.indexOf("\"") >= 0)
                    value = Utils.doSearchReplace("\"", Event.ESCAPED_QUOTE,
                        value);
                if (value.indexOf('\r') >= 0)
                    value = Utils.doSearchReplace("\r",
                        Event.ESCAPED_CARRIAGE_RETURN, value);
                if (value.indexOf('\n') >= 0)
                    value = Utils.doSearchReplace("\n", "\r", value);
            }
            text = template.substitute(pm, field, value, text);
        }

        if (repeat != null) { // extra
            StringBuffer strBuf = new StringBuffer();
            Enumeration enu = msg.getPropertyNames();
            boolean hasKey, hasValue;

            hasKey =  (repeat.containsField("key")) ? true : false;
            hasValue =  (repeat.containsField("value")) ? true : false;

            if (exclude == null)
                exclude = new HashMap();

            while (enu.hasMoreElements()) {
                key = (String) enu.nextElement();
//                if (key.startsWith("JMS"))
//                    continue;
                if (exclude.containsKey(key))
                    continue;
                field = repeat.copyText();
                if (hasKey)
                    field = repeat.substitute(pm, "key", key, field);
                if (hasValue) {
                    value = msg.getStringProperty(key);
                    if (value == null)
                        value = "";
                    else
                        value = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                            Event.ESCAPED_ITEM_SEPARATOR, value);
                    field = repeat.substitute(pm, "value", value, field);
                }
                strBuf.append(field);
            }
            value = strBuf.toString();
            if (value.indexOf('&') >= 0)
                value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND,
                    value);
            if (value.indexOf('=') >= 0)
                value = Utils.doSearchReplace("=", Event.ESCAPED_EQUAL, value);
            if (value.indexOf('\r') >= 0)
                value = Utils.doSearchReplace("\r",
                    Event.ESCAPED_CARRIAGE_RETURN, value);
            if (value.indexOf('\n') >= 0)
                value = Utils.doSearchReplace("\n", "\r", value);

            return text + value;
        }

        return text;
    }

    /**
     * returns a formatted string of the content of the message according to
     * the given set of templates and substitutions.  The prefix is used for
     * the exception message if there is an error.
     */
    public static String format(String prefix, Template[] template,
        TextSubstitution[] substitution, byte[] buffer, Message inMessage,
        Perl5Matcher pm) {
        int i, k, n;
        String text = null;
        StringBuffer strBuf ;
        if (template == null || (n = template.length) <= 0)
            return null;

        strBuf = new StringBuffer();
        if (substitution != null)
            k = substitution.length;
        else
            k = 0;
        for (i=0; i<n; i++) {
            if (template[i] != null) try { // format
                if (i > 0)
                    strBuf.append(text);
                text = format(inMessage, buffer, template[i], pm);
            }
            catch (JMSException e) {
                String str = prefix + i;
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += " Linked exception: " + ex.toString() + "\n";
                new Event(Event.ERR, str + " failed to format " + text +
                    ": " + Event.traceStack(e)).send();
                return null;
            }
            catch (Exception e) {
                new Event(Event.ERR, prefix + i + " failed to format " +
                    text + ": " + Event.traceStack(e)).send();
                return null;
            }
            if (text == null)
                text = "";

            if (i >= k) // no sub defined beyond k
                continue;
            if (substitution[i] != null) try { // substitute
                text = substitution[i].substitute(pm, text);
            }
            catch (Exception e) {
                new Event(Event.ERR, prefix + i + " failed to format " +
                    text + ": " + Event.traceStack(e)).send();
                return null;
            }
        }
        strBuf.append(text);

        return strBuf.toString();
    }

    /**
     * returns a string on the message for list purpose
     */
    public static String list(Message msg, int type) {
        StringBuffer strBuf;
        String id, cid, line, mtype, reply, str, text;
        int priority;
        long tm, expiry;

        if (msg == null)
            return null;
        try {
            Destination dest;
            tm = msg.getJMSTimestamp();
            id = msg.getJMSMessageID();
            cid = msg.getJMSCorrelationID();
            mtype = msg.getJMSType();
            expiry = msg.getJMSExpiration();
            priority = msg.getJMSPriority();
            dest = msg.getJMSReplyTo();
            if (dest != null)
                reply = (dest instanceof Queue) ? ((Queue) dest).getQueueName():
                    ((Topic) dest).getTopicName();
            else
                reply = "";
        }
        catch (Exception e) {
            return null;
        }
        strBuf = new StringBuffer();
        if (msg instanceof TextMessage)
            str = "text";
        else if (msg instanceof BytesMessage)
            str = "binary";
        else if (msg instanceof MapMessage)
            str = "map";
        else if (msg instanceof ObjectMessage)
            str = "object";
        else
            str = "stream";
        if (id == null)
            id = "";
        if (cid == null)
            cid = "";
        if (mtype == null)
            mtype = "";

        line = Event.dateFormat(new Date(tm));
        if (type == Utils.RESULT_XML) {
            strBuf.append("<Time>" + Utils.escapeXML(line) + "</Time>");
            strBuf.append("<MID>" + Utils.escapeXML(id) + "</MID>");
            strBuf.append("<CID>" + Utils.escapeXML(cid) + "</CID>");
            strBuf.append("<Priority>" + priority + "</Priority>");
            strBuf.append("<Expiry>" + expiry + "</Expiry>");
            strBuf.append("<MType>" + Utils.escapeXML(mtype) + "</MType>");
            strBuf.append("<ReplyTo>" + Utils.escapeXML(reply) + "</ReplyTo>");
            strBuf.append("<Class>" + Utils.escapeXML(str) + "</Class>");
        }
        else if (type == Utils.RESULT_JSON) {
            strBuf.append("\"Time\":\"" + Utils.escapeJSON(line) + "\"");
            strBuf.append(", \"MID\":\"" + Utils.escapeJSON(id) + "\"");
            strBuf.append(", \"CID\":\"" + Utils.escapeJSON(cid) + "\"");
            strBuf.append(", \"Priority\":" + priority);
            strBuf.append(", \"Expiry\":" + expiry);
            strBuf.append(", \"MType\":\"" + Utils.escapeJSON(mtype) + "\"");
            strBuf.append(", \"ReplyTo\":\"" + Utils.escapeJSON(reply) + "\"");
            strBuf.append(", \"Class\":\"" + Utils.escapeJSON(str) + "\"");
        }
        else { // for text
            strBuf.append(line);
            strBuf.append(" " + id);
            strBuf.append(" " + cid);
            strBuf.append(" " + priority);
            strBuf.append(" " + expiry);
            strBuf.append(" " + mtype);
            strBuf.append(" " + reply);
            strBuf.append(" " + str);
        }

        return strBuf.toString();
    }

    /**
     * returns a string for detail of the message
     */
    public static String show(Message msg, int type) {
        StringBuffer strBuf;
        byte[] buff = new byte[4096];
        String key, value, str, msgStr;

        if (msg == null)
            return null;

        strBuf = new StringBuffer(list(msg, type));
        try {
            Enumeration propNames = msg.getPropertyNames();
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key.startsWith("JMS"))
                    continue;
                value = msg.getStringProperty(key);
                if (value == null)
                    continue;
                if (type == Utils.RESULT_XML)
                    strBuf.append("<" + key + ">" + value + "</" + key + ">");
                else if (type == Utils.RESULT_JSON)
                    strBuf.append(", \"" + key + "\": \"" + value + "\"");
                else
                    strBuf.append(" ~ " + key + " ~ " + value);
            }
            msgStr = processBody(msg, buff);
        }
        catch (Exception e) {
            return null;
        }

        if (msgStr == null)
            msgStr = "";

        if (type == Utils.RESULT_XML) // for XML
            strBuf.append("<body>"+Utils.escapeXML(msgStr)+"</body>");
        else if (type == Utils.RESULT_JSON) // for JSON
            strBuf.append(", \"body\":\"" + Utils.escapeJSON(msgStr) + "\"");
        else // for text
            strBuf.append("\nbody:\n" + msgStr);

        return strBuf.toString();
    }

    /**
     * returns a string for display purpose
     */
    public static String display(Message inMessage, String msgStr,
        int displayMask, String[] propertyName) throws JMSException {

        String value;
        StringBuffer strBuf = new StringBuffer();
        // header part for display
        if (displayMask > 1) {
            if ((displayMask & SHOW_DATE) != 0) {
                strBuf.append(" Date: " +
                    Event.dateFormat(new Date(inMessage.getJMSTimestamp())));
            }
            if ((displayMask & SHOW_AGE) != 0) {
                strBuf.append(" AGE: " +
                    (System.currentTimeMillis() - inMessage.getJMSTimestamp()));
            }
            if ((displayMask & SHOW_SIZE) != 0) {
                if (inMessage instanceof TextMessage) {
                    value = ((TextMessage) inMessage).getText();
                    strBuf.append(" SIZE: "+((value != null)?value.length():0));
                }
                else if (inMessage instanceof BytesMessage) {
                    ((BytesMessage) inMessage).reset();
                    strBuf.append(" SIZE: " +
                        ((BytesMessage) inMessage).getBodyLength());
                }
                else if (msgStr != null)
                    strBuf.append(" SIZE: " + ((msgStr != null) ?
                        msgStr.length() : 0));
            }
            if ((displayMask & SHOW_DST) != 0) {
                if (inMessage.getJMSDestination() != null)
                    strBuf.append(" DST: " +
                        inMessage.getJMSDestination().toString());
            }
            if ((displayMask & SHOW_MSGID) != 0) {
                value = inMessage.getJMSMessageID();
                if (value != null)
                    strBuf.append(" MsgID: " + value);
            }
            if ((displayMask & SHOW_CORRID) != 0) {
                value = inMessage.getJMSCorrelationID();
                if (value != null)
                    strBuf.append(" CorrID: " + value);
            }
            if ((displayMask & SHOW_EXPIRY) != 0) {
                strBuf.append(" Expiry: " + inMessage.getJMSExpiration());
            }
            if ((displayMask & SHOW_PRIORITY) != 0) {
                strBuf.append(" Priority: " + inMessage.getJMSPriority());
            }
            if ((displayMask & SHOW_DMODE) != 0) {
                strBuf.append(" DMODE: " + inMessage.getJMSDeliveryMode());
            }
            if ((displayMask & SHOW_DCOUNT) != 0) {
                if (inMessage.propertyExists("JMSXDeliveryCount"))
                    strBuf.append(" DCount: " +
                        inMessage.getIntProperty("JMSXDeliveryCount"));
                else if (inMessage.getJMSRedelivered())
                    strBuf.append(" DCount: +1");
                else
                    strBuf.append(" DCount: 0");
            }
            if ((displayMask & SHOW_MSGTYPE) != 0) {
                value = inMessage.getJMSType();
                if (value != null)
                    strBuf.append(" MSGType: " + value);
            }
            if ((displayMask & SHOW_REPLY) != 0) {
                Destination dest = inMessage.getJMSReplyTo();
                if (dest != null) {
                    if (dest instanceof Queue)
                        strBuf.append(" ReplyQ: "+((Queue)dest).getQueueName());
                    else
                        strBuf.append(" ReplyT: "+((Topic)dest).getTopicName());
                }
            }
            if ((displayMask & SHOW_TSTAMP) != 0 &&
                (displayMask & SHOW_DATE) == 0) {
                    strBuf.append(" TStamp: " + inMessage.getJMSTimestamp());
            }
            if ((displayMask & SHOW_FAMILY) != 0) {
                value = getProperty(String.valueOf(SHOW_FAMILY), inMessage);
                if (value != null)
                    strBuf.append(" Family: " + value);
            }
            if ((displayMask & SHOW_PROPS) != 0) {
                Enumeration propNames = inMessage.getPropertyNames();
                while (propNames.hasMoreElements()) {
                    String name;
                    name = (String) propNames.nextElement();
                    if (name.startsWith("JMS"))
                        continue;
                    value = inMessage.getStringProperty(name);
                    if (value != null)
                        strBuf.append("\n  " + name + ": " + value);
                }
            }
            else if (propertyName != null) {
                for (int i=0; i<propertyName.length; i++) {
                    if (inMessage.propertyExists(propertyName[i])) {
                        value = inMessage.getStringProperty(propertyName[i]);
                        if (value != null) {
                            if (strBuf.length() > 0)
                                strBuf.append(" ~ ");
                            strBuf.append(propertyName[i] + ": " + value);
                        }
                    }
                    else if (inMessage instanceof JMSEvent &&
                        "priority".equals(propertyName[i])) { // for event
                        strBuf.append(" ~ priority: " +
                            Event.priorityNames[9-inMessage.getJMSPriority()]);
                    }
                }
            }
        }

        // body part for display
        if ((displayMask & SHOW_BODY) != 0 && displayMask > 0 &&
            msgStr != null) {
            int len = msgStr.length();
            if (inMessage instanceof TextMessage)
                strBuf.append("\ngot text(" + len +" bytes):");
            else if (inMessage instanceof BytesMessage)
                strBuf.append("\ngot body(" + len +" bytes):");
            else if (inMessage instanceof MapMessage)
                strBuf.append("\ngot map(" + len +" bytes):");
            else
                strBuf.append("\ngot body(" + len +" bytes):");
            if (msgStr.length() > 0) {
                strBuf.append("\n" + msgStr);
            }
        }
        return strBuf.toString();
    }

    /**
     * duplicates a message and returns a JMSEvent in the same type.
     *<br/><br/>
     * You need to provide a byte array: buffer[bufferSize] for MT-Safe.
     */
    public static Message duplicate(Message msg, byte[] buffer)
        throws JMSException {
        Message message = null;
        Object o;
        String key, value;
        StringBuffer strBuf = new StringBuffer();
        int bytesRead = 0;
        int bufferSize;
        String msgStr = "";

        if (msg == null)
            return msg;

        if (buffer == null || buffer.length <= 0)
            buffer = new byte[4096];
        bufferSize = buffer.length;
        if (msg instanceof TextMessage) {
            message = new org.qbroker.jms.TextEvent();
            msgStr = ((TextMessage) msg).getText();
            ((TextMessage) message).setText(msgStr);
        }
        else if (msg instanceof BytesMessage) {
            message = new org.qbroker.jms.BytesEvent();
            try {
                while ((bytesRead =
                    ((BytesMessage) msg).readBytes(buffer, bufferSize)) >= 0) {

                    if (bytesRead == 0)
                        break;
                    ((BytesMessage) message).writeBytes(buffer, 0, bytesRead);
                }
            }
            catch (NullPointerException e) { // hack for the bug in JMS
            }
        }
        else if (msg instanceof MapMessage) { // assuming all strings
            Enumeration mapNames;
            mapNames = ((MapMessage) msg).getMapNames();
            message = new org.qbroker.jms.MapEvent();

            while (mapNames.hasMoreElements()) {
                key = (String) mapNames.nextElement();
                if (key == null || key.equals("null"))
                    continue;
                o = ((MapMessage) msg).getObject(key);
                if (o instanceof String)
                    ((MapMessage) message).setString(key, (String) o);
                else if (o instanceof byte[])
                    ((MapMessage) message).setBytes(key, (byte[]) o);
                else
                    ((MapMessage) message).setObject(key, o);
            }
        }
        else if (msg instanceof ObjectMessage) {
            message = new org.qbroker.jms.ObjectEvent();
           ((ObjectMessage)message).setObject(((ObjectMessage)msg).getObject());
        }

        // usr folder
        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS"))
                continue;
            message.setObjectProperty(key, msg.getObjectProperty(key));
        }
        message.setJMSPriority(msg.getJMSPriority());
        message.setJMSExpiration(msg.getJMSExpiration());
        message.setJMSDeliveryMode(msg.getJMSDeliveryMode());
        message.setJMSTimestamp(msg.getJMSTimestamp());
        message.setJMSMessageID(msg.getJMSMessageID());
        message.setJMSCorrelationID(msg.getJMSCorrelationID());
        message.setJMSType(msg.getJMSType());
        message.setJMSReplyTo(msg.getJMSReplyTo());

        return message;
    }

    /**
     * converts a message into a JMSEvent in the specific type and returns
     * the new message where type 0 is for BytesMessage, 1 for TextMessage,
     * 2 for MapMessage and 3 for ObjectMessage.  In case of failure, it
     * may just return null without exception.
     *<br/><br/>
     * You need to provide a byte array: buffer[bufferSize] for MT-Safe.
     */
    public static Message convert(Message msg, int type, byte[] buffer)
        throws JMSException {
        Message message = null;
        String key, value;
        int bufferSize;
        String msgStr = "";

        if (msg == null)
            return msg;

        if (buffer == null || buffer.length <= 0)
            buffer = new byte[4096];
        bufferSize = buffer.length;
        switch (type) {
          case 1: // for TextMessage
            if (msg instanceof TextMessage)
                message = duplicate(msg, buffer);
            else { // convert to TextMessage
                message = new org.qbroker.jms.TextEvent();
                msgStr = processBody(msg, buffer);
                ((TextMessage) message).setText(msgStr);
            }
            break;
          case 0: // for BytesMessage
            if (msg instanceof BytesMessage)
                message = duplicate(msg, buffer);
            else { // convert to BytesMessage
                message = new org.qbroker.jms.BytesEvent();
                msgStr = processBody(msg, buffer);
                ((BytesMessage) message).writeBytes(msgStr.getBytes());
            }
            break;
          case 2: // // for MapMessage
            if (msg instanceof MapMessage)
                message = duplicate(msg, buffer);
            else if (msg instanceof ObjectMessage) { // convert to MapMessage
                message = new org.qbroker.jms.MapEvent();
                Object o = ((ObjectMessage) msg).getObject();
                if (o != null && o instanceof Map) {
                    Map map = (Map) o;
                    Iterator iter = map.keySet().iterator();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        if (key == null || key.equals("null"))
                            continue;
                        ((MapMessage) message).setObject(key, map.get(key));
                    }
                }
            }
            else { // convert to MapMessage
                Map map = null;
                message = new org.qbroker.jms.MapEvent();
                msgStr = processBody(msg, buffer);
                if (msgStr != null && msgStr.length() > 0) try { // json only
                    StringReader in = new StringReader(msgStr);
                    map = (Map) JSON2Map.parse(in);
                    in.close();
                }
                catch (Exception e) {
                    map = null;
                }
                if (map != null) { // xml content
                    Iterator iter = map.keySet().iterator();
                    while (iter.hasNext()) {
                        key = (String) iter.next();
                        if (key == null || key.equals("null"))
                            continue;
                        ((MapMessage) message).setObject(key, map.get(key));
                    }
                }
                else // plain text
                    ((MapMessage) message).setObject("text", msgStr);
            }
            break;
          case 3: // for ObjectMessage
            if (msg instanceof ObjectMessage)
                message = duplicate(msg, buffer);
            else if (msg instanceof MapMessage) { // convert to MapMessage
                Enumeration mapNames;
                HashMap map = new HashMap();
                mapNames = ((MapMessage) msg).getMapNames();

                while (mapNames.hasMoreElements()) {
                    key = (String) mapNames.nextElement();
                    if (key == null || key.equals("null"))
                        continue;
                    map.put(key, ((MapMessage) msg).getObject(key));
                }
                ((ObjectMessage) message).setObject(map);
            }
            else { // convert to MapMessage
                message = new org.qbroker.jms.ObjectEvent();
                msgStr = processBody(msg, buffer);
                ((ObjectMessage) message).setObject(msgStr);
            }
            break;
          default:
            return null; 
        }

        // usr folder
        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS"))
                continue;
            message.setObjectProperty(key, msg.getObjectProperty(key));
        }
        message.setJMSPriority(msg.getJMSPriority());
        message.setJMSExpiration(msg.getJMSExpiration());
        message.setJMSDeliveryMode(msg.getJMSDeliveryMode());
        message.setJMSTimestamp(msg.getJMSTimestamp());
        message.setJMSMessageID(msg.getJMSMessageID());
        message.setJMSCorrelationID(msg.getJMSCorrelationID());
        message.setJMSType(msg.getJMSType());
        message.setJMSReplyTo(msg.getJMSReplyTo());

        return message;
    }

    /** converts a set of attributes in the map into a JMS TextEvent */
    public static Message convert(Map map, String text) {
        TextEvent event = new TextEvent(text);
        Object o;
        String key;

        if (map == null)
            map = new HashMap();

        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            o = map.get(key);
            if (o == null)
                continue;
            if (o instanceof String)
                event.setAttribute(key, ((String) o).trim());
            else if (o instanceof Integer)
                event.setAttribute(key, ((Integer) o).toString());
            else if (o instanceof Long)
                event.setAttribute(key, ((Long) o).toString());
            else if (o instanceof Boolean)
                event.setAttribute(key, ((Boolean) o).toString());
            else if (o instanceof byte[])
                event.setAttribute(key, new String((byte[]) o).trim());
            else if (o instanceof String[]) {
                StringBuffer strBuf = new StringBuffer();
                String[] keys = (String[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].trim());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Integer[]) {
                StringBuffer strBuf = new StringBuffer();
                Integer[] keys = (Integer[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Long[]) {
                StringBuffer strBuf = new StringBuffer();
                Long[] keys = (Long[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Double)
                event.setAttribute(key, ((Double) o).toString());
            else if (o instanceof Double[]) {
                StringBuffer strBuf = new StringBuffer();
                Double[] keys = (Double[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Float)
                event.setAttribute(key, ((Float) o).toString());
            else if (o instanceof Float[]) {
                StringBuffer strBuf = new StringBuffer();
                Float[] keys = (Float[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else if (o instanceof Object[]) {
                StringBuffer strBuf = new StringBuffer();
                Object[] keys = (Object[]) o;
                for (int i=0; i<keys.length; i++) {
                    if (i > 0)
                        strBuf.append(",");
                    if (keys[i] != null)
                        strBuf.append(keys[i].toString());
                }
                event.setAttribute(key, strBuf.toString());
            }
            else
                event.setAttribute(key, o.toString());
        }
        return event;
    }

    /**
     * copies a message into another message and returns 1 with changes or
     * 0 without changes.
     */
    public static int copy(Message msg, Message message, byte[] buffer)
        throws JMSException {
        String key, value;
        int type, bufferSize;
        String msgStr = "";

        if (msg == null) // source is null
            return 0;

        if (message == null) // target is null
            return -1;

        if (buffer == null || buffer.length <= 0)
            buffer = new byte[4096];
        bufferSize = buffer.length;
        if (message instanceof TextMessage) { // for TextMessage
            if (msg instanceof TextMessage)
                msgStr = ((TextMessage) msg).getText();
            else
                msgStr = processBody(msg, buffer);
            ((TextMessage) message).setText(msgStr);
        }
        else if (message instanceof BytesMessage) { // for BytesMessage
            if (msg instanceof TextMessage)
                msgStr = ((TextMessage) msg).getText();
            else
                msgStr = processBody(msg, buffer);
            ((BytesMessage) message).writeBytes(msgStr.getBytes());
        }
        else {
            return 0; 
        }

        // usr folder
        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS"))
                continue;
            message.setObjectProperty(key, msg.getObjectProperty(key));
        }
        message.setJMSPriority(msg.getJMSPriority());
        message.setJMSExpiration(msg.getJMSExpiration());
        message.setJMSDeliveryMode(msg.getJMSDeliveryMode());
        message.setJMSTimestamp(msg.getJMSTimestamp());
        message.setJMSMessageID(msg.getJMSMessageID());
        message.setJMSCorrelationID(msg.getJMSCorrelationID());
        message.setJMSType(msg.getJMSType());
        message.setJMSReplyTo(msg.getJMSReplyTo());

        return 1;
    }

    /**
     * resets JMSProperty on the message so its JMSProperties are writeable.
     * All existing JMSProperties are copied over except for those of JMSX
     */
    public static void resetProperties(Message msg) throws JMSException {
        String key;
        Map h = new HashMap();
        Iterator iter;

        if (msg == null)
            return;

        Enumeration propNames = msg.getPropertyNames();
        while (propNames.hasMoreElements()) {
            key = (String) propNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            if (key.startsWith("JMS"))
                continue;
            h.put(key, msg.getObjectProperty(key));
        }
        msg.clearProperties();
        iter = h.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            msg.setObjectProperty(key, h.get(key));
        }
        if (h.size() > 0)
            h.clear();
    }

    /**
     * resets the body Map on a MapMessage so its body is writeable.  All
     * existing key-value pairs are copied over.
     */
    public static void resetMapBody(MapMessage msg) throws JMSException {
        String key;
        Map h = new HashMap();
        Iterator iter;

        if (msg == null)
            return;

        Enumeration mapNames;
        mapNames = msg.getMapNames();

        while (mapNames.hasMoreElements()) {
            key = (String) mapNames.nextElement();
            if (key == null || key.equals("null"))
                continue;
            h.put(key, msg.getObject(key));
        }

        msg.clearBody();
        iter = h.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            msg.setObject(key, h.get(key));
        }
        if (h.size() > 0)
            h.clear();
    }

    /**
     * returns a byte array for the number of the hex string 
     */
    public static byte[] hexString2Bytes(String hexStr) {
        int n;
        byte[] b;
        if (hexStr == null || hexStr.length() < 3)
            return null;
        n = hexStr.length() - 2;
        n /= 2;
        b = new byte[n];
        for (int i=0; i<n; i++)
            b[i] = (byte) Integer.parseInt(hexStr.substring(i+i+2, i+i+4), 16);
        return b;
    }

    /**
     * returns a byte array for the number
     */
    public static byte[] long2Bytes(long l) {
        int n=0, i;
        int[] num = new int[8];
        while (l >= 256) {
            num[n++] = (int) (l % 256);
            l /= 256;
        }
        num[n++] = (int) l;

        byte[] a = new byte[n];
        for (i=0; i<n; i++) {
            a[n-1-i] = (byte) num[i];
        }
        return a;
    }

    /**
     * It parses the text and fills the result into the message.  Upon success,
     * it returns null, or a throwable object otherwise.  The Object of parser
     * is supposed to have a public method of parse() that takes a String as
     * the only input argument.  On success, the method of parse() will return
     * either an Event or a Map.  For anything else, the method fails.
     */
    public static Throwable parse(java.lang.reflect.Method parse, Object parser,
        String msgStr, Message message) {
        Object o;
        Iterator iter;
        String key, value;
        boolean isText;
        int ic;
        if (parse == null || msgStr == null || message == null)
            return new IllegalArgumentException("null argument");
        isText = (message instanceof TextMessage);
        try {
            o = parse.invoke(parser, new Object[] {msgStr});
            if (o == null)
                return new IllegalArgumentException("failed to parse string");
            else if (o instanceof Event) {
                Event event = (Event) o;
                message.setJMSTimestamp(event.getTimestamp());
                message.setJMSPriority(9-event.getPriority());
                iter = event.getAttributeNames();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    value = event.getAttribute(key);
                    if (value == null)
                        continue;
                    if ("text".equals(key)) {
                        message.clearBody();
                        if (isText)
                            ((TextMessage) message).setText(value);
                        else {
                            ((BytesMessage) message).writeBytes(
                                value.getBytes());
                        }
                    }
                    else {
                        ic = setProperty(key, value, message);
                        if (ic != 0)
                            return new JMSException("failed to set property of "
                                + key);
                    }
                }
                event.clearAttributes();
            }
            else if (o instanceof Map) {
                Map h = (Map) o;
                iter = h.keySet().iterator();
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    o = h.get(key);
                    if (o == null || !(o instanceof String))
                        continue;
                    value = (String) o;
                    if ("body".equals(key)) {
                        message.clearBody();
                        if (isText)
                            ((TextMessage) message).setText(value);
                        else {
                            ((BytesMessage) message).writeBytes(
                                value.getBytes());
                        }
                    }
                    else {
                        ic = setProperty(key, value, message);
                        if (ic != 0)
                            return new JMSException("failed to set property of "
                                + key);
                    }
                }
                h.clear();
            }
            else {
                return new RuntimeException("parsing result not supported");
            }
        }
        catch (Exception e) {
            return e;
        }
        catch (Error e) {
            return e;
        }
        return null;
    }

    /**
     * It gets all the messages from the XQueue and packs them into a
     * StringBuffer according to the ResultType.  The maxMsg determines  
     * the maximum number of messages got from the XQueue.  It returns
     * the number of messages packed upon success.  Otherwise, it returns
     * -1 if any failures.
     */
    public static int getResult(int type, int maxMsg, Template temp,
        XQueue xq, byte[] buffer, StringBuffer strBuf)
        throws JMSException {
        Message msg;
        int sid, n;
        String text;
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        if (xq == null || strBuf == null)
            return -1;

        n = 0;
        while ((xq.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            if ((sid = xq.getNextCell(500)) < 0)
                break;

            msg = (Message) xq.browse(sid);
            if (msg == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            text = format(msg, buffer, temp);
            if ((type & Utils.RESULT_XML) > 0) {
                strBuf.append("<Message type=\"ARRAY\">");
                strBuf.append(Utils.escapeXML(text));
                strBuf.append("</Message>\n");
            }
            else {
                strBuf.append(Utils.escapeXML(text));
            }
            if (acked) try {
                msg.acknowledge();
            }
            catch (Exception ex) {
            }
            catch (Error ex) {
                xq.remove(sid);
                Event.flush(ex);
            }
            xq.remove(sid);
            n ++;
            if (n >= maxMsg)
                break;
        }

        return n;
    }

    /**
     * returns the number of messages packed into inMessage's body on success,
     * or -1 if failed.  The prefix is used for the exception message if
     * there is an error.
     */
    public static int pack(int maxMsg, String prefix, Template template,
        XQueue xq, byte[] buffer, Message inMessage) {
        int i, n = 0;
        if (xq == null || inMessage == null) {
            new Event(Event.ERR, prefix + " xq is null for packing").send();
            return -1;
        }
        try {
            if (xq.depth() > 0) { // queue is not empty
                StringBuffer strBuf = new StringBuffer();
                n = getResult(Utils.RESULT_TEXT, maxMsg, template, xq,
                    buffer, strBuf);
                if (n > 0) {
                    inMessage.clearBody();
                    if (inMessage instanceof TextMessage)
                        ((TextMessage) inMessage).setText(strBuf.toString());
                    else {
            ((BytesMessage) inMessage).writeBytes(strBuf.toString().getBytes());
                    }
                }
            }
        }
        catch (JMSException e) {
            String str = prefix;
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += " Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + " failed to pack msgs " +
                ": " + Event.traceStack(e)).send();
            return -1;
        }
        return n;
    }

    /** It parses the MSQL query and returns the message selector or null */
    public static String getSelector(String query) {
        int i, j, n;
        char c;
        String str;
        if (query == null || query.length() <= 7)
            return null;
        i = 0;
        n = query.length();
        while ((c = query.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = query.toUpperCase();
        j = str.indexOf(" WHERE ");
        if (j > i) {
            str = query.substring(j+7);
            return str.trim();
        }
        return null;
    }

    /** It parses the MSQL query and returns the queue name or null */
    public static String getQueue(String query) {
        int i, j, n;
        char c;
        String str;
        if (query == null || query.length() <= 7)
            return null;
        i = 0;
        n = query.length();
        while ((c = query.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return null;
        str = query.substring(i, i+7).toUpperCase();
        if ("SELECT ".equals(str)) { // for select
            i += 7;
            str = query.substring(i).toUpperCase();
            j = i + str.indexOf(" FROM ");
            if (j > i) {
                str = query.substring(j+6);
                str = str.trim();
                i = str.indexOf(' ');
                if (i > 0)
                    return str.substring(0, i);
                else
                    return str;
            }
        }
        return null;
    }

    /**
     * It parses the MSQL query and returns a sorted list of the column names
     * in the query.  In case of null or empty query, it returns null.
     */
    public static String[] getColumns(String query) {
        int i, j, n;
        char c;
        String str;
        if (query == null || query.length() <= 7)
            return null;
        i = 0;
        n = query.length();
        while ((c = query.charAt(i)) == ' ' || c == '\r'
            || c == '\n' || c == '\t' || c == '\f') { // trim at the beginning
            i ++;
            if (i >= n)
                break;
        }
        if (n - i <= 7)
            return new String[0];
        str = query.substring(i, i+7).toUpperCase();
        if ("SELECT ".equals(str)) { // for select
            i += 7;
            str = query.substring(i).toUpperCase();
            j = i + str.indexOf(" FROM ");
            if (j > i) {
                int k;
                String[] list;
                str = query.substring(i, j);
                n = str.length();
                i = 0;
                k = 0;
                while (i < n) { // count commas
                    j = str.indexOf(',', i);
                    k ++;
                    if (j < i)
                        break;
                    i = j + 1;
                }
                list = new String[k];
                i = 0;
                k = 0;
                while (i < n) {
                    j = str.indexOf(',', i);
                    if (j > i) {
                        list[k++] = str.substring(i, j);
                        i = j + 1;
                    }
                    else {
                        list[k++] = str.substring(i);
                        break;
                    }
                }
                for (i=k-1; i>=0; i--) { // trim
                    str = list[i].trim();
                    j = str.indexOf(' ');
                    if (j > 0)
                        list[i] = str.substring(0, j).trim();
                    else
                        list[i] = str;
                }
                Arrays.sort(list);
                return list;
            }
        }
        return new String[0];
    }

    /**
     * sets STOP bit on the global mask of the XQueue.
     */
    public static void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    /**
     * sets RUNNING bit on the global mask of the XQueue.
     */
    public static void resumeRunning(XQueue xq) {
        int mask = XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.KEEP_RUNNING);
    }

    /**
     * sets PAUSE bit on the global mask of the XQueue.
     */
    public static void pause(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.STANDBY;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.PAUSE);
    }

    /**
     * sets STANDBY bit on the global mask of the XQueue.
     */
    public static void standby(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE;
        xq.setGlobalMask((xq.getGlobalMask() & (~mask)) | XQueue.STANDBY);
    }

    /**
     * It copies bytes from the input stream to the body of the BytesMessage.
     * Upon success, it returns number of bytes copied to the message.
     */
    public static int copyBytes(InputStream in, byte[] buffer,
        BytesMessage msg) throws JMSException {
        int size = 0;
        int i, n, len = 0, bytesRead;
        if (in == null || msg == null)
            return -1;
        if (buffer != null)
            len = buffer.length;
        if (len <= 0) {
            len = 4096;
            buffer = new byte[len];
        }

        try {
            while ((bytesRead = in.read(buffer, 0, len)) >= 0) {
                if (bytesRead > 0)
                    msg.writeBytes(buffer, 0, bytesRead);
                size += bytesRead;
            }
        }
        catch (IOException e) {
            throw(new JMSException("failed to read bytes from input stream: " +
                e.toString()));
        }
        return size;
    }

    /**
     * It copies bytes from the body of the BytesMessage to the output stream.
     * Upon success, it returns number of bytes copied to the stream.
     */
    public static int copyBytes(BytesMessage msg, byte[] buffer,
        OutputStream out) throws JMSException {
        int size = 0;
        int len = 0, bytesRead;
        if (out == null || msg == null)
            return -1;
        if (buffer != null)
            len = buffer.length;
        if (len <= 0) {
            len = 4096;
            buffer = new byte[len];
        }

        while ((bytesRead = msg.readBytes(buffer, len)) > 0) try {
            out.write(buffer, 0, bytesRead);
            size += bytesRead;
        }
        catch (IOException e) {
            throw(new JMSException("failed to write bytes to output stream: " +
                e.toString()));
        }
        return size;
    }

    /**
     * lists all NEW messages in a specific XQ and returns a Map based on
     * the given resultType and the property list. 
     */
    public static Map<String, String> listXQ(XQueue xq, int type, String[] pn) {
        Message msg;
        StringBuffer strBuf;
        Map<String, String> h = new HashMap<String, String>();
        Browser browser;
        String key, str, text;
        int id, k, n;
        boolean hasSummary;

        if (xq == null)
            return null;
        if (xq.depth() <= 0)
            return h;

        key = xq.getName();
        hasSummary = (pn != null && pn.length > 0);
        strBuf = new StringBuffer();

        n = 0;
        browser = xq.browser();
        while ((id = browser.next()) >= 0) {
            k = xq.getCellStatus(id);
            if (k != XQueue.CELL_OCCUPIED) // not a NEW message
                continue;
            msg = (Message) xq.browse(id);
            if (msg == null)
                continue;
            text = list(msg, type);
            if (text == null || text.length() <= 0) {
                new Event(Event.WARNING, "failed to list message at " + id +
                   " from XQ/" + key).send();
                continue;
            }
            if (!hasSummary)
                str = "";
            else try {
                str = display(msg, null, SHOW_NOTHING, pn);
                if (str == null)
                    str = "";
            }
            catch (Exception e) {
                str = e.toString();
            }
            if (type == Utils.RESULT_XML) { // for xml
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<ID>" + n + "</ID>");
                strBuf.append("<CellID>" + id + "</CellID>");
                strBuf.append("<Status>NEW</Status>");
                strBuf.append(text);
                strBuf.append("<Summary>" +Utils.escapeXML(str)+ "</Summary>");
                strBuf.append("</Record>");
            }
            else if (type == Utils.RESULT_JSON) { // for json
                if (n > 0)
                    strBuf.append(", {");
                else
                    strBuf.append("{");
                strBuf.append("\"ID\":" + n);
                strBuf.append(", \"CellID\":" + id);
                strBuf.append(", \"Status\":\"NEW\"");
                strBuf.append(", " + text);
                strBuf.append(", \"Summay\":\"" + Utils.escapeJSON(str) + "\"");
                strBuf.append("}");
            }
            else { // for text
                h.put(key + "_" + n, id + " NEW " + text + " " + str);
            }
            n ++;
        }

        if (n == 0) // no message found
            return h;
        else if (type == Utils.RESULT_XML)
            h.put("XQ", strBuf.toString());
        else if (type == Utils.RESULT_JSON)
            h.put("XQ", "[" + strBuf.toString() + "]");

        return h;
    }

    /** It resets JMSReplyTo to replyTo and returns 0 for success */
    public static int resetReplyTo(Destination replyTo, Message msg)
        throws JMSException {
        if (msg != null) {
            if (replyTo != null)
                msg.setJMSReplyTo(replyTo);
            else if (msg.getJMSReplyTo() != null) 
                msg.setJMSReplyTo(null);
            return 0;
        }
        else
            return -1;
    }

    /**
     * It returns an Object array with plugin method, plugin object and a
     * method to close all resources.  The plugin method is supposed to
     * take the arguments of types specified in argClassNames.  There is no
     * requirement on what it should return.  The plugin object is initialized
     * with the only argument stored in the property hashmap.  The property
     * hashmap should contain Name, initial arguments under argName.  The
     * closerName is optional and it is OK to be null.  The prefix is used for
     * exception.
     */
    public static Object[] getPlugins(Map props, String argName,
        String methodName, String[] argClassNames, String closerName,
        String prefix) {
        int ic = 0;
        String className, name;
        Object o, pluginArg;
        Object[] plugin = new Object[3];
        Class<?> cls, cc;
        Class[] pTypes;
        if (props == null || props.size() <= 0 || methodName == null ||
            argClassNames == null)
            return null;
        name = (String) props.get("Name");
        if ((o = props.get("ClassName")) == null)
            throw(new IllegalArgumentException(prefix +
                ": ClassName is not defined for " + name));
        className = (String) o;
        if (className.length() == 0)
            throw(new IllegalArgumentException(prefix +
                ": ClassName is empty for " + name));

        o = null;
        if (closerName != null && closerName.length() > 0) try {
            cls = Class.forName(className);
            o = cls.getMethod(closerName, new Class[]{});
        }
        catch (Exception e) {
            o = null;
        }
        plugin[2] = o;

        pTypes = new Class[argClassNames.length];
        try {
            for (int i=0; i<argClassNames.length; i++)
                pTypes[i] = Class.forName(argClassNames[i]);

            cls = Class.forName(className);
            o = cls.getMethod(methodName, pTypes);
        }
        catch (Exception e) {
            o = null;
            throw(new IllegalArgumentException(prefix + ": failed to get "+
                methodName + "() method in " + className + " for " + name +
                ": " + Event.traceStack(e)));
        }
        catch (NoClassDefFoundError e) {
            o = null;
            throw(new IllegalArgumentException(prefix + ": failed to get "+
                methodName + "() method in " + className + " for " + name +
                ": " + Event.traceStack(e)));
        }
        catch (UnsupportedClassVersionError e) {
            o = null;
            throw(new IllegalArgumentException(prefix + ": failed to get "+
                methodName + "() method in " + className + " for " + name +
                ": " + Event.traceStack(e)));
        }
        if (o == null)
           throw(new IllegalArgumentException(prefix + ": no "+
               methodName + "() method in " + className + " for " + name));
        plugin[0] = o;

        try {
            if ((o = props.get(argName)) != null) {
                pluginArg = o;
                if (pluginArg instanceof String)
                    cc = String.class;
                else if (pluginArg instanceof List)
                    cc = List.class;
                else if (!(pluginArg instanceof Map))
                    throw(new IllegalArgumentException("wrong argument type"));
                else if (!"RouterArgument".equals(argName) &&
                    !"route".equals(methodName)) // generic cases for Map
                    cc = Map.class;
                else try { // special case for Map or HashMap
                    cc = Map.class;
                    o = cls.getConstructor(new Class[]{cc});
                }
                catch (Exception ex) { // old plugin requires HashMap
                    cc = HashMap.class;
                    ic = 1;
                }
            }
            else {
                pluginArg = (String) null;
                cc = String.class;
            }

            java.lang.reflect.Constructor con = cls.getConstructor(cc);
            if (ic > 0) // cast to HashMap
                o = con.newInstance(new Object[]{(HashMap) pluginArg});
            else
                o = con.newInstance(new Object[]{pluginArg});
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            o = null;
            if (ex != null && ex instanceof JMSException) {
                Exception ee = ((JMSException) ex).getLinkedException();
                if (ee != null)
                    throw(new IllegalArgumentException(prefix +
                        " failed to instantiate " + className + ": " +
                        ee.toString() + " " + Event.traceStack(ex)));
                else
                    throw(new IllegalArgumentException(prefix +
                        " failed to instantiate " + className + ": " +
                        Event.traceStack(ex)));
            }
            else {
                throw(new IllegalArgumentException(prefix +
                    " failed to instantiate " + className +
                    ": " + Event.traceStack((ex != null) ? ex : e)));
            }
        }
        catch (Exception e) {
            o = null;
            throw(new IllegalArgumentException(prefix +
                ": failed to instantiate " + className + " for " + name +
                ": " + Event.traceStack(e)));
        }
        catch (NoClassDefFoundError e) {
            o = null;
            throw(new IllegalArgumentException(prefix +
                " failed to instantiate " + className + ": " +
                Event.traceStack(e)));
        }
        catch (UnsupportedClassVersionError e) {
            o = null;
            throw(new IllegalArgumentException(prefix +
                " failed to instantiate " + className + ": " +
                Event.traceStack(e)));
        }
        catch (Error e) {
            o = null;
            Event.flush(e);
        }
        plugin[1] = o;

        return plugin;
    }

    /** enables the flow control on the SoniceMQ JMS session */
    public static void enableFlowControl(Session s) {
        if (enableFlowControl == null) try {
            initStaticMethod("enableFlowControl");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "enableFlowControl from SonicMQUtils: " + Event.traceStack(e)));
        }
        try {
            enableFlowControl.invoke(null, new Object[]{s});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to enable flow control: "
                + Event.traceStack(e)));
        }
    }

    /** disables the flow control on the SoniceMQ JMS session */
    public static void disableFlowControl(Session s) {
        if (disableFlowControl == null) try {
            initStaticMethod("disableFlowControl");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "disableFlowControl from SonicMQUtils: " +Event.traceStack(e)));
        }
        try {
            disableFlowControl.invoke(null, new Object[]{s});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to disable flow control:"
                + Event.traceStack(e)));
        }
    }

    /** initializes static methods from SonicMQUtils */
    private synchronized static void initStaticMethod(String name)
        throws ClassNotFoundException, NoSuchMethodException {
        String className = "org.qbroker.sonicmq.SonicMQUtils";
        if ("enableFlowControl".equals(name) && enableFlowControl == null) {
            Class<?> cls = Class.forName(className);
            enableFlowControl = cls.getMethod(name, new Class[]{Session.class});
        }
        else if("disableFlowControl".equals(name) && disableFlowControl ==null){
            Class<?> cls = Class.forName(className);
            disableFlowControl = cls.getMethod(name,new Class[]{Session.class});
        }
        else
            throw(new IllegalArgumentException("no such method of " + name +
                  " defined in " + className));
    }
}
