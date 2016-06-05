package org.qbroker.jms;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;
import java.io.File;
import java.text.SimpleDateFormat;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.event.Event;
import org.qbroker.jms.MessageUtils;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;

/**
 * Msg2Text is a customized formatter that formats a JMS message into a plain
 * text according to the result type.  It has two methods, format(Message) and
 * formmat(int, Message). The former is used by FormatNode to format messages.
 * It loads the formatted text into the message body. The latter will not
 * change the message. Instead, it returns the fomatted text. By default,
 * Msg2Text formats the message into an Event in terms of either postable or
 * collectible.
 *<br/><br/>
 * In case of no Template defined, the default formatter will be used. The
 * default formatter will process all properties for both JMS and user.
 * If the Template is an empty text, the simple formatter will be used. The
 * simple formatter will process all the user properties only.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class Msg2Text {
    private String name;
    private Template template = null, repeatedTemplate = null;
    private Map<String, Object> excludedProperty;
    private Perl5Matcher pm = null;
    private String myIP = null;
    private String[] keys = null, names = null;
    private String baseTag0, baseTag1;
    private int defaultType = Utils.RESULT_TEXT;
    private boolean hasRepeatedTemplate = false;
    private final SimpleDateFormat zonedDateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

    public Msg2Text(Map props) {
        Object o;

        if (props == null)
            throw(new IllegalArgumentException("property hash is null"));

        if ((o = props.get("Name")) != null)
            name = (String) o;
        else
            name = "Msg2Text";

        if ((o = props.get("ResultType")) != null)
            defaultType = Integer.parseInt((String) o);

        if ((o = props.get("BaseTag")) != null) {
            baseTag0 = "<" + (String) o + " type=\"ARRAY\">";
            baseTag1 = "</" + (String) o + ">";
        }
        else {
            baseTag0 = "<Message>\n";
            baseTag1 = "</Message>\n";
        }

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null &&
            ((String) o).length() > 0)
            template = new Template((String) o);
        else if (o != null && ((String) o).length() == 0) {
            keys = new String[0];
            names = new String[0];
        }

        if (template != null) {
            keys = template.getAllFields();
            names = new String[keys.length];
            for (int i=0; i<keys.length; i++) { // init property ids
                names[i] = keys[i];
                keys[i] = MessageUtils.getPropertyID(keys[i]);
                if (keys[i] == null)
                    keys[i] = names[i];
            }
        }

        if ((o = props.get("RepeatedTemplate")) != null &&
            ((String) o).length() > 0)
            repeatedTemplate = new Template((String) o);

        if ((o = props.get("ExcludedProperty")) != null && o instanceof Map)
            excludedProperty = Utils.cloneProperties((Map) o);
        else { // exlude basic event attributes
            excludedProperty = new HashMap<String, Object>();
            excludedProperty.put("hostname", "");
            excludedProperty.put("priority", "");
            excludedProperty.put("program", "");
            excludedProperty.put("pid", "");
            excludedProperty.put("category", "");
            excludedProperty.put("owner", "");
            excludedProperty.put("name", "");
            excludedProperty.put("type", "");
            excludedProperty.put("site", "");
            excludedProperty.put("text", "");
        }

        hasRepeatedTemplate = (repeatedTemplate != null);
        if (template != null || hasRepeatedTemplate)
            pm = new Perl5Matcher();
        myIP = Event.getIPAddress();
        if (myIP == null || myIP.length() <= 0)
            myIP = "127.0.0.1";
    }

    /**
     * It formatts the message based on the default ResultType, the predefined
     * template and the repeated template.  Upon success, it loads the
     * formatted text into the message body and returns null to indicate OK.
     * Otherwise, it returns a error text. 
     *<br/><br/>
     * It can be used as the plugin formatter in FormatNode.
     */
    public String format(Message msg) {
        String text = null;
        if (msg == null)
            return "null message";

        try {
            text = format(defaultType, msg);
            msg.clearBody();
            if (msg instanceof TextMessage) {
                ((TextMessage) msg).setText(text);
            }
            else if (msg instanceof BytesMessage) {
                ((BytesMessage) msg).writeBytes(text.getBytes());
            }
            else {
                return "message type not supported";
            }
        }
        catch (JMSException e) {
            return Event.traceStack(e);
        }
        catch (Exception e) {
            return Event.traceStack(e);
        }
        return null;
    }

    /** returns a formatted text of the message according to the type */
    public String format(int type, Message msg) {
        byte[] buffer = new byte[4096];
        String text = null;
        if (msg == null)
            return null;

        if (type <= 0)
            type = defaultType;

        try {
            if ((type & Utils.RESULT_XML) > 0)
                text = getXML(msg, buffer);
            else if ((type & Utils.RESULT_JSON) > 0)
                text = getJSON(msg, buffer);
            else
                text = getText(type, msg, buffer);
        }
        catch (JMSException e) {
            text = null;
        }
        catch (Exception e) {
            text = null;
        }
        return text;
    }

    /**
     * returns a formatted text of the message with the given template
     * and a repeated template.  It can be used to format messages into
     * either postable events or collectible events.
     */
    private String getText(int type, Message msg, byte[] buffer) throws
        JMSException {
        String str = null, text = null;
        int priority = -1;
        if (msg == null)
            return null;

        if (pm == null)
            pm = new Perl5Matcher();

        if ((type & Utils.RESULT_COLLECTABLE) > 0 ||
            (type & Utils.RESULT_POSTABLE) > 0) { // for event
            priority = msg.getJMSPriority();
            str = msg.getStringProperty("priority");
            msg.setStringProperty("priority",
                Event.priorityNames[9-priority]);
            text = MessageUtils.format(msg, buffer, template,
                repeatedTemplate, excludedProperty, pm);
            msg.setStringProperty("priority", str);
        }
        else if (hasRepeatedTemplate)
            text = MessageUtils.format(msg, buffer, template,
                repeatedTemplate, excludedProperty, pm);
        else if (template != null)
            text = MessageUtils.format(msg, buffer, template, pm);
        else
            text = MessageUtils.processBody(msg, buffer);

        if ((type & Utils.RESULT_COLLECTABLE) > 0) {
            long tm = msg.getJMSTimestamp();
            text = zonedDateFormat.format(new Date(tm)) + " " +
                myIP + " " + text;
        }
        return text;
    }

    private String getXML(Message msg, byte[] buffer) throws JMSException {
        if (msg == null)
            return null;

        StringBuffer strBuf = new StringBuffer();
        String text;
        String key;
        int i;

        if (keys == null) { // default format
            Object o;
            Enumeration propNames = msg.getPropertyNames();
            i = msg.getJMSPriority();
            strBuf.append("<priority>" + Event.priorityNames[9-i] +
                "</priority>\n");
            key = MessageUtils.processBody(msg, buffer);
            if (key != null)
                key = "";
            strBuf.append("<text>" + Utils.escapeXML(key) + "</text>\n");
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.length() <= 0)
                    continue;
                if ("priority".equals(key) || "text".equals(key))
                    continue;
                text = msg.getStringProperty(key);
                if (text == null)
                    text = "";
                strBuf.append("<" + key + ">" + Utils.escapeXML(text) +
                        "</" + key + ">\n");
            }
            strBuf.append("<JMSPriority>" + msg.getJMSPriority() +
                "</JMSPriority>\n");
            strBuf.append("<JMSExpiration>" + msg.getJMSExpiration() +
                "</JMSExpiration>\n");
            strBuf.append("<JMSDeliveryMode>" + msg.getJMSDeliveryMode() +
                "</JMSDeliveryMode>\n");
            strBuf.append("<JMSTimestamp>" + msg.getJMSTimestamp() +
                "</JMSTimestamp>\n");
            if ((key = msg.getJMSMessageID()) == null)
                key = "";
            strBuf.append("<JMSMessageID>" + key + "</JMSMessageID>\n");
            if ((key = msg.getJMSCorrelationID()) == null)
                key = "";
            strBuf.append("<JMSCorrelationID>" + Utils.escapeXML(key) +
                    "</JMSCorrelationID>\n");
            if ((key = msg.getJMSType()) == null)
                key = "";
            strBuf.append("<JMSType>" + Utils.escapeXML(key)+ "</JMSType>\n");
            if ((o = msg.getJMSReplyTo()) != null)
                key = o.toString();
            else
                key = "";
            strBuf.append("<JMSReplyTo>"+Utils.escapeXML(key)+
                "</JMSReplyTo>\n");
        }
        else if (keys.length > 0) { // format based on template
            if (keys.length == 1 && "body".equals(keys[0])) { // for body only
                text = MessageUtils.processBody(msg, buffer);
                if (text == null)
                    text = "";
                strBuf.append(Utils.escapeXML(text));
            }
            else for (i=0; i<keys.length; i++) {
                key = keys[i];
                if (key == null || key.length() <= 0)
                    continue;
                text = MessageUtils.getProperty(key, msg);
                if (text != null)
                    text = "";
                key = names[i];
                strBuf.append("<" + key + ">" + Utils.escapeXML(text) +
                    "</" + key + ">");
            }
        }
        else { // for simple format
            Object o;
            Enumeration propNames = msg.getPropertyNames();
            key = MessageUtils.processBody(msg, buffer);
            if (key != null)
                key = "";
            strBuf.append("<text>" + Utils.escapeXML(key) + "</text>\n");
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.length() <= 0)
                    continue;
                if ("text".equals(key))
                    continue;
                text = msg.getStringProperty(key);
                if (text == null)
                    text = "";
                strBuf.append("<" + key + ">" + Utils.escapeXML(text) +
                        "</" + key + ">\n");
            }
        }
        return baseTag0 + strBuf.toString() + baseTag1;
    }

    private String getJSON(Message msg, byte[] buffer) throws JMSException {
        if (msg == null)
            return null;

        StringBuffer strBuf = new StringBuffer();
        String text;
        String key;
        int i;

        if (keys == null) { // default format
            Object o;
            Enumeration propNames = msg.getPropertyNames();
            i = msg.getJMSPriority();
            strBuf.append("\"priority\":\""+Event.priorityNames[9-i] + "\"\n");
            key = MessageUtils.processBody(msg, buffer);
            if (key == null)
                key = "";
            strBuf.append(",\"text\":\"" + Utils.escapeJSON(key) + "\"\n");
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.equals("null"))
                    continue;
                if ("priority".equals(key) || "text".equals(key))
                    continue;
                text = msg.getStringProperty(key);
                if (text == null)
                    text = "";
                if (key.indexOf("-") >= 0) // extjs decoder fails on "-"
                    key = "\"" + key + "\"";
                strBuf.append(",\""+key+"\":\""+Utils.escapeJSON(text)+"\"\n");
            }
            strBuf.append(",\"JMSPriority\":" + msg.getJMSPriority() + "\n");
            strBuf.append(",\"JMSExpiration\":" + msg.getJMSExpiration()+"\n");
            strBuf.append(",\"JMSDeliveryMode\":" + msg.getJMSDeliveryMode()+
                "\n");
            strBuf.append(",\"JMSTimestamp\":" + msg.getJMSTimestamp() + "\n");
            if ((key = msg.getJMSMessageID()) == null)
                key = "";
            strBuf.append(",\"JMSMessageID\":\"" + key + "\"\n");
            if ((key = msg.getJMSCorrelationID()) == null)
                key = "";
            strBuf.append(",\"JMSCorrelationID\":\""+Utils.escapeJSON(key)+
                "\"\n");
            if ((key = msg.getJMSType()) == null)
                key = "";
            strBuf.append(",\"JMSType\":\"" + Utils.escapeJSON(key) + "\"\n");
            if ((o = msg.getJMSReplyTo()) != null)
                key = o.toString();
            else
                key = "";
            strBuf.append(",\"JMSReplyTo\":\"" + Utils.escapeJSON(key)+ "\"");
        }
        else if (keys.length > 0) { // format based on template
            if (keys.length == 1 && "body".equals(keys[0])) { // for body only
                text = MessageUtils.processBody(msg, buffer);
                if (text == null)
                    text = "";
                strBuf.append("\"" + Utils.escapeJSON(text) + "\"");
                return strBuf.toString();
            }
            else for (i=0; i<keys.length; i++) {
                key = keys[i];
                if (key == null || key.length() <= 0)
                    continue;
                if (strBuf.length() > 0)
                    strBuf.append(",");
                text = MessageUtils.getProperty(key, msg);
                if (text == null)
                    text = "";
                key = names[i];
                if (key.indexOf("-") >= 0) // extjs decoder fails on "-"
                    key = "\"" + key + "\"";
                strBuf.append("\""+key+"\":\"" + Utils.escapeJSON(text) + "\"");
            }
        }
        else { // for simple format
            Object o;
            Enumeration propNames = msg.getPropertyNames();
            key = MessageUtils.processBody(msg, buffer);
            if (key == null)
                key = "";
            strBuf.append("\"text\":\"" + Utils.escapeJSON(key) + "\"\n");
            while (propNames.hasMoreElements()) {
                key = (String) propNames.nextElement();
                if (key == null || key.equals("null"))
                    continue;
                if ("text".equals(key))
                    continue;
                text = msg.getStringProperty(key);
                if (text == null)
                    text = "";
                strBuf.append(",\""+key+"\":\""+Utils.escapeJSON(text)+"\"\n");
            }
        }
        return "{" + strBuf.toString() + "}";
    }
}
