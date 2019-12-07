package org.qbroker.event;

/* FormattedEventMailer.java - an EventAction to send formatted emails */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import javax.mail.internet.InternetAddress;
import javax.mail.MessagingException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.net.MessageMailer;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventAction;

/**
 * FormattedEventMailer implements EventAction and sends an email formatted
 * from the event to the specified recipients.
 *<br><br>
 * The method of invokeAction() is MT-Safe.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class FormattedEventMailer implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private String formatKey = "type";
    private long serialNumber;
    private int debug = 0;
    private boolean isType = true;
    private MessageMailer mailer;
    private Map<String, Map> sender;
    private InternetAddress[] recipients;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;

    public FormattedEventMailer(Map props) {
        Object o;
        Template template;
        TextSubstitution[] msgSub = null;
        Map h;
        Map<String, Object> map;
        String str, key, value, owner = null;
        String[] addressList = null;
        int i, n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "FormattedEventMailer";
        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");
        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "send email formatted from event to recipients";

        if ((o = props.get("Email")) != null && o instanceof List) {
            n = ((List) o).size();
            addressList = new String[n];
            for (i=0; i<n; i++)
                addressList[i] = (String) ((List) o).get(i);
        }

        if (addressList == null || addressList.length <= 0 ||
            addressList[0] == null || addressList[0].length() <= 0)
            throw(new IllegalArgumentException(name +
                ": null or empty recipients"));

        if ((o = props.get("From")) != null)
            owner = (String) o;

        if ((o = props.get("FormatKey")) != null) {
            formatKey = (String) o;
            if (formatKey.length() <= 0)
                formatKey = "type";
            else
                isType = false;
        }

        sender = new HashMap<String, Map>();
        key = (String) props.get("Subject");
        value = (String) props.get("Template");
        str = (String) props.get("TemplateFile");
        if (key != null && key.length() > 0 &&
           ((value != null && value.length() > 0) ||
           (str != null && str.length() > 0))) {
            map = new HashMap<String, Object>();
            map.put("Subject", new Template(key));
            if (value != null && value.length() > 0)
                map.put("Template", new Template(value));
            else
                map.put("Template", new Template(new File(str)));
            o = props.get("Substitution");
            if (o != null && o instanceof List) {
                msgSub = EventUtils.initSubstitutions((List) o);
                map.put("MsgSub", msgSub);
            }
            sender.put("Default", map);
        }
        else if ((o = props.get("Default")) != null && o instanceof Map) {
            o = ((Map) o).get("Substitution");
            if (o != null && o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
            else if ((o = props.get("Substitution")) != null &&
                o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
        }

        Iterator iter = props.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() == 0)
                continue;
            if ((o = props.get(key)) == null || !(o instanceof Map))
                continue;
            if ("StringProperty".equals(key) || "ActiveTime".equals(key) ||
                "CopiedProperty".equals(key))
                continue;

            h = (Map) o;
            if (h.containsKey("Option")) // for option
                continue;
            o = h.get("Subject");
            if (o == null)
                continue;
            value = (String) h.get("Template");
            str = (String) h.get("TemplateFile");
            if ((value == null || value.length() <= 0) &&
                (str == null || str.length() <= 0))
                continue;
            map = new HashMap<String, Object>();
            map.put("Subject", new Template((String) o));
            if (value != null && value.length() > 0)
                map.put("Template", new Template(value));
            else
                map.put("Template", new Template(new File(str)));

            o = h.get("Substitution");
            if (o != null && o instanceof List) // override
                map.put("MsgSub", EventUtils.initSubstitutions((List) o));
            else if (o == null) // use the default
                map.put("MsgSub", msgSub);

            sender.put(key, map);
        }
        if (sender.size() <= 0)
            throw(new IllegalArgumentException(name + ": no template defined"));

        recipients = new InternetAddress[addressList.length];
        for (i=0; i<addressList.length; i++)
            recipients[i]= MessageMailer.getMailAddress(addressList[i]);

        if (owner == null || owner.length() <= 0) {
            String hostName = Event.getHostName();
            owner = System.getProperty("user.name");
            if (hostName != null && hostName.length() > 0)
                owner += "@" + hostName;
        }

        mailer = new MessageMailer(addressList[0], owner, null);

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Priority")) != null)
                pattern = pc.compile((String) o);
            else
                pattern = null;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                ": failed to compile pattern: " + e.toString()));
        }

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        serialNumber = 0;
    }

    private String send(Event event, Map map) throws MessagingException {
        int i, n;
        String key, value, subject, text;
        String[] allFields;
        Template template = null;
        TextSubstitution[] msgSub = null;
        HashMap attr;
        Map change = null;

        if (event == null || map == null)
            return null;

        msgSub = (TextSubstitution[]) map.get("MsgSub");
        if (msgSub != null) {
            change = EventUtils.getChange(event, msgSub);
            if (change != null && change.size() <= 0)
                change = null;
        }

        attr = event.attribute;
        template = (Template) map.get("Subject");
        allFields = template.getAllFields();
        subject = template.copyText();
        n = allFields.length;
        for (i=0; i<n; i++) { // for subject
            key = allFields[i];
            if (attr.containsKey(key)) {
                if (change == null)
                    value = (String) attr.get(key);
                else if (change.containsKey(key))
                    value = (String) change.get(key);
                else
                    value = (String) attr.get(key);
                if (value == null)
                    value = "";
                subject = template.substitute(key, value, subject);
            }
            else {
                subject = template.substitute(key, "", subject);
            }
        }

        template = (Template) map.get("Template");
        allFields = template.getAllFields();
        text = template.copyText();
        n = allFields.length;
        for (i=0; i<n; i++) {
            key = allFields[i];
            if (attr.containsKey(key)) {
                if (change == null)
                    value = (String) attr.get(key);
                else if (change.containsKey(key))
                    value = (String) change.get(key);
                else
                    value = (String) attr.get(key);
                if (value == null)
                    value = "";
                text = template.substitute(key, value, text);
            }
            else if ("serialNumber".equals(key)) {
                text = template.substitute(key,
                    String.valueOf(serialNumber), text);
            }
            else if ("all".equals(key)) {
                text = template.substitute(key,
                    EventUtils.pretty(event, change), text);
            }
            else if ("compact".equals(key)) {
                text = template.substitute(key,
                    EventUtils.compact(event, change), text);
            }
            else {
                text = template.substitute(key, "", text);
            }
        }
        if (change != null)
            change.clear();

        mailer.send(subject, text, recipients);

        return text;
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        Map map;
        String eventKey, priorityName, str = null;

        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;
        eventKey = (String) event.attribute.get(formatKey);
        if (eventKey == null || eventKey.length() == 0) {
            if (isType)
                eventKey = "Default";
            else { // retry on type
                eventKey = (String) event.attribute.get("type");
                if (eventKey == null || eventKey.length() == 0)
                    eventKey = "Default";
            }
        }

        map = sender.get(eventKey);
        if (map == null)
            map = sender.get("Default");

        if (map != null && map.size() > 0) try {
            str = send(event, map);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to send email for " +
                eventKey + ": " + Event.traceStack(e)).send();
            return;
        }
        if (debug > 0 && str != null)
            new Event(Event.DEBUG, name + ": sent an email of " + str.length()+
                " bytes to " + recipients.length + " recipients").send();
    }

    public String getName() {
        return name;
    }

    public void close() {
        pm = null;
        pattern = null;
        mailer = null;
        if (sender != null) {
            Template temp;
            TextSubstitution[] tsub;
            Map map;
            for (String key : sender.keySet()) {
                map = sender.get(key);
                temp = (Template) map.remove("Subject");
                if (temp != null)
                temp = (Template) map.remove("Template");
                if (temp != null)
                    temp.clear();
                tsub = (TextSubstitution[]) map.remove("MsgSub");
                if (tsub != null) {
                    for (TextSubstitution sub : tsub)
                        sub.clear();
                }
                map.clear();
            }
            sender.clear();
            sender = null;
        }
    }

    protected void finalize() {
        close();
    }
}
