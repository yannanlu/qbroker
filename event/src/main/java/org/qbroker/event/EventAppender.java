package org.qbroker.event;

/* EventAppender.java - an EventAction to append event to a log */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventAction;

/**
 * EventAppender implements EventAction and appends/writes a formatted
 * event to a give file.
 *<br><br>
 * This is MT-Safe.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class EventAppender implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private String formatKey = "type";
    private long serialNumber;
    private int debug = 0;
    private Map<String, Map> appender;
    private String logfile = null, uri;
    private boolean append = true, isType = true;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;

    public EventAppender(Map props) {
        URI u = null;
        Object o;
        Template template;
        TextSubstitution[] msgSub = null;
        Map<String, Object> map;
        Map h;
        String s, key, value;
        int i, n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "EventAppender";
        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");
        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "append a text formatted from event to a file";

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(name + ": bad URI of " + uri +
                ": " + e.toString()));
        }

        logfile = u.getPath();
        if (logfile == null || logfile.length() == 0)
            throw(new IllegalArgumentException(name +
                ": URI has no path: " + uri));

        if ((s = u.getScheme()) == null || "log".equals(s)) {
            append = true;
            logfile = u.getPath();
        }
        else if ("file".equals(s)) {
            append = false;
            logfile = u.getPath();
        }
        else {
            throw(new IllegalArgumentException(name +
                ": unsupported scheme: " + s));
        }

        if ((o = props.get("FormatKey")) != null) {
            formatKey = (String) o;
            if (formatKey.length() <= 0)
                formatKey = "type";
            else
                isType = false;
        }

        appender = new HashMap<String, Map>();
        map = new HashMap<String, Object>();
        if ((o = props.get("Summary")) != null)
            map.put("Summary", new Template((String) o));

        if ((o = props.get("TemplateFile")) != null)
            map.put("Template", new Template(new File((String) o)));

        if (map.size() > 0) {
            o = props.get("Substitution");
            if (o != null && o instanceof List) {
                msgSub = EventUtils.initSubstitutions((List) o);
                map.put("MsgSub", msgSub);
            }
            appender.put("Default", map);
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
            map = new HashMap<String, Object>();
            value = (String) h.get("Summary");
            if (value != null)
                map.put("Summary", new Template(value));
            value = (String) h.get("TemplateFile");
            if (value != null)
                map.put("Template", new Template(new File(value)));

            if (map.size() > 0) {
                o = h.get("Substitution");
                if (o != null && o instanceof List) // override
                    map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                    map.put("MsgSub", msgSub);

                appender.put(key, map);
            }
        }
        if (appender.size() <= 0)
            throw(new IllegalArgumentException(name + ": no template defined"));

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

    public String append(Event event, Map map) throws IOException {
        int i, n;
        String value, headline = null, text = null;
        HashMap attr;
        Map change = null;
        Template template = null;
        TextSubstitution[] msgSub = null;

        if (event == null || map == null)
            return null;

        msgSub = (TextSubstitution[]) map.get("MsgSub");
        if (msgSub != null) {
            change = EventUtils.getChange(event, msgSub);
            if (change != null && change.size() <= 0)
                change = null;
        }

        attr = event.attribute;
        template = (Template) map.get("Summary");
        if (template != null) {
            headline = template.copyText();
            for (String key : template.keySet()) {
                if (attr.containsKey(key)) {
                    if (change == null)
                        value = (String) attr.get(key);
                    else if (change.containsKey(key))
                        value = (String) change.get(key);
                    else
                        value = (String) attr.get(key);
                    if (value == null)
                        value = "";
                    headline = template.substitute(key, value, headline);
                }
                else {
                    headline = template.substitute(key, "", headline);
                }
            }
        }

        template = (Template) map.get("Template");
        if (template != null) {
            text = template.copyText();
            for (String key : template.keySet()) {
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
        }
        if (change != null)
            change.clear();

        FileOutputStream out = new FileOutputStream(logfile, append);
        if (headline != null) {
            out.write(headline.getBytes());
            if (text != null) {
                out.write(10);
                out.write(10);
                out.write(text.getBytes());
            }
        }
        else if (text != null)
            out.write(text.getBytes());
        out.close();

        if (headline != null)
            return headline + "\n\n" + text;
        else
            return text;
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        String str = null;
        Map map;
        String eventKey;
        String priorityName;
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

        map = appender.get(eventKey);
        if (map == null)
            map = appender.get("Default");

        if (map != null && map.size() > 0) try {
            str = append(event, map);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to append text for " +
                eventKey + ": " + Event.traceStack(e)).send();
            return;
        }
        if (debug > 0 && str != null)
            new Event(Event.DEBUG, name + ": appended " + str.length() +
                " bytes to " + logfile).send();
    }

    public String getName() {
        return name;
    }

    public void close() {
        pm = null;
        pattern = null;
        if (appender != null) {
            Map map;
            Template temp;
            TextSubstitution[] tsub;
            for (String key : appender.keySet()) {
                map = appender.get(key);
                temp = (Template) map.remove("Template");
                if (temp != null)
                    temp.clear();
                temp = (Template) map.remove("Summary");
                if (temp != null)
                    temp.clear();
                tsub = (TextSubstitution[]) map.remove("MsgSub");
                if (tsub != null) {
                    for (TextSubstitution sub : tsub)
                        sub.clear();
                }
                map.clear();
            }
            appender.clear();
            appender = null;
        }
    }

    protected void finalize() {
        close();
    }
}
