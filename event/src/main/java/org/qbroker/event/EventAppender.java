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
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventAction;

/**
 * EventAppender implements EventAction and appends/writes a formatted
 * event to a give file.
 *<br/><br/>
 * This is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventAppender implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private long serialNumber;
    private int debug = 0;
    private Map<String, Object> appender;
    private String logfile = null, uri;
    private boolean append = true;
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

        appender = new HashMap<String, Object>();
        map = new HashMap<String, Object>();
        if ((o = props.get("Summary")) != null)
            map.put("Summary", new Template((String) o));

        if ((o = props.get("TemplateFile")) != null)
            map.put("Template", new Template(new File((String) o)));

        if (map.size() > 0) {
            o = props.get("Substitution");
            if (o != null && o instanceof List) {
                msgSub = Utils.initSubstitutions((List) o);
                map.put("MsgSub", msgSub);
            }
            appender.put("Default", map);
        }
        else if ((o = props.get("Default")) != null && o instanceof Map) {
            o = ((Map) o).get("Substitution");
            if (o != null && o instanceof List)
                msgSub = Utils.initSubstitutions((List) o);
            else if ((o = props.get("Substitution")) != null &&
                o instanceof List)
                msgSub = Utils.initSubstitutions((List) o);
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
                    map.put("MsgSub", Utils.initSubstitutions((List) o));
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
        String key, value, headline = null, text = null;
        HashMap attr;
        Map change = null;
        String[] allFields;
        Template template = null;
        TextSubstitution[] msgSub = null;

        if (event == null || map == null)
            return null;

        msgSub = (TextSubstitution[]) map.get("MsgSub");
        if (msgSub != null)
            change = EventUtils.getChange(event, msgSub, pm);

        attr = event.attribute;
        template = (Template) map.get("Summary");
        if (template != null) {
            allFields = template.getAllFields();
            headline = template.copyText();
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
                    headline = template.substitute(pm, key, value, headline);
                }
                else {
                    headline = template.substitute(pm, key, "", headline);
                }
            }
        }

        template = (Template) map.get("Template");
        if (template != null) {
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
                    text = template.substitute(pm, key, value, text);
                }
                else if ("serialNumber".equals(key)) {
                    text = template.substitute(pm, key,
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
                    text = template.substitute(pm, key, "", text);
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
        String eventType;
        String priorityName;
        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;
        serialNumber ++;

        eventType = (String) event.attribute.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = (Map) appender.get(eventType);
        if (map == null)
            map = (Map) appender.get("Default");

        if (map != null && map.size() > 0) try {
            str = append(event, map);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to append text for " +
                eventType + ": " + Event.traceStack(e)).send();
            return;
        }
        if (debug > 0 && str != null)
            new Event(Event.DEBUG, name + ": appended " + str.length() +
                " bytes to " + logfile).send();
    }

    public String getName() {
        return name;
    }
}
