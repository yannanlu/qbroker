package org.qbroker.event;

/* EventScriptLauncher.java - an EventAction to launch a script */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.io.File;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventAction;

/**
 * EventScriptLauncher runs a script outside JVM in response to an event.
 * The script may contains the template placeholders referecing the
 * attributes of the event.
 *<br/><br/>
 * NB. The action part is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class EventScriptLauncher implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private Map<String, Map> launcher;
    private String[] copiedProperty;
    private long serialNumber;
    private int timeout;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private String program, hostname, pid;

    public EventScriptLauncher(Map props) {
        Object o;
        Map map;
        String script, key, value;
        Template template, temp;
        TextSubstitution[] msgSub = null;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "EventScriptLauncher";

        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");

        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "launch a script in response to an event";

        launcher = new HashMap<String, Map>();

        if ((o = props.get("Timeout")) == null ||
            (timeout = 1000*Integer.parseInt((String) o)) < 0)
            timeout = 60000;

        if ((o = props.get("Script")) != null) {
            script = EventUtils.substitute((String) o, template);
            map = new HashMap();
            temp = new Template(script);
            if (temp.numberOfFields() <= 0)
                map.put("Template", script);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = props.get("Substitution");
                if (o != null && o instanceof List) {
                    msgSub = EventUtils.initSubstitutions((List) o);
                    map.put("MsgSub", msgSub);
                }
            }
            launcher.put("Default", map);
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
            if (((Map) o).containsKey("Option")) // for option hash
                continue;
            value = (String) ((Map) o).get("Script");
            if (value == null || value.length() <= 0) // not for script
                continue;
            script = EventUtils.substitute(value, template);
            map = new HashMap();
            temp = new Template(script);
            if (temp.numberOfFields() <= 0)
                map.put("Template", script);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = ((Map) o).get("Substitution");
                if (o != null && o instanceof List) // override
                   map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                   map.put("MsgSub", msgSub);
            }
            launcher.put(key, map);
        }
        if (launcher.size() <= 0)
            throw(new IllegalArgumentException(name + ": no script defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Priority")) != null)
                pattern = pc.compile((String) o);
            else
                pattern = null;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("CopiedProperty")) != null && o instanceof Map) {
            map = (Map) o;
            iter = ((Map) o).keySet().iterator();
            n = ((Map) o).size();
            copiedProperty = new String[n];
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key != null && key.length() > 0)
                    copiedProperty[n++] = key;
            }
        }
        else
            copiedProperty = new String[0];

        serialNumber = 0;
        hostname = Event.getHostName();
        program = Event.getProgramName();
        pid = String.valueOf(Event.getPID());
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        Object o;
        HashMap attr;
        Map map;
        String key, value, priorityName, eventType, script = null;
        StringBuffer strBuf;
        int i, n;

        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;
        attr = event.attribute;
        eventType = (String) attr.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = launcher.get(eventType);
        if (map == null)
            map = launcher.get("Default");
        if (map == null || map.size() <= 0)
            return;

        if ((o = map.get("Template")) != null && o instanceof Template) {
            String[] allFields;
            Map change = null;
            Template template;
            TextSubstitution[] msgSub;

            template = (Template) o;
            msgSub = (TextSubstitution[]) map.get("MsgSub");
            if (msgSub != null) {
                change = EventUtils.getChange(event, msgSub);
                if (change != null && change.size() <= 0)
                    change = null;
            }

            allFields = (String[]) map.get("Fields");
            script = template.copyText();
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
                    script = template.substitute(key, value, script);
                }
                else if ("serialNumber".equals(key)) {
                    script = template.substitute(key,
                        String.valueOf(serialNumber), script);
                }
                else {
                    script = template.substitute(key, "", script);
                }
            }
            if (change != null)
                change.clear();
        }
        else if (o != null && o instanceof String)
            script = (String) o;
        else
            return;

        Event ev = EventUtils.runScript(script, timeout);

        strBuf = new StringBuffer();
        strBuf.append((String) attr.get("date"));
        strBuf.append(" " + priorityName);
        strBuf.append(" " + (String) attr.get("name"));
        strBuf.append(" " + (String) attr.get("hostname"));
        strBuf.append(" " + (String) attr.get("program"));

        ev.setAttribute("name", name);
        if ((o = attr.get("site")) != null)
            ev.setAttribute("site", (String) o);
        else
            ev.setAttribute("site", site);
        ev.setAttribute("category", category);
        ev.setAttribute("type", type);
        ev.setAttribute("date", Event.dateFormat(new Date(ev.timestamp)));
        ev.setAttribute("description", description);
        ev.setAttribute("script", script);
        ev.setAttribute("original", strBuf.toString());

        for (i=0; i<copiedProperty.length; i++) {
            if ((o = attr.get(copiedProperty[i])) == null)
                continue;
            ev.setAttribute(copiedProperty[i], (String) o);
        }
        ev.setAttribute("program", program);
        ev.setAttribute("hostname", hostname);
        ev.setAttribute("pid", pid);

        ev.send();
    }

    /**
     * It applies the rules to the event and returns true if the script
     * is active and will be invoked upon the event, or false otherwise.
     */
    public boolean isActive(long currentTime, Event event) {
        HashMap attr;
        Map map;
        String eventType, priorityName;

        if (event == null)
            return false;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return false;

        attr = event.attribute;
        eventType = (String) attr.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = launcher.get(eventType);
        if (map == null)
            map = launcher.get("Default");
        if (map == null || map.size() <= 0)
            return false;

        return true;
    }

    public String getName() {
        return name;
    }

    public void close() {
        pm = null;
        pattern = null;
        if (launcher != null) {
            Map map;
            TextSubstitution[] tsub;
            Object o;
            for (String key : launcher.keySet()) {
                map = launcher.get(key);
                o = map.remove("Template");
                if (o != null && o instanceof Template)
                    ((Template) o).clear();
                tsub = (TextSubstitution[]) map.remove("MsgSub");
                if (tsub != null) {
                    for (TextSubstitution sub : tsub)
                        sub.clear();
                }
                map.clear();
            }
            launcher.clear();
            launcher = null;
        }
    }

    protected void finalize() {
        close();
    }
}
