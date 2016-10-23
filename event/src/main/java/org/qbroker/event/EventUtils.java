package org.qbroker.event;

/* EventUtils.java - Utility set */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.Utils;
import org.qbroker.common.RunCommand;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.event.Event;

/**
 * EventUtils has some utilities for Event processing
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class EventUtils {
    public final static String LINE_SEPARATOR =
        System.getProperty("line.separator");
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");
    public final static String defaultName = "event";
    public final static String defaultSite = "DEFAULT";
    public final static String defaultCategory = "EVENT";
    public final static String defaultType = "Unknown";
    private final static String hostname = Event.getHostName().toLowerCase();
    private final static Map emptyMap = new HashMap();
    private final static Map defaultMap = new HashMap();
    private final static Template defaultTemplate =
        new Template("##hostname##, ##HOSTNAME##, ##owner##, ##pid##");
    private final static String padding[] = {
        "",
        " ",
        "  ",
        "   ",
        "    ",
        "     ",
        "      ",
        "       ",
        "        ",
        "         ",
        "          ",
        "           ",
        "            ",
        "             ",
        "              ",
        "               ",
    };

    static {
        defaultMap.put("hostname", hostname);
        defaultMap.put("HOSTNAME", hostname.toUpperCase());
        defaultMap.put("owner", System.getProperty("user.name"));
        defaultMap.put("pid", String.valueOf(Event.getPID()));
    }

    public static String pretty(Event event) {
        return pretty(event, emptyMap);
    }

    /**
     * It returns a formatted string of the event in the compact format.
     * The Map of change should contain only key-value pairs for
     * add/replace/delete.
     * value = null: delete<br/>
     * key in event: replace<br/>
     * no key in event: add<br/>
     * NB. change will be cleared at the return
     */
    public static String pretty(Event event, Map change) {
        Object o;
        String key, value;
        HashMap attribute;
        Iterator iter;
        StringBuffer strBuf;
        if (event == null || (attribute = event.attribute) == null)
            return null;
        if (change == null)
            change = emptyMap;

        strBuf = new StringBuffer();
        iter = (attribute.keySet()).iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            if (change.containsKey(key)) {
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                change.remove(key);
                if (value == null) // skip
                    continue;
            }
            else {
                o = attribute.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
            }
            if (key.length() < 15)
                strBuf.append(padding[15 - key.length()]);
            strBuf.append(Character.toUpperCase(key.charAt(0)));
            strBuf.append(key.substring(1));
            strBuf.append(": ");
            if (value == null)
                strBuf.append(LINE_SEPARATOR);
            else {
                strBuf.append(value);
                if (! value.endsWith("\n"))
                    strBuf.append(LINE_SEPARATOR);
            }
        }
        if (change.size() > 0) { // try to append new attributes
            iter = (change.keySet()).iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                if (key.length() < 15)
                    strBuf.append(padding[15 - key.length()]);
                strBuf.append(Character.toUpperCase(key.charAt(0)));
                strBuf.append(key.substring(1));
                strBuf.append(": ");
                if (value == null)
                    strBuf.append(LINE_SEPARATOR);
                else {
                    strBuf.append(value);
                    if (! value.endsWith("\n"))
                        strBuf.append(LINE_SEPARATOR);
                }
            }
        }
        return strBuf.toString();
    }

    public static String compact(Event event) {
        return compact(event, emptyMap);
    }

    /**
     * It returns a formatted string of the event in the compact format.
     * The Map of change should contain only key-value pairs for
     * add/replace/delete.
     * value = null: delete<br/>
     * key in event: replace<br/>
     * no key in event: add<br/>
     * NB. change will be cleared at the return
     */
    public static String compact(Event event, Map change) {
        Object o;
        String key, value;
        HashMap attribute;
        Iterator iter;
        StringBuffer strBuf;
        if (event == null || (attribute = event.attribute) == null)
            return null;
        if (change == null)
            change = emptyMap;

        strBuf = new StringBuffer();
        iter = (attribute.keySet()).iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0 || "site".equals(key) ||
                "category".equals(key) || "gid".equals(key))
                continue;
            if (change.containsKey(key)) {
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                change.remove(key);
                if (value == null) // skip
                    continue;
            }
            else {
                o = attribute.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
            }
            if (key.length() < 15)
                strBuf.append(padding[15 - key.length()]);
            strBuf.append(Character.toUpperCase(key.charAt(0)));
            strBuf.append(key.substring(1));
            strBuf.append(": ");
            if (value == null)
                strBuf.append(LINE_SEPARATOR);
            else {
                strBuf.append(value);
                if (! value.endsWith("\n"))
                    strBuf.append(LINE_SEPARATOR);
            }
        }
        if (change.size() > 0) { // try to append new attributes
            iter = (change.keySet()).iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                if (key.length() < 15)
                    strBuf.append(padding[15 - key.length()]);
                strBuf.append(Character.toUpperCase(key.charAt(0)));
                strBuf.append(key.substring(1));
                strBuf.append(": ");
                if (value == null)
                    strBuf.append(LINE_SEPARATOR);
                else {
                    strBuf.append(value);
                    if (! value.endsWith("\n"))
                        strBuf.append(LINE_SEPARATOR);
                }
            }
        }
        return strBuf.toString();
    }

    public static String postable(Event event) {
        return postable(event, emptyMap);
    }

    /**
     * It returns a formatted string of the event in the postable format.
     * The postable format of an event is a uri query string ready to be
     * posted to a web server.  The Map of change should contain only
     * key-value pairs for add/replace/delete.
     *<br/>
     * value = null: delete
     *<br/>
     * key in event: replace
     *<br/>
     * no key in event: add
     *<br/><br/>
     * NB. change will be cleared at the return
     */
    public static String postable(Event event, Map change) {
        Object o;
        String key, value;
        HashMap attribute;
        Iterator iter;
        StringBuffer strBuf, content;
        boolean skip = false;
        if (event == null || (attribute = event.attribute) == null)
            return null;
        if (change == null)
            change = emptyMap;

        strBuf = new StringBuffer();
        strBuf.append("SITE=");
        if (attribute.get("site") != null)
            strBuf.append((String) attribute.get("site"));
        else
            strBuf.append(defaultSite);
        strBuf.append("&CATEGORY=");
        if (attribute.get("category") != null)
            strBuf.append((String) attribute.get("category"));
        else
            strBuf.append(defaultCategory);
        strBuf.append("&NAME=");
        if (attribute.get("name") != null)
            strBuf.append((String) attribute.get("name"));
        else
            strBuf.append(event.getDefaultName());
        strBuf.append("&type=");
        if (attribute.get("type") != null)
            strBuf.append((String) attribute.get("type"));
        else
            strBuf.append(defaultType);
        strBuf.append("&priority=");
        if ((o = attribute.get("priority")) != null)
            strBuf.append((String) attribute.get("priority"));
        else
            strBuf.append(Event.priorityNames[event.getPriority()]);
        strBuf.append("&hostname=");
        if (attribute.get("hostname") != null)
            strBuf.append((String) attribute.get("hostname"));
        else
            strBuf.append(Event.getHostName());
        strBuf.append("&program=");
        if (attribute.get("program") != null)
            strBuf.append((String) attribute.get("program"));
        else
            strBuf.append(Event.getProgramName());
        strBuf.append("&username=");
        if (attribute.get("owner") != null)
            strBuf.append((String) attribute.get("owner"));
        else
            strBuf.append(System.getProperty("user.name"));
        strBuf.append("&pid=");
        if (attribute.get("pid") != null)
            strBuf.append((String) attribute.get("pid"));
        else
            strBuf.append(String.valueOf(Event.getPID()));
        strBuf.append("&date=");
        strBuf.append(String.valueOf(event.timestamp));
        strBuf.append("&summary=");
        value = (String) attribute.get("text");
        if (value != null) {
            if (value.indexOf('&') >= 0)
                value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND,
                    value);
            if (value.indexOf('=') >= 0)
                value = Utils.doSearchReplace("=", Event.ESCAPED_EQUAL, value);
            if (value.indexOf("\"") >= 0)
                value = Utils.doSearchReplace("\"", Event.ESCAPED_QUOTE, value);
            if (value.indexOf('\r') >= 0)
                value = Utils.doSearchReplace("\r",
                    Event.ESCAPED_CARRIAGE_RETURN, value);
            if (value.indexOf('\n') >= 0)
                value = Utils.doSearchReplace("\n", "\r", value);
            strBuf.append(value);
        }
        strBuf.append("&delimiter=");
//        strBuf.append(URLEncoder.encode(Event.ITEM_SEPARATOR));
        strBuf.append(Event.ITEM_SEPARATOR);
        strBuf.append("&content=");
        int number = 0;
        content = new StringBuffer();
        iter = (attribute.keySet()).iterator();
        while (iter.hasNext()) {
            skip = false;
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            switch (key.charAt(0)) {
              case 'c':
                if (key.equals("category"))
                    skip = true;
                break;
              case 'd':
                if (key.equals("date"))
                    skip = true;
                break;
              case 'h':
                if (key.equals("hostname"))
                    skip = true;
                break;
              case 'n':
                if (key.equals("name"))
                    skip = true;
                break;
              case 'o':
                if (key.equals("owner"))
                    skip = true;
                break;
              case 'p':
                if (key.equals("priority") || key.equals("program")
                    || key.equals("pid"))
                    skip = true;
                break;
              case 's':
                if (key.equals("site"))
                    skip = true;
                break;
              case 't':
                if (key.equals("text") || key.equals("type"))
                    skip = true;
                break;
              default:
                skip = false;
                break;
            }
            if (skip) // skip
                continue;
            if (change.containsKey(key)) {
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                change.remove(key);
                if (value == null) // skip
                    continue;
            }
            else {
                o = attribute.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
            }
            if (number > 0)
                content.append(Event.ITEM_SEPARATOR);
            content.append(key);
            content.append(Event.ITEM_SEPARATOR);
            value = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                Event.ESCAPED_ITEM_SEPARATOR, value);
            content.append(value);
//            content.append(URLEncoder.encode(value));
            number ++;
        }
        if (change.size() > 0) { // try to append new attributes
            iter = (change.keySet()).iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                if (number > 0)
                    content.append(Event.ITEM_SEPARATOR);
                content.append(key);
                content.append(Event.ITEM_SEPARATOR);
                value = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                    Event.ESCAPED_ITEM_SEPARATOR, value);
                content.append(value);
//                content.append(URLEncoder.encode(value));
                number ++;
            }
        }
        value = content.toString();
        if (value.indexOf('&') >= 0)
            value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND, value);
        if (value.indexOf('=') >= 0)
            value = Utils.doSearchReplace("=", Event.ESCAPED_EQUAL, value);
        if (value.indexOf('\r') >= 0)
            value = Utils.doSearchReplace("\r", Event.ESCAPED_CARRIAGE_RETURN,
                value);
        number = strBuf.length();
        strBuf.append(value);
        for (int i=strBuf.length()-1; i>=number; i--) {
            if (strBuf.charAt(i) == '\n')
                strBuf.setCharAt(i, '\r');
        }
        return strBuf.toString();
    }

    public static String collectible(Event event) {
        return collectible(event, emptyMap);
    }

    /**
     * It returns a formatted string of the event in the collectible format.
     * The collectible format of an event is a log string ready to be parsed
     * into an event by EventParser.  But it does not contain the trailing
     * newline chararcter and the leading timestamp as well as the IP address.
     * The change should contain only key-value pairs for add/replace/delete
     *<br/>
     * value = null: delete
     *<br/>
     * key in event: replace
     *<br/>
     * no key in event: add
     *<br/><br/>
     * NB. change will be cleared at the return
     */
    public static String collectible(Event event, Map change) {
        Object o;
        String key, value;
        HashMap attribute;
        Iterator iter;
        StringBuffer strBuf, content;
        boolean skip = false;
        if (event == null || (attribute = event.attribute) == null)
            return null;
        if (change == null)
            change = emptyMap;

        strBuf = new StringBuffer();
        if (attribute.get("site") != null)
            strBuf.append((String) attribute.get("site"));
        else
            strBuf.append(defaultSite);
        strBuf.append(" ");
        if (attribute.get("category") != null)
            strBuf.append((String) attribute.get("category"));
        else
            strBuf.append(defaultCategory);
        strBuf.append(" ");
        if (attribute.get("name") != null)
            strBuf.append((String) attribute.get("name"));
        else
            strBuf.append(event.getDefaultName());
        strBuf.append(" ");
        strBuf.append(String.valueOf(event.timestamp));
        strBuf.append(" ");
        if ((o = attribute.get("priority")) != null)
            strBuf.append((String) attribute.get("priority"));
        else
            strBuf.append(Event.priorityNames[event.getPriority()]);
        strBuf.append(" ");
        if (attribute.get("hostname") != null)
            strBuf.append((String) attribute.get("hostname"));
        else
            strBuf.append(Event.getHostName());
        strBuf.append(" ");
        if (attribute.get("program") != null)
            strBuf.append((String) attribute.get("program"));
        else
            strBuf.append(Event.getProgramName());
        strBuf.append(" [");
        if (attribute.get("pid") != null)
            strBuf.append((String) attribute.get("pid"));
        else
            strBuf.append(String.valueOf(Event.getPID()));
        strBuf.append(":1] ");
        if (attribute.get("owner") != null)
            strBuf.append((String) attribute.get("owner"));
        else
            strBuf.append(System.getProperty("user.name"));
        strBuf.append(" ");
        if (attribute.get("type") != null)
            strBuf.append((String) attribute.get("type"));
        else
            strBuf.append(defaultType);
        strBuf.append(" \"");
        value = (String) attribute.get("text");
        if (value != null) {
            if (value.indexOf('&') >= 0)
                value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND,
                    value);
            if (value.indexOf('=') >= 0)
                value = Utils.doSearchReplace("=", Event.ESCAPED_EQUAL, value);
            if (value.indexOf("\"") >= 0)
                value = Utils.doSearchReplace("\"", Event.ESCAPED_QUOTE, value);
            if (value.indexOf('\r') >= 0)
                value = Utils.doSearchReplace("\r",
                    Event.ESCAPED_CARRIAGE_RETURN,value);
            if (value.indexOf('\n') >= 0)
                value = Utils.doSearchReplace("\n", "\r", value);
            strBuf.append(value);
        }
        strBuf.append("\" Content(");
//        strBuf.append(URLEncoder.encode(Event.ITEM_SEPARATOR));
        strBuf.append(Event.ITEM_SEPARATOR);
        strBuf.append("): ");
        int number = 0;
        content = new StringBuffer();
        iter = (attribute.keySet()).iterator();
        while (iter.hasNext()) {
            skip = false;
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            switch (key.charAt(0)) {
              case 'c':
                if (key.equals("category"))
                    skip = true;
                break;
              case 'd':
                if (key.equals("date"))
                    skip = true;
                break;
              case 'h':
                if (key.equals("hostname"))
                    skip = true;
                break;
              case 'n':
                if (key.equals("name"))
                    skip = true;
                break;
              case 'o':
                if (key.equals("owner"))
                    skip = true;
                break;
              case 'p':
                if (key.equals("priority") || key.equals("program")
                    || key.equals("pid"))
                    skip = true;
                break;
              case 's':
                if (key.equals("site"))
                    skip = true;
                break;
              case 't':
                if (key.equals("text") || key.equals("type"))
                    skip = true;
                break;
              default:
                skip = false;
                break;
            }
            if (skip) // skip
                continue;
            if (change.containsKey(key)) {
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                change.remove(key);
                if (value == null) // skip
                    continue;
            }
            else {
                o = attribute.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
            }
            if (number > 0)
                content.append(Event.ITEM_SEPARATOR);
            content.append(key);
            content.append(Event.ITEM_SEPARATOR);
            value = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                Event.ESCAPED_ITEM_SEPARATOR, value);
            content.append(value);
//            content.append(URLEncoder.encode(value));
            number ++;
        }
        if (change.size() > 0) { // try to append new attributes
            iter = (change.keySet()).iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = change.get(key);
                if (o != null && !(o instanceof String))
                    value = o.toString();
                else
                    value = (String) o;
                if (number > 0)
                    content.append(Event.ITEM_SEPARATOR);
                content.append(key);
                content.append(Event.ITEM_SEPARATOR);
                value = Utils.doSearchReplace(Event.ITEM_SEPARATOR,
                    Event.ESCAPED_ITEM_SEPARATOR, value);
                content.append(value);
//                content.append(URLEncoder.encode(value));
                number ++;
            }
        }
        value = content.toString();
        if (value.indexOf('&') >= 0)
            value = Utils.doSearchReplace("&", Event.ESCAPED_AMPERSAND, value);
        if (value.indexOf('=') >= 0)
            value = Utils.doSearchReplace("=", Event.ESCAPED_EQUAL, value);
        if (value.indexOf('\r') >= 0)
            value = Utils.doSearchReplace("\r", Event.ESCAPED_CARRIAGE_RETURN,
                value);
        number = strBuf.length();
        strBuf.append(value);
        for (int i=strBuf.length()-1; i>=number; i--) {
            if (strBuf.charAt(i) == '\n')
                strBuf.setCharAt(i, '\r');
        }
        return strBuf.toString();
    }

    /** returns an array of TextSubstitution for a list of maps for Sub */
    public static TextSubstitution[] initSubstitutions(List sub) {
        TextSubstitution[] msgSub = null;
        if (sub != null && sub.size() > 0) {
            int i;
            String key, value;
            Iterator iter;
            Object o;
            msgSub = new TextSubstitution[sub.size()];
            for (i=0; i<msgSub.length; i++) {
                o = sub.get(i);
                if (o instanceof Map) {
                    iter = ((Map) o).keySet().iterator();
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    value = (String) ((Map) o).get(key);
                    msgSub[i] = new TextSubstitution(key, value);
                }
                else
                    msgSub[i] = null;
            }
        }

        return msgSub;
    }

    /**
     * returns a Map contains changes only from the substitution
     */
    public static Map<String, Object> getChange(Event event,
        TextSubstitution[] sub, Perl5Matcher pm) {
        int i, n;
        String key, value;
        Map<String, Object> change = new HashMap<String, Object>();
        if (event == null)
            return null;
        if (sub == null || (n = sub.length) == 0)
            return change;

        HashMap attr = event.attribute;
        n = sub.length;
        for (i=0; i<n; i++) {
            if (sub[i] == null || (key = sub[i].getName()) == null ||
                key.length() <= 0 || !attr.containsKey(key))
                continue;
            if (change.containsKey(key))
                value = (String) change.get(key);
            else
                value = (String) attr.get(key);
            if (pm == null)
                value = sub[i].substitute(value);
            else
                value = sub[i].substitute(pm, value);
            change.put(key, value);
        }

        return change;
    }

    public static Map getChange(Event event, TextSubstitution[] sub) {
        return getChange(event, sub, null);
    }

    public static Event duplicate(Event event) {
        if (event == null)
            return null;

        Event ev = new Event(event.getPriority());
        ev.eventID = event.eventID;
        ev.groupID = event.groupID;
        ev.logMode = event.logMode;
        ev.deliveryMode = event.deliveryMode;
        ev.timestamp = event.timestamp;
        ev.expiration = event.expiration;
        ev.body = event.body;
        HashMap attr = event.attribute;
        Iterator iter = (attr.keySet()).iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            ev.attribute.put(key, attr.get(key));
        }
        return ev;
    }

    /**
     * returns a formatted string of the event with a given template
     */
    public static String format(Event event, Template template) {
        String value, field, text;
        String[] textFields;
        HashMap attr;
        int i;
        if (event == null || (attr = event.attribute) == null)
            return null;

        if (template == null)
            return (String) attr.get("text");

        text = template.copyText();
        textFields = template.getAllFields();
        for (i=0; i<textFields.length; i++) {
            field = textFields[i];
            value = (String) attr.get(field);
            if (value == null)
                value = "";
            text = template.substitute(field, value, text);
        }
        return text;
    }

    /**
     * returns a formatted string of the event with a given template
     * and a set of substitutions
     */
    public static String format(Event event, Template template, Map subMap) {
        String value, field, text;
        String[] textFields;
        HashMap attr;
        TextSubstitution[] sub;
        Object o;
        int i;

        if (subMap == null || subMap.size() <= 0)
            return format(event, template);

        if (event == null || (attr = event.attribute) == null)
            return null;

        if (template == null)
            return (String) attr.get("text");

        text = template.copyText();
        textFields = template.getAllFields();
        for (i=0; i<textFields.length; i++) {
            field = textFields[i];
            value = (String) attr.get(field);
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
     * runs a script within the timeout and return an event.
     * Caller is supposed to add name, site and other fields
     * to the event and sends it
     */
    public static Event runScript(String program, int timeout) {
        Event event;
        try {
            String output = RunCommand.exec(program, timeout);
            event = new Event(Event.INFO, "Script: '" +
                program + "' completed");
            event.setAttribute("output", output);
        }
        catch (TimeoutException e) {
            event = new Event(Event.WARNING, "Script: '" +
                program + "' timed out");
            event.setAttribute("error", e.getMessage());
        }
        catch (RuntimeException e) {
            event = new Event(Event.ERR, "Script: '" +
                program + "' failed");
            event.setAttribute("error", e.getMessage());
        }
        return event;
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
}
