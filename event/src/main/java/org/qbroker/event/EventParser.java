package org.qbroker.event;

import java.util.Date;
import java.util.HashMap;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;

/**
 * EventParser parses a text into an event according to the given pattern. If
 * the pattern is null or an empty string, the text will be assumed as an event
 * collectible. Otherwise, the text is supposed to contain an event postable
 * and the pattern has to have the only one group member for the postable.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventParser {
    private boolean isPostable = false;
    private Pattern pattern;
    private Perl5Matcher pm;

    public EventParser(String patternString) {
        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            if (patternString == null || patternString.length() <= 0) {
                // default parser for collectible
                StringBuffer defaultPS = new StringBuffer();
                defaultPS.append("^\\d\\d\\d\\d-\\d+-\\d+ ");
                defaultPS.append("\\d+:\\d+:\\d+,\\d\\d\\d [-+:/a-zA-Z_0-9]+");
                defaultPS.append(" \\d+\\.\\d+\\.\\d+\\.\\d+ ([^ ]+)");
                defaultPS.append(" ([^ ]+) ([^ ]+) (\\d+) ([^ ]+) ([^ ]+)");
                defaultPS.append(" ([^ ]+) \\[(-?\\d+):([^]]+)\\] ([^ ]+)");
                defaultPS.append(" ([^ ]+) \\\"([^\"]*)");
                defaultPS.append("\\\" Content\\(([^\\(]+)\\): ?(.*)$");
                pattern = pc.compile(defaultPS.toString());
            }
            else { // for postable
                pattern = pc.compile(patternString);
                isPostable = true;
            }
        }
        catch(Exception e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    public Event parse(String buffer) {
        if (isPostable)
            return parsePostable(buffer, pm, pattern);
        else
            return parseCollectible(buffer, pm, pattern);
    }

    @SuppressWarnings("unchecked")
    private static Event parseCollectible(String buffer, Perl5Matcher pm,
        Pattern pattern) {
        String text;
        HashMap attr;
        int i, priority;
        Event event = null;
        if (pm.contains(buffer, pattern)) {
            MatchResult mr = pm.getMatch();
            priority = Event.getPriorityByName(mr.group(5));

            if (priority < 0)
                priority = Event.INFO;
            text = mr.group(12);
            if (text == null)
                text = "";
            if (text.indexOf(Event.ESCAPED_QUOTE) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_QUOTE, "\"", text);
            if (text.indexOf(Event.ESCAPED_AMPERSAND) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_AMPERSAND, "&",text);
            if (text.indexOf(Event.ESCAPED_EQUAL) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_EQUAL, "=", text);
            if (text.indexOf("\r") >= 0)
                text = Utils.doSearchReplace("\r", "\n", text);
            if (text.indexOf(Event.ESCAPED_CARRIAGE_RETURN) >= 0)
                text = Utils.doSearchReplace(Event.ESCAPED_CARRIAGE_RETURN,
                    "\r", text);
            event = new Event(priority, text);
            event.logMode = Event.LOG_COMPACT;
            attr = event.attribute;
            attr.put("site", mr.group(1));
            attr.put("category", mr.group(2));
            attr.put("name", mr.group(3));
            event.timestamp = Long.parseLong(mr.group(4));
            attr.put("date", Event.dateFormat(new Date(event.timestamp)));
            attr.put("hostname", mr.group(6));
            attr.put("program", mr.group(7));
            attr.put("pid", mr.group(8));
            attr.put("owner", mr.group(10));
            attr.put("type", mr.group(11));
            text = mr.group(14);
            if (text != null && text.length() > 1) {
                text = Utils.doSearchReplace(Event.ESCAPED_AMPERSAND, "&",text);
                text = Utils.doSearchReplace(Event.ESCAPED_EQUAL, "=", text);
                text = Utils.doSearchReplace("\r", "\n", text);
                text = Utils.doSearchReplace(Event.ESCAPED_CARRIAGE_RETURN,
                    "\r", text);
                i = Utils.split(Event.ITEM_SEPARATOR,
                    Event.ESCAPED_ITEM_SEPARATOR, text, attr);
            }
        }

        return event;
    }

    @SuppressWarnings("unchecked")
    private static Event parsePostable(String buffer, Perl5Matcher pm,
        Pattern pattern) {
        String text, value;
        HashMap attr;
        int i, priority;
        Event event = null;
        if (pm.contains(buffer, pattern)) {
            MatchResult mr = pm.getMatch();
            text = mr.group(1);
            event = new Event(Event.INFO);
            event.logMode = Event.LOG_COMPACT;
            attr = event.attribute;
            for (String key : Utils.split("&", text)) {
                if ((i = key.indexOf("=")) <= 0)
                    continue;
                value = key.substring(i+1);
                key = key.substring(0, i).trim();
                if (key.length() <= 0)
                    continue;
                switch (key.charAt(0)) {
                  case 'S':
                    if ("SITE".equals(key) && !attr.containsKey("site"))
                        attr.put("site", value);
                    break;
                  case 'C':
                    if ("CATEGORY".equals(key) && !attr.containsKey("category"))
                        attr.put("category", value);
                    break;
                  case 'N':
                    if ("NAME".equals(key) && !attr.containsKey("name"))
                        attr.put("name", value);
                    break;
                  case 'p':
                    if ("priority".equals(key)) {
                        priority = Event.getPriorityByName(value);
                        if (priority >= 0)
                            event.setPriority(priority);
                    }
                    else if ("program".equals(key) && !attr.containsKey(key))
                        attr.put(key, value);
                    else if ("pid".equals(key) && !attr.containsKey(key))
                        attr.put(key, value);
                    break;
                  case 'd':
                    if ("date".equals(key)) {
                        event.timestamp = Long.parseLong(value);
                        attr.put("date",
                            Event.dateFormat(new Date(event.timestamp)));
                    }
                    break;
                  case 'u':
                    if ("username".equals(key) && !attr.containsKey("owner"))
                        attr.put("owner", value);
                    break;
                  case 'h':
                    if ("hostname".equals(key) && !attr.containsKey(key))
                        attr.put(key, value);
                    break;
                  case 't':
                    if ("type".equals(key) && !attr.containsKey(key))
                        attr.put(key, value);
                    else if ("tid".equals(key) && !attr.containsKey(key))
                        attr.put(key, value);
                    break;
                  case 's':
                    if ("summary".equals(key) && !attr.containsKey("text")) {
                        if (value.indexOf(Event.ESCAPED_QUOTE) >= 0)
                            value = Utils.doSearchReplace(Event.ESCAPED_QUOTE,
                                "\"", value);
                        if (value.indexOf(Event.ESCAPED_AMPERSAND) >= 0)
                            value=Utils.doSearchReplace(Event.ESCAPED_AMPERSAND,
                                "&", value);
                        if (value.indexOf(Event.ESCAPED_EQUAL) >= 0)
                            value = Utils.doSearchReplace(Event.ESCAPED_EQUAL,
                                "=", value);
                        if (value.indexOf("\r") >= 0)
                            value = Utils.doSearchReplace("\r", "\n", value);
                        if (value.indexOf(Event.ESCAPED_CARRIAGE_RETURN) >= 0)
                            value = Utils.doSearchReplace(
                                Event.ESCAPED_CARRIAGE_RETURN, "\r", value);
                        attr.put("text", value);
                    }
                    break;
                  case 'c':
                    if ("content".equals(key) && value.length() > 1) {
                        if (value.indexOf(Event.ESCAPED_AMPERSAND) >= 0)
                            value=Utils.doSearchReplace(Event.ESCAPED_AMPERSAND,
                                "&", value);
                        if (value.indexOf(Event.ESCAPED_EQUAL) >= 0)
                            value = Utils.doSearchReplace(Event.ESCAPED_EQUAL,
                                "=", value);
                        if (value.indexOf("\r") >= 0)
                            value = Utils.doSearchReplace("\r", "\n", value);
                        if (value.indexOf(Event.ESCAPED_CARRIAGE_RETURN) >= 0)
                            value = Utils.doSearchReplace(
                                Event.ESCAPED_CARRIAGE_RETURN, "\r", value);
                        i = Utils.split(Event.ITEM_SEPARATOR,
                            Event.ESCAPED_ITEM_SEPARATOR, value, attr);
                    }
                    break;
                  default:
                }
            }
        }

        return event;
    }
}
