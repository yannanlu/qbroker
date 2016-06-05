package org.qbroker.event;

/* EventSelector.java - an EventFilter for selecting events */

import java.util.Map;
import java.util.List;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.event.Event;
import org.qbroker.event.EventFilter;
import org.qbroker.event.EventPattern;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;

/**
 * EventSelector implements EventFilter for Perl5 patterns match with events
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventSelector implements EventFilter {
    private EventPattern[] eventPattern;
    private EventPattern[] xEventPattern;
    private int eventExpiration;

    public EventSelector(Map props) {
        Object o;
        List list;
        int i, n;
        if ((o = props.get("EventPattern")) != null && o instanceof List) {
            list = (List) o;
            n = list.size();
            eventPattern = new EventPattern[n];
            for (i=0; i<n; i++) {
                o = list.get(i);
                try {
                    eventPattern[i] = new EventPattern((Map) o);
                }
                catch (MalformedPatternException e) {
                    throw(new IllegalArgumentException(e.toString()));
                }
            }
        }
        if (eventPattern == null || eventPattern.length == 0)
            throw(new IllegalArgumentException("no EventPattern defined"));

        if ((o = props.get("XEventPattern")) != null && o instanceof List) {
            list = (List) o;
            n = list.size();
            xEventPattern = new EventPattern[n];
            for (i=0; i<n; i++) {
                o = list.get(i);
                try {
                    xEventPattern[i] = new EventPattern((Map) o);
                }
                catch (MalformedPatternException e) {
                    throw(new IllegalArgumentException(e.toString()));
                }
            }
        }
        if (xEventPattern == null)
            xEventPattern = new EventPattern[0];

        if ((o = props.get("EventExpiration")) != null &&
            (eventExpiration = 1000 * Integer.parseInt((String) o)) < 0)
            eventExpiration = 0;
    }

    public boolean evaluate(long currentTime, Event event) {
        int i;

        if (eventExpiration>0 && currentTime-event.timestamp >= eventExpiration)
            return false;

        for (i=0; i<eventPattern.length; i++) {
            if (eventPattern[i].evaluate(currentTime, event))
                break;
        }

        if (i >= eventPattern.length)
            return false;

        for (i=0; i<xEventPattern.length; i++) {
            if (xEventPattern[i].evaluate(currentTime, event))
                return false;
        }
        return true;
    }

    /**
     * It returns a Perl 5 pattern string on a specific key so that the
     * returned pattern string is the result of OR on all the neccessary
     * patterns in the pattern list.  The result string is the coarse-grained
     * of all patterns on the key.  If it does not match a text, the original
     * list of patterns will not match either.  So the coarse-grained pattern
     * can be used as a pre-filter.
     */
    public static String coarseGrain(String key, List list) {
        String p, str, def, patternStr;
        Object o;
        Map h;
        int i, l, k, n;
        if (key == null || key.length() <= 0)
            return null;
        def = "[^ ]+";

        patternStr = "^" + def + "$";
        if (list == null && list.size() <= 0)
            return patternStr;

        n = list.size();
        p = "";
        k = 0;
        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            h = (Map) o;
            if ((o = h.get(key)) != null) {
                str = (String) o;
            }
            else {
                str = patternStr;
            }
            if (patternStr.equals(str)) {
                k = 0;
                break;
            }
            str = expand(str, def);
            l = str.length();
            if (k > 0)
                p += "|";
            k ++;
            if (str.charAt(0) == '^' && str.charAt(l-1) == '$') { // normal
                if (str.charAt(1) == '(') {
                    p += str.substring(2, l-2);
                    k ++;
                }
                else
                    p += str.substring(1, l-1);
            }
        }
        if (i >= n && k > 0)
            patternStr = (k > 1) ? "^(" + p + ")$" : "^" + p + "$";

        return patternStr;
    }

    /**
     * With given a default Perl 5 pattern string, it expands the pattern 
     * so that the result pattern can be ORed with other patterns.  The
     * expanded pattern may not be same as the original one.  But in order
     * to match the original pattern, it is neccessary to match the expanded
     * pattern.  If it does not match the expanded pattern, it will not
     * match the original pattern.
     */
    public static String expand(String pattern, String def) {
        int i, j, k, l, m, n;
        Template template;
        String[] keys;
        String str = null, padding;
        if (def == null || (m = def.length()) <= 0)
            return pattern;
        if (pattern == null || (l = pattern.length()) <= 0)
            return "^" + def + "$";
        padding = def.substring(0, m-1) + "*";
        template = new Template(pattern, "\\([^\\(\\)]+\\)");
        keys = template.getAllFields();
        n = keys.length;
        k = 0;
        for (i=0; i<n; i++) { // look for keys without OR
            if (keys[i].indexOf("|") < 0) { // get rid of parentheses
                pattern = template.substitute(keys[i], keys[i], pattern);
                k ++;
            }
        }
        if (k > 0) { // update length and template
            l = pattern.length();
            template = new Template(pattern, "\\([^\\(\\)]+\\)");
        }

        keys = template.getSequence();
        n = keys.length;
        if (n > 1) { // truncate pattern if more than one pair of parentheses
            i = pattern.indexOf(")");
            j = pattern.indexOf("(", i+1);
            pattern = pattern.substring(0, j);
            l = pattern.length();
        }
        if (pattern.charAt(0) == '^' && pattern.charAt(l-1) == '$') { // exact
            k = 3;
            if (n == 0) // without OR
                str = pattern.substring(1, l-1);
            else { // only one pair of parentheses
                i = pattern.indexOf("(");
                j = pattern.lastIndexOf(")");
                if (i == 1 && j == l - 2) { // whole
                    str = pattern.substring(1, l-1);
                }
                else if (i == 1) { // at the beginning
                    str = Utils.doSearchReplace("|", pattern.substring(j+1,l-1)+
                        "|", pattern.substring(i+1, j));
                    str = "(" + str + pattern.substring(j+1,l-1) + ")";
                }
                else if (j == l - 2) { // at the end
                    str = Utils.doSearchReplace("|", "|"+
                        pattern.substring(1, i), pattern.substring(i+1,j));
                    str = "(" + pattern.substring(1, i) + str + ")";
                }
                else { // in the middle
                    str = Utils.doSearchReplace("|", pattern.substring(j+1,l-1)+
                        "|"+pattern.substring(1, i), pattern.substring(i+1, j));
                    str = "(" + pattern.substring(1, i) + str +
                        pattern.substring(j+1, l-1) + ")";
                }
            }
        }
        else if (pattern.charAt(0) == '^') { // match from beginning
            k = 1;
            if (n == 0) // without OR
                str = pattern.substring(1) + padding;
            else { // only one pair of parentheses
                i = pattern.indexOf("(");
                j = pattern.lastIndexOf(")");
                if (i == 1 && j == l - 1) { // whole
                    str = Utils.doSearchReplace("|", padding + "|",
                        pattern.substring(i+1, j));
                    str = "(" + str + padding + ")";
                }
                else if (i == 1) { // at the beginning
                    str = Utils.doSearchReplace("|", pattern.substring(j+1) +
                        padding + "|", pattern.substring(i+1, j));
                    str = "(" + str + pattern.substring(j+1) + padding+ ")";
                }
                else if (j == l - 1) { // at the end
                    str = Utils.doSearchReplace("|", padding + "|" +
                        pattern.substring(1, i), pattern.substring(i+1,j));
                    str = "(" + pattern.substring(1, i) + str + padding + ")";
                }
                else { // in the middle
                    str = Utils.doSearchReplace("|", pattern.substring(j+1) +
                        padding + "|" + pattern.substring(1, i),
                        pattern.substring(i+1, j));
                    str = "(" + pattern.substring(1, i) + str +
                        pattern.substring(j+1) + padding + ")";
                }
            }
        }
        else if (pattern.charAt(l-1) == '$') { // match up to end
            k = 2;
            if (n == 0) // without OR
                str = padding + pattern.substring(0, l-1);
            else { // only one pair of parentheses
                i = pattern.indexOf("(");
                j = pattern.lastIndexOf(")");
                if (i == 0 && j == l - 2) { // whole
                    str = Utils.doSearchReplace("|", "|" + padding,
                        pattern.substring(i+1, j));
                    str = "(" + padding + str + ")";
                }
                else if (i == 0) { // at the beginning
                    str = Utils.doSearchReplace("|", pattern.substring(j+1,l-1)+
                        "|" + padding, pattern.substring(i+1, j));
                    str = "(" + padding + str + pattern.substring(j+1,l-1)+ ")";
                }
                else if (j == l - 2) { // at the end
                    str = Utils.doSearchReplace("|", "|" + padding +
                        pattern.substring(0, i), pattern.substring(i+1,j));
                    str = "(" + padding + pattern.substring(0, i) + str + ")";
                }
                else { // in the middle
                    str = Utils.doSearchReplace("|", pattern.substring(j+1,l-1)+
                        "|" + padding + pattern.substring(0, i),
                        pattern.substring(i+1, j));
                    str = "(" + padding + pattern.substring(0, i) + str +
                        pattern.substring(j+1, l-1) + ")";
                }
            }
        }
        else { // match in middle
            k = 0;
            if (n == 0) // without OR
                str = padding + pattern + padding;
            else { // only one pair of parentheses
                i = pattern.indexOf("(");
                j = pattern.lastIndexOf(")");
                if (i == 0 && j == l - 1) { // whole
                    str = Utils.doSearchReplace("|", padding+"|"+padding,
                        pattern.substring(i+1, j));
                    str = "(" + padding + str + padding + ")";
                }
                else if (i == 0) { // at the beginning
                    str = Utils.doSearchReplace("|", pattern.substring(j+1)+
                        padding+"|"+padding, pattern.substring(i+1, j));
                    str = "(" + padding + str + pattern.substring(j+1) +
                        padding + ")";
                }
                else if (j == l - 1) { // at the end
                    str = Utils.doSearchReplace("|", padding+"|"+padding+
                        pattern.substring(0, i), pattern.substring(i+1,j));
                    str = "(" + padding + pattern.substring(0, i) + str +
                        padding + ")";
                }
                else { // in the middle
                    str = Utils.doSearchReplace("|", pattern.substring(j+1)+
                        padding + "|" + padding + pattern.substring(0, i),
                        pattern.substring(i+1, j));
                    str = "(" + padding + pattern.substring(0, i) + str +
                        pattern.substring(j+1) + padding + ")";
                }
            }
        }
        return "^" + str + "$";
    }
}
