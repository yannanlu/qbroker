package org.qbroker.json;

/* JSONFilter.java - a client side filter to evaluate parsed JSON data */

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.StringReader;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.DataSet;
import org.qbroker.common.Filter;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.AssetList;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONSection;
import org.qbroker.event.Event;

/**
 * JSONFilter is a client side filter with an internal formatter. The method
 * of evaluate() returns true if the filter matches the JSON data. Otherwise,
 * it returns false. The method of format is to format the JSON data with the
 * internal formatter.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSONFilter implements Filter<Map> {
    private String prefix;
    private String[] keys = null;
    private Map[] aJSONProps = null, xJSONProps = null;
    private Template temp = null;
    private TextSubstitution tsub = null;
    private Perl5Matcher pm = null;
    private int count = 0;
    private boolean hasFormatter, hasSection;

    public JSONFilter(String prefix, Map props, Perl5Matcher pm,
        Perl5Compiler pc) {
        Object o;
        TextSubstitution sub;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a fitler"));

        if (prefix == null)
            this.prefix = "";
        else
            this.prefix = prefix;
        if (pc == null)
            pc = new Perl5Compiler();
        if (pm == null)
            this.pm = new Perl5Matcher();
        else
            this.pm = pm;

        // for path-pattern group
        if(props.containsKey("patterns") || props.containsKey("xpatterns")) try{
            aJSONProps = (Map[])
                getPatternMaps("patterns", props, pc);
            xJSONProps = (Map[])
                getPatternMaps("xpatterns", props, pc);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(prefix +
                " has a bad pattern in patterns: " + e.toString()));
        }

        // for getting rid of double quotes on all variables
        sub = new TextSubstitution("s/\"(##[^#]+##)\"/$1/g");

        hasFormatter = false;
        hasSection = false;
        if ((o = props.get("template")) != null) {
            String str;
            if (o instanceof String)
                str = (String) o;
            else if (o instanceof Map)
                str = sub.substitute(pm,JSON2FmModel.toJSON((Map) o,null,null));
            else if (o instanceof List)
                str = sub.substitute(pm,JSON2FmModel.toJSON((List)o,null,null));
            else
                str = o.toString();

            temp = new Template(str);

            if ((o = props.get("substitution")) != null && o instanceof String)
                tsub = new TextSubstitution((String) o);
            hasFormatter = true;
            keys = temp.getAllFields();
            count = temp.numberOfFields();
            for (int i=0; i<count; i++) {
                if (keys[i].matches("^s:.+")) {
                    hasSection = true;
                    break;
                }
            }
        }
    }

    /**
     * formats the message with the predefined formatter and returns number of
     * fields modified upon success.
     */
    public String format(Map json, AssetList list, Map params) {
        String text, key, value;
        Object o;
        if ((!hasFormatter) || json == null)
            return null;
        if (params == null)
            params = new HashMap();

        text = temp.copyText();
        if (!hasSection || list == null) { // no sections or list is null
            for (int i=0; i<count; i++) {
                key = keys[i];
                if (key.matches("^p:.+")) { // for a parameter
                    key = key.substring(2);
                    o = params.get(key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else
                        value = o.toString();
                    text = temp.substitute(pm, keys[i], value, text);
                }
                else { // for a json path
                    o = JSON2FmModel.get(json, key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else if (o instanceof Map)
                        value = JSON2FmModel.toJSON((Map) o, null, null);
                    else if (o instanceof List)
                        value = JSON2FmModel.toJSON((List) o, null, null);
                    else
                        value = o.toString();
                    text = temp.substitute(pm, key, value, text);
                }
            }
        }
        else { // with section list
            JSONSection sect;
            for (int i=0; i<count; i++) {
                key = keys[i];
                if (key.matches("^p:.+")) { // for a parameter
                    key = key.substring(2);
                    o = params.get(key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else
                        value = o.toString();
                    text = temp.substitute(pm, keys[i], value, text);
                    continue;
                }
                else if (!key.matches("^s:.+")) { // for a json path
                    o = JSON2FmModel.get(json, key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else if (o instanceof Map)
                        value = JSON2FmModel.toJSON((Map) o, null, null);
                    else if (o instanceof List)
                        value = JSON2FmModel.toJSON((List) o, null, null);
                    else
                        value = o.toString();
                    text = temp.substitute(pm, key, value, text);
                    continue;
                }
                // for a section
                key = key.substring(2);
                sect = (JSONSection) list.get(key);
                if (sect == null) {
                    throw(new RuntimeException(prefix +
                        " failed to get section for " + key));
                }
                else try {
                    value = sect.fulfill(json, list, params);
                }
                catch (Exception e) {
                    throw(new RuntimeException(prefix + ": " + sect.getName() +
                        " failed to fulfill the section of " + key + ": " +
                        Event.traceStack(e)));
                }
                text = temp.substitute(pm, keys[i], value, text);
            }
        }
        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    public String format(List json, AssetList list, Map params) {
        String text, key, value;
        Object o;
        if ((!hasFormatter) || json == null)
            return null;
        if (params == null)
            params = new HashMap();

        text = temp.copyText();
        if (!hasSection || list == null) { // no sections or list is null
            for (int i=0; i<count; i++) {
                key = keys[i];
                if (key.matches("^p:.+")) { // for a parameter
                    key = key.substring(2);
                    o = params.get(key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else
                        value = o.toString();
                    text = temp.substitute(pm, keys[i], value, text);
                }
                else { // for a json path
                    o = JSON2FmModel.get(json, key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else if (o instanceof Map)
                        value = JSON2FmModel.toJSON((Map) o, null, null);
                    else if (o instanceof List)
                        value = JSON2FmModel.toJSON((List) o, null, null);
                    else
                        value = o.toString();
                    text = temp.substitute(pm, key, value, text);
                }
            }
        }
        else if (list != null) { // with section list
            JSONSection sect;
            for (int i=0; i<count; i++) {
                key = keys[i];
                if (key.matches("^p:.+")) { // for a parameter
                    key = key.substring(2);
                    o = params.get(key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else
                        value = o.toString();
                    text = temp.substitute(pm, keys[i], value, text);
                    continue;
                }
                else if (!key.matches("^s:.+")) { // for a json path
                    o = JSON2FmModel.get(json, key);
                    if (o == null)
                        value = "null";
                    else if (o instanceof String)
                        value = "\"" + (String) o + "\"";
                    else if (o instanceof Map)
                        value = JSON2FmModel.toJSON((Map) o, null, null);
                    else if (o instanceof List)
                        value = JSON2FmModel.toJSON((List) o, null, null);
                    else
                        value = o.toString();
                    text = temp.substitute(pm, key, value, text);
                    continue;
                }
                // for a section
                key = key.substring(2);
                sect = (JSONSection) list.get(key);
                if (sect == null) {
                    throw(new RuntimeException(prefix +
                        " failed to get section for " + key));
                }
                else try {
                    value = sect.fulfill(json, list, params);
                }
                catch (Exception e) {
                    throw(new RuntimeException(prefix + ": " + sect.getName() +
                        " failed to fulfill the section of " + key + ": " +
                        Event.traceStack(e)));
                }
                text = temp.substitute(pm, keys[i], value, text);
            }
        }

        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    public String format(String json) {
        String text;
        if ((!hasFormatter) || json == null)
            return null;

        text = temp.copyText();
        if (count > 0) try {
            text = temp.substitute(pm, keys[0], json, text);
        }
        catch (Exception e) {
            throw(new RuntimeException(prefix + ": failed to format json: " +
                Event.traceStack(e)));
        }
        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    public boolean evaluate(long tm, Map json) {
        return evaluate(json);
    }

    /**
     * It applies the Perl5 pattern match on the message and returns true
     * if the filter gets a hit or false otherwise. In case there are patterns
     * for message body, it requires the content of the message body ready.
     */
    public boolean evaluate(Map json) {
        if (json == null)
            return false;
        else if (evaluate(json, aJSONProps, pm, true) &&
            !evaluate(json, xJSONProps, pm, false))
            return true;
        else
            return false;
    }

    public boolean evaluate(List json) {
        if (json == null)
            return false;
        else if (evaluate(json, aJSONProps, pm, true) &&
            !evaluate(json, xJSONProps, pm, false))
            return true;
        else
            return false;
    }

    public boolean evaluate(String json) {
        if (json == null)
            return false;
        else if (evaluate(json, aJSONProps, pm, true) &&
            !evaluate(json, xJSONProps, pm, false))
            return true;
        else
            return false;
    }

    /**
     * returns true if any one of the pattern maps matches on the parsed
     * JSON data. A pattern map represents all the patterns in a Map where
     * each key is a json path.  In case the key of a pattern is pointing
     * a container, the size of the container will be evaluated. The default
     * boolean value is used to evaluate an empty pattern map.
     */
    public static boolean evaluate(Map json, Map[] props, Perl5Matcher pm,
        boolean def) {
        Map h;
        Iterator iter;
        Pattern p;
        DataSet d;
        Object o;
        String key, value;
        String[] keys;
        boolean status = def;
        int i, k, n;

        if (json == null || props == null)
            return (! status);

        n= props.length;
        if (n <= 0)
            return status;

        for (i=0; i<n; i++) {
            h = props[i];
            if (h == null)
                continue;
            k = h.size();
            keys = new String[k];
            iter = h.keySet().iterator();
            k = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                keys[k++] = key;
            }
            if (k > 1)
                Arrays.sort(keys, 0, k);
            status = true;
            for (int j=0; j<k; j++) {
                key = keys[j];
                o = h.get(key);
                if (o == null)
                    continue;
                try {
                    Object v = JSON2FmModel.get(json, key);
                    if (v == null)
                        value = null;
                    else if (v instanceof String)
                        value = (String) v;
                    else if (v instanceof Map) // for count
                        value = String.valueOf(((Map) v).size());
                    else if (v instanceof List) // for count
                        value = String.valueOf(((List) v).size());
                    else
                        value = v.toString();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, Event.traceStack(e)).send();
                    return (! def);
                }
                if (value == null) {
                    status = false;
                    break;
                }
                if (o instanceof DataSet) { // dataset testing
                    int m;
                    d = (DataSet) o;
                    try {
                        if ((m = d.getDataType()) == DataSet.DATA_LONG)
                            status = d.contains(Long.parseLong(value));
                        else if (m == DataSet.DATA_DOUBLE)
                            status = d.contains(Double.parseDouble(value));
                        else
                            status = false;
                    }
                    catch (Exception e) {
                        status = false;
                    }
                    if (!status)
                        break;
                }
                else { // pattern
                    p = (Pattern) o;
                    if (!pm.contains(value, p)) {
                        status = false;
                        break;
                    }
                }
            }
            if (status)
                break;
        }
        return status;
    }

    /**
     * returns true if any one of the pattern maps matches on the parsed
     * JSON data. A pattern map represents all the patterns in a Map where
     * each key is a json path.  In case the key of a pattern is pointing
     * a container, the size of the container will be evaluated. The default
     * boolean value is used to evaluate an empty pattern map.
     */
    public static boolean evaluate(List json, Map[] props, Perl5Matcher pm,
        boolean def) {
        Map h;
        Iterator iter;
        Pattern p;
        DataSet d;
        Object o;
        String key, value;
        String[] keys;
        boolean status = def;
        int i, k, n;

        if (json == null || props == null)
            return (! status);

        n= props.length;
        if (n <= 0)
            return status;

        for (i=0; i<n; i++) {
            h = props[i];
            if (h == null)
                continue;
            k = h.size();
            keys = new String[k];
            iter = h.keySet().iterator();
            k = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                keys[k++] = key;
            }
            if (k > 1)
                Arrays.sort(keys, 0, k);
            status = true;
            for (int j=0; j<k; j++) {
                key = keys[j];
                o = h.get(key);
                if (o == null)
                    continue;
                try {
                    Object v = JSON2FmModel.get(json, key);
                    if (v == null)
                        value = null;
                    else if (v instanceof String)
                        value = (String) v;
                    else if (v instanceof Map) // for count
                        value = String.valueOf(((Map) v).size());
                    else if (v instanceof List) // for count
                        value = String.valueOf(((List) v).size());
                    else
                        value = v.toString();
                }
                catch (Exception e) {
                    new Event(Event.WARNING, Event.traceStack(e)).send();
                    return (! def);
                }
                if (value == null) {
                    status = false;
                    break;
                }
                if (o instanceof DataSet) { // dataset testing
                    int m;
                    d = (DataSet) o;
                    try {
                        if ((m = d.getDataType()) == DataSet.DATA_LONG)
                            status = d.contains(Long.parseLong(value));
                        else if (m == DataSet.DATA_DOUBLE)
                            status = d.contains(Double.parseDouble(value));
                        else
                            status = false;
                    }
                    catch (Exception e) {
                        status = false;
                    }
                    if (!status)
                        break;
                }
                else { // pattern
                    p = (Pattern) o;
                    if (!pm.contains(value, p)) {
                        status = false;
                        break;
                    }
                }
            }
            if (status)
                break;
        }
        return status;
    }

    public static boolean evaluate(String value, Map[] props, Perl5Matcher pm,
        boolean def) {
        Map h;
        Iterator iter;
        Pattern p;
        DataSet d;
        Object o;
        boolean status = def;
        int i, k, n;

        if (value == null || props == null)
            return (! status);

        n = props.length;
        if (n <= 0)
            return status;

        for (i=0; i<n; i++) {
            h = props[i];
            if (h == null)
                continue;
            k = h.size();
            iter = h.entrySet().iterator();
            status = true;
            o = iter.next();
            if (o == null)
                continue;
            if (o instanceof DataSet) { // dataset testing
                int m;
                d = (DataSet) o;
                try {
                    if ((m = d.getDataType()) == DataSet.DATA_LONG)
                        status = d.contains(Long.parseLong(value));
                    else if (m == DataSet.DATA_DOUBLE)
                        status = d.contains(Double.parseDouble(value));
                    else
                        status = false;
                }
                catch (Exception e) {
                    status = false;
                }
            }
            else { // pattern
                p = (Pattern) o;
                status = pm.contains(value, p);
            }
            if (status)
                break;
        }
        return status;
    }

    public static JSONFilter[] initFilters(String prefix, String name, Map ph,
        Perl5Matcher pm) {
        Perl5Compiler pc;
        Object o;
        if (ph == null || ph.size() <= 0)
            return null;
        if (name == null || name.length() <= 0)
            return null;
        if ((o = ph.get(name)) == null || !(o instanceof List))
            return new JSONFilter[0];
        else {
            JSONFilter[] filters = null;
            JSONFilter filter = null;
            List pl, list = (List) o;
            int i, n = list.size();
            pl = new ArrayList();
            if (pm == null)
                pm = new Perl5Matcher();
            pc = new Perl5Compiler();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                filter = new JSONFilter(prefix, (Map) o, pm, pc);
                if (filter != null)
                    pl.add(filter);
            }
            n = pl.size();
            filters = new JSONFilter[n];
            for (i=0; i<n; i++)
                filters[i] = (JSONFilter) pl.get(i);
            return filters;
        }
    }

    public boolean hasFormatter() {
       return hasFormatter;
    }

    public boolean hasSection() {
       return hasSection;
    }

    public void clear() {
        if (aJSONProps != null) {
            for (int i=aJSONProps.length; i>=0; i--) {
                if (aJSONProps[i] != null) try {
                    aJSONProps[i].clear();
                }
                catch (Exception e) {
                }
                aJSONProps[i] = null;
            }
        }
        aJSONProps = null;
        if (xJSONProps != null) {
            for (int i=xJSONProps.length; i>=0; i--) {
                if (xJSONProps[i] != null) try {
                    xJSONProps[i].clear();
                }
                catch (Exception e) {
                }
                xJSONProps[i] = null;
            }
        }
        xJSONProps = null;
        pm = null;
        if (hasFormatter) {
            temp = null;
            tsub = null;
        }
    }

    /**
     * returns an array of Map with a group of patterns in each of them
     */
    public static Map[] getPatternMaps(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        int i, size = 0;
        Object o;
        List pl;
        Map[] h = new HashMap[0];
        if ((o = ph.get(name)) != null) {
            if (o instanceof List) {
                String str;
                pl = (List) o;
                size = pl.size();
                h = new HashMap[size];
                for (i=0; i<size; i++) {
                    if ((o = pl.get(i)) == null || !(o instanceof Map))
                        return null;
                    Map m = (Map) o;
                    h[i] = new HashMap();
                    Iterator iter = m.keySet().iterator();
                    while (iter.hasNext()) {
                        String key = (String) iter.next();
                        if (key == null || key.length() <= 0)
                            continue;
                        o = m.get(key);
                        if (o instanceof List)
                            h[i].put(key, new DataSet((List) o));
                        else {
                            str = (String) o;
                            h[i].put(key, pc.compile(str));
                        }
                    }
                }
            }
            else {
                return null;
            }
        }
        return h;
    }
}
