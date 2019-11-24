package org.qbroker.json;

/* JSONFilter.java - a client side filter to evaluate parsed JSON data */

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
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
 *<br><br>
 * The filter part contains two sets of EvalMaps. One set is for positive match
 * to include certain patterns. The other set is for negtive match to exclude
 * certain patterns. An EvalMap contains key-value pairs to supports 3 types of
 * evaluations: Perl5 pattern match, number range testing and evaluations on
 * variables. If a key is like "v:xxx:, the evaluation is on a variable where
 * "xxx" is the json path and the value determines the operation. Currently,
 * only "==" is supported. The dynamic value will be stored in parameter map at
 * the key of "v:xxx". For keys without namespace, they are json paths to
 * retrieve data to be evaluated. The value will be either a Perl5 Pattern or
 * a list of number ranges.
 *<br><br>
 * The template part supports JSONSection for keys with the namespace of "s:".
 * In this case, the list of sections must be provided for the format method.
 * The parameter map is supposed to contain the root JSON doc at the key of
 * "r:_JSON_".
 *<br><br>
 * It also supports the dynamic pattern or value for keys with the namespace
 * of "v:". In this case, the parameter map is supposed to contain the
 * value of the variable under the key of "v:xxx".
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JSONFilter implements Filter<Map> {
    private String prefix;
    private String indent = null;
    private String end = null;
    private String[] keys = null;
    private Map<String, Object>[] aJSONProps = null, xJSONProps = null;
    private Template temp = null;
    private TextSubstitution tsub = null;
    private int count = 0;
    private boolean hasFormatter, hasSection;
    private static ThreadLocal<Perl5Matcher> lm=new ThreadLocal<Perl5Matcher>();

    public JSONFilter(String prefix, Map props, Perl5Compiler pc) {
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

        if ((o = props.get("indent")) != null && o instanceof String)
            indent = (String) o;
        if ((o = props.get("end")) != null && o instanceof String)
            end = (String) o;

        // for path-pattern group
        if(props.containsKey("patterns") || props.containsKey("xpatterns")) try{
            aJSONProps = getPatternMaps("patterns", props, pc);
            xJSONProps = getPatternMaps("xpatterns", props, pc);
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
                str = sub.substitute(JSON2FmModel.toJSON((Map) o, null, null));
            else if (o instanceof List)
                str = sub.substitute(JSON2FmModel.toJSON((List) o, null, null));
            else
                str = o.toString();

            temp = new Template(str);

            if ((o = props.get("substitution")) != null && o instanceof String)
                tsub = new TextSubstitution((String) o);
            hasFormatter = true;
            keys = temp.getAllFields();
            count = temp.numberOfFields();
            for (int i=0; i<count; i++) {
                if (keys[i].startsWith("s:")) { // for sections
                    hasSection = true;
                    break;
                }
            }
        }
    }

    public JSONFilter(String prefix, Map props) {
        this(prefix, props, (Perl5Compiler) null);
    }

    /**
     * formats the JSON map with the predefined formatters, a given list of
     * JSONSections and a given parameter map. It returns formatted content
     * upon success.
     */
    public String format(Map json, AssetList list, Map<String, Object> params) {
        String text, value;
        Object o;
        boolean withSection;
        if ((!hasFormatter) || json == null)
            return null;
        if (params == null)
            params = new HashMap<String, Object>();

        withSection = (hasSection && list != null);
        text = temp.copyText();
        for (String key : keys) {
            if (key.startsWith("p:")) { // for a parameter
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("P:")) {//for a parameter without quotes
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("v:")) { // for a variable
                o = params.get(key);
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("V:")) { // for a variable without quotes
                o = params.get("v:"+key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (!key.startsWith("s:")) { // for a json path
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
                text = temp.substitute(key, value, text);
            }
            else if (withSection) { // for a section key
                JSONSection sect;
                sect = (JSONSection) list.get(key.substring(2));
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
                text = temp.substitute(key, value, text);
            }
        }
        if (tsub != null)
            tsub.substitute(text);

        return text;
    }

    /**
     * formats the JSON list with the predefined formatters, a given list of
     * JSONSections and a given parameter map. It returns formatted content
     * upon success.
     */
    public String format(List json, AssetList list, Map<String, Object> params){
        String text, value;
        Object o;
        boolean withSection;
        if ((!hasFormatter) || json == null)
            return null;
        if (params == null)
            params = new HashMap<String, Object>();

        withSection = (hasSection && list != null);
        text = temp.copyText();
        for (String key : keys) {
            if (key.startsWith("p:")) { // for a parameter
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("P:")) {//for a parameter without quotes
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("v:")) { // for a variable
                o = params.get(key);
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("V:")) { // for a variable without qutoes
                o = params.get("v:"+key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (!key.startsWith("s:")) { // for a json path
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
                text = temp.substitute(key, value, text);
            }
            else if (withSection) { // for a section key
                JSONSection sect;
                sect = (JSONSection) list.get(key.substring(2));
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
                text = temp.substitute(key, value, text);
            }
        }

        if (tsub != null)
            tsub.substitute(text);

        return text;
    }

    /**
     * formats the JSON text with the predefined formatters, a given list of
     * JSONSections and a given parameter map. It returns formatted content
     * upon success.
     */
    public String format(String json, AssetList list, Map<String,Object>params){
        String text, value;
        Object o;
        boolean withSection;
        if ((!hasFormatter) || json == null)
            return null;

        withSection = (hasSection && list != null);
        text = temp.copyText();
        for (String key : keys) {
            if (key.startsWith("p:")) { // for a parameter
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("P:")) {//for a parameter without quotes
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("v:")) { // for a variable
                o = params.get(key);
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("V:")) { // for a variable
                o = params.get("v:"+key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(key, value, text);
            }
            else if (!key.startsWith("s:")) { // for selected value
                text = temp.substitute(key, json, text);
            }
            else if (withSection) { // for a section
                JSONSection sect;
                sect = (JSONSection) list.get(key.substring(2));
                if (sect == null) {
                    throw(new RuntimeException(prefix +
                        " failed to get section for " + key));
                }
                else try {
                    value = sect.fulfill(new HashMap(), list, params);
                }
                catch (Exception e) {
                    throw(new RuntimeException(prefix + ": " + sect.getName() +
                        " failed to fulfill the section of " + key + ": " +
                        Event.traceStack(e)));
                }
                text = temp.substitute(key, value, text);
            }
        }
        if (tsub != null)
            tsub.substitute(text);

        return text;
    }

    /**
     * It evaluates the JSON map data with the internal predefined filters
     * and returns true if there is a hit or false otherwise.
     */
    public boolean evaluate(long tm, Map json) {
        return evaluate(json, null);
    }

    /**
     * It evaluates the JSON map data with the internal predefined filters
     * and the given parameter map. It returns true if there is a hit or
     * false otherwise.
     */
    public boolean evaluate(Map json, Map<String, Object> params) {
        if (json == null)
            return false;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            if (evaluate(json, aJSONProps, pm, true, params) &&
                !evaluate(json, xJSONProps, pm, false, params))
                return true;
            else
                return false;
        }
    }

    /**
     * It evaluates the JSON list data with the internal predefined filters
     * and the given parameter map. It returns true if there is a hit or
     * false otherwise.
     */
    public boolean evaluate(List json, Map<String, Object> params) {
        if (json == null)
            return false;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            if (evaluate(json, aJSONProps, pm, true, params) &&
                !evaluate(json, xJSONProps, pm, false, params))
                return true;
            else
                return false;
        }
    }

    /**
     * It evaluates the JSON text with the internal predefined filters
     * and the given parameter map. It returns true if there is a hit or
     * false otherwise.
     */
    public boolean evaluate(String json, Map<String, Object> params) {
        if (json == null)
            return false;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            if (evaluate(json, aJSONProps, pm, true, params) &&
                !evaluate(json, xJSONProps, pm, false, params))
                return true;
            else
                return false;
        }
    }

    /**
     * returns true if any one of the eval maps matches on the parsed JSON data.
     * An eval map contains key-value pairs with the key for either a json path
     * or a key path for a variable. The value of the key is one of the pattern,     * number range or operator. In case a key is ".", the size of the
     * container will be evaluated. If a key starts with "v:", it will be
     * treated as the variable. The rest part of the key will be the json path.
     * The default boolean value is used to evaluate an empty eval map.
     */
    private static boolean evaluate(Map json, Map<String, Object>[] props,
        Perl5Matcher pm, boolean def, Map<String, Object> params) {
        Pattern p;
        DataSet d;
        Object o;
        String key, value;
        String[] keys;
        boolean status = def;
        int k;

        if (json == null || props == null)
            return (! status);

        for (Map<String, Object> h : props) {
            keys = h.keySet().toArray(new String[h.size()]);
            k = 0;
            for (String ky : keys) {
                if (ky == null || ky.length() <= 0)
                    continue;
                keys[k++] = ky;
            }
            if (k > 1)
                Arrays.sort(keys, 0, k);
            status = true;
            for (int i=0; i<k; i++) {
                key = keys[i];
                o = h.get(key);
                if (o == null)
                    continue;
                if (key.startsWith("v:")) // for variable
                    key = key.substring(2);
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
                else if (o instanceof Pattern) { // pattern
                    p = (Pattern) o;
                    if (!pm.contains(value, p)) {
                        status = false;
                        break;
                    }
                }
                else if (params != null) { // variable testing
                    status = value.equals((String) params.get("v:" + key));
                    if (!status)
                        break;
                }
            }
            if (status)
                break;
        }
        return status;
    }

    /**
     * returns true if any one of the eval maps matches on the parsed JSON data.
     * An eval map contains key-value pairs with the key for either a json path
     * or a key path for a variable. The value of the key is one of the pattern,     * number range or operator. In case a key is ".", the size of the
     * container will be evaluated. If a key starts with "v:", it will be
     * treated as the variable. The rest part of the key will be the json path.
     * The default boolean value is used to evaluate an empty eval map.
     */
    private static boolean evaluate(List json, Map<String, Object>[] props,
        Perl5Matcher pm, boolean def, Map<String, Object> params) {
        Pattern p;
        DataSet d;
        Object o;
        String key, value;
        String[] keys;
        boolean status = def;
        int k;

        if (json == null || props == null)
            return (! status);

        for (Map<String, Object> h : props) {
            keys = h.keySet().toArray(new String[h.size()]);
            k = 0;
            for (String ky : keys) {
                if (ky == null || ky.length() <= 0)
                    continue;
                keys[k++] = ky;
            }
            if (k > 1)
                Arrays.sort(keys, 0, k);
            status = true;
            for (int i=0; i<k; i++) {
                key = keys[i];
                o = h.get(key);
                if (o == null)
                    continue;
                if (key.startsWith("v:")) // for variable
                    key = key.substring(2);
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
                else if (o instanceof Pattern) { // pattern
                    p = (Pattern) o;
                    if (!pm.contains(value, p)) {
                        status = false;
                        break;
                    }
                }
                else if (params != null) { // variable testing
                    status = value.equals((String) params.get("v:" + key));
                    if (!status)
                        break;
                }
            }
            if (status)
                break;
        }
        return status;
    }

    /**
     * returns true if any one of the eval maps matches on the parsed JSON data.
     * An eval map contains key-value pairs with the key for either a json path
     * or a key path for a variable. The value of the key is one of the pattern,     * number range or operator. In case a key is ".", the size of the
     * container will be evaluated. If a key starts with "v:", it will be
     * treated as the variable. The rest part of the key will be the json path.
     * The default boolean value is used to evaluate an empty eval map.
     */
    private static boolean evaluate(String value, Map<String, Object>[] props,
        Perl5Matcher pm, boolean def, Map<String, Object> params) {
        Pattern p;
        DataSet d;
        Object o;
        String[] keys;
        boolean status = def;
        int k;

        if (value == null || props == null)
            return (! status);

        for (Map<String, Object> h : props) {
            keys = h.keySet().toArray(new String[1]);
            status = true;
            if (keys.length <= 0 || keys[0] == null)
                continue;
            o = h.get(keys[0]);
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
            else if (o instanceof Pattern) { // pattern
                p = (Pattern) o;
                status = pm.contains(value, p);
            }
            else if (params != null && keys[0].startsWith("v:")) { // variable
                status = value.equals((String) params.get(keys[0]));
            }
            if (status)
                break;
        }
        return status;
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
        if (temp != null) {
            temp.clear();
            temp = null;
        }
        if (tsub != null) {
            tsub.clear();
            tsub = null;
        }
    }

    protected void finalize() {
        clear();
    }

    // returns an array of JSONFilters with the given property map
    protected static JSONFilter[] initFilters(String prefix,String name,Map ph){
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
            List<JSONFilter> pl;
            List list = (List) o;
            int n = list.size();
            Perl5Compiler pc = new Perl5Compiler();
            pl = new ArrayList<JSONFilter>();
            for (int i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                filter = new JSONFilter(prefix, (Map) o, pc);
                if (filter != null)
                    pl.add(filter);
            }
            filters = pl.toArray(new JSONFilter[pl.size()]);
            pl.clear();
            return filters;
        }
    }

    /**
     * returns an array of Map with a group of patterns in each of them
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] getPatternMaps(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        int i, size = 0;
        Object o;
        List pl;
        Map<String, Object>[] h = new HashMap[0];
        if (pc == null)
            pc = new Perl5Compiler();
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
                    h[i] = new HashMap<String, Object>();
                    Iterator iter = m.keySet().iterator();
                    while (iter.hasNext()) {
                        String key = (String) iter.next();
                        if (key == null || key.length() <= 0)
                            continue;
                        o = m.get(key);
                        if (o instanceof List) // for range
                            h[i].put(key, new DataSet((List) o));
                        else if (key.startsWith("v:")) // for dynamic variable
                            h[i].put(key, ".");
                        else { // for pattern
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
