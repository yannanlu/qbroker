package org.qbroker.json;

/* JSONSelector.java - a filter to select JSON data */

import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.DataSet;
import org.qbroker.event.Event;

/**
 * JSONSelector is to evaluate JSON data with patterns or number ranges.
 * The method of evaluate() returns true if the selector matches the JSON data.
 * Otherwise, it returns false. It supports 3 types of selectors, Pattern,
 * DataSet and Map with keys for JSON Paths and values for patterns or ranges.
 *<br/><br/>
 * JSONSelector of type 1 or type 3 can be used to store data for overloading.
 * One example is to store the json path on the current JSON data for
 * modifications. The method of getValue() returns the original string from
 * type 1 selector only. The method of getValue(String key) returns the String
 * value at the given key from type 3 selector only.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONSelector {
    private String prefix;
    private int type = 0;
    private Map<String, Object> aJSONProps = null;
    private Pattern pattern = null;
    private DataSet dset = null;
    private static ThreadLocal<Perl5Matcher> lm=new ThreadLocal<Perl5Matcher>();

    @SuppressWarnings("unchecked")
    public JSONSelector(String prefix, Object obj) {
        if (obj == null)
            throw(new IllegalArgumentException("null selector"));

        if (prefix == null)
            this.prefix = "";
        else
            this.prefix = prefix;

        Perl5Compiler pc = new Perl5Compiler();

        try {
            if (obj instanceof String) { // selector for keys
                pattern = pc.compile((String) obj);
                type = 1;
            }
            else if (obj instanceof List) {
                Object o = null;
                List list = (List) obj;
                int k = list.size();
                if (k > 0 && (o = list.get(0)) != null && o instanceof String) {
                    dset = new DataSet(list);
                    type = 2;
                }
                else if (k > 0 && o != null && o instanceof Map) { // for k-v
                    String key;
                    Map h = new HashMap();
                    for (int i=0; i<k; i++) {
                        o = list.get(i);
                        if (o == null || !(o instanceof Map))
                            continue;
                        key = (String) ((Map) o).get("key");
                        if (key == null || key.length() <= 0)
                            continue;
                        h.put(key, ((Map) o).get("value"));
                    }
                    Map ph = new HashMap();
                    ph.put("Selector", h);
                    aJSONProps = getPatternMap("Selector", ph, pc);
                    type = 3;
                }
            }
            else if (obj instanceof Map) { // for json config
                Map ph = new HashMap();
                ph.put("Selector", obj);
                aJSONProps = (Map) getPatternMap("Selector", ph, pc);
                type = 3;
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(prefix +
                " has a bad selector: " + e.toString()));
        }
    }

    public boolean evaluate(Map json) {
        if (json == null)
            return false;
        else if (aJSONProps == null)
            return true;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            try {
                return evaluate(json, aJSONProps, pm);
            }
            catch (Exception e) {
                new Event(Event.ERR, prefix + ": failed to evaluate json: " +
                    e.toString()).send();
                return false;
            }
        }
    }

    public boolean evaluate(List json) {
        if (json == null)
            return false;
        else if (aJSONProps == null)
            return true;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            try {
                return evaluate(json, aJSONProps, pm);
            }
            catch (Exception e) {
                new Event(Event.ERR, prefix + ": failed to evaluate json: " +
                    e.toString()).send();
                return false;
            }
        }
    }

    public boolean evaluate(String json) {
        if (json == null)
            return false;
        else if (pattern != null) {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            return pm.contains(json, pattern);
        }
        else if (dset == null)
            return true;
        else {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            try {
                if (dset.getDataType() == DataSet.DATA_LONG)
                    return dset.contains(Long.parseLong(json));
                else
                    return dset.contains(Double.parseDouble(json));
            }
            catch (Exception e) {
                    new Event(Event.ERR, prefix +
                        ": failed to evaluate number on " + json + ": " +
                        e.toString()).send();
                return false;
            }
        }
    }

    public int getType() {
        return type;
    }

    /**
     * It returns the original string representation of the pattern or null
     * if the type is not 1. This is for overloading.
     */
    public String getValue() {
        if (type == 1)
            return pattern.getPattern();
        else
            return null;
    }

    /**
     * It returns the string value at the given key from the type 3 only or
     * null if the type is not 3 otherwise. This is for overloading.
     */
    public String getValue(String key) {
        if (type == 3 && key != null && key.startsWith("."))
            return (String) aJSONProps.get(key);
        else
            return null;
    }

    public void clear() {
        if (aJSONProps != null) {
            aJSONProps.clear();
            aJSONProps = null;
        }
        pattern = null;
        dset = null;
    }

    /**
     * returns a Map with compiled pattern as the value for each json path
     */
    private static Map<String, Object> getPatternMap(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        Object o;
        Map<String, Object> map = null;
        String key;
        if ((o = ph.get(name)) != null && o instanceof Map) {
            Map h = (Map) o;
            map = new HashMap<String, Object>();
            Iterator iter = h.keySet().iterator();
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                if (!key.startsWith(".")) { // overloaded key-value pair
                    map.put(key, h.get(key));
                    continue;
                }
                o = h.get(key);
                if (o instanceof List)
                    map.put(key, new DataSet((List) o));
                else
                    map.put(key, pc.compile((String) o));
            }
        }
        return map;
    }

    private static boolean evaluate(Map json, Map<String, Object> ps,
        Perl5Matcher pm) {
        Object o, v;
        String value;
        for (String key : ps.keySet()) {
            if (key == null || key.length() <= 0 || key.startsWith("."))
                continue;
            o = ps.get(key); 
            if (o == null)
                continue;
            v = JSON2FmModel.get(json, key);
            if (v == null)
                return false;
            else if (v instanceof String)
                value = (String) v;
            else if (v instanceof Map) // for count
                value = String.valueOf(((Map) v).size());
            else if (v instanceof List) // for count
                value = String.valueOf(((List) v).size());
            else
                value = v.toString();
            if (o instanceof DataSet) { // dataset testing
                int k;
                boolean status = false;
                DataSet d = (DataSet) o;
                try {
                    if ((k = d.getDataType()) == DataSet.DATA_LONG)
                        status = d.contains(Long.parseLong(value));
                    else if (k == DataSet.DATA_DOUBLE)
                        status = d.contains(Double.parseDouble(value));
                    else
                        status = false;
                }
                catch (Exception e) {
                    status = false;
                }
                if (!status)
                    return false;
            }
            else { // pattern
                if (!pm.contains(value, (Pattern) o))
                    return false;
            }
        }
        return true;
    }

    private static boolean evaluate(List json, Map<String, Object> ps,
        Perl5Matcher pm) {
        Object o, v;
        String value;
        for (String key : ps.keySet()) {
            if (key == null || key.length() <= 0 || key.startsWith("."))
                continue;
            o = ps.get(key); 
            if (o == null)
                continue;
            v = JSON2FmModel.get(json, key);
            if (v == null)
                return false;
            else if (v instanceof String)
                value = (String) v;
            else if (v instanceof Map) // for count
                value = String.valueOf(((Map) v).size());
            else if (v instanceof List) // for count
                value = String.valueOf(((List) v).size());
            else
                value = v.toString();
            if (o instanceof DataSet) { // dataset testing
                int k;
                boolean status = false;
                DataSet d = (DataSet) o;
                try {
                    if ((k = d.getDataType()) == DataSet.DATA_LONG)
                        status = d.contains(Long.parseLong(value));
                    else if (k == DataSet.DATA_DOUBLE)
                        status = d.contains(Double.parseDouble(value));
                    else
                        status = false;
                }
                catch (Exception e) {
                    status = false;
                }
                if (!status)
                    return false;
            }
            else { // pattern
                if (!pm.contains(value, (Pattern) o))
                    return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public static void main(String args[]) {
        String filename = null, path = ".", pattern = null, str = null;
        JSONSelector selector = null;
        List list = new ArrayList();
        Map ph = new HashMap();
        int k;

        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'r':
                list.add(args[++i]);
                break;
              case 'p':
                pattern = args[++i];
                break;
              case 'h':
                str = args[++i];
                k = str.indexOf(":");
                if (k > 1)
                    ph.put(str.substring(0, k), str.substring(k+1).trim());
                break;
              case 'k':
                path = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        try {
            if (filename == null) {
                printUsage();
                System.exit(0);
            }
            else if (pattern != null) // for pattern
                selector = new JSONSelector("pattern", pattern); 
            else if (list.size() > 0) // for range
                selector = new JSONSelector("range", list); 
            else if (ph.size() > 0) {
                selector = new JSONSelector("json", ph); 
            }
            else {
                printUsage();
                System.exit(0);
            }

            Reader in = new FileReader(filename);
            char[] buffer = new char[4096];
            StringBuffer strBuf = new StringBuffer();
            while ((k = in.read(buffer, 0, 4096)) >= 0) {
                if (k > 0)
                    strBuf.append(new String(buffer, 0, k));
            }
            String json = strBuf.toString();
            in.close();
            in = new StringReader(json);
            Object obj = JSON2FmModel.parse(in);
            in.close();
            Object o;
            if (obj instanceof List)
                o = JSON2FmModel.get((List) obj, path);
            else
                o = JSON2FmModel.get((Map) obj, path);
            k = selector.getType();
            if (o == null)
                System.out.println("no data for " + path);
            else if (o instanceof Map && k == 1) {
                ph = (Map) o;
                Object[] keys = ph.keySet().toArray();
                int n = keys.length;
                for (int i=0; i<n; i++) {
                    o = keys[i];
                    if (o == null || !(o instanceof String))
                        continue;
                    str = (String) o;
                    if (!selector.evaluate(str))
                        ph.remove(str);
                }
                if (n > ph.size()) {
                    System.out.println("selected on " + path);
                    if (obj instanceof List)
                       System.out.print(JSON2FmModel.toJSON((List)obj,"","\n"));
                    else
                        System.out.print(JSON2FmModel.toJSON((Map)obj,"","\n"));
                }
                else
                    System.out.println("not selected on " + path);
            }
            else if (o instanceof List && k == 3) {
                list = (List) o;
                int n = list.size();
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map) {
                        if (!selector.evaluate((Map) o))
                            list.remove(i);
                    }
                    else if (o instanceof List) {
                        if (!selector.evaluate((List) o))
                            list.remove(i);
                    }
                }
                if (n > list.size()) {
                    System.out.println("selected on " + path);
                    if (obj instanceof List)
                       System.out.print(JSON2FmModel.toJSON((List)obj,"","\n"));
                    else
                        System.out.print(JSON2FmModel.toJSON((Map)obj,"","\n"));
                }
                else
                    System.out.println("not selected on " + path);
            }
            else if (o instanceof List && k >= 1 && k <= 2) {
                list = (List) o;
                int n = list.size();
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null || o instanceof Map || o instanceof List)
                        continue;
                    str = o.toString();
                    if (!selector.evaluate(str))
                        list.remove(i);
                }
                if (n > list.size()) {
                    System.out.println("selected on " + path);
                    if (obj instanceof List)
                       System.out.print(JSON2FmModel.toJSON((List)obj,"","\n"));
                    else
                        System.out.print(JSON2FmModel.toJSON((Map)obj,"","\n"));
                }
                else
                    System.out.println("not selected on " + path);
            }
            else
                System.out.println("data is not spported on " + path);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JSONSelector Version 1.0 (written by Yannan Lu)");
        System.out.println("JSONSelector: select JSON data in a file");
        System.out.println("Usage: java org.qbroker.json.JSONSelector -f filename -k path [-p pattern] [-r range] [-h key:value]");
        System.out.println("  -?: print this message");
        System.out.println("  -k: key path (example: .views.list[0].name)");
        System.out.println("  -p: pattern");
        System.out.println("  -r: ranges");
        System.out.println("  -h: key:value pairs");
        System.out.println("  -f: filename");
    }
}
