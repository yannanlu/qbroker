package org.qbroker.json;

/* JSONTemplate.java - a Template for formatting JSON docs */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.Reader;
import java.io.StringReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONSection;

/**
 * JSONTemaplte is a template that formats JSON data.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONTemplate extends AssetList {
    private String name;
    private Template temp;
    private TextSubstitution tsub;
    private Perl5Matcher pm = null;
    private Map<String, Object> params = new HashMap<String, Object>();

    public JSONTemplate(Map props) {
        super((String) props.get("name"), 1024);
        Object o;
        JSONSection sect;
        TextSubstitution sub;
        List list;
        Iterator iter;
        String key;
        int i, n;

        if (props == null || props.size() <= 0)
           throw(new IllegalArgumentException("empty property for a Template"));

        if ((o = props.get("name")) == null && (o = props.get("Name")) == null)
            throw(new IllegalArgumentException("name is not defined"));
        name = (String) o;

        // for getting rid of double quotes on variables
        sub = new TextSubstitution("s/\"(##[^#]+##)\"/$1/g");
        pm = new Perl5Matcher();

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
        }
        else
           throw(new IllegalArgumentException("template is not defined for " +
               name));

        if (temp.numberOfFields() <= 0)
        throw(new IllegalArgumentException("template has no field defined for "+
               name));

        if ((o = props.get("sections")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();

        n = list.size();
        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            sect = new JSONSection((Map) o);
            add(sect.getName(), new long[]{i}, sect);
        }

        iter = temp.iterator();
        while (iter.hasNext()) { // check if any section is missing
            key = (String) iter.next();
            if (!key.matches("^s:.+"))
                continue;
            key = key.substring(2);
            if (!containsKey(key))
                throw(new IllegalArgumentException(name +
                    " has no section defined for " + key));
        }
    }

    public void setParameter(String key, Object value) {
        params.put(key, value);
    }

    public Object getParameter(String key) {
        return params.get(key);
    }

    public void clearParameter() {
        params.clear();
    }

    public Iterator iterator() {
        return temp.iterator();
    }

    public String copyText() {
        return temp.copyText();
    }

    public String format(Map json) {
        JSONSection sect;
        Object o;
        String[] keys;
        String key, value, text;
        int i, n;

        if (json == null || json.size() <= 0)
            return null;

        keys = temp.getAllFields();
        n = keys.length;
        text = temp.copyText();
        for (i=0; i<n; i++) {
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
            else if (key.matches("^P:.+")) { // for a parameter without quotes
                key = key.substring(2);
                o = params.get(key);
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
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
            sect = (JSONSection) get(key);
            if (sect == null) {
                throw(new RuntimeException(name+" has no section defined for "+
                    key));
            }
            else try {
                value = sect.fulfill(json, this, params);
            }
            catch (Exception e) {
                throw(new RuntimeException(name + ": " + sect.getName() +
                    " failed to fulfill the section of " + key + ": " +
                    Utils.traceStack(e)));
            }
            text = temp.substitute(pm, keys[i], value, text);
        }

        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    public String format(List json) {
        JSONSection sect;
        Object o;
        String[] keys;
        String key, value, text;
        int i, n;

        if (json == null || json.size() <= 0)
            return null;

        keys = temp.getAllFields();
        n = keys.length;
        text = temp.copyText();
        for (i=0; i<n; i++) {
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
            else if (key.matches("^P:.+")) { // for a parameter without quotes
                key = key.substring(2);
                o = params.get(key);
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
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
            sect = (JSONSection) get(key);
            if (sect == null) {
                throw(new RuntimeException(name+" has no section defined for "+
                    key));
            }
            else try {
                value = sect.fulfill(json, this, params);
            }
            catch (Exception e) {
                throw(new RuntimeException(name + ": " + sect.getName() +
                    " failed to fulfill the section of " + key + ": " +
                    Utils.traceStack(e)));
            }
            text = temp.substitute(pm, keys[i], value, text);
        }

        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    public void clear() {
        int i;
        JSONSection sect;
        Browser b = browser();

        while ((i = b.next()) >= 0) {
            sect = (JSONSection) get(i);
            if (sect != null)
                sect.clear();
        }
        temp = null;
        tsub = null;
        super.clear();
    }

    @SuppressWarnings("unchecked")
    public static void main(String args[]) {
        String filename = null, path = null, str;
        Map<String, String> params = new HashMap<String, String>();
        JSONTemplate template;
        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 't':
                if (i+1 < args.length)
                    path = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              case 'p':
                if (i+1 < args.length) {
                    int k;
                    str = args[++i];
                    if ((k = str.indexOf(":")) > 1)
                        params.put(str.substring(0, k),
                            str.substring(k+1).trim());
                }
                break;
              default:
            }
        }

        if (filename == null || path == null) {
            printUsage();
            System.exit(0);
        }

        try {
            Reader in = new FileReader(path);
            Map ph = (Map) JSON2FmModel.parse(in);
            in.close();
            ph.put("name", path);
            template = new JSONTemplate(ph);
            Iterator iter = params.keySet().iterator();
            while (iter.hasNext()) { // set parameters
                str = (String) iter.next();
                if (str == null || str.length() <= 0)
                    continue;
                template.setParameter(str, params.get(str));
            }
            in = new FileReader(filename);
            Object o = JSON2FmModel.parse(in);
            in.close();
            if (o instanceof List)
                System.out.println(template.format((List) o));
            else
                System.out.println(template.format((Map) o));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JSONTemplate Version 1.0 (written by Yannan Lu)");
        System.out.println("JSONTemplate: format JSON data via a template");
        System.out.println("Usage: java org.qbroker.json.JSONTemplate -f jsonfile -t tempfile");
        System.out.println("  -?: print this message");
        System.out.println("  -f: json data file");
        System.out.println("  -t: json template file");
        System.out.println("  -p: parameter of key:value");
    }
}
