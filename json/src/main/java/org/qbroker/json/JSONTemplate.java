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
 * JSONTemaplte is a template that formats JSON data. It contains a template
 * and a list of JSONSections.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONTemplate extends AssetList {
    private String name;
    private Template temp;
    private TextSubstitution tsub;
    private Perl5Matcher pm = null;
    private Map<String, Object> params = new HashMap<String, Object>();
    private Map<String, String> varMap = new HashMap<String, String>();
    private boolean keepOriginalOrder = false;

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

        if ((o = props.get("keepOriginalOrder")) != null &&
            o instanceof String && "true".equals((String) o))
            keepOriginalOrder = true;

        if ((o = props.get("variable")) != null && o instanceof Map) {
            Map map = (Map) o;
            for (Object obj : map.keySet()) {
                varMap.put((String) obj, (String) map.get(obj));
            }
        }

        if ((o = props.get("sections")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();

        n = list.size();
        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            sect = new JSONSection((Map) o, keepOriginalOrder);
            add(sect.getName(), new long[]{i}, sect);
        }

        iter = temp.iterator();
        while (iter.hasNext()) { // check if any section is missing
            key = (String) iter.next();
            if (!key.startsWith("s:"))
                continue;
            key = key.substring(2);
            if (!containsKey(key))
                throw(new IllegalArgumentException(name +
                    " has no section defined for " + key));
        }
    }

    public void setParameter(String key, Object value) {
        if (key != null && key.length() > 0 && value != null &&
            !key.startsWith("r:") && !key.startsWith("v:"))
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

    /** if this returns true, the json should be parsed with ordering support */
    public boolean keepOriginalOrder() {
        return keepOriginalOrder;
    }

    /**
     * returns the formatted JSON content on the given JSON data
     */
    public String format(Map json) {
        JSONSection sect;
        Object o;
        String path, value, text;

        if (json == null || json.size() <= 0)
            return null;

        // add JSON data
        params.put("r:_JSON_", json);
        for (String key : varMap.keySet()) { // load values for variables
            if (key == null || key.length() <= 0 || key.startsWith("r:"))
                continue;
            path = varMap.get(key);
            o = JSON2FmModel.get(json, path);
            if (o != null)
                params.put("v:" + key, o);
        }
        text = temp.copyText();
        for (String key : temp.getAllFields()) {
            if (key.startsWith("p:")) { // for a parameter
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(pm, key, value, text);
            }
            else if (key.startsWith("P:")) { // for a parameter without quotes
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(pm, key, value, text);
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
                text = temp.substitute(pm, key, value, text);
            }
            else if ((sect = (JSONSection) get(key.substring(2))) != null) {
                try {
                    value = sect.fulfill(json, this, params);
                }
                catch (Exception e) {
                    params.remove("r:_JSON_");
                    for (String ky : varMap.keySet()) // clean up
                        params.remove("v:" + ky);
                    throw(new RuntimeException(name + ": " + sect.getName() +
                        " failed to fulfill the section of " + key + ": " +
                        Utils.traceStack(e)));
                }
                text = temp.substitute(pm, key, value, text);
            }
            else {
                params.remove("r:_JSON_");
                for (String ky : varMap.keySet()) // clean up
                    params.remove("v:" + ky);
                throw(new RuntimeException(name+" has no section defined for "+
                    key));
            }
        }
        // remove JSON data
        params.remove("r:_JSON_");
        for (String key : varMap.keySet()) // clean up
            params.remove("v:" + key);

        if (tsub != null)
            tsub.substitute(pm, text);

        return text;
    }

    /**
     * returns the formatted JSON content on the given JSON data
     */
    public String format(List json) {
        JSONSection sect;
        Object o;
        String path, value, text;

        if (json == null || json.size() <= 0)
            return null;

        // add JSON data
        params.put("r:_JSON_", json);
        for (String key : varMap.keySet()) { // load values for variables
            if (key == null || key.length() <= 0 || key.startsWith("r:"))
                continue;
            path = varMap.get(key);
            o = JSON2FmModel.get(json, path);
            if (o != null)
                params.put(key, o);
        }
        text = temp.copyText();
        for (String key : temp.getAllFields()) {
            if (key.startsWith("p:")) { // for a parameter
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = "\"" + (String) o + "\"";
                else
                    value = o.toString();
                text = temp.substitute(pm, key, value, text);
            }
            else if (key.startsWith("P:")) { // for a parameter without quotes
                o = params.get(key.substring(2));
                if (o == null)
                    value = "null";
                else if (o instanceof String)
                    value = (String) o;
                else
                    value = o.toString();
                text = temp.substitute(pm, key, value, text);
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
                text = temp.substitute(pm, key, value, text);
            }
            else if ((sect = (JSONSection) get(key.substring(2))) != null) {
                try {
                    value = sect.fulfill(json, this, params);
                }
                catch (Exception e) {
                    params.remove("r:_JSON_");
                    for (String ky : varMap.keySet()) // clean up
                        params.remove("v:" + ky);
                    throw(new RuntimeException(name + ": " + sect.getName() +
                        " failed to fulfill the section of " + key + ": " +
                        Utils.traceStack(e)));
                }
                text = temp.substitute(pm, key, value, text);
            }
            else {
                params.remove("r:_JSON_");
                for (String ky : varMap.keySet()) // clean up
                    params.remove("v:" + ky);
                throw(new RuntimeException(name+" has no section defined for "+
                    key));
            }
        }
        // remove JSON data
        params.remove("r:_JSON_");
        for (String key : varMap.keySet()) // clean up
            params.remove("v:" + key);

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
        if (params != null) {
            params.clear();
            params = null;
        }
        if (varMap != null) {
            varMap.clear();
            varMap = null;
        }
        super.clear();
    }

    protected void finalize() {
        clear();
        super.finalize();
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
            for (String key : params.keySet()) { // set parameters
                if (key == null || key.length() <= 0)
                    continue;
                template.setParameter(key, params.get(key));
            }
            in = new FileReader(filename);
            Object o = (!template.keepOriginalOrder()) ? JSON2FmModel.parse(in):
                JSON2Map.parseJSON(in, true);
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
