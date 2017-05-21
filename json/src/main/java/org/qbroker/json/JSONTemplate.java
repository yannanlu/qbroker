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
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONSection;

/**
 * JSONTemaplte is a template that formats JSON data. It contains a simple
 * template with parameters or varibales and a list of JSONSections. The
 * parameter is a place holder with name spaces such as "s:" for section, "p:"
 * for quoted text or "P:" for unquoted text. The values for "p:" or "P:" can
 * be set via the method of setParameter() before the format operation. The
 * other type of place holders is the variable with the name space of "v:" or
 * "V:". It is actually a JSON path for retrieving the value from the JSON data.
 * All the variables in the root level must be defined in a map with the name
 * of "variable". It contains key-value pairs with the name of the variable as
 * the key and the json path as the value. In the runtime, the values of the
 * variables will be loaded as normal parameters before the operation. After
 * the operation, they will be cleaned up.
 *<br/><br/>
 * JSONSection represents a portion of JSON data as the value in the template.
 * It supports various operations on JSON data. Some of the operations allow
 * to update the JSON data with override parameters. An override parameter has
 * valid a JSON path as the name. Its value can be any non-null JSON object.
 * It can be set via the method of setParameter() before the operation. A
 * JSONTemplate may contain multiple well defined sections. 
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONTemplate extends AssetList {
    private String name;
    private Template temp;
    private TextSubstitution tsub;
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

        // init variables with the json path to root and the value
        if ((o = props.get("variable")) != null && o instanceof Map) {
            Map map = (Map) o;
            for (Object obj : map.keySet()) {
                varMap.put((String) obj, (String) map.get(obj));
            }
        }

        // init sections
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
            !key.startsWith("r:") && !key.startsWith("v:")) {
            if (key.charAt(0) == '.' || key.charAt(0) == '[') // for override
                params.put("o:" + key, value);
            else // normal parameter for substitutions
                params.put(key, value);
        }
    }

    public Object getParameter(String key) {
        if (key != null && key.length() > 0 && !key.startsWith("r:") &&
            !key.startsWith("v:")) {
            if (key.charAt(0) == '.' || key.charAt(0) == '[') // for override
                return params.get("o:" + key);
            else // normal parameter for substitutions
                return params.get(key);
        }
        return null;
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

        // add root JSON data
        params.put("r:_JSON_", json);
        for (String key : varMap.keySet()) { // load variables from JSON data
            if (key == null || key.length() <= 0)
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
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("P:")) { // for a parameter without quotes
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
                text = temp.substitute(key, value, text);
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
            tsub.substitute(text);

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
                text = temp.substitute(key, value, text);
            }
            else if (key.startsWith("P:")) { // for a parameter without quotes
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
                text = temp.substitute(key, value, text);
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
            tsub.substitute(text);

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
        if (temp != null)
            temp.clear();
        temp = null;
        if (tsub != null)
            tsub.clear();
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
