package org.qbroker.json;

/* JSONSecion.java - for a portion JSON data as value */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.AssetList;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSONFilter;
import org.qbroker.event.Event;

/**
 * JSONSection represents a portion of JSON data as the value in a template. It
 * is referenced by the placeholder with the namespace of "s:". JSONSection
 * contains a JSON path to select the content from the root JSON, an operation
 * name and a list of JSONFilters to select the attached template by matching
 * on each of the selected JSON content. On the first match, the template of
 * the very filter will be applied to the selected content. The api method of
 * fulfill() will return the formatted JSON content.
 *<br/><br/>
 * JSONSection supports nested children JSONSections. Therefore, its json path
 * is always applied to the root JSON data. In this case, a list of sections
 * and a parameter map must be provided for the api method. The parameter map
 * is supposed to contain the root JSON data at the key of "r:_JSON_".
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class JSONSection {
    private String name;
    private String path;
    private String keyPath = ".";
    private String operation = "copy";
    private String indent = null;
    private String end = null;
    private String delimiter = ",";
    private Map<String, String> varMap;
    private JSONFilter[] filters;
    private Perl5Matcher pm = null;
    private int count = 0;
    private int oid = JSON_COPY;
    private boolean hasVariable = false;
    private boolean withOrder;
    private final static int JSON_COPY = 0;
    private final static int JSON_EACH = 1;
    private final static int JSON_UNIQ = 2;
    private final static int JSON_FORMAT = 3;

    public JSONSection(Map props, boolean withOrder) {
        Object o;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException("empty property for a section"));

        if ((o = props.get("name")) == null)
            throw(new IllegalArgumentException("name of section not defined"));
        name = (String) o;
        if ((o = props.get("path")) == null)
            throw(new IllegalArgumentException("path not defined for "+name));
        path = (String) o;
        if ((o = props.get("indent")) != null && o instanceof String)
            indent = (String) o;
        if ((o = props.get("end")) != null && o instanceof String)
            end = (String) o;
        if ((o = props.get("delimiter")) != null && o instanceof String)
            delimiter = (String) o;
        
        if ((o = props.get("operation")) != null) {
            operation = (String) o;
            if ("each".equalsIgnoreCase(operation))
                oid = JSON_EACH;
            else if ("uniq".equalsIgnoreCase(operation))
                oid = JSON_UNIQ;
            else if ("format".equalsIgnoreCase(operation))
                oid = JSON_FORMAT;
            else {
                operation = "copy";
                oid = JSON_COPY;
            }
        }

        varMap = new HashMap<String, String>();
        if ((o = props.get("variable")) != null && o instanceof Map) {
            Map map = (Map) o;
            for (Object obj : map.keySet()) {
                varMap.put((String) obj, (String) map.get(obj));
            }
        }
        hasVariable = (varMap.size() > 0);
        this.withOrder = withOrder;

        if (oid == JSON_UNIQ && (o = props.get("keypath")) != null)
            keyPath = (String) o;

        pm = new Perl5Matcher();

        // for filters
        filters = JSONFilter.initFilters(name, "filters", props, pm);
        count = filters.length;
    }

    public JSONSection(Map props) {
        this(props, false);
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getOperation() {
        return operation;
    }

    public String fulfill(Map json, AssetList list, Map<String,Object> params) {
        String text; 
        List pl = null;
        Map ph = null;
        Object o;
        if (params == null)
            params = new HashMap<String, Object>();

        if (list == null || (o = params.get("r:_JSON_")) == null ||
            !(o instanceof Map)) // root JSON not defined
            o = JSON2FmModel.get(json, path);
        else { // always retrieve data from the root JSON
            ph = (Map) o;
            o = JSON2FmModel.get(ph, path);
            ph = null;
        }
        if (o == null)
            return "";
        else if (o instanceof List)
            pl = (List) o;
        else if (o instanceof Map)
            ph = (Map) o;
        else if (o instanceof String)
            return (String) o;
        else
            return o.toString();

        switch (oid) {
          case JSON_EACH:
            text=(pl != null) ? each(pl, list, params) : each(ph, list, params);
            break;
          case JSON_UNIQ:
            if (withOrder)
                text = (pl != null) ? uniq(pl, list, params) :
                    JSON2Map.toJSON(ph, indent, end, true);
            else
                text = (pl != null) ? uniq(pl, list, params) :
                    JSON2FmModel.toJSON(ph, indent, end);
            break;
          case JSON_FORMAT:
            text=(pl != null) ? format(pl,list,params) : format(ph,list,params);
            break;
          case JSON_COPY:
            text = (pl != null) ? copy(pl, params) : copy(ph, params);
            break;
          default:
            if (withOrder)
                text = (pl != null) ? JSON2Map.toJSON(pl, indent, end, true) :
                    JSON2Map.toJSON(ph, indent, end, true);
            else
                text = (pl != null) ? JSON2FmModel.toJSON(pl, indent, end) :
                    JSON2FmModel.toJSON(ph, indent, end);
            break;
        }
        return text;
    }

    public String fulfill(List json, AssetList list, Map<String,Object> params){
        String text; 
        List pl = null;
        Map ph = null;
        Object o;
        if (params == null)
            params = new HashMap<String, Object>();

        if (list == null || (o = params.get("r:_JSON_")) == null ||
            !(o instanceof Map)) // root JSON not defined
            o = JSON2FmModel.get(json, path);
        else { // always retrieve data from the root JSON
            ph = (Map) o;
            o = JSON2FmModel.get(ph, path);
            ph = null;
        }
        if (o == null)
            return "";
        else if (o instanceof List)
            pl = (List) o;
        else if (o instanceof Map)
            ph = (Map) o;
        else if (o instanceof String)
            return (String) o;
        else
            return o.toString();

        switch (oid) {
          case JSON_EACH:
            text=(pl != null) ? each(pl, list, params) : each(ph, list, params);
            break;
          case JSON_UNIQ:
            if (withOrder)
                text = (pl != null) ? uniq(pl, list, params) :
                    JSON2Map.toJSON(ph, indent, end, true);
            else
                text = (pl != null) ? uniq(pl, list, params) :
                    JSON2FmModel.toJSON(ph, indent, end);
            break;
          case JSON_FORMAT:
            text=(pl != null) ? format(pl,list,params) : format(ph,list,params);
            break;
          case JSON_COPY:
            text = (pl != null) ? copy(pl, params) : copy(ph, params);
            break;
          default:
            if (withOrder)
                text = (pl != null) ? JSON2Map.toJSON(pl, indent, end, true) :
                    JSON2Map.toJSON(ph, indent, end, true);
            else
                text = (pl != null) ? JSON2FmModel.toJSON(pl, indent, end) :
                    JSON2FmModel.toJSON(ph, indent, end);
            break;
        }
        return text;
    }

    /**
     * returns the copied JSON content for the JSON map with the order
     * support on all the keys. The given parameter map contains the values
     * and their JSON paths for updates.
     */
    private String copy(Map json, Map<String, Object> params) {
        if (json == null)
            return "";

        if (params != null) { // set the values of parameters with valid path
            for (String key : params.keySet()) {
                if (key == null || !key.startsWith("o:" + path))
                    continue;
                JSON2FmModel.put(json, key.substring(2), params.get(key));
            }
        }
        return (withOrder) ? JSON2Map.toJSON(json, indent, end, true) :
            JSON2FmModel.toJSON(json, indent, end);
    }

    /**
     * returns the copied JSON content for the JSON list with the order
     * support on all the keys. The given parameter map contains the values
     * and their JSON paths for updates.
     */
    private String copy(List json, Map<String, Object> params) {
        if (json == null)
            return "";

        if (params != null) {
            for (String key : params.keySet()) {
                if (key == null || !key.startsWith("o:" + path))
                    continue;
                JSON2FmModel.put(json, key.substring(2), params.get(key));
            }
        }
        return (withOrder) ? JSON2Map.toJSON(json, indent, end, true) :
            JSON2FmModel.toJSON(json, indent, end);
    }

    /**
     * loops through each item of the list
     */
    private String each(List json, AssetList list, Map<String, Object> params) {
        int i, j, k, n;
        String text;
        StringBuffer strBuf;
        Object o;
        if (json == null)
            return "";

        strBuf = new StringBuffer();
        k = 0;
        n = json.size();
        for (i=0; i<n; i++) {
            o = json.get(i);
            if (o == null)
                continue;
            else if (o instanceof Map) {
                Map ph = (Map) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(ph, params))
                        break;
                }
                if (j < count) { // found the filter
                    if (!filters[j].hasFormatter())
                        text = (withOrder)?JSON2Map.toJSON(ph,indent,end,true) :
                            JSON2FmModel.toJSON(ph, indent, end);
                    else if (!filters[j].hasSection())
                        text = filters[j].format(ph, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, JSON2FmModel.get(ph,
                                varMap.get(ky)));
                        }
                        try {
                            text = filters[j].format(ph, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(ph, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof List) {
                List pl = (List) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(pl, params))
                        break;
                }
                if (j < count) { // found the filter
                    if (!filters[j].hasFormatter())
                        text = (withOrder)?JSON2Map.toJSON(pl,indent,end,true) :
                            JSON2FmModel.toJSON(pl, indent, end);
                    else if (!filters[j].hasSection())
                        text = filters[j].format(pl, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, JSON2FmModel.get(pl,
                                varMap.get(ky)));
                        }
                        try {
                            text = filters[j].format(pl, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(pl, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            else {
                String value = (o instanceof String) ? (String)o : o.toString();
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value, params))
                        break;
                }
                if (j < count) { // found the filter
                    if (!filters[j].hasFormatter())
                        text = value;
                    else if (!filters[j].hasSection())
                        text = filters[j].format(value, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, value);
                            break;
                        }
                        try {
                            text = filters[j].format(value, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(value, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            if (k > 0)
                strBuf.append(delimiter);
            strBuf.append(text);
            text = null;
            k ++;
        }

        return strBuf.toString();
    }

    /**
     * loops through each key-value pair as if it is a map with a single key
     */
    private String each(Map json, AssetList list, Map<String, Object> params) {
        int i, j, k, n;
        String text, key;
        StringBuffer strBuf;
        Iterator iter;
        Object o;
        if (json == null)
            return "";

        strBuf = new StringBuffer();
        k = 0;
        n = json.size();
        Map<String, Object> ph = new HashMap<String, Object>();
        for (Object obj : json.keySet()) {
            key = (String) obj;
            if (key == null || key.length() <= 0)
                continue;
            o = json.get(key);
            if (o == null)
                continue;

            ph.put("key", key);
            ph.put("value", o);
            for (j=0; j<count; j++) {
                if (filters[j].evaluate(ph, params))
                    break;
            }
            if (j < count) { // found the filter
                if (!filters[j].hasFormatter())
                    text = (withOrder) ? JSON2Map.toJSON(ph, indent, end, true):
                        JSON2FmModel.toJSON(ph, indent, end);
                else if (!filters[j].hasSection())
                    text = filters[j].format(ph, null, params);
                else if (hasVariable) { // with variables
                    for (String ky : varMap.keySet()) { // load variables
                        params.put("v:" + ky, JSON2FmModel.get(ph,
                            varMap.get(ky)));
                    }
                    try {
                        text = filters[j].format(ph, list, params);
                    }
                    catch (Exception e) {
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                        throw(new RuntimeException(e.toString()));
                    }
                    for (String ky : varMap.keySet()) // clean up
                        params.remove("v:" + ky);
                }
                else // without variables
                    text = filters[j].format(ph, list, params);
            }
            else { // nohit
                continue;
            }
            if (k > 0)
                strBuf.append(delimiter);
            strBuf.append(text);
            text = null;
            k ++;
        }

        return strBuf.toString();
    }

    private String format(List json, AssetList list, Map<String,Object> params){
        int j;
        String text = "";
        if (json == null)
            return "";

        for (j=0; j<count; j++) {
            if (filters[j].evaluate(json, params))
                break;
        }
        if (j < count) { // found the filter
            if (!filters[j].hasFormatter())
                text = (withOrder) ? JSON2Map.toJSON(json, indent, end, true) :
                    JSON2FmModel.toJSON(json, indent, end);
            else if (!filters[j].hasSection())
                text = filters[j].format(json, null, params);
            else if (hasVariable) { // with variables
                for (String ky : varMap.keySet()) { // load variables
                    params.put("v:" + ky, JSON2FmModel.get(json,
                        varMap.get(ky)));
                }
                try {
                    text = filters[j].format(json, list, params);
                }
                catch (Exception e) {
                    for (String ky : varMap.keySet()) // clean up
                        params.remove("v:" + ky);
                    throw(new RuntimeException(e.toString()));
                }
                for (String ky : varMap.keySet()) // clean up
                    params.remove("v:" + ky);
            }
            else // without variables
                text = filters[j].format(json, list, params);
        }

        return text;
    }

    private String format(Map json, AssetList list, Map<String,Object> params) {
        int j;
        String text = "";
        if (json == null)
            return "";

        for (j=0; j<count; j++) {
            if (filters[j].evaluate(json, params))
                break;
        }
        if (j < count) { // found the filter
            if (!filters[j].hasFormatter())
                text = (withOrder) ? JSON2Map.toJSON(json, indent, end, true) :
                    JSON2FmModel.toJSON(json, indent, end);
            else if (!filters[j].hasSection())
                text = filters[j].format(json, null, params);
            else if (hasVariable) { // with variables
                for (String ky : varMap.keySet()) { // load variables
                    params.put("v:" + ky, JSON2FmModel.get(json,
                        varMap.get(ky)));
                }
                try {
                    text = filters[j].format(json, list, params);
                }
                catch (Exception e) {
                    for (String ky : varMap.keySet()) // clean up
                        params.remove("v:" + ky);
                    throw(new RuntimeException(e.toString()));
                }
                for (String ky : varMap.keySet()) // clean up
                    params.remove("v:" + ky);
            }
            else // without variables
                text = filters[j].format(json, list, params);
        }

        return text;
    }

    private String uniq(List json, AssetList list, Map<String, Object> params) {
        int i, j, k, n;
        String text, key;
        StringBuffer strBuf;
        Set<String> uniqSet;
        Object o;
        if (json == null)
            return null;

        uniqSet = new HashSet<String>();
        strBuf = new StringBuffer();
        k = 0;
        n = json.size();
        for (i=0; i<n; i++) {
            o = json.get(i);
            if (o == null)
                continue;
            else if (o instanceof Map) {
                Map ph = (Map) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(ph, params))
                        break;
                }
                if (j < count) { // found the filter
                    o = JSON2FmModel.get(ph, keyPath);
                    if (o == null)
                        continue;
                    else if (o instanceof String)
                        key = (String) o;
                    else if (o instanceof Map)
                        key=(withOrder)?JSON2Map.toJSON((Map)o,indent,end,true):
                            JSON2FmModel.toJSON((Map) o, indent, end);
                    else if (o instanceof List)
                       key=(withOrder)?JSON2Map.toJSON((List)o,indent,end,true):
                            JSON2FmModel.toJSON((List) o, indent, end);
                    else
                        key = o.toString();
                    if (key == null || uniqSet.contains(key))
                        continue;
                    uniqSet.add(key);
                    if (!filters[j].hasFormatter())
                        text = key;
                    else if (!filters[j].hasSection())
                        text = filters[j].format(ph, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, JSON2FmModel.get(ph,
                                varMap.get(ky)));
                        }
                        try {
                            text = filters[j].format(ph, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(ph, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof List) {
                List pl = (List) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(pl, params))
                        break;
                }
                if (j < count) { // found the filter
                    o = JSON2FmModel.get(pl, keyPath);
                    if (o == null)
                        continue;
                    else if (o instanceof String)
                        key = (String) o;
                    else if (o instanceof Map)
                        key=(withOrder)?JSON2Map.toJSON((Map)o,indent,end,true):
                            JSON2FmModel.toJSON((Map) o, indent, end);
                    else if (o instanceof List)
                       key=(withOrder)?JSON2Map.toJSON((List)o,indent,end,true):
                            JSON2FmModel.toJSON((List) o, indent, end);
                    else
                        key = o.toString();
                    if (key == null || uniqSet.contains(key))
                        continue;
                    uniqSet.add(key);
                    if (!filters[j].hasFormatter())
                        text = key;
                    else if (!filters[j].hasSection())
                        text = filters[j].format(pl, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, JSON2FmModel.get(pl,
                                varMap.get(ky)));
                        }
                        try {
                            text = filters[j].format(pl, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(pl, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            else {
                String value = (o instanceof String) ? (String)o : o.toString();
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value, params))
                        break;
                }
                if (j < count) { // found the filter
                    key = value;
                    if (key == null || uniqSet.contains(key))
                        continue;
                    uniqSet.add(key);
                    if (!filters[j].hasFormatter())
                        text = value;
                    else if (!filters[j].hasSection())
                        text = filters[j].format(value, null, params);
                    else if (hasVariable) { // with variables
                        for (String ky : varMap.keySet()) { // load variables
                            params.put("v:" + ky, value);
                            break;
                        }
                        try {
                            text = filters[j].format(value, list, params);
                        }
                        catch (Exception e) {
                            for (String ky : varMap.keySet()) // clean up
                                params.remove("v:" + ky);
                            throw(new RuntimeException(e.toString()));
                        }
                        for (String ky : varMap.keySet()) // clean up
                            params.remove("v:" + ky);
                    }
                    else // without variables
                        text = filters[j].format(value, list, params);
                }
                else { // nohit
                    continue;
                }
            }
            if (k > 0)
                strBuf.append(delimiter);
            strBuf.append(text);
            text = null;
            k ++;
        }
        uniqSet.clear();

        return strBuf.toString();
    }

    public void clear() {
        count = 0;
        if (filters != null) {
            for (int i=0; i<filters.length; i++) {
                if (filters[i] != null)
                    filters[i].clear();
                filters[i] = null;
            }
            filters = null;
        }
        if (varMap != null) {
            varMap.clear();
            varMap = null;
        }
    }

    protected void finalize() {
        clear();
    }
}
