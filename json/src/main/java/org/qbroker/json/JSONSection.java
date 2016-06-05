package org.qbroker.json;

/* JSONSecion.java - for a portion JSON data as value */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.AssetList;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONFilter;
import org.qbroker.event.Event;

/**
 * JSONSection represnets a portion of JSON data as value.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSONSection {
    private String name;
    private String path;
    private String operation = "copy";
    private JSONFilter[] filters;
    private Perl5Matcher pm = null;
    private int count = 0;
    private int oid = JSON_COPY;
    private final static int JSON_COPY = 0;
    private final static int JSON_EACH = 1;
    private final static int JSON_UNIQ = 2;
    private final static int JSON_FORMAT = 3;

    public JSONSection(Map props) {
        Object o;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException("empty property for a section"));

        if ((o = props.get("name")) == null)
            throw(new IllegalArgumentException("name of section not defined"));
        name = (String) o;
        if ((o = props.get("path")) == null)
          throw(new IllegalArgumentException("path not defined for "+name));
        path = (String) o;
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

        pm = new Perl5Matcher();

        // for filters
        filters = JSONFilter.initFilters(name, "filters", props, pm);
        count = filters.length;
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

    public String fulfill(Map json, AssetList list, Map params) {
        String text; 
        List pl = null;
        Map ph = null;
        Object o = JSON2FmModel.get(json, path);
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

        if (params == null)
            params = new HashMap();

        switch (oid) {
          case JSON_EACH:
            text=(pl != null) ? each(pl, list, params) : each(ph, list, params);
            break;
          case JSON_UNIQ:
            text=(pl != null) ? uniq(pl) : JSON2FmModel.toJSON(ph, null, null);
            break;
          case JSON_FORMAT:
            text=(pl != null) ? format(pl,list,params) : format(ph,list,params);
            break;
          default:
            text = (pl != null) ? JSON2FmModel.toJSON(pl, null, null) :
                JSON2FmModel.toJSON(ph, null, null);
            break;
        }
        return text;
    }

    public String fulfill(List json, AssetList list, Map params) {
        String text; 
        List pl = null;
        Map ph = null;
        Object o = JSON2FmModel.get(json, path);
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

        if (params == null)
            params = new HashMap();

        switch (oid) {
          case JSON_EACH:
            text=(pl != null) ? each(pl, list, params) : each(ph, list, params);
            break;
          case JSON_UNIQ:
            text = (pl != null) ? uniq(pl) : JSON2FmModel.toJSON(ph, null,null);
            break;
          case JSON_FORMAT:
            text=(pl != null) ? format(pl,list,params) : format(ph,list,params);
            break;
          default:
            text = (pl != null) ? JSON2FmModel.toJSON(pl, null, null) :
                JSON2FmModel.toJSON(ph, null, null);
            break;
        }
        return text;
    }

    private String each(List json, AssetList list, Map params) {
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
                    if (filters[j].evaluate(ph))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        text = filters[j].format(ph, list, params);
                    else
                        text = JSON2FmModel.toJSON(ph, null, null);
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof String) {
                String value = (String) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        text = filters[j].format(value);
                    else
                        text = value;
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof List) {
                List pl = (List) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(pl))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        text = filters[j].format(pl, list, params);
                    else
                        text = JSON2FmModel.toJSON(pl, null, null);
                }
                else { // nohit
                    continue;
                }
            }
            else {
                String value = o.toString();
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        text = filters[j].format(value);
                    else
                        text = value;
                }
                else { // nohit
                    continue;
                }
            }
            if (k > 0)
                strBuf.append(",");
            strBuf.append(text);
            text = null;
            k ++;
        }

        return strBuf.toString();
    }

    private String each(Map json, AssetList list, Map params) {
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
        iter = json.keySet().iterator();
        Map ph = new HashMap();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = json.get(key);
            if (o == null)
                continue;

            ph.put("key", key);
            ph.put("value", o);
            for (j=0; j<count; j++) {
                if (filters[j].evaluate(ph))
                    break;
            }
            if (j < count) { // found the filter
                if (filters[j].hasFormatter())
                    text = filters[j].format(ph, list, params);
                else
                    text = JSON2FmModel.toJSON(ph, null, null);
            }
            else { // nohit
                continue;
            }
            if (k > 0)
                strBuf.append(",");
            strBuf.append(text);
            text = null;
            k ++;
        }

        return strBuf.toString();
    }

    private String format(List json, AssetList list, Map params) {
        int j;
        String text = "";
        if (json == null)
            return "";

        for (j=0; j<count; j++) {
            if (filters[j].evaluate(json))
                break;
        }
        if (j < count) { // found the filter
            if (filters[j].hasFormatter())
                text = filters[j].format(json, list, params);
            else
                text = JSON2FmModel.toJSON(json, null, null);
        }

        return text;
    }

    private String format(Map json, AssetList list, Map params) {
        int j;
        String text = "";
        if (json == null)
            return "";

        for (j=0; j<count; j++) {
            if (filters[j].evaluate(json))
                break;
        }
        if (j < count) { // found the filter
            if (filters[j].hasFormatter())
                text = filters[j].format(json, list, params);
            else
                text = JSON2FmModel.toJSON(json, null, null);
        }

        return text;
    }

    private String uniq(List json) {
        int i, j, k, n;
        String text, key;
        StringBuffer strBuf;
        Map map;
        Object o;
        if (json == null)
            return null;

        map = new HashMap();
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
                    if (filters[j].evaluate(ph))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        key = filters[j].format(ph, null, null);
                    else
                        key = JSON2FmModel.toJSON(ph, null, null);
                    if (key == null || map.containsKey(key))
                        continue;
                    map.put(key, key);
                    text = (!filters[j].hasFormatter()) ? key :
                        JSON2FmModel.toJSON(ph, null, null);
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof String) {
                String value = (String) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        key = filters[j].format(value);
                    else
                        key = value;
                    if (key == null || map.containsKey(key))
                        continue;
                    map.put(key, key);
                    text = value;
                }
                else { // nohit
                    continue;
                }
            }
            else if (o instanceof List) {
                List pl = (List) o;
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(pl))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        key = filters[j].format(pl, null, null);
                    else
                        key = JSON2FmModel.toJSON(pl, null, null);
                    if (key == null || map.containsKey(key))
                        continue;
                    map.put(key, key);
                    text = (!filters[j].hasFormatter()) ? key :
                        JSON2FmModel.toJSON(pl, null, null);
                }
                else { // nohit
                    continue;
                }
            }
            else {
                String value = o.toString();
                for (j=0; j<count; j++) {
                    if (filters[j].evaluate(value))
                        break;
                }
                if (j < count) { // found the filter
                    if (filters[j].hasFormatter())
                        key = filters[j].format(value);
                    else
                        key = value;
                    if (key == null || map.containsKey(key))
                        continue;
                    map.put(key, key);
                    text = value;
                }
                else { // nohit
                    continue;
                }
            }
            if (k > 0)
                strBuf.append(",");
            strBuf.append(text);
            text = null;
            k ++;
        }
        map.clear();

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
    }
}
