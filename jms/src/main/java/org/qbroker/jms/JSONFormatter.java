package org.qbroker.jms;

/* JSONFormatter.java - a formatter for JSON data for JMS Messages */

import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONFilter;
import org.qbroker.json.JSONSelector;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * JSONFormatter formats JSON payload in a message with a sequence of
 * operations. Certain operation may contain a template and a substitution to
 * format the JSON text from of both the message and the JSON payload. Then
 * the result will be parsed into JSON data and applied to the JSON payload.
 * Currently, the supported operations are get, set, parse, merge, union,
 * select, remove, first, last, sort and uniq, etc.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSONFormatter {
    private String name;
    private int[] dataType, action;
    private JSONSelector[] selector;
    private Template[] template;
    private TextSubstitution[] substitution;
    private String[] keyPath;
    private final static int JSON_GET = 1;
    private final static int JSON_SET = 2;
    private final static int JSON_PARSE = 3;
    private final static int JSON_MERGE = 4;
    private final static int JSON_UNION = 5;
    private final static int JSON_SELECT = 6;
    private final static int JSON_REMOVE = 7;
    private final static int JSON_FIRST = 8;
    private final static int JSON_LAST = 9;
    private final static int JSON_MIN = 10;
    private final static int JSON_MAX = 11;
    private final static int JSON_SORT = 12;
    private final static int JSON_UNIQ = 13;
    private final static int JSON_EACH = 14;

    public JSONFormatter(Map props) {
        Object o;
        String str;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException("Empty property for formatter"));

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not well defined"));
        name = (String) o;

        if ((o = props.get("JSONFormatter")) != null && o instanceof List) {
            Map map;
            List list = (List) o;
            int k = list.size();
            keyPath = new String[k];
            dataType = new int[k];
            action = new int[k];
            selector = new JSONSelector[k];
            template = new Template[k];
            substitution = new TextSubstitution[k];
            for (int i=0; i<k; i++) {
                keyPath[i] = null;
                dataType[i] = JSON2Map.JSON_UNKNOWN;
                action[i] = 0;
                selector[i] = null;
                template[i] = null;
                substitution[i] = null;
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                map = (Map) o;
                if ((o = map.get("DataType")) != null && o instanceof String) {
                    str = (String) o;
                    if ("string".equals(str))
                        dataType[i] = JSON2Map.JSON_STRING;
                    else if ("long".equals(str))
                        dataType[i] = JSON2Map.JSON_NUMBER;
                    else if ("map".equals(str))
                        dataType[i] = JSON2Map.JSON_MAP;
                    else if ("list".equals(str))
                        dataType[i] = JSON2Map.JSON_ARRAY;
                    else if ("double".equals(str))
                        dataType[i] = JSON2Map.JSON_NUMBER;
                    else if ("boolean".equals(str))
                        dataType[i] = JSON2Map.JSON_BOOLEAN;
                    else
                        dataType[i] = JSON2Map.JSON_STRING;
                }

                if ((o = map.get("Selector")) != null)
                    selector[i] = new JSONSelector(name, o);

                if ((o = map.get("Template")) != null && o instanceof String) {
                    template[i] = new Template((String) o);
                    if ((o = map.get("Substitution")) != null &&
                        o instanceof String)
                        substitution[i] = new TextSubstitution((String) o);
                }
                if ((o = map.get("JSONPath")) != null && o instanceof String)
                    keyPath[i] = (String) o;
                if ((o = map.get("Operation")) != null && o instanceof String)
                    str = (String) o;
                else
                    str = "none";
                if ("get".equalsIgnoreCase(str))
                    action[i] = JSON_GET;
                else if ("set".equalsIgnoreCase(str))
                    action[i] = JSON_SET;
                else if ("parse".equalsIgnoreCase(str))
                    action[i] = JSON_PARSE;
                else if ("merge".equalsIgnoreCase(str))
                    action[i] = JSON_MERGE;
                else if ("union".equalsIgnoreCase(str))
                    action[i] = JSON_UNION;
                else if ("select".equalsIgnoreCase(str))
                    action[i] = JSON_SELECT;
                else if ("remove".equalsIgnoreCase(str))
                    action[i] = JSON_REMOVE;
                else if ("first".equalsIgnoreCase(str))
                    action[i] = JSON_FIRST;
                else if ("last".equalsIgnoreCase(str))
                    action[i] = JSON_LAST;
                else if ("min".equalsIgnoreCase(str))
                    action[i] = JSON_MIN;
                else if ("max".equalsIgnoreCase(str))
                    action[i] = JSON_MAX;
                else if ("sort".equalsIgnoreCase(str))
                    action[i] = JSON_SORT;
                else if ("uniq".equalsIgnoreCase(str))
                    action[i] = JSON_UNIQ;
                else if ("each".equalsIgnoreCase(str))
                    action[i] = JSON_EACH;
            }
        }
    }

    public int format(Object json, Message msg) {
        int i, j, k, n;
        boolean isList;
        Object o;
        if (json == null || msg == null)
            return -1;
        isList = (json instanceof List);
        if (!isList && !(json instanceof Map))
            return -2;

        k = 0;
        n = keyPath.length;
        for (i=0; i<n; i++) {
            switch (action[i]) {
              case JSON_GET:
                o = get(i, json);
                if (o == null)
                    return -1;
                else if (o instanceof List) {
                    json = (List) o;
                    isList = true;
                    k++;
                }
                else if (o instanceof Map) {
                    json = (Map) o;
                    isList = false;
                    k++;
                }
                else
                    return -2;
                break;
              case JSON_SET:
                try {
                    j = set(i, null, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in set/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (j > 0)
                    k ++;
                break;
              case JSON_MERGE:
                try {
                    j = merge(1, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in merge/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (j > 0)
                    k ++;
                break;
              case JSON_UNION:
                try {
                    j = merge(0, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in union/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (j > 0)
                    k ++;
                break;
              case JSON_PARSE:
                try {
                    o = parse(0, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in parse/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null)
                    k ++;
                break;
              case JSON_SELECT:
                try {
                    o = parse(1, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in select/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null)
                    k ++;
                break;
              case JSON_REMOVE:
                try {
                    o = parse(-1, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in remove/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null)
                    k ++;
                break;
              case JSON_EACH:
                try {
                    o = each(1, i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in each/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null) {
                    json = o;
                    isList = (json instanceof List);
                    k ++;
                }
                break;
              case JSON_FIRST:
                try {
                    o = first(i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in first/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null) { // json changed
                    if (dataType[i] == JSON2Map.JSON_UNKNOWN)
                        k ++;
                    else if (dataType[i] == JSON2Map.JSON_MAP &&
                        o instanceof Map) { // for replacement
                        json = (Map) o;
                        isList = false;
                        k ++;
                    }
                    else if (dataType[i] == JSON2Map.JSON_ARRAY &&
                        o instanceof List) { // for replacement
                        json = (List) o;
                        isList = true;
                        k ++;
                    }
                    else { // wrong data type
                        new Event(Event.ERR, name + ": failed to format json " +
                            "in first/" + i + " with wrong data type: " +
                            dataType[i] + "/" + o.getClass().getName()).send();
                        return -1;
                    }
                }
                break;
              case JSON_LAST:
                try {
                    o = last(i, json, msg);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in last/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null) { // json changed
                    if (dataType[i] == JSON2Map.JSON_UNKNOWN)
                        k ++;
                    else if (dataType[i] == JSON2Map.JSON_MAP &&
                        o instanceof Map) { // for replacement
                        json = (Map) o;
                        isList = false;
                        k ++;
                    }
                    else if (dataType[i] == JSON2Map.JSON_ARRAY &&
                        o instanceof List) { // for replacement
                        json = (List) o;
                        isList = true;
                        k ++;
                    }
                    else { // wrong data type
                        new Event(Event.ERR, name + ": failed to format json " +
                            "in last/" + i + " with wrong data type: " +
                            dataType[i] + "/" + o.getClass().getName()).send();
                        return -1;
                    }
                }
                break;
              case JSON_SORT:
                try {
                    o = sort(i, json);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in sort/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null)
                    k ++;
                break;
              case JSON_UNIQ:
                try {
                    o = uniq(i, json);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to format json " +
                        "in uniq/" + i + ": " + Event.traceStack(e)).send();
                    return -1;
                }
                if (o != null)
                    k ++;
                break;
              default:
            }
        }

        if (k > 0) try { // payload has changed
            String text = (isList) ? JSON2FmModel.toJSON((List) json, null, ""):
                JSON2FmModel.toJSON((Map) json, null, "");
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(text);
            else
                ((BytesMessage) msg).writeBytes(text.getBytes());
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to load json to msg: " +
                Event.traceStack(e)).send();
            return -1;
        }

        return k;
    }

    /**
     * It sets the data formatted from msg to the json path and returns a
     * positive integer for success. Otherwise, it returns 0 for no change
     * or -1 for failure
     */
    private int set(int id, String key, Object obj, Message msg)
        throws JMSException {
        Reader sr;
        Object o;
        String value = null;
        int i, k;
        boolean isList = (obj instanceof List);

        i = 1;
        if (key == null)
            key = keyPath[id];
        k = dataType[id];
        if (k == JSON2Map.JSON_UNKNOWN)
            k = (isList) ? JSON2FmModel.getDataType((List) obj, key) :
                JSON2FmModel.getDataType((Map) obj, key);

        if (template[id] == null)
            return -1;
        else if (template[id].numberOfFields() <= 0) // static value
            value = template[id].copyText(); 
        else if (isList)
            value = format((List) obj, msg, template[id]);
        else
            value = format((Map) obj, msg, template[id]);

        if (substitution[id] != null)
            value = substitution[id].substitute(value);

        switch (k) {
          case JSON2Map.JSON_NULL:
            if (isList)
                JSON2FmModel.put((List) obj, key, null);
            else
                JSON2FmModel.put((Map) obj, key, null);
            break;
          case JSON2Map.JSON_STRING:
            if (isList)
                JSON2FmModel.put((List) obj, key, value);
            else
                JSON2FmModel.put((Map) obj, key, value);
            break;
          case JSON2Map.JSON_NUMBER:
            if (value.indexOf(".") >= 0)
                o = new Double(value);
            else
                o = new Long(value);
            if (isList)
                JSON2FmModel.put((List) obj, key, o);
            else
                JSON2FmModel.put((Map) obj, key, o);
            break;
          case JSON2Map.JSON_BOOLEAN:
            if (isList)
                JSON2FmModel.put((List) obj, key, new Boolean(value));
            else
                JSON2FmModel.put((Map) obj, key, new Boolean(value));
            break;
          case JSON2Map.JSON_ARRAY:
          case JSON2Map.JSON_MAP:
            try {
                sr = new StringReader(value);
                o = JSON2FmModel.parse(sr);
                sr.close();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to parse json '" +
                    value + "': " + Event.traceStack(e)).send();
                return -1;
            }
            sr = null;
            if (isList)
                JSON2FmModel.put((List) obj, key, o);
            else
                JSON2FmModel.put((Map) obj, key, o);
            break;
          default:
            i = 0;
        }
        return i;
    }

    /**
     * It gets the data at the json path from the json payload and returns a
     * non-null object for success. Otherwise, it returns null for no change.
     */
    private Object get(int id, Object obj) {
        boolean isList = (obj instanceof List);
        if (selector[id] == null) { // normal get without selector
            if (isList)
                return JSON2FmModel.get((List) obj, keyPath[id]);
            else
                return JSON2FmModel.get((Map) obj, keyPath[id]);
        }
        else { // with selector
            Object o;
            String str = null;
            int k = selector[id].getType();
            if (isList)
                o = JSON2FmModel.get((List) obj, keyPath[id]);
            else
                o = JSON2FmModel.get((Map) obj, keyPath[id]);

            if (o == null) // no such data
                return null;
            else if (o instanceof Map && k == 1) { // select on keys only
                Map map = (Map) o;
                Object[] keys = map.keySet().toArray();
                int n = keys.length;
                for (int i=0; i<n; i++) {
                    o = keys[i];
                    if (o == null || !(o instanceof String))
                        continue;
                    str = (String) o;
                    if (!selector[id].evaluate(str))
                        map.remove(str);
                }
                return map;
            }
            else if (o instanceof List && k == 3) { // select on json props
                List list = (List) o;
                int n = list.size();
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map) {
                        if (!selector[id].evaluate((Map) o))
                            list.remove(i);
                    }
                    else if (o instanceof List) {
                        if (!selector[id].evaluate((List) o))
                            list.remove(i);
                    }
                }
                return list;
            }
            else if (o instanceof List && k >= 1 && k <= 2) { //select on values
                List list = (List) o;
                int n = list.size();
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null || o instanceof Map || o instanceof List)
                        continue;
                    str = o.toString();
                    if (!selector[id].evaluate(str))
                        list.remove(i);
                }
                return list;
            }
        }
        return null;
    }

    /**
     * It supports three options on the data at the json path from the json
     * payload. They are 1 for select, 0 for parse and -1 for remove. If the
     * json payload has been changed, it returns a non-null object. Otherwise,
     * it returns null for no change. If the template is defined, it will be
     * used to generate the key to store the data. For multiple keys in a map, 
     * the dataType will be used to control if to save the properties to msg.
     */
    private Object parse(int option, int id, Object obj, Message msg)
        throws JMSException {
        boolean isList = (obj instanceof List);
        Object o;

        if (selector[id] == null) {
            String key = null;
            if (template[id] != null) { // dataField is defined
                key = (isList) ? format((List) obj, msg, template[id]) :
                    format((Map) obj, msg, template[id]);
                if (substitution[id] != null)
                    key = substitution[id].substitute(key);
            }

            if (option >= 0) { // for select and parse
                o = (isList) ? JSON2FmModel.get((List) obj, keyPath[id]) :
                    JSON2FmModel.get((Map) obj, keyPath[id]);
            }
            else { // for remove
                o = (isList) ? JSON2FmModel.remove((List) obj, keyPath[id]) :
                    JSON2FmModel.remove((Map) obj, keyPath[id]);
            }

            if (o == null || key == null) // no data or no key defined
                return (option >= 0) ? null : o;

            // store content to msg
            if (o instanceof String)
                msg.setStringProperty(key, (String) o);
            else if (o instanceof List)
                msg.setStringProperty(key,
                    JSON2FmModel.toJSON((List) o, null, ""));
            else if (o instanceof Map)
                msg.setStringProperty(key,
                    JSON2FmModel.toJSON((Map) o, null, ""));
            else
                msg.setStringProperty(key, o.toString());

            return (option >= 0) ? null : o;
        }
        else { // with selector
            String str = null;
            int k = selector[id].getType();

            if (isList)
                o = JSON2FmModel.get((List) obj, keyPath[id]);
            else
                o = JSON2FmModel.get((Map) obj, keyPath[id]);

            if (o == null) // no such data
                return null;
            else if (o instanceof Map && k == 1) { // select on keys only
                Map map = (Map) o;
                Object[] keys = map.keySet().toArray();
                int n = keys.length;
                for (int i=0; i<n; i++) {
                    o = keys[i];
                    if (o == null || !(o instanceof String))
                        continue;
                    str = (String) o;
                    if (!selector[id].evaluate(str)) { // not selected
                        if (option > 0) // for select
                            map.remove(str);
                        continue;
                    }
                    // str is selected
                    if (option < 0) { // for remove
                        o = map.remove(str);
                        if (dataType[id] == JSON2Map.JSON_UNKNOWN) // no save
                            continue;
                    }
                    else if (option > 0) { // for select
                        if (dataType[id] == JSON2Map.JSON_UNKNOWN) // no save
                            continue;
                        o = map.get(str);
                    }
                    else {
                         o = map.get(str);
                    }
                    if (o == null)
                        continue;
                    // save to msg as the property
                    if (o instanceof String)
                        msg.setStringProperty(str, (String) o);
                    else if (o instanceof List)
                        msg.setStringProperty(str,
                            JSON2FmModel.toJSON((List) o, null, ""));
                    else if (o instanceof Map)
                        msg.setStringProperty(str,
                            JSON2FmModel.toJSON((Map) o, null, ""));
                    else
                        msg.setStringProperty(str, o.toString());
                }
                if (n > map.size())
                    return map;
                else
                    return null;
            }
            else if (o instanceof List && (k == 1 || k == 2) &&
                dataType[id] != JSON2Map.JSON_UNKNOWN) { // select on indices
                List list = (List) o;
                int n = list.size();
                String key = null;
                if (template[id] != null) { // dataField is defined
                    key = (isList) ? format((List) obj, msg, template[id]):
                        format((Map) obj, msg, template[id]);
                    if (substitution[id] != null)
                        key = substitution[id].substitute(key);
                }
                for (int i=n-1; i>=0; i--) {
                    str = String.valueOf(i);
                    if (!selector[id].evaluate(str)) { // not selected
                        if (option > 0) // for select
                            list.remove(i);
                        continue;
                    }
                    // str selected
                    o = (option >= 0) ? list.get(i) : list.remove(i);
                    if (o == null || key == null)
                        continue;
                    // save to msg as the property
                    str = key + "_" + i;
                    if (o instanceof String)
                        msg.setStringProperty(str, (String) o);
                    else if (o instanceof List)
                        msg.setStringProperty(str,
                            JSON2FmModel.toJSON((List) o, null, ""));
                    else if (o instanceof Map)
                        msg.setStringProperty(str,
                            JSON2FmModel.toJSON((Map) o, null, ""));
                    else
                        msg.setStringProperty(str, o.toString());
                }
                if (n > list.size())
                    return list;
                else
                    return null;
            }
            else if(o instanceof List && (k == 1 || k == 2)){ //select on values
                List list = (List) o;
                int n = list.size();
                String key = null;
                if (template[id] != null) { // dataField is defined
                    key = (isList) ? format((List) obj, msg, template[id]):
                        format((Map) obj, msg, template[id]);
                    if (substitution[id] != null)
                        key = substitution[id].substitute(key);
                }
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null || o instanceof Map || o instanceof List)
                        continue;
                    str = o.toString();
                    if (!selector[id].evaluate(str)) { // not selected
                        if (option > 0) // for select
                            list.remove(i);
                        continue;
                    }
                    else { // str selected
                        if (option < 0) // for remove
                            list.remove(i);
                        if (key != null)
                            msg.setStringProperty(key + "_" + i, str);
                    }
                }
                if (n > list.size())
                    return list;
                else
                    return null;
            }
            else if (o instanceof List && k == 3 && option != 0) {
                // select on json properties
                List list = (List) o;
                int n = list.size();
                for (int i=n-1; i>=0; i--) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map) {
                        if (!selector[id].evaluate((Map) o)) {
                            if (option > 0) // for select
                                list.remove(i);
                        }
                        else if (option < 0) // for remove
                            list.remove(i);
                    }
                    else if (o instanceof List) {
                        if (!selector[id].evaluate((List) o)) {
                            if (option > 0) // for select
                                list.remove(i);
                        }
                        else if (option < 0) // for remove
                            list.remove(i);
                    }
                }
                if (n > list.size())
                    return list;
                else
                    return null;
            }
        }
        return null;
    }

    /**
     * It gets a json List via the json path from the json payload. Then it
     * loops through the list item by item to apply the template on both the
     * item and the msg. The result will be set back to the item via the json
     * path defined in the selector. If the json payload has been changed, it
     * returns a non-null object. Otherwise, it returns null for no change.
     */
    private Object each(int option, int id, Object obj, Message msg)
        throws JMSException, IOException {
        boolean isList = (obj instanceof List);
        Object o;

        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o == null)
            return null;
        else if (o instanceof List) { // loop on values
            List list = (List) o;
            int k = 0, n = list.size();
            if (selector[id] == null) { // for default key = "."
                int j;
                String str;
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null || o instanceof Map || o instanceof List)
                        continue;
                    str = format(o.toString(), msg, template[id]);
                    if (substitution[id] != null)
                        str = substitution[id].substitute(str);
                    list.set(i, str);
                    k ++;
                }
            }
            else if (selector[id].getType() == 3) { // with selector
                int j;
                String key = selector[id].getValue("JSONPath");
                if (key == null)
                    return null;
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map) {
                        if (!selector[id].evaluate((Map) o))
                            continue;
                        j = set(id, key, (Map) o, msg);
                        if (j > 0)
                            k ++;
                    }
                    else if (o instanceof List) {
                        if (!selector[id].evaluate((List) o))
                            continue;
                        j = set(id, key, (List) o, msg);
                        if (j > 0)
                            k ++;
                    }
                }
            }
            else if (selector[id].getType() == 1) { // without selector
                int j;
                String key = selector[id].getValue();
                if (key == null)
                    return null;
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    if (o instanceof Map) {
                        j = set(id, key, (Map) o, msg);
                        if (j > 0)
                            k ++;
                    }
                    else if (o instanceof List) {
                        j = set(id, key, (List) o, msg);
                        if (j > 0)
                            k ++;
                    }
                }
            }
            if (k > 0)
                return obj;
        }
        else if (o instanceof Map) { // loop thru a list of {"key":x, "value":y}
            String key, str;
            Object[] keys;
            Map ph = (Map) o;
            int m = 0, n;
            if (selector[id] == null)
                key = ".";
            else if ((m = selector[id].getType()) == 1)
                key = selector[id].getValue();
            else if (m == 3)
                key = selector[id].getValue("JSONPath");
            else
                return null;
            if (key == null)
                return null;
            keys = ph.keySet().toArray();
            n = keys.length;
            if (".".equals(key)) { // replace the looped item to build a list
                List list = new ArrayList();
                str = template[id].copyText();
                if ("##.key##".equals(str)) { // a list of keys
                    for (int i=0; i<n; i++)
                        list.add(keys[i]);
                }
                else if ("##.value##".equals(str)) { // a list of values
                    for (int i=0; i<n; i++)
                        list.add(ph.get(keys[i]));
                }
                else { // arbitrary template
                    StringReader sr = null;
                    Map map = new HashMap();
                    StringBuffer strBuf = new StringBuffer();
                    int k = dataType[id];
                    for (int i=0; i<n; i++) {
                        str = (String) keys[i];
                        map.put("key", str);
                        map.put("value", ph.get(str));
                        if (m == 3 && !selector[id].evaluate(map)) {
                            map.clear();
                            continue;
                        }
                        str = format(map, msg, template[id]);
                        if (substitution[id] != null)
                            str = substitution[id].substitute(str);
                        if (strBuf.length() > 0)
                            strBuf.append(",");
                        if (k == JSON2Map.JSON_STRING ||
                            k == JSON2Map.JSON_UNKNOWN)
                            strBuf.append("\"" + str + "\"");
                        else
                            strBuf.append(str);
                        map.clear();
                    }
                    strBuf.insert(0, "[");
                    strBuf.append("]");
                    sr = new StringReader(strBuf.toString());
                    list = (List) JSON2FmModel.parse(sr);
                    sr.close();
                }

                if (".".equals(keyPath[id]))
                    return list;

                if (isList)
                    JSON2FmModel.put((List) obj, keyPath[id], list);
                else
                    JSON2FmModel.put((Map) obj, keyPath[id], list);
                return obj;
            }
            else if (".key".equals(key)) { // rename the keys of the map
                Map map = new HashMap();
                int k = 0;
                for (int i=0; i<n; i++) {
                    str = (String) keys[i];
                    map.put("key", str);
                    map.put("value", ph.remove(str));
                    if (m == 3 && !selector[id].evaluate(map)) {
                        map.clear();
                        continue;
                    }
                    if (set(id, key, map, msg) > 0) { // map changed
                        str = (String) map.get("key");
                        ph.put(str, map.remove(str));
                        k ++;
                    }
                    map.clear();
                }
                if (k > 0)
                    return obj;
            }
            else { // modify the values of the map
                Map map = new HashMap();
                int k = 0;
                for (int i=0; i<n; i++) {
                    str = (String) keys[i];
                    map.put("key", str);
                    map.put("value", ph.get(str));
                    if (m == 3 && !selector[id].evaluate(map)) {
                        map.clear();
                        continue;
                    }
                    if (set(id, key, map, msg) > 0) { // map changed
                        ph.put(str, map.remove(str));
                        k ++;
                    }
                    map.clear();
                }
                if (k > 0)
                    return obj;
            }
        }

        return null;
    }

    /**
     * It selects the first data at the json path from the json payload and
     * returns a non-null object for success. Otherwise, it returns null for
     * no change.
     */
    private Object first(int id, Object obj, Message msg) throws JMSException {
        boolean isList = (obj instanceof List);
        Object o;
        String str = null;
        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o == null) // no such data
            return null;
        else if (o instanceof Map) { // select the minimum key
            Map map = (Map) o;
            str = minKey(map);
            o = (str != null) ? map.get(str) : null;
            if (o == null) // got nothing
                return null;
            if (template[id] != null) try { // set the property
                String key=(isList) ? format((List) obj, msg, template[id]):
                    format((Map) obj, msg, template[id]);
                if (substitution[id] != null)
                    key = substitution[id].substitute(key);
                MessageUtils.setProperty(key, str, msg);
            }
            catch (JMSException e) {
                new Event(Event.ERR, name + ": failed to set property of '" +
                    str + "': " + Event.traceStack(e)).send();
                return null;
            }
            if (dataType[id] != JSON2Map.JSON_UNKNOWN) { // for replace
                return o;
            }
            // for select
            Object[] keys = map.keySet().toArray();
            int n = keys.length;
            for (int i=0; i<n; i++) {
                o = keys[i];
                if (o == null || !(o instanceof String))
                    continue;
                str = (String) o;
                if (!str.equals((String) o))
                    map.remove((String) o);
            }
            if (n > map.size())
                return map;
            else
                return null;
        }
        else if (o instanceof List) { // select on first item
            List list = (List) o;
            int n = list.size();
            o = (n > 0) ? list.get(0) : null;
            if (o == null) // got nothing
                return null;
            str = "0";
            if (template[id] != null) try { // set the property
                String key=(isList) ? format((List) obj, msg, template[id]):
                    format((Map) obj, msg, template[id]);
                if (substitution[id] != null)
                    key = substitution[id].substitute(key);
                MessageUtils.setProperty(key, str, msg);
            }
            catch (JMSException e) {
                new Event(Event.ERR, name + ": failed to set property of '" +
                    str + "': " + Event.traceStack(e)).send();
                return null;
            }
            if (dataType[id] != JSON2Map.JSON_UNKNOWN) { // for replace
                return o;
            }
            // for select
            for (int i=1; i<n; i++)
                list.remove(i);
            if (n > list.size())
                return list;
            else
                return null;
        }
        return null;
    }

    /**
     * It selects the last data at the json path from the json payload and
     * returns a non-null object for success. Otherwise, it returns null for
     * no change.
     */
    private Object last(int id, Object obj, Message msg) throws JMSException {
        boolean isList = (obj instanceof List);
        Object o;
        String str = null;
        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o == null) // no such data
            return null;
        else if (o instanceof Map) { // select the maximum key
            Map map = (Map) o;
            str = maxKey(map);
            o = (str != null) ? map.get(str) : null;
            if (o == null) // got nothing
                return null;
            if (template[id] != null) { // set the property
                String key=(isList) ? format((List) obj, msg, template[id]):
                    format((Map) obj, msg, template[id]);
                if (substitution[id] != null)
                    key = substitution[id].substitute(key);
                MessageUtils.setProperty(key, str, msg);
            }
            if (dataType[id] != JSON2Map.JSON_UNKNOWN) // for replacement
                return o;

            // for select
            Object[] keys = map.keySet().toArray();
            int n = keys.length;
            for (int i=0; i<n; i++) {
                o = keys[i];
                if (o == null || !(o instanceof String))
                    continue;
                str = (String) o;
                if (!str.equals((String) o))
                    map.remove((String) o);
            }
            if (n > map.size())
                return map;
            else
                return null;
        }
        else if (o instanceof List) { // select on last item
            List list = (List) o;
            int n = list.size();
            o = (n > 0) ? list.get(n-1) : null;
            if (o == null) // got nothing
                return null;
            str = String.valueOf(n-1);
            if (template[id] != null) { // set the property
                String key=(isList) ? format((List) obj, msg, template[id]):
                    format((Map) obj, msg, template[id]);
                if (substitution[id] != null)
                    key = substitution[id].substitute(key);
                MessageUtils.setProperty(key, str, msg);
            }
            if (dataType[id] != JSON2Map.JSON_UNKNOWN) // for replacement
                return o;

            // for select
            for (int i=n-2; i>=0; i--)
                list.remove(i);
            if (n > list.size())
                return list;
            else
                return null;
        }
        return null;
    }

    /**
     * It merges json data generated from the template on the message to the
     * the json data at the json path of the json payload. There are two
     * options, 1 for merge and 0 for union. Upon success, it returns number
     * of objects changed. Otherwise, it returns -1 for failure or 0 for
     * no change.
     */
    private int merge(int option, int id, Object obj, Message msg)
        throws JMSException {
        StringReader sr;
        Object o;
        String value, str = null;
        int n = 0;
        boolean isList = (obj instanceof List);

        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o == null) // just set object
            return set(id, null, obj, msg);

        if (template[id] == null)
            return -1;
        else if (template[id].numberOfFields() <= 0) // static value
            value = template[id].copyText(); 
        else if (isList)
            value = format((List) obj, msg, template[id]);
        else
            value = format((Map) obj, msg, template[id]);

        if (substitution[id] != null)
            value = substitution[id].substitute(value);

        if (o instanceof Map && dataType[id] == JSON2Map.JSON_MAP) {
            Map map = (Map) o;
            Map ph;
            try {
                sr = new StringReader(value);
                ph = (Map) JSON2FmModel.parse(sr);
                sr.close();
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to parse json map '" +
                    value + "': " + Event.traceStack(e)).send();
                return -1;
            }
            sr = null;
            Iterator iter = ph.keySet().iterator();
            if (selector[id] == null || selector[id].getType() != 1) {
                while (iter.hasNext()) { // override all keys
                    str = (String) iter.next();
                    if (str == null || str.length() <= 0)
                        continue;
                    if (option == 0 && map.containsKey(str)) // skip for union
                        continue;
                    o = ph.get(str);
                    if (o != null) {
                        map.put(str, o);
                        n ++;
                    }
                }
            }
            else { // override selected keys only
                while (iter.hasNext()) {
                    str = (String) iter.next();
                    if (str == null || str.length() <= 0)
                        continue;
                    if (!selector[id].evaluate(str))
                        continue;
                    if (option == 0 && map.containsKey(str)) // skip for union
                        continue;
                    o = ph.get(str);
                    if (o != null) {
                        map.put(str, o);
                        n ++;
                    }
                }
            }
        }
        else if (o instanceof List && option > 0) { // for merge only
            List list = (List) o;
            List pl;
            switch (dataType[id]) {
              case JSON2Map.JSON_ARRAY:
                try {
                    sr = new StringReader(value);
                    pl = (List) JSON2FmModel.parse(sr);
                    sr.close();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to parse json list '"+
                        value + "': " + Event.traceStack(e)).send();
                    return -1;
                }
                sr = null;
                int k = pl.size();
                for (int i=0; i<k; i++) { // append
                    o = pl.get(i);
                    if (o != null) {
                        list.add(o);
                        n ++;
                    }
                }
                break;
              case JSON2Map.JSON_MAP:
                try {
                    sr = new StringReader(value);
                    o = (Map) JSON2FmModel.parse(sr);
                    sr.close();
                }
                catch (Exception e) {
                    new Event(Event.ERR, name + ": failed to parse json map '"+
                        value + "': " + Event.traceStack(e)).send();
                    return -1;
                }
                sr = null;
                list.add(o);
                n ++;
                break;
              case JSON2Map.JSON_STRING:
                list.add(value);
                n ++;
                break;
              case JSON2Map.JSON_NUMBER:
                if (value.indexOf(".") >= 0)
                    list.add(new Double(value));
                else
                    list.add(new Long(value));
                n ++;
                break;
              case JSON2Map.JSON_BOOLEAN:
                list.add(new Boolean(value));
                n ++;
                break;
              default:
            }
        }
        else {
            new Event(Event.ERR, name + ": data type " +o.getClass().getName()+
                "/" + id + " is not supported for merge/ "+ id).send();
            n = -1;
        }

        return n;
    }

    /**
     * It gets a list of json data at the json path from the json payload and
     * sorts the list with keys. The key is generated by the template on the
     * json data. If the json payload has been changed, it returns the list.
     * Otherwise, it returns null for no change.
     */
    private Object sort(int id, Object obj) {
        boolean isList = (obj instanceof List);
        Object o;
        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o != null && o instanceof List) { // for a list
            List list = (List) o;
            List pl = new ArrayList();
            Map map = new HashMap();
            String[] keys;
            int j = 0, k = 0;
            int n = list.size();
            switch (dataType[id]) {
              case JSON2Map.JSON_STRING:
                j = n - 1;
                for (int i=n-1; i>=0; i--) { //remove nulls and swap non-strings
                    o = list.get(i);
                    if (o == null) {
                        list.remove(i);
                        j --;
                    }
                    else if (!(o instanceof String)) {
                        if (i < j) { // swap
                            list.set(i, list.get(j));
                            list.set(j, o);
                        }
                        j --;
                    }
                }
                n = j;
                keys = new String[n];
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o != null && o instanceof String) {
                        keys[k] = (String) o;
                        k ++;
                    }
                }
                if (k > 1) {
                    Arrays.sort(keys, 0, k);
                    for (int i=0; i<k; i++)
                        list.set(i, keys[i]);
                }
                break;
              case JSON2Map.JSON_NUMBER:
                j = n - 1;
                for (int i=n-1; i>=0; i--) { //remove nulls and swap non-numbers
                    o = list.get(i);
                    if (o == null) {
                        list.remove(i);
                        j --;
                    }
                    else if (!(o instanceof Number)) {
                        if (i < j) { // swap
                            list.set(i, list.get(j));
                            list.set(j, o);
                        }
                        j --;
                    }
                }
                n = j;
                if (n <= 1)
                    return list;
                o = list.get(0);
                if (o instanceof Integer || o instanceof Long) {
                    Long[] lnums = new Long[n];
                    for (int i=0; i<n; i++) {
                        o = list.get(i);
                        if (o == null)
                            continue;
                        else if (o instanceof Long) {
                            lnums[k] = (Long) o;
                            k ++;
                        }
                        else if (o instanceof Integer) {
                            lnums[k] = new Long(((Integer) o).longValue());
                            k ++;
                        }
                    }
                    if (k > 1) {
                        Arrays.sort(lnums, 0, k);
                        for (int i=0; i<k; i++)
                            list.set(i, lnums[i]);
                    }
                }
                else {
                    Double[] dnums = new Double[n];
                    for (int i=0; i<n; i++) {
                        o = list.get(i);
                        if (o == null)
                            continue;
                        else if (o instanceof Double) {
                            dnums[k] = (Double) o;
                            k ++;
                        }
                        else if (o instanceof Float) {
                            dnums[k] = new Double(((Float) o).doubleValue());
                            k ++;
                        }
                    }
                    if (k > 1) {
                        Arrays.sort(dnums, 0, k);
                        for (int i=0; i<k; i++)
                            list.set(i, dnums[i]);
                    }
                }
                break;
              case JSON2Map.JSON_MAP:
                for (int i=n-1; i>=0; i--) { //remove nulls and move non-maps
                    o = list.get(i);
                    if (o == null)
                        list.remove(i);
                    else if (!(o instanceof Map)) {
                        list.remove(i);
                        pl.add(o);
                    }
                }
                k = pl.size();
                for (int i=0; i<k; i++)
                    list.add(pl.remove(i));
                n = list.size();
                keys = new String[n];
                k = n;
                for (int i=0; i<n; i++) { // generate keys
                    o = list.get(i);
                    if (o != null && o instanceof Map) {
                        keys[i] = JSON2Map.format((Map) o, template[id]);
                        if (substitution[id] != null)
                            keys[i] = substitution[id].substitute(keys[i]);
                        pl.add(o);
                    }
                }
                if (k > 1) {
                    String key;
                    Arrays.sort(keys, 0, k);
                    n = pl.size();
                    for (int i=0; i<k; i++) { // fill sorted items
                        for (j=0; j<n; j++) { // look for the same key
                            o = pl.get(j);
                            key = JSON2Map.format((Map) o, template[id]);
                            if (key.equals(keys[i])) { // found it
                                list.set(i, o);
                                pl.remove(j);
                                n = pl.size();
                            }
                        }
                    }
                }
                break;
              case JSON2Map.JSON_ARRAY:
                for (int i=n-1; i>=0; i--) { //remove nulls and move non-maps
                    o = list.get(i);
                    if (o == null)
                        list.remove(i);
                    else if (!(o instanceof Map)) {
                        list.remove(i);
                        pl.add(o);
                    }
                }
                k = pl.size();
                for (int i=0; i<k; i++)
                    list.add(pl.remove(i));
                n = list.size();
                keys = new String[n];
                k = n;
                for (int i=0; i<n; i++) { // generate keys
                    o = list.get(i);
                    if (o != null && o instanceof Map) {
                        keys[i] = JSON2Map.format((List) o, template[id]);
                        if (substitution[id] != null)
                            keys[i] = substitution[id].substitute(keys[i]);
                        pl.add(o);
                    }
                }
                if (k > 1) {
                    String key;
                    Arrays.sort(keys, 0, k);
                    n = pl.size();
                    for (int i=0; i<k; i++) { // fill sorted items
                        for (j=0; j<n; j++) { // look for the same key
                            o = pl.get(j);
                            key = JSON2Map.format((List) o, template[id]);
                            if (key.equals(keys[i])) { // found it
                                list.set(i, o);
                                pl.remove(j);
                                n = pl.size();
                            }
                        }
                    }
                }
                break;
              default:
            }
            if (k > 0)
                return list;
        }

        return null;
    }

    /**
     * It gets a list of json data at the json path from the json payload and
     * removes any object with the duplicated keys. The key is generated by the
     * template. If the json payload has been changed, it returns the list.
     * Otherwise, it returns null for no change.
     */
    private Object uniq(int id, Object obj) {
        boolean isList = (obj instanceof List);
        Object o;
        if (isList)
            o = JSON2FmModel.get((List) obj, keyPath[id]);
        else
            o = JSON2FmModel.get((Map) obj, keyPath[id]);

        if (o != null && o instanceof List) { // for a list
            List list = (List) o;
            Map map = new HashMap();
            String key;
            Number num;
            int j = 0, k = 0;
            int n = list.size();
            switch (dataType[id]) {
              case JSON2Map.JSON_STRING:
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof String) {
                        key = (String) o;
                        if (!map.containsKey(key)) {
                            map.put(key, null);
                            if (i > j) {
                                list.set(j, o);
                                k ++;
                            }
                            j ++;
                        }
                    }
                    else { // not a string so keep it
                        if (i > j) {
                            list.set(j, o);
                            k ++;
                        }
                        j ++;
                    }
                }
                for (int i=j; i<n; i++) { // remove nulls or duplicates
                    list.remove(i);
                    k ++;
                }
                break;
              case JSON2Map.JSON_NUMBER:
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Number) {
                        num = (Number) o;
                        if (!map.containsKey(num)) {
                            map.put(num, null);
                            if (i > j) {
                                list.set(j, o);
                                k ++;
                            }
                            j ++;
                        }
                    }
                    else { // not a number so keep it
                        if (i > j) {
                            list.set(j, o);
                            k ++;
                        }
                        j ++;
                    }
                }
                for (int i=j; i<n; i++) { // remove nulls or duplicates
                    list.remove(i);
                    k ++;
                }
                break;
              case JSON2Map.JSON_MAP:
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Map) {
                        key = JSON2Map.format((Map) o, template[id]);
                        if (substitution[id] != null)
                            key = substitution[id].substitute(key);
                        if (!map.containsKey(key)) {
                            map.put(key, null);
                            if (i > j) {
                                list.set(j, o);
                                k ++;
                            }
                            j ++;
                        }
                    }
                    else { // not a string so keep it
                        if (i > j) {
                            list.set(j, o);
                            k ++;
                        }
                        j ++;
                    }
                }
                for (int i=j; i<n; i++) { // remove nulls or duplicates
                    list.remove(i);
                    k ++;
                }
                break;
              case JSON2Map.JSON_ARRAY:
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof List) {
                        key = JSON2Map.format((List) o, template[id]);
                        if (substitution[id] != null)
                            key = substitution[id].substitute(key);
                        if (!map.containsKey(key)) {
                            map.put(key, null);
                            if (i > j) {
                                list.set(j, o);
                                k ++;
                            }
                            j ++;
                        }
                    }
                    else { // not a string so keep it
                        if (i > j) {
                            list.set(j, o);
                            k ++;
                        }
                        j ++;
                    }
                }
                for (int i=j; i<n; i++) { // remove nulls or duplicates
                    list.remove(i);
                    k ++;
                }
                break;
              default:
            }
            if (k > 0)
                return list;
        }

        return null;
    }

    private static String minKey(Map map) {
        Object o;
        String key = null;
        if (map == null || map.size() <= 0)
            return null;
        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            if (key == null)
                key = (String) o;
            else if (key.compareTo((String) o) > 0)
                key = (String) o;
        }
        return key;
    }

    private static String maxKey(Map map) {
        Object o;
        String key = null;
        if (map == null || map.size() <= 0)
            return null;
        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            if (key == null)
                key = (String) o;
            else if (key.compareTo((String) o) < 0)
                key = (String) o;
        }
        return key;
    }

    /**
     * returns a text formatted out of the message and the json payload with
     * the given template
     */
    public static String format(Map obj, Message msg, Template template)
        throws JMSException {
        Object o;
        String key, value, field, text;
        int i, n;

        if (template == null)
            return null;

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if (field == null || field.length() <= 0)
                continue;
            if (field.charAt(0) == '.') { // json path
                o = JSON2FmModel.get(obj, field);
                if (o == null)
                    value = "";
                else if (o instanceof String)
                    value = (String) o;
                else if (o instanceof Map)
                    value = JSON2FmModel.toJSON((Map) o, null, "");
                else if (o instanceof List)
                    value = JSON2FmModel.toJSON((List) o, null, "");
                else
                    value = o.toString();
            }
            else if ((key = MessageUtils.getPropertyID(field)) != null)
                value = MessageUtils.getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else
                value = "";
            if (value == null)
                value = "";
            text = template.substitute(field, value, text);
        }
        return text;
    }

    /**
     * returns a text formatted out of the message and the json payload with
     * the given template
     */
    public static String format(List obj, Message msg, Template template)
        throws JMSException {
        Object o;
        String key, value, field, text;
        int i, n;

        if (template == null)
            return null;

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if (field == null || field.length() <= 0)
                continue;
            if (field.charAt(0) == '.') { // json path
                o = JSON2FmModel.get(obj, field);
                if (o == null)
                    value = "";
                else if (o instanceof String)
                    value = (String) o;
                else if (o instanceof Map)
                    value = JSON2FmModel.toJSON((Map) o, null, "");
                else if (o instanceof List)
                    value = JSON2FmModel.toJSON((List) o, null, "");
                else
                    value = o.toString();
            }
            else if ((key = MessageUtils.getPropertyID(field)) != null)
                value = MessageUtils.getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else
                value = "";
            if (value == null)
                value = "";
            text = template.substitute(field, value, text);
        }
        return text;
    }

    /**
     * returns a text formatted out of the message and the json string with
     * the given template
     */
    public static String format(String obj, Message msg, Template template)
        throws JMSException {
        String key, value, field, text;
        int i, n;

        if (template == null)
            return null;

        text = template.copyText();
        n = template.numberOfFields();
        for (i=0; i<n; i++) {
            field = template.getField(i);
            if (field == null || field.length() <= 0)
                continue;
            if (field.charAt(0) == '.') { // json path
                if (".".equals(field))
                    value = obj;
                else
                    value = "";
            }
            else if ((key = MessageUtils.getPropertyID(field)) != null)
                value = MessageUtils.getProperty(key, msg);
            else if (msg.propertyExists(field))
                value = msg.getStringProperty(field);
            else
                value = "";
            if (value == null)
                value = "";
            text = template.substitute(field, value, text);
        }
        return text;
    }

    public String getName() {
        return name;
    }

    public int getSize() {
        return keyPath.length;
    }

    public void clear() {
        if (keyPath != null) {
            for (int i=keyPath.length-1; i>=0; i--) {
                keyPath[i] = null;
                if (template[i] != null)
                    template[i].clear();
                template[i] = null;
                if (substitution[i] != null)
                    substitution[i].clear();
                substitution[i] = null;
                if (selector[i] != null)
                    selector[i].clear();
                selector[i] = null;
            }
            keyPath = null;
        }
    }

    protected void finalize() {
         clear();
    }

    public static void main(String args[]) {
        String filename = null, path = null, str, key = null;
        Map<String, String> params = new HashMap<String, String>();
        JSONFormatter ft;
        Message msg;
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
                if (i+1 < args.length)
                    path = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              case 'k':
                if (i+1 < args.length)
                    key = args[++i];
                break;
              case 'p':
                if (i+1 < args.length) {
                    str = args[++i];
                    if ((k = str.indexOf(":")) > 1) {
                        params.put(str.substring(0, k),
                            str.substring(k+1).trim());
                    }
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
            ft = new JSONFormatter(ph);
            msg = new TextEvent();
            for (String ky : params.keySet()) { // set parameters
                if (ky == null || ky.length() <= 0)
                    continue;
                ((TextEvent) msg).setAttribute(ky, params.get(ky));
            }
            in = new FileReader(filename);
            Object o = JSON2FmModel.parse(in);
            in.close();
            if (o == null)
                System.out.println("failed to parse json");
            else if (o instanceof List) {
                k = ft.format((List) o, msg);
                if (k < 0)
                    System.out.println("failed to format msg");
                else if (k == 0) {
                    System.out.println("there is no change");
                    if (key != null)
                        System.out.println(key + ": " +
                            ((TextEvent) msg).getAttribute(key));
                }
                else {
                    str = ((TextEvent) msg).getText();
                    in = new StringReader(str);
                    o = JSON2FmModel.parse(in);
                    if (o == null)
                        System.out.println("failed to parse json payload");
                    else if (o instanceof List)
                       System.out.println(JSON2FmModel.toJSON((List)o,"","\n"));
                    else if (o instanceof Map)
                        System.out.println(JSON2FmModel.toJSON((Map)o,"","\n"));
                    else
                        System.out.println("data is not a container");
                }
            }
            else if (o instanceof Map) {
                k = ft.format((Map) o, msg);
                if (k < 0)
                    System.out.println("failed to format msg");
                else if (k == 0) {
                    System.out.println("there is no change");
                    if (key != null)
                        System.out.println(key + ": " +
                            ((TextEvent) msg).getAttribute(key));
                }
                else {
                    str = ((TextEvent) msg).getText();
                    in = new StringReader(str);
                    o = JSON2FmModel.parse(in);
                    if (o == null)
                        System.out.println("failed to parse json payload");
                    else if (o instanceof List)
                       System.out.println(JSON2FmModel.toJSON((List)o,"","\n"));
                    else if (o instanceof Map)
                        System.out.println(JSON2FmModel.toJSON((Map)o,"","\n"));
                    else
                        System.out.println("data is not a container");
                }
            }
            else
                System.out.println("json content is not a container");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JSONFormatter Version 1.0 (written by Yannan Lu)");
        System.out.println("JSONFormatter: format JSON data via a ruleset");
        System.out.println("Usage: java org.qbroker.jms.JSONFormatter -f jsonfile -r rulefile");
        System.out.println("  -?: print this message");
        System.out.println("  -f: path to the json data file");
        System.out.println("  -r: path to the json rule file");
        System.out.println("  -p: parameter of key:value for the msg");
        System.out.println("  -k: key to display the attribute value");
    }
}
