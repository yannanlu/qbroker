package org.qbroker.json;

/* JSON2FmModel.java - to load a FreeMarker data model from a JSON file */

import java.io.Reader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.json.JSON2Map;

/**
 * JSON2FmModel parses a JSON text stream and creates a FreeMarker compatible
 * data model in a Map or a List. It only recognizes the basic Java objects of
 * String, Number, Boolen, List and Map.  A Map or a List can contain objects
 * of those types.  They are the nodes of the data tree. A String, Number or
 * Boolean is only a leaf in the data tree. For JSON's null, it will be
 * represented by Java null. The comments in Javascript style will be ignored.
 * With these basic types of objects, any data structures will be representable.
 * The mapping is one-to-one. Therefore, the mapping is bi-directional.
 *<br/><br/>
 * The method of parse() assumes the JSON data is either a Map or a List.
 * It returns either a Map or a List representing the JSON data.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSON2FmModel {
    public final static int bufferSize = 4096;
    public final static int JSON_UNKNOWN = -1;
    public final static int JSON_NULL = 0;
    public final static int JSON_STRING = 1;
    public final static int JSON_NUMBER = 2;
    public final static int JSON_BOOLEAN = 3;
    public final static int JSON_ARRAY = 4;
    public final static int JSON_MAP = 5;
    private final static int ACTION_NONE = 0;    // for parsing failure
    private final static int ACTION_SKIP = 1;    // for a char not a white space
    private final static int ACTION_FIND = 2;    // 
    private final static int ACTION_LOOK = 3;    // for a char not a white space
    private final static int ACTION_NEXT = 4;    // for next commnet chars
    private final static int ACTION_LINE = 5;    // for line comment
    private final static int ACTION_STAR = 6;    // for star
    private final static int ACTION_COLON = 7;   // for colon or backslash
    private final static int ACTION_QUOTE = 8;   // for double quote
    private final static int ACTION_MULTI = 9;   // for multi-line comment
    private final static int ACTION_ESCAPE = 10; // for next escaped chars

    private JSON2FmModel() {
    }

    /** It returns JSON content in tabular format for a given Map */
    public static String toJSON(Map m) {
        int i = 0;
        String key;
        Object o;
        StringBuffer strBuf;
        if (m == null)
            return null;
        strBuf = new StringBuffer();
        strBuf.append("{");
        Iterator iter = m.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            if (i > 0)
                strBuf.append(",");
            strBuf.append("\n  \"" + Utils.escapeJSON(key) + "\"" + ":");
            i++;
            o = m.get(key);
            if (o == null)
                strBuf.append("null");
            else if (o instanceof List)
                strBuf.append(toJSON((List) o, null, null));
            else if (o instanceof Map)
                strBuf.append(toJSON((Map) o, null, null));
            else if (o instanceof String)
                strBuf.append("\"" + Utils.escapeJSON((String) o) + "\"");
            else if (o instanceof Number)
                strBuf.append(((Number) o).toString());
            else if (o instanceof Boolean)
                strBuf.append(((Boolean) o).toString());
            else // not a String
                strBuf.append("\"" + Utils.escapeJSON(o.toString()) + "\"");
        }
        strBuf.append("\n}");

        return strBuf.toString();
    }

    /** It returns JSON content for a given Map with indent */
    public static String toJSON(Map m, String indent, String end) {
        int i = 0;
        String key, ind, ed;
        Object o;
        StringBuffer strBuf = new StringBuffer();
        ind = (indent == null) ? "" : indent + "  ";
        ed = (end == null) ? "" : end;
        strBuf.append("{");
        Iterator iter = m.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            if (i > 0)
                strBuf.append(",");
            strBuf.append(ed + ind + "\"" + Utils.escapeJSON(key) + "\"" + ":");
            i++;
            o = m.get(key);
            if (o == null)
                strBuf.append("null");
            else if (o instanceof List)
                strBuf.append(toJSON((List) o,
                    (indent == null) ? null : indent + "  ", end));
            else if (o instanceof Map)
                strBuf.append(toJSON((Map) o,
                    (indent == null) ? null : indent + "  ", end));
            else if (o instanceof String)
                strBuf.append("\"" + Utils.escapeJSON((String) o) + "\"");
            else if (o instanceof Number)
                strBuf.append(((Number) o).toString());
            else if (o instanceof Boolean)
                strBuf.append(((Boolean) o).toString());
            else // not a String
                strBuf.append("\"" + Utils.escapeJSON(o.toString()) + "\"");
        }
        strBuf.append(ed + ((indent == null) ? "" : indent) + "}");

        return strBuf.toString();
    }

    /** It returns JSON content for a given List with indent */
    public static String toJSON(List a, String indent, String end) {
        int n;
        Object o = null;
        StringBuffer strBuf = new StringBuffer();
        String ind = (indent == null) ? "" : indent + "  ";
        String ed = (end == null) ? "" : end;
        if (a == null)
            a = new ArrayList();
        strBuf.append("[");
        n = a.size();
        for (int i=0; i<n; i++) {
            o = a.get(i);
            if (i > 0)
                strBuf.append(",");
            strBuf.append(ed + ind);
            if (o == null)
                strBuf.append("null");
            else if (o instanceof List)
                strBuf.append(toJSON((List) o,
                    (indent == null) ? null : indent + "  ", end));
            else if (o instanceof Map)
                strBuf.append(toJSON((Map) o,
                    (indent == null) ? null : indent + "  ", end));
            else if (o instanceof String)
                strBuf.append("\"" + Utils.escapeJSON((String) o) + "\"");
            else if (o instanceof Number)
                strBuf.append(((Number) o).toString());
            else if (o instanceof Boolean)
                strBuf.append(((Boolean) o).toString());
            else // not a String
                strBuf.append("\"" + Utils.escapeJSON(o.toString()) + "\"");
        }
        strBuf.append(ed + ((indent == null) ? "" : indent) + "]");

        return strBuf.toString();
    }

    /** It returns XML content in the tabular format for a given Map */
    public static String toXML(Map m) {
        if (m == null)
            return null;
        return toXML(m, null);
    }

    /** It returns XML content in the tabular format with indent */
    private static String toXML(Map m, String indent) {
        String key;
        Object o;
        Iterator iter = m.keySet().iterator();
        StringBuffer strBuf = new StringBuffer();
        String ind = (indent == null) ? "" : "  ";
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = m.get(key);
            if (o == null) {
                strBuf.append("<" + key + " type=\"NULL\"></" +
                    key + ">\n");
            }
            else if (o instanceof Map) {
                strBuf.append("<" + key + ">\n");
                strBuf.append(toXML((Map) o,
                    ((indent == null) ? "  " : null)));
                strBuf.append("</" + key + ">\n");
            }
            else if (o instanceof List) {
                strBuf.append(toXML(key, (List) o,
                    ((indent == null) ? null : "  "), null));
            }
            else if (o instanceof String) {
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML((String) o) + "</" + key + ">\n");
            }
            else if (o instanceof Number) {
                strBuf.append(ind + "<" + key + " type=\"NUMBER\">" +
                    ((Number) o).toString() + "</" + key + ">\n");
            }
            else if (o instanceof Boolean) {
                strBuf.append(ind + "<" + key + " type=\"BOOLEAN\">" +
                    ((Boolean) o).toString() + "</" + key + ">\n");
            }
            else { // not a String
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML(o.toString()) + "</" + key + ">\n");
            }
        }
        return strBuf.toString();
    }

    /** It returns XML content for a given Map with indent */
    public static String toXML(Map m, String indent, String end) {
        String key;
        Object o;
        Iterator iter = m.keySet().iterator();
        StringBuffer strBuf = new StringBuffer();
        String ind = (indent == null) ? "" : indent;
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = m.get(key);
            if (o == null) {
                strBuf.append(ind + "<" + key + " type=\"NULL\"></" +
                    key + ">" + end);
            }
            else if (o instanceof Map) {
                strBuf.append(ind + "<" + key + ">" + end);
                strBuf.append(toXML((Map) o,
                    (indent == null) ? null : indent + "  ", end));
                strBuf.append(ind + "</" + key + ">" + end);
            }
            else if (o instanceof List) {
                strBuf.append(toXML(key, (List) o, indent, end));
            }
            else if (o instanceof String) {
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML((String) o) + "</" + key + ">" + end);
            }
            else if (o instanceof Number) {
                strBuf.append(ind + "<" + key + " type=\"NUMBER\">" +
                    ((Number) o).toString() + "</" + key + ">" + end);
            }
            else if (o instanceof Boolean) {
                strBuf.append(ind + "<" + key + " type=\"BOOLEAN\">" +
                    ((Boolean) o).toString() + "</" + key + ">" + end);
            }
            else { // not a String
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML(o.toString()) + "</" + key + ">" + end);
            }
        }
        return strBuf.toString();
    }

    /** It returns XML content for a given List with indent */
    public static String toXML(String key, List a, String indent,
        String end) {
        int n = a.size();
        Object o = null;
        StringBuffer strBuf = new StringBuffer();
        String ind = (indent == null) ? "" : indent;
        for (int i=0; i<n; i++) {
            o = a.get(i);
            if (o == null) {
                strBuf.append(ind + "<" + key +
                    " type=\"NULL_ARRAY\"></" + key + ">" + end);
            }
            else if (o instanceof Map) {
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" + end);
                strBuf.append(toXML((Map) o,
                    (indent == null) ? null : indent + "  ", end));
                strBuf.append(ind + "</" + key + ">" + end);
            }
            else if (o instanceof String) {
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" +
                    Utils.escapeXML((String) o) + "</" + key + ">" + end);
            }
            else if (o instanceof Number) {
                strBuf.append(ind + "<" + key + " type=\"NUMBER\">" +
                    ((Number) o).toString() + "</" + key + ">" + end);
            }
            else if (o instanceof Boolean) {
                strBuf.append(ind + "<" + key + " type=\"BOOLEAN\">" +
                    ((Boolean) o).toString() + "</" + key + ">" + end);
            }
            else { // not a String
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" +
                    Utils.escapeXML(o.toString()) + "</" + key + ">" + end);
            }
        }
        return strBuf.toString();
    }

    /** returns true if the keyPath exists or false otherwise */
    public static boolean containsKey(Map ph, String keyPath) {
        return JSON2Map.containsKey(ph, keyPath);
    }

    /** returns true if the keyPath exists or false otherwise */
    public static boolean containsKey(List pl, String keyPath) {
        return JSON2Map.containsKey(pl, keyPath);
    }

    /** returns the number of children referenced by the keyPath or -1 */
    public static int count(Map ph, String keyPath) {
        return JSON2Map.count(ph, keyPath);
    }

    /** returns the number of children referenced by the keyPath or -1 */
    public static int count(List pl, String keyPath) {
        return JSON2Map.count(pl, keyPath);
    }

    /** returns the removed object referenced by the keyPath or null */
    public static Object remove(Map ph, String keyPath) {
        return JSON2Map.remove(ph, keyPath);
    }

    /** returns the removed object referenced by the keyPath or null */
    public static Object remove(List pl, String keyPath) {
        return JSON2Map.remove(pl, keyPath);
    }

    /** returns the parent of the object referenced by the keyPath or null */
    public static Object getParent(Map ph, String keyPath) {
        return JSON2Map.getParent(ph, keyPath);
    }

    /** returns the parent of the object referenced by the keyPath or null */
    public static Object getParent(List pl, String keyPath) {
        return JSON2Map.getParent(pl, keyPath);
    }

    /** returns the object referenced by the keyPath or null */
    public static Object get(Map ph, String keyPath) {
        return JSON2Map.get(ph, keyPath);
    }

    /** returns the object referenced by the keyPath or null */
    public static Object get(List pl, String keyPath) {
        return JSON2Map.get(pl, keyPath);
    }

    public static int getDataType(Map ph, String keyPath) {
        return JSON2Map.getDataType(ph, keyPath);
    }

    public static int getDataType(List pl, String keyPath) {
        return JSON2Map.getDataType(pl, keyPath);
    }

    /**puts the data and returns the replaced object referenced by the keyPath*/
    public static Object put(Map ph, String keyPath, Object data) {
        return JSON2Map.put(ph, keyPath, data);
    }

    /**puts the data and returns the replaced object referenced by the keyPath*/
    public static Object put(List pl, String keyPath, Object data) {
        return JSON2Map.put(pl, keyPath, data);
    }

    /** returns the formatted string for the given template or null on faluire*/
    public static String format(Map ph, Template temp) {
        return JSON2Map.format(ph, temp);
    }

    /** returns the formatted string for the given template or null on faluire*/
    public static String format(List pl, Template temp) {
        return JSON2Map.format(pl, temp);
    }

    /** parses JSON stream from in and returns either Map or List, MT-safe */
    public static Object parse(Reader in) throws IOException {
        int a = -1, level, action, offset, position, charsRead, pa, total, ln;
        boolean with_comma = false;
        String key = null, str;
        StringBuffer strBuf = null;
        Map root = new HashMap();
        Object current = null, parent = null;
        Object[] tree = new Object[1024];
        String[] nodeName = new String[1024];
        char[] buffer = new char[bufferSize];
        ln = 1;
        level = 0;
        tree[level] = root;
        nodeName[level] = "JSON";
        current = root;
        action = ACTION_SKIP;
        total = 0;
        while ((charsRead = in.read(buffer, 0, bufferSize)) >= 0) {
            if (charsRead == 0) try {
                Thread.sleep(10);
                continue;
            }
            catch (Exception e) {
                continue;
            }
            offset = 0;
            position = 0;
            do {
                switch (action) {
                  case ACTION_SKIP:
                    position = JSON2Map.skip(buffer, offset, charsRead);
                    break;
                  case ACTION_FIND:
                    position = JSON2Map.skip(buffer, offset, charsRead);
                    break;
                  case ACTION_LOOK:
                    position = JSON2Map.look(buffer, offset, charsRead);
                    break;
                  case ACTION_COLON:
                    position =JSON2Map.scan(buffer, offset, charsRead, ':');
                    break;
                  case ACTION_QUOTE:
                    position =JSON2Map.scan(buffer, offset, charsRead, '"');
                    break;
                  case ACTION_LINE:
                    position=JSON2Map.scan(buffer, offset, charsRead, '\n');
                    break;
                  case ACTION_STAR:
                    position =JSON2Map.scan(buffer, offset, charsRead, '*');
                    break;
                  case ACTION_NEXT:
                    position = JSON2Map.next(buffer, offset, charsRead);
                    break;
                  case ACTION_MULTI:
                    position = JSON2Map.next(buffer, offset, charsRead);
                    break;
                  case ACTION_ESCAPE:
                    position = JSON2Map.escape(buffer, offset, charsRead);
                    if (position < 0) { // escaped char
                        int i;
                        char c;
                        ln += getLineNo(buffer, offset);
                        for (i=offset; i>=0; i--)
                            if (buffer[i] == '\n')
                                break;
                        if (i >= 0) { // found the newline
                            position = (offset - i <= 32) ? i + 1 : offset - 32;
                            c = ',';
                        }
                        else {
                            position = (offset <= 32) ? 0 : offset - 32;
                            i = 0;
                            c = ':';
                        }
                        i = offset - i;
                        throw(new IOException("unsupported escape char '\\" +
                            buffer[offset] + "' at line " + ln + c + i +
                            " and level "+ level + ((i==0) ? ":\n\\" : ":\n") +
                            new String(buffer, position, offset - position +
                            ((charsRead-offset>32)?32:charsRead-offset))));
                    }
                    break;
                  default:
                    position = -1;
                }
                pa = action;
                if (position >= 0) {
                    char c;
                    switch ((c = buffer[position])) {
                      case '{': // begin of map
                        if (action == ACTION_SKIP) {
                            with_comma = false;
                            parent = current;
                            current = new HashMap();
                            level ++;
                            tree[level] = current;
                            nodeName[level] = null;
                            if (parent instanceof Map) {
                                key = nodeName[level - 1];
                                ((Map) parent).put(key, current);
                            }
                            else {
                                ((List) parent).add(current);
                            }
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else {
                            a = action;
                            action = ACTION_NONE;
                        }
                        break;
                      case '[': // begin of list
                        if (action == ACTION_SKIP) {
                            with_comma = false;
                            parent = current;
                            current = new ArrayList();
                            level ++;
                            tree[level] = current;
                            if (parent instanceof Map) {
                                key = nodeName[level - 1];
                                ((Map) parent).put(key, current);
                            }
                            else {
                                ((List) parent).add(current);
                            }
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '"': // double quote
                        if (action == ACTION_SKIP) { // begin of string
                            offset = position + 1;
                            strBuf = new StringBuffer();
                            action = ACTION_QUOTE;
                            with_comma = false;
                        }
                        else if (action == ACTION_QUOTE) { // end of string
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            offset = position + 1;
                            if (current instanceof Map) {
                                if (nodeName[level] == null) { // for key
                                    nodeName[level] = str;
                                    action = ACTION_COLON;
                                }
                                else { // for value
                                    key = nodeName[level];
                                    ((Map) current).put(key, str);
                                    nodeName[level] = null;
                                    action = ACTION_FIND;
                                }
                            }
                            else { // for value in an array
                                ((List) current).add(str);
                                action = ACTION_FIND;
                            }
                        }
                        else if (action == ACTION_ESCAPE) {
                            strBuf.append(c);
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case ':': // end of key
                        if (action == ACTION_COLON) {
                            key = nodeName[level];
                            offset = position + 1;
                            if (key.length() <= 0) // empty key
                                throw(new IOException("empty key"));
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case ',': // end of value
                        with_comma = true;
                        if (action == ACTION_FIND) {
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    nodeName[level] = null;
                                    ((Map) current).put(key, o);
                                }
                                else
                                    ((List) current).add(o);
                                offset = position + 1;
                                action = ACTION_SKIP;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case ']': // end of list
                        if (!(current instanceof List)) // not for a list
                            action = ACTION_NONE;
                        else if (with_comma) // trailing comma
                            action = ACTION_NONE;
                        else if (action == ACTION_FIND) {
                            level --;
                            current = parent;
                            nodeName[level] = null;
                            if (level > 1)
                                parent = tree[level-1];
                            else
                                parent = root;
                            offset = position + 1;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                ((List) current).add(o);
                                level --;
                                current = parent;
                                nodeName[level] = null;
                                if (level > 1)
                                    parent = tree[level-1];
                                else
                                    parent = root;
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else if (action == ACTION_SKIP) { // empty array
                            level --;
                            current = parent;
                            nodeName[level] = null;
                            if (level > 1)
                                parent = tree[level-1];
                            else
                                parent = root;
                            offset = position + 1;
                            action = ACTION_FIND;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '}': // end of map
                        if (!(current instanceof Map)) // not for a Map
                            action = ACTION_NONE;
                        else if (with_comma) // trailing comma
                            action = ACTION_NONE;
                        else if (action == ACTION_FIND) {
                            level --;
                            current = parent;
                            if (level > 1)
                                parent = tree[level-1];
                            else
                                parent = root;
                            nodeName[level] = null;
                            offset = position + 1;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            key = nodeName[level];
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                ((Map) current).put(key, o);
                                nodeName[level] = null;
                                level --;
                                current = parent;
                                if (level > 1)
                                    parent = tree[level-1];
                                else
                                    parent = root;
                                nodeName[level] = null;
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else if (action == ACTION_SKIP) { // empty map
                            level --;
                            current = parent;
                            if (level > 1)
                                parent = tree[level-1];
                            else
                                parent = root;
                            nodeName[level] = null;
                            offset = position + 1;
                            action = ACTION_FIND;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '/':
                        if (action == ACTION_SKIP || action == ACTION_FIND) {
                            // check comment
                            a = action;
                            offset = position + 1;
                            action = ACTION_NEXT;
                        }
                        else if (action == ACTION_NEXT) { // line comment
                            offset = position + 1;
                            action = ACTION_LINE;
                        }
                        else if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append(c);
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_MULTI) { // multi-line comment
                            offset = position + 1;
                            action = (a == ACTION_LOOK) ? ACTION_FIND : a;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    ((Map) current).put(key, o);
                                    nodeName[level] = null;
                                }
                                else
                                    ((List) current).add(o);
                                offset = position + 1;
                                a = action;
                                action = ACTION_NEXT;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '*':
                        if (action == ACTION_NEXT) { // multi-line comment
                            offset = position + 1;
                            action = ACTION_STAR;
                        }
                        else if (action == ACTION_STAR ||
                            action == ACTION_MULTI) { // check for end
                            offset = position + 1;
                            action = ACTION_MULTI;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '\\':
                        if (action == ACTION_QUOTE) { // check escape
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            offset = position + 1;
                            action = ACTION_ESCAPE;
                        }
                        else if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append(c);
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '\n':
                        if (action == ACTION_QUOTE) // illegle inside quotes
                            action = ACTION_NONE;
                        else if (action == ACTION_LINE) { // end of line comment
                            offset = position + 1;
                            action = (a == ACTION_LOOK) ? ACTION_FIND : a;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    ((Map) current).put(key, o);
                                    nodeName[level] = null;
                                }
                                else
                                    ((List) current).add(o);
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '\r':
                      case '\b':
                      case '\t':
                      case '\f':
                        if (action == ACTION_QUOTE) // illegle inside quotes
                            action = ACTION_NONE;
                        else if (action == ACTION_LOOK) { // end of no quotes
                            Object o = null;
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = JSON2Map.trim(strBuf);
                            if ("null".equals(str)) // null
                                o = null;
                            else if ("true".equals(str) || "false".equals(str))
                                o = new Boolean(str);
                            else try {
                                o = (str.indexOf(".") >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    ((Map) current).put(key, o);
                                    nodeName[level] = null;
                                }
                                else
                                    ((List) current).add(o);
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'n':
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append('\n');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_SKIP) { // begin of no quotes
                            if (current instanceof List ||
                                nodeName[level] != null) { // for value
                                offset = position;
                                strBuf = new StringBuffer();
                                action = ACTION_LOOK;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 't':
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append('\t');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_SKIP) { // begin of no quotes
                            if (current instanceof List ||
                                nodeName[level] != null) { // for value
                                offset = position;
                                strBuf = new StringBuffer();
                                action = ACTION_LOOK;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'f':
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append('\f');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_SKIP) { // begin of no quotes
                            if (current instanceof List ||
                                nodeName[level] != null) { // for value
                                offset = position;
                                strBuf = new StringBuffer();
                                action = ACTION_LOOK;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'r':
                        if (action == ACTION_ESCAPE) { // for baskslash
                            strBuf.append('\r');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'b':
                        if (action == ACTION_ESCAPE) { // for baskslash
                            strBuf.append('\b');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      default:
                        if (action == ACTION_SKIP) { // begin of no quotes
                            if (current instanceof List ||
                                nodeName[level] != null) { // for value
                                offset = position;
                                strBuf = new StringBuffer();
                                action = ACTION_LOOK;
                            }
                            else
                                action = ACTION_LOOK;
                        }
                        else
                            action = ACTION_NONE;
                    }
                    if (action == ACTION_NONE) { // parsing error
                        int i;
                        char b;
                        ln += getLineNo(buffer, position);
                        for (i=position; i>=0; i--)
                            if (buffer[i] == '\n')
                                break;
                        if (i >= 0) { // found the newline
                            offset = (position-i <= 32) ? i + 1 : position - 32;
                            b = ',';
                        }
                        else {
                            offset = (position <= 32) ? 0 : position - 32;
                            i = 0;
                            b = ':';
                        }
                        i = position - i;
                        throw(new IOException("stopped parsing on '" + c +
                            "' at line " + ln + b + i + " and level " +
                            level + " " + pa + "/" + a + ":\n" +
                            new String(buffer, offset, position - offset +
                            ((charsRead-position>32)?32:charsRead-position))));
                    }
                }
                else if (action == ACTION_SKIP || action == ACTION_FIND ||
                    action == ACTION_LINE || action == ACTION_STAR) {
                    offset = charsRead;
                }
                else if (action == ACTION_MULTI) {
                    offset ++;
                    action = ACTION_STAR;
                }
                else {
                    str = new String(buffer, offset, charsRead - offset);
                    strBuf.append(str);
                    offset = charsRead;
                }
            } while (offset < charsRead);
            total += charsRead;
            ln += getLineNo(buffer, charsRead);
        }
        if (level > 0)
            throw(new IOException("missing closed tag: " + level +"/"+ total));

        return root.remove("JSON");
    }

    /** returns the position of the first target or -1 if not found */
    private static int getLineNo(char[] buffer, int n) {
        int k = 0;
        for (int i=0; i<n; i++) {
            if (buffer[i] == '\n')
                k ++;
        }
        return k;
    }

    public static void main(String args[]) {
        String filename = null, action = "parse", type = "json", path = null;
        String data = null;
        int fromIndex = 0;
        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'a':
                action = args[++i];
                break;
              case 't':
                type = args[++i];
                break;
              case 'k':
                path = args[++i];
                break;
              case 'd':
                data = args[++i];
                break;
              case 'b':
                fromIndex = Integer.parseInt(args[++i]);
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        if (filename == null) {
            printUsage();
            System.exit(0);
        }

        try {
            Reader in = new FileReader(filename); 
            int k;
            char[] buffer = new char[4096];
            StringBuffer strBuf = new StringBuffer();
            while ((k = in.read(buffer, 0, 4096)) >= 0) {
                if (k > 0)
                    strBuf.append(new String(buffer, 0, k));
            }
            String json = strBuf.toString();
            in.close();
            if ("locate".equalsIgnoreCase(action)) {
                int i = 1;
                if (path != null)
                    i = Integer.parseInt(path);
                k = JSON2Map.locate(i, json, fromIndex);
                if (k < 0)
                    System.out.println("not found for level " + i);
                else if (i == 0)
                    System.out.println("level " + i + " begins with '" +
                        json.charAt(k) + "' at: " + k);
                else
                    System.out.println("level " + i + " ends with '" +
                        json.charAt(k) + "' at: " + k);
                System.exit(0);
            }
            in = new StringReader(json);
            Object o = parse(in);
            in.close();
            if (o == null)
                System.out.println("failed to parse json");
            else if ("parse".equalsIgnoreCase(action)) {
                if ("json".equals(type)) {
                    if (o instanceof List)
                        System.out.print(toJSON((List) o, "", "\n"));
                    else
                        System.out.print(toJSON((Map) o, "", "\n"));
                }
                else {
                    Map map = new HashMap();
                    map.put("JSON", o);
                    System.out.print(toXML(map, "", "\n"));
                }
            }
            else if (path != null && path.length() > 0) {
                if ("test".equalsIgnoreCase(action)) {
                    if (o instanceof List && containsKey((List) o, path))
                        System.out.println(path + ": Yes");
                    else if (o instanceof Map && containsKey((Map) o, path))
                        System.out.println(path + ": Yes");
                    else
                        System.out.println(path + ": No");
                }
                else if ("get".equalsIgnoreCase(action)) {
                    if (o instanceof List && containsKey((List) o, path)) {
                        o = get((List) o, path);
                        if (o == null)
                            System.out.println(path + ": null");
                        else if (o instanceof String)
                            System.out.println(path + ": " + (String) o);
                        else if (o instanceof List)
                            System.out.println(path + ": " +
                                toJSON((List) o, "", "\n"));
                        else if (o instanceof Map)
                            System.out.println(path + ": " +
                                toJSON((Map) o, "", "\n"));
                        else
                            System.out.println(path + ": " + o.toString());
                    }
                    else if (o instanceof Map && containsKey((Map) o, path)) {
                        o = get((Map) o, path);
                        if (o == null)
                            System.out.println(path + ": null");
                        else if (o instanceof String)
                            System.out.println(path + ": " + (String) o);
                        else if (o instanceof List)
                            System.out.println(path + ": " +
                                toJSON((List) o, "", "\n"));
                        else if (o instanceof Map)
                            System.out.println(path + ": " +
                                toJSON((Map) o, "", "\n"));
                        else
                            System.out.println(path + ": " + o.toString());
                    }
                    else
                        System.out.println(path + ": No");
                }
                else if ("parent".equalsIgnoreCase(action)) {
                    if (o instanceof List && containsKey((List) o, path)) {
                        o = getParent((List) o, path);
                        if (o == null)
                            System.out.println("Parent of " + path + ": null");
                        else if (o instanceof List)
                            System.out.println("Parent of " + path + ": " +
                                toJSON((List) o, "", "\n"));
                        else
                            System.out.println("Parent of " + path + ": " +
                                toJSON((Map) o, "", "\n"));
                    }
                    else if (o instanceof Map && containsKey((Map) o, path)) {
                        o = getParent((Map) o, path);
                        if (o == null)
                            System.out.println("Parent of " + path + ": null");
                        else if (o instanceof List)
                            System.out.println("Parent of " + path + ": " +
                                toJSON((List) o, "", "\n"));
                        else
                            System.out.println("Parent of " + path + ": " +
                                toJSON((Map) o, "", "\n"));
                    }
                    else
                        System.out.println("Parent of " + path + ": No");
                }
                else if ("put".equalsIgnoreCase(action)) {
                    Object obj = null;
                    if (data == null || data.length() <= 0) {
                        System.out.println("put: no data");
                    }
                    else {
                        obj = parse(new StringReader(data));
                        if (o instanceof List)
                            put((List) o, path, obj);
                        else
                            put((Map) o, path, obj);
                    }
                    if (obj != null) {
                        if (o instanceof List)
                            obj = getParent((List) o, path);
                        else
                            obj = getParent((Map) o, path);
                        if (obj instanceof List)
                            System.out.println("Parent of " + path + ": " +
                                toJSON((List) obj, "", "\n"));
                        else
                            System.out.println("Parent of " + path + ": " +
                                toJSON((Map) obj, "", "\n"));
                    }
                }
                else if ("count".equalsIgnoreCase(action)) {
                    if (o instanceof List)
                        System.out.println(path + ": " + count((List) o, path));
                    else
                        System.out.println(path + ": " + count((Map) o, path));
                }
                else if ("format".equalsIgnoreCase(action)) {
                    Template temp = new Template(path);
                    if (o instanceof List)
                        System.out.println(path + ":\n" + format((List)o,temp));
                    else
                        System.out.println(path + ":\n" + format((Map)o, temp));
                }
                else if ("remove".equalsIgnoreCase(action)) {
                    if (o instanceof List && containsKey((List) o, path)) {
                        o = remove((List) o, path);
                        if (o == null)
                            System.out.println("failed to remove: " + path);
                        else if (o instanceof String)
                            System.out.println(path + " removed: " + (String)o);
                        else if (o instanceof List)
                            System.out.println(path + " removed: " +
                                toJSON((List) o, "", "\n"));
                        else if (o instanceof Map)
                            System.out.println(path + " removed: " +
                                toJSON((Map) o, "", "\n"));
                        else
                            System.out.println(path + " removed: " +
                                o.toString());
                    }
                    else if (o instanceof Map && containsKey((Map) o, path)) {
                        o = remove((Map) o, path);
                        if (o == null)
                            System.out.println("failed to remove: " + path);
                        else if (o instanceof String)
                            System.out.println(path + " removed: " + (String)o);
                        else if (o instanceof List)
                            System.out.println(path + " removed: " +
                                toJSON((List) o, "", "\n"));
                        else if (o instanceof Map)
                            System.out.println(path + " removed: " +
                                toJSON((Map) o, "", "\n"));
                        else
                            System.out.println(path + " removed: " +
                                o.toString());
                    }
                    else
                        System.out.println(path + ": No");
                }
                else
                    System.out.println(action + ": not supported");
            }
            else
                System.out.println("KeyPath is required for: " + action);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JSON2FmModel Version 1.0 (written by Yannan Lu)");
        System.out.println("JSON2FmModel: parse JSON data in a file");
        System.out.println("Usage: java org.qbroker.json.JSON2FmModel -f filename -a action -t type -k path");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of parse, test, count, locate, get, put, remove or format (default: parse)");
        System.out.println("  -k: key path (example: .views.list[0].name)");
        System.out.println("  -d: json data for put");
        System.out.println("  -t: type of xml or json (default: json)");
        System.out.println("  -b: starting position to locate (default: 0)");
        System.out.println("  -f: filename");
    }
}
