package org.qbroker.common;

/* PHP2Map.java - to load a Map from a PHP serialized file */

import java.io.Reader;
import java.io.FileReader;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.qbroker.common.Utils;

/**
 * PHP2Map parses a PHP serialized text stream and maps the structural
 * content of the PHP object into Java object stored in a Map.  It only
 * recognizes two basic Java objects, String, Map.  A Map can contain
 * any objects of the two types.  They are the nodes of the data tree.  A
 * String is only a leaf in the data tree.  For PHP's numbers, decimals and
 * boolean values, they will be stored as String. For PHP's null, it will
 * stored as null. The PHP Object is treated same as a Map with single key
 * pointing to a PHP Array. A PHP Array is treated as a Map.  With these
 * two basic types of objects, any data structures will be representable. The
 * mapping is one-to-one in one way at least. It exposes the structure data.
 *<br><br>
 * The method of parse() returned a Map. The PHP data are stored in
 * the key of "PHP".
 *<br>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class PHP2Map {
    private final static int bufferSize = 4096;
    public final static int PHP_DEFAULT = 0;
    public final static int PHP_ARRAY = 1;
    public final static int PHP_NUMBER = 2;
    public final static int PHP_FLOAT = 3;
    public final static int PHP_BOOLEAN = 4;
    public final static int PHP_NULL = 5;
    private final static int ACTION_NONE = 0;    // for parsing failure
    private final static int ACTION_SKIP = 1;    // for next char
    private final static int ACTION_NEXT = 2;    // for next char
    private final static int ACTION_STEP = 3;    // for next char
    private final static int ACTION_LOOK = 4;    // for next char of i or s
    private final static int ACTION_FIND = 5;    // for next char
    private final static int ACTION_SIZE = 6;    // for colon
    private final static int ACTION_SEMI = 7;    // for semi colon
    private final static int ACTION_BOOL = 8;    // for next char
    private final static int ACTION_COPY = 9;    // for colon
    private final static int ACTION_COLON = 10;  // for colon

    private PHP2Map() {
    }

    /** It returns JSON content in the tabular format for a given Map */
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
            strBuf.append("\n  \"" + key + "\"" + ":");
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
            strBuf.append(ed + ind + "\"" + key + "\"" + ":");
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
            else // not a String
                strBuf.append("\"" + Utils.escapeJSON(o.toString()) + "\"");
        }
        strBuf.append(ed + ((indent == null) ? "" : indent) + "]");

        return strBuf.toString();
    }

    /** It returns the difference in JSON between two Maps */
    public static String diff(Map ps, Map pt, String indent) {
        int i = 0;
        Object o;
        String key;
        StringBuffer strBuf;

        if (ps == null && pt == null)
            return null;

        if (pt == null)
            pt = new HashMap();
        else if (ps == null)
            ps = new HashMap();

        if (pt.equals(ps))
            return null;

        if (indent == null)
            indent = "";

        // check for new keys or modified values
        strBuf = new StringBuffer();
        for (Iterator iter=pt.keySet().iterator(); iter.hasNext();) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            if (key.length() <= 0)
                continue;
            o = pt.get(key);
            if (o == null) { // null
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + key + "\": null");
                    continue;
                }
                else if ((o = ps.get(key)) == null) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + key + "\": null");
                }

                // list original value
                strBuf.append(",");
                if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else // for object
                    strBuf.append("\n" + indent + "  \"O:" + key + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof Map) { // map
                Map ph = (Map) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + key + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof Map) {
                    if (!ph.equals((Map) o)) { // different 
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" + key + "\": " +
                            diff((Map) o, ph, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + key + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": null");
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + key + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof List) { // list
                List pl = (List) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + key + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof List) {
                    if (!pl.equals((List) o)) { // different
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" + key + "\": " +
                            diff((List) o, pl, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + key + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + key + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof String) { // string
                String value = (String) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + key + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof String &&
                    value.equals((String) o)) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + key + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + key + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else { // other object
                String value = o.toString();
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"A:" + key + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                    continue;
                }
                else if ((o = ps.get(key)) != null && !(o instanceof Map ||
                    o instanceof List || o instanceof String) &&
                    value.equals(o.toString())) {
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"C:" + key + "\": \"" +
                        Utils.escapeXML(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + key + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + key + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
        }

        // check for deleted keys
        for (Iterator iter=ps.keySet().iterator(); iter.hasNext();) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            if (key.length() <= 0 || pt.containsKey(key))
                continue;
            if (i++ > 0)
                strBuf.append(",");
            o = ps.get(key);
            if (o == null) // null
                strBuf.append("\n" + indent + "  \"d:" + key + "\": null");
            else if (o instanceof Map)
                strBuf.append("\n" + indent + "  \"d:" + key + "\": " +
                    toJSON((Map) o, indent + "  ", "\n"));
            else if (o instanceof List)
                strBuf.append("\n" + indent + "  \"d:" + key + "\": " +
                    toJSON((List) o, indent + "  ", "\n"));
            else if (o instanceof String)
                strBuf.append("\n" + indent + "  \"d:" + key + "\": \"" +
                    Utils.escapeJSON((String) o) + "\"");
            else
                strBuf.append("\n" + indent + "  \"D:" + key + "\": \"" +
                    Utils.escapeJSON(o.toString()) + "\"");
        }

        if (strBuf.length() <= 0)
            return null;
        else
            return "{" + strBuf.toString() + "\n" + indent + "}";
    }

    /** It returns the difference in JSON between two Lists */
    public static String diff(List ps, List pt, String indent) {
        int i = 0, j, k, n;
        Object o;
        StringBuffer strBuf;

        if (ps == null && pt == null)
            return null;

        if (pt == null)
            pt = new ArrayList();
        else if (ps == null)
            ps = new ArrayList();

        if (pt.equals(ps))
            return null;

        if (indent == null)
            indent = "";

        k = ps.size();
        n = pt.size();
        strBuf = new StringBuffer();
        for (j=0; j<n; j++) { // check for new or modified items
            o = pt.get(j);
            if (o == null) { // null
                if (j >= k) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + j + "\": null");
                    continue;
                }
                else if ((o = ps.get(j)) == null) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + j + "\": null");
                }

                // list original value
                strBuf.append(",");
                if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else // for object
                    strBuf.append("\n" + indent + "  \"O:" + j + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof Map) { // map
                Map ph = (Map) o;
                if (j >= k) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + j + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(j)) != null && o instanceof Map) {
                    if (!ph.equals((Map) o)) { // different 
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" + j + "\": " +
                            diff((Map) o, ph, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + j + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": null");
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + j + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof List) { // list
                List pl = (List) o;
                if (j >= k) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + j + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(j)) != null && o instanceof List) {
                    if (!pl.equals((List) o)) { // different
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" + j + "\": " +
                            diff((List) o, pl, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + j + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + j + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof String) { // string
                String value = (String) o;
                if (j >= k) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" + j + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                    continue;
                }
                else if ((o = ps.get(j)) != null && o instanceof String &&
                    value.equals((String) o)) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" + j + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + j + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else { // other object
                String value = o.toString();
                if (j >= k) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"A:" + j + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                    continue;
                }
                else if ((o = ps.get(j)) != null && !(o instanceof Map ||
                    o instanceof List || o instanceof String) &&
                    value.equals(o.toString())) {
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"C:" + j + "\": \"" +
                        Utils.escapeXML(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" + j + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" + j + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
        }

        for (j=n; j<k; j++) { // check for deleted items
            o = ps.get(j);
            if (o == null) // null
                strBuf.append("\n" + indent + "  \"d:" + j + "\": null");
            else if (o instanceof Map)
                strBuf.append("\n" + indent + "  \"d:" + j + "\": " +
                    toJSON((Map) o, indent + "  ", "\n"));
            else if (o instanceof List)
                strBuf.append("\n" + indent + "  \"d:" + j + "\": " +
                    toJSON((List) o, indent + "  ", "\n"));
            else if (o instanceof String)
                strBuf.append("\n" + indent + "  \"d:" + j + "\": \"" +
                    Utils.escapeJSON((String) o) + "\"");
            else
                strBuf.append("\n" + indent + "  \"D:" + j + "\": \"" +
                    Utils.escapeJSON(o.toString()) + "\"");
        }

        if (strBuf.length() <= 0)
            return null;
        else
            return "[" + strBuf.toString() + "\n" + indent + "]";
    }

    /**
     * It converts a multi-layered map into a flat map with the leaves only.
     * The flattened keys are the JSON paths to the leaves in the original map.
     */
    public static void flatten(Map m) {
        flatten(".", m);
    }

    public static void flatten(String s, Map m) {
        Object[] keys;
        Object o;
        String key;
        int i, n;
        if (m == null || (n = m.size()) <= 0)
            return;
        keys = m.keySet().toArray();
        for (i=0; i<n; i++) {
            if (keys[i] == null || !(keys[i] instanceof String))
                continue;
            o = m.get(keys[i]);
            if (o == null || o instanceof String)
                continue;
            key = (String) keys[i];
            if (o instanceof Map) {
                m.remove(key);
                flattenMap(key, s, m, (Map) o);
            }
            else if (o instanceof List) {
                m.remove(key);
                flattenList(key, s, m, (List) o);
            }
            else if (o instanceof String[]) {
                String[] tmp = (String[]) o;
                List list = new ArrayList();
                for (int j=0; i<tmp.length; j++)
                    list.add(tmp[i]);
                m.remove(key);
                flattenList(key, s, m, list);
            }
            else if (o instanceof int[]) {
                int[] tmp = (int[]) o;
                List list = new ArrayList();
                for (int j=0; i<tmp.length; j++)
                    list.add(String.valueOf(tmp[i]));
                m.remove(key);
                flattenList(key, s, m, list);
            }
            else if (o instanceof long[]) {
                long[] tmp = (long[]) o;
                List list = new ArrayList();
                for (int j=0; i<tmp.length; j++)
                    list.add(String.valueOf(tmp[i]));
                m.remove(key);
                flattenList(key, s, m, list);
            }
            else if (o instanceof double[]) {
                double[] tmp = (double[]) o;
                List list = new ArrayList();
                for (int j=0; i<tmp.length; j++)
                    list.add(String.valueOf(tmp[i]));
                m.remove(key);
                flattenList(key, s, m, list);
            }
            else if (o instanceof float[]) {
                float[] tmp = (float[]) o;
                List list = new ArrayList();
                for (int j=0; i<tmp.length; j++)
                    list.add(String.valueOf(tmp[i]));
                m.remove(key);
                flattenList(key, s, m, list);
            }
            else {
                continue;
            }
        } 
    }

    private static void flattenMap(String key, String s, Map root, Map m) {
        Object[] keys;
        Object o;
        String str;
        int i, n;
        if (root == null || m == null || (n = m.size()) <= 0)
            return;
        keys = m.keySet().toArray();
        key += s;
        for (i=0; i<n; i++) {
            if (keys[i] == null || !(keys[i] instanceof String))
                continue;
            str = (String) keys[i];
            o = m.get(str);
            if (o == null || o instanceof String) {
                root.put(key + str, o); 
            }
            else if (o instanceof Map) {
                flattenMap(key + str, s, root, (Map) o);
            }
            else if (o instanceof List) {
                flattenList(key + str, s, root, (List) o);
            }
            else {
                root.put(key + str, o); 
            }
        }
    }

    private static void flattenList(String key, String s, Map root, List a) {
        Object o;
        int i, n;
        if (root == null || a == null || (n = a.size()) <= 0)
            return;
        for (i=0; i<n; i++) {
            o = a.get(i);
            if (o == null || o instanceof String) {
                root.put(key + "[" + i + "]", o); 
            }
            else if (o instanceof Map) {
                flattenMap(key + "[" + i + "]", s, root, (Map) o);
            }
            else if (o instanceof List) {
                flattenList(key + "[" + i + "]", s, root, (List) o);
            }
            else {
                root.put(key + "[" + i + "]", o); 
            }
        }
    }

    /** It unflattens a flat map into a multi-layered map.  */
    public static void unflatten(Map m) {
        unflatten(".", m);
    }

    public static void unflatten(String s, Map m) {
        String[] keys, ks;
        Object o, current, parent;
        String str, key = "", previousKey = null;
        int i, j, k, n, len;
        boolean isList = false;

        if (s == null || (len = s.length()) <= 0 || m == null ||
            (n = m.size()) <= 0)
            return;
        o = m.keySet().toArray();
        keys = new String[n];
        k = 0;
        for (i=0; i<n; i++) { // load all keys
            if (((Object[]) o)[i] == null ||
                !(((Object[]) o)[i] instanceof String))
                keys[i] = null;
            else {
                key = (String) ((Object[]) o)[i];
                if (key.indexOf("[") > 0) // for array access
                    key.replaceAll("[", s).replaceAll("]", "");
                keys[i] = key; 
            }
        }
        parent = m;
        current = m;
        Arrays.sort(keys);
        for (i=0; i<n; i++) { // scan all sorted keys
            if (keys[i] == null)
                continue;
            o = m.get(keys[i]);
            if (o == null || !(o instanceof String))
                continue;
            if ((k = keys[i].lastIndexOf(s)) < 0)
                continue;
            str = keys[i].substring(0, k);
            m.remove(keys[i]);
            if (!str.equals(previousKey)) { // a new key
                previousKey = str;
                ks = Utils.split(s, keys[i]);
                if (ks == null)
                    continue;
                parent = m;
                isList = false;
                key = ks[0];
                for (j=1; j<ks.length; j++) { // locate the right container
                    if (!isList) {
                        current = ((Map) parent).get(key);
                        if (current == null) { // new object
                            if ("0".equals(ks[j])) {
                                current = new ArrayList();
                                isList = true;
                            }
                            else {
                                current = new HashMap(); 
                                isList = false;
                            }
                            ((Map) parent).put(key, current);
                        }
                        else { // existing object
                            isList = (current instanceof List) ? true : false;
                        }
                        parent = current;
                    }
                    else { // List
                        k = Integer.parseInt(key);
                        if (k < ((List) current).size()) {
                            current = ((List) parent).get(k);
                            isList = (current instanceof List) ? true : false;
                        }
                        else if ("0".equals(ks[j])) {
                            current = new ArrayList();
                            isList = true;
                            ((List) parent).add(current);
                        }
                        else {
                            current = new HashMap();
                            isList = false;
                            ((List) parent).add(current);
                        }
                        parent = current;
                    }
                    key = ks[j];
                }
            }
            else {
                key = keys[i].substring(k+len);
            }

            if (isList)
                ((List) current).add(o);
            else
                ((Map) current).put(key, o);
        }
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
        String ed = (end == null) ? "" : end;
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = m.get(key);
            if (o == null) {
                strBuf.append(ind + "<" + key + " type=\"NULL\"></" +
                    key + ">" + ed);
            }
            else if (o instanceof Map) {
                strBuf.append(ind + "<" + key + ">" + ed);
                strBuf.append(toXML((Map) o,
                    (indent == null) ? null : indent + "  ", ed));
                strBuf.append(ind + "</" + key + ">" + ed);
            }
            else if (o instanceof List) {
                strBuf.append(toXML(key, (List) o, indent, ed));
            }
            else if (o instanceof String) {
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML((String) o) + "</" + key + ">" + ed);
            }
            else { // not a String
                strBuf.append(ind + "<" + key + ">" +
                    Utils.escapeXML(o.toString()) + "</" + key + ">" + ed);
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
        String ed = (end == null) ? "" : end;
        for (int i=0; i<n; i++) {
            o = a.get(i);
            if (o == null) {
                strBuf.append(ind + "<" + key +
                    " type=\"NULL_ARRAY\"></" + key + ">" + ed);
            }
            else if (o instanceof Map) {
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" + ed);
                if (end != null) {
                    strBuf.append(toXML((Map) o,
                        (indent == null) ? null : indent + "  ", end));
                    strBuf.append(ind + "</" + key + ">" + end);
                }
                else {
                    strBuf.append(toXML((Map) o, null, ""));
                    strBuf.append("</" + key + ">\n");
                }
            }
            else if (o instanceof String) {
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" +
                    Utils.escapeXML((String) o) + "</" + key + ">" +
                    ((end == null) ? "\n" : end));
            }
            else { // not a String
                strBuf.append(ind + "<" + key + " type=\"ARRAY\">" +
                    Utils.escapeXML(o.toString()) + "</" + key + ">" +
                    ((end == null) ? "\n" : end));
            }
        }
        return strBuf.toString();
    }

    /** returns true if the keyPath exists or false otherwise */
    public static boolean containsKey(Map ph, String keyPath) {
        int i, n;
        String[] keys;
        Object o;
        Map h;
        if (ph == null || keyPath == null || keyPath.length() <= 0)
            throw(new IllegalArgumentException("bad data"));
        if (ph.size() <= 0)
            return false;
        if (keyPath.charAt(0) != '/')
            return ph.containsKey(keyPath);
        keys = Utils.split("/", keyPath);
        n = keys.length;
        h = ph;
        for (i=1; i<n; i++) {
            if (!h.containsKey(keys[i]))
                break;
            else if (i+1 == n)
                return true;
            o = h.get(keys[i]);
            if (o == null)
                break;
            else if (!(o instanceof Map))
                break;
            h = (Map) o;
        }
        return false;
    }

    /** returns the number of children referenced by the keyPath or -1 */
    public static int count(Map ph, String keyPath) {
        Object o;
        if (!containsKey(ph, keyPath))
            return 0;
        if ((o = get(ph, keyPath)) == null)
            return -1;
        else if (o instanceof Map)
            return ((Map) o).size();
        else
            return 1;
    }

    /** returns the object referenced by the keyPath or null */
    public static Object get(Map ph, String keyPath) {
        int i, n;
        String[] keys;
        Object o;
        Map h;
        if (!containsKey(ph, keyPath))
            return null;
        if (keyPath.charAt(0) != '/')
            return ph.get(keyPath);
        keys = Utils.split("/", keyPath);
        n = keys.length;
        h = ph;
        for (i=1; i<n; i++) {
            if (!h.containsKey(keys[i]))
                break;
            else if (i+1 == n)
                return h.get(keys[i]);
            o = h.get(keys[i]);
            if (o == null)
                break;
            else if (!(o instanceof Map))
                break;
            h = (Map) o;
        }
        return null;
    }

    /** parses PHP stream from in and returns a Map, MT-safe */
    public static Map parse(Reader in) throws IOException {
        int a = -1, level, action, offset, position, charsRead;
        int i, n, size = 0;
        String key = null, str;
        StringBuffer strBuf = null;
        Map root = new HashMap();
        Map current = null, parent = null;
        Map[] tree = new HashMap[1024];
        String[] nodeName = new String[1024];
        int[] len = new int[1024];
        char[] buffer = new char[bufferSize];
        level = 0;
        tree[level] = root;
        current = root;
        action = ACTION_SKIP;
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
                  case ACTION_COPY:
                    n = copy(buffer, offset, charsRead, strBuf, size);
                    size -= n;
                    if (size > 0) {
                        offset = charsRead;
                        continue;
                    }
                    else if (offset + n >= charsRead) {
                        offset = charsRead;
                        size = 0;
                        continue;
                    }
                    else
                        position = offset + n;
                    size = 0;
                    break;
                  case ACTION_SIZE:
                  case ACTION_COLON:
                    position = scan(buffer, offset, charsRead, ':');
                    break;
                  case ACTION_SEMI:
                  case ACTION_BOOL:
                    position = scan(buffer, offset, charsRead, ';');
                    break;
                  case ACTION_SKIP:
                  case ACTION_STEP:
                  case ACTION_LOOK:
                  case ACTION_NEXT:
                  case ACTION_FIND:
                    if (offset >= charsRead)
                        continue;
                    else
                        position = offset;
                    break;
                  default:
                    position = -1;
                }
                a = action;
                if (position >= 0) {
                    char c;
                    switch ((c = buffer[position])) {
                      case 'a': // begin of php map
                        if (action == ACTION_SKIP) {
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 's': // begin of php string
                        if (action == ACTION_SKIP) { // for key
                            offset = position + 1;
                            action = ACTION_NEXT;
                        }
                        else if (action == ACTION_LOOK) {
                            offset = position + 1;
                            action = ACTION_NEXT;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'i': // begin of php integer
                        if (action == ACTION_SKIP) {
                            offset = position + 1;
                            action = ACTION_STEP;
                        }
                        else if (action == ACTION_LOOK) {
                            offset = position + 1;
                            action = ACTION_STEP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'd': // begin of php float number
                        if (nodeName[level] == null)
                            action = ACTION_NONE;
                        else if (action == ACTION_SKIP) {
                            offset = position + 1;
                            action = ACTION_STEP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'b': // begin of php boolean
                        if (nodeName[level] == null)
                            action = ACTION_NONE;
                        else if (action == ACTION_SKIP) {
                            offset = position + 1;
                            action = ACTION_FIND;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'N': // php null
                        if (nodeName[level] == null)
                            action = ACTION_NONE;
                        else if(action == ACTION_SKIP || action == ACTION_LOOK){
                            key = nodeName[level];
                            nodeName[level] = null;
                            current.put(key, null);
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'O': // begin of php object
                        if (action == ACTION_SKIP) {
                            parent = current;
                            current = new HashMap();
                            level ++;
                            tree[level] = current;
                            nodeName[level] = null;
                            len[level] = 1;
                            if (level == 1) { // start Doc
                                nodeName[0] = "PHP";
                                len[0] = 1;
                                root.put("PHP", current);
                            }
                            else {
                                key = nodeName[level - 1];
                                parent.put(key, current);
                                nodeName[level - 1] = null;
                            }
                            offset = position + 1;
                            action = ACTION_NEXT;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '{': // begin of map
                        if (action == ACTION_SKIP) {
                            parent = current;
                            current = new HashMap();
                            level ++;
                            tree[level] = current;
                            nodeName[level] = null;
                            if (level == 1) { // start Doc
                                nodeName[0] = "PHP";
                                len[0] = 1;
                                root.put("PHP", current);
                            }
                            else {
                                key = nodeName[level - 1];
                                parent.put(key, current);
                                nodeName[level - 1] = null;
                            }
                            offset = position + 1;
                            action = ACTION_LOOK;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '}': // end of map or array
                        if (action == ACTION_LOOK || action == ACTION_SKIP) {
                            level --;
                            current = parent;
                            if (level > 1)
                                parent = tree[level-1];
                            else
                                parent = root;
                            nodeName[level] = null;
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '"': // double quote
                        if (action == ACTION_NEXT) { // begin of string
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_COPY;
                        }
                        else if (action == ACTION_COPY) { // end of string
                            str = strBuf.toString();
                            if (nodeName[level] == null) { // for key
                                nodeName[level] = str;
                                action = ACTION_LOOK;
                            }
                            else { // for value of string
                                key = nodeName[level];
                                nodeName[level] = null;
                                current.put(key, str);
                                action = ACTION_SKIP;
                            }
                            offset = position + 1;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case ':': // end of key
                        if (action == ACTION_COLON) { // for length of key
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            size = Integer.parseInt(str);
                            action = ACTION_NEXT;
                            offset = position + 1;
                        }
                        else if (action == ACTION_SIZE) { // for size of object
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            len[level+1] = Integer.parseInt(str);
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else if (action == ACTION_NEXT) {
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_COLON;
                        }
                        else if (action == ACTION_FIND) {
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_BOOL;
                        }
                        else if (action == ACTION_STEP) {
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_SEMI;
                        }
                        else if (action == ACTION_SKIP) {
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_SIZE;
                        }
                        else if (action == ACTION_LOOK) {
                            strBuf = new StringBuffer();
                            offset = position + 1;
                            action = ACTION_SIZE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case ';': // end of field
                        if (action == ACTION_SEMI) {
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            if (nodeName[level] == null) { // for key
                                nodeName[level] = str;
                                action = ACTION_SKIP;
                            }
                            else {
                                key = nodeName[level];
                                nodeName[level] = null;
                                current.put(key, str);
                                action = ACTION_LOOK;
                            }
                            offset = position + 1;
                        }
                        else if (action == ACTION_BOOL) { // for boolean value
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            str = ("0".equals(str)) ? "false" : "true";
                            key = nodeName[level];
                            nodeName[level] = null;
                            current.put(key, str);
                            offset = position + 1;
                            action = ACTION_LOOK;
                        }
                        else if (action == ACTION_NEXT) {
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else if (action == ACTION_SKIP) {
                            offset = position + 1;
                            action = ACTION_LOOK;
                        }
                        else if (action == ACTION_STEP) {
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else if (action == ACTION_LOOK) {
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      default:
                        offset = position + 1;
                        break;
                    }
                    if (action == ACTION_NONE)
                        throw(new IOException("bad state at level: " +
                            level + " " + a + "/" + action + " " + c + ":" +
                            offset + " " + position + ": " +
                            new String(buffer, offset, position - offset) +
                            new String(buffer, position,
                            ((charsRead-position>32)?32:charsRead-position))));
                }
                else {
                    str = new String(buffer, offset, charsRead - offset);
                    strBuf.append(str);
                    offset = charsRead;
                }
            } while (offset < charsRead);
        }

        return root;
    }

    /** returns the position of the first target or -1 if not found */
    private static int scan(char[] buffer, int offset, int length, char target){
        int i;
        char c;
        for (i=offset; i<length; i++) {
            c = buffer[i];
            if (c == target)
                return i;
        }
        return -1;
    }

    /** returns number of chars copied over */
    private static int copy(char[] buffer, int offset, int length,
        StringBuffer strBuf, int size) {
        int i = length - offset;
        if (size <= i) {
            strBuf.append(new String(buffer, offset, size));
            return size;
        }
        else if (i > 0) {
            strBuf.append(new String(buffer, offset, i));
            return i;
        }
        else
            return 0;
    }

    public static void main(String args[]) {
        String filename = null, action = "parse", type = "json", path = null;
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
            FileReader in = new FileReader(filename); 
            Map ph = parse(in);
            if ("parse".equalsIgnoreCase(action)) {
                if ("json".equals(type)) {
                    ph = (Map) ph.get("PHP");
                    System.out.println(toJSON(ph, "", "\n"));
                }
                else
                    System.out.println(toXML(ph, "", "\n"));
            }
            else if (path != null && path.length() > 0) {
                ph = (Map) ph.get("PHP");
                if ("test".equalsIgnoreCase(action)) {
                    if (containsKey(ph, path))
                        System.out.println(path + ": Yes");
                    else
                        System.out.println(path + ": No");
                }
                else if ("get".equalsIgnoreCase(action)) {
                    Object o;
                    if (containsKey(ph, path)) {
                        o = get(ph, path);
                        if (o == null)
                            System.out.println(path + ": null");
                        else if (o instanceof String)
                            System.out.println(path + ": " + (String) o);
                        else
                            System.out.println(path + ": " +
                                toJSON((Map)o,"","\n"));
                    }
                    else
                        System.out.println(path + ": No");
                }
                else if ("count".equalsIgnoreCase(action)) {
                    System.out.println(path + ": " + count(ph, path));
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
        System.out.println("PHP2Map Version 1.0 (written by Yannan Lu)");
        System.out.println("PHP2Map: unserialize PHP data in a file");
        System.out.println("Usage: java org.qbroker.common.PHP2Map -f filename -a action -t type -k path");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of parse, test, count or get (default: parse)");
        System.out.println("  -k: key path (example: /views/display/cache)");
        System.out.println("  -t: type of xml or json (default: json)");
        System.out.println("  -f: filename");
    }
}
