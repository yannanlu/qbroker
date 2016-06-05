package org.qbroker.json;

/* JSON2Map.java - to load a Map from a JSON file */

import java.io.Reader;
import java.io.FileReader;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.Utils;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;

/**
 * JSON2Map parses a JSON text stream and maps the structural content of the
 * JSON into Java objects stored in a Map.  It only recognizes three basic
 * Java objects, String, List and Map.  A Map or a List can contain any objects
 * of the three types.  They are the nodes of the data tree.  A String is only
 * a leaf in the data tree.  For JSON's numbers, decimals and boolean values,
 * they will be stored as String. For JSON's null, it will stored as null.
 * The comments in Javascript style will be ignored. With these three basic
 * types of objects, any data structures will be representable. The mapping
 * is one-to-one.  Therefore, the mapping is bi-directional.
 *<br/><br/>
 * The method of parse() assumes the JSON data is either a Map or a List and
 * returns a Map or a List representing the unescaped JSON data. The method of
 * isArray() can be used to test if the JSON data is a Map or a List.
 *<br/><br/>
 * The method of get() returns the JSON object referenced by the JSONPath.
 * Currently, it only supports simple JSONPath with delimiter of dot. The array
 * index can have a number only. It does not support fancy expressions. Please
 * check with dust.js for the JSON path.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSON2Map {
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
    private final static int ACTION_NEXT = 4;    // for next comment chars
    private final static int ACTION_LINE = 5;    // for line comment
    private final static int ACTION_STAR = 6;    // for star
    private final static int ACTION_COLON = 7;   // for colon or backslash
    private final static int ACTION_QUOTE = 8;   // for double quote
    private final static int ACTION_MULTI = 9;   // for multi-line comment
    private final static int ACTION_ESCAPE = 10; // for next escaped chars

    private JSON2Map() {
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
                    strBuf.append("\n" + indent + "  \"a:" +
                        Utils.escapeJSON(key) + "\": null");
                    continue;
                }
                else if ((o = ps.get(key)) == null) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" +
                        Utils.escapeJSON(key) + "\": null");
                }

                // list original value
                strBuf.append(",");
                if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else // for object
                    strBuf.append("\n" + indent + "  \"O:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof Map) { // map
                Map ph = (Map) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof Map) {
                    if (!ph.equals((Map) o)) { // different 
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" +
                            Utils.escapeJSON(key) + "\": " +
                            diff((Map) o, ph, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON(ph, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": null");
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof List) { // list
                List pl = (List) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof List) {
                    if (!pl.equals((List) o)) { // different
                        if (i++ > 0)
                            strBuf.append(",");
                        strBuf.append("\n" + indent + "  \"m:" +
                            Utils.escapeJSON(key) + "\": " +
                            diff((List) o, pl, indent + "  "));
                    }
                    continue;
                }
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON(pl, indent + "  ", "\n"));
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else if (o instanceof String) { // string
                String value = (String) o;
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"a:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                    continue;
                }
                else if ((o = ps.get(key)) != null && o instanceof String &&
                    value.equals((String) o)) // same
                    continue;
                else {
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"c:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON(o.toString()) + "\"");
            }
            else { // other object
                String value = o.toString();
                if (!ps.containsKey(key)) { // new
                    if (i++ > 0)
                        strBuf.append(",");
                    strBuf.append("\n" + indent + "  \"A:" +
                        Utils.escapeJSON(key) + "\": \"" +
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
                    strBuf.append("\n" + indent + "  \"C:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeXML(value) + "\"");
                }

                // list original value
                strBuf.append(",");
                if (o == null)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": null");
                else if (o instanceof Map)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((Map) o, indent + "  ", "\n"));
                else if (o instanceof List)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": " +
                        toJSON((List) o, indent + "  ", "\n"));
                else if (o instanceof String)
                    strBuf.append("\n" + indent + "  \"o:" +
                        Utils.escapeJSON(key) + "\": \"" +
                        Utils.escapeJSON((String) o) + "\"");
                else
                    strBuf.append("\n" + indent + "  \"O:" +
                        Utils.escapeJSON(key) + "\": \"" +
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
                strBuf.append("\n" + indent + "  \"d:" +
                    Utils.escapeJSON(key) + "\": null");
            else if (o instanceof Map)
                strBuf.append("\n" + indent + "  \"d:" +
                    Utils.escapeJSON(key) + "\": " +
                    toJSON((Map) o, indent + "  ", "\n"));
            else if (o instanceof List)
                strBuf.append("\n" + indent + "  \"d:" +
                    Utils.escapeJSON(key) + "\": " +
                    toJSON((List) o, indent + "  ", "\n"));
            else if (o instanceof String)
                strBuf.append("\n" + indent + "  \"d:" +
                    Utils.escapeJSON(key) + "\": \"" +
                    Utils.escapeJSON((String) o) + "\"");
            else
                strBuf.append("\n" + indent + "  \"D:" +
                    Utils.escapeJSON(key) + "\": \"" +
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
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (ph == null)
            throw(new IllegalArgumentException("null data"));
        if (keyPath == null || keyPath.length() <= 0)
            throw(new IllegalArgumentException("bad path: " + keyPath));
        if (".".equals(keyPath)) // for itself
            return true;
        if (ph.size() <= 0)
            return false;
        if (keyPath.charAt(0) == '.')
            j = 1;
        if (keyPath.indexOf('[') > 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = ph;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n) // end of test
                    return true;
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n) // end of test
                    return true;
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return false;
    }

    /** returns true if the keyPath exists or false otherwise */
    public static boolean containsKey(List pl, String keyPath) {
        int i, k, n, j = 0;
        String key;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (pl == null)
            throw(new IllegalArgumentException("null data"));
        if (keyPath == null || keyPath.length() <= 0)
            throw(new IllegalArgumentException("bad path: " + keyPath));
        if (".".equals(keyPath)) // for itself
            return true;
        if (pl.size() <= 0)
            return false;
        if (keyPath.charAt(0) == '.')
            j = (keyPath.charAt(1) == '[') ? 2 : 1;
        else if (keyPath.charAt(0) == '[')
            j = 1;
        if (keyPath.indexOf('[') >= 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = pl;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n) // end of test
                    return true;
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n) // end of test
                    return true;
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return false;
    }

    /** returns the number of children referenced by the keyPath or -1 */
    public static int count(Map ph, String keyPath) {
        Object o;
        if (ph == null || keyPath == null)
            return -1;
        if (!containsKey(ph, keyPath))
            return 0;
        if ((o = get(ph, keyPath)) == null)
            return -1;
        else if (o instanceof Map)
            return ((Map) o).size();
        else if (o instanceof List)
            return ((List) o).size();
        else
            return 1;
    }

    /** returns the number of children referenced by the keyPath or -1 */
    public static int count(List pl, String keyPath) {
        Object o;
        if (pl == null || keyPath == null)
            return -1;
        if (!containsKey(pl, keyPath))
            return 0;
        if ((o = get(pl, keyPath)) == null)
            return -1;
        else if (o instanceof Map)
            return ((Map) o).size();
        else if (o instanceof List)
            return ((List) o).size();
        else
            return 1;
    }

    /** returns the removed object referenced by the keyPath or null */
    public static Object remove(Map ph, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(ph, keyPath) || ".".equals(keyPath))
            return null;
        if (keyPath.charAt(0) == '.')
            j = 1;
        if (keyPath.indexOf('[') > 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = ph;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h.remove(keys[i]);
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a.remove(k);
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    /** returns the removed object referenced by the keyPath or null */
    public static Object remove(List pl, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(pl, keyPath) || ".".equals(keyPath))
            return null;
        if (keyPath.charAt(0) == '.')
            j = (keyPath.charAt(1) == '[') ? 2 : 1;
        else if (keyPath.charAt(0) == '[')
            j = 1;
        if (keyPath.indexOf('[') >= 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = pl;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h.remove(keys[i]);
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a.remove(k);
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    /** returns the parent of the object referenced by the keyPath or null */
    public static Object getParent(Map ph, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(ph, keyPath) || ".".equals(keyPath))
            return null;
        if (keyPath.charAt(0) == '.')
            j = 1;
        if (keyPath.indexOf('[') > 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = ph;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h;
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a;
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    /** returns the parent of the object referenced by the keyPath or null */
    public static Object getParent(List pl, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(pl, keyPath) || ".".equals(keyPath))
            return null;
        if (keyPath.charAt(0) == '.')
            j = (keyPath.charAt(1) == '[') ? 2 : 1;
        else if (keyPath.charAt(0) == '[')
            j = 1;
        if (keyPath.indexOf('[') >= 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = pl;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h;
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a;
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    /** returns the raw object referenced by the keyPath or null */
    public static Object get(Map ph, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(ph, keyPath))
            return null;
        if (".".equals(keyPath)) // for itself
            return ph;
        if (keyPath.charAt(0) == '.')
            j = 1;
        if (keyPath.indexOf('[') > 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = ph;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h.get(keys[i]);
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a.get(k);
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    /** returns the raw object referenced by the keyPath or null */
    public static Object get(List pl, String keyPath) {
        int i, k, n, j = 0;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(pl, keyPath))
            return null;
        if (".".equals(keyPath)) // for itself
            return pl;
        if (keyPath.charAt(0) == '.')
            j = (keyPath.charAt(1) == '[') ? 2 : 1;
        else if (keyPath.charAt(0) == '[')
            j = 1;
        if (keyPath.indexOf('[') >= 0) // for array access
            keyPath = keyPath.replace('[', '.').replaceAll("]", "");
        keys = Utils.split(".", keyPath);
        n = keys.length;
        o = pl;
        for (i=j; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h.get(keys[i]);
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return a.get(k);
                o = a.get(k);
                if (o == null)
                    break;
                else if (!(o instanceof Map || o instanceof List))
                    break;
            }
        }
        return null;
    }

    public static int getDataType(Map ph, String keyPath) {
        Object o = get(ph, keyPath);
        if (o == null)
            return JSON_NULL;
        else if (o instanceof String)
            return JSON_STRING;
        else if (o instanceof Number)
            return JSON_NUMBER;
        else if (o instanceof Boolean)
            return JSON_BOOLEAN;
        else if (o instanceof List)
            return JSON_ARRAY;
        else if (o instanceof Map)
            return JSON_MAP;
        else
            return -1;
    }

    public static int getDataType(List pl, String keyPath) {
        Object o = get(pl, keyPath);
        if (o == null)
            return JSON_NULL;
        else if (o instanceof String)
            return JSON_STRING;
        else if (o instanceof Number)
            return JSON_NUMBER;
        else if (o instanceof Boolean)
            return JSON_BOOLEAN;
        else if (o instanceof List)
            return JSON_ARRAY;
        else if (o instanceof Map)
            return JSON_MAP;
        else
            return -1;
    }

    /**puts raw data and returns the replaced object referenced by the keyPath*/
    public static Object put(Map ph, String keyPath, Object data) {
        int i;
        String key;
        Object parent;

        if (keyPath == null || keyPath.length() <= 0 || ".".equals(keyPath))
            throw(new IllegalArgumentException("bad path: " + keyPath));

        if (data == null)
            return null;

        if (containsKey(ph, keyPath)) {
            parent = getParent(ph, keyPath);
            if (keyPath.indexOf('[') > 0) // for array access
                keyPath = keyPath.replace('[', '.').replaceAll("]", "");

            if ((i = keyPath.lastIndexOf('.')) >= 0)
                key = keyPath.substring(i+1);
            else
                key = keyPath;
        }
        else { // it is a new key
            if (keyPath.indexOf('[') > 0) // for array access
                keyPath = keyPath.replace('[', '.').replaceAll("]", "");
            if ((i = keyPath.lastIndexOf('.')) >= 0) {
                parent = get(ph, keyPath.substring(0, i));
                key = keyPath.substring(i+1);
            }
            else {
                parent = ph;
                key = keyPath;
            }
        }

        if (parent instanceof Map) { // Map
            return ((Map) parent).put(key, data);
        }
        else if (key.matches("\\d+")) { // List
            i = Integer.parseInt(key);
            return ((List) parent).set(i, data);
        }
        else
            throw(new IllegalArgumentException("not a number for array"));
    }

    /**puts raw data and returns the replaced object referenced by the keyPath*/
    public static Object put(List pl, String keyPath, Object data) {
        int i;
        String key;
        Object parent;

        if (keyPath == null || keyPath.length() <= 0 || ".".equals(keyPath))
            throw(new IllegalArgumentException("bad path: " + keyPath));

        if (data == null)
            return null;

        if (containsKey(pl, keyPath)) {
            parent = getParent(pl, keyPath);
            if (keyPath.indexOf('[') >= 0) // for array access
                keyPath = keyPath.replace('[', '.').replaceAll("]", "");

            if ((i = keyPath.lastIndexOf('.')) >= 0)
                key = keyPath.substring(i+1);
            else
                key = keyPath;
        }
        else { // it is a new key
            if (keyPath.indexOf('[') >= 0) // for array access
                keyPath = keyPath.replace('[', '.').replaceAll("]", "");

            if ((i = keyPath.lastIndexOf('.')) >= 0) {
                parent = get(pl, keyPath.substring(0, i));
                key = keyPath.substring(i+1);
            }
            else {
                parent = pl;
                key = keyPath;
            }
        }

        if (parent instanceof Map) { // Map
            return ((Map) parent).put(key, data);
        }
        else if (key.matches("\\d+")) { // List
            i = Integer.parseInt(key);
            return ((List) parent).set(i, data);
        }
        else
            throw(new IllegalArgumentException("not a number for array"));
    }

    /** returns the formatted JSON text for the given template or null */
    public static String format(Map ph, Template temp, Perl5Matcher pm) {
        String key, value, text;
        String[] textFields;
        Object o;

        if (ph == null || temp == null)
            return null;

        text = temp.copyText();
        textFields = temp.getAllFields();
        for (int i=0; i<textFields.length; i++) {
            key = textFields[i];
            o = get(ph, key); 
            if (o == null)
                value = "";
            else if (o instanceof String)
                value = (String) o;
            else if (o instanceof Map)
                value = toJSON((Map) o, null, null);
            else if (o instanceof List)
                value = toJSON((List) o, null, null);
            else
                value = o.toString();

            text = temp.substitute(pm, key, value, text);
        }
        return text;
    }

    /** returns the formatted string for the given template or null on faluire*/
    public static String format(Map ph, Template temp) {
        return format(ph, temp, null);
    }

    /** returns the formatted JSON text for the given template or null */
    public static String format(List pl, Template temp, Perl5Matcher pm) {
        String key, value, text;
        String[] textFields;
        Object o;

        if (pl == null || temp == null)
            return null;

        text = temp.copyText();
        textFields = temp.getAllFields();
        for (int i=0; i<textFields.length; i++) {
            key = textFields[i];
            o = get(pl, key); 
            if (o == null)
                value = "";
            else if (o instanceof String)
                value = (String) o;
            else if (o instanceof Map)
                value = toJSON((Map) o, null, null);
            else if (o instanceof List)
                value = toJSON((List) o, null, null);
            else
                value = o.toString();

            text = temp.substitute(pm, key, value, text);
        }
        return text;
    }

    /** returns the formatted string for the given template or null on faluire*/
    public static String format(List pl, Template temp) {
        return format(pl, temp, null);
    }

    /** parses JSON stream from in and returns either a Map or a List with
     *  unescaped JSON data. It is MT-safe
     */
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
                    position = skip(buffer, offset, charsRead);
                    break;
                  case ACTION_FIND:
                    position = skip(buffer, offset, charsRead);
                    break;
                  case ACTION_LOOK:
                    position = look(buffer, offset, charsRead);
                    break;
                  case ACTION_COLON:
                    position = scan(buffer, offset, charsRead, ':');
                    break;
                  case ACTION_QUOTE:
                    position = scan(buffer, offset, charsRead, '"');
                    break;
                  case ACTION_LINE:
                    position = scan(buffer, offset, charsRead, '\n');
                    break;
                  case ACTION_STAR:
                    position = scan(buffer, offset, charsRead, '*');
                    break;
                  case ACTION_NEXT:
                    position = next(buffer, offset, charsRead);
                    break;
                  case ACTION_MULTI:
                    position = next(buffer, offset, charsRead);
                    break;
                  case ACTION_ESCAPE:
                    position = escape(buffer, offset, charsRead);
                    if (position < 0) { // other escaped char not supported yet
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    nodeName[level] = null;
                                    ((Map) current).put(key, str);
                                }
                                else
                                    ((List) current).add(str);
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                ((List) current).add(str);
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            key = nodeName[level];
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                ((Map) current).put(key, str);
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // for null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    ((Map) current).put(key, str);
                                    nodeName[level] = null;
                                }
                                else
                                    ((List) current).add(str);
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    nodeName[level] = null;
                                    ((Map) current).put(key, str);
                                }
                                else
                                    ((List) current).add(str);
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (current instanceof Map) {
                                    key = nodeName[level];
                                    ((Map) current).put(key, str);
                                    nodeName[level] = null;
                                }
                                else
                                    ((List) current).add(str);
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
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append('\r');
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case 'b':
                        if (action == ACTION_ESCAPE) { // for backslash
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
                                action = ACTION_NONE;
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

    /** returns the position of the first target or -1 if not found */
    public static int scan(char[] buffer, int offset, int length,
        char target) {
        int i;
        char c;
        switch (target) {
          case '"':
            for (i=offset; i<length; i++) { // for quote or backslash
                c = buffer[i];
                if (c == target || c == '\\' || c == '\n' || c == '\r' ||
                    c == '\f' || c == '\t' || c == '\b')
                    return i;
            }
            break;
          case ':':
            for (i=offset; i<length; i++) { // for colon, comma, or quote, etc
                c = buffer[i];
                if (c == target || c == '"' || c == ',' || c == '{' ||
                    c == '}' || c == '[' || c == ']' || c == '\\' ||
                    c == '/') 
                    return i;
            }
            break;
          default:
            for (i=offset; i<length; i++) {
                c = buffer[i];
                if (c == target)
                    return i;
            }
        }
        return -1;
    }

    /** returns the position of the first comment char or -1 if not found */
    public static int next(char[] buffer, int offset, int length) {
        char c = buffer[offset];
        if (c == '/' || c == '*')
            return offset;
        else
            return -1;
    }

    /** returns the position of the first escaped char or -1 if not found */
    public static int escape(char[] buffer, int offset, int length) {
        char c = buffer[offset];
        if (c == '\\' || c == '"' || c == 'n' || c == 'r' || c == 'f' ||
            c == 't' || c == 'b' || c == 'u' || c == '/')
            return offset;
        else
            return -1;
    }

    /** returns the position of the first end chars or -1 if not found */
    public static int look(char[] buffer, int offset, int length) {
        int i;
        char c;
        for (i=offset; i<length; i++) {
            c = buffer[i];
            if (c == ',' || c == ']' || c == '}' ||  c == '/' || c == '\n' ||
                c == '\t' || c == '\f' || c == '\r' || c == '\b' || c == ':' ||
                c == '"' || c == '{' || c == '[' || c == '\\') {
                return i;
            }
        }
        return -1;
    }

    /** returns the position of the first non-space char or -1 if not found */
    public static int skip(char[] buffer, int offset, int length) {
        int i;
        char c;
        for (i=offset; i<length; i++) {
            c = buffer[i];
            if (c != ' ' && c != '\n' && c != '\t' && c != '\f' && c!= '\r' &&
                c != '\b')
                return i;
        }
        return -1;
    }

    /** trims off white space chars on both ends and returns the string */
    public static String trim(StringBuffer strBuf) {
        char c;
        int i;
        if (strBuf == null)
            return null;
        i = strBuf.length();
        if (i <= 0)
            return strBuf.toString();
        while ((c = strBuf.charAt(0)) == ' ' || c == '\n' || c == '\t' ||
            c == '\f' || c == '\r' || c == '\b')
            strBuf.deleteCharAt(0);
        
        i = strBuf.length();
        while (--i >= 0) {
            c = strBuf.charAt(i);
            if (c == ' ' || c == '\n' || c == '\t' || c == '\f' || c == '\r' ||
                c == '\b')
                strBuf.deleteCharAt(i);
        }

        return strBuf.toString();
    }

    public static boolean isHash(String text) {
        int i = locate(0, text);
        return (i >= 0 && text.charAt(i) == '{');
    }

    public static boolean isArray(String text) {
        int i = locate(0, text);
        return (i >= 0 && text.charAt(i) == '[');
    }

    public static int locate(int id, String text) {
        return locate(id, text, 0);
    }

    /**
     * locates the end position of the first JSON object on the id-th level
     * from the starting point of fromIndex and returns the position or -1 upon
     * failure. If id = 0, it returns the beginning position of the json data.
     */
    public static int locate(int id, String text, int fromIndex) {
        int a = -1, level, action, offset, position, charsRead, pa, ln;
        boolean with_comma = false;
        String key = null, str;
        StringBuffer strBuf = null;
        String[] nodeName = new String[1024];
        int[] nodeType = new int[1024];
        char[] buffer;
        ln = 1;
        level = 0;
        action = ACTION_SKIP;

        if (id < 0 || text == null || text.length() <= 0)
            return -1;
        buffer = text.toCharArray();
        charsRead = buffer.length;
        if (charsRead > 0) {
            offset = 0;
            position = 0;
            do {
                switch (action) {
                  case ACTION_SKIP:
                    position = skip(buffer, offset, charsRead);
                    break;
                  case ACTION_FIND:
                    position = skip(buffer, offset, charsRead);
                    break;
                  case ACTION_LOOK:
                    position = look(buffer, offset, charsRead);
                    break;
                  case ACTION_COLON:
                    position = scan(buffer, offset, charsRead, ':');
                    break;
                  case ACTION_QUOTE:
                    position = scan(buffer, offset, charsRead, '"');
                    break;
                  case ACTION_LINE:
                    position = scan(buffer, offset, charsRead, '\n');
                    break;
                  case ACTION_STAR:
                    position = scan(buffer, offset, charsRead, '*');
                    break;
                  case ACTION_NEXT:
                    position = next(buffer, offset, charsRead);
                    break;
                  case ACTION_MULTI:
                    position = next(buffer, offset, charsRead);
                    break;
                  case ACTION_ESCAPE:
                    position = escape(buffer, offset, charsRead);
                    if (position < 0) { // other escaped char not supported yet
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
                        throw(new IllegalArgumentException(
                            "unsupported escape char '\\" + buffer[offset] +
                            "' at line " + ln + c + i + " and level " +
                            level + ((i==0) ? ":\n\\" : ":\n") +
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
                            if (id == 0 && level == 0)
                                return position;
                            with_comma = false;
                            level ++;
                            nodeName[level] = null;
                            nodeType[level] = 1;
                            if (level == 1) { // start Doc
                                nodeName[0] = "JSON";
                                nodeType[0] = 1;
                            }
                            else {
                                if (nodeType[level - 1] == 1) {
                                    key = nodeName[level - 1];
                                }
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
                            if (id == 0 && level == 0)
                                return position;
                            with_comma = false;
                            level ++;
                            nodeType[level] = 0;
                            if (nodeType[level - 1] == 1) {
                                key = nodeName[level - 1];
                            }
                            offset = position + 1;
                            action = ACTION_SKIP;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      case '"': // double quote
                        if (action == ACTION_SKIP) { // begin of string
                            if (id == 0 && level == 0)
                                return position;
                            with_comma = false;
                            offset = position + 1;
                            strBuf = new StringBuffer();
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_QUOTE) { // end of string
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = strBuf.toString();
                            offset = position + 1;
                            if (nodeType[level] == 1) {
                                if (nodeName[level] == null) { // for key
                                    nodeName[level] = str;
                                    action = ACTION_COLON;
                                }
                                else {
                                    key = nodeName[level];
                                    nodeName[level] = null;
                                    action = ACTION_FIND;
                                }
                            }
                            else
                                action = ACTION_FIND;
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
                               throw(new IllegalArgumentException("empty key"));
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (nodeType[level] == 1)
                                    nodeName[level] = null;
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
                        if (nodeType[level] != 0) // not for a list
                            action = ACTION_NONE;
                        else if (with_comma) // trailing comma
                            action = ACTION_NONE;
                        else if (action == ACTION_FIND) {
                            level --;
                            nodeName[level] = null;
                            offset = position + 1;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                level --;
                                nodeName[level] = null;
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else if (action == ACTION_SKIP) { // empty array
                            level --;
                            nodeName[level] = null;
                            offset = position + 1;
                            action = ACTION_FIND;
                        }
                        else
                            action = ACTION_NONE;
                        if (position > fromIndex && level == id &&
                            action != ACTION_NONE)
                            return position;
                        break;
                      case '}': // end of map
                        if (nodeType[level] != 1) // not for a Map
                            action = ACTION_NONE;
                        else if (with_comma) // trailing comma
                            action = ACTION_NONE;
                        else if (action == ACTION_FIND) {
                            level --;
                            nodeName[level] = null;
                            offset = position + 1;
                        }
                        else if (action == ACTION_LOOK) { // end of no quotes
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                level --;
                                nodeName[level] = null;
                                offset = position + 1;
                                action = ACTION_FIND;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else if (action == ACTION_SKIP) { // empty map
                            level --;
                            nodeName[level] = null;
                            offset = position + 1;
                            action = ACTION_FIND;
                        }
                        else
                            action = ACTION_NONE;
                        if (position > fromIndex && level == id &&
                            action != ACTION_NONE)
                            return position;
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (nodeType[level] == 1)
                                    nodeName[level] = null;
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
                            str = trim(strBuf);
                            if (a == ACTION_COLON) // for key
                                nodeName[level] = str;
                            else if (nodeType[level] == 1) {
                                nodeName[level] = null;
                            }
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (nodeType[level] == 1)
                                    nodeName[level] = null;
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
                            boolean isRaw = true;
                            str = new String(buffer, offset, position - offset);
                            strBuf.append(str);
                            str = trim(strBuf);
                            if ("null".equals(str)) // null
                                str = null;
                            else if ("true".equals(str) || "false".equals(str))
                                isRaw = true;
                            else try {
                                Object o;
                                o = (str.indexOf('.') >= 0) ? new Double(str) :
                                    new Long(str);
                            }
                            catch (Exception e) {
                                isRaw = false;
                            }
                            if (isRaw) {
                                if (nodeType[level] == 1)
                                    nodeName[level] = null;
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
                      case 't':
                      case 'f':
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append(c);
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else if (action == ACTION_SKIP) { // begin of no quotes
                            if(nodeType[level] == 0 || nodeName[level] != null){
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
                      case 'b':
                        if (action == ACTION_ESCAPE) { // for backslash
                            strBuf.append(c);
                            offset = position + 1;
                            action = ACTION_QUOTE;
                        }
                        else
                            action = ACTION_NONE;
                        break;
                      default:
                        if (action == ACTION_SKIP) { // begin of no quotes
                            if(nodeType[level] == 0 || nodeName[level] != null){
                                offset = position;
                                strBuf = new StringBuffer();
                                action = ACTION_LOOK;
                            }
                            else
                                action = ACTION_NONE;
                        }
                        else
                            action = ACTION_NONE;
                    }
                    if (action == ACTION_NONE) {
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
                        throw(new IllegalArgumentException(
                            "stopped parsing on '" + c + "' at line " +
                            ln + b + i + " and level " + level + " " +
                            pa + "/" + a + ":\n" +
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
            ln += getLineNo(buffer, position);
        }

        return -1;
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
                k = locate(i, json, fromIndex);
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
                        System.out.println(toJSON((List) o, "", "\n"));
                    else
                        System.out.println(toJSON((Map) o, "", "\n"));
                }
                else {
                    Map map = new HashMap();
                    map.put("JSON", o);
                    System.out.println(toXML(map, "", "\n"));
                }
            }
            else if ("flatten".equalsIgnoreCase(action) && o instanceof Map) {
                Map map = (Map) o;
                flatten(map);
                System.out.println(toJSON(map, "", "\n"));
            }
            else if (path != null && path.length() > 0) { // with path
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
                else if ("diff".equalsIgnoreCase(action)) { // path is target
                    in = new FileReader(path);
                    Object obj = JSON2Map.parse(in);
                    in.close();
                    if (obj == null)
                        System.out.println(path + ": null");
                    else {
                        if (o instanceof List)
                            data = diff((List) o, (List) obj, "");
                        else
                            data = diff((Map) o, (Map) obj, "");
                        if (data != null)
                            System.out.println(filename + ", " + path +
                                " diff:\n" + data);
                        else
                            System.out.println(filename + ", " + path +
                                " no diff");
                    }
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
        System.out.println("JSON2Map Version 1.0 (written by Yannan Lu)");
        System.out.println("JSON2Map: parse JSON data in a file");
        System.out.println("Usage: java org.qbroker.json.JSON2Map -f filename -a action -t type -k path");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of parse, test, count, locate, get, put, remove, format, diff or flatten (default: parse)");
        System.out.println("  -k: key path (example: .views.list[0].name)");
        System.out.println("  -d: json data for put");
        System.out.println("  -t: type of xml or json (default: json)");
        System.out.println("  -b: starting position to locate (default: 0)");
        System.out.println("  -f: filename");
    }
}
