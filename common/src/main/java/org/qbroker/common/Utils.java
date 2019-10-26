package org.qbroker.common;

/* Utils.java - utilities for common operations */

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.lang.reflect.InvocationTargetException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NamedNodeMap;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.TraceStackThread;

/**
 * Generic Utilities
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class Utils {
    public static final int EVAL_NONE = 0;
    public static final int EVAL_ADD = 1;
    public static final int EVAL_SUB = 2;
    public static final int EVAL_MUL = 3;
    public static final int EVAL_DIV = 4;
    public static final int EVAL_ABS = 5;
    public static final int EVAL_PARSE = 6;
    public static final int EVAL_FORMAT = 7;
    public static final int RESULT_TEXT = 1;
    public static final int RESULT_BYTES = 2;
    public static final int RESULT_XML = 4;
    public static final int RESULT_JSON = 8;
    public static final int RESULT_LIST = 16;
    public static final int RESULT_SET = 32;
    public static final int RESULT_POSTABLE = 64;
    public static final int RESULT_COLLECTIBLE = 128;
    public static final String FS = " | ";
    public static final String RS = "\n";
    private static ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>();
    private static final String dfStr = "yyyy/MM/dd.HH:mm:ss.SSS.zz";
    private static java.lang.reflect.Method decrypt = null;

    public Utils() {
    }

    /**
     * It prints out error msg and rethrows it if it is an Error
     */
    public static void flush(Throwable e) {
        if (e instanceof Error) {
            System.err.println(new Date().toString() +
                " " + Thread.currentThread().getName() + ": "
                + e.toString());
            System.err.flush();
            throw((Error) e);
        }
        else {
            System.err.println(new Date().toString() +
                " " + Thread.currentThread().getName() + ": " +
                TraceStackThread.traceStack(e));
            System.err.flush();
        }
    }

    // method used to trace stack on an exception
    public static String traceStack(Throwable e) {
        return TraceStackThread.traceStack(e);
    }

    public static String dateFormat(Date date) {
        DateFormat dateFormat = df.get();
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(dfStr);
            df.set(dateFormat);
        }
        return dateFormat.format(date);
    }

    public static Date parseDate(String date) {
        DateFormat dateFormat = df.get();
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(dfStr);
            df.set(dateFormat);
        }
        return dateFormat.parse(date, new ParsePosition(0));
    }

    public static String escapeJSON(String text) {
        int i;
        String[] escaped = new String[] {"\\\\", "\\n", "\\r", "\\\"", "\\b",
            "\\f", "\\t"};
        String[] unescaped = new String[] {"\\", "\n", "\r", "\"", "\b", "\f",
            "\t"};
        if (text == null || text.length() <= 0)
            return text;
        for (i=0; i<escaped.length; i++)
            text = doSearchReplace(unescaped[i], escaped[i], text);
        return text;
    }

    public static String unescapeJSON(String text) {
        int i;
        String[] escaped = new String[] {"\\\\", "\\n", "\\r", "\\\"", "\\b",
            "\\f", "\\t"};
        String[] unescaped = new String[] {"\\", "\n", "\r", "\"", "\b", "\f",
            "\t"};
        if (text == null || text.length() <= 0)
            return text;
        for (i=0; i<escaped.length; i++)
            text = doSearchReplace(escaped[i], unescaped[i], text);
        return text;
    }

    public static String toJSON(Map data) {
        int k = 0;
        String key, value;
        StringBuffer strBuf;
        if (data == null)
            return null;
        strBuf = new StringBuffer();
        for (Object o : data.keySet()) {
            key = (String) o;
            if (key == null || key.length() <= 0)
                continue;
            o = data.get(key);
            if (o == null)
                continue;
            else if (o instanceof String)
                value = (String) o;
            else
                value = o.toString();
            if (k ++ > 0)
                strBuf.append(",");
            strBuf.append("\"" + escapeJSON(key) + "\": \"" +
                escapeJSON(value) + "\"\n");
        }
        return "{" + strBuf.toString() + "}";
    }

    public static String toJSON(Map data, String[] attrs) {
        int k = 0;
        String value;
        StringBuffer strBuf;
        Object o;
        if (data == null)
            return null;
        if (attrs == null || attrs.length <= 0)
            return toJSON(data);
        strBuf = new StringBuffer();
        for (String key : attrs) {
            if (key == null || key.length() <= 0)
                continue;
            o = data.get(key);
            if (o == null)
                continue;
            else if (o instanceof String)
                value = (String) o;
            else
                value = o.toString();
            if (k ++ > 0)
                strBuf.append(",");
            strBuf.append("\"" + escapeJSON(key) + "\": \"" +
                escapeJSON(value) + "\"\n");
        }
        return "{" + strBuf.toString() + "}";
    }

    public static String escapeXML(String text) {
        int i;
        String[] escaped = new String[] {"&amp;", "&lt;", "&gt;", "&quot;",
            "&apos;"};
        String[] unescaped = new String[] {"&", "<", ">", "\"", "'"};
        if (text == null || text.length() <= 0)
            return text;
        for (i=0; i<escaped.length; i++)
            text = doSearchReplace(unescaped[i], escaped[i], text);
        return text;
    }

    public static String doSearchReplace(String s, String r, String text) {
        int len, i, j, k, d;

        if (s == null || (len = s.length()) == 0 || text == null ||
            (i  = text.indexOf(s)) < 0)
            return text;

        len = s.length();
        d = r.length() - len;
        j = i;
        k = i;
        StringBuffer buffer = new StringBuffer(text);
        while (i >= j) {
            k += i - j;
            buffer.replace(k, k+len, r);
            k += d;
            j = i;
            i = text.indexOf(s, j+len);
        }
        return buffer.toString();
    }

    /**
     * splits text with delimiter of s and fills the key-value pair to the Map
     * after replacing all the escaped delimiters by the original delimiter.
     */
    public static int split(String s, String e, String text,
        Map<String, String> attr) {
        int len, i, j = 0, n = 0;
        String key, value;

        if (s == null || text == null || attr == null)
            return 0;
        else if ((i = text.indexOf(s)) < 0)
            return 0;

        len = s.length();
        while (i >= j) {
            key = text.substring(j, i);
            j = i + len;
            i = text.indexOf(s, j);
            if (i < j) // arrive at the end of text
                i = text.length();
            value = text.substring(j, i);
            if (value != null && e != null && value.indexOf(e) >= 0)
                value = doSearchReplace(e, s, value);
            attr.put(key, value);
            n++;
            j = i + len;
            i = text.indexOf(s, j);
        }

        return n;
    }

    /**
     * splits the text with delimiter of s and returns a string array with
     * each key separated by the delimiters.
     */
    public static String[] split(String s, String text) {
        String key;
        List<String> list;
        int len, i, j = 0;

        if (text == null || s == null)
            return null;
        else if ((i = text.indexOf(s)) < 0)
            return new String[] {text};

        list = new ArrayList<String>();
        len = s.length();
        while (i >= j) {
            key = text.substring(j, i);
            j = i + len;
            list.add(key);
            i = text.indexOf(s, j);
        }
        key = text.substring(j);
        list.add(key);
        return list.toArray(new String[list.size()]);
    }

    /**
     * It assumes the data points linearly distributed between (x-span, 0) and
     * (x, y).  With a given resolution, it interpolates data points within
     * the interval and puts all of them into a pre-allocated 2d-array for
     * return.  The data pionts in the array are aranged in the ascending order
     * of x.  Upon success, it returns total number of data points interpolated.
     * Otherwise, it returns -1.
     */
    public static int linearInterpolate(double x, double y, double span,
        double delta, double[][] result) {
        int i, k, n;
        double slope, xx, yy;
        if (delta <= 0.0 || result == null)
            return -1;
        if (span == 0.0 || (n = result.length) == 0)
            return 0;

        if (delta >= Math.abs(span)) {
            if (n == 1) {
                result[0][0] = x;
                result[0][1] = y;
            }
            else if (span > 0.0) {
                n = 2;
                result[0][0] = x - span;
                result[0][1] = 0.0;
                result[1][0] = x;
                result[1][1] = y;
            }
            else {
                n = 2;
                result[0][0] = x;
                result[0][1] = y;
                result[1][0] = x - span;
                result[1][1] = 0.0;
            }
        }
        else if (span > 0.0) {
            k = (int) (span / delta) + 1;
            if (span > k * delta)
                k ++;
            slope = y / span;
            xx = x;
            yy = y;
            if (n >= k) {
                for (i=k-1; i>0; i--) {
                    result[i][0] = xx;
                    result[i][1] = yy;
                    yy -= delta * slope;
                    xx -= delta;
                }
                result[0][0] = x - span;
                result[0][1] = 0.0;
                n = k;
            }
            else { // try to make n data points
                for (i=n-1; i>0; i--) {
                    result[i][0] = xx;
                    result[i][1] = yy;
                    yy -= delta * slope;
                    xx -= delta;
                }
                result[0][0] = x - span;
                result[0][1] = 0.0;
            }
        }
        else {
            k = (int) (Math.abs(span) / delta) + 1;
            if (span > k * delta)
                k ++;
            slope = y / span;
            xx = x;
            yy = y;
            if (n >= k) {
                k --;
                for (i=0; i<k; i++) {
                    result[i][0] = xx;
                    result[i][1] = yy;
                    yy += delta * slope;
                    xx += delta;
                }
                result[k][0] = x - span;
                result[k][1] = 0.0;
                k ++;
                n = k;
            }
            else { // try to make n data points
                k = n - 1;
                for (i=0; i<k; i++) {
                    result[i][0] = xx;
                    result[i][1] = yy;
                    yy += delta * slope;
                    xx += delta;
                }
                result[k][0] = x - span;
                result[k][1] = 0.0;
            }
        }
        return n;
    }

    /**
     * It assumes the data points of x-axis uniformly distributed in the
     * interval of [x - span, x].  With a given value of delta on x-axis,
     * it spreads uniformly the value x in the interval and returns the
     * array of all data points of x in the ascending order.  In case of
     * failure, it returns null.
     */
    public static double[] uniformSpread(double x, double span, double delta) {
        int i, k, n;
        double xx;
        double[] data = null;
        if (delta <= 0.0 || span == 0.0)
            return null;

        if (delta >= Math.abs(span)) {
            data = new double[]{x};
        }
        else {
            n = (int) (Math.abs(span) / delta);
            if (Math.abs(span) > n * delta)
                n ++;
            k = (span > 0.0) ? 1 : -1;
            xx = x;
            data = new double[n];
            for (i=n-1; i>=0; i--) {
                data[i] = xx;
                xx -= k * delta;
            }
        }
        return data; 
    }

    public static double evaluate(double a, double b, int option) {
        double r = 0;
        switch (option) {
          case EVAL_ADD:
            r = a + b;
            break;
          case EVAL_SUB:
            r = a - b;
            break;
          case EVAL_MUL:
            r = a * b;
            break;
          case EVAL_DIV:
            if (b != 0.0)
                r = a / b;
            else
                throw(new IllegalArgumentException("denominator is zero"));
            break;
          default:
            throw(new IllegalArgumentException("option is not suppoprted: "+
                option));
        }
        return r;
    }

    public static long evaluate(long a, long b, int option) {
        long r = 0;
        switch (option) {
          case EVAL_ADD:
            r = a + b;
            break;
          case EVAL_SUB:
            r = a - b;
            break;
          case EVAL_MUL:
            r = a * b;
            break;
          case EVAL_DIV:
            if (b != 0)
                r = (long) ((double) a / b);
            else
                throw(new IllegalArgumentException("denominator is zero"));
            break;
          default:
            throw(new IllegalArgumentException("option is not suppoprted: "+
                option));
        }
        return r;
    }

    /**
     * It assumes a property map contains a list of names as the value for
     * the key, their property maps and others. It projects out all other
     * irrelavent objects and only keeps the objects in the list. Upon
     * success, it returns the number of keys remvoed.
     */
    public static int projectProps(String key, Map ph) {
        Object o;
        Object[] keys;
        Map<String, String> map;
        List list;
        String str;
        int i, k, n;

        if (ph == null || ph.size() <= 0)
            return -1;
        if (key != null && key.length() > 0 && (o = ph.get(key)) != null &&
            o instanceof List) {
            list = (List) o;
            map = new HashMap<String, String>();
            map.put("Name", null);
            map.put(key, null);
            n = list.size();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                str = (String) o;
                map.put(str, null);
            }

            keys = ph.keySet().toArray();
            n = keys.length;
            k = 0;
            for (i=0; i<n; i++) { // remove keys not in projection
                o = keys[i];
                if (o == null || !(o instanceof String))
                    ph.remove(o);
                else if (!map.containsKey((String) o))
                    ph.remove(o);
                else
                    k++;
            }
            map.clear();

            return n - k;
        }
        else
            return 0;
    }

    /**
     * It copies the properties from the source to the target according to
     * a given list of the name defined in the target.  It returns number
     * of properties copied over.
     */
    public static int copyListProps(String name, Map<String, Object> target,
        Map source) {
        Object o;
        List list;
        String key;
        int i, k, n;

        if (name == null || name.length() <= 0 || target == null ||
            target.size() <= 0 || source == null || source.size() <= 0)
            return -1;

        o = target.get(name);
        if (o == null || !(o instanceof List))
            return 0;

        list = (List) o;
        n = list.size();
        k = 0;
        for (i=0; i<n; i++) {
            if ((o = list.get(i)) == null)
                continue;
            if (o instanceof String)
                key = (String) o;
            else
                key = (String) ((Map) o).get("Name");
            o = source.get(key);
            if (o != null && o instanceof Map) {
                target.put(key, cloneProperties((Map) o));
                k ++;
            }
        }

        return k;
    }

    /**
     * It returns an array of names for the primary includes based on the given
     * IncludeGroup, the basename and the master propery map. IncludeGroup is a
     * Map that specifies the components to be primarily included in a given
     * property Map with the name of basename. Its keys are either the basename
     * or a property name for either a map or a list. The corresponding values
     * are either a empty string or a list of names. It supports the following
     * three scenarios:
     *<br/>
     * LIST/LIST: {"MonitorGroup": ["Monitor"]}<br/>
     * LIST: {"Flow": ["Receiver", "Node", "Persister"]}<br/>
     * MAP: {"ConfigRepository": ""}<br/>
     *<br/><br/>
     * where Flow is the basename in this context.
     */
    public static String[] getIncludes(String basename,
        Map<String, Object> includeGroup, Map props) {
        List<Object> group, list;
        List elements;
        Set<String> items;
        Object o;
        String item;
        int i, j, m, n;

        if (basename == null || includeGroup == null || props == null)
            return null;

        items = new HashSet<String>();
        for (String key : includeGroup.keySet()) {
            if (key.equals(basename)) { // for a list in base
                group = new ArrayList<Object>();
                group.add(props);
            }
            else if ((o = props.get(key)) != null && o instanceof List)
                group = cloneProperties((List) o);
            else if (o != null && o instanceof String) { // for a map in base
                items.add((String) o);
                continue;
            }
            else
                continue;

            if ((o = includeGroup.get(key)) == null || !(o instanceof List))
                continue;

            elements = (List) o;
            m = elements.size();
            n = group.size();
            for (i=0; i<n; i++) { // loop thru group instances
                if ((o = group.get(i)) == null || !(o instanceof Map))
                    continue;
                Map g = (Map) o;
                for (j=0; j<m; j++) { // loop thru elements
                    o = elements.get(j);
                    if (o == null || !(o instanceof String))
                        continue;

                    item = (String) o;
                    if ((o = g.get(item)) == null || o instanceof Map)
                        continue;

                    if (o instanceof List)
                        list = cloneProperties((List) o);
                    else if (o instanceof String && key.equals(basename)) {
                        list = new ArrayList<Object>();
                        list.add(item);
                    }
                    else
                        continue;

                    for (Object obj : list) { // loop thru components
                        if (!(obj instanceof String || obj instanceof Map))
                            continue;
                        if (obj instanceof Map) // for generated items
                            item = (String) ((Map) obj).get("Name");
                        else
                            item = (String) obj;
                        items.add(item);
                    }
                }
            }
        }

        n = items.size();
        return items.toArray(new String[n]);
    }

    /**
     * It returns a Map with only modified or new props.  The deleted
     * items will be set to null in the returned Map.  If the container has
     * also been changed, it will be reflected by the object keyed by base.
     * IncludeGroup defines all included components.
     */
    public static Map<String, Object> diff(String base,
        Map<String, Object> includeGroup, Map ps, Map pt) {
        Object o;
        Map<String, Object> ph, ch = new HashMap<String, Object>();
        String[] items;
        int i, n;
        if (ps == null || pt == null || base == null)
            return null;
        if (includeGroup == null || includeGroup.size() <= 0) {
            if (!ps.equals(pt))
                ch.put(base, cloneProperties(pt));
            return ch;
        }

        // decompose source props
        ph = cloneProperties(ps);
        items = getIncludes(base, includeGroup, ph);
        if (items != null)
            n = items.length; 
        else
            n = 0;

        for (i=0; i<n; i++) { // check included components for source
            if (ch.containsKey(items[i]))
                continue;
            if ((o = ph.get(items[i])) == null || !(o instanceof Map))
                continue;
            ph.remove(items[i]);
            if (pt.containsKey(items[i]))
                ch.put(items[i], (Map) o);
            else // set null for delete
                ch.put(items[i], null);
        }
        // ph is only the master property of source, ch has all components
        ch.put(base, ph);

        // decompose target props and diff it with source
        ph = cloneProperties(pt);
        items = getIncludes(base, includeGroup, ph);
        if (items != null)
            n = items.length; 
        else
            n = 0;

        for (i=0; i<n; i++) { // check included components for target
            if((o = ph.get(items[i])) == null || !(o instanceof Map))
                continue;
            ph.remove(items[i]);
            if (!ch.containsKey(items[i])) // add new props
                ch.put(items[i], (Map) o);
            else if (((Map) o).equals((Map) ch.get(items[i])))
                ch.remove(items[i]);
            else // replace the props
                ch.put(items[i], (Map) o);
        }

        // up to here, ph is only the master property of target
        if (ph.equals((Map) ch.get(base))) // no changes on base
            ch.remove(base);
        else // replace the master with the new base
            ch.put(base, ph);

        return ch;
    }

    /** It returns a unique Map with dup count from the given list */
    public static Map<String, String> getUniqueMap(Collection list) {
        Object[] keys;
        Object o;
        Map<String, String> ph;
        String key;
        int i, k, n;
        if (list == null)
            return null;
        ph = new HashMap<String, String>();
        keys = list.toArray();
        n = keys.length;
        for (i=0; i<n; i++) { // init the unique map with dup count
            o = keys[i];
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            if (key.length() <= 0)
                continue;
            if (!ph.containsKey(key))
                ph.put(key, "1");
            else {
                o = ph.get(key);
                k = Integer.parseInt((String) o);
                ph.put(key, String.valueOf(++k));
            }
        }
        return ph;
    }

    /** It assumes the Map contains a list of the given key.  It looks for
     * any Map member in the list and moves it to the container under its
     * name.  It returns the clone of the container with clone of the list that
     * all Map member will be replaced by their names.
     */
    public static Map<String, Object> normalize(Map props, String key) {
        Object o;
        Map<String, Object> ph;
        List list;
        List<String> pl;
        int i, n;
        if (key == null || props == null || props.get(key) == null)
            return null;
        ph = cloneProperties(props);
        o = ph.remove(key);
        if (!(o instanceof List))
            return null;
        list = (List) o;
        n = list.size();
        pl = new ArrayList<String>();
        ph.put(key, pl);
        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null)
                continue;
            else if (o instanceof Map) { // real object
                key = (String) ((Map) o).get("Name");
                ph.put(key, o);
                pl.add(key);
            }
            else if (o instanceof String) { // name only
                pl.add((String) o);
            }
        }
        return ph;
    }

    /* It returns a Map with master properties only */
    public static Map<String, Object> getMasterProperties(String base,
        Map<String, Object> includeGroup, Map props) {
        int i, n;
        String[] items;
        Map<String, Object> ph;
        Object o;

        if (props == null || includeGroup == null || base == null)
            return null;
        if (base.length() <= 0 || includeGroup.size() <= 0 || props.size() <= 0)
            return null;

        // decompose props
        ph = cloneProperties(props);
        items = getIncludes(base, includeGroup, ph);
        if (items != null)
            n = items.length; 
        else
            n = 0;

        for (i=0; i<n; i++) { // remove included components
            if ((o = ph.get(items[i])) == null || !(o instanceof Map))
                continue;
            ph.remove(items[i]);
        }

        return ph;
    }

    /** It returns a Map with migrated properties */
    public static Map<String, Object> migrateProperties(Map props) {
        String key;
        Map<String, Object> ph;

        if (props == null)
            return null;

        ph = new HashMap<String, Object>();
        for (Object o : props.keySet().toArray()) {
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            ph.put(key, props.remove(o));
        }
        return ph;
    }

    /** It returns a Map with fully cloned properties */
    public static Map<String, Object> cloneProperties(Map props) {
        Object o;
        Map<String, Object> ph;
        String key;

        if (props == null)
            return null;

        ph = new HashMap<String, Object>();
        for (Iterator iter=props.keySet().iterator(); iter.hasNext();) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = props.get(key);
            if (o == null)
                ph.put(key, o);
            else if (o instanceof Map)
                ph.put(key, cloneProperties((Map) o));
            else if (o instanceof List)
                ph.put(key, cloneProperties((List) o));
            else if (o instanceof String)
                ph.put(key, (String) o);
            else
                ph.put(key, o.toString());
        }

        return ph;
    }

    /** It returns a List with fully cloned properties */
    public static List<Object> cloneProperties(List props) {
        Object o;
        List<Object> list;
        int i, n;

        if (props == null)
            return null;

        list = new ArrayList<Object>();
        n = props.size();
        for (i=0; i<n; i++) {
            o = props.get(i);
            if (o == null)
                list.add(o);
            else if (o instanceof Map)
                list.add(cloneProperties((Map) o));
            else if (o instanceof List)
                list.add(cloneProperties((List) o));
            else if (o instanceof String)
                list.add((String) o);
            else
                list.add(o.toString());
        }

        return list;
    }

    /** compares two property maps and returns true only if they are same */
    public static boolean compareProperties(Map pa, Map pb) {
        Object o, c;
        String key;

        if (pa == null && pb == null)
            return true;
        else if (pa != null && pb == null)
            return false;
        else if (pa == null && pb != null)
            return false;
        else if (pa.size() != pb.size())
            return false;

        for (Iterator iter=pa.keySet().iterator(); iter.hasNext();) {
            o = iter.next();
            if (!pb.containsKey(o))
                return false;
            if (o == null || !(o instanceof String)) // ignore null or non-s key
                continue;
            key = (String) o;
            o = pa.get(key);
            c = pb.get(key);
            if (o == null && c == null)
                continue;
            else if (o == null && c != null)
                return false;
            else if (o != null && c == null)
                return false;
            else if (o instanceof Map && c instanceof Map) {
                if (!compareProperties((Map) o, (Map) c))
                    return false;
                else
                    continue;
            }
            else if (o instanceof List && c instanceof List) {
                if (!compareProperties((List) o, (List) c))
                    return false;
                else
                    continue;
            }
            else if (o instanceof String && c instanceof String) {
                if (!((String) o).equals((String) c))
                    return false;
                else
                    continue;
            }
            else
                return false;
        }

        return true;
    }

    /** compares two property lists and returns true only if they are same */
    public static boolean compareProperties(List la, List lb) {
        Object o, c;
        String key;
        int i, n;

        if (la == null && lb == null)
            return true;
        else if (la != null && lb == null)
            return false;
        else if (la == null && lb != null)
            return false;
        else if (la.size() != lb.size())
            return false;

        n = la.size();
        for (i=0; i<n; i++) {
            o = la.get(i);
            c = lb.get(i);
            if (o == null && c == null)
                continue;
            else if (o == null && c != null)
                return false;
            else if (o != null && c == null)
                return false;
            else if (o instanceof Map && c instanceof Map) {
                if (!compareProperties((Map) o, (Map) c))
                    return false;
                else
                    continue;
            }
            else if (o instanceof List && c instanceof List) {
                if (!compareProperties((List) o, (List) c))
                    return false;
                else
                    continue;
            }
            else if (o instanceof String && c instanceof String) {
                if (!((String) o).equals((String) c))
                    return false;
                else
                    continue;
            }
            else
                return false;
        }

        return true;
    }

    /** returns the object for the contant value of the field in the class */
    public static Object getDeclaredConstant(String classname, String name,
        String prefix) {
        Class cls;
        java.lang.reflect.Field field = null;

        if (classname == null || classname.length() <= 0)
            throw(new IllegalArgumentException(prefix +
                ": ClassName is not defined for " + name));
        else if (name == null || name.length() <= 0)
            throw(new IllegalArgumentException(prefix +
                ": FieldName is not defined for " + classname));

        try {
            cls = Class.forName(classname);
            field = cls.getDeclaredField(name);
        }
        catch (Exception e) {
            field = null;
            throw(new IllegalArgumentException(prefix + ": failed to get "+
                "field of " + name + " in " + classname));
        }
        if (field == null)
            throw(new IllegalArgumentException(prefix+": no such field in "+
                classname + " for " + name));

        int k = field.getModifiers();
        if ((k & java.lang.reflect.Modifier.FINAL) == 0 ||
            (k & java.lang.reflect.Modifier.PUBLIC) == 0 ||
            (k & java.lang.reflect.Modifier.STATIC) == 0)
            return null;

        Object o = null;
        try {
            o = field.get(null);
        }
        catch (Exception e) {
            o = null;
        }

        return o;
    }

    /**
     * returns a byte array for the number of the hex string
     */
    public static byte[] hexString2Bytes(String hexStr) {
        int n;
        byte[] b;
        if (hexStr == null || hexStr.length() < 3)
            return null;
        n = hexStr.length() - 2;
        n /= 2;
        b = new byte[n];
        for (int i=0; i<n; i++)
            b[i] = (byte) Integer.parseInt(hexStr.substring(i+i+2, i+i+4), 16);
        return b;
    }

    public static DocumentBuilder getDocBuilder() throws ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(true);
        factory.setFeature("http://xml.org/sax/features/namespaces", false);
        factory.setFeature("http://xml.org/sax/features/validation", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        return factory.newDocumentBuilder();
    }

    /**
     * converts a DOM node into the text representation of the hierachy.
     * In case of ENTITY, NOTATION, or DOCUMENT, it return an empty string.
     */
    public static String nodeToText(Node nd) {
        String name = nd.getNodeName();
        short type = nd.getNodeType();

        switch (type) {
          case Node.CDATA_SECTION_NODE:
            return "<![CDATA[" + nd.getNodeValue() + "]]>";
          case Node.COMMENT_NODE:
            return "<!--" + nd.getNodeValue() + "-->";
          case Node.TEXT_NODE:
          case Node.ATTRIBUTE_NODE:
            return nd.getNodeValue();
          case Node.ELEMENT_NODE:
            StringBuffer sb = new StringBuffer();
            sb.append("<").append(name);

            NamedNodeMap attrs = nd.getAttributes();
            if (attrs != null) { // has attributes
                for (int i = 0; i < attrs.getLength(); i++) {
                    Node attr = attrs.item(i);
                    sb.append(" ").append(attr.getNodeName()).append("=\"");
                    sb.append(attr.getNodeValue()).append("\"");
                }
            }

            NodeList children = nd.getChildNodes();

            if (children.getLength() == 0) { // empty
                sb.append("/>");
            }
            else { // not empty
                sb.append(">");
                for (int i=0; i<children.getLength(); i++) {
                    String childText = nodeToText(children.item(i));
                    if (childText != null && childText.length() > 0) {
                        sb.append(childText);
                    }
                }

                sb.append("</").append(name).append(">");
            }
            return sb.toString();
          default:
            return "";
        }
    }

    /**
     * converts a DOM NodeList into the text representation of the hierachy.
     */
    public static String nodeListToText(NodeList list) {
        int i, n;
        if (list == null)
            return null;
        else if ((n = list.getLength()) <= 0)
            return "";
        StringBuffer strBuf = new StringBuffer();
        for (i=0; i<n; i++)
            strBuf.append(nodeToText(list.item(i)));
        return strBuf.toString();
    }

    /**
     * aggregates all attributes of name spaces from the parent nodes on
     * a given node and returns the string of attributes or null if no NS
     */
    public static String getParentNameSpaces(Node nd) {
        Node na;
        NamedNodeMap map = null;
        Map<String, String> ph = new HashMap<String, String>();
        int i, k;

        if (nd == null)
            return null;

        nd = nd.getParentNode();
        while (nd != null) { // walk thru all parents
            k = 0;
            if (nd.hasAttributes()) {
                map = nd.getAttributes();
                if (map != null)
                    k = map.getLength();
            }
            for (i=0; i<k; i++) { // scan all attributes
                na = map.item(i);
                String key = na.getPrefix();
                if (key == null || !"xmlns".equals(key)) // not for name space
                    continue;
                key = na.getNodeName();
                if (ph.containsKey(key))
                    continue;
                ph.put(key, na.getNodeValue());
            }
            nd = nd.getParentNode();
        }

        if (ph.size() > 0) {
            StringBuffer sb = new StringBuffer();
            for (String key : ph.keySet()) {
                if (key == null || key.length() <= 0)
                    continue;
                if (sb.length() > 0)
                    sb.append(" ");
                sb.append(key).append("=\"");
                sb.append((String) ph.get(key)).append("\"");
            }
            if (sb.length() > 0)
                return sb.toString();
        }

        return null;
    }

    /**
     * evaluates the node with xpath expressions stored in the expression map
     * and returns the key-value map for the node
     */
    public static Map<String, String> evaluate(Node nd, Map expression) {
        if (expression != null && expression.size() > 0) try {
            Object o;
            XPathExpression xpe;
            Iterator iter;
            String key;
            Map<String, String> map = new HashMap<String, String>();
            for (iter=expression.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = expression.get(key);
                if (o == null)
                    continue;
                else if (o instanceof XPathExpression)
                    xpe = (XPathExpression) o;
                else
                    continue;
                o = xpe.evaluate(nd, XPathConstants.NODESET);
                if (o == null)
                    continue;
                map.put(key, nodeListToText((NodeList) o));
            }
            return map;
        }
        catch (Exception e) {
        }

        return null;
    }

    /**
     * It copies bytes from the InputStream and writes them into the
     * OutputStream.  It returns the number of bytes copied or -1 otherwise.
     */
    public static long copyStream(InputStream in, OutputStream out,
        byte[] buffer, int count) throws IOException {
        int bytesRead = 0;
        long totalBytes = 0;
        if (in == null || out == null || buffer == null || buffer.length <= 0)
            return -1;
        if (count <= 0) {
            while ((bytesRead = in.read(buffer)) >= 0) {
                if (bytesRead > 0)
                    out.write(buffer, 0, bytesRead);
                else if (bytesRead < 0) { // end of stream
                    if (totalBytes > 0)
                        break;
                    else // end of stream
                        return -1;
                }
                totalBytes += bytesRead;
            }
        }
        else {
            for (int i=0; i<count; i++) {
                bytesRead = in.read(buffer);
                if (bytesRead > 0)
                    out.write(buffer, 0, bytesRead);
                else if (bytesRead < 0) { // end of stream
                    if (totalBytes > 0)
                        break;
                    else // end of stream
                        return -1;
                }
                totalBytes += bytesRead;
            }
        }
        return totalBytes;
    }

    /**
     * It copies bytes from the InputStream and writes them into two
     * OutputStreams. It returns a string array with error messages for
     * each output or an empty string array for all success. The total number
     * of bytes copied are stored in the given long array of rc.
     */
    public static String[] copyStream(InputStream in, OutputStream out1,
        OutputStream out2, byte[] buffer, int count, long[] rc)
        throws IOException {
        String err1 = null, err2 = null;
        int bytesRead = 0;
        if (in == null || out1 == null || out2 == null || buffer == null ||
            buffer.length <= 0 || rc == null || rc.length < 2)
            return null;
        rc[0] = 0;
        rc[1] = 0;
        if (count <= 0) {
            while ((bytesRead = in.read(buffer)) >= 0) {
                if (bytesRead > 0) {
                    if (err1 == null) try {
                        out1.write(buffer, 0, bytesRead);
                        rc[0] += bytesRead;
                    }
                    catch(Exception e) {
                        err1 = e.toString();
                    }
                    if (err2 == null) try {
                        out2.write(buffer, 0, bytesRead);
                        rc[1] += bytesRead;
                    }
                    catch(Exception e) {
                        err2 = e.toString();
                    }
                    if (err1 != null && err2 != null)
                        break;
                }
                else if (bytesRead < 0) { // end of stream
                    break;
                }
            }
        }
        else {
            for (int i=0; i<count; i++) {
                bytesRead = in.read(buffer);
                if (bytesRead > 0) {
                    if (err1 == null) try {
                        out1.write(buffer, 0, bytesRead);
                        rc[0] += bytesRead;
                    }
                    catch(Exception e) {
                        err1 = e.toString();
                    }
                    if (err2 == null) try {
                        out2.write(buffer, 0, bytesRead);
                        rc[1] += bytesRead;
                    }
                    catch(Exception e) {
                        err2 = e.toString();
                    }
                    if (err1 != null && err2 != null)
                        break;
                }
                else if (bytesRead < 0) { // end of stream
                    break;
                }
            }
        }
        if (err1 == null && err2 == null)
            return new String[0];
        else
            return new String[]{err1, err2};
    }

    public static String read(InputStream in, byte[] buffer) throws IOException{
        int bytesRead = 0;
        StringBuffer strBuf;
        if (in == null || buffer == null || buffer.length <= 0)
            return null;
        strBuf = new StringBuffer();
        while ((bytesRead = in.read(buffer)) >= 0) {
            if (bytesRead > 0)
                strBuf.append(new String(buffer, 0, bytesRead, "UTF-8"));
        }

        return strBuf.toString();
    }

    public static byte[] sign(byte[] msg, String key, String algorithm)
        throws InvalidKeyException, NoSuchAlgorithmException {
        if (msg == null || key == null || algorithm == null)
            return null;
        SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(), algorithm);
        Mac mac = Mac.getInstance(algorithm);
        mac.init(keySpec);
        return mac.doFinal(msg);
    }

    public static String encode(String str) throws IOException {
        return java.net.URLEncoder.encode(str, "UTF-8");
    }

    public static String decode(String str) throws IOException {
        return java.net.URLDecoder.decode(str, "UTF-8");
    }

    /** returns number of outstanding messages for the given xqueue */
    public static int getOutstandingSize(XQueue xq, int begin, int len) {
        int i, k, n = 0;
        if (xq == null || begin < 0 || len < 0 || begin+len > xq.getCapacity())
            return -1;

        if (len == 0)
            n = xq.size();
        else if (len == 1) {
            k = xq.getCellStatus(begin);
            n = (k == XQueue.CELL_OCCUPIED || k == XQueue.CELL_TAKEN) ? 1 : 0;
        }
        else {
            n = 0;
            for (i=begin+len-1; i>=0; i--) { // scan partitioned cells
                k = xq.getCellStatus(i);
                if (k == XQueue.CELL_OCCUPIED || k == XQueue.CELL_TAKEN)
                    n ++;
            }
        }
        return n;
    }

    /** decrypts the encrypted text with OpenSSL plugin */
    public static String decrypt(String text) throws IOException,
        GeneralSecurityException {
        if (decrypt == null) try {
            initStaticMethod("decrypt");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "decrypt from plugin: " + e.toString()));
        }
        try {
            return (String) decrypt.invoke(null, new Object[]{text});
        }
        catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t == null)
                throw(new IllegalArgumentException("failed to invoke: "+
                    traceStack(e)));
            else if (t instanceof IOException)
                throw((IOException) t);
            else if (t instanceof GeneralSecurityException)
                throw((GeneralSecurityException) t);
            else if (t instanceof RuntimeException)
                throw((RuntimeException) t);
            else
                throw(new IOException("failed to invoke: "+ traceStack(t)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to decrypt: "+
                traceStack(e)));
        }
    }

    /**
     * It returns dirname of the filename or null if it is a relative path.
     */
    public static String getParent(String filename) {
        char fs;
        int i, k;
        if (filename == null || (k = filename.length()) <= 0) // bad argument
            return null;
        fs = filename.charAt(0);
        if (fs != '/' && fs != '\\') // no file separator found
            return null;
        if ((i = filename.lastIndexOf(fs, k-1)) >= 0)
            return filename.substring(0, i);
        else
            return null;
    }

    /**
     * It returns the value of the http header for the key or null if there is
     * no such key in the headers.
     */
    public static String getHttpHeader(String key, String headers) {
        int i, j, k;
        if (headers == null || headers.length() <= 0 || key == null ||
            key.length() <= 0)
            return null;
        if ((i = headers.indexOf(key + ": ")) < 0)
            return null;
        else if (i == 0) {
           k = key.length() + 2;
           if ((j = headers.indexOf("\r\n", i + k)) >= i + k)
               return headers.substring(i+k, j);
           else
               return null;
        }
        else if ((i = headers.indexOf("\n" + key + ": ")) > 0) {
           i ++;
           k = key.length() + 2;
           if ((j = headers.indexOf("\r\n", i + k)) >= i + k)
               return headers.substring(i+k, j);
           else
               return null;
        }
        else
            return null;
    }

    /** initializes static methods from OpenSSL plugin */
    private synchronized static void initStaticMethod(String name)
        throws ClassNotFoundException, NoSuchMethodException {
        String className = System.getProperty("OpenSSLPlugin",
            "org.qbroker.common.AES");
        if ("decrypt".equals(name) && decrypt == null) {
            Class<?> cls = Class.forName(className);
            decrypt = cls.getMethod(name, new Class[]{String.class});
        }
        else
            throw(new IllegalArgumentException("method of " + name +
                  " is not supported for OpenSSL plugin"));
    }
}
