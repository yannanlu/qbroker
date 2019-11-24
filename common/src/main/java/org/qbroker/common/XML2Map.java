package org.qbroker.common;

/* XML2Map.java - to load a property Map from an XML file */

import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.xml.sax.XMLReader;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXException;
import org.qbroker.common.Utils;
import org.qbroker.common.PHP2Map;

/**
 * XML2Map parses an XML file and maps the structural content of the
 * XML into Java objects stored in a Map.  It only recognizes three basic
 * Java objects, String, List and Map.  A Map or a List
 * can contain any objects of the three types.  They are the nodes of the
 * data tree.  A String is only a leaf in the data tree.  With these three
 * basic types of objects, any data structures will be representable.
 * The mapping is one-to-one.  Therefore, the mapping is bidirectional.
 *<br><br>
 * It recognizes the only attribute: type.  The type defines the storage type
 * of the data object, either scalar in default or vector.  Its value of
 * "ARRAY" for an element of an List, "NULL" for a null scalar and
 * "NULL_ARRAY" for a null element of an List.
 *<br>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class XML2Map extends DefaultHandler {
    private Map root;
    private Object current, parent;
    private Object[] tree = new Object[1024];
    private String[] nodeName = new String[1024];
    private StringBuffer strBuf = null;
    private int level = 0;
    private int[] dataType = new int[1024];
    private XMLReader xr = null;
    public final static int XML_DEFAULT = 0;
    public final static int XML_ARRAY = 1;
    public final static int XML_NULL = 2;

    public XML2Map (String parser) throws SAXException {
        super();
        xr = XMLReaderFactory.createXMLReader(parser);
	xr.setContentHandler(this);
	xr.setErrorHandler(this);
    }

    public Map getMap(Reader in) throws IOException, SAXException {
        xr.parse(new InputSource(in));
        return root;
    }

    public Map getMap(InputStream in) throws IOException, SAXException {
        xr.parse(new InputSource(in));
        return root;
    }

    public Map getMap(String url) throws IOException, SAXException {
        xr.parse(new InputSource(url));
        return root;
    }

    /** returns true if the keyPath exists or false otherwise */
    public static boolean containsKey(Map ph, String keyPath) {
        int i, k, n;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (ph == null || keyPath == null || keyPath.length() <= 0)
            throw(new IllegalArgumentException("bad data"));
        if (ph.size() <= 0)
            return false;
        if (keyPath.charAt(0) != '/')
            return ph.containsKey(keyPath);
        keys = Utils.split("/", keyPath);
        n = keys.length;
        o = ph;
        for (i=1; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return true;
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (o instanceof String)
                    break;
            }
            else if (o instanceof List) {
                a = (List) o;
                k = Integer.parseInt(keys[i]);
                if (k < 0 || k >= a.size())
                    break;
                else if (i+1 == n)
                    return true;
                o = a.get(k);
                if (o == null)
                    break;
                else if (o instanceof String)
                    break;
            }
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
        else if (o instanceof List)
            return ((List) o).size();
        else
            return 1;
    }

    /** returns the object referenced by the keyPath or null */
    public static Object get(Map ph, String keyPath) {
        int i, k, n;
        String[] keys;
        Object o;
        Map h;
        List a;
        if (!containsKey(ph, keyPath))
            return null;
        if (keyPath.charAt(0) != '/')
            return ph.get(keyPath);
        keys = Utils.split("/", keyPath);
        n = keys.length;
        o = ph;
        for (i=1; i<n; i++) {
            if (o instanceof Map) {
                h = (Map) o;
                if (!h.containsKey(keys[i]))
                    break;
                else if (i+1 == n)
                    return h.get(keys[i]);
                o = h.get(keys[i]);
                if (o == null)
                    break;
                else if (o instanceof String)
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
                else if (o instanceof String)
                    break;
            }
        }
        return null;
    }

    private static boolean isInteger(String i) {
        if (i == null || i.length() <= 0)
            return false;
        if (!Character.isDigit(i.charAt(0)))
            return false;
        try {
            Integer.parseInt(i);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    public void startDocument () {
//        System.out.println("Start document");
        root = new HashMap();
        level = 0;
        tree[level] = root;
        dataType[level] = XML_DEFAULT;
        nodeName[level] = null;
    }

    public void endDocument () {
//        System.out.println("End document");
    }

    public void startElement (String uri, String name, String qName,
        Attributes atts) {
        String type = null;
        int len = atts.getLength();
        parent = tree[level];
        level ++;
        current = tree[level];
        if (len > 0)
            type = atts.getValue("type");
        if (type != null) {
            Map ph;
            List pa;
            String pName;
            if ("ARRAY".equals(type)) {
                dataType[level] = XML_ARRAY;
                if ((dataType[level-1] & XML_ARRAY) > 0) { //parent is an array
                    pa = (List) parent;
                    pName = nodeName[level-1];
                    if (qName.equals(pName)) { // multi-dimensional
                        if (current != null && current instanceof List) {
                            nodeName[level] = qName;
                            current = tree[level];
                        }
                        else {
                            nodeName[level] = qName;
                            current = new ArrayList();
                            pa.add(current);
                        }
                    }
                    else { // insert the extra layer of hashmap
                        dataType[level] = XML_DEFAULT;
                        ph = new HashMap();
                        current = new ArrayList();
                        ph.put(qName, current);
                        tree[level] = ph;
                        nodeName[level] = null;
                        parent = ph;
                        level ++;
                        nodeName[level] = qName;
                        dataType[level] = XML_ARRAY;
                    }
                }
                else { // parent is a hash node
                    ph = (Map) parent;
                    if (!ph.containsKey(qName))
                        ph.put(qName, new ArrayList());
                    nodeName[level] = qName;
                    current = (List) ph.get(qName);
                }
            }
            else if ("NULL_ARRAY".equals(type)) {
                dataType[level] = XML_ARRAY | XML_NULL;
                if (dataType[level-1] == XML_ARRAY) { // parent is an array node
                    pa = (List) parent;
                    pName = nodeName[level-1];
                    if (qName.equals(pName)) { // multi-dimensional
                        if (current != null && current instanceof List) {
                            nodeName[level] = qName;
                            current = tree[level];
                        }
                        else {
                            nodeName[level] = qName;
                            current = new ArrayList();
                            pa.add(current);
                        }
                    }
                    else { // insert the extra layer of hashmap
                        dataType[level] = XML_DEFAULT;
                        ph = new HashMap();
                        current = new ArrayList();
                        ph.put(qName, current);
                        tree[level] = ph;
                        parent = ph;
                        level ++;
                        nodeName[level] = qName;
                        dataType[level] = XML_ARRAY;
                    }
                }
                else { // parent is a hash node
                    ph = (Map) parent;
                    if (!ph.containsKey(qName))
                        ph.put(qName, new ArrayList());
                    nodeName[level] = qName;
                    current = (List) ph.get(qName);
                }
            }
            else if ("NULL".equals(type)) {
                dataType[level] = XML_NULL;
                if (dataType[level-1] == XML_ARRAY) { // parent is an array node
                    parent = new HashMap();
                    nodeName[level] = null;
                    tree[level] = parent;
                    level ++;
                    dataType[level] = XML_NULL;
                }
                nodeName[level] = null;
                current = new HashMap();
            }
            else {
                dataType[level] = XML_DEFAULT;
                if (dataType[level-1] == XML_ARRAY) { // parent is an array node
                    parent = new HashMap();
                    tree[level] = parent;
                    nodeName[level] = null;
                    level ++;
                    dataType[level] = XML_DEFAULT;
                }
                nodeName[level] = null;
                current = new HashMap();
            }
        }
        else {
            dataType[level] = XML_DEFAULT;
            if (dataType[level-1] == XML_ARRAY) { // parent is an array node
                parent = new HashMap();
                tree[level] = parent;
                nodeName[level] = null;
                level ++;
                dataType[level] = XML_DEFAULT;
            }
            nodeName[level] = null;
            current = new HashMap();
        }
        tree[level] = current;
        strBuf = new StringBuffer();
/*
	if ("".equals (uri)) {
	    System.out.println("Start element: " + qName);
        }
	else
	    System.out.println("Start element: {" + uri + "}" + name);

        for (int i=0; i<len; i++) {
            current.put(atts.getQName(i), atts.getValue(i));
            System.out.println(atts.getQName(i) + ": " + atts.getValue(i));
        }
*/
    }

    public void endElement (String uri, String name, String qName) {
/*
	if ("".equals (uri))
	    System.out.println("End element: " + qName);
	else
	    System.out.println("End element:   {" + uri + "}" + name);
*/
        int pType = dataType[level-1];

        if (current == tree[level]) { // end of a leaf 
            String text = (strBuf.length() > 0) ? strBuf.toString() : "";

            switch (dataType[level]) {
              case XML_ARRAY:
                ((List) current).add(text);
                nodeName[level] = null;
                break;
              case XML_ARRAY | XML_NULL:
                if (strBuf.length() == 0)
                    ((List) current).add(null);
                else
                    ((List) current).add(text);
                nodeName[level] = null;
                break;
              case XML_NULL:
                if ((pType & XML_ARRAY) > 0)
                    ((List) parent).add((strBuf.length() == 0) ? null : text);
                else
                    ((Map) parent).put(qName, (strBuf.length()==0)?null:text);
                break;
              default:
                if ((pType & XML_ARRAY) > 0)
                    ((List) parent).add(text);
                else
                    ((Map) parent).put(qName, text);
                break;
            }
        }
        else { // container is not empty
            current = tree[level];
            switch (dataType[level]) {
              case XML_ARRAY | XML_NULL:
              case XML_ARRAY:
                tree[level+1] = null;
                nodeName[level] = null;
                break;
              default:
                if ((pType & XML_ARRAY) > 0) { // skip the extra level up
                    ((List) parent).add(current);
                    level --;
                }
                else
                    ((Map) parent).put(qName, current);
                break;
            }
        }
        level --;

        if (level > 0)
            parent = tree[level-1];
        strBuf = null;
    }

    public void characters (char ch[], int start, int length) {
/*
	System.out.print("Characters:    \"");
	for (int i = start; i < start + length; i++) {
	    switch (ch[i]) {
	    case '\\':
		System.out.print("\\\\");
		break;
	    case '"':
		System.out.print("\\\"");
		break;
	    case '\n':
		System.out.print("\\n");
		break;
	    case '\r':
		System.out.print("\\r");
		break;
	    case '\t':
		System.out.print("\\t");
		break;
	    default:
		System.out.print(ch[i]);
		break;
	    }
	}
	System.out.print("\"\n");
*/
        if (strBuf != null)
            strBuf.append(ch, start, length);
    }

    public static void main (String args[]) throws Exception {
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
            XML2Map xml;
            java.io.FileInputStream fs = new java.io.FileInputStream(filename);
            //for Crimson parser, use "org.apache.crimson.parsers.XMLReaderImpl"
            xml = new XML2Map("org.apache.xerces.parsers.SAXParser");
            Map ph = xml.getMap(fs);
            fs.close();
            if ("parse".equalsIgnoreCase(action)) {
                if ("json".equals(type))
                    System.out.println(PHP2Map.toJSON(ph, "", "\n"));
                else
                    System.out.println(PHP2Map.toXML(ph, "", "\n"));
            }
            else if ("flatten".equalsIgnoreCase(action)) {
                PHP2Map.flatten(ph);
                System.out.println(PHP2Map.toJSON(ph, "", "\n"));
            }
            else if (path != null && path.length() > 0) {
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
                        else if (o instanceof List)
                            System.out.println(path + ": " +
                                PHP2Map.toJSON((List) o, "", "\n"));
                        else
                            System.out.println(path + ": " +
                                PHP2Map.toJSON((Map) o, "", "\n"));
                    }
                    else
                        System.out.println(path + ": No");
                }
                else if ("count".equalsIgnoreCase(action)) {
                    System.out.println(path + ": " + count(ph, path));
                }
                else if ("diff".equalsIgnoreCase(action)) { // path is target
                    fs = new java.io.FileInputStream(path);
                    Map pt = xml.getMap(fs);
                    fs.close();
                    if (pt == null)
                        System.out.println(path + ": null");
                    else {
                        String key;
                        key = (String) ph.keySet().toArray()[0];
                        ph = (Map) ph.get(key);
                        key = (String) pt.keySet().toArray()[0];
                        pt = (Map) pt.get(key);
                        key = PHP2Map.diff(ph, pt, "");
                        if (key != null)
                            System.out.println(filename + ", " + path +
                                " diff:\n" + key);
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
        System.out.println("XML2Map Version 1.0 (written by Yannan Lu)");
        System.out.println("XML2Map: parse XML properties in a file");
        System.out.println("Usage: java org.qbroker.common.XML2Map -f filename -a action -t type -k path");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of parse, test, count, get, diff or flatten (default: parse)");
        System.out.println("  -k: key path (example: /views/list/0/name)");
        System.out.println("  -t: type of xml or json (default: json)");
        System.out.println("  -f: filename");
    }
}
