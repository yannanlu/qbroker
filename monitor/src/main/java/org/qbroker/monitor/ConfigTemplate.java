package org.qbroker.monitor;

/* ConfigTemplate - a Template for generated property maps of configurations */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Arrays;
import java.io.StringReader;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.event.Event;

/**
 * ConfigTemplate represents a list of similar configuration properties sharing
 * with the same property template. The property template has at least one
 * variable. The template is used to generate the property configurations for
 * all the instances with distinct names from a given list of value sets. Apart
 * from the property template, ConfigTemplate has also a name template to
 * generate a list of keys as the distinct names for each of the instances,
 * as well as the list of items as the value sets for all the variables.
 * Moreover, a name substitution is optional to further normalize the names.
 * Both the name template and the list of items are updatable and resetable
 * on demand. ConfigTemplate also supports updates on the property template.
 *<br><br>
 * ConfigTemplate also supports dynamic updates of the item list. In this case,
 * Item has to be a Map object specifying a MonitorReport to query or update the
 * list of items dynamically. The instance of the MonitorReport will be used
 * to generate the report frequently from which the list of items will be
 * updated automatically. The method of isDynamic() is used to check if the
 * instance of ConfigTemplate supporting dynamic item list or not. If the
 * managed object is the type of Monitor, it also supports the private report
 * for each dynamically generated monitor.
 *<br><br>
 * ConfigTemplate supports overriding on the configuration properties if a list
 * of property maps defined as Override. In each of the property map, it must
 * contains the property of KeyRegex that is the regex expression to match
 * against the name of the configuration property map. Once there is a match,
 * its property map will be merged into the configuration properties as the
 * override.
 *<br><br>
 * In case of dynamicly generated monitors, ConfigTemplate supports both private
 * report and/or shared report. A private report is the report dedicated to
 * each generated monitor. The reporter makes the private reports available
 * for each of the monitor. The container will pass the report to the job
 * thread so that the runtime of the monitor will be able to retrieve the data
 * in the report. A shared report is just a list of names for items. It is only
 * shared in creation scope by the future reporters within same MonitorGroup.
 * Its purpose is to save the resources for ConfigTemplate's reporters in the
 * back. The keyword for the private report is KeyTemplate. The keyword for
 * the shared report is the value of REPORT_LOCAL defined for ReportMode.
 *<br><br>
 * It is not MT-Safe, always use it within the same thread.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class ConfigTemplate {
    private String name, title;
    private String[] keys = null;
    private Template template;
    private Template idTemp = null;
    private TextSubstitution tsub = null;
    private Map<String, String> keyMap = null;
    private Map<String, Object> privateReport = null;
    private Map cachedProps = null;
    private MonitorReport reporter = null;
    private AssetList overrideList = null;
    private String dataField = null;
    private String jsonPath = null;
    private TextSubstitution postSub = null;
    private Perl5Matcher pm = null;
    private Pattern pattern = null;
    private DocumentBuilder builder = null;
    private XPathExpression xpe = null;
    private int count = 0, size = 0, resultType = Utils.RESULT_LIST;
    private boolean isDynamic = false;
    private boolean withPrivateReport = false;
    private boolean withSharedReport = false;

    public ConfigTemplate(Map props) {
        Object o;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("Template")) == null || !(o instanceof String))
            throw(new IllegalArgumentException(name +
                ": name template is not defined or not a string"));
        title = (String) o;
        if (title.length() <= 0 || title.indexOf("##") < 0)
            throw(new IllegalArgumentException(name +
                ": name template is empty or not well defined"));

        // substitution for Name only
        if ((o = props.get("Substitution")) != null && o instanceof String)
            tsub = new TextSubstitution((String) o);

        // for property template
        if ((o = props.get("Property")) != null && o instanceof Map) {
            Map<String, Object> ph = Utils.cloneProperties((Map) o);

            // reset the property template with title
            ph.put("Name", title);
            template = new Template(JSON2Map.toJSON(ph));
            count = template.size();
            ph.clear();
            if (count <= 0) // empty template
                throw(new IllegalArgumentException(name +
                    ": property template has no variables"));
        }
        else
            throw(new IllegalArgumentException(name +
                ": no property template defined"));

        if (count > 1) { // init idTemp
            StringBuffer strBuf = new StringBuffer();
            keys = template.keySet().toArray(new String[count]);
            Arrays.sort(keys);
            strBuf.append("{\"" + keys[0] + "\":\"##" + keys[0] + "##\"");
            for (int i=1; i<count; i++)
                strBuf.append(",\"" + keys[i] + "\":\"##" + keys[i] + "##\"");
            strBuf.append("}");
            idTemp = new Template(strBuf.toString());
            keys = null;
        }
        else
            idTemp = new Template("##" + template.getField(0) + "##");

        keyMap = new HashMap<String, String>();
        if ((o = props.get("Item")) == null)
            throw(new IllegalArgumentException(name + ": no item defined"));
        else if (o instanceof List) // for static item list
            updateItems((List) o);
        else if (o instanceof Map) { // with the reporter
            resetReporter((Map) o);
            isDynamic = true;
        }
        else
            throw(new IllegalArgumentException(name +": bad item"));

        overrideList = new AssetList(name, 32);
        if ((o = props.get("Override")) != null && o instanceof List) {
            List list = (List) o;
            for (Object obj : list) {
                if (!(obj instanceof Map))
                    continue;
                Map map = Utils.cloneProperties((Map) obj);
                String str = (String) map.remove("KeyRegex");
                map.remove("Name");
                if (str == null || str.length() <= 0)
                    continue;
                if (overrideList.containsKey(str))
                    continue;
                overrideList.add(str, new long[]{0}, map);
            }
        }
    }

    /** initializes or reloads the reporter */
    public void resetReporter(Map props) {
        Object o;
        String className;
        Map<String, Object> rep = Utils.cloneProperties(props);

        rep.put("Name", name);
        if ((o = rep.get("ClassName")) != null)
            className = (String) o;
        else if ((o = rep.get("Type")) != null)
            className = "org.qbroker.monitor." + (String) o;
        else
            className = "org.qbroker.monitor.ReportQuery";

        if ((o = rep.get("KeyTemplate")) != null) { // with privateReport
            withPrivateReport = true;
            privateReport = new HashMap<String, Object>();
        }

        if (reporter != null)
            reporter.destroy();
        reporter = null;

        try {
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            reporter = (MonitorReport) con.newInstance(new Object[]{rep});
        }
        catch (java.lang.reflect.InvocationTargetException e) {
            throw(new IllegalArgumentException(name +" failed to init reporter"+
                 " : " + Event.traceStack(e.getTargetException())));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to init reporter for " + className));
        }

        pm = null;
        pattern = null;
        jsonPath = null;
        builder = null;
        xpe = null;
        postSub = null;

        if ((o = rep.get("DataField")) != null && o instanceof String)
            dataField = (String) o;
        else
            dataField = reporter.getReportName();

        if ((o = rep.get("Pattern")) != null && // for pattern
            o instanceof String && ((String) o).length() > 0) {
            Perl5Compiler pc;
            try { // compile the pattern
                pc = new Perl5Compiler();
                pm = new Perl5Matcher();
                pattern = pc.compile((String) o);
                resultType = Utils.RESULT_TEXT;
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to compile the pattern: " + e.toString()));
            }
        }
        else if ((o = rep.get("JSONPath")) != null && // for jpath
            o instanceof String && ((String) o).length() > 0) {
            jsonPath = (String) o;
            resultType = Utils.RESULT_JSON;
        }
        else if ((o = rep.get("XPath")) != null && // for xpath
            o instanceof String && ((String) o).length() > 0) {
            resultType = Utils.RESULT_XML;
            try { // compile the expression
                XPath xpath = XPathFactory.newInstance().newXPath();
                xpe = xpath.compile((String) o);
                builder = Utils.getDocBuilder();
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to compile the xpath: " + e.toString()));
            }
        }
        else { // default case
            resultType = Utils.RESULT_LIST;
        }

        // postSub for the list
        if ((o = rep.get("PostSubstitution")) != null && o instanceof String)
            postSub = new TextSubstitution((String) o);

        withSharedReport=(reporter.getReportMode()==MonitorReport.REPORT_LOCAL);
        cachedProps = Utils.cloneProperties(props);
    }

    /** it resets the name template and keys and returns true on success */
    public boolean resetTemplate(String text) {
        return resetTemplate(text, ((tsub != null) ? tsub.getName() : null));
    }

    /**
     * it resets the name template and the name substitution. Then it updates
     * all keys and returns true on success
     */
    @SuppressWarnings("unchecked")
    public boolean resetTemplate(String text, String expr) {
        if (text == null || text.length() <= 0)
            return false;

        if (title.equals(text)) { // no change on name template
            if (expr != null) { // check tsub
                if (tsub == null || !expr.equals(tsub.getName())) // reset tsub
                    tsub = new TextSubstitution(expr);
                else
                    return false;
            }
            else if (tsub != null) // reset tsub
                tsub = null;
            else
                return false;
        }
        else { // name template changed
            title = text;
            if (title.indexOf("##") >= 0) { // valid template for name
                Map ph = null;
                try {
                    StringReader in = new StringReader(template.copyText());
                    ph = (Map) JSON2Map.parse(in);
                    in.close();
                }
                catch (Exception e) {
                    ph = null;
                }
                if (ph == null || ph.size() <= 0)
                    return false;

                // reset name template with title
                ph.put("Name", title);
                template = new Template(JSON2Map.toJSON(ph));
                count = template.size();
                ph.clear();
            }
            else
                return false;
            if (expr != null) { // check tsub
                if (tsub == null || !expr.equals(tsub.getName())) // reset tsub
                    tsub = new TextSubstitution(expr);
            }
            else if (tsub != null) // reset tsub
                tsub = null;
        }

        String key = null;
        if (count == 1) { // for items of String
            key = template.getField(0);
            for (int i=0; i<size; i++) { // reset all keys
                String str = (String) keyMap.remove(keys[i]);
                keys[i] = template.substitute(key, str, title);
                if (tsub != null)
                    keys[i] = tsub.substitute(keys[i]);
                if (!keyMap.containsKey(keys[i]))
                    keyMap.put(keys[i], str);
            }
        }
        else try { // for items of Map
            StringReader in;
            for (int i=0; i<size; i++) { // reset all keys
                key = keyMap.remove(keys[i]);
                in = new StringReader(key);
                Map map = (Map) JSON2Map.parse(in);
                in.close();
                keys[i] = template.substitute(map, title);
                map.clear();
                if (tsub != null)
                    keys[i] = tsub.substitute(keys[i]);
                if (!keyMap.containsKey(keys[i]))
                    keyMap.put(keys[i], key);
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to parse json: "+
                e.toString() + "\n\t" + key));
        }

        return true;
    }

    /** 
     * It returns the list of items generated by the reporter or null if it is
     * static or skipped. Currently, it works only if count = 1.
     */
    public List getItemList() throws Exception {
        Object o;
        List<String> list = new ArrayList<String>();
        String text = null;
        int i, n;
        Map<String, Object> r;

        if (!isDynamic || reporter == null) // no changes at all
            return null;

        r = reporter.generateReport(System.currentTimeMillis());
        if (reporter.getSkippingStatus() != reporter.NOSKIP)
            return null;

        if ((o = r.remove(dataField)) == null)
            throw(new IllegalArgumentException(name + " got null from " +
                dataField));
        else if (o instanceof List) { // a list of String
            List pl = (List) o;
            n = pl.size();
            for (i=0; i<n; i++) {
                o = pl.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                text = (String) o;
                if (postSub != null)
                    text = postSub.substitute(text);
                if (text != null && text.length() > 0)
                    list.add(text);
            }
        }
        else if (o instanceof String || o instanceof StringBuffer) { // text
            text = (o instanceof String) ? (String) o : 
                ((StringBuffer) o).toString();
            if (resultType == Utils.RESULT_JSON) {
                StringReader sr;
                try {
                    sr = new StringReader(text);
                    o = JSON2Map.parse(sr);
                    sr.close();
                    if (o != null) {
                        if (o instanceof Map)
                            o = JSON2Map.get((Map) o, jsonPath);
                        else
                            o = JSON2Map.get((List) o, jsonPath);
                        if (o != null && o instanceof List) {
                            List pl = (List) o;
                            n = pl.size();
                            for (i=0; i<n; i++) {
                                o = list.get(i);
                                if (o == null)
                                    text = "";
                                else if (o instanceof String)
                                    text = (String) o;
                                else if (o instanceof Map)
                                    text = JSON2Map.toJSON((Map) o, null, null);
                                else if (o instanceof List)
                                    text = JSON2Map.toJSON((List)o, null, null);
                                else
                                    text = o.toString();
                                if (postSub != null)
                                    text = postSub.substitute(text);
                                if (text != null && text.length() > 0)
                                    list.add(text);
                            }
                        }
                    }
                }
                catch (Exception e) {
                }
                sr = null;
            }
            else if (resultType == Utils.RESULT_XML) {
                StringReader sr;
                Document doc;
                try {
                    sr = new StringReader(text);
                    doc = builder.parse(new InputSource(sr));
                    o = xpe.evaluate(doc, XPathConstants.NODESET);
                    text = null;
                    sr.close();
                    if (o != null && o instanceof NodeList) {
                        NodeList nl = (NodeList) o;
                        n = nl.getLength();
                        for (i=0; i<n; i++) {
                            text = Utils.nodeToText(nl.item(i));
                            if (postSub != null)
                                text = postSub.substitute(text);
                            if (text != null && text.length() > 0)
                                list.add(text);
                        }
                    }
                }
                catch (Exception e) {
                }
                sr = null;
            }
            else if (resultType == Utils.RESULT_TEXT) {
                PatternMatcherInput pin;
                try {
                    pin = new PatternMatcherInput(text);
                    while (pm.contains(pin, pattern)) {
                        text = pm.getMatch().group(1);
                        if (postSub != null)
                            text = postSub.substitute(text);
                        if (text != null && text.length() > 0)
                            list.add(text);
                    }
                }
                catch (Exception e) {
                }
                pin = null;
            }
            else {
                throw(new IllegalArgumentException(name +
                    " result type not supported: " + resultType));
            }
        }
        else if (o instanceof Map) { // just for a list of keys
            Object[] kys = ((Map) o).keySet().toArray();
            n = kys.length;
            for (i=0; i<n; i++) {
                if (kys[i] == null || !(kys[i] instanceof String))
                    continue;
                
                text = (String) kys[i];
                if (postSub != null)
                    text = postSub.substitute(text);
                if (text != null && text.length() > 0)
                    list.add(text);
            }
        }
        else
            throw(new IllegalArgumentException(name +
                " got unsupported data type from " + dataField));

        if (withPrivateReport) { // for private report
            privateReport.clear();
            o = r.remove("Data");
            if (o != null && o instanceof List) try {
                List pl = (List) o;
                n = list.size();
                for (i=0; i<n; i++) {
                    String item = list.get(i);
                    if (item == null || item.length() <= 0)
                        continue;
                    o = pl.get(i);
                    if (o == null)
                        continue;
                    else if (o instanceof Map)
                        privateReport.put(item, o);
                    else { // wrapped with a map since only Map is supported
                        HashMap<String,Object>mp = new HashMap<String,Object>();
                        mp.put("body", o);
                        privateReport.put(item, mp);
                    }
                }
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": name list at " +
                    dataField + " not in alignment with the data: " +
                    Event.traceStack(e)));
            }
        }

        return list;
    }

    /**
     * It updates the internal lists for items and keys on a given list of
     * strings as the new items and returns the total number of items
     **/
    public int updateItems(List list) {
        int n;
        if (count > 1)
            return updateMapItems(list);

        size = 0;
        keyMap.clear();
        if (list != null && (n = list.size()) > 0) {
            String key, str;
            Object o;
            List<String> pl = new ArrayList<String>();
            for (int i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String) ||
                    ((String) o).length() <= 0)
                    continue;
                pl.add((String) o);
            }
            n = pl.size();
            keys = new String[n];
            if (n <= 0)
                return 0;
            key = template.getField(0);
            for (int i=0; i<n; i++) {
                str = pl.get(i);
                keys[i] = template.substitute(key, str, title);
                if (tsub != null)
                    keys[i] = tsub.substitute(keys[i]);
                if (!keyMap.containsKey(keys[i]))
                    keyMap.put(keys[i], str);
            }
            pl.clear();
            size = n;
        }
        else {
            keys = new String[0];
        }

        return size;
    }

    /**
     * It updates the internal lists for items and keys on a given list of
     * strings as the new items and returns the total number of items
     **/
    private int updateMapItems(List list) {
        int n;
        size = 0;
        keyMap.clear();
        if (list != null && (n = list.size()) > 0) {
            String str;
            Object o;
            List<Map> pl = new ArrayList<Map>();
            for (int i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map) || ((Map) o).size() <= 0)
                    continue;
                pl.add((Map) o);
            }
            n = pl.size();
            keys = new String[n];
            if (n <= 0)
                return 0;
            for (int i=0; i<n; i++) {
                str = idTemp.substitute(pl.get(i), idTemp.copyText());
                keys[i] = template.substitute(pl.get(i), title);
                if (tsub != null)
                    keys[i] = tsub.substitute(keys[i]);
                if (!keyMap.containsKey(keys[i]))
                    keyMap.put(keys[i], str);
            }
            pl.clear();
            size = n;
        }
        else {
            keys = new String[0];
        }

        return size;
    }

    /** removes the item from list and returns its key or null otherwise */
    public String remove(String item) {
        if (item == null || item.length() <= 0)
            return null;
        String key;
        for (int i=0; i<size; i++) {
            if (item.equals(keyMap.get(keys[i]))) {
                keyMap.remove(keys[i]);
                key = keys[i];
                size --;
                while (i < size) { // pack the array
                    keys[i] = keys[i+1];
                    i ++;
                }
                if (withPrivateReport)
                    privateReport.remove(item);
                return key;
            }
        }
        return null;
    }

    /** returns the property map for a given item or id */
    @SuppressWarnings("unchecked")
    public Map getProps(String item) {
        String text;
        Map ph = null;
        if (item == null || item.length() <= 0)
            return null;
        if (count == 1) {
            String key = template.getField(0);
            text = template.substitute(key, item, template.copyText());
        }
        else try {
            StringReader in = new StringReader(item);
            Map map = (Map) JSON2Map.parse(in);
            in.close();
            text = template.substitute(map, template.copyText());
            map.clear();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to parse json: "+
                e.toString() + "\n\t" + item));
        }

        try {
            StringReader in = new StringReader(text);
            ph = (Map) JSON2Map.parse(in);
            in.close();
        }
        catch (Exception e) {
            return null;
        }

        if (tsub != null) { // normalize the value for Name
            text = (String) ph.get("Name");
            ph.put("Name", tsub.substitute(text));
        }
        if (overrideList.size() > 0) {
            int i;
            String regex;
            Map map;
            Browser b = overrideList.browser();
            text = (String) ph.get("Name");
            while ((i = b.next()) >= 0) {
                regex = overrideList.getKey(i);
                if (!text.matches(regex))
                    continue;
                map = (Map) overrideList.get(i);
                for (Object o : map.keySet()) // override
                    ph.put(o, map.get(o));
                break;
            }
        }
        return ph;
    }

    /** returns true if the property template differs from the internal cache */
    @SuppressWarnings("unchecked")
    public boolean isPropertyChanged(Map props) {
        Map ph = null;
        try {
            StringReader in = new StringReader(template.copyText());
            ph = (Map) JSON2Map.parse(in);
            in.close();
        }
        catch (Exception e) {
            return true;
        }
        if (ph != null)
            ph.put("Name", title);
        return !Utils.compareProperties(props, ph);
    }

    public String getName() {
        return name;
    }

    public boolean isDynamic() {
        return isDynamic;
    }

    public boolean withSharedReport() {
        if (isDynamic)
            return withSharedReport;
        else
            return false;
    }

    /** returns the shared report with the list of items */
    public Map getSharedReport() {
        if (withSharedReport) {
            List<String> lst = new ArrayList<String>();
            for (String key : keys)
                lst.add(keyMap.get(key));
            Map<String, Object> mp = new HashMap<String, Object>();
            mp.put("List", lst);
            mp.put("Name", name);
            mp.put("TestTime", String.valueOf(System.currentTimeMillis()));
            mp.put("Status", "0");
            mp.put("SkippingStatus", "0");
            return mp;
        }
        else
            return null;
    }

    public boolean withPrivateReport() {
        if (isDynamic)
            return withPrivateReport;
        else
            return false;
    }

    /** returns the removed private report for the Sting item or id */
    public Map removePrivateReport(String item) {
        if (withPrivateReport)
            return (Map) privateReport.remove(item);
        else
            return null;
    }

    public void clearPrivateReport() {
        if (withPrivateReport)
            privateReport.clear();
    }

    /** returns the total number of items or keys */
    public int getSize() {
        return size;
    }

    /** returns the total number of variables in the property template */
    public int getCount() {
        return count;
    }

    /** returns the unique id for the Map item */
    public String getID(Map item) {
        if (item == null || item.size() <= 0)
            return null;
        return idTemp.substitute(item, idTemp.copyText());
    }

    /** returns the name of the i-th config */
    public String getKey(int i) {
        if (i >= 0 && i < size)
            return keys[i];
        else
            return null;
    }

    /** returns the value set of the i-th config */
    public String getItem(int i) {
        if (i >= 0 && i < size)
            return keyMap.get(keys[i]);
        else
            return null;
    }

    public boolean containsKey(String key) {
        return keyMap.containsKey(key);
    }

    public boolean containsItem(String item) {
        return keyMap.containsValue(item);
    }

    /** 
     * It diffs the props with the internal data and returns a Map with details
     * of differences, excluding the property template if it is included in the
     * props. If the returned Map contains the key of Item, there is a change
     * on it. Its value may be one of the three different data types. If the
     * value is a List, it will contains all the items to be removed from the
     * internal item list. If the value is a Map, it will be the new property
     * map for the existing reporter and its evaluations. If the value is a
     * String, it means the instance of ConfigTemplate will switch between
     * roles of dynamic and static. In this case, the exising instance of
     * ConfigTemplate has to be terminated and closed. If there are changes on
     * the name template and the substitution, they will also be included. If
     * any of the keys does not exist in the returned map, there is no change
     * on that object. If there is no any change, it will return null.
     */
    public Map<String, Object> diff(Map props) {
        Object o;
        Map<String, Object> ph = new HashMap<String, Object>();
        if ((o = props.get("Item")) == null)
            throw(new IllegalArgumentException(name + ": no Item defined"));
        else if (o instanceof List) {
            if (isDynamic) { // switched to static list
                ph.put("Item", name);
                if ((o = props.get("Template")) != null){//compare name template
                    if (!title.equals((String) o))
                        ph.put("Template", o);
                    if ((o = props.get("Substitution")) != null) {//compare tsub
                        if (tsub == null || !tsub.getName().equals((String) o))
                            ph.put("Substitution", o);
                    }
                    else if (tsub != null)
                        ph.put("Substitution", null);
                }
                else
                    throw(new IllegalArgumentException(name +
                        ": name Template is not defined"));
                return ph;
            }
            List<String> list = diff((List) o);
            if (list != null)
                ph.put("Item", list);
        }
        else if (o instanceof Map) {
            if (!isDynamic) { // switched to dynamic list
                ph.put("Item", name);
                if ((o = props.get("Template")) != null){//compare name template
                    if (!title.equals((String) o))
                        ph.put("Template", o);
                    if ((o = props.get("Substitution")) != null) {//compare tsub
                        if (tsub == null || !tsub.getName().equals((String) o))
                            ph.put("Substitution", o);
                    }
                    else if (tsub != null)
                        ph.put("Substitution", null);
                }
                else
                    throw(new IllegalArgumentException(name +
                        ": name Template is not defined"));
                return ph;
            }
            if (!Utils.compareProperties(cachedProps, (Map) o))
                ph.put("Item", Utils.cloneProperties((Map) o));
        }
        else
            throw(new IllegalArgumentException(name + ": bad Item defined"));

        if ((o = props.get("Template")) != null) { // compare name template
            if (!title.equals((String) o))
                ph.put("Template", o);
            if ((o = props.get("Substitution")) != null) { // compare tsub
                if (tsub == null || !tsub.getName().equals((String) o))
                    ph.put("Substitution", o);
            }
            else if (tsub != null)
                ph.put("Substitution", null);
        }
        else
            throw(new IllegalArgumentException(name +
                ": name Template is not defined"));
        if (ph.size() <= 0)
            return null;
        else
            return ph;
    }

    /** 
     * It diffs the list with the internal data and returns a new List with
     * details of the differences. If the returned list is null, there is no
     * difference between the two. Otherwise, the returned list will contains
     * all existing items to be removed from the internal data. In this case,
     * there may be new items to be added to the internal data.
     */
    public List<String> diff(List a) {
        HashSet<String> hSet = new HashSet<String>();
        List<String> deleted = new ArrayList<String>();
        String str;

        if (a == null)
            a = new ArrayList();

        if (count > 1) {
            for (Object obj : a) { // init hset
                if (obj instanceof Map && ((Map) obj).size() > 0)
                    hSet.add(getID((Map) obj));
            }
        }
        else {
            for (Object obj : a) { // init hset
                if (obj instanceof String && ((String) obj).length() > 0)
                    hSet.add((String) obj);
            }
        }
        for (int i=0; i<size; i++) { // check deleted first
            str = keyMap.get(keys[i]);
            if (!hSet.contains(str))
                deleted.add(str);
        }
        hSet.clear();
        if (deleted.size() > 0 || a.size() > size) // with deleted or new items
            return deleted;
        else if (count > 1) { // check the order for items
            for (int i=0; i<size; i++) {
                str = keyMap.get(keys[i]);
                if (!str.equals(getID((Map) a.get(i)))) // order changed
                    return new ArrayList<String>();
            }
        }
        else { // check the order for items
            for (int i=0; i<size; i++) {
                str = keyMap.get(keys[i]);
                if (!str.equals((String) a.get(i))) // order changed
                    return new ArrayList<String>();
            }
        }

        return null;
    }

    public void close() {
        if (reporter != null) {
            reporter.destroy();
            reporter = null;
        }
        if (keyMap != null) {
            keyMap.clear();
            keyMap = null;
        }
        if (cachedProps != null) {
            cachedProps.clear();
            cachedProps = null;
        }
        if (overrideList != null) {
            overrideList.clear();
            overrideList = null;
        }
        if (withPrivateReport && privateReport != null)
            privateReport.clear();
        privateReport = null;
        keys = null;
        size = 0;
        pm = null;
        if (template != null) {
            template.clear();
            template = null;
        }
        if (tsub != null) {
            tsub.clear();
            tsub = null;
        }
        if (postSub != null) {
            postSub.clear();
            postSub = null;
        }
        pattern = null;
        builder = null;
        xpe = null;
    }

    protected void finalize() {
        close();
    }

    public static void main(String[] args) {
        int n;
        String filename = null, target = "keys", str, data = null;
        String newfile = null, title = null, expr = null;
        ConfigTemplate cfgTemp = null;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'I':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    newfile = args[++i];
                break;
              case 'd':
                if (i+1 < args.length)
                    data = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    title = args[++i];
                break;
              case 's':
                if (i+1 < args.length)
                    expr = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    target = args[++i];
                break;
              default:
            }
        }

        if (filename == null)
            printUsage();
        else try {
            Map ph;
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map props = (Map) JSON2Map.parse(fr);
            fr.close();

            cfgTemp = new ConfigTemplate(props);
            if (target.equals("keys")) { // display all keys
                if (newfile != null) {
                    fr = new java.io.FileReader(newfile);
                    ph = (Map) JSON2Map.parse(fr);
                    fr.close();
                    List list = (List) ph.get("Item");
                    if (list == null)
                        System.out.println(target + ": newfile has no Item");
                    else
                        cfgTemp.updateItems(list);
                }
                else if (data != null) {
                    if (cfgTemp.getCount() > 1) {
                        StringReader in = new StringReader(data);
                        ph = (Map) JSON2Map.parse(in);
                        in.close();
                        cfgTemp.remove(cfgTemp.getID(ph));
                    }
                    else
                        cfgTemp.remove(data);
                }
                if (expr != null) {
                    if (title == null)
                        title = (String) props.get("Template");
                    cfgTemp.resetTemplate(title, ((expr.length()>0)?expr:null));
                }
                else if (title != null)
                    cfgTemp.resetTemplate(title);
                n = cfgTemp.getSize();
                System.out.println(target + ": size=" + n + " count=" +
                    cfgTemp.getCount());
                for (int i=0; i<n; i++) {
                    System.out.println(i + ": " + cfgTemp.getKey(i));
                }
            }
            else if (target.equals("items")) { // display all items
                if (newfile != null) {
                    fr = new java.io.FileReader(newfile);
                    ph = (Map) JSON2Map.parse(fr);
                    fr.close();
                    List list = (List) ph.get("Item");
                    if (list == null)
                        System.out.println(target + ": newfile has no Item");
                    else
                        cfgTemp.updateItems(list);
                }
                else if (data != null) {
                    if (cfgTemp.getCount() > 1) {
                        StringReader in = new StringReader(data);
                        ph = (Map) JSON2Map.parse(in);
                        in.close();
                        cfgTemp.remove(cfgTemp.getID(ph));
                    }
                    else
                        cfgTemp.remove(data);
                }
                if (expr != null) {
                    if (title == null)
                        title = (String) props.get("Template");
                    cfgTemp.resetTemplate(title, ((expr.length()>0)?expr:null));
                }
                else if (title != null)
                    cfgTemp.resetTemplate(title);
                n = cfgTemp.getSize();
                System.out.println(target + ": size=" + n + " count=" +
                    cfgTemp.getCount());
                for (int i=0; i<n; i++) {
                    System.out.println(i + ": " + cfgTemp.getItem(i));
                }
            }
            else if (target.equals("config")) { // for config on a specific item
                if (title != null)
                    cfgTemp.resetTemplate(title);
                if (data == null)
                    data = cfgTemp.getItem(0);
                else if (cfgTemp.getCount() > 1) {
                    StringReader in = new StringReader(data);
                    ph = (Map) JSON2Map.parse(in);
                    in.close();
                    data = cfgTemp.getID(ph);
                }
                ph = cfgTemp.getProps(data);
                System.out.println(JSON2Map.toJSON(ph));
            }
            else if (target.equals("list")) { // for diffs on new list
                if (newfile != null) {
                    fr = new java.io.FileReader(newfile);
                    ph = (Map) JSON2Map.parse(fr);
                    fr.close();
                    List list = (List) ph.get("Item");
                    if (list == null)
                        System.out.println(target + ": newfile has no Item");
                    else if ((list = cfgTemp.diff(list)) != null) {
                        System.out.println(target + ": " + list.size());
                        System.out.println(JSON2Map.toJSON(list, "", "\n"));
                    }
                    else
                        System.out.println(target + ": found no change");
                }
                else
                    System.out.println(target + ": newfile is not defined");
            }
            else if (target.equals("change")) { // for diffs on new config
                if (newfile != null) {
                    fr = new java.io.FileReader(newfile);
                    ph = (Map) JSON2Map.parse(fr);
                    fr.close();
                    if (ph == null || ph.size() <= 0)
                        System.out.println(target + ": newfile is bad");
                    else if ((ph = cfgTemp.diff(ph)) != null) {
                        System.out.println(target + ": " + ph.size());
                        System.out.println(JSON2Map.toJSON(ph));
                    }
                    else
                        System.out.println(target + ": config has no change");
                }
                else
                    System.out.println(target + ": newfile is not defined");
            }
            else
                System.out.println(target + " is not supported");
            if (cfgTemp != null)
                cfgTemp.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (cfgTemp != null)
                cfgTemp.close();
        }
    }

    private static void printUsage() {
        System.out.println("ConfigTemplate Version 1.0 (written by Yannan Lu)");
        System.out.println("ConfigTemplate: a list of configuration properties generated from the same template");
        System.out.println("Usage: java org.qbroker.monitor.ConfigTemplate -I cfg.json -t target [-f new.json -d data]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -t: target for display on keys, items, config, change and list (default: keys)");
        System.out.println("  -n: value for name template");
        System.out.println("  -s: value for name substitution");
        System.out.println("  -d: data of the value set as the item for config");
        System.out.println("  -f: path to the json file as new config for diffs (required by change or list)");
    }
}
