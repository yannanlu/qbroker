package org.qbroker.monitor;

/* ConfigTemplate - a Template for generated property maps of configurations */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
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
 * ConfigTemplate manages a list of similar configuration properties with a
 * property template for a specific object. The property template has at least
 * one variable. It is used to generate the property configurations for all the
 * instances with unique names in the list. Apart from the property template,
 * ConfigTemplate has also a name template to generate a list of keys as the
 * distinct names for each of the instances, as well as a list of items as the
 * values for all the variables. Moreover, a name substitution is optional to
 * further normalize the value for the names. Both the name template and the
 * list of items are updatable and resetable on demand. ConfigTemplate supports
 * updates on the property template also.
 *<br><br>
 * ConfigTemplate also supports dynamic creations of the item list. In this
 * case, Item has to be a Map specifying a MonitorReport to query or update the
 * list of items dynamically. The instance of the MonitorReport will be used
 * to generate the report frequently from which the list of items will be
 * updated automatically. The method of isDynamic() is used to check if the
 * instance of ConfigTemplate supporting dynamic item list or not. If the
 * managed object is of Monitor, it also supports the private report for each
 * dynamically generated monitor.
 *<br><br>
 * ConfigTemplate supports overriding on the configuration properties if a list
 * of property maps defined as Override. In each of the property map, it must
 * contains the property of KeyRegex that is the regex expression to match
 * against the name of the configuration property map. Once there is a match,
 * its property map will be merged into the configuration properties as the
 * override.
 *<br><br>
 * In case of dynamic generated monitors, ConfigTemplate supports both private
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
    private String name, key;
    private String content = null;
    private String[] items = null, keys = null;
    private Template template;
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
    private int size = 0, resultType = Utils.RESULT_LIST;
    private boolean isDynamic = false;
    private boolean withPrivateReport = false;
    private boolean withSharedReport = false;

    public ConfigTemplate(Map props) {
        Object o;
        String saxParser = null;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("Template")) == null)
            throw(new IllegalArgumentException("Name template is not defined" +
                " for " + name));
        template = new Template((String) o);
        if (template.size() > 0)
            key = template.getField(0);
        else
            throw(new IllegalArgumentException("Name template is empty for " +
                name));

        // substitution for Name only
        if ((o = props.get("Substitution")) != null && o instanceof String)
            tsub = new TextSubstitution((String) o);

        // for property map
        if ((o = props.get("Property")) != null && o instanceof Map) {
            Map<String, Object> ph = Utils.cloneProperties((Map) o);

            // reset name
            ph.put("Name", template.copyText());
            content = JSON2Map.toJSON(ph);
            ph.clear();
        }
        else
            throw(new IllegalArgumentException(name +
                ": no property template defined"));

        keyMap = new HashMap<String, String>();
        if ((o = props.get("Item")) == null)
            throw(new IllegalArgumentException(name + ": no item defined"));
        else if (o instanceof List)
            updateItems((List) o);
        else if (o instanceof Map) { // for the reporter
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

        withSharedReport =
            (reporter.getReportMode() == MonitorReport.REPORT_LOCAL);
        cachedProps = Utils.cloneProperties(props);
    }

    /** it resets the name template and keys and returns true on success */
    @SuppressWarnings("unchecked")
    public boolean resetTemplate(String text) {
        if (text == null || text.length() <= 0)
            return false;

        Template temp = new Template(text);
        if (temp.size() > 0) {
            Map ph = null;
            try {
                StringReader in = new StringReader(content);
                ph = (Map) JSON2Map.parse(in);
                in.close();
            }
            catch (Exception e) {
            }
            if (ph != null && ph.size() > 0) {
                String str = temp.copyText();
                ph.put("Name", str);
                key = temp.getField(0);
                content = JSON2Map.toJSON(ph);
                ph.clear();
                keyMap.clear();
                for (int i=0; i<size; i++) { // build keyMap for new keys
                    keys[i] = temp.substitute(key, items[i], str);
                    if (tsub != null)
                        keys[i] = tsub.substitute(keys[i]);
                    keyMap.put(keys[i], items[i]);
                }
                template = temp;
                return true;
            }
        }

        return false;
    }

    /** 
     * It returns the list of items generated by the reporter or null if it is
     * static or skipped
     */
    public List<String> getItemList() throws Exception {
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

    /** updates the internal item list and returns number of items */
    public int updateItems(List list) {
        Object o;
        size = 0;
        keyMap.clear();
        if (list != null && list.size() > 0) {
            String str = template.copyText();
            int i, k = 0, n = list.size();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String) ||
                    ((String) o).length() <= 0)
                    continue;
                k ++;
            }
            items = new String[k];
            keys = new String[k];
            if (k <= 0)
                return 0;
            k = 0;
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String) ||
                    ((String) o).length() <= 0)
                    continue;
                items[k] = (String) o;
                keys[k] = template.substitute(key, items[k], str);
                if (tsub != null)
                    keys[k] = tsub.substitute(keys[k]);
                if (!keyMap.containsKey(keys[k]))
                    keyMap.put(keys[k], items[k]);
                k ++;
            }
            size = k;
        }
        else {
            items = new String[0];
            keys = new String[0];
            size = 0;
        }

        return size;
    }

    /** returns the property map for a given item */
    @SuppressWarnings("unchecked")
    public Map getProps(String item) {
        String text;
        Map ph = null;
        if (item == null || item.length() <= 0)
            return null;
        try {
            text = template.substitute(key, item, content);
            StringReader in = new StringReader(text);
            ph = (Map) JSON2Map.parse(in);
            in.close();
        }
        catch (Exception e) {
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
            StringReader in = new StringReader(content);
            ph = (Map) JSON2Map.parse(in);
            in.close();
        }
        catch (Exception e) {
            return true;
        }
        if (ph != null)
            ph.put("Name", key);
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
            if (items != null) { // for list of items only
                for (String ky : items)
                    lst.add(ky);
            }
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

    /** returns the removed private report for the item */
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

    /** returns the name of the i-th config */
    public String getKey(int i) {
        if (i >= 0 && i < size)
            return keys[i];
        else
            return null;
    }

    /** returns the i-th item */
    public String getItem(int i) {
        if (i >= 0 && i < size)
            return items[i];
        else
            return null;
    }

    public boolean containsKey(String key) {
        return keyMap.containsKey(key);
    }

    public boolean containsItem(String item) {
        return keyMap.containsValue(item);
    }

    /** removes an item from the list and returns its key or null otherwise */
    public String remove(String item) {
        int i;
        String str = null;
        for (i=0; i<size; i++) {
            if (items[i].equals(item)) {
                break;
            }
        }
        if (i < size) {
            str = keys[i];
            keyMap.remove(str);
            size --;
            while (i < size) {
                keys[i] = keys[i+1];
                items[i] = items[i+1];
                i ++;
            }
        }
        if (withPrivateReport)
            privateReport.remove(item);
        return str;
    }

    /** 
     * It diffs the props with the internal data and returns a Map with
     * details of differences, excluding the property template if it is
     * included in the props. If the returned Map contains the key of
     * Item, there is a change on it. Its value may be one of the three
     * different data types. If the value is an empty List, it means new
     * items are added to the list. If the list is not empty, it contains all
     * the items to be removed. If the value is a Map, it means there is
     * a change on the reporter or the evaluations. The value is the property
     * map for the reporter and the evaluations. If the value is a String,
     * it means the instance of ConfigTemplate has swapped the dynamic role.
     * In this case, the instance of ConfigTemplate has to be deleted.
     * If the name template is different, it will also be included.
     */
    public Map<String, Object> diff(Map props) {
        Object o;
        Map<String, Object> ph = new HashMap<String, Object>();
        List<String> list;
        if ((o = props.get("Item")) == null)
            throw(new IllegalArgumentException(name + ": no Item defined"));
        else if (o instanceof List) {
            if (isDynamic) { // switched to static list
                ph.put("Item", name);
                return ph;
            }
            List pl = (List) o;
            int n = pl.size();
            for (int i=0; i<n; i++) {
                o = pl.get(i);
                if (o == null || !(o instanceof String) ||
                    ((String) o).length() <= 0)
                    continue;
                ph.put((String) o, name);
            }
            list = new ArrayList<String>();
            for (int i=0; i<size; i++) {
                if (!ph.containsKey(items[i]))
                    list.add(items[i]);
                ph.remove(items[i]);
            }
            if (list.size() > 0) // with deleted
                ph.put("Item", list);
            else if (ph.size() > 0) { // has new ones
                ph.clear();
                ph.put("Item", list);
            }
        }
        else if (o instanceof Map) {
            if (!isDynamic) { // switched to dynamic list
                ph.put("Item", name);
                return ph;
            }
            if (!Utils.compareProperties(cachedProps, (Map) o))
                ph.put("Item", Utils.cloneProperties((Map) o));
        }
        else
            throw(new IllegalArgumentException(name + ": bad Item defined"));

        if ((o = props.get("Template")) != null) { // compare name template
            if (!template.copyText().equals((String) o))
                ph.put("Template", o);
        }
        if (ph.size() <= 0)
            return null;
        else
            return ph;
    }

    /** 
     * It diffs the list with the internal data and returns an List with
     * details of differences. If the returned list is not null, there is a
     * change on the list. If the list is empty, it means new items are added
     * to the list. If the list is not empty, it contains all the items to be
     * removed.
     */
    public List<String> diff(List a) {
        int i, n;
        Object o;
        Map<String, String> ph = new HashMap<String, String>();
        List<String> list;

        if (a == null)
            a = new ArrayList();

        n = a.size();
        if (n == size) { // same number of items
            for (i=0; i<n; i++) {
                o = a.get(i);
                if (o == null || !(o instanceof String) ||
                    ((String) o).length() <= 0)
                    break;
                if (!items[i].equals((String) o))
                    break;
            }
            if (i >= n) // list is identical
                return null;
        }
        for (i=0; i<n; i++) {
            o = a.get(i);
            if (o == null || !(o instanceof String) ||
                ((String) o).length() <= 0)
                continue;
            ph.put((String) o, name);
        }
        list = new ArrayList<String>();
        for (i=0; i<size; i++) {
            if (!ph.containsKey(items[i]))
                list.add(items[i]);
            ph.remove(items[i]);
        }
        if (ph.size() > 0) // has new ones
            ph.clear();

        return list;
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
        items = null;
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

    /** not tested yet */
    public static void main(String[] args) {
        int n, index = -1;
        String filename = null, target = "key", key, str, data = "test";
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
              case 'd':
                if (i+1 < args.length)
                    data = args[++i];
                break;
              case 'i':
                if (i+1 < args.length)
                    index = Integer.parseInt(args[++i]);
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
            java.io.FileReader fr = new java.io.FileReader(filename);
            Map ph = (Map) JSON2Map.parse(fr);
            fr.close();

            cfgTemp = new ConfigTemplate(ph);
            n = cfgTemp.getSize();
            if (target.equals("key")) { // display all keys
                for (int i=0; i<n; i++) {
                    key = cfgTemp.getKey(i);
                    System.out.println(target + ": size=" + n + " count=n/a");
                    System.out.println(i++ + ": " + key);
                }
            }
            else if (target.equals("item")) { // display all items
                for (int i=0; i<n; i++) {
                    str = cfgTemp.getItem(i);
                    System.out.println(target + ": size=" + n + " count=n/a");
                    System.out.println(i++ + ": " + str);
                }
            }
            else if (target.equals("config")) {// for config properties at index
                str = cfgTemp.getItem(index);
                ph = cfgTemp.getProps(str);
                System.out.println(JSON2Map.toJSON(ph, "  ", "\n"));
            }
            else if (target.equals("data")) { // for config properties with data
                if (0 > 1) {
                    List<Map> list = new ArrayList<Map>();
                    StringReader in = new StringReader(data);
                    ph = (Map) JSON2Map.parse(in);
                    in.close();
                    list.add(ph);
                    cfgTemp.updateItems(list);
                    str = JSON2Map.normalize(ph);
                    ph = cfgTemp.getProps(str);
                    System.out.println(JSON2Map.toJSON(ph, "  ", "\n"));
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(data);
                    cfgTemp.updateItems(list);
                    ph = cfgTemp.getProps(data);
                    System.out.println(JSON2Map.toJSON(ph, "  ", "\n"));
                }
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
        System.out.println("Usage: java org.qbroker.monitor.ConfigTemplate -I cfg.json -t key [-i index -d data]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -t: target for display on key, item, data, config (default: key)");
        System.out.println("  -i: index of the item for displaying config (default: 0)");
        System.out.println("  -d: data of the values for item (default: test)");
    }
}
