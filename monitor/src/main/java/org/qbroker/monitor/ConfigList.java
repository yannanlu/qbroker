package org.qbroker.monitor;

/* ConfigList - a list of downloaded property maps for configurations */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.PropertyMonitor;
import org.qbroker.event.Event;

/**
 * ConfigList manages a list of configuration properties for various objects.
 * It contains a property monitor to download the configurations for all the
 * items of the list from a repository or a data source. Each downloaded
 * configuration has two identifiers, a unique id as the item in the list and
 * a unique name as the key for the object. ConfigList provides a set of
 * methods to maintain and manage these configurations loaded from a remote
 * repository or a remote data source.
 *<br/><br/>
 * One of the use cases is the support of external rulesets for message nodes.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ConfigList {
    private String name, basename = null;
    private String dataField = null;
    private String[] items = null;
    private PropertyMonitor pm = null;
    private Template template = null;
    private Map cachedProps = null, dataMap;
    private Map<String, Object> map;
    private int size = 0, debug = 0;

    public ConfigList(Map props) {
        Object o;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("DataField")) != null && o instanceof Map)
            dataMap = (Map) o;
        else
            throw(new IllegalArgumentException("DataField is not defined"));

        template = new Template(name + "__##item##");

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        map = new HashMap<String, Object>();
        resetReporter(props);
    }

    /** initializes or reloads the reporter */
    public void resetReporter(Map props) {
        Object o;
        String operation = null;

        basename = (String) props.get("Basename");
        if (pm == null) { // create the reporter
            pm = new PropertyMonitor(props);
            operation = "created";
        }
        else { // reload
            try {
                o = new PropertyMonitor(props);
            }
            catch (Exception e) {
                o = null;
            }
            if (o != null) {
                pm.destroy();
                pm = (PropertyMonitor) o;
                if (size > 0)
                    pm.setOriginalProperty(cachedProps);
                else
                    pm.setOriginalProperty(new HashMap());
                operation = "reloaded";

                if ((o = props.get("DataField")) != null &&
                    o instanceof Map) // update dataMap
                    dataMap = (Map) o;
                else
                    new Event(Event.ERR, name +
                        ": DataField is not defined").send();
            }
            else // failed to init so roll it back
                operation = "rolled back";
        }

        if (operation != null) {
            new Event(Event.INFO, name + " ConfigList: " + basename +
                " has been " + operation).send();
        }
    }

    /** initializes or reloads the template for names */
    public void resetTemplate(String key) {
        if (key != null && key.length() > 0 && !name.equals(key)) {
            String str;
            name = key;
            template = new Template(name + "__##item##");
            map.clear();
            for (int i=0; i<size; i++) {
                str = template.substitute("item",items[i],template.copyText());
                map.put(str, items[i]);
            }
        }
    }

    /** sets the data field with the given key on the property monitor */
    public void setDataField(String key) {
        Object o;
        String str;
        Iterator iter = dataMap.keySet().iterator();
        while (iter.hasNext()) {
            str = (String) iter.next();
            if (str == null || str.length() <= 0)
                continue;
            o = dataMap.get(str);
            if (o == null || !(o instanceof String))
                continue;
            if (!((String) o).equals(key))
                continue;
            // found the dataField for the key
            dataField = str;
            if (pm != null)
                pm.setDataField(dataField);
            return;
        }
        new Event(Event.ERR, name + " ConfigList: dataField not found for " +
            key).send();
    }

    /**
     * It downloads the config property map from the given data source and
     * returns the change hashmap or null if it is skipped or it has no change.
     */
    public Map loadList() {
        Object o;
        List list;
        Map ph;
        Map<String, Object> latest;
        String str = null;
        long currentTime;
        int i, n;

        if (pm == null) {
            new Event(Event.ERR, name +
                ": no configuration repository initialized").send();
            return null;
        }

        currentTime = System.currentTimeMillis();
        try { // load configuration
            latest = pm.generateReport(currentTime);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " Exception in report: " +
                Event.traceStack(e)).send();
            return null;
        }

        // skip both check and the action if the skip flag is set
        if (pm.getSkippingStatus() != MonitorReport.NOSKIP)
            return null;

        // retrieve the new properties first
        ph = (Map) latest.get("Properties");
        if (ph == null || ph.size() <= 0)
            return null;

        if ((o = ph.get(dataField)) == null || !(o instanceof List)) {
            new Event(Event.ERR, name + ": bad list at " + dataField).send();
            return null;
        }
        list = (List) o;
        if (debug > 0 && cachedProps != null) {
            new Event(Event.DEBUG, name + ": " + basename +
                " has been changed with detailed diff:\n" +
                JSON2Map.diff(cachedProps, ph, "")).send();
        }
        cachedProps = ph;

        try { // get change and update the local files
            pm.performAction(TimeWindows.NORMAL, currentTime, latest);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + " Exception in action: " +
                Event.traceStack(e)).send();
            return null;
        }

        // check the difference
        if ((o = latest.get("Properties")) == null || ((Map) o).size() <= 0)
            return null;
        else {
            size = list.size();
            items = new String[size];
            for (i=0; i<size; i++) {
                items[i] = (String) list.get(i);
                str = template.substitute("item",items[i],template.copyText());
                map.put(str, items[i]);
            }
            return (Map) o;
        }
    }

    /** returns the property map for a given item */
    public Map<String, Object> getProps(String item) {
        String key;
        Map<String, Object> ph = null;
        if (item == null || item.length() <= 0)
            return null;
        if (!cachedProps.containsKey(item))
            return null;
        key = template.substitute("item", item, template.copyText());
        ph = Utils.cloneProperties((Map) cachedProps.get(item));
        ph.put("Name", key);
        return ph;
    }

    public String getName() {
        return name;
    }

    /** returns the total number of items */
    public int getSize() {
        return size;
    }

    /** returns the i-th item */
    public String getItem(int i) {
        if (i >= 0 && i < size)
            return items[i];
        else
            return null;
    }

    /** returns the name of the i-th config */
    public String getKey(int i) {
        if (i >= 0 && i < size)
            return template.substitute("item", items[i], template.copyText());
        else
            return null;
    }

    /** returns the key for the item */
    public String getKey(String item) {
        return template.substitute("item", item, template.copyText());
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public boolean containsItem(String item) {
        return map.containsValue(item);
    }

    public void close() {
        if (pm != null) {
            pm.destroy();
            pm = null;
        }
        if (cachedProps != null)
            cachedProps.clear();
        cachedProps = null;
        if (dataMap != null) {
            dataMap.clear();
            dataMap = null;
        }
        if (map != null) {
            map.clear();
            map = null;
        }
        if (template != null) {
            template.clear();
            template = null;
        }
        items = null;
        size = 0;
    }

    protected void finalize() {
        close();
    }
}
