package org.qbroker.monitor;

/* MonitorGroup.java - a group of monitor on various monitors */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Utils;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.ConfigTemplate;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorAction;
import org.qbroker.event.Event;

/**
 * MonitorGroup manages a group of monitors that can be used to monitor
 * various monitors periodically.  Each monitor contains three components,
 * an MonitorReport object to generate the latest report on the monitor,
 * an MonitorAction object to handle the monitor, and a set of time
 * windows in which the monitor is expected to active. MonitorGroup also
 * supports ConfigTemplate for a list of similar monitors. The method of
 * reload() is to reload monitor properties dynamically.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MonitorGroup {
    private String name;
    private String type;
    private String description;
    private String checkpointDir = null;
    private long mtime;
    private int debug = 0;
    private int maxRetry = -1;
    private int heartbeat = 0;
    private int timeout = 0;
    private boolean isDynamic = false;
    private AssetList monitorList;
    private AssetList templateList;
    private Map cachedProps = null;
    private static Map<String, Object> componentMap, includeMap;
    private static ThreadLocal<Map> sharedReport = new ThreadLocal<Map>();

    public final static int TASK_ID = 0;
    public final static int TASK_GID = 1;
    public final static int TASK_SIZE = 2;
    public final static int TASK_TYPE = 3;
    public final static int TASK_RETRY = 4;
    public final static int TASK_STATUS = 5;
    public final static int TASK_COUNT = 6;
    public final static int TASK_TIME = 7;

    public final static int GROUP_CAPACITY = 512;
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");
    private final static String[] statusText = Service.statusText;

    public MonitorGroup(Map props) {
        Object o;
        String key = null;
        MonitorAction action;
        Map<String, Object> c, monitor;
        List list;
        long[] taskInfo;
        StringBuffer strBuf = null;
        int i, j, k, n, id, capacity = 128;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;

        if ((o = props.get("Type")) != null)
            type = (String) o;

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        if (capacity <= 0)
            capacity = 128;
        else if (capacity > GROUP_CAPACITY) // hard limit
            capacity = GROUP_CAPACITY;

        if ((o = props.get("Heartbeat")) != null)
            heartbeat = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("Timeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("MaxRetry")) != null)
            maxRetry = Integer.parseInt((String) o);

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        mtime = System.currentTimeMillis();
        if ((o = props.get("Description")) != null)
            description = (String) o;
        else
            description = "a group of monitors for various monitors";

        if ((o = props.get("CheckpointDir")) != null)
            checkpointDir = (String) o;

        monitorList = new AssetList(name, capacity);
        templateList = new AssetList(name, capacity);

        if ((o = props.get("Monitor")) != null && o instanceof List)
            list = (List) o;
        else if (type != null && !"MonitorGroup".equals(type)) {
            capacity = GROUP_CAPACITY;
            if ((o = props.get("Reporter")) != null && o instanceof List)
                list = (List) o;
            else
                list = new ArrayList();
        }
        else
            list = new ArrayList();

        n = list.size();
        for (i=0; i<n; i++) { // loop thru all monitors
            o = list.get(i);
            if (o == null || !(o instanceof String || o instanceof Map)) {
                new Event(Event.WARNING, name + " Config: " +
                    " an illegal monitor at " + i).send();
                continue;
            }
            if (o instanceof Map) { // for generated monitors
                ConfigTemplate temp = null;
                String str;
                try {
                    c = Utils.cloneProperties((Map) o);
                    key = (String) c.get("Name");
                    c.put("Property", props.get(key));
                    temp = new ConfigTemplate(c);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name +
                        " Config: failed to init monitor template for " +
                        key + ": " + Event.traceStack(e)).send();
                    continue;
                }
                k = temp.getSize();
                if (!templateList.containsKey(key))
                    templateList.add(key, new long[]{k, 0}, temp);
                else {
                    new Event(Event.ERR, name +
                        " Config: duplicated key for monitor template: " +
                        key).send();
                    temp.close();
                    continue;
                }
                if (temp.isDynamic())
                    isDynamic = true;
                for (j=0; j<k; j++) { // init all generated monitors
                    str = temp.getItem(j);
                    o = temp.getProps(str);
                    if (o == null || !(o instanceof Map))
                        continue;
                    str = (String) ((Map) o).get("Name");
                    if (str == null || str.length() <= 0)
                        continue;
                    if (monitorList.containsKey(str)) {
                        new Event(Event.WARNING, name + ": duplicated " +
                            "monitor name at " + j + " in " + key).send();
                        continue;
                    }
                    monitor = initMonitor((Map) o, name);
                    if (monitor != null && monitor.size() > 0) {
                        taskInfo = new long[TASK_TIME+1];
                        for (int l=0; l<=TASK_TIME; l++)
                            taskInfo[l] = 0; 
                        taskInfo[TASK_TYPE] = 1;
                        taskInfo[TASK_TIME] = mtime;
                        id = monitorList.add(str, taskInfo, monitor);
                        if (id >= 0) {
                            restore((MonitorAction) monitor.get("Action"));
                            monitor.put("GroupName", name);
                            monitor.put("TaskInfo", taskInfo);
                            if ((debug & Service.DEBUG_INIT) > 0)
                                new Event(Event.DEBUG, name + " Config: " +str+
                                    " has been created at " + id).send();
                        }
                        else { // list is full
                            id = monitorList.size();
                            new Event(Event.ERR, name + " failed to add " +
                                "instance for " + str + " at " + id +" out of "+
                                monitorList.getCapacity()).send();
                            ((MonitorReport) monitor.get("Report")).destroy();
                            monitor.clear();
                        }
                    }
                }
            }
            else { // normal case
                Map<String, Object> ph;
                key = (String) o;
                if (key.length() <= 0) {
                    new Event(Event.ERR, name + " Config: " +
                        " empty monitor name at " + i).send();
                    continue;
                }
                if (monitorList.containsKey(key)) {
                    new Event(Event.WARNING, name + ": duplicated " +
                        "monitor name at " + i + " for " + key).send();
                    continue;
                }
                ph = Utils.cloneProperties((Map) props.get(key));
                monitor = initMonitor(ph, name);
                if (monitor != null && monitor.size() > 0) {
                    taskInfo = new long[TASK_TIME+1];
                    for (int l=0; l<=TASK_TIME; l++)
                        taskInfo[l] = 0; 
                    taskInfo[TASK_TIME] = mtime;
                    id = monitorList.add(key, taskInfo, monitor);
                    if (id >= 0) {
                        restore((MonitorAction) monitor.get("Action"));
                        monitor.put("GroupName", name);
                        monitor.put("TaskInfo", taskInfo);
                        if ((debug & Service.DEBUG_INIT) > 0)
                            new Event(Event.DEBUG, name + " Config: " + key +
                                " has been created at " + id).send();
                    }
                    else { // list is full
                        id = monitorList.size();
                        new Event(Event.ERR,name+" failed to add instance for "+
                            key + " at " + id + " out of " +
                            monitorList.getCapacity()).send();
                        ((MonitorReport) monitor.get("Report")).destroy();
                        monitor.clear();
                    }
                }
            }
        }

        cachedProps = props;
    }

    /**
     * It gets the new item list for each dynamic ConfigTemplates and removes
     * those items not in the new lists. Then it updates the monitor list
     * and returns number of items either removed or added.
     *<br/><br/>
     * In case of the private reports generated by the dynamic templates, they
     * will be added to the monitor tasks on the key of "PrivateReport".
     * For all shared reports from dynamic templates, they will be available
     * during the refreshing process.
     */
    @SuppressWarnings("unchecked")
    private int refreshMonitors() {
        int i, id, j, k, n, size = 0;
        String key, str, operation;
        Browser b;
        ConfigTemplate temp = null;
        List<String> list = null, pl;
        Map<String, Object> monitor, reports;
        Map ph;

        if (!isDynamic || templateList.size() <= 0)
            return 0;
        reports = new HashMap<String, Object>();
        sharedReport.set(reports);
        n = 0;
        b = templateList.browser();
        while ((i = b.next()) >= 0) {
            temp = (ConfigTemplate) templateList.get(i);
            if (temp == null)
                continue;
            if (!temp.isDynamic())
                continue;
            size ++;
            try {
                list = temp.getItemList();
            }
            catch (Exception e) {
                list = null;
                new Event(Event.ERR, name + ": failed to get item list for " +
                    temp.getName() + ": " + Event.traceStack(e)).send();
            }
            if (list == null) // skipped or exceptioned
                continue;
            key = temp.getName();
            pl = temp.diff(list);
            if (pl == null) { // no changes
                if (temp.withSharedReport() &&
                    (ph = temp.getSharedReport()) != null) {
                    reports.put(key, ph);
                    if ((debug & Service.DEBUG_LOOP) > 0)
                        new Event(Event.DEBUG, name +
                            " added a shared report for " + key).send();
                }
                if (!temp.withPrivateReport())
                    continue;
                k = list.size();
                for (j=0; j<k; j++) { // for private reports
                    str = temp.getKey(j);
                    String item = temp.getItem(j);
                    Map task = (Map) monitorList.get(str);
                    task.put("PrivateReport", temp.removePrivateReport(item));
                    if ((debug & Service.DEBUG_LOOP) > 0)
                        new Event(Event.DEBUG, name +
                            " added a private report for " + item).send();
                }
                temp.clearPrivateReport();
                continue;
            }
            k = pl.size();
            for (j=0; j<k; j++) {
                str = (String) pl.get(j);
                if (str == null || str.length() <= 0)
                    continue;
                if (temp.containsItem(str)) {
                    str = temp.remove(str);
                    n += destroyMonitor(str);
                    if ((debug & (Service.DEBUG_LOAD | Service.DEBUG_INIT)) > 0)
                        new Event(Event.DEBUG, name + ": " + str +
                            " has been removed for dynamic " + key).send();
                }
            }
            temp.updateItems(list);
            if (temp.withSharedReport() && (ph=temp.getSharedReport()) != null){
                reports.put(key, ph);
                if ((debug & Service.DEBUG_LOOP) > 0)
                    new Event(Event.DEBUG, name +
                        " added a shared report for " + key).send();
            }
            // add new ones
            k = temp.getSize();
            for (j=0; j<k; j++) { // loop thru all items
                str = temp.getKey(j);
                id = monitorList.getID(str);
                if (id >= 0) { // existing one so rotate
                    monitorList.rotate(id);
                    if ((debug & Service.DEBUG_LOAD) > 0 &&
                        (debug & Service.DEBUG_TRAN) > 0)
                        new Event(Event.DEBUG, name + " Config: " +
                            str +" has no change for "+ key).send();
                    if (temp.withPrivateReport()) {
                        Map task = (Map) monitorList.get(id);
                        String item = temp.getItem(j);
                       task.put("PrivateReport",temp.removePrivateReport(item));
                        if ((debug & Service.DEBUG_LOOP) > 0)
                            new Event(Event.DEBUG, name +
                                " added a private report for " + item).send();
                    }
                    continue;
                }
                // new generated object or retry
                n ++;
                String item = temp.getItem(j);
                ph = temp.getProps(item);
                if (ph == null)
                    monitor = null;
                else {
                    str = (String) ph.get("Name");
                    if (str == null || str.length() <= 0)
                        monitor = null;
                    else
                        monitor = initMonitor(ph, name);
                    ph.clear();
                }
                if (monitor == null) // failed
                    operation = "failed";
                else if (monitor.size() <= 0) // disabled
                    operation = "disabled";
                else { // created
                    long[] taskInfo = new long[TASK_TIME+1];
                    for (int l=0; l<=TASK_TIME; l++)
                        taskInfo[l] = 0;
                    taskInfo[TASK_TYPE] = 1;
                    taskInfo[TASK_TIME] = mtime;
                    id = monitorList.add(str, taskInfo, monitor);
                    if (id >= 0) {
                        restore((MonitorAction) monitor.get("Action"));
                        monitor.put("GroupName", name);
                        monitor.put("TaskInfo", taskInfo);
                        if (temp.withPrivateReport()) {
                            monitor.put("PrivateReport",
                                temp.removePrivateReport(item));
                            if ((debug & Service.DEBUG_LOOP) > 0)
                                new Event(Event.DEBUG, name +
                                    " added a private report for "+item).send();
                        }
                        operation = "created at " + id;
                    }
                    else { // list is full
                        id = monitorList.size();
                        new Event(Event.ERR,name+" failed to add instance for "+
                            str + " at " + id + " out of " +
                            monitorList.getCapacity()).send();
                        ((MonitorReport) monitor.get("Report")).destroy();
                        monitor.clear();
                        continue;
                    }
                }
                if ((debug & (Service.DEBUG_LOAD | Service.DEBUG_INIT)) > 0)
                    new Event(Event.DEBUG, name + " Config: " + str +
                        " has been " + operation +" for "+ key).send();
            }
            if (temp.withPrivateReport())
                temp.clearPrivateReport();
        }
        sharedReport.remove();
        reports.clear();

        if (n > 0) { // rotate all the monitors in the right order
            Object o;
            List lst;
            if ((o = cachedProps.get("Monitor")) != null && o instanceof List)
                lst = (List) o;
            else if (type != null && !"MonitorGroup".equals(type)) {
                if((o=cachedProps.get("Reporter")) != null && o instanceof List)
                    lst = (List) o;
                else
                    lst = new ArrayList();
            }
            else
                lst = new ArrayList();

            k = lst.size();
            for (i=0; i<k; i++) { // loop thru all monitors
                o = lst.get(i);
                if (o == null || !(o instanceof String || o instanceof Map))
                    continue;
                if (o instanceof Map) { // for generated monitors
                    int m;
                    key = (String) ((Map) o).get("Name");
                    temp = (ConfigTemplate) templateList.get(key);
                    if (temp == null)
                        continue;
                    m = temp.getSize();
                    for (j=0; j<m; j++) {
                        str = temp.getKey(j);
                        if ((id = monitorList.getID(str)) >= 0) {
                            monitorList.rotate(id);
                        }
                    }
                }
                else { // normal case
                    str = (String) o;
                    if ((id = monitorList.getID(str)) >= 0) {
                        monitorList.rotate(id);
                    }
                }
            }
        }

        if (size <= 0) // no dynamic templates any more
            isDynamic = false;

        return n;
    }

    /**
     * It deletes all removed monitors including the generated ones.
     * The list contains all the monitors in the new group property map.
     * The Map of change contains key-null pairs for those removed
     * monitors.  Upon success, all the key-null paris will be removed
     * from change.  It returns the number of monitors deleted from
     * the group.
     */
    private int deleteMonitors(List list, Map change) {
        Object o;
        Object[] keys;
        Map ph, c;
        ConfigTemplate temp = null;
        String key, str, operation;
        int i, k, n, size;

        if (change == null)
            change = new HashMap();

        k = 0;
        // look for keys with null content to remove those deleted components
        keys = change.keySet().toArray();
        n = keys.length;
        for (i=0; i<n; i++) {
            o = keys[i];
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            if ((o = change.get(key)) == null) { // null for deletion
                o = cachedProps.remove(key);
                change.remove(key);

                if (templateList.containsKey(key)) { // for generated objs
                    temp = (ConfigTemplate) templateList.remove(key);
                    if (temp != null)
                        size = temp.getSize();
                    else
                        size = 0;
                    for (int j=0; j<size; j++) {
                        k += destroyMonitor(temp.getKey(j));
                        new Event(Event.INFO, name + ": " + temp.getKey(j) +
                            " has been removed for " + key).send();
                    }
                    if (temp != null)
                        temp.close();
                    operation = "deleted";
                }
                else if (monitorList.containsKey(key)) {
                    k += destroyMonitor(key);
                    operation = "deleted";
                }
                else
                    operation = "removed";

                new Event(Event.INFO, name + ": " + key + " has been " +
                    operation).send();
            }
        }

        if (list != null && list.size() > 0)
            n = list.size();
        else
            n = 0;

        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            ph = (Map) o;
            if ((o = ph.get("Name")) == null || !(o instanceof String)) 
                continue;
            key = (String) o;
            if (key.length() <= 0 || !templateList.containsKey(key))
                continue;
            temp = (ConfigTemplate) templateList.get(key);
            if (temp == null) {
                templateList.remove(key);
                continue;
            }
            try {
                c = temp.diff(ph);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + " Config: diff failed for " +
                    key + ": " + e.toString()).send();
                c = null;
            }
            if (c == null) // no change on names and list
                continue;
            else if (c.containsKey("Item")) { // some items removed
                o = c.get("Item");
                if (o == null)
                    o = new ArrayList();
                else if (o instanceof String) { // swap the dynamic roles
                    if (!change.containsKey(key)) {
                        size = temp.getSize();
                        for (int j=0; j<size; j++) {
                            str = temp.getKey(j);
                            k += destroyMonitor(str);
                            new Event(Event.INFO, name + ": " + str +
                                " has been removed for " + key).send();
                        }
                        operation = (temp.isDynamic()) ? "disable" : "enable";
                        templateList.remove(key);
                        temp.close();
                        new Event(Event.INFO, name + ": " + key +
                            " has been deleted to "+ operation +
                            " dynamics").send();
                    }
                    continue;
                }
                else if (o instanceof Map) { // reset reporter
                    if (!change.containsKey(key)) {
                        temp.resetReporter((Map) o);
                        new Event(Event.INFO, name + ": dynamic " + key +
                            " has been reset").send();
                    }
                    o = new ArrayList();
                }

                size = ((List) o).size();
                for (int j=0; j<size; j++) {
                    str = (String) ((List) o).get(j);
                    if (str == null || str.length() <= 0)
                        continue;
                    if (temp.containsItem(str)) {
                        str = temp.remove(str);
                        k += destroyMonitor(str);
                        new Event(Event.INFO, name + ": " + str +
                            " has been removed for " + key).send();
                    }
                }
                if (temp.getSize() <= 0 && !temp.isDynamic()) { // empty
                    templateList.remove(key);
                    temp.close();
                    new Event(Event.INFO, name + ": " + key +
                        " has been deleted").send();
                }
                else if (!change.containsKey(key)) { // no change on key
                    if ((o = c.get("Template")) != null) { // renamed
                        size = temp.getSize();
                        int[] ids = new int[size];
                        for (int j=0; j<size; j++)
                            ids[j] = monitorList.getID(temp.getKey(j));
                        if (temp.resetTemplate((String) o)) { // reset
                            for (int j=0; j<size; j++)
                                if (ids[j] >= 0)
                                    monitorList.replaceKey(ids[j],
                                        temp.getKey(j));
                        }
                        else
                            new Event(Event.ERR, name +
                                " Config: failed to reset template for " +
                                key).send();
                    }
                    // in case new items added
                    o = c.get("Item");
                    if (o != null && o instanceof List)
                        temp.updateItems((List) o);
                }
            }
            else if (!change.containsKey(key)) { // no change on key
                if ((o = c.get("Template")) != null) { // renamed
                    size = temp.getSize();
                    int[] ids = new int[size];
                    for (int j=0; j<size; j++)
                        ids[j] = monitorList.getID(temp.getKey(j));
                    if (temp.resetTemplate((String) o)) { // reset
                        for (int j=0; j<size; j++)
                            if (ids[j] >= 0)
                                monitorList.replaceKey(ids[j],
                                    temp.getKey(j));
                    }
                    else
                        new Event(Event.ERR, name +
                            " Config: failed to reset template for " +
                            key).send();
                }
                // in case new items added
                o = c.get("Item");
                if (o != null && o instanceof List)
                    temp.updateItems((List) o);
            }
        }

        return k;
    }

    /**
     * With the new property Map, props, of the group and the Map
     * of change for the group, it creates or updates all the monitors
     * according to the order specified in the props. The Map of change
     * should not contain any null value.  Each key-value pair in change is
     * for either a new monitor or an existing monitor.  After it is
     * done, props should have all property Map for the group.
     */
    @SuppressWarnings("unchecked")
    private void updateMonitors(Map props, Map change) {
        Object o;
        Object[] keys;
        MonitorAction action = null;
        MonitorReport report = null;
        Map<String, Object> c, monitor;
        Map map;
        Iterator iter;
        List list = null;
        String key, str, operation;
        long[] taskInfo;
        int i, j, k, n, id;
        boolean isNewMaster = true;

        if (props == null || props.size() <= 0) { // no changes on master
            props = cachedProps;
            isNewMaster = false;
        }

        if ((o = props.get("Monitor")) != null && o instanceof List)
            list = (List) o;
        else if ((o = props.get("Reporter")) != null && o instanceof List)
            list = (List) o;
        else
            list = new ArrayList();

        mtime = System.currentTimeMillis();
        n = list.size();
        for (i=0; i<n; i++) { // loop thru all monitors
            o = list.get(i);
            if (o == null || !(o instanceof String || o instanceof Map)) {
                new Event(Event.WARNING, name + " Config: " +
                    " an illegal monitor at " + i).send();
                continue;
            }
            if (o instanceof Map) // for generated monitors
                key = (String) ((Map) o).get("Name");
            else  // normal case
                key = (String) o;

            if (key.length() <= 0) {
                new Event(Event.ERR, name + " Config: " +
                    " empty monitor name at " + i).send();
                continue;
            }

            if (!change.containsKey(key)) { // existing obj with no change
                if (isNewMaster && !props.containsKey(key)) // copy it over
                    props.put(key, cachedProps.get(key));

                if (monitorList.containsKey(key)) { // existing one
                    id = monitorList.getID(key);
                    monitorList.rotate(id);
                    operation = "shuffled";
                    if ((debug & Service.DEBUG_DIFF) > 0 &&
                        (debug & Service.DEBUG_TRAN) > 0)
                        new Event(Event.DEBUG, name + " Config: " + key +
                            " has been " + operation).send();
                    continue;
                }
                else if (!(o instanceof Map)) { // failed or disabled
                    o = cachedProps.get(key);
                    monitor = initMonitor((Map) o, name);
                    if (monitor == null) // failed
                        operation = "failed";
                    else if (monitor.size() <= 0) // disabled
                        operation = "redisabled";
                    else { // created
                        taskInfo = new long[TASK_TIME+1];
                        for (j=0; j<=TASK_TIME; j++)
                            taskInfo[j] = 0; 
                        taskInfo[TASK_TIME] = mtime;
                        id = monitorList.add(key, taskInfo, monitor);
                        if (id >= 0) {
                            restore((MonitorAction) monitor.get("Action"));
                            monitor.put("GroupName", name);
                            monitor.put("TaskInfo", taskInfo);
                            operation = "recreated at " + id;
                        }
                        else { // list is full
                            id = monitorList.size();
                            new Event(Event.ERR, name +
                                " failed to add instance for " + key +
                                " at " + id + " out of " +
                                monitorList.getCapacity()).send();
                            ((MonitorReport) monitor.get("Report")).destroy();
                            monitor.clear();
                            continue;
                        }
                    }
                    if ((debug & Service.DEBUG_LOAD) > 0)
                        new Event(Event.DEBUG, name + " Config: " + key +
                            " has been " + operation).send();
                    continue;
                }
                else if (!templateList.containsKey(key)) { // create objs 
                    ConfigTemplate temp = null;
                    try {
                        c = Utils.cloneProperties((Map) o);
                        c.put("Property", props.get(key));
                        temp = new ConfigTemplate(c);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, name +
                            " Config: failed to init monitor template for " +
                            key + ": " + Event.traceStack(e)).send();
                        continue;
                    }
                    k = temp.getSize();
                    if (!templateList.containsKey(key))
                        templateList.add(key, new long[]{k, 0}, temp);
                    else {
                        new Event(Event.ERR, name +
                            " Config: duplicated key for monitor template: " +
                            key).send();
                        temp.close();
                        continue;
                    }
                    if (temp.isDynamic())
                        isDynamic = true;
                    for (j=0; j<k; j++) { // loop thru all items
                        str = temp.getItem(j);
                        o = temp.getProps(str);
                        if (o == null || !(o instanceof Map))
                            continue;
                        str = (String) ((Map) o).get("Name");
                        if (str == null || str.length() <= 0)
                            continue;
                        if (monitorList.containsKey(str)) { // duplicated
                            new Event(Event.WARNING, name + ": duplicated " +
                                "monitor name of " + str + " for " +
                                key).send();
                            continue;
                        }
                        else { // new generated object or retry
                            monitor = initMonitor((Map) o, name);
                            if (monitor == null) // failed
                                operation = "failed";
                            else if (monitor.size() <= 0) // disabled
                                operation = "disabled";
                            else { // recreated
                                taskInfo = new long[TASK_TIME+1];
                                for (int l=0; l<=TASK_TIME; l++)
                                    taskInfo[l] = 0; 
                                taskInfo[TASK_TYPE] = 1;
                                taskInfo[TASK_TIME] = mtime;
                                id = monitorList.add(str, taskInfo, monitor);
                                if (id >= 0) {
                                  restore((MonitorAction)monitor.get("Action"));
                                    monitor.put("GroupName", name);
                                    monitor.put("TaskInfo", taskInfo);
                                    operation = "created at " + id;
                                }
                                else { // list is full
                                    id = monitorList.size();
                                    new Event(Event.ERR, name +
                                        " failed to add instance for " + str +
                                        " at " + id + " out of " +
                                        monitorList.getCapacity()).send();
                               ((MonitorReport)monitor.get("Report")).destroy();
                                    monitor.clear();
                                    continue;
                                }
                            }
                            if ((debug & Service.DEBUG_LOAD) > 0)
                                new Event(Event.DEBUG, name + " Config: " +str+
                                    " has been "+ operation+" for "+key).send();
                        }
                    }
                }
                else { // rotate or rename generated objs 
                    ConfigTemplate temp = null;
                    temp = (ConfigTemplate) templateList.get(key);
                    k = temp.getSize();
                    for (j=0; j<k; j++) { // loop thru all items
                        str = temp.getKey(j);
                        id = monitorList.getID(str);
                        if (id >= 0) { // existing one so rotate
                            monitorList.rotate(id);
                            map = (Map) monitorList.get(id);
                            if (str.equals((String) map.get("Name"))) {
                                if ((debug & Service.DEBUG_LOAD) > 0 &&
                                    (debug & Service.DEBUG_TRAN) > 0)
                                    new Event(Event.DEBUG, name + " Config: " +
                                        str +" has no change for "+ key).send();
                                continue;
                            }
                            else { // for renamed
                                action = (MonitorAction) map.remove("Action");
                                report = (MonitorReport) map.remove("Report");
                                taskInfo = (long[]) map.get("TaskInfo");
                                taskInfo[TASK_TIME] = mtime;
                                new Event(Event.INFO, name+": "+map.get("Name")+
                                    " has been removed for " + key).send();
                            }
                        }
                        else
                            map = null;
                        // new generated object or retry
                        str = temp.getItem(j);
                        o = temp.getProps(str);
                        if (o == null || !(o instanceof Map))
                            monitor = null;
                        else {
                            str = (String) ((Map) o).get("Name");
                            if (str == null || str.length() <= 0)
                                monitor = null;
                            else
                                monitor = initMonitor((Map) o, name);
                        }
                        if (monitor == null) // failed
                            operation = "failed";
                        else if (monitor.size() <= 0) // disabled
                            operation = "disabled";
                        else if (id >= 0) { // renamed
                            c = action.checkpoint();
                            report.destroy();
                            action = (MonitorAction) monitor.get("Action");
                            action.restoreFromCheckpoint(c);
                            map.put("Name", action.getName());
                            map.put("Action", action);
                            map.put("Report", monitor.remove("Report"));
                            monitor.clear();
                            operation = "renamed";
                        }
                        else { // created
                            taskInfo = new long[TASK_TIME+1];
                            for (int l=0; l<=TASK_TIME; l++)
                                taskInfo[l] = 0; 
                            taskInfo[TASK_TYPE] = 1;
                            taskInfo[TASK_TIME] = mtime;
                            id = monitorList.add(str, taskInfo, monitor);
                            if (id >= 0) {
                                restore((MonitorAction) monitor.get("Action"));
                                monitor.put("GroupName", name);
                                monitor.put("TaskInfo", taskInfo);
                                operation = "created at " + id;
                            }
                            else { // list is full
                                id = monitorList.size();
                                new Event(Event.ERR, name +
                                    " failed to add instance for " + str +
                                    " at " + id + " out of " +
                                    monitorList.getCapacity()).send();
                               ((MonitorReport)monitor.get("Report")).destroy();
                                monitor.clear();
                                continue;
                            }
                        }
                        if ((debug & Service.DEBUG_LOAD) > 0)
                            new Event(Event.DEBUG, name + " Config: " + str +
                                " has been " + operation +" for "+ key).send();
                    }
                }
            }
            else if (!(o instanceof Map)) { // regular obj with changes
                o = change.remove(key); // assume the key is used only once
                props.put(key, o);
                if (!cachedProps.containsKey(key)) { // new obj
                    monitor = initMonitor((Map) o, name);
                    if (monitor == null) // new obj failed
                        operation = "failed";
                    else if (monitor.size() <= 0) // new obj disabled
                        operation = "disabled";
                    else { // new obj created
                        taskInfo = new long[TASK_TIME+1];
                        for (j=0; j<=TASK_TIME; j++)
                            taskInfo[j] = 0;
                        taskInfo[TASK_TIME] = mtime;
                        id = monitorList.add(key, taskInfo, monitor);
                        if (id >= 0) {
                            restore((MonitorAction) monitor.get("Action"));
                            monitor.put("GroupName", name);
                            monitor.put("TaskInfo", taskInfo);
                            operation = "created at " + id;
                        }
                        else { // list is full
                            id = monitorList.size();
                            new Event(Event.ERR, name + " failed to add " +
                                "instance for " + key + " at "+ id + " out of "+
                                monitorList.getCapacity()).send();
                            ((MonitorReport) monitor.get("Report")).destroy();
                            monitor.clear();
                            continue;
                        }
                    }
                }
                else if (!monitorList.containsKey(key)) { // failed or disabled
                    monitor = initMonitor((Map) o, name);
                    if (monitor == null) // failed
                        operation = "failed";
                    else if (monitor.size() <= 0) // disabled
                        operation = "redisabled";
                    else { // created
                        taskInfo = new long[TASK_TIME+1];
                        for (j=0; j<=TASK_TIME; j++)
                            taskInfo[j] = 0;
                        taskInfo[TASK_TIME] = mtime;
                        id = monitorList.add(key, taskInfo, monitor);
                        if (id >= 0) {
                            restore((MonitorAction) monitor.get("Action"));
                            monitor.put("GroupName", name);
                            monitor.put("TaskInfo", taskInfo);
                            operation = "recreated at " + id;
                        }
                        else { // list is full
                            id = monitorList.size();
                            new Event(Event.ERR, name + " failed to add " +
                                "instance for " + key + " at "+ id + " out of "+
                                monitorList.getCapacity()).send();
                            ((MonitorReport) monitor.get("Report")).destroy();
                            monitor.clear();
                            continue;
                        }
                    }
                }
                else { // reload existing obj
                    id = monitorList.getID(key);
                    map = (Map) monitorList.get(key);
                    action = (MonitorAction) map.get("Action");
                    if (action != null)
                        c = action.checkpoint();
                    else
                        c = null;
                    report = (MonitorReport) map.get("Report");
                    if (report != null)
                        report.destroy();
                    new Event(Event.INFO, name + ": " + key +
                        " has been removed").send();
                    map.clear();
                    monitor = initMonitor((Map) o, name);
                    if (monitor == null) { // failed
                        removeMonitor(key);
                        operation = "failed";
                    }
                    else if (monitor.size() <= 0) { // disabled
                        removeMonitor(key);
                        operation = "disabled";
                    }
                    else { // replace the existing obj
                        action = (MonitorAction) monitor.get("Action");
                        if (action != null && c != null)
                            action.restoreFromCheckpoint(c);
                        monitorList.set(key, monitor);
                        monitor.put("GroupName", name);
                        taskInfo = monitorList.getMetaData(key);
                        taskInfo[TASK_TIME] = mtime;
                        monitor.put("TaskInfo", taskInfo);
                        monitorList.rotate(id);
                        operation = "reloaded";
                    }
                }
                if ((debug & Service.DEBUG_LOAD) > 0)
                    new Event(Event.DEBUG, name + " Config: " + key +
                        " has been " + operation).send();
            }
            else if (!templateList.containsKey(key)) { // for generated objects
                ConfigTemplate temp = null;
                boolean isNew = !cachedProps.containsKey(key);
                map = (Map) change.remove(key); // assume it used only once
                props.put(key, map);
                try {
                    c = Utils.cloneProperties((Map) o);
                    c.put("Property", props.get(key));
                    temp = new ConfigTemplate(c);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name +
                        " Config: failed to init monitor template for " +
                        key + ": " + Event.traceStack(e)).send();
                    continue;
                }
                k = temp.getSize();
                if (!templateList.containsKey(key))
                    templateList.add(key, new long[]{k, 0}, temp);
                else {
                    new Event(Event.ERR, name +
                        " Config: duplicated key for monitor template: " +
                        key).send();
                    temp.close();
                    continue;
                }
                if (temp.isDynamic())
                    isDynamic = true;
                for (j=0; j<k; j++) { // loop thru all items
                    str = temp.getItem(j);
                    o = temp.getProps(str);
                    if (o == null || !(o instanceof Map))
                        continue;
                    str = (String) ((Map) o).get("Name");
                    if (str == null || str.length() <= 0)
                        continue;
                    if (monitorList.containsKey(str)) { // duplicated
                        new Event(Event.WARNING, name + ": duplicated " +
                            "monitor name of " + str + " for " + key).send();
                        continue;
                    }
                    else { // new generated object or retry
                        monitor = initMonitor((Map) o, name);
                        if (monitor == null) // failed
                            operation = "failed";
                        else if (monitor.size() <= 0) // disabled
                            operation = "disabled";
                        else { // created
                            taskInfo = new long[TASK_TIME+1];
                            for (int l=0; l<=TASK_TIME; l++)
                                taskInfo[l] = 0; 
                            taskInfo[TASK_TYPE] = 1;
                            taskInfo[TASK_TIME] = mtime;
                            id = monitorList.add(str, taskInfo, monitor);
                            if (id >= 0) {
                                restore((MonitorAction) monitor.get("Action"));
                                monitor.put("GroupName", name);
                                monitor.put("TaskInfo", taskInfo);
                                operation = "created at " + id;
                            }
                            else { // list is full
                                id = monitorList.size();
                                new Event(Event.ERR, name +
                                    " failed to add instance for " + str +
                                    " at "+ id + " out of " +
                                    monitorList.getCapacity()).send();
                               ((MonitorReport)monitor.get("Report")).destroy();
                                monitor.clear();
                                continue;
                            }
                        }
                        if ((debug & Service.DEBUG_LOAD) > 0) {
                            new Event(Event.DEBUG, name + " Config: " + str +
                                " has been " + operation +" for "+ key).send();
                        }
                    }
                }
            }
            else { // update for generated objs
                ConfigTemplate temp = null;
                Map<String, Object> h = new HashMap<String, Object>();
                map = (Map) change.remove(key); // assume it used only once
                props.put(key, map);
                temp = (ConfigTemplate) templateList.remove(key);
                k = temp.getSize();
                for (j=0; j<k; j++) { // loop thru all items
                    str = temp.getKey(j);
                    map = (Map) monitorList.remove(str);
                    // saved for checkpointing
                    h.put(temp.getItem(j), map);
                    new Event(Event.INFO, name + ": " + str +
                        " has been removed for " + key).send();
                }
                temp.close();
                temp = null;
                try {
                    c = Utils.cloneProperties((Map) o);
                    c.put("Property", props.get(key));
                    temp = new ConfigTemplate(c);
                }
                catch (Exception e) {
                    new Event(Event.ERR, name +
                        " Config: failed to init monitor template for " +
                        key + ": " + Event.traceStack(e)).send();
                    // cleanup in case h is not empty
                    iter = h.keySet().iterator();
                    while (iter.hasNext()) {
                        str = (String) iter.next();
                        map = (Map) h.get(str);
                        if (map == null)
                            continue;
                        report = (MonitorReport) map.remove("Report");
                        if (report != null)
                            report.destroy();
                        map.clear();
                    }
                    h.clear();
                    continue;
                }
                k = temp.getSize();
                if (!templateList.containsKey(key))
                    templateList.add(key, new long[]{k, 0}, temp);
                else {
                    new Event(Event.ERR, name +
                        " Config: duplicated key for monitor template: " +
                        key).send();
                    temp.close();
                    // cleanup in case h is not empty
                    iter = h.keySet().iterator();
                    while (iter.hasNext()) {
                        str = (String) iter.next();
                        map = (Map) h.get(str);
                        if (map == null)
                            continue;
                        report = (MonitorReport) map.remove("Report");
                        if (report != null)
                            report.destroy();
                        map.clear();
                    }
                    h.clear();
                    continue;
                }
                if (temp.isDynamic())
                    isDynamic = true;
                for (j=0; j<k; j++) { // loop thru all items
                    str = temp.getItem(j);
                    o = temp.getProps(str);
                    if (o == null || !(o instanceof Map))
                        continue;
                    str = (String) ((Map) o).get("Name");
                    if (str == null || str.length() <= 0)
                        continue;
                    if (monitorList.containsKey(str)) { // duplicated
                        new Event(Event.WARNING, name + ": duplicated " +
                            "monitor name of " + str + " for " +
                            key).send();
                        continue;
                    }
                    else { // new generated object or retry
                        monitor = initMonitor((Map) o, name);
                        if (monitor == null) // failed
                            operation = "failed";
                        else if (monitor.size() <= 0) { // disabled
                            operation = "redisabled";
                        }
                        else if (!h.containsKey(temp.getItem(j))) { // created
                            taskInfo = new long[TASK_TIME+1];
                            for (int l=0; l<=TASK_TIME; l++)
                                taskInfo[l] = 0; 
                            taskInfo[TASK_TYPE] = 1;
                            taskInfo[TASK_TIME] = mtime;
                            id = monitorList.add(str, taskInfo, monitor);
                            if (id >= 0) {
                                restore((MonitorAction) monitor.get("Action"));
                                monitor.put("GroupName", name);
                                monitor.put("TaskInfo", taskInfo);
                                operation = "created at " + id;
                            }
                            else { // list is full
                                id = monitorList.size();
                                new Event(Event.ERR, name +
                                    " failed to add instance for " + str +
                                    " at "+ id + " out of " +
                                    monitorList.getCapacity()).send();
                               ((MonitorReport)monitor.get("Report")).destroy();
                                monitor.clear();
                                continue;
                            }
                        }
                        else { // reload
                            String cn;
                            // recover the checkpoint first
                            map = (Map) h.remove(temp.getItem(j));
                            action = (MonitorAction) monitor.get("Action");
                            cn = action.getClass().getName();
                            o = map.get("Action");
                            if (cn.equals(o.getClass().getName())) {//checkpoint
                                action.restoreFromCheckpoint(
                                    ((MonitorAction) o).checkpoint());
                                taskInfo = (long[]) map.get("TaskInfo");
                            }
                            else { // no restore
                                taskInfo = new long[TASK_TIME+1];
                                for (int l=0; l<=TASK_TIME; l++)
                                    taskInfo[l] = 0; 
                                taskInfo[TASK_TYPE] = 1;
                            }
                            report = (MonitorReport) map.get("Report");
                            if (report != null)
                                report.destroy();
                            map.clear();
                            monitorList.add(str, taskInfo, monitor);
                            monitor.put("GroupName", name);
                            taskInfo[TASK_TIME] = mtime;
                            monitor.put("TaskInfo", taskInfo);
                            operation = "reloaded";
                        }
                        if ((debug & Service.DEBUG_LOAD) > 0)
                            new Event(Event.DEBUG, name + " Config: " + str +
                                " has been " + operation +" for "+ key).send();
                    }
                }
                // cleanup in case h is not empty
                iter = h.keySet().iterator();
                while (iter.hasNext()) {
                    str = (String) iter.next();
                    map = (Map) h.get(str);
                    if (map == null)
                        continue;
                    report = (MonitorReport) map.remove("Report");
                    if (report != null)
                        report.destroy();
                    map.clear();
                }
                h.clear();
            }
        }
    }

    /**
     * It returns a Map with only modified or new props.  The deleted
     * items will be set to null in the returned Map.  If the container
     * has been changed, it will be reflected by the object keyed by base.
     */
    public Map<String, Object> diff(Map props) {
        if (cachedProps.equals(props))
            return null;
        if (type != null && !"MonitorGroup".equals(type)) // for Reporter
            return Utils.diff("GROUP", includeMap, cachedProps, props);
        else
            return Utils.diff("GROUP", componentMap, cachedProps, props);
    }

    /** It loggs the details of the change returned by diff()  */
    public void showChange(String prefix, String base, Map change,
        boolean detail) {
        Object o;
        Map h;
        String key;
        StringBuffer strBuf = new StringBuffer();
        int n = 0;
        if (change == null)
            return;
        for (Iterator iter=change.keySet().iterator(); iter.hasNext();) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = change.get(key);
            n ++;
            if (o == null)
                strBuf.append("\n\t"+n+": "+ key + " has been removed");
            else if (key.equals(base)) {
                strBuf.append("\n\t"+n+": "+ key + " has changes");
                if(!detail)
                    continue;
                if (!"GROUP".equals(type)) // for reporters
                    h = Utils.getMasterProperties(base,includeMap,cachedProps);
                else // for groups 
                    h=Utils.getMasterProperties(base,componentMap,cachedProps);
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
            else if (!cachedProps.containsKey(key))
                strBuf.append("\n\t"+n+": "+ key + " is new");
            else if ((h = (Map) cachedProps.get(key)) == null || h.size() <= 0)
                strBuf.append("\n\t"+n+": "+ key + " was empty");
            else if (h.equals((Map) o))
                strBuf.append("\n\t"+n+": "+ key + " has no diff");
            else {
                strBuf.append("\n\t"+n+": "+ key + " has been changed");
                if (!detail)
                    continue;
                new Event(Event.DEBUG, prefix + ": " + key +
                    " has been changed with detailed diff:\n" +
                    JSON2Map.diff(h, (Map) o, "")).send();
            }
        }
        if (n > 0)
            new Event(Event.DEBUG, prefix +" diff:"+ strBuf.toString()).send();
    }

    /**
     * It reloads the changes for the entire group.
     */
    public int reload(Map change) {
        Object o;
        Map master = null;
        List list = null;
        String key, basename = "GROUP";
        int i, j, k, n, size;

        k = 0;
        if (change.containsKey(basename)) { // for new master
            master = (Map) change.remove(basename);
            if (master != null) {
                if ((o = master.get("Heartbeat")) != null)
                    heartbeat = 1000 * Integer.parseInt((String) o);

                if ((o = master.get("Timeout")) != null)
                    timeout = 1000 * Integer.parseInt((String) o);

                if ((o = master.get("MaxRetry")) != null)
                    maxRetry = Integer.parseInt((String) o);

                if ((o = master.get("Debug")) != null)
                    debug = Integer.parseInt((String) o);

                if ((o = master.get("CheckpointDir")) != null)
                    checkpointDir = (String) o;
                else
                    checkpointDir = null;

                if ((o = master.get("Monitor")) != null &&
                    o instanceof List)
                    list = (List) o;

                k = deleteMonitors(list, change);
            }
        }

        updateMonitors(master, change);

        if (change != null)
            change.clear();

        if (master != null && master.size() > 0) { // reset the cached props
            cachedProps.clear();
            cachedProps = master;
        }

        return k;
    }

    /**
     * It dispatches refreshed monitors to xq and returns total number of
     *  dispached monitor jobs 
     */
    public int dispatch(XQueue xq) {
        int i, id, k;
        Object o;
        if (isDynamic) { // refresh dynamic item list
            k = refreshMonitors();
            if ((debug & Service.DEBUG_LOAD) > 0 &&
                (debug & Service.DEBUG_TRAN) > 0)
                new Event(Event.DEBUG, name + " Config: " + k +
                    " dynamic monitors either removed or added").send();
        }
        Browser b = monitorList.browser();
        k = 0;
        while ((id = b.next()) >= 0) { // dispatch tasks
            o = monitorList.get(id);
            if (o != null && o instanceof Map) {
                i = xq.reserve(100, k);
                if (i < 0) // not reserved
                    continue;
                xq.add(o, k);
                k ++;
            }
        }
        return k;
    }

    /**
     * returns a Map containing queried information of the group according to
     * the result type
     */
    public Map<String, String> queryInfo(long currentTime, long sessionTime,
        String target, String key, int type) {
        int i, j, k, n, id, size;
        long tm;
        String str, text;
        Map<String, String> h = null;
        Browser browser;
        long[] taskInfo;
        Object o;
        boolean isXML = (type & Utils.RESULT_XML) > 0;
        boolean isJSON = (type & Utils.RESULT_JSON) > 0;
        if (key == null)
            key = target;
        if ("GROUP".equals(target)) { // for group instance
            StringBuffer strBuf = new StringBuffer();
            browser = monitorList.browser();
            h = new HashMap<String, String>();
            while ((i = browser.next()) >= 0) {
                str = monitorList.getKey(i);
                taskInfo = monitorList.getMetaData(i);
                tm = taskInfo[TASK_TIME];
                text = statusText[((int) taskInfo[TASK_STATUS] +
                    statusText.length) % statusText.length];
                if (isXML) {
                    strBuf.append("<Record type=\"ARRAY\">");
                    strBuf.append("<Name>" + Utils.escapeXML(str) +
                        "</Name>");
                    strBuf.append("<ID>" + i + "</ID>");
                    strBuf.append("<Count>" + taskInfo[TASK_COUNT] +
                        "</Count>");
                    strBuf.append("<Time>" +
                        Utils.escapeXML(Event.dateFormat(new Date(tm)))+
                        "</Time>");
                    strBuf.append("<Status>" + Utils.escapeXML(text) +
                        "</Status>");
                    strBuf.append("</Record>");
                }
                else if (isJSON) {
                    if (strBuf.length() > 0)
                        strBuf.append(",");
                    strBuf.append("{\"Name\":\"" + Utils.escapeJSON(str)+"\"");
                    strBuf.append(",\"ID\":\"" + i + "\"");
                    strBuf.append(",\"Count\":\"" + taskInfo[TASK_COUNT]+"\"");
                    strBuf.append(",\"Time\":\"" +
                        Utils.escapeJSON(Event.dateFormat(new Date(tm))) +"\"");
                    strBuf.append(",\"Status\":\""+
                        Utils.escapeJSON(text)+"\"}");
                }
                else
                    h.put(str, i + " " + taskInfo[TASK_COUNT] +
                        " "+ Event.dateFormat(new Date(tm)) + " " + text);
            }
            if (isXML)
                h.put(target, strBuf.toString());
            else if (isJSON)
                h.put(target, "[" + strBuf.toString() + "]");
        }

        return h;
    }

    private void checkpoint(MonitorAction action) {
        if (action == null || checkpointDir == null)
            return;
        String key = action.getName();
        Map<String, Object> chkpt = action.checkpoint();
        if (chkpt != null && chkpt.size() > 0) try {
            File file = new File(checkpointDir);
            if (!file.exists())
                file.mkdirs();
            file = new File(checkpointDir + FILE_SEPARATOR + key + ".json");
            PrintWriter fout = new PrintWriter(new FileOutputStream(file));
            fout.println(JSON2Map.toJSON(chkpt, "", "\n"));
            fout.close();
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": checkpoint failed for " + key +
                ": " + e.toString()).send();
        }
    }

    private void restore(MonitorAction action) {
        if (action == null || checkpointDir == null)
            return;
        String key = action.getName();
        File file = new File(checkpointDir + FILE_SEPARATOR + key + ".json");
        if (!file.exists() || !file.canRead())
            return;
        try {
            FileReader fr = new FileReader(file);
            Map h = (Map) JSON2Map.parse(fr);
            fr.close();
            action.restoreFromCheckpoint(Utils.cloneProperties(h));
            if (h != null)
                h.clear();
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to load checkpoint for " +
                key + ": " + Event.traceStack(e)).send();
        }
    }

    /**
     * It removes the monitor of the name from the group
     * and returns the removed object.
     */
    private Map removeMonitor(String key) {
        long[] taskInfo = monitorList.getMetaData(key);
        Object o = monitorList.remove(key);
        return (Map) o;
    }

    /**
     * It removes the monitor of the name from the group and
     * destroies the monitor.  It returns 1 for success or 0 otherwise.
     */
    private int destroyMonitor(String key) {
        Object o;
        MonitorAction action;
        Map monitor = (Map) removeMonitor(key);
        if (monitor != null) {
            action = (MonitorAction) monitor.get("Action");
            checkpoint(action);
            o = monitor.get("Report");
            if (o != null && o instanceof MonitorReport)
                ((MonitorReport) o).destroy();
            monitor.clear();
            return 1;
        }
        else
            return 0;
    }

    public String getName() {
        return name;
    }

    public int getSize() {
        return monitorList.size();
    }

    public int getCapacity() {
        return monitorList.getCapacity();
    }

    public int getHeartbeat() {
        return heartbeat;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public long getMTime() {
        return mtime;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public boolean isDynamic() {
        return isDynamic;
    }

    public void close() {
        Browser browser;
        String key;
        int i, id;
        browser = monitorList.browser();
        while ((id = browser.next()) >= 0) {
            key = monitorList.getKey(id);
            destroyMonitor(key);
        }
        monitorList.clear();
        browser = templateList.browser();
        while ((id = browser.next()) >= 0) {
            ConfigTemplate temp = (ConfigTemplate) templateList.get(id);
            if (temp != null)
                temp.close();
        }
        templateList.clear();
        cachedProps.clear();
        cachedProps = null;
    }

    /**
     * It initializes an monitor object with the given property Map
     * and returns a Map upon success or null otherwise.  If the
     * monitor is successfully initialized but it has been disabled,
     * the returned Map is empty.  Otherwise, the returned Map
     * contains all the objects for the monitor.
     */
    public static Map<String, Object> initMonitor(Map ph, String prefix) {
        Object o = null;
        MonitorAction action;
        MonitorReport report;
        TimeWindows tw;
        String className, key, type;

        if (ph == null || ph.size() <= 0) {
            new Event(Event.ERR, prefix +": null or empty property Map").send();
            return null;
        }
        if ((o = ph.get("Name")) == null || !(o instanceof String)) {
            new Event(Event.ERR, prefix + ": Name not defined").send();
            return null;
        }

        key = (String) o;
        type = (String) ph.get("Type");
        className = (String) ph.get("ClassName");
        if ((type == null || type.length() == 0) &&
            (className == null || className.length() == 0)) {
            new Event(Event.ERR, prefix + ": Type or ClassName " +
                "not defined for " + key).send();
            return null;
        }

        if ("ShortJob".equals(type)) {
            try {
                action = new org.qbroker.monitor.ShortJob(ph);
                report = new org.qbroker.monitor.ActionSkipper(ph);
                tw = new TimeWindows((Map) ph.get("ActiveTime"));
            }
            catch (org.qbroker.common.DisabledException e) {
                new Event(Event.INFO, prefix + " " + type + ": " + key +
                    " " + e.getMessage()).send();
                return new HashMap<String, Object>();
            }
            catch (Exception e) {
                new Event(Event.ERR, prefix + " " + type + ": " + key +
                  " failed to be initialized: "+Event.traceStack(e)).send();
                return null;
            }
        }
        else try { // generic type
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            report = (MonitorReport) con.newInstance(new Object[]{ph});
            if (report instanceof MonitorAction)
                action = (MonitorAction) report;
            else {
                action = new org.qbroker.monitor.DummyAction(ph);
            }
            tw = new TimeWindows((Map) ph.get("ActiveTime"));
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            if (ex!=null && ex instanceof org.qbroker.common.DisabledException){
                new Event(Event.INFO, prefix + " " + type + ": " + key +
                    " " + ex.toString()).send();
                return new HashMap<String, Object>();
            }
            else {
                new Event(Event.ERR, prefix + " " + type + ": " + key +
                    " failed to be initialized: "+Event.traceStack(ex)).send();
                return null;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, prefix + " " + type + ": " + key +
                " failed to be initialized: "+Event.traceStack(e)).send();
            return null;
        }

        Map<String, Object> task = new HashMap<String, Object>();
        task.put("TimeWindow", tw);
        task.put("Report", report);
        task.put("KeyList", report.getReportKeys());
        task.put("Action", action);
        task.put("Name", action.getName());
        if ((o = ph.get("Site")) != null)
            task.put("Site", o);
        if ((o = ph.get("Category")) != null)
            task.put("Category", o);
        if ((o = ph.get("Type")) != null)
            task.put("Type", o);
        new Event(Event.INFO, prefix + " " + type + ": " + key +
            " is initialized").send();
        return task;
    }

    /** returns the report map stored in the sharedReport on the rptName */
    public static Map getReport(String rptName) {
        Map map = sharedReport.get();
        if (map != null)
            return (Map) map.get(rptName);
        else
            return null;
    }

    static {
        List<Object> list = new ArrayList<Object>();
        componentMap = new HashMap<String, Object>();
        list.add("Monitor");
        componentMap.put("GROUP", list);
        list = new ArrayList<Object>();
        includeMap = new HashMap<String, Object>();
        list.add("Reporter");
        includeMap.put("GROUP", list);
    }
}
