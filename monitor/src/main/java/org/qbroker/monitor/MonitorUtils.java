package org.qbroker.monitor;

/* MonitorUtils.java - a wrapper for utilities */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Date;
import java.util.Arrays;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.DisabledException;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.monitor.MonitorReport;
import org.qbroker.monitor.MonitorAction;
import org.qbroker.monitor.DummyAction;
import org.qbroker.monitor.ActionSkipper;
import org.qbroker.monitor.ShortJob;
import org.qbroker.event.Event;

/**
 * MonitorUtils wrappes some utilities for Monitor stuff
 *<br>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class MonitorUtils {
    public final static int REPT_STATUS = 0;
    public final static int REPT_SKIP = 1;
    public final static int REPT_TIME = 2;
    public final static String OS_NAME =System.getProperty("os.name");
    public final static String OS_ARCH =System.getProperty("os.arch");
    public final static String OS_VERSION = System.getProperty("os.version");
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");
    public final static String USER_NAME = System.getProperty("user.name");
    public final static String JAVA_VERSION =
        System.getProperty("java.specification.version");
    private final static String hostname = Event.getHostName().toLowerCase();
    private final static Map<String, String> defaultMap =
        new HashMap<String, String>();
    private final static Template defaultTemplate =
        new Template("##hostname## ##HOSTNAME## ##host## ##HOST## ##owner## ##pid##");
    private final static int[] prime = {2, 3, 5, 7, 11, 13, 17, 19,
        23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67,
        71, 73, 79, 83, 89, 97, 101};
    private static java.lang.reflect.Method getReport = null;
    private static java.lang.reflect.Method initReport = null;
    private static java.lang.reflect.Method initReportWithKeys = null;
    private static java.lang.reflect.Method getPrivateReport = null;

    public MonitorUtils() {
    }

    static {
        int i = hostname.indexOf('.');
        String host = (i > 0) ? hostname.substring(0, i) : hostname;
        defaultMap.put("hostname", hostname);
        defaultMap.put("HOSTNAME", hostname.toUpperCase());
        defaultMap.put("host", host);
        defaultMap.put("HOST", host.toUpperCase());
        defaultMap.put("owner", USER_NAME);
        defaultMap.put("pid", String.valueOf(Event.getPID()));
    }

    public static List[] getDependencies(List groupList) {
        int i, n, size;
        Object o;
        List[] depGroup;
        List list;
        n = groupList.size();
        depGroup = new ArrayList[n];

        for (i=0; i<n; i++) {
            o = groupList.get(i);
            if (o == null || !(o instanceof Map))
                continue;
            list = (List) ((Map) o).get("Dependency");
            if (list == null || !(list instanceof List))
                continue;
            size = list.size();
            List group = new ArrayList();
            for (int j=0; j<size; j++) {
                o = list.get(j);
                Map dep;
                if (o instanceof String) {
                    dep = new HashMap();
                    dep.put("Name", (String) o);
                    dep.put("Type", "ReportQuery");
                }
                else if (o instanceof Map) {
                    dep = Utils.cloneProperties((Map) o);
                }
                else
                    continue;
                if (dep.containsKey("StaticDependencyGroup"))
                    dep.remove("StaticDependencyGroup");
                if (dep.containsKey("DependencyGroup"))
                    dep.remove("DependencyGroup");
                if (dep.containsKey("Reference"))
                    dep.remove("Reference");
                if (dep.containsKey("ActionGroup"))
                    dep.remove("ActionGroup");
                if (dep.containsKey("MaxRetry"))
                    dep.remove("MaxRetry");
                if (dep.get("DisableMode") == null)
                    dep.put("DisableMode", "1");
                if (dep.get("Step") == null)
                    dep.put("Step", "1");
                group.add(getNewReport(dep));
            }
            depGroup[i] = group;
        }
        return depGroup;
    }

    /**
     * return a MonitorReport for static dependency
     */
    public static MonitorReport getStaticDependency(Map props) {
        Map dep = Utils.cloneProperties(props);
        if (dep.containsKey("ReportMode"))
            dep.remove("ReportMode");
        if (dep.containsKey("DependencyGroup"))
            dep.remove("DependencyGroup");
        if (dep.containsKey("Reference"))
            dep.remove("Reference");
        if (dep.containsKey("ActionProgram"))
            dep.remove("ActionProgram");
        if (dep.get("DisableMode") == null)
            dep.put("DisableMode", "1");
        dep.put("Step", "1");
        return getNewReport(dep);
    }

    /**
     * It returns MonitorReport.NOSKIP if there is at least one group of
     * dependencies with status of NOSKIP. Otherwise, it returns either
     * MonitorReport.DISABLED without exceptions or MonitorReport.EXCEPTION
     * with exceptions.
     *<br><br>
     * NOSKIP:             true<br>
     * SKIPPED, DISABLED:  false<br>
     * EXCEPTION:          exception<br>
     */
    public static int checkDependencies(long currentTime, List[] dependencies,
        String prefix) {
        if (dependencies == null || dependencies.length == 0)
            return MonitorReport.NOSKIP;
        List depGroup;
        MonitorReport dep;
        int numException = 0;
        int i, k, n, skip = MonitorReport.NOSKIP;
        n = dependencies.length;

        for (i=0; i<n; i++) {
            depGroup = dependencies[i];
            if (depGroup == null)
                continue;
            k = depGroup.size();
            for (int j=0; j<k; j++) {
                dep = (MonitorReport) depGroup.get(j);
                try {
                    dep.generateReport(currentTime);
                }
                catch (Exception e) {
                    skip = MonitorReport.EXCEPTION;
                    new Event(Event.ERR, prefix +
                        " failed to generate report for dependency " +
                        dep.getReportName() + " at " + i + ":" + j + " of " +
                        dep.getClass().getName() + ": " + e.toString()).send();
                    break;
                }
                skip = dep.getSkippingStatus();
                if (skip != MonitorReport.NOSKIP)
                    break;
            }
            if (skip == MonitorReport.NOSKIP) // found one group NOSKIP
                break;
            else if (skip == MonitorReport.EXCEPTION) // count number of ex
                numException ++;
        }

        if (skip == MonitorReport.NOSKIP) // at least one group with NOSKIP
            return skip;
        else if (numException > 0) // failed to get conclusion due to exception
            return MonitorReport.EXCEPTION;
        else // all groups are disabled
            return MonitorReport.DISABLED;
    }

    /**
     * It cleans up all the dependency objects.
     */
    public static void clearDependencies(List[] dependencies) {
        List depGroup;
        Iterator iter;
        MonitorReport dep;
        Object o;
        int numException = 0;
        int i, k, n, skip = MonitorReport.NOSKIP;

        if (dependencies == null || dependencies.length == 0)
            return;
        n = dependencies.length;

        for (i=0; i<n; i++) {
            depGroup = dependencies[i];
            if (depGroup == null)
                continue;
            k = depGroup.size();
            for (int j=0; j<k; j++) {
                if ((o = depGroup.get(j)) == null ||
                    !(o instanceof MonitorReport))
                    continue;
                dep = (MonitorReport) o;
                try {
                    dep.destroy();
                }
                catch (Exception e) {
                }
                dep = null;
            }
            depGroup.clear();
        }
    }

    public static int getReportMode(String mode) {
        if (mode != null) {
            if ("shared".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_SHARED;
            else if ("final".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_FINAL;
            else if ("cluster".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_CLUSTER;
            else if ("node".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_NODE;
            else if ("group".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_NODE;
            else if ("flow".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_FLOW;
            else if ("cached".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_CACHED;
            else if ("local".equals(mode.toLowerCase()))
                return MonitorReport.REPORT_LOCAL;
            else
                return MonitorReport.REPORT_NONE;
        }
        else
            return MonitorReport.REPORT_NONE;
    }

    public static MonitorReport getNewReport(Map props) {
        Object o = null;
        String name = (String) props.get("Name");
        String className = (String) props.get("ClassName");
        if (className == null && (o = props.get("Type")) != null)
            className = "org.qbroker.monitor." + (String) o;
        if (className != null) {
            try {
                java.lang.reflect.Constructor con =
                    Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                return (MonitorReport) con.newInstance(new Object[] {props});
            }
            catch (InvocationTargetException e) {
                throw(new IllegalArgumentException(name + ": at " + className +
                    ": " + Event.traceStack(e.getTargetException())));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": at " +
                    className + ": " + e.toString()));
            }
        }
        else
            throw(new IllegalArgumentException(name + ": at " +
                (String) o + ": failed to instantiate the object"));
    }

    public static MonitorAction getNewAction(Map props) {
        Object o = null;
        String name = (String) props.get("Name");
        String className = (String) props.get("ClassName");
        if (className == null && (o = props.get("Type")) != null)
            className = "org.qbroker.monitor." + (String) o;

        if (className != null) {
            try {
                java.lang.reflect.Constructor con =
                    Class.forName(className).getConstructor(
                    new Class[] {Class.forName("java.util.Map")});
                return (MonitorAction) con.newInstance(new Object[] {props});
            }
            catch (InvocationTargetException e) {
                throw(new IllegalArgumentException(name + ": at " + className +
                    ": " + Event.traceStack(e.getTargetException())));
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + ": at " +
                    className + ": " + e.toString()));
            }
        }
        else
            throw(new IllegalArgumentException(name + ": at " +
                (String) o + ": failed to instantiate the object"));
    }

    /**
     * runs a script within the timeout and return an event.
     * Caller is supposed to add name, site and other fields
     * to the event and sends it
     */
    public static Event runScript(String program, int timeout) {
        Event event;
        try {
            String output = RunCommand.exec(program, timeout);
            event = new Event(Event.INFO, "Script: '" +
                program + "' completed");
            event.setAttribute("output", output);
        }
        catch (TimeoutException e) {
            event = new Event(Event.WARNING, "Script: '" +
                program + "' timed out");
            event.setAttribute("error", e.getMessage());
        }
        catch (RuntimeException e) {
            event = new Event(Event.ERR, "Script: '" +
                program + "' failed");
            event.setAttribute("error", e.getMessage());
        }
        return event;
    }

    public static String substitute(String input, Template template) {
        if (template == null)
            template = defaultTemplate;
        return Utils.substitute(input, template, defaultMap);
    }

    public static String substitute(String input, Template template,
        Map<String, String> data) {
        if (template == null)
            template = defaultTemplate;
        if (data == null)
            data = defaultMap;
        return Utils.substitute(input, template, data);
    }

    public static Pattern[][] getPatterns(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        return getPatterns(name, ph, pc, defaultTemplate, defaultMap);
    }

    public static Pattern[][] getPatterns(String name, Map ph,
        Perl5Compiler pc, Template template, Map map)
        throws MalformedPatternException {
        int i, j, n, size = 0;
        Object o;
        Map h;
        List pl, pp;
        String pStr;
        Pattern[][] p = new Pattern[0][];
        if ((o = ph.get(name)) != null) {
            if (o instanceof List) {
                pl = (List) o;
                size = pl.size();
                p = new Pattern[size][];
                for (i=0; i<size; i++) {
                    h = (Map) pl.get(i);
                    pp = (List) h.get("Pattern");
                    if (pp == null)
                        n = 0;
                    else
                        n = pp.size();
                    Pattern[] q = new Pattern[n];
                    for (j=0; j<n; j++) {
                        pStr = select(pp.get(j));
                        if (pStr == null)
                            pStr = "";
                        if (template != null && map != null)
                            pStr = substitute(pStr, template, map);
                        q[j] = pc.compile(pStr);
                    }
                    p[i] = q;
                }
            }
            else {
                return null;
            }
        }
        return p;
    }

    public static boolean filter(String text, Pattern[][] patterns,
        Perl5Matcher pm, boolean def) {
        int i, j, n, size;
        Pattern[] p;
        boolean status = def;

        if (text == null || patterns == null)
            return (!status);

        size = patterns.length;
        if (size <= 0)
            return status;

        for (i=0; i<size; i++) {
            p = patterns[i];
            if (p == null)
                continue;
            n = p.length;
            status = true;
            for (j=0; j<n; j++) {
                if (!pm.contains(text, p[j])) {
                    status = false;
                    break;
                }
            }
            if (status)
                break;
        }
        return status;
    }

    public static void clearMapArray(Map[] hmGroup) {
        if (hmGroup == null || hmGroup.length == 0)
            return;
        for (int i=0; i<hmGroup.length; i++) {
            if (hmGroup[i] != null)
                hmGroup[i].clear();
            hmGroup[i] = null;
        }
    }

    /**
     * use this with care since the dependency may not be defined.
     * It returns the obj if obj is a String, otherwise return the String
     * value for the first hit evaluation.
     */
    public static String select(Object obj) {
        if (obj == null)
            return null;
        if (obj instanceof String)
            return (String) obj;
        else if (obj instanceof Map) {
            Object o;
            MonitorReport reporter;
            String key, value = null;
            int i, n;
            boolean hasQuery;
            Map dep;
            List list;
            if (((Map) obj).containsKey("Option")) {
                o = ((Map) obj).get("Option");
                hasQuery = false;
            }
            else {
                o = ((Map) obj).get("Query");
                hasQuery = true;
            }
            if (o == null || !(o instanceof List))
                return null;
            list = (List) o;
            n = list.size();
            for (i=0; i<n; i++) {
                if ((o = list.get(i)) == null || !(o instanceof Map))
                    continue;
                dep = (Map) o;
                if (dep.size() <= 0)
                    continue;
                Iterator iter = dep.keySet().iterator();
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                o = dep.get(key);
                if ("default".equals(key)) {
                    value = (String) o;
                    continue;
                }
                dep = new HashMap();
                dep.put("Name", key);
                dep.put("Type", "ReportQuery");
                dep.put("Step", "1");
                if (hasQuery)
                    dep.put("ReportKey", new ArrayList());
                reporter = getNewReport(dep);
                dep.clear();
                dep = null;
                if (reporter == null)
                    continue;
                try {
                    dep = reporter.generateReport(System.currentTimeMillis());
                }
                catch (Exception e) { // no report found or other exception
                    reporter.destroy();
                    reporter = null;
                    continue;
                }

                if (hasQuery) { // query from the report and return the result
                    if (dep != null && dep.containsKey((String) o)) {
                        key = (String) dep.get((String) o);
                        reporter.destroy();
                        reporter = null;
                        return key;
                    }
                    reporter.destroy();
                }
                else if(reporter.getSkippingStatus()==MonitorReport.NOSKIP){
                    reporter.destroy();
                    return (String) o;
                }
                else
                    reporter.destroy();
                reporter = null;
                dep = null;
            }
            return value;
        }
        else
            return null;
    }

    /**
     * It returns a Map with the same keys of IncludeGroup and values of List
     * that contains Sets for names of the primary includes. IncludeGroup is a
     * Map that specifies the components to be primarily included in a given
     * property Map with the name of basename. Its keys are either the basename
     * or a property name for either a map or a list. The corresponding values
     * are either a empty string or a list of names. It supports the following
     * three scenarios:
     *<br>
     * LIST/LIST: {"MonitorGroup": ["Monitor"]}<br>
     * LIST: {"Flow": ["Receiver", "Node", "Persister"]}<br>
     * MAP: {"ConfigRepository": ""}<br>
     *<br><br>
     * where Flow is the basename in this context.
     *<br><br>
     * The returned Map contains the same keys as those of IncludeGroup. But
     * the value for ecah key is a List of Set for names of the primary
     * includes. The index of the Set in the List is corresponding to the
     * index of the group instance that contains the primary includes.
     */
    public static Map<String, List> getIncludes(String basename,
        Map<String, Object> includeGroup, Map props) {
        List group, list, elements;
        Set<String> items;
        Map<String, List> includeMap;
        Object o;
        String item;
        int i, j, m, n;

        if (basename == null || includeGroup == null || props == null)
            return null;

        includeMap = new HashMap<String, List>();
        for (String key : includeGroup.keySet()) {
            List<Set> pl = new ArrayList<Set>();
            includeMap.put(key, pl);
            if (key.equals(basename)) { // for a list in base
                group = new ArrayList<Object>();
                group.add(props);
            }
            else if ((o = props.get(key)) != null && o instanceof List)
                group = (List) o;
            else if (o != null && o instanceof String) { // for a map in base
                items = new HashSet<String>();
                items.add((String) o);
                pl.add(items);
                continue;
            }
            else
                continue;

            if ((o = includeGroup.get(key)) == null || !(o instanceof List))
                continue;

            elements = (List) o;
            n = group.size();
            m = elements.size();
            for (i=0; i<n; i++) { // loop thru group instances
                items = new HashSet<String>();
                pl.add(items);
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
                        list = (List) o;
                    else if (o instanceof String && key.equals(basename)) {
                        list = new ArrayList();
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

        return includeMap;
    }

    /**
     * Given the names of primary includes, the policy map for secondary
     * includes, and the master property map, it looks for all the names of
     * secondary includes required by those the primarily included objects.
     * The policyMap defines what types of the primary objects need to be
     * examined for the secondary includes, via their classnames. It returns
     * a String Array with all the names of the secondary includes. In case of
     * failure, it returns an empty array.
     */
    public static String[] getSecondaryIncludes(String[] primaryItems,
        Map policyMap, Map props) {
        String key;
        Set<String> hSet;
        Map ph;
        Object o;

        if (primaryItems == null || primaryItems.length <= 0 ||
            policyMap == null || policyMap.size() <= 0 || props == null)
            return new String[0];

        hSet = new HashSet<String>();
        for (String item : primaryItems) { // loop thru the primary list
            if (item.length() <= 0)
                continue;
            o = props.get(item);
            if (o == null || !(o instanceof Map))
                continue;
            ph = (Map) o;
            if (!(ph.containsKey("ClassName")))
                continue;
            key = (String) ph.get("ClassName");
            if (!policyMap.containsKey(key)) // not a candidate
                continue;
            o = policyMap.get(key);
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = ph.get(key);
            if (o == null || !(o instanceof List))
                continue;
            for (Object obj : (List) o) { // loop thru list for 2nd include
                if (!(obj instanceof String))
                    continue;
                key = (String) obj;
                if ((o = ph.get(key)) != null && o instanceof Map) // included
                    continue;
                hSet.add(key);
            }
        }

        return hSet.toArray(new String[hSet.size()]);
    }

    /**
     * Given the names of primary includes, the policy map for secondary
     * includes, and the name of the configration folder, it looks for names
     * of all the secondary includes, required by the given primary includes.
     * The policyMap defines what types of the primary includes need to be
     * examined for the secondary includes, via their classnames. Since the
     * master property map is not provided, it tries to load the primary
     * includes from the specified folder to look up properties. If saxParser
     * is null, the config files are assumed to have the names with the
     * extension of ".json". Upon success, it returns a String Array with the
     * names of the all secondary includes. In case of failure, it returns
     * an empty array.
     */
    public static String[] getSecondaryIncludes(String[] primaryItems,
        String cfgDir, Map policyMap) {
        String key;
        Map ph;
        Set<String> hSet;
        Object o;
        File file;

        if (primaryItems == null || primaryItems.length <= 0 ||
            cfgDir == null || cfgDir.length() <= 0 ||
            policyMap == null || policyMap.size() <= 0)
            return new String[0];

        hSet = new HashSet<String>();
        for (String item : primaryItems) { // loop thru the primary list
            if (item.length() <= 0)
                continue;
            file = new File(cfgDir + FILE_SEPARATOR + item + ".json");
            ph = null;
            if (file.exists() && file.isFile() && file.canRead()) try {
                FileReader fr = new FileReader(file);
                ph = (Map) JSON2Map.parse(fr);
                fr.close();
            }
            catch (Exception e) {
                ph = null;
            }
            if (ph == null || !(ph.containsKey("ClassName")))
                continue;
            key = (String) ph.get("ClassName");
            if (!policyMap.containsKey(key)) // not a candidate
                continue;
            o = policyMap.get(key);
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = ph.get(key);
            if (o == null || !(o instanceof List))
                continue;
            for (Object obj : (List) o) { // loop thru list for 2nd include
                if (!(obj instanceof String))
                    continue;
                key = (String) obj;
                if ((o = ph.get(key)) != null && o instanceof Map) // included
                    continue;
                hSet.add(key);
            }
        }

        return hSet.toArray(new String[hSet.size()]);
    }

    /**
     * It returns a Map with all names secondarily included in a given
     * list of the primary objects.  The listName is the name of the list for
     * the primary objects that need to be included from the configuration
     * directory.  The policyMap defines what types of the primary objects
     * need to be examined for the secondary includes, via their classnames. The
     * group contains all the 2nd includes temporarily. The props contains all
     * the listed primary includes.  Upon return, all the 2nd includes will be
     * merged into their primary includes. The returned map contains the names
     * for merged 2nd includes in the keySet. The value for each key is the
     * name of the primary include.
     */
    public static Map<String, String> mergeSecondaryIncludes(String listName,
        Map policyMap, Map group, Map props) {
        int i, n;
        String item, key;
        List list;
        Map ph;
        Map<String, String> uniqueMap;
        Object o;

        if ((o = group.get(listName)) == null || !(o instanceof List))
            return null;
        list = (List) o;

        uniqueMap = new HashMap<String, String>();
        n = list.size();
        for (i=0; i<n; ++i) {
            item = (String) list.get(i);
            if (item == null || item.length() <= 0)
                continue;
            if ((o = props.get(item)) == null || !(o instanceof Map))
                continue;
            ph = (Map) o;
            if (ph == null || !(ph.containsKey("ClassName")))
                continue;
            key = (String) ph.get("ClassName");
            if (!policyMap.containsKey(key)) // not a candidate
                continue;
            o = policyMap.get(key);
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            o = ph.get(key);
            if (o == null || !(o instanceof List))
                continue;
            for (Object obj : (List) o) { // look for 2nd includes
                if (!(obj instanceof String))
                    continue;
                key = (String) obj;
                if ((o = ph.get(key)) != null && o instanceof Map) // merged
                    continue;
                // 2nd include is supposed to be pre-defined in group
                if ((o = group.get(key)) == null || !(o instanceof Map))
                    continue;
                ph.put(key, o);
                uniqueMap.put(key, item);
            }
        }

        return uniqueMap;
    }

    /** returns the private report of current thread from default container */
    public static Map getPrivateReport() {
        if (getPrivateReport == null) try {
            initStaticMethod("getPrivateReport");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "getPrivateReport from MonitorAgent: " + Event.traceStack(e)));
        }
        try {
            return (Map) getPrivateReport.invoke(null);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to get private report: "+
                Event.traceStack(e)));
        }
    }

    /** returns the report map of the name from the default container */
    public static Map getReport(String name) {
        if (getReport == null) try {
            initStaticMethod("getReport");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "getReport from MonitorAgent: " + Event.traceStack(e)));
        }
        try {
            return (Map) getReport.invoke(null, new Object[]{name});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to get report for " +
                name + ": " + Event.traceStack(e)));
        }
    }

    /** initializes the report of the name in the default container */
    public static void initReport(String name, long tm, int status, int skip) {
        if (initReport == null) try {
            initStaticMethod("initReport");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
                "initReport from MonitorAgent: " + Event.traceStack(e)));
        }
        try {
            initReport.invoke(null, new Object[]{name, new Long(tm),
                new Integer(status), new Integer(skip)});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init report for " +
                name + ": " + Event.traceStack(e)));
        }
    }

    /** initializes the report of the name in the default container */
    public static void initReport(String name, String[] keys, Map report,
        int mode) {
        if (initReportWithKeys == null) try {
            initStaticMethod("initReportWithKeys");
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init method of "+
               "initReport with keys from MonitorAgent: "+Event.traceStack(e)));
        }
        try {
            initReportWithKeys.invoke(null, new Object[]{name, keys, report,
                new Integer(mode)});
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to init report for " +
                name + ": " + Event.traceStack(e)));
        }
    }

    /**
     * It returns the Greatest Common Divisor for a given integer array
     * the input arrary should not contain elements of zero or negative values
     */
    private static int gcd(int[] in) {
        boolean run = true;
        int hmax;
        int i, j, p, gcd = 1;
        int[] h = new int[in.length];
        for (i=0; i<h.length; i++)
            h[i] = in[i];
        Arrays.sort(h);
        hmax = h[h.length - 1];
        if (h[0] <= 0)
            return 0;
        for (i=0; i<prime.length; i++) {
            p = prime[i];
            if (p < hmax)
                run = true;
            else
                run = false;
            while (run) {
                for (j=0; j<h.length; j++) {
                    if (h[j] == 1) {
                        run = false;
                        break;
                    }
                    if ((h[j] % p) > 0)
                        break;
                }
                if (j < h.length)
                    break;
                gcd *= p;
                for (j=0; j<h.length; j++) {
                    h[j] /= p;
                }
            }
            if (!run)
                break;
        }
        return gcd;
    }

    /**
     * It generates the heartbeat array from the input array.
     */
    public static int[] getHeartbeat(int[] group) {
        int i, j, k, m, n, gcd, lcm;
        int[] h, hbeat;
        n = group.length;
        h = new int[n];
        for (i=0; i<n; i++) {
            h[i] = group[i] / 1000;
        }

        Arrays.sort(h);
        k = 0;
        for (i=1; i<n; i++) {
            if (h[i] > h[k])
                h[++k] = h[i];
        }
        k++;

        hbeat = new int[k];
        for (i=0; i<k; i++)
            hbeat[i] = h[i];
        gcd = gcd(hbeat);
        if (gcd <= 0)
            return null;

        lcm = 1;
        for (i=0; i<k; i++) {
            h[i] /= gcd;
            lcm *= h[i];
        }
        n = 0;
        for (i=0; i<k; i++)
            n += lcm / h[i];
        hbeat = new int[n];
        n = 0;
        for (i=0; i<k; i++) {
            m = lcm / h[i];
            for (j=0; j<m; j++) {
                hbeat[n++] = (j+1) * h[i];
            }
        }
        Arrays.sort(hbeat);
        k = 0;
        for (i=1; i<n; i++) {
            if (hbeat[i] > hbeat[k])
                hbeat[++k] = hbeat[i];
        }
        k++;
        h = new int[k];
        for (i=0; i<k; i++) {
            h[i] = hbeat[i] * gcd * 1000;
        }

        return h;
    }

    /** initializes static methods from the default container */
    private synchronized static void initStaticMethod(String name)
        throws ClassNotFoundException, NoSuchMethodException {
        String className = "org.qbroker.flow.MonitorAgent";
        if ("getPrivateReport".equals(name) && getPrivateReport == null) {
            Class<?> cls = Class.forName(className);
            getPrivateReport = cls.getMethod(name, new Class[]{});
        }
        else if ("getReport".equals(name) && getReport == null) {
            Class<?> cls = Class.forName(className);
            getReport = cls.getMethod(name, new Class[]{String.class});
        }
        else if ("initReport".equals(name) && initReport == null) {
            Class<?> cls = Class.forName(className);
            initReport = cls.getMethod(name, new Class[]{String.class,
                long.class, int.class, int.class});
        }
        else if ("initReportWithKeys".equals(name) &&
            initReportWithKeys == null) {
            Class<?> cls = Class.forName(className);
            initReportWithKeys = cls.getMethod(name, new Class[]{String.class,
                String[].class, Map.class, int.class});
        }
        else
            throw(new IllegalArgumentException("no such method of " + name +
                  " defined in " + className));
    }

    /**
     * It returns a list of maps with each map contains three objects of
     * Pattern, TextSubstitution and Template. The list is for mapping a name
     * into a new name with the hit pattern to select the substitution and/or
     * the template.
     */
    public static List<Map> getGenericMapList(List list, Perl5Compiler pc,
        Template template) throws MalformedPatternException {
        String patternStr, expStr;
        Map<String, Object> ph;
        List<Map> mapList = new ArrayList<Map>();

        if (list != null) for (Object obj : list) {
            if (!(obj instanceof Map))
                continue;
            Map map = (Map) obj;
            patternStr = select(map.get("Pattern"));
            if (patternStr == null || patternStr.length() <= 0)
                continue;
            patternStr = substitute(patternStr, template);
            ph = new HashMap<String, Object>();
            ph.put("Pattern", pc.compile(patternStr));
            expStr = select(map.get("Substitution"));
            if (expStr != null && expStr.length() > 0) {
                expStr = substitute(expStr, template);
                ph.put("Substitution", new TextSubstitution(patternStr,expStr));
            }
            expStr = select(map.get("Template"));
            if (expStr != null && expStr.length() > 0) {
                expStr = substitute(expStr, template);
                ph.put("Template", new Template(patternStr, expStr));
            }
            mapList.add(ph);
        }

        return mapList;
    }

    /**
     * Based on a list of generic name mapping rules, it trys to match the 
     * given name with the pattern to select the substition. Once it finds
     * a match, the name will be transformed with the substition rule.
     * The result will be returned as the new name. Otherwise, the original
     * name will be returned.
     */
    public static String getMappedName(String name, List<Map> mapList,
        Perl5Matcher pm) {
        Pattern pattern;
        TextSubstitution tsub;

        if (mapList == null || mapList.isEmpty() || name == null ||
            name.length() <= 0)
            return name;

        for (Map map : mapList) {
            pattern = (Pattern) map.get("Pattern");
            if (pattern == null)
                continue;
            if (!pm.contains(name, pattern))
                continue;
            tsub = (TextSubstitution) map.get("Substitution");
            if (tsub == null)
                return name;
            else
                return tsub.substitute(name);
        }

        return name;
    }

    /**
     * Based on a list of generic name mapping rules, it trys to match the 
     * given name with the pattern to select the template. Once it finds
     * a match, the template will be returned. Otherwise, it returns null.
     */
    public static Template getMappedTemplate(String name, List<Map> mapList,
        Perl5Matcher pm) {
        Pattern pattern;

        if (mapList == null || mapList.isEmpty() || name == null ||
            name.length() <= 0)
            return null;

        for (Map map : mapList) {
            pattern = (Pattern) map.get("Pattern");
            if (pattern == null)
                continue;
            if (!pm.contains(name, pattern))
                continue;
            return (Template) map.get("Template");
        }

        return null;
    }
}
