package org.qbroker.monitor;

/* PropertyMonitor.java - a monitor watching property's change */

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.json.JSON2Map;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.WebTester;

/**
 * PropertyMonitor monitors a given set of properties on their last modified
 * time and content.  If its mtime is newer, it will load the content and
 * converts it into a property map and stores it to the report.  The action
 * part will compare the new property to the existing property to get changes.
 * If there is any change, the map with changes will replace the new property
 * map in the report. Meanwhile, if PropertyFile is defined, it will also
 * update the local files. If there is no change at all, the value of the new
 * property map stored in the report will be reset to null to indicate no
 * change.
 *<br/><br/>
 * Use PropertyMonitor to get the update time of a given set of properties.
 * The properties can be stored in a file, a web page or a database table.
 * It is assumed there is a timestamp for each set of properties. If the
 * timestamp is newer, the downloaded property map will be stored in the
 * report with the key of "Property".
 *<br/><br/>
 * You can use it to monitor changes on properties of an application.  If the
 * properties have been updated, performAction() will compare the new set of
 * properties to the existing ones and returns the change map via the latest
 * report map under the key of "Property". The change map is a map with
 * only modified or new properties. The deleted item will have null as the
 * value. If there is no change, it will be set to null. Otherwise, you can
 * retrieve the change map from the report. With this property map, you can
 * reload the objects directly.
 *<br/><br/>
 * PropertyMonitor is able to include properties defined outside the master
 * file.  IncludeGroup is a map specifying all the components to be
 * loaded from outside of the master file.  It supports three scenarios:<br/>
 * LIST/LIST: {"MonitorGroup": ["Monitor"]}<br/>
 * LIST: {"Flow": ["Reporter"]}<br/>
 * MAP:  {"ConfigRepository":""}<br/>
 * where Flow is the name of the master property file.
 *<br/><br/>
 * PropertyMonitor also supports secondary includes on certain type of primary
 * included components.  It means those components may contain listed objects
 * defined outside of their configuration files.  SecondaryInclude is a map
 * specifying what lists of primary includes may contain secondary includes.
 * Any key of SecondaryInclude has to be one of the values of IncludeGroup.
 * For example:<br/>
 * {"Node": "IncludePolicy"}<br/>
 * where Node is the name of the list with primary includes.  IncludePolicy is
 * the name of the map specifying what classnames of Node may have secondary
 * includes in their lists specified by the values.  For example:<br/>
 * {"org.qbroker.node.EventMonitor": "Ruleset"}<br/>
 * where Ruleset is the name of the list that may contain secondary includes.
 * IncludePolicy should be defined in the master configuration for that Node
 * object.
 *<br/><br/>
 * If DataField is defined, PropertyMonitor will use it to retrieve a list from
 * the property map. Then those objects not referenced by the list will be
 * removed for the support of projections.
 *<br/><br/>
 * Currently, it only supports web based or local JSON properties. Since it is
 * always instantiated before the static reports, it can not use any of the
 * global variables defined in the static reports. However, it does support
 * the special variables loaded from a set of environment variables. But they
 * are limited to URI and authentication stuff. Those environment variables
 * have the higher precedence over the inline properties.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class PropertyMonitor extends Monitor {
    private Map property = null;
    private Map<String, Object> baseMap, includeGroup, includeMap;
    private Map<String, List> secondaryMap;
    private String uri, basename, configDir;
    private String dataField = null;
    private File propertyFile = null;
    private WebTester webTester;
    private File file;
    private Pattern pattern;
    private boolean isRemote;
    private long gmtOffset = 0L, timeDifference, previousTime;
    private int webStatusOffset, debug = 0;
    private SimpleDateFormat dateFormat;
    private String[] ignoredFields;
    private final static String webStatusText[] = {"Exception",
        "Test OK", "Protocol error", "Pattern not matched",
        "Not Multipart", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Server is down"};
    public final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    public PropertyMonitor(Map props) {
        super(props);
        String filename = null;
        Object o;
        Map<String, Object> ph = new HashMap<String, Object>();
        URI u;
        int n;

        if (type == null)
            type = "PropertyMonitor";

        if (description == null)
            description = "monitor changes on JSON properties";

        if ((o = props.get("EnvironmentVariable")) != null && o instanceof Map){
            String str, key;
            Map pm = (Map) o;
            Map<String, String> map = System.getenv();
            for (Object obj : pm.keySet()) {
                if (obj == null || !(obj instanceof String))
                    continue;
                key = (String) obj;
                if (key.length() <= 0)
                    continue;
                if ((str = map.get(key)) == null)
                    continue;
                if ((key = (String) pm.get(key)) != null && key.length() > 0) 
                    ph.put(key, str);
            }
        }

        if ((o = ph.get("URI")) != null) // env has higher precedence
            uri = (String) o;
        else if ((o = MonitorUtils.select(props.get("URI"))) != null)
            uri = MonitorUtils.substitute((String) o, template);
        else
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ("http".equals(u.getScheme()) || "https".equals(u.getScheme())) {
            boolean hasEV = false;
            isRemote = true;
            if ((o = ph.get("BasicAuthorization")) != null)
                hasEV = true;
            else if ((o = ph.get("AuthString")) != null)
                hasEV = true;
            else if ((o = props.get("Username")) != null) {
                if ((o = ph.get("Password")) != null)
                    hasEV = true;
                else if ((o = ph.get("EncryptedPassword")) != null)
                    hasEV = true;
                else if ((o = props.get("Password")) != null)
                    ph.put("Password", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedPassword")) != null)
                    ph.put("EncryptedPassword", MonitorUtils.select(o));
            }
            else if ((o = ph.get("Password")) != null) {
                if ((o = props.get("Username")) != null)
                    ph.put("Username", MonitorUtils.select(o));
            }
            else if ((o = ph.get("EncryptedPassword")) != null) {
                if ((o = props.get("Username")) != null)
                    ph.put("Username", MonitorUtils.select(o));
            }
            else if ((o = props.get("BasicAuthorization")) != null) {
                ph.put("BasicAuthorization", MonitorUtils.select(o));
            }
            else if ((o = props.get("AuthString")) != null) {
                ph.put("AuthString", MonitorUtils.select(o));
            }
            else if ((o = props.get("Username")) != null) {
                ph.put("Username", MonitorUtils.select(o));
                if ((o = props.get("Password")) != null)
                    ph.put("Password", MonitorUtils.select(o));
                else if ((o = props.get("EncryptedPassword")) != null)
                    ph.put("EncryptedPassword", MonitorUtils.select(o));
            }

            if ((o = props.get("Request")) != null)
                ph.put("Request", MonitorUtils.select(o));
            else if ((o = props.get("Operation")) != null)
                ph.put("Operation", MonitorUtils.select(o));

            if ((o = props.get("Accept")) != null)
                ph.put("Accept", (String) o);

            if ((o = props.get("ContentType")) != null)
                ph.put("ContentType", (String) o);

            if ((o = props.get("Data")) != null)
                ph.put("Data", (String) o);

            ph.put("Name", name);
            ph.put("URI", uri);
            ph.put("Step", "1");
            ph.put("Timeout", (String) props.get("Timeout"));
            if ((o = props.get("MaxBytes")) != null)
                ph.put("MaxBytes", (String) o);
            else
                ph.put("MaxBytes", "0");

            if ((o = props.get("TrustAllCertificates")) != null)
                ph.put("TrustAllCertificates", (String) o);
            if ((o = props.get("WebDebug")) != null)
                ph.put("Debug", (String) o);
            webTester = new WebTester(ph);

            gmtOffset = Calendar.getInstance().get(Calendar.ZONE_OFFSET) +
                Calendar.getInstance().get(Calendar.DST_OFFSET);

            if ((o = MonitorUtils.select(props.get("DateFormat"))) == null)
          throw(new IllegalArgumentException("DateFormat is not well defined"));
            dateFormat = new SimpleDateFormat((String) o);

            if ((o = MonitorUtils.select(props.get("Pattern"))) == null)
            throw(new IllegalArgumentException("Pattern is not well defined"));

            try {
                Perl5Compiler pc = new Perl5Compiler();
                pm = new Perl5Matcher();
                pattern = pc.compile((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(e.toString()));
            }
        }
        else if ("file".equals(u.getScheme())) {
            String fileName = u.getPath();
            isRemote = false;
            if (fileName == null || fileName.length() == 0)
                throw(new IllegalArgumentException("URI has no path: "+uri));
            try {
                file = new File(Utils.decode(fileName));
            }
            catch (Exception e) {
             throw(new IllegalArgumentException("failed to decode: "+fileName));
            }
        }
        else
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = MonitorUtils.select(props.get("Basename"))) == null ||
            (basename = (String) o)  == null)
            throw(new IllegalArgumentException("basename is not well defined"));

        if ((o = props.get("IgnoredField")) != null && o instanceof List) {
            List list = (List) o;
            ignoredFields = new String[list.size()];
            for (int i=0; i<ignoredFields.length; i++)
                ignoredFields[i] = (String) list.get(i);
        }
        else
            ignoredFields = new String[0];

        if ((o = props.get("IncludeGroup")) != null && o instanceof Map)
            includeGroup = Utils.cloneProperties((Map) o);
        else
            includeGroup = new HashMap<String, Object>();

        if ((o = props.get("SecondaryInclude")) != null && o instanceof Map)
            includeMap = Utils.cloneProperties((Map) o);
        else
            includeMap = new HashMap<String, Object>();
        secondaryMap = new HashMap<String, List>();

        if ((o = MonitorUtils.select(props.get("PropertyFile"))) != null) {
            if (!((String) o).endsWith(".json"))
        throw(new IllegalArgumentException("PropertyFile is not well defined"));
            propertyFile = new File((String) o);
        }
        else
            propertyFile = null;

        try {
            if (property == null && propertyFile != null &&
                propertyFile.canRead()) {
                FileReader fr = new FileReader(propertyFile);
                property = (Map) JSON2Map.parse(fr);
                fr.close();
            }
            else if (property == null) {
                property = new HashMap<String, Object>();
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(propertyFile+": "+e.toString()));
        }

        baseMap = new HashMap<String, Object>();
        previousTime = -1L;
        webStatusOffset = 0 - webTester.TESTFAILED;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        Map props = null;
        String response = null;
        long size = -1L;
        long mtime = -10L;
        int returnCode = -1;
        report.put("Properties", null);
        report.clear();

        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                return report;
            }
            else {
                skip = NOSKIP;
                serialNumber ++;
            }
        }
        else {
            skip = NOSKIP;
            serialNumber ++;
        }

        if (dependencyGroup != null) { // check dependency
            skip = MonitorUtils.checkDependencies(currentTime, dependencyGroup,
                name);
            if (skip != NOSKIP) {
                if (skip == EXCEPTION) {
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                    return report;
                }
                else if (skip == SKIPPED)
                    return report;
                else if (!disabledWithReport)
                    return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        report.put("URI", uri);

        // check mtime of the uri
        if (isRemote) {
            Object o;
            Map<String, Object> r = webTester.generateReport(currentTime);
            if ((o = r.get("ReturnCode")) != null) {
                returnCode = Integer.parseInt((String) o);
            }
            else {
                returnCode = -1;
            }
            if (returnCode != 0) { // failed and retry once
                try {
                    Thread.sleep(500);
                }
                catch (Exception e) {
                }
                r = webTester.generateReport(currentTime);
                if ((o = r.get("ReturnCode")) != null) {
                    returnCode = Integer.parseInt((String) o);
                }
                else {
                    throw(new IOException("web test failed on " + uri));
                }
            }

            if (returnCode == 0) { // got the web page
                int n;
                response = webTester.getContent();
                if (pm.contains(response, pattern)) {
                    StringBuffer strBuf = new StringBuffer();
                    MatchResult mr = pm.getMatch();
                    char c;
                    n = mr.groups() - 1;
                    for (int i=1; i<=n; i++) {
                        if (i > 1)
                            strBuf.append(" ");
                        if ((mr.group(i)).length() == 1) // hack on a.m./p.m.
                            c = (mr.group(i)).charAt(0);
                        else
                            c = 'F';
                        if (c == 'a' || c == 'A' || c == 'p' || c == 'P') {
                            strBuf.append(c);
                            strBuf.append('M');
                        }
                        else
                            strBuf.append(mr.group(i));
                    }
                    Date date = dateFormat.parse(strBuf.toString(),
                        new ParsePosition(0));
                    if (date != null)
                        mtime = date.getTime() - timeDifference;
                    if (mtime + timeDifference < 0L)
                        throw(new IOException("failed to parse mtime: " +
                            strBuf.toString()));
                }
                else {
                    throw(new IOException("failed to match mtime: " +response));
                }
                if ((size = (long) response.length()) <= 0)
                    throw(new IOException("failed to get size: " + size));

                report.put("MTime", String.valueOf(mtime));
                report.put("Content", response);
            }
            else {
                throw(new IOException("web test failed on " + uri +
                    ": " + returnCode));
            }

            if (mtime <= previousTime) // not updated
                return report;

            StringReader in = new StringReader(response);
            props = (Map) JSON2Map.parse(in);
            in.close();

            if (props == null)
                throw(new IOException("failed to get object from "+ uri));

            merge(props);
        }
        else { // local file
            if (file.exists() && file.canRead()) {
                mtime = file.lastModified() - timeDifference;
                size = file.length();
            }
            else {
                throw(new IOException(uri + " not found"));
            }

            if (mtime <= previousTime) // not updated
                return report;

            FileReader fr = new FileReader(file);
            props = (Map) JSON2Map.parse(fr);
            fr.close();

            String dir = file.getParent();
            List<Object> group, list;
            List elements;
            Map g;
            Object o;
            Iterator iter;
            String item;
            int i, j, k, m, n;
            for (String key : includeGroup.keySet()) {
                if (key.equals(basename)) { // for array or hash in basename
                    group = new ArrayList<Object>();
                    group.add(props);
                }
                else if ((o = props.get(key)) != null && o instanceof List)
                    group = Utils.cloneProperties((List) o);
                else
                    continue;

                if ((o = includeGroup.get(key)) == null || !(o instanceof List))
                    continue;

                elements = (List) o;
                m = elements.size();
                for (i=0; i<group.size(); i++) { // loop thru groups
                    if ((o = group.get(i)) == null || !(o instanceof Map))
                        continue;
                    g = (Map) o;
                    for (j=0; j<m; j++) { // loop thru elements
                        if ((o = elements.get(j)) == null ||
                            !(o instanceof String))
                            continue;

                        item = (String) o;
                        if ((o = g.get(item)) == null || o instanceof Map)
                            continue;

                        if (o instanceof List)
                            list = Utils.cloneProperties((List) o);
                        else if (o instanceof String && key.equals(basename)) {
                            list = new ArrayList<Object>();
                            list.add(item);
                        }
                        else
                            continue;

                        n = list.size();
                        for (k=0; k<n; k++) { // loop thru components
                            o = list.get(k);
                            if (o == null || !(o instanceof String ||
                                o instanceof Map))
                                continue;
                            if (o instanceof Map) // for generated item
                                item = (String) ((Map) o).get("Name");
                            else
                                item = (String) o;
                            if ((o = props.get(item)) != null &&
                                o instanceof Map)
                                continue;
                            File f = new File(dir+FILE_SEPARATOR+item+".json");
                            if (!f.exists() || !f.isFile() || !f.canRead())
                                throw(new IOException("failed to open " +
                                    f.getPath()));
                            fr = new FileReader(f);
                            o = (Map) JSON2Map.parse(fr);
                            fr.close();
                            props.put(item, o);
                        }
                    }
                }
            }
        }

        if (props == null)
            throw(new IOException("empty object of "+ basename + " on " +uri));

        for (int i=0; i<ignoredFields.length; i++)
            if (props.containsKey(ignoredFields[i]))
                props.remove(ignoredFields[i]);

        // if dataField is defined, make the projections first
        if (dataField != null && dataField.length() > 0)
            Utils.projectProps(dataField, props);

        if (property == null)
            property = props;

        report.put("Properties", props);

        props = null;

        return report;
    }

    /**
     * It checks and merges the secondary includes into the primary includes
     */
    private void merge(Map props) {
        Object o;
        String str;
        String[] keys;
        int i, j, k, n, size;
        Map group;
        Map<String, String> uniqueMap, map;
        List list;

        secondaryMap.clear();
        if (includeMap == null || includeMap.size() <= 0)
            return;

        for (String key : includeGroup.keySet()) {
            if ((o = includeGroup.get(key)) == null || !(o instanceof List))
                continue;

            uniqueMap = new HashMap<String, String>();
            list = (List) o;
            size = list.size();
            for (i=0; i<size; i++) { // look for components with 2nd include
                if ((o = list.get(i)) == null || !(o instanceof String))
                    continue;
                str = (String) o;
                if ((o = includeMap.get(str)) != null && o instanceof String)
                    uniqueMap.put(str, key);
            }
            n = uniqueMap.size();
            if (n <= 0) // no 2nd includes
                continue;
            keys = uniqueMap.keySet().toArray(new String[n]);
            // assumes only one item with 2nd includes
            if (keys[0] == null || keys[0].length() <= 0 ||
                (o = includeMap.get(keys[0])) == null)
                continue;

            // str is the value for the only key of SecondaryInclude map
            str = (String) o;

            List<Object> pl;
            if (key.equals(basename)) { // fake the master for primary includes
                pl = new ArrayList<Object>();
                pl.add(props);
                o = pl;
            }
            else if ((o = props.get(key)) == null || !(o instanceof List))
                continue;

            list = (List) o;
            size = list.size();
            pl = new ArrayList<Object>();

            // secondaryMap stores a list of maps at each includeGroup key
            // where the keys of each map are the names of 2nd includes
            // and the values are the corresponding primary includes
            secondaryMap.put(key, pl);
            for (i=0; i<size; i++) { // loop thru group instances
                pl.add(null);
                if ((o = list.get(i)) == null || !(o instanceof Map))
                    continue;
                // group is a master config map that contains primary includes
                group = (Map) o;

                // check if secondary include policy is defined in the master
                if ((o = group.get(str)) == null || !(o instanceof Map))
                    continue;

                uniqueMap = new HashMap<String, String>();
                pl.set(i, uniqueMap);
                for (j=0; j<n; j++) { // loop thru components to merge 2nd incls
                    map = MonitorUtils.mergeSecondaryIncludes(keys[j],
                        (Map) o, group, props);

                    // update the unique map for 2nd includes
                    for (String item : map.keySet()) {
                        if (item.length() <= 0)
                            continue;
                        if (uniqueMap.containsKey(item)) // duplicates
                            continue;
                        uniqueMap.put(item, map.get(item));
                        // remove the 2nd includes from group container
                        group.remove(item);
                    }
                }
            }
        }
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long mtime;
        String response;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        Map props;

        if ((o = latest.get("MTime")) != null && o instanceof String)
            mtime = Long.parseLong((String) o);
        else
            mtime = -1L;

        if ((o = latest.get("Content")) != null && o instanceof String)
            response = (String) o;
        else
            response = null;

        o = latest.remove("Properties");
        props = (o != null && o instanceof Map) ? (Map) o : null;

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                exceptionCount = 0;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(uri);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.NORMAL:
            level = Event.INFO;
            exceptionCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append(uri);
            strBuf.append(" has been updated recently");
            if (props != null) {
                Map<String, Object> change =
                    Utils.diff(basename, includeGroup, property, props);
                if (change == null || change.size() <= 0) { // no changes at all
                    change = null;
                    actionCount ++;
                    strBuf.append(" but without any changes");
                }
                else if (propertyFile != null) { // update files
                    List list;
                    baseMap.clear();
                    for (String key : includeGroup.keySet()) {
                        // store all map in base
                        if (!key.equals(basename))
                            continue;
                        if ((o = includeGroup.get(key)) == null &&
                            !(o instanceof List))
                            continue;
                        list = (List) o;
                        for (int i=0; i<list.size(); i++) { //loop on components
                            o = list.get(i);
                            if (o == null || !(o instanceof String ||
                                o instanceof Map))
                                continue;
                            if (o instanceof Map) // for generated items
                                key = (String) ((Map) o).get("Name");
                            else
                                key = (String) o;
                            if ((o = props.get(key)) != null &&
                                o instanceof Map && !baseMap.containsKey(key))
                                baseMap.put(key, o);
                        }
                    }
                    if (isRemote)
                        persistJSON(mtime, response, change);
                    else
                        copy(mtime, change);
                    actionCount = 0;
                    strBuf.append(" with changes");
                    property = props;
                    baseMap.clear();
                }
                else { // no file updated
                    actionCount = 0;
                    strBuf.append(" with changes");
                    property = props;
                }
                previousTime = mtime;
                // reset properties
                latest.put("Properties", change);
            }
            else { // no time difference
                actionCount ++;
                strBuf.append(" with no differences");
            }
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            if ((o = latest.get("Exception")) != null)
                strBuf.append(Event.traceStack((Exception) o));
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            if ((o = latest.get("Exception")) != null)
                strBuf.append(Event.traceStack((Exception) o));
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append(uri);
            strBuf.append(" is not being checked due to blackout");
            break;
          default: // should never reach here
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
          default:
            if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
        }
        previousStatus = status;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        if (mtime >= 0)
            event.setAttribute("lastModified",
                Event.dateFormat(new Date(mtime)));
        if (response != null)
            event.setAttribute("size", String.valueOf(response.length()));

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        event.setAttribute("actionScript", actionStatus);
        event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public int getDebugMode() {
        return debug;
    }

    /** sets the dataField for projection support */
    public void setDataField(String key) {
        dataField = key;

        // if dataField is defined, make the projections first
        if (dataField != null && dataField.length() > 0)
            Utils.projectProps(dataField, property);
    }

    public void setOriginalProperty(Map props) {
        property = Utils.cloneProperties(props);

        // if dataField is defined, make the projections first
        if (dataField != null && dataField.length() > 0)
            Utils.projectProps(dataField, property);
    }

    /**
     * copies local files due to changes
     */
    private void copy(long mtime, Map change) {
        String key, value, from, to;
        int bytesRead, bufferSize = 4096;
        byte[] buffer;
        if (propertyFile == null || file == null || change == null ||
            change.size() <= 0)
            return;
        from = file.getParent();
        to = propertyFile.getParent();
        if (from.equals(to))
            return;
        buffer = new byte[bufferSize];
        for (Iterator iter=change.keySet().iterator(); iter.hasNext();) {
            key = (String) iter.next();
            if (key == null)
                continue;

            if (change.get(key) == null) try { // item should be deleted
                File file = new File(to + FILE_SEPARATOR + key + ".json");
                file.delete();
                continue;
            }
            catch (Exception e) {
                continue;
            }

            if (key.equals(basename)) {
                value = propertyFile.getPath();
                key = file.getPath();
            }
            else {
                value = to + FILE_SEPARATOR + key + ".json";
                key = from + FILE_SEPARATOR + key + ".json";
            }
            try {
                FileInputStream in = new FileInputStream(key);
                FileOutputStream out = new FileOutputStream(value, false);
                bytesRead = 0;
                while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                    if (bytesRead > 0)
                        out.write(buffer, 0, bytesRead);
                    else try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException ex) {
                    }
                }
                out.close();
                in.close();
            }
            catch (IOException e) {
                new Event(Event.ERR, name + ": failed to copy to "+
                    value + ": " + e.toString()).send();
                continue;
            }
        }
        if (mtime > 0)
            propertyFile.setLastModified(mtime);
    }

    /**
     * persists changes in JSON downloaded from repository to local files
     */
    private void persistJSON(long mtime, String content, Map change) {
        Object o;
        String key, dir, master, text;
        int[] positions;
        int i, j, m, n;
        if (propertyFile == null || content == null || change == null ||
            change.size() <= 0)
            return;
        dir = propertyFile.getParent();
        master = propertyFile.getPath();
        for (Iterator iter = change.keySet().iterator(); iter.hasNext();) {
            key = (String) iter.next();
            if (key == null || key.equals(basename))
                continue;

            if (change.get(key) == null) try { // item should be deleted
                File file = new File(dir + FILE_SEPARATOR + key + ".json");
                file.delete();
                if ((debug & Service.DEBUG_UPDT) > 0)
                    new Event(Event.DEBUG, name + " deleted " + key).send();
                continue;
            }
            catch (Exception e) {
                continue;
            }

            // persists either new or changed components
            text = getContentJSON(0, key, content);
            if (text != null && text.length() > 0) {
                key = dir + FILE_SEPARATOR + key + ".json";
                try {
                    saveToFile(key, text);
                    if ((debug & Service.DEBUG_UPDT) > 0)
                        new Event(Event.DEBUG, name + " updated " + key).send();
                }
                catch (IOException e) {
                    new Event(Event.ERR, name + ": failed to write to "+
                        key + ": " + e.toString()).send();
                }
            }
            else
                new Event(Event.ERR, name + ": failed to retrieve content for "+
                    key).send();
        }

        // secondaryMap stores a list of maps at each includeGroup key
        // where the keys of each map are the names of 2nd includes
        // and the values are the corresponding primary includes
        for (String ky : secondaryMap.keySet()) { // for secondary includes
            List list = secondaryMap.get(ky);
            if (list == null)
                continue;

            n = list.size();
            positions = null;
            if (!ky.equals(basename)) {
                // scan for end positions for ky and its children
                positions = locateJSON(ky, content);
                if (positions.length != n + 2) {
                    new Event(Event.ERR, name +
                        ": failed to locate the end positions for "+ ky + ": "+
                        positions.length + "/" + n).send();
                    continue;
                }
            }
            for (i=n-1; i>=0; i--) { // loop thru each child of ky
                if ((o = list.get(i)) == null || !(o instanceof Map))
                    continue;

                // keys of the map are names of 2nd includes, values of the
                // map are names of the container with the 2nd includes
                Map uniqueMap = (Map) o;
                for (Iterator r = uniqueMap.keySet().iterator(); r.hasNext();) {
                    String item = (String) r.next();
                    o = uniqueMap.get(item);
                    if (!change.containsKey((String) o)) // parent has no change
                        continue;

                    if (ky.equals(basename))
                        text = getContentJSON(0, item, content);
                    else { // for the i-th child of ky
                        int k = positions[i];
                        j = content.indexOf(",\"" + item + "\":", k+1);
                        if (j > k) { // found the name
                            k = JSON2Map.locate(3, content, j);
                            j = content.indexOf("{", j);
                            if (j > 0 && k > j && k < positions[i + 1])
                                text = content.substring(j, k+1);
                            else
                                text = "";
                        }
                        else
                            text = null;
                    }
                    if (text != null && text.length() > 0) {
                        key = dir + FILE_SEPARATOR + item + ".json";
                        try { // persist changes
                            saveToFile(key, text);
                            if ((debug & Service.DEBUG_UPDT) > 0)
                                new Event(Event.DEBUG, name + " updated " +
                                    item + " for " + (String) o).send();
                        }
                        catch (IOException e) {
                            new Event(Event.ERR, name + ": failed to write to "+
                                key + ": "+e.toString()).send();
                        }
                    }
                    else
                        new Event(Event.ERR, name +
                            ": failed to retrieve content for "+ item).send();
                }
            }
        }

        // cleanup content for master if it has changed
        if ((o = change.get(basename)) != null) {
            Set<String> hSet;
            text = content;

            // clean up primary includes for the new master config
            String [] items = Utils.getIncludes(basename, includeGroup, (Map)o);
            text = cutContent(",\"", "\":", "}", items, text, 0);
 
            // get rid of the ignored field
            if (ignoredFields.length > 0 && ignoredFields[0] != null) {
                i = text.indexOf(" \"" + ignoredFields[0] + "\":");
                j = (i > 0) ? text.indexOf("\",", i) : 0;
                if (i > 0 && j > i)
                    text = text.substring(0, i) + text.substring(j+2);
            }

            // clean up secondary includes
            for (String ky : secondaryMap.keySet()) {
                List list = secondaryMap.get(ky);
                if (list == null)
                    continue;

                n = list.size();
                positions = null;
                if (!ky.equals(basename)) {
                    // scan for end positions for ky and its children
                    positions = locateJSON(ky, text);
                    if (positions.length != n + 2) {
                        new Event(Event.ERR, name +
                            ": failed to locate the end positions for "+ ky +
                            ": "+ positions.length + "/" + n).send();
                        continue;
                    }
                }
                for (i=n-1; i>=0; i--) { // loop thru each child of ky
                    if ((o = list.get(i)) == null || !(o instanceof Map))
                        continue;

                    // keys of the map are names of 2nd includes, values of the
                    // map are names of the container with the 2nd includes
                    Map uniqueMap = (Map) o;
                    hSet = new HashSet<String>();
                    for (Object obj : uniqueMap.keySet())
                        hSet.add((String) obj);
                    m = hSet.size();
                    if (m <= 0)
                        continue;
                    if (ky.equals(basename)) {
                        text = cutContent(",\"", "\":", "}",
                            hSet.toArray(new String[m]), text, 0);
                    }
                    else { // for i-th child of ky
                        text = cutContent(",\"", "\":", null,
                            hSet.toArray(new String[m]), text, positions[i+1]);
                    }
                }
            }

            // make sure to update the list of dataField only
            if (dataField != null && dataField.length() > 0) {
                String str, data;
                i = text.indexOf("\"" + dataField + "\":");
                if (i > 0) {
                    j = JSON2Map.locate(1, text, i);
                    str = (j > i) ? text.substring(i, j + 1) : "";
                }
                else
                    str = "";

                try {
                    data = loadFromFile(master);
                }
                catch (IOException e) {
                    data = "";
                    new Event(Event.ERR, name+": failed to load content from "+
                        master + ": " + e.toString()).send();
                }
                if (data.length() > 0) { // got original master content
                    text = data;
                    i = text.indexOf("\"" + dataField + "\":");
                    if (i > 0) {
                        j = JSON2Map.locate(1, text, i);
                        if (j > i)
                            text = text.substring(0, i) + str +
                                text.substring(j+1);
                        else {
                            new Event(Event.ERR, name + ": failed to find the "+
                                 "end position for  " + dataField + " in " +
                                 master).send();
                            return;
                        }
                    }
                    else {
                        i = text.lastIndexOf("}");
                        if (i > 0)
                            text = text.substring(0, i) + str +
                                text.substring(i);
                        else {
                            new Event(Event.ERR, name + ": failed to find the "+
                                 "end position of level 0 in "+master).send();
                            return;
                        }
                    }
                }
            }

            try { // persist the master config
                saveToFile(master, text);
                if ((debug & Service.DEBUG_UPDT) > 0)
                    new Event(Event.DEBUG, name + " updated " + master).send();
            }
            catch (IOException e) {
                new Event(Event.ERR, name + ": failed to write to "+
                    master + ": " + e.toString()).send();
            }
        }

        propertyFile.setLastModified(mtime);
    }

    /** saves content correctly to a file specified by the path */
    private static void saveToFile(String path, String text) throws IOException{
        int i, j, l;
        if (path == null || text == null || path.length() <= 0)
            return; 
        PrintWriter out = new PrintWriter(new FileOutputStream(path, false));
        String str;
        l = text.length();
        i = 0;
        j = l;
        while (i < j) {
            if ((j = text.indexOf("\r\n", i)) >= 0) { // DOS
                str = text.substring(i, j);
                if (str.lastIndexOf("</") >= 0)
                    out.println(str);
                else
                    out.print(str + "\r\n");
                i = j + 2;
                j = l;
            }
            else if ((j = text.indexOf("\n", i)) >= 0) { // Unix
                str = text.substring(i, j);
                if (str.lastIndexOf("</") >= 0)
                    out.println(str);
                else
                    out.print(str + "\n");
                i = j + 1;
                j = l;
            }
            else { // no newline
                str = text.substring(i);
                out.println(str);
                j = l;
                i = j;
            }
        }
        out.close();
    }

    /** loads content from a file and returns the content */
    private static String loadFromFile(String filename) throws IOException {
        int k;
        char[] buffer = new char[4096];
        StringBuffer strBuf = new StringBuffer();
        FileReader in = new FileReader(filename);
        while ((k = in.read(buffer, 0, 4096)) >= 0) {
            if (k > 0)
                strBuf.append(new String(buffer, 0, k));
        }
        in.close();
        return strBuf.toString();
    }

    /** returns the content after cleaning up the includes in keys */
    private static String cutContent(String prefix, String surfix, String end,
        String[] keys, String content, int from) {
        int i, j, k, m;
        if (content == null || keys == null || (m = content.length()) <= 0)
            return content;
        if (from <= 0 || from >= m)
            from = m - 1;
        k = from;
        for (i=0; i<keys.length; i++) {
            j = content.lastIndexOf(prefix + keys[i] + surfix, from); 
            if (j > 0 && j < k)
                k = j;
        }
        if (end != null) { // for normal cases 
            if (k < from) // found at least one key
                content = content.substring(0, k) + end;
        }
        else if (k < from) { // found at least one key for special JSON case
            j = JSON2Map.locate(2, content, k);
            if (j > k)
                content = content.substring(0, k) + content.substring(j);
        }
        return content;
    }

    /** returns the positions of all items in a list with the given key. The
     * first position is the starting point for the name of the list. The last
     * position is the end point of the list. The rest are the positions for
     * the end points for the members of the list in the natural order.
     * If there is no such list, it returns an empty array.
     */
    private static int[] locateJSON(String key, String content) {
        int i, j, k, m, n;
        List<Integer> list;
        if (content == null || key == null || key.length() <= 0)
            return null;

        list = new ArrayList<Integer>();
        m = content.length() - 1;
        k = 0;
        j = content.indexOf("\"" + key + "\":");
        while (j > k) {
            k = j;
            list.add(new Integer(k));
            j = JSON2Map.locate(2, content, k);
        }
        j = JSON2Map.locate(1, content, k);
        if (j > k && j < m)
            list.add(new Integer(j));
        n = list.size();
        int[] positions = new int[n];
        for (i=0; i<n; i++)
            positions[i] = list.get(i).intValue();

        return positions;
    }

    /** returns the JSON content retrieved from the master content */
    private static String getContentJSON(int s, String key, String content) {
        int i, j;
        if (key == null || content == null)
            return null;
        if (s > 0)
            i = content.indexOf(",\"" + key + "\":", s);
        else
            i = content.indexOf(",\"" + key + "\":");
        j = (i > 0) ? content.indexOf("{", i) : -1;
        if (j > 0) {
            int k = JSON2Map.locate(1, content, j);
            return (k > j) ? content.substring(j, k + 1) : "";
        }
        else
            return "";
    }

    /** returns the full clone of includeGroup for includes */
    public Map<String, Object> getIncludeMap() {
        return Utils.cloneProperties(includeGroup);
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousTime", String.valueOf(previousTime));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct, pTime;
        int aCount, eCount, pStatus, sNumber;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            ct = Long.parseLong((String) o);
            if (ct <= System.currentTimeMillis() - checkpointTimeout)
                return;
        }
        else
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ActionCount")) != null)
            aCount = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ExceptionCount")) != null)
            eCount = Integer.parseInt((String) o);
        else
            return;

        if ((o = chkpt.get("PreviousTime")) != null)
            pTime = Long.parseLong((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousTime = pTime;
    }

    public void destroy() {
        super.destroy();
        if (property != null) {
            property.clear();
            property = null;
        }
        if (webTester != null) {
            webTester.destroy();
            webTester = null;
        }
    }

    protected void finalize() {
        destroy();
    }

    /**
     * It builds the complete JSON property file according to
     * the master configuration file and the files for individual components.
     * The configuration parameters are stored in the json file, pm.json,
     * that defines how to handle all applications, etc.
     *<br/><br/>
     * Usage: java PropertyMonitor [-?|-l|-I configFile|...]
     */
    public static void main(String args[]) {
        int i, j, k, n, show = 0, size = 0, num = 0;
        int[] positions, queries = new int[20];
        Map includeMap, ph = null, props = null;
        Map<String, Object> includeGroup;
        Map<String, List> primaryMap;
        Set<String> hSet;
        String cfgDir, basename = null, configFile = null;
        String indicatorFile, propertyFile, application = null, category = null;
        String content, lastModified, includeFile = null;
        String item, str;
        String[] includes, keys;
        StringBuffer strBuf = new StringBuffer();
        SimpleDateFormat dateFormat = null;
        Template template;
        File file;
        Object o;
        final int QUERY_REPOS = 1;
        final int QUERY_FILE = 2;
        final int QUERY_BASE = 3;
        final int QUERY_PERM = 4;
        final int QUERY_OWNER = 5;

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2)
                continue;
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'l':
                show = 1;
                break;
              case 'a':
                application = args[i+1];
                break;
              case 'c':
                category = args[i+1];
                break;
              case 'I':
                if (i+1 < args.length) {
                    configFile = args[i+1];
                }
                break;
              case 'R':
                queries[num++] = QUERY_REPOS;
                break;
              case 'F':
                queries[num++] = QUERY_FILE;
                break;
              case 'B':
                queries[num++] = QUERY_BASE;
                break;
              case 'P':
                queries[num++] = QUERY_PERM;
                break;
              case 'O':
                queries[num++] = QUERY_OWNER;
                break;
              default:
                break;
            }
        }

        if (configFile == null)
            configFile = "/opt/qbroker/agent/pm.json";

        try {
            FileReader fr = new FileReader(configFile);
            ph = (Map) JSON2Map.parse(fr);
            fr.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (ph == null || ph.size() <= 0)
            throw(new IllegalArgumentException("empty property"));

        if (application == null || application.length() <= 0)
            throw(new IllegalArgumentException("application not specified"));

        if (category == null || category.length() <= 0)
            throw(new IllegalArgumentException("category not specified"));

        template = new Template("##category##, ##filename## ##mtime##");

        if (num > 0) { // query only
            if ((o = ph.get(application)) != null && o instanceof Map)
                props = (Map) o;
            else
         throw(new IllegalArgumentException(application + " not well defined"));

            for (i=0; i<num; i++) {
                switch (queries[i]) {
                  case QUERY_REPOS:
                    if ((o = props.get("RepositoryDir")) != null)
                        strBuf.append(template.substitute("category",
                            category, (String) o) + " ");
                    else
               throw(new IllegalArgumentException("RepositoryDir not defined"));
                    break;
                  case QUERY_FILE:
                    if ((o = props.get("IndicatorFile")) != null)
                        strBuf.append(template.substitute("category",
                            category, (String) o) + " ");
                    else
               throw(new IllegalArgumentException("IndicatorFile not defined"));
                    break;
                  case QUERY_BASE:
                    if ((o = props.get("Basename")) != null)
                        strBuf.append((String) o + " ");
                    else
                        strBuf.append(application + " ");
                    break;
                  case QUERY_PERM:
                    if ((o = props.get("Permission")) != null)
                        strBuf.append((String) o + " ");
                    else
                  throw(new IllegalArgumentException("Permission not defined"));
                    break;
                  case QUERY_OWNER:
                    if ((o = props.get("Ownership")) != null)
                        strBuf.append((String) o + " ");
                    else
                   throw(new IllegalArgumentException("Ownership not defined"));
                    break;
                  default:
                    break;
                }
            }
            if (strBuf.length() > 0)
                System.out.println(strBuf.toString());
            System.exit(0);
        }

        if ((o = ph.get("TimeStampTemplate")) == null || !(o instanceof String))
      throw(new IllegalArgumentException("TimeStampTemplate not well defined"));

        lastModified = (String) o;

        if ((o = ph.get("DateFormat")) == null || !(o instanceof String))
            throw(new IllegalArgumentException( "DateFormat not well defined"));

        dateFormat = new SimpleDateFormat((String) o);

        if ((o = ph.get(application)) != null && o instanceof Map)
            props = (Map) o;
        else
         throw(new IllegalArgumentException(application + " not well defined"));

        if ((o = props.get("Basename")) != null)
            basename = (String) o;
        else
            basename = application;

        if ((o = props.get("RepositoryDir")) == null)
            throw(new IllegalArgumentException(
                "RepositoryDir not well defined for: " + application));

        cfgDir = template.substitute("category", category, (String) o);

        if ((o = props.get("PropertyFile")) == null)
            throw(new IllegalArgumentException(
                "PropertyFile not well defined for: " + application));
        propertyFile = cfgDir + FILE_SEPARATOR + (String) o;

        if ((o = props.get("IncludeGroup")) != null && o instanceof Map)
            includeGroup = Utils.cloneProperties((Map) o);
        else
            includeGroup = new HashMap<String, Object>();

        if ((o = props.get("SecondaryInclude")) != null && o instanceof Map)
            includeMap = (Map) o;
        else
            includeMap = new HashMap();

        file = new File(cfgDir);
        if (!file.exists() || !file.isDirectory() || !file.canRead())
            throw(new IllegalArgumentException("failed to open " + cfgDir));

        file = new File(propertyFile);
        if (!file.exists() || !file.canRead())
            throw(new IllegalArgumentException("failed to open "+propertyFile));

        if ((o = props.get("IndicatorFile")) == null)
            throw(new IllegalArgumentException(
                "IndicatorFile not well defined for: " + application));
        indicatorFile = template.substitute("category", category, (String) o);
        file = new File(indicatorFile);
        lastModified = template.substitute("mtime",
            dateFormat.format(new Date()), lastModified);
        String dir = file.getParent() + FILE_SEPARATOR;
        if ((i = cfgDir.indexOf(dir)) >= 0)
            dir = cfgDir.substring(i+dir.length());
        else
            dir = ".";

        props = null;
        content = null;
        try {
            String line;
            FileInputStream fin = new FileInputStream(propertyFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fin));
            while ((line = br.readLine()) != null)
                strBuf.append(line + "\n");
            fin.close();
            br.close();

            content = strBuf.toString();

            StringReader in = new StringReader(content);
            props = (Map) JSON2Map.parse(in);
            in.close();
            if (props == null)
                throw(new IOException("failed to get object from "+
                    file.getPath()));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        hSet = new HashSet<String>();
        // get primary includes before any actions
        primaryMap = MonitorUtils.getIncludes(basename, includeGroup, props);

        // process secondary includes first
        for (String key : includeGroup.keySet()) {
            if ((o = includeGroup.get(key)) == null || !(o instanceof List))
                continue;

            hSet.clear();
            List list = (List) o;
            size = list.size();
            for (i=0; i<size; i++) { // look for components with 2nd include
                if ((o = list.get(i)) == null || !(o instanceof String))
                    continue;
                item = (String) o;
                if ((o = includeMap.get(item)) != null && o instanceof String)
                    hSet.add(item);
            }
            n = hSet.size();
            if (n <= 0) // no 2nd includes
                continue;

            keys = hSet.toArray(new String[n]);
            // assumes only one item with 2nd includes
            if (keys[0] == null || keys[0].length() <= 0 ||
                (o = includeMap.get(keys[0])) == null)
                continue;

            // str is the value for the only key of SecondaryInclude
            str = (String) o;

            if (key.equals(basename)) { // for Flow
                List<Object> pl = new ArrayList<Object>();
                pl.add(props);
                o = pl;
            }
            else if ((o = props.get(key)) == null || !(o instanceof List))
                continue;

            // only one case may reach here
            list = (List) o;
            size = list.size();
            positions = null;
            if (!key.equals(basename)) {
                // scan for positions for key and its members
                positions = locateJSON(key, content);
                if (positions.length != size + 2) { // failure
                    throw(new IllegalArgumentException(
                        "failed to locate end positions for "+
                        key + ": " + positions.length + "/" + size));
                }
            }

            for (i=size-1; i>=0; i--) { // loop thru group instances
                if ((o = list.get(i)) == null || !(o instanceof Map))
                    continue;
                Map group = (Map) o;

                // for IncludePolicy
                if ((o = group.get(str)) == null || !(o instanceof Map))
                    continue;

                Map policyMap = (Map) o;
                hSet.clear();

                // retrieve the set for primary includes for current group
                for (Object obj : (Set) ((List) primaryMap.get(key)).get(i)) {
                    if (obj instanceof String)
                        hSet.add((String) obj);
                }
                n = hSet.size();

                // get all secondary includes
                includes = MonitorUtils.getSecondaryIncludes(
                    hSet.toArray(new String[n]), cfgDir, policyMap);

                strBuf = new StringBuffer();
                for (j=0; j<includes.length; j++) {
                    if (includes[j] == null || includes[j].length() <= 0)
                        continue;
                    String text;
                    item = cfgDir + FILE_SEPARATOR + includes[j] + ".json";
                    try {
                        text = loadFromFile(item);
                    }
                    catch (Exception e) {
                        text = "failed to load the content from " +
                            item + ": " + e.toString();
                    }
                    strBuf.append(",\"" + includes[j] + "\":\n" + text);
                }
                if (includes.length <= 0 || strBuf.length() <= 0) //no 2nd incls
                    continue;
                if (basename.equals(key))
                    k = content.lastIndexOf("}");
                else // JSON for other group instances
                    k = positions[i+1];
                if (k > 0) // insert the content
                    content = content.substring(0, k) + strBuf.toString() +
                        content.substring(k);
            }
        }

        // add mtime
        if ((i = JSON2Map.locate(0, content, 0)) >= 0)
            content = content.substring(0, i+1) +
                lastModified + content.substring(i+1);

        // get names for all primary includes
        includes = Utils.getIncludes(basename, includeGroup, props);

        n = includes.length;
        strBuf = new StringBuffer();
        for (i=0; i<n; i++) { // include individual configs into master config
            String text;
            item = cfgDir + FILE_SEPARATOR + includes[i] + ".json";
            try {
                text = loadFromFile(item);
            }
            catch (Exception e) {
                text = "failed to load the content from " + item +
                    ": " + e.toString();
            }
            strBuf.append(",\"" + includes[i] + "\":\n" + text);
        }

        if ((i = content.lastIndexOf("}")) > 0)
            content = content.substring(0, i) + strBuf.toString() +
                content.substring(i);

        if (show > 0) { // output content to stdout
            System.out.print(content);
        }
        else try { // save content to the indicatorFile
            saveToFile(indicatorFile, content);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("PropertyMonitor Version 2.0 (written by Yannan Lu)");
        System.out.println("PropertyMonitor: a deployment tool for JSON Properties");
        System.out.println("Usage: java org.qbroker.monitor.PropertyMonitor [-?|-l|-I ConfigFile]");
        System.out.println("  -?: print this usage page");
        System.out.println("  -l: list all properties of the indicator file");
        System.out.println("  -I: ConfigFile (default: /opt/qbroker/agent/pm.json)");
        System.out.println("  -c: Category");
        System.out.println("  -a: Application");
        System.out.println("  -R: query the full path of the Repository Directory");
        System.out.println("  -F: query the full path of the indicator file");
        System.out.println("  -B: query the basename of the indicator file");
        System.out.println("  -O: query the ownership of the indicator file");
        System.out.println("  -P: query the permission of the indicator file");
    }
}
