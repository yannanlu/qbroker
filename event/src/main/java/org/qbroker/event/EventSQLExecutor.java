package org.qbroker.event;

/* EventSQLExecutor.java - an EventAction to execute an query on a database */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.io.File;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.net.DBConnector;
import org.qbroker.event.Event;
import org.qbroker.event.EventAction;

/**
 * EventSQLExecutor runs a SQL statement on a database in response to an event.
 * The SQL statement may contains the template placeholders referecing the
 * attributes of the event.
 *<br/><br/>
 * NB. The action part is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class EventSQLExecutor implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private Map<String, Map> launcher;
    private String[] copiedProperty;
    private long serialNumber;
    private int timeout;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private String program, hostname, pid;
    private DBConnector dbConn = null;

    public EventSQLExecutor(Map props) {
        Object o;
        Map map;
        String sql, key, value;
        Template template, temp;
        TextSubstitution[] msgSub = null;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "EventSQLExecutor";

        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");

        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "execute a SQL query in response to an event";

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));

        map = new HashMap();
        map.put("URI", o);
        if ((o = props.get("Username")) != null) {
            map.put("Username", o);
            if ((o = props.get("Password")) != null)
                map.put("Password", o);
            else if ((o = props.get("EncryptedPassword")) != null)
                map.put("EncryptedPassword", o);
        }
        map.put("DBTimeout", props.get("DBTimeout"));
        map.put("SQLExecTimeout", props.get("SQLExecTimeout"));
        map.put("ConnectOnInit", "false");
        dbConn = new DBConnector(map);

        launcher = new HashMap<String, Map>();

        if ((o = props.get("Timeout")) == null ||
            (timeout = 1000*Integer.parseInt((String) o)) < 0)
            timeout = 60000;

        if ((o = props.get("SQLStatement")) != null) {
            sql = EventUtils.substitute((String) o, template);
            map = new HashMap();
            temp = new Template(sql);
            if (temp.numberOfFields() <= 0)
                map.put("Template", sql);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = props.get("Substitution");
                if (o != null && o instanceof List) {
                    msgSub = EventUtils.initSubstitutions((List) o);
                    map.put("MsgSub", msgSub);
                }
            }
            launcher.put("Default", map);
        }
        else if ((o = props.get("Default")) != null && o instanceof Map) {
            o = ((Map) o).get("Substitution");
            if (o != null && o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
            else if ((o = props.get("Substitution")) != null &&
                o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
        }

        Iterator iter = props.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() == 0)
                continue;
            if ((o = props.get(key)) == null || !(o instanceof Map))
                continue;
            if ("StringProperty".equals(key) || "ActiveTime".equals(key) ||
                "CopiedProperty".equals(key))
                continue;
            if (((Map) o).containsKey("Option")) // for option hash
                continue;
            value = (String) ((Map) o).get("SQLStatement");
            if (value == null || value.length() <= 0) // not for SQL
                continue;
            sql = EventUtils.substitute(value, template);
            map = new HashMap();
            temp = new Template(sql);
            if (temp.numberOfFields() <= 0)
                map.put("Template", sql);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = ((Map) o).get("Substitution");
                if (o != null && o instanceof List) // override
                   map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                   map.put("MsgSub", msgSub);
            }
            launcher.put(key, map);
        }
        if (launcher.size() <= 0)
            throw(new IllegalArgumentException(name + ": no SQL defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Priority")) != null)
                pattern = pc.compile((String) o);
            else
                pattern = null;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("CopiedProperty")) != null && o instanceof Map) {
            map = (Map) o;
            iter = ((Map) o).keySet().iterator();
            n = ((Map) o).size();
            copiedProperty = new String[n];
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key != null && key.length() > 0)
                    copiedProperty[n++] = key;
            }
        }
        else
            copiedProperty = new String[0];

        serialNumber = 0;
        hostname = Event.getHostName();
        program = Event.getProgramName();
        pid = String.valueOf(Event.getPID());
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        Object o;
        HashMap attr;
        Map map;
        String key, value, priorityName, eventType, sql = null, str = null;
        StringBuffer strBuf;
        int i, n;

        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;
        attr = event.attribute;
        eventType = (String) attr.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = launcher.get(eventType);
        if (map == null)
            map = launcher.get("Default");
        if (map == null || map.size() <= 0)
            return;

        if ((o = map.get("Template")) != null && o instanceof Template) {
            String[] allFields;
            Map change = null;
            Template template;
            TextSubstitution[] msgSub;

            template = (Template) o;
            msgSub = (TextSubstitution[]) map.get("MsgSub");
            if (msgSub != null) {
                change = EventUtils.getChange(event, msgSub);
                if (change != null && change.size() <= 0)
                    change = null;
            }

            allFields = (String[]) map.get("Fields");
            sql = template.copyText();
            n = allFields.length;
            for (i=0; i<n; i++) {
                key = allFields[i];
                if (attr.containsKey(key)) {
                    if (change == null)
                        value = (String) attr.get(key);
                    else if (change.containsKey(key))
                        value = (String) change.get(key);
                    else
                        value = (String) attr.get(key);
                    if (value == null)
                        value = "";
                    sql = template.substitute(key, value, sql);
                }
                else if ("serialNumber".equals(key)) {
                    sql = template.substitute(key,
                        String.valueOf(serialNumber), sql);
                }
                else {
                    sql = template.substitute(key, "", sql);
                }
            }
            if (change != null)
                change.clear();
        }
        else if (o != null && o instanceof String)
            sql = (String) o;
        else
            return;

        n = -1;
        str = dbConn.reconnect();
        if (str == null) try {
            dbConn.setAutoCommit(true);
            n = dbConn.executeQuery(sql);
        }
        catch (Exception e) {
            str = e.toString();
        }
        dbConn.close();
        if (str != null)
            i = Event.ERR;
        else {
            i = Event.INFO;
            str = "SQL executed with the return code: " + n;
        }

        Event ev = new Event(i, str);
        strBuf = new StringBuffer();
        strBuf.append((String) attr.get("date"));
        strBuf.append(" " + priorityName);
        strBuf.append(" " + (String) attr.get("name"));
        strBuf.append(" " + (String) attr.get("hostname"));
        strBuf.append(" " + (String) attr.get("program"));

        ev.setAttribute("name", name);
        if ((o = attr.get("site")) != null)
            ev.setAttribute("site", (String) o);
        else
            ev.setAttribute("site", site);
        ev.setAttribute("category", category);
        ev.setAttribute("type", type);
        ev.setAttribute("date", Event.dateFormat(new Date(ev.timestamp)));
        ev.setAttribute("description", description);
        ev.setAttribute("sql", sql);
        ev.setAttribute("original", strBuf.toString());

        for (i=0; i<copiedProperty.length; i++) {
            if ((o = attr.get(copiedProperty[i])) == null)
                continue;
            ev.setAttribute(copiedProperty[i], (String) o);
        }
        ev.setAttribute("program", program);
        ev.setAttribute("hostname", hostname);
        ev.setAttribute("pid", pid);

        ev.send();
    }

    /**
     * It applies the rules to the event and returns true if the action
     * is active and will be invoked upon the event, or false otherwise.
     */
    public boolean isActive(long currentTime, Event event) {
        HashMap attr;
        Map map;
        String eventType, priorityName;

        if (event == null)
            return false;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return false;

        attr = event.attribute;
        eventType = (String) attr.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = launcher.get(eventType);
        if (map == null)
            map = launcher.get("Default");
        if (map == null || map.size() <= 0)
            return false;

        return true;
    }

    public String getName() {
        return name;
    }

    public void close() {
        pm = null;
        pattern = null;
        if (dbConn != null) {
            dbConn.close();
            dbConn = null;
        }
        if (launcher != null) {
            Map map;
            Object o;
            TextSubstitution[] tsub;
            for (String key : launcher.keySet()) {
                map = launcher.get(key);
                o = map.get("Template");
                if (o != null && o instanceof Template)
                    ((Template) o).clear();
                tsub = (TextSubstitution[]) map.remove("MsgSub");
                if (tsub != null) {
                    for (TextSubstitution sub : tsub)
                        sub.clear();
                }
                map.clear();
            }
            launcher.clear();
            launcher = null;
        }
    }

    protected void finalize() {
        close();
    }
}
