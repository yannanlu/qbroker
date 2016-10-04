package org.qbroker.event;

/* EventActionGroup.java - a group of EventActions */

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.lang.reflect.InvocationTargetException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.event.EventAction;
import org.qbroker.event.EventScriptLauncher;

/**
 * EventActionGroup is a wrapper for a group of EventAction objects.
 * With it, you can fire off a group of actions on an event.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventActionGroup implements EventAction {
    private String name;
    private String site;
    private String type;
    private String category;
    private String description;
    private EventAction[] action = null;
    private boolean scriptDisabled = false;
    private int[] scriptIndex = null;

    public EventActionGroup(Map props) {
        URI u;
        Object o;
        List list;
        String uri, s, className;
        int i, n, m;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "EventActionGroup";
        if ((o = props.get("Description")) != null)
            description = (String) o;
        else
            description = "invoke a group of actions upon an event";

        if ((o = props.get("ActionGroup")) == null || !(o instanceof List) ||
            ((List) o).size() <= 0)
            throw(new IllegalArgumentException(name +
                ": ActionGroup is not well defined"));

        list = (List) o;

        m = 0;
        n = list.size();
        action = new EventAction[n];
        for (i=0; i<n; i++) { // pack all scripts at the front
            if ((o = list.get(i)) == null || !(o instanceof Map)) {
                action[i] = null;
                continue;
            }

            Map<String, Object> ph = Utils.cloneProperties((Map) o);
            if (!ph.containsKey("Name"))
                ph.put("Name", name + "_" + i);
            if (!ph.containsKey("Site"))
                ph.put("Site", site);
            if (!ph.containsKey("Category"))
                ph.put("Category", category);
            if (!ph.containsKey("Description"))
                ph.put("Description", description);

            className = (String) ph.get("ClassName");
            uri = (String) ph.get("URI");

            if (uri == null || uri.length() <= 0) // default uri for script
                uri = "script://localhost";

            if (className == null || className.length() <= 0) try {
                u = new URI(uri);
                s = u.getScheme();
                if (s == null || "script".equals(s))
                    className = "org.qbroker.event.EventScriptLauncher";
                else if ("smtp".equals(s))
                    className = "org.qbroker.event.FormattedEventMailer";
                else if ("http".equals(s))
                    className = "org.qbroker.event.EventPoster";
                else if ("log".equals(s) || "file".equals(s))
                    className = "org.qbroker.event.EventAppender";
                else if ("syslog".equals(s))
                    className = "org.qbroker.event.EventSyslogger";
                else if ("snmp".equals(s))
                    className = "org.qbroker.event.EventTrapSender";
                else if ("jdbc".equals(s))
                    className = "org.qbroker.event.EventSQLExecutor";
                else
                    throw(new IllegalArgumentException(s + " not supported"));
            }
            catch (URISyntaxException e) {
                throw(new IllegalArgumentException(name+": bad URI for action "+
                    i + " of " + uri + ": " + e.toString()));
            }

            if (className == null || className.length() <= 0)
                throw(new IllegalArgumentException(name + ": action " + i +
                    " is not supported: " + uri));
            else try {
                java.lang.reflect.Constructor con;
                Class<?> cls = Class.forName(className);
                con = cls.getConstructor(new Class[]{Map.class});
                action[i] = (EventAction) con.newInstance(new Object[]{ph});
            }
            catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                new Event(Event.ERR, name +" failed to instantiate "+
                    className + ": " + Event.traceStack(ex)).send();
                throw(new IllegalArgumentException(name +
                    ": failed to instantiate " + className));
            }
            catch (Exception e) {
                new Event(Event.ERR, name +" failed to instantiate "+
                    className + ": " + Event.traceStack(e)).send();
                throw(new IllegalArgumentException(name +
                    ": failed to instantiate " + className));
            }
            if (action[i] instanceof EventScriptLauncher ||
                action[i] instanceof EventSQLExecutor)
                m ++;
        }

        scriptIndex = new int[m];
        m = 0;
        for (i=0; i<action.length; i++) {
            if (action[i] instanceof EventScriptLauncher)
                scriptIndex[m++] = i;
            else if (action[i] instanceof EventSQLExecutor)
                scriptIndex[m++] = i;
        }
    }

    public void invokeAction(long currentTime, Event event) {
        int i;
        if (scriptIndex.length > 0 && scriptDisabled) {
            for (i=0; i<action.length; i++) {
                if (action[i] != null &&
                    !(action[i] instanceof EventScriptLauncher) &&
                    !(action[i] instanceof EventSQLExecutor))
                    action[i].invokeAction(currentTime, event);
            }
        }
        else {
            for (i=0; i<action.length; i++) {
                if (action[i] != null)
                    action[i].invokeAction(currentTime, event);
            }
        }
    }

    /**
     * It returns true if any script is active, or false otherwise
     */
    public boolean isActive(long currentTime, Event event) {
        int i, j;
        for (i=0; i<scriptIndex.length; i++) {
            j = scriptIndex[i];
            if (action[j] == null)
                continue;
            if (action[j] instanceof EventScriptLauncher &&
                ((EventScriptLauncher) action[j]).isActive(currentTime, event))
                return true;
            else if (action[j] instanceof EventSQLExecutor &&
                ((EventSQLExecutor) action[j]).isActive(currentTime, event))
                return true;
        }
        return false;
    }

    public int getNumberScripts() {
        return scriptIndex.length;
    }

    public void disableActionScript() {
        scriptDisabled = true;
    }

    public void enableActionScript() {
        scriptDisabled = false;
    }

    public String getName() {
        return name;
    }

    public void close() {
        if (action != null) {
            for (int i=0; i<action.length; i++) {
                if (action[i] != null)
                    action[i].close();
                action[i] = null;
            }
        }
    }

    protected void finalize() {
        close();
    }
}
