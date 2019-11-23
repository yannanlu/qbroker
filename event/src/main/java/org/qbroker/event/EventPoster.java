package org.qbroker.event;

/* EventPoster.java - an EventAction to post/get content to/from an URL */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.HttpURLConnection;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.HTTPConnector;
import org.qbroker.net.HTTPClient;
import org.qbroker.event.Event;
import org.qbroker.event.EventAction;

/**
 * EventPoster implements EventAction and posts/gets content to/from a
 * given URL.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventPoster implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private String uri, host = null;
    private HTTPConnector conn = null;
    private long serialNumber;
    private int debug = 0;
    private Map<String, Map> poster;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private boolean isPost = true;
    private final static String hostname = Event.getHostName().toLowerCase();

    public EventPoster(Map props) {
        URI u = null;
        Object o;
        Template template, temp;
        TextSubstitution[] msgSub = null;
        Map<String, Object> map, ph;
        String s, key, value;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "EventPoster";
        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");
        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "post/get content to/from an URL";

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));
        uri = EventUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        s = u.getScheme();
        if (s == null || ((!"http".equals(s)) && (!"https".equals(s))))
            throw(new IllegalArgumentException(name+": wrong scheme: "+uri));
        host = u.getHost();
        if (host == null || host.length() <= 0)
            throw(new IllegalArgumentException(name +
                ": host not defined in " + uri));

        poster = new HashMap<String, Map>();
        ph = Utils.cloneProperties(props);
        ph.put("URI", uri);
        ph.remove("Site");
        ph.remove("Type");
        ph.remove("Category");
        ph.remove("Priority");
        if ((o = ph.remove("Timeout")) != null)
            ph.put("SOTimeout", o);
        if (!props.containsKey("IsPost")) // default is POST
            ph.put("IsPost", "true");
        if ((o = ph.get("ProxyHost")) != null)
            conn = new HTTPClient(ph);
        else
            conn = new org.qbroker.net.HTTPConnector(ph);
        isPost = conn.isPost();

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

        if ((o = ph.get("QueryTemplate")) != null && o instanceof String) {
            value = EventUtils.substitute((String) o, template);
            map = new HashMap<String, Object>();
            temp = new Template(value);
            if (temp.numberOfFields() <= 0)
                map.put("Template", value);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = ph.get("Substitution");
                if (o != null && o instanceof List) {
                    msgSub = EventUtils.initSubstitutions((List) o);
                    map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                }
            }
            poster.put("Default", map);
        }

        if (msgSub == null && (o = ph.get("Substitution")) != null &&
            o instanceof List) { // for default msgSub
            msgSub = EventUtils.initSubstitutions((List) o);
        }

        Iterator iter = ph.keySet().iterator();
        while (iter.hasNext()) { // looking for Map
            key = (String) iter.next();
            if (key == null || key.length() == 0)
                continue;
            if ((o = ph.get(key)) == null || !(o instanceof Map))
                continue;
            if ("StringProperty".equals(key) || "ActiveTime".equals(key) ||
                "CopiedProperty".equals(key))
                continue;
            if (((Map) o).containsKey("Option")) // for option
                continue;

            value = (String) ((Map) o).get("QueryTemplate");
            if (value == null || value.length() <= 0) // not for query
                continue;
            value = EventUtils.substitute(value, template);
            map = new HashMap<String, Object>();
            temp = new Template(value);
            if (temp.numberOfFields() <= 0)
                map.put("Template", value);
            else { // with variables
                map.put("Template", temp);
                map.put("Fields", temp.getAllFields());
                o = ((Map) o).get("Substitution");
                if (o != null && o instanceof List) // for override
                    map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                    map.put("MsgSub", msgSub);
            }
            poster.put(key, map);
        }
        ph.clear();

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        serialNumber = 0;
    }

    public String doPost(Event event, Map map) throws IOException {
        int rc = -1;
        if (event == null)
            throw(new IOException("Empty event"));

        String line;
        StringBuffer strBuf = new StringBuffer();
        HashMap attr = event.attribute;
        Map<String, Object> change = null;

        if (map != null) { // send content
            Object o;
            if ((o = map.get("Template")) != null && o instanceof Template) {
                int i, n;
                TextSubstitution[] msgSub;
                String key, value;
                Template template = (Template) o;
                String[] allFields = (String[]) map.get("Fields");
                msgSub = (TextSubstitution[]) map.get("MsgSub");
                if (msgSub != null) {
                    change = EventUtils.getChange(event, msgSub);
                    if (change != null && change.size() <= 0)
                        change = null;
                }
                line = template.copyText();
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
                        line = template.substitute(key, value, line);
                    }
                    else if ("serialNumber".equals(key)) {
                        line = template.substitute(key,
                            String.valueOf(serialNumber), line);
                    }
                    else {
                        line = template.substitute(key, "", line);
                    }
                }
            }
            else if (o != null && o instanceof String)
                line = (String) o;
            else // do nothing
                return null;
        }
        else { // post event
            if ((line = (String) attr.get("relayHost")) != null &&
                host.equalsIgnoreCase(line)) // stop ping-pong here
                return null;

            change = new HashMap<String, Object>();
            if (line == null)
                change.put("relayHost", hostname);

            if (attr.containsKey("gid"))
                change.put("gid", null);

            line = EventUtils.postable(event, change);
        }
        try {
            conn.reconnect();
            rc = conn.doPost(null, line, strBuf);
            conn.close();
        }
        catch (Exception e) {
            conn.close();
            throw(new IOException(e.toString()));
        }

        if (rc != HttpURLConnection.HTTP_OK)
            throw(new IOException("ErrorCode: " + rc));
        else
            return strBuf.toString();
    }

    public String doGet(Event event, Map map) throws IOException {
        int rc = -1;

        if (event == null)
            throw(new IOException("Empty event"));

        StringBuffer strBuf = new StringBuffer();

        if (map != null) { // dynamic uri
            Object o;
            String url;
            if ((o = map.get("Template")) != null && o instanceof Template) {
                int i, n;
                HashMap attr;
                Map change = null;
                TextSubstitution[] msgSub;
                String key, value;
                Template template = (Template) o;
                String[] allFields = (String[]) map.get("Fields");
                msgSub = (TextSubstitution[]) map.get("MsgSub");
                if (msgSub != null) {
                    change = EventUtils.getChange(event, msgSub);
                    if (change != null && change.size() <= 0)
                        change = null;
                }

                url = template.copyText();
                attr = event.attribute;
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
                        url = template.substitute(key, value, url);
                    }
                    else if ("serialNumber".equals(key)) {
                        url = template.substitute(key,
                            String.valueOf(serialNumber), url);
                    }
                    else {
                        url = template.substitute(key, "", url);
                    }
                }
                if (change != null)
                    change.clear();
            }
            else if (o != null && o instanceof String)
                url = (String) o;
            else
                url = null;
            conn.reconnect();
            rc = conn.doGet(url, strBuf);
            conn.close();
        }
        else { // static uri
            conn.reconnect();
            rc = conn.doGet(null, strBuf);
            conn.close();
        }

        if (rc != HttpURLConnection.HTTP_OK)
            throw(new IOException("ErrorCode: " + rc));
        else
            return strBuf.toString();
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        String eventType, priorityName, str = null;
        Map map;

        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;
        eventType = (String) event.attribute.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = poster.get(eventType);
        if (map == null)
            map = poster.get("Default");

        try {
            if (isPost)
                str = doPost(event, map);
            else
                str = doGet(event, map);
        }
        catch (Exception e) {
            str = (isPost) ? "post " : "query ";
            new Event(Event.ERR, name + ": failed to " + str + uri + " for " +
                eventType + ": " + e.toString()).send();
            return;
        }
        if (debug > 0 && str != null)
            new Event(Event.DEBUG, name + ": got response from " + uri +
                ": " + str).send();
    }

    /**
     * It applies the rules to the event and returns true if the action
     * is active and will be invoked upon the event, or false otherwise.
     */
    public boolean isActive(long currentTime, Event event) {
        HashMap attr;
        Map map;
        String eventType, priorityName;

        if (pattern == null)
            return true;

        if (event == null)
            return false;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return false;

        attr = event.attribute;
        eventType = (String) attr.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        map = poster.get(eventType);
        if (map == null)
            map = poster.get("Default");
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
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (poster != null) {
            Map map;
            Object o;
            TextSubstitution[] tsub;
            for (String key : poster.keySet()) {
                map = poster.get(key);
                o = map.remove("Template");
                if (o != null && o instanceof Template)
                    ((Template) o).clear();
                tsub = (TextSubstitution[]) map.remove("MsgSub");
                if (tsub != null) {
                    for (TextSubstitution sub : tsub)
                        sub.clear();
                }
                map.clear();
            }
            poster.clear();
            poster = null;
        }
    }

    protected void finalize() {
        close();
    }
}
