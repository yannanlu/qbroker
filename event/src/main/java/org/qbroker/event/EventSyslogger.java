package org.qbroker.event;

/* EventSyslogger.java - an EventAction to log events via syslog */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.URISyntaxException;
import java.net.URI;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.event.EventUtils;
import org.qbroker.event.EventAction;

/**
 * EventSyslogger implements EventAction and logs a message to a syslogd
 * via an UDP packet.
 *<br><br>
 * This is MT-Safe.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class EventSyslogger implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private long serialNumber;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private static DatagramSocket socket = null;
    private DatagramPacket packet;
    private Map<String, Map> logger;
    private String uri, hostname = "localhost", formatKey = "type";
    private int port = 514, debug = 0;
    private int defaultFacility = LOG_USER;
    private int defaultPriority = Event.INFO;
    private boolean isType = true;
    private SimpleDateFormat dateFormat=new SimpleDateFormat("MMM d HH:mm:ss ");

    // Syslog Facility
    public final static int LOG_KERN = 0;    // kernel messages
    public final static int LOG_USER = 1;    // user-level messages
    public final static int LOG_MAIL = 2;    // mail system
    public final static int LOG_DAEMON = 3;  // system daemons
    public final static int LOG_AUTH = 4;    // security/authorization messages
    public final static int LOG_SYSLOG = 5;  // internal msg from syslogd
    public final static int LOG_LPR = 6;     // line printer subsystem
    public final static int LOG_NEWS = 7;    // news subsystem
    public final static int LOG_UUCP = 8;    // UUCP subsystem
    public final static int LOG_CRON = 9;    // cron daemon
    public final static int LOG_SECURITY=10; // security/authorization
    public final static int LOG_FTP = 11;    // FTP daemon
    public final static int LOG_NTP = 12;    // NTP subsystem
    public final static int LOG_AUDIT = 13;  // log audit
    public final static int LOG_ALERT = 14;  // log alert
    public final static int LOG_CLOCK = 15;  // clock daemon
    public final static int LOG_LOCAL0 = 16; // reserved for local use
    public final static int LOG_LOCAL1 = 17; // reserved for local use
    public final static int LOG_LOCAL2 = 18; // reserved for local use
    public final static int LOG_LOCAL3 = 19; // reserved for local use
    public final static int LOG_LOCAL4 = 20; // reserved for local use
    public final static int LOG_LOCAL5 = 21; // reserved for local use
    public final static int LOG_LOCAL6 = 22; // reserved for local use
    public final static int LOG_LOCAL7 = 23; // reserved for local use

    public EventSyslogger(Map props) {
        URI u = null;
        Object o;
        Template template, temp;
        TextSubstitution[] msgSub = null;
        Map<String, Object> map;
        Map h;
        String s, key, value;
        int i, n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "EventSyslogger";
        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");
        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "send email formatted from event to recipients";

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
        if (s == null || !"syslog".equals(s))
            throw(new IllegalArgumentException(name+": wrong scheme: "+uri));
        port = u.getPort();
        if (port <= 0)
            port = 514;

        hostname = u.getHost();
        if (hostname == null || hostname.length() <= 0 ||
            "localhost".equals(hostname.toLowerCase())) try {
            hostname=java.net.InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (java.net.UnknownHostException e) {
            hostname = "localhost";
        }

        InetAddress address;
        try {
            address = InetAddress.getByName(hostname);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException("unknown host: " + hostname));
        }

        if ((o = props.get("Facility")) != null) {
            defaultFacility = getFacilityByName((String) o);
            if (defaultFacility < 0)
                defaultFacility = LOG_USER;
        }

        if ((o = props.get("FormatKey")) != null) {
            formatKey = (String) o;
            if (formatKey.length() <= 0)
                formatKey = "type";
            else
                isType = false;
        }

        logger = new HashMap<String, Map>();
        if ((o = props.get("Message")) != null) { // default
            value = EventUtils.substitute((String) o, template);
            map = new HashMap<String, Object>();
            temp = new Template(value);
            if (temp.size() <= 0)
                map.put("Template", value);
            else { // with variables
                map.put("Template", temp);
                o = props.get("Substitution");
                if (o != null && o instanceof List) {
                   msgSub = EventUtils.initSubstitutions((List) o);
                   map.put("MsgSub", msgSub);
                }
            }
            map.put("Facility", new Integer(defaultFacility));
            logger.put("Default", map);
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
            h = (Map) o;
            if (h.containsKey("Option")) // for option hash
                continue;
            value = EventUtils.substitute((String) h.get("Message"), template);
            if (value == null || value.length() <= 0) // not for message
                continue;
            value = EventUtils.substitute(value, template);
            map = new HashMap<String, Object>();
            temp = new Template(value);
            if (temp.size() <= 0)
                map.put("Template", value);
            else { // with variables
                map.put("Template", temp);
                o = h.get("Substitution");
                if (o != null && o instanceof List) // override
                   map.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                   map.put("MsgSub", msgSub);
            }
            if ((s = (String) h.get("Facility")) == null)
                map.put("Facility", new Integer(defaultFacility));
            else if ((i = getFacilityByName(s)) >= 0)
                map.put("Facility", new Integer(i));
            else
                map.put("Facility", new Integer(defaultFacility));
            logger.put(key, map);
        }

        i = (defaultFacility << 3) | defaultPriority;
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("<"+ String.valueOf(i)+">");
        strBuf.append("Jan 29 20:15:51 " + hostname + " This is a test ");
        i = strBuf.length();
        byte[] buffer = new byte[i];
        System.arraycopy(strBuf.toString().getBytes(), 0, buffer, 0, i);
        buffer[i-1] = 0;
        packet = new DatagramPacket(buffer, 0, i, address, this.port);

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();

            if ((o = props.get("Priority")) != null)
                pattern = pc.compile((String) o);
            else
                pattern = null;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                ": failed to compile pattern: " + e.toString()));
        }

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        serialNumber = 0;
    }

    public String log(Event event) throws IOException {
        return log(defaultFacility, event);
    }

    public String log(int facility, Event event) throws IOException {
        String text;
        if (socket == null || event == null)
            return null;

        if (facility < LOG_KERN || facility > LOG_LOCAL7)
            facility = defaultFacility;
        text = event.getAttribute("text");
        log(facility, event.getPriority(), event.timestamp, text);
        return text;
    }

    public String log(Event event, Map map) throws IOException {
        String text = null;
        if (socket == null || event == null)
            return null;

        if (map != null && map.size() > 0) {
            Object o;
            String value;
            int facility;
            HashMap attr = event.attribute;
            if ((o = map.get("Facility")) != null && o instanceof Integer) {
                facility = ((Integer) o).intValue();
                if (facility < LOG_KERN || facility > LOG_LOCAL7)
                    facility = defaultFacility;
            }
            else
                facility = defaultFacility;

            if ((o = map.get("Template")) != null && o instanceof Template) {
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

                text = template.copyText();
                for (String key : template.keySet()) {
                    if (attr.containsKey(key)) {
                        if (change == null)
                            value = (String) attr.get(key);
                        else if (change.containsKey(key))
                            value = (String) change.get(key);
                        else
                            value = (String) attr.get(key);
                        if (value == null)
                            value = "";
                        text = template.substitute(key, value, text);
                    }
                    else if ("serialNumber".equals(key)) {
                        text = template.substitute(key,
                            String.valueOf(serialNumber), text);
                    }
                    else {
                        text = template.substitute(key, "", text);
                    }
                }
            }
            else if (o != null && o instanceof String) {
                text = (String) o;
            }
            else {
                text = (String) attr.get("text");
            }
            log(facility, event.getPriority(), event.timestamp, text);
        }
        else {
            text = log(defaultFacility, event);
        }

        return text;
    }

    public void log(int facility, int priority, long timestamp, String text)
        throws IOException {
        int i;
        StringBuffer strBuf;
        byte[] buffer;

        if (socket == null || text == null)
            return;

        if (facility < LOG_KERN || facility > LOG_LOCAL7)
            facility = defaultFacility;

        if (priority < Event.EMERG)
            priority = Event.EMERG;
        else if(priority > Event.DEBUG)
            priority = Event.DEBUG;

        priority = (facility << 3) | priority;
        strBuf = new StringBuffer("<" + String.valueOf(priority) + ">");
        i = strBuf.length();
        strBuf.append(dateFormat.format(new Date(timestamp)));
        if (strBuf.length() - i == 15) // try to add a space if date < 10
            strBuf.insert(i+4, ' ');
        i = strBuf.length();
        buffer = new byte[i+text.length()+1];
        System.arraycopy(strBuf.toString().getBytes(), 0, buffer, 0, i);
        System.arraycopy(text.getBytes(), 0, buffer, i, text.length());
        i += text.length();
        buffer[i++] = 0;
        packet.setData(buffer, 0, ((i > 1024) ? 1024 : i));

        socket.send(packet);
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        String eventKey, priorityName, str = null;
        Map map;

        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;

        serialNumber ++;
        eventKey = (String) event.attribute.get(formatKey);
        if (eventKey == null || eventKey.length() == 0) {
            if (isType)
                eventKey = "Default";
            else { // retry on type
                eventKey = (String) event.attribute.get("type");
                if (eventKey == null || eventKey.length() == 0)
                    eventKey = "Default";
            }
        }

        map = logger.get(eventKey);
        if (map == null)
            map = logger.get("Default");

        try {
            str = log(event, map);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to log to " + uri + " for " +
                eventKey + ": " + Event.traceStack(e)).send();
            return;
        }
        if (debug > 0 && str != null)
            new Event(Event.DEBUG, name + ": sent a log to " + uri +
                ": " + str).send();
    }

    public String getName() {
        return name;
    }

    /**
     * It returns syslog facility in number or -1 on a bad facility name
     */
    public static int getFacilityByName(String name) {
        if (name == null || name.length() <= 0)
            return -1;

        switch (name.charAt(0)) {
          case 'd':
          case 'D':
            if ("DAEMON".equals(name.toUpperCase()))
                return LOG_DAEMON;
            break;
          case 'k':
          case 'K':
            if ("KERN".equals(name.toUpperCase()))
                return LOG_KERN;
            break;
          case 'u':
          case 'U':
            if ("USER".equals(name.toUpperCase()))
                return LOG_USER;
            else if ("UUCP".equals(name.toUpperCase()))
                return LOG_UUCP;
            break;
          case 'm':
          case 'M':
            if ("MAIL".equals(name.toUpperCase()))
                return LOG_MAIL;
            break;
          case 'a':
          case 'A':
            if ("AUTH".equals(name.toUpperCase()))
                return LOG_AUTH;
            break;
          case 'c':
          case 'C':
            if ("CRON".equals(name.toUpperCase()))
                return LOG_CRON;
            break;
          case 's':
          case 'S':
            if ("SYSLOG".equals(name.toUpperCase()))
                return LOG_SYSLOG;
            break;
          case 'n':
          case 'N':
            if ("NEWS".equals(name.toUpperCase()))
                return LOG_NEWS;
            break;
          case 'l':
          case 'L':
            if ("LPR".equals(name.toUpperCase()))
                return LOG_LPR;
            else if (name.toUpperCase().startsWith("LOCAL")) {
                int i = Integer.parseInt(name.substring(5));
                if (i >=0 && i <= 7)
                    return LOG_LOCAL0 + i;
            }
            break;
          default:
        }
        return -1;
    }

    public void close() {
        pm = null;
        pattern = null;
        packet = null;
        if (logger != null) {
            Map map;
            Object o;
            TextSubstitution[] tsub;
            for (String key : logger.keySet()) {
                map = logger.get(key);
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
            logger.clear();
            logger = null;
        }
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    protected void finalize() {
        close();
    }

    static {
        try {
            socket = new DatagramSocket();
        }
        catch (SocketException e) {
          throw(new IllegalArgumentException("failed to create syslog socket"));
        }
    }

    public static void main(String args[]) {
        EventSyslogger logger = null;
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("Name", "test");
        props.put("URI", "syslog://localhost:514");
        try {
            logger = new EventSyslogger(props);
            logger.log(new Event(Event.ERR, "This is a new test"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
