package org.qbroker.event;

/* EventTrapSender.java - an EventAction to send events via SNMP traps */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.net.SNMPConnector;
import org.qbroker.event.Event;
import org.qbroker.event.EventAction;

/**
 * EventTrapSender implements EventAction and sends multiple traps to an SNMP
 * management station.  Currently, it supports v1 and v2c traps.
 *<br/><br/>
 * This is MT-Safe.  It requires Java 1.4 or above due to SNMP4J.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventTrapSender extends SNMPConnector implements EventAction {
    private String name;
    private String site;
    private String type;
    private String description;
    private String category;
    private long serialNumber;
    private AssetList ruleList;
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private int genericTrap = 6, specificTrap = 1, version = 1, debug = 0;

    public EventTrapSender(Map props) {
        super(props);
        int i, k, n, rid;
        Object o;
        Map<String, Object> h = null;
        List<Object> list;
        Template template;
        TextSubstitution[] msgSub = null;
        String[] fields;
        Iterator iter;
        String key;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        site = (String) props.get("Site");
        category = (String) props.get("Category");
        if (props.get("Type") != null)
            type = (String) props.get("Type");
        else
            type = "EventTrapSender";
        template = new Template("__hostname__, __HOSTNAME__", "__[^_]+__");
        if ((o = props.get("Description")) != null)
            description = EventUtils.substitute((String) o, template);
        else
            description = "send snmp traps";

        ruleList = new AssetList("SNMPTrap", 64);
        if ((o = props.get("TrapData")) != null && o instanceof List) {
            list = Utils.cloneProperties((List) o);
            k = 0;
            n = list.size();
            for (i=n-1; i>=0; i--) {
                o = list.get(i);
                if (o == null || !(o instanceof Map)) {
                    list.remove(i);
                    continue;
                }
                h = Utils.cloneProperties((Map) o);
                list.set(i, h);
                if ((o = h.get("Value")) != null) { // may contain variables
                    template = new Template((String) o);
                    fields = template.getAllFields();
                    if (fields != null && fields.length > 0) { // variables
                        h.put("Value", template);
                        h.put("Fields", fields);
                        k += fields.length;
                    }
                }
            }
            rid = ruleList.add("Default", new long[]{n, k}, list);
            if (k > 0 && rid >= 0) { // variables may need to be formatted
                o = props.get("Substitution");
                if (o != null && o instanceof List) { // save to fisrt map
                    msgSub = EventUtils.initSubstitutions((List) o);
                    h.put("MsgSub", msgSub);
                }
            }
        }
        else if ((o = props.get("Default")) != null && o instanceof Map) {
            o = ((Map) o).get("Substitution");
            if (o != null && o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
            else if ((o = props.get("Substitution")) != null &&
                o instanceof List)
                msgSub = EventUtils.initSubstitutions((List) o);
        }

        iter = props.keySet().iterator();
        while (iter.hasNext()) { // looking for Map for types
            key = (String) iter.next();
            if (key == null || key.length() == 0)
                continue;
            if ((o = props.get(key)) == null || !(o instanceof Map))
                continue;
            if ("StringProperty".equals(key) || "ActiveTime".equals(key) ||
                "CopiedProperty".equals(key))
                continue;
            Map ph = (Map) o;
            if (ph.containsKey("Option")) // for option
                continue;
            if ((o = ph.get("TrapData")) == null || !(o instanceof List))
                continue;
            list = Utils.cloneProperties((List) o);
            n = list.size();
            k = 0;
            for (i=n-1; i>=0; i--) {
                o = list.get(i);
                if (o == null || !(o instanceof Map)) {
                    list.remove(i);
                    continue;
                }
                h = Utils.cloneProperties((Map) o);
                list.set(i, h);
                if ((o = h.get("Value")) != null) { // may contain variables
                    template = new Template((String) o);
                    fields = template.getAllFields();
                    if (fields != null && fields.length > 0) { // variables
                        h.put("Value", template);
                        h.put("Fields", fields);
                        k += fields.length;
                    }
                }
            }
            if ((rid = ruleList.getID(key)) >= 0)
                ruleList.remove(rid);

            rid = ruleList.add(key, new long[]{n, k}, list);
            if (k > 0 && rid >= 0) { // variables may need to be formatted
                o = ph.get("Substitution");
                if (o != null && o instanceof List) // save to fisrt map
                    h.put("MsgSub", EventUtils.initSubstitutions((List) o));
                else if (o == null) // use the default
                    h.put("MsgSub", msgSub);
            }
        }
        if (ruleList.size() <= 0)
            throw(new IllegalArgumentException(name + ": no TrapData defined"));

        if ((o = props.get("GenericTrap")) != null)
            genericTrap = Integer.parseInt((String) o);

        if (genericTrap < 0 || genericTrap > 6)
            genericTrap = 6;

        if ((o = props.get("SpecificTrap")) != null)
            specificTrap = Integer.parseInt((String) o);

        if ((o = props.get("Version")) != null && !"1".equals((String) o))
            version = 2;

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

    private int send(Event event, List list) throws IOException {
        int i, j, n;
        Object o;
        Map map, change = null;
        HashMap attr;
        String text, key, value;
        String[] allFields;
        String[][] data;
        Template template = null;
        TextSubstitution[] msgSub = null;

        if (event == null || list == null || list.size() <= 0)
            return -1;

        map = (Map) list.get(0);
        if (map == null || map.size() <= 0)
            return -1;

        msgSub = (TextSubstitution[]) map.get("MsgSub");
        if (msgSub != null) {
            change = EventUtils.getChange(event, msgSub);
            if (change != null && change.size() <= 0)
                change = null;
        }

        attr = event.attribute;
        n = list.size();
        data = new String[n][3];
        for (i=0; i<n; i++) { // loop thru all data set
            o = list.get(i);
            if (o == null || !(o instanceof Map)) {
                data[i][0] = null;
                data[i][1] = null;
                continue;
            }
            map = (Map) o;
            data[i][0] = (String) map.get("OID");
            data[i][1] = (String) map.get("Type");
            o = map.get("Value");
            if (!(o instanceof Template)) { // no variable defined
                data[i][2] = (String) o;
                continue;
            }
            template = (Template) o;
            allFields = (String[]) map.get("Fields");
            text = template.copyText();
            for (j=0; j<allFields.length; j++) {
                key = allFields[j];
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
                else {
                    text = template.substitute(key, "", text);
                }
            }
            data[i][2] = text;
        }
        if (change != null)
            change.clear();

        if (version == 1)
            sendTrap(null, null, genericTrap, specificTrap, data);
        else
            snmpNotify(null, data);

        return n;
    }

    public synchronized void invokeAction(long currentTime, Event event) {
        int n = 0;
        List list;
        String eventType;
        String priorityName;
        if (event == null)
            return;

        priorityName = Event.priorityNames[event.getPriority()];
        if (pattern != null && !pm.contains(priorityName, pattern))
            return;
        serialNumber ++;

        eventType = (String) event.attribute.get("type");
        if (eventType == null || eventType.length() == 0)
            eventType = "Default";

        list = (List) ruleList.get(eventType);
        if (list == null)
            list = (List) ruleList.get("Default");

        if (list != null && list.size() > 0) try {
            n = send(event, list);
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to send trap for " +
                eventType + ": " + Event.traceStack(e)).send();
            return;
        }

        if (debug > 0 && n > 0)
            new Event(Event.DEBUG, name + ": sent " + n +
                " traps to " + uri).send();
    }

    public String getName() {
        return name;
    }

    public void close() {
        pm = null;
        pattern = null;
        if (ruleList != null) {
            List list = (List) ruleList.get("Default");
            if (list != null) {
                Map map;
                for (Object obj : list) {
                    if (obj != null && obj instanceof Map) {
                        map = (Map) obj;
                        obj = map.remove("Value");
                        if (obj != null && obj instanceof Template)
                            ((Template) obj).clear();
                        map.clear();
                    }
                }
                list.clear();
            }
            ruleList.clear();
            ruleList = null;
        }
    }

    protected void finalize() {
        close();
    }
}
