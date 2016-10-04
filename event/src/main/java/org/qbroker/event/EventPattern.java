package org.qbroker.event;

/* EventPattern.java is a Perl5 pattern to match with events */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.event.Event;
import org.qbroker.common.DataSet;

/**
 * EventPattern implements EventFilter for Perl5 pattern match with events
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventPattern implements EventFilter {
    private Map<String, Object> eventPattern;
    private String[] keys;
    private int[] types;
    private Perl5Matcher pm;
    private int size;

    public EventPattern(Map pattern) throws MalformedPatternException {
        String key;
        Object o;
        Perl5Compiler pc = new Perl5Compiler();
        eventPattern = new HashMap<String, Object>();
        Iterator iter = pattern.keySet().iterator();
        while (iter.hasNext()) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = pattern.get(key);
            if (o == null)
                eventPattern.put(key, null);
            else if (o instanceof List)
                eventPattern.put(key, new DataSet((List) o));
            else
                eventPattern.put(key, pc.compile((String) o));
        }
        size = eventPattern.size();
        keys = eventPattern.keySet().toArray(new String[size]);
        types = new int[size];
        for (int i=0; i<size; i++) {
            types[i] = 0;
            o = eventPattern.get(keys[i]);
            if (o != null && o instanceof DataSet)
                types[i] = ((DataSet) o).getDataType();
        }
        pm = new Perl5Matcher();
    }

    public boolean evaluate(long currentTime, Event event) {
        int i=0;
        String key, value;
        HashMap attr;
        Pattern pattern;
        DataSet dataSet;
        if (event == null)
            return false;

        attr = event.attribute;
        for (i=0; i<size; i++) {
            key = keys[i];
            if (types[i] > 0) {
                dataSet = (DataSet) eventPattern.get(key);
                if (dataSet != null) {
                    boolean b = false;
                    if (!attr.containsKey(key)) {
                        b = false;
                    }
                    else try {
                        value = (String) attr.get(key);
                        if (types[i] == DataSet.DATA_LONG)
                            b = dataSet.contains(Long.parseLong(value));
                        else if (types[i] == DataSet.DATA_DOUBLE)
                            b = dataSet.contains(Double.parseDouble(value));
                        else
                            b = false;
                    }
                    catch (Exception e) {
                        b = false;
                    }
                    if (!b)
                        return false;
                }
                else if (attr.containsKey(key)) {
                    return false;
                }
            }
            else {
                pattern = (Pattern) eventPattern.get(key);
                if (pattern != null) {
                    if (attr.containsKey(key)) {
                        value = (String) attr.get(key);
                        if (!pm.contains(value, pattern))
                            return false;
                    }
                    else {
                        return false;
                    }
                }
                else if (attr.containsKey(key)) {
                    return false;
                }
            }
        }

        return true;
    }

    public void clear() {
        if (eventPattern != null) {
            eventPattern.clear();
            eventPattern = null;
        }
        pm = null;
    }

    protected void finalize() {
        clear();
    }
}
