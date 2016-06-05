package org.qbroker.json;

/* JSONParser.java - for parsing JSON data */

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import java.io.StringReader;
import org.qbroker.json.JSON2Map;

/**
 * JSONParser parses JSON data to a Map.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class JSONParser {
    public JSONParser(String name) {
    }

    public Map parse(String buffer) throws IOException {
        String key;
        Object o;
        if (buffer == null || buffer.length() <= 0)
            return null;
        StringReader sr = new StringReader(buffer);
        Map ph = (Map) JSON2Map.parse(sr);
        sr.close();

        Iterator iter = ph.keySet().iterator();
        while (iter.hasNext()) { // normalize
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = ph.get(key);
            if (o instanceof String)
                continue;
            else if (o instanceof List)
                ph.put(key, JSON2Map.toJSON((Map) o, null, null));
            else if (o instanceof List)
                ph.put(key, JSON2Map.toJSON((List) o, null, null));
            else
                ph.put(key, o.toString());
        }

        return ph;
    }
}
