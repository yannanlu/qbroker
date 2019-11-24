package org.qbroker.json;

/* JSONParser.java - for parsing JSON data */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.io.StringReader;
import java.io.IOException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TextSubstitution;
import org.qbroker.json.JSON2Map;

/**
 * JSONParser parses a text into a Map according to the given pattern and
 * JSONpath. It takes a property Map to construct the instance. If Pattern is
 * defined in the property map, the JSON content will be parsed out of the
 * text via the given pattern first. Otherwise, the entire text is supposed to
 * be the JSON payload. If JSONPath map is defined in the property map, the
 * values of its keys will be used as the JSONPath expressions to retrive data
 * from the JSON payload. In this case, the returned Map will filled with the
 * key-value pairs specified in JSONPath map. Otherwise, the returned map will
 * contain the original parsing result.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class JSONParser {
    private Pattern pattern = null;
    private Perl5Matcher pm = null;
    private Map<String, String> expression = null;
    private TextSubstitution[] tsub = null;

    public JSONParser(Map props) {
        Object o;
        if ((o = props.get("JSONPath")) != null && o instanceof Map) {
            String key;
            Map ph = (Map) o;
            expression = new HashMap<String, String>();
            for (Object obj : ph.keySet()) {
                key = (String) obj;
                if (key == null || key.length() <= 0)
                    continue;
                if ((o = ph.get(key)) != null && o instanceof String &&
                    ((String) o).length() > 0)
                    expression.put(key, (String) o);
            }
        }

        if ((o = props.get("Pattern")) != null && o instanceof String &&
            ((String) o).length() > 0) try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            pattern = pc.compile((String) o);

            if ((o = props.get("Substitution")) != null && o instanceof List) {
                List list = (List) o;
                int n = list.size();
                tsub = new TextSubstitution[n];
                for (int i=0; i<n; i++) {
                    tsub[i] = null;
                    o = list.get(i);
                    if (o == null || !(o instanceof String))
                        continue;
                    tsub[i] = new TextSubstitution((String) o);
                }
            }
        }
        catch(Exception e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public Map parse(String buffer) throws IOException {
        Object o;
        StringReader sr = null;

        if (buffer == null || buffer.length() <= 0)
            return null;
        if (pattern == null)
            sr = new StringReader(buffer);
        else if (pm.contains(buffer, pattern)) {
            MatchResult mr = pm.getMatch();
            String text = mr.group(1);
            if (tsub != null) {
                for (int i=0; i<tsub.length; i++)
                    if (tsub[i] != null)
                        text = tsub[i].substitute(text);
            }
            sr = new StringReader(text);
        }

        if (sr == null)
            return null;

        Map ph = (Map) JSON2Map.parse(sr);
        sr.close();

        if (expression != null && expression.size() > 0) {
            String path;
            Map map = new HashMap();
            for (String key : expression.keySet()) {
                if (key == null || key.length() <= 0)
                    continue;
                path = expression.get(key);
                if (path == null || path.length() <= 0)
                    continue;
                o = JSON2Map.get(ph, path);
                if (o == null)
                    continue;
                else if (o instanceof String)
                    map.put(key, (String) o);
                else if (o instanceof Map)
                    map.put(key, JSON2Map.toJSON((Map) o, null, null));
                else if (o instanceof List)
                    map.put(key, JSON2Map.toJSON((List) o, null, null));
                else
                    map.put(key, o.toString());
            }
            ph.clear();
            return map;
        }
        else {
            String key;
            for (Object obj : ph.keySet().toArray()) { // normalize
                key = (String) obj;
                if (key == null || key.length() <= 0) {
                    ph.remove(obj);
                    continue;
                }
                o = ph.get(key);
                if (o == null) {
                    ph.remove(obj);
                    continue;
                }
                else if (o instanceof String)
                    continue;
                else if (o instanceof Map)
                    ph.put(key, JSON2Map.toJSON((Map) o, null, null));
                else if (o instanceof List)
                    ph.put(key, JSON2Map.toJSON((List) o, null, null));
                else
                    ph.put(key, o.toString());
            }
            return ph;
        }
    }
}
