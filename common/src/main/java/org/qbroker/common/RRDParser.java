package org.qbroker.common;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/**
 * RRDParser parses the RRDTool fetch result into a JSON document.
 * By default, the returned JSON is in matrix format with the key name on
 * each column. If the type is defined and it is not "1", the returned JSON
 * will be in array format.
 *<br>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class RRDParser {
    private int resultType = 1;
    public RRDParser(String type) {
        if (type != null && !"1".equals(type))
            resultType = 0;
    }

    public Map parse(String text) {
        Map map;
        StringBuffer strBuf;
        String[] lines, keys, values;
        int i, j, k, n;
        if (text == null || text.length() <= 0)
            return null;
        lines = text.split("\n");
        n = lines.length;
        if (n > 2 && lines[n-1].startsWith("OK")) {
            String str;
            StringBuffer line;
            map = new HashMap();
            strBuf = new StringBuffer();
            keys = lines[0].split("\\s+");
            k = keys.length;
            keys[0] = "rrd_time";
            if (resultType == 1) { // matrix
                strBuf.append("[");
                for (i=2; i<n; i++) {
                    values = lines[i].split(" ");
                    k = values.length;
                    if (k > 1 && "nan".equals(values[1]))
                        continue;
                    line = new StringBuffer();
                    j = values[0].length();
                    str = values[0].substring(0, j-1);
                    line.append("{\"" + keys[0] + "\":" + str);
                    for (j=1; j<k; j++)
                        line.append(",\"" + keys[i] + "\":" + values[i]);
                    line.append("}");
                    if (i > 2)
                        strBuf.append("," + line.toString());
                    else
                        strBuf.append(line.toString());
                }
                strBuf.append("]");
                map.put("body", "{\"result\":" + strBuf.toString() + "}");
            }
            else { // array
                List list = new ArrayList();
                for (i=2; i<n; i++) {
                    values = lines[i].split(" ");
                    k = values.length;
                    if (k > 1 && "nan".equals(values[1]))
                        continue;
                    j = values[0].length();
                    values[0] = values[0].substring(0, j-1);
                    list.add(values);
                }
                n = list.size();
                strBuf.append("{");
                for (j=0; j<k; j++) {
                    line = new StringBuffer();
                    line.append("[");
                    for (i=0; i<n; i++) {
                        values = (String[]) list.get(i);
                        if (i > 0)
                            line.append(",");
                        line.append(values[j]);
                    }
                    line.append("]");
                    if (j > 0)
                        strBuf.append("\n,");
                    strBuf.append("\"" + keys[j] + "\":" + line.toString());
                }
                strBuf.append("}");
                map.put("body", strBuf.toString());
                list.clear();
            }
            return map;
        }
        else
            return null;
    }
}
