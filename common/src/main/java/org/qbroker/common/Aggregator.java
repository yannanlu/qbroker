package org.qbroker.common;

/* Aggregator.java - Aggregator for aggregations on map data */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DataSet;
import org.qbroker.common.Utils;

/**
 * Aggregator contains a list of stateless operations for aggregations
 * on the properties retrieved from a Map.
 *<br><br>
 * In case of average, AveragedOver is required.  Its referenced name should be
 * defined before it. In case of standard deviation, VarianceOf is requires.
 * Its referenced name should be defined before it. In case of max or min, the 
 * name of the field does not have to exist in the incoming message.  But the
 * referenced name by MaxOf or MinOf should be defined before it. If TimePattern
 * is defined for max or min, Aggregation will parse the value to get the
 * timestamp for comparisons.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class Aggregator {
    private String name;
    private AssetList aggrList;
    private long serialNumber = 0;
    public final static int AGGR_NONE = -1;
    public final static int AGGR_COUNT = 0;
    public final static int AGGR_SUM = 1;
    public final static int AGGR_MIN = 2;
    public final static int AGGR_MAX = 3;
    public final static int AGGR_APPEND = 4;
    public final static int AGGR_FIRST = 5;
    public final static int AGGR_LAST = 6;
    public final static int AGGR_AVG = 7;
    public final static int AGGR_STD = 8;
    public final static int AGGR_MERGE = 9;
    public final static int AGGR_UNION = 10;
    public final static int AGGR_XUNION = 11;
    public final static int AGGR_JOIN = 12;
    public final static int AGGR_END = 12;
    private final static int AGGR_DSUM = 13;
    private final static int AGGR_DMIN = 14;
    private final static int AGGR_DMAX = 15;
    private final static int AGGR_TMIN = 16;
    private final static int AGGR_TMAX = 17;
    private static final Map<String, String> optionMap =
        new HashMap<String, String>();

    static {
        optionMap.put("count", String.valueOf(AGGR_COUNT));
        optionMap.put("sum", String.valueOf(AGGR_SUM));
        optionMap.put("min", String.valueOf(AGGR_MIN));
        optionMap.put("max", String.valueOf(AGGR_MAX));
        optionMap.put("append", String.valueOf(AGGR_APPEND));
        optionMap.put("first", String.valueOf(AGGR_FIRST));
        optionMap.put("last", String.valueOf(AGGR_LAST));
        optionMap.put("avg", String.valueOf(AGGR_AVG));
        optionMap.put("std", String.valueOf(AGGR_STD));
        optionMap.put("merge", String.valueOf(AGGR_MERGE));
        optionMap.put("union", String.valueOf(AGGR_UNION));
        optionMap.put("xunion", String.valueOf(AGGR_XUNION));
        optionMap.put("join", String.valueOf(AGGR_JOIN));
        optionMap.put("dsum", String.valueOf(AGGR_DSUM));
        optionMap.put("dmin", String.valueOf(AGGR_DMIN));
        optionMap.put("dmax", String.valueOf(AGGR_DMAX));
        optionMap.put("tmin", String.valueOf(AGGR_TMIN));
        optionMap.put("tmax", String.valueOf(AGGR_TMAX));
    }

    @SuppressWarnings("unchecked")
    public Aggregator(String name, List list) {
        Object o;
        Browser browser;
        StringBuffer strBuf = new StringBuffer();
        Map<String, Object> map;
        Map ph;
        List<String> pl;
        String key, str;
        long[] aggrInfo;
        int i, j, n, id, option;

        if (name == null || name.length() <= 0)
            throw(new IllegalArgumentException("name is not well defined"));
        this.name = name;

        if (list == null || list.size() <= 0)
            throw(new IllegalArgumentException("list is empty"));

        n = list.size();
        aggrList = new AssetList(name, n);
        for (i=0; i<n; i++) {
            ph = (Map) list.get(i);
            key = (String) ph.get("FieldName");
            if (key == null || key.length() <= 0)
                continue;
            str = (String) ph.get("Operation");
            if (str != null && optionMap.containsKey(str.toLowerCase())) {
                 str = (String) optionMap.get(str.toLowerCase());
            }
            else if (str != null) { // wrong operation
                throw(new IllegalArgumentException(name + ": operation " +
                    str + " on " + key + " not supported"));
            }
            else { // assume the default operation of none
                str = String.valueOf(AGGR_NONE);
            }
            map = new HashMap<String, Object>();
            map.put("Operation", str);
            option = Integer.parseInt(str);
            switch (option) {
              case AGGR_COUNT:
                str = (String) ph.get("DefaultValue");
                if (str != null && str.length() > 0)
                    map.put("DefaultValue", str);
                str = (String) ph.get("Condition");
                if (str != null && str.length() > 0) {
                    pl = new ArrayList<String>();
                    pl.add(str);
                    map.put("Condition", new DataSet(pl));
                    if (!map.containsKey("DefaultValue"))
                        map.put("DefaultValue", "0");
                }
                if (!map.containsKey("DefaultValue"))
                    map.put("DefaultValue", "1");
                break;
              case AGGR_SUM:
              case AGGR_MIN:
              case AGGR_MAX:
                if (option == AGGR_MAX) {
                    str = (String) ph.get("MaxOf");
                    if (str != null && !key.equals(str) &&
                        aggrList.containsKey(str)) {
                        Map h = (Map) aggrList.get(str);
                        map.put("MaxOf", str);
                        str = (String) h.get("DefaultValue");
                    }
                    else
                        str = (String) ph.get("DefaultValue");
                    if ((o = ph.get("TimePattern")) != null &&
                        o instanceof String)
                        map.put("DateFormat", new SimpleDateFormat((String) o));
                }
                else if (option == AGGR_MIN) {
                    str = (String) ph.get("MinOf");
                    if (str != null && !key.equals(str) &&
                        aggrList.containsKey(str)) {
                        Map h = (Map) aggrList.get(str);
                        map.put("MinOf", str);
                        str = (String) h.get("DefaultValue");
                    }
                    else
                        str = (String) ph.get("DefaultValue");
                    if ((o = ph.get("TimePattern")) != null &&
                        o instanceof String)
                        map.put("DateFormat", new SimpleDateFormat((String) o));
                }
                else
                    str = (String) ph.get("DefaultValue");
                if (str == null || str.length() <= 0) {
                    map.put("DefaultValue", "0");
                }
                else try {
                    double y = Double.parseDouble(str);
                    if (str.indexOf(".") >= 0) { // double
                        map.put("DefaultValue", String.valueOf(y));
                        option += AGGR_END;
                    }
                    else { // long
                        map.put("DefaultValue", String.valueOf((long) y));
                        if (option != AGGR_SUM && map.containsKey("DateFormat"))
                            option += AGGR_END + 2; // for time
                    }
                }
                catch (NumberFormatException e) {
                    throw(new IllegalArgumentException(name+
                        " failed to parse DefaultValue "+ str + " on " +
                        key + ": " + e.toString()));
                }
                break;
              case AGGR_AVG:
                str = (String) ph.get("DefaultValue");
                if (str == null || str.length() <= 0) {
                    map.put("DefaultValue", "0.0");
                }
                else try {
                    double y = Double.parseDouble(str);
                    map.put("DefaultValue", String.valueOf(y));
                }
                catch (NumberFormatException e) {
                    throw(new IllegalArgumentException(name+
                        " failed to parse DefaultValue "+ str + " on " +
                        key + ": " + e.toString()));
                }
                str = (String) ph.get("AveragedOver");
                if (str != null) { // make sure str defined already
                    long[] meta = aggrList.getMetaData(str);
                    Map h = (Map) aggrList.get(str);
                    if (h != null && meta != null && meta.length > 0) {
                        // make sure its default value is set to 0 or 1
                        if (meta[0] == AGGR_SUM)
                            h.put("DefaultValue", "0");
                        else if (meta[0] == AGGR_DSUM)
                            h.put("DefaultValue", "0.0");
                        else if (meta[0] == AGGR_COUNT)
                            h.put("DefaultValue", "1");
                        map.put("AveragedOver", str);
                    }
                    else { // str has not defined yet
                        throw(new IllegalArgumentException(name+
                            " failed to find AveragedOver " + str +
                            " for "+ key));
                    }
                }
                else {
                    throw(new IllegalArgumentException(name+
                        ": AveragedOver not defined for "+ key));
                }
                break;
              case AGGR_STD:
                str = (String) ph.get("DefaultValue");
                if (str == null || str.length() <= 0) {
                    map.put("DefaultValue", "0.0");
                }
                else try {
                    double y = Double.parseDouble(str);
                    map.put("DefaultValue", String.valueOf(y));
                }
                catch (NumberFormatException e) {
                    throw(new IllegalArgumentException(name+
                        " failed to parse DefaultValue "+ str + " on " +
                        key + ": " + e.toString()));
                }
                str = (String) ph.get("VarianceOf");
                if (str != null) { // make sure str defined already
                    long[] meta = aggrList.getMetaData(str);
                    Map h = (Map) aggrList.get(str);
                    if (h != null && meta != null && meta.length > 0 &&
                        meta[0] == AGGR_AVG) {
                        map.put("VarianceOf", str);
                        map.put("DefaultValue", h.get("DefaultValue"));
                    }
                    else { // str has not defined yet
                        throw(new IllegalArgumentException(name+
                            " failed to find VarianceOf " + str+" for "+ key));
                    }
                }
                else {
                    throw(new IllegalArgumentException(name+
                        " VarianceOf not defined for "+ key));
                }
                break;
              case AGGR_APPEND:
                str = (String) ph.get("DefaultValue");
                if (str == null)
                    map.put("DefaultValue", "");
                else
                    map.put("DefaultValue", str);
                str = (String) ph.get("Delimiter");
                if (str != null && str.length() > 0)
                    map.put("Delimiter", str);
                break;
              default:
                str = (String) ph.get("DefaultValue");
                if (str == null)
                    map.put("DefaultValue", "");
                else
                    map.put("DefaultValue", str);
                break;
            }

            aggrInfo = new long[] {option, 0L};
            id = aggrList.add(key, aggrInfo, map);
        }
    }

    /**
     * It initializes the aggregation report and aggregates the fisrt incoming
     * report according to the aggregation operations.  Upon success, it returns
     * 0.  Otherwise, it returns -1 to indicate errors.
     */
    @SuppressWarnings("unchecked")
    public int initialize(long currentTime, Map report, Map rpt) {
        int i, k, n, option;
        Map map;
        DataSet dataSet;
        long[] meta, aggrInfo;
        long x;
        double y;
        String key, value, str = null;
        String[] result;

        k = 0;
        n = aggrList.size();
        result = new String[n];
        for (i=0; i<n; i++) {
            result[i] = null;
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            aggrInfo = aggrList.getMetaData(i);
            option = (int) aggrInfo[0];
            if (option == AGGR_NONE)
                continue;
            map = (Map) aggrList.get(i);
            try {
                value = (String) report.get(key);

                if (option == AGGR_MIN || option == AGGR_MAX ||
                    option == AGGR_DMIN || option == AGGR_DMAX ||
                    option == AGGR_TMIN || option == AGGR_TMAX ||
                    option == AGGR_FIRST) {
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && (k = aggrList.getID(str)) >= 0)
                            value = result[k];
                    }
                    else if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && (k = aggrList.getID(str)) >= 0)
                            value = result[k];
                    }

                    if (value == null) // bypass setting the default value
                        continue;
                }
                else if (option == AGGR_TMIN || option == AGGR_TMAX) {
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && (k = aggrList.getID(str)) >= 0)
                            value = result[k];
                    }
                    else if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && (k = aggrList.getID(str)) >= 0)
                            value = result[k];
                    }

                    if (value == null) // bypass setting the default value
                        continue;
                }
                else if (option == AGGR_COUNT) {
                    if ((dataSet=(DataSet) map.get("Condition")) != null) {
                        if (value == null) // no such value, use default
                            value = (String) map.get("DefaultValue");
                        if (dataSet.getDataType() == DataSet.DATA_DOUBLE) {
                            y = Double.parseDouble(value);
                            value = (dataSet.contains(y)) ? "1" : "0";
                        }
                        else {
                            x = Long.parseLong(value);
                            value = (dataSet.contains(x)) ? "1" : "0";
                        }
                    }
                    else {
                        value = "1";
                    }
                }

                if (option == AGGR_AVG) { // for average
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    y = Double.parseDouble(value);
                    str = (String) map.get("AveragedOver");
                    if (str != null && str.length() > 0) {
                        double d;
                        k = aggrList.getID(str);
                        d = Double.parseDouble(result[k]);
                        if (d == 0.0) { // bad data
                            throw(new IllegalArgumentException(name+
                                ": got zero on "+ str +" in average for "+key));
                        }
                        else
                            y /= d;
                        result[i] = String.valueOf(y);
                    }
                }
                else if (option == AGGR_STD) { // for standard deviation
                    result[i] = "0";
                }
                else {
                    if (value == null) // use the default
                        value = (String) map.get("DefaultValue");
                    result[i] = new String(value);
                }
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name+
                    " failed to init property of "+ key + ": "+ e.toString()));
            }
        }

        // save the updated data to report
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            rpt.put(key, result[i]);
        }
        serialNumber ++;
        return 0;
    }

    /**
     * It aggregates the incoming data report with the aggregation report
     * according to the aggregation options.  It extracts data from the
     * incoming report and aggregates the values into the cached report.
     * Upon success, it returns 0.  Otherwise, it returns -1 to indicate errors.
     */
    @SuppressWarnings("unchecked")
    public int aggregate(long currentTime, Map report, Map rpt) {
        int i, k, n, option;
        Map map;
        DataSet dataSet;
        DateFormat dateFormat = null;
        long[] meta;
        long x;
        double y;
        String key = null, value, str = null;
        String[] result;

        k = 0;
        n = aggrList.size();
        result = new String[n];
        for (i=0; i<n; i++) try {
            result[i] = null;
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            meta = aggrList.getMetaData(i);
            option = (int) meta[0];
            if (option == AGGR_NONE)
                continue;
            map = (Map) aggrList.get(i);

            switch (option) {
              case AGGR_COUNT:
                if ((dataSet = (DataSet) map.get("Condition")) != null) {
                    value = (String) report.get(key);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    if (dataSet.getDataType() == DataSet.DATA_DOUBLE) {
                        y = Double.parseDouble(value);
                        x = (dataSet.contains(y)) ? 1 : 0;
                    }
                    else {
                        x = Long.parseLong(value);
                        x = (dataSet.contains(x)) ? 1 : 0;
                    }
                }
                else { // use default condition to check existence
                    x = 1;
                }
                value = (String) rpt.get(key);
                result[i] = String.valueOf(x + Long.parseLong(value));
                break;
              case AGGR_SUM:
                value = (String) rpt.get(key);
                if (value == null) // no such value
                    break;
                x = Long.parseLong(value);
                value = (String) report.get(key);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                x += Long.parseLong(value);
                result[i] = String.valueOf(x);
                break;
              case AGGR_AVG:
                value = (String) rpt.get(key);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                str = (String) map.get("AveragedOver");
                if (str != null && str.length() > 0) {
                    double d, dd;
                    k = aggrList.getID(str);
                    value = (String) rpt.get(str);
                    d = Double.parseDouble(value);
                    dd = Double.parseDouble(result[k]);
                    value = (String) report.get(key);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    if (dd == 0.0 || dd == d) // zero sum
                        throw(new IllegalArgumentException(name +
                            " got zero on " + str + " in average for " + key));
                    else {
                        y = (y * d + Double.parseDouble(value)) / dd;
                        result[i] = String.valueOf(y);
                    }
                }
                break;
              case AGGR_STD:
                str = (String) map.get("VarianceOf");
                if (str != null && str.length() > 0) { // for averaged
                    Map h;
                    double s, ss, yy;
                    k = aggrList.getID(str);
                    yy = Double.parseDouble(result[k]);
                    value = (String) rpt.get(str);
                    y = Double.parseDouble(value);
                    value = (String) report.get(str);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    ss = Double.parseDouble(value);
                    h = (Map) aggrList.get(k);
                    str = (String) h.get("AveragedOver");
                    if (str != null && str.length() > 0) { // for count or sum
                        double d, dd;
                        k = aggrList.getID(str);
                        value = (String) rpt.get(str);
                        d = Double.parseDouble(value);
                        dd = Double.parseDouble(result[k]);
                        value = (String) rpt.get(key);
                        s = Double.parseDouble(value);
                        if (dd == 0.0 || dd == d) // zero sum
                            throw(new IllegalArgumentException(name +
                                ": got zero on "+ str+" in average for "+key));
                        else {
                            y = Math.sqrt(((s*s + y*y) * d + ss*ss) / dd);
                            if (y >= yy) {
                                y = Math.sqrt((y - yy)*(y + yy));
                                result[i] = String.valueOf(y);
                            }
                            else
                                throw(new IllegalArgumentException(name +
                                    ": got negtive deviation of " + (y-yy) +
                                    " for " + key));
                        }
                    }
                }
                break;
              case AGGR_MIN:
                value = (String) report.get(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = (String) rpt.get(key);
                if (value != null && x < Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_MAX:
                value = (String) report.get(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = (String) rpt.get(key);
                if (value != null && x > Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_DSUM:
                value = (String) rpt.get(key);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                value = (String) report.get(key);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                y += Double.parseDouble(value);
                result[i] = String.valueOf(y);
                break;
              case AGGR_DMIN:
                value = (String) report.get(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = (String) rpt.get(key);
                if (value != null && y < Double.parseDouble(value))//update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_DMAX:
                value = (String) report.get(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = (String) rpt.get(key);
                if (value != null && y > Double.parseDouble(value))//update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_TMIN:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = (String) report.get(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = (String) rpt.get(key);
                if (str != null && x < dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_TMAX:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = (String) report.get(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = (String) report.get(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = (String) rpt.get(key);
                if (str != null && x > dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_APPEND:
                str = (String) rpt.get(key);
                if (str == null) // no such value
                    str = "";
                value = (String) report.get(key);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                if (map.containsKey("Delimiter")) {
                    if (str.length() > 0)
                        value = (String) map.get("Delimiter") + value;
                }
                result[i] = str + value;
                break;
              case AGGR_FIRST:
                break;
              case AGGR_LAST:
                result[i] = (String) report.get(key);
                break;
              default:
                break;
            }
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to aggreate report for "+ key + ": " + e.toString()));
        }

        // save the updated data to report
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            rpt.put(key, result[i]);
        }
        // update internal count
        serialNumber ++;

        return 0;

    }

    public String getName() {
        return name;
    }

    public int getSize() {
        return aggrList.size();
    }

    public boolean containsKey(String key) {
        return aggrList.containsKey(key);
    }

    public String getKey(int id) {
        return aggrList.getKey(id);
    }

    public Browser browser() {
        return aggrList.browser();
    }

    public void clear() {
        aggrList.clear();
    }

    protected void finalize() {
        clear();
    }
}
