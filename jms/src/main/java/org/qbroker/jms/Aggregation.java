package org.qbroker.jms;

/* Aggregation.java - Aggregation operations */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Date;
import java.io.Reader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.DataSet;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.event.Event;

/**
 * Aggregation contains a list of stateless operations for aggregations
 * on the properties retrieved from either JMS messages or Events. The
 * aggregation process will not touch the message body due to its different
 * data types. For aggregation on the message body, Aggregation provides
 * a bunch of assistant methods, such as merge(), and stores the aggregation
 * map with various policies and operations. Together, they can be used to
 * aggregate the message body externally.
 *<br/><br/>
 * In case of average, AveragedOver is required.  Its referenced name should be
 * defined before it. In case of standard deviation, VarianceOf is requires.
 * Its referenced name should be defined before it. In case of max or min, the 
 * name of the field does not have to exist in the incoming message.  But the
 * referenced name by MaxOf or MinOf should be defined before it. If TimePattern
 * is defined for max or min, Aggregation will parse the value to get the
 * timestamp for comparisons.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class Aggregation {
    private String name;
    private AssetList aggrList;
    private Map bodyMap;
    private long serialNumber = 0;
    private int bodyOperation = AGGR_NONE;
    private int bodyType = Utils.RESULT_TEXT, dataType = -1;
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
    public Aggregation(String name, List list) {
        Object o;
        Browser browser;
        StringBuffer strBuf = new StringBuffer();
        Map<String, Object> map;
        Map ph;
        Iterator iter;
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
            if (key == null || key.length() == 0)
                key = String.valueOf(i);
            else if (key.startsWith("JMS")) {
                key = MessageUtils.getPropertyID(key);
            }
            if ("body".equals(key)) { // no aggregation on body
                new Event(Event.WARNING, name + ": no aggregation on " + key +
                    ", skip it for now").send();
                str = null;
            }
            else
                str = (String) ph.get("Operation");
            if (str != null && optionMap.containsKey(str.toLowerCase())) {
                 str = (String) optionMap.get(str.toLowerCase());
            }
            else if (str != null) { // wrong operation
                new Event(Event.ERR, name + ": operation " + str +
                    " on " + key + " not supported, ignore it").send();
                str = String.valueOf(AGGR_NONE);
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
                    new Event(Event.ERR, name+": failed to parse DefaultValue "+
                        str + " on " + key + ", set it to 0").send();
                    map.put("DefaultValue", "0");
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
                    new Event(Event.ERR, name+": failed to parse DefaultValue "+
                        str + " on " + key + ", set it to 0.0").send();
                    map.put("DefaultValue", "0.0");
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
                        new Event(Event.ERR, name + ": " +
                            " failed to find AveragedOver " + str +
                            " for "+ key +", so disable it").send();
                        option = AGGR_NONE;
                    }
                }
                else {
                    new Event(Event.ERR,name+": AveragedOver not defined for "+
                        key +", so disable it").send();
                    option = AGGR_NONE;
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
                    new Event(Event.ERR, name+": failed to parse DefaultValue "+
                        str + " on " + key + ", set it to 0.0").send();
                    map.put("DefaultValue", "0.0");
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
                        new Event(Event.ERR, name + ": " +
                            " failed to find VarianceOf " + str +
                            " for "+ key +", so disable it").send();
                        option = AGGR_NONE;
                    }
                }
                else {
                    new Event(Event.ERR,name+": VarianceOf not defined for "+
                        key +", so disable it").send();
                    option = AGGR_NONE;
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

        bodyMap = new HashMap();
        bodyOperation = AGGR_NONE;
    }

    /**
     * It initializes the aggregation event and aggregates the fisrt incoming
     * event according to the aggregation operations.  Upon success, it returns
     * 0.  Otherwise, it returns -1 to indicate errors.
     */
    public int initialize(long currentTime, Event event, TextEvent msg) {
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
                value = event.getAttribute(key);

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
                            y = 0.0;
                            new Event(Event.WARNING, name + ": got zero on "+
                                str + " in average for " + key).send();
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
                new Event(Event.ERR, name + ": failed to init property of "+
                    key).send();
                return -1;
            }
        }

        // save the updated data to msg
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            msg.setAttribute(key, result[i]);
        }
        serialNumber ++;
        return 0;
    }

    /**
     * It aggregates the incoming event with the aggregation message
     * according to the aggregation options.  It extracts data from the
     * incoming message and aggregates the values into the cached text
     * message.  Upon success, it returns 0.  Otherwise, it returns -1
     * to indicate errors.
     */
    public int aggregate(long currentTime, Event event, TextEvent msg) {
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
                    value = event.getAttribute(key);
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
                value = msg.getAttribute(key);
                result[i] = String.valueOf(x + Long.parseLong(value));
                break;
              case AGGR_SUM:
                value = msg.getAttribute(key);
                if (value == null) // no such value
                    break;
                x = Long.parseLong(value);
                value = event.getAttribute(key);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                x += Long.parseLong(value);
                result[i] = String.valueOf(x);
                break;
              case AGGR_AVG:
                value = msg.getAttribute(key);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                str = (String) map.get("AveragedOver");
                if (str != null && str.length() > 0) {
                    double d, dd;
                    k = aggrList.getID(str);
                    value = msg.getAttribute(str);
                    d = Double.parseDouble(value);
                    dd = Double.parseDouble(result[k]);
                    value = event.getAttribute(key);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    if (dd == 0.0 || dd == d) // zero sum
                        new Event(Event.WARNING, name + ": got zero on "+
                            str + " in average for " + key).send();
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
                    value = msg.getAttribute(str);
                    y = Double.parseDouble(value);
                    value = event.getAttribute(str);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    ss = Double.parseDouble(value);
                    h = (Map) aggrList.get(k);
                    str = (String) h.get("AveragedOver");
                    if (str != null && str.length() > 0) { // for count or sum
                        double d, dd;
                        k = aggrList.getID(str);
                        value = msg.getAttribute(str);
                        d = Double.parseDouble(value);
                        dd = Double.parseDouble(result[k]);
                        value = msg.getAttribute(key);
                        s = Double.parseDouble(value);
                        if (dd == 0.0 || dd == d) // zero sum
                            new Event(Event.WARNING, name + ": got zero on "+
                                str + " in average for " + key).send();
                        else {
                            y = Math.sqrt(((s*s + y*y) * d + ss*ss) / dd);
                            if (y >= yy) {
                                y = Math.sqrt((y - yy)*(y + yy));
                                result[i] = String.valueOf(y);
                            }
                            else
                                new Event(Event.WARNING, name + ": got " +
                                    "negtive deviation of " + (y-yy) +
                                    " for " + key).send();
                        }
                    }
                }
                break;
              case AGGR_MIN:
                value = event.getAttribute(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = msg.getAttribute(key);
                if (value != null && x < Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_MAX:
                value = event.getAttribute(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = msg.getAttribute(key);
                if (value != null && x > Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_DSUM:
                value = msg.getAttribute(key);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                value = event.getAttribute(key);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                y += Double.parseDouble(value);
                result[i] = String.valueOf(y);
                break;
              case AGGR_DMIN:
                value = event.getAttribute(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = msg.getAttribute(key);
                if (value != null && y < Double.parseDouble(value))//update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_DMAX:
                value = event.getAttribute(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = msg.getAttribute(key);
                if (value != null && y > Double.parseDouble(value))//update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_TMIN:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = event.getAttribute(key);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = msg.getAttribute(key);
                if (str != null && x < dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_TMAX:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = event.getAttribute(key);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = event.getAttribute(str);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = msg.getAttribute(key);
                if (str != null && x > dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_APPEND:
                str = msg.getAttribute(key);
                if (str == null) // no such value
                    str = "";
                value = event.getAttribute(key);
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
                result[i] = event.getAttribute(key);
                break;
              default:
                break;
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, name + ": failed to aggreate msg for "+
                key + ": " + Event.traceStack(e)).send();
            return -1;
        }

        // save the updated data to msg
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            msg.setAttribute(key, result[i]);
        }
        // update internal count
        serialNumber ++;

        return 0;

    }

    /**
     * not implemented yet
     */
    public int rollback(long currentTime, Message inMessage, Message msg) {
        int k;
        serialNumber --;

        k = 0;
        return k;
    }

    /**
     * It initializes the aggregation message and aggregates the fisrt incoming
     * message according to the aggregation operations.  Upon success, it
     * returns 0.  Otherwise, it returns -1 to indicate errors.
     */
    public int initialize(long currentTime, Message inMessage, Message msg)
        throws JMSException {
        int i, k, n, option;
        Map map;
        DataSet dataSet;
        long[] aggrInfo;
        long x;
        double y;
        String key, value, str = null;
        String[] result;

        k = 0;
        n = aggrList.size();
        result = new String[n];
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            aggrInfo = aggrList.getMetaData(i);
            option = (int) aggrInfo[0];
            if (option == AGGR_NONE)
                continue;
            map = (Map) aggrList.get(i);
            try {
                value = MessageUtils.getProperty(key, inMessage);

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
                else if (option == AGGR_COUNT) {
                    if ((dataSet=(DataSet) map.get("Condition"))!=null) {
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
                            y = 0.0;
                            new Event(Event.WARNING, name + ": got zero on "+
                                str + " in average for " + key).send();
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
                    result[i] = value;
                }
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to init property of "+
                    key).send();
                return -1;
            }
        }

        // save the updated data to msg
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            try {
                MessageUtils.setProperty(key, result[i], msg);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to update msg for "+
                    key + ": " + Event.traceStack(e)).send();
                return -2;
            }
        }
        serialNumber ++;
        return 0;
    }

    /**
     * It aggregates the incoming message with the aggregation message
     * according to the aggregation options.  It extracts data from the
     * incoming message and aggregates the values into the cached text
     * message.  Upon success, it returns 0.  Otherwise, it returns -1
     * to indicate errors.
     */
    public int aggregate(long currentTime, Message inMessage, Message msg)
        throws JMSException {
        int i, k, n, option;
        Map map;
        DataSet dataSet;
        DateFormat dateFormat;
        long[] aggrInfo;
        long x;
        double y;
        String key = null, value, str = null;
        String[] result;

        k = 0;
        n = aggrList.size();
        result = new String[n];
        for (i=0; i<n; i++) try {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            aggrInfo = aggrList.getMetaData(i);
            option = (int) aggrInfo[0];
            if (option == AGGR_NONE)
                continue;
            map = (Map) aggrList.get(i);

            switch (option) {
              case AGGR_COUNT:
                if ((dataSet = (DataSet) map.get("Condition")) != null) {
                    value = MessageUtils.getProperty(key, inMessage);
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
                value = MessageUtils.getProperty(key, msg);
                result[i] = String.valueOf(x + Long.parseLong(value));
                break;
              case AGGR_SUM:
                value = MessageUtils.getProperty(key, msg);
                if (value == null) // no such value
                    break;
                x = Long.parseLong(value);
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                x += Long.parseLong(value);
                result[i] = String.valueOf(x);
                break;
              case AGGR_AVG:
                value = MessageUtils.getProperty(key, msg);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                str = (String) map.get("AveragedOver");
                if (str != null && str.length() > 0) {
                    double d, dd;
                    k = aggrList.getID(str);
                    value = MessageUtils.getProperty(str, msg);
                    d = Double.parseDouble(value);
                    dd = Double.parseDouble(result[k]);
                    value = MessageUtils.getProperty(key, inMessage);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    if (dd == 0.0 || dd == d) // zero sum
                        new Event(Event.WARNING, name + ": got zero on "+
                            str + " in average for " + key).send();
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
                    value = MessageUtils.getProperty(str, msg);
                    y = Double.parseDouble(value);
                    value = MessageUtils.getProperty(str, inMessage);
                    if (value == null) // no such value, use default
                        value = (String) map.get("DefaultValue");
                    ss = Double.parseDouble(value);
                    h = (Map) aggrList.get(k);
                    str = (String) h.get("AveragedOver");
                    if (str != null && str.length() > 0) { // for count or sum
                        double d, dd;
                        k = aggrList.getID(str);
                        value = MessageUtils.getProperty(str, msg);
                        d = Double.parseDouble(value);
                        dd = Double.parseDouble(result[k]);
                        value = MessageUtils.getProperty(key, msg);
                        s = Double.parseDouble(value);
                        if (dd == 0.0 || dd == d) // zero sum
                            new Event(Event.WARNING, name + ": got zero on "+
                                str + " in average for " + key).send();
                        else {
                            y = Math.sqrt(((s*s + y*y) * d + ss*ss) / dd);
                            if (y >= yy) {
                                y = Math.sqrt((y - yy)*(y + yy));
                                result[i] = String.valueOf(y);
                            }
                            else
                                new Event(Event.WARNING, name + ": got " +
                                    "negtive deviation of " + (y-yy) +
                                    " for " + key).send();
                        }
                    }
                }
                break;
              case AGGR_MIN:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = MessageUtils.getProperty(key, msg);
                if (value != null && x < Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_MAX:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = MessageUtils.getProperty(key, msg);
                if (value != null && x > Long.parseLong(value)) // update
                    result[i] = String.valueOf(x);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(x);
                break;
              case AGGR_DSUM:
                value = MessageUtils.getProperty(key, msg);
                if (value == null) // no such value
                    break;
                y = Double.parseDouble(value);
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) // no such value, use default
                    value = (String) map.get("DefaultValue");
                y += Double.parseDouble(value);
                result[i] = String.valueOf(y);
                break;
              case AGGR_DMIN:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = MessageUtils.getProperty(key, msg);
                if (value != null && y < Double.parseDouble(value)) //update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_DMAX:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = MessageUtils.getProperty(key, msg);
                if (value != null && y > Double.parseDouble(value)) //update
                    result[i] = String.valueOf(y);
                else if (value == null) // first one so set the value
                    result[i] = String.valueOf(y);
                break;
              case AGGR_TMIN:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = MessageUtils.getProperty(key, msg);
                if (str != null && x < dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_TMAX:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                str = MessageUtils.getProperty(key, msg);
                if (str != null && x > dateFormat.parse(str).getTime()) //update
                    result[i] = value;
                else if (str == null) // first one so set the value
                    result[i] = value;
                break;
              case AGGR_APPEND:
                str = MessageUtils.getProperty(key, msg);
                
                if (str == null) // no such value
                    str = "";
                value = MessageUtils.getProperty(key, inMessage);
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
                result[i] = MessageUtils.getProperty(key, inMessage);
                break;
              default:
                break;
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, name + ": failed to retrieve data for "+
                key + ": " + Event.traceStack(e)).send();
            return -2;
        }
        catch (Exception e) {
            new Event(Event.ERR, name+ ": failed to aggregate data for "+
                key + ": " + Event.traceStack(e)).send();
            return -1;
        }

        // save the updated data to msg
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                continue;
            if (result[i] == null)
                continue;
            try {
                MessageUtils.setProperty(key, result[i], msg);
            }
            catch (Exception e) {
                new Event(Event.ERR, name + ": failed to update msg for "+
                    key + ": " + Event.traceStack(e)).send();
                return -2;
            }
        }
        serialNumber ++;

        return 0;
    }

    /**
     * It compares the value of the i-th property between the incoming message
     * and the cached message to determine whose message body will be cached.
     * It returns 1 for update, 0 for same value, -1 for no update, -2 for
     * failure.
     */
    public int compare(int i, Message inMessage, Message msg) {
        int k, option;
        Map map;
        DateFormat dateFormat;
        long[] aggrInfo;
        long x, xx;
        double y, yy;
        String key = null, value, str = null;

        k = 0;
        try {
            key = aggrList.getKey(i);
            if (key == null || key.length() <= 0)
                return -2;
            aggrInfo = aggrList.getMetaData(i);
            option = (int) aggrInfo[0];
            if (option == AGGR_NONE)
                return -2;
            map = (Map) aggrList.get(i);

            switch (option) {
              case AGGR_MIN:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                xx = Long.parseLong(value);
                if (x < xx) // update
                    return 1;
                else if (x > xx)
                    return -1;
                else
                    return 0;
              case AGGR_MAX:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = Long.parseLong(value);
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                xx = Long.parseLong(value);
                if (x > xx) // update
                    return 1;
                else if (x < xx)
                    return -1;
                else
                    return 0;
              case AGGR_DMIN:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                yy = Double.parseDouble(value);
                if (y < yy) // update
                    return 1;
                else if (y > yy)
                    return -1;
                else
                    return 0;
              case AGGR_DMAX:
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                y = Double.parseDouble(value);
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                yy = Double.parseDouble(value);
                if (y > yy) // update
                    return 1;
                else if (y < yy)
                    return -1;
                else
                    return 0;
              case AGGR_TMIN:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MinOf
                    if (map.containsKey("MinOf")) {
                        str = (String) map.get("MinOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                xx = dateFormat.parse(value).getTime();
                if (x < xx) // update
                    return 1;
                else if (x > xx)
                    return -1;
                else
                    return 0;
              case AGGR_TMAX:
                dateFormat = (DateFormat) map.get("DateFormat");
                value = MessageUtils.getProperty(key, inMessage);
                if (value == null) { // check MaxOf
                    if (map.containsKey("MaxOf")) {
                        str = (String) map.get("MaxOf");
                        if (str != null && aggrList.getID(str) >= 0)
                            value = MessageUtils.getProperty(str, inMessage);
                    }
                    if (value == null) // no such value so skip
                        break;
                }
                x = dateFormat.parse(value).getTime();
                value = MessageUtils.getProperty(key, msg);
                if (value == null)
                    break;
                xx = dateFormat.parse(value).getTime();
                if (x < xx) // update
                    return 1;
                else if (x > xx)
                    return -1;
                else
                    return 0;
              default:
                return 1;
            }
        }
        catch (Exception e) {
        }
        return -2;
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

    /** returns true if body aggregation map is NOT empty */
    public boolean hasBody() {
        return (bodyMap.size() > 0);
    }

    /** returns true if body aggregation Map contains the key */
    public boolean hasKeyInBody(String key) {
        return bodyMap.containsKey(key);
    }

    /** returns the object stored in the body map for the key */
    public Object getFromBody(String key) {
        return bodyMap.get(key);
    }

    /** returns aggregation id for body */
    public int getBodyOperation() {
        return bodyOperation;
    }

    /** returns result type for body */
    public int getBodyType() {
        return bodyType;
    }

    /** returns data type for body */
    public int getBodyDataType() {
        return dataType;
    }

    /** sets the body map and returns the old one */
    public Map setBodyMap(Map map) {
        Object o;
        Map h = bodyMap;
        if (map == null)
            return null;
        if ((o = map.get("Operation")) != null) {
            int i;
            String str = (String) o;
            if ("append".equalsIgnoreCase(str))
                i = AGGR_APPEND;
            else if ("first".equalsIgnoreCase(str))
                i = AGGR_FIRST;
            else if ("last".equalsIgnoreCase(str))
                i = AGGR_LAST;
            else if ("merge".equalsIgnoreCase(str))
                i = AGGR_MERGE;
            else if ("union".equalsIgnoreCase(str))
                i = AGGR_UNION;
            else if ("xunion".equalsIgnoreCase(str))
                i = AGGR_XUNION;
            else if ("join".equalsIgnoreCase(str))
                i = AGGR_JOIN;
            else
                i = AGGR_APPEND;
            bodyOperation = i;
            if ((o = map.get("XPath")) != null)
                bodyType = Utils.RESULT_XML;
            else if ((o = map.get("JSONPath")) != null)
                bodyType = Utils.RESULT_JSON;
            else
                bodyType = Utils.RESULT_TEXT;
            if ((o = map.get("DataType")) != null && o instanceof String) {
                str = (String) o;
                if ("string".equals(str))
                    dataType = JSON2FmModel.JSON_STRING;
                else if ("long".equals(str))
                    dataType = JSON2FmModel.JSON_NUMBER;
                else if ("map".equals(str))
                    dataType = JSON2FmModel.JSON_MAP;
                else if ("list".equals(str))
                    dataType = JSON2FmModel.JSON_ARRAY;
                else if ("double".equals(str))
                    dataType = JSON2FmModel.JSON_NUMBER;
                else if ("boolean".equals(str))
                    dataType = JSON2FmModel.JSON_BOOLEAN;
                else
                    dataType = JSON2FmModel.JSON_UNKNOWN;
            }

            bodyMap = map;
            return h;
        }
        return null;
    }

    /** clears the bodyMap */
    public void clearBodyMap() {
        bodyMap.clear();
    }

    /**
     * merges the xml content from the given reader to the xml doc and
     * returns number of objects merged or -1 for failure
     */
    public int merge(Reader sr, Document doc, DocumentBuilder builder)
        throws IOException, SAXException, XPathExpressionException {
        XPathExpression xpe;
        NodeList list = null, nodes;
        Object o;
        int i, n = -1;

        if (sr == null || doc == null || builder == null)
            return -1;

        xpe = (XPathExpression) bodyMap.get("XPath");
        if (xpe == null)
            throw(new IOException("XPath expression not defined"));
        else {
            Document d = builder.parse(new InputSource(sr));
            list = (NodeList) xpe.evaluate(d, XPathConstants.NODESET);
        }

        n = list.getLength();
        if (n <= 0)
            return 0;

        if (bodyMap.containsKey("TargetXPath")) { // different target XPath
            xpe = (XPathExpression) bodyMap.get("TargetXPath");
            if (xpe == null)
                throw(new IOException("target XPath is null"));
        }
        nodes = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);

        o = null;
        if (nodes != null && (i = nodes.getLength()) > 0) {
            o = nodes.item(0).getParentNode();
            if (o != null) { // found the parent node
                org.w3c.dom.Node parent = (org.w3c.dom.Node) o;
                for (i=0; i<n; i++) // import all nodes from the source
                    parent.appendChild(doc.importNode(list.item(i), true));
                return n;
            }
        }
        else {
            throw(new IOException("empty target list"));
        }

        return 0;
    }

    /**
     * merges the json content from the given reader to the parsed json map
     * and returns 1 for success, 0 for empty source and -1 for failure
     */
    @SuppressWarnings("unchecked")
    public int merge(Reader sr, String key, Map map) throws IOException {
        Object o, obj;
        String path, tpath;

        if(sr == null || map == null || map.size() <= 0)
            return -1;

        path = (String) bodyMap.get("JSONPath");
        if (path == null)
            throw(new IOException("JSON path not defined"));
        else {
            obj = JSON2FmModel.parse(sr);
            if (obj instanceof List)
                o = JSON2FmModel.get((List) obj, path);
            else
                o = JSON2FmModel.get((Map) obj, path);
        }

        if (o == null)
            return 0;

        tpath = path;
        if (bodyMap.containsKey("TargetJSONPath")) { // different JSON Path
            tpath = (String) bodyMap.get("TargetJSONPath");
            if (tpath == null)
                throw(new IOException("target JSON path is null"));
        }
        obj = JSON2FmModel.get(map, tpath);

        if (obj == null)
            throw(new IOException("empty JSON target for " + tpath));
        else if (obj instanceof List) { // merge to the list
            List list = (List) obj;
            if (o instanceof List) { // merge two lists
                List pl = (List) o;
                int n = pl.size();
                for (int i=0; i<n; i++) {
                    o = pl.get(i);
                    if (o != null)
                        list.add(o);
                }
            }
            else
                list.add(o);
        }
        else if (obj instanceof Map) { // merge to the map
            if (key != null && key.length() > 0)
                ((Map) obj).put(key, o);
            else if (o instanceof Map) { // merge two maps
                Iterator iter = ((Map) o).keySet().iterator();
                Map ph = (Map) obj;
                while (iter.hasNext()) {
                    key = (String) iter.next();
                    if (key == null || key.length() <= 0)
                        continue;
                    if (!ph.containsKey(key))
                        ph.put(key, ((Map) o).get(key));
                }
            }
            else
                throw(new IOException("can not merge object for " + path));
        }
        else
            throw(new IOException("JSON target is not an object for " + path));

        return 1;
    }

    /**
     * unions the xml content from the given reader to the parsed xml doc
     * and returns 1 for success, 0 for empty source and -1 for failure
     */
    @SuppressWarnings("unchecked")
    public int union(Reader sr, Document doc, DocumentBuilder builder, Map map)
        throws IOException, SAXException, XPathExpressionException {
        Object o;
        String key;
        NodeList nodes, list;
        XPathExpression xpe;
        int i, n;

        if (doc == null || map == null || map.size() <= 0)
            return -1;

        xpe = (XPathExpression) bodyMap.get("XPath");
        if (xpe == null)
            throw(new IOException("XPath expression not defined"));
        else {
            Document d = builder.parse(new InputSource(sr));
            list = (NodeList) xpe.evaluate(d, XPathConstants.NODESET);
        }

        n = list.getLength();
        if (n <= 0)
            return 0;

        if (bodyMap.containsKey("TargetXPath")) { // different target XPath
            xpe = (XPathExpression) bodyMap.get("TargetXPath");
            if (xpe == null)
                throw(new IOException("target XPath is null"));
        }
        nodes = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);

        o = null;
        if (nodes != null && (i = nodes.getLength()) > 0) {
            o = nodes.item(0).getParentNode();
            if (o == null)  // no parent node found
                throw(new IOException("no parent node found"));
            org.w3c.dom.Node parent = (org.w3c.dom.Node) o;
            Template tmp = (Template) bodyMap.get("Template");
            TextSubstitution sub=(TextSubstitution)bodyMap.get("Substitution");
            Map expr = (Map) bodyMap.get("XPathExpression");
            Map cache = (Map) map.get("__cache__");
            if (sub != null) {
                for (i=0; i<n; i++) {
                    o = Utils.evaluate(list.item(i), expr);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    key = sub.substitute(key);
                    if (!cache.containsKey(key)) {
                        cache.put(key, null);
                        parent.appendChild(doc.importNode(list.item(i), true));
                    }
                }
            }
            else {
                for (i=0; i<n; i++) {
                    o = Utils.evaluate(nodes.item(i), expr);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    if (!cache.containsKey(key)) {
                        cache.put(key, null);
                        parent.appendChild(doc.importNode(list.item(i), true));
                    }
                }
            }
        }
        else
            throw(new IOException("empty target list"));

        return 1;
    }

    /**
     * unions the json content from the given reader to the parsed json map
     * and returns 1 for success, 0 for empty source and -1 for failure
     */
    @SuppressWarnings("unchecked")
    public int union(Reader sr, Map map) throws IOException {
        Object o, obj;
        String key, path;

        if(sr == null || map == null || map.size() <= 0)
            return -1;

        path = (String) bodyMap.get("JSONPath");
        if (path == null)
            throw(new IOException("JSON path not defined"));
        else {
            obj = JSON2FmModel.parse(sr);
            if (obj instanceof List)
                o = JSON2FmModel.get((List) obj, path);
            else
                o = JSON2FmModel.get((Map) obj, path);
        }

        if (o == null)
            return 0;

        if (bodyMap.containsKey("TargetJSONPath")) { // different JSON Path
            path = (String) bodyMap.get("TargetJSONPath");
            if (path == null)
                throw(new IOException("target JSON path is null"));
        }
        obj = JSON2FmModel.get(map, path);

        if (obj == null)
            throw(new IOException("empty JSON target for " + path));
        else if (obj instanceof List) { // union two lists
            List list = (List) obj;
            List pl = (List) o;
            int n = pl.size();
            Template tmp = (Template) bodyMap.get("Template");
            TextSubstitution sub=(TextSubstitution)bodyMap.get("Substitution");
            Map cache = (Map) map.get("__cache__");
            if (cache == null)
                throw(new IOException("cache not initialized " + path));
            else if (sub != null) {
                for (int i=0; i<n; i++) {
                    o = pl.get(i);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = JSON2FmModel.format((Map) o, tmp);
                    key = sub.substitute(key);
                    if (!cache.containsKey(key)) {
                        cache.put(key, null);
                        list.add(o);
                    }
                }
            }
            else {
                for (int i=0; i<n; i++) {
                    o = pl.get(i);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = JSON2FmModel.format((Map) o, tmp);
                    if (!cache.containsKey(key)) {
                        cache.put(key, null);
                        list.add(o);
                    }
                }
            }
        }
        else
            throw(new IOException("JSON target not a list " + path));

        return 1;
    }

    @SuppressWarnings("unchecked")
    public int initCache(Document doc, Map map) throws IOException,
        XPathExpressionException {
        Object o;
        String key;
        NodeList nodes;
        XPathExpression xpe;

        if (doc == null || map == null || map.size() <= 0)
            return -1;

        xpe = (XPathExpression) bodyMap.get("XPath");
        if (xpe == null)
            throw(new IOException("XPath expression not defined"));

        if (bodyMap.containsKey("TargetXPath")) { // different target XPath
            xpe = (XPathExpression) bodyMap.get("TargetXPath");
            if (xpe == null)
                throw(new IOException("target XPath is null"));
        }
        nodes = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);

        if (nodes != null) {
            Template tmp = (Template) bodyMap.get("Template");
            TextSubstitution sub=(TextSubstitution)bodyMap.get("Substitution");
            Map expr = (Map) bodyMap.get("XPathExpression");
            Map<String, Object> cache = new HashMap<String, Object>();
            map.put("__cache__", cache);
            int n = nodes.getLength();
            if (sub != null) {
                for (int i=0; i<n; i++) {
                    o = Utils.evaluate(nodes.item(i), expr);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    cache.put(sub.substitute(key), null);
                }
            }
            else {
                for (int i=0; i<n; i++) {
                    o = Utils.evaluate(nodes.item(i), expr);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    cache.put(key, null);
                }
            }
        }
        else
            throw(new IOException("empty target list"));

        return 1;
    }

    @SuppressWarnings("unchecked")
    public int initCache(Map map) throws IOException {
        Object o, obj;
        String key, path;

        if (map == null || map.size() <= 0)
            return -1;

        path = (String) bodyMap.get("JSONPath");
        if (path == null)
            throw(new IOException("JSON path not defined"));

        if (bodyMap.containsKey("TargetJSONPath")) { // different JSON Path
            path = (String) bodyMap.get("TargetJSONPath");
            if (path == null)
                throw(new IOException("target JSON path is null"));
        }
        obj = JSON2FmModel.get(map, path);

        if (obj == null)
            throw(new IOException("empty JSON target for " + path));
        else if (obj instanceof List) {
            List list = (List) obj;
            int n = list.size();
            Template tmp = (Template) bodyMap.get("Template");
            TextSubstitution sub=(TextSubstitution)bodyMap.get("Substitution");
            Map<String, Object> cache = new HashMap<String, Object>();
            map.put("__cache__", cache);
            if (sub != null) {
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    cache.put(sub.substitute(key), null);
                }
            }
            else {
                for (int i=0; i<n; i++) {
                    o = list.get(i);
                    if (o == null || !(o instanceof Map))
                        continue;
                    key = tmp.substitute(tmp.copyText(), (Map) o);
                    cache.put(key, null);
                }
            }
        }
        else
            throw(new IOException("JSON target not a list " + path));

        return 1;
    }

    /** returns a text with all aggregate actions */
    public String displayList(String indent) {
        int i, n;
        long[] meta;
        String key;
        StringBuffer strBuf = new StringBuffer();
        Map map;

        if (indent == null)
            indent = "\n\t";
        n = aggrList.size();
        for (i=0; i<n; i++) {
            key = aggrList.getKey(i);
            meta = aggrList.getMetaData(i);
            map = (Map) aggrList.get(i);
            strBuf.append(indent + i + ": " + key + " " + meta[0] + " " +
                (String) map.get("DefaultValue"));
            if (AGGR_AVG == meta[0] && map.containsKey("AveragedOver"))
                strBuf.append(" " + (String) map.get("AveragedOver"));
        }

        return strBuf.toString();
    }
}
