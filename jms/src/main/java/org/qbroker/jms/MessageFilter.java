package org.qbroker.jms;

/* MessageFilter.java - a client side filter to evaluate JMS messages */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.DataSet;
import org.qbroker.common.Filter;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.event.Event;

/**
 * MessageFilter is a client side filter with an internal formatter. The method
 * of evaluate() returns true if the filter matches the message. Otherwise, it
 * returns false. The method of format is to format the message with the
 * internal formatter. To skip loading the internal formatter, please make sure
 * ClassName defined.
 *<br/><br/>
 * As to formatter, you can define TemplateMap and/or SubstitutionMap in the
 * property Map. It will update the maps and try to reuse the objects
 * already created.
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class MessageFilter implements Filter<Message> {
    private Map[] aJMSProps = null, xJMSProps = null;
    private Pattern[][] aPatterns = null, xPatterns = null;
    private String[] dataField = null, mapKey = null;
    private int[] dataType = null;
    private Template[][] temp = null;
    private TextSubstitution[][] tsub = null;
    private Perl5Matcher pm = null;
    private String name;
    private boolean check_header, check_body, hasFormatter, onlyJMSType;
    private boolean updated_body = false;
    public final static int RESET_NONE = 0;
    public final static int RESET_SOME = 1;
    public final static int RESET_ALL = 2;
    public final static int RESET_MAP = 3;

    public MessageFilter(Map props) {
        Object o;
        Perl5Compiler pc = null;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException("Empty property for a fitler"));

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not well defined"));
        name = (String) o;

        pc = new Perl5Compiler();
        pm = new Perl5Matcher();

        // for Pattern group on message body
        check_body = false;
        if (props.containsKey("PatternGroup") ||
            props.containsKey("XPatternGroup")) try {
            aPatterns = (Pattern[][]) getPatterns("PatternGroup", props, pc);
            xPatterns = (Pattern[][]) getPatterns("XPatternGroup", props, pc);
            check_body = true;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " has a bad pattern in PatternGroup: " + e.toString()));
        }

        // for JMSProperty group
        check_header = false;
        if (props.containsKey("JMSPropertyGroup") ||
            props.containsKey("XJMSPropertyGroup")) try {
            aJMSProps = (Map[]) getPatternMaps("JMSPropertyGroup", props, pc);
            xJMSProps = (Map[]) getPatternMaps("XJMSPropertyGroup", props, pc);
            check_header = true;
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " has a bad pattern in JMSPropertyGroup: " + e.toString()));
        }

        // for formatter
        hasFormatter = false;
        onlyJMSType = false;
        if ((o = props.get("FormatterArgument")) != null &&
            o instanceof List) { // default formatter defined
            Map map;
            Map<String, Template> tmpMap = null;
            Map<String, TextSubstitution> subMap = null;
/*
            if ((o = props.get("TemplateMap")) != null && o instanceof Map)
                tmpMap = (Map) o;
            if((o=props.get("SubstitutionMap")) != null && o instanceof Map)
                subMap = (Map) o;
*/
            map = initFormatter("FormatterArgument", props, tmpMap, subMap);
            dataField = (String[]) map.get("DataField");
            mapKey = (String[]) map.get("MapKey");
            dataType = (int[]) map.get("DataType");
            temp = (Template[][]) map.get("Template");
            tsub = (TextSubstitution[][]) map.get("Substitution");
            if (dataField != null && dataField.length > 0) {
                int k = dataField.length; 
                hasFormatter = true;
                if (dataField.length == 1 && "4096".equals(dataField[0]))
                    onlyJMSType = true;
                for (int i=0; i<k; i++) {
                    if ("body".equals(dataField[i])) {
                        updated_body = true;
                        break;
                    }
                }
            }
            map.clear();
        }
    }

    /**
     * initializes the formatter and the parameters from the property map.
     * Upon success, it returns the Map containing all of objects.
     * tmpMap and subMap are used to track existing templates and subs.
     */
    private static Map initFormatter(String key, Map ph,
        Map<String, Template> tmpMap, Map<String, TextSubstitution> subMap) {
        Object o;
        Map map;
        Map<String, Object> rule;
        List list, pl;
        String[] dataField;
        String[] mapKey;
        int[] dataType;
        Template[][] template;
        TextSubstitution[][] substitution;
        Template[] tmp;
        TextSubstitution[] sub;
        String str;
        int i, j, k, n;

        if (key == null || key.length() <= 0 || ph == null)
            return null;

        if ((o = ph.get(key)) == null || !(o instanceof List))
            return null;

        list = (List) o;
        k = list.size();
        dataField = new String[k];
        mapKey = new String[k];
        dataType = new int[k];
        template = new Template[k][];
        substitution = new TextSubstitution[k][];
        rule = new HashMap<String, Object>();
        for (i=0; i<k; i++) {
            dataField[i] = null;
            mapKey[i] = null;
            dataType[i] = JMSEvent.TYPE_STRING;
            template[i] = null;
            substitution[i] = null;
            if ((o = list.get(i)) == null || !(o instanceof Map))
                continue;
            map = (Map) o;
            if (map.size() <= 0)
                continue;
            if((o = map.get("FieldName")) == null || !(o instanceof String))
                continue;
            str = (String) o;
            if (str.length() <= 0)
                continue;
            dataField[i] = MessageUtils.getPropertyID(str);
            if (dataField[i] == null)
                dataField[i] = str;
            if ((o = map.get("MapKey")) != null && o instanceof String)
                mapKey[i] = (String) o;
            if ((o = map.get("DataType")) != null && o instanceof String) {
                str = (String) o;
                if ("string".equals(str))
                    dataType[i] = JMSEvent.TYPE_STRING;
                else if ("int".equals(str))
                    dataType[i] = JMSEvent.TYPE_INTEGER;
                else if ("long".equals(str))
                    dataType[i] = JMSEvent.TYPE_LONG;
                else if ("short".equals(str))
                    dataType[i] = JMSEvent.TYPE_SHORT;
                else if ("float".equals(str))
                    dataType[i] = JMSEvent.TYPE_FLOAT;
                else if ("double".equals(str))
                    dataType[i] = JMSEvent.TYPE_DOUBLE;
                else if ("byte".equals(str))
                    dataType[i] = JMSEvent.TYPE_BYTE;
                else if ("boolean".equals(str))
                    dataType[i] = JMSEvent.TYPE_BOOLEAN;
                else if ("char".equals(str))
                    dataType[i] = JMSEvent.TYPE_BOOLEAN +
                        JMSEvent.TYPE_DOUBLE;
                else if ("byte[]".equals(str))
                    dataType[i] = JMSEvent.TYPE_BYTE +
                        JMSEvent.TYPE_DOUBLE;
                else
                    dataType[i] = JMSEvent.TYPE_STRING;
            }

            n = 0;
            if ((o = map.get("Template")) != null && o instanceof List) {
                n = ((List) o).size();
                if ((o = map.get("Substitution")) != null &&
                    o instanceof List && n < ((List) o).size())
                    n = ((List) o).size();
            }
            else if ((o = map.get("Substitution")) != null &&
                o instanceof List) {
                n = ((List) o).size();
            }
            if (n <= 0) // no template nor substitution defined
                continue;

            // found largest n
            tmp = new Template[n];
            sub = new TextSubstitution[n];
            template[i] = tmp;
            substitution[i] = sub;
            for (j=0; j<n; j++) { // reset to null
                tmp[j] = null;
                sub[j] = null;
            }

            // for templates
            if ((o = map.get("Template")) != null && o instanceof List) {
                pl = (List) o;
                n = pl.size();
            }
            else { // templates not defined
                pl = null;
                n = 0;
            }

            for (j=0; j<n; j++) { // templates including empty ones
                if ((o = pl.get(j)) == null || !(o instanceof String))
                    continue;
                str = (String) o;
                if (j > 0 && str.length() <= 0) // empty tmp for continue
                    continue;
                if (tmpMap == null) // no tmpMap defined
                    tmp[j] = new Template(str);
                else if (tmpMap.containsKey(str)) // check for reuse
                    tmp[j] = (Template) tmpMap.get(str);
                else { // update tmpMap for reuse
                    tmp[j] = new Template(str);
                    tmpMap.put(str, tmp[j]);
                }
            }

            // for substitutions
            if ((o = map.get("Substitution")) != null &&
                o instanceof List) { // substitutions defined
                pl = (List) o;
                n = pl.size();
            }
            else { // substitutions not defined
                pl = null;
                n = 0;
            }

            for (j=0; j<n; j++) { // substitutions excluding empty ones
                if ((o = pl.get(j)) == null || !(o instanceof String))
                    continue;
                str = (String) o;
                if (str.length() <= 0) // empty sub is treated as null
                    continue;
                if (subMap == null) // subMap not defined
                    sub[j] = new TextSubstitution(str);
                else if (subMap.containsKey(str)) // check for reuse
                    sub[j] = (TextSubstitution) subMap.get(str);
                else { // update subMap for reuse
                    sub[j] = new TextSubstitution(str);
                    subMap.put(str, sub[j]);
                }
            }

            if (tmp[0] == null) { // for setting initial text
                str = "##" + dataField[i] + "##";
                if (tmpMap == null) // no tmpMap defined
                    tmp[0] = new Template(str);
                else if (tmpMap.containsKey(str))
                    tmp[0] = (Template) tmpMap.get(str);
                else {
                    tmp[0] = new Template(str);
                    tmpMap.put(str, tmp[0]);
                }
            }
        }
        rule.put("DataField", dataField);
        rule.put("MapKey", mapKey);
        rule.put("DataType", dataType);
        rule.put("Template", template);
        rule.put("Substitution", substitution);

        return rule;
    }

    /**
     * returns an array of pattern groups with the name from property Map
     */
    public static Pattern[][] getPatterns(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        int i, j, n, size = 0;
        Object o;
        Map h;
        List pl, pp;
        Pattern[][] p = new Pattern[0][];
        if ((o = ph.get(name)) != null) {
            if (o instanceof List) {
                String str;
                pl = (List) o;
                size = pl.size();
                p = new Pattern[size][];
                for (i=0; i<size; i++) {
                    h = (Map) pl.get(i);
                    pp = (List) h.get("Pattern");
                    if (pp == null)
                        n = 0;
                    else
                        n = pp.size();
                    Pattern[] q = new Pattern[n];
                    for (j=0; j<n; j++) {
                        str = (String) pp.get(j);
                        q[j] = pc.compile(str);
                    }
                    p[i] = q;
                }
            }
            else {
                return null;
            }
        }
        return p;
    }

    /**
     * returns an array of Map with a group of patterns in each of them
     */
    public static Map[] getPatternMaps(String name, Map ph,
        Perl5Compiler pc) throws MalformedPatternException {
        int i, size = 0;
        Object o;
        List pl;
        if ((o = ph.get(name)) != null) {
            if (o instanceof List) {
                String str;
                pl = (List) o;
                size = pl.size();
                Map[] h = new HashMap[size];
                for (i=0; i<size; i++) {
                    h[i] = new HashMap<String, Object>();
                    if ((o = pl.get(i)) == null || !(o instanceof Map))
                        return null;
                    Map m = (Map) o;
                    Iterator iter = m.keySet().iterator();
                    while (iter.hasNext()) {
                        String key = (String) iter.next();
                        if (key == null)
                            continue;
                        o = m.get(key);
                        if (key.startsWith("JMS")) {
                            if (o instanceof List)
                                h[i].put(MessageUtils.getPropertyID(key),
                                    new DataSet((List) o));
                            else {
                                str = (String) o;
                                h[i].put(MessageUtils.getPropertyID(key),
                                    pc.compile(str));
                            }
                        }
                        else {
                            if (o instanceof List)
                                h[i].put(key, new DataSet((List) o));
                            else {
                                str = (String) o;
                                h[i].put(key, pc.compile(str));
                            }
                        }
                    }
                }
                return h;
            }
            else {
                return null;
            }
        }
        else
            return new HashMap[0];
    }

    public boolean evaluate(long tm, Message message) {
        boolean ic;
        try {
            ic = evaluate(message, null);
        }
        catch (JMSException e) {
            String str = "";
            Exception ex = e.getLinkedException();
            if (ex != null)
                str += "Linked exception: " + ex.toString() + " ";
            throw(new RuntimeException(name + " failed to evaluate msg " +
                str + ": " + Event.traceStack(e)));
        }
        return ic;
    }

    /**
     * It applies the Perl5 pattern match on the message and returns true
     * if the filter gets a hit or false otherwise. In case there are patterns
     * for message body, it requires the content of the message body ready.
     */
    public boolean evaluate(Message message, String msgStr) throws JMSException{
        if (message == null)
            return false;
        else if (check_header) {
            if (evaluate(message, aJMSProps, pm, true) &&
                !evaluate(message, xJMSProps, pm, false)) {
                if (!check_body)
                    return true;
                return (evaluate(msgStr, aPatterns, pm, true) &&
                    !evaluate(msgStr, xPatterns, pm, false));
            }
            else
                return false;
        }
        else if (check_body) {
            return (evaluate(msgStr, aPatterns, pm, true) &&
                !evaluate(msgStr, xPatterns, pm, false));
        }
        return true;
    }

    /** evaluates the msgStr as the message body only */
    public boolean evaluate(String msgStr) {
        if (check_body) {
            return (evaluate(msgStr, aPatterns, pm, true) &&
                !evaluate(msgStr, xPatterns, pm, false));
        }
        return true;
    }

    /**
     * returns true if any one of the pattern groups matches on the messages.
     * A pattern group represents all the patterns in an array member.
     * The default value is used to evaluate an empty pattern group.
     */
    public static boolean evaluate(String msgStr, Pattern[][] patterns,
        Perl5Matcher pm, boolean def) {
        int i, j, n, size;
        Pattern[] p;
        boolean status = def;

        if (msgStr == null || patterns == null)
            return (! status);

        size = patterns.length;
        if (size <= 0)
            return status;

        for (i=0; i<size; i++) {
            p = patterns[i];
            if (p == null)
                continue;
            n = p.length;
            status = true;
            for (j=0; j<n; j++) {
                if (!pm.contains(msgStr, p[j])) {
                    status = false;
                    break;
                }
            }
            if (status)
                break;
        }
        return status;
    }

    /**
     * returns true if any one of the pattern groups matches on the messages.
     * A pattern group represents all the patterns in a Map.
     * The default value is used to evaluate an empty pattern group.
     */
    public static boolean evaluate(Message msg, Map[] props, Perl5Matcher pm,
        boolean def) {
        Map h;
        Iterator iter;
        Pattern p;
        DataSet d;
        Object o;
        String name, value;
        boolean status = def;
        int i, n;

        if (msg == null || props == null)
            return (! status);

        n= props.length;
        if (n <= 0)
            return status;

        for (i=0; i<n; i++) {
            h = props[i];
            if (h == null)
                continue;
            iter = h.keySet().iterator();
            status = true;
            while (iter.hasNext()) {
                name = (String) iter.next();
                if (name == null)
                    continue;
                o = h.get(name);
                if (o == null)
                    continue;
                try {
                    value = MessageUtils.getProperty(name, msg);
                }
                catch (JMSException e) {
                    new Event(Event.WARNING, Event.traceStack(e)).send();
                    return (! def);
                }
                if (value == null) {
                    status = false;
                    break;
                }
                if (o instanceof DataSet) { // dataset testing
                    int k;
                    d = (DataSet) o;
                    try {
                        if ((k = d.getDataType()) == DataSet.DATA_LONG)
                            status = d.contains(Long.parseLong(value));
                        else if (k == DataSet.DATA_DOUBLE)
                            status = d.contains(Double.parseDouble(value));
                        else
                            status = false;
                    }
                    catch (Exception e) {
                        status = false;
                    }
                    if (!status)
                        break;
                }
                else { // pattern
                    p = (Pattern) o;
                    if (!pm.contains(value, p)) {
                        status = false;
                        break;
                    }
                }
            }
            if (status)
                break;
        }
        return status;
    }

    /**
     * formats the message with the predefined formatter and returns number of
     * fields modified upon success.
     */
    public int format(Message message, byte[] buffer) throws JMSException {
        if ((!hasFormatter) || message == null)
            return -1;

        if (message instanceof MapMessage) // for MapMessage
            return format((MapMessage) message, pm);
        else
            return format(message, buffer, pm);
    }

    /**
     * Format engine formats certain fields of the incoming message according
     * to the predefined templates and substitutions. It returns number of
     * fields modified upon success or throws JMSException otherwise.
     */
    private int format(Message msg, byte[] buffer, Perl5Matcher pm)
        throws JMSException{
        int i, j, k, n, count;
        String text;
        StringBuffer strBuf = null;
        boolean isBytes;

        isBytes = (msg instanceof BytesMessage);
        count = 0;
        n = dataField.length;
        for (i=0; i<n; i++) { // for each dataField
            k = temp[i].length;
            if (k <= 0)
                continue;
            else
                strBuf = new StringBuffer();

            text = null;
            for (j=0; j<k; j++) { // for all templates and substitutions
                if (temp[i][j] != null) try { // format
                    if (j > 0)
                        strBuf.append(text);
                    text = MessageUtils.format(msg, buffer, temp[i][j], pm);
                }
                catch (JMSException e) {
                    String str = " at " + i + "/" + j + " ";
                    Exception ex = e.getLinkedException();
                    if (ex != null)
                        str += "Linked exception: " + ex.toString() + " ";
                    throw(new JMSException("failed to format " + dataField[i] +
                        str + ": " + Event.traceStack(e)));
                }
                catch (Exception e) {
                    String str = " at " + i + "/" + j + " ";
                    throw(new JMSException("failed to format " + dataField[i] +
                        str + ": " + Event.traceStack(e)));
                }
                if (text == null)
                    text = "";

                if (tsub[i][j] != null) try { // substitute
                    String str = tsub[i][j].substitute(pm, text);
                    if (str != null) // sub should not return null
                        text = str;
                    else
                        throw(new JMSException("failed to substitute " + text +
                            " for " + dataField[i] + " at " + i + "/" + j));
                }
                catch (Exception e) {
                    String str = " at " + i + "/" + j + " ";
                    throw(new JMSException("failed to substitute " + text +
                        " for " + dataField[i] +str+": "+Event.traceStack(e)));
                }
            }
            strBuf.append(text);
            text = strBuf.toString();

            try {
                if ("body".equals(dataField[i])) {
                    msg.clearBody();
                    if (isBytes) {
                        ((BytesMessage) msg).writeBytes(text.getBytes());
                    }
                    else {
                        ((TextMessage) msg).setText(text);
                    }
                }
                else
                    k = MessageUtils.setProperty(dataField[i], dataType[i],
                        text, msg);
            }
            catch (JMSException e) {
                String str = " at " + i + " ";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex.toString() + " ";
                throw(new JMSException("failed to set property on " +
                    dataField[i] + str + ": " + Event.traceStack(e)));
            }
            catch (Exception e) {
                String str = " at " + i + " ";
                throw(new JMSException("failed to set property on " +
                    dataField[i] + str + ": " + Event.traceStack(e)));
            }
            count ++;
        }

        return count;
    }

    /**
     * Format engine formats certain fields of the incoming MapMessage according
     * to the predefined templates and substitutions.  In case that the field
     * name is "body", the value of the corresponding MapKey will be formatted.
     * If the field is not "body", any template can reference only one MapKey at
     * a time. So the content of Map can be formatted to the header. This way,
     * they are able to participate format process on the message.  Currently,
     * there is only one key can be defined for each template for MapMessage.
     *<br/><br/>
     * Upon success, it returns the number of fields modified.  Otherwise, it
     * throws JMSException with detailed error.
     */
    private int format(MapMessage msg, Perl5Matcher pm) throws JMSException {
        int i, j, k, n, count;
        String text;
        StringBuffer strBuf = null;

        count = 0;
        n = dataField.length;
        for (i=0; i<n; i++) { // for each dataField
            k = temp[i].length;
            if (k <= 0)
                continue;
            else if ("body".equals(dataField[i]) && (mapKey[i] == null ||
                mapKey[i].length() <= 0))
                continue;
            else
                strBuf = new StringBuffer();

            text = null;
            for (j=0; j<k; j++) { // for all templates and substitutions
                if (temp[i][j] != null) try { // format
                    if (j > 0)
                        strBuf.append(text);
                    text = MessageUtils.format(msg, mapKey[i], temp[i][j], pm);
                }
                catch (JMSException e) {
                    String str = " at " + i + "/" + j + " ";
                    Exception ex = e.getLinkedException();
                    if (ex != null)
                        str += "Linked exception: " + ex.toString() + " ";
                    throw(new JMSException("failed to format " + dataField[i] +
                        str + ": " + Event.traceStack(e)));
                }
                catch (Exception e) {
                    String str = " at " + i + "/" + j + " ";
                    throw(new JMSException("failed to format " + dataField[i] +
                        str + ": " + Event.traceStack(e)));
                }
                if (text == null)
                    text = "";

                if (tsub[i][j] != null) try { // substitute
                    String str = tsub[i][j].substitute(pm, text);
                    if (str != null) // sub should not return null
                        text = str;
                    else
                        throw(new JMSException("failed to substitute " + text +
                            " for " + dataField[i] + " at " + i + "/" + j));
                }
                catch (Exception e) {
                    String str = " at " + i + "/" + j + " ";
                    throw(new JMSException("failed to substitute " + text +
                        " for "+ dataField[i] + str +": "+Event.traceStack(e)));
                }
            }
            strBuf.append(text);
            text = strBuf.toString();

            try {
                if (!"body".equals(dataField[i]))
                    k = MessageUtils.setProperty(dataField[i], dataType[i],
                        text, msg);
                else
                    k = MessageUtils.setMapProperty(mapKey[i], dataType[i],
                        text, msg);
            }
            catch (JMSException e) {
                String str = " at " + i + " ";
                Exception ex = e.getLinkedException();
                if (ex != null)
                    str += "Linked exception: " + ex.toString() + " ";
                throw(new JMSException("failed to set property on " +
                    dataField[i] + str + ": " + Event.traceStack(e)));
            }
            catch (Exception e) {
                String str = " at " + i + " ";
                throw(new JMSException("failed to set property on " +
                    dataField[i] + str + ": " + Event.traceStack(e)));
            }
            count ++;
        }

        return count;
    }

    public static MessageFilter[] initFilters(Map ph) {
        Object o;
        if (ph == null || ph.size() <= 0)
            return null;
        if ((o = ph.get("Ruleset")) == null || !(o instanceof List))
            return null;
        else {
            MessageFilter[] filters = null;
            MessageFilter filter = null;
            List<MessageFilter> pl;
            List list = (List) o;
            int i, n = list.size();
            pl = new ArrayList<MessageFilter>();
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof Map))
                    continue;
                filter = new MessageFilter((Map) o);
                if (filter != null)
                    pl.add(filter);
            }
            n = pl.size();
            filters = new MessageFilter[n];
            for (i=0; i<n; i++)
                filters[i] = pl.get(i);
            return filters;
        }
    }

    public boolean checkHeader() {
       return check_header;
    }

    public boolean checkBody() {
       return check_body;
    }

    public boolean updatedBody() {
       return updated_body;
    }

    public boolean hasFormatter() {
       return hasFormatter;
    }

    public void clear() {
        if (aJMSProps != null) {
            for (int i=aJMSProps.length-1; i>=0; i--) {
                if (aJMSProps[i] != null) try {
                    aJMSProps[i].clear();
                }
                catch (Exception e) {
                }
                aJMSProps[i] = null;
            }
        }
        aJMSProps = null;
        if (xJMSProps != null) {
            for (int i=xJMSProps.length-1; i>=0; i--) {
                if (xJMSProps[i] != null) try {
                    xJMSProps[i].clear();
                }
                catch (Exception e) {
                }
                xJMSProps[i] = null;
            }
        }
        xJMSProps = null;
        if (aPatterns != null) {
            for (int i=aPatterns.length-1; i>=0; i--) {
                if (aPatterns[i] != null) try {
                    for (int j=aPatterns[i].length-1; j>=0; j--)
                        aPatterns[i][j] = null;
                }
                catch (Exception e) {
                }
                aPatterns[i] = null;
            }
        }
        aPatterns = null;
        if (xPatterns != null) {
            for (int i=xPatterns.length-1; i>=0; i--) {
                if (xPatterns[i] != null) try {
                    for (int j=xPatterns[i].length-1; j>=0; j--)
                        xPatterns[i][j] = null;
                }
                catch (Exception e) {
                }
                xPatterns[i] = null;
            }
        }
        xPatterns = null;
        pm = null;
        if (dataField != null) {
            int n = dataField.length;
            for (int i=0; i<n; i++) {
                dataField[i] = null;
                mapKey[i] = null;
                if (temp[i] != null) try {
                    for (int j=temp[i].length-1; j>=0; j--)
                        temp[i][j] = null;
                }
                catch (Exception e) {
                }
                temp[i] = null;
                if (tsub[i] != null) try {
                    for (int j=tsub[i].length-1; j>=0; j--)
                        tsub[i][j] = null;
                }
                catch (Exception e) {
                }
                tsub[i] = null;
            }
            dataField = null;
            mapKey = null;
            dataType = null;
            temp = null;
            tsub = null;
        }
    }
}
