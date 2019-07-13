package org.qbroker.persister;

/* MessageEvaluator.java - a persister evaluating JMS messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.StringReader;
import java.io.FileReader;
import java.io.StringWriter;
import java.io.IOException;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.QList;
import org.qbroker.common.Utils;
import org.qbroker.common.QuickCache;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.DataSet;
import org.qbroker.common.Template;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.SimpleParser;
import org.qbroker.common.XML2Map;
import org.qbroker.common.PHP2Map;
import org.qbroker.json.JSON2Map;
import org.qbroker.json.JSON2FmModel;
import org.qbroker.json.JSONTemplate;
import org.qbroker.jms.JSONFormatter;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.MessageFilter;
import org.qbroker.jms.JMSEvent;
import org.qbroker.persister.Persister;
import org.qbroker.event.Event;

/**
 * MessageEvaluator evaluates JMS Messages according to rulesets.  The incoming
 * messages are supposed to specify what rules to be invoked for the evaluation.
 * The evaluation consists two separate steps. The first one is to apply the
 * filter on the message with the pattern matches, etc. The second is to invoke
 * the formatter to transform the message properties and the payload.  If the
 * pattern does not match, the evaluation process will abort with the return
 * code of the message set to 1 for FAILURE to indicate NOHIT. Otherwise, the
 * message will be transformed according to the ruleset and its return code
 * will be set to 0 for SUCCESS. In case of failures, the return code will be
 * set to -1 for EXCEPTION. MessageEvaluator will not consume any messages,
 * nor commit any messages. It just bounces them back via removing them from
 * the input XQueue.
 *<br/><br/>
 * MessageEvaluator contains a number of predefined rulesets.  Each ruleset
 * has its own unique name and the evaluation rules.  It may also contain the
 * formatting rules or plugins for modifying the properties of the messages.
 * There are many built-in transformation supports.  The default one is the
 * simple Templates and TextSubstitutions.  Others are simple template files,
 * JSONPath evaluations, JSONT templates, XPath evaluations and XSLT templates,
 * etc. MessageEvaluator also supports CommaList, PipeList or ColonList for
 * evaluations on multiple rulesets.
 *<br/><br/>
 * For the default formatting support, the ruleset is supposed to have an array
 * of FormatterArgument.  A FormatterArgument contains a name specifying what
 * property to be modifued, and two sets of format operations. The first set
 * of operations lists format templates in the name of Template. The other lists
 * substitutions with the name of Substitution.  Each Template appends the
 * previous non-empty text to the variable and sets the next initial text for
 * its corresponding Substitutions.  If the first template is null, the initial
 * text will be the variable itself.  Each Substitution modifies the text
 * before it is appended to the variable.  The associations between the
 * Templates and Substitutions are based on their positions.  Either Template
 * or Substitution can be null for no action and a place holder.  Therefore,
 * you can insert multiple null Templates so that the associated Substitutions
 * will be able to modify the same text in turns.  MessageEvaluator will apply
 * all the operations of FormatterArguments on each SUCCESS message in the
 * order of the list.  If any of the operations fails, the message will be
 * marked as EXCEPTION.
 *<br/><br/>
 * Besides FormatterArgument, MessageEvaluator also allows to have a simple
 * template file defined in the name of TemplateFile for a ruleset. In this
 * case, MessageEvaluator will load the template file at the start up and
 * stores it into the cache. It will be used to format the message body at the
 * end of the format process. If NeedChop is defined with the value of "true",
 * MessageEvaluator will try to chop the newline off the end of the loaded
 * content.
 *<br/><br/>
 * MessageEvaluator also supports SimpleParser with a list of ParserArgument
 * defined. It is used to parse the message payload and updates the various
 * properties of the message.
 *<br/><br/>
 * MessageEvaluator also supports JSONPath evaluations with a map of JSONPath
 * defined. It is used to retrieve data from the JSON payload and updates the
 * various properties of the message. Therefore, the map of JSONPath should
 * specify the property name as the key and a JSONPath expression as the value.
 *<br/><br/>
 * For JSONT support, the ruleset must not have any FormatterArgument defined.
 * Instead, the full path of the JSONT template file must be specified in the
 * name of JTMPFile. If there is any parameter to be set, they should be defined
 * in the map of JSONParameter where the key is the name of the parameter and
 * the value is a template for message properties. MessageEvaluator will load
 * the JSONT template file, compiles it at the startup and stores it to cache.
 * The parameters will be set dynamically in case there is any reference on the
 * properties of the incoming message.
 *<br/><br/>
 * MessageEvaluator also supports JSON formatter with a list of JSONFormatter
 * defined. A JSONFormatter is a map containing JSONPath, Operation, DataType,
 * Selector, and Template and Substitution. It supports various operations on
 * the messages and their JSON payload, such as get, set, remove, select,
 * first, last and merge, etc.
 *<br/><br/>
 * MessageEvaluator also supports XPath evaluations, similar to the evaluation
 * of JSONPath. It also supports operations of xmerge and xcut. XMerge is to
 * merge an XML content stored in XMLField into the XML payload of the message.
 * The ruleset should define SourceXPath and TargetXPath. Currently, only
 * append is supported for XMerge. XCut is to remove the object at a given
 * XPath expression.
 *<br/><br/>
 * For XSLT support, the ruleset must not have any FormatterArgument defined.
 * Instead, the full path of the XSL template file must be specified
 * in the name of XSLFile.  If there is any parameters to be set, they should
 * be defined in the map of XSLParameter.  MessageEvaluator will load the XSL
 * template file, compiles it at startup and stores it to cache. The parameters
 * will be set dynamically in case there is any references on the data of the
 * incoming message.
 *<br/><br/>
 * A PipeList is a list of pipe delimited names which reference a bunch of
 * existing rulesets. MessageEvaluator will match the message to the patterns
 * of each ruleset and invokes the formatter on the first hit only.
 *<br/><br/>
 * A ColonList is a list of colon delimited names which reference a bunch of
 * existing rulesets. MessageEvaluator will match the message to the patterns
 * of each ruleset and invokes the formatter on every ruleset that is a hit.
 *<br/><br/>
 * A CommaList is a list of comma delimited names which reference a bunch of
 * existing rulesets. MessageEvaluator will evaluate all the rulesets one by
 * one at the listed order. If any ruleset fails or does not match, the entire
 * evaluation process also fails.
 *<br/><br/>
 * For time window support, the ruleset can evalute the age of the message.
 * It requires ActiveTime, KeyTemplate and TimePattern to be defined in the
 * ruleset.  If it is for age, please make sure the threshold contains at
 * lease two numbers.  For occurrence, please make sure it contains only
 * one negative number.
 *<br/><br/>
 * For static caching support, the ruleset must have StaticCache defined as
 * a map with key-value pairs. Optionally, ResultField, KeyTemplate and
 * KeySubstitution can be defined also. ResultField specifies where to store
 * the cache result. By default, it is stored to message body. KeyTemplate
 * and KeySubstitution are used to retrieve the cache key. By default,
 * KeyTemplate is "##body##".
 *<br/><br/>
 * MessageEvaluator allows developers to plugin their own transformations by
 * specifying the full classname in the rules.  The requirement is minimum.
 * First, the method of the transformation has to be defined.  The second,
 * the method must take a JMS Message to be transformed as the only argument.
 * The return object must be a String of null meaning OK or error message,
 * otherwise.  It must have a constructor taking a Map with a unique
 * value for the key of the Name, or a List or a String as the single
 * argument for configurations.  Based on the data type of the constructor
 * argument, developers should define configuration parameters in the base
 * tag of FormatterArgument.  MessageEvaluator will pass the data to the
 * plugin's constructor as an opaque object during the instantiation of the
 * plugin.  In the normal operation, MessageEvaluator will invoke the method
 * to format the SUCCESS messages.  The method should never acknowledge or
 * commit any messages in any case.
 *<br/><br/>
 * In case a plugin needs to connect to external resources for dynamic
 * format process, it should define an extra method of close() to close all
 * the external resources gracefully.  Its format method should also be able
 * to detect the disconnections and cleanly reconnect to the resources
 * automatically.  If the container wants to stop the node, it will call the
 * methods of close() on all transformers in order to release all external
 * resources.
 *<br/><br/>
 * MessageEvaluator always copies the original value of RCField to the
 * OriginalRCField before resetting it.  If you want to evaluate the value of
 * the property in RCField, please ensure to reference the OriginalRCField in
 * your pattern group.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MessageEvaluator extends Persister {
    private String msgID = null;
    private int bufferSize = 4096;
    private int debug = 0;
    private int maxMsgLength = 4194304;
    private long sessionTime;

    private String fieldName, rcField, orcField;
    private QuickCache cache;
    private QList assetList;
    private TransformerFactory tFactory = null;
    private DocumentBuilder builder = null;
    private Transformer defaultTransformer = null;
    private XPath xpath = null;
    private XML2Map xh = null;

    private int retryCount;
    private int ruleSize = 128;
    private final static int SUCCESS = 0;
    private final static int FAILURE = 1;
    private final static int EXCEPTION = -1;
    private final static int EVAL_NONE = 0;
    private final static int EVAL_PATTERN = 1;
    private final static int EVAL_AGE = 2;
    private final static int EVAL_FILE = 4;
    private final static int EVAL_FILTER = 8;
    private final static int EVAL_PARSE = 16;
    private final static int EVAL_FORMAT = 32;
    private final static int EVAL_JSONPATH = 64;
    private final static int EVAL_JSONT = 128;
    private final static int EVAL_XPATH = 256;
    private final static int EVAL_XSLT = 512;
    private final static int EVAL_TRANS = 1024;
    private final static int EVAL_CACHE = 2048;
    private final static int EVAL_PLUGIN = 4096;
    private final static int ASSET_NAME = 0;
    private final static int ASSET_PNAME = 1;
    private final static int ASSET_DATA = 2;
    private final static int ASSET_TEMP = 3;
    private final static int ASSET_TSUB = 4;
    private final static int ASSET_OBJECT = 5;
    private final static int ASSET_MNAME = 6;
    private final static int ASSET_METHOD = 7;
    private final static int ASSET_CLOSE = 8;
    private final static int JSON_GET = 0;
    private final static int JSON_CUT = 1;
    private final static int JSON_MOVE = 2;
    private final static int JSON_PARSE = 3;
    private final static int JSON_MIN = 4;
    private final static int JSON_MAX = 5;
    private final static int JSON_FIRST = 6;
    private final static int JSON_LAST = 7;
    private final static int JSON_COUNT = 8;

    public MessageEvaluator(Map props) {
        super(props);
        Object o;

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        if (operation == null)
            operation = "evaluate";
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberRules")) != null)
            ruleSize =Integer.parseInt((String) o);

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("OriginalRC")) != null && o instanceof String)
            orcField = (String) o;
        else
            orcField = "OriginalRC";

        if ((o = props.get("FieldName")) != null && o instanceof String)
            fieldName = (String) o;
        else
            fieldName = "RuleName";

        try { // init rulesets
            initRulesets(System.currentTimeMillis(), ruleSize, props);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri+": failed to init Rules: "+
                Event.traceStack(e)));
        }

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    /**
     * It initializes the rulesets and returns the number of rules.
     */
    private int initRulesets(long tm, int size, Map props) {
        Object o;
        Object[] asset;
        Map<String, Object> hmap, keyMap, xslMap, tmpMap, subMap, jsonMap;
        Map map, ph, hp;
        Map[] h;
        MessageFilter filter = null;
        Iterator iter, iterator;
        List pl, list;
        String key, str;
        StringBuffer strBuf = new StringBuffer();
        int i, j, k, n, id, mask, option, dmask;

        assetList = new QList(uri, ruleSize);
        cache = new QuickCache(uri, QuickCache.META_MTAC, 0, 0); 

        if ((o = props.get("Ruleset")) != null && o instanceof Map)
            map = (Map) o;
        else
            map = new HashMap();

        tmpMap = new HashMap<String, Object>();
        subMap = new HashMap<String, Object>();
        keyMap = new HashMap<String, Object>();
        xslMap = new HashMap<String, Object>();
        jsonMap = new HashMap<String, Object>();

        iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            o = iterator.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            ph = (Map) map.get(key);
            id = assetList.reserve();
            if (id < 0)
                throw(new IllegalArgumentException(uri +
                    " failed to reserve on " + id + "/" + ruleSize));

            dmask = displayMask;
            if ((o = ph.get("DisplayMask")) != null)
                dmask = Integer.parseInt((String) o);

            hmap = new HashMap<String, Object>();
            hmap.put("Name", key);
            if ((o = ph.get("JMSPropertyGroup")) != null)
                hmap.put("JMSPropertyGroup", o);
            if ((o = ph.get("XJMSPropertyGroup")) != null)
                hmap.put("XJMSPropertyGroup", o);
            if ((o = ph.get("PatternGroup")) != null)
                hmap.put("PatternGroup", o);
            if ((o = ph.get("XPatternGroup")) != null)
                hmap.put("XPatternGroup", o);
            if ((o = ph.get("FormatterArgument")) != null &&
                o instanceof List && !ph.containsKey("ClassName")) {
                hmap.put("FormatterArgument", o);
                if ((o = ph.get("ResetOption")) != null)
                    hmap.put("ResetOption", o);
                else
                    hmap.put("ResetOption", "0");
                hmap.put("TemplateMap", tmpMap);
                hmap.put("SubstitutionMap", subMap);
            }
            filter = new MessageFilter(hmap);
            hmap.clear();
            mask = EVAL_NONE;
            if (filter.checkBody()) // need to access message body
                mask += EVAL_PATTERN;
            else if ((dmask & MessageUtils.SHOW_BODY) > 0) // for display body
                mask += EVAL_PATTERN;
            option = 0;
            asset = new Object[ASSET_CLOSE+1];
            asset[ASSET_NAME] = key;
            for (j=1; j<=ASSET_CLOSE; j++)
                asset[j] = null;
            if ((o = ph.get("ClassName")) != null) { // plug-in
                mask += EVAL_PLUGIN;
                str = (String) o;
                if ((o = ph.get("MethodName")) != null)
                    asset[ASSET_MNAME] = (String) o;
                else
                    asset[ASSET_MNAME] = "format";
                str += "::" + (String) o;
                if ((o = ph.get("FormatterArgument")) != null) {
                    h = new HashMap[1];
                    if (o instanceof List) {
                        pl = (List) o;
                        k = pl.size();
                        for (j=0; j<k; j++) {
                            if ((o = pl.get(j)) == null)
                                continue;
                            if (!(o instanceof Map))
                                continue;
                            hp = (Map) o;
                            if (hp.size() <= 0)
                                continue;
                            iter = hp.keySet().iterator();
                            if ((o = iter.next()) == null)
                                continue;
                            str += "::" + (String) o;
                            str += "::" + (String) hp.get((String) o);
                        }
                    }
                    else if (o instanceof Map) {
                        str += (String) ((Map) o).get("Name");
                    }
                    else
                        str += "::" + (String) o;
                }

                if (keyMap.containsKey(str)) { // plugin initialized already
                    id = Integer.parseInt((String) keyMap.get(str));
                    o = (Object[]) assetList.browse(id);
                    asset[ASSET_METHOD] = ((Object[]) o)[ASSET_METHOD];
                    asset[ASSET_OBJECT] = ((Object[]) o)[ASSET_OBJECT];
                    asset[ASSET_CLOSE] = ((Object[]) o)[ASSET_CLOSE];
                }
                else { // new plugin
                    o = MessageUtils.getPlugins(ph, "FormatterArgument",
                        (String) asset[ASSET_MNAME],
                        new String[]{"javax.jms.Message"}, "close", uri);
                    asset[ASSET_METHOD] = ((Object[]) o)[0];
                    asset[ASSET_OBJECT] = ((Object[]) o)[1];
                    asset[ASSET_CLOSE] = ((Object[]) o)[2];
                    keyMap.put(str, String.valueOf(id));
                }
            }
            else if ((o = ph.get("FormatterArgument")) != null &&
                o instanceof List) { // default formatter already in filter
                if (filter != null && filter.hasFormatter())
                    mask += EVAL_FORMAT;
                if ((o = ph.get("ResetOption")) != null)
                    option = Integer.parseInt((String) o);
                if ((o = ph.get("TemplateFile")) != null &&
                    o instanceof String) { // temp file for body
                    boolean chop;
                    str = (String) o;
                    asset[ASSET_DATA] = str;
                    chop = ((o = ph.get("NeedChop")) != null &&
                        o instanceof String && "true".equals((String) o));
                    try {
                        asset[ASSET_TEMP] = new Template(new File(str), chop);
                    }
                    catch (Exception e) {
                        throw(new IllegalArgumentException(uri +
                             " failed to load template from " + str + ": " +
                             Event.traceStack(e)));
                    }
                }
            }
            else if ((o = ph.get("TemplateFile")) != null &&
                o instanceof String) { // temp file for body
                boolean chop;
                mask += EVAL_FORMAT;
                str = (String) o;
                asset[ASSET_DATA] = str;
                chop = ((o = ph.get("NeedChop")) != null &&
                    o instanceof String && "true".equals((String) o));
                try {
                    asset[ASSET_TEMP] = new Template(new File(str), chop);
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(uri +
                         " failed to load template from " + str + ": " +
                         Event.traceStack(e)));
                }
            }
            else if ((o = ph.get("ParserArgument")) != null &&
                o instanceof List) { // simple parser
                pl = (List) o;
                option = pl.size();
                mask += EVAL_PARSE;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                asset[ASSET_TEMP] = new SimpleParser(pl);
            }
            else if ((o = ph.get("StaticCache")) != null &&
                o instanceof Map) { // for static cache
                asset[ASSET_DATA] = Utils.cloneProperties((Map) o);
                mask += EVAL_CACHE;
                if ((o = ph.get("KeyTemplate")) != null && o instanceof String)
                    asset[ASSET_TEMP] = new Template((String) o);
                else
                    asset[ASSET_TEMP] = new Template("##body##");
                if ((o = ph.get("KeySubstitution")) != null &&
                    o instanceof String)
                    asset[ASSET_TSUB] = new TextSubstitution((String) o);
                if ((o = ph.get("ResultField")) != null && o instanceof String)
                    asset[ASSET_MNAME] = (String) o;
                else
                    asset[ASSET_MNAME] = "body";
            }
            else if ((o = ph.get("JSONPath")) != null && o instanceof Map) {
                mask += EVAL_JSONPATH;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                asset[ASSET_TSUB] = Utils.cloneProperties((Map) o);
            }
            else if ((o = ph.get("JTMPFile")) != null) { // JSONT
                mask += EVAL_JSONT;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                str = (String) o;
                if (!jsonMap.containsKey(str)) try {
                    o = JSON2FmModel.parse(new FileReader(str));
                    jsonMap.put(str, new JSONTemplate((Map) o));
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to compile JSON template of " + str + ": " +
                         Event.traceStack(e)));
                }
                asset[ASSET_DATA] = str;
                asset[ASSET_TEMP] = jsonMap.get(str);
                if ((o = ph.get("JSONParameter")) != null && o instanceof Map) {
                    String value;
                    Template tp;
                    iter = ((Map) o).keySet().iterator();
                    hmap = new HashMap<String, Object>();
                    while (iter.hasNext()) {
                        str = (String) iter.next();
                        if (str == null || str.length() <= 0)
                            continue;
                        value = (String) ((Map) o).get(str);
                        tp = new Template(value);
                        if (tp == null || tp.numberOfFields() <= 0)
                            hmap.put(str, value);
                        else { // dynamic parameters
                            hmap.put(str, tp);
                            option ++;
                        }
                    }
                    if (hmap.size() > 0)
                        asset[ASSET_TSUB] = hmap;
                }
            }
            else if((o = ph.get("JSONFormatter")) != null && o instanceof List){
                mask += EVAL_JSONT;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                hmap = new HashMap<String, Object>();
                hmap.put("Name", key);
                hmap.put("JSONFormatter", (List) o);
                asset[ASSET_TSUB] = new JSONFormatter(hmap);
                hmap.clear();
            }
            else if ((o = ph.get("XPath")) != null && o instanceof Map) {//XPath
                mask += EVAL_XPATH;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                if (xpath == null) try {
                    builder = Utils.getDocBuilder();
                    xpath = XPathFactory.newInstance().newXPath();
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to instantiate XPath: " + Event.traceStack(ex)));
                }
                catch (Error ex) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XPath for "+ uri +": "+ Event.traceStack(ex)));
                }
                String value;
                Template tp;
                XPathExpression xpe = null;
                iter = ((Map) o).keySet().iterator();
                hmap = new HashMap<String, Object>();
                str = null;
                while (iter.hasNext()) try {
                    str = (String) iter.next();
                    if (str == null || str.length() <= 0)
                        continue;
                    value = (String) ((Map) o).get(str);
                    tp = new Template(value);
                    if (tp == null || tp.numberOfFields() <= 0) {
                        xpe = xpath.compile(value);
                        hmap.put(str, xpe);
                    }
                    else { // dynamic parameters
                        hmap.put(str, tp);
                        option ++;
                    }
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + ": failed to "+
                        "compile XPath expression of '"+ str + "' for " + key));
                }
                asset[ASSET_TSUB] = hmap;
            }
            else if((o = ph.get("XPath")) != null && o instanceof String){//XCut
                mask += EVAL_XPATH + EVAL_XSLT;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                if (xpath == null) try {
                    builder = Utils.getDocBuilder();
                    xpath = XPathFactory.newInstance().newXPath();
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to instantiate XPath: " + Event.traceStack(ex)));
                }
                catch (Error ex) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XPath for "+ uri +": "+ Event.traceStack(ex)));
                }
                str = (String) o;
                XPathExpression xpe;
                try {
                    xpe = xpath.compile(str);
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + ": failed to "+
                        "compile XPath expression of '"+ str + "' for " + key));
                }
                asset[ASSET_TSUB] = xpe;
            }
            else if ((o = ph.get("XMLField")) != null) { // XMerge
                XPathExpression xpe = null;
                asset[ASSET_DATA] = (String) o;
                mask += EVAL_XPATH + EVAL_XSLT;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                if (xpath == null) try {
                    builder = Utils.getDocBuilder();
                    xpath = XPathFactory.newInstance().newXPath();
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to instantiate XPath: " + Event.traceStack(ex)));
                }
                catch (Error ex) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XPath for "+ uri +": "+ Event.traceStack(ex)));
                }
                if ((o = ph.get("TargetXPath")) != null) {
                    str = (String) o;
                }
                else
                    str = "/*/*";
                try {
                    xpe = xpath.compile(str);
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + ": failed to "+
                        "compile XPath expression of '" +str+ "' for " + key));
                }
                asset[ASSET_TSUB] = xpe;

                if ((o = ph.get("SourceXPath")) != null) {
                    str = (String) o;
                }
                else
                    str = "/*/*";
                try {
                    xpe = xpath.compile(str);
                }
                catch (Exception ex) {
                    throw(new IllegalArgumentException(uri + ": failed to "+
                        "compile XPath expression of '" +str+ "' for " + key));
                }
                asset[ASSET_TEMP] = xpe;

                if (defaultTransformer == null) try {
                    if (tFactory == null)
                        tFactory = TransformerFactory.newInstance();
                    defaultTransformer = tFactory.newTransformer();
                  defaultTransformer.setOutputProperty(OutputKeys.INDENT,"yes");
                }
                catch (TransformerConfigurationException e) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to get the default transformer: " +
                         Event.traceStack(e)));
                }
                catch (Error e) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XSLT factory for "+ uri +" "+Event.traceStack(e)));
                }

                if((o = ph.get("ReplaceMode")) != null && o instanceof String &&
                    "true".equalsIgnoreCase((String) o))
                    option = 1;
            }
            else if ((o = ph.get("XSLFile")) != null) { // XSLTs
                mask += EVAL_XSLT;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                str = (String) o;
                if (!xslMap.containsKey(str)) try {
                    if (tFactory == null)
                        tFactory = TransformerFactory.newInstance();
                    o = tFactory.newTemplates(new StreamSource(str));
                    xslMap.put(str, o);
                }
                catch (TransformerConfigurationException e) {
                    throw(new IllegalArgumentException(uri + " failed " +
                         "to compile XSL template of " + str + ": " +
                         Event.traceStack(e)));
                }
                catch (Error e) {
                    throw(new IllegalArgumentException("failed to get " +
                        "XSLT factory for "+ uri +" "+Event.traceStack(e)));
                }
                asset[ASSET_DATA] = str;
                asset[ASSET_TEMP] = xslMap.get(str);
                if ((o = ph.get("XSLParameter")) != null && o instanceof Map) {
                    String value;
                    Template tp;
                    iter = ((Map) o).keySet().iterator();
                    hmap = new HashMap<String, Object>();
                    while (iter.hasNext()) {
                        str = (String) iter.next();
                        if (str == null || str.length() <= 0)
                            continue;
                        value = (String) ((Map) o).get(str);
                        tp = new Template(value);
                        if (tp == null || tp.numberOfFields() <= 0)
                            hmap.put(str, value);
                        else { // dynamic parameters
                            hmap.put(str, tp);
                            option ++;
                        }
                    }
                    if (hmap.size() > 0)
                        asset[ASSET_TSUB] = hmap;
                }
            }
            else if ((o = ph.get("ActiveTime")) != null && // for time window
                o instanceof Map && ph.containsKey("KeyTemplate")) {
                mask += EVAL_AGE;
                TimeWindows tw = new TimeWindows((Map) o);
                asset[ASSET_OBJECT] = tw;
                asset[ASSET_TEMP] = new Template((String) o);
                if ((o = ph.get("KeySubstitution")) != null)
                    asset[ASSET_TSUB] = new TextSubstitution((String) o);
                if ((o = ph.get("TimePattern")) != null)
                    asset[ASSET_METHOD] = new SimpleDateFormat((String) o);
                option = tw.getThresholdLength();
            }
            else if ((o = ph.get("AgeRange")) != null && // for file age
                o instanceof List && ((List) o).size() > 0) {
                mask += EVAL_FILE;
                asset[ASSET_OBJECT] = new DataSet((List) o);
                if ((o = ph.get("Template")) != null)
                    asset[ASSET_TEMP] = new Template((String) o);
                if ((o = ph.get("Substitution")) != null)
                    asset[ASSET_TSUB] = new TextSubstitution((String) o);
                if ((o = ph.get("ResultField")) != null && o instanceof String)
                    asset[ASSET_MNAME] = (String) o;
                option = 0;
            }
            else if ((o = ph.get("SizeRange")) != null && // for file size
                o instanceof List && ((List) o).size() > 0) {
                mask += EVAL_FILE;
                asset[ASSET_OBJECT] = new DataSet((List) o);
                if ((o = ph.get("Template")) != null)
                    asset[ASSET_TEMP] = new Template((String) o);
                if ((o = ph.get("Substitution")) != null)
                    asset[ASSET_TSUB] = new TextSubstitution((String) o);
                if ((o = ph.get("ResultField")) != null && o instanceof String)
                    asset[ASSET_MNAME] = (String) o;
                option = 1;
            }
            else if ((o = ph.get("Translation")) != null) { // for translations
                mask += EVAL_TRANS;
                if ((mask & EVAL_PATTERN) == 0)
                    mask += EVAL_PATTERN;
                if ("XML2JSON".equalsIgnoreCase((String) o)) {
                    option = 1;
                    if (xh == null) try {
                        str = (String) System.getProperty("org.xml.sax.driver",
                            null);
                        if (str == null)
                            str = "org.apache.xerces.parsers.SAXParser";
                        xh = new XML2Map(str);
                    }
                    catch (Exception e) {
                        throw(new IllegalArgumentException(uri + " failed " +
                            "to init XML parser: " + Event.traceStack(e)));
                    }
                }
                else if ("JSON2XML".equalsIgnoreCase((String) o)) {
                    option = 2;
                }
                else if ("PHP2XML".equalsIgnoreCase((String) o)) {
                    option = 3;
                }
                else if ("PHP2JSON".equalsIgnoreCase((String) o)) {
                    option = 4;
                }
                else
                    option = 0;
            }
            assetList.add(asset, id);

            if ((o = ph.get("StringProperty")) != null && o instanceof Map) {
                iter = ((Map) o).keySet().iterator();
                k = ((Map) o).size();
                String[] pn = new String[k];
                k = 0;
                while (iter.hasNext()) {
                    str  = (String) iter.next();
                    if ((pn[k] = MessageUtils.getPropertyID(str)) == null)
                        pn[k] = str;
                    k ++;
                }
                asset[ASSET_PNAME] = pn;
            }
            cache.insert(key, tm, 0, new int[]{id, mask, option, dmask},filter);
            if (debug > 0)
                strBuf.append("\n\t" + id + ": " + key + " " + mask + " " +
                    option + " " + dmask);
        }
        if (debug > 0)
            new Event(Event.DEBUG, uri + " id: Rule Mask Option DMask - "+
                assetList.depth()+ " rulesets initialized" +
                strBuf.toString()).send();

        return assetList.depth();
    }

    public void persist(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(PSTR_READY, PSTR_RUNNING);

        for (;;) {
            while (keepRunning(xq) && (status == PSTR_RUNNING ||
                status == PSTR_RETRYING)) { // session
                evaluate(xq);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // disabled temporarily
                    if (status == PSTR_READY) { // for confirmation
                        setStatus(PSTR_DISABLED);
                    }
                    else if (status == PSTR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > PSTR_RETRYING && status < PSTR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == PSTR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == PSTR_PAUSE) {
                if (status > PSTR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > PSTR_PAUSE)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            while ((xq.getGlobalMask() & XQueue.STANDBY) > 0 ||
                status == PSTR_STANDBY) {
                if (status > PSTR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > PSTR_STANDBY)
                        break;
                    try {
                        Thread.sleep(waitTime);
                    }
                    catch (Exception e) {
                    }
                    if (tt <= System.currentTimeMillis())
                        break;
                }
            }

            if (isStopped(xq) || status >= PSTR_STOPPED)
                break;
            if (status == PSTR_READY) {
                setStatus(PSTR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
            sessionTime = System.currentTimeMillis();
        }
        if (status < PSTR_STOPPED)
            setStatus(PSTR_STOPPED);

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * It picks up a message from input queue and evaluates its content to
     * decide what return code to propagate.  The evaluation may
     * modify the content.
     */
    public void evaluate(XQueue in) {
        Message outMessage;
        String key, str, msgStr = null;
        int[] list;
        long currentTime, count = 0;
        int i = 0, id, mask, option, dmask;
        int sid = -1; // the cell id of the message in input queue
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String failRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String excpRC = String.valueOf(MessageUtils.RC_UNKNOWN);

        byte[] buffer = new byte[bufferSize];

        while (((mask = in.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = in.getNextCell(waitTime)) < 0) {
                continue;
            }

            if ((outMessage = (Message) in.browse(sid)) == null) {
                in.remove(sid);
                new Event(Event.WARNING, uri + " dropped a null msg from " +
                    in.getName()).send();
                continue;
            }

            // copy the original RC and set the default RC
            try {
                msgStr = MessageUtils.getProperty(rcField, outMessage);
                MessageUtils.setProperty(orcField, msgStr, outMessage);
                MessageUtils.setProperty(rcField, excpRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(orcField, msgStr, outMessage);
                    MessageUtils.setProperty(rcField, excpRC, outMessage);
                }
                catch (Exception ex) {
                    in.remove(sid);
                    new Event(Event.WARNING, uri +
                        " failed to set RC on msg from "+ in.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                in.remove(sid);
                new Event(Event.WARNING, uri + " failed to set RC on msg from "+
                    in.getName()).send();
                outMessage = null;
                continue;
            }

            key = null;
            try {
                key = MessageUtils.getProperty(fieldName, outMessage);
            }
            catch (JMSException e) {
            }

            if (key == null || key.length() <= 0) {
                new Event(Event.WARNING, uri + " no such key defined at " +
                    fieldName).send();
            }
            else if (key.indexOf(",") > 0) { // for CommaList of multiple rules
                int j = 0;
                String[] keyList = Utils.split(",", key);
                msgStr = null;
                i = FAILURE;
                id = -1;
                dmask = 0;
                for (String ky : keyList) {
                    ky = ky.trim();
                    if (!cache.containsKey(ky)) {
                        new Event(Event.ERR, uri +
                            " no such rule found for " + ky + " at " +
                            j + " in CommaList: " + key).send();
                        i = EXCEPTION;
                        break;
                    }
                    list = cache.getMetaData(ky);
                    id = list[0];
                    mask = list[1];
                    option = list[2];
                    dmask = list[3];

                    if ((mask & EVAL_PATTERN) > 0) try {
                        msgStr = MessageUtils.processBody(outMessage, buffer);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, uri + " " + ky +
                           " failed to get message body: "+e.toString()).send();
                        i = EXCEPTION;
                        break;
                    }

                    currentTime = System.currentTimeMillis();
                    i = checkRule(currentTime, id, mask, option, ky,
                        outMessage, msgStr);

                    if (i != SUCCESS) // nohit or exception
                        break;
                    else if (mask > EVAL_FILTER) try { // invoke formatter
                        i = invokeRule(list, ky, outMessage, msgStr, buffer);
                    }
                    catch (Exception e) {
                        i = EXCEPTION;
                        break;
                    }
                    catch (Error e) {
                        in.remove(sid);
                        outMessage = null;
                        Event.flush(e);
                    }

                    try {
                        MessageUtils.setProperty(rcField, okRC, outMessage);
                        if (dmask > 0) { // display the message
                            Object[] asset = (Object[]) assetList.browse(id);
                            String[] pname = (String[]) asset[ASSET_PNAME];
                            new Event(Event.INFO, uri + " " + ky + ", " +
                                (count+1) + "/" + sid + " " + i + " " + mask +
                                " " + MessageUtils.display(outMessage, msgStr,
                                dmask, pname)).send();
                        }
                        MessageUtils.setProperty(orcField, okRC, outMessage);
                    }
                    catch (Exception e) {
                    }
                    j++;
                }
                count ++;

                str = (i == SUCCESS) ? okRC :((i == FAILURE) ? failRC : excpRC);
                try {
                    MessageUtils.setProperty(rcField, str, outMessage);
                    if (i != SUCCESS && dmask > 0) { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + keyList[j].trim() +
                            ", " + count + "/" + sid + " " + i + " " + mask +
                            " " + MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                }
                catch (Exception e) {
                }
            }
            else if (key.indexOf("|") > 0) { // for PipeList of multiple rules
                int j = 0;
                String[] keyList = Utils.split("|", key);
                msgStr = null;
                i = FAILURE;
                id = -1;
                dmask = 0;
                for (String ky : keyList) {
                    ky = ky.trim();
                    if (!cache.containsKey(ky)) {
                        new Event(Event.ERR, uri +
                            " no such rule found for " + ky + " at " +
                            j + " in PipeList: " + key).send();
                        i = EXCEPTION;
                        break;
                    }
                    list = cache.getMetaData(ky);
                    id = list[0];
                    mask = list[1];
                    option = list[2];
                    dmask = list[3];

                    if (msgStr == null && (mask & EVAL_PATTERN) > 0) try {
                        msgStr = MessageUtils.processBody(outMessage, buffer);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, uri + " " + ky +
                           " failed to get message body: "+e.toString()).send();
                        i = EXCEPTION;
                        break;
                    }

                    currentTime = System.currentTimeMillis();
                    i = checkRule(currentTime, id, mask, option, ky,
                        outMessage, msgStr);

                    if (i == EXCEPTION)
                        break;
                    else if (i == FAILURE) {
                        j ++;
                        continue;
                    }
                    else if (mask > EVAL_FILTER) try { // invoke formatter
                        i = invokeRule(list, ky, outMessage, msgStr, buffer);
                    }
                    catch (Exception e) {
                        i = EXCEPTION;
                        break;
                    }
                    catch (Error e) {
                        in.remove(sid);
                        outMessage = null;
                        Event.flush(e);
                    }

                    if (dmask > 0) try { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + ky + "| " +
                            (count+1) + "/" + sid + " " + i + " " + mask +
                            " " + MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                    catch (Exception e) {
                    }
                    break;
                }
                count ++;

                str = (i == SUCCESS) ? okRC :((i == FAILURE) ? failRC : excpRC);
                try {
                    MessageUtils.setProperty(rcField, str, outMessage);
                    if (i != SUCCESS && dmask > 0) { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + keyList[j].trim() +
                            "| " + count + "/" + sid + " " + i + " " + mask +
                            " " + MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                }
                catch (Exception e) {
                }
            }
            else if (key.indexOf(":") > 0) { // for ColonList of multiple rules
                int j = 0;
                String[] keyList = Utils.split(":", key);
                msgStr = null;
                i = FAILURE;
                id = -1;
                dmask = 0;
                for (String ky : keyList) {
                    ky = ky.trim();
                    if (!cache.containsKey(ky)) {
                        new Event(Event.ERR, uri +
                            " no such rule found for " + ky + " at " +
                            j + " in ColonList:" + key).send();
                        i = EXCEPTION;
                        break;
                    }
                    list = cache.getMetaData(ky);
                    id = list[0];
                    mask = list[1];
                    option = list[2];
                    dmask = list[3];

                    if (msgStr == null && (mask & EVAL_PATTERN) > 0) try {
                        msgStr = MessageUtils.processBody(outMessage, buffer);
                    }
                    catch (Exception e) {
                        new Event(Event.ERR, uri + " " + ky +
                           " failed to get message body: "+e.toString()).send();
                        i = EXCEPTION;
                        break;
                    }

                    currentTime = System.currentTimeMillis();
                    i = checkRule(currentTime, id, mask, option, ky,
                        outMessage, msgStr);

                    if (i == EXCEPTION)
                        break;
                    else if (i == FAILURE) {
                        j ++;
                        continue;
                    }
                    else if (mask > EVAL_FILTER) try { // invoke formatter
                        i = invokeRule(list, ky, outMessage, msgStr, buffer);
                    }
                    catch (Exception e) {
                        i = EXCEPTION;
                        break;
                    }
                    catch (Error e) {
                        in.remove(sid);
                        outMessage = null;
                        Event.flush(e);
                    }

                    if (dmask > 0) try { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + ky + ": " +
                            (count+1) + "/" + sid + " " + i + " " + mask +
                            " " + MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                    catch (Exception e) {
                    }
                    j++;
                }
                count ++;

                str = (i == SUCCESS) ? okRC :((i == FAILURE) ? failRC : excpRC);
                try {
                    MessageUtils.setProperty(rcField, str, outMessage);
                    if (i != SUCCESS && dmask > 0) { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + keyList[j].trim() +
                            ": " + count + "/" + sid + " " + i + " " + mask +
                            " " + MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                }
                catch (Exception e) {
                }
            }
            else if (!cache.containsKey(key)) { // no such rule
                new Event(Event.WARNING, uri + " no such rule found for: " +
                    key).send();
            }
            else { // for a single rule
                list = cache.getMetaData(key);
                id = list[0];
                mask = list[1];
                option = list[2];
                dmask = list[3];

                msgStr = null;
                if ((mask & EVAL_PATTERN) > 0) try {
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                }
                catch (Exception e) {
                    in.remove(sid);
                    new Event(Event.ERR, uri + ": " + key +
                        " failed to get message body: "+ e.toString()).send();
                    outMessage = null;
                    continue;
                }

                currentTime = System.currentTimeMillis();
                i = checkRule(currentTime, id, mask, option, key,
                    outMessage, msgStr);
                count ++;

                if (i == SUCCESS && mask > EVAL_FILTER) try { //invoke formatter
                    i = invokeRule(list, key, outMessage, msgStr, buffer);
                }
                catch (Exception e) {
                    i = EXCEPTION;
                }
                catch (Error e) {
                    in.remove(sid);
                    outMessage = null;
                    Event.flush(e);
                }

                str = (i == SUCCESS) ? okRC :((i == FAILURE) ? failRC : excpRC);
                try {
                    MessageUtils.setProperty(rcField, str, outMessage);
                    if (dmask > 0) { // display the message
                        Object[] asset = (Object[]) assetList.browse(id);
                        String[] pname = (String[]) asset[ASSET_PNAME];
                        new Event(Event.INFO, uri + " " + key + " " +
                            count + "/" + sid + " " + i + " " + mask + " " +
                            MessageUtils.display(outMessage, msgStr,
                            dmask, pname)).send();
                    }
                }
                catch (Exception e) {
                }
            }
            in.remove(sid);
            outMessage = null;
        }
    }

    /**
     * checks the specific rule on the message with the filter and returns
     * SUCCESS for hit, or FAILURE for nohit, or EXCEPTION otherwise.
     */
    private int checkRule(long currentTime, int id, int mask, int option,
        String key, Message outMessage, String msgStr) {
        int i = FAILURE;
        MessageFilter filter = (MessageFilter) cache.get(key);

        try {
            if (filter.evaluate(outMessage, msgStr))
                i = SUCCESS;
        }
        catch (Exception e) {
            i = EXCEPTION;
            new Event(Event.ERR, uri + " " + key +
                ": failed to evaluate: "+Event.traceStack(e)).send();
        }

        if (i != SUCCESS) { // nohit or exceptioned
            return i;
        }
        else if ((mask & EVAL_AGE) > 0) {
            Date d = null;
            byte[] buffer = new byte[4096];
            Object[] asset = (Object[]) assetList.browse(id);
            TimeWindows tw = (TimeWindows) asset[ASSET_OBJECT];
            Template tmp = (Template) asset[ASSET_TEMP];
            TextSubstitution sub = (TextSubstitution) asset[ASSET_TSUB];
            DateFormat dateFormat = (DateFormat) asset[ASSET_METHOD];
            i = EXCEPTION;
            if (tmp != null && dateFormat != null) try {
                String str = MessageUtils.format(outMessage, buffer, tmp);
                if (sub != null)
                    str = sub.substitute(str);
                if (str != null)
                    d = dateFormat.parse(str, new ParsePosition(0));
            }
            catch (Exception e) {
                new Event(Event.ERR, uri + " " + key +
                    ": failed to get age: "+Event.traceStack(e)).send();
            }
            if (d != null && tw != null) {
                i = tw.check(currentTime, d.getTime());
                if (option >= 2) // for age
                    i = (i == TimeWindows.NORMAL) ? SUCCESS : FAILURE;
                else
                    i = (i == TimeWindows.OCCURRED) ? SUCCESS : FAILURE;
            }
        }
        else if ((mask & EVAL_FILE) > 0) {
            File file = null;
            byte[] buffer = new byte[4096];
            Object[] asset = (Object[]) assetList.browse(id);
            Template tmp = (Template) asset[ASSET_TEMP];
            TextSubstitution sub = (TextSubstitution) asset[ASSET_TSUB];
            DataSet ds = (DataSet) asset[ASSET_OBJECT];
            i = EXCEPTION;
            if (tmp != null) try {
                String str = MessageUtils.format(outMessage, buffer, tmp);
                if (sub != null)
                    str = sub.substitute(str);
                if (str != null)
                    file = new File(str);
            }
            catch (Exception e) {
                new Event(Event.ERR, uri + ": " + key +
                    " failed to get url: "+Event.traceStack(e)).send();
            }

            if (file != null && file.exists() && file.canRead()) {
                long tm;
                String str = (String) asset[ASSET_MNAME];
                if (option > 0) // check file size
                    tm = file.length();
                else // check file age
                    tm = currentTime - file.lastModified();
                i = (ds.contains(tm)) ? SUCCESS : FAILURE;
                if (str != null && str.length() > 0) try {
                    MessageUtils.setProperty(str,String.valueOf(tm),outMessage);
                }
                catch (Exception e) {
                    new Event(Event.ERR, uri + ": " + key +
                       " failed to set property at " + str +
                       ": "+ Event.traceStack(e)).send();
                }
            }
            else if (file != null) {
                new Event(Event.ERR, uri + ": " + key +
                    " failed to access the file: " + file.getPath()).send();
            }
        }

        return i;
    }

    /**
     * invokes the formatter of the specific rule and returns SUCCESS for OK,
     * or FAILURE for failure. Otherwise, it returns EXCEPTION.
     */
    private int invokeRule(int[] list, String key, Message outMessage,
        String msgStr, byte[] buffer) {
        int i = SUCCESS;

        int id = list[0];
        int mask = list[1];
        int option = list[2];

        Object[] asset = (Object[]) assetList.browse(id);

        if ((mask & EVAL_FORMAT) > 0) {
            MessageFilter filter = (MessageFilter) cache.get(key);
            if (filter.hasFormatter()) try {
                i = filter.format(outMessage, buffer);
                if (i >= 0)
                    i = SUCCESS;
                else
                    i = FAILURE;
            }
            catch (Exception e) {
                i = EXCEPTION;
                new Event(Event.ERR, uri + ": " + key +
                    " failed to format the msg: "+ e.toString()).send();
            }
            if (i == SUCCESS && asset[ASSET_DATA] != null) try { // body format
                Template tmp = (Template) asset[ASSET_TEMP];
                msgStr = MessageUtils.format(outMessage, buffer, tmp);
                outMessage.clearBody();
                if (outMessage instanceof TextMessage)
                    ((TextMessage) outMessage).setText(msgStr);
                else
                    ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());
            }
            catch (Exception e) {
                i = EXCEPTION;
                new Event(Event.ERR, uri + ": " + key +
                    " failed to format body: " + e.toString()).send();
            }
        }
        else if ((mask & EVAL_CACHE) > 0 && asset[ASSET_DATA] != null) {
            Template tmp = (Template) asset[ASSET_TEMP];
            TextSubstitution sub = (TextSubstitution) asset[ASSET_TSUB];
            Map map = (Map) asset[ASSET_DATA];
            String str = null;
            try {
                str = MessageUtils.format(outMessage, buffer, tmp);
                if (sub != null)
                    str = sub.substitute(str);
                if (str != null && str.length() > 0)
                    i = SUCCESS;
                else {
                    i = EXCEPTION;
                    new Event(Event.ERR, uri + ": " + key +
                        " failed to get cache key").send();
                }
            }
            catch (Exception e) {
                i = EXCEPTION;
                new Event(Event.ERR, uri + ": " + key +
                    " failed to get cache key: "+ e.toString()).send();
            }
            if (i == SUCCESS) {
                msgStr = (String) map.get(str);
                if (msgStr == null) {
                    i = EXCEPTION;
                    new Event(Event.WARNING, uri + ": " + key +
                        " found no cache for " + str).send();
                }
                else try {
                    str = (String) asset[ASSET_MNAME];
                    if ("body".equals(str)) {  
                        outMessage.clearBody();
                        if (outMessage instanceof TextMessage)
                            ((TextMessage) outMessage).setText(msgStr);
                        else
                      ((BytesMessage) outMessage).writeBytes(msgStr.getBytes());
                    }
                    else
                        MessageUtils.setProperty(str, msgStr, outMessage);
                }
                catch (Exception e) {
                    i = EXCEPTION;
                    new Event(Event.ERR, uri + ": " + key +
                        " failed to set property of " + str + ": "+
                        e.toString()).send();
                }
            }
        }
        else if ((mask & EVAL_PARSE) > 0 && asset[ASSET_TEMP] != null &&
            option > 0) { // for parse
            i = parse(key, msgStr, option, (SimpleParser) asset[ASSET_TEMP],
                outMessage);
        }
        else if ((mask & EVAL_JSONT) > 0 && asset[ASSET_DATA] != null &&
            asset[ASSET_TEMP] != null && msgStr != null) { // for jsont
            i = transform(key, (String) asset[ASSET_DATA], msgStr, option,
                (JSONTemplate) asset[ASSET_TEMP], (Map) asset[ASSET_TSUB],
                buffer, outMessage);
        }
        else if ((mask & EVAL_JSONT) > 0 && asset[ASSET_TSUB] != null &&
            asset[ASSET_TSUB] instanceof JSONFormatter) { // for json formatter
            i = jformat(key, msgStr, (JSONFormatter) asset[ASSET_TSUB],
                outMessage);
        }
        else if ((mask & EVAL_JSONPATH) > 0 && asset[ASSET_TSUB] != null &&
            asset[ASSET_TSUB] instanceof Map) { // for json path
            i = jparse(key, msgStr, 0, (Map)asset[ASSET_TSUB], buffer,
                outMessage);
        }
        else if ((mask & EVAL_XSLT) > 0 && (mask & EVAL_XPATH) > 0 &&
            asset[ASSET_DATA] != null && msgStr != null) { // for xmerge
            i = xmerge(key, (String) asset[ASSET_DATA], msgStr, option,
                (XPathExpression) asset[ASSET_TEMP],
                (XPathExpression) asset[ASSET_TSUB], defaultTransformer,
                outMessage);
        }
        else if ((mask & EVAL_XSLT) > 0 && (mask & EVAL_XPATH) > 0 &&
            msgStr != null) { // for xcut
            i = xcut(key, msgStr, (XPathExpression) asset[ASSET_TSUB],
                defaultTransformer, outMessage);
        }
        else if ((mask & EVAL_XSLT) > 0 && asset[ASSET_DATA] != null &&
            tFactory != null && msgStr != null) { // for xslt
            i = transform(key, (String) asset[ASSET_DATA], msgStr, option,
                (Templates) asset[ASSET_TEMP], (Map) asset[ASSET_TSUB],
                buffer, outMessage);
        }
        else if ((mask & EVAL_XPATH) > 0 && builder != null &&
            xpath != null && msgStr != null) { // for xpath
            i = xparse(key, msgStr, option, (Map) asset[ASSET_TSUB], buffer,
                outMessage);
        }
        else if ((mask & EVAL_TRANS) > 0 && msgStr != null) { // for translation
            i = translation(key, option, msgStr, outMessage);
        }
        else if ((mask & EVAL_PLUGIN) == 0 ||
            asset[ASSET_MNAME] == null) { // something wrong
            i = EXCEPTION;
            new Event(Event.WARNING, uri + ": " + key +
                " no name defined for post process: " + mask).send();
        }
        else try { // for plug-ins
            Object o;
            java.lang.reflect.Method method =
                (java.lang.reflect.Method) asset[ASSET_METHOD];
            o = method.invoke(asset[ASSET_OBJECT], new Object[]{outMessage});
            if (o != null) {
                i = EXCEPTION;
                new Event(Event.WARNING, uri + ": " + key +
                    " failed to transform the msg: "+ o.toString()).send();
            }
        }
        catch (Exception e) {
            String str = uri + " " + key;
            Exception ex = null;
            if (e instanceof JMSException)
                ex = ((JMSException) e).getLinkedException();
            if (ex != null)
                str += " Linked exception: " + ex.toString() + "\n";
            i = EXCEPTION;
            new Event(Event.ERR, str + " failed to transform the msg: "+
                Event.traceStack(e)).send();
        }
        catch (Error e) {
            i = EXCEPTION;
            String str = uri + ": " + key;
            new Event(Event.ERR, uri + ": " + key +
                " failed to transform the msg: "+ e.toString()).send();
            Event.flush(e);
        }

        return i;
    }

    /**
     * It transforms the XML content according to the template of xsl and the
     * parameters.  The message will be modified as the result of the
     * transformation.  Upon success, it returns SUCCESS as the index
     * of the outlink.  Otherwise, EXCEPTION is returned.
     */
    private int transform(String name, String xsl, String xml, int option,
        Templates template, Map params, byte[] buffer, Message msg) {
        int i;
        Iterator iter;
        Object o;
        Transformer transformer;
        String key = null, value;

        if (xml == null || xml.length() <= 0 || template == null || msg == null)
            return EXCEPTION;

        try {
            transformer = template.newTransformer();
        }
        catch (TransformerException e) {
            new Event(Event.ERR, uri +": " + name +
                " failed to get transformer for " + xsl + ": " +
                Event.traceStack(e)).send();
            return EXCEPTION;
        }

        if (params != null && option == 0) { // without dynamic parameters
            for (iter=params.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null)
                    continue;
                o = params.get(key);
                if (o == null || !(o instanceof String))
                    continue;
                value = (String) o;
                transformer.setParameter(key, value);
            }
        }
        else if (params != null) try { // with dynamic parameters
            for (iter=params.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null)
                    continue;
                o = params.get(key);
                if (o == null)
                    continue;
                else if (o instanceof Template)
                    value = MessageUtils.format(msg, buffer, (Template) o);
                else if (o instanceof String)
                    value = (String) o;
                else
                    continue;
                transformer.setParameter(key, value);
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to get params at " + key + " for " + xsl + ": " +
               Event.traceStack(e)).send();
            return EXCEPTION;
        }

        StringReader sr = new StringReader(xml);
        try {
            StringWriter sw = new StringWriter();
            transformer.transform(new StreamSource(sr), new StreamResult(sw));
            value = sw.toString();
            sw.close();
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to write for " + xsl +
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (TransformerException e) {
            new Event(Event.ERR, uri + ": " + name +
               " transform failed for " + xsl +
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        sr.close();

        if (value != null && (i = value.length()) > 0) try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(value);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(value.getBytes(), 0, i);
            else {
                new Event(Event.ERR, uri + ": " + name + 
                    " got unsupported type for " + xsl).send();
                return EXCEPTION;
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to set message body for " + xsl+
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    /**
     * It transforms the JSON content according to the JSON template and the
     * parameters.  The message will be modified as the result of the
     * transformation.  Upon success, it returns SUCCESS as the index
     * of the outlink.  Otherwise, EXCEPTION is returned.
     */
    private int transform(String name, String tag, String json, int option,
        JSONTemplate template, Map params, byte[] buffer, Message msg) {
        int i;
        Object o;
        Map ph = null;
        List pl = null;
        String key = null, value;
        boolean isArray = false;

        if (json== null ||json.length() <= 0 || template == null || msg == null)
            return EXCEPTION;

        StringReader sr = new StringReader(json);

        try {
            o = (!template.keepOriginalOrder()) ? JSON2FmModel.parse(sr) :
                JSON2Map.parseJSON(sr, true);
        }
        catch (IOException e) {
            sr.close();
            new Event(Event.ERR, uri + ": " + name +
               " failed to parse json payload: " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        sr.close();

        if (o == null)
            return EXCEPTION;

        template.clearParameter();
        if (params != null && option == 0) { // without dynamic parameters
            for (Iterator iter=params.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                template.setParameter(key, params.get(key));
            }
        }
        else if (params != null) try { // with dynamic parameters
            Object obj;
            for (Iterator iter=params.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                obj = params.get(key);
                if (obj == null)
                    continue;
                else if (obj instanceof Template)
                    template.setParameter(key, MessageUtils.format(msg,
                        buffer, (Template) obj));
                else
                    template.setParameter(key, obj);
            }
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to get params at " + key + " for json: " +
               Event.traceStack(e)).send();
            return EXCEPTION;
        }

        if (o instanceof Map)
            ph = (Map) o;
        else {
            pl = (List) o;
            isArray = true;
        }

        try {
            if (isArray)
                value = template.format(pl);
            else
                value = template.format(ph);
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to format json with " + tag +
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        if (value != null && (i = value.length()) > 0) try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(value);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(value.getBytes(), 0, i);
            else {
                new Event(Event.ERR, uri + ": " + name +
                    " got unsupported type for " + tag).send();
                return EXCEPTION;
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to set message body for " + tag+
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    /**
     * It evaluates the XML content based on the XPath expressions and
     * sets message properties with the evaluation data.  Upon success, it
     * returns SUCCESS as the index of the outlink.  Otherwise, EXCEPTION
     * is returned.
     */
    private int xparse(String name, String xml, int option, Map expression,
        byte[] buffer, Message msg) {
        Iterator iter;
        Object o;
        XPathExpression xpe;
        Document doc;
        String key = null, value;

        if (xml == null || xml.length() <= 0 || xpath == null || builder==null)
            return EXCEPTION;

        StringReader sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse XML payload in " +
                name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (SAXException e) {
            new Event(Event.ERR, uri + " failed to build XML DOM in " +
                name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        sr.close();

        if (expression != null && expression.size() > 0) try {
            for (iter=expression.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                o = expression.get(key);
                if (o == null)
                    continue;
                else if (o instanceof XPathExpression)
                    xpe = (XPathExpression) o;
                else if (o instanceof Template) {
                    value = MessageUtils.format(msg, buffer, (Template) o);
                    xpe = xpath.compile(value);
                }
                else
                    continue;
                value = xpe.evaluate(doc);
                MessageUtils.setProperty(key, value, msg);
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri+" failed to format msg for property at "+
                key + " in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to evaluate xpath property at "+
                key + " in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    /**
     * It extracts the XML content from the source XML specified by the key
     * based on the source XPath expressions, and merges the xml content into
     * the XML payload of the message at the position specified by the target
     * XPath expression. Upon success, it returns SUCCESS as the index of the
     * outlink.  Otherwise, EXCEPTION is returned.
     */
    private int xmerge(String name, String key, String xml, int option,
        XPathExpression source, XPathExpression target,
        Transformer transformer, Message msg) {
        XPathExpression xpe;
        NodeList list, nodes;
        Document doc;
        Object o;
        String str = null, value;
        int i, n;

        if (xml == null || xml.length() <= 0 || xpath == null ||
            builder == null || transformer == null)
            return EXCEPTION;

        if (key != null) try {
            str = MessageUtils.getProperty(key, msg);
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to get XML source from msg at "+
                key + " in " + name + ": " + Event.traceStack(e)).send();
            str = null;
        }

        if (str == null)
            return EXCEPTION;

        StringReader sr = new StringReader(str);
        try {
            doc = builder.parse(new InputSource(sr));
            list = (NodeList) source.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse XML source for " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (SAXException e) {
            new Event(Event.ERR, uri + " failed to build XML DOM for " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR, uri + " failed to evaluate xpath for " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        sr.close();

        sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
            nodes = (NodeList) target.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse XML payload in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (SAXException e) {
            new Event(Event.ERR, uri + " failed to build XML DOM in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR,uri+" failed to evaluate xpath on paylaod in "+
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        sr.close();

        o = null;
        try {
            n = nodes.getLength();
            if (n > 0)
                o = nodes.item(0).getParentNode();
            if (o != null) { // found the parent node
                Node parent = (org.w3c.dom.Node) o;
                if (option > 0) { // remove the seleceted nodes
                    for (i=0; i<n; i++) // remove all nodes from the target
                        parent.removeChild(nodes.item(i));
                }
                n = list.getLength();
                for (i=0; i<n; i++) // append all nodes from the source
                    parent.appendChild(doc.importNode(list.item(i), true));
                if (n > 0 || option > 0) { // target changed
                    StreamResult result = new StreamResult(new StringWriter());
                    transformer.transform(new DOMSource(doc), result);
                    msg.clearBody();
                    if (msg instanceof TextMessage)
                     ((TextMessage) msg).setText(result.getWriter().toString());
                    else
      ((BytesMessage) msg).writeBytes(result.getWriter().toString().getBytes());
                }
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + " failed to format msg for xml at " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to merge xml at " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    /**
     * It removes the XML content from the XML payload according to the
     * XPath expression. Upon success, it writes the updated XML payload
     * into the message body and returns SUCCESS as the index of the outlink.
     * Otherwise, EXCEPTION is returned.
     */
    private int xcut(String name, String xml, XPathExpression xpe,
        Transformer transformer, Message msg) {
        NodeList nodes;
        Document doc;
        Object o;
        int i, n;

        if (xml == null || xml.length() <= 0 || xpath == null ||
            builder == null || transformer == null || xpe == null)
            return EXCEPTION;

        StringReader sr = new StringReader(xml);
        try {
            doc = builder.parse(new InputSource(sr));
            nodes = (NodeList) xpe.evaluate(doc, XPathConstants.NODESET);
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse XML payload in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (SAXException e) {
            new Event(Event.ERR, uri + " failed to build XML DOM in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        catch (XPathExpressionException e) {
            new Event(Event.ERR,uri+" failed to evaluate xpath on paylaod in "+
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        sr.close();

        o = null;
        try {
            n = nodes.getLength();
            if (n > 0)
                o = nodes.item(0).getParentNode();
            if (o != null) { // found the parent node
                Node parent = (org.w3c.dom.Node) o;
                for (i=0; i<n; i++) // remove all nodes
                    parent.removeChild(nodes.item(i));
                if (n > 0) { // doc changed
                    StreamResult result = new StreamResult(new StringWriter());
                    transformer.transform(new DOMSource(doc), result);
                    msg.clearBody();
                    if (msg instanceof TextMessage)
                     ((TextMessage) msg).setText(result.getWriter().toString());
                    else
      ((BytesMessage) msg).writeBytes(result.getWriter().toString().getBytes());
                }
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + " failed to format msg of xml in " +
                name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to cut xml in " +
                name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    private int parse(String name, String text, int option,
        SimpleParser parser, Message msg) {
        int i;
        Object o;
        Iterator iter;
        String key, value;
        if (text == null || text.length() <= 0 || parser == null || msg == null)
            return EXCEPTION;

        boolean isText = (msg instanceof TextMessage);
        o = parser.parse(text);
        if (o == null || !(o instanceof Map)) {
            return EXCEPTION;
        }
        else try {
            Map hmap = (Map) o;
            for (iter=hmap.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                o = hmap.get(key);
                if (o == null || !(o instanceof String))
                    continue;
                value = (String) o;
                if ("body".equals(key)) {
                    msg.clearBody();
                    if (isText)
                        ((TextMessage) msg).setText(value);
                    else
                        ((BytesMessage) msg).writeBytes(value.getBytes());
                }
                else {
                    i = MessageUtils.setProperty(key, value, msg);
                }
            }
            hmap.clear();
        }
        catch (Exception e) {
            String str = uri + ": " + name;
            Exception ex = null;
            if (e instanceof JMSException)
                ex = ((JMSException) e).getLinkedException();
            if (ex != null)
                str += " Linked exception: " + ex.toString() + "\n";
            new Event(Event.ERR, str + " failed to parse the msg: "+
                Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    private int jparse(String name, String json, int option, Map expression,
        byte[] buffer, Message msg) {
        Iterator iter;
        Map ph = null;
        List pl = null;
        Object o;
        StringReader sr;
        String key = null, str = null, value;
        boolean isArray = false;

        if (json == null || json.length() <= 0)
            return EXCEPTION;

        sr = new StringReader(json);
        try {
            o = JSON2Map.parse(sr);
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse JSON payload in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        sr.close();
        if (o == null)
            return EXCEPTION;

        if (o instanceof Map)
            ph = (Map) o;
        else {
            pl = (List) o;
            isArray = true;
        }

        if (expression != null && expression.size() > 0) try {
            for (iter=expression.keySet().iterator(); iter.hasNext();) {
                key = (String) iter.next();
                o = expression.get(key);
                if (o == null)
                    continue;
                else if (o instanceof String)
                    str = (String) o;
                else if (o instanceof Template)
                    str = MessageUtils.format(msg, buffer, (Template) o);
                else
                    continue;
                if (isArray)
                    o = JSON2Map.get(pl, str);
                else
                    o = JSON2Map.get(ph, str);
                if (o == null)
                    continue;
                else if (o instanceof String)
                    value = (String) o;
                else if (o instanceof Map)
                    value = JSON2Map.toJSON((Map) o, null, "");
                else if (o instanceof List)
                    value = JSON2Map.toJSON((List) o, null, "");
                else
                    value = o.toString();
                MessageUtils.setProperty(key, value, msg);
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + " failed to format property at " +
                key + " in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }
        catch (Exception e) {
            new Event(Event.ERR, uri + " failed to format msg for json path '"+
                str + "' in " + name + ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        return SUCCESS;
    }

    /**
     * It parsed the json payload and formats it with the JSONFormatter. Upon
     * success, it loads JSON data to the message body and returns SUCCESS.
     * Otherwise, EXCEPTION is returned.
     */
    private int jformat(String name, String json, JSONFormatter formatter,
        Message msg) {
        Object o;
        StringReader sr;
        int k = 0;

        if (json == null || json.length() <= 0 || formatter == null)
            return EXCEPTION;

        sr = new StringReader(json);
        try {
            o = JSON2FmModel.parse(sr);
        }
        catch (IOException e) {
            new Event(Event.ERR, uri + " failed to parse JSON payload in " +
                name + ": " + Event.traceStack(e)).send();
            sr.close();
            return EXCEPTION;
        }
        sr.close();
        sr = null;
        if (o == null)
            return EXCEPTION;

        try {
            if (o instanceof Map)
                k = formatter.format((Map) o, msg);
            else
                k = formatter.format((List) o, msg);
        }
        catch (Exception e) {
            k = -1;
            new Event(Event.ERR, uri + " failed to format json payload for "+
                name + ": " + Event.traceStack(e)).send();
        }

        if (k < 0)
            return EXCEPTION;
        else
            return SUCCESS;
    }

    /**
     * It translates the text document to a different format
     */
    private int translation(String name, int option, String text, Message msg) {
        int i;
        String value = null;
        Object o;
        Map ph;
        Map<String, Object> map;
        try {
            StringReader sr = new StringReader(text);
            switch (option) {
              case 1:
                ph = xh.getMap(sr);
                value = JSON2Map.toJSON(ph);
                break;
              case 2:
                o = JSON2Map.parse(sr);
                map = new HashMap<String, Object>();
                map.put("JSON", o);
                value = JSON2Map.toXML(map);
                break;
              case 3:
                ph = PHP2Map.parse(sr);
                value = PHP2Map.toXML(ph);
                break;
              case 4:
                ph = PHP2Map.parse(sr);
                value = PHP2Map.toJSON(ph);
                break;
              default:
            }
            sr.close();
        }
        catch (Exception e) {
            new Event(Event.ERR, uri +": " + name +
               " failed to parse message body for translation " + option +
               ": " + Event.traceStack(e)).send();
            return EXCEPTION;
        }

        if (value != null && (i = value.length()) > 0) try {
            msg.clearBody();
            if (msg instanceof TextMessage)
                ((TextMessage) msg).setText(value);
            else if (msg instanceof BytesMessage)
                ((BytesMessage) msg).writeBytes(value.getBytes(), 0, i);
            else {
                new Event(Event.ERR, uri + ": " + name +
                    " got unsupported msg type for translation " +
                    option).send();
                return EXCEPTION;
            }
        }
        catch (JMSException e) {
            new Event(Event.ERR, uri + ": " + name +
               " failed to set message body for translation " + option + ": " +
               Event.traceStack(e)).send();
            return EXCEPTION;
        }
        return SUCCESS;
    }

    public String getRuleName(int i) {
        int n = assetList.depth();
        if (i >= 0 && i < n) {
            Object[] asset = (Object[]) assetList.browse(i);
            if (asset != null)
                return (String) asset[ASSET_NAME];
            else
                return null;
        }
        else
            return null;
    }

    public int getNumberOfRules() {
        return assetList.depth();
    }

    public void close() {
        int i, id, n;
        int[] list;
        Object[] asset;
        java.lang.reflect.Method close;
        if (status != PSTR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(PSTR_CLOSED);
        n = assetList.depth();
        list = new int[n];
        n = assetList.queryIDs(list, XQueue.CELL_OCCUPIED);
        for (i=0; i<n; i++) {
           id = list[i];
           asset = (Object[]) assetList.browse(id);
           close = (java.lang.reflect.Method) asset[ASSET_CLOSE];
           if (close != null) try {
               close.invoke(asset[ASSET_OBJECT], new Object[] {});
           }
           catch (Exception e) {
           }
           if (asset[ASSET_DATA] != null && asset[ASSET_DATA] instanceof Map)
               ((Map) asset[ASSET_DATA]).clear();
           if (asset[ASSET_TSUB] != null && asset[ASSET_TSUB] instanceof Map)
               ((Map) asset[ASSET_TSUB]).clear();
           assetList.getNextID(id);
           assetList.remove(id);
        }
        assetList.clear();
        cache.clear();
        tFactory = null;
        builder = null;
        xpath = null;
        defaultTransformer = null;
    }

    protected void finalize() {
        close();
    }
}
