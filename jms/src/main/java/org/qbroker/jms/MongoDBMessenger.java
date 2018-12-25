package org.qbroker.jms;

/* MongoDBMessenger.java - a MongoDatabase messenger for JMS messages */

import java.io.File;
import java.io.StringReader;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Iterator;
import java.text.SimpleDateFormat;
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
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.WriteResult;
import com.mongodb.WriteConcern;
import com.mongodb.MongoException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.QuickCache;
import org.qbroker.common.Utils;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.NonSQLException;
import org.qbroker.json.JSON2Map;
import org.qbroker.net.MongoDBConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * MongoDBMessenger connects to a Mongo database specified by URI and
 * initializes the operations of find and update, etc with JMS Messages on
 * various document collections.
 *<br/><br/>
 * There are four methods, findone(), list(), find() and update() for MongoDB
 * operations.  The method of findone() executes a predefined query on the
 * default collection.  The rest of methods extract both the collection name
 * and the command from the field of CollectionField and the JSON query and/or
 * the JSON data from the incoming messages. The method of list() is to list
 * all collections with the filter support.  The method of find() is
 * used for synchronous requests, whereas update() is for asynchronous
 * persisting request.  Therefore, if you what to update collections
 * automatically, please use update().  It will keep retry upon database
 * failures until the message expires.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MongoDBMessenger extends MongoDBConnector {
    private int timeout;
    private Template template;
    private QuickCache cache = null;
    private String[] propertyName = null;
    private String[] propertyValue = null;
    private int displayMask = 0;
    private int minSize, capacity, maxRetry, sleepTime = 0;
    private int resultType = Utils.RESULT_JSON;
    private int xaMode = 0;
    private int mode = 0;
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private SimpleDateFormat dateFormat = null;

    private int[] partition;
    private long maxMsgLength = 4194304;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int bufferSize = 4096;
    private String queryField, collField, rcField, resultField;
    private String skipField, limitField;
    private String operation = "find", defaultCollection = null;
    private DBObject defaultProjection = null, defaultQuery = null;

    public MongoDBMessenger(Map props) {
        super(props);
        Object o;
        
        if ((o = props.get("CollectionField")) != null)
            collField = (String) o;
        else
            collField = "Collection";

        if ((o = props.get("QueryField")) != null)
            queryField = (String) o;
        else
            queryField = "Query";

        if ((o = props.get("SkipField")) != null)
            skipField = (String) o;
        else
            skipField = "Skip";

        if ((o = props.get("LimitField")) != null)
            limitField = (String) o;
        else
            limitField = "Limit";

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("ResultField")) != null && o instanceof String)
            resultField = (String) o;
        else
            resultField = "QueryResult";

        if ((o = props.get("Capacity")) != null)
            capacity = Integer.parseInt((String) o);
        else
            capacity = 1;
        if ((o = props.get("MinSize")) != null)
            minSize = Integer.parseInt((String) o);
        else
            minSize = 1;
        if (minSize > capacity)
            minSize = capacity;

        if ((o = props.get("Retry")) == null ||
            (maxRetry = Integer.parseInt((String) o)) <= 0)
            maxRetry = 3;

        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String key, value, cellID;
            Template temp = new Template("${CellID}", "\\$\\{[^\\$\\{\\}]+\\}");
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            propertyName = new String[n];
            propertyValue = new String[n];
            cellID = (String) props.get("CellID");
            if (cellID == null || cellID.length() <= 0)
                cellID = "0";
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                value = (String) ((Map) o).get(key);
                if ((propertyName[n] = MessageUtils.getPropertyID(key)) == null)
                    propertyName[n] = key;
                if (value != null && value.length() > 0) {
                    propertyValue[n] = temp.substitute("CellID", cellID, value);
                }
                n ++;
            }
        }

        if ((o = props.get("TemplateFile")) != null && ((String)o).length() > 0)
            template = new Template(new File((String) o));
        else if ((o = props.get("Template")) != null && ((String) o).length()>0)
            template = new Template((String) o);

        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if ((o = props.get("Operation")) != null)
            operation = (String) props.get("Operation");
        if ((o = props.get("Partition")) != null) {
            partition = TimeWindows.parseThreshold((String) o);
            partition[0] /= 1000;
            partition[1] /= 1000;
        }
        else if ((o = props.get("CellID")) != null) {
            partition = new int[2];
            partition[0] = Integer.parseInt((String) o);
            partition[1] = 1;
        }
        else {
            partition = new int[2];
            partition[0] = 0;
            partition[1] = 0;
        }
        if ((o = props.get("BufferSize")) != null)
            bufferSize = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
        if ((o = props.get("ResultType")) != null)
            resultType = Integer.parseInt((String) o);
        if ((o = props.get("DisplayMask")) != null)
            displayMask = Integer.parseInt((String) o);
        if ((o = props.get("MaxNumberMessage")) != null)
            maxNumberMsg = Integer.parseInt((String) o);
        if ((o = props.get("MaxIdleTime")) != null) {
            maxIdleTime = 1000 * Integer.parseInt((String) o);
            if (maxIdleTime < 0)
                maxIdleTime = 0;
        }
        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }
        if ((o = props.get("WaitTime")) != null) {
            waitTime = Long.parseLong((String) o);
            if (waitTime <= 0L)
                waitTime = 500L;
        }
        if ((o = props.get("SleepTime")) != null)
            sleepTime= Integer.parseInt((String) o);
        if ((o = props.get("MaxMsgLength")) != null)
            maxMsgLength =Integer.parseInt((String) o);

        if ((o = props.get("DefaultCollection")) != null)
            defaultCollection = (String) o;

        if ((o = props.get("DefaultQuery")) != null) try {
            defaultQuery = parse((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri +
                " failed to parse DefaultQuery: " + e.toString()));
        }
        if (defaultQuery == null)
            defaultQuery = new BasicDBObject();

        if ((o = props.get("DefaultProjection")) != null) try {
            defaultProjection = parse((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(uri +
                " failed to parse DefaultProjection: " + e.toString()));
        }

        if ("list".equals(operation))
            cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 0);
    }

    /**
     * It executes the predfined query on the default collection and finds the
     * only one doc.  The doc will be put into the body of a new message.
     * The new message will be added to the XQueue as the output.
     */
    public void findone(XQueue xq) throws NonSQLException, JMSException {
        Message outMessage;
        StringBuffer strBuf = null;
        DBCollection coll = null;
        String collName = defaultCollection;
        int sid = -1, cid, rid, mask = 0;
        int i, retry;
        long currentTime, ttl = 0L, count = 0;
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

        switch (len) {
          case 0:
            do { // reserve an empty cell
                sid = xq.reserve(waitTime);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          case 1:
            do { // reserve the empty cell
                sid = xq.reserve(waitTime, shift);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
          default:
            do { // reserve a partitioned empty cell
                sid = xq.reserve(waitTime, shift, len);
                if (sid >= 0)
                    break;
                mask = xq.getGlobalMask();
            } while((mask & XQueue.KEEP_RUNNING)>0 && (mask & XQueue.PAUSE)==0);
            break;
        }

        if (sid >= 0) {
            if ((resultType & Utils.RESULT_BYTES) > 0)
                outMessage = (Message) new BytesEvent();
            else if ((resultType & Utils.RESULT_TEXT) > 0 ||
                (resultType & Utils.RESULT_XML) > 0 ||
                (resultType & Utils.RESULT_JSON) > 0)
                outMessage = (Message) new TextEvent();
            else
                outMessage = (Message) new ObjectEvent();
            outMessage.clearBody();
            isText = (outMessage instanceof TextMessage);
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return;
        }
        else {
            return;
        }

        coll = getCollection(collName);
        strBuf = null;

        try {
            DBObject o;
            if (defaultProjection != null)
                o = coll.findOne(defaultQuery, defaultProjection);
            else
                o = coll.findOne(defaultQuery);
            if (o != null)
                strBuf = new StringBuffer(o.toString());
        }
        catch (MongoException e) {
            xq.cancel(sid);
            throw(new NonSQLException(e, e.toString()));
        }
        catch (Exception e) {
            xq.cancel(sid);
            throw(new RuntimeException(e.getMessage()));
        }
        catch (Error e) {
            xq.cancel(sid);
            Event.flush(e);
        }

        currentTime = System.currentTimeMillis();
        if (strBuf == null) {
            xq.cancel(sid);
            return;
        }
        else try {
            if (propertyName != null && propertyValue != null) {
                outMessage.clearProperties();
                for (i=0; i<propertyName.length; i++)
                    MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], outMessage);
            }

            if (isText) {
                ((TextMessage) outMessage).setText(strBuf.toString());
            }
            else if (outMessage instanceof BytesMessage) {
                ((BytesMessage) outMessage).writeBytes(
                    strBuf.toString().getBytes());
            }
            MessageUtils.setProperty(resultField, "0", outMessage);
            outMessage.setJMSTimestamp(currentTime);
        }
        catch (JMSException e) {
            xq.cancel(sid);
            new Event(Event.WARNING, "failed to load msg with the doc " +
                "found from " + collName + ": " + Event.traceStack(e)).send();
            return;
        }

        cid = xq.add(outMessage, sid);
        if (cid >= 0) {
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, null);
                new Event(Event.INFO, "found the doc from " + collName +
                    " and set it into a msg (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "found the doc from " + collName +
                    " and set it into a msg").send();
            }
            count ++;
        }
        else {
            xq.cancel(sid);
        }
    }

    /** It lists all collections in the db */
    public void list(XQueue xq) throws NonSQLException, TimeoutException {
        Message outMessage;
        DBCursor cursor = null;
        DBObject o;
        DBCollection coll = null;
        long currentTime, tm, idleTime;
        long count = 0, stm = 10;
        int retry = 0, rc = 0, heartbeat = 600000, ttl = 7200000;
        int i, k, sid = -1, cid, rid, n, mask;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null, str = null, msgStr = null, collName = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            collName = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                collName = MessageUtils.getProperty(collField, outMessage);
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }
            if (collName == null || collName.length() <= 0) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, uri + ": failed to get " +
                    "collection name from msg").send();
                outMessage = null;
                continue;
            }

            msgStr = null;
            if ("*".equals(collName)) try { // query for the collection names
                MessageFilter[] filters = null;
                Map ph;
                StringBuffer strBuf;
                String[] keys = null;

                // get dynamic content filters
                msgStr = null;
                try { // retrieve dynamic content filters from body
                    msgStr = MessageUtils.processBody(outMessage, buffer);
                    if (msgStr == null || msgStr.length() <= 0)
                        filters = null;
                    else if (msgStr.indexOf("Ruleset") < 0)
                        filters = null;
                    else if (cache.containsKey(msgStr) &&
                        !cache.isExpired(msgStr, currentTime))
                        filters=(MessageFilter[])cache.get(msgStr,currentTime);
                    else { // new or expired
                        StringReader ins = new StringReader(msgStr);
                        ph = (Map) JSON2Map.parse(ins);
                        ins.close();
                        if (currentTime - cache.getMTime() >= heartbeat) {
                            cache.disfragment(currentTime);
                            cache.setStatus(cache.getStatus(), currentTime);
                        }
                        filters = MessageFilter.initFilters(ph);
                        if (filters != null && filters.length > 0)
                            cache.insert(msgStr,currentTime,ttl,null,filters);
                        else
                            filters = null;
                    }
                }
                catch (Exception e) {
                    filters = null;
                    new Event(Event.WARNING, xq.getName() +
                        ": failed to retrieve content filters for " + uri +
                        ": " + msgStr + ": " + Event.traceStack(e)).send();
                }

                if (filters != null && filters.length > 0) {
                    int j;
                    String[] list = list();
                    rc = list.length;
                    for (k=rc-1; k>=0; k--) {
                        str = list[k];
                        if (str == null || str.length() <= 0) {
                            rc --;
                            continue;
                        }
                        for (j=0; j<filters.length; j++) {
                            if (filters[j].evaluate(str))
                                break;
                        }

                        if (j >= filters.length) { // no hit
                            list[k] = null;
                            rc --;
                        }
                    }
                    keys = new String[rc];
                    k = 0;
                    for (j=0; j<list.length; j++) {
                        str = list[j];
                        if (str == null || str.length() <= 0)
                            continue;
                        keys[k++] = str;
                    }
                }
                else
                    keys = list();
                rc = keys.length;

                strBuf = getResult(resultType, keys);
                if (outMessage instanceof TextMessage) {
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    try {
                        MessageUtils.setProperty(rcField, msgRC, outMessage);
                    }
                    catch (Exception ex) {
                    }
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    outMessage);
                if (displayMask > 0) {
                    String line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
                    new Event(Event.INFO, xq.getName() + " found "+ rc +
                        " collections and set them into a msg ("+
                        line + " )").send();
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                outMessage = null;
                continue;
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to list collections for " + uri +
                    ": " + Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }
            coll = getCollection(collName);

            try {
                msgStr = MessageUtils.getProperty(queryField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    if (template != null)
                        msgStr = MessageUtils.format(outMessage, buffer,
                            template);
                    else
                        msgStr = MessageUtils.processBody(outMessage, buffer);
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            if (msgStr != null && msgStr.length() <= 0) {
                msgStr = null;
                o = null;
            }
            else try {
                o = parse(msgStr);
            }
            catch (Exception e) { // bad json data
                if (ack) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                String line = "";
                if (displayMask > 0) try {
                    line = MessageUtils.display(outMessage, null, displayMask,
                        propertyName);
                }
                catch (Exception ex) {
                    line = ex.toString();
                }
                new Event(Event.ERR, uri + ": failed to parse JSON data for " +
                    collName + ": " + e.toString() + "\nfrom a msg with " +
                    line).send();
                outMessage = null;
                continue;
            }

            cursor = null;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                // execute query
                try {
                    if (o == null)
                        cursor = coll.find();
                    else
                        cursor = coll.find(o);
                }
                catch (MongoException e) {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) {
                        new Event(Event.ERR, uri + " " + collName +
                            ": failed to execute query: "+ msgStr + ": " +
                            e.getMessage()).send();
                        xq.putback(sid);
                        return;
                    }
                    if (k <= 0)
                        new Event(Event.WARNING, uri + " " + collName +
                            ": failed to execute query: "+ msgStr + ": " +
                            e.getMessage()).send();
                    else if (k + 1 >= maxRetry) { // give up
                        xq.putback(sid);
                        throw(new NonSQLException(e, e.toString()));
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    coll = getCollection(collName);
                    continue;
                }
                catch (Exception e) {
                    k = -1;
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    String line = "";
                    if (displayMask > 0) try {
                        line = MessageUtils.display(outMessage,null,displayMask,
                            propertyName);
                    }
                    catch (Exception ex) {
                        line = ex.toString();
                    }
                    new Event(Event.ERR, uri + ": failed to find data on " +
                        collName + ": " + e.toString() + "\nfrom a msg with " +
                        line).send();
                    outMessage = null;
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    Event.flush(e);
                }
                break;
            }

            if (k >= maxRetry) { // failed to query on all retries
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                outMessage = null;
                continue;
            }
            else if (k < 0) // internal data error handled already
                continue;

            rc = 0;
            if (cursor != null) try {
                rc = cursor.size();
            }
            catch (Exception e) {
                rc = -1;
            }
            currentTime = System.currentTimeMillis();

            try { // for select
                StringBuffer strBuf;
                if (outMessage instanceof TextMessage) {
                    strBuf = getResult(resultType, cursor);
                    try {
                        cursor.close();
                    }
                    catch (Exception ex) {
                    }
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    try {
                        cursor.close();
                    }
                    catch (Exception ex) {
                    }
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (MongoException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                throw(new NonSQLException(e, e.toString()));
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                try {
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                new Event(Event.WARNING, "failed to load msg with docs " +
                    "found from " + collName +": "+Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            try {
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    outMessage);
                if (displayMask > 0) {
                    String line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
                    new Event(Event.INFO, xq.getName() + " found "+ rc +
                        " docs from "+ collName + " and set them into a msg ("+
                        line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " found " + rc +
                    " docs from "+ collName +" and set them into a msg").send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries").send();
    }

    /**
     * It listens to an XQueue for incoming JMS messages and extracts the
     * query JSON statement and data from each message.  After compiling and
     * setting data on the statement, it executes the JSON statement to query
     * the collection. Based on the result type, the query result can be
     * formatted into the customized content.  Or the DBCursor of the query
     * can be put into the message before it is removed from the XQueue.  The
     * requester will be able to get the result content or DBCursor out of
     * the message.
     *<br/><br/>
     * Since the query operation relies on the request, this method will
     * intercept the request and processes it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the query is successful.  Otherwise, the
     * message body will not contain the content out of query operation.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void find(XQueue xq) throws NonSQLException, TimeoutException {
        Message outMessage;
        DBCursor cursor = null;
        DBObject o, p = null, emptyDBObject = new BasicDBObject();
        DBCollection coll = null;
        long currentTime, tm, idleTime;
        long count = 0, stm = 10;
        int retry = 0, rc = 0, heartbeat = 600000, ttl = 7200000;
        int i, k, sid = -1, cid, rid, n, mask, skip = 0, limit = 0;
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean checkIdle = (maxIdleTime > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null, str = null, msgStr = null, collName = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            outMessage = (Message) xq.browse(sid);

            if (outMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            try {
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(outMessage);
                    MessageUtils.setProperty(rcField, uriRC, outMessage);
                }
                catch (Exception ex) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ee) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            collName = null;
            tm = 0L;
            try {
                tm = outMessage.getJMSExpiration();
                collName = MessageUtils.getProperty(collField, outMessage);
                MessageUtils.setProperty(rcField, uriRC, outMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, outMessage);
                }
                catch (Exception e) {
                }
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                outMessage = null;
                continue;
            }
            if (collName == null || collName.length() <= 0) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, uri + ": failed to get " +
                    "collection name from msg").send();
                outMessage = null;
                continue;
            }

            str = null;
            skip = 0;
            try {
                str = MessageUtils.getProperty(skipField, outMessage);
                if (str != null)
                    skip = Integer.parseInt(str);
            }
            catch (Exception e) {
                skip = 0;
            }

            str = null;
            limit = 0;
            try {
                str = MessageUtils.getProperty(limitField, outMessage);
                if (str != null)
                    limit = Integer.parseInt(str);
            }
            catch (Exception e) {
                limit = 0;
            }

            p = null;
            msgStr = null;
            coll = getCollection(collName);

            try {
                msgStr = MessageUtils.getProperty(queryField, outMessage);
                if (msgStr == null || msgStr.length() <= 0) {
                    if (template != null)
                        msgStr = MessageUtils.format(outMessage, buffer,
                            template);
                    else
                        msgStr = MessageUtils.processBody(outMessage, buffer);
                }
                else try { // query is defined so retrieve keys to return
                    str = MessageUtils.processBody(outMessage, buffer);
                    if (str != null && str.length() > 0) // converts keys to obj
                        p = parse(str);
                }
                catch (Exception ex) {
                    new Event(Event.ERR, uri + ": failed to parse projection" +
                        " for " + collName + ": " + ex.toString()).send();
                }
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (JMSException e) {
            }

            if (msgStr != null && msgStr.length() <= 0) {
                msgStr = null;
                o = emptyDBObject;
            }
            else try {
                o = parse(msgStr);
            }
            catch (Exception e) { // bad json data
                if (ack) try {
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                String line = "";
                if (displayMask > 0) try {
                    line = MessageUtils.display(outMessage, null, displayMask,
                        propertyName);
                }
                catch (Exception ex) {
                    line = ex.toString();
                }
                new Event(Event.ERR, uri + ": failed to parse JSON data for " +
                    collName + ": " + e.toString() + "\nfrom a msg with " +
                    line).send();
                outMessage = null;
                continue;
            }

            cursor = null;
            for (k=0; k<maxRetry; k++) { // up to maxRetry
                // execute query
                try {
                    if (p != null) {
                        if (skip > 0 && limit > 0)
                            cursor = coll.find(o, p).skip(skip).limit(limit);
                        else if (limit > 0)
                            cursor = coll.find(o, p).limit(limit);
                        else if (skip > 0)
                            cursor = coll.find(o, p).skip(skip);
                        else
                            cursor = coll.find(o, p);
                    }
                    else {
                        if (skip > 0 && limit > 0)
                            cursor = coll.find(o).skip(skip).limit(limit);
                        else if (limit > 0)
                            cursor = coll.find(o).limit(limit);
                        else if (skip > 0)
                            cursor = coll.find(o).skip(skip);
                        else
                            cursor = coll.find(o);
                    }
                }
                catch (MongoException e) {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) {
                        new Event(Event.ERR, uri + " " + collName +
                            ": failed to execute query: "+ msgStr + ": " +
                            e.getMessage()).send();
                        xq.putback(sid);
                        return;
                    }
                    if (k <= 0)
                        new Event(Event.WARNING, uri + " " + collName +
                            ": failed to execute query: "+ msgStr + ": " +
                            e.getMessage()).send();
                    else if (k + 1 >= maxRetry) { // give up
                        xq.putback(sid);
                        throw(new NonSQLException(e, e.toString()));
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    coll = getCollection(collName);
                    continue;
                }
                catch (Exception e) {
                    k = -1;
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    String line = "";
                    if (displayMask > 0) try {
                        line = MessageUtils.display(outMessage,null,displayMask,
                            propertyName);
                    }
                    catch (Exception ex) {
                        line = ex.toString();
                    }
                    new Event(Event.ERR, uri + ": failed to find data on " +
                        collName + ": " + e.toString() + "\nfrom a msg with " +
                        line).send();
                    outMessage = null;
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    Event.flush(e);
                }
                break;
            }

            if (k >= maxRetry) { // failed to query on all retries
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                outMessage = null;
                continue;
            }
            else if (k < 0) // internal data error handled already
                continue;

            rc = 0;
            if (cursor != null) try {
                rc = cursor.size();
            }
            catch (Exception e) {
                rc = -1;
            }
            currentTime = System.currentTimeMillis();

            try { // for select
                StringBuffer strBuf;
                if (outMessage instanceof TextMessage) {
                    strBuf = getResult(resultType, cursor);
                    try {
                        cursor.close();
                    }
                    catch (Exception ex) {
                    }
                    cursor = null;
                    if ((resultType & Utils.RESULT_XML) > 0) {
                        strBuf.insert(0, "<Result>" + Utils.RS);
                        strBuf.append("</Result>");
                    }
                    else if ((resultType & Utils.RESULT_JSON) > 0) {
                        strBuf.insert(0, "{" + Utils.RS + "\"Record\":");
                        strBuf.append("}");
                    }
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    if (cursor != null) try {
                        cursor.close();
                    }
                    catch (Exception ex) {
                    }
                    cursor = null;
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (MongoException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                throw(new NonSQLException(e, e.toString()));
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                try {
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (cursor != null) try {
                    cursor.close();
                }
                catch (Exception ex) {
                }
                new Event(Event.WARNING, "failed to load msg with docs " +
                    "found from " + collName +": "+Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            try {
                MessageUtils.setProperty(rcField, okRC, outMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    outMessage);
                if (displayMask > 0) {
                    String line = MessageUtils.display(outMessage, msgStr,
                        displayMask, propertyName);
                    new Event(Event.INFO, xq.getName() + " found "+ rc +
                        " docs from "+ collName + " and set them into a msg ("+
                        line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, xq.getName() + " found " + rc +
                    " docs from "+ collName +" and set them into a msg").send();
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "completed " + count + " queries").send();
    }

    /**
     * It listens to the XQueue for incoming JMS messages and extracts the
     * collection name and JSON data from each message.  After the parsing the
     * data, it updates the collection with the query and the data.
     * CollectionField is supposed to contain the name of the collection and
     * the method name separated via the char of '.', similar to the shell.
     * If the collection name contains the char of '.', the method has to be
     * appended to it. If there is no '.' in the name, the default method of
     * update will be assumed.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * It is up to the caller to handle this exception.
     *<br/><br/>
     * The original message will not be modified.  If the XQueue has enabled
     * the EXTERNAL_XA bit, it will also acknowledge the messages.
     * It is NOT MT-Safe.
     */
    public void update(XQueue xq) throws NonSQLException, TimeoutException {
        Message inMessage;
        DBObject o, q;
        DBCollection coll = null;
        WriteConcern w = new WriteConcern(0);
        WriteResult r = null;
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, idleTime, tm, count = 0, stm = 10;
        int retry = 0, mask, rc, cmd;
        int n, k, sid = -1;
        int dmask = MessageUtils.SHOW_DATE;
        String dt = null, msgStr = null, query, collName = null;
        byte[] buffer = new byte[bufferSize];
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
                        currentTime = System.currentTimeMillis();
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        currentTime = System.currentTimeMillis();
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            collName = null;
            tm = 0L;
            try {
                tm = inMessage.getJMSExpiration();
                collName = MessageUtils.getProperty(collField, inMessage);
                MessageUtils.setProperty(rcField, uriRC, inMessage);
            }
            catch (JMSException e) {
            }

            currentTime = System.currentTimeMillis();
            if (tm > 0L && currentTime - tm >= 0L) { // msg expired
                try {
                    MessageUtils.setProperty(rcField, expRC, inMessage);
                }
                catch (Exception e) {
                }
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, uri + ": message expired at " +
                    Event.dateFormat(new Date(tm))).send();
                inMessage = null;
                continue;
            }

            cmd = ACTION_UPDATE;
            if (collName != null && (k = collName.lastIndexOf(".")) >= 0) {
                cmd = getCommandID(collName.substring(k+1));
                collName = collName.substring(0, k);
            }

            if (collName == null || collName.length() <= 0 || cmd < 0) {
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                xq.remove(sid);
                new Event(Event.ERR, uri +
                    ": failed to get collection name from msg").send();
                inMessage = null;
                continue;
            }

            coll = getCollection(collName);
            msgStr = null;
            query = null;
            try {
                if (cmd != ACTION_INSERT)
                    query = MessageUtils.getProperty(queryField, inMessage);
                if (cmd != ACTION_REMOVE)
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            o = null;
            q = null;
            try {
                switch (cmd) {
                  case ACTION_INSERT:
                    if (msgStr == null || msgStr.length() <= 0)
                    throw(new IllegalArgumentException("empty data to insert"));
                    o = parse(msgStr);
                    break;
                  case ACTION_REMOVE:
                    if (query == null || query.length() <= 0)
                  throw(new IllegalArgumentException("empty query for remove"));
                    q = parse(query);
                    break;
                  case ACTION_UPDATE:
                  case ACTION_UPDATEMULTI:
                    if (msgStr == null || msgStr.length() <= 0)
                    throw(new IllegalArgumentException("empty data to update"));
                    if (query == null || query.length() <= 0)
                  throw(new IllegalArgumentException("empty query for remove"));
                    q = parse(query);
                    o = parse(msgStr);
                    break;
                  default:
                    throw(new IllegalArgumentException("bad command: " + cmd));
                }
            }
            catch (Exception e) { // bad json data
                if (ack) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                String line = "";
                if (displayMask > 0) try {
                    line = MessageUtils.display(inMessage, null, displayMask,
                        propertyName);
                }
                catch (Exception ex) {
                    line = ex.toString();
                }
                new Event(Event.ERR, uri + ": failed to parse JSON data for " +
                    collName + ": " + e.toString() + "\nfrom a msg with " +
                    line).send();
                inMessage = null;
                continue;
            }

            for (k=0; k<maxRetry; k++) { // up to maxRetry
                r = null;
                currentTime = System.currentTimeMillis();
                try {
                    switch (cmd) {
                      case ACTION_INSERT:
                        r = coll.insert(o, w);
                        break;
                      case ACTION_REMOVE:
                        r = coll.remove(q);
                        break;
                      case ACTION_UPDATE:
                        r = coll.update(q, o);
                        break;
                      case ACTION_UPDATEMULTI:
                        r = coll.updateMulti(q, o);
                        break;
                      default:
                    }
                }
                catch (MongoException e) {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) {
                        new Event(Event.ERR, uri + ": failed to update data: " +
                            e.getMessage()).send();
                        xq.putback(sid);
                        return;
                    }
                    if (k <= 0)
                        new Event(Event.WARNING, uri +
                            ": failed to update data: "+ e.getMessage()).send();
                    else if (k + 1 >= maxRetry) {
                        xq.putback(sid);
                        throw(new NonSQLException(e, e.toString()));
                    }
                    else try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException ex) {
                    }
                    reconnect();
                    coll = getCollection(collName);
                    continue;
                }
                catch (Exception e) { // internal data error
                    k = -1;
                    if (ack) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    String line = "";
                    if (displayMask > 0) try {
                        line = MessageUtils.display(inMessage, null,displayMask,
                            propertyName);
                    }
                    catch (Exception ex) {
                        line = ex.toString();
                    }
                    new Event(Event.ERR, uri + ": failed to update data on " +
                        collName + ": " + e.toString() + "\nfrom a msg with " +
                        line + ": " + cmd).send();
                    inMessage = null;
                }
                catch (Error e) {
                    xq.putback(sid);
                    Event.flush(e);
                }
                break;
            }
            rc = 0;
            if (r != null) {
                String str;
                try {
                    str = r.getError();
                }
                catch (Exception e) {
                    xq.putback(sid);
                    throw(new NonSQLException(e, e.toString()));
                }
                if (str != null) { // failed
                    if (ack) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception e) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, uri + ": failed to update data on " +
                        collName + " with Mongo error: " + str).send();
                    inMessage = null;
                    continue;
                }
                else if (cmd == ACTION_INSERT)
                    rc = 1;
                else try {
                    rc = r.getN();
                }
                catch (Exception e) {
                    xq.putback(sid);
                    throw(new NonSQLException(e, e.toString()));
                }
            }
            else if (k < 0) // internal data error handled already
                continue;

            dt = null;
            try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                MessageUtils.setProperty(resultField, String.valueOf(rc),
                    inMessage);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    dt =Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }

            if (ack) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after update "+
                    "on "+uri+": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after update "+
                    "on " + uri + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after update " +
                    "on " + uri + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            count ++;

            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "updated " + rc + " docs on " +
                        collName + " with a msg ( Date: " + dt +
                        line + " )").send();
                else // no date
                    new Event(Event.INFO, "updated " + rc + " docs on " +
                        collName + " with a msg (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "updated " + rc + " docs on " + collName+
                    " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long ts = System.currentTimeMillis() + sleepTime;
                do {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    else try {
                        Thread.sleep(stm);
                    }
                    catch (InterruptedException e) {
                    }
                } while (ts > System.currentTimeMillis());
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, "processed " + count + " msgs on " +
               uri).send();
    }

    public static StringBuffer getResult(int type, DBCursor cursor) {
        int i = 0;
        if (cursor == null)
            return null;
        StringBuffer strBuf = new StringBuffer();
        if ((type & Utils.RESULT_JSON) > 0) {
            strBuf.append("[");
            while (cursor.hasNext()) {
                if (i++ > 0)
                    strBuf.append("," + Utils.RS);
                strBuf.append(cursor.next().toString());
            }
            strBuf.append("]" + Utils.RS);
        }
        return strBuf;
    }

    public StringBuffer getResult(int type,String[] list) throws MongoException{
        if (list == null)
            return null;
        int i, k;
        String collName;
        StringBuffer strBuf = new StringBuffer();
        DBCollection coll = null;
        k = list.length;
        if ((type & Utils.RESULT_JSON) > 0) {
            strBuf.append("[");
            for (i=0; i<k; i++) {
                collName = list[i];
                if (collName == null || collName.length() <= 0)
                    continue;
                coll = getCollection(collName);
                if (i > 0)
                    strBuf.append("," + Utils.RS);
                strBuf.append("{\"CollName\": ");
                strBuf.append("\"" + Utils.escapeJSON(collName) + "\"");
                strBuf.append(", \"Count\": " + coll.count());
                strBuf.append("}");
            }
            strBuf.append("]" + Utils.RS);
        }
        else if ((type & Utils.RESULT_XML) > 0) {
            for (i=0; i<k; i++) {
                collName = list[i];
                if (collName == null || collName.length() <= 0)
                    continue;
                coll = getCollection(collName);
                strBuf.append("<Record type=\"ARRAY\">");
                strBuf.append("<CollName>");
                strBuf.append(Utils.escapeXML(collName));
                strBuf.append("</CollName>");
                strBuf.append("<Count>" + coll.count() + "</Count>");
                strBuf.append("</Record>" + Utils.RS);
            }
        }

        return strBuf;
    }

    public String getOperation() {
        return operation;
    }
}
