package org.qbroker.jms;

/* NNTPMessenger.java - an NNTP messenger for JMS messages */

import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.TextMessage;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;
import org.apache.commons.net.nntp.Article;
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.TimeoutException;
import org.qbroker.net.NNTPConnector;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * NNTPMessenger connects to an NNTP server and initializes one of the
 * operations, such as fetch, retrieve and post.  It carries out the
 * operation with JMS Messages.
 *<br/><br/>
 * There are three methods, fetch(), retrieve(), list() and post().  The
 * first method, fetch(), is used to fetch new articles from NNTP servers.
 * The method of retrieve() is used for synchronous requests.  The method of
 * post() is used for asynchronous posting content to the NNTP server.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class NNTPMessenger extends NNTPConnector {
    private String msgID = null;
    private int bufferSize = 4096;
    private Template template = null, subTemplate = null;
    private MessageFilter filter = null;

    private String path;
    private String hostField;
    private String groupField;
    private String subjectField;
    private String articleField;
    private String rcField;
    private int sessionTimeout = 300000;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;
    private int pathType = PATH_NONE;

    private String correlationID = null;
    private String operation = "retrieve";
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int sleepTime = 0;

    private int mode = 0;
    private int textMode = 1;
    private int xaMode = 0;
    private int retry = 3;

    private int offtail = 0;
    private int offhead = 0;
    private StringBuffer textBuffer = null;
    private long maxArticleSize = 4194304;
    private byte[] sotBytes = new byte[0];
    private byte[] eotBytes = new byte[0];
    private int sotPosition = 0;
    private int eotPosition = 0;
    private int boundary = 0;
    private int[] partition;

    private String[] propertyName = null;
    private String[] propertyValue = null;

    private boolean check_body = false;
    private boolean check_jms = false;
    private final static int PATH_NONE = 0;
    private final static int PATH_STATIC = 1;
    private final static int PATH_DYNAMIC = 2;
    private final static int MS_SOT = 1;
    private final static int MS_EOT = 2;

    /** Creates new NNTPMessenger */
    public NNTPMessenger(Map props) throws IOException {
        super(props);

        Object o;
        URI u;

        if (uri == null)
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((path = u.getPath()) == null || path.length() == 0)
            path = null;
        else
            pathType |= PATH_STATIC;

        if ((o = props.get("PropertyMap")) != null && o instanceof Map) {
            hostField = (String) ((Map) o).get("HostName");
            groupField = (String) ((Map) o).get("GroupName");
            subjectField = (String) ((Map) o).get("Subject");
            articleField = (String) ((Map) o).get("ArticleID");
            pathType |= PATH_DYNAMIC;
        }
        if (hostField == null || hostField.length() == 0)
            hostField = "HostName";
        if (groupField == null || groupField.length() == 0)
            groupField = "GroupName";
        if (subjectField == null || subjectField.length() == 0)
            subjectField = "Subject";
        if (articleField == null || articleField.length() == 0)
            articleField = "ArticleID";

        if ((o = props.get("RCField")) != null)
            rcField = (String) o;
        else
            rcField = "ReturnCode";

        if ((o = props.get("SubjectTemplate")) != null) {
            String str = (String) o;
            subTemplate = new Template(str);
        }

        if ((o = props.get("Template")) != null && ((String) o).length() > 0) {
            String str = (String) o;
            template = new Template(str);
        }

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if ((o = props.get("SessionTimeout")) != null)
            sessionTimeout = 1000 * Integer.parseInt((String) o);
        else
            sessionTimeout = 300000;

        if ((o = props.get("Retry")) == null ||
            (retry = Integer.parseInt((String) o)) <= 0)
            retry = 3;

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
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        if ((o = props.get("Offhead")) != null)
            offhead = Integer.parseInt((String) o);
        if (offhead < 0)
            offhead = 0;
        if ((o = props.get("Offtail")) != null)
            offtail = Integer.parseInt((String) o);
        if (offtail < 0)
            offtail = 0;
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

        boundary = 0;
        if ((o = props.get("EOTBytes")) != null) {
            eotBytes = MessageUtils.hexString2Bytes((String) o);
            if (eotBytes != null && eotBytes.length > 0)
                boundary += MS_EOT;
        }
        if ((o = props.get("SOTBytes")) != null) {
            sotBytes = MessageUtils.hexString2Bytes((String) o);
            if (sotBytes != null && sotBytes.length > 0)
                boundary += MS_SOT;
        }
        if ((o = props.get("MaxArticleSize")) != null)
            maxArticleSize =Long.parseLong((String) o);

        if (displayMask > 0 && ((displayMask & MessageUtils.SHOW_BODY) > 0 ||
            (displayMask & MessageUtils.SHOW_SIZE) > 0))
            check_body = true;

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

        if ("fetch".equals(operation)) try {
            Map<String, Object> hmap = new HashMap<String, Object>();

            hmap.put("Name", uri);
            if ((o = props.get("PatternGroup")) != null)
                hmap.put("PatternGroup", o);
            if ((o = props.get("XPatternGroup")) != null)
                hmap.put("XPatternGroup", o);
            filter = new MessageFilter(hmap);
            hmap.clear();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(Event.traceStack(e)));
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * It fetches new articles of a news group from the NNTP server and puts
     * them to the XQ. It is NOT reentry safe.
     */
    public void fetch(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        Date dt = null;
        String reply = null, aid, subject;
        StringBuffer strBuf = new StringBuffer();
        Article[] articles;
        Article article = null;
        long t = 0, tm, count = 0, stm = 10;
        int i, k, n, sid = -1, cid, anumber, mask = 0;
        boolean isText;
        boolean isSleepy = (sleepTime > 0);
        int shift = partition[0];
        int len = partition[1];

        if (path == null)
            throw(new IOException("path is null: " + uri));

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

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
            if (textMode == 0)
                outMessage = new BytesEvent();
            else
                outMessage = new TextEvent();
            outMessage.clearBody();
            isText = (outMessage instanceof TextMessage);
        }
        else if((mask & XQueue.KEEP_RUNNING) > 0 && (mask & XQueue.PAUSE) == 0){
            new Event(Event.ERR, "failed to reserve a cell on " +
                xq.getName() + " for " + uri).send();
            return;
        }
        else {
            if (displayMask != 0)
                new Event(Event.INFO, xq.getName() + ": fetched " + count +
                    " articles from " + uri).send();
            return;
        }

        if (! isConnected()) try {
            connect();
        }
        catch (Exception e) {
        }
        catch (Error e) {
            xq.cancel(sid);
            Event.flush(e);
        }

        articles = null;
        try { // not ready yet, ylu ***
            articles = getArticleInfo(0, 1);
        }
        catch (IOException e) {
            xq.cancel(sid);
            throw(e);
        }

        if (articles == null || articles.length <= 0) {
            xq.cancel(sid);
            if (displayMask != 0)
                new Event(Event.INFO, xq.getName() + ": fetched " + count +
                    " articles from " + uri).send();
            return;
        }

        tm = System.currentTimeMillis();
        n = articles.length;
        for (k=0; k<n; k++) { // loop thru all new articles
            article = articles[k];
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) == 0 || (mask &  XQueue.PAUSE) > 0)
                break;
            if (article == null)
                continue;
            aid = article.getArticleId();
            subject = article.getSubject();
            anumber = article.getArticleNumber();
            dt = getDate(article);
            if (filter.evaluate(outMessage, subject)) {
                strBuf = new StringBuffer();
            }
            else { // no pattern matched
                if (dt != null) // date is parsed OK
                    tm = dt.getTime();
                //saveReference(anumber, tm, 1);
                continue;
            }
            if (dt != null) // date is parsed OK
                tm = dt.getTime();
            else
                new Event(Event.WARNING, "failed to parse timestamp for " +
                    "article " + anumber + " from " + uri + ": " +
                    article.getDate()).send();

            for (i=0; i<retry; i++) {
                try {
                    reply = nntpGet(strBuf, aid, ARTICLE_BODY);
                }
                catch (Exception e) {
                    reply = "" + e.getMessage();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0) {
                        try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException ex) {
                        }
                        reconnect();
                        setNewsgroup(path);
                    }
                }
                catch (Error e) {
                    xq.cancel(sid);
                    new Event(Event.ERR, "failed to fetch " +
                        path + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (reply == null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // temporarily paused
                    xq.cancel(sid);
                    new Event(Event.WARNING, "aborted to fetch from " +
                        uri).send();
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }
            if (reply != null) {
                xq.cancel(sid);
                throw(new IOException("failed to fetch article from " + path +
                    " with " + retry + " retries: " + reply));
            }

            try {
                if (propertyName != null && propertyValue != null) {
                    outMessage.clearProperties();
                    for (i=0; i<propertyName.length; i++)
                        MessageUtils.setProperty(propertyName[i],
                            propertyValue[i], outMessage);
                }
                MessageUtils.setProperty(subjectField, subject, outMessage);
                MessageUtils.setProperty(articleField, aid, outMessage);
                outMessage.setJMSTimestamp(tm);

                if (isText)
                    ((TextMessage) outMessage).setText(strBuf.toString());
                else
                    ((BytesMessage)outMessage).writeBytes(
                        strBuf.toString().getBytes());
            }
            catch (JMSException e) {
                xq.cancel(sid);
                new Event(Event.WARNING, "failed to load msg for " + path +
                    " fetched from " + uri + ": " + Event.traceStack(e)).send();
                return;
            }

            cid = xq.add(outMessage, sid);
            if (cid >= 0) {
                //saveReference(anumber, tm, 1);
                
                if (displayMask > 0) try {
                    String line = MessageUtils.display(outMessage,
                        strBuf.toString(), displayMask, null);
                    new Event(Event.INFO, "fetched an article of " +
                        strBuf.length()+ " bytes from " + uri + " to a msg (" +
                        line + " )").send();
                }
                catch (Exception e) {
                    new Event(Event.INFO, "fetched an article of " +
                        strBuf.length()+ " bytes from " + uri +
                        " to a msg").send();
                }
                count ++;

                if (isSleepy) { // slow down a while
                    long ts = System.currentTimeMillis() + sleepTime;
                    do {
                        mask = xq.getGlobalMask();
                        if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                            (mask & XQueue.PAUSE) > 0) // temporarily disabled
                            break;
                        else try {
                            Thread.sleep(stm);
                        }
                        catch (InterruptedException e) {
                        }
                    } while (ts > System.currentTimeMillis());
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) > 0) // temporarily disabled
                        break;
                }

                sid = -1;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) == 0) switch (len) {
                  case 0:
                    do { // reserve an empty cell
                        sid = xq.reserve(waitTime);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                  case 1:
                    do { // reserve the empty cell
                        sid = xq.reserve(waitTime, shift);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                  default:
                    do { // reserve a partitioned empty cell
                        sid = xq.reserve(waitTime, shift, len);
                        if (sid >= 0)
                            break;
                        mask = xq.getGlobalMask();
                    } while ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0);
                    break;
                }

                if (sid >= 0) {
                    if (isText)
                        outMessage = new TextEvent();
                    else
                        outMessage = new BytesEvent();
                    outMessage.clearBody();
                }
                else {
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.PAUSE) == 0) // no luck
                        new Event(Event.ERR, "failed to reserve a cell on " +
                            xq.getName() + " for " + uri).send();
                    else if ((mask & XQueue.KEEP_RUNNING) > 0) // paused
                        new Event(Event.WARNING, "aborted to fetch from " +
                            uri).send();
                    else if (displayMask != 0)
                        new Event(Event.INFO, xq.getName() + ": fetched " +
                            count+" out of "+n+" articles from " + uri).send();
                    return;
                }
            }
            else {
                xq.cancel(sid);
                new Event(Event.ERR, "failed to add msg for " + uri +
                    " to " + xq.getName()).send();
                return;
            }
        }
        if (sid >= 0 && xq != null)
            xq.cancel(sid);

        if (displayMask != 0)
            new Event(Event.INFO, xq.getName() + ": fetched " + count +
                " articles from " + uri).send();
    }

    /**
     * It gets a JMS message from xq that contains the information about an
     * article to be retrieved and retrieves the article via nntp.  It puts
     * the header info into the message header and puts the content of the
     * article into the message body and sends it back.  The requester at the
     * other end of the XQueue can easily read the content out of the message.
     *<br/><br/>
     * Since the retrieve operation relies on a request, this method will  
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the retrieve is successful.  Otherwise, the
     * message body will not contain the content out of retrieve operation.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void retrieve(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        StringBuffer strBuf = new StringBuffer();
        String aid, groupname, reply=null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, sessionTime, idleTime, size;
        long count = 0, stm = 10;
        int sid = -1;
        int i, m, n, mask;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
                    }
                    m = 0;
                }
                if (checkIdle) {
                    if (n++ == 0) {
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;
            m = 0;
            sessionTime = System.currentTimeMillis();

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is supposed not to be null
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

            aid = null;
            groupname = null;
            try {
                groupname = MessageUtils.getProperty(groupField, outMessage);
                if (groupname == null)
                    groupname = path;
                aid = MessageUtils.getProperty(articleField, outMessage);
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get article ID for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            if (groupname == null || aid == null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "empty article ID or group name for " +
                    uri + groupname).send();
                outMessage = null;
                continue;
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
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

            // set the news group
            for (i=0; i<retry; i++) {
                if ((reply = setNewsgroup(groupname)) == null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) // temporarily disabled
                    break;
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException ex) {
                }
                reconnect();
            }
            if (reply != null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to set " + groupname +
                    " with " + retry + " retries: " + reply));
            }

            // retrieve article
            for (i=0; i<retry; i++) {
                strBuf = new StringBuffer();
                try {
                    reply = nntpGet(strBuf, aid);
                }
                catch (Exception e) {
                    reply = "" + e.getMessage();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) == 0) {
                        try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException ex) {
                        }
                        reconnect();
                        setNewsgroup(groupname);
                    }
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to retrieve article of " +
                        aid + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (reply == null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    xq.putback(sid);
                    new Event(Event.WARNING, "aborted to retrieve article of "+
                        aid + " from " + uri).send();
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }
            if (reply != null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to retrieve " + aid +
                    " with " + retry + " retries: " + reply));
            }

            try {
                size = (long) strBuf.length();
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with " +
                    aid + " retrieved from " + uri +": " +
                    Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
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
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, propertyName);
                new Event(Event.INFO, "retrieved " + aid + " of " +
                    size + " bytes from " + uri + " with a msg ("+ line +
                    " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "retrieved " + aid + " of " +
                    size + " bytes from " + uri + " with a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long tm = System.currentTimeMillis() + sleepTime;
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
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "retrieved " + count + " articles from " +
                uri).send();
    }

    /**
     * It gets a JMS message from xq that contains a newsgroup name for listing
     * articles in it and gets the list via nntp.  It puts the list of articles
     * into the message body one articleID per line and sends it back.  The
     * requester at the other end of the XQueue can easily read the list out
     * of the message.
     *<br/><br/>
     * Since the list operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the list is successful.  Otherwise, the
     * message body will not contain the content out of the list operation.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     */
    public void list(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        String[] list;
        StringBuffer strBuf = new StringBuffer();
        String groupname;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        boolean checkIdle = (maxIdleTime > 0);
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        long currentTime, sessionTime, idleTime, size;
        long count = 0, stm = 10;
        int sid = -1;
        int i, k, m, n, mask;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
                    }
                    m = 0;
                }
                if (checkIdle) {
                    if (n++ == 0) {
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            n = 0;
            m = 0;
            sessionTime = System.currentTimeMillis();

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is supposed not to be null
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

            groupname = null;
            try {
                groupname = MessageUtils.getProperty(groupField, outMessage);
                if (groupname == null) {
                    if (path == null)
                        throw(new JMSException("path is null"));
                    groupname = path;
                }
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get groupname for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
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

            list = null;
            for (i=0; i<retry; i++) {
                try {
                    list = nntpListArticles(groupname, 0);
                }
                catch (Exception e) {
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) == 0) {
                        try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException ex) {
                        }
                        reconnect();
                    }
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to list " + groupname + ": " +
                        e.toString()).send();
                    Event.flush(e);
                }
                if (list != null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    xq.putback(sid);
                    new Event(Event.WARNING, "aborted to list " + groupname +
                        " from " + uri).send();
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }
            if (list == null) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to list " + groupname));
            }

            k = list.length;
            strBuf.delete(0, strBuf.length());
            for (i=0; i<k; i++) {
                if (list[i] != null)
                    strBuf.append(list[i] + "\n");
                list[i] = null;
            }
            list = null;
            try {
                size = (long) strBuf.length();
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    MessageUtils.setProperty(rcField, msgRC, outMessage);
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        uri).send();
                    outMessage = null;
                    continue;
                }
                MessageUtils.setProperty(rcField, okRC, outMessage);
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with list from " +
                    uri + groupname + ": " +Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            if (ack) try { // try to ack the msg
                outMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
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
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, propertyName);
                new Event(Event.INFO, "listed " + k + " articles from " + uri +
                   groupname + " with a msg ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "listed " + k + " articles from " + uri +
                   groupname + " with a msg").send();
            }
            count ++;
            outMessage = null;
            strBuf.delete(0, strBuf.length());

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long tm = System.currentTimeMillis() + sleepTime;
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
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "listed "+count+" groups from "+uri).send();
    }

    /**
     * It gets a JMS Message from the XQueue and posts its content into the
     * NNTP server continuously.
     *<br/><br/>
     * It will always try to retrieve the header info from the message based
     * on HeaderField property.  If the message does not contain the header
     * info, the method will use the default header.
     *<br/><br/>
     * The post operation also supports the requests.  If the XAMode has
     * enabled the XA_CLIENT bit, it will treat the message as a request from
     * the uplink.  In this case, the incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is supposed to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the operation is successful.  Otherwise, the
     * request will be dropped due to failures.  On the other hand, if the
     * XA_CLIENT bit has been disabled by XAMode, the incoming message will
     * not be modified.
     *<br/><br/>
     * If the MaxIdleTime is set to none-zero, it will monitor the idle time
     * and will throw TimeoutException once the idle time exceeds MaxIdleTime.
     * it is up to the caller to handle this exception.
     *<br/><br/>
     * If the XQueue has enabled the EXTERNAL_XA bit, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void post(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage;
        int i, m, n, mask;
        int sid = -1;
        int totalCount = 0;
        int dmask = MessageUtils.SHOW_DATE;
        long currentTime, sessionTime, idleTime, size = 0, count = 0, stm = 10;
        boolean checkIdle = (maxIdleTime > 0);
        boolean isDirectory = false;
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isSleepy = (sleepTime > 0);
        String dt = null, msgStr = null, subject, reply, groupname;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        byte[] buffer = new byte[bufferSize];

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        sessionTime = currentTime;
        idleTime = currentTime;
        n = 0;
        m = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (m++ == 0) {
                    sessionTime = currentTime;
                }
                else if (m > 5) {
                    currentTime = System.currentTimeMillis();
                    if (currentTime - sessionTime >= sessionTimeout) {
                        close();
                        sessionTime = currentTime;
                    }
                    m = 0;
                }
                if (checkIdle) {
                    if (n++ == 0) {
                        idleTime = currentTime;
                    }
                    else if (n > 10) {
                        if (currentTime >= idleTime + maxIdleTime)
                          throw(new TimeoutException("idled for too long"));
                    }
                }
                continue;
            }
            currentTime = System.currentTimeMillis();
            n = 0;
            m = 0;

            inMessage = (Message) xq.browse(sid);

            if (inMessage == null) { // msg is not supposed to be null
                xq.remove(sid);
                new Event(Event.WARNING, "dropped a null msg from " +
                    xq.getName()).send();
                continue;
            }

            // setting default RC
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, uriRC, inMessage);
            }
            catch (MessageNotWriteableException e) {
                try {
                    MessageUtils.resetProperties(inMessage);
                    MessageUtils.setProperty(rcField, uriRC, inMessage);
                }
                catch (Exception ee) {
                    new Event(Event.WARNING,
                       "failed to set RC on msg from "+xq.getName()).send();
                    if (acked) try {
                        inMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    catch (Error ex) {
                        xq.remove(sid);
                        Event.flush(ex);
                    }
                    xq.remove(sid);
                    inMessage = null;
                    continue;
                }
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to set RC on msg from " +
                    xq.getName()).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            groupname = null;
            try {
                groupname = MessageUtils.getProperty(groupField, inMessage);
                if (isWriteable)
                    MessageUtils.setProperty(rcField, jmsRC, inMessage);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to get properties on " +
                    "msg from " + xq.getName() + ": " +e.toString()).send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                catch (Error ex) {
                    xq.remove(sid);
                    Event.flush(ex);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            if (! isConnected()) try {
                connect();
            }
            catch (Exception e) {
            }
            catch (Error e) {
                xq.putback(sid);
                Event.flush(e);
            }

            msgStr = null;
            subject = null;
            try {
                if (subTemplate != null)
                    subject = MessageUtils.format(inMessage,buffer,subTemplate);
                else
                    subject = MessageUtils.getProperty(subjectField, inMessage);

                if (template != null)
                    msgStr = MessageUtils.format(inMessage, buffer, template);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null || groupname == null) {
                new Event(Event.WARNING, uri + "(" + groupname + ")" +
                    ": unknown msg type").send();
                if (acked) try {
                    inMessage.acknowledge();
                }
                catch (Exception e) {
                }
                catch (Error e) {
                    xq.remove(sid);
                    Event.flush(e);
                }
                xq.remove(sid);
                inMessage = null;
                continue;
            }

            reply = null;
            size = msgStr.length();
            for (i=0; i<=retry; i++) {
                try {
                    reply = nntpPost(subject, msgStr, groupname);
                }
                catch (Exception e) {
                    reply = "" + e.getMessage();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) == 0) {
                        try {
                            Thread.sleep(receiveTime);
                        }
                        catch (InterruptedException ex) {
                        }
                        reconnect();
                    }
                }
                catch (Error e) {
                    xq.putback(sid);
                    new Event(Event.ERR, "failed to post " + groupname +
                        ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (reply == null)
                    break;
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    xq.putback(sid);
                    new Event(Event.WARNING, "aborted to post " + groupname +
                        " to " + uri).send();
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
            }
            if (reply != null) {
                xq.putback(sid);
                throw(new IOException("failed to post to " + uri +
                    groupname + " with " + retry + " retries: " + reply));
            }

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR,"failed to ack msg after post on "+
                    uri + groupname + ": " + str +
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after post on "+
                    uri + groupname + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after post on "+
                    uri + groupname + ": " + e.toString()).send();
                Event.flush(e);
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt= Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "post on " + uri + groupname).send();
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0) {
                    new Event(Event.INFO, "posted " + size +" bytes to " + uri +
                        groupname + " with a msg ( Date: " + dt +
                        line + " )").send();
                }
                else { // no date
                    new Event(Event.INFO, "posted " + size + " bytes to " +
                        uri + groupname + " with a msg (" + line + " )").send();
                }
            }
            catch (Exception e) {
                new Event(Event.INFO, "posted " + size + " bytes to " +
                    uri + groupname + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down a while
                long tm = System.currentTimeMillis() + sleepTime;
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
                } while (tm > System.currentTimeMillis());
            }
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "posted " + count + " msgs to " + uri).send();
    }

    public String getOperation() {
        return operation;
    }
}
