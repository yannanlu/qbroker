package org.qbroker.jms;

/* HTTPMessenger.java - an HTTP messenger for JMS messages */

import java.net.HttpURLConnection;
import java.io.File;
import java.io.StringReader;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Date;
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
import org.qbroker.common.XQueue;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.QuickCache;
import org.qbroker.common.TextSubstitution;
import org.qbroker.common.Utils;
import org.qbroker.common.BytesBuffer;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.HTTPConnector;
import org.qbroker.net.HTTPClient;
import org.qbroker.jms.MessageOutputStream;
import org.qbroker.jms.MessageUtils;
import org.qbroker.jms.Msg2Text;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.event.Event;

/**
 * HTTPMessenger connects to a web server and initializes one of the operations,
 * such as retrieve, download and store, or other commands.
 *<br/><br/>
 * There are four methods, retrieve(), download(), fulfill() and store().  The
 * first method, retrieve(), is used to retrieve content from a web server.
 * The method of download() is used for synchronous requests.  The method of
 * fulfill() is for asynchronous requests with the initial request and multiple
 * subsequent checks on the status.  The method of store() is used for putting
 * or posting content to a web server. Except for retrieve(), the rest of
 * methods treat the return code of 2xx as a success. If IgnoreHTTP412 is set
 * to be true, the methods of download(), fulfill() and store() will not treat
 * the return code of 412 as a failure.
 *<br/><br/>
 * ResponseProperty is a map for retrieving the header properties from an HTTP
 * response. It contains multiple key-value pairs with key for an HTTP header
 * and value for a property name of messages. Once the response is back,
 * the values of the selected headers will be copied into the message. On the
 * other hand, RequestProperty is a map for setting header properties on an
 * HTTP request. Before sending the request to an HTTP service, those selected
 * headers will be set via copying the properties from the message.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class HTTPMessenger {
    private String uri = null;
    private String msgID = null;
    private int bufferSize = 4096;
    private int port = 80;
    private Msg2Text msg2Text = null;
    private QuickCache cache = null;
    private HTTPConnector conn = null;

    private String fieldName;
    private String path;
    private String rcField;
    private String subField = null;
    private String sizeField = null;
    private String storeField = null;

    private int displayMask = 0;
    private SimpleDateFormat dateFormat = null;

    private int mode = 0;
    private int pathType = PATH_NONE;

    private String correlationID = null;
    private String operation = "store";
    private int maxNumberMsg = 0;
    private int maxIdleTime = 0;
    private long waitTime = 500L;
    private int receiveTime = 1000;
    private int sleepTime = 0;

    private int textMode = 1;
    private int xaMode = 0;
    private int retry = 3;
    private int resultType = 8;

    private int offtail = 0;
    private int offhead = 0;
    private StringBuffer textBuffer = null;
    private long maxFileSize = 4194304 * 1024;
    private int[] partition;

    private String[] propertyName = null;
    private String[] propertyValue = null;
    private String[] reqPropertyName = null;
    private String[] reqPropertyValue = null;
    private String[] respPropertyName = null;
    private String[] respPropertyValue = null;

    private boolean check_body = false;
    private boolean check_jms = false;
    private boolean ignoreTimeout = false;
    private boolean ignoreHTTP_412 = false;
    private boolean verifyDirectory = false;
    private boolean usePut = false;
    private boolean copyHeader = false;
    private final static int PATH_NONE = 0;
    private final static int PATH_STATIC = 1;
    private final static int PATH_DYNAMIC = 2;
    private final static int MS_SOT = 1;
    private final static int MS_EOT = 2;

    /** Creates new HTTPMessenger */
    public HTTPMessenger(Map props) {
        Object o;
        Map<String, Object> ph;
        URI u;

        if ((o = props.get("URI")) != null)
            uri = (String) o;
        else
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((!"http".equals(u.getScheme())) && (!"https".equals(u.getScheme())))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((path = u.getPath()) == null || path.length() == 0)
            path = null;
        else
            pathType |= PATH_STATIC;

        if ((fieldName = (String) props.get("FieldName")) == null ||
            fieldName.length() == 0) {
            fieldName = null;
        }
        else {
            pathType |= PATH_DYNAMIC;
            if ("JMSType".equals(fieldName))
                fieldName = MessageUtils.getPropertyID(fieldName);
        }

        if (pathType == PATH_NONE)
          throw(new IllegalArgumentException("Path and FieldName not defined"));

        if ((o = props.get("ProxyHost")) != null)
            conn = new HTTPClient(props);
        else
            conn = new org.qbroker.net.HTTPConnector(props);

        if ((o = props.get("RCField")) != null && o instanceof String)
            rcField = (String) o; 
        else
            rcField = "ReturnCode";

        ph = new HashMap<String, Object>();
        if ((o = props.get("TemplateFile")) != null)
            ph.put("TemplateFile", o);
        else if ((o = props.get("Template")) != null)
            ph.put("Template", o);
        if ((o = props.get("RepeatedTemplate")) != null)
            ph.put("RepeatedTemplate", o);
        if ((o = props.get("ExcludedProperty")) != null)
            ph.put("ExcludedProperty", o);
        if (ph.size() > 0) {
            if ((o = props.get("ResultType")) != null)
                ph.put("ResultType", o);
            msg2Text = new Msg2Text(ph);
            ph.clear();
        }
 
        if ((o = props.get("Retry")) == null ||
            (retry = Integer.parseInt((String) o)) <= 0)
            retry = 3;
        if ((o = props.get("IgnoreTimeout")) != null &&
            "true".equalsIgnoreCase((String) o))
            ignoreTimeout = true;
        if ((o = props.get("Mode")) != null && "daemon".equals((String) o))
            mode = 1;
        if ((o = props.get("Operation")) != null)
            operation = (String) o;

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
        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        if ((o = props.get("XAMode")) != null)
            xaMode = Integer.parseInt((String) o);
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
        if ((o = props.get("MaxFileSize")) != null)
            maxFileSize =Long.parseLong((String) o);

        if ((o = props.get("IgnoreHTTP412")) != null) //ignore 412 error on post
            ignoreHTTP_412 = "true".equalsIgnoreCase((String) o);

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

        if ((o = props.get("RequestProperty")) != null && o instanceof Map) {
            String key;
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            reqPropertyName = new String[n];
            reqPropertyValue = new String[n];
            for (int i=0; i<n; i++)
                reqPropertyName[i] = null;
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                reqPropertyName[n] = key;
                key = (String) ((Map) o).get(key);
                if (key == null || key.length() <= 0)
                    continue;
                reqPropertyValue[n] = MessageUtils.getPropertyID(key);
                if (reqPropertyValue[n] == null)
                    reqPropertyValue[n] = key;
                n ++;
            }
            if (n > 0)
                copyHeader = true;
        }

        if ((o = props.get("ResponseProperty")) != null && o instanceof Map) {
            String key;
            Iterator iter = ((Map) o).keySet().iterator();
            int n = ((Map) o).size();
            respPropertyName = new String[n];
            respPropertyValue = new String[n];
            for (int i=0; i<n; i++)
                respPropertyName[i] = null;
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                if (key == null || key.length() <= 0)
                    continue;
                respPropertyName[n] = key;
                key = (String) ((Map) o).get(key);
                if (key == null || key.length() <= 0)
                    continue;
                respPropertyValue[n] = MessageUtils.getPropertyID(key);
                if (respPropertyValue[n] == null)
                    respPropertyValue[n] = key;
                n ++;
            }
            if (n > 0)
                copyHeader = true;
        }

        if ("query".equals(operation)) {
            if ((o = props.get("SubField")) != null && o instanceof String)
                subField = (String) o;
            else
                subField = "SubField";
            cache = new QuickCache(uri, QuickCache.META_DEFAULT, 0, 0);
        }
        else { // for download or store
            if ((o = props.get("StoreField")) != null)
                storeField = (String) o;
            if ((o = props.get("FileSize")) != null)
                sizeField = (String) o;
            if ((o = props.get("VerifyDirectory")) != null &&
                "true".equals(((String) o).toLowerCase()))
                verifyDirectory = true;
            if ((o = props.get("UsePut")) != null &&
                "true".equals(((String) o).toLowerCase()))
                usePut = true;
        }

        dateFormat = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss.SSS");
    }

    /**
     * retrieve a page from web server and put it to an XQ
     */
    public void retrieve(XQueue xq) throws IOException, JMSException {
        Message outMessage;
        Map<String, String> extraHeader = null;
        BytesBuffer msgBuf = null;
        OutputStream out = null;
        StringBuffer strBuf = new StringBuffer();
        int i, sid = -1, cid, mask = 0;
        int retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        long count = 0;
        boolean isText;
        int shift = partition[0];
        int len = partition[1];

        if (path == null)
            throw(new IOException("path is null: " + uri));

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
            return;
        }

        if (isText)
            msgBuf = new BytesBuffer();
        else
            out = new MessageOutputStream((BytesMessage) outMessage);

        if (copyHeader && reqPropertyName != null) { // extra headers
            String str = null;
            extraHeader = new HashMap<String, String>();
            for (i=0; i<reqPropertyName.length; i++) {
                if (reqPropertyName[i] == null)
                    continue;
                str = reqPropertyValue[i];
                if (str == null || str.length() <= 0)
                    continue;
                extraHeader.put(reqPropertyName[i], str);
            }
        }

        for (i=0; i<=retry; i++) {
            strBuf = new StringBuffer();
            try {
                if (isText)
                    retCode = conn.doGet(uri, extraHeader, strBuf, msgBuf);
                else
                    retCode = conn.doGet(uri, extraHeader, strBuf, out);
            }
            catch (Exception e) {
                retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                new Event(Event.WARNING, "failed to retrieve from " + uri +
                    ": " + e.toString()).send();
            }
            catch (Error e) {
                xq.cancel(sid);
                new Event(Event.ERR, "failed to retrieve from " + uri +
                   ": " + e.toString()).send();
                if (isText)
                    msgBuf.close();
                else if (out != null)
                    out.close();
                Event.flush(e);
            }
            if (retCode == HttpURLConnection.HTTP_OK)
                break;
            mask = xq.getGlobalMask();
            if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                (mask & XQueue.PAUSE) > 0) { // temporarily paused
                xq.cancel(sid);
                if (isText)
                    msgBuf.close();
                else if (out != null)
                    out.close();
                return;
            }
            try {
                Thread.sleep(receiveTime);
            }
            catch (InterruptedException e) {
            }
            if (i < retry && conn.reconnect() != null)
                break;
            if (isText)
                msgBuf.reset();
            else try {
                outMessage.clearBody();
            }
            catch (Exception e) {
            }
        }
        if (retCode != HttpURLConnection.HTTP_OK) {
            xq.cancel(sid);
            if (isText)
                msgBuf.close();
            else if (out != null)
                out.close();
            throw(new IOException("failed to retrieve data from " + uri +
                " after " + i + " retries with error: " + retCode +
                " " + strBuf.toString()));
        }

        try {
            if (isText) {
                ((TextMessage) outMessage).setText(msgBuf.toString());
                msgBuf.close();
            }
            else if (out != null)
                out.close();

            outMessage.setJMSTimestamp(System.currentTimeMillis());

            if (copyHeader && respPropertyName != null) {
                String value, str = strBuf.toString();
                for (i=0; i<respPropertyName.length; i++) {
                     if (respPropertyName[i] == null)
                         continue;
                     value = Utils.getHttpHeader(respPropertyName[i], str);
                     if (value == null || value.length() <= 0)
                         continue;
                     MessageUtils.setProperty(respPropertyValue[i],
                         value, outMessage);
                }
            }

            if (propertyName != null && propertyValue != null) {
                for (i=0; i<propertyName.length; i++)
                    MessageUtils.setProperty(propertyName[i],
                        propertyValue[i], outMessage);
            }
        }
        catch (JMSException e) {
            xq.cancel(sid);
            new Event(Event.WARNING, "failed to load msg with result" +
                " retrieved from " + uri + ": " + Event.traceStack(e)).send();
            return;
        }
        catch (Exception e) {
            xq.cancel(sid);
            new Event(Event.WARNING, "failed to load msg with result" +
                " retrieved from " + uri + ": " + Event.traceStack(e)).send();
            return;
        }

        cid = xq.add(outMessage, sid);
        if (cid >= 0) {
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, null);
                new Event(Event.INFO, "retrieved " + strBuf.length()+
                    " bytes from " + uri + " to a msg (" + line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "retrieved " + strBuf.length()+
                    " bytes from " + uri + " to a msg").send();
            }
            count ++;
        }
        else {
            xq.cancel(sid);
            new Event(Event.ERR, xq.getName() +
                ": failed to add a msg from " + uri).send();
        }
    }

    /**
     * It gets a JMS message from xq that contains a URL to be downloaded and
     * downloads the web page via http.  It puts the content of the page into
     * the message body and sends it back.  The requester at the other end
     * of the XQueue can easily read the content out of the message.
     *<br/><br/>
     * Since the download operation relies on a request, this method will
     * intercept the request and process it.  The incoming message is required
     * to be writeable.  The method will set a String property of the message
     * specified by the RCField with the return code, indicating the status
     * of the process to fulfill the request.  The requester is required to
     * check the value of the property once it gets the message back.  If
     * the return code is 0, the download is successful.  Otherwise, the
     * message body will not contain the content out of download operation.
     *<br/><br/>
     * It supports GET, POST and PUT.  For POST, IsPost must be set to true.
     * For PUT, UsePut has to be set to true. The Template or message body
     * must contain the query content for POST or PUT. If it is GET, Template
     * will be used to set the dynamic path for REST.
     */
    public void download(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        Map<String, String> extraHeader = null;
        OutputStream out = null;
        BytesBuffer buf = null, msgBuf = null;
        StringBuffer strBuf = new StringBuffer();
        byte[] buffer = new byte[bufferSize];
        String urlName = null, msgStr = null, localFilename = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        int retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        boolean checkIdle = (maxIdleTime > 0);
        boolean hasTemplate = (msg2Text != null);
        boolean isPost = conn.isPost();
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean isLocal = false;
        boolean isBytes = false;
        boolean isSleepy = (sleepTime > 0);
        long count = 0, stm = 10, idleTime, currentTime;
        int sid = -1;
        int i, mask, n = 0;

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
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
            currentTime = System.currentTimeMillis();

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

            urlName = null;
            localFilename = null;
            if (fieldName != null) try {
                urlName = MessageUtils.getProperty(fieldName, outMessage);
                if (storeField != null) // for local store
                    localFilename = MessageUtils.getProperty(storeField,
                        outMessage);
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get urlname for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }
            if (urlName == null)
                urlName = uri;

            msgStr = null;
            try {
                if (hasTemplate) // for JMSEvent only
                    msgStr = msg2Text.format(0, outMessage);
                else if (isPost || usePut)
                    msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to get content for " +
                    urlName + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            isBytes = (outMessage instanceof BytesMessage);
            if (isPost || usePut) {
                buf = new BytesBuffer();
                if (isBytes) {
                    ((BytesMessage) outMessage).reset();
                    MessageUtils.copyBytes((BytesMessage) outMessage,
                        buffer, buf);
                }
                else {
                    buf.write(msgStr.getBytes(), 0, msgStr.length());
                }
            }
            if (localFilename != null && localFilename.length() > 0) {
                String folder = null;
                isLocal = true;
                // make sure local dir exists
                if (verifyDirectory &&
                    (folder = Utils.getParent(localFilename)) != null) {
                    File dir = new File(folder);
                    if (!dir.exists()) {
                        for (i=0; i<=retry; i++) {
                            if (dir.exists())
                                break;
                            dir.mkdirs();
                        }
                    }
                    if (!dir.exists())
                        new Event(Event.WARNING, "failed to create dir " +
                            folder + " for " + localFilename).send();
                }
                if (copyHeader && reqPropertyName != null) { // extra headers
                    String str = null;
                    extraHeader = new HashMap<String, String>();
                    for (i=0; i<reqPropertyName.length; i++) {
                        if (reqPropertyName[i] == null)
                            continue;
                        str = outMessage.getStringProperty(reqPropertyValue[i]);
                        if (str == null || str.length() <= 0)
                            continue;
                        extraHeader.put(reqPropertyName[i], str);
                    }
                }
                else if (extraHeader != null)
                    extraHeader = null;
                for (i=0; i<=retry; i++) {
                    strBuf = new StringBuffer();
                    try {
                        out = new FileOutputStream(new File(localFilename));
                        if (isPost)
                            retCode = conn.doPost(urlName, extraHeader, buf,
                                strBuf, out);
                        else if (usePut)
                            retCode = conn.doPut(urlName, extraHeader, buf,
                                strBuf, out);
                        else if (msgStr != null && msgStr.length() > 0)
                            retCode = conn.doGet(urlName + msgStr, extraHeader,
                                strBuf, out);
                        else {
                            msgStr = "";
                            retCode =conn.doGet(urlName,extraHeader,strBuf,out);
                        }
                    }
                    catch (Exception e) {
                        retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                        strBuf.insert(0, e.toString() + " ");
                    }
                    catch (Error e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "failed to save content from "+
                            urlName + ((isPost || usePut) ? "" : msgStr)+" to "+
                            localFilename + ": " + e.toString()).send();
                        if (out != null) try {
                            out.close();
                        }
                        catch (Exception ex) {
                        }
                        if (isPost || usePut) {
                            buf.close();
                            buf = null;
                        }
                        Event.flush(e);
                    }
                    if (out != null) try {
                        out.close();
                    }
                    catch (Exception e) {
                    }
                    if (retCode >= HttpURLConnection.HTTP_OK &&
                        retCode < HttpURLConnection.HTTP_MULT_CHOICE)
                        break;
                    else if (retCode == HttpURLConnection.HTTP_PRECON_FAILED &&
                        ignoreHTTP_412)
                        break;
                    if (i == 0)
                        new Event(Event.WARNING, "failed to save content from "+
                            urlName + ((isPost || usePut) ? "" : msgStr)+" to "+
                            localFilename + ": " + strBuf.toString()).send();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                        xq.putback(sid);
                        new Event(Event.WARNING, "aborted to download from " +
                            urlName + ((isPost || usePut) ? "" : msgStr) +
                            " due to being disabled").send();
                        if (isPost || usePut) {
                            buf.close();
                            buf = null;
                        }
                        return;
                    }
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                    if (i < retry && conn.reconnect() != null)
                        break;
                }
            }
            else { // store content into message or msgBuf
                isLocal = false;
                if (isBytes)
                    out = new MessageOutputStream((BytesMessage) outMessage);
                else
                    msgBuf = new BytesBuffer();
                if (copyHeader && reqPropertyName != null) { // extra headers
                    String str = null;
                    extraHeader = new HashMap<String, String>();
                    for (i=0; i<reqPropertyName.length; i++) {
                        if (reqPropertyName[i] == null)
                            continue;
                        str = outMessage.getStringProperty(reqPropertyValue[i]);
                        if (str == null || str.length() <= 0)
                            continue;
                        extraHeader.put(reqPropertyName[i], str);
                    }
                }
                else if (extraHeader != null)
                    extraHeader = null;
                for (i=0; i<=retry; i++) {
                    strBuf = new StringBuffer();
                    try {
                        if (isBytes) { // bytes
                            if (isPost)
                                retCode = conn.doPost(urlName, extraHeader, buf,
                                    strBuf, out);
                            else if (usePut)
                                retCode = conn.doPut(urlName, extraHeader, buf,
                                    strBuf, out);
                            else if (msgStr != null && msgStr.length() > 0)
                                retCode = conn.doGet(urlName+msgStr,extraHeader,
                                    strBuf, out);
                            else {
                                msgStr = "";
                                retCode = conn.doGet(urlName, extraHeader,
                                    strBuf, out);
                            }
                        }
                        else if (isPost)
                            retCode = conn.doPost(urlName, extraHeader, buf,
                                strBuf, msgBuf);
                        else if (usePut)
                            retCode = conn.doPut(urlName, extraHeader, buf,
                                strBuf, msgBuf);
                        else if (msgStr != null && msgStr.length() > 0)
                            retCode = conn.doGet(urlName + msgStr, extraHeader,
                                strBuf, msgBuf);
                        else {
                            msgStr = "";
                            retCode = conn.doGet(urlName, extraHeader, strBuf,
                                msgBuf);
                        }
                    }
                    catch (Exception e) {
                        retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                        strBuf.insert(0, e.toString() + " ");
                    }
                    catch (Error e) {
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.ERR, "failed to download from " +
                            urlName + ((isPost || usePut) ? "" : msgStr) +
                            ": "+ e.toString()).send();
                        if (isBytes) {
                            out.close();
                            out = null;
                        }
                        else {
                            msgBuf.close();
                            msgBuf = null;
                        }
                        if (isPost || usePut) {
                            buf.close();
                            buf = null;
                        }
                        Event.flush(e);
                    }
                    if (retCode >= HttpURLConnection.HTTP_OK &&
                        retCode < HttpURLConnection.HTTP_MULT_CHOICE)
                        break;
                    else if (retCode == HttpURLConnection.HTTP_PRECON_FAILED &&
                        ignoreHTTP_412)
                        break;
                    if (i == 0)
                        new Event(Event.WARNING, "failed to download from " +
                            urlName+((isPost || usePut) ? "" : msgStr)+" with "+
                            retCode + ": " + strBuf.toString()).send();
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) // temporarily disabled
                        break;
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                    if (i < retry && conn.reconnect() != null)
                        break;
                    if (!isBytes)
                        msgBuf.reset();
                    else try {
                        outMessage.clearBody();
                    }
                    catch (JMSException e) {
                        new Event(Event.ERR, "failed to clear body for " +
                            urlName + ((isPost || usePut) ? "" : msgStr) +
                            ": "+ e.toString()).send();
                        break;
                    }
                }
            }
            if (isPost || usePut) {
                buf.close();
                buf = null;
            }
            if (retCode >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                if (!isLocal) {
                    if (isBytes) {
                        out.close();
                        out = null;
                    }
                    else {
                        msgBuf.close();
                        msgBuf = null;
                    }
                }
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    new Event(Event.WARNING, "aborted to download " +
                        urlName + ((isPost || usePut) ? "" : msgStr) +
                        " due to being disabled").send();
                    break;
                }
                else
                    throw(new IOException("failed to download from " + urlName+
                        ((isPost || usePut) ? "" : msgStr) + " after " + i +
                        " retries with error: " + retCode + " " +
                        strBuf.toString()));
            }
            else if (retCode >= HttpURLConnection.HTTP_MULT_CHOICE &&
                (retCode != HttpURLConnection.HTTP_PRECON_FAILED ||
                !ignoreHTTP_412)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to download from " + urlName +
                    ((isPost || usePut) ? "" : msgStr) + " after " + i +
                    " retries with error: " + retCode + " " +
                    strBuf.toString()).send();
                outMessage = null;
                if (!isLocal) {
                    if (isBytes) {
                        out.close();
                        out = null;
                    }
                    else {
                        msgBuf.close();
                        msgBuf = null;
                    }
                }
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    new Event(Event.WARNING, "aborted to download " +
                        urlName + ((isPost || usePut) ? "" : msgStr) +
                        " due to being disabled").send();
                    break;
                }
                else
                    continue;
            }

            try { // store content into message
                if (isLocal)
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                else if (isBytes) {
                    out.close();
                    out = null;
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                    if (copyHeader && respPropertyName != null) {
                        String value, str = strBuf.toString();
                        for (i=0; i<respPropertyName.length; i++) {
                             if (respPropertyName[i] == null)
                                 continue;
                             value=Utils.getHttpHeader(respPropertyName[i],str);
                             if (value == null || value.length() <= 0)
                                 continue;
                             MessageUtils.setProperty(respPropertyValue[i],
                                 value, outMessage);
                        }
                    }
                }
                else if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(msgBuf.toString());
                    msgBuf.close();
                    msgBuf = null;
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                    if (copyHeader && respPropertyName != null) {
                        String value, str = strBuf.toString();
                        for (i=0; i<respPropertyName.length; i++) {
                             if (respPropertyName[i] == null)
                                 continue;
                             value=Utils.getHttpHeader(respPropertyName[i],str);
                             if (value == null || value.length() <= 0)
                                 continue;
                             MessageUtils.setProperty(respPropertyValue[i],
                                 value, outMessage);
                        }
                    }
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
                        urlName).send();
                    outMessage = null;
                    msgBuf.close();
                    msgBuf = null;
                    continue;
                }
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with result" +
                    " downloaded from " + urlName +
                    ((isPost || usePut) ? "" : msgStr) +
                    ": " + e.getMessage()).send();
                outMessage = null;
                if (isBytes) {
                    if (out != null) {
                        out.close();
                        out = null;
                    }
                }
                else {
                    if (msgBuf != null) {
                        msgBuf.close();
                        msgBuf = null;
                    }
                }
                continue;
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with result" +
                    " downloaded from " + urlName +
                    ((isPost || usePut) ? "" : msgStr) +
                    ": " + e.getMessage()).send();
                outMessage = null;
                if (isBytes) {
                    if (out != null) {
                        out.close();
                        out = null;
                    }
                }
                else {
                    if (msgBuf != null) {
                        msgBuf.close();
                        msgBuf = null;
                    }
                }
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
                    urlName + ((isPost || usePut) ? "" : msgStr) + ": " + str +
                    Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after query on " +
                    urlName + ((isPost || usePut) ? "" : msgStr) + ": " +
                    Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after query on " +
                    urlName + ((isPost || usePut) ? "" : msgStr) + ": " +
                    e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, propertyName);
                new Event(Event.INFO, "downloaded " + strBuf.length() +
                    " bytes from " + urlName+((isPost || usePut) ? "" : msgStr)+
                    " with a msg ("+ line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "downloaded " + strBuf.length() +
                    " bytes from " + urlName+((isPost || usePut) ? "" : msgStr)+
                    " with a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
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
            new Event(Event.INFO, "downloaded " + count + " pages").send();
    }

    /**
     * It gets a JMS message from xq that contains a static URL for the initial
     * asynchronous request and a dynamic URL template for a subsequent checking
     * on the status of the fulfillment via http. After the initial request, it
     * waits and keeps checking on the status of the fulfillment. Once the
     * request is fulfilled, it puts the fulfilled response into the message
     * body and sends it back. The requester at the other end of the XQueue can
     * easily read the content out of the message.
     *<br/><br/>
     * Since the fulfill operation relies on an initial request and multiple
     * subsequent checks, this method will intercept the JMS request and
     * process it.  The incoming message is required to be writeable.  The
     * method will set a String property of the message specified by the
     * RCField with the return code, indicating the status of the fulfillment
     * process. The requester is required to check the value of the property
     * once it gets the message back.  If the return code is 0, the fulfillment
     * is successful.  Otherwise, the message body will not contain the result.
     *<br/><br/>
     * For the initial request, it supports both GET and POST.  If it is POST,
     * IsPost must be set to true.  The Template or message body must contains
     * the fulfillment content for POST.  If it is GET, Template will be used
     * to set the dynamic path for the REST calls.
     *<br/><br/>
     * The incoming message is also supposed to have a substitution statement
     * to rewrite the URL for the subsequent rest calls to check the response.
     * It should be stored in SubField if it is defined.  For the subsequent
     * rest calls, it only supports GET.  The URL substitution only formats the
     * content from the initial response.  For each subsequent rest call,
     * the method evaluates the response status.  It it is OK, the content of
     * the response will be set to the message body.  Otherwise, it will
     * sleep for a while defined by SleepTime and retries up to Retry times.
     */
    public void fulfill(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message outMessage;
        BytesBuffer msgBuf = null;
        StringBuffer strBuf = new StringBuffer();
        TextSubstitution sub = null;
        byte[] buffer = new byte[bufferSize];
        String urlName = null, msgStr = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String reqRC = String.valueOf(MessageUtils.RC_REQERROR);
        String expRC = String.valueOf(MessageUtils.RC_EXPIRED);
        String msgRC = String.valueOf(MessageUtils.RC_MSGERROR);
        long currentTime, idleTime, tm;
        int retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        boolean checkIdle = (maxIdleTime > 0);
        boolean hasTemplate = (msg2Text != null);
        boolean isPost = conn.isPost();
        boolean ack = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        int ttl = 3600000;
        long count = 0;
        int sid = -1;
        int i, mask, n = 0;

        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
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
            currentTime = System.currentTimeMillis();

            outMessage = (Message) xq.browse(sid);
            if (outMessage == null) { // msg is supposed not to be null
                xq.remove(sid);
                new Event(Event.ERR, "dropped a null msg from " +
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
                    new Event(Event.ERR,
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
                new Event(Event.ERR, "failed to set RC on msg from " +
                    xq.getName()).send();
                outMessage = null;
                continue;
            }

            urlName = null;
            if (fieldName != null) try {
                urlName = MessageUtils.getProperty(fieldName, outMessage);
                MessageUtils.setProperty(rcField, jmsRC, outMessage);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to get urlname for " +
                    uri + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }
            if (urlName == null)
                urlName = uri;

            // get dynamic content formatter
            msgStr = null;
            try { // retrieve dynamic content formatter from msg
                msgStr = MessageUtils.getProperty(subField, outMessage);
                if (msgStr == null || msgStr.length() <= 0)
                    sub = null;
                else if (msgStr.indexOf("s/") < 0)
                    sub = null;
                else if (cache.containsKey(msgStr) &&
                    !cache.isExpired(msgStr, currentTime))
                    sub = (TextSubstitution) cache.get(msgStr, currentTime);
                else { // new or expired
                    if (currentTime - cache.getMTime() >= ttl) {
                        cache.disfragment(currentTime);
                        cache.setStatus(cache.getStatus(), currentTime);
                    }
                    sub = new TextSubstitution(msgStr);
                    cache.insert(msgStr, currentTime, ttl, null, sub);
                }
            }
            catch (Exception e) {
                sub = null;
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, xq.getName() +
                    ": failed to retrieve content formatter for " + urlName +
                    ": " + msgStr + ": " + Event.traceStack(e)).send();
                outMessage = null;
                continue;
            }

            msgStr = null;
            try {
                if (hasTemplate) // for JMSEvent only
                    msgStr = msg2Text.format(0, outMessage);
                else if (isPost)
                    msgStr = MessageUtils.processBody(outMessage, buffer);
            }
            catch (Exception e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, xq.getName() +"failed to get content for "+
                    urlName + ": " + e.toString()).send();
                outMessage = null;
                continue;
            }

            // reset rc
            try {
                MessageUtils.setProperty(rcField, reqRC, outMessage);
            }
            catch (Exception e) {
            }

            // initial request
            if (isPost) {
                msgBuf = new BytesBuffer();
                msgBuf.write(msgStr.getBytes());
            }

            strBuf = new StringBuffer();
            try {
                if (isPost)
                    retCode = conn.doPost(urlName, msgBuf, strBuf);
                else if (msgStr != null && msgStr.length() > 0)
                    retCode = conn.doGet(urlName + msgStr, strBuf);
                else
                    retCode = conn.doGet(urlName, strBuf);
            }
            catch (Exception e) {
                retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                new Event(Event.WARNING, "failed to fulfill a request to " +
                    urlName + ": " + e.toString()).send();
            }
            catch (Error e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to fulfill a request to " +
                    urlName + ": " + e.toString()).send();
                if (isPost) {
                    msgBuf.close();
                    msgBuf = null;
                }
                Event.flush(e);
            }
            if (isPost) {
                msgBuf.close();
                msgBuf = null;
            }

            if (retCode >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to fulfill a request to " +
                    urlName+" with error: "+ retCode +" "+ strBuf.toString()));
            }
            else if (retCode >= HttpURLConnection.HTTP_MULT_CHOICE &&
                (retCode != HttpURLConnection.HTTP_PRECON_FAILED ||
                !ignoreHTTP_412)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "failed to fulfill a request to " +
                    urlName + " with error: " + retCode + " " +
                    strBuf.toString()).send();
                outMessage = null;
                continue;
            }

            // reset rc
            try {
                MessageUtils.setProperty(rcField, expRC, outMessage);
            }
            catch (Exception e) {
            }

            // rebuild the url with the initial response
            if (sub != null)
                urlName += sub.substitute(strBuf.toString());
            else
                urlName += "/" + strBuf.toString();

            for (i=0; i<=retry; i++) { // subsequent checks
                tm = System.currentTimeMillis() + sleepTime;
                do { // sleep up to sleepTime
                    mask = xq.getGlobalMask();
                    if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                        (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                        if (ack) try { // try to ack the msg
                            outMessage.acknowledge();
                        }
                        catch (Exception ex) {
                        }
                        xq.remove(sid);
                        new Event(Event.WARNING, "aborted the fulfillment to " +
                            urlName + " due to being disabled").send();
                        return;
                    }
                    try {
                        Thread.sleep(receiveTime);
                    }
                    catch (InterruptedException e) {
                    }
                } while (tm > System.currentTimeMillis());
                strBuf = new StringBuffer();
                try {
                    retCode = conn.doGet(urlName, strBuf);
                }
                catch (Exception e) {
                    retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                    new Event(Event.WARNING, "failed to check fulfillment on " +
                        urlName + ": " + e.toString()).send();
                }
                catch (Error e) {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.ERR, "failed to check fulfillment on " +
                        urlName + ": " + e.toString()).send();
                    Event.flush(e);
                }
                if (retCode >= HttpURLConnection.HTTP_OK &&
                    retCode < HttpURLConnection.HTTP_MULT_CHOICE)
                    break;
                else if (retCode == HttpURLConnection.HTTP_PRECON_FAILED &&
                    ignoreHTTP_412)
                    break;
                if (i == 0 && retCode >= HttpURLConnection.HTTP_INTERNAL_ERROR)
                    new Event(Event.WARNING, "failed to check fulfillment on " +
                        urlName + ": " + retCode).send();
                if (i < retry && conn.reconnect() != null)
                    break;
            }
            if (retCode >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                throw(new IOException("failed to check fulfillment on " +
                    urlName + " after " + i + " retries with error: " +
                    retCode + " " + strBuf.toString()));
            }
            else if (retCode >= HttpURLConnection.HTTP_MULT_CHOICE &&
                (retCode != HttpURLConnection.HTTP_PRECON_FAILED ||
                !ignoreHTTP_412)) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.ERR, "fulfillment timed out on " + urlName +
                    " after " + i + " retries with error: " + retCode +
                    " " + strBuf.toString()).send();
                outMessage = null;
                continue;
            }

            // reset rc
            try {
                MessageUtils.setProperty(rcField, msgRC, outMessage);
            }
            catch (Exception e) {
            }

            try {
                if (outMessage instanceof TextMessage) {
                    outMessage.clearBody();
                    ((TextMessage) outMessage).setText(strBuf.toString());
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else if (outMessage instanceof BytesMessage) {
                    outMessage.clearBody();
                    ((BytesMessage) outMessage).writeBytes(
                        strBuf.toString().getBytes());
                    MessageUtils.setProperty(rcField, okRC, outMessage);
                }
                else {
                    if (ack) try { // try to ack the msg
                        outMessage.acknowledge();
                    }
                    catch (Exception ex) {
                    }
                    xq.remove(sid);
                    new Event(Event.WARNING, "unexpected JMS msg for " +
                        urlName).send();
                    outMessage = null;
                    continue;
                }
            }
            catch (JMSException e) {
                if (ack) try { // try to ack the msg
                    outMessage.acknowledge();
                }
                catch (Exception ex) {
                }
                xq.remove(sid);
                new Event(Event.WARNING, "failed to load msg with fulfillment "+
                    "result from " + urlName + ": " + e.getMessage()).send();
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
                new Event(Event.WARNING, "failed to load msg with fulfillment "+
                    "result from " + urlName + ": " + e.getMessage()).send();
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
                new Event(Event.ERR, "failed to ack msg after fulfillment to "+
                    urlName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to ack msg after fulfillment to "+
                    urlName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to ack msg after fulfillment to "+
                    urlName + ": " + e.toString()).send();
                Event.flush(e);
            }
            xq.remove(sid);
            if (displayMask > 0) try {
                String line = MessageUtils.display(outMessage,
                    strBuf.toString(), displayMask, propertyName);
                new Event(Event.INFO, "fultilled " + strBuf.length() +
                    " bytes from " + urlName + " with a msg ("+ line +
                    " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "fulfilled " + strBuf.length() +
                    " bytes from " + urlName + " with a msg").send();
            }
            count ++;
            outMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;
        }
        if (displayMask != 0 && maxNumberMsg != 1)
            new Event(Event.INFO, "fulfilled " + count + " requests").send();
    }

    /**
     * It gets a JMS Message from the XQueue and stores its content into a
     * URL via http POST continuously.
     *<br/><br/>
     * It will always try to retrieve the URL from the field specified by
     * FieldName property.  If the message does not contain the URL, the method
     * will use the default URL defined by URI property.
     *<br/><br/>
     * The store operation also supports the requests.  If the XAMode has
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
     * If the XQueue has enabled the EXTERNAL_XA bit, it will also acknowledge
     * the messages.  This method is MT-Safe.
     */
    public void store(XQueue xq) throws IOException, TimeoutException,
        JMSException {
        Message inMessage;
        Map<String, String> extraHeader = null;
        BytesBuffer msgBuf = null;
        long count = 0, stm = 10, idleTime, currentTime;
        int sid = -1, i, mask, n = 0;
        int retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        int dmask = MessageUtils.SHOW_DATE;
        StringBuffer strBuf = new StringBuffer();
        String dt = null, msgStr = null;
        String urlName = null;
        String okRC = String.valueOf(MessageUtils.RC_OK);
        String jmsRC = String.valueOf(MessageUtils.RC_JMSERROR);
        String uriRC = String.valueOf(MessageUtils.RC_NOTFOUND);
        byte[] buffer = new byte[bufferSize];
        boolean checkIdle = (maxIdleTime > 0);
        boolean isWriteable = ((xaMode & MessageUtils.XA_CLIENT) > 0);
        boolean acked = ((xq.getGlobalMask() & XQueue.EXTERNAL_XA) > 0);
        boolean hasTemplate = (msg2Text != null);
        boolean isBytes = false;
        boolean isSleepy = (sleepTime > 0);

        if (isSleepy)
            stm = (sleepTime > waitTime) ? waitTime : sleepTime;

        dmask ^= displayMask;
        dmask &= displayMask;
        currentTime = System.currentTimeMillis();
        idleTime = currentTime;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.STANDBY) > 0) // standby temporarily
                break;
            if ((sid = xq.getNextCell(waitTime)) < 0) {
                if (checkIdle) {
                    if (n++ == 0) {
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
            currentTime = System.currentTimeMillis();

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

            urlName = null;
            if (fieldName != null) try {
                urlName = MessageUtils.getProperty(fieldName, inMessage);
                if (isWriteable)
                    MessageUtils.setProperty(rcField, jmsRC, inMessage);
            }
            catch (Exception e) {
                new Event(Event.WARNING, "failed to get urlname on msg " +
                    "from " + xq.getName() + ": " + e.toString()).send();
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
            if (urlName == null)
                urlName = uri;

            msgStr = null;
            try { // for JMS messages
                if (hasTemplate) // for JMSEvent only
                    msgStr = msg2Text.format(0, inMessage);
                else
                    msgStr = MessageUtils.processBody(inMessage, buffer);
            }
            catch (JMSException e) {
            }

            if (msgStr == null) {
                new Event(Event.WARNING, uri + ": unknown msg type").send();
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

            isBytes = (inMessage instanceof BytesMessage);
            msgBuf = new BytesBuffer();
            if (isBytes) {
                ((BytesMessage) inMessage).reset();
                MessageUtils.copyBytes((BytesMessage) inMessage, buffer,msgBuf);
            }
            else {
                msgBuf.write(msgStr.getBytes(), 0, msgStr.length());
            }
            if (copyHeader && reqPropertyName != null) { // extra headers
                String str = null;
                extraHeader = new HashMap<String, String>();
                for (i=0; i<reqPropertyName.length; i++) {
                    if (reqPropertyName[i] == null)
                        continue;
                    str = inMessage.getStringProperty(reqPropertyValue[i]);
                    if (str == null || str.length() <= 0)
                        continue;
                    extraHeader.put(reqPropertyName[i], str);
                }
            }
            else if (extraHeader != null)
                extraHeader = null;
            for (i=0; i<=retry; i++) {
                strBuf = new StringBuffer();
                try {
                    if (usePut)
                        retCode = conn.doPut(urlName, extraHeader, msgBuf,
                            strBuf, new BytesBuffer());
                    else
                        retCode = conn.doPost(urlName, extraHeader, msgBuf,
                            strBuf, new BytesBuffer());
                }
                catch (Exception e) {
                    retCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                    strBuf.insert(0, e.toString() + " ");
                }
                catch (Error e) {
                    xq.putback(sid);
                    new Event(Event.ERR, "failed to store on " +
                        urlName + ": " + e.toString()).send();
                    msgBuf.close();
                    msgBuf = null;
                    Event.flush(e);
                }
                if (retCode >= HttpURLConnection.HTTP_OK &&
                    retCode < HttpURLConnection.HTTP_MULT_CHOICE)
                    break;
                else if (retCode == HttpURLConnection.HTTP_PRECON_FAILED &&
                    ignoreHTTP_412)
                    break;
                if (i == 0)
                    new Event(Event.WARNING, "failed to store to " +
                        urlName + " with " + retCode +
                        ": " + strBuf.toString()).send();
                mask = xq.getGlobalMask();
                if ((mask & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.STANDBY) > 0) { // temporarily disabled
                    xq.putback(sid);
                    new Event(Event.WARNING, "aborted to store on " +
                        urlName + " due to being disabled").send();
                    msgBuf.close();
                    msgBuf = null;
                    return;
                }
                try {
                    Thread.sleep(receiveTime);
                }
                catch (InterruptedException e) {
                }
                if (i < retry && conn.reconnect() != null)
                    break;
            }
            msgBuf.close();
            msgBuf = null;
            if (retCode == HttpURLConnection.HTTP_GATEWAY_TIMEOUT &&
                ignoreTimeout) {
                new Event(Event.WARNING, "timed out to store on " +
                    urlName + ": " + retCode).send();
            }
            else if (retCode >= HttpURLConnection.HTTP_INTERNAL_ERROR ||
                retCode == HttpURLConnection.HTTP_PRECON_FAILED) {
                xq.putback(sid);
                throw(new IOException("failed to store to " + urlName +
                    " after " + i + " retries with error: " + retCode +
                    " " + strBuf.toString()));
            }

            if (acked) try {
                inMessage.acknowledge();
            }
            catch (JMSException e) {
                String str = "";
                Exception ee = e.getLinkedException();
                if (ee != null)
                    str += "Linked exception: " + ee.getMessage()+ "\n";
                new Event(Event.ERR, "failed to ack msg after store on " +
                    urlName + ": " + str + Event.traceStack(e)).send();
            }
            catch (Exception e) {
                new Event(Event.ERR,"failed to ack msg after store on "+
                    urlName + ": " + Event.traceStack(e)).send();
            }
            catch (Error e) {
                xq.remove(sid);
                new Event(Event.ERR,"failed to ack msg after store on "+
                    urlName + ": " + e.toString()).send();
                Event.flush(e);
            }

            if (retCode >= HttpURLConnection.HTTP_MULT_CHOICE &&
                (retCode != HttpURLConnection.HTTP_PRECON_FAILED ||
                !ignoreHTTP_412)) {
                xq.remove(sid);
                new Event(Event.ERR, "failed to store msg to " + urlName +
                    " after " + i + " retries with error: " + retCode +
                    " " + strBuf.toString()).send();
                inMessage = null;
                continue;
            }

            dt = null;
            if ((displayMask & MessageUtils.SHOW_DATE) > 0) try {
                dt = Event.dateFormat(new Date(inMessage.getJMSTimestamp()));
            }
            catch (JMSException e) {
            }
            if (isWriteable) try {
                MessageUtils.setProperty(rcField, okRC, inMessage);
                inMessage.setJMSTimestamp(currentTime);
            }
            catch (JMSException e) {
                new Event(Event.WARNING, "failed to set RC_OK after " +
                    "store on " + urlName).send();
            }
            xq.remove(sid);
            count ++;
            if (displayMask > 0) try { // display the message
                String line = MessageUtils.display(inMessage, msgStr, dmask,
                    propertyName);
                if ((displayMask & MessageUtils.SHOW_DATE) > 0)
                    new Event(Event.INFO, "stored " + msgStr.length() +
                        " bytes to " + urlName + " with a msg ( Date: " +
                        dt + line + " )").send();
                else // no date
                    new Event(Event.INFO, "stored " + msgStr.length() +
                        " bytes to " + urlName + " with a msg (" +
                        line + " )").send();
            }
            catch (Exception e) {
                new Event(Event.INFO, "stored " + msgStr.length() +
                    " bytes to " + urlName + " with a msg").send();
            }
            inMessage = null;

            if (maxNumberMsg > 0 && count >= maxNumberMsg)
                break;

            if (isSleepy) { // slow down for a while
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
            new Event(Event.INFO, "stored " + count + " msgs to " + uri).send();
    }

    public void close() {
        if (conn != null)
            conn.close();
    }

    public String reconnect() {
        return conn.reconnect();
    }

    public String getOperation() {
        return operation;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return conn.isConnected();
    }
}
