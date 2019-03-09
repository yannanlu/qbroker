package org.qbroker.receiver;

/* HeartbeatGenerator.java - a receiver generating time sequence messages */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Date;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.Service;
import org.qbroker.common.XQueue;
import org.qbroker.common.Browser;
import org.qbroker.common.AssetList;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.jms.JMSEvent;
import org.qbroker.jms.TextEvent;
import org.qbroker.jms.BytesEvent;
import org.qbroker.jms.MessageUtils;
import org.qbroker.receiver.Receiver;
import org.qbroker.event.Event;

/**
 * HeartbeatGenerator as a daemon periodically generates time sequence events
 * of JMS messages according to predefined policies.  It sends them to
 * the output XQueue with the predefined name and time signatures.
 * These messages can then be used to trigger other actions down the stream.
 *<br/><br/>
 * In case of non-daemon mode, it only runs once but with the support of Limit.
 * The Limit is number of copies for the generated messages.
 *<br/><br/>
 * HeartbeatGenerator allows to define a Template or a TemplateFile to build
 * message body for each Monitor rule. If TemplateFile is an empty string,
 * it is assumed to use the shared template file. The shared template file
 * is defined in the property of URI on Receiver level. In this case, make sure
 * the file exists and is readable. The templates depend on properties of
 * StringProperty on the container level. Among the variable names in a
 * template, "name" and "sequence" are two reserved words. Please do not
 * defined them in the StringProperty on Receiver level.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class HeartbeatGenerator extends Receiver {
    private long sessionTime;
    private int textMode, retryCount;
    private int defaultHeartbeat;
    private int receiveTime = 1000;
    private int[] heartbeat;
    private AssetList assetList = null;
    private Map<String, String> sharedProps;
    private Template sharedTemplate = null;
    private final static int OCC_ID = 0;
    private final static int OCC_LIMIT = 1;
    private final static int OCC_MODE = 2;
    private final static int OCC_OPTION = 3;
    private final static int OCC_HBEAT = 4;
    private final static int OCC_COUNT = 5;
    private final static int OCC_STATUS = 6;
    private final static int OCC_TIME = 7;
    public final static int OF_HOLDON = 0;
    public final static int OF_KEEPNEW = 1;
    public final static int OF_KEEPOLD = 2;

    public HeartbeatGenerator(Map props) {
        super(props);
        Object o;
        Map h;
        Map<String, Object> task;
        List group = null;
        URI u;
        String key, scheme, filename;
        int i, n, id;
        byte[] buffer = new byte[4096];

        if (uri == null || uri.length() <= 0)
            throw(new IllegalArgumentException("URI is not defined"));

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        scheme = u.getScheme();
        if (!"hb".equals(scheme))
            throw(new IllegalArgumentException("unsupported scheme: " +scheme));

        filename = u.getPath();
        if (filename != null && filename.length() > 0 &&
            !filename.endsWith("/")) try {
            FileInputStream fs = new FileInputStream(filename);
            sharedTemplate = new Template(Utils.read(fs, buffer));
            fs.close();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to read from shared " +
                filename + ": " + e.toString()));
        }

        if ((o = props.get("TextMode")) != null)
            textMode = Integer.parseInt((String) o);
        else
            textMode = 1;

        if ((o = props.get("Heartbeat")) != null)
            defaultHeartbeat = 1000*Integer.parseInt((String) o);
        else
            defaultHeartbeat = 60000;

        if ((o = props.get("ReceiveTime")) != null) {
            receiveTime = Integer.parseInt((String) o);
            if (receiveTime <= 0)
                receiveTime = 1000;
        }

        if ((o = props.get("Operation")) != null)
            operation = (String) o;
        else
            operation = "generate";

        if (!"generate".equals(operation))
            throw(new IllegalArgumentException("unsupported operation: " +
                operation));

        sharedProps = new HashMap<String, String>();
        if ((o = props.get("StringProperty")) != null && o instanceof Map) {
            String str;
            h = (Map) o;
            for (Object obj : h.keySet()) {
                str = (String) obj;
                if ((key = MessageUtils.getPropertyID(str)) == null)
                    key = str;
                sharedProps.put(key, (String) h.get(str));
            }
        }
        if (!sharedProps.containsKey("hostname"))
            sharedProps.put("hostname", Event.getHostName().toLowerCase());

        if((o = props.get("Monitor")) != null && o instanceof List)
            group = (List) o;

        if (group == null || group.size() == 0) {
            new Event(Event.WARNING, "no Monitor defined").send();
        }

        assetList = new AssetList(uri, 1024);
        n = group.size();
        long[] meta;
        int[] hbeat = new int[n];
        int hmax = 0;
        for (i=0; i<hbeat.length; i++) {
            h = (Map) group.get(i);

            hbeat[i] = defaultHeartbeat;
            key = (String) h.get("Name");
            if (assetList.containsKey(key))
                continue;
            meta = new long[OCC_TIME + 1];
            for (int j=0; j<=OCC_TIME; j++)
                meta[j] = 0;
            if ((o = h.get("Heartbeat")) != null)
                hbeat[i] = 1000*Integer.parseInt((String) o);
            if (hbeat[i] <= 0)
                hbeat[i] = defaultHeartbeat;
            if (hbeat[i] > hmax)
                hmax = hbeat[i];
            meta[OCC_HBEAT] = hbeat[i];
            if ((o = h.get("TextMode")) != null)
                meta[OCC_MODE] = Integer.parseInt((String) o);
            else
                meta[OCC_MODE] = textMode;
            if ((o = h.get("OverflowOption")) == null)
                meta[OCC_OPTION] = OF_KEEPOLD;
            else if ("KeepNew".equals((String) o))
                meta[OCC_OPTION] = OF_KEEPNEW;
            else if ("KeepOld".equals((String) o))
                meta[OCC_OPTION] = OF_KEEPOLD;
            else
                meta[OCC_OPTION] = OF_HOLDON;
            if ((o = h.get("Limit")) != null) {
                meta[OCC_LIMIT] = Integer.parseInt((String) o);
                if (meta[OCC_LIMIT] <= 0)
                    meta[OCC_LIMIT] = 0;
            }

            task = new HashMap<String, Object>();
            if ((o = h.get("StringProperty")) != null && o instanceof Map) {
                String str;
                String[] pn, pv;
                Iterator iter = ((Map) o).keySet().iterator();
                int k = ((Map) o).size();
                pn = new String[k];
                pv = new String[k];
                k = 0;
                while (iter.hasNext()) {
                    str = (String) iter.next();
                    if ((pn[k] = MessageUtils.getPropertyID(str)) == null)
                        pn[k] = str;
                    pv[k] = (String) ((Map) o).get(str);
                    k ++;
                }
                task.put("PropertyName", pn);
                task.put("PropertyValue", pv);
            }
            task.put("Name", key);

            if ((o = h.get("Template")) != null)
                task.put("template", new Template((String) o));
            else if ((o = h.get("TemplateFile")) != null) {
                filename = (String) o;
                if (filename.length() == 0) // shared msg template 
                    task.put("template", sharedTemplate);
                else try {
                    FileInputStream fs = new FileInputStream(filename);
                    task.put("template", new Template(Utils.read(fs, buffer)));
                    fs.close();
                }
                catch (Exception e) {
                    throw(new IllegalArgumentException("failed to read from " +
                        filename + ": " + e.toString()));
                }
            }
            id = assetList.add(key, meta, task);
        }

        n = assetList.size();
        if (n > 0)
            heartbeat = MonitorUtils.getHeartbeat(hbeat);
        else
            heartbeat = new int[] {defaultHeartbeat};

        new Event(Event.INFO, uri + " opened and ready to " + operation +
            " on " + linkName).send();

        retryCount = 0;
        sessionTime = 0L;
    }

    public void receive(XQueue xq, int baseTime) {
        String str = xq.getName();
        int mask;

        if (str != null && !linkName.equals(str))
            linkName = str;
        capacity = xq.getCapacity();
        retryCount = 0;
        sessionTime = System.currentTimeMillis();
        resetStatus(RCVR_READY, RCVR_RUNNING);
        if (baseTime <= 0)
            baseTime = pauseTime;

        for (;;) {
            while (keepRunning(xq) && (status == RCVR_RUNNING ||
                status == RCVR_RETRYING)) { // session
                heartbeatOperation(xq, baseTime);

                if (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0 &&
                    (mask & XQueue.PAUSE) > 0) { // pause temporarily
                    if (status == RCVR_READY) // for confirmation
                        setStatus(RCVR_DISABLED);
                    else if (status == RCVR_RUNNING) try {
                        // no state change so just yield
                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                    }
                }

                if (status > RCVR_RETRYING && status < RCVR_STOPPED)
                    new Event(Event.INFO, uri + " is " + // state changed
                        Service.statusText[status] + " on " + linkName).send();
            }

            while (status == RCVR_DISABLED) { // disabled
                if (!keepRunning(xq))
                    break;
                try {
                    Thread.sleep(waitTime);
                }
                catch (Exception e) {
                }
            }

            while ((xq.getGlobalMask() & XQueue.PAUSE) > 0 ||
                status == RCVR_PAUSE) {
                if (status > RCVR_PAUSE)
                    break;
                long tt = System.currentTimeMillis() + pauseTime;
                while ((xq.getGlobalMask() & XQueue.PAUSE) > 0) {
                    if (status > RCVR_PAUSE)
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
                status == RCVR_STANDBY) {
                if (status > RCVR_STANDBY)
                    break;
                long tt = System.currentTimeMillis() + standbyTime;
                while ((xq.getGlobalMask() & XQueue.STANDBY) > 0) {
                    if (status > RCVR_STANDBY)
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

            if (isStopped(xq) || status >= RCVR_STOPPED)
                break;
            if (status == RCVR_READY) {
                setStatus(RCVR_RUNNING);
                new Event(Event.INFO, uri + " restarted on " + linkName).send();
            }
        }
        if (status < RCVR_STOPPED)
            setStatus(RCVR_STOPPED);

        new Event(Event.INFO, uri + " stopped on " + linkName).send();
    }

    /**
     * real implementation of receive() with exception handling and retry
     */
    private int heartbeatOperation(XQueue xq, int baseTime) {
        int i = 0;

        if (baseTime <= 0)
            baseTime = pauseTime;

        try {
            if (mode == 0)
                runonce(xq);
            else
                generate(xq);
        }
        catch (Exception e) {
            new Event(Event.ERR, linkName + " " + uri + ": " +
                Event.traceStack(e)).send();
            setStatus(RCVR_STOPPED);
            return -1;
        }
        int mask = xq.getGlobalMask();
        if ((mask & XQueue.KEEP_RUNNING) > 0 && status == RCVR_RUNNING &&
            (mask & XQueue.PAUSE) == 0) // job is done
            setStatus(RCVR_STOPPED);
        sessionTime = System.currentTimeMillis();
        return 0;
    }

    private void generate(XQueue xq) {
        Object o;
        Message msg;
        Browser browser;
        Map h;
        Template temp;
        String key;
        String[] propertyName, propertyValue;
        long[] meta;
        long sessionTime, currentTime, count = 0;
        int mask;
        int i, j, k, cid, min = 0, size, leftover;
        int[] activeItem;
        int sid = 0; // the id of the occurrence group
        int id = 0;  // the id of the thread or job queue
        int hid = 0; // the id of the heartbeat array
        int n, hbeat, defaultHBeat;
        int shift = partition[0];
        int len = partition[1];

        n = assetList.size();
        activeItem = new int[n];
        hid = heartbeat.length - 1;
        hbeat = heartbeat[hid];
        sessionTime = System.currentTimeMillis();
        leftover = 0;
        n = 0;
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            currentTime = System.currentTimeMillis();
            if (currentTime < sessionTime) { // session not due yet
                try {
                    Thread.sleep(receiveTime);
                }
                catch (Exception e) {
                }
                continue;
            }

            if (leftover <= 0) { // session starting
                n = 0;
                browser = assetList.browser();
                while ((id = browser.next()) >= 0) {
                    meta = assetList.getMetaData(id);
                    if ((hbeat % (int) meta[OCC_HBEAT]) != 0)
                        continue;
                    activeItem[n++] = id;
                }
            }

            if (status == RCVR_RUNNING) { // not disabled
                if (leftover > 0) // jumped back
                    j = n - leftover;
                else
                    j = 0;
                leftover = 0;
                for (i=j; i<n; i++) { // generate msgs
                    id = activeItem[i];
                    key = assetList.getKey(id);
                    switch (len) {
                      case 0:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime)) >= 0)
                                break;
                        }
                        break;
                      case 1:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime, shift)) >= 0)
                                break;
                        }
                        break;
                      default:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime, shift, len)) >= 0)
                                break;
                        }
                        break;
                    }
                    if (sid < 0) { // xq is full
                        new Event(Event.WARNING, "xq is full for " +
                            key + " at " + count).send();
                        leftover = n-i;
                        break;
                    }
                    meta = assetList.getMetaData(id);
                    h = (Map) assetList.get(key);
                    temp = (Template) h.get("template");
                    if (temp == null)
                        msg = new TextEvent();
                    else if (temp.numberOfFields() <= 0)
                        msg = new TextEvent(temp.copyText());
                    else { // with template variables
                        sharedProps.put("name", key);
                        sharedProps.put("sequence",
                            String.valueOf(meta[OCC_COUNT]));
                        msg = new TextEvent(temp.substitute(temp.copyText(),
                            sharedProps));
                    }
                    propertyName = (String[]) h.get("PropertyName");
                    propertyValue = (String[]) h.get("PropertyValue");
                    if (propertyName != null && propertyValue != null) {
                        for (j=0; j<propertyName.length; j++)
                            ((JMSEvent) msg).setAttribute(propertyName[j],
                                propertyValue[j]);
                    }
                    ((JMSEvent) msg).setAttribute("name", key);
                    ((JMSEvent) msg).setAttribute("sequence",
                        String.valueOf(meta[OCC_COUNT]));
                    j = xq.add(msg, sid);
                    if (j < 0) { // failed to add the msg to the xq
                        new Event(Event.WARNING, "failed to add msg for " +
                            key + " with " + meta[OCC_COUNT]).send();
                        xq.cancel(sid);
                        continue;
                    }
                    else if (displayMask > 0) {
                        new Event(Event.INFO, "generated a msg for " + key +
                            " with " + meta[OCC_COUNT]).send();
                    }
                    count ++;
                    meta[OCC_COUNT] ++;
                    meta[OCC_TIME] = currentTime;
                    msg = null;
                }
                if (leftover > 0) // go back to check incoming events
                    continue;
            }
            leftover = 0;
            hid ++;
            if (hid >= heartbeat.length) { // reset session
                hid = 0;
                sessionTime += heartbeat[hid];
            }
            else {
                sessionTime += heartbeat[hid] - hbeat;
            }
            hbeat = heartbeat[hid];
            currentTime = System.currentTimeMillis();
            if (currentTime > sessionTime) // reset sesstionTime
                sessionTime = currentTime;
        }
        if (displayMask != 0)
            new Event(Event.INFO, uri + " generated " + count + " msgs to " +
                xq.getName()).send();
    }

    private void runonce(XQueue xq) {
        Object o;
        Message msg;
        Browser browser;
        Map h;
        Template temp;
        String key;
        String[] propertyName, propertyValue;
        long[] meta;
        long currentTime, count = 0;
        int mask;
        int i, j, k, n, loop, limit;
        int sid = 0; // the id of the occurrence group
        int id = 0;  // the id of the thread or job queue
        int shift = partition[0];
        int len = partition[1];

        n = assetList.size();
        limit = 0;
        browser = assetList.browser();
        while ((id = browser.next()) >= 0) {
            meta = assetList.getMetaData(id);
            if (limit < meta[OCC_LIMIT])
                limit = (int) meta[OCC_LIMIT];
        }
        k = 0;
        count = 0;
        loop = 0;
        sessionTime = System.currentTimeMillis();
        currentTime = System.currentTimeMillis();
        while (((mask = xq.getGlobalMask()) & XQueue.KEEP_RUNNING) > 0) {
            if ((mask & XQueue.PAUSE) > 0) // paused temporarily
                break;
            if (status == RCVR_RUNNING) { // not disabled
                for (id=k; id<n; id++) { // generate msgs
                    meta = assetList.getMetaData(id);
                    if (meta[OCC_COUNT] >= meta[OCC_LIMIT]) // no more to flush
                        continue;
                    switch (len) {
                      case 0:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime)) >= 0)
                                break;
                        }
                        break;
                      case 1:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime, shift)) >= 0)
                                break;
                        }
                        break;
                      default:
                        for (j=0; j<5; j++) { // reserve an empty cell
                            if ((sid = xq.reserve(waitTime, shift, len)) >= 0)
                                break;
                        }
                        break;
                    }
                    if (sid < 0) { // xq is full
                        new Event(Event.WARNING, "xq is full for " +
                            assetList.getKey(id) + " at " + loop).send();
                        k = id;
                        break;
                    }
                    key = assetList.getKey(id);
                    h = (Map) assetList.get(key);
                    temp = (Template) h.get("template");
                    if (temp == null)
                        msg = new TextEvent();
                    else if (temp.numberOfFields() <= 0)
                        msg = new TextEvent(temp.copyText());
                    else {
                        sharedProps.put("name", key);
                        sharedProps.put("sequence",
                            String.valueOf(meta[OCC_COUNT]));
                        msg = new TextEvent(temp.substitute(temp.copyText(),
                            sharedProps));
                    }
                    propertyName = (String[]) h.get("PropertyName");
                    propertyValue = (String[]) h.get("PropertyValue");
                    if (propertyName != null && propertyValue != null) {
                        for (j=0; j<propertyName.length; j++)
                            ((JMSEvent) msg).setAttribute(propertyName[j],
                                propertyValue[j]);
                    }
                    ((JMSEvent) msg).setAttribute("name", key);
                    ((JMSEvent) msg).setAttribute("sequence",
                        String.valueOf(meta[OCC_COUNT]));
                    j = xq.add(msg, sid);
                    if (j < 0) { // failed to add the msg to the xq
                        new Event(Event.WARNING, "failed to add msg for " +
                            key + " with " + meta[OCC_COUNT]).send();
                        xq.cancel(sid);
                        continue;
                    }
                    else if (displayMask > 0) {
                        new Event(Event.INFO, "generated a msg for " + key +
                            " with " + (meta[OCC_COUNT]+1)).send();
                    }
                    count ++;
                    meta[OCC_COUNT] ++;
                    meta[OCC_TIME] = currentTime;
                    msg = null;
                }
                k = 0;
                loop ++;
                if (loop >= limit) // done with flush
                    break;
                j = 0;
                currentTime = System.currentTimeMillis();
            }
            else {
                break;
            }
        }
        if (displayMask != 0)
            new Event(Event.INFO, uri + " generated " + count + " msgs to " +
                xq.getName()).send();
    }

    public void close() {
        int id;
        String key;
        Map task;
        Browser browser = assetList.browser();
        while ((id = browser.next()) >= 0) {
           key = assetList.getKey(id);
           task = (Map) assetList.get(key);
           task.clear();
        }
        assetList.clear();
        sharedProps.clear();
        
        if (status != RCVR_CLOSED)
            new Event(Event.INFO, uri + " closed on " + linkName).send();
        setStatus(RCVR_CLOSED);
    }

    protected void finalize() {
        close();
    }
}
