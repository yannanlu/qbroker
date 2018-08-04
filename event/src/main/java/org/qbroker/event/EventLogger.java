package org.qbroker.event;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.qbroker.event.Event;
import org.qbroker.event.EventUtils;
import org.qbroker.net.HTTPConnector;

/**
 * EventLogger is a logger using log4j to log events.  It also supports
 * web logging.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventLogger {
    private static HTTPConnector webConn = null;
    private static String relayLog = null;
    private static String url = null;
    private Logger eLogger;
    private final SimpleDateFormat zonedDateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zz");

    /** Creates new EventLogger */
    public EventLogger(String loggerName, Properties props) {
       eLogger = Logger.getLogger(loggerName);
       PropertyConfigurator.configure(props);
    }

    public static StringBuffer format(Event event) {
        String value = null;
        StringBuffer logEntry = new StringBuffer();
        logEntry.append((String) event.attribute.get("priority"));
        logEntry.append(" ");
        if (event.attribute.containsKey("name"))
            logEntry.append((String) event.attribute.get("name"));
        else
            logEntry.append(event.getDefaultName());
        if (event.logMode != Event.LOG_LOCAL) {
            logEntry.append(" ");
            logEntry.append((String) event.attribute.get("hostname"));
        }
        if (event.attribute.containsKey("text")) {
            logEntry.append(" ");
            value = (String) event.attribute.get("text");
            logEntry.append((String) event.attribute.get("text"));
        }
        switch (event.logMode) {
          case Event.LOG_JMS:
          case Event.LOG_CENTRAL:
            for (String key : event.attribute.keySet()) {
                if (key == null || key.length() <= 0)
                    continue;
                if (key.equals("priority")) continue;
                if (key.equals("name")) continue;
                if (key.equals("hostname")) continue;
                if (key.equals("program")) continue;
                if (key.equals("date")) continue;
                if (key.equals("text")) continue;
                if (value != null && value.endsWith(EventUtils.LINE_SEPARATOR))
                    logEntry.append("\t");
                else
                    logEntry.append(EventUtils.LINE_SEPARATOR + "\t");
                logEntry.append(key);
                logEntry.append(": ");
                value = (String) event.attribute.get(key);
                logEntry.append(value);
            }
            break;
          case Event.LOG_COMPACT:
            for (String key : event.attribute.keySet()) {
                if (key == null || key.length() <= 0)
                    continue;
                if (key.equals("priority")) continue;
                if (key.equals("name")) continue;
                if (key.equals("hostname")) continue;
                if (key.equals("program")) continue;
                if (key.equals("date")) continue;
                if (key.equals("site")) continue;
                if (key.equals("category")) continue;
                if (key.equals("text")) continue;
                if (value != null && value.endsWith(EventUtils.LINE_SEPARATOR))
                    logEntry.append("\t");
                else
                    logEntry.append(EventUtils.LINE_SEPARATOR + "\t");
                logEntry.append(key);
                logEntry.append(": ");
                value = (String) event.attribute.get(key);
                logEntry.append(value);
            }
            break;
          default:
            for (String key : event.attribute.keySet()) {
                if (key == null || key.length() <= 0)
                    continue;
                if (key.equals("priority")) continue;
                if (key.equals("name")) continue;
                if (key.equals("hostname")) continue;
                if (key.equals("program")) continue;
                if (key.equals("pid")) continue;
                if (key.equals("owner")) continue;
                if (key.equals("date")) continue;
                if (key.equals("site")) continue;
                if (key.equals("category")) continue;
                if (key.equals("text")) continue;
                if (value != null && value.endsWith(EventUtils.LINE_SEPARATOR))
                    logEntry.append("\t");
                else
                    logEntry.append(EventUtils.LINE_SEPARATOR + "\t");
                logEntry.append(key);
                logEntry.append(": ");
                value = (String) event.attribute.get(key);
                logEntry.append(value);
            }
            break;
        }
        return logEntry;
    }

    public int log(Event event) {
        StringBuffer logEntry = format(event);
        try {
            eLogger.info(logEntry.toString());
        }
        catch (Exception e) {
            System.err.println(new Date().toString() +
                " Failed to log the following event: ");
            System.err.println(e.getMessage());
            System.err.flush();
        }

        if (event.getPriority() >= Event.DEBUG) //no sending to remote for DEBUG
            return 0;
        else if (url != null) { // web logging
            int rc;
            StringBuffer response = new StringBuffer();
            String line = EventUtils.postable(event);
            try {
                webConn.reconnect();
                rc = webConn.doPost(url, line, response);
                webConn.close();
                if (rc != HttpURLConnection.HTTP_OK) {
                    response.append(": " + rc);
                }
            }
            catch (Exception e) {
                webConn.close();
                System.err.println(new Date().toString() +
                    " Failed to send the event to: " + url + "\n" + line +
                    "\n" + e.getMessage());
                System.err.flush();
            }
        }
        else if (relayLog != null) { // log relay
            String line = EventUtils.collectible(event);
            try {
                FileOutputStream out = new FileOutputStream(relayLog, true);
                out.write(zonedDateFormat.format(new Date()).getBytes());
                out.write((byte) 32);
                out.write(Event.getIPAddress().getBytes());
                out.write((byte) 32);
                out.write(line.getBytes());
                out.write((byte) 10);
                out.flush();
                out.close();
            }
            catch (Exception e) {
                System.err.println(new Date().toString() +
                    " Failed to append the event to : " + relayLog);
                System.err.println(e.getMessage());
                System.err.flush();
            }
        }

        return 0;
    }

    public Logger getLogger() {
        return eLogger;
    }

    public synchronized static void enableWeblog(String urlStr)
        throws MalformedURLException {
        URI u;
        String scheme;
        try {
            u = new URI(urlStr);
        }
        catch (URISyntaxException e) {
            throw(new MalformedURLException(e.toString()));
        }
        catch (Exception e) {
            throw(new MalformedURLException(e.toString()));
        }

        scheme = u.getScheme();
        if ("file".equals(scheme)) {
            relayLog = u.getPath();
            url = null;
        }
        else if ("http".equals(scheme) || "https".equals(scheme)) {
            url = urlStr;
            Map<String, String> h = new HashMap<String, String>();
            h.put("URI", urlStr);
            h.put("IsPost", "true");
            h.put("SOTimeout", "30");
            webConn = new HTTPConnector(h);
        }
        else {
            url = null;
        }
    }

    public void close() {
        eLogger = null;
        if (webConn != null) {
            webConn.close();
            webConn = null;
        }
    }

    protected void finalize() {
        close();
    }
}
