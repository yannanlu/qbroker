package org.qbroker.event;

/* Event.java - a JMS like message */

import java.io.*;
import java.util.Properties;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Date;
import java.net.InetAddress;
import org.qbroker.event.EventLogger;
import org.qbroker.event.EventUtils;
import org.qbroker.common.TraceStackThread;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Utils;

/**
 * Event is a JMS like message for carrying payload to communicate
 * to a Centralized Repository.  It has its own transport method, send(),
 * which can be used to send event to the repository if the logger has been
 * initialized.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class Event implements org.qbroker.common.Event {
    public final static int LOG_LOCAL = 0;
    public final static int LOG_CENTRAL = 1;
    public final static int LOG_COMPACT = 2;
    public final static int LOG_JMS = 3;
    public final static int LOG_SYSLOG = 4;

    // JNI method for getting OS PID of the current JVM
    private static native int getOSPID();
    // JNI method for getting HOSTID of the machine
    private static native int getHOSTID();

    private int priority;   // priority of the event
    protected int eventID;  // event ID
    protected int groupID;  // group ID of event
    protected int logMode;  // log mode
    protected int deliveryMode;   // delivery mode of event
    protected long timestamp;     // timestamp of event
    protected long expiration;    // expiration of event
    protected final HashMap<String, Object> attribute =
        new HashMap<String, Object>();// attributes of the event
    protected Object body = null; // body of the event

    public final static String  ITEM_SEPARATOR          = " ~ ";
    public final static String  ESCAPED_ITEM_SEPARATOR  = " __tilde__ ";
    public final static String  ESCAPED_CARRIAGE_RETURN  = "__cr__";
    public final static String  ESCAPED_AMPERSAND  = "__ampersand__";
    public final static String  ESCAPED_QUOTE  = "__quote__";
    public final static String  ESCAPED_EQUAL  = "__equal__";

    private static int pid;
    private static int hostid;
    private static String logDir = null;
    private static String eventDir = null;
    private static String programName;
    private static String hostName;
    private static String ipAddr;
    private static String owner;
    private static String loggerName;
    private static String defaultName = "event";
    private static String defaultSite = "DEFAULT";
    private static String defaultCategory = "EVENT";
    private static String defaultType = "Unknown";
    private static EventLogger eLogger = null;
    private static boolean isDisabled = false;

    public Event(int priority) {
        if (priority >= EMERG && priority <= NONE) {
            this.priority = priority;
            attribute.put("priority", priorityNames[priority]);
        }
        else
            throw new IllegalArgumentException("Illegal priority");
        logMode = LOG_LOCAL;
        groupID = -1;
        eventID = -1;
        timestamp = System.currentTimeMillis();
        expiration = 0L;
        deliveryMode = -1;
        body = null;
    }

    public Event(int priority, String message) {
        this(priority);
        if (message != null && message != "")
            attribute.put("text", message);
    }

    public Event(int priority, String message, String filename) {
        this(priority, message);
        if (filename != null && filename != "")
            attribute.put("filename", filename);
    }

    /**
     * returns priority in number or -1 on a bad priority name
     */
    public static int getPriorityByName(String name) {
        if (name == null || name.length() <= 0)
            return -1;
        switch (name.charAt(0)) {
          case 'T':
            if ("TRACE".equals(name))
                return TRACE;
            break;
          case 'D':
            if ("DEBUG".equals(name))
                return DEBUG;
            break;
          case 'I':
            if ("INFO".equals(name))
                return INFO;
            break;
          case 'N':
            if ("NOTICE".equals(name))
                return NOTICE;
            else if ("NONE".equals(name))
                return NONE;
            break;
          case 'W':
            if ("WARNING".equals(name))
                return WARNING;
            break;
          case 'E':
            if ("ERR".equals(name))
                return ERR;
            else if ("EMERG".equals(name))
                return EMERG;
            break;
          case 'C':
            if ("CRIT".equals(name))
                return CRIT;
            break;
          case 'A':
            if ("ALERT".equals(name))
                return ALERT;
            break;
          default:
        }
        return -1;
    }

    public static int getPID() {
        return pid;
    }

    public static int getHostID() {
        return hostid;
    }

    public static String getIPAddress() {
        return ipAddr;
    }

    public static String getHostName() {
        if (hostName == null)
            setHostName();
        return hostName;
    }

    public static String getProgramName() {
        return programName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long ts) {
        timestamp = ts;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long ts) {
        expiration = ts;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int p) {
        if (priority >= EMERG && priority <= NONE) {
            priority = p;
            attribute.put("priority", priorityNames[priority]);
        }
    }

    public int getGroupID() {
        return groupID;
    }

    public void setGroupID(int gid) {
        groupID = gid;
    }

    public String getAttribute(String name) {
        Object o;
        o = attribute.get(name);
        if (o != null && !(o instanceof String))
            return o.toString();
        else
            return (String) o;
    }

    /**
     * It sets an attribute of the Event
     *<br/>
     * @param name      the name of the attribute
     * @param value     the value of the attribute
     */
    public synchronized void setAttribute(String name, String value) {
        //  skip the empty fields
        if ( name != null && name != "" && value != null && value != "")
            attribute.put(name, value);
    }

    public Iterator getAttributeNames() {
        return attribute.keySet().iterator();
    }

    public void clearAttributes() {
        attribute.clear();
        attribute.put("priority", priorityNames[priority]);
    }

    /**
     * It sets the body of the Event with an Object which may not be
     * transportable.
     */
    public void setBody(Object obj) {
        body = obj;
    }

    public Object getBody() {
        return body;
    }

    public String removeAttribute(String name) {
        return (String) attribute.remove(name);
    }

    public boolean attributeExists(String name) {
        return attribute.containsKey(name);
    }

    private String priorityName() {
	return priorityNames[priority];
    }

    /**
     * It sets the log dir and initializes the logger without date pattern.
     */
    public static void setLogDir(String dir) {
        if (logDir != null)
            return;
        setLogDir(dir, null);
    }

    /**
     * It sets the log dir for the events and initializes the logger with the
     * given date pattern.
     */
    public synchronized static void setLogDir(String dir, String datePattern) {
        if (logDir != null)
            return;
        if (dir == null || dir.length() == 0)
            logDir = ".";
        else
            logDir = dir;

        loggerName = System.getProperty("LoggerName");
        if (loggerName == null)
            loggerName = programName;
        Properties logProperty = new Properties();

        if ("-".equals(dir)) { // for stdout
            logProperty.setProperty("log4j.rootCategory", "Debug, "+loggerName);
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.ConsoleAppender");
        }
        else if (datePattern != null && datePattern.startsWith("'.'")) {
            logProperty.setProperty("log4j.rootCategory", "Info, "+loggerName);
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.DailyRollingFileAppender");
            logProperty.setProperty("log4j.appender." + loggerName +
                ".DatePattern", datePattern);
            logProperty.setProperty("log4j.appender." + loggerName + ".File",
                logDir + System.getProperty("file.separator") +
                loggerName + ".log");
        }
        else {
            logProperty.setProperty("log4j.rootCategory", "Info, "+loggerName);
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.FileAppender");
            logProperty.setProperty("log4j.appender." + loggerName + ".File",
                logDir + System.getProperty("file.separator") +
                loggerName + ".log");
        }
        logProperty.setProperty("log4j.appender." + loggerName + ".layout",
            "org.apache.log4j.PatternLayout");
        logProperty.setProperty("log4j.appender." + loggerName +
            ".layout.ConversionPattern", "%d [%t] %m%n");
        eLogger = new EventLogger(loggerName, logProperty);

        if ("-".equals(dir)) // for stdout
            return;

        eventDir = logDir + EventUtils.FILE_SEPARATOR + "events";
        File ed = new File(eventDir); 
        if (!ed.exists()) {
            if (!ed.mkdir())
                eventDir = logDir;
        }
        else if (!ed.isDirectory())
            eventDir = logDir;
        return;
    }

    /**
     * It sets the log dir for the events and initializes the logger with the
     * given size limit and number of backups
     */
    public synchronized static void setLogDir(String dir, String maxSize,
        int maxBackups) {
        if (logDir != null)
            return;
        if (dir == null || dir.length() == 0)
            logDir = ".";
        else
            logDir = dir;

        loggerName = System.getProperty("LoggerName");
        if (loggerName == null)
            loggerName = programName;
        Properties logProperty = new Properties();
        logProperty.setProperty("log4j.rootCategory", "Debug, " + loggerName);

        if ("-".equals(dir)) { // for stdout
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.ConsoleAppender");
        }
        else if (maxSize != null && maxBackups > 0) {
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.RollingFileAppender");
            logProperty.setProperty("log4j.appender." + loggerName +
                ".MaxFileSize", maxSize);
            logProperty.setProperty("log4j.appender." + loggerName +
                ".MaxBackupIndex", String.valueOf(maxBackups));
            logProperty.setProperty("log4j.appender." + loggerName + ".File",
                logDir + System.getProperty("file.separator") +
                loggerName + ".log");
        }
        else {
            logProperty.setProperty("log4j.appender." + loggerName,
                "org.apache.log4j.FileAppender");
            logProperty.setProperty("log4j.appender." + loggerName + ".File",
                logDir + System.getProperty("file.separator") +
                loggerName + ".log");
        }
        logProperty.setProperty("log4j.appender." + loggerName + ".layout",
            "org.apache.log4j.PatternLayout");
        logProperty.setProperty("log4j.appender." + loggerName +
            ".layout.ConversionPattern", "%d [%t] %m%n");
        eLogger = new EventLogger(loggerName, logProperty);

        if ("-".equals(dir)) // for stdout
            return;

        eventDir = logDir + EventUtils.FILE_SEPARATOR + "events";
        File ed = new File(eventDir); 
        if (!ed.exists()) {
            if (!ed.mkdir())
                eventDir = logDir;
        }
        else if (!ed.isDirectory())
            eventDir = logDir;
        return;
    }

    private synchronized static void setOwner() {
        if (owner != null)
            return;
        owner = System.getProperty("user.name");
        if (owner == null)
            owner = "";
        return;
    }

    private synchronized static void setHostName() {
        if (hostName != null)
            return;

        hostName = "";
        try {
            java.net.InetAddress ipAddress =java.net.InetAddress.getLocalHost();
            ipAddr = ipAddress.getHostAddress();
            try {
                hostName = ipAddress.getCanonicalHostName();
                if (hostName == null || hostName.length() == 0)
                    hostName = ipAddress.getHostAddress();
/*
                else {
                    int i = hostName.indexOf(".");
                    if (i > 0)
                        hostName = hostName.substring(0, i);
                }
*/
            }
            catch (Exception e) {
                hostName = ipAddr;
            }
        }
        catch (java.net.UnknownHostException e) {
            return;
        }
        return;
    }

    public synchronized static void setDefaultName(String name) {
        if (name == null || name.length() == 0)
            return;
        defaultName = name;
    }

    protected static String getDefaultName() {
        return defaultName;
    }

    public synchronized static void setDefaultSite(String site) {
        if (site == null || site.length() == 0)
            return;
        defaultSite = site;
    }

    public synchronized static void setDefaultCategory(String category) {
        if (category == null || category.length() == 0)
            return;
        defaultCategory = category;
    }

    public synchronized static void setDefaultType(String type) {
        if (type == null || type.length() == 0)
            return;
        defaultType = type;
    }

    public void print(PrintStream stream) {
        String key, value;
        Iterator iter = (attribute.keySet()).iterator();

        stream.println("Event Priority: "+priorityName());

        while ( iter.hasNext() ) {
            key = (String) iter.next();
            value = (String) attribute.get(key);
            stream.println(key + ": " + value);
        }
    }

    public synchronized void send() {
        long currentTime;
        if (logDir == null) {
            if (!isDisabled)
                System.out.println((String) attribute.get("text"));
            return;
        }
        currentTime = System.currentTimeMillis();
        if (attribute.get("priority") == null)
            attribute.put("priority", priorityNames[priority]);
        if (attribute.get("program") == null)
            attribute.put("program", programName);
        if (attribute.get("date") == null)
            attribute.put("date", new Date(currentTime).toString());
        if (attribute.get("hostname") == null)
            attribute.put("hostname", hostName);
        if (attribute.get("owner") == null)
            attribute.put("owner", owner);
        if (attribute.get("pid") == null)
            attribute.put("pid", String.valueOf(pid));

        eLogger.log(this);
    }

    /**
     * It prints out error msg and rethrows it if it is an Error
     */
    public static void flush(Throwable e) {
        if (e instanceof Error) {
            System.err.println(new Date().toString() +
                " " + Thread.currentThread().getName() + ": "
                + e.toString());
            System.err.flush();
            throw((Error) e);
        }
        else {
            System.err.println(new Date().toString() +
                " " + Thread.currentThread().getName() + ": " +
                TraceStackThread.traceStack(e));
            System.err.flush();
        }
    }

    // method used to trace stack on an exception
    public static String traceStack(Throwable e) {
        return TraceStackThread.traceStack(e);
    }

    // method used to pipe the StackTrace to another thread
    private static Reader getPipeInput(Throwable e) throws IOException {
        PipedWriter pipeOut = new PipedWriter();
        PipedReader pipeIn = new PipedReader(pipeOut);
        PrintWriter out = new PrintWriter(pipeOut);

        new TraceStackThread(out, e).start();

        return pipeIn;
    }

    // this method should only ever be called once
    private static void setProgramName() {
        if (programName != null)
            return;

        programName = "";
        try {
            Reader pin = getPipeInput(new Exception());

            BufferedReader in = new BufferedReader(pin);

            String line, lastline = "";
            while((line = in.readLine()) != null) {
                lastline = line;
                //System.err.println(line);
            }
            in.close();

            int j;
            j = lastline.lastIndexOf("(");
            if (j >= 0) {
                j = lastline.lastIndexOf(".", j);
            }

            int i = lastline.lastIndexOf("at ");
            if (i >= 0 && j >=0) {
                line = lastline.substring(i + 3, j);
                programName = line.substring(line.lastIndexOf(".") + 1);
                //System.err.println("Setting ProgramName to " + programName);
            }
        }
        catch (Exception e) {
        }
        return;
    }

    public static String dateFormat(Date date) {
        return Utils.dateFormat(date);
    }

    public static Date parseDate(String date) {
        return Utils.parseDate(date);
    }

    /** logs to the file according to programName */
    public void log() {
        if (programName == null) {
            System.err.println(new Date().toString() +
                " " + (String) attribute.get("text"));
            System.err.flush();
            return;
        }
        if (attribute.get("priority") == null)
            attribute.put("priority", priorityNames[priority]);
        if (attribute.get("program") == null)
            attribute.put("program", programName);
        if (attribute.get("date") == null)
            attribute.put("date", Utils.dateFormat(new Date()));
        if (attribute.get("hostname") == null)
            attribute.put("hostname", hostName);
        if (attribute.get("owner") == null)
            attribute.put("owner", owner);
        if (attribute.get("pid") == null)
            attribute.put("pid", String.valueOf(pid));

        String program = (String) attribute.get("program");
        int i;
        i = program.lastIndexOf(" ");
        if (i > 0)
            program = program.substring(0, i);
        i = program.lastIndexOf(EventUtils.FILE_SEPARATOR);
        if (i >= 0)
            program = program.substring(i);
        i = program.lastIndexOf(".");
        if (i > 0)
            program = program.substring(0, i);
        StringBuffer logEntry = EventLogger.format(this);

        i = GenericLogger.log((String) attribute.get("date") + " " +
            logEntry.toString(), eventDir + EventUtils.FILE_SEPARATOR +
            program + ".log");
        if (i != 0) {
            System.err.println(new Date().toString() +
                " Failed to log the following event: ");
            System.err.println(logEntry.toString());
            System.err.flush();
        }
    }

    protected void finalize() {
        if (body != null)
            body = null;
        if (attribute != null)
            attribute.clear();
    }

    static {
        try {
            System.loadLibrary("Event");
            pid = getOSPID();
            hostid = getHOSTID();
        }
        catch (UnsatisfiedLinkError e) {
            pid = 0;
            hostid = 0;
        }
        setHostName();
        setOwner();
        setProgramName();
        if ("OFF".equals(System.getProperty("EVENT_STDOUT")))
            isDisabled = true;
    }
}
