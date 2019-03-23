package org.qbroker.common;

/* GenericLogger.java - a generic logger for appending or overwriting */

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Category;
import org.apache.log4j.PropertyConfigurator;

/**
 * GenericLogger is a generic logger with append() and overwrite().
 * It opens the log file everytime and closes it after the output.
 *<br/><br/>
 * It also supports event logging via the static methods of info(), notice(),
 * warning(), err(), and crit(). It is for breaking the circular dependencies
 * of Maven modules.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class GenericLogger {
    public Category logger;
    private String loggerName;
    private final static byte[] LINE_SEPARATOR =
        System.getProperty("line.separator").getBytes();
    private static java.lang.reflect.Method method = null;
    private static java.lang.reflect.Constructor con = null;

    public GenericLogger(String loggerName) {
        this.loggerName = loggerName;
    }

    public GenericLogger(String loggerName, Properties props) {
        this.loggerName = loggerName;
    }

    /** Creates new GenericLogger */
/*
    public GenericLogger(String loggerName, Properties props) {
        logger = Category.getInstance(loggerName);
        PropertyConfigurator.configure(props);
    }

    public GenericLogger(String loggerName) {
        Properties logProperty = new Properties();
        String fileName = loggerName;
        StringBuffer strBuf = new StringBuffer(loggerName);
        int n = strBuf.length();
        for (int i=0; i<n; i++) {
            if (strBuf.charAt(i) == '.')
                strBuf.setCharAt(i, '_');
        }
        loggerName = strBuf.toString();
//        logProperty.setProperty("log4j.rootCategory", "Debug, " +
//            loggerName);
        logProperty.setProperty("log4j.category", "Debug, " +
            loggerName);
        logProperty.setProperty("log4j.additivity." + loggerName, "false");
        logProperty.setProperty("log4j.appender." + loggerName,
            "org.apache.log4j.FileAppender");
        logProperty.setProperty("log4j.appender." + loggerName + ".File",
            fileName);
        logProperty.setProperty("log4j.appender." + loggerName + ".layout",
            "org.apache.log4j.PatternLayout");
        logProperty.setProperty("log4j.appender." + loggerName +
            ".layout.ConversionPattern", "%d %m%n");

        logger = Category.getInstance(loggerName);
        PropertyConfigurator.configure(logProperty);
    }
*/

    public int log(String line) {
        try {
            FileOutputStream out = new FileOutputStream(loggerName, true);
            out.write(line.getBytes());
            out.write(LINE_SEPARATOR);
            out.close();
        }
        catch (IOException e) {
            return 1;
        }
//        logger.info(line);
        return 0;
    }

    public int overwrite(String line) {
        try {
            FileOutputStream out = new FileOutputStream(loggerName, false);
            out.write(line.getBytes());
            out.write(LINE_SEPARATOR);
            out.close();
        }
        catch (IOException e) {
            return 1;
        }
        return 0;
    }

    public static int log(String line, String loggerName) {
        try {
            FileOutputStream out = new FileOutputStream(loggerName, true);
            out.write(line.getBytes());
            out.write(LINE_SEPARATOR);
            out.close();
        }
        catch (IOException e) {
            return 1;
        }
        return 0;
    }

    public static int overwrite(String line, String loggerName) {
        try {
            FileOutputStream out = new FileOutputStream(loggerName, false);
            out.write(line.getBytes());
            out.write(LINE_SEPARATOR);
            out.close();
        }
        catch (IOException e) {
            return 1;
        }
        return 0;
    }

    public static void log(int p, String text) {
        if (con == null || method == null)
            initEventConstructor();
        if (con != null && method != null) try {
            Object event = con.newInstance(new Object[]{new Integer(p), text});
            method.invoke(event);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getLoggerName() {
        return loggerName;
    }

    private synchronized static void initEventConstructor() {
        if (con != null)
            return;
        try {
            Class<?> cls = Class.forName("org.qbroker.event.Event");
            method = cls.getMethod("send", new Class[]{});
            con = cls.getConstructor(new Class<?>[]{int.class,
                Class.forName("java.lang.String")});
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
