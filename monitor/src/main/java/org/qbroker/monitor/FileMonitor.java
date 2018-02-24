package org.qbroker.monitor;

/* FileMonitor.java - a monitor checking the timestamp of a file */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;
import org.qbroker.net.FTPConnector;
import org.qbroker.net.SFTPConnector;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.FTPTester;

/**
 * FileMonitor.java monitors a given file on its last modified time and/or size
 *<br/><br/>
 * Use FileMonitor to get the last modified time and/or size of a given file.
 * It is assumed that the file exists and is readable all times.  FileMonitor
 * can monitor a remote file as long as the protocol is defined.  In case of
 * FTP protocol, FileMonitor supports two implementations.  One is the
 * traditional FTP with SetPassiveMode set to true or false, or without
 * any datalink via using two new cmd of MDTZ and SIZE.
 *<br/><br/>
 * You can use FileMonitor to monitor when the file has been updated.  If not,
 * how late it is.  In case of very late, FileMonitor sends alerts.
 *<br/><br/>
 * FileMonitor also supports mtime correlations and size correlations between
 * two files.  In this case, there are two files involved.  One is the
 * reference file.  The other is the target file to be monitored.  The
 * reference file controls the correlation process.  Whenever the reference
 * file is updated or modified, FileMonitor adjusts its timer and correlates
 * this change with the target file.  If the target file has been updated
 * accordingly, the FileMonitor treats it OK.  Otherwise, it will send alerts
 * according to the predefined tolerance on the lateness of the target file
 * being updated.
 *<br/><br/>
 * In order to configure FileMonitor to do time correlations, you have to
 * specify a Map named as reference in its property hash.  The reference
 * hash table contains most of the properties required by a FileMonitor object,
 * such as filename, name, etc.  The tolerance of the lateness will be
 * controlled by the threshold parameters.  In fact, FileMonitor will create
 * a separate instance for the reference file.  The method of performAction()
 * will actually do the time correlations between two files.
 *<br/><br/>
 * In case of the size correlations, you must specify the triggerSize in the
 * property hashmap.  The triggerSize is zero or any positive number that
 * defines two different states.  One is the state that the file size is
 * less than the triggerSize.  The other is the opposite.  In case state
 * of the reference file changes, FileMonitor will check the state of
 * the target file.  If both files are in the same states, FileMonitor thinks
 * it OK.  Otherwise, FileMonitor will send alerts according to the
 * predefined tolerance on the lateness of the target file keeping its
 * state in sync.
 *<br/><br/>
 * In case of monitoring timestamp on a remote box, there is an issue with
 * its Daylight Time on its TimeZone.  For example of EST, the timestamp of
 * Oct. 31 2004, 01:30:00 EST is 3600 seconds later than Oct. 31 2004,
 * 01:30:00 EDT.  The same time strings give two different Unix timestamps.
 * If there is no TimeZone info available in the time string, you have to
 * guess its real value.  The hack implemented here is based on an assumption
 * that the remote timestamp is close to the currentTime of the local box.
 * Therefore, if the difference between the remote time and the currentTime
 * is large than the half DST_OFFSET of the remote TimeZone, the remote time
 * will be adjusted with the DST_OFFSET.  Otherwise, no adjustment.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FileMonitor extends Monitor {
    private File file;
    private boolean isRemote;
    private String uri, baseURI, referenceName, fileName = null;
    private String hostname, username, password;
    private FTPTester ftpTester = null;
    private FTPConnector ftpConn = null;
    private SFTPConnector sftpConn = null;
    private long gmtOffset, calOffset, dstOffset, timeDifference;
    private long previousTime, previousLeadTime, previousRefTime;
    private int timeout, ftpStatusOffset, setPassiveMode;
    private int triggerSize, timeOffset, previousTrigger;
    private int updateCount, previousLevel;
    private SimpleDateFormat dateFormat;
    private MonitorReport reference;
    private TimeWindows tw = null;
    private Calendar cal;
    private TimeZone tz;
    private final static String ftpStatusText[] = {"Exception",
        "Test OK", "Protocol error", "Commond failed",
        "File not found", "Client error", "Server error",
        "Read timeout", "Write timeout", "Connection timeout",
        "Server is down"};

    public FileMonitor(Map props) {
        super(props);
        Object o;
        URI u;
        String s, className;
        int n;
        if (type == null)
            type = "FileMonitor";

        if (description == null)
            description = "monitor mtime on a local or remote file";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        cal = null;
        tz = null;
        setPassiveMode = 0;
        fileName = u.getPath();
        if (fileName == null || fileName.length() == 0)
            throw(new IllegalArgumentException("URI has no path: " + uri));

        try {
            fileName = Utils.decode(fileName);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to decode: "+ fileName));
        }

        // check time dependency on fileName
        n = initTimeTemplate(fileName);
        if (n > 0)
            fileName = updateContent(System.currentTimeMillis());

        if ((s = u.getScheme()) == null || "file".equals(s)) {
            isRemote = false;
            file = new File(fileName);
        }
        else if ("sftp".equals(s)) { // for sftp
            isRemote = true;
            ftpConn = null;
            ftpTester = null;
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("SOTimeout", props.get("Timeout"));

            try {
                sftpConn = new SFTPConnector(h);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to init sftp: " +
                    e.toString()));
            }
            if ((o = props.get("TimeZone")) != null)
                tz = TimeZone.getTimeZone((String) o);
            else
                tz = TimeZone.getDefault();
            cal = Calendar.getInstance(tz);
            gmtOffset = cal.get(Calendar.ZONE_OFFSET);
            dstOffset = 0;
            if (tz.useDaylightTime()) {
                long t = System.currentTimeMillis();
                for (int i=0; i<4; i++) {
                    cal.setTime(new Date(t));
                    dstOffset = cal.get(Calendar.DST_OFFSET);
                    if (dstOffset > 0)
                        break;
                    t += (i+1) * 8640000000L;
                }
            }
            cal = Calendar.getInstance();
            calOffset = cal.get(Calendar.ZONE_OFFSET);
        }
        else if ("ftp".equals(s) && props.get("SetPassiveMode") == null) {
            isRemote = true;
            sftpConn = null;
            ftpConn = null;
            List<String> request = new ArrayList<String>();
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("Name", name);
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("User", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("Timeout", (String) props.get("Timeout"));
            h.put("Step", "1");
            request.add("TYPE I\r\n");
            request.add("MDTM " + fileName + "\r\n");
            request.add("SIZE " + fileName + "\r\n");
            h.put("Request", request);
            ftpTester = new FTPTester(h);
            dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            if ((o = props.get("TimeZone")) != null)
                tz = TimeZone.getTimeZone((String) o);
            else
                tz = TimeZone.getDefault();
            cal = Calendar.getInstance(tz);
            gmtOffset = cal.get(Calendar.ZONE_OFFSET);
            dstOffset = 0;
            if (tz.useDaylightTime()) {
                long t = System.currentTimeMillis();
                for (int i=0; i<4; i++) {
                    cal.setTime(new Date(t));
                    dstOffset = cal.get(Calendar.DST_OFFSET);
                    if (dstOffset > 0)
                        break;
                    t += (i+1) * 8640000000L;
                }
            }
            cal = Calendar.getInstance();
            calOffset = cal.get(Calendar.ZONE_OFFSET);
        }
        else if ("ftp".equals(s) && (o = props.get("SetPassiveMode")) != null) {
            isRemote = true;
            sftpConn = null;
            ftpTester = null;
            Map<String, Object> h = new HashMap<String, Object>();
            h.put("URI", uri);
            if ((o = props.get("User")) != null) {
                h.put("Username", o);
                if ((o = props.get("Password")) != null)
                    h.put("Password", o);
                else if ((o = props.get("EncryptedPassword")) != null)
                    h.put("EncryptedPassword", o);
            }
            h.put("SOTimeout", props.get("Timeout"));
            h.put("SetPassiveMode", props.get("SetPassiveMode"));

            try {
                ftpConn = new FTPConnector(h);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to init ftp: " +
                    e.toString()));
            }
            if ((o = props.get("TimeZone")) != null)
                tz = TimeZone.getTimeZone((String) o);
            else
                tz = TimeZone.getDefault();
            cal = Calendar.getInstance(tz);
            gmtOffset = cal.get(Calendar.ZONE_OFFSET);
            dstOffset = 0;
            if (tz.useDaylightTime()) {
                long t = System.currentTimeMillis();
                for (int i=0; i<4; i++) {
                    cal.setTime(new Date(t));
                    dstOffset = cal.get(Calendar.DST_OFFSET);
                    if (dstOffset > 0)
                        break;
                    t += (i+1) * 8640000000L;
                }
            }
            cal = Calendar.getInstance();
            calOffset = cal.get(Calendar.ZONE_OFFSET);
        }
        else {
            throw(new IllegalArgumentException("unsupported scheme: " + s));
        }
        n = uri.indexOf(u.getPath());
        baseURI = (n > 0) ? uri.substring(0, n) : "";

        timeDifference = 0L;
        if ((o = props.get("TimeDifference")) != null)
            timeDifference = 1000L * Long.parseLong((String) o);

        previousTime = -1;
        previousLeadTime = -1;
        timeOffset = 0;
        tolerance = 0;
        triggerSize = -1;
        if ((o = props.get("Reference")) != null) {
            if (!(o instanceof Map))
              throw(new IllegalArgumentException("Reference is not a Map"));
            Map<String, Object> ref = Utils.cloneProperties((Map) o);
            if (ref.get("Reference") != null)
                ref.remove("Reference");
            if (ref.get("Dependency") != null)
                ref.remove("Dependency");
            if (ref.get("ActionProgram") != null)
                ref.remove("ActionProgram");
            if (ref.get("MaxRetry") != null)
                ref.remove("MaxRetry");
            ref.put("Step", "1");
            referenceName = (String) ref.get("Name");

            if ((o = ref.get("ClassName")) != null)
                className = (String) o;
            else if ((o = ref.get("Type")) != null)
                className = "org.qbroker.monitor." + (String) o;
            else
                className = "org.qbroker.monitor.AgeMonitor";
            try {
                java.lang.reflect.Constructor con;
                Class<?> cls = Class.forName(className);
                con = cls.getConstructor(new Class[]{Map.class});
                reference = (MonitorReport) con.newInstance(new Object[]{ref});
            }
            catch (InvocationTargetException e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e.getTargetException())).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }
            catch (Exception e) {
                new Event(Event.ERR, "failed to init Reference "+referenceName+
                    ": " + Event.traceStack(e)).send();
                throw(new IllegalArgumentException("failed to init Reference"));
            }

            if ((o = props.get("TimeOffset")) == null ||
                (timeOffset = 1000 * Integer.parseInt((String) o)) < 0)
                timeOffset = 30000;
            if ((o = props.get("TriggerSize")) == null ||
                (triggerSize = Integer.parseInt((String) o)) < -1)
                triggerSize = -1;
            if ((o = props.get("Tolerance")) != null)
                tolerance = Integer.parseInt((String) o);
        }
        else
            reference = null;

        if (referenceName == null)
            referenceName = "";

        previousTrigger = -1;
        previousRefTime = -1;
        previousLevel = -1;
        updateCount = 0;
        ftpStatusOffset = 0 - FTPTester.TESTFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long size = -1L;
        long mtime = -10L;
        int returnCode;

        report.clear();
        if (step > 0) {
            if ((serialNumber % step) != 0) {
                skip = SKIPPED;
                serialNumber ++;
                return report;
            }
            else {
                skip = NOSKIP;
                serialNumber ++;
            }
        }
        else {
            skip = NOSKIP;
            serialNumber ++;
        }

        if (dependencyGroup != null) { // check dependency
            skip = MonitorUtils.checkDependencies(currentTime, dependencyGroup,
                name);
            if (skip != NOSKIP) {
                if (skip == EXCEPTION) {
                    report.put("Exception",
                        new Exception("failed to check dependencies"));
                    return report;
                }
                else if (skip == SKIPPED)
                    return report;
                else if (!disabledWithReport)
                    return report;
            }
        }
        else if (reportMode == REPORT_CACHED) { // use the cached report
            skip = cachedSkip;
            return report;
        }

        if (reference != null) { // check reference
            try {
                report = reference.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("reference failed: " +
                    Event.traceStack(e)));
            }
        }

        if (timeFormat.length > 0) { // time-depenendent fileName
            fileName = updateContent(currentTime);
            uri = baseURI + fileName;
            if (isRemote && setPassiveMode == 0 && sftpConn == null) {
                ftpTester.updateRequest(3, "MDTM " + fileName + "\r\n");
                ftpTester.updateRequest(4, "SIZE " + fileName + "\r\n");
            }
            else if (!isRemote) {
                file = new File(fileName);
            }
        }
        report.put("URI", uri);

        if (isRemote && sftpConn != null) { // for sftp
            sftpConn.reconnect();
            try {
                size = sftpConn.getSize(fileName);
                mtime = sftpConn.getTimestamp(fileName);
            }
            catch (Exception e) { // failed
                throw(new IOException("sftp failed on " + uri + ": " +
                    e.toString()));
            }
            // success
            cal.setTime(new Date(mtime));
            mtime +=  calOffset + cal.get(Calendar.DST_OFFSET);
            mtime -= gmtOffset + timeDifference;
            if (dstOffset > 0) {
                boolean inDst = tz.inDaylightTime(new Date(mtime));
                boolean agoDst = tz.inDaylightTime(new Date(mtime - dstOffset));
                if (inDst) {
                    mtime -= dstOffset;
                }
                else if ((!inDst) && agoDst) { // around end of DST
                    if (mtime - currentTime > 0.5 * dstOffset)
                        mtime -= dstOffset;
                }
            }
            try {
                sftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else if (isRemote && setPassiveMode == 0) { // for ftpTester
            int i;
            int [] responseCode;
            String [] responseText;
            Map r = ftpTester.generateReport(currentTime);
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
                responseCode = (int []) r.get("ResponseCode");
                responseText = (String []) r.get("ResponseText");
                if (returnCode == FTPTester.TESTOK) {
                    mtime = dateFormat.parse(responseText[3],
                        new ParsePosition(0)).getTime();
                    cal.setTime(new Date(mtime));
                    mtime +=  calOffset + cal.get(Calendar.DST_OFFSET);
                    mtime -= gmtOffset + timeDifference;
                    if (dstOffset > 0) {
                        boolean inDst = tz.inDaylightTime(new Date(mtime));
                        boolean agoDst = tz.inDaylightTime(new Date(mtime -
                            dstOffset));
                        if (inDst) {
                            mtime -= dstOffset;
                        }
                        else if ((!inDst) && agoDst) { // around end of DST
                            if (mtime - currentTime > 0.5 * dstOffset)
                                mtime -= dstOffset;
                        }
                    }
                    size = (long) Integer.parseInt(responseText[4]);
                }
                else if (returnCode >= FTPTester.CMDFAILED &&
                    returnCode <= FTPTester.SERVERERROR) {
                    for (i=responseCode.length-1; i>=0; i--) {
                        if (responseCode[i] > 0 &&
                            responseText[i] != null)
                            break;
                    }
                    throw(new IOException("ftp error:(" + i + ") " +
                        responseCode[i] + " " + responseText[i]));
                }
                else {
                    throw(new IOException("ftp failed: " +
                        ftpStatusText[returnCode + ftpStatusOffset]));
                }
            }
            else {
                throw(new IOException("ftp failed on " + uri));
            }
        }
        else if (isRemote && setPassiveMode != 0) { // for ftp
            ftpConn.reconnect();
            size = ftpConn.getSize(fileName);
            mtime = ftpConn.getTimestamp(fileName);
            if (size < 0 || mtime < 0) { // failed
                throw(new IOException("ftp failed on " + uri));
            }
            else { // success
                cal.setTime(new Date(mtime));
                mtime +=  calOffset + cal.get(Calendar.DST_OFFSET);
                mtime -= gmtOffset + timeDifference;
                if (dstOffset > 0) {
                    boolean inDst = tz.inDaylightTime(new Date(mtime));
                    boolean agoDst = tz.inDaylightTime(new Date(mtime -
                        dstOffset));
                    if (inDst) {
                        mtime -= dstOffset;
                    }
                    else if ((!inDst) && agoDst) { // around end of DST
                        if (mtime - currentTime > 0.5 * dstOffset)
                            mtime -= dstOffset;
                    }
                }
            }
            try {
                ftpConn.close();
            }
            catch (Exception e) {
            }
        }
        else { // local file
            if (file.exists()) {
                mtime = file.lastModified() - timeDifference;
                size = file.length();
            }
            else {
                throw(new IOException(uri + " not found"));
            }
        }

        if (disableMode != 0 && mtime > 0) {
            if (reference != null) { // compare against reference
                Object o;
                if ((o = report.get("SampleTime")) != null &&
                    o instanceof long[] && ((long[]) o).length > 0) {
                    long tt = ((long[]) o)[0];
                    if (mtime - tt > -timeOffset) // caught up
                        skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    else // not caught up yet
                        skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
                else
                    throw(new IOException("failed to get reference time"));
            }
            else if (tw != null) { // time window defined
                int i = tw.getThresholdLength();
                if (i >= 2) switch (tw.check(currentTime, mtime)) {
                  case TimeWindows.NORMAL:
                  case TimeWindows.SLATE:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.ALATE:
                  case TimeWindows.ELATE:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
                else switch (tw.check(currentTime, mtime)) {
                  case TimeWindows.NORMAL:
                    skip = (disableMode < 0) ? NOSKIP : DISABLED;
                    break;
                  case TimeWindows.OCCURRED:
                    skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    break;
                  default:
                    break;
                }
            }
            else if (mtime > previousTime) // updated
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else // not updated yet
                skip = (disableMode < 0) ? NOSKIP : DISABLED;

            previousTime = mtime;
        }
        else if (mtime > 0 && timeFormat.length > 0) {
            // disableMode = 0 and time-depenendent fileName
            // use previousTime to store the latest time
            if (mtime > previousTime)
                previousTime = mtime;
            else
                mtime = previousTime;
        }

        if (reference != null) {
            report.put("LatestTime", new long[] {mtime});
            report.put("FileSize", new long[] {size});
        }
        else {
            report.put("SampleTime", new long[] {mtime});
            report.put("SampleSize", new long[] {size});
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long sampleTime, fileSize;
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if (reference != null) {
            return correlate(status, currentTime, latest);
        }

        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        else
            sampleTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            fileSize = ((long[]) o)[0];
        }
        else
            fileSize = -1L;

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                exceptionCount = 0;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.NORMAL:
            if (previousStatus == status) { // always normal
                exceptionCount = 0;
                return null;
            }
            else { // just back to normal
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' has been updated recently");
            }
            break;
          case TimeWindows.SLATE: // somewhat late
            level = Event.WARNING;
            exceptionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' was updated ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes ago");
            break;
          case TimeWindows.ELATE: // extremely late
          case TimeWindows.ALATE: // very late
            level = Event.ERR;
            exceptionCount = 0;
            if (previousStatus < TimeWindows.ALATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            actionCount ++;
            strBuf.append("'" + uri);
            strBuf.append("' has not been updated");
            strBuf.append(" in the last ");
            strBuf.append((int) ((currentTime - sampleTime)/60000));
            strBuf.append(" minutes");
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status)
                return null;
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not checked due to blackout");
            break;
          default: // should never reach here
            break;
        }

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level) {//late
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level) {
                return null;
            }
            break;
        }
        previousStatus = status;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        if (sampleTime >= 0)
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(sampleTime)));
        else
            event.setAttribute("latestTime", "N/A");
        if (fileSize >= 0)
            event.setAttribute("size", String.valueOf(fileSize));
        else
            event.setAttribute("size", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        event.setAttribute("actionScript", actionStatus);
        event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    private Event correlate(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0, triggerStatus = -1;
        long sampleTime, sampleSize, latestTime = -1L, fileSize = -1L;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleTime = ((long[]) o)[0];
        }
        else
            sampleTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            sampleSize = ((long[]) o)[0];
        }
        else
            sampleSize = -1L;

        if ((o = latest.get("LatestTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            latestTime = ((long[]) o)[0];
        }
        else
            latestTime = -1L;

        if ((o = latest.get("FileSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            fileSize = ((long[]) o)[0];
        }
        else
            fileSize = -1L;

        if (status >= TimeWindows.NORMAL) { // good test
            if (latestTime - sampleTime > - timeOffset) {
                if (triggerSize >= 0) { // trigger enabled
                    triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
                    if ((fileSize >= triggerSize && triggerStatus == 1) ||
                        (fileSize < triggerSize && triggerStatus == 0)) {
                        status = TimeWindows.OCCURRED;
                    }
                }
                else { // trigger disabled
                    status = TimeWindows.OCCURRED;
                }
            }
            else if (triggerSize >= 0) { // trigger enabled but no update
                triggerStatus = (sampleSize >= triggerSize) ? 1 : 0;
            }
        }

        // check the test status and exceptions, figure out the priority
        switch (status) {
          case TimeWindows.DISABLED:
            if (previousStatus == status) { // always disabled
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just disabled
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append(name);
                strBuf.append(" has been disabled");
            }
            break;
          case TimeWindows.OCCURRED:
            if (previousStatus == status &&
                previousTrigger == triggerStatus) { // always OK
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            else { // just updated
                level = Event.INFO;
                actionCount = 0;
                exceptionCount = 0;
                updateCount = 0;
                if (normalStep > 0)
                    step = normalStep;
                strBuf.append("'" + uri);
                strBuf.append("' has been updated accordingly");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
            }
            break;
          case TimeWindows.NORMAL:// just updated
          case TimeWindows.SLATE: // somewhat late
          case TimeWindows.ALATE: // very late
            exceptionCount = 0;
            if (previousStatus < TimeWindows.NORMAL ||
                previousStatus == TimeWindows.OCCURRED ||
                previousStatus >= TimeWindows.ELATE) {
                // reset count and adjust step
                actionCount = 0;
                if (step > 0)
                    step = 0;
            }
            if (latestTime > previousLeadTime) { // target just updated
                actionCount = 0;
                updateCount = 0;
            }
            if (sampleTime > previousRefTime) // reference updated
                updateCount ++;

            if (status == TimeWindows.NORMAL)
                level = Event.INFO;
            else if (status == TimeWindows.SLATE)
                level = Event.WARNING;
            else
                level = Event.ERR;

            if (updateCount <= 1) { // only one update on reference
                if (previousLevel != level)
                    actionCount = 0;
            }
            else if (updateCount > tolerance) { // upgrade to ERR
                level = Event.ERR;
                if (previousLevel > level)
                    actionCount = 0;
            }
            else {
                level = Event.WARNING;
                if (previousLevel != level)
                    actionCount = 0;
            }
            actionCount ++;
            if (level == Event.INFO) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' has been updated recently");
                if (previousTrigger != triggerStatus)
                    strBuf.append(" with the state change");
            }
            else if (level == Event.WARNING) {
                strBuf.append("Reference: '");
                strBuf.append(referenceName);
                strBuf.append("' was updated ");
                if (previousTrigger != triggerStatus)
                    strBuf.append("with the state change ");
                strBuf.append((int) ((currentTime - sampleTime)/60000));
                strBuf.append(" minutes ago");
            }
            else {
                strBuf.append("'" + uri);
                strBuf.append("' has not been updated");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
                strBuf.append(" in the last ");
                strBuf.append((int) ((currentTime - latestTime)/60000));
                strBuf.append(" minutes");
            }
            break;
          case TimeWindows.EXCEPTION: // exception
            level = Event.WARNING;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
                exceptionCount = 0;
                if (step > 0)
                    step = 0;
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 1;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            actionCount = 0;
            exceptionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("'" + uri);
            strBuf.append("' is not checked due to blackout");
            break;
          case TimeWindows.ELATE: // extremely late and do not care
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (latestTime > 0L)
                    previousLeadTime = latestTime;
                if (sampleTime > 0L)
                    previousRefTime = sampleTime;
                return null;
            }
            level = Event.INFO;
            exceptionCount = 0;
            actionCount = 0;
            updateCount = 0;
            if (normalStep > 0)
                step = normalStep;
            strBuf.append("Reference: '");
            strBuf.append(referenceName);
            strBuf.append("' has not been updated since ");
            strBuf.append(Event.dateFormat(new Date(sampleTime)));
            break;
          default: // should never reach here
            break;
        }
        if (latestTime > 0L)
            previousLeadTime = latestTime;
        if (sampleTime > 0L)
            previousRefTime = sampleTime;

        int count = 0;
        switch (level) {
          case Event.ERR: // very late
            count = actionCount;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                previousStatus = status;
                previousTrigger = triggerStatus;
                previousLevel = level;
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // either slate or exception
            if (exceptionCount > exceptionTolerance && exceptionTolerance >= 0){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    previousStatus = status;
                    previousTrigger = triggerStatus;
                    previousLevel = level;
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus) {
                return null;
            }
            break;
          default:
            if (previousStatus == status && previousLevel == level &&
                previousTrigger == triggerStatus) {
                return null;
            }
            break;
        }
        previousStatus = status;
        previousTrigger = triggerStatus;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
        }

        event.setAttribute("reference", referenceName);
        event.setAttribute("updateCount", String.valueOf(updateCount));
        if (sampleTime >= 0)
            event.setAttribute("referenceTime",
                Event.dateFormat(new Date(sampleTime)));
        else
            event.setAttribute("referenceTime", "N/A");
        if (sampleSize >= 0)
            event.setAttribute("referenceSize", String.valueOf(sampleSize));
        else
            event.setAttribute("referenceSize", "N/A");
        if (latestTime >= 0)
            event.setAttribute("latestTime",
                Event.dateFormat(new Date(latestTime)));
        else
            event.setAttribute("latestTime", "N/A");
        if (fileSize >= 0)
            event.setAttribute("size", String.valueOf(fileSize));
        else
            event.setAttribute("size", "N/A");

        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("testTime",
            Event.dateFormat(new Date(currentTime)));

        String actionStatus;
        if (actionGroup != null && actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "not configured";

        event.setAttribute("actionScript", actionStatus);
        event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return event;
    }

    private void close() {
        if (ftpConn != null) try {
            ftpConn.close();
        }
        catch (Exception e) {
        }
        if (sftpConn != null) try {
            sftpConn.close();
        }
        catch (Exception e) {
        }
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousLevel", String.valueOf(previousLevel));
        chkpt.put("PreviousTrigger", String.valueOf(previousTrigger));
        chkpt.put("PreviousTime", String.valueOf(previousTime));
        chkpt.put("PreviousLeadTime", String.valueOf(previousLeadTime));
        chkpt.put("PreviousRefTime", String.valueOf(previousRefTime));
        chkpt.put("UpdateCount", String.valueOf(updateCount));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, uCount, pTrigger, pStatus, pLevel, sNumber;
        long pTime, pLeadTime, pRefTime;
        if (chkpt == null || chkpt.size() == 0 || serialNumber > 0)
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            ct = Long.parseLong((String) o);
            if (ct <= System.currentTimeMillis() - checkpointTimeout)
                return;
        }
        else
            return;

        if ((o = chkpt.get("SerialNumber")) != null)
            sNumber = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ActionCount")) != null)
            aCount = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("ExceptionCount")) != null)
            eCount = Integer.parseInt((String) o);
        else
            return;

        if ((o = chkpt.get("PreviousLevel")) != null)
            pLevel = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTrigger")) != null)
            pTrigger = Integer.parseInt((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousTime")) != null)
            pTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousLeadTime")) != null)
            pLeadTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("PreviousRefTime")) != null)
            pRefTime = Long.parseLong((String) o);
        else
            return;
        if ((o = chkpt.get("UpdateCount")) != null)
            uCount = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousLevel = pLevel;
        previousTrigger = pTrigger;
        previousTime = pTime;
        previousLeadTime = pLeadTime;
        previousRefTime = pRefTime;
        updateCount = uCount;
    }

    public void destroy() {
        super.destroy();
        if (ftpTester != null) {
            ftpTester.destroy();
            ftpTester = null;
        }
        close();
        if (ftpConn != null) {
            ftpConn = null;
        }
        if (sftpConn != null) {
            sftpConn = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
