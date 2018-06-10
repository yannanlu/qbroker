package org.qbroker.monitor;

/* MultiFileMonitor.java - a monitor watching a group of files */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Arrays;
import java.util.Comparator;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;
import org.qbroker.monitor.Report;
import org.qbroker.monitor.FTPTester;

/**
 * MultiFileMonitor monitors a group of files on their mtime and/or size
 *<br/><br/>
 * Use MultiFileMonitor to get the mtime and/or size of a group of files.
 * It is assumed that all the files exist and are readable all the time.
 * MultiFileMonitor can monitor files on a remote server as long as it runs
 * the new version of ftpd and accepts ftp requests.
 *<br/><br/>
 * You can use MultiFileMonitor to monitor when the files have been updated.
 * If not, how late they are.  In case of very late, MultiFileMonitor sends
 * alerts or invokes the preconfigured script.
 *<br/><br/>
 * MultiFileMonitor also supports mtime correlations and file size correlations
 * between two sets of files.  One is a single reference file, either locally
 * or remotely.  The other set contains the target files to be monitored.
 * The reference file controls the correlation process.  Whenever the reference
 * file updates, MultiFileMonitor adjusts its timer and correlates
 * this change with the target files.  If each of the target file has been
 * updated accordingly, MultiFileMonitor treats it OK.  Otherwise, it will send
 * alerts according to the predefined tolerance on the lateness of the target
 * files.
 *<br/><br/>
 * In order to configure MultiFileMonitor to do time correlations, you have to
 * specify a Map named 'reference' in its property hash.  The reference
 * hash map contains most of the properties required by a FileMonitor object,
 * such as uri, name, etc.  The tolerance of the lateness will be
 * controlled by the threshold parameters.  In fact, MultiFileMonitor will
 * create a separate instance for the reference file.  The method of
 * performAction() will actually do the time correlations between two files.
 *<br/><br/>
 * In case of the size correlations, you must specify the triggerSize in the
 * property hash map.  The triggerSize is zero or any positive number that
 * defines two different states.  One is the state that the file size is less
 * than the triggerSize.  The other is the opposite.  In case state of the
 * reference file changes, MultiFileMonitor will check the state of the
 * target files.  If both of them are in the same states, MultiFileMonitor
 * thinks it OK.  Otherwise, MultiFileMonitor will send alerts according
 * to the predefined tolerance on the lateness of the target file keeping
 * its state in sync.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MultiFileMonitor extends Monitor implements Comparator<long[]> {
    private File[] fileList;
    private String[] fileNameList;
    private boolean isRemote;
    private String uri, referenceName, details;
    private FTPTester ftpTester;
    private long gmtOffset = 0L, calOffset, dstOffset, timeDifference;
    private long previousTime, previousLeadTime, previousRefTime;
    private int ftpStatusOffset, previousLevel;
    private int triggerSize, timeOffset, previousTrigger, updateCount;
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

    public MultiFileMonitor(Map props) {
        super(props);
        Object o;
        String s, path = "", className;
        URI u;
        StringBuffer strBuf = new StringBuffer();
        int i, n;

        if (type == null)
            type = "MultiFileMonitor";

        if (description == null)
            description = "monitor mtime on a group of files";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((o = props.get("FileName")) == null || !(o instanceof List))
            throw(new IllegalArgumentException("FileName is not well defined"));

        n = ((List) o).size();
        fileNameList = new String[n];
        for (i=0; i<n; i++) {
            fileNameList[i] =
                MonitorUtils.substitute((String)((List) o).get(i), template);
            if (i > 0)
                strBuf.append(" ");
            strBuf.append(fileNameList[i]);
        }
        details = strBuf.toString();

        if ((path = u.getPath()) == null)
            path = "";

        try {
            path = Utils.decode(path);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to decode: " + path));
        }

        cal = null;
        tz = null;
        if ((s = u.getScheme()) == null || "file".equals(s)) {
            isRemote = false;
            n = fileNameList.length;
            fileList = new File[n];
            for (i=0; i<n; i++)
                fileList[i] = new File(path + fileNameList[i]);
        }
        else if ("ftp".equals(s)) {
            isRemote = true;
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
            for (i=0; i<fileNameList.length; i++) {
               request.add("MDTM " + path + fileNameList[i] + "\r\n");
               request.add("SIZE " + path + fileNameList[i] + "\r\n");
            }
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
                for (i=0; i<4; i++) {
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
            if (ref.get("ActionProgram") != null)
                ref.remove("ActionProgram");
            if (ref.get("MaxRetry") != null)
                ref.remove("MaxRetry");
            ref.put("Step", "1");
            referenceName = (String) ref.get("Name");

            if ((o = ref.get("ClassName")) != null)
                className = (String) o;
            else if ((o = ref.get("Type")) != null)
                className = "org.qbroker.mointor." + (String) o;
            else
                className = "org.qbroker.mointor.AgeMonitor";
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

        updateCount = 0;
        previousTrigger = -1;
        previousRefTime = -1;
        previousLevel = -1;
        ftpStatusOffset = 0 - FTPTester.TESTFAILED;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        long[][] fileStats;
        long mtime, size, leadingTime, fileSize;
        int i, returnCode;

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

        if (reference != null) {
            try {
                report = reference.generateReport(currentTime);
            }
            catch (Exception e) {
                throw(new IOException("reference failed: " +
                    Event.traceStack(e)));
            }
        }

        fileStats = new long[fileNameList.length][3];

        report.put("FileNameList", fileNameList);
        report.put("FileStats", fileStats);
        leadingTime = currentTime;
        fileSize = 0;
        if (isRemote) {
            int [] responseCode;
            String [] responseText;
            Map<String, Object> r = ftpTester.generateReport(currentTime);
            if (r.get("ReturnCode") != null) {
                returnCode = Integer.parseInt((String) r.get("ReturnCode"));
                responseCode = (int []) r.get("ResponseCode");
                responseText = (String []) r.get("ResponseText");
                if (returnCode == FTPTester.TESTOK) {
                    int k = 3;
                    for (i=0; i<fileNameList.length; i++) {
                        fileStats[i][0] = (long) i;
                        mtime = dateFormat.parse(responseText[k++],
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
                        size = (long) Integer.parseInt(responseText[k++]);
                        fileStats[i][1] = mtime;
                        fileStats[i][2] = size;
                        if (mtime < leadingTime) {
                            leadingTime = mtime;
                            fileSize = size;
                        }
                    }
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
                throw(new IOException("ftp failed"));
            }
        }
        else {
            for (i=0; i<fileNameList.length; i++) {
                fileStats[i][0] = (long) i;
                if (fileList[i].exists()) {
                    mtime = fileList[i].lastModified() - timeDifference;
                    size = fileList[i].length();
                    fileStats[i][1] = mtime;
                    fileStats[i][2] = size;
                    if (mtime < leadingTime) {
                        leadingTime = mtime;
                        fileSize = size;
                    }
                }
                else {
                    throw(new IOException(fileNameList[i] + " not found"));
                }
            }
        }

        for (i=0; i<fileNameList.length; i++) {
            if (fileStats[i][1] < 0L || fileStats[i][2] < 0L)
             throw(new IOException("failed to stat '" + fileNameList[i] + "'"));
        }
        if (reference != null) {
            report.put("LeadingTime", new long[] {leadingTime});
            report.put("FileSize", new long[] {fileSize});
        }
        else {
            report.put("SampleTime", new long[] {leadingTime});
            report.put("SampleSize", new long[] {fileSize});
        }

        if (disableMode != 0 && leadingTime > 0) {
            if (reference != null) { // compare against reference
                Object o;
                if ((o = report.get("SampleTime")) != null &&
                    o instanceof long[] && ((long[]) o).length > 0) {
                    long tt = ((long[]) o)[0];
                    if (leadingTime - tt > -timeOffset) // caught up
                        skip = (disableMode > 0) ? NOSKIP : DISABLED;
                    else // not caught up yet
                        skip = (disableMode < 0) ? NOSKIP : DISABLED;
                }
                else
                    throw(new IOException("failed to get reference time"));
            }
            else if (tw != null) { // time window defined
                i = tw.getThresholdLength();
                if (i >= 2) switch (tw.check(currentTime, leadingTime)) {
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
                else switch (tw.check(currentTime, leadingTime)) {
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
            else if (leadingTime > previousTime) // updated
                skip = (disableMode > 0) ? NOSKIP : DISABLED;
            else // not updated yet
                skip = (disableMode < 0) ? NOSKIP : DISABLED;

            previousTime = leadingTime;
        }

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int level = 0;
        long leadingTime, fileSize, fileStats[][];
        StringBuffer strBuf = new StringBuffer();
        Object o;

        if (reference != null) {
            return correlate(status, currentTime, latest);
        }

        if ((o = latest.get("SampleTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            leadingTime = ((long[]) o)[0];
        }
        else
            leadingTime = -1L;

        if ((o = latest.get("SampleSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            fileSize = ((long[]) o)[0];
        }
        else
            fileSize = -1L;

        if ((o = latest.get("FileStats")) != null &&
            o instanceof long[][]) {
            fileStats = (long[][]) o;
        }
        else
            fileStats = new long[0][3];

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
                strBuf.append(uri + ": all files have been updated recently");
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
            strBuf.append(uri + ": the leading file was updated ");
            strBuf.append((int) ((currentTime - leadingTime)/60000));
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
            strBuf.append(uri + ": at lease one of files has not been updated"+
                " in the last ");
            strBuf.append((int) ((currentTime - leadingTime)/60000));
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
            strBuf.append(uri + ": none of the files is being checked due to " +
                "blackout");
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
            if (previousStatus == status && previousLevel == level)
                return null;
            break;
        }
        previousStatus = status;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
            event.setAttribute("details", details);
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
            event.setAttribute("details", getDetails(currentTime, fileStats));
        }

        if (leadingTime >= 0)
            event.setAttribute("leadingTime",
                Event.dateFormat(new Date(leadingTime)));
        else
            event.setAttribute("leadingTime", "N/A");

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
        long sampleTime, sampleSize, leadingTime = -1L, fileSize = -1L;
        long[][] fileStats;
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

        if ((o = latest.get("LeadingTime")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            leadingTime = ((long[]) o)[0];
        }
        else
            leadingTime = -1L;

        if ((o = latest.get("FileSize")) != null &&
            o instanceof long[] && ((long[]) o).length > 0) {
            fileSize = ((long[]) o)[0];
        }
        else
            fileSize = -1L;

        if ((o = latest.get("FileStats")) != null &&
            o instanceof long[][]) {
            fileStats = (long[][]) o;
        }
        else
            fileStats = new long[0][3];

        if (status >= TimeWindows.NORMAL) { // good test
            if (leadingTime - sampleTime > - timeOffset) {
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
                if (leadingTime > 0L)
                    previousLeadTime = leadingTime;
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
            if (previousStatus == status && previousTrigger == triggerStatus) {
                // always OK
                if (leadingTime > 0L)
                    previousLeadTime = leadingTime;
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
                strBuf.append(uri+": all files have been updated accordingly");
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
            if (leadingTime > previousLeadTime) { // target just updated
                actionCount = 0;
                updateCount = 0;
            }
            if (sampleTime > previousRefTime) // reference updated
                updateCount ++;

            if (leadingTime > previousLeadTime) // target just updated
                actionCount = 0;
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
                strBuf.append(uri);
                strBuf.append(": at least one of files has not been updated ");
                if (triggerSize >= 0)
                    strBuf.append(" to the right state");
                strBuf.append(" in the last ");
                strBuf.append((int) ((currentTime - leadingTime)/60000));
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
                if (leadingTime > 0L)
                    previousLeadTime = leadingTime;
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
                if (leadingTime > 0L)
                    previousLeadTime = leadingTime;
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
            strBuf.append(uri + ": none of files is checked due to blackout");
            break;
          case TimeWindows.ELATE: // extremely late and do not care
            if (previousStatus == status && previousTrigger == triggerStatus) {
                if (leadingTime > 0L)
                    previousLeadTime = leadingTime;
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
        if (leadingTime > 0L)
            previousLeadTime = leadingTime;
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
                previousTrigger == triggerStatus)
                return null;
            break;
        }
        previousStatus = status;
        previousTrigger = triggerStatus;
        previousLevel = level;

        Event event = new Event(level, strBuf.toString());
        if (status < TimeWindows.BLACKOUT) {
            event.setAttribute("actionCount", String.valueOf(exceptionCount));
            event.setAttribute("details", details);
        }
        else {
            event.setAttribute("actionCount", String.valueOf(actionCount));
            event.setAttribute("details", getDetails(currentTime, fileStats));
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
        if (leadingTime >= 0)
            event.setAttribute("leadingTime",
                Event.dateFormat(new Date(leadingTime)));
        else
            event.setAttribute("leadingTime", "N/A");
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

    private String getDetails(long currentTime, long[][] fileStats) {
        int i, j;
        StringBuffer strBuf = new StringBuffer();
        Arrays.sort(fileStats, this);
        for (i=0; i<fileNameList.length; i++) {
            if (i > 0)
                strBuf.append("\n\t ");
            j = (int) fileStats[i][0];
            strBuf.append(fileNameList[j] + ": ");
            strBuf.append(String.valueOf(fileStats[i][2]) + " ");
            strBuf.append(Event.dateFormat(new Date(fileStats[i][1])));
        }
        return strBuf.toString();
    }

    public int compare(long[] a, long[] b) {
        if (a[1] > b[1])
            return 1;
        else if (a[1] < b[1])
            return -1;
        else
            return 0;
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
        if (reference != null) {
            reference.destroy();
            reference = null;
        }
        if (ftpTester != null) {
            ftpTester.destroy();
            ftpTester = null;
        }
    }

    protected void finalize() {
        destroy();
    }
}
