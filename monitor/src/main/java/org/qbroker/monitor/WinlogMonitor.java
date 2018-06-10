package org.qbroker.monitor;

/* WinlogMonitor.java - a log monitor watching Windows Eventlog */

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Date;
import java.util.Calendar;
import java.util.Comparator;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.RunCommand;
import org.qbroker.common.Template;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.Monitor;

/**
 * WinlogMonitor queries Windows Event log and searches for patterns.
 * It relies on an external script to query the Event logs.  Currently,
 * we are using extractLog.js to do the job.
 *<br/><br/>
 * Use WinlogMonitor to check Windows Event log repetitively.  It runs
 * a predefined JScript to query the Event log with certain selector rules
 * and picks up the newly updated log entries.  It guarantees the order and
 * the integrity of the new logs, as long as the query script is well defined
 * and it updates the RecordNumber and TimeGenerated with each query.
 *<br/><br/>
 * In order to keep tracking the RecordNumbers, WinlogMonitor relies on a
 * reference file.  The reference file contains the RecordNumber of the
 * reference Event log, its TimeGenerated and the length of the log.
 * A reference Event log is a log entry with valid attributes of RecordNumber
 * and TimeGenerated, and is used as the reference to locate the object log.
 * It may or may not be in the current log stream.  The objective log is the
 * first log entry in the current log stream that has either a later
 * TimeGenerated or a larger RecordNumber as compared to those of the reference
 * log.  In fact, the object log is the first new log in the current log stream.
 *<br/><br/>
 * Those three parameters are stored in the reference file in the names of
 * position, timestamp and offset.  RecordNumber is represented by position.
 * TimeGenerated is repressented by timestamp.  The offset is the byte count
 * or length of the reference log.
 *<br/><br/>
 * Here are the rules for the reference file:<br/><br/>
 * (1) If there is the reference file, use it and trust it.<br/>
 * (2) If there is no reference file, use the current time as the timestamp
 * of the reference log and set the position of reference log
 * and the offset to 0.<br/>
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class WinlogMonitor extends Monitor implements Comparator<int[]> {
    private Template cmdTemplate;
    private String uri, script;
    private Process proc = null;
    private InputStream in;
    private File referenceFile;
    private int errorIgnored, maxNumberLogs, previousNumber;
    private int logSize, leftover, startBytes, bufferSize = 4096;
    private int numberDataFields, maxScannedLogs;
    private long position, timestamp, offset;
    private long previousPosition, previousTimestamp, previousOffset;
    private byte[] buffer;
    private SimpleDateFormat dateFormat;
    private Pattern pattern;
    public final static int MAXNUMBERLOGS = 40960;
    public final static int MAXSCANNEDLOGS = 40960;
    public final static int MAXLOGLENGTH = 10240;

    public WinlogMonitor(Map props) {
        super(props);
        Object o;
        URI u;
        String s, str;
        int n;

        if (type == null)
            type = "WinlogMonitor";

        if (description == null)
            description = "monitor Eventlog";

        if ((o = MonitorUtils.select(props.get("URI"))) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = MonitorUtils.substitute((String) o, template);

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if ((s = u.getScheme()) == null || !"wlog".equals(s))
            throw(new IllegalArgumentException("unsupported scheme: " + s));

        if ((o = MonitorUtils.select(props.get("Script"))) != null)
            str = MonitorUtils.substitute((String) o, template);
        else
            throw(new IllegalArgumentException("Script not defined"));

        cmdTemplate = new Template(str);

        if ((o = props.get("ReferenceFile")) == null)
            throw(new IllegalArgumentException("ReferenceFile is not defined"));
        referenceFile = new File((String) o);

        startBytes = 0;
        leftover = 0;
        buffer = new byte[bufferSize];
        str = null;
        if (referenceFile.exists()) {
            int len = 0;
            try {
                FileInputStream fin = new FileInputStream(referenceFile);
                while ((n = fin.read(buffer, len, bufferSize - len)) >= 0) {
                    len += n;
                    if (len >= 128)
                        break;
                }
                fin.close();
                str = new String(buffer, 0, len);
            }
            catch (IOException e) {
            }
        }
        if (str != null && str.length() > 0) {
            n = str.indexOf(" ");
            position = Long.parseLong(str.substring(0, n));
            str = str.substring(n+1, str.length()-1);
            n = str.indexOf(" ");
            timestamp = Long.parseLong(str.substring(0, n));
            offset = Long.parseLong(str.substring(n+1));
        }
        else { // otherwise, use the current time
            position = 0L;
            timestamp = System.currentTimeMillis();
            offset = 0L;
        }

        if((o = props.get("PatternGroup")) == null || !(o instanceof List))
            throw(new IllegalArgumentException("PatternGroup is not defined"));

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            aPatternGroup = MonitorUtils.getPatterns("PatternGroup",props,pc);
            xPatternGroup = MonitorUtils.getPatterns("XPatternGroup",props,pc);
        }
        catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed" + e.getMessage()));
        }
        if (aPatternGroup.length <= 0)
            throw(new IllegalArgumentException("PatternGroup is empty"));

        if ((o = props.get("NumberDataFields")) == null ||
            (numberDataFields = Integer.parseInt((String) o)) < 0)
            numberDataFields = 0;

        if ((o = props.get("TimePattern")) != null)
            dateFormat = new SimpleDateFormat((String) o);
        else
            dateFormat = new SimpleDateFormat("yyyyMMddHHmmss.SSS");

        if ((o = props.get("ErrorIgnored")) != null)
            errorIgnored = Integer.parseInt((String) o);
        else
            errorIgnored = 0;

        if ((o = props.get("MaxNumberLogs")) == null ||
            (maxNumberLogs = Integer.parseInt((String) o)) <= 0)
            maxNumberLogs = MAXNUMBERLOGS;

        if ((o = props.get("MaxScannedLogs")) == null ||
            (maxScannedLogs = Integer.parseInt((String) o)) <= 0)
            maxScannedLogs = MAXSCANNEDLOGS;

        if (maxPage == 0 && errorIgnored > 0)
            errorIgnored = 0;

        if ((o = props.get("LogSize")) == null ||
            (logSize = Integer.parseInt((String) o)) < 0)
            logSize = 1;
        if (logSize > 50)
            logSize = 50;

        previousPosition = -1L;
        previousTimestamp = -1L;
        previousOffset = 0L;
        previousNumber = -10;
    }

    public Map<String, Object> generateReport(long currentTime)
        throws IOException {
        int maxLogs = 100 * MAXSCANNEDLOGS;
        int[] rank = new int[2];
        String[] cmdArray;

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

        int i, k, number = 0, totalNumber = 0, n = 0, m = 0;
        long pos = 0L, t = 0L, l, t0, p0;
        List<Object> logBuffer = new ArrayList<Object>();
        StringBuffer strBuf = new StringBuffer();
        String line = null;
        if (proc != null)
            close();

        script = cmdTemplate.copyText();
        if (cmdTemplate.numberOfFields() > 0) {
            update(position, timestamp, report);
            script = cmdTemplate.substitute(script, report);
            report.clear();
        }

        cmdArray = RunCommand.parseCmd(script);
        try {
            proc = Runtime.getRuntime().exec(cmdArray, null);
            in = proc.getInputStream();
        }
        catch (IOException e) {
            throw(new IOException("failed to query Event log: " +
                Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IOException("failed to query new entries: " +
                Event.traceStack(e)));
        }

        rank[0] = 0;
        rank[1] = (int) pos;
        t0 = timestamp;
        p0 = position;
        n = 0;
        while ((line = getLine()) != null) {
            l = line.length();
            if ((t = getTimestamp(line)) >= 0) { // valid log
                if (n > 0) { // matching the patterns
                    String entry = strBuf.toString();
                    strBuf = new StringBuffer();
                    m = 0;
                    if (MonitorUtils.filter(entry, aPatternGroup, pm, true)) {
                        if (numberDataFields > 0) { // number in the last p
                            MatchResult mr = pm.getMatch();
                            if (mr.groups() > 1)
                                m = Integer.parseInt(mr.group(1));
                        }
                        if (!MonitorUtils.filter(entry,xPatternGroup,pm,false)){
                            if (numberDataFields > 0)
                                totalNumber += m;
                            else
                                logBuffer.add(new Object[]{rank, entry});
                            number ++;
                        }
                    }
                }
                if (number >= maxNumberLogs) {
                    n = 0;
                    break;
                }
                k = line.indexOf(' ', 19);
                pos = -1;
                try {
                    pos = Integer.parseInt(line.substring(19, k));
                }
                catch (Exception e) {
                    n = 0;
                    continue;
                }
                if (pos > p0 || t > t0) {
                    updateReference(pos, t, l);
                    n = 1;
                    strBuf.append(line);
                    if (rank[0] == number)
                        rank[1] = (int) pos;
                    else
                        rank = new int[]{number, (int) pos};
                }
                else {
                    n = 0;
                }
            }
            else if (n > 0 && n < logSize) {
                strBuf.append(line);
                if (pos == position && t == timestamp)
                    updateReference(l);
                n ++;
            }
        }
        close();
        timestamp ++; // try to rease the timestamp a bit
        saveReference();
        if (n > 0) {
            String entry = strBuf.toString();
            m = 0;
            if (MonitorUtils.filter(entry, aPatternGroup, pm, true)) {
                if (numberDataFields > 0) { // number in the last pattern
                    MatchResult mr = pm.getMatch();
                    if (mr.groups() > 1)
                        m = Integer.parseInt(mr.group(1));
                }
                if (!MonitorUtils.filter(entry, xPatternGroup, pm, false)) {
                    if (numberDataFields > 0)
                        totalNumber += m;
                    else
                        logBuffer.add(new Object[]{rank, entry});
                    number ++;
                }
            }
        }

        if (numberDataFields <= 0) { // sort logs
            List<Object> list = new ArrayList<Object>();
            totalNumber = number;
            if (number > 0) { 
                Object o;
                int[][] ranks = new int[number][];
                for (i=0; i<number; i++) {
                    o = logBuffer.get(i);
                    ranks[i] = (int[]) ((Object[]) o)[0];
                }
                Arrays.sort(ranks, this);
                for (i=0; i<number; i++) {
                    k = ranks[i][0];
                    o = logBuffer.get(k);
                    list.add(((Object[]) o)[1]);
                }
                logBuffer.clear();
            }
            report.put("LogBuffer", list);
        }
        else
            report.put("LogBuffer", logBuffer);

        report.put("TotalNumber", String.valueOf(totalNumber));

        if ((disableMode > 0 && number == 0) || (disableMode < 0 && number > 0))
            skip = DISABLED;

        return report;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        int numberLogs, level = 0;
        List logBuffer;
        StringBuffer strBuf = new StringBuffer();
        Object o;
        if ((o = latest.get("LogBuffer")) != null && o instanceof List) {
            logBuffer = (List) o;
        }
        else
            logBuffer = new ArrayList();
        numberLogs = logBuffer.size();

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
          case TimeWindows.BLACKEXCEPTION: // exception in blackout
            level = Event.INFO;
            actionCount = 0;
            if (previousStatus != status) { // reset count and adjust step
               exceptionCount = 0;
               if (normalStep > 0)
                   step = normalStep;
            }
          case TimeWindows.EXCEPTION: // exception
            actionCount = 0;
            if (status == TimeWindows.EXCEPTION) {
                level = Event.WARNING;
                if (previousStatus != status) { // reset count and adjust step
                    exceptionCount = 0;
                    if (step > 0)
                        step = 0;
                }
            }
            exceptionCount ++;
            strBuf.append("Exception: ");
            strBuf.append(((Exception) latest.get("Exception")).toString());
            break;
          case TimeWindows.BLACKOUT: // blackout
            level = Event.INFO;
            if (previousStatus != status) {
                if (normalStep > 0)
                    step = normalStep;
                actionCount = 0;
                exceptionCount = 0;
            }
          default: // normal cases
            level = Event.INFO;
            exceptionCount = 0;
            if (status != TimeWindows.BLACKOUT &&
                previousStatus == TimeWindows.BLACKOUT)
                actionCount = 0;
            actionCount ++;
            if (numberLogs > 0) {
                if (previousNumber == 0)
                    actionCount = 1;
                if (status != TimeWindows.BLACKOUT) { // for normal case
                    level = Event.ERR;
                    if (step > 0)
                        step = 0;
                    if (errorIgnored > 0 && numberLogs > errorIgnored &&
                        previousNumber <= errorIgnored) // upgrade at once
                        level = Event.CRIT;
                    else if (errorIgnored < 0 && numberLogs <= -errorIgnored)
                        level = Event.WARNING;
                }
            }
            else if (previousNumber > 0) {
                actionCount = 1;
                if (normalStep > 0)
                    step = normalStep;
            }
            break;
        }
        previousNumber = numberLogs;
        previousStatus = status;

        int count = 0;
        switch (level) {
          case Event.CRIT: // found fatal errors
            break;
          case Event.ERR: // found matching logs that may be ignored
            count = actionCount;
            if (count <= tolerance) { // downgrade to WARNING
                level = Event.WARNING;
                if (count == 1 || count == tolerance) // react only 
                    break;
                return null;
            }
            count -= tolerance;
            if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                count = (count - 1) % repeatPeriod + 1;
            if (count > maxRetry) { // upgraded to CRIT
                if (count > maxRetry + maxPage)
                    return null;
                level = Event.CRIT;
            }
            break;
          case Event.WARNING: // exceptions or downgrades
            if (exceptionTolerance >= 0 && exceptionCount > exceptionTolerance){
                level = Event.ERR;
                count = exceptionCount - exceptionTolerance;
                if (repeatPeriod >= maxRetry && repeatPeriod > 0)
                    count = (count - 1) % repeatPeriod + 1;
                if (count > maxRetry) { // upgraded to CRIT
                    if (count > maxRetry + maxPage)
                        return null;
                    level = Event.CRIT;
                }
            }
            else if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
          default:
            if (actionCount > 1 || exceptionCount > 1)
                return null;
            break;
        }

        if (status < TimeWindows.BLACKOUT) {
            count = exceptionCount;
        }
        else {
            count = actionCount;
            strBuf.append("'" + uri);
            strBuf.append("' has ");
            strBuf.append(numberLogs + " new matching entries");
        }

        Event event = new Event(level, strBuf.toString());
        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        event.setAttribute("actionCount", String.valueOf(count));
        event.setAttribute("status", statusText[status + statusOffset]);
        event.setAttribute("uri", uri);
        event.setAttribute("numberLogs", String.valueOf(numberLogs));
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));
        if (numberLogs > 0)
            event.setAttribute("lastEntry",(String)logBuffer.get(numberLogs-1));
        else
            event.setAttribute("lastEntry", "");

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

    private void update(long n, long t, Map<String, Object> map) {
        int i;
        String str;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(t));
        i = cal.get(Calendar.YEAR);
        map.put("yyyy", String.valueOf(i));
        i = cal.get(Calendar.MONTH) + 1;
        str = (i < 10) ? "0" : "";
        map.put("MM", str + i);
        i = cal.get(Calendar.DAY_OF_MONTH);
        str = (i < 10) ? "0" : "";
        map.put("dd", str + i);
        i = cal.get(Calendar.HOUR_OF_DAY);
        str = (i < 10) ? "0" : "";
        map.put("HH", str + i);
        i = cal.get(Calendar.MINUTE);
        str = (i < 10) ? "0" : "";
        map.put("mm", str + i);
        i = cal.get(Calendar.SECOND);
        str = (i < 10) ? "0" : "";
        map.put("ss", str + i);
        i = (int) (t % 1000);
        if (i < 10)
            str = "00";
        else if (i < 100)
            str = "0";
        else
            str = "";
        map.put("SSS", str + i);
        map.put("RN", String.valueOf(n));
    }

    /**
     * update the current reference info. use it only after found a new log
     * you have to provide all three arguments
     *<br/>
     * position: RecordNumber of the log entry
     * timestamp: TimeGenerated of the log entry
     * offset: length of the new log entry
     */
    private void updateReference(long position, long timestamp, long offset) {
        if (position > this.position || timestamp > this.timestamp) {
            this.timestamp = timestamp;
            this.position = position;
            this.offset = offset;
        }
    }

    private void updateReference(long offset) {
        this.offset += offset;
    }

    /**
     * flush the current reference info to a given file for log tracking
     * if the saveReference is not disabled.
     */
    private void saveReference() throws IOException {
        long pos = 0L;
        int updated = 0;
        if (position != previousPosition) {
            previousPosition = position;
            updated ++;
        }
        if (timestamp != previousTimestamp) {
            previousTimestamp = timestamp;
            updated ++;
        }
        if (offset != previousOffset) {
            previousOffset = offset;
            updated ++;
        }

        if (updated <= 0)
            return;
        try {
            FileWriter out = new FileWriter(referenceFile);
            out.write(position + " " + timestamp + " " + offset + "\n");
            out.flush();
            out.close();
        }
        catch (IOException e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Event.traceStack(e)));
        }
    }

    /**
     * flush the specified reference info to a given file for log tracking
     * no matter whether the savereference is disabled or not.  It is designed
     * for public to control the log tracking only.  So you have to provide
     * all three arguments with care and disable the saveReference to prevent
     * the reference log from overwritten by the object itself.
     */
    private void saveReference(long position, long timestamp, long offset)
        throws IOException {
        try {
            FileWriter out = new FileWriter(referenceFile);
            out.write(position + " " + timestamp + " " + offset + "\n");
            out.flush();
            out.close();
        }
        catch (IOException e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Event.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Event.traceStack(e)));
        }
    }

    private long getTimestamp(String line) {
        Date date = null;
        if (line == null || line.length() == 0)
            return -1L;
        date = dateFormat.parse(line, new ParsePosition(0));

        if (date != null) {
            return date.getTime();
        }
        else
            return -1L;
    }

    /** to return a line as a String read from the input
     * The char of newline, '\n', will be at the end of the line
     * In case of empty input, it returns null
     *<br/>
     * NB. make sure the buffer is reset at close of the InputStream
     * It is not MT-Safe due to buffer, leftover, and startBytes
     */
    private String getLine() throws IOException {
        int bytesRead = 0;
        int i, len = 0;
        if (in == null)
            return null;
        StringBuffer strBuf = new StringBuffer();
        for (i=0; i<leftover; i++) {
            if (buffer[i+startBytes] == 0x0a) {
                strBuf.append(new String(buffer, startBytes, i+1));
                startBytes += i+1;
                leftover -= i+1;
                return strBuf.toString();
            }
        }
        if (leftover > 0)
            strBuf.append(new String(buffer, startBytes, leftover));
        leftover = 0;
        startBytes = 0;

        while (bytesRead >= 0) {
            while ((bytesRead = in.read(buffer, 0, bufferSize)) >= 0) {
                for (i=0; i<bytesRead; i++) {
                    if (buffer[i] == 0x0a) {
                        strBuf.append(new String(buffer, 0, i+1));
                        startBytes = i+1;
                        leftover = bytesRead - startBytes;
                        return strBuf.toString();
                    }
                }
                if (bytesRead > 0) {
                    strBuf.append(new String(buffer, 0, bytesRead));
                    leftover = 0;
                }
            }
            leftover = 0;
            startBytes = 0;
            len = strBuf.length();
            if (len == 0 || len >= MAXLOGLENGTH)
                break;
            try {
                Thread.sleep(5);
            }
            catch (InterruptedException e) {
            }
        }

        if (len > 0) { // line is not complete
            new Event(Event.WARNING, "got a broken line from '" +
                script + "': " + strBuf).send();
            return strBuf.toString();
        }
        else
            return null;
    }

    private void close() {
        leftover = 0;
        startBytes = 0;
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        if (proc != null)
            proc.destroy();
        proc = null;
        in = null;
    }

    public void destroy() {
        super.destroy();
        close();
    }

    protected void finalize() {
        destroy();
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = super.checkpoint();
        chkpt.put("PreviousNumber", String.valueOf(previousNumber));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int aCount, eCount, pNumber, pStatus, sNumber;
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

        if ((o = chkpt.get("PreviousNumber")) != null)
            pNumber = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        actionCount = aCount;
        exceptionCount = eCount;
        previousStatus = pStatus;
        serialNumber = sNumber;
        previousNumber = pNumber;
    }

    public int compare(int[] a, int[] b) {
        int i, j;
        i = a[1];
        j = b[1];
        if (i > j)
            return 1;
        else if (i < j)
            return -1;
        else
            return 0;
    }
}
