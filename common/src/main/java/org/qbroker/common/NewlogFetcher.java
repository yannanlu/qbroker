package org.qbroker.common;

/* NewlogFetcher.java - A log reader to fetch new entries periodically */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.EOFException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Template;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * NewlogFetcher reads the new log entries from a sequential log file
 *<br><br>
 * Use NewlogFetcher to check a sequential log file repetitively.  It only
 * picks up the newly updated log entries.  It guarantees the order and the
 * integrity of the new logs.  In order to guarantee the order and
 * the integrity, each log entry should have a timestamp and the value of
 * the timestamp has to be in non-decreasing order, i.e., the timestamp
 * of the log is always no later than those of the following logs.
 *<br><br>
 * In order to keep tracking the positions, NewlogFetcher relies on a
 * reference file.  The reference file contains the position of the reference
 * log, its timestamp and the offset of the objective log.  A reference
 * log is a log entry with a valid timestamp and is used as the reference to
 * locate the objective log.  It may or may not be in the current log file.
 * The objective log is the first log entry in the current log file that has
 * the timestamp later than the reference timestamp.  The offset is the byte
 * count from the beginning of the objective log to end of the last processed
 * log entry.  Any log after the offset is treated as a new log.  In fact,
 * NewlogFetcher frequently opens the log file and looks for the objective log.
 * It then scans the offset bytes to locate the new logs.
 *<br><br>
 * Here are the rules for the reference file:<br><br>
 * (1) If there is the reference file, use it and trust it.<br>
 * (2) If there is no reference file but AheadTime is defined, use currentTime
 * minus AheadTime as the timestamp of the reference log and set the position
 * of reference log and the offset to 0.<br>
 * (3) If there is no reference file and AheadTime is not defined, use the
 * timestamp of the current log file as the timestamp of the reference log
 * and set the position of reference log and the offset to 0.<br>
 * (4) If there is no current log file, try the timestamp of the old log
 * file.<br>
 * (5) If still no reference, use the default reference (0 currentTime 0).<br>
 * (6) If there is a mismatch to the position of the reference log, trust
 * the timestamp of the reference log and search for the objective log in
 * the current log file.  The objective log is the first log entry with
 * a timestamp later than the reference timestamp.<br>
 * (7) If there is a mismatch to the offset of the objective log, trust the
 * current log first.<br>
 * (8) If it does not find a objective log, reset the offset to 0.<br>
 *<br><br>
 * The reference file is not required.  But if the file is not defined, you
 * will have to set SaveReference to false to disable the update on the
 * state info.
 *<br><br>
 * To speed up sequential IO, it has opened two IO Streams on the same FD.
 * RandomAccessFile is used for random IO and seek. BufferedReader is used for
 * sequential IO.  The speed-up is more than 10 times.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class NewlogFetcher {
    private long timestamp, objTimestamp, position, objPosition, offset;
    private long previousTimestamp, previousPosition, previousOffset;
    private long previousSize = -1;
    private SimpleDateFormat dateFormat, orDateFormat = null;
    private SimpleDateFormat[] timeFormat;
    private Template pathTemplate = null;
    private File logFile, referenceFile;
    private String currentYear;
    private Calendar now;
    private Map<String, Object> oldLogProps;
    private RandomAccessFile log;
    private FileInputStream fin;
    private ParsePosition pp;
    private int maxNumberLogs, maxLogLength, debug, mode, aheadTime = 0;
    boolean foundFirstObj, saveRef, yearRecovery;
    private byte[] buffer;
    private int startBytes, leftover, bufferSize = 4096;
    private long resolution = 1000, scale = 1;
    private Pattern pattern;
    private Perl5Matcher pm = null;
    public List<String> logBuffer;
    public final static int MAXNUMBERLOGS = 40960;
    public final static int MAXLOGLENGTH = 10240;
    public final static long YEARMILLIS = 365*86400000;
    public final static int DEBUG_INIT = 1;
    public final static int DEBUG_LOCATE = 2;
    public final static int DEBUG_BISECT = 4;
    public final static int DEBUG_SCAN = 8;
    public final static int DEBUG_SAVE = 16;
    public final static int DEBUG_LINE = 32;
    public final static int DEBUG_TIME = 64;
    public final static int DEBUG_NEWLOG = 128;
    private final static int MODE_DEFAULT = 1;
    private final static int MODE_WITH_S = 2;
    private final static int MODE_WITH_MS = 3;
    private final static int MODE_WITH_SEC = 4;
    private final static String formatText[] = {"yyyy",
        "yy", "MMM", "MM", "dd", "HH", "mm", "ss"};

    public NewlogFetcher(Map props) {
        Object o;
        String str = new String();
        int n;
        int[] index;
        File oldLogFile = null;

        if ((o = props.get("LogFile")) == null)
            throw(new IllegalArgumentException("LogFile is not defined"));
        str = (String) o;

        // check time dependency on logfile
        pathTemplate = new Template(str, "__[a-zA-Z]+__");
        n = 0;
        index = new int[formatText.length];
        for (int i=0; i<formatText.length; i++) {
            if (pathTemplate.containsField(formatText[i])) {
                index[n] = i;
                n ++;
            }
        }
        if (n > 0) {
            timeFormat = new SimpleDateFormat[n];
            for (int i=0; i<n; i++) {
                timeFormat[i] = new SimpleDateFormat(formatText[index[i]]);
            }
            if ((o = props.get("Resolution")) == null ||
                (resolution = 1000 * Integer.parseInt((String) o)) < 0)
                resolution = 1000;
            str = updatePath(System.currentTimeMillis());
        }
        else
            timeFormat = new SimpleDateFormat[0];

        logFile = new File(str);

        if ((o = props.get("ReferenceFile")) == null)
            throw(new IllegalArgumentException("ReferenceFile is not defined"));
        referenceFile = new File((String) o);

        if ((o = props.get("SaveReference")) != null &&
            "false".equals((String) o))
            saveRef = false;
        else
            saveRef = true;

        if ((o = props.get("OldLogFile")) != null) {
            oldLogProps = new HashMap<String, Object>();
            oldLogFile = new File((String) o);
            oldLogProps.put("LogFile", (String) o);
            oldLogProps.put("ReferenceFile", referenceFile.getPath());
            oldLogProps.put("TimePattern", props.get("TimePattern"));
            oldLogProps.put("OrTimePattern", props.get("OrTimePattern"));
            oldLogProps.put("Debug", "0");
            if (!saveRef)
                oldLogProps.put("SaveReference", "false");
        }
        else
            oldLogProps = null;

        if ((o = props.get("MaxNumberLogs")) == null ||
            (maxNumberLogs = Integer.parseInt((String) o)) <= 0)
            maxNumberLogs = MAXNUMBERLOGS;

        if ((o = props.get("MaxLogLength")) == null ||
            (maxLogLength = Integer.parseInt((String) o)) <= 0)
            maxLogLength = MAXLOGLENGTH;

        if ((o = props.get("AheadTime")) == null ||
            (aheadTime = Integer.parseInt((String) o)) <= 0)
            aheadTime = 0;

        if ((o = props.get("Debug")) != null)
            debug = Integer.parseInt((String) o);
        else
            debug = 0;

        if ((o = props.get("PerlPattern")) != null) {
            try {
                Perl5Compiler pc = new Perl5Compiler();
                pm = new Perl5Matcher();
                pattern = pc.compile((String) o);
            }
            catch (Exception e) {
         throw(new IllegalArgumentException("Pattern failed" + e.getMessage()));
            }
        }
        else
            pm = null;

        pp = new ParsePosition(0);

        mode = MODE_DEFAULT;
        yearRecovery = false;
        now = Calendar.getInstance();
        currentYear = String.valueOf(now.get(Calendar.YEAR));
        if ((o = props.get("TimePattern")) != null) {
            str = (String) o;
            if (str.indexOf("yy") >= 0)
                dateFormat = new SimpleDateFormat(str);
            else if (str.indexOf("MM") >= 0) { // month defined but no yy
                yearRecovery = true;
                dateFormat = new SimpleDateFormat("yyyy" + str);
            }
            else if (str.indexOf("mm") < 0) { // special case for ms or sec
                dateFormat = null;
                if ("SSS".equals(str))
                    mode = MODE_WITH_MS;
                else if ("ss".equals(str))
                    mode = MODE_WITH_S;
                else if ("ss.SSS".equals(str))
                    mode = MODE_WITH_SEC;
                else
                    dateFormat = new SimpleDateFormat(str);
            }
            else // for anything else
                dateFormat = new SimpleDateFormat(str);
        }
        else
            dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

        if ((o = props.get("OrTimePattern")) != null) {
            orDateFormat = new SimpleDateFormat((String) o);
        }

        startBytes = 0;
        leftover = 0;
        buffer = new byte[bufferSize];
        str = "";
        if (referenceFile.exists()) {
            int len = 0;
            try {
                FileInputStream in = new FileInputStream(referenceFile);
                while ((n = in.read(buffer, len, bufferSize - len)) >= 0) {
                    len += n;
                    if (len >= 128)
                        break;
                }
                in.close();
                str = new String(buffer, 0, len);
            }
            catch (IOException e) {
            }
        }
        if (str.length() > 0) {
            n = str.indexOf(" ");
            position = Long.parseLong(str.substring(0, n));
            str = str.substring(n+1, str.length()-1);
            n = str.indexOf(" ");
            timestamp = Long.parseLong(str.substring(0, n));
            offset = Long.parseLong(str.substring(n+1));
        }
        else if (aheadTime > 0) { // use currentTime minus aheadTime
            position = 0L;
            timestamp = System.currentTimeMillis() - 1000 * aheadTime;
            offset = 0L;
        }
        else if (logFile.exists()) {
            // if not get the time, use the mtime of the log file
            position = 0L;
            timestamp = logFile.lastModified();
            offset = 0L;
        }
        else if (oldLogFile != null && oldLogFile.exists()) {
            // if not get the time, use the mtime of the oldLog file
            position = 0L;
            timestamp = oldLogFile.lastModified();
            offset = 0L;
        }
        else { // otherwise, use the current time
            position = 0L;
            timestamp = System.currentTimeMillis();
            offset = 0L;
        }

        objTimestamp = 0L;
        objPosition = 0L;
        foundFirstObj = false;
        logBuffer = new ArrayList<String>();
    }

    /**
     * It locates the first new log entry according to the reference info of
     * the logfile and returns the position of the EOF or new log entry.
     * Reference info contains file position and timestamp of
     * the reference log, and the offset from the objective log.
     */
    public long locateNewlog() throws IOException {
        byte c;
        long t, pos, endOfFile = 0, end, begin, invalidEnd, invalidBegin;
        String line;
        long status = 0L;
        boolean checkBOL = true;
        boolean checkOldLog = false;
        boolean sameFile = false;

        // save the previous reference first
        previousPosition = position;
        previousOffset = offset;
        previousTimestamp = timestamp;
        if (logBuffer != null && logBuffer.size() > 0)
            logBuffer.clear();

        if (timeFormat.length > 0) { // for time-depenendent logfile
            String fileName = updatePath(System.currentTimeMillis());
            if (!logFile.getPath().equals(fileName)) {
                close();
                if (oldLogProps != null)
                    oldLogProps.put("LogFile", logFile.getPath());
                logFile = new File(fileName);
            }
        }
        pos = position;

        /* sometimes, the file may be in transient state.  The size of the
           may be less than the previous size.  The retry has been added
           to workaround the problem.
        */
        for (int i=0; i<5; i++) { // retry if the file is not fully loaded
            close();

            try {
                log = new RandomAccessFile(logFile.getPath(), "r");
                fin = new FileInputStream(log.getFD());
            }
            catch (FileNotFoundException e) {
                throw(new IOException("failed to find " + logFile.getPath() +
                    ": " + Utils.traceStack(e)));
            }
            catch (Exception e) {
                throw(new IOException("failed to open " + logFile.getPath() +
                    ": " + Utils.traceStack(e)));
            }

            endOfFile = log.length();
            if (endOfFile >= previousSize)
                break;
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
        }

        begin = 0L;
        end = endOfFile;
        invalidEnd = end;
        invalidBegin = invalidEnd;
        if ((debug & DEBUG_INIT) > 0)
            System.out.println(Utils.dateFormat(new Date()) +
                " ref: " + position + " " + offset + " " +
                begin + " " + end + ": time=" + timestamp +
                " " + Utils.dateFormat(new Date(timestamp)));

        if (pos >= end || pos <= begin) { // invalid reference position
            if (position > 0L && oldLogProps != null)
                logBuffer = fetchOldLogs(oldLogProps);
            position = begin;
            pos = begin;
            checkBOL = false;
        }

        if (pos > 0)
            log.seek(pos);

        endOfFile = log.length();
        if (endOfFile > pos) { // not at EOF
            if (checkBOL && pos > begin) { // check Begin of Line
                log.seek(pos-1);
                if ((c = log.readByte()) != 0x0a) { // invalid position
                    if ((debug & DEBUG_INIT) > 0)
                        System.out.println("  invalid position: " + pos + " " +
                            begin+ " " + end);
                    if (position > 0L && oldLogProps != null)
                        logBuffer = fetchOldLogs(oldLogProps);
                    line = log.readLine();
                    position = begin;  // invalidate ref position
                    if (pos < invalidBegin)
                        invalidBegin = pos;
                    pos = log.getFilePointer();
                    if (pos >= invalidEnd) {
                        pos = begin;
                        invalidEnd = invalidBegin;
                        if (pos > 0)
                            log.seek(pos);
                    }
                    endOfFile = log.length();
                    if (endOfFile == pos) { // EOF, log file being modified
                        pos = begin;
                        if (pos > 0)
                            log.seek(pos);
                    }
                }
            }
            line = log.readLine();

            if ((t = getTimestamp(line)) >= 0) {
                status = t - timestamp;
                if ((debug & DEBUG_INIT) > 0)
                    System.out.println("  " + status + ": time=" + t + " " +
                        Utils.dateFormat(new Date(t)));
                if (status > 0L) { // too far
                    if (position > 0L && oldLogProps != null)
                        logBuffer = fetchOldLogs(oldLogProps);
                    end = pos;
                    pos = begin;
                    invalidEnd = end;
                    position = begin;  // invalidate ref_position
                    if (!foundFirstObj) {
                        objTimestamp = t;
                    }
                }
                else if (status < 0L) { // not there yet
                    if (position > 0L && oldLogProps != null)
                        logBuffer = fetchOldLogs(oldLogProps);
                    position = pos; // invalidate ref_position
                    pos = log.getFilePointer();
                    begin = pos;
                }
                else { // almost there
                    position = pos; // invalidate ref_position
                    pos = log.getFilePointer();
                    begin = pos;
                    if (foundFirstObj)
                        pos = objPosition;
                }
                invalidBegin = invalidEnd;
            }
            else { // invalid log
                if ((debug & DEBUG_INIT) > 0)
                    System.out.println("  invalid log: " + pos + " " +
                        begin + " " + end);
                if (position > 0L && oldLogProps != null)
                    logBuffer = fetchOldLogs(oldLogProps);
                if (pos < invalidBegin)
                    invalidBegin = pos;
                if (pos == begin) {  // move one step up
                    pos = log.getFilePointer();
                    begin = pos;
                }
                else { // try next log
                    pos = log.getFilePointer();
                }
                position = begin;  // invalidate ref_position
                if (pos >= invalidEnd) {
                    pos = begin;
                    invalidEnd = invalidBegin;
                }
            }
        }
        else { // EOF, log file being modified
            if (position > 0L && oldLogProps != null)
                logBuffer = fetchOldLogs(oldLogProps);
            position = begin;  // invalidate ref_position
            pos = begin;
        }
        checkBOL = false;

        if ((debug & DEBUG_BISECT) > 0)
            System.out.println(" a: " + pos + " " + begin + " " + end);

        while (end - begin > 0L) { // bisection search for obj log
            log.seek(pos);

            endOfFile = log.length();
            if (endOfFile > pos) { // not at EOF
                if (checkBOL && pos > begin) { // check BOL
                    log.seek(pos-1);
                    if ((c = log.readByte()) != 0x0a) { //invalid position
                        if ((debug & DEBUG_BISECT) > 0)
                            System.out.println("  invalid position: " + pos +
                                " " + begin + " " + end);
                        line = log.readLine();
                        if (pos < invalidBegin)
                            invalidBegin = pos;
                        pos = log.getFilePointer();
                        if (pos >= invalidEnd) {
                            invalidEnd = invalidBegin;
                            if (invalidEnd - begin > 1024) {
                                pos = begin + (long) (0.5*(invalidEnd - begin));
                                checkBOL = true;
                            }
                            else {
                                pos = begin;
                                checkBOL = false;
                            }
                            continue;
                        }
                        endOfFile = log.length();
                        if (endOfFile == pos) { // at EOF
                            end = pos;
                            if (end < begin)
                                begin = 0;
                            pos = begin + (long) (0.5 * (end - begin));
                            checkBOL = true;
                            continue;
                        }
                    }
                }
                line = log.readLine();
                if ((t = getTimestamp(line)) >= 0) {
                    status = t - timestamp;
                    if ((debug & DEBUG_BISECT) > 0)
                        System.out.println(" b: " + pos + " " + begin  + " " +
                            end + "\n  " + status + ": time=" + t + " " +
                            Utils.dateFormat(new Date(t)));
                    if (status > 0L) { // too far
                        end = pos;
                        pos = begin + (long) (0.5 *(end - begin));
                        invalidEnd = end;
                        if (!foundFirstObj) {
                            objTimestamp = t;
                        }
                        checkBOL = true;
                    }
                    else if (status < 0L) { // not there yet
                        if (pos > position)
                            position = pos;
                        begin = log.getFilePointer();
                        pos = begin + (long) (0.5 *(end - begin));
                        checkBOL = true;
                    }
                    else { // almost there
                        if (pos > position)
                            position = pos;
                        begin = log.getFilePointer();
                        if (end - begin > MAXLOGLENGTH) {
                            pos = begin + (long) (0.5 * (end - begin));
                            checkBOL = true;
                        }
                        else {
                            pos = begin;
                            checkBOL = false;
                        }
                    }
                    invalidBegin = invalidEnd;
                }
                else {  // invalid log
                    if ((debug & DEBUG_BISECT) > 0)
                        System.out.println("  invalid log: " + pos + " " +
                            " " + begin + " " + end);
                    if (pos < invalidBegin)
                        invalidBegin = pos;
                    if (pos == begin) {  // move one step up
                        pos = log.getFilePointer();
                        begin = pos;
                    }
                    else { // try next log
                        pos = log.getFilePointer();
                    }
                    if (pos >= invalidEnd) {
                        invalidEnd = invalidBegin;
                        if (invalidEnd <= begin) {
                            begin = end;
                            break;
                        }
                        pos = begin;
                    }
                    checkBOL = false;
                }
            }
            else {
                end = pos;
                endOfFile = end;
                if (end < begin)
                    begin = 0;
                pos = begin + (long) (0.5 * (end - begin));
                checkBOL = true;
            }
        }

        // there are only two cases to reach here: found obj_log or reached EOF
        // since begin = end, begin is pointing to either obj_log or EOF

        pos = begin;
        objPosition = begin;
        log.seek(pos);
        if (begin >= endOfFile && !foundFirstObj) { // no obj_log found
            offset = 0;
            previousSize = pos;
            if ((debug & DEBUG_LOCATE) > 0)
                System.out.println(Utils.dateFormat(new Date()) +
                    " LOCATE: " + pos + " " + objPosition + " " + position +
                    " " + offset + "\n  " + status + ": time=" + objTimestamp +
                    " " + Utils.dateFormat(new Date(objTimestamp)));
            return pos;
        }
        else {
            foundFirstObj = true;
        }

        end = (begin + offset > endOfFile) ? endOfFile : begin + offset;

        if ((debug & DEBUG_SCAN) > 0)
            System.out.println(" c: " + offset + " " + begin + " " + end);

        if (offset == 0) { //  the obj_log is a new log
            if ((debug & DEBUG_LOCATE) > 0)
                System.out.println(Utils.dateFormat(new Date()) +
                    " Locate: " + pos + " " + objPosition + " " + position +
                    " " + offset + "\n  " + status + ": time=" + objTimestamp +
                    " " + Utils.dateFormat(new Date(objTimestamp)));
            previousSize = pos;
            return pos;
        }

        while ((line = log.readLine()) != null) {
            if ((t = getTimestamp(line)) >= 0) {
                status = t - objTimestamp;
                if ((debug & DEBUG_SCAN) > 0)
                    System.out.println(" d: " + pos + " " + begin + " " +
                        end + "\n  " + status + ": time=" + t + " " +
                        Utils.dateFormat(new Date(t)));
                if (status <= 0L) {
                    if (begin < end) {
                        pos = log.getFilePointer();
                        begin = pos;
                        continue;
                    }
                    updateReference(position, t, begin - end);
                    saveReference();
                }
            }
            else {  // invalid log
                pos = log.getFilePointer();
                begin = pos;
                continue;
            }
            log.seek(pos);
            break;
        }

        // there are only two cases to reach here: found a new log or EOF
        offset = pos - objPosition;
        previousSize = pos;

        if ((debug & DEBUG_LOCATE) > 0)
            System.out.println(Utils.dateFormat(new Date()) +
                " locate: " + pos + " " + objPosition + " " +
                position + " " + offset + "\n  " + status + ": time=" +
                objTimestamp + " " + Utils.dateFormat(new Date(objTimestamp)));

        return pos;
    }

    /**
     * It opens the log file and locates the first new entry according to
     * the reference info, reads the all log entries from the location line
     * by line up to the end of the logfile or until the buffer is full.
     * It stores them in the logBuffer.  It closes the log file and
     * returns the List of new logs.
     */
    public List<String> getNewlogs() throws IOException {
        long pos, t, l;
        int number = 0;
        StringBuffer strBuf = new StringBuffer();
        String line;
        pos = locateNewlog();

        if (logBuffer == null)
            logBuffer = new ArrayList<String>();
        while ((line = getLine()) != null) {
            l = line.length();
            if ((t = getTimestamp(line)) >= 0) { // valid log
                if ((debug & DEBUG_NEWLOG) > 0)
                    System.out.println(number + ": " +
                        line.substring(0, 48) + "!" + t);

                if (strBuf.length() > 0) {
                    if (number > 0)
                        logBuffer.add(strBuf.toString());
                    strBuf = new StringBuffer();
                }
                if (number++ >= maxNumberLogs) {
                    break;
                }
                updateReference(pos, t, l);
            }
            else {
                 offset += l;
            }
            strBuf.append(line);
            pos += l;
        }
        if (strBuf.length() > 0 && number > 0)
            logBuffer.add(strBuf.toString());
        saveReference();
        close();

        return logBuffer;
    }

    /**
     * It sets the current reference info.  It is required that
     * you have to provide all three arguments
     */
    public void setReference (long position, long timestamp, long offset) {
        this.timestamp = timestamp;
        this.position = position;
        this.offset = offset;
        objTimestamp = 0L;
        objPosition = 0L;
        foundFirstObj = false;
    }

    /**
     * It updates the current reference info. Use it only after you have
     * found a new log.  You have to provide all three arguments
     *<br><br>
     * position: beginning of the new log entry
     * timestamp: timestamp of the new log entry
     * offset: length of the new log entry
     */
    public void updateReference(long position, long timestamp, long offset) {
        if (this.offset == 0) { // the new log is the obj log
            objTimestamp = timestamp;
            objPosition = position;
            this.offset = offset;
        }
        else if (timestamp > objTimestamp) {
            this.position = objPosition;
            this.offset = offset;
            this.timestamp = objTimestamp;
            objTimestamp = timestamp;
            objPosition = position;
        }
        else {  // same timestamp or log is not monotonously increasing
            this.offset += offset;
        }
    }

    public void updateReference(long offset) {
        this.offset += offset;
    }

    /**
     * It flushes the current reference info to a given file for log tracking
     * only if the SaveReference is enabled.
     */
    public void saveReference() throws IOException {
        long pos = 0L;
        if (saveRef) {
            int updated = 0;
            if (position != previousPosition)
                updated ++;
            if (offset != previousOffset)
                updated ++;
            if (timestamp != previousTimestamp)
                updated ++;

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
                    referenceFile.getName() + "': " + Utils.traceStack(e)));
            }
            catch (Exception e) {
                throw(new IOException("failed to save reference to '" +
                    referenceFile.getName() + "': " + Utils.traceStack(e)));
            }
            if ((debug & DEBUG_SAVE) > 0)
                System.out.println(Utils.dateFormat(new Date()) +
                    " save: " + position + " " + timestamp +
                    " " + offset + " " + objPosition + " " + objTimestamp +
                    ": " + Utils.dateFormat(new Date(timestamp)));
        }
    }

    /**
     * It flushes the specified reference info to the reference file for log
     * tracking no matter the SaveReference is enabled or not.  It is designed
     * for public to control the log tracking only.  So you have to provide
     * all three arguments with care and disable the SaveReference to prevent
     * the reference log from overwritten by NewlogFetcher itself.  Please also
     * make sure the reference file is defined.
     */
    public void saveReference(long position, long timestamp, long offset)
        throws IOException {
        try {
            FileWriter out = new FileWriter(referenceFile);
            out.write(position + " " + timestamp + " " + offset + "\n");
            out.flush();
            out.close();
        }
        catch (IOException e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Utils.traceStack(e)));
        }
        catch (Exception e) {
            throw(new IOException("failed to save reference to '" +
                referenceFile.getName() + "': " + Utils.traceStack(e)));
        }
        if ((debug & DEBUG_SAVE) > 0)
            System.out.println(Utils.dateFormat(new Date()) +
                " Save: " + position + " " + timestamp +
                " " + offset + " " + objPosition + " " + objTimestamp +
                ": " + Utils.dateFormat(new Date(timestamp)));
    }

    private long getTimestampMS(String line) {
        long t;
        int i;

        if (pm != null) { // if PerlPattern defined
            if (!pm.contains(line, pattern))
                return -1L;
            line = pm.getMatch().group(1);
            if (line == null || line.length() == 0)
                return -1L;
        }
        else if ((i = line.indexOf(" ")) > 0)
            line = line.substring(0, i);

        try {
            switch (mode) {
              case MODE_WITH_S:
                t = Long.parseLong(line) * 1000L;
                break;
              case MODE_WITH_SEC:
                t = (long) (Double.parseDouble(line) * 1000.0 + 0.00001);
                break;
              default:
                t = Long.parseLong(line);
            }
        }
        catch (Exception e) {
            t = -1L;
        }

        return t;
    }

    /**
     * returns the timestamp in msec for the log entry or -1L otherwise
     */
    public long getTimestamp(String line) {
        Date date = null;
        if (line == null || line.length() == 0)
            return -1L;

        if (dateFormat == null) // special case for millis or seconds
            return getTimestampMS(line);

        if (pm != null) { // if PerlPattern defined
            if (!pm.contains(line, pattern))
                return -1L;
            line = pm.getMatch().group(1);
            if (line == null || line.length() == 0)
                return -1L;
        }

        if (yearRecovery) {
            long t, ct;
            int cy, cm;
            pp.setIndex(0);
            date = dateFormat.parse(currentYear + line, pp);
            if (date != null)
                t = date.getTime();
            else
                return -1L;
            ct = System.currentTimeMillis();
            if (t - ct > 60000) { // adjust time after rollover
                now.setTime(new Date(ct));
                cy = now.get(Calendar.YEAR);
                cm = now.get(Calendar.MONTH) + 1;
                if (cm == 1 && cy == Integer.parseInt(currentYear)) {
                    pp.setIndex(0);
                    date = dateFormat.parse(String.valueOf(cy-1) + line, pp);
                }
            }
            else if (ct - t >= YEARMILLIS - 86400000) { // rollover on year
                now.setTime(new Date(ct));
                cy = now.get(Calendar.YEAR);
                cm = now.get(Calendar.MONTH) + 1;
                now.setTime(new Date(t));
                cm -= now.get(Calendar.MONTH) + 1;
                if (cm >= 0 && cy - 1 == Integer.parseInt(currentYear)) {
                    pp.setIndex(0);
                    currentYear = String.valueOf(cy);
                    date = dateFormat.parse(currentYear+line, pp);
                }
            }
        }
        else if (orDateFormat != null) {
            pp.setIndex(0);
            date = dateFormat.parse(line, pp);
            if (date == null) {
                pp.setIndex(0);
                date = orDateFormat.parse(line, pp);
            }
        }
        else {
            pp.setIndex(0);
            date = dateFormat.parse(line, pp);
        }

        if (date != null)
            return date.getTime();
        else
            return -1L;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getPosition() {
        return position;
    }

    public long getOffset() {
        return offset;
    }

    public String getPath() {
        return logFile.getPath();
    }

    /**
     * It returns a line as a String read from the input.
     * The char of newline, '\n', will be at the end of the line.
     * In case of empty input, it returns null.
     *<br>
     * NB. make sure the buffer is reset at close of the InputStream
     * It is not MT-Safe due to buffer, leftover, and startBytes
     */
    public String getLine() throws IOException {
        int bytesRead = 0;
        int i, len = 0;
        if (fin == null)
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

        for (int k=0; k<100; k++) {
            while ((bytesRead = fin.read(buffer, 0, bufferSize)) >= 0) {
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
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
            }
        }

        if (len > 0) { // line is not complete
            GenericLogger.log(Event.WARNING, "got a broken line from '" +
                logFile.getPath() + "': " + strBuf);
            return strBuf.toString();
        }
        else
            return null;
    }

    public void seek(long pos) throws IOException {
        if (log != null)
            log.seek(pos);
        else
            throw(new IOException("failed to seek: file pointer is null"));
    }

    public void close() {
        // reset buffer first
        leftover = 0;
        startBytes = 0;

        if (logBuffer != null)
            logBuffer.clear();

        if (fin != null) try {
            fin.close();
        }
        catch (Exception e) {
        }

        if (log != null) try {
            log.close();
        }
        catch (Exception e) {
        }
    }

    private String updatePath(long currentTime) {
        int i;
        String key, value, path;
        if (pathTemplate == null)
            return null;
        path = pathTemplate.copyText();
        Date now = new Date(currentTime - (currentTime % resolution));
        for (i=0; i<timeFormat.length; i++) {
            key = timeFormat[i].toPattern();
            value = timeFormat[i].format(now);
            path = pathTemplate.substitute(key, value, path);
        }
        return path;
    }

    private List<String> fetchOldLogs(Map props) {
        File oldLogFile = new File((String) props.get("LogFile"));
        if (oldLogFile != null && oldLogFile.exists()) {
            NewlogFetcher oldLog;
            try {
                oldLog = new NewlogFetcher(props);
                oldLog.setReference(position, timestamp, offset);
                oldLog.getNewlogs();
            }
            catch (Exception e) {
                GenericLogger.log(Event.WARNING,
                "failed to fetch new entries from '" +
                oldLogFile.getPath() + "' at (" + position + " " + timestamp +
                " " + offset + "): " + Utils.traceStack(e));
                return new ArrayList<String>();
            }
            setReference(oldLog.getPosition(), oldLog.getTimestamp(),
                oldLog.getOffset());
            return oldLog.logBuffer;
        }
        else {
            return new ArrayList<String>();
        }
    }
}
