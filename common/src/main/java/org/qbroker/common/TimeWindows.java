package org.qbroker.common;

/* TimeWindows.java -  time slots in which something is expected to occur */

import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;

/**
 * TimeWindows - A set of time slots in which something is expected to occur
 * and a method to check if it has occurred or is late.
 *<br/><br/>
 * A TimeWindows contains a set of time windows in which something is expected
 * to occur at least once, and other rules.  Its method, check(), requires two
 * time stamps, the current time of testing and the latest time to be checked.
 * It compares the two time stamps with the preconfigured time windows and
 * the other timing rules.  It returns an integer which gives the status of
 * the timing event.  If the current time is outside any of the given time
 * windows but within its lifetime, BLACKOUT is returned indicating it is
 * outside any active time windows. If the current time is beyond the lifetime,
 * DISABLED is returned to indicate that all the time windows have been closed.
 * Within any active time windows, it returns NORMAL if nothing is wrong or
 * the timing event occurs.  Otherwise it returns a positive integer depending
 * upon what you are interested in.
 *<br/><br/>
 * If the user is expecting something occurred in a recent time range before
 * the test, the threshold should contain only one negative value defining how
 * recent in seconds.  If the occurrence takes place as expected,
 * the method, check(), returns OCCURRED.  Otherwise, it returns NORMAL
 * indicating nothing happened or BLACKOUT indicating outside of time window.
 *<br/><br/>
 * If the user is concerned about something's lateness regarding to the previous
 * occurrence, he/she has to specify the allowable lateness time spans, or
 * late-thresholds.  If the first late-threshold is reached or passed,
 * the method, check(), returns SLATE indicating somewhat late.  If the next
 * threshold is reached or passed, it returns ALATE indicating alarmingly late.
 * The next level of lateness is ELATE, indicating extremely late.  User can
 * specify multi-level late thresholds.  If there is no late, it returns NORMAL.
 *<br/><br/>
 * The TimeWindows is just a set of rules and a check method for something
 * expected to occur sometime somehow.  It does not care how to obtain the
 * time stamp of the latest occurrence.  It has no action to handle the
 * consequence either.  It can be used as a scheduler as long as the time
 * resolution is acceptable.  It also behaves in dealing with the DST time line.
 * In order to know what kind of thresholds defined for the TimeWindows, you
 * can check the integer returned by getThresholdLength().
 *<br/>
 * @author yannanlu@yahoo.com
 */

@SuppressWarnings("unchecked")
public class TimeWindows {
    public final static int EXCEPTION = -4;
    public final static int BLACKEXCEPTION = -3;
    public final static int DISABLED = -2;
    public final static int BLACKOUT = -1;
    public final static int NORMAL = 0;
    public final static int OCCURRED = 1;
    public final static int SLATE = 2;
    public final static int ALATE = 3;
    public final static int ELATE = 4;
    public final static int EXPIRED = 5;
    public final static String[] STATUS_TEXT = {
        "EXCEPTION",
        "BLACKEXCEPTION",
        "DISABLED",
        "BLACKOUT",
        "NORMAL",
        "OCCURRED",
        "SLATE",
        "ALATE",
        "ELATE",
        "EXPIRED"
    };
    public final static int weekDayMask[] = {1,2,4,8,16,32,64};
    public final static int monthDayMask[] = new int[31];
    public final static int monthMask[] =
        {1,2,4,8,16,32,64,128,256,512,1024,2048};
    public int defaultStatus;
    private Map blackout;
    private boolean activeToday;
    private boolean activeYesterday;
    private int activeMonth;
    private int activeMonthDay;
    private int activeWeekDay;
    private long baseTime, startTime, stopTime, previousTime;
    private int intervals[][];
    private int blackoutIntervals[][];
    private int thresholds[][];
    private int tcID0 = -1, tcID1 = -1, tcYear, minThresholdLength = 3;
    private long tcStart, tcEnd, tcOffset;
    private String tcKey0 = null, tcKey1 = null;
    private Calendar today;
    private Calendar yesterday;
    private static long dstStart, dstEnd, dstOffset;
    private static int dstYear = -1;
    private static TimeZone tz = TimeZone.getDefault();
    private final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zz");

    public TimeWindows(Map props) {
        Object o;
        List l;
        int i, m, n;
        activeToday = false;
        activeYesterday = false;
        activeMonth = 0;
        activeMonthDay = 0;
        activeWeekDay = 0;
        baseTime = 0L;
        blackout = new HashMap();
        intervals = new int[0][2];
        blackoutIntervals = new int[0][2];
        thresholds = new int[0][];
        defaultStatus = BLACKOUT;
        startTime = Long.MIN_VALUE;
        stopTime = Long.MAX_VALUE;
        previousTime = Long.MAX_VALUE;

        if ((o = props.get("StartTime")) != null && o instanceof String) {
           String str = (String) o;
           startTime = dateFormat.parse(str, new ParsePosition(0)).getTime();
        }

        if ((o = props.get("StopTime")) != null && o instanceof String) {
           String str = (String) o;
           stopTime = dateFormat.parse(str, new ParsePosition(0)).getTime();
        }

        if ((o = props.get("TimeWindow")) != null && o instanceof List) {
            String str;
            String[] tmp = new String[5];
            Map h;
            int k, t, j;
            l = (List) o;
            int[][] tw = new int[l.size()][2];
            int[][] th = new int[l.size()][];
            n = 0;
            for (i=0; i<l.size(); i++) {
                h = (Map) l.get(i);
                str = (String) h.get("Interval");
                if (str == null)
                    continue;
                tw[n] = parseInterval(str);
                str = (String) h.get("Threshold");
                th[n] = parseThreshold(str);
                n++;
            }

            intervals = new int[n][2];
            thresholds = new int[n][];
            for (i=0; i<n; i++) {
                intervals[i][0] = tw[i][0];
                intervals[i][1] = tw[i][1];
                thresholds[i] = th[i];
                if (th[i] != null && th[i].length < minThresholdLength)
                    minThresholdLength = th[i].length;
            }
        }
        if (intervals.length == 0) {
            intervals = new int[1][2];
            intervals[0][0] = 0;
            intervals[0][1] = 86400000;
            thresholds[0] = new int[0];
            minThresholdLength = 0;
        }

        today = Calendar.getInstance(tz);
        tcYear = today.get(Calendar.YEAR);
        if (tz.useDaylightTime()) {
            long[] dst;
            dst = getDSTInfo(tcYear);
            tcStart = dst[0];
            tcEnd = dst[1];
            tcOffset = dst[2];
        }
        else {
            tcStart = Long.MAX_VALUE;
            tcEnd = Long.MIN_VALUE;
            tcOffset = 0L;
        }
        if ((o = props.get("Blackout")) != null && o instanceof List) {
            int k;
            l = (List) o;
            for (i=0; i<l.size(); i++) {
                int[] tw = new int[2];
                String str = (String) l.get(i);
                m = str.indexOf("/");
                if ("timechange0".equals(str.toLowerCase())) {
                    if (tcOffset == 0L)
                        continue;
                    today.setTime(new Date(tcStart));
                    tw[0] = 1000*((today.get(Calendar.HOUR_OF_DAY) * 60 +
                        today.get(Calendar.MINUTE)) * 60 +
                        today.get(Calendar.SECOND));
                    tw[1] = tw[0] + (int) tcOffset;
                    tcKey0 = (today.get(Calendar.MONTH) + 1) + "/" +
                        today.get(Calendar.DAY_OF_MONTH);
                    str = tcKey0;
                }
                else if ("timechange1".equals(str.toLowerCase())) {
                    if (tcOffset == 0L)
                        continue;
                    today.setTime(new Date(tcEnd));
                    tw[0] = 1000*((today.get(Calendar.HOUR_OF_DAY) * 60 +
                        today.get(Calendar.MINUTE)) * 60 +
                        today.get(Calendar.SECOND)) + (int) tcOffset; // shifted
                    tw[1] = tw[0] + (int) tcOffset;
                    tcKey1 = (today.get(Calendar.MONTH) + 1) + "/" +
                        today.get(Calendar.DAY_OF_MONTH);
                    str = tcKey1;
                }
                else if (m <= 0) { // weekday
                    m = str.indexOf(" ");
                    if (m > 0) { // interval defined
                        k = Integer.parseInt(str.substring(0, m));
                        tw = parseInterval(str.substring(m+1));
                    }
                    else { // default interval
                        k = Integer.parseInt(str);
                        tw[0] = 0;
                        tw[1] = 86400000;
                    }
                    if (k < 0 || k > 7)
                        continue;
                    str = String.valueOf((k + 6) % 7 + 1);
                }
                else { // monthday
                    k = Integer.parseInt(str.substring(0, m));
                    if (k <= 0 || k > 12)
                        continue;
                    str = str.substring(m+1);
                    m = str.indexOf(" ");
                    if (m > 0) { // interval defined
                        tw = parseInterval(str.substring(m+1));
                        m = Integer.parseInt(str.substring(0, m));
                        if (m <= 0 || m > 31)
                            continue;
                    }
                    else { // default interval
                        m = Integer.parseInt(str);
                        if (m <= 0 || m > 31)
                            continue;
                        tw[0] = 0;
                        tw[1] = 86400000;
                    }
                    str = k + "/" + m;
                }
                List list;
                if (blackout.containsKey(str)) {
                    list = (List) blackout.get(str);
                }
                else {
                    list = new ArrayList();
                    blackout.put(str, list);
                }
                list.add(tw);
                if (tcKey0 != null && tcID0 < 0)
                    tcID0 = list.size() - 1;
                else if (tcKey1 != null && tcID1 < 0)
                    tcID1 = list.size() - 1;
            }

            Iterator iter = blackout.keySet().iterator();
            while (iter.hasNext()) {
                String key = (String) iter.next();
                Object value = blackout.get(key);
                if (!(value instanceof List))
                    continue;
                n = ((List) value).size();
                int[][] tw = new int[n][2];
                for (i=0; i<n; i++)
                    tw[i] = (int[]) ((List) value).get(i);
                blackout.put(key, tw);
            }
        }

        m = 1;
        for (i=0; i<31; i++) {
            monthDayMask[i] = m;
            m += m;
        }

        if ((o = props.get("Month")) != null && o instanceof List) {
            l = (List) o;
            n = l.size();
            for(i=0; i<n; i++) {
                m = Integer.parseInt((String) l.get(i));
                if(m > 0 && m <= 12)
                    activeMonth |= monthMask[m-1];
            }
        }
        else {
            for(i=0; i<12; i++)
                activeMonth |= monthMask[i];
        }

        if ((o = props.get("MonthDay")) != null && o instanceof List) {
            l = (List) o;
            n = l.size();
            for(i=0; i<n; i++) {
                m = Integer.parseInt((String) l.get(i));
                if(m > 0 && m <= 31)
                    activeMonthDay |= monthDayMask[m-1];
            }
        }
        else {
            for(i=0; i<31; i++)
                activeMonthDay |= monthDayMask[i];
        }

        if ((o = props.get("WeekDay")) != null && o instanceof List) {
            l = (List) o;
            n = l.size();
            for (i=0; i<n; i++) {
                m = Integer.parseInt((String) l.get(i));
                if (m >= 0 && m <= 7)
                    activeWeekDay |= weekDayMask[m%7];
            }
        }
        else {
            for(i=0; i<7; i++)
                activeWeekDay |= weekDayMask[i];
        }

        long t = System.currentTimeMillis();
        yesterday = Calendar.getInstance();
        today.setTime(new Date(t));
        baseTime = t - (t%1000) - 1000*((today.get(Calendar.HOUR_OF_DAY) * 60 +
            today.get(Calendar.MINUTE)) * 60 + today.get(Calendar.SECOND));
        today.setTime(new Date(baseTime));
        yesterday.setTime(new Date(baseTime - 86400000L));
        updateToday(t);
    }

    private void updateToday(long currentTime) {
        if (currentTime < 0L)
            currentTime = System.currentTimeMillis();

        if (currentTime - baseTime >= 86400000L &&
            currentTime - baseTime < 172800000L) {
            activeYesterday = activeToday;
            yesterday.setTime(new Date(baseTime));
            baseTime += 86400000L;
        }
        else {
            today.setTime(new Date(currentTime));
            baseTime = currentTime - (currentTime%1000) -
                1000*((today.get(Calendar.HOUR_OF_DAY) * 60 +
                today.get(Calendar.MINUTE)) * 60 + today.get(Calendar.SECOND));
            yesterday.setTime(new Date(baseTime - 86400000L));
            activeYesterday = isActive(yesterday);
        }
        today.setTime(new Date(baseTime));
        if (tcYear != today.get(Calendar.YEAR)) { // update tc blackout
            tcYear = today.get(Calendar.YEAR);
            if (tz.useDaylightTime()) {
                long[] dst;
                dst = getDSTInfo(tcYear);  
                tcStart = dst[0];
                tcEnd = dst[1];
                tcOffset = dst[2];
            }
            else {
                tcStart = Long.MAX_VALUE;
                tcEnd = Long.MIN_VALUE;
                tcOffset = 0L;
            }
            if (tcKey0 != null && tcID0 >= 0)
                updateTimechange(tcID0, tcKey0, tcStart, tcOffset);
            if (tcKey1 != null && tcID1 >= 0) // shifted timeline 
                updateTimechange(tcID1, tcKey1, tcEnd + tcOffset, tcOffset);
        }
        activeToday = isActive(today);
    }

    public boolean isActive(Calendar day) {
        int month = day.get(Calendar.MONTH);
        int monthDay = day.get(Calendar.DAY_OF_MONTH) - 1;
        int weekDay = day.get(Calendar.DAY_OF_WEEK) - 1;

        if ((activeMonth & monthMask[month%12]) == 0 ||
            (activeMonthDay & monthDayMask[monthDay%31]) == 0 ||
            (activeWeekDay & weekDayMask[weekDay%7]) == 0)
            return false;

        if (blackout.size() > 0) { // check blackout
            String mKey = (month + 1) + "/" + (monthDay + 1);
            String wKey = String.valueOf((weekDay + 6) % 7 + 1);
            if (blackout.containsKey(mKey))
                blackoutIntervals = (int[][]) blackout.get(mKey);
            else if (blackout.containsKey(wKey))
                blackoutIntervals = (int[][]) blackout.get(wKey);
            else
                blackoutIntervals = new int[0][2];
        }
        else {
            blackoutIntervals = new int[0][2];
        }

        return true;
    }

    /**
     * It returns 0 for no threshold defined, 1 for occurred thresholds or
     * 2 and above for lateness thresholds.
     */
    public int getThresholdLength() {
        return minThresholdLength;
    }

    /**
     * It returns copy of the active threshold in millisec for the given time
     * or null if it is blackout or expired.
     */
    public int[] getCurrentThreshold(long currentTime) {
        int i, j, n;
        int[] threshold = null, th;
        long beginTime, endTime, begin, end;
        if (currentTime < baseTime || currentTime - baseTime >= 86400000L)
            updateToday(currentTime);

        if (tcOffset != 0L) { // adjust baseTime if it is crossing DST timeline
            if (previousTime > currentTime) // reset previousTime
                previousTime = currentTime;
            if (currentTime >= tcStart && previousTime < tcStart) // enter DST
                baseTime -= tcOffset;
            else if (currentTime >= tcEnd && previousTime < tcEnd) { // exit DST
                baseTime += tcOffset;
                if (tcKey1 != null && tcID1 >= 0 &&
                    blackout.containsKey(tcKey1)) { // adjust blackout DST time
                    int[][] tw = (int[][]) blackout.get(tcKey1);
                    if (tw.length > tcID1) { // adjust DST windows
                        tw[tcID1][0] -=  tcOffset;
                        tw[tcID1][1] -=  tcOffset;
                    }
                }
            }
            previousTime = currentTime;
        }

        if (currentTime < startTime)
            return null;
        else if (currentTime >= stopTime)
            return null;

        for (i=0; i<intervals.length; i++) {
            beginTime = baseTime + (long) intervals[i][0];
            endTime = baseTime + (long) intervals[i][1];
            if (activeToday) { // today is an active day, check the timing
                if (beginTime > endTime) { // time window across midnight
                    if (activeYesterday) { // yesterday is an active day
                        begin = beginTime;
                        end = endTime;
                        if (currentTime <= end)
                            begin -= 86400000L;
                        else
                            end += 86400000L;
                    }
                    else { // today is the beginning day
                        begin = beginTime;
                        end = endTime + 86400000L;
                    }
                }
                else { // regular time window
                    begin = beginTime;
                    end = endTime;
                }
            }
            else if (activeYesterday) { // yesterday is an active day
                if (beginTime > endTime) { // time window across midnight
                    begin = beginTime - 86400000L;
                    end = endTime;
                }
                else { // regular time window but black out
                    return null;
                }
            }
            else { // black out
                return null;
            }

            // find the first active time_window
            if (currentTime >= begin && currentTime < end) { // found timewindow
                threshold = thresholds[i];
                break;
            }
        }

        if (threshold == null)
            return null;

        for (i=0; i<blackoutIntervals.length; i++) { // check blackout
            beginTime = baseTime + (long) blackoutIntervals[i][0];
            endTime = baseTime + (long) blackoutIntervals[i][1];
            if (currentTime >= beginTime && currentTime < endTime) {
                return null;
            }
        }

        n = threshold.length;
        th = new int[n];

        for (i=0; i<n; i++)
            th[i] = threshold[i];

        return th;
    }

    public int check(long currentTime, long sampleTime) {
        int status = defaultStatus;
        int i, j;
        int[] threshold;
        long beginTime, endTime, begin, end;
        if (currentTime < baseTime || currentTime - baseTime >= 86400000L)
            updateToday(currentTime);

        if (tcOffset != 0L) { // adjust baseTime if it is crossing DST timeline
            if (previousTime > currentTime) // reset previousTime
                previousTime = currentTime;
            if (currentTime >= tcStart && previousTime < tcStart) // enter DST
                baseTime -= tcOffset;
            else if (currentTime >= tcEnd && previousTime < tcEnd) { // exit DST
                baseTime += tcOffset;
                if (tcKey1 != null && tcID1 >= 0 &&
                    blackout.containsKey(tcKey1)) { // adjust blackout DST time
                    int[][] tw = (int[][]) blackout.get(tcKey1);
                    if (tw.length > tcID1) { // adjust DST windows
                        tw[tcID1][0] -=  tcOffset;
                        tw[tcID1][1] -=  tcOffset;
                    }
                }
            }
            previousTime = currentTime;
        }

        if (currentTime < startTime)
            return BLACKOUT;
        else if (currentTime >= stopTime)
            return EXPIRED;

        for (i=0; i<intervals.length; i++) {
            beginTime = baseTime + (long) intervals[i][0];
            endTime = baseTime + (long) intervals[i][1];
            threshold = thresholds[i];
            if (activeToday) { // today is an active day, check the timing
                if (beginTime > endTime) { // time window across midnight
                    if (activeYesterday) { // yesterday is an active day
                        begin = beginTime;
                        end = endTime;
                        if (currentTime <= end)
                            begin -= 86400000L;
                        else
                            end += 86400000L;
                    }
                    else { // today is the beginning day
                        begin = beginTime;
                        end = endTime + 86400000L;
                    }
                }
                else { // regular time window
                    begin = beginTime;
                    end = endTime;
                }
            }
            else if (activeYesterday) { // yesterday is an active day
                if (beginTime > endTime) { // time window across midnight
                    begin = beginTime - 86400000L;
                    end = endTime;
                }
                else { // regular time window but black out
                    break;
                }
            }
            else { // black out
                break;
            }

            if (currentTime >= begin && currentTime < end) { // on-duty check
                status = NORMAL;
                if (threshold.length > 0) {
                    if (threshold[0] >= 0) { // check if it is late
                        /* we should never modify sampleTime */
                        //if (sampleTime < begin)
                        //    sampleTime = begin;
                        for (j=threshold.length-1; j>=0; j--) {
                            if (currentTime - sampleTime >= (long)threshold[j]){
                                status = j + SLATE;
                                break;
                            }
                        }
                    }
                    else if (currentTime - sampleTime < (long) (-threshold[0])){
                        status = OCCURRED; // check if it is recent
                    }
                }
                else if (sampleTime >= begin) { // check if it is in time
                    status = OCCURRED;
                }
            }
            // check the first active time_window only
            if (status > defaultStatus)
                break;
        }

        for (i=0; i<blackoutIntervals.length; i++) { // check blackout
            beginTime = baseTime + (long) blackoutIntervals[i][0];
            endTime = baseTime + (long) blackoutIntervals[i][1];
            if (currentTime >= beginTime && currentTime < endTime) {
                status = BLACKOUT;
                break;
            }
        }

        return status;
    }

    public static int parseTimeString(String timeStr) {
        String str;
        int i, t;
        if (timeStr == null || timeStr.length() == 0)
            return -1;
        str = timeStr;
        i = str.indexOf(":");
        if (i <= 0) {// only second field specified
            t = Integer.parseInt(str.trim());
        }
        else { // minute field is not empty
            t = 60 * Integer.parseInt(str.substring(0, i).trim());
            str = str.substring(i+1);
            i = str.indexOf(":");
            if (i <= 0) { // hour field empty
                t += Integer.parseInt(str.trim());
            }
            else { // hour field is not empty
                t = 60 * (t + Integer.parseInt(str.substring(0, i).trim())) +
                    Integer.parseInt(str.substring(i+1).trim());
            }
        }
        return t;
    }

    public static int[] parseInterval(String intervalStr) {
        String str;
        int i, t;
        int[] tw = new int[2];
        if (intervalStr == null || intervalStr.length() == 0)
            return new int[0];
        str = intervalStr;
        i = str.indexOf("-");
        if (i <= 0)
            return new int[0];
        t = parseTimeString(str.substring(0, i));
        if (t < 0 || t > 86400)
            return new int[0];
        tw[0] = t * 1000;
        t = parseTimeString(str.substring(i+1));
        if (t < 0 || t > 86400)
            return new int[0];
        tw[1] = t * 1000;

        return tw;
    }

    public static int[] parseThreshold(String thresholdStr) {
        String str;
        int i, t;
        int[] th;
        if (thresholdStr == null || thresholdStr.length() == 0)
            return new int[0];
        str = thresholdStr;
        i = str.indexOf(",");
        if (i > 0) {
            t = parseTimeString(str.substring(0, i));
            str = str.substring(i+1);
            i = str.indexOf(",");
            if (i > 0) {
                th = new int[3];
                th[0] = t * 1000;
                th[1] = 1000 * parseTimeString(str.substring(0, i));
                th[2] = 1000 * parseTimeString(str.substring(i+1));
            }
            else {
                th = new int[2];
                th[0] = t * 1000;
                th[1] = 1000 * parseTimeString(str);
            }
        }
        else {
            th = new int[1];
            th[0] = 1000 * parseTimeString(str);
        }

        return th;
    }

    public Calendar getToday() {
        return today;
    }

    public Calendar getYesterday() {
        return yesterday;
    }

    public long getBaseTime() {
        return baseTime;
    }

    public void list() {
        int i, j;
        System.out.println("<ActiveTime>");
        if (startTime > Long.MIN_VALUE)
            System.out.println("  <StartTime>" +
                dateFormat.format(new Date(startTime)) + "</StartTime>");

        if (stopTime < Long.MAX_VALUE)
            System.out.println("  <StopTime>" +
                dateFormat.format(new Date(stopTime)) + "</StopTime>");

        for (i=0; i<12; i++) {
            if ((activeMonth & monthMask[i]) > 0)
                System.out.println("  <Month type=\"ARRAY\">" + (i + 1) +
                    "</Month>");
        }

        for (i=0; i<31; i++) {
            if ((activeMonthDay & monthDayMask[i]) > 0)
                System.out.println("  <MonthDay type=\"ARRAY\">" + (i + 1) +
                    "</MonthDay>");
        }

        for (i=0; i<7; i++) {
            if ((activeWeekDay & weekDayMask[i]) > 0)
                System.out.println("  <WeekDay type=\"ARRAY\">" +
                (i <= 0 ? 7 : i) + "</WeekDay>");
        }

        Iterator iter = blackout.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            int[][] bins = (int[][]) blackout.get(key);
            for (i=0; i<bins.length; i++) {
                System.out.println("  <Blackout type=\"ARRAY\">" + key + " " +
                    bins[i][0]/1000 + "-" + bins[i][1]/1000+ "</Blackout>");
            }
        }

        for (i=0; i<intervals.length; i++) {
            System.out.println("  <TimeWindow type=\"ARRAY\" id=\"" + i +"\">");
            System.out.println("    <Interval type=\"ARRAY\">" +
                intervals[i][0]/1000 + "-" + intervals[i][1]/1000 +
                    "</Interval>");
            if (thresholds[i].length > 0) {
                System.out.print("    <Threshold type=\"ARRAY\">" +
                    thresholds[i][0]/1000);
                for (j=1; j<thresholds[i].length; j++)
                    System.out.print("," + thresholds[i][j]/1000);
                System.out.println("</Threshold>");
            }
            System.out.println("  </TimeWindow>");
        }

        System.out.println("</ActiveTime>");
    }

    private void updateTimechange(int id, String key, long dst, long offset) {
        int i, k;
        int[][] tw, w;
        Calendar cal;
        if (key == null || id < 0)
            return;
        if (blackout.containsKey(key)) { // update tw of blackout for key
            w = (int[][]) blackout.get(key);
            if (w.length > id) { // remove the old entry
                tw = new int[w.length-1][2];
                k = 0;
                for (i=0; i<w.length; i++) { // copy over
                    if (i == id)
                        continue;
                    tw[k][0] = w[i][0];
                    tw[k++][1] = w[i][1];
                }
                blackout.put(key, tw);
            }
        }
        if (offset == 0L) // there is no DST in the new year
            return;
        cal = Calendar.getInstance();
        cal.setTime(new Date(dst));
        key = (cal.get(Calendar.MONTH)+1) +"/"+ cal.get(Calendar.DAY_OF_MONTH);
        if (blackout.containsKey(key)) { // update tw of blackout for new key
            w = (int[][]) blackout.get(key);
            tw = new int[w.length+1][2];
            for (i=0; i<w.length; i++) { // copy over
                tw[i][0] = w[i][0];
                tw[i][1] = w[i][1];
            }
        }
        else { // or just add it if not there
            tw = new int[1][2];
            i = 0;
        }
        tw[i][0] = 1000*((cal.get(Calendar.HOUR_OF_DAY) * 60 +
            cal.get(Calendar.MINUTE)) * 60 + cal.get(Calendar.SECOND));
        tw[i][1] = tw[i][0] + (int) offset;
        blackout.put(key, tw);
    }

    /**
     * return tripplet of DST info: dstStart, dstEnd, dstOffset 
     */
    public synchronized static long[] getDSTInfo(int year) {
        long[] dst;
        if (!tz.useDaylightTime())
            return new long[0];
        if (year == dstYear)
            return new long[] {dstStart, dstEnd, dstOffset};
        dstYear = year; 
        dst = getDSTInfo(year, tz);
        dstStart = dst[0];
        dstEnd = dst[1];
        dstOffset = dst[2];
        return dst;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStartTime(long tm) {
        startTime = tm;
    }

    public void setStopTime(long tm) {
        stopTime = tm;
    }

    /**
     * return tripplet of DST info: dstStart, dstEnd, dstOffset 
     */
    public static long[] getDSTInfo(int year, TimeZone zone) {
        int i, k, n, d;
        long t, tt, d0 = 0L, d1 = 0L, d2 = 0L;
        Calendar cal;
        if (zone == null)
            zone = TimeZone.getDefault();
        if (!zone.useDaylightTime())
            return new long[0];
        cal = Calendar.getInstance(zone);
        cal.set(year, 7, 1, 0, 0, 0);
        d = cal.get(Calendar.DST_OFFSET);
        t = tt = cal.getTime().getTime();
        for (i=1; i<=183; i++) {
            t += 86400000L;
            cal.setTime(new Date(t));
            k = cal.get(Calendar.DST_OFFSET);
            if (k == d)
                continue;
            // found the day of time-change
            t -= 86400000L;
            for (i=1; i<=144; i++) {
                t += 600000L;
                cal.setTime(new Date(t));
                k = cal.get(Calendar.DST_OFFSET);
                if (k != d)
                    break;
            }
            if (k == 0) { // end of daylight time
                d1 = t;
                d2 = d;
            }
            else { // start of daylight time
                d0 = t;
                d2 = k;
            }
            break;
        }
        t = tt;
        for (i=1; i<=181; i++) {
            t -= 86400000L;
            cal.setTime(new Date(t));
            k = cal.get(Calendar.DST_OFFSET);
            if (k == d)
                continue;
            // found the day of time-change
            for (i=1; i<=144; i++) {
                t += 600000L;
                cal.setTime(new Date(t));
                k = cal.get(Calendar.DST_OFFSET);
                if (k == d)
                    break;
            }
            if (k == 0) { // start of daylight time
                d1 = t;
                d2 = d;
            }
            else { // end of daylight time
                d0 = t;
                d2 = k;
            }
            break;
        }

        return new long[] {d0, d1, d2};
    }
}
