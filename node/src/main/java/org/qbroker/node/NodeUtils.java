package org.qbroker.node;

/* NodeUtils.java - a wrapper of utilites for Node */

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.common.Template;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.AssetList;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.ThreadPool;
import org.qbroker.json.JSON2Map;
import org.qbroker.node.MessageNode;
import org.qbroker.event.EventUtils;
import org.qbroker.event.Event;

/**
 * NodeUtils contains a bunch of constants and static methods on brokers
 * for common uses
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class NodeUtils {
    public final static int OF_HOLDON = 0;
    public final static int OF_KEEPNEW = 1;
    public final static int OF_KEEPOLD = 2;
    public final static int RESULT_OUT = 1;
    public final static int FAILURE_OUT = 2;
    public final static int NOHIT_OUT = 2;
    private final static String FILE_SEPARATOR =
        System.getProperty("file.separator");

    /** default constructor for NodeUtils */
    public NodeUtils() {
    }

    /**
     * It initializes all fixed outLinks stored in list and returns an
     * instance of AssetList with metaData on each of outLinks.
     * The array of overlap determines what outLinks are allowed to be 
     * overlapped with others before it.  It also initializes non-overlapping
     * outLinks after the last allowed overlap outLinks.  For those,
     * it will skip any duplicated outLinks. 
     */
    public static AssetList initFixedOutLinks(long tm, int outCapacity,
        int size, int[] overlap, String name, List list) {
        Object o;
        int[] partition;
        long[] outInfo;
        String key, str;
        AssetList outList;
        int i, j, k, m, id, n, option, capacity;

        if (list == null || size < 0 || overlap == null)
            return null;

        n = list.size();
        if (n < size)
            outList = new AssetList(name, size);
        else
            outList = new AssetList(name, n);

        k = 0;
        m = (overlap.length > 0) ? overlap[overlap.length - 1] : n - 1;
        for (i=0; i<=m; i++) { // searching for overlapped names
            o = list.get(i);
            if (o == null)
                throw(new IllegalArgumentException(name +
                    ": empty OutLink at " + i));
            if (o instanceof Map) {
                key = (String) ((Map) o).get("Name");
                partition = new int[0];
                str = (String) ((Map) o).get("Partition");
                if (str != null)
                    partition = TimeWindows.parseThreshold(str);
                for (j=0; j<partition.length; j++)
                    partition[j] /= 1000;
                str = (String) ((Map) o).get("Capacity");
                if (str != null)
                    capacity = Integer.parseInt(str);
                else
                    capacity = outCapacity;
                str = (String) ((Map) o).get("ReportName");
            }
            else {
                key = (String) o;
                partition = new int[0];
                capacity = outCapacity;
                str = null;
            }
            if (key == null || key.length() <= 0)
                throw(new IllegalArgumentException(name +
                    ": no name defined for OutLink at " + i));
            else if (!outList.containsKey(key)) {
                outInfo = new long[MessageNode.OUT_QTIME + 1];
                for (j=0; j<=MessageNode.OUT_QTIME; j++)
                    outInfo[j] = 0;
                outInfo[MessageNode.OUT_TIME] = tm;
                outInfo[MessageNode.OUT_STATUS] = MessageNode.NODE_RUNNING;
                outInfo[MessageNode.OUT_CAPACITY] = capacity;
                if (partition.length >= 1) {
                    outInfo[MessageNode.OUT_OFFSET] = partition[0];
                    outInfo[MessageNode.OUT_LENGTH]=(partition.length == 1)?1:
                        partition[1];
                }
                id = outList.add(key, outInfo, new Object[]{null, str, null});
            }
            else if (overlap.length == 1) { // overlap on last one allowed
                if (i == overlap[0] && outList.getID(key) != 0)
                    overlap[0] = outList.getID(key);
                else
                    throw(new IllegalArgumentException(name +
                        ": duplicated OutLink not allowed at " + i +
                        " for " + key));
            }
            else if (overlap.length == 2) { // overlap on last two allowed
                if (i == overlap[0]) {
                    overlap[0] = outList.getID(key);
                    overlap[1] --;
                    k ++;
                }
                else if (i == overlap[1] + k)
                    overlap[1] = outList.getID(key);
                else
                    throw(new IllegalArgumentException(name +
                        ": duplicated OutLink not allowed at " + i +
                        " for " + key));
            }
            else if (overlap.length == 3) { // overlap on last three allowed
                if (i == overlap[0]) {
                    overlap[0] = outList.getID(key);
                    overlap[1] --;
                    overlap[2] --;
                    k ++;
                }
                else if (i == overlap[1] + k) {
                    overlap[1] = outList.getID(key);
                    overlap[2] --;
                    k ++;
                }
                else if (i == overlap[2] + k)
                    overlap[2] = outList.getID(key);
                else
                    throw(new IllegalArgumentException(name +
                        ": duplicated OutLink for " + key));
            }
            else { // no overlap allowed
                throw(new IllegalArgumentException(name +
                    ": duplicated OutLink not allowed at " + i + " for " +key));
            }
        }

        for (i=m+1; i<n; i++) { // process non-overlapping outLinks
            o = list.get(i);
            if (o == null)
                throw(new IllegalArgumentException(name +
                    ": empty OutLink at " + i));
            if (o instanceof Map) {
                key = (String) ((Map) o).get("Name");
                partition = new int[0];
                str = (String) ((Map) o).get("Partition");
                if (str != null)
                    partition = TimeWindows.parseThreshold(str);
                for (j=0; j<partition.length; j++)
                    partition[j] /= 1000;
                str = (String) ((Map) o).get("Capacity");
                if (str != null)
                    capacity = Integer.parseInt(str);
                else
                    capacity = outCapacity;
                str = (String) ((Map) o).get("ReportName");
            }
            else {
                key = (String) o;
                partition = new int[0];
                capacity = outCapacity;
                str = null;
            }
            if (key == null || key.length() <= 0)
                new Event(Event.WARNING, name+": ignored unnamed OutLink at " +
                    i).send();
            else if (!outList.containsKey(key)) {
                outInfo = new long[MessageNode.OUT_QTIME + 1];
                for (j=0; j<=MessageNode.OUT_QTIME; j++)
                    outInfo[j] = 0;
                outInfo[MessageNode.OUT_TIME] = tm;
                outInfo[MessageNode.OUT_STATUS] = MessageNode.NODE_RUNNING;
                outInfo[MessageNode.OUT_CAPACITY] = capacity;
                if (partition.length >= 1) {
                    outInfo[MessageNode.OUT_OFFSET] = partition[0];
                    outInfo[MessageNode.OUT_LENGTH]=(partition.length == 1)?1:
                        partition[1];
                }
                id = outList.add(key, outInfo, new Object[]{null, str, null});
            }
            else { // skip overlapped outLinks
                new Event(Event.WARNING,name+": skipped duplicate OutLink at "+
                    i).send();
            }
        }
        return outList;
    }

    /**
     * It initializes all outLinks stored in list and returns an
     * instance of AssetList with metaData on each of outLinks. 
     * Any duplicated or malformed outLinks will be ignored.
     * If overflow is positive, OverflowOption will be stored in
     * outInfo at position of overflow.
     */
    public static AssetList initOutLinks(long tm, int outCapacity,
        int size, int overflow, String name, List list) {
        Object o;
        int[] partition;
        long[] outInfo;
        String key, str;
        AssetList outList;
        int i, j, id, n, option, capacity;

        if (list == null || size < 0)
            return null;

        n = list.size();
        if (n < size)
            outList = new AssetList(name, size);
        else
            outList = new AssetList(name, n);

        for (i=0; i<n; i++) { // searching for overlapped names
            o = list.get(i);
            if (o == null) {
                new Event(Event.WARNING, name+": ignored empty OutLink at " +
                    i).send();
                continue;
            }
            if (o instanceof Map) {
                key = (String) ((Map) o).get("Name");
                partition = new int[0];
                str = (String) ((Map) o).get("Partition");
                if (str != null)
                    partition = TimeWindows.parseThreshold(str);
                for (j=0; j<partition.length; j++)
                    partition[j] /= 1000;
                str = (String) ((Map) o).get("Capacity");
                if (str != null)
                    capacity = Integer.parseInt(str);
                else
                    capacity = outCapacity;
                str = (String) ((Map) o).get("OverflowOption");
                if (str == null)
                    option = OF_HOLDON;
                else if ("keepnew".equals(str.toLowerCase()))
                    option = OF_KEEPNEW;
                else if ("keepold".equals(str.toLowerCase()))
                    option = OF_KEEPOLD;
                else if ("holdon".equals(str.toLowerCase()))
                    option = OF_HOLDON;
                else
                    option = OF_HOLDON;
                str = (String) ((Map) o).get("ReportName");
            }
            else {
                key = (String) o;
                partition = new int[0];
                capacity = outCapacity;
                option = OF_HOLDON;
                str = null;
            }
            if (key == null || key.length() <= 0)
                new Event(Event.WARNING, name+": ignored unnamed OutLink at " +
                    i).send();
            else if (outList.containsKey(key))
                new Event(Event.WARNING,name+": skipped duplicate OutLink at "+
                    i).send();
            else {
                outInfo = new long[MessageNode.OUT_QTIME + 1];
                for (j=0; j<=MessageNode.OUT_QTIME; j++)
                    outInfo[j] = 0;
                outInfo[MessageNode.OUT_TIME] = tm;
                outInfo[MessageNode.OUT_STATUS] = MessageNode.NODE_RUNNING;
                outInfo[MessageNode.OUT_CAPACITY] = capacity;
                if (partition.length >= 1) {
                    outInfo[MessageNode.OUT_OFFSET] = partition[0];
                    outInfo[MessageNode.OUT_LENGTH] =
                        (partition.length == 1) ? 1 : partition[1];
                }
                if (overflow >= 0 && overflow <= MessageNode.OUT_QTIME)
                    outInfo[overflow] = option;
                id = outList.add(key, outInfo, new Object[]{null, str});
            }
        }

        return outList;
    }

    /**
     * puts a msg to out XQ with overflowOption implemented and returns
     * a non-negative number upon success or -1 otherwise
     */
    public static int passthru(long waitTime, Message msg, XQueue in,
        XQueue out, long[] outInfo, int option) {
        int cid, len, shift;
        cid = -1;
        if (msg == null || in == null || out == null || outInfo == null)
            return -1;
        len = (int) outInfo[MessageNode.OUT_LENGTH];
        switch (option) {
          case OF_HOLDON:
            switch (len) {
              case 0:
                do {
                    cid = out.reserve(waitTime);
                } while (cid < 0 &&
                    (in.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
                break;
              case 1:
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                do {
                    cid = out.reserve(waitTime, shift);
                } while (cid < 0 &&
                    (in.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
                break;
              default:
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                do {
                    cid = out.reserve(waitTime, shift, len);
                } while (cid < 0 &&
                    (in.getGlobalMask() & XQueue.KEEP_RUNNING) > 0);
                break;
            }
            break;
          case OF_KEEPNEW:
            switch (len) {
              case 0:
                cid = out.reserve(waitTime);
                if (cid < 0 && out.depth() > 0) {
                    cid = out.getNextCell(waitTime);
                    if (cid >= 0)
                        out.remove(cid);
                    cid = out.reserve(waitTime);
                }
                break;
              case 1:
                // do not try to get off the msg
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                for (int i=0; i<10; i++) {
                    cid = out.reserve(waitTime, shift);
                    if (cid >= 0 ||
                      (in.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                        break;
                }
                break;
              default:
                // do not try to get off a msg
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                for (int i=0; i<10; i++) {
                    cid = out.reserve(waitTime, shift, len);
                    if (cid >= 0 ||
                      (in.getGlobalMask() & XQueue.KEEP_RUNNING) == 0)
                        break;
                }
                break;
            }
            break;
          case OF_KEEPOLD:
            switch (len) {
              case 0:
                cid = out.reserve(waitTime);
                break;
              case 1:
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                cid = out.reserve(waitTime, shift);
                break;
              default:
                shift = (int) outInfo[MessageNode.OUT_OFFSET];
                cid = out.reserve(waitTime, shift, len);
                break;
            }
            break;
          default:
        }
        if (cid >= 0) { // reserved
            out.add(msg, cid);
            outInfo[MessageNode.OUT_COUNT] ++;
            outInfo[MessageNode.OUT_TIME] = System.currentTimeMillis();
        }
        return cid;
    }

    public static void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }

    /** returns number of outstanding messages for the given xqueue */
    public static int getOutstandingSize(XQueue xq, int begin, int len) {
        int i, k, n = 0;
        if (xq == null || begin < 0 || len < 0 || begin+len > xq.getCapacity())
            return -1;

        if (len == 0)
            n = xq.size();
        else if (len == 1) {
            k = xq.getCellStatus(begin);
            n = (k == XQueue.CELL_OCCUPIED || k == XQueue.CELL_TAKEN) ? 1 : 0;
        }
        else {
            n = 0;
            for (i=begin+len-1; i>=0; i--) { // scan partitioned cells
                k = xq.getCellStatus(i);
                if (k == XQueue.CELL_OCCUPIED || k == XQueue.CELL_TAKEN)
                    n ++;
            }
        }
        return n;
    }

    /** returns the report map of the name from the default container */
    public static Map<String, Object> getReport(String name) {
        return new HashMap<String, Object>();
    }
}
