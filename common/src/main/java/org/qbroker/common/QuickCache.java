package org.qbroker.common;

/* QuickCache.java - a dynamic cache for objects with TTL (time-to-live) */

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Comparator;
import java.util.Date;

/**
 * QuickCache is a HashMap with the unique keys and objects, TTLs (time-to-live)
 * and timestamps, as well as the MetaData.  All the objects have their
 * own TTLs to control when to expire.  Once an object expires, it will not be
 * visible any more.  The expired object will be removed from the cache
 * eventually by the automatic or passive disfragmentation.  Therefore, the
 * size of QuickCache is self adjustable with the time and the load.
 *<br><br>
 * QuickCache caches objects with their own expiration time.  Since each object
 * automatically expires once it passes its TTL, the size of the cache
 * changes with time and load.  Therefore, you can control the size of the cache
 * by adjusting the TTLs according to the load.
 *<br><br>
 * QuickCache also has timeout and threshold to control the automatic
 * disfragmentations.  When certain objects expire, QuickCache will not
 * clean them up immediately.  It waits until the certain conditions are met.
 * You can also turn on/off the chache temporarily via its status.  Each time
 * the status of the cache changes, its mtime changes too.  For auto part,
 * if the timeout is set to 0, the time-driven disfragmentation is disabled.
 * If the threshold is set to 0, the size-driven disfragmentation is disabled.
 *<br><br>
 * Besides the objects, QuickCache also stores MetaData for each objects.
 * Metadata is an opaque array with integers.  It is up to the developers to
 * decide how many integers and what they stand for.  For example, users can
 * put id and gid into the MetaData array.
 *<br><br>
 * For each object, QuickCache will create some internal properties, like
 * TIME, COUNT and TTL.  QuickCache uses them to manage the cached objects.
 * However, these internal properties may mean differently according to
 * their initial options.  For example, TIME is the timestamp of either the
 * modification time (mtime) or the access time (atime).  COUNT may mean
 * the access count (acount) or the modification count (mcount).  TTL controls
 * if and when the object is expired and is ready to be removed by the process
 * of disfragmentation.  According to the cache option, TTL may be either
 * the number of milliseconds as the expiration time or the maximum count
 * as the threshold to determine if the object has expired or not.  In case of
 * the expiration time, TTL really means Time-To-Live which controls when
 * the object expires.  In case of the count, TTL is the maximum count required
 * for an object to be treaded as expired.  Hence if the COUNT is less than or
 * equal to TTL, the object is treated as expired since it is not used often
 * enough.  The expired objects will be removed eventually by disfragmentation.
 *<br><br>
 * Since QuickCache is a dynamic container, it may actively or passively
 * remove those objects that are not needed any longer, in order to free memory.
 * The disfragmentation may base on either TIME or COUNT.  Therefore, there
 * are 8 different ways to interpret the internal properties.  Each combination
 * defines a unique way for QuickCache to determin what objects can be removed.
 * Here is the list for all 8 options:
 * <table>
 * <caption>Options</caption>
 * <tr><th>Option</th><th>Time</th><th>Count</th><th>TTL</th></tr>
 * <tr><td>META_MTAC</td><td>mtime</td><td>acount</td><td>TIME</td></tr>
 * <tr><td>META_MCMT</td><td>mtime</td><td>mcount</td><td>COUNT</td></tr>
 * <tr><td>META_MTMC</td><td>mtime</td><td>mcount</td><td>TIME</td></tr>
 * <tr><td>META_MCAT</td><td>atime</td><td>mcount</td><td>COUNT</td></tr>
 * <tr><td>META_ATAC</td><td>atime</td><td>acount</td><td>TIME</td></tr>
 * <tr><td>META_ACMT</td><td>mtime</td><td>acount</td><td>COUNT</td></tr>
 * <tr><td>META_ATMC</td><td>atime</td><td>mcount</td><td>TIME</td></tr>
 * <tr><td>META_ACAT</td><td>atime</td><td>acount</td><td>COUNT</td></tr>
 * </table>
 * The default is META_MTAC.
 *<br><br>
 * This is NOT MT-Safe.  Therefore please use it with care.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class QuickCache implements Comparator<long[]> {
    private int threshold;           // threshold for disfragmentation
    private String name;             // name of the cache
    private int timeout = 0;         // expiration time of the entire cache
    private final int option;        // option for internal properties
    private long previousTime;       // time of previous auto-disfragmentation
    private List<Object> elements;   // list of objects
    private List<int[]> metadata;    // list of metadata
    private Map<String,long[]> cache;// hashmap of the cache
    private long mtime;              // time of the latest status change
    private int status;              // status of the cache
    private long sequence;           // sequence of the cache

    private final long initCount;
    private final boolean isTime;    // (TTL is for TIME)
    private final boolean isModify;  // (time == mtime)
    private final boolean isAccess;  // (count == acount)
    private final static int QC_IND = 0;   // index to the object
    private final static int QC_SEQ = 1;   // sequence of the object
    private final static int QC_COUNT = 2; // mcount or acount to the object
    private final static int QC_TTL = 3;   // Time-To-Live of the object
    private final static int QC_TIME = 4;  // timestamp of the object
    public final static int CACHE_NONE = 0;
    public final static int CACHE_ON = 1;
    public final static int CACHE_OFF = 2;
    public final static int CACHE_EXPIRED = 4;
    public final static int META_DEFAULT = 0;
    public final static int META_MTAC = 0;
    public final static int META_MCMT = 1;
    public final static int META_MTMC = 2;
    public final static int META_MCAT = 3;
    public final static int META_ATAC = 4;
    public final static int META_ACMT = 5;
    public final static int META_ATMC = 6;
    public final static int META_ACAT = 7;

    public QuickCache(String name, int option, int timeout,
        int threshold) {
        if (name == null)
            throw(new IllegalArgumentException("name is null"));
        this.name = name;
        if (option >= META_MTAC && option <= META_ACAT)
            this.option = option;
        else
            throw(new IllegalArgumentException("option is bad"));
        cache = new HashMap<String, long[]>();
        elements = new ArrayList<Object>();
        metadata = new ArrayList<int[]>();
        if (threshold >= 0)
            this.threshold = threshold;
        else
            this.threshold = 512;
        if (timeout >= 0)
            this.timeout = timeout;
        switch (this.option) {
          default:
          case META_MTAC:
            isTime = true;
            isModify = true;
            isAccess = true;
            initCount = 0;
            break;
          case META_MCMT:
            isTime = false;
            isModify = true;
            isAccess = false;
            initCount = 1;
            break;
          case META_MTMC:
            isTime = true;
            isModify = true;
            isAccess = false;
            initCount = 1;
            break;
          case META_MCAT:
            isTime = false;
            isModify = false;
            isAccess = true;
            initCount = 1;
            break;
          case META_ATAC:
            isTime = true;
            isModify = false;
            isAccess = true;
            initCount = 0;
            break;
          case META_ACMT:
            isTime = false;
            isModify = true;
            isAccess = false;
            initCount = 0;
            break;
          case META_ATMC:
            isTime = true;
            isModify = false;
            isAccess = false;
            initCount = 1;
            break;
          case META_ACAT:
            isTime = false;
            isModify = false;
            isAccess = true;
            initCount = 0;
            break;
        }

        if (!isTime) { // disable auto disfragmentation since new obj expired
            this.timeout = 0;
            this.threshold = 0;
        }
        previousTime = 0L;
        status = CACHE_ON;
        mtime = System.currentTimeMillis();
        sequence = 0;
    }

    public int size() {
        return cache.size();
    }

    public String getName() {
        return name;
    }

    public int getOption() {
        return option;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getMTime() {
        return mtime;
    }

    public int getStatus() {
        return status;
    }

    /**
     * sets the CacheStatus and the modify time
     */
    public void setStatus(int status, long currentTime) {
        if (status == CACHE_ON || status == CACHE_OFF) {
            this.status = status;
            mtime = currentTime;
        }
    }

    /**
     * sets the CacheStatus and the modify time as the currentTime
     */
    public void setStatus(int status) {
        setStatus(status, System.currentTimeMillis());
    }

    /**
     * It checks the existence of the key first.  If the key exists, it checks
     * the TTL.  If the key has expired, it updates the object and timestamp,
     * then returns 0.  If the key has not expired, it returns -1 for insert
     * failure.  If the key does not exist, it just inserts the object and
     * timestamp, then returns 1.
     *<br><br>
     * return code:<br>
     * 1: new object inserted successfully<br>
     * 0: object expired, so updated the object and its meta data<br>
     * -1: object exists, no changes made to it
     */
    public int insert(String key, long timestamp, int ttl, int[] meta,
        Object obj, long currentTime) {
        if (ttl < 0)
            ttl = timeout;
        if (cache.containsKey(key)) { // key exists
            long[] ts = cache.get(key);
            if (isTime) { // TTL is for TIME
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL]) {
                    ts[QC_TTL] = ttl;
                    if (isAccess)
                        ts[QC_COUNT] = initCount;
                    else
                        ts[QC_COUNT] ++;
                    ts[QC_TIME] = timestamp;
                    ts[QC_SEQ] = sequence ++;
                    int i = (int) ts[QC_IND];
                    elements.set(i, obj);
                    metadata.set(i, meta);
                    return 0;
                }
                return -1;
            }
            else { // TTL is for COUNT
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL]) {
                    ts[QC_TTL] = ttl;
                    if (isAccess)
                        ts[QC_COUNT] = 0;
                    else
                        ts[QC_COUNT] ++;
                    ts[QC_TIME] = timestamp;
                    ts[QC_SEQ] = sequence ++;
                    int i = (int) ts[QC_IND];
                    elements.set(i, obj);
                    metadata.set(i, meta);
                    return 0;
                }
                return -1;
            }
        }

        int size = cache.size();
        cache.put(key, new long[]{(long) size, sequence++, initCount,
            (long) ttl, timestamp});
        elements.add(obj);
        metadata.add(meta);

        if (threshold > 0 && cache.size() >= threshold) { // size-driven
            if (currentTime >= timeout + previousTime) {
                disfragment(currentTime);
                previousTime = currentTime;
            }
        }
        else if (timeout > 0) { // time-driven disfragmentation
            if (currentTime >= timeout + previousTime) {
                disfragment(currentTime);
                previousTime = currentTime;
            }
        }

        return 1;
    }

    public int insert(String key, long timestamp, int ttl, int[] meta,
        Object obj) {
        return insert(key, timestamp, ttl, meta, obj,
            System.currentTimeMillis());
    }

    /**
     * It checks the existence of the key first.  If the key exists, it check
     * the expiration of the key.  If the key has expired, it updates the object
     * and timestamp, then returns 1.  If the key has not expired, it updates
     * anyway and returns 0.  If the key does not exist, it returns -1 for
     * failure.
     *<br><br>
     * return code:<br>
     * 1: object expired, but it and its meta data are updated<br>
     * 0: unexpired object updated successfully<br>
     * -1: update failed due to its not existence
     */
    public int update(String key, long timestamp, int ttl, int[] meta,
        Object obj, long currentTime) {
        if (ttl < 0)
            ttl = timeout;
        if (cache.containsKey(key)) { // key exists
            int i;
            long[] ts = cache.get(key);
            ts[QC_SEQ] = sequence ++;
            i = (int) ts[QC_IND];
            elements.set(i, obj);
            metadata.set(i, meta);
            i = 0;
            if (isTime) {
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL])
                    i = 1;
            }
            else {
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL])
                    i = 1;
            }
            ts[QC_TTL] = ttl;
            ts[QC_TIME] = timestamp;
            if (isAccess)
                ts[QC_COUNT] = initCount;
            else
                ts[QC_COUNT] ++;

            return i;
        }

        return -1;
    }

    public int update(String key, long timestamp, int ttl, int[] meta,
        Object obj) {
        return update(key, timestamp, ttl, meta, obj,
            System.currentTimeMillis());
    }

    /**
     * It updates mtime or mcount of the key if it has not expired at the given
     * currentTime yet.  It retruns 0 on success, -1 if no key is found or 1
     * if the object has expired already.
     */
    public int touch(String key, long timestamp, long currentTime) {
        if (cache.containsKey(key)) {
            long[] ts = cache.get(key);
            if (isTime) { // TTL is for TIME
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL])
                    return 1;
                ts[QC_TIME] = timestamp;
            }
            else { // TTL is for COUNT
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL])
                    return 1;
                ts[QC_COUNT] = timestamp;
            }
            return 0;
        }
        return -1;
    }

    public int touch(String key, long timestamp) {
        return touch(key, timestamp, System.currentTimeMillis());
    }

    /**
     * It returns true if the object of the key has expired, or false otherwise.
     * If the key is not found, it throws IllegalArgumentException for failure.
     */
    public boolean isExpired(String key, long currentTime) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME)
            throw(new IllegalArgumentException(name + ": no such key " + key));
        if (isTime) { // TTL is for TIME
            if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL])
                return true;
        }
        else { // TTL is for COUNT
            if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL])
                return true;
        }
        return false;
    }

    public boolean isExpired(String key) {
        return isExpired(key, System.currentTimeMillis());
    }

    /**
     * It replaces the existing object or inserts as the new object if there
     * is no such key in the cache. It updates the metadata and checks the
     * TTL to determine what to return. If the object does not exist, it just
     * returns the new object back. If the existing object has not expired yet,
     * it just returns the replaced object. If the existing object has expired,
     * normally the method returns null except for the case that TTL is on count
     * and COUNT is reset to 0 by touch(). In that case, it returns the new
     * object, just like the original object does not exist.
     *<br><br>
     * returned object:<br>
     * itself: new object inserted successfully or invalid object replaced<br>
     * null: object expired and replaced<br>
     * other: replaced object, its meta data updated
     */
    public Object replace(String key, long timestamp, int ttl, int[] meta,
        Object obj, long currentTime) {
        if (ttl < 0)
            ttl = timeout;
        if (cache.containsKey(key)) { // key exists
            long[] ts = cache.get(key);
            int i = (int) ts[QC_IND];
            Object o = elements.get(i);
            ts[QC_SEQ] = sequence ++;
            elements.set(i, obj);
            metadata.set(i, meta);
            if (isTime) { // TTL is for TIME
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL])
                    o = null;
            }
            else { // TTL is for COUNT
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL]) // expired
                    o = (ts[QC_COUNT] == 0L) ? obj : null;
            }
            ts[QC_TTL] = ttl;
            ts[QC_TIME] = timestamp;
            if (isAccess)
                ts[QC_COUNT] = initCount;
            else
                ts[QC_COUNT] ++;
            return o;
        }

        int size = cache.size();
        cache.put(key, new long[] {(long) size, sequence ++, initCount,
            (long) ttl, timestamp});
        elements.add(obj);
        metadata.add(meta);

        if (threshold > 0 && cache.size() >= threshold) { // size-driven
            if (currentTime >= timeout + previousTime) {
                disfragment(currentTime);
                previousTime = currentTime;
            }
        }
        else if (timeout > 0) { // time-driven disfragmentation
            if (currentTime >= timeout + previousTime) {
                disfragment(currentTime);
                previousTime = currentTime;
            }
        }

        return obj;
    }

    public Object replace(String key, long timestamp, int ttl, int[] meta,
        Object obj) {
        return replace(key, timestamp, ttl, meta, obj,
            System.currentTimeMillis());
    }

    /**
     * It only checks the existence of the object identified by the key,
     * no matter whether the object has expired or not.
     */
    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    /**
     * It returns the ttl for the object referenced by the key.
     */
    public int getTTL(String key) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME) // key does not exist
            throw(new IllegalArgumentException(name + ": no such key " + key));
        return (int) ts[QC_TTL];
    }

    /**
     * It returns the timestamp for the object referenced by the key.
     */
    public long getTimestamp(String key) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME) // key does not exist
            throw(new IllegalArgumentException(name + ": no such key " + key));
        return ts[QC_TIME];
    }

    /**
     * It returns the count for the object referenced by the key.
     */
    public long getCount(String key) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME) // key does not exist
            throw(new IllegalArgumentException(name + ": no such key " + key));
        return ts[QC_COUNT];
    }

    /**
     * It returns the metadata for the object referenced by the key no matter
     * whether the object has expired or not.  However, it returns null if the
     * key is not found.
     */
    public int[] getMetaData(String key) {
        long[] ts = cache.get(key);
        if (ts != null && ts.length > QC_TIME) // key exists
            return (int[]) metadata.get((int) ts[QC_IND]);
        else
            return null;
    }

    /**
     * It returns the object of the key only if it has not expired yet.
     * Otherwise, it returns null if TTL is for TIME.  If TTL is for COUNT, it
     * returns the object anyway.  Since the object for the key may be null
     * originally, the existence of the key should be checked to tell whether
     * the object has expired or has never existed before, or is just null.
     */
    public Object get(String key, long currentTime) {
        long[] ts = cache.get(key);
        if (ts != null) { // key exists
            int i = (int) ts[QC_IND];
            Object o = elements.get(i);
            if (isTime) { // TTL is for TIME
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL])
                    return null;
            }
            else { // TTL is for COUNT
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL])
                    return o;
            }
            if (!isModify)
                ts[QC_TIME] = currentTime;
            if (isAccess)
                ts[QC_COUNT] ++;
            return o;
        }

        return null;
    }

    public Object get(String key) {
        return get(key, System.currentTimeMillis());
    }

    /**
     * It expires the object and returns 0 if the ttl is larger than zero.
     * For an evergreen object(ttl=0), it just returns 1 with no changes to
     * its state.  If the key does not exist, it returns -1.  The expiration
     * action will not modify any metadata and the object.
     */
    public int expire(String key, long currentTime) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME)
            return -1;
        if (isTime) { // TTL is for TIME
            if (ts[QC_TTL] > 0) {
                ts[QC_TIME] = currentTime - ts[QC_TTL] - 1000;;
                return 0;
            }
        }
        else { // TTL is for COUNT
            if (ts[QC_TTL] > 0) {
                ts[QC_COUNT] = ts[QC_TTL];
                return 0;
            }
        }
        return 1;
    }

    public int expire(String key) {
        return expire(key, System.currentTimeMillis());
    }

    /**
     * It sets the object into null no matter what the ttl is.  For a
     * non-evergreen object(ttl!=0), it also expires the object.
     * If the key does not exist, it returns -1.  Otherwise, it returns 0.
     * The invalidation will set the object to null.  But it will not
     * touch the metadata.
     */
    protected int invalidate(String key, long currentTime) {
        long[] ts = cache.get(key);
        if (ts == null || ts.length <= QC_TIME)
            return -1;
        if (isTime) { // TTL is for TIME
            if (ts[QC_TTL] > 0)
                ts[QC_TIME] = currentTime - ts[QC_TTL] - 1000;;
        }
        else { // TTL is for COUNT
            if (ts[QC_TTL] > 0)
                ts[QC_COUNT] = ts[QC_TTL];
        }
        elements.set((int) ts[QC_IND], null);
        return 0;
    }

    /**
     * It removes all expired objects, resets all sequence numbers and returns
     * an array of all expired keys that have been just removed.  If TTL is
     * for COUNT, the count will also be reset for all existing objects.
     */
    public String[] disfragment(long currentTime) {
        int i, k, n;
        Set<String> keySet;
        long[] ts;
        int[] pos, shift;
        String[] keys;
        long[][] state;
        int size = cache.size();
        if (size <= 0) {
            sequence = 0;
            return new String[0];
        }

        shift = new int[size];
        keys = new String[size];
        k = 0;
        keySet = cache.keySet();
        if (isTime) { // TTL is for TIME
            for (String key : keySet) {
                ts = cache.get(key);
                i = (int) ts[QC_IND];
                if (ts[QC_TTL] > 0 && currentTime >= ts[QC_TIME] + ts[QC_TTL]) {
                    shift[i] = 1;
                    keys[k ++] = key;
                }
                else
                    shift[i] = 0;
            }
        }
        else { // TTL is for COUNT
            for (String key : keySet) {
                ts = cache.get(key);
                i = (int) ts[QC_IND];
                if (ts[QC_TTL] > 0 && ts[QC_COUNT] <= ts[QC_TTL]) {
                    shift[i] = 1;
                    keys[k ++] = key;
                }
                else {
                    shift[i] = 0;
                    ts[QC_COUNT] = initCount; // reset count
                }
            }
        }

        if (k > 0) { // some objects expired
            for (i=0; i<k; i++) // removing expired keys
                cache.remove(keys[i]);

            for (i=shift.length-1; i>=0; i--) { // removing expired objects
                if (shift[i] > 0) {
                    elements.remove(i);
                    metadata.remove(i);
                }
            }

            n = 0;
            for (i=0; i<shift.length; i++) {
                if (shift[i] > 0)
                    n ++;
                shift[i] = n;
            }
        }
        size = cache.size();
        if (size <= 0) {
            sequence = 0;
            return keys;
        }

        state = new long[size][];
        pos = new int[size];
        n = 0;
        keySet = cache.keySet();
        for (String key : keySet) {
            ts = cache.get(key);
            i = (int) ts[QC_IND];
            ts[QC_IND] -= shift[i];
            i = (int) ts[QC_IND];
            pos[i] = n;
            state[n++] = ts;
        }

        Arrays.sort(state, this);
        for (i=0; i<n; i++) // reset sequence
            state[i][QC_SEQ] = i;
        sequence = n;

        String[] expired = new String[k];
        for (i=0; i<k; i++) // copy expired keys
            expired[i] = keys[i];

        return expired;
    }

    public String[] disfragment() {
        return disfragment(System.currentTimeMillis());
    }

    /**
     * It returns an array of all the existing keys in the NATURAL order
     * no matter whether they have expired or not. 
     */
    public String[] sortedKeys() {
        int i, k, n;
        Set<String> keySet;
        int[] pos;
        long[] ts;
        long[][] state;
        String[] keys;
        n = cache.size();

        if (n <= 0)
            return new String[0];

        pos = new int[n];
        keys = new String[n];
        state = new long[n][];

        k = 0;
        keySet = cache.keySet();
        for (String key : keySet) {
            ts = cache.get(key);
            pos[(int) ts[QC_IND]] = k;
            keys[k] = key;
            state[k++] = ts;
        }

        Arrays.sort(state, this);
        String[] sorted = new String[k];
        for (i=0; i<k; i++)
            sorted[i] = keys[pos[(int) state[i][QC_IND]]];

        return sorted;
    }

    public Object[] sortedEntries() {
        int i, k, n;
        Set<String> keySet;
        long[][] state;
        n = cache.size();

        if (n <= 0)
            return new Object[0];

        state = new long[n][];

        k = 0;
        keySet = cache.keySet();
        for (String key : keySet) {
            state[k++] = cache.get(key);
        }

        Arrays.sort(state, this);
        Object[] sorted = new Object[k];
        for (i=0; i<k; i++)
            sorted[i] = elements.get((int) state[i][QC_IND]);

        return sorted;
    }

    public void clear() {
        cache.clear();
        elements.clear();
        metadata.clear();
        previousTime = 0L;
        status = CACHE_ON;
        mtime = System.currentTimeMillis();
        sequence = 0;
    }

    protected void finalize() {
        clear();
    }

    /**
     * returns a set for all existing keys
     */
    public Set<String> keySet() {
        return cache.keySet();
    }

    public int compare(long[] a, long[] b) {
        long i, j;
        i = a[QC_SEQ];
        j = b[QC_SEQ];
        if (i > j)
            return 1;
        else if (i < j)
            return -1;
        else
            return 0;
    }
}
