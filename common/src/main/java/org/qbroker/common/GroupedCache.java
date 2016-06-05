package org.qbroker.common;

/* GroupedCache.java - a QuickCache with group support */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.QuickCache;

/**
 * GroupedCache is a QuickCache with group support.  Each cached object is
 * identified by its unique key.  The object also belongs to at least one
 * group.  Therefore, the objects in GroupedCache are related to each other
 * based on their groups.
 *<br/><br/>
 * A group is identified by either its name or its id.  There is no limit on
 * the capacity of a group.  But there is a limit on maximum number of groups.
 * With group support, it is much easier to manage the cached objects.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class GroupedCache {
    private String name;
    private int capacity;            // capacity of the list for groups
    private AssetList list;          // list of groups
    private QuickCache cache;        // cache of objects

    public GroupedCache(String name, int capacity, int option, int timeout,
        int threshold) {
        this.name = name;
        this.capacity = capacity;
        cache = new QuickCache(name, option, timeout, threshold);
        list = new AssetList(name, capacity);
    }

    public GroupedCache(String name, int capacity) {
        this(name, capacity, QuickCache.META_DEFAULT, 0, 0);
    }

    /**
     * It checks the existence of the key first.  If the key exists but has
     * expired, it updates the object, metadata, timestamp, and a array of
     * group name, then returns 0.  If the key has not expired yet, it returns
     * -1 for insert failure.  If the key does not exist, it just inserts the
     * key, object, timestamp and the group, then returns 1.  The given group
     * should not be empty.  Otherwise, it throws IllegalArgumentException.
     */
    @SuppressWarnings("unchecked")
    public int insert(String key, long timestamp, int ttl, int[] meta,
        String[] groups, Object obj) {
        int i=-1, id, n = 0;
        String str;
        if (groups != null && groups.length > 0) {
            Map h;
            for (i=0; i<groups.length; i++) {
                str = groups[i]; 
                if (str == null || str.length() <= 0)
                    continue;
                if ((id = list.getID(str)) >= 0)
                    h = (Map) list.get(id);
                else {
                    h = new HashMap();
                    id = list.add(str, new long[0], h); 
                }
                h.put(key, null);
                n ++;
            }
            i = cache.insert(key, timestamp, ttl, meta, obj);
        }
        else
            throw(new IllegalArgumentException("empty groups"));
        return i;
    }

    /**
     * It checks the existence of the key first.  If the key exists, it checks
     * the expiration.  If the key has expired, it updates the object and
     * timestamp, and returns 1.  If the key has not expired, it updates anyway
     * and returns 0.  If the key does not exist, it returns -1 for failure.
     */
    public int update(String key, long timestamp, int ttl, int[] meta,
        Object obj) {
        return cache.update(key, timestamp, ttl, meta, obj);
    }

    /**
     * It only returns the object of the key if it has not expired yet.
     * Otherwise, it returns null.  Since the object for the key may be null
     * originally, please check existence of the key to figure out if the
     * object has expired or it just does not exist at all.
     */
    public Object get(String key, long currentTime) {
        return cache.get(key, currentTime);
    }

    /**
     * It expires the object and returns 0 if the ttl is larger than zero.
     * For an evergreen object, it just returns 1 with no changes to its state.
     * If the key does not exist, it returns -1.  The expire action will not
     * modify any metadata and object.
     */
    public int expire(String key, long currentTime) {
        return cache.expire(key, currentTime);
    }

    /**
     * updates mtime of key if it has not expired.  It retruns 0 on success,
     * -1 if no key is found or 1 if the object has expired already
     */
    public int touch(String key, long timestamp) {
        return cache.touch(key, timestamp);
    }

    /**
     * updates mtime of key with a given reference time for expiration.
     * It retruns 0 on success, -1 if no key is found or 1 if the object
     * has expired already
     */
    public int touch(String key, long timestamp, long currentTime) {
        return cache.touch(key, timestamp, currentTime);
    }

    /**
     * It expires all the object belonging to the group of id and returns
     * number of the objects expired.
     */
    public int expireGroup(int id, long currentTime) {
        int n = -1;
        String key;
        Iterator iter = keyIterator(id);
        if (iter != null) {
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                cache.expire(key, currentTime);
                n ++;
            }
        }
        return n;
    }

    /**
     * It invalidates all the object belonging to the group of id and returns
     * number of the objects invalidated.
     */
    public int invalidateGroup(int id, long currentTime) {
        int n = -1;
        String key;
        Iterator iter = keyIterator(id);
        if (iter != null) {
            n = 0;
            while (iter.hasNext()) {
                key = (String) iter.next();
                cache.invalidate(key, currentTime);
                n ++;
            }
        }
        return n;
    }

    /**
     * returns true if the object of the key has expired, or false otherwise.
     * It throws IllegalArgumentException if the key is not found.
     */
    public boolean isExpired(String key, long currentTime) {
        return cache.isExpired(key, currentTime);
    }

    /**
     * It only checks the existence of the object identified by the key,
     * no matter whether the object has expired or not.
     */
    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    /**
     * returns the ttl for the object referenced by the key.
     */
    public int getTTL(String key) {
        return cache.getTTL(key);
    }

    /**
     * returns the timestamp for the object referenced by the key.
     */
    public long getTimestamp(String key) {
        return cache.getTimestamp(key);
    }

    /**
     * returns the access count for the object referenced by the key.
     */
    public long getCount(String key) {
        return cache.getCount(key);
    }

    /**
     * It returns the metadata for the object referenced by the key no matter
     * whether the object is expired or not.  However, it returns null if the
     * key is not found.
     */
    public int[] getMetaData(String key) {
        return cache.getMetaData(key);
    }

    /**
     * returns the total number of cached objects
     */
    public int size() {
        return cache.size();
    }

    /**
     * It removes all expired objects, resets all sequence numbers and returns
     * an array of all expired keys that have been just removed.
     */
    public String[] disfragment(long currentTime) {
        int i, id, n;
        Map h;
        Browser browser;
        String key;
        String[] expired = cache.disfragment(currentTime);
        n = expired.length;
        if (n <= 0)
            return expired;
        browser = list.browser();
        while ((id = browser.next()) >= 0) {
            h = (Map) list.get(id);
            if (h == null || h.size() <= 0)
                continue;
            for (i=0; i<n; i++) {
                key = expired[i];
                if (h.containsKey(key))
                    h.remove(key);
            }
        }
        return expired;
    }

    /**
     * returns the array of all the existing keys in the NATURAL order
     * no matter whether they have expired or not.
     */
    public String[] sortedKeys() {
        return cache.sortedKeys();
    }

    /**
     * returns a set for all existing keys of the entire cache
     */
    public Set<String> keySet() {
        return cache.keySet();
    }

    /**
     * returns an Iterator for all existing keys in the group with id or
     * null if the group id does not exist.
     */
    public Iterator keyIterator(int id) {
        Object o = list.get(id);
        if (o != null)
            return ((Map) o).keySet().iterator();
        else
            return null;
    }

    /**
     * returns a Browser for all group IDs in the natural order
     */
    public Browser groupBrowser() {
        return list.browser();
    }

    /**
     * returns true if the group with the id exists
     */
    public boolean existsGroup(int id) {
        return list.existsID(id);
    }

    /**
     * returns true if the group with the name exists
     */
    public boolean existsGroup(String name) {
        return list.containsKey(name);
    }

    public String groupName(int id) {
        return list.getKey(id);
    }

    public int groupID(String name) {
        return list.getID(name);
    }

    public int groupSize() {
        return list.size();
    }

    public int getCapacity() {
        return capacity;
    }

    public String getName() {
        return name;
    }

    public void clear() {
        cache.clear();
        list.clear();
    }

    protected void finalize() {
        clear();
    }
}
