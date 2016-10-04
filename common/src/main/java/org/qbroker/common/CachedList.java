package org.qbroker.common;

/* CachedList.java - an AssetList with classification and cache support */

import java.util.HashMap;
import java.util.Set;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Matcher;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.QuickCache;

/**
 * CachedList is an AssetList with cache and classification support.  Each
 * asset consists of a well-defined (not null) object and its MedaData, as
 * well as a Perl5 Pattern for classifications.  All assets in the list are
 * indexed and can also be identified by their unique keys.  CachedList
 * also maintains a internal cache for dynamic topics.  A topic is a text
 * to be classified via the pattern match.  The classification process will
 * associate a topic to a number of assets if their patterns match the text.
 * In fact, CachedList is an AssetList of objects and a QuickCache of topics
 * plus Patterns for classifications.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class CachedList extends AssetList {
    private String name;
    private int capacity;            // capacity of the list for assets
    private QList list;              // list of patterns
    private QuickCache cache;        // cache of topics
    private int[] patternList;       // active patterns
    private int[] topicCount;        // count of cache topics for each asset
    private Perl5Matcher pm;

    public CachedList(String name, int capacity, int option, int timeout,
        int threshold) {
        super(name, capacity);
        this.name = name;
        this.capacity = capacity;
        cache = new QuickCache(name, option, timeout, threshold);
        list = new QList(name, capacity);
        patternList = new int[0];
        topicCount = new int[capacity];
        for (int i=0; i<capacity; i++)
            topicCount[i] = 0;
        pm = new Perl5Matcher();
    }

    public CachedList(String name, int capacity) {
        this(name, capacity, QuickCache.META_DEFAULT, 0, 0);
    }

    /**
     * It returns a QList with all the IDs of associated assets to the topic.
     */
    private QList classify(String topic) {
        int i, id, n;
        Pattern pattern;
        boolean isNew;
        if (topic == null || topic.length() <= 0)
            return null;
        isNew = !cache.containsKey(topic);
        QList subList = new QList(topic, capacity);
        n = patternList.length;
        for (i=0; i<n; i++) {
            id = patternList[i];
            pattern = (Pattern) list.browse(id);
            if (pattern == null)
                continue;
            if (!pm.contains(topic, pattern))
                continue;
            subList.reserve(id);
            subList.add(null, id);
            if (isNew)
                topicCount[id] ++;
        }
        return subList;
    }

    /**
     * It matchs the pattern to each topic and updates the id list if the
     * pattern matches the topic. It returns the number of matches or -1
     * for bad arguments.
     */
    private int updateCache(int id, Pattern pattern) {
        int n = 0;
        QList subList;
        Set<String> keySet;
        if (id < 0 || id >= capacity || pattern == null)
            return -1;
        keySet = cache.keySet();
        for (String topic : keySet) {
            if (topic.length() <= 0)
                continue;
            if (!pm.contains(topic, pattern))
                continue;
            subList = (QList) cache.get(topic);
            if (subList != null) {
                subList.reserve(id);
                subList.add(null, id);
            } 
            else {
                subList = new QList(topic, capacity);
                subList.reserve(id);
                subList.add(null, id);
                cache.update(topic, cache.getTimestamp(topic),
                    cache.getTTL(topic), cache.getMetaData(topic), subList);
            }
            n ++;
        }
        return n;
    }

    /**
     * It scans all topic caches to remove the id from the list of the topic.
     * It returns the number of removals or -1 in case the id is out of range.
     */
    private int cleanupCache(int id) {
        int n = 0;
        QList subList;
        Set<String> keySet;
        if (id < 0 || id >= capacity)
            return -1;
        keySet = cache.keySet();
        for (String topic : keySet) {
            if (topic.length() <= 0)
                continue;
            subList = (QList) cache.get(topic);
            if (subList == null)
                continue;
            if (subList.getType(id) == XQueue.CELL_OCCUPIED) {
                subList.takeback(id);
                n ++;
            }
        }
        return n;
    }

    /**
     * It checks the existence of the topic first.  If the topic exists but has 
     * not expired yet, it just returns the existing browser of the asset list.
     * Otherwise, the topic will be inserted with the timestamp and the ttl.
     * Upon success, it returns a Browser for the list of assets, or null in
     * case of bad topic.  With the return Browser, it lists all the asset IDs
     * that match the topic. 
     */
    public Browser insertTopic(String topic, long timestamp, int ttl,
        int[] meta) {
        QList subList = null;
        if (topic == null || topic.length() <= 0)
            return null;
        if (cache.containsKey(topic) && !cache.isExpired(topic, timestamp))
            subList = (QList) cache.get(topic, timestamp);
        else {
            subList = classify(topic);
            cache.insert(topic, timestamp, ttl, meta, subList, timestamp);
        }
        return subList.browser();
    }

    /**
     * As long as the topic exists, it updates the timestamp, ttl and metadata
     * no matter whether the topic has expired or not.  It returns 0 for if the
     * topic has not expired yet, or 1 if the topic has expired.  If the topic
     * does not exist, it just returns -1 for failure.
     */
    public int updateTopic(String topic, long timestamp, int ttl, int[] meta) {
        return cache.update(topic, timestamp, ttl, meta, cache.get(topic));
    }

    /**
     * It expires the topic and returns 0 if the ttl is larger than zero.
     * For an evergreen topic, it just returns 1 with no changes to its state.
     * If the topic does not exist, it returns -1.  The expiration action will
     * not modify any metadata.
     */
    public int expire(String topic, long currentTime) {
        return cache.expire(topic, currentTime);
    }

    /**
     * It updates mtime of the topic if it has not expired yet.  It retruns 0
     * on success, or -1 if the topic is not found or 1 if the topic has
     * expired already.
     */
    public int touch(String topic, long timestamp) {
        return cache.touch(topic, timestamp);
    }

    /**
     * It updates mtime of the topic with a given reference time for expiration.
     * It retruns 0 on success, or -1 if the topic is not found or 1 if the
     * topic has expired already.
     */
    public int touch(String topic, long timestamp, long currentTime) {
        return cache.touch(topic, timestamp, currentTime);
    }

    /**
     * It returns true if the topic has expired, or false otherwise.
     * It throws IllegalArgumentException if the topic is not found.
     */
    public boolean isExpired(String topic, long currentTime) {
        return cache.isExpired(topic, currentTime);
    }

    /**
     * It only checks the existence of the topic no matter whether it has
     * expired or not.
     */
    public boolean containsTopic(String topic) {
        return cache.containsKey(topic);
    }

    /**
     * returns the total number of cached topics.
     */
    public int getTopicCount() {
        return cache.size();
    }

    /**
     * returns the number of cached topics associated to an given asset.
     */
    public int getTopicCount(int id) {
        if (id >= 0 && id < capacity)
            return topicCount[id];
        else
            return -1;
    }

    /**
     * returns the ttl of the topic.
     */
    public int getTTL(String topic) {
        return cache.getTTL(topic);
    }

    /**
     * returns the timestamp of the topic.
     */
    public long getTimestamp(String topic) {
        return cache.getTimestamp(topic);
    }

    /**
     * returns the access count of the topic.
     */
    public long getCount(String topic) {
        return cache.getCount(topic);
    }

    /**
     * It removes all expired topics, resets all sequence numbers and updates
     * topic counts for each associated asset.  It returns an array of all
     * expired topics that have been just removed.
     */
    public String[] disfragment(long currentTime) {
        String[] keys = cache.disfragment(currentTime);
        String key;
        Pattern pattern;
        int i, j, k, n, id;
        k = keys.length;
        if (k > 0) { // some expired topics removed 
            n = patternList.length;
            for (i=0; i<n; i++) {
                id = patternList[i];
                pattern = (Pattern) list.browse(id);
                for (j=0; j<k; j++) {
                    key = keys[j];
                    if (key == null || key.length() <= 0)
                        continue;
                    if (pm.contains(key, pattern)) {
                        topicCount[id] --;
                    }
                }
            }
        }
        return keys;
    }

    /**
     * returns an array of all existing topics in the NATURAL order.
     */
    public String[] sortedTopics() {
        return cache.sortedKeys();
    }

    /**
     * returns an Iterator for all cached topics no matter whether they have
     * expired or not.
     */
    public Set<String> topicSet() {
        return cache.keySet();
    }

    /**
     * It checks object and the existence of the key first.  If no such key
     * exists,  it adds the object and the metadata to the list.  It returns
     * the unique ID upon success or -1 otherwise.
     */
    public int add(String key, long[] meta, Pattern pattern, Object obj) {
        int id = add(key, meta, obj);
        if (id >= 0) { // object and metadata added so replace the null pattern
            list.takeback(id);
            list.reserve(id);
            list.add(pattern, id);
            int n = updateCache(id, pattern);
            if (n > 0)
                topicCount[id] += n;
        }
        return id;
    }

    /** overrides the default behavior and adds a null as the pattern */
    public int add(String key, long[] meta, Object obj) {
        // invoke the overriden method only
        int id = super.add(key, meta, obj);
        if (id >= 0) {
            list.reserve(id);
            list.add(null, id);
            int n = size();
            int[] tmp = new int[n];
            queryIDs(tmp);
            patternList = tmp;
        }
        return id;
    }

    /**
     * It checks object and the existence of the key first.  If no such key
     * exists,  it reserves the id-th cell from the list and adds the object
     * and the metadata to the cell with the index of id.  It returns the id
     * upon success or -1 otherwise.
     */
    public int add(String key, long[] meta, Pattern pattern, Object obj,
        int id) {
        int i = add(key, meta, obj, id);
        if (i >= 0) { // object and metadata added so replace the null pattern
            list.takeback(i);
            list.reserve(i);
            list.add(pattern, i);
            int n = updateCache(i, pattern);
            if (n > 0)
                topicCount[i] += n;
        }
        return i;
    }

    /** overrides the default behavior and adds a null as the pattern */
    public int add(String key, long[] meta, Object obj, int id) {
        // invoke the overriden method only
        int i = super.add(key, meta, obj, id);
        if (i >= 0) {
            list.reserve(i);
            list.add(null, i);
            int n = size();
            int[] tmp = new int[n];
            queryIDs(tmp);
            patternList = tmp;
        }
        return i;
    }

    /**
     * It checks the object and the existence of the key first.  If no such
     * key exists,  it inserts the object and the metadata to the list right
     * before the object with the id.  It returns the unique ID upon success
     * or -1 otherwise.
     */
    public int insert(String key, long[] meta, Pattern pattern, Object obj,
        int id) {
        int i = insert(key, meta, obj, id);
        if (i >= 0) { // object and metadata added so replace the null pattern
            list.takeback(i);
            list.reserve(i);
            list.add(pattern, i);
            int n = updateCache(i, pattern);
            if (n > 0)
                topicCount[i] += n;
        }
        return i;
    }

    /** overrides the default behavior and inserts a null as the pattern */
    public int insert(String key, long[] meta, Object obj, int id) {
        // invoke the overriden method only
        int i = super.insert(key, meta, obj, id);
        if (i >= 0) {
            list.reserve(i);
            list.add(null, i);
            int n = size();
            int[] tmp = new int[n];
            queryIDs(tmp);
            patternList = tmp;
        }
        return i;
    }

    /**
     * It removes both the object of the key and its metadata.  It also cleans
     * up the list on all the cached topics.  It returns the removed object
     * upon success or null otherwise.
     */
    public Object remove(String key) {
        int id = getID(key);
        if (id >= 0) { // key exists
            // invoke the overriden method only
            Object o = super.remove(key);
            list.takeback(id);
            int n = size();
            int[] tmp = new int[n];
            queryIDs(tmp);
            patternList = tmp;
            cleanupCache(id);
            topicCount[id] = 0;
            return o;
        }

        return null;
    }

    /**
     * It removes both the object of the id and its metadata.  It also cleans
     * up the list on all the cached topics.  It returns the removed object
     * upon success or null otherwise.
     */
    public Object remove(int id) {
        return remove(getKey(id));
    }

    /**
     * It returns the pattern for the id-th asset
     */
    public Pattern getPattern(int id) {
        return (Pattern) list.browse(id);
    }

    /**
     * It returns the pattern for the asset with the key
     */
    public Pattern getPattern(String key) {
        return (Pattern) list.browse(getID(key));
    }

    /**
     * returns a Browser for all asset IDs associated with the topic or
     * null if the topic does not exist or has expired by the time.
     */
    public Browser browser(String topic, long currentTime) {
        Object o = cache.get(topic, currentTime);
        if (o != null)
            return ((QList) o).browser();
        else
            return null;
    }

    public void clear() {
        // invoke the overriden method only
        super.clear();
        cache.clear();
        list.clear();
        patternList = new int[0];
        for (int i=0; i<capacity; i++)
            topicCount[i] = 0;
    }

    protected void finalize() {
        clear();
    }
}
