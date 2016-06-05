package org.qbroker.common;

/* AssetList.java - a dynamic list for objects with MetaData and unique keys */

import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.QList;

/**
 * AssetList is a list of assets with the unique keys and IDs.  Each asset has
 * a well-defined object (not null) and its MetaData which is an opaque
 * array of longs.  It is up to the developer to decide what consists of
 * the MetaData. For example, the gid, ttl or option can be stored in the
 * MetaData for better descriptions of the asset.  The range of IDs is
 * from 0 thru capacity - 1.
 *<br/><br/>
 * This class is only a data container.  For access efficiency, there is
 * no protection on data integrity.  It is NOT MT-Safe either.  Therefore,
 * please use it with care.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class AssetList {
    private String name;             // name of the list
    private int capacity;            // capacity of the list
    private HashMap<String, Object> cache;  // hashmap for the metadata
    private QList list;              // list of assets

    public AssetList(String name, int capacity) {
        if (name == null)
            throw(new IllegalArgumentException("name is null"));
        this.name = name;
        cache = new HashMap<String, Object>();
        list = new QList(name, capacity);
        this.capacity = list.getCapacity();
    }

    public int size() {
        return list.size();
    }

    public String getName() {
        return name;
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * It checks object and the existence of the key first.  If no such key
     * exists,  it adds the object and the metadata to the list.  It returns
     * the unique ID upon success or -1 otherwise.
     */
    public int add(String key, long[] meta, Object obj) {
        int id = -1;
        if (obj != null && !cache.containsKey(key) &&
            (id = list.reserve()) >= 0) { // new object
            if (meta == null)
                meta = new long[0];
            cache.put(key, new Object[]{new int[]{id}, meta});
            list.add(new Object[]{key, obj}, id);
        }
        return id;
    }

    /**
     * It checks object and the existence of the key first.  If no such key
     * exists,  it reserves the id-th cell from the list and adds the object
     * and the metadata to the cell with the index of id.  It returns the id
     * upon success or -1 otherwise.
     */
    public int add(String key, long[] meta, Object obj, int id) {
        int i = -1;
        if (obj != null && !cache.containsKey(key) &&
            (i = list.reserve(id)) >= 0) { // new object
            if (meta == null)
                meta = new long[0];
            cache.put(key, new Object[]{new int[]{id}, meta});
            list.add(new Object[]{key, obj}, id);
        }
        return i;
    }

    /**
     * It checks the object and the existence of the key first.  If no such
     * key exists,  it inserts the object and the metadata to the list right
     * before the object with the id.  It returns the unique ID upon success
     * or -1 otherwise.
     */
    public int insert(String key, long[] meta, Object obj, int id) {
        int jd = -1;
        if (list.getType(id) != XQueue.CELL_OCCUPIED)
            return -2;
        jd = add(key, meta, obj);
        if (jd >= 0) { // new object added sucessfully
            int i, j = -1;
            Browser b = list.browser();
            while ((i=b.next())>=0) {
                if (j >= 0) { // swap
                    if (j == i) // reach the end
                        break;
                    list.swap(i, j);
                    j = i;
                }
                else if (i == id) { // found the starting point
                    list.swap(i, jd);
                    j = i;
                }
            }
            if (j <= 0) {
                remove(jd);
                jd = -3;
            }
        }
        return jd;
    }

    /**
     * swaps two objects and their metadata identified by id and jd
     * and returns 1 for success, 0 for failure or -1 for out of bound.
     */
    public int swap(int id, int jd) {
        return list.swap(id, jd);
    }

    /**
     * rotates the object with the id and its metadata to the end of the list
     * and returns 1 for success, 0 for failure or -1 for out of bound.
     */
    public int rotate(int id) {
        return list.rotate(id);
    }

    /**
     * It checks the id first.  If the id is verified, it replaces the object.
     * It returns the original object upon success or null otherwise.
     */
    public Object set(int id, Object obj) {
        if (obj != null && XQueue.CELL_OCCUPIED == list.getType(id)) {
            Object[] a;
            Object o;
            a = (Object[]) list.browse(id);
            o = a[1];
            a[1] = obj;
            return o;
        }
        return null;
    }

    /**
     * It checks the object and the existence of the key first.  If the key
     * exists, it replaces the object.  It returns the original object upon
     * success or null otherwise.
     */
    public Object set(String key, Object obj) {
        return set(getID(key), obj);
    }

    /**
     * returns the id of the object on the key or null if no object on the key
     */
    public int getID(String key) {
        int id = -1;
        if (cache.containsKey(key)) { // key exists
            Object[] o = (Object[]) cache.get(key);
            id = ((int[]) o[0])[0];
        }
        return id;
    }

    /**
     * returns the key of the id or null if no object at the id
     */
    public String getKey(int id) {
        String key = null;
        if (XQueue.CELL_OCCUPIED == list.getType(id)) { // id is occupied
            Object[] o = (Object[]) list.browse(id);
            key = (String) o[0];
        }
        return key;
    }

    /**
     * replaces the key of the id and returns the original key upon success.
     * Otherwise, it returns null to indicate failure.
     */
    public String replaceKey(int id, String key) {
        if (key == null || key.length() <= 0 || cache.containsKey(key))
            return null;
        if (XQueue.CELL_OCCUPIED == list.getType(id)) { // id is occupied
            Object[] o = (Object[]) list.browse(id);
            String str = (String) o[0];
            o[0] = key;
            cache.put(key, cache.remove(str));
            return str;
        }
        else
            return null;
    }

    /**
     * returns the object of the key
     */
    public Object get(int id) {
        if (XQueue.CELL_OCCUPIED == list.getType(id)) { // id is occupied
            Object[] o = (Object[]) list.browse(id);
            return o[1];
        }

        return null;
    }

    /**
     * returns the object of the key
     */
    public Object get(String key) {
        return get(getID(key));
    }

    /**
     * removes both the object of the key and its metadata.  It returns the
     * removed object upon success or null otherwise.
     */
    public Object remove(String key) {
        int id = getID(key);
        if (id >= 0) { // key exists
            cache.remove(key);
            Object[] o = (Object[]) list.browse(id);
            list.getNextID(id);
            list.remove(id);
            return o[1];
        }

        return null;
    }

    /**
     * removes both the object of the id and its metadata.  It returns the
     * removed object upon success or null otherwise.
     */
    public Object remove(int id) {
        return remove(getKey(id));
    }

    /**
     * returns true if the the list contains the key
     */
    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    /**
     * returns true if the assset for the id exists
     */
    public boolean existsID(int id) {
        return (list.getType(id) == XQueue.CELL_OCCUPIED);
    }

    /**
     * returns the metadata for the object referenced by the key
     */
    public long[] getMetaData(String key) {
        if (cache.containsKey(key)) {
            Object[] o = (Object[]) cache.get(key);
            return (long[]) o[1];
        }
        return null;
    }

    /**
     * returns the metadata for the object of id
     */
    public long[] getMetaData(int id) {
        return getMetaData(getKey(id));
    }

    /**
     * returns number of assets after loading the ids to the buffer in natural
     * order
     */
    public int queryIDs(int[] ids) {
        return list.queryIDs(ids, XQueue.CELL_OCCUPIED);
    }

    /* returns true if the list is full or false otherwise */
    public boolean isFull() {
        return list.isFull();
    }

    /**
     * returns a Browser for all IDs in the natural order
     */
    public Browser browser() {
        return list.browser();
    }

    public void clear() {
        cache.clear();
        list.clear();
    }

    protected void finalize() {
        clear();
    }
}
