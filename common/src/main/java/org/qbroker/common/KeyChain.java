package org.qbroker.common;

/* KeyChain.java - a sequential cache for objects with sorting support */

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * KeyChain contains an array of the sequence keys, timestamp, metaData and
 * objects.  The sequence key is stored in an array with the internal index
 * to the object and the timestamp. The sequnce key can be one of the data
 * types of string, int, long, float or double number. KeyChain has also the
 * support for the meataData as an option so that the extra information can be
 * stored to a separate array together with each object. The metadata is an
 * int array. KeyChain has a public method of sort(). It can be used to sort
 * the objects in the ascending order according to their sequence keys.  The
 * index of the keys may not be same as their internal index to the object.
 *<br><br>
 * <table>
 * <caption>Key Type</caption>
 * <tr><th>Key Type</th><th>Data Type</th></tr>
 * <tr><td>KEY_STRING</td><td>String</td></tr>
 * <tr><td>KEY_INT</td><td>int</td></tr>
 * <tr><td>KEY_LONG</td><td>long</td></tr>
 * <tr><td>KEY_FLOAT</td><td>float</td></tr>
 * <tr><td>KEY_DOUBLE</td><td>double</td></tr>
 * <tr><td>KEY_TIME</td><td>time</td></tr>
 * <tr><td>KEY_SEQUENCE</td><td>sequence number</td></tr>
 * </table>
 *<br><br>
 * This is NOT MT-Safe.  Therefore please use it with care.
 *<br>
 * @author yannanlu@yahoo.com
 */

public abstract class KeyChain implements Comparator {
    private String name;             // name of the KeyChain
    private int capacity;            // maximum number of elements
    private final int keyType;       // type of the sequence keys
    protected int size;              // number of active keys
    protected long ctime;            // ctime for the KeyChain
    protected long mtime;            // mtime for the KeyChain
    protected boolean isSorted;      // true if without order change since sort
    protected long[] ts;             // array of timestamp for objects
    protected ArrayList<Object> elements;    // list of Objects
    protected ArrayList<int[]> metadata;     // list of MetaData

    public final static int KEY_STRING = 0;
    public final static int KEY_INT = 1;
    public final static int KEY_LONG = 2;
    public final static int KEY_FLOAT = 3;
    public final static int KEY_DOUBLE = 4;
    public final static int KEY_TIME = 5;
    public final static int KEY_SEQUENCE = 6;

    public KeyChain(String name, int type, int capacity) {
        if (name == null)
            throw(new IllegalArgumentException("name is null"));
        this.name = name;
        if (type >= KEY_STRING && type <= KEY_SEQUENCE)
            keyType = type;
        else
            throw(new IllegalArgumentException("type not supported: "+type));

        elements = new ArrayList<Object>();
        metadata = new ArrayList<int[]>();
        if (capacity > 0)
            this.capacity = capacity;
        else
            this.capacity = 512;

        ts = new long[capacity];
        for (int i=0; i<capacity; i++)
            ts[i] = 0;

        size = 0;
        ctime = 0;
        mtime = 0;
        isSorted = false;
    }

    /**
     * returns capacity of the cache
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * returns total number of the objects
     */
    public int size() {
        return size;
    }

    /**
     * returns number of the objects whose keys have been removed
     */
    public int removable() {
        return elements.size() - size;
    }

    public String getName() {
        return name;
    }

    public int getKeyType() {
        return keyType;
    }

    /** It returns the ctime of the chain */
    public long getCTime() {
        return ctime;
    }

    /** It returns the mtime of the chain */
    public long getMTime() {
        return mtime;
    }

    /**
     * It adds the key and the object to the chain, and returns total number
     * of keys in the chain.  If the chain is in the sorted order, it will
     * break the order of keys.
     */
    public abstract int add(Object key, long mtime, int[] meta, Object obj);

    /**
     * It inserts the key and the object to the sorted chain and keeps the
     * sorted order.  It returns total number of keys in the chain.
     */
    public abstract int insert(Object key, long mtime, int[] meta, Object obj);

    /**
     * It removes the key at the given index from the chain and disables its
     * metadata and the object so that they will not be available any more.
     * However, they still take space in the chain.  To delete it permanently,
     * the method of truncate() must be called.  It returns the object upon
     * success or null otherwise.
     */
    public abstract Object remove(int i);

    /**
     * It returns the object of the key at the given index or null if the
     * index is out of bound.
     */
    public abstract Object get(int i);

    /**
     * It returns the timestamp for the object at the given index or -1
     * if the index is out of bound.
     */
    public abstract long getTimestamp(int i);

    /**
     * It returns the metaData of the key at the given index or null if the
     * index is out of bound.
     */
    public abstract int[] getMetaData(int i);

    /**
     * It returns the key in its data type at the given index or null if the
     * index is out of bound.
     */
    public abstract Object getKey(int i);

    /**
     * It looks for the given key in a sorted instance of KeyChain.  If the
     * key is found, the last index of the key will be returned.  Otherwise,
     * it returns a negative number.  For a negative return value of r, the
     * index of -(r+1) specifies the index at which the specified key can be
     * inserted to maintain the sorted order.
     */
    public abstract int find(Object key);

    /** It sorts the array of keys in ascending order. */
    public abstract int sort();

    /**
     * It assumes the key chain has been sorted in ascending order. Then it
     * looks for the last fully baked key. Upon found the key, it returns the
     * number of keys with lower values or same value as compared to the last
     * fully baked key. Otherwise, it returns 0.
     */
    public abstract int locate(long currentTime, int bakeTime);

    /**
     * It truncates the chain by deleting the first given number of keys
     * permanently and returns the number of keys deleted. It assumes all
     * keys have already been sorted. If there are active keys left, it will
     * also reset the ctime to that of the most baked key. If k is less than or
     * equal to 0, it will delete all removed keys.
     */
    public abstract int truncate(int k);

    /** It cleans up the chain.  */
    public void clear() {
        size = 0;
        ctime = 0;
        mtime = 0;
        elements.clear();
        metadata.clear();
        for (int i=0; i<capacity; i++)
            ts[i] = 0;
        isSorted = false;
    }

    protected void finalize() {
        clear();
    }

    /** It compares thekeys of a and b in the ascending order */
    public abstract int compare(Object a, Object b);

    public boolean equals(Object o) {
        return (this == o);
    }
}
