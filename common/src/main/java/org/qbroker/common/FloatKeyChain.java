package org.qbroker.common;

/* FloatKeyChain.java - a sequential cache for objects with sorting support */

import java.util.Arrays;
import java.util.Comparator;

/**
 * FloatKeyChain contains an array of the sequence keys, timestamp, metadata
 * and objects. The sequence key is stored in an array with the internal index
 * to the object and the timestamp. The data type of the sequence key is Float.
 * FloatKeyChain has also the support for metadata as an option so that extra
 * information can be stored to a separate array together with each object.
 * The metadata is an int array. FloatKeyChain has a public method of sort().
 * It sorts the objects in the ascending order according to their sequence
 * keys. The index of the keys may not be same as their internal index to
 * the object.
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore please use it with care.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FloatKeyChain extends KeyChain {
    private float[][] floatChain;    // keyChain of float numbers

    public FloatKeyChain(String name, int capacity) {
        super(name, KEY_FLOAT, capacity);
        floatChain = new float[capacity][2];
    }

    /**
     * It adds the key and the object to the chain, and returns total number
     * of keys in the chain.  If the chain is in the sorted order, it will
     * break the order of keys.
     */
    public int add(Object key, long currentTime, int[] meta, Object obj) {
        if (key == null || !(key instanceof Float))
            throw new IllegalArgumentException("bad key");
        return add(((Float) key).floatValue(), currentTime, meta, obj);
    }

    /**
     * It adds the key and the object to the chain, and returns total number
     * of keys in the chain.  If the chain is in the sorted order, it will
     * break the order of keys.
     */
    public int add(float key, long currentTime, int[] meta, Object obj) {
        int n = elements.size();
        elements.add(obj);
        metadata.add(meta);
        floatChain[n][0] = key;
        floatChain[n][1] = n + (float) 0.1;
        ts[n] = currentTime;
        mtime = currentTime;
        if (size == 0) {
            ctime = mtime;
            isSorted = true;
        }
        else if (isSorted)
            isSorted = false;
        if (meta == null)
            meta = new int[0];
        metadata.add(meta);
        size ++;
        return size;
    }

    /**
     * It inserts the key and the object to the sorted chain and keeps the
     * sorted order.  It returns total number of keys in the chain.
     */
    public int insert(Object key, long currentTime, int[] meta, Object obj) {
        int i, j, n;
        float a;
        if (key == null || !(key instanceof Float))
            throw new IllegalArgumentException("bad key");
        a = ((Float) key).floatValue();
        n = elements.size();
        i = find(key);
        if (i < 0) // insert the key at -(i+1)
            i = -(i+1);
        for (j=n; j>i; j--) { // shuffle to the right
            floatChain[j][0] = floatChain[j-1][0];
            floatChain[j][1] = floatChain[j-1][1];
        }
        elements.add(obj);
        floatChain[i][0] = a;
        floatChain[i][1] = n;
        ts[n] = currentTime;
        mtime = currentTime;
        if (size == 0) {
            ctime = mtime;
            isSorted = true;
        }
        if (meta == null)
            meta = new int[0];
        metadata.add(meta);
        size ++;
        return size;
    }

    /**
     * It removes the key at the given index from the chain and disables its
     * metadata and the object so that they will not be available any more.
     * However, they still take space in the chain.  To delete it permanently,
     * the method of truncate() must be called.  It returns the object upon
     * success or null otherwise.
     */
    public Object remove(int i) {
        int j, k;
        Object o;
        if (i < 0 || i >= size)
            return null;

        k = (int) floatChain[i][1];
        floatChain[i][1] = -1 - (float) 0.1;
        if (i < size - 1) { // move it to the end
            float a, b;
            a = floatChain[i][0];
            b = floatChain[i][1];
            for (j=i+1; j<size; j++) { // shuffle to the left
                floatChain[j-1][0] = floatChain[j][0];
                floatChain[j-1][1] = floatChain[j][1];
            }
            floatChain[size-1][0] = a;
            floatChain[size-1][1] = b;
        }
        if (k >= 0) {
            o = elements.set(k, null);
            metadata.set(k, null);
            ts[k] = -1;
            size --;
            return o;
        }
        return null;
    }

    /**
     * It returns the object of the key at the given index or null if the
     * index is out of bound.
     */
    public Object get(int i) {
        if (i < 0 || i >= size)
            return null;
        i = (int) floatChain[i][1];
        if (i >= 0 && i < elements.size())
            return elements.get(i);
        else
            return null;
    }

    /**
     * It returns the metadata of the key at the given index or null if the
     * index is out of bound.
     */
    public int[] getMetaData(int i) {
        if (i < 0 || i >= size)
            return null;
        i = (int) floatChain[i][1];
        if (i >= 0 && i < metadata.size())
            return metadata.get(i);
        else
            return null;
    }

    /**
     * It returns the timestamp of the key at the given index or -1 if the
     * index is out of bound.
     */
    public long getTimestamp(int i) {
        if (i < 0 || i >= size)
            return -1;
        i = (int) floatChain[i][1];
        if (i >= 0 && i < elements.size())
            return ts[i];
        else
            return -1;
    }

    /**
     * It returns the key in its data type at the given index or null if the
     * index is out of bound.
     */
    public Object getKey(int i) {
        if (i < 0 || i >= size)
            return null;
        return new Float(floatChain[i][0]);
    }

    /**
     * It looks for the given key in a sorted instance of KeyChain.  If the
     * key is found, the last index of the key will be returned.  Otherwise,
     * it returns a negative number.  For a negative return value of r, the
     * index of -(r+1) specifies the index at which the specified key can be
     * inserted to maintain the sorted order.
     */
    public int find(Object key) {
        int i, j, k, n;
        float a, aa;
        if (key == null || !(key instanceof Float))
            throw new IllegalArgumentException("bad key");
        if (size == 0)
            return -1;
        else if (!isSorted)
            sort();

        n = size;
        i = 0;
        aa = ((Float) key).floatValue();
        a = floatChain[i][0];
        if (a == aa) { // found it, check next neighbour
            while (++i < n && aa == floatChain[i][0]);
            i --;
        }
        else if (a > aa) // aa is not in chain
            i = -1;
        else { // check the right end
            i = n - 1;
            a = floatChain[i][0];
            if (a == aa) // found it
                i = n - 1;
            else if (a < aa) // aa is not in chain
                i = -(n + 1);
            else { // start binary search
                j = n - 1;
                i = 0;
                k = i + (int) ((j - i) * 0.5);
                if (k == i) {
                    k ++;
                    i ++;
                }
                while (j > i) {
                    a = floatChain[k][0];
                    if (a < aa) {
                        i = k;
                        k += (int) ((j - i) * 0.5);
                    }
                    else if (a > aa) {
                        j = k;
                        k = i + (int) ((j - i) * 0.5);
                    }
                    else {
                        i = k;
                        break;
                    }
                    if (k == i) { // adjust for resolution
                        k ++;
                        i ++;
                    }
                }
                if (aa != floatChain[i][0]) // aa is not in chain
                    i = -(i + 1);
                else { // found it, check next neighbour
                    while (++i < n && aa == floatChain[i][0]);
                    i --;
                }
            }
        }
        return i;
    }

    /** It sorts the array of keys in ascending order.  */
    @SuppressWarnings("unchecked")
    public int sort() {
        if (size <= 0)
            return 0;
        else if (isSorted)
            return 0;
        else if (size == 1 && !isSorted) {
            isSorted = true;
            return 1;
        }

        Arrays.sort(floatChain, 0, size, this);
        isSorted = true;

        return size;
    }

    /**
     * It assumes the key chain has been sorted in ascending order. Then it
     * looks for the last fully baked key. Upon found the key, it returns the
     * number of keys with lower values or same value as compared to the last
     * fully baked key. Otherwise, it returns 0.
     */
    public int locate(long currentTime, int bakeTime) {
        int i, j, k, n;
        float a;

        if (bakeTime <= 0)
            return -1;

        n = size;
        if (n <= 0)
            return -1;
        else if (!isSorted)
            sort();

        for (i=n-1; i>=0; i--) { // look for the last fully baked key
            k = (int) floatChain[i][1];
            if (currentTime - ts[k] >= bakeTime) { // found it
                a = floatChain[i][0];
                while (++i < n && a == floatChain[i][0]); // move right
                break;
            }
        }

        return i;
    }

    /**
     * It truncates the chain by deleting the first given number of keys
     * permanently and returns the number of keys deleted.  It assumes all
     * keys have already been sorted. If there are active keys left, it will
     * also reset the ctime to that of the most baked key. If k <= 0, it will
     * delete all removed keys.
     */
    public int truncate(int k) {
        int i, j, m, n;
        int[] shift;
        n = elements.size();
        if (n <= 0 || !isSorted)
            return 0;

        if (size <= 0) { // just clean up
            clear();
            return 0;
        }

        if (k > 0) { // reset id to -1
            if (k > n)
                k = n;
            for (i=0; i<k; i++)
                floatChain[i][1] = -1 - (float) 0.1;
        }
        else
            k = 0;

        m = 0;
        shift = new int[n];
        Arrays.fill(shift, 1);
        for (i=k; i<size; i++) { // mark the objects
            j = (int) floatChain[i][1];
            if (j >= 0 && j < n)
                shift[j] = 0;
            else // count the violation
                m ++;
        }
        for (i=n-1; i>=0; i--) { // remove the objects
            if (shift[i] > 0) {
                elements.remove(i);
                metadata.remove(i);
            }
        }

        for (i=1; i<n; i++) { // sum the shifts and pack the ts
            if (shift[i] == 0) { // pack to the left
                shift[i] += shift[i-1];
                ts[i-shift[i]] = ts[i];
                ts[i] = -1;
            }
            else // skip
                shift[i] += shift[i-1];
        }

        if (k > 0) { // need to shuffle to the left
            size -= k;
            for (i=0; i<size; i++) { // compress the chain and adjust index
                floatChain[i][0] = floatChain[k+i][0];
                floatChain[i][1] = floatChain[k+i][1];
                j = (int) floatChain[i][1];
                floatChain[i][1] -= shift[j];
            }
        }
        else {
            for (i=0; i<size; i++) { // adjust index
                j = (int) floatChain[i][1];
                floatChain[i][1] -= shift[j];
            }
        }

        if (size > 0) { // reset ctime
            ctime = mtime;
            for (i=0; i<size; i++) { // reset ctime
                if (ts[i] < ctime)
                    ctime = ts[i];
            }
        }

        return m;
    }

    /** It compares a and b in the ascending order */
    @SuppressWarnings("unchecked")
    public int compare(Object a, Object b) {
        if (((float[]) a)[0] > ((float[]) b)[0])
            return 1;
        else if (((float[]) a)[0] < ((float[]) b)[0])
            return -1;
        else if (((float[]) a)[1] > ((float[]) b)[1])
            return 1;
        else if (((float[]) a)[1] < ((float[]) b)[1])
            return -1;
        else
            return 0;
    }
}
