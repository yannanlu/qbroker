package org.qbroker.common;

/* HashChain.java - a KeyChain with consistent hash spport */

import java.util.Arrays;
import java.util.Comparator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.qbroker.common.AssetList;

/**
 * HashChain contains a list of the unique keys, and the sorted integer values
 * hashed from the replicas of these keys. These integer values have divided
 * the entire number axis into plenty of intervals. Each key will have many
 * intervals due to its replicas.  But any interval belongs only to a specific
 * key.  Therefore, HashChain can be used to map a message to one of the
 * keys in a consistent way. The method of add() and remove() are used to
 * add/remove a key to/from HashChain.  The method of map() returns the id
 * of a specific key for the message. The number of the replicas is predfined.
 *<br><br>
 * The default hash method is MD5.
 *<br><br>
 * This is NOT MT-Safe.  Therefore please use it with care.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class HashChain implements Comparator<int[]> {
    private int size = 0;
    private int capacity = 1024;
    private int numOfReplica = 256;
    private int[][] intChain;
    private AssetList keyList;
    private MessageDigest md;

    public HashChain(String name, int capacity, int numOfReplica) {
        int n;
        if (capacity > 0)
            this.capacity = capacity;
        if (numOfReplica > 0)
            this.numOfReplica = numOfReplica;
        keyList = new AssetList(name, capacity);
        n = this.numOfReplica * this.capacity;
        intChain = new int[n][2];
        try {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            throw(new IllegalArgumentException(e.toString()));
        }
    }

    public HashChain(String name, int capacity) {
        this(name, capacity, 0);
    }

    public HashChain(String name) {
        this(name, 0, 0);
    }

    /**
     * It adds the key to the KeyChain and returns its id
     */
    public int add(String key, int weight) {
        int i, j, k, m, n, x, id;
        String str;
        Integer o;
        if (key == null || keyList.containsKey(key))
            return -1;
        id = keyList.add(key, null, new Integer(0));
        n = 0;
        if (weight <= 0)
            weight = 1;
        m = weight * numOfReplica;
        for (i=0; i<m; i++) {
            str = key + "_" + i;
            x = hash(str);
            k = find(x);
            if (k >= 0) { // got a conflict
                str = keyList.getKey(intChain[k][1]);
                if (key.compareTo(str) > 0) { 
                    j = intChain[k][1];
                    intChain[k][1] = id;
                    n ++;
                    o = (Integer) keyList.get(j);
                    keyList.set(j, new Integer(o.intValue()-1));
                }
            }
            else { // append to the chain
                intChain[size+n][0] = x;
                intChain[size+n][1] = id;
                n ++;
            }
        }
        if (n <= 0) {
            keyList.remove(id);
            id = -1;
        }
        else
            keyList.set(id, new Integer(n));
        size += n;
        // sort the chain
        Arrays.sort(intChain, 0, size, this);
        return id;
    }

    public int add(String key) {
        return add(key, 1);
    }

    /**
     * It removes the key from the HashChain and returns its id
     */
    public int remove(String key) {
        int id, n;
        if (key == null || !keyList.containsKey(key))
            return -1;
        id = keyList.getID(key);
        n = delete(id);
        keyList.remove(id);
        return id;
    }

    /**
     * It maps the msg to a key and returns its id upon success or -1 otherwise
     */
    public int map(String msg) {
        int k, x;
        if (size < 0 || msg == null)
            return -1;
        x = hash(msg);
        k = find(x);
        if (k < 0)
            k = -(k+1);
        if (k >= size) // wrapped to the first
            k = 0;

        return intChain[k][1];
    }

    /** returns the hash code for the msg */
    private int hash(String msg) {
        byte[] b;
        if (msg == null)
            throw(new IllegalArgumentException("null msg"));
        md.reset();
        md.update(msg.getBytes());
        b = md.digest();
        return (((int) (b[3] & 0xff) << 24) | ((int) (b[2] & 0xff) << 16) |
                   ((int) (b[1] & 0xff) << 8) | (int) (b[0] & 0xff));
    }

    /** returns the measure in the HashChain for the given node */
    public int getMeasure(String key) {
        Integer o = (Integer) keyList.get(key);
        if (o != null)
            return o.intValue();
        else
            return 0;
    }

    public String getKey(int id) {
        return keyList.getKey(id);
    }

    public int getID(String key) {
        return keyList.getID(key);
    }

    /** returns a browser for all keys */
    public Browser browser() {
        return keyList.browser();
    }

    public int getCapacity() {
        return keyList.getCapacity();
    }

    public int getSize() {
        return keyList.size();
    }

    public void clear() {
        keyList.clear();
        size = 0;
    }

    /**
     * It deletes the hash values for all the replicas on the given id in
     * the HashChain and returns the number of replicas deleted.
     */
    private int delete(int id) {
        int i, j, k, n;
        if (id < 0 || id >= size)
            return 0;

        n = 0;
        k = 0;
        for (i=0; i<size; i++) {
            if (intChain[i][1] == id) { // delete it;
                n ++;
            }
            else if (i > k) {
                intChain[k][0] = intChain[i][0];
                intChain[k][1] = intChain[i][1];
                k ++;
            }
            else {
                k ++;
            }
        }
        size = k;
        return n;
    }

    /**
     * It looks for the given number in a sorted instance of HashChain.  If the
     * number is found, the index of the number will be returned.  Otherwise,
     * it returns a negative number.  For a negative return value of r, the
     * index of -(r+1) specifies the index at which the specified number can be
     * inserted to maintain the sorted order.
     */
    private int find(int xx) {
        int i, j, k, n;
        int x;
        if (size == 0)
            return -1;
        n = size;
        i = 0;
        x = intChain[i][0];
        if (x > xx) // xx is not in chain
            i = -1;
        else if (x < xx) { // check the right end 
            i = n - 1;
            x = intChain[i][0];
            if (x < xx) // xx is not in chain
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
                    x = intChain[k][0];
                    if (x < xx) {
                        i = k;
                        k += (int) ((j - i) * 0.5);
                    }
                    else if (x > xx) {
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
                if (xx != intChain[i][0]) // xx is not in chain 
                    i = -(i + 1);
            }
        }
        return i;
    }


    /** It compares a and b in the ascending order */
    public int compare(int[] a, int[] b) {
        if (a[0] > b[0])
            return 1;
        else if (a[0] < b[0])
            return -1;
        else
            return 0;
    }

    public void dump() {
        for (int i=0; i<size; i++) {
            System.out.println(i + ": " + intChain[i][1] + " "+intChain[i][0]);
        }
    }

    public static void main(String args[]) {
        HashChain hash;
        int k, n = 10, c = 256;
        int[] dist;
        String filename = null, action = "parse", key = "", str, msg;
        for (int i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'a':
                action = args[++i];
                break;
              case 'n':
                n = Integer.parseInt(args[++i]);
                break;
              case 'c':
                c = Integer.parseInt(args[++i]);
                break;
              case 'k':
                key = args[++i];
                break;
              case 'f':
                if (i+1 < args.length)
                    filename = args[++i];
                break;
              default:
            }
        }

        dist = new int[n];
        msg = "this is a test";

        try {
            hash = new HashChain(key, n, c);
            for (int i=0; i<n; i++) {
                dist[i] = 0;
                str = i + key;
                hash.add(str);
            }
            for (int i=0; i<10000; i++) {
                str = msg + "/" + i;
                k = hash.map(str);
                dist[k] ++;
            }
            for (int i=0; i<n; i++) {
                System.out.println(i + ": " + dist[i] + " / " +
                    hash.getMeasure(hash.getKey(i)));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("HashChain Version 1.0 (written by Yannan Lu)");
        System.out.println("HashChain: manage a consistent hash chain");
        System.out.println("Usage: java org.qbroker.common.HashCahin -f file name -a action -t type -k path");
        System.out.println("  -?: print this message");
        System.out.println("  -a: action of parse, test, count, locate or get (default: parse)");
        System.out.println("  -k: key (example: /views/list/0/name)");
        System.out.println("  -n: number of keys (default: 10)");
        System.out.println("  -c: number of replicas (default: 256)");
        System.out.println("  -f: filename");
    }
}
