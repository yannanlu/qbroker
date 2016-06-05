package org.qbroker.common;

/* LoadManager.java - it allocates loads among the available nodes */

import java.util.Comparator;
import java.util.Arrays;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.StringSubstitution;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.AssetList;
import org.qbroker.common.Browser;
import org.qbroker.common.Template;

/**
 * LoadManager manages generic work loads among the available nodes dynamically.
 * A work load can be a service, a task or a set of resources.  With the
 * given nodes, LoadManager dynamically associates work loads to the available
 * nodes according to the predefined policies and their weights.  All the work
 * loads are independent of each other.  Each active work load must be
 * allocated to one of the nodes.  A node can be occupied by a number of the
 * work loads.  But any active work load must not occupy two different nodes
 * at any given time.  There are multiple combinations for the work load
 * allocations.  But LoadManager will always find the unique allocations.
 * The allocation process is incremental and stateful.  The result depends on
 * its history and state. 
 *<br/><br/>
 * A work load has three properties, type, weight and pattern.  The type is
 * either preferred or sticky.  A preferred work load has its preferred node
 * defined via the pattern.  As long as there is a node matching the pattern is
 * available, its allocation is always straight forward.  If there is no
 * preferred node available, it will be degraded to sticky.  A sticky work load
 * has no preference with any node.  So its allocation is dynamically
 * determined by the state and the policies.  The weights of work loads are
 * used to calculate the NORM of various allocation layouts.  The most
 * favorable allocation will have the minimum impact to the existing
 * configuration and the maximum balance of work loads.  The pattern is used
 * to select preferable nodes.  For any sticky work loads, its pattern should
 * be set to a universal match.
 *<br/><br/>
 * A node has the only one property, capacity or maximum weight.  The capacity
 * determines the limit on total weight of the work loads.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class LoadManager implements Comparator<String> {
    private String name;                 // name of the list
    private AssetList loadList;          // list of work loads
    private AssetList nodeList;          // list of nodes
    private int capacity;                // capacity of the list
    private long norm = 0;
    private long previousNorm = 0;
    private long mtime;
    private Perl5Matcher pm = new Perl5Matcher();
    private Perl5Compiler pc = new Perl5Compiler();
    public static final int LOAD_OID = 0;
    public static final int LOAD_PID = 1;
    public static final int LOAD_TYPE = 2;
    public static final int LOAD_WEIGHT = 3;
    public static final int LOAD_STATUS = 4;
    public static final int LOAD_SIZE = 5;
    public static final int LOAD_COUNT = 6;
    public static final int LOAD_TIME = 7;
    public static final int NODE_GID = 0;
    public static final int NODE_WEIGHT = 1;
    public static final int NODE_MAX = 2;
    public static final int NODE_STATUS = 3;
    public static final int NODE_SIZE = 4;
    public static final int NODE_COUNT = 5;
    public static final int NODE_OPTION = 6;
    public static final int NODE_TIME = 7;
    public static final int LM_NODE = 0;
    public static final int LM_LOAD = 1;
    public static final int TYPE_NONE = 0;
    public static final int TYPE_STICKY = 1;
    public static final int TYPE_PREFERRED = 2;

    public LoadManager(String name, int capacity) {
        if (name == null)
            throw(new IllegalArgumentException("name is null"));
        this.name = name;
        loadList = new AssetList(name, capacity);
        nodeList = new AssetList(name, capacity);
        mtime = System.currentTimeMillis();
    }

    /** returns id for the node upon success or -1 otherwise */
    public int addNode(String key, int gid, long max, Object obj) {
        long[] meta;
        int i, id;
        if (key == null || key.length() <= 0 || obj == null)
            return -1;
        if (nodeList.containsKey(key))
            return -1;
        meta = new long[NODE_TIME+1]; 
        for (i=0; i<=NODE_TIME; i++)
            meta[i] = 0;
        meta[NODE_GID] = gid;
        meta[NODE_MAX] = max;
        meta[NODE_TIME] = System.currentTimeMillis();
        id = nodeList.add(key, meta, obj);
        return id;
    }

    /** returns an array containing unassigned work loads on the node */
    public int[] removeNode(String key) {
        Object o;
        Pattern pattern = null;
        long[] meta;
        int[] list = new int[0];
        String[] load = null;
        int i, id, k, weight;
        if (key == null || key.length() <= 0 || !nodeList.containsKey(key))
            return null;
        id = nodeList.getID(key);
        if (id < 0)
            return null;
        meta = nodeList.getMetaData(id);
        o = nodeList.remove(id);
        if (meta[NODE_SIZE] > 0 || meta[NODE_WEIGHT] > 0) {
            Browser browser = loadList.browser();
            k = (int) meta[NODE_SIZE];
            weight = (int) meta[LOAD_WEIGHT];
            list = new int[k];
            load = new String[k];
            k = 0;
            while ((i = browser.next()) >= 0) { // try to init PID
                meta = loadList.getMetaData(i);
                if (meta[LOAD_TYPE] == TYPE_PREFERRED && meta[LOAD_PID] == id)
                    meta[LOAD_PID] = -1;
                if (meta[LOAD_OID] == id) {
                    load[k] = loadList.getKey(i);
                    list[k++] = i;
                    meta[LOAD_OID] = -1;
                    meta[LOAD_TIME] = System.currentTimeMillis();
                }
            }
            previousNorm = norm;
            norm -= weight * weight;
        }

        if (load != null && load.length > 0 && nodeList.size() > 0) { // move
            k = load.length;
            Arrays.sort(load, this);
            for (int j=0; j<k; j++) { // for preferred loads
                if (load[j] == null || load[j].length() <= 0)
                    continue;
                i = loadList.getID(load[j]);
                meta = loadList.getMetaData(i);
                if (meta[LOAD_TYPE] != TYPE_PREFERRED)
                    continue;
                pattern = (Pattern) loadList.get(i);
                id = locate(i);
                if (id < 0)
                    continue;
                meta[LOAD_OID] = id;
                if (!pm.contains(nodeList.getKey(id), pattern))
                    continue;
                meta[LOAD_PID] = id;
                weight = (int) meta[LOAD_WEIGHT];
                meta = nodeList.getMetaData(id);
                meta[NODE_WEIGHT] += weight;
                meta[NODE_SIZE] ++;
                meta[NODE_TIME] = mtime;
                previousNorm = norm;
                norm += weight*(meta[NODE_WEIGHT] + meta[NODE_WEIGHT] - weight);
            }

            for (int j=0; j<k; j++) { // for sticky loads
                if (load[j] == null || load[j].length() <= 0)
                    continue;
                i = loadList.getID(load[j]);
                meta = loadList.getMetaData(i);
                if (meta[LOAD_PID] >= 0) // bypass preferred loads
                    continue;
                pattern = (Pattern) loadList.get(i);
                id = locate(i);
                if (id < 0)
                    continue;
                meta[LOAD_OID] = id;
                weight = (int) meta[LOAD_WEIGHT];
                meta = nodeList.getMetaData(id);
                meta[NODE_WEIGHT] += weight;
                meta[NODE_SIZE] ++;
                meta[NODE_TIME] = mtime;
                previousNorm = norm;
                norm += weight*(meta[NODE_WEIGHT] + meta[NODE_WEIGHT] - weight);
            }
        }

        return list;
    }

    /** returns the id of the load upon success or -1 otherwise */
    public int add(String key, int type, int weight, String patternStr) {
        long[] meta;
        Pattern pattern = null;
        int id, k;
        if (key == null || key.length() <= 0 || patternStr == null)
            return -1;
        if (loadList.containsKey(key))
            return -2;
        mtime = System.currentTimeMillis();
        meta = new long[LOAD_TIME+1]; 
        for (int i=0; i<=LOAD_TIME; i++)
            meta[i] = 0;
        meta[LOAD_TYPE] = type;
        meta[LOAD_WEIGHT] = weight;
        meta[LOAD_OID] = -1;
        meta[LOAD_PID] = -1;
        meta[LOAD_TIME] = mtime;
        try {
            pattern = pc.compile(patternStr);
        }
        catch (Exception e) {
        }
        id = loadList.add(key, meta, pattern);
        if (id < 0)
            return id;
        k = locate(id);
        if (k < 0)
            return id;
        meta[LOAD_OID] = k;
        if (type == TYPE_PREFERRED && pm.contains(nodeList.getKey(k), pattern))
            meta[LOAD_PID] = k;
        meta = nodeList.getMetaData(k);
        meta[NODE_WEIGHT] += weight;
        meta[NODE_SIZE] ++;
        meta[NODE_TIME] = mtime;
        previousNorm = norm;
        norm += weight *(meta[NODE_WEIGHT] + meta[NODE_WEIGHT] - weight);

        return id;
    }

    /** returns the id of the load upon success or -1 otherwise */
    public int remove(String key) {
        long[] meta;
        int id, k, w;
        if (key == null)
            return -2;
        id = loadList.getID(key);
        if (id < 0)
            return id;
        meta = loadList.getMetaData(id);
        w = (int) meta[LOAD_WEIGHT];
        k = (int) meta[LOAD_OID];
        loadList.remove(id);
        mtime = System.currentTimeMillis();
        if (k < 0)
            return id;
        meta = nodeList.getMetaData(k);
        if (meta != null) {
            previousNorm = norm;
            meta[NODE_WEIGHT] -= w;
            norm -= w * (meta[NODE_WEIGHT] + meta[NODE_WEIGHT] + w);
            meta[NODE_TIME] = mtime;
        }

        return id;
    }

    /** returns the id of the node with minumum weight or weight overflow */
    private int locate(int id) {
        Browser browser;
        Pattern pattern = null;
        String key;
        long[] meta;
        long w, w0 = Long.MAX_VALUE;
        int i, j = -1, k = -1, weight;
        if (nodeList.size() <= 0)
            return -1;
        if (nodeList.size() == 1)
            return nodeList.browser().next();
        weight = 0;
        if (id >= 0) { // for a specific load
            meta = loadList.getMetaData(id);
            if (meta != null) {
                weight = (int) meta[LOAD_WEIGHT];
                if (meta[LOAD_TYPE] == TYPE_PREFERRED)
                    pattern = (Pattern) loadList.get(id);
            }
        }
        if (pattern != null) { // look among all matched nodes first
            browser = nodeList.browser();
            while ((i = browser.next()) >= 0) {
                meta = nodeList.getMetaData(i);
                key = nodeList.getKey(i);
                if (!pm.contains(key, pattern)) // enforce pattern match
                    continue;
                if (meta[NODE_WEIGHT] <= 0 || meta[NODE_SIZE] <= 0) { // empty
                    k = i;
                    break;
                }
                w = meta[NODE_WEIGHT] + weight - meta[NODE_MAX];
                if (w < w0) {
                    k = i;
                    w0 = w;
                }
            }
            if (k >= 0) // found it
                return k;
        }
        browser = nodeList.browser();
        while ((i = browser.next()) >= 0) {
            meta = nodeList.getMetaData(i);
            if (meta[NODE_WEIGHT] <= 0 || meta[NODE_SIZE] <= 0) { // empty
                k = i;
                break;
            }
            w = meta[NODE_WEIGHT] + weight - meta[NODE_MAX];
            if (w < w0) {
                k = i;
                w0 = w;
            }
        }

        return k;
    }

    /** returns the site of the specified type */
    public int getSize(int type) {
        if (type == LM_NODE)
            return nodeList.size();
        else
            return loadList.size();
    }

    /** returns the status for the specified load of id */
    public int getStatus(int id, int type) {
        long[] meta = loadList.getMetaData(id);
        if (meta == null)
            return -1;
        else
            return (int) meta[LOAD_STATUS];
    }

    /** sets the status for the specified load of id */
    public void setStatus(int id, int status) {
        long[] meta;
        if(status >= Service.SERVICE_READY && status <= Service.SERVICE_CLOSED){
            meta = loadList.getMetaData(id);
            if (meta != null) {
                meta[LOAD_STATUS] = status;
                meta[LOAD_TIME] = System.currentTimeMillis();
            }
        }
    }

    public int getID(String key, int type) {
        if (type == LM_NODE)
            return nodeList.getID(key);
        else
            return loadList.getID(key);
    }

    public Object get(int id, int type) {
        if (type == LM_NODE)
            return nodeList.get(id);
        else
            return loadList.get(id);
    }

    /** returns the node ID for the specified load of id */
    public int getNodeId(int id) {
        long[] meta;
        meta = loadList.getMetaData(id);
        if (meta != null)
            return (int) meta[LOAD_OID];
        else
            return -1;
    }

    /** returns the sorted key list of all loads assigned to the node of id */
    public String[] getList(int id) {
        long[] meta;
        String[] load = new String[0];
        int i, k;
        meta = nodeList.getMetaData(id);
        if (meta == null)
            return null;
        if (meta[NODE_SIZE] > 0) {
            int[] list;
            Browser browser = loadList.browser();
            k = (int) meta[NODE_SIZE];
            load = new String[k];
            k = 0;
            while ((i = browser.next()) >= 0) {
                meta = loadList.getMetaData(i);
                if (meta[LOAD_OID] == id)
                    load[k++] = loadList.getKey(i);
            }
            if (k > 1)
                Arrays.sort(load, this);
        }
        return load;
    }

    public String getName(int id, int type) {
        if (type == LM_NODE)
            return nodeList.getKey(id);
        else
            return loadList.getKey(id);
    }

    public String getName() {
        return name;
    }

    public int getCapacity() {
        return capacity;
    }

    public long getNorm() {
        return norm;
    }

    public long getMTime() {
        return mtime;
    }

    public int compare(String a, String b) {
        long[] meta0, meta1;
        meta0 = loadList.getMetaData(a);
        meta1 = loadList.getMetaData(b);
        if (meta0[LOAD_WEIGHT] > meta1[LOAD_WEIGHT])
            return 1;
        else if (meta0[LOAD_WEIGHT] < meta1[LOAD_WEIGHT])
            return -1;
        else {
            int i, j;
            i = loadList.getID(a);
            j = loadList.getID(b);
            if (i < j)
                return 1;
            else if (i > j)
                return -1;
            else
                return 0;
        }
    }

    /**
     * returns a Browser for all node IDs in the natural order
     */
    public Browser browser() {
        return nodeList.browser();
    }

    public void clear() {
        nodeList.clear();
        loadList.clear();
        norm = 0;
        previousNorm = 0;
        mtime = 0;
    }
}
