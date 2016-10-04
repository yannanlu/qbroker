package org.qbroker.common;

/* DirectoryTree.java - a container of files and directories of a filesystem */

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.io.File;

/**
 * This is NOT MT-Safe.  Therefore please use it with care.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DirectoryTree {
    private String name;             // name of the top directory
    private Map<String, Map> tree;   // Map representation of the tree
    private int depth;
    private File root;

    public DirectoryTree(String name, String path, int depth) {
        if (name == null)
            throw(new IllegalArgumentException("name is null"));
        this.name = name;
        this.depth = depth;
        root = new File(path);
        if (!root.exists() || !root.isDirectory() || !root.canRead())
            throw(new IllegalArgumentException("directory is not right: " +
                path));
        tree = new HashMap<String, Map>();
        tree.put(path, getAllFiles(root));
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return root.getPath();
    }

    /**
     * returns the tree map of the given directory
     */
    private Map<String, Object> getAllFiles(File dir) {
        if (dir == null)
            return null;
        if (!dir.exists() || !dir.isDirectory() || !dir.canRead())
            throw(new IllegalArgumentException("directory is not right: " +
                dir.getPath()));

        File file;
        Map<String, Object> h = new HashMap<String, Object>();
        for (String fname : dir.list()) {
            if (fname == null || fname.length() <= 0 || fname.charAt(0) == '.')
                continue;
            file = new File(dir, fname);
            if (file.isDirectory())
                h.put(fname, getAllFiles(file));
            else
                h.put(fname, file);
        }
        String[] list = h.keySet().toArray(new String[h.size()]);
        if (list.length > 1)
            Arrays.sort(list);
        h.put(".SortedList", list);
        return h;
    }

    /**
     * returns JSON representation of the directory tree
     */
    public String getTreeMap() {
        String path = root.getPath();
        return getTreeMap(path, tree.get(path), "", "\n");
    }

    /**
     * returns JSON representation of the directory tree on the give node
     */
    public String getTreeMap(String dirName, Map node, String indent,
        String end) {
        Object o;
        String[] list;
        String ind, indd, path;
        int i = 0;
        if (dirName == null || dirName.length() <= 0)
            return null;
        StringBuffer strBuf = new StringBuffer();
        o = node.get(".SortedList");
        if (o == null || !(o instanceof String[]))
            return null;
        ind = (indent == null) ? "" : indent + "  ";
        indd = (indent == null) ? "" : ind + "  ";
        path = root.getPath();
        strBuf.append(indent + "{" + end);
        if (path.equals(dirName) && node == tree.get(path)) // top level
            strBuf.append(ind + "\"Name\":\"" + name + "\"," + end);
        strBuf.append(ind + "\"DirName\":\"" + dirName + "\"," + end);
        strBuf.append(ind + "\"File\": [");
        list = (String[]) o;
        for (String fname : list) {
            if ((o = node.get(fname)) == null)
                continue;
            if (o instanceof Map) { // directory
                if (i > 0)
                    strBuf.append(",");
                strBuf.append(end + getTreeMap(fname, (Map) o,
                    (indent == null) ? null : indd, end));
                i ++;
            }
            else if (o instanceof File) { // file
                if (i > 0)
                    strBuf.append(",");
                strBuf.append(end + indd + "\"" + fname + "\"");
                i ++;
            }
        }
        strBuf.append(end + ind + "]");
        strBuf.append(end + indent + "}");
        return strBuf.toString();
    }

    /**
     * clean up the tree and metadata
     */
    public void clear() {
        for (String key : tree.keySet()) {
            Map map = tree.get(key);
            if (map != null)
                map.clear();
        }
        tree.clear();
    }

    protected void finalize() {
        clear();
    }
}
