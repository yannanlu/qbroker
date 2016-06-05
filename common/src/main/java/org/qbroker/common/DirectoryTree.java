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
        Map h = getAllFiles(root);
        tree.put(path, h);
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return root.getPath();
    }

    private Map<String, Object> getAllFiles(File dir) {
        if (dir == null)
            return null;
        if (!dir.exists() || !dir.isDirectory() || !dir.canRead())
            throw(new IllegalArgumentException("directory is not right: " +
                dir.getPath()));
        String[] list = dir.list();
        Arrays.sort(list);

        Map<String, Object> h = new HashMap<String, Object>();
        File file;
        String fname;
        for (int i=0; i<list.length; i++) {
            fname = list[i];
            if (fname == null || fname.length() <= 0 || fname.charAt(0) == '.')
                continue;
            file = new File(dir, fname);
            if (file.isDirectory()) {
                Map o = getAllFiles(file);
                h.put(fname, getAllFiles(file));
            }
            else {
                h.put(fname, file);
            }
        }
        if (h.size() > 0)
            h.put(".SortedList", list);
        return h;
    }

    public String getTreeMap() {
        return getTreeMap(name, tree, "  ", "\n");
    }

    public String getTreeMap(String baseName, Map node, String indent,
        String end) {
        StringBuffer strBuf = new StringBuffer();
        if (baseName == null || baseName.length() <= 0)
            baseName = name;
        String fname;
        if (node == tree) { // root level
            fname = root.getPath();
            strBuf.append("<" + baseName + ">" + end);
            strBuf.append(indent + "<Name>" + baseName + "</Name>" + end);
            strBuf.append(getTreeMap(fname, (Map) node.get(fname), indent,
                end));
            strBuf.append("</" + baseName + ">");
            return strBuf.toString();
        }
        else {
            strBuf.append(indent + "<DirName>" + baseName + "</DirName>" + end);
        }
        String[] list;
        Object o;
        o = node.get(".SortedList");
        if (o == null || !(o instanceof String[]))
          return null;
        list = (String[]) o;
        for (int i=0; i<list.length; i++) {
            fname = list[i];
            if ((o = node.get(fname)) == null)
                continue;
            if (o instanceof Map) {
                strBuf.append(indent + "<File type=\"ARRAY\">" + end);
                strBuf.append(getTreeMap(fname, (Map) o, indent + indent,
                    end));
                strBuf.append(indent + "</File>\n");
            }
            else if (o instanceof File) {
                strBuf.append(indent + "<File type=\"ARRAY\">");
                strBuf.append(fname);
                strBuf.append("</File>" + end);
            }
        }
        return strBuf.toString();
    }

    /**
     * clean up the tree and metadata
     */
    public void clear() {
        tree.clear();
    }
}
