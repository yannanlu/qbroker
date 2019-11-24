package org.qbroker.common;

/* DataSet.java -  a set of intervals specifying a set of numbers */

import java.util.List;
import java.util.ArrayList;

/**
 * DataSet - A set of data intervals defining a set of number.
 *<br><br>
 * A DataSet contains a set of intervals on the number axis.  Its method
 * of contains() evalues a given number and returns true if it belongs to the
 * set, or false otherwise.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DataSet {
    private int[] operation;             // operation
    private long intervals[][];          // intervals for long
    private double realpairs[][];        // intervals for real
    private int dataType;

    public final static int DATA_STRING = 0;
    public final static int DATA_INT = 1;
    public final static int DATA_LONG = 2;
    public final static int DATA_FLOAT = 3;
    public final static int DATA_DOUBLE = 4;
    public final static int DATA_TIME = 5;
    public final static int DATA_SEQUENCE = 6;
    private final static int GT = 1;
    private final static int EQ = 2;
    private final static int GE = 3;
    private final static int LT = 4;
    private final static int GTLT = 5;
    private final static int GELT = 7;
    private final static int QE = 8;
    private final static int LE = 12;
    private final static int GTLE = 13;
    private final static int GELE = 15;

    public DataSet(List list) {
        Object o;
        String str;
        String[] range;
        int i, k, n;
        if (list == null || list.size() <= 0)
            throw new IllegalArgumentException("empty list");
        n = list.size();
        intervals = new long[n][2];
        realpairs = new double[0][2];
        operation = new int[n];
        dataType = DATA_LONG;
        k = 0;
        for (i=0; i<n; i++) {
            o = list.get(i);
            if (o == null || !(o instanceof String))
                continue;
            str = (String) o;
            range = parseInterval(str);
            if (range == null)
                continue;
            if (range[0].indexOf(".") >=0 || range[1].indexOf(".") >= 0) {
                dataType = DATA_DOUBLE;
                break;
            }
            operation[k] = Integer.parseInt(range[2]);
            if ((operation[k] & GE) > 0)
                intervals[k][0] = Long.parseLong(range[0]);
            else
                intervals[k][0] = Long.parseLong(range[1]);

            if ((operation[k] & LE) > 0)
                intervals[k][1] = Long.parseLong(range[1]);
            else
                intervals[k][1] = intervals[k][0];
            if (intervals[k][1] >= intervals[k][0])
                k ++;
        }
        if (dataType == DATA_DOUBLE) { // for real numbers
            intervals = new long[0][2];
            realpairs = new double[n][2];
            k = 0;
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                str = (String) o;
                range = parseInterval(str);
                if (range == null)
                    continue;
                operation[k] = Integer.parseInt(range[2]);
                if ((operation[k] & GE) > 0)
                    realpairs[k][0] = Double.parseDouble(range[0]);
                else
                    realpairs[k][0] = Double.parseDouble(range[1]);

                if ((operation[k] & LE) > 0)
                    realpairs[k][1] = Double.parseDouble(range[1]);
                else
                    realpairs[k][1] = realpairs[k][0];
                if (realpairs[k][1] >= realpairs[k][0])
                    k ++;
            }
        }
        if (k != n)
            throw new IllegalArgumentException("bad intervals: "+ k +"/" + n);
    }

    public boolean contains(long x) {
        int i;

        for (i=0; i<operation.length; i++) {
            switch (operation[i]) {
              case EQ:
                if (x == intervals[i][0])
                    return true;
                break;
              case QE:
                if (x == intervals[i][1])
                    return true;
                break;
              case GT:
                if (x > intervals[i][0])
                    return true;
                break;
              case GE:
                if (x >= intervals[i][0])
                    return true;
                break;
              case LT:
                if (x < intervals[i][1])
                    return true;
                break;
              case LE:
                if (x <= intervals[i][1])
                    return true;
                break;
              case GTLT:
                if (x > intervals[i][0] && x < intervals[i][1])
                    return true;
                break;
              case GELT:
                if (x >= intervals[i][0] && x < intervals[i][1])
                    return true;
                break;
              case GTLE:
                if (x > intervals[i][0] && x <= intervals[i][1])
                    return true;
                break;
              case GELE:
                if (x >= intervals[i][0] && x <= intervals[i][1])
                    return true;
                break;
              default:
            }
        }

        return false;
    }

    public boolean contains(double x) {
        int i;

        for (i=0; i<operation.length; i++) {
            switch (operation[i]) {
              case EQ:
                if (x == realpairs[i][0])
                    return true;
                break;
              case QE:
                if (x == realpairs[i][1])
                    return true;
                break;
              case GT:
                if (x > realpairs[i][0])
                    return true;
                break;
              case GE:
                if (x >= realpairs[i][0])
                    return true;
                break;
              case LT:
                if (x < realpairs[i][1])
                    return true;
                break;
              case LE:
                if (x <= realpairs[i][1])
                    return true;
                break;
              case GTLT:
                if (x > realpairs[i][0] && x < realpairs[i][1])
                    return true;
                break;
              case GELT:
                if (x >= realpairs[i][0] && x < realpairs[i][1])
                    return true;
                break;
              case GTLE:
                if (x > realpairs[i][0] && x <= realpairs[i][1])
                    return true;
                break;
              case GELE:
                if (x >= realpairs[i][0] && x <= realpairs[i][1])
                    return true;
                break;
              default:
            }
        }

        return false;
    }

    public static String[] parseInterval(String intervalStr) {
        String str;
        int i, j, k, op = 0;
        String[] r = new String[3];
        if (intervalStr == null || intervalStr.length() < 4)
            return null;
        str = intervalStr;
        i = str.indexOf("(");
        if (i >= 0)
            op = GT;
        else { // try to look for left braket
            i = str.indexOf("[");
            if (i >= 0)
                op = GE;
        }
        if (i < 0)
            return null;
        j = str.indexOf(",", i+1);
        if (j <= i)
            return null;
        else if (j == i+1) {
            op = 0;
            r[0] = "";
        }
        else {
            r[0] = intervalStr.substring(i+1, j).trim();
            if (r[0].length() <= 0)
                op = 0;
        }

        k = str.indexOf(")", j+1);
        if (k > j)
            op += LT;
        else { // try to look for right braket
            k = str.indexOf("]", j+1);
            if (k > j)
                op += LE;
        }
        if (k <= j)
            return null;
        else if (k == j+1) {
            op &= GE;
            r[1] = "";
        }
        else {
            r[1]= intervalStr.substring(j+1, k).trim();
            if (r[1].length() <= 0)
                op &= GE;
        }
        if (op <= 0)
            return null;
        r[2] = String.valueOf(op);

        return r;
    }

    public void list() {
        int i;
        String str;
        if (dataType == DATA_LONG) {
            for (i=0; i<operation.length; i++) {
                switch (operation[i]) {
                  case GT:
                    System.out.println("(" + intervals[i][0] + "," + ")");
                    break;
                  case GE:
                    System.out.println("[" + intervals[i][0] + "," + ")");
                    break;
                  case LT:
                    System.out.println("(" + "," + intervals[i][1] + ")");
                    break;
                  case LE:
                    System.out.println("(" + "," + intervals[i][1] + "]");
                    break;
                  case GTLT:
                    System.out.println("(" + intervals[i][0] +","+
                        intervals[i][1] + ")");
                    break;
                  case GTLE:
                    System.out.println("(" + intervals[i][0] +","+
                        intervals[i][1] + "]");
                    break;
                  case EQ:
                    System.out.println("[" + intervals[i][0] +","+
                        intervals[i][0] + "]");
                    break;
                  case QE:
                    System.out.println("[" + intervals[i][1] +","+
                        intervals[i][1] + "]");
                    break;
                  case GELT:
                    System.out.println("[" + intervals[i][0] +","+
                        intervals[i][1] + ")");
                    break;
                  case GELE:
                    System.out.println("[" + intervals[i][0] +","+
                        intervals[i][1] + "]");
                    break;
                  default:
                }
            }
        }
        else if (dataType == DATA_DOUBLE) {
            for (i=0; i<operation.length; i++) {
                switch (operation[i]) {
                  case GT:
                    System.out.println("(" + realpairs[i][0] + "," + ")");
                    break;
                  case GE:
                    System.out.println("[" + realpairs[i][0] + "," + ")");
                    break;
                  case LT:
                    System.out.println("(" + "," + realpairs[i][1] + ")");
                    break;
                  case LE:
                    System.out.println("(" + "," + realpairs[i][1] + "]");
                    break;
                  case GTLT:
                    System.out.println("(" + realpairs[i][0] +","+
                        realpairs[i][1] + ")");
                    break;
                  case GTLE:
                    System.out.println("(" + realpairs[i][0] +","+
                        realpairs[i][1] + "]");
                    break;
                  case EQ:
                    System.out.println("[" + realpairs[i][0] +","+
                        realpairs[i][0] + "]");
                    break;
                  case QE:
                    System.out.println("[" + realpairs[i][1] +","+
                        realpairs[i][1] + "]");
                    break;
                  case GELT:
                    System.out.println("[" + realpairs[i][0] +","+
                        realpairs[i][1] + ")");
                    break;
                  case GELE:
                    System.out.println("[" + realpairs[i][0] +","+
                        realpairs[i][1] + "]");
                    break;
                  default:
                }
            }
        }
    }

    public int getDataType() {
        return dataType;
    }

    public static void main(String args[]) {
        DataSet ds;
        int i, n;
        List<String> list = new ArrayList<String>();
        for (i=0; i<args.length; i++)
            list.add(args[i]);
        if (list.size() <= 0)
            list.add("(0,1)");
        try {
            ds = new DataSet(list);
            ds.list();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
