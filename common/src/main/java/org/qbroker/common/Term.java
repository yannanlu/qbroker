package org.qbroker.common;

/* Term.java - a basic and the smallest unit of an expression */

import org.qbroker.common.Expression;

/**
 * Term implements the interface of Expression. It is the smallest unit of
 * an expression.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class Term implements Expression {
    private Number value = null;
    private String text;
    private int size = 1;
    private int operation = EVAL_NONE;
    private boolean isNegative = false;
    private boolean isNumeric;

    public static final int COMP_NONE = 0;
    public static final int COMP_EQ = 1;
    public static final int COMP_NE = 2;
    public static final int COMP_GT = 3;
    public static final int COMP_GE = 4;
    public static final int COMP_LT = 5;
    public static final int COMP_LE = 6;
    public static final int COMP_EQS = 7;
    public static final int COMP_NES = 8;
    public static final int COMP_MTS = 9;
    public static final int COMP_NMS = 10;

    public Term(Number value) {
        if (value == null)
            throw(new IllegalArgumentException("number is null"));
        else if (value instanceof Long)
            this.value = value;
        else if (value instanceof Double)
            this.value = value;
        else if (value instanceof Integer)
            this.value = (value.intValue() != 0) ? TRUE : FALSE;
        else
            throw(new IllegalArgumentException("number type is not supported"));
        text = value.toString();
        isNumeric = !(value instanceof Integer);
    }

    public Term(String text) {
        if (text == null)
            throw(new IllegalArgumentException("text is null"));
        text = trim(text);
        if (text.length() <= 0)
            throw(new IllegalArgumentException("text is empty"));
        this.text = text;

        isNumeric = !(text.indexOf(">") > 0);
    }

    private static Term parse(String value) {
        Number v;
        if (value == null)
            return null;
        value = trim(value);
        if (value.length() <= 0)
            return null;
        if (value.indexOf('.') >= 0) { // for double or float
            try {
                v = new Double(value);
            }
            catch (NumberFormatException e) {
                throw(new IllegalArgumentException(e.toString() + ": "+ value));
            }
        }
        else { // for int or long
            try {
                v = new Long(value);
            }
            catch (NumberFormatException e) {
                throw(new IllegalArgumentException(e.toString() + ": "+ value));
            }
        }
        return new Term(v);
    }

    /** trims off white space chars on both ends and returns the string */
    private static String trim(String str) {
        StringBuffer strBuf = new StringBuffer();
        char c;
        int i;
        if (str == null)
            return null;
        strBuf.append(str);
        i = strBuf.length();
        if (i <= 0)
            return strBuf.toString();
        while ((c = strBuf.charAt(0)) == ' ' || c == '\n' || c == '\t' ||
            c == '\f' || c == '\r' || c == '\b')
            strBuf.deleteCharAt(0);

        i = strBuf.length();
        while (--i >= 0) {
            c = strBuf.charAt(i);
            if (c == ' ' || c == '\n' || c == '\t' || c == '\f' || c == '\r' ||
                c == '\b')
                strBuf.deleteCharAt(i);
        }

        return strBuf.toString();
    }

    public Number evaluate() {
        return value;
    }

    public int size() {
        return size;
    }

    public boolean isNumeric() {
        return isNumeric;
    }

    public String getText() {
        return text;
    }

    public int getOperation() {
        return operation;
    }

    public void setOperation(int op) {
        operation = op;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public void negate() {
        isNegative = true;
    }
}
