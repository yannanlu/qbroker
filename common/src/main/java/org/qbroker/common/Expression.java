package org.qbroker.common;

/**
 * Expression is an Interface to evaluate a numeric or boolen expression
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface Expression {
    /** returns the value of the expression */
    public Number evaluate();
    /** returns the operation id of the expression */
    public int getOperation();
    /** sets the operation to the expression */
    public void setOperation(int op);
    /** returns total number of the items in the expression */
    public int size();
    /** returns true if the expression is numeric or false otherwise */
    public boolean isNumeric();
    /** returns the isNegative flag of the expression */
    public boolean isNegative();
    /** sets the isNegative flag to true */
    public void negate();
    /** returns the original text of the expression */
    public String getText();

    public static final int EVAL_NONE = 0;
    public static final int EVAL_ADD = 1;
    public static final int EVAL_SUB = 2;
    public static final int EVAL_MUL = 3;
    public static final int EVAL_DIV = 4;
    public static final int EVAL_MOD = 5;
    public static final int EVAL_AND = 6;
    public static final int EVAL_OR = 7;
    public static final Integer TRUE = new Integer(1);
    public static final Integer FALSE = new Integer(0);
}
