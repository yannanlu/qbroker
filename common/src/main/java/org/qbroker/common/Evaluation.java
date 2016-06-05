package org.qbroker.common;

/* Evaluation.java - an evaluator on a simple numeric or boolean expression */

/**
 * Evaluation evaluates a simple expression with just numbers and the 5 basic
 * numeric operations, such as addition, substruction, multiplication, division,
 * modulation and ternary operations. The method of evaluate() takes a string as
 * the input expression and returns either a Long or a Double for a numeric
 * expression, or an Integer for a boolean expression. In case of a boolean
 * expression, the Integer 1 is for true and the Integer 0 for false.
 *<br/><br/>
 * In boolean expressions or ternary expressions, parentheses are required to
 * group terms and operations.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class Evaluation {
    private static final int SIZE = 256;
    private static final int EVAL_ADD = 1;
    private static final int EVAL_SUB = 2;
    private static final int EVAL_MUL = 3;
    private static final int EVAL_DIV = 4;
    private static final int EVAL_MOD = 5;
    private static final int EVAL_AND = 6;
    private static final int EVAL_OR = 7;
    private static final int EVAL_EQ = 8;
    private static final int EVAL_NE = 9;
    private static final int EVAL_GT = 10;
    private static final int EVAL_GE = 11;
    private static final int EVAL_LT = 12;
    private static final int EVAL_LE = 13;
    private final static int ACTION_NONE = 0;    // for parsing failure
    private final static int ACTION_SKIP = 1;    // for a char not a white space
    private final static int ACTION_FIND = 2;    // for operator
    private final static int ACTION_LOOK = 3;    // for a char not a white space
    private final static int ACTION_NEXT = 4;    // for next char
    private final static int ACTION_SIGN = 5;    // for sign of the number
    private final static int ACTION_EXPR = 6;    // for end of expression
    private final static int ACTION_SAME = 7;    // for equal
    private final static int ACTION_DIFF = 8;    // for not euaual
    private final static int ACTION_MORE = 9;    // for larger than
    private final static int ACTION_LESS = 10;   // for less than
    private final static int ACTION_COLON = 11;  // for ternery expression
    private final static int ACTION_AND = 12;    // for logic AND
    private final static int ACTION_OR = 13;     // for logic OR

    private Evaluation() { // static only
    }

    public static Number evaluate(String expr) {
        int i, k, n, action, offset, position, sign = 1;
        String str;
        char[] buffer;
        Number r = null;
        Number[] number = new Number[SIZE];
        int[] operation = new int[SIZE];

        if (expr == null || (n = expr.length()) <= 0)
            return null;

        for (i=0; i<SIZE; i++) {
            number[i] = null;
            operation[i] = 0;
        }
        buffer = expr.toCharArray();
        action = ACTION_SKIP;
        k = 0;
        offset = 0;
        position = 0;
        do {
            switch (action) {
              case ACTION_SKIP:
              case ACTION_NEXT:
              case ACTION_SIGN:
                position = skip(buffer, position, n);
                break;
              case ACTION_LOOK:
              case ACTION_FIND:
                position = scan(buffer, position, n);
                break;
              case ACTION_EXPR:
                position = find(buffer, position, n);
                break;
              case ACTION_COLON:
                position = look(buffer, position, n);
                break;
              case ACTION_SAME:
              case ACTION_DIFF:
              case ACTION_MORE:
              case ACTION_LESS:
              case ACTION_AND:
              case ACTION_OR:
                break;
              default:
                position = -1;
            }
            if (position >= 0) {
                char c;
                switch ((c = buffer[position])) {
                  case '0': // begin of a number
                  case '1':
                  case '2':
                  case '3':
                  case '4':
                  case '5':
                  case '6':
                  case '7':
                  case '8':
                  case '9':
                  case '.':
                    if (action == ACTION_SKIP) { // begin of the first number
                        offset = position;
                        action = ACTION_LOOK;
                    }
                    else if (action == ACTION_NEXT) {
                        offset = position;
                        action = ACTION_LOOK;
                    }
                    else if (action == ACTION_MORE) {
                        operation[k] = EVAL_GT;
                        offset = position;
                        action = ACTION_LOOK;
                    }
                    else if (action == ACTION_LESS) {
                        operation[k] = EVAL_LT;
                        offset = position;
                        action = ACTION_LOOK;
                    }
                    else if (action == ACTION_SIGN) {
                        action = ACTION_LOOK;
                    }
                    break;
                  case '+':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        operation[k] = EVAL_ADD;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_SKIP) { // for a sign
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_FIND) { // for a expression
                        operation[k] = EVAL_ADD;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '-':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        operation[k] = EVAL_SUB;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_SKIP) { // for a sign
                        position ++;
                        action = ACTION_SIGN;
                    }
                    else if (action == ACTION_NEXT) { // for a sign
                        position ++;
                        action = ACTION_SIGN;
                    }
                    else if (action == ACTION_MORE) {
                        operation[k] = EVAL_GT;
                        position ++;
                        action = ACTION_SIGN;
                    }
                    else if (action == ACTION_LESS) {
                        operation[k] = EVAL_LT;
                        position ++;
                        action = ACTION_SIGN;
                    }
                    else if (action == ACTION_FIND) { // for a expression
                        operation[k] = EVAL_SUB;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '*':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        operation[k] = EVAL_MUL;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_FIND) { // for a expression
                        operation[k] = EVAL_MUL;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '/':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        operation[k] = EVAL_DIV;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_FIND) { // for a expression
                        operation[k] = EVAL_DIV;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '%':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        operation[k] = EVAL_MOD;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_FIND) { // for a expression
                        operation[k] = EVAL_MOD;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '(':
                    if (action == ACTION_SKIP) { // begin of expression
                        position ++;
                        offset = position;
                        action = ACTION_EXPR;
                    }
                    else if (action == ACTION_NEXT) {
                        position ++;
                        offset = position;
                        action = ACTION_EXPR;
                    }
                    else if (action == ACTION_SIGN) {
                        sign = -1;
                        position ++;
                        offset = position;
                        action = ACTION_EXPR;
                    }
                    else if (action == ACTION_MORE) {
                        operation[k] = EVAL_GT;
                        position ++;
                        offset = position;
                        action = ACTION_EXPR;
                    }
                    else if (action == ACTION_LESS) {
                        operation[k] = EVAL_LT;
                        position ++;
                        offset = position;
                        action = ACTION_EXPR;
                    }
                    break;
                  case ')':
                    if (action == ACTION_EXPR) { // end of expression
                        str = new String(buffer, offset, position - offset);
                        if (sign < 0) {
                            number[k++] = negate(evaluate(str));
                            sign = 1;
                        }
                        else
                            number[k++] = evaluate(str);
                        position ++;
                        offset = position;
                        action = ACTION_FIND;
                    }
                    break;
                  case '?':
                    if (action == ACTION_FIND) { // end of ternery condition
                        position ++;
                        offset = position;
                        action = ACTION_COLON;
                    }
                    break;
                  case ':':
                    if (action == ACTION_COLON) { // end of first expression
                        if (number[k-1].intValue() != 0) { // condition is true
                            str = new String(buffer, offset, position - offset); 
                            number[k-1] = evaluate(str);
                        }
                        else { // ternery condition is false
                            position ++;
                            offset = position;
                            str = new String(buffer, offset, n - offset); 
                            number[k-1] = evaluate(str);
                        }
                        offset = n;
                        break;
                    }
                    break;
                  case '=':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_SAME;
                    }
                    else if (action == ACTION_SAME) { // end of equal
                        operation[k] = EVAL_EQ;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_DIFF) { // end of not equal
                        operation[k] = EVAL_NE;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_MORE) { // end of larger or equal
                        operation[k] = EVAL_GE;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_LESS) { // end of less or equal
                        operation[k] = EVAL_LE;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '!':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_DIFF;
                    }
                    break;
                  case '>':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_MORE;
                    }
                    break;
                  case '<':
                    if (action == ACTION_LOOK) { // end of number
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_LESS;
                    }
                    break;
                  case '&':
                    if (action == ACTION_FIND) { // start of AND
                        position ++;
                        offset = position;
                        action = ACTION_AND;
                    }
                    else if (action == ACTION_AND) { // end of AND
                        operation[k] = EVAL_AND;
                        if (number[k-1].intValue() == 0) { // no need to go on
                             offset = n;
                             break;
                        }
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '|':
                    if (action == ACTION_FIND) { // start of OR
                        position ++;
                        offset = position;
                        action = ACTION_OR;
                    }
                    else if (action == ACTION_OR) { // end of OR
                        operation[k] = EVAL_OR;
                        if (number[k-1].intValue() != 0) { // no need to go on
                             offset = n;
                             break;
                        }
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case ' ':  // for white spaces
                  case '\r':
                  case '\n':
                  case '\b':
                  case '\t':
                  case '\f':
                    if (action == ACTION_MORE) {
                        operation[k] = EVAL_GT;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_LESS) {
                        operation[k] = EVAL_LT;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  default:
                }
                if (action == ACTION_NONE)
                    throw(new IllegalArgumentException("parser failed: " +
                        c + " " + offset + " " + position));
            }
            else { // end of expr
                if (number[k] == null)
                    number[k++] = parse(expr.substring(offset));
                break;
            }
        } while (offset < n);

        if (k <= 0)
            return null;
        else if (k == 1)
            return number[0];
        else if (k == 2) {
            if (operation[1] < EVAL_AND)
                return evaluate(number[0], operation[1], number[1]);
            else if (operation[1] > EVAL_OR) {
                Boolean b = evaluate(operation[1], number[0], number[1]);
                if (b == null)
                    return null;
                else if (b.booleanValue())
                    return new Integer(1);
                else
                    return new Integer(0);
            }
            else
                return evaluate(number[0], operation[1], number[1]);
        }
        else if (operation[1] < EVAL_AND) { // for numbers
            for (i=1; i<k; i++) { // eval on MUL, DIV and MOD first
                if (operation[i] >= EVAL_MUL) {
                    r = evaluate(number[i-1], operation[i], number[i]);
                    number[i] = r;
                    number[i-1] = new Long(0);
                    if (i > 1) {
                        operation[i] = operation[i-1];
                        operation[i-1] = EVAL_ADD;
                    }
                    else
                        operation[i] = EVAL_ADD;
                }
            }

            r = number[0];
            for (i=1; i<k; i++) // sum over on all terms
                r = evaluate(r, operation[i], number[i]);

            return r;
        }
        else { // for booleans
            for (i=1; i<k; i++) { // eval on AND first
                if (operation[i] == EVAL_AND) {
                    r = evaluate(number[i-1], operation[i], number[i]);
                    number[i] = r;
                    number[i-1] = new Integer(0);
                    if (i > 1) {
                        operation[i] = operation[i-1];
                        operation[i-1] = EVAL_OR;
                    }
                    else
                        operation[i] = EVAL_OR;
                }
            }

            r = number[0];
            for (i=1; i<k; i++) // or over on all terms
                r = evaluate(r, operation[i], number[i]);

            return r;
        }
    }

    /** returns result or null if a is devided by 0 */
    private static Number evaluate(Number a, int oper, Number b) {
        boolean isDouble = false;
        if (a == null || b == null)
            throw new IllegalArgumentException("null numbers");

        if (a instanceof Double || b instanceof Double)
            isDouble = true;
        switch (oper) {
          case EVAL_ADD:
            if (isDouble)
                return new Double(a.doubleValue() + b.doubleValue());
            else
                return new Long(a.longValue() + b.longValue());
          case EVAL_SUB:
            if (isDouble)
                return new Double(a.doubleValue() - b.doubleValue());
            else
                return new Long(a.longValue() - b.longValue());
          case EVAL_MUL:
            if (isDouble)
                return new Double(a.doubleValue() * b.doubleValue());
            else
                return new Long(a.longValue() * b.longValue());
          case EVAL_DIV:
            if (isDouble) {
                if (b.doubleValue() != 0.0)
                    return new Double(a.doubleValue() / b.doubleValue());
                else
                    return null;
            }
            else {
                if (b.longValue() != 0)
                    return new Long(a.longValue() / b.longValue());
                else
                    return null;
            }
          case EVAL_MOD:
            if (isDouble) {
                if (b.doubleValue() != 0.0)
                    return new Double(a.doubleValue() % b.doubleValue());
                else
                    return null;
            }
            else {
                if (b.longValue() != 0)
                    return new Long(a.longValue() % b.longValue());
                else
                    return null;
            }
          case EVAL_OR:
             if (a.intValue() == 0 && b.intValue() == 0)
                 return new Integer(0);
             else
                 return new Integer(1);
          case EVAL_AND:
             if (a.intValue() == 0 || b.intValue() == 0)
                 return new Integer(0);
             else
                 return new Integer(1);
          default:
        }
        throw new IllegalArgumentException("operation not supported: " + oper);
    }

    /** returns boolean result or null if operation is not supported */
    private static Boolean evaluate(int oper, Number a, Number b) {
        boolean isDouble = false;
        if (a == null || b == null)
            throw new IllegalArgumentException("null numbers");

        if (a instanceof Double || b instanceof Double)
            isDouble = true;
        switch (oper) {
          case EVAL_EQ:
            if (isDouble)
                return new Boolean((a.doubleValue() == b.doubleValue()));
            else
                return new Boolean((a.longValue() == b.longValue()));
          case EVAL_NE:
            if (isDouble)
                return new Boolean((a.doubleValue() != b.doubleValue()));
            else
                return new Boolean((a.longValue() != b.longValue()));
          case EVAL_GE:
            if (isDouble)
                return new Boolean((a.doubleValue() >= b.doubleValue()));
            else
                return new Boolean((a.longValue() >= b.longValue()));
          case EVAL_LE:
            if (isDouble)
                return new Boolean((a.doubleValue() <= b.doubleValue()));
            else
                return new Boolean((a.longValue() <= b.longValue()));
          case EVAL_GT:
            if (isDouble)
                return new Boolean((a.doubleValue() > b.doubleValue()));
            else
                return new Boolean((a.longValue() > b.longValue()));
          case EVAL_LT:
            if (isDouble)
                return new Boolean((a.doubleValue() < b.doubleValue()));
            else
                return new Boolean((a.longValue() < b.longValue()));
          default:
        }
        throw new IllegalArgumentException("operation not supported: " + oper);
    }

    private static Number negate(Number a) {
        if (a == null)
            return null;
        else if (a instanceof Double)
            return new Double(-a.doubleValue());
        else if (a instanceof Long)
            return new Long(-a.longValue());
        else // for boolean
            return new Integer((a.intValue() == 0) ? 1 : 0);
    }

    private static Number parse(String value) {
        Number a = null;
        if (value == null || value.length() <= 0)
            return null;
        value = trim(value);
        if (value.indexOf('.') >= 0) {
            try {
                a = new Double(value);
            }
            catch (NumberFormatException e) {
                throw(new IllegalArgumentException(e.toString() + ": "+ value));
            }
        }
        else {
            try {
                a = new Long(value);
            }
            catch (NumberFormatException e) {
                throw(new IllegalArgumentException(e.toString() + ": "+ value));
            }
        }
        return a;
    }

    /** returns the position of the first operator or -1 if not found */
    private static int scan(char[] buffer, int offset, int length) {
        int i;
        char c;
        for (i=offset; i<length; i++) { // for quote or backslash
            c = buffer[i];
            if (c == '+' || c == '-' || c == '*' || c == '/' || c == '%' ||
                c == '=' || c == '!' || c == '>' || c == '<' || c == '&' ||
                c == '|' || c == '?' || c == ':')
                return i;
        }
        return -1;
    }

    /** returns the position of the corresponding ')' or -1 if not found */
    private static int find(char[] buffer, int offset, int length) {
        int i, level = 0;
        char c;
        for (i=offset; i<length; i++) {
            c = buffer[i];
            if (c == '(')
                level ++;
            else if (c == ')') {
                if (level > 0)
                    level --;
                else
                    return i;
            }
        }
        return -1;
    }

    /** returns the position of the first ':' or -1 if not found */
    private static int look(char[] buffer, int offset, int length) {
        for (int i=offset; i<length; i++) {
            if (buffer[i] == ':')
                return i;
        }
        return -1;
    }

    /** returns the position of the first non-space char or -1 if not found */
    private static int skip(char[] buffer, int offset, int length) {
        int i;
        char c;
        for (i=offset; i<length; i++) {
            c = buffer[i];
            if (c != ' ' && c != '\n' && c != '\t' && c != '\f' && c!= '\r' &&
                c != '\b')
                return i;
        }
        return -1;
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

    public static void main(String[] args) {
        Number o = null;
        String expr = null;
        if (args.length > 0)
            expr = args[0];
        else {
            System.out.println("Usage: java org.qbroker.common.Evaluation "+
                "expression");
            System.exit(0);
        }

        try {
            o = evaluate(expr);
            if (o == null)
                System.out.println("null for: " + expr);
            else if (o instanceof Double)
                System.out.println(expr + " = " + ((Double) o).doubleValue());
            else if (o instanceof Long)
                System.out.println(expr + " = " + ((Long) o).longValue());
            else { // Integer for boolean values
                Boolean b = new Boolean(((Integer) o).intValue() != 0);
                System.out.println(expr + " = " + b.booleanValue());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
