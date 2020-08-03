package org.qbroker.common;

/* Evaluation.java - an evaluator on a simple numeric or a boolean expression */

import java.util.Map;

/**
 * Evaluation evaluates a simple numeric expression, or a boolean expression
 * with either numbers or single quoted strings, or a ternary expression for
 * either numbers or single quoted strings. In case of the numeric expression,
 * it supports 5 basic numeric operations, such as addition, substruction,
 * multiplication, division, and modulation. For a boolean expression, it
 * supports basic numeric comparisons and 4 string comparisons for single
 * quoted strings, such as "==", "!=", "=~" and "!~". The match operation is
 * based on String.match().
 *<br><br>
 * There are 3 public methods, evaluate(), choose() and isStringTernary().
 * They all take a text as the input expression. The first returns either a
 * Long or a Double for a numeric expression, or an Integer for a boolean
 * expression. In case of a boolean expression, Integer 1 is for true and
 * Integer 0 for false. The second evaluates a ternary expression with a boolan
 * expression and two single quoted strings. It returns one of the quoted
 * strings based on evaluation of the boolean experssion. The last returns true
 * if the expression is a ternary expression for single quoted strings, or
 * false otherwise.
 *<br>
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
    private static final int EVAL_EQS = 14;
    private static final int EVAL_NES = 15;
    private static final int EVAL_MTS = 16;
    private static final int EVAL_NMS = 17;
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
    private final static int ACTION_QUOTE = 12;  // for string
    private final static int ACTION_AND = 13;    // for logic AND
    private final static int ACTION_OR = 14;     // for logic OR
    private final static Integer TRUE = new Integer(1);
    private final static Integer FALSE = new Integer(0);
    private final static char[] ALLOWED_START = {'0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', '.', '-', '+', '(', '\''};

    private Evaluation() { // static only
    }

    /**
     * It evaluates a numeric expression or a boolean expression and returns a
     * Number as the result up on success. Otherwise it returns a null or
     * throws an IllegalArgumentException for failure. For a numeric expression,
     * the result is either a Long or a Double. In case of a boolean expression,
     * the result is an Integer of either 1 for TRUE or 0 for FALSE.
     */
    public static Number evaluate(String expr) {
        int i, k, n, action, offset, position, sign = 1;
        String str, key = null;
        char[] buffer;
        Number[] number = new Number[SIZE];
        int[] operation = new int[SIZE];

        expr = trim(expr);
        if (expr == null || (n = expr.length()) <= 0)
            return null;

        if (!isAllowedAtStart(expr.charAt(0))) // found illegal start
           throw(new IllegalArgumentException("expression can not start with '"+
                expr.charAt(0) + "': " + expr));

        for (i=0; i<SIZE; i++) {
            number[i] = null;
            operation[i] = 0;
        }
        buffer = expr.toCharArray();
        action = ACTION_SKIP;
        k = 0;
        i = k;
        offset = 0;
        position = 0;
        do {
            switch (action) {
              case ACTION_SKIP:
              case ACTION_SIGN:
                position = skip(buffer, position, n);
                break;
              case ACTION_NEXT:
                position = skip(buffer, position, n);
                if (position >= 0 && !isAllowedAtStart(buffer[position]))
                    throw(new IllegalArgumentException(
                        "next term can not start with '" + buffer[position] +
                        "' at " + position + ": " + expr));
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
              case ACTION_QUOTE:
                position = locate(buffer, position, n);
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
                        i = k;
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
                    else if (action == ACTION_MORE) {
                        position ++;
                    }
                    else if (action == ACTION_LESS) {
                        position ++;
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
                    else if (action == ACTION_LOOK) { // end of condition
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_COLON;
                        if (k > i + 1) { // more than one numbers
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        if (k > 1) {
                            Boolean b;
                            k --;
                            b = evaluate(operation[k], number[k-1], number[k]);
                            number[k-1] = b.booleanValue() ? TRUE : FALSE;
                            number[k] = null;
                            operation[k] = 0;
                            i = k;
                        }
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
                    if (action == ACTION_LOOK) { // end of 1st group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_SAME;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        else
                            i ++;
                    }
                    else if (action == ACTION_FIND) { // end of 1st group
                        position ++;
                        offset = position;
                        action = ACTION_SAME;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                    }
                    else if (action == ACTION_SAME) { // end of equal
                        operation[k] = (key == null) ? EVAL_EQ : EVAL_EQS;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_DIFF) { // end of not equal
                        operation[k] = (key == null) ? EVAL_NE : EVAL_NES;
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
                    if (action == ACTION_LOOK) { // end of 1st group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_DIFF;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        else
                            i ++;
                    }
                    else if (action == ACTION_FIND) { // end of 1st group
                        position ++;
                        offset = position;
                        action = ACTION_DIFF;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                    }
                    break;
                  case '>':
                    if (action == ACTION_LOOK) { // end of 1st group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_MORE;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        else
                            i ++;
                    }
                    else if (action == ACTION_FIND) { // end of 1st group
                        position ++;
                        offset = position;
                        action = ACTION_MORE;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                    }
                    break;
                  case '<':
                    if (action == ACTION_LOOK) { // end of 1st group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_LESS;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        else
                            i ++;
                    }
                    else if (action == ACTION_FIND) { // end of 1st group
                        position ++;
                        offset = position;
                        action = ACTION_LESS;
                        if (k > i + 1) {
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                    }
                    break;
                  case '~':
                    if (action == ACTION_SAME) { // pattern match
                        operation[k] = EVAL_MTS;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    else if (action == ACTION_DIFF) { // pattern not match
                        operation[k] = EVAL_NMS;
                        position ++;
                        offset = position;
                        action = ACTION_NEXT;
                    }
                    break;
                  case '&':
                    if (action == ACTION_LOOK) { // end of 2nd group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_AND;
                        if (k > i + 1) { // more than one numbers
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        if (k > 1) {
                            Boolean b;
                            k --;
                            b = evaluate(operation[k], number[k-1], number[k]);
                            number[k-1] = b.booleanValue() ? TRUE : FALSE;
                            number[k] = null;
                            operation[k] = 0;
                            i = k;
                        }
                    }
                    else if (action == ACTION_FIND) { // beginning of AND
                        position ++;
                        offset = position;
                        action = ACTION_AND;
                    }
                    else if (action == ACTION_AND) { // end of AND
                        operation[k] = EVAL_AND;
                        i = k;
/*
                        if (k > 0 && number[k-1].intValue() == 0) { // no go on
                             offset = n;
                             break;
                        }
*/
                        position ++;
                        offset = position;
                        action = ACTION_SKIP;
                    }
                    break;
                  case '|':
                    if (action == ACTION_LOOK) { // end of 2nd group of numbers
                        str = new String(buffer, offset, position - offset); 
                        number[k++] = parse(str);
                        position ++;
                        offset = position;
                        action = ACTION_OR;
                        if (k > i + 1) { // more than one numbers
                            number[i] = evaluate(i, k, number, operation);
                            for (int j=i+1; j<k; j++) { // reset
                                number[j] = null;
                                operation[j] = 0;
                            }
                            k = ++i;
                        }
                        if (k > 1) {
                            Boolean b;
                            k --;
                            b = evaluate(operation[k], number[k-1], number[k]);
                            number[k-1] = b.booleanValue() ? TRUE : FALSE;
                            number[k] = null;
                            operation[k] = 0;
                            i = k;
                         }
                    }
                    else if (action == ACTION_FIND) { // beginning of OR
                        position ++;
                        offset = position;
                        action = ACTION_OR;
                    }
                    else if (action == ACTION_OR) { // end of OR
                        operation[k] = EVAL_OR;
                        i = k;
/*
                        if (k > 0 && number[k-1].intValue() != 0) { // no go on
                             offset = n;
                             break;
                        }
*/
                        position ++;
                        offset = position;
                        action = ACTION_SKIP;
                    }
                    break;
                  case '\'':
                    if (action == ACTION_SKIP) { // start of 1st string
                        position ++;
                        offset = position;
                        action = ACTION_QUOTE;
                    }
                    else if (action == ACTION_NEXT) { // start of 2nd string
                        position ++;
                        offset = position;
                        action = ACTION_QUOTE;
                    }
                    else if (action == ACTION_QUOTE) { // end of string
                        int j = operation[k];
                        str = new String(buffer, offset, position - offset);
                        action = ACTION_FIND;
                        switch(j) {
                          case 0: // end of 1st string
                            key = str;
                            break;
                          case EVAL_EQS: // for ==
                            number[k] = key.equals(str) ? TRUE : FALSE;
                            key = null;
                            operation[k++] = 0;
                            break;
                          case EVAL_NES: // for !=
                            number[k] = key.equals(str) ? FALSE : TRUE;
                            key = null;
                            operation[k++] = 0;
                            break;
                          case EVAL_MTS: // for =~
                            number[k] = key.matches(str) ? TRUE : FALSE;
                            key = null;
                            operation[k++] = 0;
                            break;
                          case EVAL_NMS: // for !~
                            number[k] = key.matches(str) ? FALSE : TRUE;
                            key = null;
                            operation[k++] = 0;
                            break;
                          default:
                            action = ACTION_NONE;
                            break;
                        }
                        position ++;
                        offset = position;
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
        else if (operation[1] < EVAL_AND) // for numeric
            return evaluate(0, k, number, operation);
        else { // for boolean
            if (operation[k-1] < EVAL_AND) { // last numeric
                number[i] = evaluate(i, k, number, operation);
                k = ++i;
            }
            if (k == 1)
                return number[0];
            else if (operation[k-1] > EVAL_OR) { // last boolean
                Boolean b;
                k --;
                b = evaluate(operation[k], number[k-1], number[k]);
                number[k-1] = b.booleanValue() ? TRUE : FALSE;
            }
            if (k == 1)
                return number[0];
            else
                return evaluate(0, k, operation, number);
        }
    }

    private static boolean isAllowedAtStart(char c) {
        int i;
        for (i=0; i<ALLOWED_START.length; i++) { // check illegal start
           if (c == ALLOWED_START[i])
               break;
        }
        return (i < ALLOWED_START.length);
    }

    /**
     * It evaluates a ternary expression with a boolean expression and two
     * single quoted strings and returns one of the strings with quotes as
     * the result upon success. Otherwise it returns null or just throws an
     * IllegalArgumentException to indicate failure. Any single quoted string
     * can be replaced by a ternary expression as long as its result is also
     * a single quoted string.
     */
    public static String choose(String expr) {
        int i, j, k, n;
        Number r;
        char[] buffer;

        if (expr == null || (n = expr.length()) <= 0)
            return null;

        buffer = expr.toCharArray();
        // look for 1st '?' outside the single quotes
        i = search(buffer, 0, n);
        if (i < 0) { // not a ternary expression
            i = expr.indexOf('\'');
            j = expr.lastIndexOf('\'');
            return (i >= 0 && j >= i) ? expr.substring(i, j+1) : expr;
        }
        else if ((j = look(buffer, i+1, n)) < 0) // ':' not found
            return null;
        else if ((k = expr.indexOf('(')) >= 0 && k < i) { // with parenthese
            if (find(buffer, k+1, n) > i)
                return (evaluate(expr.substring(k+1, i)).intValue() != 0) ?
                    choose(expr.substring(i+1,j)) : choose(expr.substring(j+1));
            else
                return (evaluate(expr.substring(0, i)).intValue() != 0) ?
                    choose(expr.substring(i+1,j)) : choose(expr.substring(j+1));
        }
        else if (evaluate(expr.substring(0, i)).intValue() != 0)
            return choose(expr.substring(i+1, j));
        else
            return choose(expr.substring(j+1));
    }

    /** returns true if expression is a ternary expression for quoted strings */
    public static boolean isStringTernary(String expr) {
        int i;
        if (expr == null || (i = expr.lastIndexOf('\'')) <= 0 ||
            expr.indexOf('?', i+1) > 0)
            return false;
        if (expr.lastIndexOf('?', i) > 0)
            return true;
        if ((i = expr.lastIndexOf('\'', i-1)) <= 0) // single quote?
            return false;
        if (expr.charAt(i-1) == '\\') { // escaped
            int k = 1;
            char c = expr.charAt(0);
            for (int j=0; j<i; j++) {
                if (expr.charAt(i) == '\'') { // founded
                    if (c != '\\') { // not escaped
                        if (++k > 2)
                            return false;
                    }
                }
            }
            return true;
        }
        else { // look for extra single quotes
            for (int j=0; j<i; j++) {
                if (expr.charAt(i) == '\'') // founded
                    return false;
            }
            return true;
        }
    }

    public static String unquote(String expr) {
        if (expr == null)
            return null;
        else {
            int i = expr.indexOf('\'');
            int j = expr.lastIndexOf('\'');
            if (i >= 0 && j > i)
                return expr.substring(i+1, j);
            else
                return expr;
        }
    }

    /** returns a number of the evaluation on a numeric expression */
    private static Number evaluate(int i, int j, Number[] number,
        int[] operation) {
        int k = j - i;
        if (i < 0 || j <= i || number == null || operation == null ||
            number.length < k || number.length != operation.length)
            return null;
        else if (k == 1)
            return number[i];
        else if (k == 2)
            return evaluate(number[i], operation[i+1], number[i+1]);
        else { // for numbers
            Number r = null;
            for (j=1; j<k; j++) { // eval on MUL, DIV and MOD first
                if (operation[i+j] >= EVAL_MUL) {
                    r = evaluate(number[i+j-1], operation[i+j], number[i+j]);
                    number[i+j] = r;
                    number[i+j-1] = new Long(0);
                    if (j > 1) {
                        operation[i+j] = operation[i+j-1];
                        operation[i+j-1] = EVAL_ADD;
                    }
                    else
                        operation[i+j] = EVAL_ADD;
                }
            }

            r = number[0];
            for (j=1; j<k; j++) // sum over on all terms
                r = evaluate(r, operation[i+j], number[i+j]);

            return r;
        }
    }

    /** returns an Integer of the evaluation on a boolean expression */
    private static Integer evaluate(int i, int j, int[] operation,
        Number[] number) {
        int k = j - i;
        if (i < 0 || j <= i || number == null || operation == null ||
            number.length < k || number.length != operation.length)
            return null;
        else if (k == 1)
            return (Integer) number[i];
        else if (k == 2)
            return (Integer) evaluate(number[i], operation[i+1], number[i+1]);
        else { // for booleans
            Integer r = null;
            for (j=1; j<k; j++) { // eval on AND first
                if (operation[i+j] == EVAL_AND) {
                    r = (Integer) evaluate(number[i+j-1], operation[i+j],
                        number[i+j]);
                    number[i+j] = r;
                    number[i+j-1] = FALSE;
                    if (j > 1) {
                        operation[i+j] = operation[i+j-1];
                        operation[i+j-1] = EVAL_OR;
                    }
                    else
                        operation[i+j] = EVAL_OR;
                }
            }

            r = (Integer) number[0];
            for (j=1; j<k; j++) // sum over on all terms
                r = (Integer) evaluate(r, operation[i+j], number[i+j]);

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
            if (a instanceof Integer && b instanceof Integer)
                return (a.intValue() == 0 && b.intValue() == 0) ? FALSE : TRUE;
          case EVAL_AND:
            if (a instanceof Integer && b instanceof Integer)
                return (a.intValue() == 0 || b.intValue() == 0) ? FALSE : TRUE;
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
            throw new IllegalArgumentException("nagative sign not allowed");
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
        for (i=offset; i<length; i++) { // for first operator
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
        int level = 0;
        char c, b = buffer[offset];
        boolean inQuotes = false;
        for (int i=offset; i<length; i++) {
            c = buffer[i];
            if (c == '\'') { // found a quote
                if (b != '\\') // not escaped
                    inQuotes = !inQuotes;
            }
            else if (!inQuotes) { // not in quotes
                if (c == '(')
                    level ++;
                else if (c == ')') {
                    if (level > 0)
                        level --;
                    else
                        return i;
                }
            }
            b = c;
        }
        return -1;
    }

    /** returns the position of the corresponding ':' or -1 if not found */
    private static int look(char[] buffer, int offset, int length) {
        int level = 0;
        char c, b = buffer[offset];
        boolean inQuotes = false;
        for (int i=offset; i<length; i++) {
            c = buffer[i];
            if (c == '\'') { // found a quote
                if (b != '\\') // not escaped
                    inQuotes = !inQuotes;
            }
            else if (!inQuotes) { // not in quotes
                if (c == '?')
                    level ++;
                else if (c == ':') {
                    if (level > 0)
                        level --;
                    else
                        return i;
                }
            }
            b = c;
        }
        return -1;
    }

    /** returns the position of the 1st corresponding '?' or -1 if not found */
    private static int search(char[] buffer, int offset, int length) {
        int level = 0;
        char c, b = buffer[offset];
        boolean inQuotes = false;
        for (int i=offset; i<length; i++) {
            c = buffer[i];
            if (c == '\'') { // found a quote
                if (b != '\\') // not escaped
                    inQuotes = !inQuotes;
            }
            else if (!inQuotes) { // not in quotes
                if (c == '?')
                    return i;
            }
            b = c;
        }
        return -1;
    }

    /** returns the position of the unescaped '\'' or -1 if not found */
    private static int locate(char[] buffer, int offset, int length) {
        int level = 0;
        char c = buffer[offset];
        for (int i=offset; i<length; i++) {
            if (buffer[i] == '\'') {
                if (c != '\\') // not escaped
                    return i;
            }
            c = buffer[i];
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


    /** returns boolean result on the data map with the evaluation template */
    public static boolean evaluate(Map map, Template temp) {
        Object o;
        if (map == null || map.size() <= 0 || temp == null || temp.size() <= 0)
            throw(new IllegalArgumentException(
                "either data map or eval template is null or empty"));

        String str = temp.substitute(map, temp.copyText());
        try {
            o = evaluate(str);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException("failed to evaluate data map: " +
                e.toString()));
        }

        if (o == null)
            throw(new IllegalArgumentException("unexpected null for " + str));
        else if (!(o instanceof Integer))
            throw(new IllegalArgumentException("unexpected result: " +
                    o.getClass().getName() + " for " + str));

        return (((Integer) o).intValue() == 1);
    }

    public static void main(String[] args) {
        Number r = null;
        String expr = null;
        if (args.length > 0)
            expr = args[0];
        else {
            System.out.println("Usage: java org.qbroker.common.Evaluation "+
                "expression");
            System.exit(0);
        }

        if (isStringTernary(expr)) { // for string ternary expression
            try {
                String str = choose(expr);
                if (str == null)
                    System.out.println("got null from: " + expr);
                else
                    System.out.println(expr + " = " + str);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        else try { // for numeric or boolean expression
            r = evaluate(expr);
            if (r == null)
                System.out.println("null for: " + expr);
            else if (r instanceof Double)
                System.out.println(expr + " = " + ((Double) r).doubleValue());
            else if (r instanceof Long)
                System.out.println(expr + " = " + ((Long) r).longValue());
            else { // Integer for boolean values
                Boolean b = new Boolean(((Integer) r).intValue() != 0);
                System.out.println(expr + " = " + b.booleanValue());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
