package org.qbroker.common;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.io.IOException;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Substitution;
import org.apache.oro.text.regex.Perl5Substitution;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Utils;
import org.qbroker.common.DataSet;
import org.qbroker.common.RandomNumber;
import org.qbroker.common.Evaluation;

/**
 * TextSubstitution takes a Perl5 substitution expression and parses it into
 * the Pattern part and the Substitution part.  It also supports something
 * like "s/a/b/gm" where m means applying the substitution on each lines.
 * Its method of substitute() applies the Perl5 substitution on an input
 * string and returns the result.  It supports the substitution between
 * LineFeed (\n) and CarrigeReturn (\r). It also supports the substitution like
 * "s/a/c/g" or "s/c/b/g" where c is the hexdecimal char, such as \x02.
 *<br/><br/>
 * It also supports expressions in a different way as compared to Perl5.
 * In this case, the substitution expression is supposed to be something like
 * "s//?=xxx/e" where 'e' at the end indicates the expression substitution.
 * It is assumed that the text in the context will be treated as the input
 * string which will be parsed into whatever type of data.  When you call the
 * method of substitute(), please make sure to check the returned string.
 * It should never be null nor empty.  Otherwise, something is wrong.  The
 * caller is supposed to log the details or to throw exception with details.
 * Here lists all supported exressions:
 * <table>
 * <tr><th>Expression</th><th>Operation</th><th>Type</th><th>Example</th></tr>
 * <tr><td>s//+=n/e</td><td>Add</td><td>Number</td><td>s//+=3/e</td></tr>
 * <tr><td>s//-=n/e</td><td>Substract</td><td>Number</td><td>s//-=3/e</td></tr>
 * <tr><td>s//*=n/e</td><td>Multiply</td><td>Number</td><td>s//*=3/e</td></tr>
 * <tr><td>s///=n/e</td><td>Devide</td><td>Number</td><td>s///=3/e</td></tr>
 * <tr><td>s//%=n/e</td><td>Modulate</td><td>Integer</td><td>s//%=3/e</td></tr>
 * <tr><td>s//~=s/e</td><td>Parse</td><td>Time Pattern</td><td>s//~=MMM d HH:mm:ss yyyy/e</td></tr>
 * <tr><td>s//#=s/e</td><td>Format</td><td>Time Pattern</td><td>s//#=yyyy-MM-dd HH:mm:ss.SSS/e</td></tr>
 * <tr><td>s//?=s/e</td><td>Condition</td><td>String</td><td>s//?=[0,100]15/e</td></tr>
 * <tr><td>s//:=lower/e</td><td>toLowerCase</td><td>String</td><td>s//:=lower/e</td></tr>
 * <tr><td>s//:=upper/e</td><td>toUperCase</td><td>String</td><td>s//:=uper/e</td></tr>
 * <tr><td>s//:=encode/e</td><td>URLEncode</td><td>String</td><td>s//:=encode/e</td></tr>
 * <tr><td>s//:=decode/e</td><td>URLDecode</td><td>String</td><td>s//:=decode/e</td></tr>
 * <tr><td>s//:=time/e</td><td>CurrentTimeMillis</td><td>String</td><td>s//:=time/e</td></tr>
 * <tr><td>s//:=rand/e</td><td>Random Number</td><td>Number</td><td>s//:=rand 1.0/e</td></tr>
 * <tr><td>s//:=pow d/e</td><td>Power</td><td>Real Number</td><td>s//:=pow 0.5/e</td></tr>
 * <tr><td>s//:=sqrt/e</td><td>Square Root</td><td>Number</td><td>s//:=sqrt/e</td></tr>
 * <tr><td>s//:=abs/e</td><td>Absolute Value</td><td>Number</td><td>s//:=abs/e</td></tr>
 * <tr><td>s//:=eval/e</td><td>Evaluation</td><td>Number or String</td><td>s//:=eval/e</td></tr>
 * <tr><td>s//:=md5/e</td><td>MD5 Checksum</td><td>String</td><td>s//:=md5/e</td></tr>
 * <tr><td>s//:=chop/e</td><td>Chop</td><td>String</td><td>s//:=chop/e</td></tr>
 * <tr><td>s//:=replace d/e</td><td>Search Replace</td><td>String</td><td>s//:=replace !/e</td></tr>
 * <tr><td>s//:=sub d/e</td><td>regex replaceFirst</td><td>String</td><td>s//:=sub !/e</td></tr>
 * <tr><td>s//:=gsub d/e</td><td>regex replaceAll</td><td>String</td><td>s//:=gsub !/e</td></tr>
 * </table>
 * For eval(), it applies to a template with a simple numeric expression for
 * numbers or a ternary expression on numbers or single quoted strings. In case
 * of string, the result string will have the quotes trimmed off.
 * For replace(), the following parameter, d, is the delimiter used to parse
 * the text of the template. The text of the template should contain 3 parts
 * delimitered by the delimiter, d. The first part is the original text to be
 * processed. The second part is the string to search for in the first part.
 * The last part is the string as the replacement. For sub() or gsub(), it
 * replaces either the first regex match or all the regex maches with the
 * replacement. So the second part is a regex string.
 * <br/><br/>
 * The constructor may throw any errors in initialization of the substitution.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TextSubstitution {
    private static ThreadLocal<Perl5Matcher> lm=new ThreadLocal<Perl5Matcher>();
    private Pattern pattern = null;
    private Substitution substitution = null;
    private SimpleDateFormat dateFormat = null;
    private RandomNumber rand = null;
    private DataSet dset = null;
    private MessageDigest md = null;
    private String name;
    private String d1 = "!";
    private double x1 = 1.0;
    private long m1 = 1;
    private boolean isReal = false;
    private int operation, scope, defaultNumSubs, expr;
    private static final int SCOPE_NONE = 0;
    private static final int SCOPE_SINGLE = 1;
    private static final int SCOPE_GLOBAL = 2;
    private static final int SCOPE_MLINES = 4;
    private static final int EXPR_NONE = 0;
    private static final int EXPR_ADD = 1;
    private static final int EXPR_SUB = 2;
    private static final int EXPR_MUL = 3;
    private static final int EXPR_DIV = 4;
    private static final int EXPR_MOD = 5;
    private static final int EXPR_OR = 6;
    private static final int EXPR_AND = 7;
    private static final int EXPR_XOR = 8;
    private static final int EXPR_PARSE = 9;
    private static final int EXPR_FORMAT = 10;
    private static final int EXPR_COND = 11;
    private static final int EXPR_ABS = 12;
    private static final int EXPR_POW = 13;
    private static final int EXPR_EXP = 14;
    private static final int EXPR_LOG = 15;
    private static final int EXPR_SQRT = 16;
    private static final int EXPR_RAND = 17;
    private static final int EXPR_TIME = 18;
    private static final int EXPR_LOWER = 19;
    private static final int EXPR_UPPER = 20;
    private static final int EXPR_EVAL = 21;
    private static final int EXPR_ENCODE = 22;
    private static final int EXPR_DECODE = 23;
    private static final int EXPR_MD5 = 24;
    private static final int EXPR_CHOP = 25;
    private static final int EXPR_REPLACE = 26;
    private static final int EXPR_RSUB = 27;
    private static final int EXPR_GSUB = 28;
    private static final int OP_EXPR = 0;
    private static final int OP_SUB = 1;
    private static final int OP_N2R = 2;
    private static final int OP_R2N = 4;
    private static final int OP_RN2N = 8;
    private static final int OP_N2RN = 16;
    private static final int OP_R2RN = 32;
    private static final int OP_S2X = 64;
    private static final int OP_X2S = 128;

    public TextSubstitution(String name, String expression) {
        char d;
        String key, value;
        int i, k;
        this.name = name;

        Perl5Compiler pc = new Perl5Compiler();
        Perl5Matcher pm = lm.get();
        if (pm == null) {
            pm = new Perl5Matcher();
            lm.set(pm);
        }

        if (expression == null || expression.length() <= 4)
            throw(new IllegalArgumentException("null or empty expression"));

        defaultNumSubs = 1;
        d = expression.charAt(0);
        if (d == 's') {
            operation = OP_SUB;
            if ("s/\\n/\\r/g".equals(expression))
                operation = OP_N2R;
            else if ("s/\\r/\\n/g".equals(expression))
                operation = OP_R2N;
            else if ("s/\\r\\n/\\n/g".equals(expression))
                operation = OP_RN2N;
            else if ("s/\\n/\\r\\n/g".equals(expression))
                operation = OP_N2RN;
            else if ("s/\\r/\\r\\n/g".equals(expression))
                operation = OP_R2RN;
            else if (expression.matches("s/\\\\[xX][0-9a-fA-F][0-9a-fA-F]/.*"))
                operation = OP_X2S;
            else if (expression.matches("s/.*[^\\\\]/\\\\[xX][0-9a-fA-F][0-9a-fA-F]/g?"))
                operation = OP_S2X;
            else if (expression.lastIndexOf("/e") == expression.length() - 2)
                operation = OP_EXPR;
        }
        else {
            throw(new IllegalArgumentException("operation not supported: "+ d));
        }

        if (operation > OP_EXPR) {
            expr = EXPR_NONE;
            d = expression.charAt(1);
            i = 2;
            while ((k = expression.indexOf(d, i)) >= i) {
                if (expression.charAt(k-1) != '\\') // not a escape
                    break;
                else
                    i = k+1;
            }
            if (k > 2) {
                try {
                    if (operation == OP_X2S)
                        pattern = pc.compile(new String(
                        Utils.hexString2Bytes("0"+expression.substring(3, k))));
                    else
                        pattern = pc.compile(expression.substring(2, k));
                }
                catch (MalformedPatternException e) {
                    throw(new IllegalArgumentException("failed to compile" +
                        " the pattern in " + expression));
                }
            }
            else {
                throw(new IllegalArgumentException("no pattern found in " +
                    expression));
            }

            i = expression.lastIndexOf(d);
            if (i > k) {
                if (operation == OP_S2X)
                    substitution = new Perl5Substitution(new String(
                       Utils.hexString2Bytes("0"+expression.substring(k+2,i))));
                else
                    substitution=new Perl5Substitution(expression.substring(k+1,
                        i));
            }
            else {
                throw(new IllegalArgumentException("missing ending '" + d +
                    "' in " + expression));
            }
            scope = SCOPE_NONE;
            k = expression.length();
            while (++i < k) {
                switch(expression.charAt(i)) {
                  case 's':
                    scope += SCOPE_SINGLE;
                    break;
                  case 'g':
                    scope += SCOPE_GLOBAL;
                    defaultNumSubs = (operation == OP_SUB ||
                        operation == OP_S2X || operation == OP_X2S) ?
                        Util.SUBSTITUTE_ALL:0;
                    break;
                  case 'm':
                    scope += SCOPE_MLINES;
                    break;
                  default:
                    break;
                }
            }
        }
        else { // for expression support
            String text = null;
            expr = EXPR_NONE;
            d = expression.charAt(1);
            i = 2;
            while ((k = expression.indexOf(d, i)) >= i) { // look for 2nd /
                if (expression.charAt(k-1) != '\\') // not a escape
                    break;
                else
                    i = k+1;
            }
            if (k >= 2) // found the 2nd /, so look for =
                k = expression.indexOf('=', k+1);
            if (k < 2)
                throw(new IllegalArgumentException("no pattern found in " +
                    expression));
            i = expression.lastIndexOf(d);
            if (i > k+1) { // set the expression to text
                text = expression.substring(k+1, i);
            }
            if (text == null || text.length() <= 0) 
                throw(new IllegalArgumentException("bad expression: " +
                    expression));
            isReal = (text.indexOf(".") >=0);
            switch(expression.charAt(k-1)) {
              case '+':
                expr = EXPR_ADD;
                if (isReal)
                    x1 = Double.parseDouble(text.trim());
                else
                    m1 = Long.parseLong(text.trim());
                break;
              case '-':
                expr = EXPR_SUB;
                if (isReal)
                    x1 = Double.parseDouble(text.trim());
                else
                    m1 = Long.parseLong(text.trim());
                break;
              case '*':
                expr = EXPR_MUL;
                if (isReal)
                    x1 = Double.parseDouble(text.trim());
                else
                    m1 = Long.parseLong(text.trim());
                break;
              case '/':
                expr = EXPR_DIV;
                if (isReal) {
                    x1 = Double.parseDouble(text.trim());
                    if (x1 == 0.0)
                        throw(new IllegalArgumentException("zero denominator: "+
                            text));
                }
                else {
                    m1 = Long.parseLong(text.trim());
                    if (m1 == 0)
                        throw(new IllegalArgumentException("zero denominator: "+
                            text));
                }
                break;
              case '%':
                expr = EXPR_MOD;
                if (isReal) {
                    x1 = Double.parseDouble(text.trim());
                    if (x1 == 0.0)
                        throw(new IllegalArgumentException("zero denominator: "+
                            text));
                }
                else {
                    m1 = Long.parseLong(text.trim());
                    if (m1 == 0)
                        throw(new IllegalArgumentException("zero denominator: "+
                            text));
                }
                break;
              case '~':
                expr = EXPR_PARSE;
                dateFormat = new SimpleDateFormat(text);
                break;
              case '#':
                expr = EXPR_FORMAT;
                dateFormat = new SimpleDateFormat(text);
                break;
              case '?':
                text.trim();
                i = text.lastIndexOf(')');
                if (i <= 0)
                    i = text.lastIndexOf(']');
                if (i <= 0) {
                    expr = EXPR_NONE;
                    break;
                }
                x1 = Double.parseDouble(text.substring(i+1).trim());
                text = text.substring(0, i+1);
                text.trim();
                expr = EXPR_COND;
                ArrayList<String> list = new ArrayList<String>();
                list.add(text);
                dset = new DataSet(list);
                if (dset.getDataType() == DataSet.DATA_DOUBLE) {
                    isReal = true;
                }
                else {
                    isReal = false;
                    m1 = (long) x1;
                }
                break;
              case ':':
                text.trim();
                i = text.indexOf(' ');
                if (i > 0) { // operand defined
                    key = text.substring(0, i);
                    if ("pow".equalsIgnoreCase(key)) {
                        x1 = Double.parseDouble(text.substring(i).trim());
                        expr = EXPR_POW;
                    }
                    else if ("rand".equalsIgnoreCase(key)) {
                        if (isReal)
                            x1 = Double.parseDouble(text.substring(i).trim());
                        else
                            m1 = Long.parseLong(text.substring(i).trim());
                        rand= new RandomNumber((int)System.currentTimeMillis());
                        expr = EXPR_RAND;
                    }
                    else if ("replace".equalsIgnoreCase(key)) {
                        expr = EXPR_REPLACE;
                        d1 = text.substring(i).trim();
                    }
                    else if ("sub".equalsIgnoreCase(key)) {
                        expr = EXPR_RSUB;
                        d1 = text.substring(i).trim();
                    }
                    else if ("gsub".equalsIgnoreCase(key)) {
                        expr = EXPR_GSUB;
                        d1 = text.substring(i).trim();
                    }
                    else {
                        expr = EXPR_NONE;
                    }
                }
                else if ("abs".equalsIgnoreCase(text)) {
                    expr = EXPR_ABS;
                }
                else if ("exp".equalsIgnoreCase(text)) {
                    expr = EXPR_EXP;
                }
                else if ("log".equalsIgnoreCase(text)) {
                    expr = EXPR_LOG;
                }
                else if ("sqrt".equalsIgnoreCase(text)) {
                    expr = EXPR_SQRT;
                }
                else if ("time".equalsIgnoreCase(text)) {
                    expr = EXPR_TIME;
                }
                else if ("lower".equalsIgnoreCase(text)) {
                    expr = EXPR_LOWER;
                }
                else if ("upper".equalsIgnoreCase(text)) {
                    expr = EXPR_UPPER;
                }
                else if ("eval".equalsIgnoreCase(text)) {
                    expr = EXPR_EVAL;
                }
                else if ("encode".equalsIgnoreCase(text)) {
                    expr = EXPR_ENCODE;
                }
                else if ("decode".equalsIgnoreCase(text)) {
                    expr = EXPR_DECODE;
                }
                else if ("md5".equalsIgnoreCase(text)) {
                    expr = EXPR_MD5;
                }
                else if ("chop".equalsIgnoreCase(text)) {
                    expr = EXPR_CHOP;
                }
                else if ("replace".equalsIgnoreCase(text)) {
                    expr = EXPR_REPLACE;
                    d1 = "!";
                }
                else if ("sub".equalsIgnoreCase(text)) {
                    expr = EXPR_RSUB;
                    d1 = "!";
                }
                else if ("gsub".equalsIgnoreCase(text)) {
                    expr = EXPR_GSUB;
                    d1 = "!";
                }
                else {
                    expr = EXPR_NONE;
                }
                break;
              default:
            }

            if (expr == EXPR_NONE)
                throw(new IllegalArgumentException("bad expression: " +
                    expression));
            else if (expr == EXPR_MD5) try {
                md = MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException e) {
                throw(new IllegalArgumentException(e.toString()));
            }
        }
    }

    public TextSubstitution(String expression) {
        this(expression, expression);
    }

    /** returns the MD5 checksum for the msg */
    private String checksum(String msg) {
        byte[] b;
        StringBuffer strBuf;
        if (msg == null)
            throw(new IllegalArgumentException("null msg"));
        md.reset();
        md.update(msg.getBytes());
        b = md.digest();
        strBuf = new StringBuffer();
        for (int i=0; i<b.length; i++)
            strBuf.append(Integer.toString((b[i]&0xff)+0x100,16).substring(1));
        return strBuf.toString().toLowerCase();
    }

    /**
     * It evalues the input string and the expression and returns
     * the result of the evaluation.  If anything is wrong, it
     * returns null.
     */
    private String eSubstitute(String input, int dataType){
        String str = null;
        double x0;
        long m0;
        if (input == null)
            return null;
        switch (expr) {
          case EXPR_ADD:
            try {
                if (isReal) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(x0 + x1);
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(m0 + m1);
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_SUB:
            try {
                if (isReal) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(x0 - x1);
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(m0 - m1);
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_MUL:
            try {
                if (isReal) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(x0 * x1);
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(m0 * m1);
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_DIV:
            try {
                if (isReal) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(x0 / x1);
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(m0 / m1);
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_MOD:
            try {
                if (isReal) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(x0 % x1);
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(m0 % m1);
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_ABS:
            try {
                if (input.indexOf('.') >= 0) {
                    x0 = Double.parseDouble(input);
                    str = String.valueOf(Math.abs(x0));
                }
                else {
                    m0 = Long.parseLong(input);
                    str = String.valueOf(Math.abs(m0));
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_EXP:
            try {
                x0 = Double.parseDouble(input);
                str = String.valueOf(Math.exp(x0));
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_LOG:
            try {
                x0 = Double.parseDouble(input);
                if (x0 > 0.0)
                    str = String.valueOf(Math.log(x0));
                else
                    str = null;
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_POW:
            try {
                x0 = Double.parseDouble(input);
                str = String.valueOf(Math.pow(x0, x1));
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_SQRT:
            try {
                x0 = Double.parseDouble(input);
                if (x0 >= 0.0)
                    str = String.valueOf(Math.sqrt(x0));
                else
                    str = null;
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_RAND:
            x0 = rand.getNext();
            if (isReal)
                str = String.valueOf(x0 * x1);
            else {
                m0 = (long) (x0 * m1);
                str = String.valueOf(m0);
            }
            break;
          case EXPR_TIME:
            m0 = System.currentTimeMillis();
            str = String.valueOf(m0);
            break;
          case EXPR_PARSE:
            try {
                Date d = dateFormat.parse(input, new ParsePosition(0));
                str = String.valueOf(d.getTime());
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_FORMAT:
            try {
                long l = Long.parseLong(input);
                str = dateFormat.format(new Date(l));
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_COND:
            if (isReal) {
                x0 = Double.parseDouble(input);
                if (dset.contains(x0))
                    x0 = x1;
                str = String.valueOf(x0);
            }
            else {
                m0 = Long.parseLong(input);
                if (dset.contains(m0))
                    m0 = m1;
                str = String.valueOf(m0);
            }
            break;
          case EXPR_LOWER:
            str = input.toLowerCase();
            break;
          case EXPR_UPPER:
            str = input.toUpperCase();
            break;
          case EXPR_EVAL:
            try {
                if (Evaluation.isStringExpression(input)) {//between two strings
                    str = Evaluation.choose(input);
                    if (str != null)
                        str = Evaluation.unquote(str);
                }
                else { // numeric exporession
                    Number r = Evaluation.evaluate(input);
                    if (r != null)
                        str = r.toString();
                    else
                        str = null;
                }
            }
            catch (Exception e) {
                str = null;
            }
            break;
          case EXPR_ENCODE:
            try {
                str = URLEncoder.encode(input, "UTF-8");
            }
            catch(IOException e) {
                throw(new IllegalArgumentException("failed to encode: " +
                    e.toString()));
            }
            break;
          case EXPR_DECODE:
            try {
                str = URLDecoder.decode(input, "UTF-8");
            }
            catch(IOException e) {
                throw(new IllegalArgumentException("failed to decode: " +
                    e.toString()));
            }
            break;
          case EXPR_MD5:
            str = checksum(input);
            break;
          case EXPR_CHOP:
            if (input != null) {
                int i = input.length();
                if (i > 0 && input.charAt(i-1) == '\n')
                    str = input.substring(0, i-1);
                else
                    str = input;
            }
            else
                str = input;
            break;
          case EXPR_REPLACE:
            if (input == null)
                str = input;
            else {
                int i = input.indexOf(d1);
                int k = d1.length();
                if (i < 0)
                    str = input;
                else if (i == 0)
                    str = "";
                else {
                    int j = input.indexOf(d1, i+k);
                    str = input.substring(0, i);
                    if (j > i+k)
                        str = Utils.doSearchReplace(input.substring(i+k, j),
                            input.substring(j+k), str);
                }
            }
            break;
          case EXPR_RSUB:
            if (input == null)
                str = input;
            else {
                int i = input.indexOf(d1);
                int k = d1.length();
                if (i < 0)
                    str = input;
                else if (i == 0)
                    str = "";
                else {
                    int j = input.indexOf(d1, i+k);
                    str = input.substring(0, i);
                    if (j > i+k)
                        str = str.replaceFirst(input.substring(i+k, j),
                            input.substring(j+k));
                }
            }
            break;
          case EXPR_GSUB:
            if (input == null)
                str = input;
            else {
                int i = input.indexOf(d1);
                int k = d1.length();
                if (i < 0)
                    str = input;
                else if (i == 0)
                    str = "";
                else {
                    int j = input.indexOf(d1, i+k);
                    str = input.substring(0, i);
                    if (j > i+k)
                        str = str.replaceAll(input.substring(i+k, j),
                            input.substring(j+k));
                }
            }
            break;
          default:
            str = null;
            break;
        }
        return str;
    }

    // for multiline support
    private String mSubstitute(String input, int numSubs) {
        int len, i, j;
        String text;
        StringBuffer buffer;

        if (input == null || (i = input.indexOf('\n')) < 0) {
            return safeSubstitute(input, numSubs);
        }

        len = 1;
        j = 0;
        buffer = new StringBuffer();
        do {
            if (i > j) {
                text = safeSubstitute(input.substring(j, i), numSubs);
                buffer.append(text);
            }
            buffer.append('\n');
            j = i + len;
            i = input.indexOf('\n', j+len);
        } while (i >= j);
        return buffer.toString();
    }

    public String substitute(String input, int numSubs) {
        if (input == null)
            return input;
        else if (operation == OP_EXPR)
            return eSubstitute(input, numSubs);
        else {
            if ((scope & SCOPE_MLINES) > 0)
                return mSubstitute(input, numSubs);
            else if (operation == OP_R2N)
                return doSearchReplace("\r", "\n", input, numSubs);
            else if (operation == OP_N2R)
                return doSearchReplace("\n", "\r", input, numSubs);
            else if (operation == OP_RN2N)
                return doSearchReplace("\r\n", "\n", input, numSubs);
            else if (operation == OP_N2RN)
                return doSearchReplace("\n", "\r\n", input, numSubs);
            else if (operation == OP_R2RN)
                return doSearchReplace("\r", "\r\n", input, numSubs);
            else
                return safeSubstitute(input, numSubs);
        }
    }

    public String substitute(String input) {
        if (operation != OP_EXPR)
            return substitute(input, defaultNumSubs);
        else // for expression support
            return eSubstitute(input, 0);
    }

    private String safeSubstitute(String input, int numSubs) {
        Perl5Matcher pm = lm.get();
        if (pm == null) {
            pm = new Perl5Matcher();
            lm.set(pm);
        }
        return Util.substitute(pm, pattern, substitution, input, numSubs);
    }

    public Pattern getPattern() {
        return pattern;
    }

    public Substitution getSubstitution() {
        return substitution;
    }

    public int getScope() {
        return scope;
    }

    public String getName() {
        return name;
    }

    public void clear() {
        pattern = null;
        substitution = null;
        dateFormat = null;
        dset = null;
        rand = null;
        md = null;
    }

    protected void finalize() {
        clear();
    }

    private static String doSearchReplace(String s,String r,String text,int n) {
        int len, i, j, k, d;

        if (s == null || (len = s.length()) == 0 || (i  = text.indexOf(s)) < 0)
            return text;

        len = s.length();
        d = r.length() - len;
        j = i;
        k = i;
        StringBuffer buffer = new StringBuffer(text);
        if (n > 0) {
            while (i >= j && n-- > 0) {
                k += i - j;
                buffer.replace(k, k+len, r);
                k += d;
                j = i;
                i = text.indexOf(s, j+len);
            }
        }
        else {
            while (i >= j) {
                k += i - j;
                buffer.replace(k, k+len, r);
                k += d;
                j = i;
                i = text.indexOf(s, j+len);
            }
        }
        return buffer.toString();
    }

    /**
     * returns a Map with the key-value pair to a set of TextSubstitutions
     */
    public static Map<String, TextSubstitution[]> getSubstitutionMap(Map map) {
        Map<String, TextSubstitution[]> subMap;
        List list;
        Iterator iter;
        String key;
        TextSubstitution[] sub;
        Object o;
        int i, k, n;
        subMap = new HashMap<String, TextSubstitution[]>();
        if (map == null || map.size() <= 0)
            return subMap;
        for (iter=map.keySet().iterator(); iter.hasNext();) {
            key = (String) iter.next();
            if (key == null || key.length() <= 0)
                continue;
            o = map.get(key);
            if (o == null || !(o instanceof ArrayList))
                continue;
            list = (List) o;
            n = list.size();
            sub = new TextSubstitution[n];
            k = 0;
            for (i=0; i<n; i++) {
                o = list.get(i);
                if (o == null || !(o instanceof String))
                    continue;
                sub[k++] = new TextSubstitution((String) o); 
            }
            if (k < n) {
                TextSubstitution[] s = new TextSubstitution[k];
                for (i=0; i<k; i++) {
                    s[i] = sub[i];
                    sub[i] = null;
                }
                subMap.put(key, s);
            }
            else
                subMap.put(key, sub);
        }
        return subMap;
    }
}
