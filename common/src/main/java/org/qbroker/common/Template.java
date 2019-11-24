package org.qbroker.common;

/* Template.java - a simple template for substitutions */

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.StringSubstitution;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.qbroker.common.Utils;

/**
 * Template is a template to format a text string.  A Template contains
 * placeholders or variables which are to be substituted by their values. These
 * placeholders or variables are delimited with certain character sequences.
 * The default sequence is '##' on both sides of the placeholder name.
 * For example, a template might look like
 *<br>
 *   First Name: ##firstname##<br>
 *    Last Name: ##lastname##<br>
 * where ##firstname## and ##lastname## are two placeholders. The substitute
 * method will replace them with the given values. The name of the placeholder
 * is called field. It is OK for a field to contain certain Perl5 meta chars.
 *<br><br>
 * When a Template object is instantiated, the text is scanned for any pattern
 * of ##[^#]+## as the placeholders it may contain.  In this case, a legal field
 * name is any sequence of characters that does not contain a hashsign.
 * In general, Template supports customized patterns for placeholders or
 * variables, such as \([^\(\)]+\) or \$\{[^\$\{\}]+\}, etc.
 *<br><br>
 * Template also provides the methods for the actual substitutions.
 * They require either a key-value pair or a Map with all key-value pairs.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class Template {
    private static ThreadLocal<Perl5Matcher> lm=new ThreadLocal<Perl5Matcher>();
    private static String defaultPattern = "##[^#]+##";
    private Pattern pattern;
    private Map<String, Pattern> fields;   // storing rawString => pattern pairs
    private String[] allFields; // storing all rawStrings
    private String text;
    private int head, tail;

    public Template(Object obj, String pattern, boolean needChop) {
        int k;
        String patternString = pattern;
        PatternMatcherInput input;
        MatchResult mr;
        Pattern p;
        String rawString, headStr, tailStr;

        Perl5Compiler pc = new Perl5Compiler();
        Perl5Matcher pm = lm.get();
        if (pm == null) {
            pm = new Perl5Matcher();
            lm.set(pm);
        }

        if (patternString == null || patternString.length() == 0) { // default
            patternString = defaultPattern;
            head = 2;
            tail = 2;
            headStr = "##";
            tailStr = "##";
        }
        else { // customized
            head = patternString.indexOf("[");
            if (head < 0) // in case of " \w+ "
                head = patternString.indexOf("\\");
            head = (head < 0) ? 0 : head;
            headStr = patternString.substring(0, head);
            if (headStr.indexOf("\\") >= 0) { // search for escaped chars
                k = head;
                for (int i=0; i<k; i++) {
                    if (headStr.charAt(i) == '\\') { // escape and adjust head
                        i ++;
                        head --;
                    }
                }
            }

            k = patternString.lastIndexOf("+");
            if (k > 0)
                tailStr = patternString.substring(k+1);
            else
                tailStr = "";
            tail = tailStr.length();
            if (tailStr.indexOf("\\") >= 0) { // search for escaped chars
                k = tail;
                for (int i=0; i<k; i++) {
                    if (tailStr.charAt(i) == '\\') { // escape and adjust tail
                        i ++;
                        tail --;
                    }
                }
            }
        }

        try {
            this.pattern = pc.compile("(" + patternString + ")");
        }
        catch (MalformedPatternException e) {
            throw(new IllegalArgumentException("pattern failed: " + e));
        }

        if (obj instanceof String) {
            text = (String) obj;
        }
        else if (obj instanceof File) {
            File file = (File) obj;
            if (!file.exists() || !file.isFile() || !file.canRead())
                throw(new IllegalArgumentException("can not read: " + file));

            try {
                text = read(new FileInputStream(file));
            }
            catch (IOException e) {
                throw(new IllegalArgumentException("failed to read: " + e));
            }
        }
        else {
            throw(new IllegalArgumentException("illegal object"));
        }

        if (needChop && text != null) { // chop the trailing newline
            int i = text.length();
            if (i > 0 && text.charAt(i-1) == '\n')
                text = text.substring(0, i-1);
        }

        fields = new HashMap<String, Pattern>();
        input = new PatternMatcherInput(text);
        while (pm.contains(input, this.pattern)) {
            mr = pm.getMatch();
            patternString = mr.group(1);
            rawString = patternString.substring(head,
                patternString.length() - tail);
            if (fields.containsKey(rawString))
                continue;
            try {
                p = pc.compile(headStr + rawString + tailStr);
            }
            catch (MalformedPatternException e) {
                throw(new IllegalArgumentException("bad pattern: " + e));
            }

            // test the pattern on rawString for any meta chars
            if (pm.matches(patternString, p))
                fields.put(rawString, p);
            else { // pattern not matched, so try to escape certan chars
                String str = Utils.doSearchReplace("\\", "\\\\", rawString);
                str = Utils.doSearchReplace("$", "\\$", str);
                str = Utils.doSearchReplace(".", "\\.", str);
                str = Utils.doSearchReplace("[", "\\[", str);
                str = Utils.doSearchReplace("*", "\\*", str);
                str = Utils.doSearchReplace("?", "\\?", str);
                str = Utils.doSearchReplace("+", "\\+", str);
                str = Utils.doSearchReplace("(", "\\(", str);
                str = Utils.doSearchReplace("|", "\\|", str);
                str = Utils.doSearchReplace("^", "\\^", str);
                try {
                    p = pc.compile(headStr + str + tailStr);
                }
                catch (MalformedPatternException e) {
                    throw(new IllegalArgumentException("wrong pattern: " + e));
                }
                fields.put(rawString, p);
            }
        }

        Iterator iterator = (fields.keySet()).iterator();
        k = fields.size();
        allFields = new String[k];
        k = 0;
        while (iterator.hasNext()) {
            allFields[k++] = (String) iterator.next();
        }
    }

    public Template(Object obj, String pattern) {
        this(obj, pattern, false);
    }

    public Template(File file, boolean needChop) {
        this(file, null, needChop);
    }

    public Template(String text) {
        this(text, null, false);
    }

    public Template(File file) {
        this(file, null, false);
    }

    public boolean containsField(String key) {
        return fields.containsKey(key);
    }

    public int numberOfFields() {
        return fields.size();
    }

    public String copyText() {
        return new String(text);
    }

    public String toString() {
        return text;
    }

    public Pattern getPattern(String field) {
        return fields.get(field);
    }

    public String getField(int i) {
        if (i >= 0 && i < allFields.length)
            return allFields[i];
        else
            return null;
    }

    public String[] getAllFields() {
        String[] all = new String[allFields.length];
        for (int i=0; i<allFields.length; i++)
            all[i] = allFields[i];
        return all;
    }

    public String[] getSequence() {
        ArrayList<String> list = new ArrayList<String>();
        String rawString, patternString;
        MatchResult mr;
        PatternMatcherInput input;
        Perl5Matcher pm = lm.get();
        if (pm == null) {
            pm = new Perl5Matcher();
            lm.set(pm);
        }
        input = new PatternMatcherInput(text);
        while (pm.contains(input, pattern)) {
            mr = pm.getMatch();
            patternString = mr.group(1);
            rawString = patternString.substring(head,
                patternString.length() - tail);
            list.add(rawString);
        }
        return list.toArray(new String[list.size()]);
    }

    public Iterator iterator() {
        return (fields.keySet()).iterator();
    }

    public String substitute(String field, String value, String input) {
        if (input == null || !fields.containsKey(field))
            return input;
        Perl5Matcher pm = lm.get();
        if (pm == null) {
            pm = new Perl5Matcher();
            lm.set(pm);
        }
        return Util.substitute(pm, (Pattern) fields.get(field),
            new StringSubstitution(value), input, Util.SUBSTITUTE_ALL);
    }

    public String substitute(String input, Map map) {
        String field, value;
        if (map == null || map.size() == 0)
            return input;
        for (int i=0; i<allFields.length; i++) {
            field = allFields[i];
            value = (String) map.get(field);
            if (value != null && input.indexOf(field) >= 0)
                input = substitute(field, value, input);
        }
        return input;
    }

    public int substitute(StringBuffer result, String field, String value,
        String input) {
        if (fields.containsKey(field)) {
            Perl5Matcher pm = lm.get();
            if (pm == null) {
                pm = new Perl5Matcher();
                lm.set(pm);
            }
            return Util.substitute(result, pm, (Pattern) fields.get(field),
                new StringSubstitution(value), input, Util.SUBSTITUTE_ALL);
        }
        else
            return -1;
    }

    public void clear() {
        pattern = null;
        allFields = null;
        if (fields != null) {
            fields.clear();
            fields = null;
        }
    }

    protected void finalize() {
        clear();
    }

    private String read(InputStream fis) throws IOException {
        int bytesRead = 0, totalBytes = 0, bufferSize = 4096;
        int maxBytes = bufferSize * 1024;
        StringBuffer strBuf = new StringBuffer();
        byte [] buffer = new byte[bufferSize];

        while ((bytesRead = fis.read(buffer, 0, bufferSize)) >= 0) {
            strBuf.append(new String(buffer, 0, bytesRead));
            totalBytes += bytesRead;
            if (totalBytes >= maxBytes)
                break;
        }
        fis.close();
        return strBuf.toString();
    }
}
