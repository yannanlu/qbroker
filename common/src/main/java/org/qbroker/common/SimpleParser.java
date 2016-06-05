package org.qbroker.common;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.MalformedPatternException;

/**
 * SimpleParser parses a text into a Map according to the given patterns
 * with single-line match enabled.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class SimpleParser {
    private Pattern pattern;
    private Perl5Matcher pm;
    private String[] keys;

    public SimpleParser(List patternArray) {
        Object o;
        Iterator iter;
        Map h;
        StringBuffer strBuf = new StringBuffer();
        String key;
        int i, k, n;
        if (patternArray == null || patternArray.size() <= 0)
            throw new IllegalArgumentException("PatternArray is null or empty");
        n = patternArray.size();
        keys = new String[n];
        k = 0;
        for (i=0; i<n; i++) {
            if ((o = patternArray.get(i)) == null)
                continue;
            if (!(o instanceof Map))
                continue;
            h = (Map) o;
            if (h.size() <= 0)
                continue;
            iter = h.keySet().iterator();
            key = (String) iter.next();
            if (key == null || (o = h.get(key)) == null)
                continue;
            keys[k++] = key;
            strBuf.append((String) o);
        }
        if (k < n)
            throw new IllegalArgumentException("PatternArray has bad patterns");

        try {
            Perl5Compiler pc = new Perl5Compiler();
            pm = new Perl5Matcher();
            pattern = pc.compile(strBuf.toString(),
                Perl5Compiler.SINGLELINE_MASK);
        }
        catch(Exception e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    public Map<String, String> parse(String buffer) {
        Map<String, String> attr = new HashMap<String, String>();
        int i;
        if (pm.contains(buffer, pattern)) {
            MatchResult mr = pm.getMatch();
            for (i=0; i<keys.length; i++)
                attr.put(keys[i], mr.group(i+1));
            return attr;
        }
        else
            return null;
    }
}
