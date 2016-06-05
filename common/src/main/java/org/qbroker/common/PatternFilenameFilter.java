package org.qbroker.common;

/* PatternFilenameFilter.java - a FilenameFilter with Pattern support */

import java.io.File;
import java.io.FilenameFilter;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.StringSubstitution;
import org.apache.oro.text.regex.Util;
import org.apache.oro.text.regex.MalformedPatternException;

public class PatternFilenameFilter implements FilenameFilter {
    private Pattern pattern;
    private Perl5Matcher pm = null;

    public PatternFilenameFilter(String pattern) {
        if (pattern == null || pattern.length() <= 0)
            throw(new IllegalArgumentException("empty pattern"));
        try {
            pm = new Perl5Matcher();
            Perl5Compiler pc = new Perl5Compiler();
            this.pattern = pc.compile(pattern);
        }
        catch (MalformedPatternException e) {
            throw(new IllegalArgumentException("bad pattern: " + e.toString()));
        }
    }

    public boolean accept(File dir, String name) {
        if (name == null)
            return false;
        else {
            return pm.contains(name, pattern);
        }
    }
}
