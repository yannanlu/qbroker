package org.qbroker.common;

import static org.junit.Assert.*;
import org.junit.*;
import java.util.Map;
import java.util.HashMap;

/** Unit test for TextSubstituation */
public class TextSubstitutionTest {
    private static String expr = "('abc' == 'ABC') ? 1 : 0";
    private static String strExpr = "(2 > 1) ? 'ABC' : 'abc'";
    private static String strReplace ="This test is a good unit test!test!TEST";
    private static Map<String, TextSubstitution> tSub =
        new HashMap<String, TextSubstitution>();

    @BeforeClass
    public static void init() {
        tSub.put("eval", new TextSubstitution("s//:=eval/e"));
        tSub.put("parse", new TextSubstitution("s//~=yyyy-MM-dd HH:mm:ss.SSS/e"));
        tSub.put("format", new TextSubstitution("s//#=yyyy-MM-dd HH:mm:ss.SSS/e"));
        tSub.put("pow", new TextSubstitution("s//:=pow 2.0/e"));
        tSub.put("rand", new TextSubstitution("s//:=rand 100/e"));
        tSub.put("abs", new TextSubstitution("s//:=abs/e"));
        tSub.put("exp", new TextSubstitution("s//:=exp/e"));
        tSub.put("log", new TextSubstitution("s//:=log/e"));
        tSub.put("sqrt", new TextSubstitution("s//:=sqrt/e"));
        tSub.put("time", new TextSubstitution("s//:=time/e"));
        tSub.put("lower", new TextSubstitution("s//:=lower/e"));
        tSub.put("upper", new TextSubstitution("s//:=upper/e"));
        tSub.put("encode", new TextSubstitution("s//:=encode/e"));
        tSub.put("decode", new TextSubstitution("s//:=decode/e"));
        tSub.put("md5", new TextSubstitution("s//:=md5/e"));
        tSub.put("chop", new TextSubstitution("s//:=chop/e"));
        tSub.put("replace", new TextSubstitution("s//:=replace !/e"));
        tSub.put("rsub", new TextSubstitution("s//:=sub !/e"));
        tSub.put("gsub", new TextSubstitution("s//:=gsub !/e"));
    }

    @Test
    public void testFormat() {
        assertEquals("2020-08-04 07:17:13.484",
            tSub.get("format").substitute("1596539833484"));
    }

    @Test
    public void testParse() {
        assertEquals("1596539833484",
            tSub.get("parse").substitute("2020-08-04 07:17:13.484"));
    }

    @Test
    public void testEval() {
        assertEquals("0", tSub.get("eval").substitute(expr));
    }

    @Test
    public void testEval1() {
        assertEquals("ABC", tSub.get("eval").substitute(strExpr));
    }

    @Test
    public void testReplace() {
        String str ="This test is a good unit test!test!TEST";
        assertEquals("This TEST is a good unit TEST",
            tSub.get("replace").substitute(strReplace));
    }

    @Test
    public void testRSub() {
        String str ="<service><pid>1203</pid></service>!^.+<pid>(\\d+)<.+$!$1";
        assertEquals("1203", tSub.get("rsub").substitute(str));
    }

    @Test
    public void testGSub() {
        String str ="1203, 675, 3871, 904!(\\d+)![0, $1]";
        assertEquals("[0, 1203], [0, 675], [0, 3871], [0, 904]",
            tSub.get("gsub").substitute(str));
    }

    @AfterClass
    public static void close() {
        for (String key : tSub.keySet())
            tSub.get(key).clear();
        tSub.clear();
    }
}
