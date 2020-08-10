package org.qbroker.common;

import static org.junit.Assert.*;
import org.junit.*;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/** Unit test for SimpleParser */
public class SimpleParserTest {
    private static String text = "[07/Aug/2020:11:23:53 -0400] 127.0.0.1 - testuser \"GET / HTTP/1.1\" 200 2310 \"https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1\" \"Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0\" 12609";
    private static List<Map> list = new ArrayList<Map>();
    private static Map<String, String> map;

    @BeforeClass
    public static void init() {
        map = new HashMap<String, String>();
        map.put("LogTime","^\\[(\\d+\\/\\w+\\/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+)\\] ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("Source", "(\\d+\\.\\d+\\.\\d+\\.\\d+) [^ ]+ ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("owner", "([^ ]+) ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("program", "\"([^ ]+) [^\"]+\" ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("pid", "(\\d+) ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("Size", "(\\d+) ");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("text", "\"([^\"]+)\"");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("Duration", ".+ (\\d+)$");
        list.add(map);
        SimpleParser parser = new SimpleParser(list);
        map = parser.parse(text);
    }

    @Test
    public void testLogTime() {
        assertEquals("07/Aug/2020:11:23:53 -0400", map.get("LogTime"));
    }

    @Test
    public void testSource() {
        assertEquals("127.0.0.1", map.get("Source"));
    }

    @Test
    public void testOwner() {
        assertEquals("testuser", map.get("owner"));
    }

    @Test
    public void testProgram() {
        assertEquals("GET", map.get("program"));
    }

    @Test
    public void testPid() {
        assertEquals("200", map.get("pid"));
    }

    @Test
    public void testSize() {
        assertEquals("2310", map.get("Size"));
    }

    @Test
    public void testText() {
        assertEquals("https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1", map.get("text"));
    }

    @Test
    public void testDuration() {
        assertEquals("12609", map.get("Duration"));
    }

    @AfterClass
    public static void close() {
        for (Map m : list)
            m.clear();
        list.clear();
        if (map != null)
            map.clear();
    }
}
