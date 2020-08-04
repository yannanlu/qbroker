package org.qbroker.json;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.util.Map;
import org.qbroker.common.Utils;

/** Unit test for JSON2Map */
public class JSON2MapTest {
    private static String path = ".store.book[0].title";
    private static Map data;

    @BeforeClass
    public static void init() throws IOException {
        InputStream in = path.getClass().getResourceAsStream("/store.json");
        String text = Utils.read(in, new byte[4096]);
        in.close();
        data = (Map) JSON2Map.parse(new StringReader(text));
    }

    @Test
    public void testPath() {
        assertTrue(JSON2Map.containsKey(data, path));
    }

    @Test
    public void testGet() {
        assertEquals("Sayings of the Century", JSON2Map.get(data, path));
    }

    @Test
    public void testEval() {
        assertEquals("Sword of Honour", JSON2Map.get(data,
            ".store.book['#{author}' =~ '^Evelyn.+$'].title"));
    }

    @AfterClass
    public static void close() {
        data.clear();
    }
}
