package org.qbroker.monitor;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.net.URL;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;

/** Unit test for GenericList */
public class GenericListTest {
    private static List<String> list = new ArrayList<String>();

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void init() throws IOException {
        URL url = list.getClass().getResource("/monit.xml");
        InputStream in = list.getClass().getResourceAsStream("/monit.json");
        String text = Utils.read(in, new byte[4096]);
        in.close();
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        props.put("URI", "file://" + url.getPath());
        GenericList report = new GenericList(props);
        Map r = report.generateReport(0L);
        List pl = (List) r.remove("List");
        r.clear();
        if (pl != null) {
            for (Object o : pl) {
                list.add((String) o);
            }
        }
        props.clear();
        report.destroy();
    }

    @Test
    public void testCount() {
        assertEquals(14, list.size());
    }

    @Test
    public void testItem() {
        assertEquals("monitorAgent", list.get(5));
    }

    @AfterClass
    public static void close() {
        list.clear();
    }
}
