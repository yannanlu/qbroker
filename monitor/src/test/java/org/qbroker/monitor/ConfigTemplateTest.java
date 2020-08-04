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

/** Unit test for ConfigTemplate */
public class ConfigTemplateTest {
    private static String file = "/monit_cfg.json";
    private static ConfigTemplate cfgTemp, cfgTemp1, cfgTemp2;
    private static Map map;

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void init() throws Exception {
        URL url = file.getClass().getResource("/monit.xml");
        InputStream in = file.getClass().getResourceAsStream(file);
        String text = Utils.read(in, new byte[4096]);
        in.close();
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        Map ph = (Map) props.get("Item");
        ph.put("URI", "file://" + url.getPath());
        cfgTemp = new ConfigTemplate(props);
        cfgTemp.updateItems(cfgTemp.getItemList());

        // for dynamic template with count = 2
        props.put("Template", "##name##_##pid##");
        ph.put("DataType", "0");
        ph.put("EvalTemplate", "'##name##' !~ '^(host|process).+$'");
        ph.remove("KeyTemplate");
        ph.remove("KeySubstitution");
        ph.remove("XPatternGroup");
        ph = (Map) props.get("Property");
        ph.put("ServiceName", "##name##");
        ph.put("StatsLog", "/var/log/qbroker/stats/##name##.proc");
        ph.put("Description", "monitoring the process ##pid## on Monit");
        cfgTemp1 = new ConfigTemplate(props);
        cfgTemp1.updateItems(cfgTemp1.getItemList());
        props.clear();

        // for static template with count = 4
        in = file.getClass().getResourceAsStream("/queue_cfg.json");
        text = Utils.read(in, new byte[4096]);
        in.close();
        props = (Map) JSON2Map.parse(new StringReader(text));
        map = (Map) ((List) props.get("Item")).get(2);
        cfgTemp2 = new ConfigTemplate(props);
    }

    @Test
    public void testCount() {
        assertEquals(1, cfgTemp.getCount());
    }

    @Test
    public void testSize() {
        assertEquals(14, cfgTemp.getSize());
    }

    @Test
    public void testKey() {
        assertEquals("monitorAgent", cfgTemp.getKey(5));
    }

    @Test
    public void testIsDynamic() {
        assertTrue(cfgTemp.isDynamic());
    }

    @Test
    public void testWithPrivateReport() {
        assertTrue(cfgTemp.withPrivateReport());
    }

    @Test
    public void testPrivateReport() {
        Object o;
        String text = null;
        Map r = cfgTemp.removePrivateReport(cfgTemp.getItem(5));
        if (r != null && (o = r.get("body")) != null)
            text = ((String) o).replaceFirst("^.+<pid>(\\d+)<.+$", "$1");
        assertEquals("1709", text);
    }

    @Test
    public void testCount1() {
        assertEquals(2, cfgTemp1.getCount());
    }

    @Test
    public void testSize1() {
        assertEquals(12, cfgTemp1.getSize());
    }

    @Test
    public void testKey1() {
        assertEquals("monitorAgent_1709", cfgTemp1.getKey(4));
    }

    @Test
    public void testItem1() {
        assertEquals("{\"name\":\"monitorAgent\",\"pid\":\"1709\"}",
            cfgTemp1.getItem(4));
    }

    @Test
    public void testConfig1() {
        Map ph = cfgTemp1.getProps(cfgTemp1.getItem(4));
        assertEquals("monitoring the process 1709 on Monit",
            (String) ph.get("Description"));
    }

    @Test
    public void testIsDynamic1() {
        assertTrue(cfgTemp1.isDynamic());
    }

    @Test
    public void testWithPrivateReport1() {
        assertFalse(cfgTemp1.withPrivateReport());
    }

    @Test
    public void testCount2() {
        assertEquals(4, cfgTemp2.getCount());
    }

    @Test
    public void testSize2() {
        assertEquals(4, cfgTemp2.getSize());
    }

    @Test
    public void testKey2() {
        assertEquals("IdmServiceQueue", cfgTemp2.getKey(2));
    }

    @Test
    public void testItem2() {
        assertEquals(JSON2Map.normalize(cfgTemp2.normalizeItem(map),null,null),
            cfgTemp2.getItem(2));
    }

    @Test
    public void testIsDynamic2() {
        assertFalse(cfgTemp2.isDynamic());
    }

    @AfterClass
    public static void close() {
        cfgTemp.close();
        cfgTemp1.close();
        cfgTemp2.close();
        map.clear();
    }
}
