package org.qbroker.common;

import static org.junit.Assert.*;
import org.junit.*;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/** Unit test for Aggregator */
public class AggregatorTest {
    private static Map<String, String> rpt = new HashMap<String, String>();

    @BeforeClass
    public static void init() {
        long tm = System.currentTimeMillis();
        Map<String, String> map;
        List<Map> data = new ArrayList<Map>();
        List<Map> list = new ArrayList<Map>();
        map = new HashMap<String, String>();
        map.put("user", "testuser");
        map.put("ip", "127.0.0.1");
        map.put("site", "ABC");
        map.put("time", "2020-08-02 09:46:18,697");
        map.put("count", "10");
        data.add(map);
        map = new HashMap<String, String>();
        map.put("user", "admuser");
        map.put("ip", "127.0.0.1");
        map.put("site", "XYZ");
        map.put("time", "2020-06-06 10:06:23,000");
        map.put("count", "5");
        data.add(map);
        map = new HashMap<String, String>();
        map.put("user", "testuser");
        map.put("ip", "127.0.0.1");
        map.put("site", "TEST");
        map.put("time", "2020-04-28 13:17:58,123");
        map.put("count", "2");
        data.add(map);
        map = new HashMap<String, String>();
        map.put("FieldName", "user");
        map.put("Operation", "unique_count");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("FieldName", "count");
        map.put("Operation", "sum");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("FieldName", "site");
        map.put("Operation", "append");
        map.put("Delimiter", ",");
        list.add(map);
        map = new HashMap<String, String>();
        map.put("FieldName", "time");
        map.put("Operation", "latest");
        map.put("TimePattern", "yyyy-MM-dd HH:mm:ss,SSS");
        list.add(map);
        Aggregator aggr = new Aggregator("test", list);
        aggr.initialize(tm, data.get(0), rpt);
        aggr.aggregate(tm+1000, data.get(1), rpt);
        aggr.aggregate(tm+2000, data.get(2), rpt);
        aggr.clear();
        for (Map m : list)
            m.clear();
        list.clear();
        for (Map m : data)
            m.clear();
        data.clear();
    }

    @Test
    public void testUniqueUser() {
        assertEquals("2", rpt.get("user"));
    }

    @Test
    public void testTotolCount() {
        assertEquals("17", rpt.get("count"));
    }

    @Test
    public void testSite() {
        assertEquals("ABC,XYZ,TEST", rpt.get("site"));
    }

    @Test
    public void testTime() {
        assertEquals("2020-08-02 09:46:18,697", rpt.get("time"));
    }

    @AfterClass
    public static void close() {
        rpt.clear();
    }
}
