package org.qbroker.jms;

import static org.junit.Assert.*;
import org.junit.*;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import javax.jms.Message;
import javax.jms.JMSException;
import org.qbroker.event.Event;

/** Unit test for Aggregation */
public class AggregationTest {
    private static TextEvent msg = new TextEvent();
    private static TextEvent rpt = new TextEvent();

    @BeforeClass
    public static void init() throws JMSException {
        long tm = System.currentTimeMillis();
        List<Map> list = new ArrayList<Map>();
        List<Message> data = new ArrayList<Message>();
        TextEvent event;
        event = new TextEvent();
        MessageUtils.setProperty("user", "testuser", event);
        MessageUtils.setProperty("ip", "127.0.0.1", event);
        MessageUtils.setProperty("site", "ABC", event);
        MessageUtils.setProperty("time", "2020-08-02 09:46:18,697", event);
        MessageUtils.setProperty("count", "10", event);
        data.add(event);
        event = new TextEvent();
        MessageUtils.setProperty("user", "admuser", event);
        MessageUtils.setProperty("ip", "127.0.0.1", event);
        MessageUtils.setProperty("site", "XYZ", event);
        MessageUtils.setProperty("time", "2020-06-06 10:06:23,000", event);
        MessageUtils.setProperty("count", "5", event);
        data.add(event);
        event = new TextEvent();
        MessageUtils.setProperty("user", "testuser", event);
        MessageUtils.setProperty("ip", "127.0.0.1", event);
        MessageUtils.setProperty("site", "TEST", event);
        MessageUtils.setProperty("time", "2020-04-28 13:17:58,123", event);
        MessageUtils.setProperty("count", "2", event);
        data.add(event);
        Map<String, String> map = new HashMap<String, String>();
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
        Aggregation aggr = new Aggregation("test", list);
        aggr.initialize(tm, data.get(0), msg);
        aggr.aggregate(tm+1000, data.get(1), msg);
        aggr.aggregate(tm+2000, data.get(2), msg);
        aggr.initialize(tm, (Event) data.get(0), rpt);
        aggr.aggregate(tm+1000, (Event) data.get(1), rpt);
        aggr.aggregate(tm+2000, (Event) data.get(2), rpt);
        aggr.clear();
        for (Map m : list)
            m.clear();
        list.clear();
        data.clear();
    }

    @Test
    public void testUniqueUser() throws JMSException {
        assertEquals("2", MessageUtils.getProperty("user", msg));
    }

    @Test
    public void testTotolCount() throws JMSException {
        assertEquals("17", MessageUtils.getProperty("count", msg));
    }

    @Test
    public void testSite() throws JMSException {
        assertEquals("ABC,XYZ,TEST", MessageUtils.getProperty("site", msg));
    }

    @Test
    public void testTime() throws JMSException {
        assertEquals("2020-08-02 09:46:18,697",MessageUtils.getProperty("time",msg));
    }

    @Test
    public void testUniqueUser1() throws JMSException {
        assertEquals("2", MessageUtils.getProperty("user", rpt));
    }

    @Test
    public void testTotolCount1() throws JMSException {
        assertEquals("17", MessageUtils.getProperty("count", rpt));
    }

    @Test
    public void testSite1() throws JMSException {
        assertEquals("ABC,XYZ,TEST", MessageUtils.getProperty("site", rpt));
    }

    @Test
    public void testTime1() throws JMSException {
        assertEquals("2020-08-02 09:46:18,697",MessageUtils.getProperty("time",rpt));
    }
}
