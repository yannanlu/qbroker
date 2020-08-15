package org.qbroker.net;

import java.util.Map;
import java.util.HashMap;
import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for DBConnector */
public class DBConnectorTest {
    private static DBConnector conn = null;
    private static DBConnector conn2 = null;
    private static String uri = "jdbc:hsqldb:mem:testdb;shutdown=true";
    private static String uri2 = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";

    @BeforeClass
    public static void init() throws Exception {
        Map<String, String> props = new HashMap<String, String>();
        props.put("Name", "test");
        props.put("URI", uri);
        props.put("DBTimeout", "10");
        props.put("Username", "sa");
        props.put("Password", "");
        conn = new DBConnector(props);
        conn.executeQuery("create table foo (id integer not null, name varchar(64) not null)");
        conn.executeQuery("insert into foo (id, name) values (1, 'test')");
        conn.executeQuery("insert into foo (id, name) values (2, 'unit')");
        props.put("URI", uri2);
        conn2 = new DBConnector(props);
        conn2.executeQuery("create table foo (id integer not null, name varchar(64) not null)");
        conn2.executeQuery("insert into foo (id, name) values (1, 'test')");
        conn2.executeQuery("insert into foo (id, name) values (2, 'unit')");
        props.clear();
    }

    @Test
    public void testSelect() throws Exception {
        StringBuffer strBuf;
        int[] rc = new int[2];
        strBuf = conn.select("SELECT name FROM foo WHERE id = 1", rc);
        assertEquals(1, rc[0]);
        assertEquals(1, rc[1]);
        assertEquals("test\n", strBuf.toString());
    }

    @Test
    public void testUpdate() throws Exception {
        StringBuffer strBuf;
        int[] rc = new int[2];
        int i = conn.executeQuery("UPDATE foo SET name = 'junit' WHERE id = 2");
        strBuf = conn.select("SELECT * FROM foo WHERE id = 2", rc);
        assertEquals(1, i);
        assertEquals(1, rc[0]);
        assertEquals(2, rc[1]);
        assertEquals("2 | junit\n", strBuf.toString());
    }

    @Test
    public void testSelect2() throws Exception {
        StringBuffer strBuf;
        int[] rc = new int[2];
        strBuf = conn2.select("SELECT name FROM foo WHERE id = 1", rc);
        assertEquals(1, rc[0]);
        assertEquals(1, rc[1]);
        assertEquals("test\n", strBuf.toString());
    }

    @Test
    public void testUpdate2() throws Exception {
        StringBuffer strBuf;
        int[] rc = new int[2];
        int i =conn2.executeQuery("UPDATE foo SET name = 'junit' WHERE id = 2");
        strBuf = conn2.select("SELECT * FROM foo WHERE id = 2", rc);
        assertEquals(1, i);
        assertEquals(1, rc[0]);
        assertEquals(2, rc[1]);
        assertEquals("2 | junit\n", strBuf.toString());
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        if (conn2 != null) try {
            conn2.close();
        }
        catch (Exception e) {
        }
    }
}
