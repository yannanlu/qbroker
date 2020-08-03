package org.qbroker.net;

import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.BytesBuffer;
import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for SimpleHttpServer */
public class SimpleHttpServerTest {
    private static SimpleHttpServer server = null;
    private static HTTPConnector conn = null;
    private static String path = "/test";
    private static String uri = "http://localhost:8088/test";

    @BeforeClass
    public static void init() {
        Map<String, String> props = new HashMap<String, String>();
        props.put("Name", "test");
        props.put("URI", uri);
        props.put("Timeout", "10");
        props.put("SOTimeout", "10");
        try {
            server = new SimpleHttpServer(props);
            server.addContext(server, path);
            server.start();
            conn = new HTTPConnector(props);
        }
        catch (Exception e) {
        }
    }

    @Test
    public void testHttpGet() {
        int i = -2;
        StringBuffer strBuf = new StringBuffer();
        BytesBuffer msgBuf = new BytesBuffer();
        if (conn != null) try {
            i = conn.doGet(uri, strBuf, msgBuf);
        }
        catch (Exception e) {
            i = -1;
        }
        if (i != 200)
            System.out.println(i + ": " + strBuf.toString());
        assertTrue( i == 200 );
    }

    @AfterClass
    public static void tearDown() {
        if (server != null) try {
            server.stop();
        }
        catch (Exception e) {
        }
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
    }
}
