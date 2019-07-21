package org.qbroker.net;

import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.BytesBuffer;
import org.junit.Test;
import static org.junit.Assert.*;

/** Unit test for JettyServer */
public class JettyServerTest {
    private Map<String, String> props = new HashMap<String, String>();
    private JettyServer server = null;
    private HTTPConnector conn = null;
    private String path = "/test";
    private String uri = "http://localhost:8088/test";

    @Test
    public void testHttpGet() {
        int i;
        StringBuffer strBuf = new StringBuffer();
        BytesBuffer msgBuf = new BytesBuffer();
        props.put("Name", "test");
        props.put("URI", uri);
        props.put("Timeout", "10");
        props.put("SOTimeout", "10");
        try {
            server = new JettyServer(props);
            server.addContext(server, path + "/*");
            server.start();
            conn = new HTTPConnector(props);
            i = conn.doGet(uri, strBuf, msgBuf);
            server.stop();
            conn.close();
        }
        catch (Exception e) {
            i = -1;
        }
        if (i != 200)
            System.out.println(i + ": " + strBuf.toString());
        assertTrue( i == 200 );
    }
}
