package org.qbroker.jms;

import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.BytesBuffer;
import org.qbroker.net.JettyServer;
import org.qbroker.net.HTTPConnector;
import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for MessageServlet */
public class MessageServletTest {
    private static JettyServer server = null;
    private static EchoService service = new EchoService();
    private static HTTPConnector conn = null;
    private static BytesBuffer payload = new BytesBuffer();
    private static String path = "/test";
    private static String uri = "http://localhost:8088/test/jms";
    private static String query = "NAME=TEST&SITE=DEVOPS&CATEGORY=JETTY&view=deployment&site_name=PANDA&operation=list&service=WebAdmin&short_name=Console&asset=panda&type=JSON&priority=INFO&date=1563374278000&hostname=localhost&pid=0&tid=1&program=java&username=admin&summary=this is a test&delimiter= ~ &content=jsp ~ /toJSON.jsp";

    @BeforeClass
    public static void init() {
        MessageServlet servlet;
        Map<String, String> props = new HashMap<String, String>();
        props.put("Name", "test");
        props.put("URI", uri);
        props.put("SOTimeout", "10");
        try {
            server = new JettyServer(props);
            servlet = new MessageServlet(props);
            servlet.setService(service);
            server.addContext(servlet, path + "/*");
            service.start();
            server.start();
            conn = new HTTPConnector(props);
        }
        catch (Exception e) {
        }
        payload.write(query.getBytes(), 0, query.length());
    }

    @Test
    public void testHttpPost() {
        int i = -2;
        StringBuffer strBuf = new StringBuffer();
        BytesBuffer msgBuf = new BytesBuffer();
        if (server != null) try {
            i = conn.doPost(uri, payload, strBuf, msgBuf);
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
        if (service != null) try {
            service.stop();
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
