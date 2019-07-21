package org.qbroker.jms;

import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.BytesBuffer;
import org.qbroker.net.JettyServer;
import org.qbroker.net.HTTPConnector;
import org.junit.Test;
import static org.junit.Assert.*;

/** Unit test for MessageServlet */
public class MessageServletTest {
    private Map<String, String> props = new HashMap<String, String>();
    private JettyServer server = null;
    private EchoService service = new EchoService();
    private HTTPConnector conn = null;
    private String path = "/test";
    private String uri = "http://localhost:8088/test/jms";
    private String query = "NAME=TEST&SITE=DEVOPS&CATEGORY=JETTY&view=deployment&site_name=PANDA&operation=list&service=WebAdmin&short_name=Console&asset=panda&type=JSON&priority=INFO&date=1563374278000&hostname=localhost&pid=0&tid=1&program=java&username=admin&summary=this is a test&delimiter= ~ &content=jsp ~ /toJSON.jsp";

    @Test
    public void testHttpPost() {
        int i;
        StringBuffer strBuf = new StringBuffer();
        BytesBuffer msgBuf = new BytesBuffer();
        BytesBuffer payload = new BytesBuffer();
        MessageServlet servlet;
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
            payload.write(query.getBytes(), 0, query.length());
            i = conn.doPost(uri, payload, strBuf, msgBuf);
            server.stop();
            service.stop();
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
