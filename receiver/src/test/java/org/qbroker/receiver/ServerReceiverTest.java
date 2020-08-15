package org.qbroker.receiver;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.Utils;
import org.qbroker.net.ClientSocket;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;

/** Unit test for ServerReceiver */
public class ServerReceiverTest {
    private static String file = "/rcvr_server.json";
    private static String content = "This is a test";
    private static GenericProvider provider;
    private static GenericProvider service;

    @BeforeClass
    public static void init() throws Exception {
        InputStream in = file.getClass().getResourceAsStream(file);
        String text = Utils.read(in, new byte[4096]);
        in.close();
        System.setProperty("ReportClass", "org.qbroker.jms.MessageUtils");
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        provider = new GenericProvider(props);
        props.put("Name", (String) props.get("Name") + "_1");
        props.put("Operation", "respond");
        props.put("URI", "tcp://localhost:8089");
        service = new GenericProvider(props);
        props.clear();
    }

    @Test
    public void testAcquire() throws Exception {
        Socket sock = ClientSocket.connect("localhost", 8087, 5000);
        sock.getOutputStream().write(content.getBytes(), 0, content.length());
        sock.getOutputStream().write(10);
        sock.getOutputStream().flush();
        sock.close();
        TextEvent msg = (TextEvent) provider.getMessage(1000);
        if (msg == null) // retry once more
            msg = (TextEvent) provider.getMessage(1000);
        assertEquals(5, msg.getPriority());
        assertEquals(content + "\n", msg.getText());
        assertEquals("TEST", msg.getAttribute("category"));
    }

    @Test
    public void testRespond() throws Exception {
        Socket sock = ClientSocket.connect("localhost", 8089, 5000);
        sock.getOutputStream().write(content.getBytes(), 0, content.length());
        sock.getOutputStream().write(10);
        sock.getOutputStream().flush();
        TextEvent msg = (TextEvent) service.getMessage(1000);
        if (msg == null) // retry once more
            msg = (TextEvent) service.getMessage(1000);
        sock.close();
        assertEquals(content + "\n", msg.getText());
    }

    @AfterClass
    public static void tearDown() {
        if (service != null)
            service.close();
        service = null;
        if (provider != null)
            provider.close();
        provider = null;
    }
}
