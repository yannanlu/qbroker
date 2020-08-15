package org.qbroker.persister;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.Utils;
import org.qbroker.common.XQueue;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;
import org.qbroker.receiver.GenericProvider;

/** Unit test for StreamPersister */
public class StreamPersisterTest {
    private static String file = "/rcvr_server.json";
    private static String cfg = "/pstr_stream.json";
    private static String content = "This is a test";
    private static GenericProvider provider;
    private static GenericProvider server;
    private static GenericRequester writer;
    private static GenericRequester requester;
    private static GenericRequester nohit;

    @BeforeClass
    public static void init() throws Exception {
        InputStream in = file.getClass().getResourceAsStream(file);
        String text = Utils.read(in, new byte[4096]);
        in.close();
        System.setProperty("ReportClass", "org.qbroker.jms.MessageUtils");
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        server = new GenericProvider(props);
        XQueue xq = server.getXQueue();
        props.put("Name", (String) props.get("Name") + "_1");
        props.put("Operation", "acquire");
        props.put("URI", "tcp://localhost:8089");
        provider = new GenericProvider(props);
        props.clear();
        props.put("Name", "nohit");
        props.put("ClassName", "org.qbroker.persister.LogPersister");
        props.put("URI", "log:///dev/null");
        props.put("Operation", "append");
        props.put("DataField", "body");
        nohit = new GenericRequester(props, xq);
        props.clear();
        in = file.getClass().getResourceAsStream(cfg);
        text = Utils.read(in, new byte[4096]);
        in.close();
        props = (Map) JSON2Map.parse(new StringReader(text));
        requester = new GenericRequester(props);
        props.put("Name", (String) props.get("Name") + "_1");
        props.put("Operation", "write");
        props.put("URI", "tcp://localhost:8089");
        writer = new GenericRequester(props);
        props.clear();
    }

    @Test
    public void testRequest() throws Exception {
        StringBuffer strBuf = new StringBuffer();
        int k = requester.getResponse(content + "\n", strBuf, false);
        assertEquals(1, k);
        assertEquals(content + "\n", strBuf.toString());
    }

/**
    @Test
    public void testRequest1() throws Exception {
        TextEvent msg = new TextEvent("Hello, World!\n");
        int k = requester.doRequest(msg, 2000);
        assertEquals(1, k);
        assertEquals("Hello, World!\n", msg.getText());
    }
*/

    @Test
    public void testWrite() throws Exception {
        StringBuffer strBuf = new StringBuffer();
        int k = writer.getResponse(content + "\n", strBuf, false);
        TextEvent msg = (TextEvent) provider.getMessage(1000);
        if (msg == null) // retry once more
            msg = (TextEvent) provider.getMessage(1000);
        assertEquals(1, k);
        assertEquals(content + "\n", msg.getText());
    }

    @AfterClass
    public static void tearDown() {
        if (writer != null)
            writer.close();
        writer = null;
        if (requester != null)
            requester.close();
        requester = null;
        if (nohit != null)
            nohit.close();
        nohit = null;
        if (provider != null)
            provider.close();
        provider = null;
        if (server != null)
            server.close();
        server = null;
    }
}
