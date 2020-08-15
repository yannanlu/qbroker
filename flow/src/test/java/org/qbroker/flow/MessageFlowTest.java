package org.qbroker.flow;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.Utils;
import org.qbroker.event.Event;
import org.qbroker.event.EventParser;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;
import org.qbroker.persister.GenericRequester;

/** Unit test for MessageFlow */
public class MessageFlowTest {
    private static String cfg = "/pstr_stream.json";
    private static String file = "/flow_parser.json";
    private static String content = "[07/Aug/2020:11:23:53 -0400] 127.0.0.1 - testuser \"GET / HTTP/1.1\" 200 2310 \"https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1\" \"Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0\" 12609";
    private static GenericRequester requester;
    private static EventParser parser;
    private static MessageFlow flow;

    @BeforeClass
    public static void init() throws Exception {
        InputStream in = file.getClass().getResourceAsStream(file);
        String text = Utils.read(in, new byte[4096]);
        in.close();
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        flow = new MessageFlow(props);
        flow.start();
        props.clear();
        in = file.getClass().getResourceAsStream(cfg);
        text = Utils.read(in, new byte[4096]);
        in.close();
        props = (Map) JSON2Map.parse(new StringReader(text));
        requester = new GenericRequester(props);
        props.clear();
        parser = new EventParser("^(.+)$");
    }

    @Test
    public void testParse() throws Exception {
        StringBuffer strBuf = new StringBuffer();
        int k = requester.getResponse(content + "\n", strBuf, false);
        strBuf.deleteCharAt(strBuf.length() - 1);
        Event event = parser.parse(strBuf.toString());
        assertEquals(1, k);
        assertEquals("07/Aug/2020:11:23:53 -0400",
            event.getAttribute("LogTime"));
        assertEquals("127.0.0.1", event.getAttribute("Source"));
        assertEquals("testuser", event.getAttribute("owner"));
        assertEquals("GET", event.getAttribute("program"));
        assertEquals("200", event.getAttribute("pid"));
        assertEquals("2310", event.getAttribute("Size"));
        assertEquals("https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1", event.getAttribute("text"));
        assertEquals("12609", event.getAttribute("Duration"));
    }

    @AfterClass
    public static void tearDown() {
        if (requester != null)
            requester.close();
        requester = null;
        if (flow != null)
            flow.stop();
        flow = null;
    }
}
