package org.qbroker.node;

import static org.junit.Assert.*;
import org.junit.*;
import java.io.InputStream;
import java.io.StringReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.jms.TextEvent;

/** Unit test for ParserNode */
public class ParserNodeTest {
    private static String file = "/service_parser.json";
    private static String content = "[07/Aug/2020:11:23:53 -0400] 127.0.0.1 - testuser \"GET / HTTP/1.1\" 200 2310 \"https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1\" \"Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0\" 12609";
    private static TextEvent msg;

    @BeforeClass
    public static void init() throws Exception {
        InputStream in = file.getClass().getResourceAsStream(file);
        String text = Utils.read(in, new byte[4096]);
        in.close();
        System.setProperty("ReportClass", "org.qbroker.jms.MessageUtils");
        Map props = (Map) JSON2Map.parse(new StringReader(text));
        SingleNodeService service = SingleNodeService.initService(props);
        service.start();

        msg = new TextEvent(content);
        int k = service.doRequest(msg, 2000);
        msg.setGroupID(k);
         
        if (service != null) try {
            service.close();
        }
        catch (Exception e) {
        }
    }

    @Test
    public void testReturnCode() {
        assertEquals(1, msg.getGroupID());
    }

    @Test
    public void testLogTime() {
        assertEquals("07/Aug/2020:11:23:53 -0400", msg.getAttribute("LogTime"));
    }

    @Test
    public void testSource() {
        assertEquals("127.0.0.1", msg.getAttribute("Source"));
    }

    @Test
    public void testOwner() {
        assertEquals("testuser", msg.getAttribute("owner"));
    }

    @Test
    public void testProgram() {
        assertEquals("GET", msg.getAttribute("program"));
    }

    @Test
    public void testPid() {
        assertEquals("200", msg.getAttribute("pid"));
    }

    @Test
    public void testSize() {
        assertEquals("2310", msg.getAttribute("Size"));
    }

    @Test
    public void testText() {
        assertEquals("https://login.qbroker.org/idp/profile/SAML2/Redirect/SSO?execution=e1s1", msg.getAttribute("text"));
    }

    @Test
    public void testDuration() {
        assertEquals("12609", msg.getAttribute("Duration"));
    }
}
