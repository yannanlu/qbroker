package org.qbroker.net;

/* HtmlUnitDriverWithHeaders.java - an HtmlUnitDriver with headers */

import java.util.Map;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;

public class HtmlUnitDriverWithHeaders extends HtmlUnitDriver {
    public HtmlUnitDriverWithHeaders(BrowserVersion version,
        Map<String, String> ph) {
        super(version);
        if (ph != null) {
            WebClient client = this.getWebClient();
            for (String key : ph.keySet())
                client.addRequestHeader(key, ph.get(key));
        }
    }

    public HtmlUnitDriverWithHeaders(Map<String, String> ph) {
        super();
        if (ph != null) {
            WebClient client = this.getWebClient();
            for (String key : ph.keySet())
                client.addRequestHeader(key, ph.get(key));
        }
    }
}
