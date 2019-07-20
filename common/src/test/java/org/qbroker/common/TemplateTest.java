package org.qbroker.common;

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit test for Template */
public class TemplateTest {
    private String text = "##hostname## is managed by ##owner##";
    private Template temp = new Template(text);

    @Test
    public void testContainsField() {
        assertTrue(temp.containsField("owner")&&temp.containsField("hostname"));
    }

    @Test
    public void testSubstitute() {
        String line = temp.substitute("owner", "my team", temp.copyText());
        line = temp.substitute("hostname", "localhost", line);
        assertEquals( "localhost is managed by my team", line );
    }
}
