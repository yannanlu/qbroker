package org.qbroker.common;

import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for Template */
public class TemplateTest {
    private static String text = "##hostname## is managed by ##owner##";
    private static Template temp = new Template(text);

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
