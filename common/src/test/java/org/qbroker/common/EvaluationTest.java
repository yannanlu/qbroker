package org.qbroker.common;

import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for Evaluation */
public class EvaluationTest {
    private String expr = "('abc' == 'ABC') ? 1 : 0";
    private String strExpr = "(2 > 1) ? 'ABC' : 'abc'";
    private Template temp = new Template("'##name##' =~ '^prod.+$'"); 

    @Test
    public void testStringTernary() {
        assertTrue(Evaluation.isStringTernary(strExpr));
    }

    @Test
    public void testNotStringTernary() {
        assertFalse(Evaluation.isStringTernary(expr));
    }

    @Test
    public void testEvaluate() {
        assertEquals( new Long(0), Evaluation.evaluate(expr) );
    }

    @Test
    public void testCalculate() {
        assertEquals( new Long(1), Evaluation.evaluate("(1 + 2 * 3) % 3") );
    }

    @Test
    public void testChoose() {
        assertEquals("'ABC'", Evaluation.choose(strExpr));
    }

    @Test
    public void testMatch() {
        String line = temp.substitute("name", "prod1", temp.copyText());
        assertTrue(Evaluation.evaluate(line).intValue() == 1 );
    }

    @Test
    public void testNotMatch() {
        String line = temp.substitute("name", "Prod1", temp.copyText());
        assertTrue(Evaluation.evaluate(line).intValue() == 0 );
    }
}
