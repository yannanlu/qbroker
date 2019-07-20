package org.qbroker.common;

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit test for Evaluation */
public class EvaluationTest {
    private String expr = "('abc' == 'ABC') ? 1 : 0";
    private String strExpr = "(2 > 1) ? 'ABC' : 'abc'";

    @Test
    public void testStringExpression() {
        assertTrue(Evaluation.isStringExpression(strExpr));
    }

    @Test
    public void testNotStringExpression() {
        assertFalse(Evaluation.isStringExpression(expr));
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
}
