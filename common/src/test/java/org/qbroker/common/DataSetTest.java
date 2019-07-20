package org.qbroker.common;

import java.util.List;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

/** Unit test for simple DataSet. */
public class DataSetTest {
    private List<String> interval = Arrays.asList("(0, 5]", "[11, 16)");
    private List<String> range = Arrays.asList("(0.0, 5.0]", "[11.0, 16.0)");
    private DataSet ds = new DataSet(interval);
    private DataSet dd = new DataSet(range);

    @Test
    public void testContainsLong() {
        assertTrue( ds.contains(3) );
    }

    @Test
    public void testNotContainsLong() {
        assertFalse( ds.contains(8) );
    }

    @Test
    public void testContainsDouble() {
        assertTrue( dd.contains(3.0) );
    }

    @Test
    public void testNotContainsDouble() {
        assertFalse( dd.contains(8.0) );
    }
}
