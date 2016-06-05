package org.qbroker.common;

/**
 * RandomNumber generates random numbers between 0 and 1.
 *<br/><br/>
 * It is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RandomNumber {
    private double y;
    private long[] table;

    private final static long MAX_INT = 65535L;
    private final static double NORM = 0.2328306437e-9;

    public RandomNumber(int seed) {
        setSeed(seed);
    }

    public synchronized double getNext() {
        int rn, id;
        id = (int) (97 * y);
        id %= 97;
        rn = (int) table[id];
        y = 0.5 + NORM * rn;
        rn = (int) (rn * MAX_INT + 1L);
        table[id] = (long) rn;
        return y;
    }

    private void setSeed(int seed) {
        int rn;
        if (seed != 0)
            rn = seed;
        else
            rn = (int) System.currentTimeMillis();

        for (int i=0; i<13; i++)
            rn = (int) (rn * MAX_INT + 1L);

        table = new long[97];
        for (int i=0; i<97; i++) {
            rn = (int) (rn * MAX_INT + 1L);
            table[i] = (long) rn;
        }

        rn = (int) (rn * MAX_INT + 1L);
        y = 0.5 + NORM * rn;
    }
}
