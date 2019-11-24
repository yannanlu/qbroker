package org.qbroker.common;

import java.util.Iterator;
import java.util.Enumeration;

/**
 * Enumerator implements Enumeration with an Iterator.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class Enumerator implements Enumeration {
    private Iterator iterator = null;

    public Enumerator(Iterator iter) {
        if (iter == null)
            throw(new IllegalArgumentException("null iterator"));
        iterator = iter;
    }

    public boolean hasMoreElements() {
        return iterator.hasNext();
    }

    public Object nextElement() {
        return iterator.next();
    }
}
