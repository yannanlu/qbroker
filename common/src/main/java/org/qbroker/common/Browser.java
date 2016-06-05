package org.qbroker.common;

/* Browser.java - an Interface of browser for ids */

/**
 * Browser is an Interface for browsing items from an indexed data set.
 * It is assumed that there is an unique, non-negative ID associated with
 * each item in the data set.
 */

public interface Browser {
    /** returns the id of the next item or -1 if none left */
    public int next();
    /** resets the browser so that it can browse again */
    public void reset();
}
