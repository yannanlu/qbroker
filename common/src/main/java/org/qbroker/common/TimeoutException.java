package org.qbroker.common;

/**
 * TimeoutException is an exception for generic timeout.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TimeoutException extends Exception {

    public TimeoutException(String msg) {
        super(msg);
    }

    public TimeoutException() {
        this("timeout");
    }
}
