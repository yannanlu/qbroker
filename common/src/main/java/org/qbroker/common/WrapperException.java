package org.qbroker.common;

/**
 * WrapperException is an Exception to chain a generic Exception.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class WrapperException extends Exception {

    public WrapperException(String msg, Exception ex) {
        super(msg, ex);
    }

    public WrapperException(String msg) {
        super(msg, null);
    }

    public WrapperException() {
        this("empty wrapper", null);
    }
}
