package org.qbroker.common;

/**
 * NonSQLException is an exception for generic non-SQL data store. It is a
 * wrapper of vendor's exception.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class NonSQLException extends Exception {
    private Exception ex = null;

    public NonSQLException(Exception e, String msg) {
        super(msg);
        ex = e;
    }

    public NonSQLException(String msg) {
        this(null, msg);
    }

    public NonSQLException() {
        this("non-SQL failure");
    }

    public Exception getLinkedException() {
        return ex;
    }
}
