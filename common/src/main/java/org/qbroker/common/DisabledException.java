package org.qbroker.common;

/**
 * DisabledException is a generic exception for a disabled object.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class DisabledException extends RuntimeException {

    public DisabledException(String msg) {
        super(msg);
    }

    public DisabledException() {
        this("disabled");
    }
}
