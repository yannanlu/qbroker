package org.qbroker.jaas;

/* SimpleCallbackHandler.java - a handler for known username and password */

import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * @author yannanlu@yahoo.com
 */

public class SimpleCallbackHandler implements CallbackHandler {
    private String username;
    private String password;

    public SimpleCallbackHandler() {
        username = "";
        password = "";
    }

    public void handle(final Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
        int i, n;
        if (callbacks == null || (n = callbacks.length) <= 0)
            throw new IllegalArgumentException("null or empty callbacks");
        for (i=0; i<n; i++) {
            final Callback cb = callbacks[i];
            if (cb == null)
                continue;
            else if (cb instanceof NameCallback)
                ((NameCallback) cb).setName(username);
            else if (cb instanceof PasswordCallback)
                ((PasswordCallback) cb).setPassword(password.toCharArray());
            else
                throw new UnsupportedCallbackException(cb);
        }
    }

    public void setName(String username) {
        this.username = (username == null) ? "" : username;
    }

    public void setPassword(String password) {
        this.password = (password == null) ? "" : password;
    }
}
