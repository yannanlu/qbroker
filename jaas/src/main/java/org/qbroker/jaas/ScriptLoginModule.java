package org.qbroker.jaas;

/* ScriptLoginModule.java - a LoginModule running script for authentications */

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.net.UnknownHostException;
import java.io.IOException;
import javax.security.auth.*;
import javax.security.auth.callback.*;
import javax.security.auth.login.*;
import javax.security.auth.spi.*;
import com.sun.security.auth.UnixPrincipal;
import org.qbroker.event.Event;
import org.qbroker.monitor.ScriptLauncher;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;

/**
 * @author yannanlu@yahoo.com
 */

public class ScriptLoginModule implements LoginModule {
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map sharedState;
    private Map options;
    private boolean debug = false;
    private boolean succeeded = false;
    private boolean commitSucceeded = false;
    private String username;
    private Map<String, Object> props;
    private Template template = null;
    private UnixPrincipal userPrincipal;

    public void initialize(Subject subject, CallbackHandler callbackHandler,
        Map sharedState, Map options) {
        Object o;
        String hostname = "localhost";
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
        debug = "true".equalsIgnoreCase((String)options.get("debug"));
        if (options == null)
            throw new IllegalArgumentException("empty options");
        props = new HashMap<String, Object>();
        Iterator iter = options.keySet().iterator();
        while (iter.hasNext()) {
            o = iter.next();
            if (o == null || !(o instanceof String) ||
                "debug".equals((String) o))
                continue;
            props.put((String) o, options.get((String) o));
            if ("Script".equals((String) o))
                template = new Template((String) props.get("Script"));
        }
        if (template == null)
            throw new IllegalArgumentException("Script not defined");
    }

    public boolean login() throws LoginException {
        char[] password;
        // prompt for a user name and password
        if (callbackHandler == null)
            throw new LoginException("Error: no CallbackHandler available " +
                "to garner authentication information from the user");

        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("username: ");
        callbacks[1] = new PasswordCallback("password: ", false);
 
        try {
            callbackHandler.handle(callbacks);
            username = ((NameCallback) callbacks[0]).getName();
            char[] tmpPassword = ((PasswordCallback)callbacks[1]).getPassword();
            if (tmpPassword == null) {
                // treat a NULL password as an empty password
                tmpPassword = new char[0];
            }
            password = new char[tmpPassword.length];
            System.arraycopy(tmpPassword, 0,
                password, 0, tmpPassword.length);
            ((PasswordCallback) callbacks[1]).clearPassword();
        }
        catch (java.io.IOException ioe) {
            throw new LoginException(ioe.toString());
        }
        catch (UnsupportedCallbackException uce) {
            throw new LoginException("Error: " + uce.getCallback().toString() +
                " not available to garner authentication information " +
                "from the user");
        }

        String script = template.copyText();
        // print debugging information
        if (debug) {
            new Event(Event.DEBUG, "[ScriptLoginModule] username: " +
                username).send();
            new Event(Event.DEBUG, "[ScriptLoginModule] script: " +
                script).send();
        }

        Map<String, Object> ph = Utils.cloneProperties(props);
        // verify the username/password
        try {
            script = template.substitute("username", username, script);
            script= template.substitute("password",new String(password),script);
            ph.put("Script", script);
            ScriptLauncher reporter = new ScriptLauncher(ph);
            ph = reporter.generateReport(System.currentTimeMillis());
        }
        catch (Exception e) {
            succeeded = false;
            username = null;
            throw new LoginException(e.toString());
        }
        String rc =  (String) ph.get("ReturnCode");
        if ("0".equals(rc)) {
            succeeded = true;
            if (debug)
                new Event(Event.DEBUG, "[ScriptLoginModule] authentication " +
                    "succeeded").send();
        }
        else {
            succeeded = false;
            if (debug)
                new Event(Event.DEBUG, "[ScriptLoginModule] authentication " +
                    "failed: " + rc).send();
        }
        return succeeded;
    }

    public boolean commit() throws LoginException {
        if (succeeded == false) {
            return false;
        }
        else {
            // assume the user we authenticated is the UnixPrincipal
            userPrincipal = new UnixPrincipal(username);
            if (!subject.getPrincipals().contains(userPrincipal))
                subject.getPrincipals().add(userPrincipal);

            if (debug) {
                new Event(Event.DEBUG, "[ScriptLoginModule] added " +
                    "the UnixPrincipal to Subject").send();
            }

            // in any case, clean out state
            username = null;

            commitSucceeded = true;
            return true;
        }
    }

    public boolean abort() throws LoginException {
        userPrincipal = null;
        if (succeeded == false) {
            return false;
        }
        else if (succeeded == true && commitSucceeded == false) {
            // login succeeded but overall authentication failed
            succeeded = false;
            username = null;
        }
        else {
            // overall authentication succeeded and commit succeeded,
            // but someone else's commit failed
            logout();
        }
        return true;
    }

    public boolean logout() throws LoginException {
        subject.getPrincipals().remove(userPrincipal);
        succeeded = false;
        succeeded = commitSucceeded;
        username = null;
        userPrincipal = null;
        return true;
    }
}
