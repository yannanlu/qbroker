package org.qbroker.jaas;

/* CIFSLoginModule.java - a LoginModule using CIFS on Win2K authentications */

import java.util.List;
import java.util.Map;
import java.net.UnknownHostException;
import java.io.IOException;
import javax.security.auth.*;
import javax.security.auth.callback.*;
import javax.security.auth.login.*;
import javax.security.auth.spi.*;
import com.sun.security.auth.NTUserPrincipal;
import jcifs.UniAddress;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbSession;
import jcifs.smb.SmbException;
import jcifs.smb.SmbAuthException;
import org.qbroker.event.Event;

/**
 * @author yannanlu@yahoo.com
 */

public class CIFSLoginModule implements LoginModule {
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map sharedState;
    private Map options;
    private boolean debug = false;
    private boolean succeeded = false;
    private boolean commitSucceeded = false;
    private String username;
    private char[] password;
    private String domain = "localdomain";
    private UniAddress dc;
    private NTUserPrincipal userPrincipal;

    public void initialize(Subject subject, CallbackHandler callbackHandler,
        Map sharedState, Map options) {
        Object o;
        String hostname = "localhost";
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
        if ((o = options.get("domain")) != null)
            domain = (String) o;
        if ((o = options.get("hostname")) != null)
            hostname = (String) o;
        try {
            dc = UniAddress.getByName(hostname);
        }
        catch (UnknownHostException e) {
            throw(new IllegalArgumentException(e.toString()));
        }
        debug = "true".equalsIgnoreCase((String)options.get("debug"));
    }

    public boolean login() throws LoginException {
        NtlmPasswordAuthentication auth;
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
        } catch (java.io.IOException ioe) {
            throw new LoginException(ioe.toString());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException("Error: " + uce.getCallback().toString() +
                " not available to garner authentication information " +
                "from the user");
        }

        // print debugging information
        if (debug) {
            new Event(Event.DEBUG, "[CIFSLoginModule] username: " +
                username).send();
            new Event(Event.DEBUG, "[CIFSLoginModule] domain: " +
                domain).send();
            new Event(Event.DEBUG, "[CIFSLoginModule] hostname: " +
                dc.getHostName()).send();
        }

        // verify the username/password
        boolean usernameCorrect = false;
        boolean passwordCorrect = false;
        try {
            auth = new NtlmPasswordAuthentication(domain, username,
                new String(password));
            SmbSession.logon(dc, auth);
        }
        catch (SmbAuthException e) {
            // authentication failed -- clean out state
            if (debug)
                new Event(Event.DEBUG, "[CIFSLoginModule] authentication " +
                    "failed: " + e.toString()).send();
            succeeded = false;
            username = null;
            for (int i = 0; i < password.length; i++)
                password[i] = ' ';
            password = null;
            if (!usernameCorrect) {
                throw new FailedLoginException("User Name Incorrect");
            } else {
                throw new FailedLoginException("Password Incorrect");
            }
        }
        catch (SmbException e) {
            succeeded = false;
            username = null;
            for (int i = 0; i < password.length; i++)
                password[i] = ' ';
            password = null;
            throw new LoginException(e.toString());
        }
        // authentication succeeded!!!
        passwordCorrect = true;
        if (debug)
            new Event(Event.DEBUG, "[CIFSLoginModule] authentication " +
                "succeeded").send();
        succeeded = true;
        return true;
    }

    public boolean commit() throws LoginException {
        if (succeeded == false) {
            return false;
        } else {
            // assume the user we authenticated is the CIFSPrincipal
            userPrincipal = new NTUserPrincipal(username);
            if (!subject.getPrincipals().contains(userPrincipal))
                subject.getPrincipals().add(userPrincipal);

            if (debug) {
                System.out.println("\t[CIFSLoginModule] " +
                    "added NTUserPrincipal to Subject");
            }

            // in any case, clean out state
            username = null;
            for (int i = 0; i < password.length; i++)
                password[i] = ' ';
            password = null;

            commitSucceeded = true;
            return true;
        }
    }

    public boolean abort() throws LoginException {
        userPrincipal = null;
        if (succeeded == false) {
            return false;
        } else if (succeeded == true && commitSucceeded == false) {
            // login succeeded but overall authentication failed
            succeeded = false;
            username = null;
            if (password != null) {
                for (int i = 0; i < password.length; i++)
                    password[i] = ' ';
                password = null;
            }
        } else {
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
        if (password != null) {
            for (int i = 0; i < password.length; i++)
                password[i] = ' ';
            password = null;
        }
        userPrincipal = null;
        return true;
    }
}
