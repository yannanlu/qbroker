package org.qbroker.jndi;

/* QpidWrapperCtx - Wrapper Context for Qpid JNDI */

import javax.naming.*;
import java.util.Hashtable;

/**
 * QpidWrapperCtx initializes the context via
 * org.apache.qpid.jndi.PropertiesFileInitialContextFactory. The URL should
 * be the real one to the Qpid Messaging Service. The connection factory name
 * has to be defined in the query string in terms of "cf=xxx". Please make
 * sure this query string is at the end of the real URL.
 */
public class QpidWrapperCtx extends WrapperCtx {

    QpidWrapperCtx(Hashtable environment) throws NamingException {
        Hashtable<String, String> env = new Hashtable<String, String>();
        String str, url;
        int i;
        url = (String) environment.get(Context.PROVIDER_URL);
        i = url.lastIndexOf("&cf=");
        if (i < 0)
            throw(new NamingException("empty query string for CF: " + url));
        env.put(Context.INITIAL_CONTEXT_FACTORY,
            "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        str = url.substring(i + 4);
        url = url.substring(0, i);
        if ((i = str.indexOf("&")) > 0)
            str = str.substring(0, i);
        env.put("connectionfactory." + str, url);
        ctx = new InitialContext(env);
    }
}
