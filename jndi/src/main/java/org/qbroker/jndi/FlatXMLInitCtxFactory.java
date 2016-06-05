package org.qbroker.jndi;

/* FlatXMLInitCtxFactory - Initialize an XML Context with XML config files */

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/*
 * FlatXMLInitCtxFactory returns an instance of flat Context consisting of
 * XML configuration files in the directory specified by PROVIDER_URL.
 * Each XML file is a reference to an Object.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class FlatXMLInitCtxFactory implements InitialContextFactory {
    public Context getInitialContext(Hashtable env) throws NamingException {
        if (env == null || !(env.containsKey(Context.PROVIDER_URL)))
            throw new NamingException("Environment not well defined");
        return new FlatXMLCtx(env);
    }
}
