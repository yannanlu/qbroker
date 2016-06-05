package org.qbroker.jndi;

/* WrapperInitCtxFactory - Initialize a Wrapper Context */

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/*
 * WrapperInitCtxFactory returns an instance of WrapperContext for certain
 * JNDI Context.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class WrapperInitCtxFactory implements InitialContextFactory {
    public Context getInitialContext(Hashtable env) throws NamingException {
        Object o;
        String str;
        int i;
        if (env == null || (o = env.get(Context.PROVIDER_URL)) == null)
            throw new NamingException("Environment not well defined");
        str = (String) o;
        i = str.indexOf(":");
        if (i <= 0)
            throw new NamingException("PROVIDER_URL is not well defined");
        str = str.substring(0, i);
        if ("imq".equals(str))
            return new ImqWrapperCtx(env);
        else if ("amqp".equals(str))
            return new QpidWrapperCtx(env);
        else if ("cached".equals(str))
            return new CachedCtx(env);
        else
            throw new NamingException("PROVIDER_URL is not supported");
    }
}
