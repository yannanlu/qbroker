package org.qbroker.jndi;

/* WrapperCtx - Wrapper Context for a JNDI */

import javax.naming.*;
import java.util.Hashtable;

/* A Wrapper Context */
public abstract class WrapperCtx implements Context {
    protected Context ctx = null;

    public Object lookup(String name) throws NamingException {
        return ctx.lookup(name);
    }

    public Object lookup(Name name) throws NamingException {
        return ctx.lookup(name);
    }

    public void bind(String name, Object obj) throws NamingException {
        ctx.bind(name, obj);
    }

    public void bind(Name name, Object obj) throws NamingException {
        ctx.bind(name, obj);
    }

    public void rebind(String name, Object obj) throws NamingException {
        ctx.rebind(name, obj);
    }

    public void rebind(Name name, Object obj) throws NamingException {
        ctx.rebind(name, obj);
    }

    public void unbind(String name) throws NamingException {
        ctx.unbind(name);
    }

    public void unbind(Name name) throws NamingException {
        ctx.unbind(name);
    }

    public void rename(String oldname, String newname) throws NamingException {
        ctx.rename(oldname, newname);
    }

    public void rename(Name oldname, Name newname) throws NamingException {
        ctx.rename(oldname, newname);
    }

    public NamingEnumeration<NameClassPair> list(String name)
        throws NamingException {
        return ctx.list(name);
    }

    public NamingEnumeration<NameClassPair> list(Name name)
        throws NamingException {
        return ctx.list(name);
    }

    public NamingEnumeration<Binding> listBindings(String name)
        throws NamingException {
        return ctx.listBindings(name);
    }

    public NamingEnumeration<Binding> listBindings(Name name)
        throws NamingException {
        return ctx.listBindings(name);
    }

    public void destroySubcontext(String name) throws NamingException {
        ctx.destroySubcontext(name);
    }

    public void destroySubcontext(Name name) throws NamingException {
        ctx.destroySubcontext(name);
    }

    public Context createSubcontext(String name) throws NamingException {
        return ctx.createSubcontext(name);
    }

    public Context createSubcontext(Name name) throws NamingException {
        return ctx.createSubcontext(name);
    }

    public Object lookupLink(String name) throws NamingException {
        return ctx.lookupLink(name);
    }

    public Object lookupLink(Name name) throws NamingException {
        return ctx.lookupLink(name);
    }

    public NameParser getNameParser(String name) throws NamingException {
        return ctx.getNameParser(name);
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return ctx.getNameParser(name);
    }

    public String composeName(String name,String prefix) throws NamingException{
        return ctx.composeName(name, prefix);
    }

    public Name composeName(Name name, Name prefix) throws NamingException {
        return ctx.composeName(name, prefix);
    }

    public Object addToEnvironment(String propName, Object propVal)
        throws NamingException {
        return ctx.addToEnvironment(propName, propVal);
    }

    public Object removeFromEnvironment(String propName)
        throws NamingException {
        return ctx.removeFromEnvironment(propName);
    }

    public Hashtable getEnvironment() throws NamingException {
        return ctx.getEnvironment();
    }

    public String getNameInNamespace() throws NamingException {
        return ctx.getNameInNamespace();
    }

    public void close() throws NamingException {
        if (ctx != null)
            ctx.close();
        ctx = null;
    }
}
