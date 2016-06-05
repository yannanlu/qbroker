package org.qbroker.jndi;

/* CachedCtx - A Context that caches any objects */

import javax.naming.*;
import java.util.Hashtable;
import java.util.Enumeration;
import org.qbroker.jndi.FlatNameParser;
import org.qbroker.jndi.FlatNames;
import org.qbroker.jndi.FlatBindings;

/**
* A simple service provider that caches any bound objects.
*/
public class CachedCtx implements Context {
    Hashtable<String, Object> envTable = new Hashtable<String, Object>(5,0.75f);
    protected Hashtable<String,Object>bindings=new Hashtable<String,Object>(11);
    private NameParser parser;

    CachedCtx(Hashtable environment) {
        if (environment == null)
            throw new IllegalArgumentException("environment is empty");

        Enumeration keys = environment.keys();
        while (keys.hasMoreElements()) {
            Object o = keys.nextElement();
            if (o != null && o instanceof String)
                envTable.put((String) o, environment.get((String) o));
        }
        parser = new FlatNameParser();
    }

    public Object lookup(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot lookup empty name");

        Object obj = bindings.get(name);
        if (obj == null)
            throw new NameNotFoundException(name + " not found");
        return obj;
    }

    public Object lookup(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot lookup empty name");
        return lookup(name.toString());
    }

    public void bind(String name, Object obj) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot bind empty name");

        if (bindings.containsKey(name))
            throw new NameAlreadyBoundException("Use rebind to override");
        else
            bindings.put(name, obj);
    }

    public void bind(Name name, Object obj) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot bind empty name");
        bind(name.get(0), obj);
    }

    public void rebind(String name, Object obj) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot rebind empty name");
        bindings.remove(name);
        bindings.put(name, obj);
    }

    public void rebind(Name name, Object obj) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot rebind empty name");
        rebind(name.get(0), obj);
    }

    public void unbind(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot unbind empty name");
        bindings.remove(name);
    }

    public void unbind(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot unbind empty name");
        unbind(name.get(0));
    }

    public void rename(String oldname, String newname) throws NamingException {
        if (oldname.length() <= 0 || newname.length() <= 0)
            throw new InvalidNameException("Cannot rename empty name");
        if (!bindings.containsKey(oldname))
            throw new NameNotFoundException(oldname + " not bound");
        Object obj = bindings.remove(oldname);
        bindings.put(newname, obj);
    }

    public void rename(Name oldname, Name newname) throws NamingException {
        if (oldname.isEmpty() || newname.isEmpty())
            throw new InvalidNameException("Cannot rename empty name");
        rename(oldname.get(0), newname.get(0));
    }

    public NamingEnumeration<NameClassPair> list(String name)
        throws NamingException {
        if (name.equals(""))
            return new FlatNames(this, bindings.keys());
        // Perhaps name is a context
        Object target = lookup(name);
        if (target instanceof Context)
            return ((Context) target).list("");
        throw new NotContextException(name + " is not a Context");
    }

    public NamingEnumeration<NameClassPair> list(Name name)
        throws NamingException {
        if (name.isEmpty())
            return list("");
        else
            return list(name.get(0));
    }

    public NamingEnumeration<Binding> listBindings(String name)
        throws NamingException {
        if (name.equals(""))
            return new FlatBindings(this, bindings.keys());
        // Perhaps name is a context
        Object target = lookup(name);
        if (target instanceof Context)
            return ((Context) target).listBindings("");
        throw new NotContextException(name + " is not a Context");
    }

    public NamingEnumeration<Binding> listBindings(Name name)
        throws NamingException {
        if (name.isEmpty())
            return listBindings("");
        else
            return listBindings(name.get(0));
    }

    public void destroySubcontext(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot destroy on empty name");
        throw new OperationNotSupportedException("No support on subcontexts");
    }

    public void destroySubcontext(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot destroy on empty name");
        destroySubcontext(name.get(0));
    }

    public Context createSubcontext(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot create on empty name");
        throw new OperationNotSupportedException("No support on subcontexts");
    }

    public Context createSubcontext(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot create on empty name");
        return createSubcontext(name.get(0));
    }

    public Object lookupLink(String name) throws NamingException {
        return lookup(name);
    }

    public Object lookupLink(Name name) throws NamingException {
        return lookupLink(name.get(0));
    }

    public NameParser getNameParser(String name) throws NamingException {
        return parser;
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return getNameParser(name.get(0));
    }

    public String composeName(String name, String prefix)
        throws NamingException {
        Name result = composeName(new CompositeName(name),
            new CompositeName(prefix));
        return result.toString();
    }

    public Name composeName(Name name, Name prefix) throws NamingException {
        Name result = (Name)(prefix.clone());
        result.addAll(name);
        return result;
    }

    public Object addToEnvironment(String propName, Object propVal)
        throws NamingException {
        return envTable.put(propName, propVal);
    }

    public Object removeFromEnvironment(String propName)
        throws NamingException {
        return envTable.remove(propName);
    }

    public Hashtable getEnvironment() throws NamingException {
        return (Hashtable) envTable.clone();
    }

    public String getNameInNamespace() throws NamingException {
        return "";
    }

    public void close() throws NamingException {
        envTable.clear();
        envTable = null;
        bindings.clear();
        bindings = null;
    }
}
