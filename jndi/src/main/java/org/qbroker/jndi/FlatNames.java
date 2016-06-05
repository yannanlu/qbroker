package org.qbroker.jndi;

/* FlatNames - NamingEnumeration of the flat context */

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NameClassPair;
import javax.naming.Binding;
import javax.naming.NamingException;
import java.util.Enumeration;
import java.util.Map;

public class FlatNames implements NamingEnumeration<NameClassPair> {
    Enumeration names;
    Context ctx;

    FlatNames (Context ctx, Enumeration names) {
        this.names = names;
        this.ctx = ctx;
    }

    public boolean hasMoreElements() {
        return names.hasMoreElements();
    }

    public boolean hasMore() throws NamingException {
        return hasMoreElements();
    }

    public NameClassPair nextElement() {
        Object o;
        String className = null, name = (String) names.nextElement();
        if (ctx instanceof FlatXMLCtx) {
            o = ((FlatXMLCtx) ctx).bindings.get(name);
            if (o != null && o instanceof Map)
                className = (String) ((Map) o).get("ClassName");
        }
        else try {
            o = ctx.lookup(name);
            className = o.getClass().getName();
        }
        catch (Exception e) {
        }
        if (className == null)
            return null;
        else
            return new NameClassPair(name, className);
    }

    public NameClassPair next() throws NamingException {
        Object o = nextElement();

        if (o == null)
            throw new NamingException("Failed to get classname");

        return (NameClassPair) o;
    }

    public void close() {
        ctx = null;
        names = null;
    }
}
