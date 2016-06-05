package org.qbroker.jndi;

/* FlatBindings - Name bindings of a flat Context */

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.Binding;
import javax.naming.NamingException;
import java.util.Enumeration;

public class FlatBindings implements NamingEnumeration<Binding> {
    Context ctx;
    Enumeration names;

    FlatBindings(Context ctx, Enumeration names) {
        this.ctx = ctx;
        this.names = names;
    }

    public boolean hasMoreElements() {
        return names.hasMoreElements();
    }

    public boolean hasMore() throws NamingException {
        return hasMoreElements();
    }

    public Binding nextElement() {
        Object o;
        String name = (String) names.nextElement();
        try {
            o = ctx.lookup(name);
        }
        catch (Exception e) {
            o = null;
        }
        if (o == null)
            return null;
        else
            return new Binding(name, o);
    }

    public Binding next() throws NamingException {
        Object o = nextElement();
        if (o == null)
            throw new NamingException("Failed to lookup");
        return (Binding) o;
    }

    public void close() {
        ctx = null;
        names = null;
    }
}
