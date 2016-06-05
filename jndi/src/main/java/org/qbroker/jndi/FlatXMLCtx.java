package org.qbroker.jndi;

/* FlatXMLCtx - flat Context based on XML configuration files */

import javax.naming.*;
import java.util.Map;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import org.qbroker.common.XML2Map;
import org.qbroker.jndi.FlatNameParser;
import org.qbroker.jndi.FlatNames;
import org.qbroker.jndi.FlatBindings;

/**
* A simple service provider that implements a flat XML namespace.
*/

public class FlatXMLCtx implements Context {
    Hashtable<String, Object> envTable = new Hashtable<String, Object>(5,0.75f);
    protected Hashtable<String,Object>bindings=new Hashtable<String,Object>(11);
    private NameParser parser;
    private String configDir;
    private File contextDir;
    private String saxParser = "org.apache.xerces.parsers.SAXParser";

    FlatXMLCtx(Hashtable environment) {
        if (environment == null)
            throw new IllegalArgumentException("environment is empty");

        Enumeration keys = environment.keys();
        while (keys.hasMoreElements()) {
            Object o = keys.nextElement();
            if (o != null && o instanceof String)
                envTable.put((String) o, environment.get((String) o));
        }
        parser = new FlatNameParser();
        configDir = (String) envTable.get(Context.PROVIDER_URL);
        if (configDir == null || ".".equals(configDir))
            configDir = ".";
        else if (configDir.startsWith("file://"))
            configDir = configDir.substring(7);
        contextDir = new File(configDir);

        if (contextDir.isDirectory() && contextDir.canRead()) {
            Object o;
            String[] list = contextDir.list();
            int l, n = list.length;
            String name, path;
            for (int i=0; i<n; i++) {
                l = list[i].lastIndexOf(".xml");
                if (l <= 0 || l + 4 != list[i].length())
                    continue;
                name = list[i].substring(0, l);
                path = configDir + "/" + name + ".xml";
                try {
                    o = load(name, path);
                }
                catch (Exception e) {
                    o = null;
                }
                if (o == null || !(o instanceof Map))
                    continue;
                if (((Map) o).containsKey("ClassName"))
                    bindings.put(name, o);
            }
        }
    }

    public Object lookup(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot lookup empty name");

        Object obj = bindings.get(name);
        if (obj != null && obj instanceof Map) {
            Map ph = (Map) obj;
            obj = init(name, ph);
            if (obj == null)
                throw new NameNotFoundException(name + " failed to init");
        }
        else {
            throw new NameNotFoundException(name + " not found");
        }
        return obj;
    }

    public Object lookup(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot lookup empty name");
        return lookup(name.get(0));
    }

    public void bind(String name, Object obj) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot bind empty name");
        throw new InvalidNameException("Readonly Context");
    }

    public void bind(Name name, Object obj) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot bind empty name");
        bind(name.get(0), obj);
    }

    public void rebind(String name, Object obj) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot rebind empty name");
        throw new InvalidNameException("Readonly Context");
    }

    public void rebind(Name name, Object obj) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot rebind empty name");
        rebind(name.get(0), obj);
    }

    public void unbind(String name) throws NamingException {
        if (name.length() <= 0)
            throw new InvalidNameException("Cannot unbind empty name");
        throw new InvalidNameException("Readonly Context");
    }

    public void unbind(Name name) throws NamingException {
        if (name.isEmpty())
            throw new InvalidNameException("Cannot unbind empty name");
        unbind(name.get(0));
    }

    public void rename(String oldname, String newname) throws NamingException {
        if (oldname.length() <= 0 || newname.length() <= 0)
            throw new InvalidNameException("Cannot rename empty name");
        throw new InvalidNameException("Readonly Context");
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
        if (target != null && target instanceof Context)
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
        if (target != null && target instanceof Context)
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
        // This flat XML context does not treat links specially
        return lookup(name);
    }

    public Object lookupLink(Name name) throws NamingException {
        // Flat XML namespace; no federation; just call string version
        return lookupLink(name.get(0));
    }

    public NameParser getNameParser(String name) throws NamingException {
        return parser;
    }

    public NameParser getNameParser(Name name) throws NamingException {
        // Flat XML namespace; no federation; just call string version
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

    /**
     * load the property Map for the name from the XML file at path
     */
    private Map load(String name, String path) throws NamingException {
        File file;
        Object o;

        file = new File(path);
        if (file.exists() && file.isFile() && file.canRead()) {
            try {
                FileInputStream propStream = new FileInputStream(file);
                XML2Map xh = new XML2Map(saxParser);
                Map h = xh.getMap(propStream);
                propStream.close();
                o = (Map) h.get(name);
            }
            catch (Exception e) {
                throw new NamingException(e.toString());
            }
        }
        else {
            o = null;
        }
        return (Map) o;
    }

    /**
     * instantiate the object for the name from the property Map
     */
    private Object init(String name, Map props) throws NamingException {
        Object o = null;
        String className;

        className = (String) props.get("ClassName");
        if (className == null || className.length() == 0) {
            return null;
        }

        try {
            java.lang.reflect.Constructor con;
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            o = con.newInstance(new Object[]{props});
        }
        catch (InvocationTargetException e) {
            Exception ex = (Exception) e.getTargetException();
            if (ex == null)
                ex = e;
            throw new NamingException(ex.toString());
        }
        catch (Exception e) {
            throw new NamingException(e.toString());
        }
        return o;
    }
}
