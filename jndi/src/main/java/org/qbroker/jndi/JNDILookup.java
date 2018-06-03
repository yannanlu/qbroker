package org.qbroker.jndi;

/* JNDILookup - A utility to look up JNDI names */

import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.HashMap;
import javax.naming.NamingEnumeration;
import javax.naming.NameClassPair;
import javax.naming.Context;
import javax.naming.InitialContext;

public class JNDILookup {
    public static void main(String args[]) {
        Hashtable<String, Object> env = new Hashtable<String, Object>();
        NamingEnumeration en;
        NameClassPair np;
        Context ctx = null;
        Object o;
        String cf = null;
        String url = null;
        String key, pkg= null, name = null;
        String username = null, password = null;
        int i, n, bufferSize = 1024;

        if (args.length <= 1) {
            printUsage();
            System.exit(0);
        }

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'u':
                if (i+1 < args.length)
                    url = args[++i];
                break;
              case 'c':
                if (i+1 < args.length)
                    cf = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    pkg = args[++i];
                break;
              case 'n':
                if (i+1 < args.length)
                    name = args[++i];
                break;
              case 'l':
                if (i+1 < args.length)
                    username = args[++i];
                break;
              case 'w':
                if (i+1 < args.length)
                    password = args[++i];
                break;
              default:
            }
        }

        try {
            if (cf != null) {
                env.put(Context.INITIAL_CONTEXT_FACTORY, cf);
                if (url != null)
                    env.put(Context.PROVIDER_URL, url);
                if (username != null) {
                    env.put(Context.SECURITY_PRINCIPAL, username);
                    env.put(Context.SECURITY_CREDENTIALS, password);
                }
                if (pkg != null)
                    env.put(Context.URL_PKG_PREFIXES, pkg);
                if ("com.sun.enterprise.naming.SerialInitContextFactory".equals(cf)) {
//                   env.put("java.naming.factory.state",
//                "com.sun.corba.ee.impl.presentation.rmi.JNDIStateFactoryImpl");
//                   env.put("org.omg.CORBA.ORBInitialHost", "localhost");
//                   env.put("org.omg.CORBA.ORBInitialPost", "3700");
                }
                ctx = new InitialContext(env);
                if (name == null)
//                    en = ctx.listBindings("");
                    en = ctx.list("");
                else
                    en = ctx.list(name);
                while (en.hasMore()) {
                    o = en.next();
                    if (o instanceof NameClassPair) {
                        np = (NameClassPair) o;
                        System.out.println(np.getName() + ": " +
                            np.getClassName());
                    }
                    else
                        System.out.println(o.getClass().getName());
                }
            }
            else {
                //System.out.println("lookup: " + name);
                ctx = new InitialContext();
                en = ctx.listBindings("");
                while (en.hasMore()) {
                    o = en.next();
                    if (o instanceof NameClassPair) {
                        np = (NameClassPair) o;
                        System.out.println(np.getName() + ": " +
                            np.getClassName());
                    }
                    else
                        System.out.println(o.getClass().getName());
                }
                //ctx.lookup(name);
            }
            ctx.close();
        }
        catch (Exception e) {
            if (ctx != null) try {
                ctx.close();
            }
            catch (Exception ex) {
            }
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("JNDILookup Version 1.0 (written by Yannan Lu)");
        System.out.println("JNDILookup: lookup a JNDI name from the give url");
        System.out.println("Usage: java JNDILookup -u url -c contextFatory");
        System.out.println("  -?: print this message");
        System.out.println("  -u: service url");
        System.out.println("  -c: contextFactory");
        System.out.println("  -n: lookup name");
        System.out.println("  -l: princial");
        System.out.println("  -w: credentials");
        System.out.println("  -p: package prefix");
    }
}
