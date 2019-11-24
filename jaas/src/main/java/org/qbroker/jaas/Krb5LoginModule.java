package org.qbroker.jaas;

/* Krb5LoginModule.java - a LoginModule using Kerberos V5 for authentications */

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * Krb5LoginModule extends com.sun.security.auth.module.Krb5LoginModule
 * with support of more options for realm and kdc.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class Krb5LoginModule extends
    com.sun.security.auth.module.Krb5LoginModule {

    public void initialize(Subject subject, CallbackHandler callbackHandler,
        Map<String, ?> sharedState, Map<String, ?> options) {
        Object o;
        Map<String, Object> props;
        String key, kdc = null, realm = null;
        if (options == null || options.size() <= 0) {
            super.initialize(subject, callbackHandler, sharedState, options); 
            return;
        }
        props = new HashMap<String, Object>();
        Iterator iter = options.keySet().iterator();
        while (iter.hasNext()) {
            o = iter.next();
            if (o == null || !(o instanceof String))
                continue;
            key = (String) o;
            if ("kdc".equals(key.toLowerCase())) {
                kdc = (String) options.get(key);
                continue;
            }
            else if ("realm".equals(key.toLowerCase())) {
                realm = (String) options.get(key);
                continue;
            }
            props.put(key, options.get(key));
        }
        if (kdc == null || realm == null || kdc.length() <= 0 ||
            realm.length() <=0) {
            super.initialize(subject, callbackHandler, sharedState, options); 
        }
        else { // try to set realm and kdc for krb5
            System.setProperty("java.security.krb5.realm", realm);
            System.setProperty("java.security.krb5.kdc", kdc);
            super.initialize(subject, callbackHandler, sharedState, props); 
        }
    }
}
