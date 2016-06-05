package org.qbroker.jndi;

/* ImqWrapperCtx - Wrapper Context for OpenMQ JMS objects */

import javax.naming.*;
import java.util.Hashtable;
import javax.jms.JMSException;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import com.sun.messaging.ConnectionConfiguration;

/**
 * ImqWrapperCtx extends WrapperCtx and tries to init the context of
 * CachedCtx with the real URL and the connection factory name. The connection
 * factory name has to be defined in the query string as either "qcf=xxx" or
 * "tcf=xxx" so that the object will be able to bind to the name.
 */
public class ImqWrapperCtx extends WrapperCtx {

    ImqWrapperCtx(Hashtable environment) throws NamingException {
        Hashtable<String, String> env = new Hashtable<String, String>();
        String str, url;
        int i;
        url = (String) environment.get(Context.PROVIDER_URL);
        env.put(Context.INITIAL_CONTEXT_FACTORY,
            "org.qbroker.jndi.WrapperInitCtxFactory");
        env.put(Context.PROVIDER_URL, "cached://");
        ctx = new InitialContext(env);

        if ((i = url.lastIndexOf("?qcf=")) > 6 ||
            (i = url.lastIndexOf("&qcf=")) > 6) { // for qcf
            QueueConnectionFactory qcf;
            str = url.substring(i + 5);
            url = url.substring(6, i);
            if ((i = str.indexOf("&")) > 0)
                str = str.substring(0, i);
            if ((i = url.indexOf("/")) > 0)
                url = url.substring(0, i);
            if (url.indexOf(":") < 0)
                url += ":7676";
            try {
                qcf = initializeQCF(url);
            }
            catch (Exception e) {
                throw(new NamingException("failed to init QCF: "+e.toString()));
            }
            ctx.rebind(str, qcf);
        }
        else if ((i = url.lastIndexOf("?tcf=")) > 6 ||
            (i = url.lastIndexOf("&tcf=")) > 6) { // for tcf
            TopicConnectionFactory tcf;
            str = url.substring(i + 5);
            url = url.substring(6, i);
            if ((i = str.indexOf("&")) > 0)
                str = str.substring(0, i);
            if ((i = url.indexOf("/")) > 0)
                url = url.substring(0, i);
            if (url.indexOf(":") < 0)
                url += ":7676";
            try {
                tcf = initializeTCF(url);
            }
            catch (Exception e) {
                throw(new NamingException("failed to init TCF: "+e.toString()));
            }
            ctx.rebind(str, tcf);
        }
        else
            throw(new NamingException("empty query string for CF: " + url));
    }

    private QueueConnectionFactory initializeQCF(String url)
        throws JMSException {
        // get an IMQ-specific factory and set its props
        com.sun.messaging.QueueConnectionFactory mqFactory =
            new com.sun.messaging.QueueConnectionFactory();
        mqFactory.setProperty(ConnectionConfiguration.imqAddressList, url);

        // return a casted generic JMS factory for connect
        return (QueueConnectionFactory) mqFactory;
    }

    private TopicConnectionFactory initializeTCF(String url)
        throws JMSException {
        // get an IMQ-specific factory and set its props
        com.sun.messaging.TopicConnectionFactory mqFactory =
            new com.sun.messaging.TopicConnectionFactory();
        mqFactory.setProperty(ConnectionConfiguration.imqAddressList, url);

        // return a casted generic JMS factory for connect
        return (TopicConnectionFactory) mqFactory;
    }
}
