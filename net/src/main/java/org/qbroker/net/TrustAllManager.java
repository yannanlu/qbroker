package org.qbroker.net;

/* TrustAllManager.java - an X509TrustManager that trusts all certificates */

import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import javax.net.ssl.X509TrustManager;

/**
 * TrustAllManager implements X509TrustManager and trusts all certificates
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class TrustAllManager implements X509TrustManager {
    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }

    public void checkServerTrusted(X509Certificate[] certs, String authType)
        throws CertificateException {
        return;
    }

    public void checkClientTrusted(X509Certificate[] certs, String authType)
        throws CertificateException {
        return;
    }
}
