package org.qbroker.net;
  
/* SimpleHttpServer.java - a wrapper of com.sun.net.httpserver.HttpServer */

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLHandshakeException;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.BasicAuthenticator;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import org.qbroker.common.Base64Encoder;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;

public class SimpleHttpServer implements HttpHandler {
    private String name = null;
    private String uri = null;
    private String username = null;
    private String password = null;
    private int port = 80;
    private int backlog = 64;
    private int timeout = 60000;
    private HttpServer server = null;
    private BasicAuthenticator auth = null;
    private int bufferSize = 8192;
    private boolean isHTTPS = false;
    private boolean trustAll = false;

    /** Creates new SimpleHttpServer */
    public SimpleHttpServer(Map props) {
        Object o;
        URI u;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));
        name = (String) o;
        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException(name + ": URI is not defined"));
        try {
            u = new URI((String) o);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name + " failed to parse URI: "+
                e.toString()));
        }

        if ("http".equals(u.getScheme()))
            isHTTPS = false;
        else if ("https".equals(u.getScheme())) {
            isHTTPS = true;
            if ((o = props.get("TrustAllCertificates")) != null &&
                "true".equalsIgnoreCase((String) o))
                trustAll = true;
        }
        else
            throw(new IllegalArgumentException(name + " unsupported scheme: " +
                u.getScheme()));

        port = u.getPort();
        if (port <= 0)
            port = (isHTTPS) ? 443 : 80;

        if ((o = props.get("Backlog")) != null) {
            backlog = Integer.parseInt((String) o);
            if (backlog < 0)
                backlog = 64;
        }

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            if ((o = props.get("Password")) != null) {
                password = (String) o;
            }
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + " failed to decrypt "+
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null || password.length() <= 0)
                throw(new IllegalArgumentException(name +
                    ": Password not defined for " + username));
        }

        if (!isHTTPS) {
            try {
                server = HttpServer.create(new InetSocketAddress(port),backlog);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to create the http server: " + e.toString()));
            }
        }
        else try {
            HttpsConfigurator cfg;
//            SSLContext sc = SSLContext.getDefault();
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[]{new TrustAllManager()}, null);
            server = HttpsServer.create(new InetSocketAddress(port), backlog);
            cfg = new HttpsConfigurator(sc);
            ((HttpsServer) server).setHttpsConfigurator(cfg);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to create the https server: " + e.toString()));
        }

        if (username != null && password != null) { // default Authenticator
            auth = new BasicAuthenticator(name) {
                @Override
                public boolean checkCredentials(String user, String passwd) {
                    return username.equals(user) && password.equals(passwd);
                }
            };
        }
    }

    /** returns SSLContext with given keyStore, trustStore and the password */
    private static SSLContext getSSLContext(byte[] tsSample1, byte[] ksSample1,
        String passwd) throws Exception {
        byte[] sampleTruststore1 = Base64Encoder.decode(tsSample1);
        byte[] sampleKeystore1 = Base64Encoder.decode(ksSample1);
        File keystore = File.createTempFile("AcceptAllGetTest", ".keystore");
        File truststore = File.createTempFile("AcceptAllGetTest",".truststore");
        FileOutputStream keystoreFileOut = new FileOutputStream(keystore);
        try { // write bytes of keystore
            keystoreFileOut.write(sampleKeystore1);
        } finally {
            keystoreFileOut.close();
        }
        FileOutputStream truststoreFileOut = new FileOutputStream(truststore);
        try { // write bytes of truststore
            truststoreFileOut.write(sampleTruststore1);
        } finally {
            truststoreFileOut.close();
        }
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(new FileInputStream(keystore), passwd.toCharArray());
        KeyManagerFactory kmf =
         KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, passwd.toCharArray());
        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(new FileInputStream(truststore), passwd.toCharArray());
        TrustManagerFactory tmf =
     TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return sc;
    }

    /** default handler listing request details in json */
    public void handle(HttpExchange he) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        Headers headers = he.getRequestHeaders();
        HttpPrincipal principal = he.getPrincipal();
        URI u = he.getRequestURI();
        String query = u.getQuery();
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("  \"Method\": \"" + he.getRequestMethod() + "\",\n");
        strBuf.append("  \"Path\": \"" + u.getPath() + "\",\n");
        if (principal != null)
            strBuf.append("  \"Principal\": \""+principal.toString()+"\",\n");
        if (query != null)
            strBuf.append("  \"Query\": \"" + query + "\",\n");
        if (!headers.isEmpty()) {
            int i = 0;
            strBuf.append("  \"Headers\": {" + "\n");
            for (String key : headers.keySet()) {
                if (i++ > 0)
                    strBuf.append(",\n");
                strBuf.append("    \""+key+"\": \""+headers.getFirst(key)+"\"");
            }
            strBuf.append("\n  }\n");
        }
        String response = "{\n" + strBuf.toString() + "}\n";
        headers = he.getResponseHeaders();
        headers.set("Content-type", "application/json");
        he.sendResponseHeaders(200, response.getBytes().length);
        OutputStream os = he.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    public String getName() {
        return name;
    }

    /** adds a context on the given path and returns number of contexts */
    public int addContext(String path, HttpHandler handler) {
        if (path == null || path.length() <= 0 || handler == null)
            return -1;
        if (server != null) {
            HttpContext ctx = server.createContext(path, handler);
            if (auth != null)
                ctx.setAuthenticator(auth);
            return 1;
        }
        else
            return -2;
    }

    /** removes the context at given path and returns number of contexts left */
    public int removeContext(String path) {
        if (path == null || path.length() <= 0)
            return -1;
        if (server != null) {
            server.removeContext(path);
            return 0;
        }
        else
            return -2;
    }

    /** starts the server without blocking */
    public void start() {
        if (server != null) {
            server.start();
        }
    }

    /** stops the server with delays */
    public void stop() {
        if (server != null) {
            server.stop(1);
        }
    }

    protected void finalize() {
        if (server != null) {
            server.stop(1);
            server = null;
        }
    }

    public static void main(String[] args) {
        Map<String, String> props = new HashMap<String, String>();
        SimpleHttpServer server = null;
        String path = "/", uri;
        int i, port = 0;
        boolean isHTTPS = false;

        props.put("Name", "testServer");
        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                continue;
            }
            switch (args[i].charAt(1)) {
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 's':
                isHTTPS = true;
                break;
              case 'c':
                if (i+1 < args.length)
                    path = args[++i];
                break;
              case 'p':
                if (i+1 < args.length)
                    port = Integer.parseInt(args[++i]);
                break;
              case 'n':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'w':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              default:
                break;
            }
        }
        if (!path.startsWith("/"))
            path = "/" + path;
        if (port <= 0)
            port = (isHTTPS) ? 443 : 80;
        if (isHTTPS)
            uri = "https://localhost:" + port;
        else
            uri = "http://localhost:" + port;
        props.put("URI", uri);

        try {
            server = new SimpleHttpServer(props);
            server.addContext(path, server);
            server.start();
            System.out.println("Server started. Please run the following command to test:");
            System.out.println("curl " + uri + path);
            System.out.println("Enter Ctrl+C to stop the server");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("SimpleHttpServer Version 1.0 (written by Yannan Lu)");
        System.out.println("SimpleHttpServer: a simple http server");
        System.out.println("Usage: java org.qbroker.net.SimpleHttpServer -p port-c context");
        System.out.println("  -?: print this message");
        System.out.println("  -s: https mode");
        System.out.println("  -c: context (default: '/')");
        System.out.println("  -p: port (default: 80 or 443)");
        System.out.println("  -n: username");
        System.out.println("  -w: password");
    }
}
