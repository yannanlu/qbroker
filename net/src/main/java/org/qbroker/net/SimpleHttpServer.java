package org.qbroker.net;
  
/* SimpleHttpServer.java - a wrapper of HttpServer for embedded HTTP servers */

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.lang.reflect.InvocationTargetException;
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
import org.qbroker.net.HTTPServer;
import org.qbroker.common.Base64Encoder;
import org.qbroker.common.Service;
import org.qbroker.common.Utils;
import org.qbroker.common.TraceStackThread;

public class SimpleHttpServer implements HttpHandler, HTTPServer {
    private String name = null;
    private String uri = null;
    private String username = null;
    private String password = null;
    private int port = 80;
    private int backlog = 32;
    private int maxThreads = 0;
    private int timeout = 60000;
    private HttpServer server = null;
    private ExecutorService es = null;
    private BasicAuthenticator auth = null;
    private int bufferSize = 8192;
    private boolean isHTTPS = false;
    private boolean trustAll = false;

    /** Creates new SimpleHttpServer */
    public SimpleHttpServer(Map props) {
        Object o;
        URI u;
        String ksPath = null, ksPassword = null;

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
                backlog = 32;
        }

        if ((o = props.get("MaxNumberThread")) != null) {
            maxThreads = Integer.parseInt((String) o);
            if (maxThreads < 0)
                maxThreads = 0;
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

        if (isHTTPS) {
            if ((o = props.get("KeyStoreFile")) != null)
                ksPath = (String) o;
            else
                throw(new IllegalArgumentException(name +
                    " KeyStoreFile is not defined"));

            if ((o = props.get("KeyStorePassword")) != null)
                ksPassword = (String) o;
            else if ((o = props.get("EncryptedKeyStorePassword")) != null) try {
                ksPassword = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name + " failed to decrypt "+
                    "EncryptedKeyStorePassword: " + e.toString()));
            }
            if (ksPassword == null || ksPassword.length() <= 0)
                throw(new IllegalArgumentException(name +
                    " KeySTorePassword is not defined"));
            File ks = new File(ksPath);
            if (!ks.exists() && !ks.canRead())
                throw(new IllegalArgumentException(name +
                    " can not open KeyStoreFile to read: " + ksPath));
        }

        if (!isHTTPS) {
            try {
                server = HttpServer.create(new InetSocketAddress(port),backlog);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +
                    " failed to create http server: " + e.toString()));
            }
        }
        else try {
            SSLContext sc = getSSLContext(ksPath, ksPassword);
            HttpsConfigurator cfg = new HttpsConfigurator(sc);
            server = HttpsServer.create(new InetSocketAddress(port), backlog);
            ((HttpsServer) server).setHttpsConfigurator(cfg);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +
                " failed to create https server: " + e.toString()));
        }

        if (maxThreads > 0) {
            es = Executors.newFixedThreadPool(maxThreads);
            server.setExecutor(es);
        }
        else
            server.setExecutor(null);

        if (username != null && password != null) { // default Authenticator
            auth = new BasicAuthenticator(name) {
                @Override
                public boolean checkCredentials(String user, String passwd) {
                    return username.equals(user) && password.equals(passwd);
                }
            };
        }
    }

    /** returns SSLContext with given path to keyStore, the password for it */
    public static SSLContext getSSLContext(String ksPath, String ksPassword)
        throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(new FileInputStream(ksPath), ksPassword.toCharArray());
        KeyManagerFactory kmf =
         KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, ksPassword.toCharArray());
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(kmf.getKeyManagers(), new TrustManager[]{new TrustAllManager()},
            null);

        return sc;
    }

    /** sends the http status code and the text back as the response */
    public static void sendResponse(HttpExchange he, int rc, String text)
        throws IOException {
        Headers headers = he.getResponseHeaders();
        headers.set("Content-Type", "application/json");
        if (rc != 200) {
            text = "{\n  \"success\":false,\n  \"errors\":{\"title\":\"" +
                he.getRequestMethod()+"\"},\n  \"errormsg\":\""+text+"\"\n}\n";
            he.sendResponseHeaders(rc, text.getBytes().length);
            OutputStream os = he.getResponseBody();
            os.write(text.getBytes());
            os.close();
        }
        else if (text != null) {
            he.sendResponseHeaders(rc, text.getBytes().length);
            OutputStream os = he.getResponseBody();
            os.write(text.getBytes());
            os.close();
        }
        else {
            he.sendResponseHeaders(rc, 0);
            OutputStream os = he.getResponseBody();
            os.close();
        }
    }

    /** parses the GET parameters and returns the map loaded with parameters */
    public static Map<String,List<String>> parseGetParameters(HttpExchange he) {
        URI u = he.getRequestURI();
        String query = u.getRawQuery();
        return parseQuery(query, new HashMap<String, List<String>>());
    }

    /** parses the POST parameters and returns the map loaded with parameters */
    public static int parsePostParameters(HttpExchange he,
        Map<String,List<String>> params) throws IOException {
        int n = params.size();
        InputStream in = he.getRequestBody();
        String query = Utils.read(in, new byte[8192]);
        in.close();
        return parseQuery(query, params).size() - n;
    }

    /** parses the query string and returns the map loaded with parameters */
    private static Map<String, List<String>> parseQuery(String query,
        Map<String, List<String>> params) {
        if (query != null) try {
            for (String pair : query.split("[&]")) {
                final String param[] = pair.split("[=]");
                String paramName = "";
                String paramValue = "";
                if (param.length > 0) {
                    paramName = URLDecoder.decode(param[0], "UTF-8");
                }

                if (param.length > 1) {
                    paramValue = URLDecoder.decode(param[1], "UTF-8");
                }

                if (params.containsKey(paramName)) {
                    final List<String> values = params.get(paramName);
                    values.add(paramValue);
                }
                else {
                    final List<String> values = new ArrayList<String>();
                    values.add(paramValue);
                    params.put(paramName, values);
                }
            }
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return params;
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

    /**
     * It creates the handler with the given property map and sets the service
     * on the handler. Then it adds the given handler at the given path to
     * the context
     */
    public void setService(String path, Map props, Service service) {
        HttpHandler handler = null;
        java.lang.reflect.Method method = null;
        String className, str = null;
        Object o;

        if (props == null || props.size() <= 0)
            throw(new IllegalArgumentException(name + ": props is empty"));
        else if (service == null)
            throw(new IllegalArgumentException(name + ": service is null"));
        else if ((o = props.get("ClassName")) == null)
            throw(new IllegalArgumentException(name + ": ClassName is null"));
        else try { // instantiate the handler
            className = (String) o;
            java.lang.reflect.Constructor con;
            str = name + " failed to instantiate servlet of " + className + ":";
            Class<?> cls = Class.forName(className);
            con = cls.getConstructor(new Class[]{Map.class});
            o = con.newInstance(new Object[]{props});
            if (o instanceof HttpHandler) { // handler has instantiated
                handler = (HttpHandler) o;
                addContext(path, handler);
                method = cls.getMethod("setService",new Class[]{Service.class});
            }
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            str += e.toString() + " with Target exception: " + "\n";
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(ex)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(e)));
        }

        if (handler == null)
            throw(new IllegalArgumentException(str + " not an Httphandler"));
        else if (method == null)
            throw(new IllegalArgumentException(str+" no method of setService"));
        else try { // set the service on the handler
            str = name + " failed to set service of " + service.getName() + ":";
            method.invoke(handler, new Object[]{service});
        }
        catch (InvocationTargetException e) {
            Throwable ex = e.getTargetException();
            str += e.toString() + " with Target exception: " + "\n";
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(ex)));
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(str +
                TraceStackThread.traceStack(e)));
        }
    }

    /** adds the given servlet at the given path to the context */
    public void addContext(Object obj, String path) {
        if (path == null || path.length() <= 0)
            throw(new IllegalArgumentException(name + ": path is empty"));
        else if (obj == null || !(obj instanceof HttpHandler))
            throw(new IllegalArgumentException(name + ": not a handler"));
        else
            addContext(path, (HttpHandler) obj);
    }

    /** adds a context on the given path and returns number of contexts added */
    private int addContext(String path, HttpHandler handler) {
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

    public String getName() {
        return name;
    }

    /** starts the server without blocking */
    public void start() {
        if (server != null)
            server.start();
    }

    /** stops the server with delays */
    public void stop() {
        if (server != null)
            server.stop(1);
        if (es != null)
            es.shutdown();
    }

    public void join() {
    }

    protected void finalize() {
        if (server != null) {
            server.stop(1);
            server = null;
        }
        if (es != null) {
            es.shutdown();
            es = null;
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
              case 'f':
                if (i+1 < args.length)
                    props.put("KeyStoreFile", args[++i]);
                break;
              case 'k':
                if (i+1 < args.length)
                    props.put("KeyStorePassword", args[++i]);
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
            server.addContext(server, path);
            server.start();
            System.out.println("Server started. Please run the following command to test:");
            System.out.println("curl " + uri + path);
            System.out.println("Enter Ctrl+C to stop the server");
            server.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("SimpleHttpServer Version 1.0 (written by Yannan Lu)");
        System.out.println("SimpleHttpServer: a simple http server");
        System.out.println("Usage: java org.qbroker.net.SimpleHttpServer -p port -c context");
        System.out.println("  -?: print this message");
        System.out.println("  -s: https mode");
        System.out.println("  -c: context (default: '/')");
        System.out.println("  -p: port (default: 80 or 443)");
        System.out.println("  -n: username");
        System.out.println("  -w: password");
        System.out.println("  -f: path to keystore file");
        System.out.println("  -k: password for keystore");
    }
}
