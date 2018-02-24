package org.qbroker.net;

/* NNTPConnector.java - a NNTP connector for news groups */

import org.apache.commons.net.nntp.NNTP;
import org.apache.commons.net.nntp.NNTPClient;
import org.apache.commons.net.nntp.NNTPReply;
import org.apache.commons.net.nntp.NewGroupsOrNewsQuery;
import org.apache.commons.net.nntp.NewsgroupInfo;
import org.apache.commons.net.nntp.Article;
import org.apache.commons.net.nntp.SimpleNNTPHeader;
import org.apache.commons.net.nntp.NNTPConnectionClosedException;
import org.apache.commons.net.MalformedServerReplyException;
import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import org.qbroker.common.Utils;
import org.qbroker.common.Connector;
import org.qbroker.common.TraceStackThread;

/**
 * NNTPConnector connects to a NNTP server and provides the following methods
 * for news group operations: nntpGet(), nntpPost(), nntpListArticles(),
 * and nntpListGroups().
 *<br/><br/>
 * This is NOT MT-Safe.  Therefore, you need to use multiple instances to
 * achieve your MT-Safty goal.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class NNTPConnector implements Connector {
    protected String hostname;
    protected String uri;
    protected String username = null;
    protected String password = null;
    protected int port = 119;
    protected int timeout = 60000;
    protected int maxArticle = 1024;
    protected boolean isConnected = false;

    private NNTPClient nntpClient = null;
    private String poster = null;
    private int bufferSize = 4096;
    private SimpleDateFormat dateFormat;
    private boolean supportNEWNEWS = true;
    public final static int ARTICLE_ALL = 0;
    public final static int ARTICLE_HEADER = 1;
    public final static int ARTICLE_BODY = 2;
    private final static int[] inStatus = {220, 221, 222};

    /** Creates new NNTPConnector */
    public NNTPConnector(Map props) throws IOException {
        Object o;
        URI u;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        try {
            u = new URI(uri);
        }
        catch (URISyntaxException e) {
            throw(new IllegalArgumentException(e.toString()));
        }

        if (!"nntp".equals(u.getScheme()))
            throw(new IllegalArgumentException("unsupported scheme: " +
                u.getScheme()));

        if ((port = u.getPort()) <= 0)
            port = 119;

        if ((hostname = u.getHost()) == null || hostname.length() == 0)
            throw(new IllegalArgumentException("no host specified in URI"));

        if ((o = props.get("Username")) != null) {
            username = (String) o;

            if ((o = props.get("Password")) != null)
                password = (String) o;
            else if ((o = props.get("EncryptedPassword")) != null) try {
                password = Utils.decrypt((String) o);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException("failed to decrypt " +
                    "EncryptedPassword: " + e.toString()));
            }
            if (password == null)
                throw(new IllegalArgumentException("Password is not defined"));
        }

        if ((o = props.get("MaxNumberArticle")) != null)
            maxArticle = Integer.parseInt((String) o);

        if ((o = props.get("SOTimeout")) != null)
            timeout = 1000 * Integer.parseInt((String) o);
        else
            timeout = 60000;

        if (props.get("BufferSize") != null) {
            bufferSize = Integer.parseInt((String) props.get("BufferSize"));
        }

        if ((o = props.get("Poster")) != null)
            poster = (String) o;

        if (poster == null || poster.length() <= 0) {
            String host;
            try {
                host =
                    java.net.InetAddress.getLocalHost().getCanonicalHostName();
            }
            catch (java.net.UnknownHostException e) {
                host = "localhost";
            }
            if (host != null && host.length() > 0)
                poster = System.getProperty("user.name") + "@" + host;
            else
                poster = System.getProperty("user.name") + "@localhost";
        }
        dateFormat = new SimpleDateFormat("EE, d MMM yy H:mm:ss zz");

        if ((o = props.get("ConnectOnInit")) == null || // check ConnectOnInit
            !"false".equalsIgnoreCase((String) o))
            connect();
    }

    protected void connect() throws IOException {
        int replyCode;
        nntpClient = new NNTPClient();
        nntpClient.setDefaultTimeout(timeout + timeout);
        nntpClient.connect(hostname, port);

        replyCode = nntpClient.getReplyCode();
        if (! NNTPReply.isPositiveCompletion (replyCode))
            throw new IOException ("unable to connect to " + hostname + ": " +
                nntpClient.getReplyString());

        nntpClient.setSoTimeout(timeout);

        if (username != null && ! nntpClient.authenticate(username, password))
            throw new IOException ("login failed for " + username + '@' +
                hostname + ": " + nntpClient.getReplyString());

        isConnected = true;
    }

    public String getURI() {
        return uri;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /** It reconnects and returns null or error message upon failure */
    public String reconnect() {
        close();
        try {
            connect();
        }
        catch (IOException e) {
            return e.toString();
        }
        return null;
    }

    public void close() {
        if (!isConnected)
            return;
        if (nntpClient != null) try {
            nntpClient.logout();
            if (nntpClient.isConnected())
                nntpClient.disconnect();
        }
        catch (Exception e) {
        }
        nntpClient = null;
        isConnected = false;
    }

    public boolean isAllowedToPost() {
        if (nntpClient != null)
            return nntpClient.isAllowedToPost();
        else
            return false;
    }

    /**
     * It retrieves the entire article of the specific article id from the
     * current newsgroup.  The content is stored in the provided StringBuffer.
     * It returns null upon succes or the error message otherwise.
     */
    public String nntpGet(StringBuffer strBuf, String aid) throws IOException {
        return nntpGet(strBuf, aid, 0);
    }

    /**
     * It retrieves the content of the article of the specific article id
     * from the current newsgroup.  The content is stored in the provided
     * StringBuffer.  It returns null upon succes or the error message
     * otherwise.
     */
    public String nntpGet(StringBuffer strBuf, String aid, int contentType)
        throws IOException {
        Reader in = null;
        int replyCode;
        int charsRead = 0;
        char[] buffer = new char[bufferSize];

        if (strBuf == null)
            return "strBuf is null";

        strBuf.setLength(0);
        switch (contentType) {
          case ARTICLE_HEADER:
            in = nntpClient.retrieveArticleHeader(aid);
            break;
          case ARTICLE_BODY:
            in = nntpClient.retrieveArticleBody(aid);
            break;
          default:
            contentType = ARTICLE_ALL;
            in = nntpClient.retrieveArticle(aid);
            break;
        }
        if (in == null) {
            return "failed to get reader: " + nntpClient.getReplyString();
        }
        replyCode = nntpClient.getReplyCode();
        if (replyCode != inStatus[contentType]) {
            try {
                in.close();
            }
            catch (Exception e) {
            }
            return "reader is not ready: " + nntpClient.getReplyString();
        }

        while ((charsRead = in.read(buffer, 0, bufferSize)) >= 0) {
            if (charsRead == 0) {
                try {
                    Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                }
                continue;
            }

            strBuf.append(new String(buffer, 0, charsRead));
        }

        try {
            in.close();
        }
        catch (Exception e) {
        }

        return null;
    }

    /**
     * It posts the article with the header and the body to the current
     * newsgroup.  It returns null upon succes or the error message
     * otherwise.
     */
    public String nntpPost(String header, String body) throws IOException {
        Writer out = null;
        int replyCode;

        if (body == null)
            return "body is null";

        out = nntpClient.postArticle();
        if (out == null) {
            return "failed to get writer: " + nntpClient.getReplyString();
        }
        replyCode = nntpClient.getReplyCode();
        if (replyCode != 240) {
            try {
                out.close();
            }
            catch (Exception e) {
            }
            return "writer is not ready: " + nntpClient.getReplyString();
        }

        if (header != null)
            out.write(header);
        out.write(body);

        try {
            out.close();
        }
        catch (Exception e) {
        }

        // Must call completePendingCommand() to finish command.
        if (! nntpClient.completePendingCommand()) {
            return "post failed: " + nntpClient.getReplyString();
        }
        return null;
    }

    /**
     * It posts the article with the subject and the story to the current
     * newsgroup.  It returns null upon succes or the error message
     * otherwise.
     */
    public String nntpPost(String subject, String story, String groupName)
        throws IOException {
        SimpleNNTPHeader header = null;

        header = new SimpleNNTPHeader(poster, subject);
        header.addNewsgroup(groupName);
        header.addHeaderField("Organiztion", "QBroker.ORG");

        return nntpPost(header.toString(), story);
    }

    /**
     * It returns a list of news groups on the NNTP server.  If distName is
     * null, it returns all available news groups.  Otherwise, distName can
     * be a pattern as the wildmat or the name of a specific news group.
     * If timestamp is positive, it will return all new groups added since
     * the timestamp.
     */
    public NewsgroupInfo[] nntpListGroups(String distName, long tm)
        throws IOException {
        if (distName == null || distName.length() <= 0) { // list all newsgroups
            return nntpClient.listNewsgroups();
        }
        else if (tm <= 0) { // list newgroups match the pattern
            return nntpClient.listNewsgroups(distName);
        }
        else {
            Calendar cal = Calendar.getInstance(TimeZone.getDefault());
            cal.setTime(new Date(tm));
            NewGroupsOrNewsQuery query = new NewGroupsOrNewsQuery(cal, false);
            query.addDistribution(distName);
            return nntpClient.listNewNewsgroups(query);
        }
    }

    /**
     * It returns an array of article IDs for requested articles in the
     * specified news group.  If the timestamp is positive, the returned list
     * only contains the articles added since the give timestamp.  In case of
     * failure, it returns null.
     */
    public String[] nntpListArticles(String groupName, long tm)
        throws IOException {
        String[] list = null;
        if (groupName == null || groupName.length() <= 0)
            return null;
        if (supportNEWNEWS) {
            Calendar cal = Calendar.getInstance(TimeZone.getDefault());
            cal.setTime(new Date(tm));
            NewGroupsOrNewsQuery query = new NewGroupsOrNewsQuery(cal, false);
            query.addNewsgroup(groupName);
            list = nntpClient.listNewNews(query);
            if (list == null) {
                int replyCode = nntpClient.getReplyCode();
                if (replyCode == NNTPReply.PERMISSION_DENIED) {
                    supportNEWNEWS = false;
                    return nntpListArticles(groupName, tm);
                }
            }
        }
        else { // no support for NEWNEWS
            int i, n;
            Article[] aInfo = getArticleInfo(groupName, 0, tm);
            if (aInfo != null && aInfo.length > 0) {
                n = aInfo.length;
                list = new String[n];
                for (i=0; i<n; i++)
                    list[i] = aInfo[i].getArticleId();
            }
            else if (aInfo == null)
                list = null;
            else
                list = new String[0];
        }
        return list;
    }

    /**
     * It returns an array of article info for the given range of articles
     * in the current newsgroup.
     */
    protected Article[] getArticleInfo(int lowNumber, int highNumber)
        throws IOException {
        Reader in = null;
        Article[] list = null;
        int replyCode;
        int charsRead = 0;
        char[] buffer;
        StringBuffer strBuf;

        if (lowNumber <= 0 || lowNumber > highNumber)
            return null;

        in = nntpClient.retrieveArticleInfo(lowNumber, highNumber);
        if (in == null)
            return null;
        replyCode = nntpClient.getReplyCode();
        if (replyCode != 224) {
            try {
                in.close();
            }
            catch (Exception e) {
            }
            return null;
        }

        strBuf = new StringBuffer();
        buffer = new char[bufferSize];
        while ((charsRead = in.read(buffer, 0, bufferSize)) >= 0) {
            if (charsRead == 0) {
                try {
                    Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                }
                continue;
            }

            strBuf.append(new String(buffer, 0, charsRead));
        }

        try {
            in.close();
        }
        catch (Exception e) {
        }

        StringTokenizer st = new StringTokenizer(strBuf.toString(), "\n");
        int n = st.countTokens();
        list = new Article[n];
        for (int i=0; i<n; i++) {
            String line = st.nextToken();
            StringTokenizer stt = new StringTokenizer(line, "\t");
            Article article = new Article();
            article.setArticleNumber(Integer.parseInt(stt.nextToken()));
            article.setSubject(stt.nextToken());
            article.setFrom(stt.nextToken());
            article.setDate(stt.nextToken());
            article.setArticleId(stt.nextToken());
            article.addHeaderField("References", stt.nextToken());
            list[i] = article;
        }

        return list;
    }

    /**
     * It returns the article info for the given article number in the
     * current newsgroup or null for failure.
     */
    protected Article getArticleInfo(int anumber) throws IOException {
         Article[] a = getArticleInfo(anumber, anumber);
         if (a != null && a.length > 0)
             return a[0];
         else
             return null;
    }

    /**
     * It returns an array of article info for new articles in the specified
     * newsgroup since the give timestamp.  If the timestamp is positive,
     * the returned list only contains the new articles since the given
     * timestamp.  In case of failure, it returns null.
     */
    public Article[] getArticleInfo(String gname, int anumber, long tm)
        throws IOException {
        NewsgroupInfo[] groups;
        Article[] articles = new Article[0];
        Date date;
        int n, lowNumber, highNumber;
        groups = nntpListGroups(gname, 0);
        if (groups == null || groups.length <= 0) {
            throw(new IOException("failed to get group info for " + gname +
                ": " + nntpClient.getReplyString()));
        }

        if ((n = groups[0].getArticleCount()) <= 0) { // no new articles
            return articles;
        }

        lowNumber = groups[0].getFirstArticle();
        highNumber = groups[0].getLastArticle();
        try {
            if (nntpClient.selectNewsgroup(gname)) {
                Article a;
                long t;
                if (tm > 0 && anumber == 0) {
                    a = locateArticle(lowNumber, highNumber, tm);
                    if (a == null) { // try it once more
                        try {
                            Thread.sleep(2000);
                        }
                        catch (Exception ex) {
                        }
                        a = locateArticle(lowNumber, highNumber, tm);
                        if (a == null)
                            return null;
                    }
                    anumber = a.getArticleNumber();
                    t = getDate(a).getTime();
                    if (t >= tm)
                        lowNumber = anumber;
                    else // no new articles
                        return articles;
                }
                else if (anumber > 0 && tm > 0 && lowNumber < anumber) {
                    a = getArticleInfo(anumber);
                    if (a != null && (date = getDate(a)) != null &&
                        tm == date.getTime())
                        lowNumber = anumber + 1;
                    else {
                        a = locateArticle(lowNumber, highNumber, tm);
                        if (a == null)
                            return null;
                        anumber = a.getArticleNumber();
                        t = getDate(a).getTime();
                        if (t >= tm)
                            lowNumber = anumber;
                        else // now new articles
                            return articles;
                    }
                }
                if (highNumber - lowNumber > maxArticle - 1)
                    highNumber = lowNumber + maxArticle - 1;
                Article[] aInfo = getArticleInfo(lowNumber, highNumber);
                if (aInfo != null && aInfo.length > 0) {
                    n = aInfo.length;
                    if (tm <= 0) {
                        articles = aInfo;
                    }
                    else {
                        int i, k;
                        for (i=0; i<n; i++) {
                            date = getDate(aInfo[i]);
                            if (date != null && date.getTime() >= tm)
                                break;
                        }
                        k = i;
                        articles = new Article[n-k];
                        for (i=k; i<n; i++)
                            articles[i] = aInfo[i];
                    }
                }
                else if (aInfo == null)
                    throw(new IOException(nntpClient.getReplyString()));
            }
            else
                throw(new IOException(nntpClient.getReplyString()));
        }
        catch (Exception e) {
            throw(new IOException(e.toString() + ": " +
                nntpClient.getReplyString()));
        }
        return articles;
    }

    /**
     * Within the given range, it looks for the first article with the
     * timestamp later than a given timestamp in the current newsgroup.
     * It returns the new article upon success.  If there is no new
     * article available, the last article will be returned.  In case
     * of failure, it just returns null.
     */
    private Article locateArticle(int lowNumber, int highNumber, long tm) {
        Article article = null, lowArticle = null, highArticle = null;
        Article[] articles = null;
        Date date = null;
        long t;
        int anumber, begin, end, i, n, delta;
        if (lowNumber <= 0 || lowNumber > highNumber)
            return null;
        begin = lowNumber;
        end = highNumber;
        try {
            anumber = begin + 9;
            if (anumber > end)
                anumber = end;
            // search for the first valid begin
            articles = getArticleInfo(begin, anumber);
            if (articles == null)
                return null;
            for (i=0; i<articles.length; i++) {
                date = getDate(articles[i]);
                if (date != null) {
                    anumber = articles[i].getArticleNumber();
                    t = date.getTime();
                    if (t >= tm) { // located
                        end = anumber;
                        highArticle = articles[i];
                        break;
                    }
                    else {
                        begin = anumber;
                        lowArticle = articles[i];
                    }
                }
            }
            if (highArticle != null)
                return highArticle;
            else if (lowArticle == null) // first 10 articles are not found
                return null;
            else if (end - lowNumber <= 9)
                return highArticle;
            else { // search for the last valid end
                anumber = end - 9;
                if (anumber <= begin)
                    anumber = begin + 1;
                articles = getArticleInfo(anumber, end);
                if (articles == null)
                    return null;
                for (i=0; i<articles.length; i++) {
                    date = getDate(articles[i]);
                    if (date != null) {
                        anumber = articles[i].getArticleNumber();
                        t = date.getTime();
                        if (t >= tm) { // located
                            end = anumber;
                            highArticle = articles[i];
                            break;
                        }
                        else {
                            begin = anumber;
                            lowArticle = articles[i];
                        }
                    }
                }
            }

            if (highNumber - lowNumber <= 19) {
                if (highArticle != null)
                    return highArticle;
                else
                    return lowArticle;
            }

            anumber = begin + (end - begin) / 2;
            delta = 2;
            while (end - begin > 9) { // bisection search
                if (anumber + delta >= end)
                    delta = end - anumber - 1;
                articles = getArticleInfo(anumber, anumber + delta);
                if (articles == null)
                    return null;
                anumber = 0;
                for (i=0; i<articles.length; i++) {
                    date = getDate(articles[i]);
                    if (date != null) {
                        anumber = articles[i].getArticleNumber();
                        t = date.getTime();
                        if (t >= tm) { // too far
                            end = anumber;
                            highArticle = articles[i];
                            break;
                        }
                        else { // not there yet
                            begin = anumber;
                            lowArticle = articles[i];
                        }
                    }
                }
                if (anumber > 0) {
                    anumber = begin + (end - begin) / 2;
                    delta = 2;
                }
                else {
                    delta *= 2;
                }
            }

            if (end - begin > 1) { // the gap has been narrowed down
                articles = getArticleInfo(begin+1, end);
                if (articles == null)
                    return lowArticle;
                for (i=0; i<articles.length; i++) {
                    date = getDate(articles[i]);
                    if (date != null) {
                        anumber = articles[i].getArticleNumber();
                        t = date.getTime();
                        if (t >= tm) { // located finally
                            end = anumber;
                            highArticle = articles[i];
                            break;
                        }
                        else {
                            begin = anumber;
                            lowArticle = articles[i];
                        }
                    }
                }
            }

            if (highArticle != null)
                return highArticle;
            else
                return lowArticle;
        }
        catch (Exception e) {
        }
        return null;
    }

    /**
     * It parses the date field of an article and returns Date object or
     * null upon failure.
     */ 
    public Date getDate(Article article) {
        String line;
        Date date;
        if (article == null)
            return null;
        line = article.getDate();
        if (line == null || line.length() <= 0)
            return null;
        date = dateFormat.parse(line, new ParsePosition(0));
        return date;
    }

    public String getReplyString() {
        return nntpClient.getReplyString();
    }

    /**
     * It sets the current newsgroup and returns null upon success
     * or the error text otherwise.
     */
    public String setNewsgroup(String groupName) {
        try {
            if (nntpClient.selectNewsgroup(groupName))
                return null;
            else
                return nntpClient.getReplyString();
        }
        catch (Exception e) {
            return nntpClient.getReplyString() + ": " + e.toString();
        }
    }

    public NNTPClient getNNTPClient() {
        return nntpClient;
    }

    public static void main(String args[]) {
        String group = "*";
        NewsgroupInfo[] gInfo;
        Article[] aInfo;
        Map<String, Object> props = new HashMap<String, Object>();
        NNTPConnector conn = null;
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss");
        Date dd;
        String msg, reply;
        long tm = 0;
        int anumber = 0, mask = 2;
        int i, k, n = 0, size = 0;

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }

        for (i=0; i<args.length; i++) {
            if (args[i].charAt(0) != '-' || args[i].length() != 2) {
                if (i == args.length-1)
                    msg = args[i];
                continue;
            }

            switch (args[i].charAt(1)) {
              case 'u':
                if (i+1 < args.length)
                    props.put("URI", args[++i]);
                break;
              case '?':
                printUsage();
                System.exit(0);
                break;
              case 'g':
                if (i+1 < args.length)
                    group = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    tm = dateFormat.parse(args[++i],
                        new ParsePosition(0)).getTime();
                break;
              case 'U':
                if (i+1 < args.length)
                    props.put("Username", args[++i]);
                break;
              case 'P':
                if (i+1 < args.length)
                    props.put("Password", args[++i]);
                break;
              case 'n':
                if (i+1 < args.length)
                    anumber = Integer.parseInt(args[++i]);
                break;
              case 's':
                if (i+1 < args.length)
                    props.put("MaxNumberArticle", args[++i]);
                break;
              case 'm':
                if (i+1 < args.length)
                    mask = Integer.parseInt(args[++i]);
                if (mask < 0 || mask > 2)
                    mask = 0;
                break;
              default:
            }
        }

        try {
            conn = new NNTPConnector(props);
            gInfo = conn.nntpListGroups(group, 0);
            if (gInfo != null && gInfo.length > 0) {
                n = gInfo.length;
                for (i=0; i<n; i++) {
                    System.out.println(i + ": " + gInfo[i].getNewsgroup() +
                        " " + gInfo[i].getFirstArticle() +
                        " " + gInfo[i].getLastArticle() +
                        " " + gInfo[i].getArticleCount());
                }
            }
            if (n == 1) { // list articles in a group
                aInfo = conn.getArticleInfo(group, anumber, tm);
                n = aInfo.length;
                k = -1;
                for (i=0; i<n; i++) {
                    if (anumber == aInfo[i].getArticleNumber())
                        k = i;
                    if (anumber > 0 && i != k) // skip output
                        continue;
                    dd = conn.getDate(aInfo[i]);
                    System.out.println(i + ": " + aInfo[i].getArticleNumber() +
                        " " + ((dd != null) ? dateFormat.format(dd) :
                        aInfo[i].getDate()) + " " + aInfo[i].getSubject());
                }
                if (k >= 0) { // query a specific article of a group
                    StringBuffer strBuf = new StringBuffer();
                    reply = conn.nntpGet(strBuf, aInfo[k].getArticleId(), mask);
                    System.out.println(aInfo[k].getArticleNumber()+ ": " +
                        strBuf.toString());
                }
            }
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (conn != null) try {
                conn.close();
            }
            catch (Exception ex) {
            }
        }
    }

    private static void printUsage() {
        System.out.println("NNTPConnector Version 1.0 (written by Yannan Lu)");
        System.out.println("NNTPConnector: a connector for posting or retrievving news articles"); 
        System.out.println("Usage: java org.qbroker.net.NNTPConnector -u uri -t tmStr -n anumber -g group [message]");
        System.out.println("  -?: print this message");
        System.out.println("  -u: uri of the NNTP service");
        System.out.println("  -g: name of the news group");
        System.out.println("  -U: username for connections");
        System.out.println("  -P: password");
        System.out.println("  -t: string of timestamp in format: yyyy/MM/dd.HH:mm:ss");
        System.out.println("  -n: article number for querying the content");
        System.out.println("  -s: max number of articles to be retrieved (0: no limit)");
        System.out.println("  -m: mask for querying the content (1: header, 2: body, 0: all)");
        System.out.println("  msg: empty for retrieve)");
    }
}
