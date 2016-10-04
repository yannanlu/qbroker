package org.qbroker.net;

/* ScriptedBrowser.java - a headless browser for testing purpose */

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.openqa.selenium.support.ui.ExpectedConditions;
import com.gargoylesoftware.htmlunit.BrowserVersion;
//import com.gargoylesoftware.htmlunit.WebClient;

public class ScriptedBrowser {
    protected String uri;
    private String username = null;
    private String password = null;
    private HtmlUnitDriver driver = null;
    private boolean withJavascriptEnabled = true;

    public final static int BY_ID = 0;
    public final static int BY_NAME = 1;
    public final static int BY_TAGNAME = 2;
    public final static int BY_LINKTEXT = 3;
    public final static int BY_PARTIALLINKTEXT = 4;
    public final static int BY_CLASSNAME = 5;
    public final static int BY_CSSSELECTOR = 6;
    public final static int BY_XPATH = 7;

    public final static int GET = 0;
    public final static int PAUSE = 1;
    public final static int GETTITLE = 2;
    public final static int GETSOURCE = 3;
    public final static int FIND_SEND = 4;
    public final static int FIND_CLICK = 5;
    public final static int FIND2_CLICK = 6;
    public final static int WAIT_CLICK = 7;
    public final static int WAIT2_CLICK = 8;
    public final static int WAIT_SEND = 9;

    /** create a new ScriptedBrowser */
    public ScriptedBrowser(Map props) {
        Object o;
        String str;

        if ((o = props.get("URI")) == null)
            throw(new IllegalArgumentException("URI is not defined"));
        uri = (String) o;

        if ((o = props.get("Username")) != null) {
            username = (String) o;
            password = (String) props.get("Password");
        }

        if ((o = props.get("WithJavascriptEnabled")) != null &&
            "false".equalsIgnoreCase((String) o))
            withJavascriptEnabled = false;

        str = (String) props.get("BrowserVersion");
        if (str == null)
            driver = new HtmlUnitDriver();
        else if ("fireforx_38".equalsIgnoreCase(str))
            driver = new HtmlUnitDriver(BrowserVersion.FIREFOX_38);
        else if ("chrome".equalsIgnoreCase(str))
            driver = new HtmlUnitDriver(BrowserVersion.CHROME);
        else if ("internet_explorer".equalsIgnoreCase(str))
            driver = new HtmlUnitDriver(BrowserVersion.INTERNET_EXPLORER);
        else
            driver = new HtmlUnitDriver();

        if (withJavascriptEnabled)
            driver.setJavascriptEnabled(true);

        if ((o = props.get("PageLoadTimeout")) != null) {
            long tm = Long.parseLong((String) o);
            if (tm > 0)
                driver.manage().timeouts().pageLoadTimeout(tm,
                    TimeUnit.MILLISECONDS);
        }

        if ((o = props.get("ScriptTimeout")) != null) {
            long tm = Long.parseLong((String) o);
            if (tm > 0)
                driver.manage().timeouts().setScriptTimeout(tm,
                    TimeUnit.MILLISECONDS);
        }

        if ((o = props.get("ProxyHost")) != null) {
            int port = Integer.parseInt((String) props.get("ProxyPort"));
            driver.setProxy((String) o, port);
        }
    }

    public String findAndClick(int type, String text, long tm) {
        WebElement element = find(type, text);
        if (element != null) {
            String str = element.getText();
            if (str == null || str.length() <= 0)
                str = element.getTagName();
            if (tm > 0) try {
                Thread.sleep(tm);
            }
            catch (Exception e) {
            }
            element.click();
            return str;
        }
        else
            return null;
    }

    public String find2AndClick(int type, String text, int type2,
        String text2, long tm) {
        WebElement element = find(type, text);
        if (element == null)
            return null;
        else {
            WebElement wl = element.findElement(locate(type2, text2));
            if (wl != null) {
                String str = element.getText();
                if (str == null || str.length() <= 0)
                    str = element.getTagName();
                if (tm > 0) try {
                    Thread.sleep(tm);
                }
                catch (Exception e) {
                }
                wl.click();
                return str;
            }
            else
                return null;
        }
    }

    public String waitAndClick(int type, String text, int sec, long tm) {
        WebElement element = waitAndFind(type, text, sec);
        if (element != null) {
            String str = element.getText();
            if (str == null || str.length() <= 0)
                str = element.getTagName();
            if (tm > 0) try {
                Thread.sleep(tm);
            }
            catch (Exception e) {
            }
            element.click();
            return str;
        }
        else
            return null;
    }

    public String waitAndFindClick(int type, String text, int sec,
        int type2, String text2, long tm) {
        WebElement element = waitAndFindClickable(type, text, sec);
        if (element == null)
            return null;
        else {
            WebElement wl = element.findElement(locate(type2, text2));
            if (wl != null) {
                String str = element.getText();
                if (str == null || str.length() <= 0)
                    str = element.getTagName();
                if (tm > 0) try {
                    Thread.sleep(tm);
                }
                catch (Exception e) {
                }
                wl.click();
                return str;
            }
            else
                return null;
        }
    }

    public String findAndSend(int type, String text, String key) {
        WebElement element = find(type, text);
        if (element != null) {
            String str = element.getText();
            if (str == null || str.length() <= 0)
                str = element.getTagName();
            element.sendKeys(key);
            return str;
        }
        else
            return null;
    }

    public String waitAndSend(int type, String text, int sec, String key) {
        WebElement element = waitAndFind(type, text, sec);
        if (element != null) {
            String str = element.getText();
            if (str == null || str.length() <= 0)
                str = element.getTagName();
            element.sendKeys(key);
            return str;
        }
        else
            return null;
    }

    private WebElement find(int type, String text) {
        By by = locate(type, text);
        if (by != null)
            return driver.findElement(by); 
        else
            throw(new IllegalArgumentException("illegal locator type " + type +
                " for " + text));
    }

    private static By locate(int type, String text) {
        By by = null;
        switch (type) {
          case BY_ID:
            by = By.id(text);
            break;
          case BY_NAME:
            by = By.name(text);
            break;
          case BY_TAGNAME:
            by = By.tagName(text);
            break;
          case BY_LINKTEXT:
            by = By.linkText(text);
            break;
          case BY_PARTIALLINKTEXT:
            by = By.partialLinkText(text);
            break;
          case BY_CLASSNAME:
            by = By.className(text);
            break;
          case BY_CSSSELECTOR:
            by = By.cssSelector(text);
            break;
          case BY_XPATH:
            by = By.xpath(text);
            break;
          default:
        }
        return by;
    }

    private WebElement waitAndFind(int type, String text, int sec) {
        By by = locate(type, text);
        if (by == null)
            throw(new IllegalArgumentException("illegal locator type " + type +
                " for " + text));
        return (new WebDriverWait(driver, sec)).until(
            ExpectedConditions.presenceOfElementLocated(by));
    }

    private WebElement waitAndFindClickable(int type, String text, int sec) {
        By by = locate(type, text);
        if (by == null)
            throw(new IllegalArgumentException("illegal locator type " + type +
                " for " + text));
        return (new WebDriverWait(driver, sec)).until(
            ExpectedConditions.elementToBeClickable(by));
    }

    private WebElement waitAndFindVisible(int type, String text, int sec) {
        By by = locate(type, text);
        if (by == null)
            throw(new IllegalArgumentException("illegal locator type " + type +
                " for " + text));
        return (new WebDriverWait(driver, sec)).until(
            ExpectedConditions.visibilityOfElementLocated(by));
    }

    public String get(String url) {
        if (driver != null) {
            driver.get(url);
            return driver.getCurrentUrl();
        }
        else
            return null;
    }

    public String getTitle() {
        if (driver != null)
            return driver.getTitle();
        else
            return null;
    }

    public String getPageSource() {
        if (driver != null)
            return driver.getPageSource();
        else
            return null;
    }

    public void close() {
        if (driver != null) try {
            driver.quit();
        }
        catch (Exception e) {
        }
        driver = null;
    }

    public static int parseOperation(String str) {
        if (str == null)
            return -1;
        else if ("get".equalsIgnoreCase(str))
            return GET;
        else if ("pause".equalsIgnoreCase(str))
            return PAUSE;
        else if ("gettitle".equalsIgnoreCase(str))
            return GETTITLE;
        else if ("getpagesource".equalsIgnoreCase(str))
            return GETSOURCE;
        else if ("findandclick".equalsIgnoreCase(str))
            return FIND_CLICK;
        else if ("find2andclick".equalsIgnoreCase(str))
            return FIND2_CLICK;
        else if ("waitandclick".equalsIgnoreCase(str))
            return WAIT_CLICK;
        else if ("waitandfindclick".equalsIgnoreCase(str))
            return WAIT2_CLICK;
        else if ("findandsend".equalsIgnoreCase(str))
            return FIND_SEND;
        else if ("waitandsend".equalsIgnoreCase(str))
            return WAIT_SEND;
        else
            return -1;
    }

    public static int parseType(String str) {
        if (str == null)
            return -1;
        else if ("By.id".equals(str))
            return BY_ID;
        else if ("By.name".equals(str))
            return BY_NAME;
        else if ("By.tagName".equals(str))
            return BY_TAGNAME;
        else if ("By.linkText".equals(str))
            return BY_LINKTEXT;
        else if ("By.partialLinkText".equals(str))
            return BY_PARTIALLINKTEXT;
        else if ("By.className".equals(str))
            return BY_CLASSNAME;
        else if ("By.cssSelector".equals(str))
            return BY_CSSSELECTOR;
        else if ("By.xpath".equals(str))
            return BY_XPATH;
        else
            return -1;
    }

    /** tests ScripotedBrowser operations */
    public static void main(String args[]) {
        HashMap<String, Object> props;
        ScriptedBrowser browser = null;
        Object o;
        String uri = null, path = null, str = null, bVersion = null;
        long tm;
        int i, k, n, debug = 0;
        int[][] taskInfo = null;
        String[][] taskData = null;
        List<String> list = new ArrayList<String>();
        int TASK_ID = 0;
        int TASK_TYPE = 1;
        int TASK_PAUSE = 2;
        int TASK_WAIT = 3;
        int TASK_EXTRA = 4;

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }
        props = new HashMap<String, Object>();
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
                    uri = args[++i];
                break;
              case 't':
                if (i+1 < args.length)
                    list.add(args[++i]);
                break;
              case 'b':
                if (i+1 < args.length)
                    bVersion = args[++i];
                break;
              case 'd':
                if (i+1 < args.length)
                    debug = Integer.parseInt(args[++i]);
                break;
              default:
            }
        }

        if (uri == null) {
            printUsage();
            System.exit(0);
        }
        else
            props.put("URI", uri);
        props.put("WithJavascriptEnabled", "true");
        if (bVersion != null)
            props.put("BrowserVersion", bVersion);

        k = list.size();
        taskInfo = new int[k][];
        taskData = new String[k][];
        for (i=0; i<k; i++) {
            String attr = list.get(i);
            taskInfo[i] = new int[]{-1, -1, -1, -1, -1};
            taskData[i] = new String[]{null, null, null, null, null};
            if ((n = attr.indexOf(':')) > 0) { // for multiple attributes
                List<String> pl = new ArrayList<String>();
                int j = 0;
                do {
                    pl.add(attr.substring(j, n));
                    j = n + 1;
                } while ((n = attr.indexOf(':', j)) >= j);
                pl.add(attr.substring(j));
                str = pl.get(TASK_ID);
                taskInfo[i][TASK_ID] = ScriptedBrowser.parseOperation(str);
                str = pl.get(TASK_TYPE);
                taskInfo[i][TASK_TYPE] = ScriptedBrowser.parseType(str);
                str = pl.get(TASK_EXTRA+2);
                taskInfo[i][TASK_EXTRA] = ScriptedBrowser.parseType(str);
                str = pl.get(TASK_WAIT);
                taskInfo[i][TASK_PAUSE] =
                    (str.length() > 0) ? Integer.parseInt(str) : -1;
                str = pl.get(TASK_EXTRA);
                taskInfo[i][TASK_WAIT] =
                    (str.length() > 0) ? Integer.parseInt(str) : -1;
                taskData[i][TASK_ID] = pl.get(TASK_EXTRA+1);
                taskData[i][TASK_TYPE] = pl.get(TASK_PAUSE);
                taskData[i][TASK_EXTRA] = pl.get(TASK_EXTRA+3);
                if (debug == 3)
                    System.out.println(i + ": " + taskInfo[i][0] + " " +
                        taskInfo[i][1] + " " + taskInfo[i][2] + " " +
                        taskInfo[i][3] + " " + taskInfo[i][4] + "\n\t" +
                        taskData[i][0]+" "+taskData[i][1]+" "+taskData[i][4]);
            }
        }
        if (debug == 3)
            System.exit(0);

        LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log",
            "org.apache.commons.logging.impl.NoOpLog");
        Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);
        Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.OFF);
        try {
            browser = new ScriptedBrowser(props);
            tm = System.currentTimeMillis();
            str = browser.get(uri);
        }
        catch (Exception e) {
            e.printStackTrace();
            if (browser != null)
                browser.close();
            System.exit(0);
        }

        for (i=0; i<taskInfo.length; i++) {
            k = 0;
            if (debug > 0)
                System.out.println(i + ": " + taskInfo[i][0] + " " +
                    taskInfo[i][1] + " " + taskInfo[i][2] + " " +
                    taskInfo[i][3] + " " + taskInfo[i][4] + " " +str+"\n\t" +
                    taskData[i][0] + " " + taskData[i][1] +" "+ taskData[i][4]);
            str = null;
            try {
                switch (taskInfo[i][TASK_ID]) {
                  case GET:
                    str = browser.get(taskData[i][TASK_ID]);
                    break;
                  case PAUSE:
                    if (taskInfo[i][TASK_PAUSE] > 0) try {
                        Thread.sleep((long) taskInfo[i][TASK_PAUSE]);
                    }
                    catch (Exception e) {
                    }
                    break;
                  case GETTITLE:
                    str = browser.getTitle();
                    break;
                  case GETSOURCE:
                    str = browser.getPageSource();
                    break;
                  case FIND_CLICK:
                    str = browser.findAndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case FIND2_CLICK:
                    str = browser.find2AndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_EXTRA],
                        taskData[i][TASK_EXTRA], (long)taskInfo[i][TASK_PAUSE]);
                    break;
                  case WAIT_CLICK:
                    str = browser.waitAndClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case WAIT2_CLICK:
                    str = browser.waitAndFindClick(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        taskInfo[i][TASK_EXTRA], taskData[i][TASK_EXTRA],
                        (long) taskInfo[i][TASK_PAUSE]);
                    break;
                  case FIND_SEND:
                    str = browser.findAndSend(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskData[i][TASK_ID]);
                    break;
                  case WAIT_SEND:
                    str = browser.waitAndSend(taskInfo[i][TASK_TYPE],
                        taskData[i][TASK_TYPE], taskInfo[i][TASK_WAIT],
                        taskData[i][TASK_ID]);
                    break;
                  default:
                    k = -1;
                    str = "Wrong Type: " + taskInfo[i][TASK_ID];
                }
            }
            catch (Exception e) {
                k = -2;
                str = e.toString();
                System.out.println("Step " + i + " failed: " + e.toString());
                break;
            }
            if (k < 0) {
                System.out.println("Step " + i + " failed: " + str);
                break;
            }
        }
        browser.close();
        if (k >= 0)
            System.out.println("Successful test of " + taskInfo.length +
                " steps on " + uri);
    }

    private static void printUsage() {
        System.out.println("ScriptedBrowser Version 1.0 (written by Yannan Lu)");
        System.out.println("ScriptedBrowser: an HtmlUnit Webdriver tester");
        System.out.println("Usage: java org.qbroker.net.ScriptedBrowser -u uri -t attrs -d 1");
        System.out.println("  -?: print this message");
        System.out.println("  -u: uri");
        System.out.println("  -t: task attributes delimitered by ':'");
        System.out.println("  -b: browser version");
        System.out.println("  -d: debug mode");
    }
}
