package org.qbroker.common;

/* RunCommand.java - a thread to exec a OS command */

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import org.qbroker.common.TimeoutException;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * RunCommand runs a given command with timeout constrains.  The
 * static method, exec(), returns the stdout of the command if it
 * is successful before it is timed out.  Otherwise, exec() throws
 * TimeoutException for timeout or RuntimeException for errors.
 *<br/><br/>
 * RunCommand parses the cmdLine into a cmdArray before execute it.
 * Therefore, you can use double-quotes in the commandLine.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class RunCommand implements Runnable {

    public static String exec(String cmd, int timeout)
        throws TimeoutException {
        if (timeout < 0)
            throw(new IllegalArgumentException("negative timeout: " + timeout));
        if (cmd == null || cmd.length() == 0)
            throw(new IllegalArgumentException("no cmd specified"));
        String[] cmdArray = RunCommand.parseCmd(cmd);
        return RunCommand.exec(cmdArray, timeout);
    }

    public static String exec(String[] cmdArray, int timeout)
        throws TimeoutException {
        if (timeout < 0)
            throw(new IllegalArgumentException("negative timeout: " + timeout));
        if (cmdArray == null || cmdArray.length == 0)
            throw(new IllegalArgumentException("no cmd specified"));
        int timeLeft = timeout;
        Process p = null;
        RunCommand rc = new RunCommand(cmdArray[0], timeout);
        Thread t1 = new Thread(rc, "stdout");
        Thread t2 = new Thread(rc, "stderr");
        try {
            p = Runtime.getRuntime().exec(cmdArray, null);
            rc.setErrorStream(p.getErrorStream());
            rc.setInputStream(p.getInputStream());
            try { // close stdin into the process
                p.getOutputStream().close();
            }
            catch (Exception ex) {
            }
        }
        catch (IOException e) {
            GenericLogger.log(Event.WARNING, "Process failed for: " +
                cmdArray[0] + ": " + Utils.traceStack(e));
            if (p != null)
                p.destroy();
             p = null;
            return "Process failed: " + e.toString();
        }

        t1.start();
        t2.start();
        long timeStart = System.currentTimeMillis();
        for (;;) {
            try {
                if (t1.isAlive() && timeLeft >= 0)
                    t1.join(timeLeft);
                if (timeout > 0)
                    timeLeft = timeout - (int) (System.currentTimeMillis() -
                        timeStart);
                if (t2.isAlive() && timeLeft >= 0)
                    t2.join(timeLeft);
                Thread.sleep(1000);
                // in case of timedout, interrupt thread
                if (t1.isAlive() || t2.isAlive()) try {
                    rc.timeout();
                    t1.interrupt();
                    t2.interrupt();
                    Thread.sleep(1000);
                    t1.interrupt();
                    t2.interrupt();
                    GenericLogger.log(Event.INFO, Utils.traceStack(
                        new TimeoutException("Process is killed: " +
                            cmdArray[0])));
                }
                catch (Exception ex) {
                }
            }
            catch (InterruptedException e) {
                if (timeout > 0)
                    timeLeft = timeout - (int) (System.currentTimeMillis() -
                       timeStart);
                if (timeLeft > 0 || (timeLeft == 0 && timeout == 0))
                    continue;
                else if (t1.isAlive() || t2.isAlive()) try {
                    rc.timeout();
                    t1.interrupt();
                    t2.interrupt();
                    GenericLogger.log(Event.INFO, Utils.traceStack(
                        new TimeoutException("Process is aborted: " +
                            cmdArray[0])));
                }
                catch (Exception ex) {
                }
            }
            break;
        }

        for (;;) {// in case the process is still running
            try {
                p.exitValue();
            }
            catch (Exception e) {
                if (timeout > 0)
                    timeLeft = timeout - (int) (System.currentTimeMillis() -
                        timeStart);
                if (timeLeft <= 0) {
                    p.destroy();
                    break;
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ex) {
                }
                continue;
            }
            break;
        }
        String line = rc.getOutput();
        int status = rc.getStatus();
        rc.close();
        if (status < 0)
            throw(new RuntimeException(line));
        else if (status == 3)
            return line;
        else
            throw(new TimeoutException(line));
    }

    public RunCommand(String cmd, int timeout) {
        this.keepWaiting = true;
        this.cmd = cmd;
        this.strBuf = new StringBuffer();
        this.timeout = timeout;
        this.status = 0;
        this.err = null;
        this.in = null;
    }

    public static String[] parseCmd(String cmd) {
        int i, n, quo = -1, pos = -1;
        char c;
        String[] cmdArray = null;
        ArrayList<String> cmdList = new ArrayList<String>();

        if (cmd == null)
            return null;
        n = cmd.length();
        for (i=0; i<n; i++) {
            c = cmd.charAt(i);
            if (c == ' ') {
                if (pos >= 0) {
                    cmdList.add(cmd.substring(pos, i));
                    pos = -1;
                }
            }
            else if (c == '\"') {
                if (quo >= 0 && pos >= 0) {
                    cmdList.add(cmd.substring(pos, quo) +
                        cmd.substring(quo+1, i));
                    quo = -1;
                    pos = -1;
                }
                else if (quo >= 0 && i > quo + 1) {
                    cmdList.add(cmd.substring(quo+1, i));
                    quo = -1;
                }
                else if (quo < 0)
                    quo = i;
                else
                    quo = -1;
            }
            else if (quo < 0 && pos < 0) {
                pos = i;
            }
        }
        if (quo >= 0 && pos >= 0)
            cmdList.add(cmd.substring(pos, quo) + cmd.substring(quo+1));
        else if (quo >= 0 && pos < 0)
            cmdList.add(cmd.substring(quo+1));
        else if (pos >= 0)
            cmdList.add(cmd.substring(pos));
        n = cmdList.size();
        cmdArray = new String[n];
        for (i=0; i<n; i++)
            cmdArray[i] = cmdList.get(i);
        return cmdArray;
    }

    public void run() {
        int id, n = 0, bytesRead = 0;
        InputStream br = null;
        byte[] buffer = new byte[4096]; 
        StringBuffer strBuf = new StringBuffer();
        String threadName = Thread.currentThread().getName();

        if ("stderr".equals(threadName)) { // stderr
            br = err;
            id = 2;
        }
        else {
            br = in;
            id = 1;
        }

        long t = System.currentTimeMillis();
        long timeEnd = t + ((timeout > 0) ? timeout : 100);
        do {
            try {
                while((bytesRead = br.read(buffer, 0, 4096)) > 0){
                    strBuf.append(new String(buffer, 0, bytesRead));
                }
            }
            catch (InterruptedIOException e) {
            }
            catch (Exception e) {
                GenericLogger.log(Event.WARNING, "Read failed from " + id +
                    " for: " + cmd + ": " + e.toString());
                append(" read failed from " + id);
                setStatus(-id);
                timeout();
                break;
            }
            n = strBuf.length();
            for (int i=n-1; i>=0; i--) {
                if (strBuf.charAt(i) == '\n') {
                    append(strBuf.substring(0, i));
                    strBuf.delete(0, i);
                    n -= i + 1;
                    break;
                }
            }
            if (timeout > 0)
                t = System.currentTimeMillis();
        } while (bytesRead >= 0 && keepWaiting && t < timeEnd);
        if (n > 0)
            append(strBuf.toString());

        if (br != null) try {
            br.close();
        }
        catch (Exception e) {
        }
        br = null;
        setStatus(id);
        if (id == 2)
            err = null;
        else
            in = null;
    }

    public int getStatus() {
        return status;
    }

    public String getOutput() {
        return strBuf.toString();
    }

    public void setInputStream(InputStream ins) {
       in = ins;
    }

    public void setErrorStream(InputStream ins) {
       err = ins;
    }

    public synchronized void timeout() {
        keepWaiting = false;
    }

    private synchronized void setStatus(int s) {
       if (s < 0)
           status = s;
       else if (status >= 0)
           status += s;
    }

    private synchronized void append(String str) {
       strBuf.append(str);
    }

    public void close() {
        cmd = null;
        if (strBuf != null) {
            strBuf.delete(0, strBuf.length());
            strBuf = null;
        }
        if (err != null) try {
            err.close();
        }
        catch (Exception e) {
        }
        err = null;
        if (in != null) try {
            in.close();
        }
        catch (Exception e) {
        }
        in = null;
    }

    protected void finalize() {
        close();
    }

    private int status, timeout;
    private boolean keepWaiting;
    private String cmd;
    private StringBuffer strBuf;
    private InputStream err, in;
    private OutputStream out;
}
