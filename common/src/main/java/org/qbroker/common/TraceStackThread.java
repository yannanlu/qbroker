package org.qbroker.common;

import java.io.PrintWriter;
import java.io.PipedWriter;
import java.io.PipedReader;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * TraceStackThread is a thread used to trace the stack info of a java program.
 *<br/>
 * @author  yannanlu@yahoo.com
 */

public class TraceStackThread extends Thread {
    private PrintWriter out = null;
    private Throwable throwable = null;

    public TraceStackThread(PrintWriter out, Throwable e) {
        this.out = out;
        this.throwable = e;
    }

    public void run() {
        if (out != null && throwable != null) {
            throwable.printStackTrace(out);
            out.flush();
            out.close();
        }
    }

    protected void finalize() {
        if (out != null) try {
            out.close();
            out = null;
        }
        catch (Exception e) {
        }
    }

    // method to get String of the StackTrace for an Exception
    public static String traceStack(Throwable e) {
        StringBuffer strBuf = new StringBuffer();
        PipedWriter pipeOut = new PipedWriter();
        BufferedReader in;
        try {
            in = new BufferedReader(new PipedReader(pipeOut));
        }
        catch (Exception ex) {
            strBuf.append(e.getMessage());
            if (strBuf.length() == 0)
                strBuf.append(e.toString());
            return strBuf.toString();
        }

        new TraceStackThread(new PrintWriter(pipeOut), e).start();

        try {
            String line;
            while ((line = in.readLine()) != null) {
                strBuf.append(line);
                strBuf.append("\n");
            }
            in.close();
        }
        catch (Exception ex) {
        }
        if (strBuf.length() == 0) {
            strBuf.append(e.getMessage());
            if (strBuf.length() == 0)
                strBuf.append(e.toString());
        }
        return strBuf.toString();
    }
}
