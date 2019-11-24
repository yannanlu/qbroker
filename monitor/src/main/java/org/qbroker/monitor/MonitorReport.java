package org.qbroker.monitor;

/**
 * MonitorReport is an Interface to get a report on a monitored entity
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface MonitorReport {
    /**
     * generates a report on a monitored entity and returns it as a Map.
     * The report Map will be passed to the associated MonitorAction
     * as an opaque object.  The action object is supposed to know how to
     * retrieve the information from the report Map.
     */
    public java.util.Map<String, Object> generateReport(long currentTime)
        throws Exception;
    /** returns the skipping status */
    public int getSkippingStatus();
    /** returns the name of the report */
    public String getReportName();
    /** returns the report mode */
    public int getReportMode();
    /** returns a copy of the shared report keys */
    public String[] getReportKeys();
    /** closes and releases all opened resources */
    public void destroy();

    /** report process gets exception */
    public final static int EXCEPTION = -2;
    /** report process is disabled */
    public final static int DISABLED = -1;
    /** report process is done */
    public final static int NOSKIP = 0;
    /** report process is skipped */
    public final static int SKIPPED = 1;

    /** not a report */
    public final static int REPORT_NONE = 0;
    /** private report */
    public final static int REPORT_LOCAL = 1;
    /** private cached report */
    public final static int REPORT_CACHED = 2;
    /** shared final report for the local instance */
    public final static int REPORT_FINAL = 3;
    /** shared report for the local instance */
    public final static int REPORT_SHARED = 4;
    /** shared report for the current node */
    public final static int REPORT_NODE = 5;
    /** shared report for a MessageFlow on the current node */
    public final static int REPORT_FLOW = 6;
    /** shared report for all nodes of cluster */
    public final static int REPORT_CLUSTER = 7;
}
