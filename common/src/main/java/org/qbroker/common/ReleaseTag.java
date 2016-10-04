package org.qbroker.common;

/* ReleaseTag.java - release tag for all the packages of qbroker */

/**
 * V1.0.2 (2016/06/14): got rid of the support for XML properties, from now on,
 * QBroker only supports JSON config files, added getEvn() to StaticReport for
 * environment variables, added Template to FormattedEventMailer before
 * TemplateFile so that it will not soly rely on TemplateFile
 *<br/>
 * V1.0.1 (2016/06/08): changed to log to stderr for query on MontiorAgent and
 * QFlow, added MonitorUtils.select() and MonitorUtils.substitute() on
 * RequestCommand to GenericList, AgeMonitor, NumberMonitor and
 * IncrementalMonitor, fixed the issue with the null type in initMonitor() of
 * MonitorGroup, removed the exception logging on close() for StreamReceiver
 * and StreamPersister.
 *<br/>
 * V1.0.0 (2016/06/05): it is a long painful process to rewrite the source code
 * of tangam completely. The code name is renamed to QBroker. It is a Maven 2
 * project now. A lot of features have been added. One of them is to fully
 * support JSON configurations.
 */
public class ReleaseTag {
    private static String TAG = null;
    private static String ReleaseTAG = "QBroker V 1.0.2 2016/06/14 20:11:06";

    public ReleaseTag() {
    }

    public static String getTag() {
        if (TAG == null) {
            int j, i = ReleaseTAG.indexOf(" V ");
            if (i > 0) {
                j = ReleaseTAG.indexOf(" ", i+3);
                TAG = ReleaseTAG.substring(i+3, j);
            }
        }
        return TAG;
    }

    public static String getReleaseTag() {
        return ReleaseTAG;
    }
}
