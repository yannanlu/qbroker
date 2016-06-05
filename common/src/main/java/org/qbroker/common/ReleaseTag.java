package org.qbroker.common;

/* ReleaseTag.java - release tag for all the packages of qbroker */

/**
 * V1.0.0 (2016/06/05): it is a long painful process to rewrite the source code
 * of tangam completely. The code name is renamed to QBroker. It is a Maven 2
 * project now. A lot of features have been added. One of them is to fully
 * support JSON configurations.
 */
public class ReleaseTag {
    private static String TAG = null;
    private static String ReleaseTAG = "QBroker V 1.0.0 2016/06/05 18:41:36";

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
