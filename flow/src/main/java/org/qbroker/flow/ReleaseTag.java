package org.qbroker.flow;

/* ReleaseTag.java - release tag for all the packages of qbroker */

/**
 * V1.1.6 (2016/11/12): added logBuffer.clear() to close() in NewlogFeture,
 * replaced get() with remove() for dataBlock in NumberMonitor, AgeMonitor and
 * IncrementalMonitor, added logBuffer.clear() to performAction() in
 * UnixlogMonitor
 *<br/>
 * V1.1.5 (2016/11/05): added GroupDebug and FlowDebug to QFlow, added cleanup
 * on report for MonitorAgent, added cleanup on dataBlock in AgeMonitor,
 * NumberMonitor and IncrementalMonitor, renamed some metric keys in
 * SonicMQRequester such as from messages.Count to messages_Count, etc, updated
 * SonicMQMonitor for this rename, fixed a deep bug of empty changes in
 * hasMajorChange() of MessageFlow, normalized the debug logging in QFlow
 * MonitorAgent and QServlet, added debug logging on changes of base properties,
 * fixed a bug on the list of keys in normalize() of Utils
 *<br/>
 * V1.1.4 (2016/10/23): fixed the issue with queryInfo in MonitorAgent and
 * QFlow where the key of Record is missing for JSON, added the missing part
 * to clear parameters for JSONTemplate in MessageEvaluator, added RCRequired
 * to MapReduceNode to handle the case of missing RC, added to include the
 * pattern text if the pattern failed to match in URLMonitor, fixed the query
 * issue with PROPERTY for QFlow and MonitorAgent
 *<br/>
 * V1.1.3 (2016/10/16): cleaned up all the pom.xml, removed unused import
 * from GenericList, moved JSONFormatter from json to jms and updated JSONTNode,
 * CascadeNode and MessageEvaluator due to this change, removed newTextEvent()
 * from EventUtils due to this change, added new name space of "o:" to parameter
 * map for overrides in JSONSection and JSONTemplate, rearranged the code blocks
 * for MessageEvaluator
 *<br/>
 * V1.1.2 (2016/10/14): replaced "Filter" with "Ruleset" for dynamic content
 * filter in JDBCMessenger, JMSQConnector, JMXMessenger, MongoDBMessenger,
 * QMFMessenger, PCFRequester and SonicMQMessenger, added okRC for CommaList
 * in MessageEvaluator, removed unused imports for wmq from QClient
 *<br/>
 * V1.1.1 (2016/10/13): moved ReleaseTag from common to flow, updated QFlow
 * and MonitorAgent for that, changed QServlet and MsgServelt to save the
 * response data map to the context path rather than the name of the request,
 * replaced the support of Branch and Loop with PipeList, ColonList and
 * CommaList in MessageEvaluator for multple rules
 *<br/>
 * V1.1.0 (2016/10/12): made a lot of changes on JSON template support, tested
 * on WebAdmin for Console, etc
 *<br/>
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
    private static String ReleaseTAG = "QBroker V 1.1.6 2016/11/12 13:48:21";

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
