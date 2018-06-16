package org.qbroker.flow;

/* ReleaseTag.java - release tag for all the packages of qbroker */

/**
 * V1.2.9 (2018/06/09): removed the space between release version and "V" from
 * ReleaseTag, added the support of base64 encoded names for checkpointing to
 * MonintorGroup and Node, changed CheckpointTimeout to CheckpointExpration for
 * MonintorGroup, added cache to checkpoint() and restore() of SonicMQMonitor,
 * removed CheckpointTimeout from all implementations of Monitor, added the
 * initial reset to all queue monitors, added reply() to QueueConnector and
 * JMSQueueConnector, added handling of JMSException for session expiration
 * on creating a temporary queue in request() of JMSQConnector
 *<br/>
 * V1.2.8 (2018/06/03): fixed a typo in the comments of ASE, added
 * createTemporaryQueue() to JMSQConnector, added ctx.close() to QConnector,
 * TConnector and QMFRequester for cleanups, added request() to QueueConnector
 * and JMSQConnector, updated QConnector in both jms and wmq packages, updated
 * JMSPersister to support the operation of request, added query of metrics
 * on connections with support for a specific user to SonicMQRequester, added
 * query support on connection metrics for a queue or a topic to SonicMQMonitor,
 * added getMappedName() and getGenericMapList() to MonitorUtils for generic
 * name mappings, changed SonicMQMonitor so that curDepth is for
 * FlowToDiskMemorySize rather than TopicDBSize and oppsCount is for
 * TopicDBSize, added JNDILookup as a utility to jndi package
 *<br/>
 * V1.2.7 (2018/03/31): added the interface of HTTPConnector to common package
 * and added HTTPClient as the wrapper of HttpClient to implement HTTPConnector,
 * changed net.HTTPConnector to implement the same interface, updated
 * HTTPMessenger to use HTTPClient in case of ProxyHost defined, updated
 * EventPoster, FileSyncronizer to use HTTPClient in case of ProxyHost
 * defined, changed AWSClient to use HTTPClient to support proxy, added debug
 * on the request header to WebTester, added LogDetail to AgeMonitor,
 * NumberMonitor and IncrementalMonitor to limit logging on leadingBlock,
 * fixed a bug in RunCommand on its method of parseCmd for handling escapted
 * quotes, added support of https on EventActionGroup, upgraded sonicmq lib
 * from 7.6 to 10.0
 *<br/>
 * V1.2.6 (2018/03/24): added TrustAllCertificates to WebTester so that
 * it will trust self-signed certificates for HTTPS, updated those using
 * WebTester to query pages, fixed a typo in PropertyMonitor causing
 * NullPoniterException, removded those deprecated methods from ClientSocket
 * and changed it to a static class with partial support for proxy
 *<br/>
 * V1.2.5 (2018/02/24): added AES for OpenSSL-like encryptions, added two
 * methods of scramble and unscramble to Base64Encoder, added support of
 * EncryptedData to all the low level classes so that they will be able to
 * decrypt the OpenSSL encrypted data for sensitive data like password,
 * added the method of decrypt to Utils, added support to PropertyMonitor for
 * loading properties from environment variables with EnvironmentVariable map,
 * added support to StaticReport for loading properties from a property file,
 * changed StaticReport to use EnvironmentVariable map to load environment
 * variables, renamed EncryptedAuthorization to BasicAuthorization in WebTester
 *<br/>
 * V1.2.4 (2018/02/14): added aws package for AWS service, added support for
 * multiple properties to QClient, fixed some typoes
 *<br/>
 * V1.2.3 (2017/08/19): added missing event initializtion in QServlet and
 * MsgServlet, added logging in case jsp does not exist, changed to use println,
 * removed the jdbc check from CQLConnector
 *<br/>
 * V1.2.2 (2017/06/11): added RequestNode to support async requests, changed
 * BOUNDARY to the lowest id of outlinks for collectibles, updated comments on
 * bunch of implementations of MessageNode, added support of static cache
 * updates to CacheNode, added outLonkMap to CacheNode, CollectNode,
 * MapReduceNode and MonitorNode
 *<br/>
 * V1.2.1 (2017/05/27): added the support of post formatters to EventMonitor
 * and EventCorrelator for newly generated events, removed hardcoded String
 * encoding from MessageUtils, fixed a bug on the request path in QServlet
 * introduced in release 1.1.7, changed to display the new msg body in
 * FormatNode, fixed a deep bug in Msg2Text.format(Message) that it failed
 * to catch exceptions due to calling overloaded API, fixed a bug in both
 * postable() and collectible() of EventUtils that failed to handle null
 * attributes, added a check on null value of the input string to the method
 * of searchAndReplace() in Utils.
 *<br/>
 * V1.2.0 (2017/05/21): changed Perl5Matcher from a instance object to a local
 * object got from ThreadLocal in Template and TextSubstitution, removed the
 * public APIs with Perl5Matcher from Template and TextSubstitution, also
 * removed the synchronized methods from Template and TextSubstitution, updated
 * all the dependent classes due to this API change, also removed two
 * synchonized methods from Utils with DateFormat object stored in ThreadLocal,
 * added cleanups on used Template and TextSubstitution for MessageFilter, etc,
 * added Expression as an interface and Term as an implementation
 *<br/>
 * V1.1.13 (2017/05/20): added support for simple expressions on JSON path
 * to JSON2Map so that the index of a list is either evaluated from a numeric
 * expression or is selected from the evaluations on its children
 *<br/>
 * V1.1.12 (2017/04/15): added DisabledWithReport to handle DISABLED case of
 * DependecyGroup on Report and Monitor, updated all the monitors, removed the
 * support of EventPostable from HTTPMessenger, added direct support of
 * EventPostable and EventCollectible to Msg2Text for TextEvent only, added
 * the recovery of the property of priority for JMSEvent in duplicate() of
 * MessageUtils, added IgnoreHTTP412 to HTTPMessenger, added LogHTTP412 to
 * MonitorAgent for ignoring HTTP 412 error, fixed a bug in Evaluation on
 * the boolean expressions, fixed a typo on rpt_global_var in QFlow, added
 * main() to ReleaseTag, removed Version
 *<br/>
 * V1.1.11 (2017/03/19): added group support for outlinks to DispatchNode so
 * that the load balance works on separate groups of outlinks, changed the way
 * to generate the unique key for plugin caches via JSON2Map.toJSON() in 7
 * implementations of MessageNode, added the check on the header of 
 * "Connection: close" to WebTester, fixed a missing if statement in
 * replaceRule() for 7 MessageNode implementations, fixed a typo on ids of
 * SelectedOutLink in DuplicateNode, changed the handling on HTTP return code
 * in HTTPMessenger so that 2xx is a success for download(), fulfill() and
 * store()
 *<br/>
 * V1.1.10 (2017/03/04): added ResetPriority and Template to EventMonitor for
 * the default timeout escalations, added updateParameters() to EventMonitor,
 * fixed the issue of missing reference timestamp on QucikCache instances in
 * rest of MessageNode implementations, removed in.available() from testHTTP()
 * of WebTester to avoid socket exceptions, fixed a bug with choose() on two
 * single quoted strings in Evaluation, merged the function of choose to eval
 * in TextSubstitution, fixed a bug in put() of JSON2Map if a new key is
 * something like ".xxx", added a check in copy() of JSONSection so that
 * the override only happens on the existing keys, added SessionSize to
 * StreamPersister to support auto disconnections whenever processed msg count
 * is same as SessionSize for TCP sockets, disabled the pattern match on TCP
 * test for WebTester only if Pattern is not defined, added previousQStatus to
 * JMSLogMonitor to reset the actionCount
 *<br/>
 * V1.1.9 (2017/02/12): rewritten JSONParser to add the support for JSONPath, 
 * Pattern and Substitution, added two headers to WebTester to support soap
 * testing, added try-catch block to CACHE rule of EventMonitor in case of
 * Exceptions, added ReadErrorIgnored to WebOperator, added detail log for
 * debug mode to WebTester, modified reactions on EXCEPTION for checks of
 * DependencyGroup in MonitorUtils, changed URLMonitor to use the cloned
 * property map for WebTester, added support for Accept and ContentType headers
 * to PropertyMonitor for HTTP/HTTPS
 *<br/>
 * V1.1.8 (2016/12/23): fixed a bug in deleteMonitors() of MonitorGroup for
 * not updating the list of items for generated monitors, normalized the debug
 * logging in MonitorGroup, added main() to most of monitors for testing,
 * added output of pagesource and title to SyntheticMonitor for testings
 *<br/>
 * V1.1.7 (2016/11/17): fixed the external tracking issue in flush() for
 * EventMonitor and ActionNode, added stats tracking on new messages for
 * SelectNode and DuplicateNode, added timeout ruleset to EventMonitor,
 * added protection on request path in processRequest() of QServlet,
 * refactored evaluate() in Evaluation for boolean operations, added the
 * support for string comparisons to boolean operations, added From to
 * EventFormatterMailer, added DEBUG_DIFF to Node for replaceRule() logging,
 * updated PublishNode, EventDisaptcher, AggregateNode, ScreenNode and
 * CascadeNode for DEBUG_DIFF, added WAIT_FIND to ScriptedBrowser and
 * SyntheticMonitor, added OBJ_PROC to GenericList
 *<br/>
 * V1.1.6 (2016/11/12): added logBuffer.clear() to close() in NewlogFeture,
 * replaced get() with remove() for dataBlock in NumberMonitor, AgeMonitor and
 * IncrementalMonitor, added logBuffer.clear() to performAction() in
 * UnixlogMonitor, added support for termination ruleset to ScreenNode, added
 * support for reset of COUNT to touch() of QuickCache
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
    private static String ReleaseTAG = "QBroker V1.2.9 2018/06/09 11:47:03";

    public ReleaseTag() {
    }

    public static String getTag() {
        if (TAG == null) {
            int j, i = ReleaseTAG.indexOf(" V");
            if (i > 0) {
                j = ReleaseTAG.indexOf(" ", i+2);
                TAG = ReleaseTAG.substring(i+2, j);
            }
        }
        return TAG;
    }

    public static String getReleaseTag() {
        return ReleaseTAG;
    }

    public static void main(String args[]) {
        System.out.println(getReleaseTag());
    }
}
