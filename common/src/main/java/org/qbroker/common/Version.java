package org.qbroker.common;

/* Version.java - build version of the all packages of QBroker */

/**
 * T.1.0.0 (2015-12-20): added common::Event, common::Filter to solve the
 * circular Maven dependency issue, moved jms::Requester to common::Requester,
 * added CQLConnector for Cassandra support, added support of Generics to
 * most of the classes, moved MonitorNode, EventMonitor, EventDispatcher,
 * EventCorrelator to node package, moved MonitorPersister and EventPersister
 * to persister package, etc.
 *</br/>
 * GIT (2015-12-19): In Spring of 2015, panda lost the power and failed to
 * boot up due to the failure of the hard disks. So the SVN repository got
 * lost completely. Lucky enough, the source code tree has been backed up.
 * I have decided to migrate the project to a GIT repository on Cloud.
 * Meanwhile, I would like to convert QBroker project to a Maven 2 project.
 * A lot of efforts have been put into this migration. It is finally ready.
 *</br/>
 *</br/>
 * R.937 (2014-11-21): fixed api change on xq for UDPSocket and MulticastGroup,
 * updated comments in CascadeNode
 *</br/>
 * R.934 (2014-10-27): replaced JSON2Map with JSON2FmModel in Aggregation and
 * MapReduceNode, added WrapperException and the interface of Requester,
 * added the implementation of Requester to PCFRequester, added updateContent()
 * to most of the monitor reports, added CascadeNode, fixed typos in
 * MessageFilter, fixed a bug in JSONPath select part in MapReduceNode
 *</br/>
 * R.912 (2014-07-28): added support of templates to reference both msg
 * properties and json properties with '.' in the beginning of the key for
 * JSONSelector, changed getPattern() to getValue() and added getValue(int) in
 * JSONSelector for overloading, added support for xml config on selector map,
 * fixed a bug on parsing comments in JSON2FmModel, added convertion of json
 * data in each() for JSONFormatter
 *</br/>
 * R.909 (2014-07-22): added support for sub and gsub to TextSubstitution,
 * changed its default delimiter for replace, sub and gsub to '!', added
 * getField() to Template and changed MessageUtils to use getField() in its
 * methods of format(), added support of each to JSONFormatter, also added its
 * support for JSON path as the template fields, added missing check on null
 * filter in XPathNode and JSONPathNode
 *</br/>
 * R.904 (2014-07-16): changed the delimiter for replace to '|' in
 * TextSubstitution, lifted the restriction on bypassing msgs to the first
 * outlink in AggregateNode, removed the support of jaction from JSONPathNode
 *</br/>
 * R.901 (2014-07-08): fixed a bug in monitor() of DeliverNode that failed to
 * detect any stuck queue, added isDebug to monitor() in DeliverNode and
 * PickupNode, fixed a bug in SelectNode that failed to reset OUT_SIZE,
 * changed to have OUT_STATUS to take NODE_STATUS instead of XQueue.STATUS
 *</br/>
 * R.998 (2014-07-07): fixed bugs in CacheNode, SelectNode, MapReduceNode,
 * and ActionNode, EventMonitor, added rule level dmask, dropped static
 * property set, added post format in most of the nodes
 *</br/>
 * R.871 (2014-07-03): changed JSONPathExpression to JSONPath in SelectNode,
 * JSONPathNode and MessageEvaluator, added getPattern() to JSONSelector,
 * add support of sort and uniq to JSONFormatter
 *</br/>
 * R.876 (2014-06-30): moved script engine from JSONTNode to ScriptNode, moved
 * some static methods from MessageUtils to MessageFilter for localization,
 * added updatedBody() to MessageFilter, added support of Formatters to
 * JSONTNode and ScriptNode so that they can pre-format messages, added
 * plugin support and JSONFormatter to JSONTNode, removed jformat() from
 * JSONPATHNode, replaced jaction() and jformat() with JSONFormatter in
 * MessageEvaluator, fixed the leading "." issue with put() in JSON2Map,
 * added JSONFormatter and JSONSelector
 *</br/>
 * R.855 (2014-06-27): replaced xerces URI with net URI on most of classes,
 * added support of replace to TextSubstitution, added support of select to
 * the map process of MapReduceNode
 *</br/>
 * R.832 (2014-06-17): fixed typos in jformat() for JSONPathNode and
 * MessageEvaluator, fixed the double count on RESULT_OUT of SelectNode
 *</br/>
 * R.829 (2014-06-09): added jaction() and jformat() to JSONPathNode and
 * MessageEvaluator, added getDataType() to JSON2Map and JSON2FmModel, add
 * support on random session and natural session for flow controls in PipeNode
 *<br/>
 * R.824 (2014-04-23): added JettyServer and HTTPServer as the wrapper for
 * the embbeded Jetty Server, updated QFlow and MonitorAgent to use
 * the HTTPServer interface, fixed the issue with returned Throwable from
 * getTargetException() on QFlow, MonitorAgent and others, added
 * a test on httpServer in doRequest() of QFlow for escalations
 *<br/>
 * R.816 (2014-04-22): added embedded Jetty Server as the admin server to
 * QFlow and MonitorAgent, doubled the max number of receivers in
 * MessageFlow, updated AgeMonitor, NumberMonitor and IncrementalMonitor due
 * to the api change on XQueue, fixed a typo in IndexedXQueue
 *<br/>
 * R.809 (2014-04-11): added callback to respond() in MessageStream with
 * api changes, updated ServerReceiver, StreamReceiver and ServiceNode due to
 * the api changes on respond(), added callback to reply() in MessagePacket
 * without api changes
 *<br/>
 * R.804 (2014-04-11): fixed the issue of the null fed-back message with
 * respond() in MessageStream, added acknowledge() to CollectibleCells for
 * JMS messages, added finalize() to most of the data objects in common
 *<br/>
 * R.797 (2014-04-09): fixed the reconnect issue with FTPSConnector, added
 * isFull() to AssetList, added getParent() to HTTPConnector, changed api of
 * remove() and putback() in XQueue to de-reference obj at remove(), changed
 * api of putback() in QList, updated all impacted classes due to the api
 * changes
 *<br/>
 * R.762 (2014-03-27): added callback support to PipeNode, ServiceNode and
 * SelectNode, fixed a typo in ActionNode, added missing break to feedback()
 * in JobNode, fixed a typo in comments of MessageStream
 *<br/>
 * R.755 (2014-03-26): added FTPSConnector and FTPSMessenger to support ftps,
 * added support of ftps to NodeUtils, FileReceiver and FilePersister,
 * added genetics to FTPConnector and NTTPConnector for commons-net-3.3,
 * added missing cancel() on sid in LogSlicer, changed api in ThreadPool and
 * updated ServerListener, ServerReceiver, ServerPersister and ServiceNode,
 * added flush before close in StreamReceiver and StreamPersister
 *<br/>
 * R.744 (2014-03-16): added support for boolean expressions and ternary
 * expressions to Evaluation, fixed a huge naive bug on invalidations in
 * CacheNode, fixed the issue with session never expires in AggregateNode,
 * changed to allow LAST to take DefaultValue in Aggregation, added try-catch
 * block on ClientSocket.connect() to a bunch of classes for protection on
 * DNS errors, added getDepth() and changed getCount() in ThreadPool, updated
 * all other classes using ThreadPool, added callback supports to CacheNode,
 * JobNode and ScreenNode
 *<br/>
 * R.728 (2014-02-24): added support for generated msgs to passthru() in
 * Node, changed to use instance passthru() for generated msgs in a bunch of
 * nodes, added callback support to DeliverNode, PickupNode and MapReduceNode,
 * removed overlap protection in ActionNode, EventMonitor, EventCorrelator and
 * MonitorNode
 *<br/>
 * R.702 (2014-02-20): changed count from int to long for all implementations
 * of MessageNode, added CollectibleCells and the support of callbacks to Node,
 * added callback support to most of the implementations of MessageNode, except
 * for those with complicated routing features, such as SelectNode, CacheNode,
 * DeliverNode, ServiceNode, ScreenNode, PickupNode, PipeNode, JobNode, and
 * MapReduceNode, added overlap protection on the first outlink in ActionNode,
 * AggregateNode, PublishNode, SpreadNode, EventMonitor, EventDispatcher,
 * EventCorrelator and MonitorAgent
 *<br/>
 * R.661 (2014-02-19): added CollectibleCells, ReservableCells and
 * XQCallbackWrapper, used CollectibleCells and ReservableCells in doRequest()
 * of QFlow and MonitorAgent, added callback to XQueue and IndexedXQueue,
 * fixed TimeZone issue in URLMonitor and AgeMonitor, added jmerge() and jcut()
 * to MessageEvaluator, redone list selection part with ListKey in XPathNode
 *<br/>
 * R.650 (2014-02-12): fixed the path reset issue for MonitorAgent and QFlow
 * in case configRepository reload in the test, added chop support to
 * Template and TextSubstitution, added NeedChop to FormatNode for TemplateFile,
 * added TemplateFile with chop option to MessageEvaluator, added chop action
 * to HTTPConnector's main() for testing
 *<br/>
 * R.644 (2014-02-10): added support for xml or json content posted via form
 * to QServlet, added JSON to queryInfo() in QFlow, MonitorAgent and MessageFlow
 *<br/>
 * R.640 (2014-01-14): removed toXML(), diffXML(), display(), flatten() from
 * XML2Map, added them to JSON2Map, added JSONParser, added support of isJSON
 * to QFlow, MonitorAgent, EventDispatcher, PropertyMonitor, etc,
 * added debugForReload to QFlow and MonitorAgent for testing,
 * updated many classes due to this change
 *<br/>
 * R.635 (2013-12-02): added global var support to QFlow, removed the
 * support of substitutions on global vars from all low level classes
 *<br/>
 * R.634 (2013-12-02): added MessageTerminator, added substituteProperties()
 * to MessageUtils for substituting on global vars, added ResultField for
 * age on file age in MessageEvaluator, added EnvironmentVariable to
 * StaticReport for global vars, added global var support to all nodes in
 * MessageFlow
 *<br/>
 * R.629 (2013-11-21): added a reconnect to LogSlicer in case of ack failed,
 * added partition support to the first outlink in PublishNode, added
 * CheckpointTimeout to Node and SpreadNode to control checkpointing, fixed
 * an issue with static json path in SelectNode, added PUT to download() in
 * HTTPMessenger, added loadReference() to JDBCMessenger, fixed an issue
 * with doDelete() in HTTPConnector, fixed an issue with toJSON() with array
 * in JSON2Map, JSON2FmModel and JSONSection
 *<br/>
 * R.618 (2013-10-31): fixed a bug with JSONT in MessageEvaluator, added a
 * missing part for JSONT in MessageEvaluator, added a missing part to set
 * rule and ruleInfo if no rule is found in check() of CacheNode, fixed the
 * issue with debugging logs in WebTester, added retry on report if it fails
 * in PropertyMonitor
 *<br/>
 * R.613 (2013-10-30): changed JSONTemplate and JSONFilter to use JSONFmModel
 * for real json payload, added setParameter() to JSONTemplate, updated
 * JSONSection due to params, added a bunch of methods to JSONFmModel, updated
 * JSON2Map so that it can handle json data parsed by JSONFmModel, changed to
 * use JSON2FmModel for JSONT and added setParameter() in JSONTNode and
 * MessageEvaluator 
 *<br/>
 * R.605 (2013-10-28): added size() to GroupedCache, added 3 RC constants to 
 * MessageUtils, added rollback protection on the failure of final exits to
 * MapReduceNode, added rollback protection on the failure of final exits to
 * CacheNode, fixed bugs on its invalidation rules, also added support for
 * TYPE_COLLECT and Heartbeat, check() to clean up pending requests frequently,
 * also added RCRequired to forgive responses without RC defined
 *<br/>
 * R.601 (2013-10-25): added -f to main() of HTTPConnector, changed
 * HeaderProperty to RequestProperty and ResponseProperty in HTTPMessenger,
 * added FieldName, Template and Substitution to dynamic rules of CacheNode,
 * dropped escaping/unescaping on '/' for JSON in Utils, changed parse() to
 * return either a Map or a List in JSON2Map and JSON2FmModel, updated a bunch
 * of dependent classes due to this change
 *<br/>
 * R.564 (2013-10-18): fixed the issue with DispatchNdoe that is not picking
 * up the outlink with low load if the current persister is paused, rewritten
 * most of its update() and routing part; added obj.toString() to
 * cloneProperties() in Utils, fixed a bug in ReportQuery
 *<br/>
 * R.559 (2013-10-15): fixed the type of collectible and updated a bunch of
 * code, renamed collectable() to collectible() in EventUtils, XQueue and
 * IndexedXQueue, updated the classes impacted by this change
 *<br/>
 * R.535 (2013-10-12): fixed the issue with ack propagation by repacking the
 * array of node ids in the generation order in MessageFlow, added the support
 * for mandatory ack propagation on outlinks to MessageFlow, added static cache
 * to CacheNode and MessageEvaluator, added getOutLinkBoundary() for mandatory
 * ack propagation on collectables in MapReduceNode, added the support of
 * dynamic link to SelectedOutLink in MapReduceNode
 *<br/>
 * R.529 (2013-10-09): added merge of two maps for JSON in Aggregation,
 * fixed a typo on one of RULE_PID in JSONPathNode, added FieldName and
 * Template to SelectedOutLink map to format messages
 *<br/>
 * R.527 (2013-10-02): added a skipField and limitField to MongoDBMessenger
 * to support paging on find(), added to use the default template to substitute
 * global variables in case the report of GlobalProperties is not defined
 *<br/>
 * R.526 (2013-09-25): added a backward slash to the line breakers for comments,
 * changed finalize() in ClientSocket not to log errors
 *<br/>
 * R.493 (2013-09-19): added TrustAllManager to support self-signed HTTPS
 * servers, added TYPE_SSL to ClientSocket, added TrustAllCertificates to
 * HTTPConnector and fixed a bug in it, add HeaderRegex to QServlet
 * to copy headers over for PUT and POST
 *<br/>
 * R.489 (2013-09-17): added repeated select to JDBCMessenger, added the
 * cleanup of DependencyGroup for some receivers, removed the public method
 * of resetQueueBrowser() from QueueConnector, added a call of resetQueueBrowser
 * to browse() of JMSQConnector for repeated browse, added DependencyGroup to
 * JMSReceiver for repeated browse, changed to set step=1 for dependencies
 * only if it is not defined for MonitorUtils
 *<br/>
 * R.482 (2013-09-16): replaced the traps for pause and standby with finer
 * loops in all persisters and receivers, changed some logging to use
 * statusText[status] in QFlow, MessageFlow and MonitorAgent
 *<br/>
 * R.471 (2013-07-24): added reset on the group status in MonitorAgent,
 * updated some comments in SortNode, DistinguishNode, QFlow and MessageFlow,
 * added mapping select to MapReduceNode, added support of WITH to JDBCMessenger
 *<br/>
 * R.464 (2013-05-13): added quorum to MapReduceNode, added JSON template
 * file to JSONTNode, added TYPE_SCRIPT to Node, add JSONT implementation
 * to MessageEvaluator, added uniq() to JSONSection, dropped Name from
 * the constructor of JSONTemplate
 *<br/>
 * R.461 (2013-05-09): moved JSON2Map and JSON2FmModel to package of json,
 * added JSONFilter, JSONSection and JSONTemplate for JSON transformations,
 * updated package path of JSON2Map on those classes depending on JSON2Map,
 * added isArray() to JSON2Map and added support on List to JSON2Map,
 * added JSONFilter to JSONPathNode, added JSONTNode with supports of
 * JSONTemplate and JSR223 script engine, added escapes on the special chars
 * to Template in case some fields contains them
 *<br/>
 * R.455 (2013-04-27): changed JSON2Map to support the simple common JSONPath
 * expression in DOT format, updated the JSONPath expression in RMQMonitor,
 * JSONPathNode, SelectNode and MessageEvaluator, added a hack to NodeUtils
 * for the old plugins requiring HashMap in their contructors, add a check to
 * enforce the requirement on reports for dynamic sessions in DispatchNode
 *<br/>
 * R.452 (2013-04-25): added RMQMonitor for RabbitMQ, changed on the internal
 * report in JMXQMonitor and QMFQMonitor, removed to check isValid() for
 * object properties in JMSEvent, renamed query() to fulfill() in HTTPMessenger,
 * reset the default receiveTime to 1000 for HTTPMessenger, FTPMessenger and
 * SFTPMessenger, added a loop for sleepTime in all the messengers, added
 * timeout to get() in RMQConnector
 *<br/>
 * R.447 (2013-04-12): renamed XML2HashMap to XML2Map, changed HashMap to Map
 * and ArrayList to List on most of the classes, changed Object[] to Class[] in
 * ThreadPool and updated those classes depending on it, added Class[] to
 * the constructor of GenericPool and updated those classes depending on it 
 *<br/>
 * R.443 (2013-04-11): added AWSSignature for AWS, fixed a bug on authString
 * in HTTPConnector and WebTester, added sign() to Utils
 *<br/>
 * R.441 (2013-03-26): renamed JSON2HashMap to JSON2Map; PHP2HashMap to
 * PHP2Map, changed them to take Map or List only, updated AggregateNode,
 * JSONPathNode, MessageEvaluator and SelectNode due to the renaming,
 * added format() and evaluate() to Utils, added union() to Aggregation,
 * added the support for union to MapReduceNode
 *<br/>
 * R.439 (2013-03-07): added header property map to two get() methods in
 * RMQConnector, changed the base methods of toJSON() and toXML() in
 * JSON2HashMap, JSON2FmModel and XML2HashMap to return the data in tabular
 * format, changed toJSON of JSON2HashMap and JSON2FmModel to the map
 * representing JSON data, added double quotes to keys of JSON results on
 * a bunch of classes, added getBodyType() to Aggregation and updated
 * AggregateNode and MapReduceNode, fixed a bug in merge() for JSON in
 * Aggregation
 *<br/>
 * R.431 (2013-03-03): added getKey() and browser() to Aggregation, added
 * a check on number of available outlinks before map operations to
 * MapReduceNode, fixed a bug in acknowledge() in MapReduceNode
 *<br/>
 * R.427 (2013-02-28): removed checkIdle as the member variable in
 * RedisMessenger, RiakMessenger and MongoDBMessenger, added try-catch block
 * in cleanup() for ReceiverPool and PersisterPool, added missing try-catch
 * block on http in disconnect() for FileReceiver and FilePersister, added
 * a check on null in disconnect() for most of the persisters and receivers
 * removed mask in MapReduceNode
 *<br/>
 * R.419 (2013-02-14): renamed RedisConnector to JedisConnector, added
 * RedisConnector with new implementation for synchronous subscriptions,
 * changed RedisMessenger not to extend RedisConnector, added sub() to it,
 * added sub() to RedisReceiver
 *<br/>
 * R.414 (2013-02-03): added merge() and compare() to Aggregation, changed key
 * names of XPath and JSONPath for BodyAggrgation in Aggregation, removed
 * xmerge() and jmerge() from AggregateNode, updated AggregateNode with key
 * names and comments on BodyAggregation, added error handling on null
 * event for QServlet, added TYPE_MAPREDUCE to Node, dropped JMSException
 * in the constructor for all Nodes, added comments on ListXPE to XPathNode,
 * added MapReduceNode, added try-catch block to protect Mongo operation in
 * MongoDBMessenger
 *<br/>
 * R.400 (2013-01-19): changed to sleep waitTime in a loop in case of disabled
 * operation for FileReceiver, JDBCReceiver and DocumentReceiver, added
 * checkpoint support to MonitorGroup, MessageFlow and DistinguishNode, removed
 * checkpointDir from MonitorAgent and QFlow, cleaned up the checkpoint
 * IO in SpreadNode, changed to use DateFormat instead of SimpleDateFormat in
 * a bunch of classes, added the support to check name space of XML in
 * SelectNode
 *<br/>
 * R.389 (2013-01-19): fixed the issue with missing uri in retrieve() for
 * HTTPMessenger, fixed the issue of missing header as response for HEAD and
 * DELETE in HTTPConnector, added ETag support and the missing Request part
 * in URLMonitor, added isHEAD to WebTester, added decent support to timestamp
 * to all KeyChains, updated SortNode due to API changes of KeyChain, added
 * support on versions to DistinguishNode, added new method of touch() with
 * ref time to QuickCache, CachedList and GroupedCache, changed to use new
 * touch() with ref time in AggregateNode, added getParentNameSpaces() to Utils
 *<br/>
 * R.380 (2013-01-12): fixed the support issues of HTTP 1.1 on HTTPConnector,
 * added detail path to logging in HTTPMessenger, added new methods of
 * updateMulti() and getCommandID() to MongoDBConnector, added list() and
 * replaced insert() with update() on MongoDBMessenger, added the support of
 * other methods, such as remove and insert, to update(), updated
 * DocumentPersister due to API changes of MongoDBMessenger
 *<br/>
 * R.373 (2013-01-07): added support of new types of parameters for Date, Time
 * and Timestamp to SQLUtils, added dateFormat to its setParameter(), changed
 * dateFormat to instance scope due to its MT-safety issue in Msg2Text,
 * TimeWindows, EventSyslogger and EventLogger, replaced deprecated object of
 * Category with Logger in EventLogger, added synchronized wrapper to parse()
 * and format() for MT-safety in Event, added dateFormat to setParameters()
 * in JDBCMessenger and made cache-only for those statements without parameters
 *<br/>
 * R.365 (2012-12-06): added support for Riak to NodeUtils, added the part to
 * jump out of the retry loop in FTPMessenger and SFTPMessenger
 *<br/>
 * R.362 (2012-12-03): added RiakConnector, RiakMessenger and NonSQLException,
 * added Riak support to DocumentPersister and DocumentReceiver, added
 * jumping out of retry loop part to JDBCMessenger and MongoDBMessenger,
 * changed to use Map instead of HashMap for constructors in RedisConnector,
 * MongoDBConnector, RMQConnector and QMFConnector
 *<br/>
 * R.353 (2012-11-29): added support for ConfigTemplate on rules to Node,
 * PublishNode and EventDispatcher, updated MessageFlow for the rename of
 * the key for the property template
 *<br/>
 * R.348 (2012-11-28): renamed MonitorTemplate to ConfigTemplate and added
 * isPropertyChanged() to it, changed it to use the fixed name of "Property"
 * as the key to store the property map, updated MonitorGroup for the change,
 * added a check on missing levels to JSON2Hashmap and JSON2FmModel, changed
 * getHeader() to static in HTTPConnector, added HeaderProperty map to
 * download() in HTTPMessenger
 *<br/>
 * R.339 (2012-10-28): fixed the issue with TopicPattern on PublishNode and
 * changed it to publish copies of messages with pubURI set only 
 *<br/>
 * R.334 (2012-10-16): added logging of version to QFlow and
 * MonitorAgent at the startup, fixed a typo in JMS:QConnector, added a missing
 * drop on ReplyTo for non-JMSEvent in MessageUtils, added support on ReplyTo
 * in QClient
 *<br/>
 * R.330 (2012-10-15): added QMF support with QMFRequester, QMFMessenger and
 * QMFQMonitor, added QMF support to JMXPersister and NodeUtils
 *<br/>
 * R.328 (2012-10-15): added createQueue() to QueueConnector and TopicConnector,
 * added resetOption and resetReplyTo to get(), browse() and sub(), moved
 * convert() from JMXMessenger to MessageUtils, removed the filter on JMSReplyTo
 * in MessageUtils, renamed DelimiteredBuffer to DelimitedBuffer in
 * wmq:TConnector
 *<br/>
 * R.324 (2012-10-15): added the missing init on deliveryMode to Event and
 * JMSEvent, added URI separator to ReceiverPool and PersisterPool in case
 * the original URI contains '?', renamed DelimiteredBuffer to DelimitedBuffer
 * to fix the typo, added support of JMSReplyTo to QClient, added comment
 * on delimited attributes to JMXRequester and IMQRequester
 *<br/>
 * R.317 (2012-09-29): added a filter in duplicate operation on JMSReplyQ in
 * MessageUtils, added stack trace on Error in MessageFlow, fixed a typo in
 * disfragment() of DistinguishNode that causes recursive calling on itself
 *<br/>
 * R.313 (2012-08-24): added persistent connection to HTTPConnector,
 * removed char encoding part for BytesMessages on HTTPMessenger, FTPMessenger,
 * MessageLogger, SFTPMessenger and RMQMessenger, removed status = RETRYING
 * from StreamPersister and StreamReceiver, moved global var supports from
 * connectors to Persister and Receiver, fixed a bug in HashChain init part
 * in SwitchNode and DispatchNode, fixed a bug of migration of 0 rules in
 * DispatchNode, added doPut() to QServlet, etc
 *<br/>
 * R.283 (2012-06-22): fixed a typo in QList
 *<br/>
 * R.281 (2012-06-11): added sequence number to IndexedXQueue and QList to
 * fix the ordering issue, changed their browser implementations with
 * sequence number support
 *<br/>
 * R.277 (2012-06-06): added JAAS support in ServerReceiver, ServerListener and
 * ServerPersister, added RptLoginModule, changed the default logging level
 * from Debug to Info on Event
 *<br/>
 * R.265 (2012-05-28): merged changes in recent 6 months, added a bunch of
 * noew nodes, etc
 *<br/>
 * R.233 (2011-11-05): fixed the duplicated stats issue with JMSMonitor,
 * added PHP2HashMap to parse serialized PHP data, moved split() from
 * XML2HashMap to Utils, added containsKey() and get() to XML2HashMap and
 * JSON2HashMap
 *<br/>
 * R.226 (2011-10-23): renamed jms:Connectors to Messengers due to name
 * conflicts, updated persisters and receivers for the new names
 *<br/>
 * R.220 (2011-10-23): rolled back the change on '/'  so that it is escaped
 * again in Utils:escapeJSON(), added dynamic MonitorTemplate support in
 * MonitorGroup and MonitorAgent, added most of HTTP1.1 support to
 * HTTPConnector, changed restore part in DBRecord
 *<br/>
 * R.212 (2011-10-22): added MongoDBConnector, DocumentPersister and
 * DocumentReceiver, added compareProperties() to Utils, removed '/' from
 * escapeJSON() in Utils, fixed indent issue with XML2HashMap,
 * added JSON2HashMap, added MonitorTemplate, changed to use Utils.RESULT
 * in SQLUtils and DBConnector, changed enum into enumeration in PCFRequester,
 * fixed a bug in SelectNode on its number of rules
 *<br/>
 * R.196 (2011-10-11): reset keyList to null and fixed statusText in
 * Monitor and Report, removed unused imports in monitors
 *<br/>
 * R.189 (2011-10-11): added some basic actions to FTPConnector and
 * SFTPConnector, added SFTP support to FileSynchronizer and JobPersister
 *<br/>
 * R.185 (2011-10-09): added support for sftp and https for all monitors,
 * changed to disable at startup for ChannelMonitor and JMSLogMonitor,
 * changed to disable at startup regardless if it is dependency or not for
 * ExpectedLog
 *<br/>
 * R.180 (2011-10-08): added SFTPConnectors and the support for SFTP, 
 * added UnicastNode and fixed some bugs in PacketNode and SerialPortNode
 *<br/>
 * R.173 (2011-10-08): fixed a bug in NumberMonitor on previousLevel, 
 * added to disable at startup if no log on ExpectedLog, changed dependency
 * behavior on reference for all mtime monitors
 *<br/>
 * R.165 (2011-09-26): added missing reset on updateCount in AgeMonitor and
 * other mtime monitors
 *<br/>
 * R.163 (2011-07-28): removed isXA from IndexedXQueue so that it always calls
 * notifyAll(), replaced currentTime with waitTime in calling
 * NodeUtils.passthru() in bunch of MessageNode implementations
 *<br/>
 * R.153 (2011-06-27): added ftpGet(OutputStream, String), getReplyCode() and
 * getReplyString() to net:FTPConnector for downloading files to a stream,
 * added pickup() and the support of abort to jms:FTPConnector and changed its
 * copy() to use ftpGet() instead of ftpPut(), added support on abort to
 * JobNode, added JOB status array to track jobs in JobNode,
 *<br/>
 * R.142 (2011-06-23): added listPendings() and (s|g)etDisplayProperties() to
 * MessageNode and Node, added the implementation of listPendings to PipeNode,
 * ScreenNode, SortNode, MonitorNode and DistinguishNode, added JobNode and
 * JobPersister, added copy() and upload() to FTPConnector, added some basic
 * methods to net:FTPConnector for streams, moved listXQ() from Node to
 * MessageUtils.
 *<br/>
 * R.105 (2011-06-03): added support for ConfigRepository and
 * IncludePolicy in QServlet, changed to use adminServer to determine
 * if to build collectable for internal responses in QFlow and
 * MonitorAgent, added support of EventPostable in HTTPConnector#download(),
 * added protection in case priority is not defined in EventUtils.
 *<br/>
 * R.101 (2011-05-27): added support for REST and XML requests in
 * QServlet, changed to JMSEvent in QFlow#doRequest().
 *<br/>
 * R.96 (2011-04-21): added XMerge to XSLTNode and MessageEvaluator,
 * added support to STD/MAX/MIN on Aggregation 
 *<br/>
 * R.84 (2011-04-14): added REPORT_FLOW, added folder watch on
 * FileTester, added ResetOption on CollectNode, fixed a typo on PipeNode,
 * replaced SplitNode by SelectNode, fixed reset counter on MonitorAgent
 *<br/>
 * R.68 (2011-03-07): added RESET_MAP for MapMessage format, added ActionDelay
 * to ActionNode, added resetMapBody() in MessageUtils, added a case to store
 * output into ScriptField in case the message is not Text nor Bytes for
 * FilePersister, fixed a bug in reloading generated items for MonitorGroup,
 * fixed a bug in retry count of runaway threads for MonitorAgent
 *<br/>
 * R.59 (2011-03-05): added DisplayMask to rules in MessageEvaluator, added
 * TemplateFile to FormatNode
 *<br/>
 * R.56 (2011-03-03): removed msg.reset() for BytesMessage and added reset() to
 * processBody() in MessageUtils
 *<br/>
 * R.50 (2011-02-19): added TouchEnabled in AggregateNode, added support on
 * ReturnCode for non-MQ reports in DispatchNode, fixed display of body in
 * browse() of JMSQConnector, added missing SHOW_FAMILAY to MessageUtils,
 * added IncrementalMonitor
 *<br/><br/>
 * QBroker project has been migrated from CVS to SVN in the end of 2010. So
 * all the change hsitrory got lost since I do not have idea how to keep them.
 * Since the migration, I have kept tracking changes on the project here.
 *<br/>
 * @author yannanlu@yahoo.com
 */
public class Version {
    private static String VERSION = null;
    private static String BUILD_ID =
        "$Id: Version.java 937 2014-11-21 21:38:48Z ylu $";

    public Version() {
    }

    public static String getVersion() {
        if (VERSION == null) {
            int j, i = BUILD_ID.indexOf(".java ");
            if (i > 0) {
                j = BUILD_ID.indexOf(" ", i+6); 
                VERSION = BUILD_ID.substring(i+6, j);
            }
        }
        return VERSION;
    }

    public static String getBuildID() {
        return BUILD_ID;
    }
}
