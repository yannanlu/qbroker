package org.qbroker.jms;

import java.util.Map;
import java.util.HashMap;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.QueueSender;
import javax.jms.QueueReceiver;
import org.apache.activemq.broker.BrokerService;
import org.qbroker.jms.TextEvent;
import static org.junit.Assert.*;
import org.junit.*;

/** Unit test for QConnector */
public class QConnectorTest {
    private static BrokerService broker = null;
    private static QConnector conn = null;
    private static String uri = "tcp://localhost:61616";

    @BeforeClass
    public static void init() throws Exception {
        Map<String, String> props = new HashMap<String, String>();
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.addConnector(uri);
        broker.start();
        props.put("Name", "test");
        props.put("URI", uri);
        props.put("ContextFactory",
            "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        props.put("ConnectionFactoryName", "QueueConnectionFactory");
        props.put("QueueName", "TestQueue");
        props.put("IsPhysical", "true");
        props.put("Operation", "put");
        props.put("XMode", "0");
        conn = new QConnector(props);
        QueueSender qSender = conn.getQueueSender();
        qSender.send(new TextEvent("this is a test"));
        props.clear();
    }

    @Test
    public void testGet() throws Exception {
        QueueReceiver qReceiver = conn.getQueueReceiver();
        Message msg = qReceiver.receive(1000L);
        assertTrue("Failed to get msg from " +
            qReceiver.getQueue().getQueueName(), (msg != null));
        assertTrue(msg instanceof TextMessage);
        assertEquals(4, msg.getJMSPriority());
        assertEquals("this is a test", ((TextMessage) msg).getText());
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) try {
            conn.close();
        }
        catch (Exception e) {
        }
        if (broker != null) try {
            broker.stop();
        }
        catch (Exception e) {
        }
    }
}
