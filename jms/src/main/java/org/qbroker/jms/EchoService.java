package org.qbroker.jms;

import javax.jms.Message;
import org.qbroker.common.Service;
import org.qbroker.event.Event;
import org.qbroker.event.EventLogger;
import org.qbroker.jms.MessageUtils;

/**
 * EchoService echos requests for test purpose.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EchoService implements Service {
    private String name = "EchoService";
    private String operation = "echo";
    private int status = SERVICE_READY;
    private int debug = DEBUG_NONE;

    public EchoService() {
    }

    public EchoService(String name) {
        this();

        if (name != null && name.length() > 0)
            this.name = name;
    }

    public int doRequest(org.qbroker.common.Event event, int timeout) {
        if (event == null)
            return -1;
        else if (!(event instanceof Message) && event instanceof Event) {
            String line = EventLogger.format((Event) event).toString();
            if (debug != DEBUG_NONE)
                new Event(Event.INFO, name + " echoed an event: "+ line).send();
        }
        else if (!(event instanceof Message)) { // neither an event nor a msg
            return -2;
        }
        else try { // for JMS msg
            byte[] buffer = new byte[4096];
            String line = MessageUtils.processBody((Message) event, buffer);
            line = MessageUtils.display((Message) event, line, 20551, null);
            if (debug != DEBUG_NONE)
                new Event(Event.INFO, name + " echoed a msg ("+line+")").send();
        }
        catch (Exception e) {
            return -3;
        }
        return 1;
    }

    public String getName() {
        return name;
    }

    public String getOperation() {
        return operation;
    }

    public int getStatus() {
        return status;
    }

    public int getDebugMode() {
        return debug;
    }

    public void setDebugMode(int debug) {
        this.debug = debug;
    }

    public void start() {
        status = SERVICE_RUNNING;
    }

    public void stop() {
        status = SERVICE_STOPPED;
    }

    public void close() {
        status = SERVICE_CLOSED;
    }
}
