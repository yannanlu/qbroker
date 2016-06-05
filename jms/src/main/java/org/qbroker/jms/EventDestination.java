package org.qbroker.jms;

import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;
import javax.jms.Destination;

/**
 * EventDestination stores JMS Destination for event
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class EventDestination implements Destination {
    private String description, uriString = null;
    private URI uri;

    public EventDestination(String uriString) throws JMSException {
        if (uriString == null || uriString.length() <= 0)
            throw(new JMSException("empty uri"));

        try {
            uri = new URI(uriString);
        }
        catch (URISyntaxException e) {
            throw(new JMSException(e.toString() + ": " + uriString));
        }
        this.uriString = uriString;
    }

    public EventDestination(URI uri) {
        this.uri = uri;
    }

    public void setDescription(String x) {
        description = x;
    }

    public String getDescription() {
        return description;
    }

    public String toString() {
        return uriString;
    }

    public String getScheme() {
        return uri.getScheme();
    }

    public String getPath() {
        return uri.getPath();
    }

    public String getQuery() {
        return uri.getQuery();
    }
}
