package org.qbroker.jms;

import java.util.Date;
import java.util.Map;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.text.SimpleDateFormat;
import org.qbroker.event.Event;
import org.qbroker.jms.MessageUtils;
import org.qbroker.common.Template;
import org.qbroker.common.Utils;

/**
 * UniformSplitter splits a JMS message into a plain text containing multiple
 * lines.  Currently, it only supports uniform spreadings of certain properties.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class UniformSplitter {
    private String name;
    private Template template = null;
    private SimpleDateFormat dateFormat;
    private String timeField, dataField, durationField;
    private long delta = 60000;
    private double scale = 1.0;

    public UniformSplitter(Map props) {
        Object o;

        if (props == null)
            throw(new IllegalArgumentException("property hash is null"));

        if ((o = props.get("Name")) != null)
            name = (String) o;
        else
            name = "Splitter";

        if ((o = props.get("Interval")) != null)
            delta = 1000 * Integer.parseInt((String) o);

        if ((o = props.get("Scale")) != null)
            scale = Double.parseDouble((String) o);

        if ((o = props.get("DurationField")) != null)
            durationField = (String) o;
        else
            throw(new IllegalArgumentException("DurationField not defined"));

        if ((o = props.get("DataField")) != null)
            dataField = (String) o;
        else
            throw(new IllegalArgumentException("DataField not defined"));

        if ((o = props.get("TimeField")) != null)
            timeField = (String) o;
        else
            throw(new IllegalArgumentException("TimeField not defined"));

        if ((o = props.get("TimePattern")) != null)
            dateFormat = new SimpleDateFormat((String) o);
        else
            throw(new IllegalArgumentException("TimePattern not defined"));

        if ((o = props.get("Template")) != null && ((String) o).length() > 0)
            template = new Template((String) o);
    }

    /**
     * returns a formatted string of the message with the given template
     * and a repeated template.  It is used for formatting messages into
     * either postable or collectible events.
     */
    public String split(Message msg) {
        double y0, y;
        long t, duration;
        int i, n;
        long[] data;
        String str;
        StringBuffer strBuf;
        if (msg == null)
            return null;
        try {
            str = MessageUtils.getProperty(durationField, msg);
            duration = (long) (Double.parseDouble(str) * scale);
            str = MessageUtils.getProperty(dataField, msg);
            y = Double.parseDouble(str);
            str = MessageUtils.getProperty(timeField, msg);
            t = dateFormat.parse(str).getTime();
        }
        catch (Exception e) {
            return null;
        }
        data = uniformSpread(t, duration, delta);
        if (data == null)
            return null;
        strBuf = new StringBuffer();
        n = data.length;
        str = dateFormat.format(new Date(data[0]));
        y /= duration;
        y0 = (data[0] + duration - t) * y / delta;
        strBuf.append(str + " " + data[0] + " " + y0 + "\n");
        for (i=1; i<n; ++i) {
            str = dateFormat.format(new Date(data[i]));
            strBuf.append(str + " " + data[i] + " " + y + "\n");
        }
        return strBuf.toString();
    }

    public static long[] uniformSpread(long x, long span, long delta) {
        int i, k, n;
        long xx;
        long[] data = null;
        if (delta <= 0 || span == 0)
            return null;

        if (delta >= Math.abs(span)) {
            data = new long[]{x};
        }
        else {
            n = (int) (Math.abs(span) / delta);
            if (Math.abs(span) > n * delta)
                n ++;
            k = (span > 0.0) ? 1 : -1;
            xx = x;
            data = new long[n];
            for (i=n-1; i>=0; i--) {
                data[i] = xx;
                xx -= k * delta;
            }
        }
        return data;
    }
}
