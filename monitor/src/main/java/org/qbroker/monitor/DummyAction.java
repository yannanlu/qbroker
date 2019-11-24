package org.qbroker.monitor;

/* DummyAction.java - a dummy MonitorAction */

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Date;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Template;
import org.qbroker.event.EventActionGroup;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.MonitorAction;

/**
 * DummyAction is a dummy action only.  The actions will be invoked on the
 * event only if the status is OCCURRED.  However, the event will be sent out
 * up to tolerance times with QuietPeriod support.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class DummyAction implements MonitorAction {
    protected String name;
    protected String site;
    protected String type;
    protected String description;
    protected String category;
    protected String uri = null, msg = null;
    protected int previousStatus;
    protected int tolerance, repeatPeriod, actionCount;
    protected EventActionGroup actionGroup;
    protected final static String statusText[] = {"Exception",
        "Exception in blackout", "Disabled", "Blackout", "Normal",
        "Updated", "Late", "Very late", "Extremely late", "Expired"};

    public DummyAction(Map props) {
        Object o;
        Template template;
        int n;

        if ((o = props.get("Name")) == null)
            throw(new IllegalArgumentException("Name is not defined"));

        name = (String) o;
        site = MonitorUtils.select(props.get("Site"));
        category = MonitorUtils.select(props.get("Category"));
        if ((o = props.get("Type")) != null)
            type = (String) o;
        else
            type = "DummyAction";

        template = new Template("##hostname## ##HOSTNAME## ##host## ##HOST##");

        if ((o = props.get("Description")) != null)
            description = MonitorUtils.substitute((String) o, template);
        else
            description = "dummy action";

        if ((o = MonitorUtils.select(props.get("URI"))) != null)
            uri = MonitorUtils.substitute((String) o, template);

        if ((o = props.get("ActionGroup")) != null && o instanceof List &&
            ((List) o).size() > 0) {
            actionGroup = new EventActionGroup(props);
            if ((o = props.get("Message")) != null)
            msg = (String) o;
        }
        else
            actionGroup = null;

        if ((o = props.get("Tolerance")) == null ||
            (tolerance = Integer.parseInt((String) o)) <= 0)
            tolerance = 1;

        if ((o = props.get("QuietPeriod")) == null ||
            (n = Integer.parseInt((String) o)) < 0)
            n = 0;
        repeatPeriod = tolerance + n;

        previousStatus = TimeWindows.EXCEPTION;
        actionCount = 0;
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {
        String actionStatus;
        if (actionGroup == null || status != TimeWindows.OCCURRED) {//not active
            previousStatus = status;
            return null;
        }

        Event event = new Event(Event.INFO, msg);
        event.setAttribute("name", name);
        event.setAttribute("site", site);
        event.setAttribute("category", category);
        event.setAttribute("type", type);
        event.setAttribute("description", description);
        if (uri != null && uri.length() > 0)
            event.setAttribute("uri", uri);
        event.setAttribute("status", statusText[status-TimeWindows.EXCEPTION]);
        event.setAttribute("testTime", Event.dateFormat(new Date(currentTime)));

        if (actionGroup.getNumberScripts() > 0) {
            if (status != TimeWindows.EXCEPTION &&
                status != TimeWindows.BLACKEXCEPTION) {
                if (actionGroup.isActive(currentTime, event))
                    actionStatus = "executed";
                else
                    actionStatus = "skipped";
            }
            else
                actionStatus = "skipped";
        }
        else
            actionStatus = "executed";

        event.setAttribute("actionScript", actionStatus);
        if (status != previousStatus)
            actionCount = 1;
        else
            actionCount ++;
        previousStatus = status;

        int count = actionCount;
        if (repeatPeriod >= tolerance && repeatPeriod > 0)
            count = (count - 1) % repeatPeriod + 1;
        if (count <= tolerance)
            event.send();

        if ("skipped".equals(actionStatus)) {
            actionGroup.disableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if ("executed".equals(actionStatus)) {
            actionGroup.enableActionScript();
            actionGroup.invokeAction(currentTime, event);
        }
        else if (actionGroup != null) {
            actionGroup.invokeAction(currentTime, event);
        }

        return null;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        chkpt.put("Name", name);
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long ct;
        int pStatus;
        if (chkpt == null || chkpt.size() == 0)
            return;
    }
}
