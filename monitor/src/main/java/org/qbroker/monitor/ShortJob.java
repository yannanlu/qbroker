package org.qbroker.monitor;

/* ShortJob.java - a job scheduler */

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.IOException;
import org.qbroker.common.TimeWindows;
import org.qbroker.common.Utils;
import org.qbroker.json.JSON2Map;
import org.qbroker.event.Event;
import org.qbroker.monitor.MonitorUtils;
import org.qbroker.monitor.DummyAction;
import org.qbroker.monitor.ScriptLauncher;

/**
 * ShortJob runs a short-lived script or a java job upon changes of the
 * status of certain occurrence
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ShortJob extends DummyAction {
    private String script;
    private File checkpointFile;
    private MonitorAction job;
    private int scriptTimeout;
    private int checkpointExpiration;

    public ShortJob(Map props) {
        super(props);
        Map<String, Object> h;
        Object o;

        if (type == null)
            type = "ShortJob";
        if (description == null)
            description = "run a short-lived script or a java job";

        job = null;
        if ((o = props.get("ShortJob")) != null && o instanceof Map) {
            h = Utils.cloneProperties((Map) o);
            String className = (String) h.get("ClassName");
            if (h.get("Name") == null)
                h.put("Name", name);
            if (h.get("Site") == null)
                h.put("Site", site);
            if (h.get("Category") == null)
                h.put("Category", category);
            if (h.get("DependencyGroup") != null)
                h.remove("DependencyGroup");
            if (h.get("DisableMode") == null)
                h.put("DisableMode", "1");
            h.put("Step", "1");
            if (className != null) {
                job = MonitorUtils.getNewAction(h);
            }
            else if ((o = h.get("Type")) != null &&
                "ScriptLauncher".equals((String) o))
                job = new ScriptLauncher(h);
            else
                job = new ScriptLauncher(h);
        }

        if ((o = props.get("Script")) != null)
            script = MonitorUtils.select(o);

        if (script == null && job == null)
            throw(new IllegalArgumentException("Script or Job is not defined"));

        if ((o = props.get("ScriptTimeout")) == null ||
            (scriptTimeout = 1000*Integer.parseInt((String) o)) < 0)
            scriptTimeout = 60000;

        if ((o = props.get("CheckpointExpiration")) == null ||
            (checkpointExpiration = 1000*Integer.parseInt((String) o)) <= 0)
            checkpointExpiration = 480000;

        checkpointFile = null;
        if ((o = MonitorUtils.select(props.get("CheckpointFile"))) != null)
            checkpointFile = new File((String) o);

        if (checkpointFile != null && checkpointFile.exists() &&
            checkpointFile.canRead()) try {
            FileReader fr = new FileReader(checkpointFile);
            h = Utils.cloneProperties((Map) JSON2Map.parse(fr));
            fr.close();
            restoreFromCheckpoint(h);
            if (h != null)
                h.clear();
        }
        catch (Exception e) {
            new Event(Event.WARNING, "failed to load checkpoint for " +
                name + ": " + e.toString()).send();
        }
    }

    public Event performAction(int status, long currentTime,
        Map<String, Object> latest) {

        if (status == TimeWindows.OCCURRED &&
            status != previousStatus) {
            Event event;
            previousStatus = status;
            if (job != null) {
                Map<String, Object> r = null;
                int s = status;
                try {
                    r = ((MonitorReport) job).generateReport(currentTime);
                }
                catch (Exception e) {
                    r = new HashMap<String, Object>();
                    r.put("Exception", e);
                    s = TimeWindows.EXCEPTION;
                }
                event = job.performAction(s, currentTime, r);
            }
            else {
                event = MonitorUtils.runScript(script, scriptTimeout);
                event.setAttribute("name", name);
                event.setAttribute("site", site);
                event.setAttribute("category", category);
                event.setAttribute("type", "Script");
                event.send();
            }
            if (checkpointFile != null) try {
                PrintWriter out = new PrintWriter(
                    new FileOutputStream(checkpointFile));
                out.println(JSON2Map.toJSON(checkpoint(), "", "\n"));
                out.close();
            }
            catch (IOException e) {
                new Event(Event.WARNING, "checkpoint failed for "+name + ": " +
                    e.toString()).send();
            }
            return event;
        }
        else if (previousStatus == TimeWindows.OCCURRED &&
            status != previousStatus) {
            previousStatus = status;
            if (checkpointFile != null) try {
                PrintWriter out = new PrintWriter(
                    new FileOutputStream(checkpointFile));
                out.println(JSON2Map.toJSON(checkpoint(), "", "\n"));
                out.close();
            }
            catch (IOException e) {
                new Event(Event.WARNING, "checkpoint failed for " + name + ": "+
                    e.toString()).send();
            }
        }
        else
            previousStatus = status;

        return null;
    }

    public Map<String, Object> checkpoint() {
        Map<String, Object> chkpt = new HashMap<String, Object>();
        chkpt.put("Name", name);
        chkpt.put("CheckpointTime", String.valueOf(System.currentTimeMillis()));
        chkpt.put("PreviousStatus", String.valueOf(previousStatus));
        return chkpt;
    }

    public void restoreFromCheckpoint(Map<String, Object> chkpt) {
        Object o;
        long dt;
        int pStatus;
        if (chkpt == null || chkpt.size() == 0) // bad checkpoint
            return;
        if ((o = chkpt.get("Name")) == null || !name.equals((String) o))
            return;
        if ((o = chkpt.get("CheckpointTime")) != null) {
            dt = System.currentTimeMillis() - Long.parseLong((String) o);
            if (dt >= checkpointExpiration) // expired
                return;
        }
        else // bad checkpoint
            return;

        if ((o = chkpt.get("PreviousStatus")) != null)
            pStatus = Integer.parseInt((String) o);
        else
            return;

        // restore the parameters from the checkpoint
        previousStatus = pStatus;
    }
}
