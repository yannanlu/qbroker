package org.qbroker.common;

/* XQCallbackWrapper.java - a Callback Wrapper for XQueue callbacks */

import java.lang.reflect.Method;

/**
 * XQCallbackWrapper is a callback wrapper for XQueue callbacks. Its public
 * method of callback() takes two Strings as the key and the id for callbacks.
 * It returns null upon success or an instance of Exception otherwise.
 * The callback object and the method should be set at construction time.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class XQCallbackWrapper {
    private String name;
    private Object obj = null;
    private Method method = null;

    public XQCallbackWrapper(String name, Object obj, Method method) {
        this.name = name;
        this.obj = obj;
        this.method = method;
    }

    public Exception callback(String key, String id) {
        if (obj != null && method != null) try {
            method.invoke(obj, new Object[] {key, id});
        }
        catch (Exception e) {
            return e;
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void clear() {
        obj = null;
        method = null;
    }

    protected void finalize() {
        clear();
    }
}
