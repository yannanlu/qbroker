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
    private Object object = null;
    private Method method = null;
    private boolean isUsable = false;

    public XQCallbackWrapper(String name, Object object, Method method) {
        if (name != null && name.length() > 0)
            this.name = name;
        else
            throw(new IllegalArgumentException("XQCallbackWrapper: " +
                "name is null"));
        if (object != null)
            this.object = object;
        else
            throw(new IllegalArgumentException(name + ": object is null"));
        if (method != null)
            this.method = method;
        else
            throw(new IllegalArgumentException(name + ": method is null"));
        isUsable = true;
    }

    public Exception callback(String key, String id) {
        if (isUsable) try {
            method.invoke(object, new Object[] {key, id});
            return null;
        }
        catch (Exception e) {
            return e;
        }
        else
            return new IllegalStateException(name + " is not usable any more");
    }

    public String getName() {
        return name;
    }

    public void clear() {
        object = null;
        method = null;
        isUsable = false;
    }

    protected void finalize() {
        clear();
    }
}
