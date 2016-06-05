package org.qbroker.common;

/** GenericNotificationListener.java - a generic JMX Notification listener */

import javax.management.NotificationListener;
import javax.management.Notification;

/**
 * GenericNotificationListener is a JMX notification listener for generic purpose.
 * It takes the full classname and the name of the method to handle the
 * notifications for the constructor. It assumes that the method to hanle the
 * notifications is void method with the notification as the only one argument.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class GenericNotificationListener implements NotificationListener {
    private String methodName = null;
    private static java.lang.reflect.Method method = null;

    public GenericNotificationListener(String className, String methodName) {
        if (className == null || className.length() <= 0)
            throw(new IllegalArgumentException("Empty className for " +
                methodName));
        if (methodName == null || methodName.length() <= 0)
            throw(new IllegalArgumentException("Empty methodName for " +
                className));
        try {
            Class<?> cls = Class.forName(className);
            method = cls.getMethod(methodName, new Class[]{Notification.class});
        }
        catch(Exception e) {
            throw(new IllegalArgumentException("Failed to init method " +
                methodName + ": " + e.toString()));
        }
        this.methodName = methodName;
    }

    public void handleNotification(Notification notification, Object handback) {
        try {
            method.invoke(handback, notification);
        }
        catch(Exception e) {
            throw(new RuntimeException("Failed to invoke method " + methodName +
                ": " + e.toString()));
        }
    }
}
