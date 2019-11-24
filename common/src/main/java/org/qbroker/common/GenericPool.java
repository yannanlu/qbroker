package org.qbroker.common;

/* GenericPool.java - a GenericPool to store multiple instance of the object */

import java.util.HashMap;
import java.util.Map;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * GenericPool is a container to store multiple instances of the same object.
 * The number of active instances ranges from the minimum to its capacity.
 * The application is able to checkout an active instance from the pool.
 * Once the application is done with the instance, it is supposed to check it
 * back in the pool so that others may use it sooner or later.  If all the
 * active instances are checked out, GenericPool will create a new instance
 * as long as the pool is not full.  GenericPool will always try to maintain
 * the minmum number of active instances.  Therefore, in case there are more
 * active instances than the minimum required number, any instance idled for
 * too long may get closed to save resources.
 *<br><br>
 * GenericPool supports any concrete class or interface, as long as it is not
 * any Java primary types.  The argument of obj can be a String for
 * the full Java classname or a Class of the object.  Otherwise, GenericPool
 * will treat the object as a factory generator for an interface object.
 * In this case, you have to specify the name of the method to destroy
 * the object and the name of the method to generate the object.  The argument
 * of initArgs is used to create the new instnances.  It must not contain any
 * nulls. In case there is a conflict with the class name and the argument,
 * please specify the class name for the pTypes.
 *<br><br>
 * In case of a concrete class, it is up to the object itself to decide
 * how to close an instance.  If there is an instance method of close() or
 * destroy(), GenericPool will invoke it to close the active instance.
 * Otherwise, GenericPool simply resets its status and timestamp, as well as
 * sets the instance object to null.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class GenericPool {
    private IndexedXQueue pool;
    private Object[] initArgs;
    private Object object;
    private java.lang.reflect.Method closer, creater;
    private java.lang.reflect.Constructor con;
    private int capacity, minSize;
    private long waitTime;
    private int[] status;
    private long[] timestamp;
    private Map<Object, int[]> idMap;
    public static final int POOL_OK = 0;
    public static final int POOL_CHECKOUT = 1;
    public static final int POOL_INVALID = 2;
    public static final int POOL_CLOSED = 3;

    @SuppressWarnings("unchecked")
    public GenericPool(String name, int minSize, int capacity, Object obj,
        String closerName, String createrName, Object[] initArgs, Class[] pt) {
        Object o;
        String className = null;
        Class cls = null;
        Class[] pTypes = null;
        int n;

        if (capacity < 1)
            throw(new IllegalArgumentException("illegal Capacity:" + capacity));
        if (minSize < 0 || minSize > capacity)
            throw(new IllegalArgumentException("illegal MinSize:" + minSize));
        this.capacity = capacity;
        this.minSize = minSize;

        object = null;
        if (obj == null) {
            throw(new IllegalArgumentException(name + ": Object is null"));
        }
        else if (obj instanceof String) {
            className = (String) obj;
            try {
                cls = Class.forName(className);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +": failed to get " +
                    "Class for "+ className + ": "+ Utils.traceStack(e)));
            }
            catch (Error e) {
                Utils.flush(e);
            }
        }
        else if (obj instanceof Class) {
            cls = (Class) obj;
            className = cls.getName();
        }
        else if (createrName != null && createrName.length() > 0) {
            cls = obj.getClass();
            className = cls.getName();
            object = obj;
        }
        else {
            throw(new IllegalArgumentException(name +
                ": FactoryName not defined"));
        }

        if (initArgs == null)
            throw(new IllegalArgumentException("null initArgs"));

        n = initArgs.length;
        this.initArgs = new Object[n];
        for (int i=0; i<n; i++) {
            if (initArgs[i] == null)
                throw(new IllegalArgumentException("null initArgs[" + i +"]"));
            this.initArgs[i] = initArgs[i];
        }

        if (pt != null && pt.length == n)
            pTypes = pt;
        else if (pt != null)
            throw(new IllegalArgumentException(name +": wrong number of " +
                "pTypes for "+ className + ": "+ pt.length + "/" + n));
        else try {
            pTypes = new Class[n];
            for (int i=0; i<n; i++)
                pTypes[i] = initArgs[i].getClass();
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +": failed to get " +
                "pTypes for "+ className + ": "+ Utils.traceStack(e)));
        }

        if (object != null) { // for interface
            con = null;
            try {
                creater = cls.getMethod(createrName, pTypes);
                cls = creater.getClass();
                className = cls.getName();
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +": failed to get " +
                    " the creater of "+createrName+": "+ Utils.traceStack(e)));
            }

            closer = null;
            try {
                closer = cls.getMethod("closerName",
                    new Class[]{Class.forName("java.lang.Object")});
            }
            catch (Exception e) {
            }
        }
        else { // for class
            creater = null;
            try {
                con = cls.getConstructor(pTypes);
            }
            catch (Exception e) {
                throw(new IllegalArgumentException(name +": failed to get " +
                    "constructor for "+ className + ": "+ Utils.traceStack(e)));
            }

            closer = null;
            try {
                closer = cls.getMethod("close", new Class[0]);
            }
            catch (Exception e) {
            }
            if (closer == null) try {
                closer = cls.getMethod("destroy", new Class[0]);
            }
            catch (Exception e) {
            }
        }

        waitTime = 50L;
        pool = new IndexedXQueue(name, capacity);

        status = new int[capacity];
        timestamp = new long[capacity];
        idMap = new HashMap<Object, int[]>();

        for (int i=0; i<capacity; i++) {
            status[i] = POOL_CLOSED;
            timestamp[i] = System.currentTimeMillis();
            if (i < minSize)
                create(initArgs);
        }
    }

    public GenericPool(String name, int minSize, int capacity, Object obj,
        String closerName, String createrName, Object[] initArgs) {
        this(name, minSize, capacity, obj, closerName, createrName, initArgs,
            null);
    }

    public GenericPool(String name, int minSize, int capacity, Object obj,
        Object[] initArgs) {
        this(name, minSize, capacity, obj, null, null, initArgs, null);
    }

    /**
     * It checks out an active instance of the object.  If there is no
     * active instance available and there is no room for any new instance,
     * it simply returns null to indicate no instance available.  If the pool 
     * is not full yet, it will try to create a new instance.  If you want
     * to create a customized instance, you have to set minSize to ZERO and
     * specify your customized initial arguments, args.  Zero minSize will
     * guarentee no instance gets recycled or reused.  If minSize is none zero
     * or args is null, the pool will use the default initArgs to create
     * new instances.  If the creation is sucessful, it returns the instance.
     * Otherwise, the exception is returned to indicate failure.  Therefore,
     * applications are supposed to check the object before any use.
     *<br><br>
     * Due to MT features, you have to provide milliSec as the waitinmg time.
     */
    public Object checkout(long milliSec, Object[] args) {
        int id = pool.getNextCell(milliSec);
        if (id >= 0) {
            status[id] = POOL_CHECKOUT;
            timestamp[id] = System.currentTimeMillis();
            return pool.browse(id);
        }
        else if (pool.size() < capacity) {
            Object obj = (minSize > 0) ? create(null) : create(args);
            if (obj instanceof Exception)
                return obj;
            id = pool.getNextCell(milliSec);
            if (id >= 0) {
                status[id] = POOL_CHECKOUT;
                timestamp[id] = System.currentTimeMillis();
                return pool.browse(id);
            }
        }
        return null;
    }

    /**
     * returns an active instance of the object with the default args.
     */
    public Object checkout(long milliSec) {
        return checkout(milliSec, null);
    }

    /**
     * It checks in the used instance of the object.  If the instance is not
     * in working state, you have to specify the proper status to notify
     * the pool.  The pool will decide if to recycle the object or destroy it.
     */
    public void checkin(Object obj, int s) {
        int i;
        i = getId(obj);
        if (i >= 0 && status[i] == POOL_CHECKOUT) { // found the id
            if (pool.size() > minSize) { // destroy it
                destroy(i);
                pool.remove(i);
            }
            else if (s != POOL_OK) { // recreate the object
                Object o;
                destroy(i);
                pool.remove(i);
                o = create(initArgs);
                if (o instanceof Exception)
                    GenericLogger.log(Event.WARNING, pool.getName() +
                        ": failed to create the instance: " +
                        Utils.traceStack((Exception) o));
            }
            else { // recycle the object
                status[i] = POOL_OK;
                timestamp[i] = System.currentTimeMillis();
                pool.putback(i);
            }
        }
    }

    public String getName() {
        return pool.getName();
    }

    /** returns capacity of the pool */
    public int getCapacity() {
        return pool.getCapacity();
    }

    /** returns number of active instances regardless it is used or not */
    public int getSize() {
        return pool.size();
    }

    /** returns number of active instances not in use */
    public int getCount() {
        return pool.depth();
    }

    /** returns the mask of internal XQueue */
    public int getStatus() {
        return pool.getGlobalMask();
    }

    /** returns the length of the default initArgs */
    public int getNumberOfArgs() {
        return initArgs.length;
    }

    /**
     * It returns the i-th default init argument or null if out of range.
     * It is for read-only purpose.  Therefore, it is not MT-safe.
     */
    public Object getInitArg(int i) {
        if (i >= 0 && i < initArgs.length)
            return initArgs[i];
        else
            return null;
    }

    /** returns the ID of the object or -1 if it has no such object */
    public synchronized int getId(Object obj) {
        if (obj != null && idMap.containsKey(obj))
            return idMap.get(obj)[0];
        else
            return -1;
    }

    public void close() {
        int i;
        if (pool != null) {
            stopRunning(pool);
            for (i=0; i<capacity; i++) {
                if (status[i] == POOL_CLOSED)
                    continue;
                status[i] = POOL_CLOSED;
                destroy(i);
            }
            pool.clear();
        }
        object = null;
        closer = null;
        creater = null;
        con = null;
        for (i=0; i<capacity; i++) {
            status[i] = POOL_CLOSED;
            timestamp[i] = System.currentTimeMillis();
        }
        if (initArgs != null)
            for (i=0; i<initArgs.length; i++)
                initArgs[i] = null;
    }

    protected void finalize() {
        close();
    }

    protected synchronized Object create(Object[] initArgs) {
        int id;
        Object obj = null;

        if (initArgs == null)
            initArgs = this.initArgs;

        if ((id = pool.reserve(waitTime)) >= 0) {
            status[id] = POOL_INVALID;
            timestamp[id] = System.currentTimeMillis();
            if (creater != null) {
                if (object != null) try {
                    obj = creater.invoke(object, initArgs);
                }
                catch (Exception e) {
                    pool.cancel(id);
                    return e;
                }
            }
            else {
                try {
                    obj = con.newInstance(initArgs);
                }
                catch (Exception e) {
                    pool.cancel(id);
                    return e;
                }
            }
            if (obj == null) {
                pool.cancel(id);
            }
            else {
                pool.add(obj, id);
                status[id] = POOL_OK;
                timestamp[id] = System.currentTimeMillis();
                idMap.put(obj, new int[]{id});
            }
        }
        return obj;
    }

    protected synchronized void destroy(int id) {
        if (id < 0 || id >= capacity)
            return;
        if (status[id] == POOL_CLOSED)
            return;
        Object obj = pool.browse(id);
        idMap.remove(obj);
        if (closer != null) {
            if (creater != null) {
                if (obj != null && object != null) try {
                    closer.invoke(obj, new Object[]{obj});
                }
                catch (Exception e) {
                    GenericLogger.log(Event.WARNING, pool.getName() +
                        ": failed to close the instance: " +
                        Utils.traceStack(e));
                }
            }
            else if (obj != null) try {
                closer.invoke(obj, new Object[0]);
            }
            catch (Exception e) {
                GenericLogger.log(Event.WARNING, pool.getName() +
                    ": failed to close the instance: " +
                    Utils.traceStack(e));
            }
        }

        status[id] = POOL_CLOSED;
        timestamp[id] = System.currentTimeMillis();
    }

    private void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }
}
