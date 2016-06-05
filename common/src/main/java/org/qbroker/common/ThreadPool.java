package org.qbroker.common;

/* ThreadPool.java - a ThreadPool to manage multiple threads of the object */

import java.util.HashMap;
import java.util.Map;
import org.qbroker.common.XQueue;
import org.qbroker.common.IndexedXQueue;
import org.qbroker.common.GenericLogger;
import org.qbroker.common.Event;
import org.qbroker.common.Utils;

/**
 * ThreadPool is a container to maintain multiple threads of the same object.
 * The number of active threads ranges from the minimum to its capacity.
 * The application is able to checkout an active thread from the pool.
 * As long as the active thread is checked out, the application is able to
 * assign a task to the thread.  The task will be picked up by the checed-out 
 * thread sooner or later.  Once the thread picks up the task, it will invoke
 * the given method with the task as the only argument. At this time, the thread
 * stays busy until the method exits. The thread will be checked in the pool.
 * The application has control over the number of active threads by checking
 * out or checking in.
 *<br/><br/>
 * Once a task is completed by a thread, the thread will automatically
 * check itself in.  The pool will decide the fate of the thread.  If the
 * thread is to be removed from the pool, the thread will stop and exit.
 * Otherwise, it will be idle, waiting for the next checkout.  If all
 * the threads are active, ThreadPool will create a new thread as long as
 * the pool is not full.  ThreadPool will always try to maintain the minmum
 * number of running threads.  Therefore, in case there are more running
 * threads than the minimum required number, any newly checked-in thread
 * may get stopped to save resources.
 *<br/><br/>
 * ThreadPool supports any method, as long as it is MT-Safe. The argument
 * of the method has to be an Array of Class.  It will be used to locate
 * the method on the object.  Therefore, you have to specify the name of
 * the method, its pTypes array and the object itself in order to create
 * a ThreadPool.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ThreadPool implements Runnable {
    private IndexedXQueue pool; // for track threads
    private IndexedXQueue root; // for tracking tasks
    private Object object;
    private java.lang.reflect.Method method;
    private Map<Thread, int[]> idMap;
    private String name;
    private int capacity, minSize, length;
    private long waitTime;
    public static final int POOL_OK = 0;
    public static final int POOL_CHECKOUT = 1;
    public static final int POOL_INVALID = 2;
    public static final int POOL_CLOSED = 3;

    @SuppressWarnings("unchecked")
    public ThreadPool(String name, int minSize, int capacity, Object obj,
        String methodName, Class[] pTypes) {
        Object o;
        String className = null;
        Class cls = null;

        if (capacity < 1)
            throw(new IllegalArgumentException("illegal Capacity:" + capacity));
        if (minSize < 0 || minSize > capacity)
            throw(new IllegalArgumentException("illegal MinSize:" + minSize));
        this.capacity = capacity;
        this.minSize = minSize;
        this.name = name;

        object = null;
        if (obj == null) {
            throw(new IllegalArgumentException(name + ": Object is null"));
        }
        else {
            cls = obj.getClass();
            className = cls.getName();
            object = obj;
        }

        if (pTypes == null)
            throw(new IllegalArgumentException("null pTypes"));

        length = pTypes.length;

        try {
            method = cls.getMethod(methodName, pTypes);
        }
        catch (Exception e) {
            throw(new IllegalArgumentException(name +": failed to find " +
                "the method of "+ methodName+ ": "+ Utils.traceStack(e)));
        }

        waitTime = 2000L;
        pool = new IndexedXQueue(name, capacity);
        root = new IndexedXQueue(name, capacity);

        idMap = new HashMap<Thread, int[]>();

        for (int i=0; i<minSize; i++) {
            create();
        }
    }

    /**
     * It checks out an idle thread of the object and activates it.
     * If there is no idle thread available and there is no room for any
     * new thread, it simply returns null to indicate no thread available.
     * If the pool is not full yet, it will try to create a new thread.
     * If the creation is sucessful, it returns the thread.  Otherwise,
     * null is returned.  Application is supposed to check the size if
     * it gets null.
     *<br/><br/>
     * Due to MT features, you have to provide milliSec as the waitinmg time.
     */
    public Thread checkout(long milliSec) {
        int id = pool.getNextCell(milliSec);
        if (id >= 0) {
            Thread thr = (Thread) pool.browse(id);
            if (thr == null) {
                pool.remove(id);
                return null;
            }
            if (id == root.reserve(50L, id)) {
                thr.interrupt();
                return thr;
            }
            pool.putback(id);
        }
        else if (pool.size() < capacity) {
            try {
                create();
            }
            catch (Exception e) {
                return null;
            }
            id = pool.getNextCell(milliSec);
            if (id >= 0) {
                Thread thr = (Thread) pool.browse(id);
                if (thr == null) {
                    pool.remove(id);
                    return null;
                }
                if (id == root.reserve(50L, id)) {
                    thr.interrupt();
                    return thr;
                }
                pool.putback(id);
            }
        }
        return null;
    }

    /**
     * It checks in an unused thread and returns the status code for it.
     * If it is recycled, POOL_OK will be returned.  Otherwise, POLL_CLOSED
     * is for removed, POOL_INVALID for recreated, and -1 for no such thread
     * in the pool.
     */
    public synchronized int checkin(Thread thr) {
        return checkin(thr, POOL_CHECKOUT);
    }

    /**
     * It checks in a thread and returns it to the pool based on its status.
     * The pool will decide if to recycle the thread or destroy it.
     * If it is recycled, POOL_OK will be returned.  Otherwise, POLL_CLOSED
     * is for removed, POOL_INVALID for recreated, and -1 for no such thread
     * in the pool.
     */
    protected synchronized int checkin(Thread thr, int s) {
        int id = getId(thr);
        if (id >= 0) {
            int status;
            if (s == POOL_CHECKOUT) // for user
                root.cancel(id);
            else // for thread itself
                root.remove(id);
            if (pool.size() > minSize) { // destroy it
                idMap.remove(thr);
                pool.remove(id);
                status = POOL_CLOSED;
            }
            else if (s != POOL_OK) { // recreate the object
                idMap.remove(thr);
                pool.remove(id);
                try {
                    create();
                }
                catch (Exception e) {
                }
                status = POOL_INVALID;
            }
            else { // recycle the object
                pool.putback(id);
                status = POOL_OK;
            }
            return status;
        }
        return -1;
    }

    /**
     * It assigns a task to the newly activated id-th thread.  A task is the
     * argument array for the method. Up on success, it returns the number of
     * outstanding tasks or -1 otherwise.
     */
    public int assign(Object[] task, int id) {
        if (task == null || task.length != length)
            return -3;
        if (id < 0 || id >= capacity)
            return -2;

        if (root.add(task, id) <= 0)
            return -1;
        else
            return root.size();
    }

    public void run() {
        Object[] args = null;
        Thread thr = Thread.currentThread();
        int cs = -1, cid = -1, id, count = 0;

        if ((id = getId(thr)) < 0) // destroyed
            return;

        GenericLogger.log(Event.INFO, name + ": thread " + id +" is activated");

        while ((pool.getGlobalMask() & XQueue.KEEP_RUNNING) > 0) {
            cs = pool.getCellStatus(id);
            if (cs == XQueue.CELL_OCCUPIED) {
                try {
                    Thread.sleep(waitTime);
                }
                catch (InterruptedException e) {
                }
                if (id != getId(thr)) { // destroyed
                    cs = -1;
                    break;
                 }
                continue;
            }
            else if (id != getId(thr)) { // destroyed
                cs = -1;
                break;
            }
            else if (cs != XQueue.CELL_TAKEN) // destroyed
                break;

            cid = -1;
            // wait for a task since the thread has been checked out
            do {
                cid = root.getNextCell(5*waitTime, id);
                if (cid >= 0) {
                    if (id != getId(thr)) { // destroyed
                        root.putback(id);
                        cid = 2;
                    }
                    // got the task
                    cid = 1;
                }
                else if (id != getId(thr)) // destroyed
                    cid = 2;
                else if (pool.getCellStatus(id) != XQueue.CELL_TAKEN)
                    cid = 0; // back to idle
            } while (cid < 0 && (pool.getGlobalMask() & XQueue.KEEP_RUNNING)>0);

            if (cid < 0 || cid > 1) // pool stopped or thread destroyed
                break;
            else if (cid == 0) // thread checked in
                continue;
            else if (cid == 1 && (args = (Object[]) root.browse(id)) != null) {
                GenericLogger.log(Event.INFO, name + ": thread " + id +
                    " is working on task " + id);

                try {
                    method.invoke(object, args);
                }
                catch (Exception e) {
                    GenericLogger.log(Event.ERR, name +
                        ": failed to invoke method: "+ Utils.traceStack(e));
                }
                catch (Error e) {
                    GenericLogger.log(Event.ERR, name +
                        ": failed to invoke method: "+ Utils.traceStack(e));
                    checkin(thr, POOL_INVALID);
                    Utils.flush(e);
                }
                count ++;
            }
            else { // cid == 1
                GenericLogger.log(Event.WARNING, name + ": null task for "+ id);
                count ++;
            }

            // checkin the thread
            if (POOL_OK != checkin(thr, POOL_OK))
                break;
            GenericLogger.log(Event.INFO,name+": thread "+id+" completed task "+
                id + " and gone back to idle");
        }
        if (cs != XQueue.CELL_TAKEN) // destroyed before checkout
            GenericLogger.log(Event.INFO, name + ": thread " + id +
                " destroyed with " + count +" tasks completed before checkout");
        else if (cid != 1) // destroyed after checkout
            GenericLogger.log(Event.INFO, name + ": thread " + id +
                " destroyed with " + count + " tasks completed after checkout");
        else // normal exit
            GenericLogger.log(Event.INFO, name + ": thread " + id +
                " completed " + count + " tasks and stopped");
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
    public int getDepth() {
        return pool.depth();
    }

    /** returns number of outstanding tasks */
    public int getCount() {
        return root.size();
    }

    public int getStatus(int id) {
        return pool.getCellStatus(id);
    }

    public int getStatus() {
        return pool.getGlobalMask();
    }

    /** return the ID of the object or -1 if it does not contain such object */
    public synchronized int getId(Thread thr) {
        if (thr != null && idMap.containsKey(thr))
            return idMap.get(thr)[0];
        else
            return -1;
    }

    public void close() {
        int i;
        if (pool != null) {
            stopRunning(pool);
            stopRunning(root);
            pool.clear();
            root.clear();
        }
        idMap.clear();
    }

    protected void finalize() {
        close();
    }

    protected synchronized Thread create() throws RuntimeException {
        int id;

        if ((id = pool.reserve(waitTime)) >= 0) {
            Thread thr = null;
            if (object != null) try {
                thr = new Thread(this, name + "_" + id);
                thr.setPriority(Thread.NORM_PRIORITY);
                thr.setDaemon(true);
            }
            catch (RuntimeException e) {
                thr = null;
                pool.cancel(id);
                throw(e);
            }
            if (thr != null) {
                pool.add(thr, id);
                idMap.put(thr, new int[]{id});
                try {
                    thr.start();
                }
                catch (RuntimeException e) {
                    idMap.remove(thr);
                    pool.takeback(id);
                    throw(e);
                }
                return thr;
            }
            else {
                pool.cancel(id);
            }
        }
        return null;
    }

    private void stopRunning(XQueue xq) {
        int mask = XQueue.KEEP_RUNNING | XQueue.PAUSE | XQueue.STANDBY;
        xq.setGlobalMask(xq.getGlobalMask() & (~mask));
    }
}
