package org.qbroker.common;

/* XQueue.java - An Interface of FIFO in-memory storage */

/**
 * XQueue is an Interface of First-In-First-Out in-memory storage with support
 * on both tracking and transactions.
 *<br><br>
 * XQueue contains a fixed number of cells with unique ids. Each cell can hold
 * one object.  A cell can be in one of four different states: EMPTY, RESERVED,
 * OCCUPIED and TAKEN.  Only an empty cell is able to be reserved.  Only
 * reserved cells can be occupied.  Once a reserved cell is occupied, the
 * object can be taken by an application to process.  If an object is taken,
 * the cell is in taken state, meaning the cell is not empty yet.  The object
 * may be put back to its original cell or be removed from the cell.  Once the
 * object is removed, the cell is empty again and it is available for a new
 * reservation.
 *<br><br>
 * Further more, the empty cells can be collected either implicitly or
 * explicitly. It means they are divided into two separate groups, collected
 * or collectible. If an empty cell has not been collected yet, it is a
 * collectible empty cell. Otherwise, it is a collected empty cell. The method
 * of collect() is to collect an empty cell explicitly. However, the method of
 * reserve() will implicitly collect the first collectible cell if there is no
 * collected empty cells available.
 *<br><br>
 * Similarly, each object in an XQueue can be in one of four states, too.  Once
 * an object is added to the queue or a cell is occupied by an object, it is
 * new and available to be taken. Once it is taken, the object is in-use.
 * Next state will be either done or new due to rollback.  Once the object is
 * removed from the cell, it is done. The next state is called ready which
 * means it is ready for recycle. Similarly, the done objects are divided into
 * two separate groups, collected or collectible.
 *<br><br>
 * Once the object is put into a cell, the association with the cell will
 * not change until the object is removed from the cell.  Therefore, the
 * unique cell IDs can be used to track or operate the objects in the queue.
 * The method reserve() is to have the empty cell ready to be occupied.  The
 * method cancel() cancels the reservation on the reserved cell and makes
 * it empty again.  The method add() is to have the reserved cell occupied.
 * Before to add, the cell has to be reserved.  The method getCellStatus() is
 * used to get the status of a specific cell.  The method getNextCell() returns
 * the ID of the next occupied cell and makes the cell taken so that the object
 * can be retrieved via browse() for processing.  The method remove() is to
 * remove the in-use object from the cell and makes the cell empty.  The method
 * putback() is to put an in-use object back to the cell and to have the cell
 * occupied again.  The method takeback() is to take a new object out of the
 * occupied cell and makes the cell empty again.  The method browser() gives
 * an instance of Browser for application to browse all the new objects. The
 * method collect() is to collect the empty cells. Please remember that the
 * method reserve() may collect those colletible empty cells implicitly. 
 *<br><br>
 * capacity -- maximum number of cells in the queue<br>
 * size -- number of the new and in-use objects in the queue<br>
 * depth -- number of new objects in the queue<br>
 * collectible -- number of empty cells that have not been collected yet<br>
 * status -- status of a cell or the object<br>
 * id -- the id of a cell or the object<br>
 *<br>
 * The implementation can focus on either the objects or on the cells.  If the
 * focus is on the cells, the implementation can assign an unique ID to each of
 * the cells for tracking support.  The tracking support allows application to
 * easily track the status of the objects via their cell IDs.
 *<br><br>
 * It is up to the implementation to support MT-Safty and XA.
 *<br>
 * @author yannanlu@yahoo.com
 */

public interface XQueue {
    /** returns the number of objects in the queue, new or in-use */
    public int size();

    /** returns the number of new objects in the queue */
    public int depth();

    /**
     * returns the number of collectible empty cells in the queue. In case
     * the implementation does not support collectible cells, make sure it
     * always returns 0.
     */
    public int collectible();

    /** returns the number of objects removed from the queue since the reset */
    public long getCount();

    /** returns the timestamp of the last reset on the queue */
    public long getMTime();

    /**
     * resets the counter to 0, updates the timestamp and returns the total
     * number of objects removed from the queue since the previous reset
     */
    public long reset();

    /** returns the name of the queue */
    public String getName();
    /** returns the capacity of the queue */
    public int getCapacity();
    /** returns the global mask of the queue */
    public int getGlobalMask();
    /** sets the global mask of the queue */
    public void setGlobalMask(int mask);

    /** returns true if any new objects in the queue or false otherwise */
    public boolean available();

    /** returns true if the queue is full or false otherwise */
    public boolean isFull();

    /** returns true if the queue supports collect() and collectible() */
    public boolean isCollectible();

    /** returns the status of the cell at the id */
    public int getCellStatus(int id);

    /**
     * collects the empty cell at the id and marks it as collected.
     * It returns the id upon success or -1 if the cell is not a collectible.
     */
    public int collect(long milliSec, int id);

    /**
     * collects the first collectible cell out of len cells starting from begin
     * and returns its id
     */
    public int collect(long milliSec, int begin, int len);

    /**
     * collects the next collectible cell and returns its id
     */
    public int collect(long milliSec);

    /**
     * reserves an empty cell and returns its id upon success or -1 otherwise.
     * In case of success, it sets the cell to RESERVED so that the cell
     * is available to be occupied. The operation is not supposed to change
     * the order of other empty cells. In case of the queue supporting
     * collecibles, it will collect the first collectible empty cell implicitly
     * if all empty cells have not been collected yet.
     */
    public int reserve(long milliSec);

    /**
     * reserves the empty cell at the id and returns the id upon success or -1
     * otherwise.  The operation is not supposed to change the order of other
     * empty cells. In case of the queue supporting collecibles, it will
     * collect that collectible empty cell implicitly if the cell has not been
     * collected yet.
     */
    public int reserve(long milliSec, int id);

    /**
     * reserves an empty cell in the range of len cells starting from begin
     * and returns the id of the cell upon success or -1 otherwise.  The
     * operation is not supposed to change the order of other empty cells.
     * In case of the queue supporting collecibles, it will collect the first
     * collectible empty cell implicitly if all empty cells in that range have
     * not been collected yet.
     */
    public int reserve(long milliSec, int begin, int len);

    /**
     * cancels the reservation on the cell at the id and returns the
     * watermark upon success or -1 otherwise.  In case of success, it sets
     * the cell to EMPTY so that it is available for reservation again.  The
     * operation is not supposed to change the order of other reserved cells.
     */
    public int cancel(int id);

    /**
     * adds a new object to the cell at the id and returns the depth of the
     * queue upon success or -1 to indicate failure.  In case of success, it
     * sets the cell to OCCUPIED so that it is available to be taken.  The
     * operation is not supposed to change the order of other reserved cells.
     */
    public int add(Object obj, int index);

    /**
     * adds a new object and the callback wrapper to the cell at the id and
     * returns the depth of the queue upon success or -1 to indicate failure.
     * In case of success, it sets the cell to OCCUPIED so that it is available
     * to be taken.  The operation is not supposed to change the order of other
     * reserved cells. The callback wrapper will be invoked when the object is
     * dequeued.
     */
    public int add(Object obj, int index, XQCallbackWrapper callback);

    /**
     * takes back the new object in the cell at the id and sets the cell
     * EMPTY again.  It returns the object upon success or null otherwise.  The
     * operation is not supposed to change the order of other occupied cells.
     */
    public Object takeback(int index);

    /**
     * returns the id of the next occupied cell or -1 if there is none of
     * the occupied cells available.  In case of success, it sets the cell
     * to TAKEN to indicate the object is in-use.
     */
    public int getNextCell(long milliSec);

    /**
     * returns the id of the first available occupied cell in the range of
     * len cells starting from begin or -1 if there is none of the occupied
     * cells available.  In case of success, it sets the cell to TAKEN to
     * indicate the object is in-use.  The operation is not supposed to
     * change the order of other occupied cells.
     */
    public int getNextCell(long milliSec, int begin, int len);

    /**
     * verifies the occupency on the cell at the id and returns the id if it
     * is occupied or -1 otherwise.  In case of verified, it sets the cell to
     * TAKEN to indicate the object is in-use.  The operation is not
     * supposed to change the order of other occupied cells.
     */
    public int getNextCell(long milliSec, int id);

    /**
     * returns the object in the cell at the id or null if the id is out of
     * bound or the cell at the id is empty. There is no change on the status
     * of the cell or object. The application should always get the id via
     * getNextCell() before calling this method. Otherwise, the object may be
     * processed by other thread.
     */
    public Object browse(int id);

    /**
     * puts the in-use object back to the cell at the id and returns the depth
     * of the queue upon success or -1 otherwise.  In case of success, it sets
     * the cell OCCUPIED again so that the object is available to be taken.
     * The operation is not supposed to change the order of other taken cells.
     */
    public int putback(int id);

    /**
     * removes the in-use object from the cell at the id and returns the size
     * of the queue upon success or -1 otherwise.  In case of success, it sets
     * the cell to EMPTY so that it is available to be collected or reserved.
     * The operation is not supposed to change the order of other taken cells.
     */
    public int remove(int id);

    /**
     * locates all unreserved empty cells among the given ids in the
     * list ranging from the id of begin thru the id of begin+len-1 and
     * returns the number of ids found or -1 to indicate errors.  All the
     * found ids are stored in the list starting from begin and in the same
     * order of the queue.
     */
    public int locate(int[] ids, int begin, int len);

    /** clear all cells and reset all marks */
    public void clear();

    /** returns an instance of Browser for browsing ids of all occupied cells */
    public Browser browser();

    public final static int XQ_ERROR = -2;
    public final static int KEEP_RUNNING = 1;
    public final static int PAUSE = 2;
    public final static int STANDBY = 4;
    public final static int EXTERNAL_XA = 8;
    public final static int CELL_OUTBOUND = -1;
    public final static int CELL_EMPTY = 0;
    public final static int CELL_RESERVED = 1;
    public final static int CELL_OCCUPIED = 2;
    public final static int CELL_TAKEN = 3;
}
