package org.qbroker.common;

/* QList.java - a FIFO list for storing indexed objects */

/**
 * QList combines a List and Queue as a FIFO storage for storing objects
 * with associated unigue IDs.  It also has a Browser for quick browsing on
 * all the new objects or all occupied cells.
 *<br><br>
 * All cells in the QList line up in a circular array.  The cells in the same
 * status always stay together in a group.  Therefore the circle is divided into
 * four segments of arcs.  There are four marks separating those four arcs, ie,
 * the four different groups of cells.  Head divides taken cells and occupied
 * cells.  Tail separates occupied cells and reserved cells.  Lead divides
 * taken cells and empty cells.  Mark separates reserved cells and empty cells.
 *<br><br>
 * capacity -- maximum number of cells in the queue<br>
 * size -- number of the new and taken objects in the queue<br>
 * depth -- number of new objects in the queue<br>
 * watermark -- size + number of reserved cells<br>
 * collectible -- number of empty cells that have not been collected yet<br>
 * type -- type of a cell or status of the object<br>
 * id -- the id of a cell or the object<br>
 *<br>
 * This is not MT-Safe. It should only used by its owner.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class QList {
    private final int capacity;
    private String name = "qList";
    private Object[] elements = null;
    private int[] cells = null;
    private int[] positions = null;
    private int[] sequences = null;
    private int head = 0;   // points to the first occupied cell to be taken
    private int tail = 0;   // points to the first reserved cell to be occupied
    private int mark = 0;   // points to the first empty cell to be reserved
    private int lead = 0;   // points to the first taken cell to be empty
    private int cusp = 0;   // points to the first collectible cell
    private int size = 0;
    private int depth = 0;
    private int watermark = 0;
    private int collectible = 0;
    private int snumber = 0;
    private int offset = 0;
    private static final int OFFSET1 = Integer.MAX_VALUE;
    private static final int OFFSET2 = Integer.MAX_VALUE / 2;

    /**
     * initialzes a queue with capacity of cells
     */
    public QList(String name, int capacity) {
        if (capacity > 0)
            this.capacity = capacity;
        else
            this.capacity = 1;
        this.name = name;

        cells = new int[this.capacity];
        positions = new int[this.capacity];
        sequences = new int[this.capacity];
        elements = new Object[this.capacity];
        for (int i=0; i<this.capacity; i++) {
            cells[i] = i;
            positions[i] = i;
            sequences[i] = -1;
            elements[i] = null;
        }

        head = 0;
        tail = 0;
        lead = 0;
        mark = 0;
        size = 0;
        depth = 0;
        watermark = 0;
        collectible = 0;
        snumber = 0;
        offset = 0;
    }

    public String getName() {
        return name;
    }

    /**
     * returns the number of objects in the queue, new and in-use 
     */
    public int size() {
        return size;
    }

    /**
     * returns the number of new objects in the queue
     */
    public int depth() {
        return depth;
    }

    /**
     * returns the number of collectible objects in the queue
     */
    public int collectible() {
        return collectible;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean isFull() {
        if (size >= capacity)
            return true;
        else
            return false;
    }

    /**
     * returns the type of the cell with the id if it is not out of bound
       or -1 otherwise
     */
    public int getType(int id) {
        if (id < 0 || id >= capacity)
            return XQueue.CELL_OUTBOUND;

        int current = positions[id];
        if ((current - mark + capacity) % capacity < capacity - watermark)
            return XQueue.CELL_EMPTY;
        else if ((current - tail + capacity) % capacity < watermark - size)
            return XQueue.CELL_RESERVED;
        else if ((current - head + capacity) % capacity < depth)
            return XQueue.CELL_OCCUPIED;
        else if ((current - lead + capacity) % capacity < size - depth)
            return XQueue.CELL_TAKEN;
        else
            return XQueue.XQ_ERROR;
    }

    /**
     * collects the next collectible cell so that it is ready to be reserved
     */
    public int collect() {
        return collect(cells[cusp]);
    }

    /**
     * collects the id-th cell so that it is ready to be reserved.
     */
    public int collect(int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity)
             return -1;
        current = positions[id];
        if ((current - cusp + capacity) % capacity >= collectible)
            return -1;

        while (current != cusp) { // shift to left for keeping the order
            k = (current - 1 + capacity) % capacity;
            cid = cells[k];
            cells[current] = cid;
            positions[cid] = current;
            current = k;
        }
        cells[cusp] = id;
        positions[id] = cusp;
        collectible --;
        cusp = (cusp + 1) % capacity;
        return id;
    }

    /**
     * reserves the empty cell with the id and returns the id upon success or -1
     * otherwise.  The operation is not supposed to change the order of other
     * empty cells.
     */
    public int reserve() {
        return reserve(cells[mark]);
    }

    /**
     * reserves an empty cell in the range of len cells starting from begin
     * and returns the id of the cell upon success or -1 otherwise.  The
     * operation is not supposed to change the order of other empty cells.
     */
    public int reserve(int id) {
        int cid, k;
        if (id < 0 || id >= capacity)
            return -1;
        int current = positions[id];
        if ((current - mark + capacity) % capacity >= capacity - watermark)
            return -1;

        if (collectible > 0) { // try to collect it first
            collect(id);
            current = positions[id];
        }
        while (current != mark) { // shift to left for keeping the order
            k = (current - 1 + capacity) % capacity;
            cid = cells[k];
            cells[current] = cid;
            positions[cid] = current;
            current = k;
        }
        cells[mark] = id;
        positions[id] = mark;
        watermark ++;
        mark = (mark + 1) % capacity;
        return id;
    }

    /**
     * cancels the reservation on the cell with the id and returns the
     * watermark upon success or -1 otherwise.  In case of success, it sets
     * the cell to EMPTY so that it is available for reservation again.  The
     * operation is not supposed to change the order of other reserved cells.
     */
    public int cancel(int id) {
        if (id < 0 || id >= capacity || watermark <= size)
            return -1;

        int current = positions[id];
        if ((current - tail + capacity) % capacity >= watermark - size)
            return -1;

        mark = (mark - 1 + capacity) % capacity;
        if (current != mark) {
            int cid = cells[mark];
            cells[current] = cid;
            positions[cid] = current;
            cells[mark]= id;
            positions[id] = mark;
        }
        watermark --;
        return watermark;
    }

    /**
     * adds a new object to the cell with the id and returns the depth of the
     * queue upon success or -1 to indicate failure.  In case of success, it
     * sets the cell to OCCUPIED so that it is available to be taken.  The
     * operation is not supposed to change the order of other reserved cells.
     */
    public int add(Object obj, int id) {
        if (id < 0 || id >= capacity || size >= watermark)
            return -1;

        int current = positions[id];
        if ((current - tail + capacity) % capacity >= watermark - size)
            return -1;

        if (current != tail) {
            int cid = cells[tail];
            cells[current] = cid;
            positions[cid] = current;
            cells[tail]= id;
            positions[id] = tail;
        }
        tail = (tail + 1) % capacity;
        elements[id] = obj;
        size ++;
        depth ++;
        if (snumber < OFFSET1)
            snumber ++;
        else { // reset sequence numbers
            int cid;
            offset = (offset != OFFSET2) ? OFFSET2 : OFFSET1;
            current = lead;
            while (current != tail) {
                cid = cells[current];
                sequences[cid] -= offset;
                current = (current + 1) % capacity;
            }
            snumber -= offset;;
            snumber ++;
        }
        sequences[id] = snumber;
        return depth;
    }

    /**
     * takes back the new object in the cell with the id and sets the cell
     * EMPTY again.  It returns the object upon success or null otherwise.  The
     * operation is not supposed to change the order of other occupied cells.
     */
    public Object takeback(int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity || depth <= 0)
            return null;
        current = positions[id];
        if ((current - head + capacity) % capacity >= depth)
            return null;
        tail = (tail - 1 + capacity) % capacity;
        while (current != tail) { // shift to right for keeping the order
            k = (current + 1) % capacity;
            cid = cells[k];
            cells[current] = cid;
            positions[cid] = current;
            current = k;
        }
        cells[tail] = id;
        positions[id] = tail;
        depth --;
        size --;
        cancel(id);
        Object o = elements[id];
        elements[id] = null;
        sequences[id] = -1;
        return o;
    }

    /**
     * returns the id of the next occupied cell or -1 if there is none of
     * the occupied cells available.  In case of success, it sets the cell
     * to TAKEN to indicate the object is in-use.
     */
    public int getNextID() {
        if (depth <= 0)
            return -1;
        int id = cells[head];
        head = (head + 1) % capacity;
        depth --;
        return  id;
    }

    /**
     * verifies the occupency on the cell with the id and returns the id if it
     * is occupied or -1 otherwise.  In case of verified, it sets the cell to
     * TAKEN to indicate the object is in-use.  The operation is not
     * supposed to change the order of other occupied cells.
     */
    public int getNextID(int id) {
        int cid, k;
        if (id < 0 || id >= capacity)
            return -1;
        int current = positions[id];
        if ((current - head + capacity) % capacity >= depth)
            return -1;

        while (current != head) { // shift to left for keeping the order
            k = (current - 1 + capacity) % capacity;
            cid = cells[k];
            cells[current] = cid;
            positions[cid] = current;
            current = k;
        }
        cells[head] = id;
        positions[id] = head;
        depth --;
        head = (head + 1 + capacity) % capacity;
        return id;
    }

    /**
     * returns the object in the cell with the id or null if id is out of bound.
     */
    public Object browse(int id) {
        if (id >=0 && id < capacity)
            return elements[id];
        else
            return null;
    }

    /**
     * puts an in-use object back to the cell with the id and returns the depth
     * of the queue upon success or -1 otherwise.  In case of success, it sets
     * the cell OCCUPIED again so that the object is available to be taken.
     * The operation is not supposed to change the order of other taken cells.
     */
    public int putback(int id) {
        if (id < 0 || id >= capacity || depth >= capacity)
            return -1;

        int current = positions[id];
        if ((current - lead + capacity) % capacity >= size - depth)
            return -1;

        head = (head - 1 + capacity) % capacity;
        if (current != head) {
            int cid = cells[head];
            cells[current] = cid;
            positions[cid] = current;
            cells[head]= id;
            positions[id] = head;
        }
        if (depth > 0) { // move it back to its position
            long sn = sequences[id];
            int k, cid, n = 0;
            current = head;
            do {
                k = (current + 1) % capacity;
                cid = cells[k];
                if (sequences[cid] >= sn)
                    break;
                // swap the positions
                cells[current] = cid;
                positions[cid] = current;
                cells[k] = id;
                positions[id] = k;
                current = k;
            } while (++n < depth);
        }
        depth ++;
        return depth;
    }

    /**
     * removes the in-use object from the cell with the id and returns the
     * original object upon success or null otherwise.  In case of success, it
     * sets the cell to EMPTY so that it is available for reservation again.
     * The operation is not supposed to change the order of other taken cells.
     */
    public Object remove(int id) {
        if (id < 0 || id >= capacity || size <= 0)
            return null;

        int current = positions[id];
        if ((current - lead + capacity) % capacity >= size - depth)
            return null;

        if (current != lead) {
           int cid = cells[lead];
           cells[current] = cid;
           positions[cid] = current;
           cells[lead]= id;
           positions[id] = lead;
        }
        lead = (lead + 1) % capacity;
        size --;
        watermark --;
        collectible ++;
        Object o = elements[id];
        elements[id] = null;
        sequences[id] = -1;
        return o;
    }

    /**
     * swaps the positions between the two occupied cells referenced by their
     * ids and returns 1 for success, 0 for failure or -1 for out of bound.
     */
    public int swap(int id, int jd) {
        if (id < 0 || jd < 0 || id >= capacity || jd >= capacity)
            return -1;
        if ((positions[id] - head + capacity) % capacity < depth &&
            (positions[jd] - head + capacity) % capacity < depth) {
            int k = positions[id];
            positions[id] = positions[jd];
            cells[k] = jd;
            positions[jd] = k;
            k = positions[id];
            cells[k] = id;
            k = sequences[id];
            sequences[id] = sequences[jd];
            sequences[jd] = k;
            return 1;
        }
        return 0;
    }

    /**
     * rotates the object with the id to the end of the list and returns 1 for
     * success, 0 for failure or -1 for out of bound.
     */
    public int rotate(int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity || depth <= 0)
            return -1;
        current = positions[id];
        if ((current - head + capacity) % capacity >= depth)
            return 0;
        k = (current + 1) % capacity;
        while (k != tail) { // shift to right while keeping the right order
            cid = cells[k];
            cells[k] = id;
            positions[id] = k;
            cells[current] = cid;
            positions[cid] = current;
            current = k;
            k = sequences[id];
            sequences[id] = sequences[cid];
            sequences[cid] = k;
            k = (current + 1) % capacity;
        }
        return 1;
    }

    /**
     * loads the ids of the type of cells in the natual order to the given array
     */
    public int queryIDs(int[] ids, int type) {
        int i, m, n;
        if (ids == null)
            return -3;
        switch (type) {
          case XQueue.CELL_EMPTY:
            n = capacity - watermark;
            if (n > ids.length)
                return -1;
            m = mark;
            break;
          case XQueue.CELL_RESERVED:
            n = watermark - size;
            if (n > ids.length)
                return -1;
            m = tail;
            break;
          case XQueue.CELL_OCCUPIED:
            n = depth;
            if (n > ids.length)
                return -1;
            m = head;
            break;
          case XQueue.CELL_TAKEN:
            n = size - depth;
            if (n > ids.length)
                return -1;
            m = lead;
            break;
          default:
            return -2;
        }
        for (i=0; i<n; i++)
            ids[i] = cells[(m + i)%capacity]; 
        return n;
    }

    /** returns an instance of Browser for ids of occupied cells */
    public Browser browser() {
        return new MyBrowser();
    }

    /** clear all cells and reset all marks */
    public void clear() {
        for (int i=0; i<capacity; i++) {
            cells[i] = i;
            positions[i] = i;
            sequences[i] = -1;
            elements[i] = null;
        }

        head = 0;
        tail = 0;
        lead = 0;
        mark = 0;
        cusp = 0;
        size = 0;
        depth = 0;
        watermark = 0;
        collectible = 0;
        snumber = 0;
        offset = 0;
    }

    protected void finalize() {
        clear();
    }

    /**
     * A Browser for all the occupied cells in the first snapshot
     */
    private class MyBrowser implements Browser {
        private long total = 0;
        private int id = -1, f = 0, sn = 0, current = -1;

        public MyBrowser() {
        }
        
        public int next() {
            int i, s, ss;
            if (depth <= 0) { // nothing to browse
                id = -1;
                return id;
            }
            if (id >= 0) {
                s = sequences[id];
                current = positions[id];
                i = (current + 1) % capacity;
                ss = sequences[cells[i]];
            }
            else { // reset id to head
                current = head;
                id = cells[head];
                s = sequences[id];
                i = (head + 1) % capacity;
                ss = sequences[cells[i]];
            }

            if (total == 0) { // first time
                f = offset;
                current = head;
                total ++;
                sn = s;
                return id;
            }
            else if (f != offset) { // reset sequence number
                sn -= offset;
                f = offset;
            }

            if (s == sn) { // same content in the id-th cell
                if ((current - head + capacity) % capacity < depth) {
                    current ++;
                    if (current >= capacity)
                        current %= capacity;
                    if (current == tail) // end of browse
                        return -1;
                    if ((current - head + capacity) % capacity < depth) {
                        id = cells[current];
                        sn = ss;
                        total ++;
                        return id;
                    }
                    else
                        return -1;
                }
                else { // out of range, hence jump to head to scan
                    for (i=0; i<depth; i++) {
                        current = (head + i) % capacity;
                        id = cells[current];
                        if (sn > sequences[id])
                            continue;
                        sn = sequences[id];
                        total ++;
                        return id;
                    }
                    id = -1;
                    return id;
                }
            }
            else { // new content in the id-th cell
                for (i=0; i<depth; i++) {
                    current = (head + i) % capacity;
                    id = cells[current];
                    if (sn > sequences[id])
                        continue;
                    sn = sequences[id];
                    total ++;
                    return id;
                }
                id = -1;
                return id;
            }
        }

        public void reset() {
            total = 0;
            id = -1;
            sn = 0;
            f = 0;
        }
    }
}
