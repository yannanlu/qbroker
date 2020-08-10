package org.qbroker.common;

/* IndexedXQueue.java - an XQueue with indexed cells supporting collectibles */

/**
 * IndexedXQueue implements XQueue as a FIFO in-memory storage with a circular
 * array to support tracking and transactions. It also support callbacks.
 *<br><br>
 * All cells in an XQueue line up in a circle implemented by an array.  The
 * cells on the same status always stay together in a group.  Therefore the
 * circle is divided into five segments of arcs.  There are five marks
 * separating those five arcs, ie, the five different groups of cells.  Head
 * divides taken cells and occupied cells.  Tail separates occupied cells and
 * reserved cells.  Lead divides taken cells and empty cells.  Mark separates
 * reserved cells and empty cells.  Cusp separates collected empty cells and
 * the collected empty cells.
 *<br><br>
 * capacity -- maximum number of cells in the queue<br>
 * size -- number of the new and in-use objects in the queue<br>
 * depth -- number of new objects in the queue<br>
 * watermark -- size + number of reserved cells<br>
 * collectible -- number of empty cells that have not been collected yet<br>
 * count -- number of objects removed from the queue since last reset<br>
 * mtime -- timestamp of the reset on the queue<br>
 * status -- status of a cell or the object<br>
 * id -- the id of a cell or the object<br>
 *<br><br>
 * It is MT-Safe with transaction control on each object or cell.
 *<br>
 * @author yannanlu@yahoo.com
 */

public class IndexedXQueue implements XQueue {
    private final int capacity;
    private String name = "xq";
    private XQCallbackWrapper[] callbacks = null; // for callback wrappers
    private Object[] elements = null;// objects on their indices
    private int[] cells = null;      // indices to the elements on the positions
    private int[] positions = null;  // positions of the cells
    private int[] sequences = null;  // sequence ids for occupied cells
    private int head = 0;   // points to the first occupied cell to be taken
    private int tail = 0;   // points to the first reserved cell to be occupied
    private int lead = 0;   // points to the first taken cell to be empty
    private int mark = 0;   // points to the first empty cell to be reserved
    private int cusp = 0;   // points to the first collectible cell
    private int size = 0;
    private int depth = 0;
    private int watermark = 0;
    private int collectible = 0;
    private long count = 0;
    private long mtime;
    private int snumber = 0;
    private int offset = 0;
    private int mask = XQueue.KEEP_RUNNING;
    private static final int OFFSET1 = Integer.MAX_VALUE;
    private static final int OFFSET2 = Integer.MAX_VALUE / 2;

    /**
     * initialzes a queue with capacity of cells
     */
    public IndexedXQueue(String name, int capacity) {
        if (capacity > 0)
            this.capacity = capacity;
        else
            this.capacity = 1;

        cells = new int[this.capacity];
        positions = new int[this.capacity];
        sequences = new int[this.capacity];
        elements = new Object[this.capacity];
        callbacks = new XQCallbackWrapper[this.capacity];
        for (int i=0; i<this.capacity; i++) {
            cells[i] = i;
            positions[i] = i;
            sequences[i] = -1;
            elements[i] = null;
            callbacks[i] = null;
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
        count = 0;
        mtime = System.currentTimeMillis();
        snumber = 0;
        offset = 0;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * returns the number of objects in the queue, new and taken
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

    /** returns number of objects removed from the queue since the last reset */
    public long getCount() {
        return count;
    }

    /** returns the timestamp of the last reset */
    public long getMTime() {
        return mtime;
    }

    /**
     * resets the timer and count, and returns the number of removed objects
     * since the previous reset
     */
    public synchronized long reset() {
        long n = count;
        count = 0;
        mtime = System.currentTimeMillis();
        return n;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getGlobalMask() {
        return mask;
    }

    public void setGlobalMask(int mask) {
        this.mask = mask;
    }

    /**
     * tests if any cell is occupied or any object is ready to be taken
     */
    public synchronized boolean available() {
        if (depth > 0)
            return true;
        else
            return false;
    }

    public synchronized boolean isFull() {
        if (size >= capacity)
            return true;
        else
            return false;
    }

    public boolean isCollectible() {
        return true;
    }

    /**
     * collects the id-th cell so that it is ready to be reserved.
     * If milliSec is less than 0, there is no wait.
     */
    public synchronized int collect(long milliSec, int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity)
             return -1;
        current = positions[id];
        if ((current - cusp + capacity) % capacity >= collectible) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            current = positions[id];
            if ((current - cusp + capacity) % capacity >= collectible)
                return -1;
        }

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
     * collects the first collectible cell out of len cells starting from begin
     */
    public synchronized int collect(long milliSec, int begin, int len) {
        int current, cid, i, k, end;
        if (begin < 0 || len <= 0 || len+begin > capacity)
            return -2;
        if (len == 1)
            return collect(milliSec, begin);

        k = collectible;
        end = len + begin;
        if (k <= len) {
            for (i=0; i<k; i++) { // scan on collectible cells
                current = (cusp + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = collect(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (k > 0) { // scan on given ids for 1st collectible empty cell
            int d;
            current = k;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - cusp + capacity) % capacity;
                if (d < current) {
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return collect(-1L, cid);
        }
        if (milliSec < 0)
            return -1;

        try {
            wait(milliSec);
        }
        catch (InterruptedException e) {
        }
        k = collectible;
        if (k <= len) {
            for (i=0; i<k; i++) { // scan on collectible cells
                current = (cusp + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = collect(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (k > 0) { // scan on given ids for 1st collectible empty cell
            int d;
            current = k;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - cusp + capacity) % capacity;
                if (d < current) {
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return collect(-1L, cid);
        }
        return -1;
    }

    /**
     * collects the next collectible cell so that it is ready to be reserved
     */
    public synchronized int collect(long milliSec) {
        int cid, current;
        int id = cells[cusp];
        current = positions[id];
        if ((current - cusp + capacity) % capacity >= collectible) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            id = cells[cusp];
            current = positions[id];
            if ((current - cusp + capacity) % capacity >= collectible)
                return -1;
        }
        collectible --;
        cusp = (cusp + 1) % capacity;
        return id;
    }

    /**
     * reserves the id-th cell so that it is ready to be occupied.
     * If milliSec is less than 0, there is no wait.
     */
    public synchronized int reserve(long milliSec, int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity)
             return -1;
        current = positions[id];
        if ((current - mark + capacity) % capacity >= capacity - watermark) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            current = positions[id];
            if ((current - mark + capacity) % capacity >= capacity - watermark)
                return -1;
        }

        if (collectible > 0) { // try to collect it first
            collect(-1L, id);
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
     * reserves the first empty cell out of len cells starting from begin
     */
    public synchronized int reserve(long milliSec, int begin, int len) {
        int current, cid, i, k, end;
        if (begin < 0 || len <= 0 || len+begin > capacity)
            return -2;
        if (len == 1)
            return reserve(milliSec, begin);

        k = capacity - watermark;
        end = len + begin;
        if (k <= len) {
            for (i=0; i<k; i++) { // scan on empty cells
                current = (mark + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = reserve(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (k > 0) { // scan on given ids for 1st empty cell
            int d;
            current = k;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - mark + capacity) % capacity;
                if (d < current) {
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return reserve(-1L, cid);
        }
        if (milliSec < 0)
            return -1;

        try {
            wait(milliSec);
        }
        catch (InterruptedException e) {
        }
        k = capacity - watermark;
        if (k <= len) {
            for (i=0; i<k; i++) {
                current = (mark + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = reserve(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (k > 0) { // scan on given ids for 1st empty cell
            int d;
            current = k;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - mark + capacity) % capacity;
                if (d < current) {
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return reserve(-1L, cid);
        }
        return -1;
    }

    /**
     * reserves the next empty cell so that it is ready to be occupied
     */
    public synchronized int reserve(long milliSec) {
        int cid, current;
        int id = cells[mark];
        current = positions[id];
        if ((current - mark + capacity) % capacity >= capacity - watermark) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            id = cells[mark];
            current = positions[id];
            if ((current - mark + capacity) % capacity >= capacity - watermark)
                return -1;
        }

        if (collectible > 0) { // try to collect it first
            collect(-1L, id);
            id = cells[mark];
            current = positions[id];
        }
        watermark ++;
        mark = (mark + 1) % capacity;
        return id;
    }

    /**
     * cancels the reservation on the id-th cell and returns watermark
     */
    public synchronized int cancel(int id) {
        if (id < 0 || id >= capacity)
             return -1;
        if (watermark <= size)
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
        notifyAll();
        return watermark;
    }

    /**
     * adds a new object to the reserved cell at id so that it is ready to be
     * taken
     */
    public synchronized int add(Object obj, int id) {
        return add(obj, id, null);
    }

    /**
     * adds a new object and a callback wrapper to the reserved cell at id
     * so that it is ready to be taken
     */
    public synchronized int add(Object obj, int id, XQCallbackWrapper cbw) {
        if (id < 0 || id >= capacity)
             return -1;
        if (size >= watermark)
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
        if (cbw != null)
            callbacks[id] = cbw;
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
        notifyAll();
        return depth;
    }

    /**
     * takes back the new object in the id-th cell and makes the cell empty
     * again while keeping the order on other occupied cells
     */
    public synchronized Object takeback(int id) {
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
        callbacks[id] = null;
        sequences[id] = -1;
        notifyAll();
        return o;
    }

    /**
     * returns the id of the next occupied cell or -1 if there is none
     */
    public synchronized int getNextCell(long milliSec) {
        if (depth <= 0) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            if (depth <= 0)
                return -1;
        }
        int id = cells[head];
        head = (head + 1) % capacity;
        depth --;
        return  id;
    }

    /**
     * checks the cell at the index of id for its occupancy and returns the id
     * if it is occupied or -1 if it is not.
     */
    public synchronized int getNextCell(long milliSec, int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity)
            return -1;
        current = positions[id];
        if ((current - head + capacity) % capacity >= depth) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            current = positions[id];
            if ((current - head + capacity) % capacity >= depth)
                return -1;
        }

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
        head = (head + 1) % capacity;
        return id;
    }

    /**
     * gets the first occupied cell out of len cells starting from begin
     */
    public synchronized int getNextCell(long milliSec, int begin, int len) {
        int current, cid, i, end;
        if (begin < 0 || len <= 0 || len+begin > capacity)
            return -2;
        if (len == 1)
            return getNextCell(milliSec, begin);

        end = len + begin;
        if (depth <= len) {
            for (i=0; i<depth; i++) { // scan on occupied cells
                current = (head + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = getNextCell(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (depth > 0) { // scan on given range for 1st occupied cell
            int d;
            current = depth;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - head + capacity) % capacity;
                if (d < current) { // look for the leftest one
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return getNextCell(-1L, cid);
        }
        if (milliSec < 0)
            return -1;

        try {
            wait(milliSec);
        }
        catch (InterruptedException e) {
        }
        if (depth <= len) {
            for (i=0; i<depth; i++) { // scan on occupied cells
                current = (head + i) % capacity;
                cid = cells[current];
                if (cid >= begin && cid < end) { // within the range
                    cid = getNextCell(-1L, cid);
                    if (cid >= 0)
                        return cid;
                }
            }
        }
        else if (depth > 0) { // scan on given range for 1st occupied cell
            int d;
            current = depth;
            cid = -1;
            for (i=begin; i<end; i++) {
                d = (positions[i] - head + capacity) % capacity;
                if (d < current) { // look for the leftest one
                    cid = i;
                    current = d;
                    if (current == 0)
                        break;
                }
            }
            if (cid >= 0) // found it
                return getNextCell(-1L, cid);
        }
        return -1;
    }

    /**
     * returns the object in the id-th cell or null if id out of bound
     */
    public Object browse(int id) {
        if (id >= 0 && id < capacity)
            return elements[id];
        else
            return null;
    }

    /**
     * puts the in-use object back to the cell at the id and makes the cell
     * occupied again so that the object is ready to be taken.
     */
    public synchronized int putback(int id) {
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
        notifyAll();
        return depth;
    }

    /**
     * removes the object from the id-th cell and makes the cell empty so that
     * it can be occupied again.
     */
    public synchronized int remove(int id) {
        if (id < 0 || id >= capacity || size <= 0)
             return -1;

        int current = positions[id];
        if ((current - lead + capacity) % capacity >= size - depth)
            return -1;

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
        count ++;
        sequences[id] = -1;
        elements[id] = null;
        if (callbacks[id] != null) { // invoke callback
            callbacks[id].callback(name, String.valueOf(id));
            callbacks[id] = null;
        }
        notifyAll();
        return size;
    }

    /**
     * returns the status of the id-th cell if it is not out of bound,  or -1
     * otherwise
     */
    public synchronized int getCellStatus(int id) {
        if (id < 0 || id >= capacity)
            return CELL_OUTBOUND;

        int current = positions[id];
        if ((current - mark + capacity) % capacity < capacity - watermark)
            return CELL_EMPTY;
        else if ((current - tail + capacity) % capacity < watermark - size)
            return CELL_RESERVED;
        else if ((current - head + capacity) % capacity < depth)
            return CELL_OCCUPIED;
        else if ((current - lead + capacity) % capacity < size - depth)
            return CELL_TAKEN;
        else
            return XQ_ERROR;
    }

    /**
     * locates all unreserved empty cells among the given ids in the list
     * starting from the index of begin thru the index of begin+len-1.
     * It returns the number of id found or -1 to indicate error.  All the
     * found ids will be stored in the list starting from begin and in the
     * same order of the queue.
     */
    public synchronized int locate(int[] ids, int begin, int len) {
        if (begin < 0 || len + begin > capacity || ids == null ||
             ids.length <= 0)
             return -1;

        int cid, i, j, n, k, end;
        int current = mark;
        k = capacity - watermark;
        end = len + begin;
        n = begin;
        for (i=0; i<k; i++) { // scan all uncollected empty cells
            current = (mark + i) % capacity;
            cid = cells[current];
            for (j=n; j<end; j++) { // compare with given list
                if (ids[j] == cid) { // found one and swap it
                    ids[j] = ids[n];
                    ids[n++] = cid;
                }
            }
            if (n >= end)
                break;
        }
        n -= begin;
        return n;
    }

    /** returns an instance of Browser for ids of occupied cells */
    public Browser browser() {
        return new MyBrowser();
    }

    public void clear() {
        for (int i=0; i<capacity; i++) {
            cells[i] = i;
            positions[i] = i;
            sequences[i] = -1;
            elements[i] = null;
            callbacks[i] = null;
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
        count = 0;
        snumber = 0;
        offset = 0;
    }

    protected void finalize() {
        clear();
    }

    /**
     * A Browser for all the occupied cells
     */
    private class MyBrowser implements Browser {
        private long total = 0;
        private int id = -1, f = 0, sn = 0, current = -1;

        public MyBrowser() {
        }

        public int next() {
            int i, h, d, t, s, ss;
            synchronized (IndexedXQueue.this) {
                if (depth <= 0) {
                    id = -1;
                    return id;
                }
                h = head;
                d = depth;
                t = tail;
                if (id >= 0) {
                    s = sequences[id];
                    current = positions[id];
                    i = (current + 1) % capacity;
                    ss = sequences[cells[i]];
                }
                else { // reset id to head
                    current = h;
                    id = cells[h];
                    s = sequences[id];
                    i = (h + 1) % capacity;
                    ss = sequences[cells[i]];
                }
                if (total == 0)
                    f = offset;
                else if (f != offset) {
                    sn -= offset;
                    f = offset;
                }
            }

            if (total == 0) { // first time
                current = h;
                sn = s;
                total ++;
                return id;
            }

            if (s == sn) { // same content in the id-th cell
                if ((current - h + capacity) % capacity < d) { // still occupied
                    current ++;
                    if (current >= capacity)
                        current %= capacity;
                    if (current == t) // end of browse
                        return -1;
                    if ((current - h + capacity) % capacity < d) { // occupied
                        id = cells[current];
                        sn = ss;
                        total ++;
                        return id;
                    }
                    else
                        return -1;
                }
                else { // out of range, hence jump to head to scan
                    for (i=0; i<d; i++) {
                        current = (h + i) % capacity;
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
                for (i=0; i<d; i++) {
                    current = (h + i) % capacity;
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
