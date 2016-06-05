package org.qbroker.common;

/* ReservableCells.java - an indexed cells that are reservable */

import org.qbroker.common.XQueue;

/**
 * ReservableCells are bunch of indexed cells that are reservable with the
 * associated unigue IDs. All the reserved cells are always kept in their
 * natural order.
 *<br/><br/>
 * A cell always is in one of the two different status, EMPTY, and RESERVED. All
 * cells line up in a circular array.  The cells in the same status always stay
 * together in a group. Therefore the circle is divided into two segments of
 * arcs. There are two marks separating those two arcs, ie, the two different
 * groups of cells. Tail divieds empty cells and reserved cells. Head separates
 * reserved cells and empty cells.
 *<br/><br/>
 * There are two methods to change the status of a cell. The method of
 * reserve() is to reserve an empty cell and toggles its status from EMPTY to
 * RESERVED. The method of cancel() is to cancel the reservation and swithes
 * the cell from RESERVED to EMPTY.
 *<br/><br/>
 * capacity -- maximum number of cells in the list<br/>
 * size -- number of reserved cells<br/>
 * status -- status of a cell<br/>
 * id -- the fixed id of a cell<br/>
 *<br/>
 * This is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class ReservableCells {
    private final int capacity;
    private String name = "cells";
    private int[] cells = null;
    private int[] positions = null;
    private int head = 0;   // points to the first reserved cell to be cancelled
    private int tail = 0;   // points to the first empty cell to be reserved
    private int size = 0;   // total number of reserved cells

    /**
     * initialzes the list with capacity of cells
     */
    public ReservableCells(String name, int capacity) {
        if (capacity > 0)
            this.capacity = capacity;
        else
            this.capacity = 1;
        this.name = name;

        cells = new int[this.capacity];
        positions = new int[this.capacity];
        for (int i=0; i<this.capacity; i++) {
            cells[i] = i;
            positions[i] = i;
        }

        head = 0;
        tail = 0;
        size = 0;
    }

    public String getName() {
        return name;
    }

    /**
     * returns the number of reserved cells
     */
    public synchronized int size() {
        return size;
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * returns the status of the cell with the id if it is not out of bound
       or -1 otherwise
     */
    public synchronized int getCellStatus(int id) {
        if (id < 0 || id >= capacity)
            return XQueue.CELL_OUTBOUND;

        int current = positions[id];
        if ((current - tail + capacity) % capacity < capacity - size)
            return XQueue.CELL_EMPTY;
        else if ((current - head + capacity) % capacity < size)
            return XQueue.CELL_RESERVED;
        else
            return XQueue.XQ_ERROR;
    }

    /**
     * cancels the reservation on the id-th cell and returns the size upon
     * success or -1 otherwise.
     */
    public synchronized int cancel(int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity || size <= 0)
             return -1;
        current = positions[id];
        if ((current - head + capacity) % capacity >= size)
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
        size --;
        head = (head + 1) % capacity;
        notifyAll();
        return size;
    }

    /**
     * reserves the id-th cell and returns the id upon success or -1 otherwise.
     * If milliSec < 0, there is no wait.
     */
    public synchronized int reserve(long milliSec, int id) {
        if (id < 0 || id >= capacity)
            return -1;
        int current = positions[id];
        if ((current - tail + capacity) % capacity >= capacity - size) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            current = positions[id];
            if ((current - tail + capacity) % capacity >= capacity - size)
                return -1;
        }

        if (current != tail) { // swap with the tail
            int cid = cells[tail];
            cells[current] = cid;
            positions[cid] = current;
            cells[tail]= id;
            positions[id] = tail;
        }
        tail = (tail + 1) % capacity;
        size ++;
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

        k = capacity - size;
        end = len + begin;
        if (k <= len) {
            for (i=0; i<k; i++) { // scan on empty cells
                current = (tail + i) % capacity;
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
                d = (positions[i] - tail + capacity) % capacity;
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
        k = capacity - tail;
        if (k <= len) {
            for (i=0; i<k; i++) {
                current = (tail + i) % capacity;
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
                d = (positions[i] - tail + capacity) % capacity;
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
        return reserve(milliSec, cells[tail]);
    }

    /**
     * loads the ids of the type of cells in the natual order to the given array
     */
    public synchronized int queryIDs(int[] ids, int type) {
        int i, m, n;
        if (ids == null)
            return -3;
        switch (type) {
          case XQueue.CELL_EMPTY:
            n = capacity - size;
            if (n > ids.length)
                return -1;
            m = tail;
            break;
          case XQueue.CELL_RESERVED:
            n = size;
            if (n > ids.length)
                return -1;
            m = head;
            break;
          default:
            return -2;
        }
        for (i=0; i<n; i++)
            ids[i] = cells[(m + i)%capacity]; 
        return n;
    }

    /** clear all cells and reset all marks */
    public synchronized void clear() {
        for (int i=0; i<capacity; i++) {
            cells[i] = i;
            positions[i] = i;
        }

        head = 0;
        tail = 0;
        size = 0;
    }

    protected void finalize() {
        clear();
    }
}
