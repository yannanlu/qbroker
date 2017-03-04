package org.qbroker.common;

/* CollectibleCells.java - an indexed cells that are collectible */

import org.qbroker.common.XQueue;

/**
 * CollectibleCells are a fixed numner of indexed cells that are collectible
 * with the associated unigue IDs. All the collectible (TAKEN) cells are always
 * kept in their natural order.
 *<br/><br/>
 * A cell is always in one of the two different status, EMPTY, and TAKEN. All
 * cells line up in a circular array.  The cells in the same status always stay
 * together in a group. Therefore the circle is divided into two segments of
 * arcs. There are two marks separating those two arcs, ie, the two different
 * groups of cells. Tail divieds empty cells and taken cells. Head separates
 * taken cells and empty cells.
 *<br/><br/>
 * There are two methods to change the status of a cell. The method of
 * take() is to take an empty cell and toggles its status from EMPTY to TAKEN.
 * The method of collect() is to collect a taken cell and switches it from
 * TAKEN to EMPTY.
 *<br/><br/>
 * capacity -- maximum number of cells in the list<br/>
 * size -- number of taken cells<br/>
 * status -- status of a cell<br/>
 * id -- the fixed id of a cell<br/>
 *<br/>
 * This is MT-Safe.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class CollectibleCells {
    private final int capacity;
    private String name = "cells";
    private int[] cells = null;
    private int[] positions = null;
    private int head = 0;   // points to the first taken cell to be collected
    private int tail = 0;   // points to the first empty cell to be taken
    private int size = 0;   // total number of taken cells

    /**
     * initialzes the list with capacity of cells
     */
    public CollectibleCells(String name, int capacity) {
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
     * returns the number of taken cells in the list
     */
    public synchronized int size() {
        return size;
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * returns the status of the cell with the id if it is not out of bound
     * or -1 otherwise
     */
    public synchronized int getCellStatus(int id) {
        if (id < 0 || id >= capacity)
            return XQueue.CELL_OUTBOUND;

        int current = positions[id];
        if ((current - tail + capacity) % capacity < capacity - size)
            return XQueue.CELL_EMPTY;
        else if ((current - head + capacity) % capacity < size)
            return XQueue.CELL_TAKEN;
        else
            return XQueue.XQ_ERROR;
    }

    /**
     * collects the id-th cell and returns the id upon success or -1 otherwise.
     * If milliSec < 0, there is no wait.
     */
    public synchronized int collect(long milliSec, int id) {
        int cid, k, current;
        if (id < 0 || id >= capacity)
             return -1;
        current = positions[id];
        if ((current - head + capacity) % capacity >= size) {
            if (milliSec < 0)
                return -1;
            else try {
                wait(milliSec);
            }
            catch (InterruptedException e) {
            }
            current = positions[id];
            if ((current - head + capacity) % capacity >= size)
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
        size --;
        head = (head + 1) % capacity;
        return id;
    }

    /**
     * collects the next collectible cell and returns its id upon success or -1
     * otherwise.
     */
    public synchronized int collect(long milliSec) {
        return collect(milliSec, cells[head]);
    }

    /**
     * takes the empty cell with the id and returns the size upon success or
     * -1 otherwise.
     */
    public synchronized int take(int id) {
        if (id < 0 || id >= capacity)
            return -1;
        int current = positions[id];
        if ((current - tail + capacity) % capacity >= capacity - size)
            return -1;

        if (current != tail) { // swap with the tail
            int cid = cells[tail];
            cells[current] = cid;
            positions[cid] = current;
            cells[tail]= id;
            positions[id] = tail;
        }
        tail = (tail + 1) % capacity;
        size ++;
        notifyAll();
        return size;
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
          case XQueue.CELL_TAKEN:
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

    /** acknowledges the request msgs only */
    public void acknowledge(long[] state) {
        if (state != null && state.length > 0) {
            int i = take((int) state[0]);
        }
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
