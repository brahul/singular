package com.singular.core;

/**
 * Nodecapacity , currently limited to what yarn supports , memory only.
 *
 * @author Rahul Bhattacharjee
 */
public class NodeCapacity {

    private static final int DEFAULT_MEMORY = 100;

    private int memory;

    public NodeCapacity() {
        this(DEFAULT_MEMORY);
    }

    public NodeCapacity(int memory) {
        this.memory = memory;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }
}
