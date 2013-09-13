package com.singular.api;

/**
 * Job specification defining the jobs properties.
 *
 * @author Rahul Bhattacharjee
 */
public class JobSpecification {

    private int memory;
    private int cores;
    private int noOfContainers;
    private String applicationName;

    public JobSpecification setMemory(int memory) {
        this.memory = memory;
        return this;
    }

    public JobSpecification setCores(int no) {
        this.cores = no;
        return this;
    }

    public JobSpecification setNumberOfContainers(int noOfContainers) {
        this.noOfContainers = noOfContainers;
        return this;
    }

    public int getMemory() {
        return memory;
    }

    public int getCores() {
        return cores;
    }

    public int getNoOfContainers() {
        return noOfContainers;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public JobSpecification setApplicationName(String applicationName) {
        this.applicationName = applicationName;
        return this;
    }
}
