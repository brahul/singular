package com.singular.core;

import org.apache.hadoop.fs.Path;

/**
 * User context - user and job related details.
 *
 * @author Rahul Bhattacharjee
 */
public class RunContext {

    private String queueName;
    private Path applicationBasePath;

    public RunContext(){}

    public String getQueueName() {
        return queueName;
    }

    public RunContext setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public Path getApplicationBasePath() {
        return applicationBasePath;
    }

    public RunContext setApplicationBasePath(Path applicationBasePath) {
        this.applicationBasePath = applicationBasePath;
        return this;
    }
}
