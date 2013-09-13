package com.singular.api;

/**
 *
 * Core interface defining the task to perform in Yarn containers.
 *
 * @author Rahul Bhattacharjee
 */
public interface SingularRunnable extends Runnable {

    JobSpecification getSpecification();

    @Override
    public void run();
}
