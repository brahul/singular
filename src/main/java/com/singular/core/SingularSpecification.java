package com.singular.core;

/**
 * @author Rahul Bhattacharjee
 */
public class SingularSpecification {

    private int no;

    public SingularSpecification(int no) {
        this.no = no;
    }

    public int getNumberOfContainers(){
        return this.no;
    }
}
