package com.singular.core;

import org.apache.hadoop.fs.Path;

/**
 * Name of resource and path pair.
 *
 * @author Rahul Bhattacharjee
 */
public class PathNamePair {
    private Path path;
    private String resourceName;

    public PathNamePair(Path path, String resourceName) {
        this.path = path;
        this.resourceName = resourceName;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    @Override
    public String toString() {
        return "com.singular.poc.PathNamePair{" +
                "path=" + path +
                ", resourceName='" + resourceName + '\'' +
                '}';
    }
}
