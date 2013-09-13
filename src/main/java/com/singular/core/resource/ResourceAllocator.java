package com.singular.core.resource;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.util.List;

/**
 * @author Rahul Bhattacharjee
 */
public interface ResourceAllocator {

    public AllocateResponse allocateContainers(int noOfContainersToRequest , List<ContainerId> releasedContainers,
                                                            int noOfCompletedContainers);

}
