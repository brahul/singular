package com.singular.core.resource;

import com.singular.SingularConstants;
import com.singular.core.AMResourceManagerHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Rahul Bhattacharjee
 */
public class ResourceAllocatorImpl implements ResourceAllocator {

    private Configuration configuration;
    private Properties jobConfig;
    private int initialRequestId = 0;
    private ApplicationAttemptId applicationAttemptId;
    private int cores;
    private int memory;
    private int maxContainers;
    private AMResourceManagerHelper resourceManagerHelper = null;

    public ResourceAllocatorImpl(Configuration configuration,Properties properties,ApplicationAttemptId attemptId) {
        this.configuration = configuration;
        this.jobConfig = properties;
        this.cores = getNoOfCores();
        this.memory = getMemoryRequirement();
        this.applicationAttemptId = attemptId;
        this.maxContainers = getNoOfContainers();
        resourceManagerHelper = new AMResourceManagerHelper(configuration);
    }

    private int getMemoryRequirement() {
        String memory = jobConfig.getProperty(SingularConstants.KEY_MEM);
        return  Integer.parseInt(memory);
    }

    private int getNoOfCores() {
        String noOfCores = jobConfig.getProperty(SingularConstants.KEY_NO_OF_CORES);
        return Integer.parseInt(noOfCores);
    }

    private int getNoOfContainers() {
        String noOfContainers = jobConfig.getProperty(SingularConstants.KEY_NO_OF_CONTAINERS);
        return Integer.parseInt(noOfContainers);
    }

    @Override
    public AllocateResponse allocateContainers(int noOfContainersToRequest , List<ContainerId> releasedContainers,int noOfCompletedContainers) {
        AllocateRequest request = requestForContainers(noOfContainersToRequest,releasedContainers,noOfCompletedContainers);
        return resourceManagerHelper.allocate(request);
    }

    private AllocateRequest requestForContainers(int noOfContainersToRequest , List<ContainerId> releasedContainers,int noOfCompletedContainers) {
        List<ResourceRequest> requestedContainers = getContainerRequestForN(noOfContainersToRequest);

        AllocateRequest request = Records.newRecord(AllocateRequest.class);

        request.setResponseId(initialRequestId++);
        request.setApplicationAttemptId(applicationAttemptId);

        request.addAllAsks(requestedContainers);
        request.addAllReleases(releasedContainers);
        request.setProgress(noOfCompletedContainers/maxContainers);
        return request;
    }

    private List<ResourceRequest> getContainerRequestForN(int noOfContainers) {
        ResourceRequest resourceRequest = Records.newRecord(ResourceRequest.class);

        resourceRequest.setHostName("*");
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(1);
        resourceRequest.setPriority(pri);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(this.memory);
        resourceRequest.setCapability(capability);
        resourceRequest.setNumContainers(noOfContainers);

        List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
        requestedContainers.add(resourceRequest);
        return requestedContainers;
    }
}
