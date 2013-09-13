package com.singular.core.master;

import com.singular.SingularConstants;
import com.singular.core.ContainerResourceHelper;
import com.singular.core.container.ContainerRunnable;
import com.singular.core.resource.ResourceAllocator;
import com.singular.core.resource.ResourceAllocatorImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Default application master , that would do the minimal job of -
 *
 * 1. request for containers.
 * 2. launch containers.
 * 3. track for completed containers.
 * 4. doesn't support retries.
 *
 * @author Rahul Bhattacharjee
 */
public class DefaultApplicationMaster extends AbstractApplicationMaster implements Runnable {

    private ResourceAllocator resourceAllocator;
    private ExecutorService executorService;

    private static final int MAX_THREADS = 10;

    public DefaultApplicationMaster() {
        resourceAllocator = new ResourceAllocatorImpl(getConfiguration(),getJobConfiguration(),getApplicationAttemptId());
        executorService = Executors.newFixedThreadPool(MAX_THREADS);
    }

    @Override
    public void launchContainers() throws Exception {
        String noOfContainers = getJobConfiguration().getProperty(SingularConstants.KEY_NO_OF_CONTAINERS);
        String containerClassName = getJobConfiguration().getProperty(SingularConstants.KEY_CONTAINER_CLASS);
        String containerCores = getJobConfiguration().getProperty(SingularConstants.KEY_NO_OF_CORES);
        String containerMemory = getJobConfiguration().getProperty(SingularConstants.KEY_MEM);
        String stagingPath = getJobConfiguration().getProperty(SingularConstants.KEY_APPLICATION_STAGING_PATH);

        System.out.println("No of container " + noOfContainers + " " + containerClassName + " " +
        containerCores + " " + containerMemory);

        int noOfContainersRequired = Integer.parseInt(noOfContainers);
        int numberOfCompletedContainers = 0;
        int noOfContainersReceived = 0;
        int totalNoOfContainersReceived = 0;
        int noOfContainersToAsk = noOfContainersRequired;
        List<ContainerId> releasedContainers = new ArrayList<ContainerId>();

        while(totalNoOfContainersReceived < noOfContainersRequired || numberOfCompletedContainers < noOfContainersRequired) {
            //allocate would also act as heart beat to resource manager as well.
            AllocateResponse allocateResponse = resourceAllocator.allocateContainers(noOfContainersToAsk,releasedContainers,
                                                                                              numberOfCompletedContainers);

            List<ContainerStatus> statuses = allocateResponse.getAMResponse().getCompletedContainersStatuses();
            for(ContainerStatus status : statuses) {
                if(status.getState() == ContainerState.COMPLETE)
                    numberOfCompletedContainers++;
            }
            List<Container> containers = allocateResponse.getAMResponse().getAllocatedContainers();
            noOfContainersReceived = containers.size();
            totalNoOfContainersReceived += noOfContainersReceived;

            for(Container container : containers) {
                Runnable runnable = new ContainerRunnable(container,containerClassName,new Path(stagingPath));
                executorService.submit(runnable);
            }
            releasedContainers.clear();

            for(ContainerStatus containerStatus : allocateResponse.getAMResponse().getCompletedContainersStatuses()) {
                ContainerId containerId = containerStatus.getContainerId();
                releasedContainers.add(containerId);
            }

            System.out.println("Requested for " + noOfContainersToAsk + " , got " + noOfContainersReceived + " and no of completed containers " + numberOfCompletedContainers);

            // not to hit RM is quick succession.
            TimeUnit.SECONDS.sleep(2);
            noOfContainersToAsk = (noOfContainersRequired - totalNoOfContainersReceived);
        }

        executorService.shutdown();

        // waiting for the executor to finish all the threads.
        while (!executorService.isTerminated()) {
            TimeUnit.SECONDS.sleep(2);
        }
    }

    /**
     * Launch of the default application master which would launch containers.
     *
     * @param argv
     */
    public static void main(String [] argv) {
        Runnable runnable = new DefaultApplicationMaster();
        runnable.run();
    }
}
