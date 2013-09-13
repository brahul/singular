package com.singular.core.container;

import com.singular.SingularConstants;
import com.singular.SingularException;
import com.singular.core.ContainerResourceHelper;
import com.singular.core.PathNamePair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

import java.util.List;

/**
 * @author Rahul Bhattacharjee
 */
public class ContainerRunnable implements Runnable {

    private Container container;
    private String className;
    private ContainerResourceHelper containerResourceHelper;
    private Path path;

    /**
     * ContainerRunnable
     *
     * @param container
     * @param runnableClass
     */
    public ContainerRunnable(Container container , String runnableClass,Path stagingPath) {
        this.className = runnableClass;
        this.container = container;
        this.containerResourceHelper = new ContainerResourceHelper(container);
        this.path = stagingPath;
    }

    @Override
    public void run() {
        System.out.println("Running container " + container);
        try {
            ContainerLaunchContext containerLaunchContext = containerResourceHelper.createContainerLaunchContext(container,
                    SingularConstants.KEY_DEFAULT_CONTAINER_LAUNCHER,className,path);

            containerResourceHelper.starContainer(containerLaunchContext);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Container failed " + container.getId() , e);
        }
    }
}
