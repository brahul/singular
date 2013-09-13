package com.singular.core;

import com.singular.SingularException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Helper to be used by the various containers.
 *
 * @author Rahul Bhattacharjee
 */
public class ContainerResourceHelper {

    private ContainerManager containerManager;
    private Configuration configuration;
    private Container container;

    public ContainerResourceHelper(Container container) {
        this.container = container;
        this.configuration = new Configuration();
        this.containerManager = getContainerManager();
    }

    private ContainerManager getContainerManager() {
        ContainerManager containerManager = null;
        try {
            String containerUrl = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
            InetSocketAddress containerManagersUrl = NetUtils.createSocketAddr(containerUrl);

            YarnConfiguration yarnConf = new YarnConfiguration(configuration);
            YarnRPC rpc = YarnRPC.create(yarnConf);
            containerManager =  (ContainerManager) rpc.getProxy(ContainerManager.class, containerManagersUrl , configuration);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while creating container manager.",e);
        }
        return containerManager;
    }

    public StartContainerResponse starContainer(ContainerLaunchContext containerLaunchContext) throws Exception {
        Lock lock = new ReentrantLock();
        StartContainerRequest startContainerRequest = Records.newRecord(StartContainerRequest.class);
        startContainerRequest.setContainerLaunchContext(containerLaunchContext);
        StartContainerResponse response = null;

        lock.lock();
        try {
            response = containerManager.startContainer(startContainerRequest);
        }finally {
            lock.unlock();
        }
        return response;
    }

    public ContainerLaunchContext createContainerLaunchContext(Container container ,String containerClass ,String className,Path applicationStagingPath) {
        ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);

        try {
            containerLaunchContext.setContainerId(container.getId());
            containerLaunchContext.setResource(container.getResource());
            containerLaunchContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            FileSystem fileSystem = FileSystem.get(configuration);

            containerLaunchContext.setLocalResources(localResources);

            addResource(new Path(applicationStagingPath,"application.jar"),fileSystem,localResources);
            addResource(new Path(applicationStagingPath,"config.out"),fileSystem,localResources);

            Map<String, String> env = new HashMap<String, String>();
            env.putAll(System.getenv());

            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./application.jar");
            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./config.out");

            for (String c : configuration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
            }

            containerLaunchContext.setEnvironment(env);

            String command = getCommand(className,containerClass);
            List<String> commands = new ArrayList<String>();
            commands.add(command);

            containerLaunchContext.setCommands(commands);

        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while creating container launch context.",e);
        }
        return containerLaunchContext;
    }

    private String getCommand(String appMasterClassName,String containerLauncherClass) {
        if(appMasterClassName == null) {
            throw new IllegalArgumentException("Application master classname cannot be null");
        }
        String command =
                "${JAVA_HOME}" + "/bin/java " +
                         containerLauncherClass + " "
                        + appMasterClassName +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        return command;
    }

    private void addResource(Path path , FileSystem fileSystem ,Map<String,LocalResource> resources) {
        try {
            FileStatus status = fileSystem.getFileStatus(path);
            LocalResource jarSource = Records.newRecord(LocalResource.class);
            jarSource.setType(LocalResourceType.FILE);
            jarSource.setVisibility(LocalResourceVisibility.APPLICATION);

            URL url = ConverterUtils.getYarnUrlFromPath(path);

            jarSource.setResource(url);
            long length = status.getLen();
            jarSource.setTimestamp(status.getModificationTime());
            jarSource.setSize(length);
            resources.put(path.getName(), jarSource);
        }catch (Exception e) {
            throw new SingularException("Exception while adding resource.",e);
        }
    }


    private void addSystemRuntimeResources(FileSystem fs, List<PathNamePair> pathNamePairs, Map<String, LocalResource> localResources) {
        try{
            for(PathNamePair namePair : pathNamePairs) {
                System.out.println("Adding " + namePair);

                Path jarPath = namePair.getPath();
                FileStatus status = fs.getFileStatus(jarPath);
                LocalResource jarSource = Records.newRecord(LocalResource.class);
                jarSource.setType(LocalResourceType.FILE);
                jarSource.setVisibility(LocalResourceVisibility.APPLICATION);

                URL url = ConverterUtils.getYarnUrlFromPath(jarPath);
                jarSource.setResource(url);

                long length = status.getLen();
                jarSource.setTimestamp(status.getModificationTime());
                jarSource.setSize(length);

                localResources.put(namePair.getResourceName(), jarSource);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while adding system runtime resources to staging.",e);
        }
    }
}
