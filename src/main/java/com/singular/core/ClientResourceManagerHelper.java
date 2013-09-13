package com.singular.core;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.singular.SingularException;
import com.singular.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper to talk to RM and submit jobs and submit a application master.
 *
 * @author Rahul Bhattacharjee
 */
public class ClientResourceManagerHelper {

    private ClientRMProtocol resourceManager;
    private Configuration configuration;

    public ClientResourceManagerHelper(Configuration configuration) {
        this.configuration = configuration;
        try{
            resourceManager = getApplicationManager(configuration);
        }catch (Exception e) {
            throw new SingularException("Exception while creating resource manager.",e);
        }
    }

    public ClientResourceManagerHelper(){
        this(new Configuration());
    }

    public ApplicationId getNewApplicationId() {
        ApplicationId appId = null;
        try {
            GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
            GetNewApplicationResponse response = resourceManager.getNewApplication(request);

            appId = response.getApplicationId();
            System.out.println("Got new ApplicationId =" + appId);
        }catch (Exception e){
            throw new SingularException("Could not get a application id from RM.",e);
        }
        return appId;
    }

    public ClientRMProtocol getApplicationManager(Configuration configuration) {
        ClientRMProtocol resourceManager = null;
        try {
            YarnConfiguration yarnConf = new YarnConfiguration(configuration);
            InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS,
                                                                                    YarnConfiguration.DEFAULT_RM_ADDRESS));
            Configuration appsManagerServerConf = new Configuration(configuration);

            YarnRPC rpc = YarnRPC.create(yarnConf);
            resourceManager = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));

            System.out.println("Resource manager's proxy " + resourceManager);
        }catch (Exception e){
            throw new SingularException("Exception while establishing connection with AM." ,e);
        }
        return resourceManager;
    }

    public SubmitApplicationResponse submitApplication(ApplicationSubmissionContext submissionContext) {
        SubmitApplicationResponse response = null;
        try{
            SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
            appRequest.setApplicationSubmissionContext(submissionContext);
            response = resourceManager.submitApplication(appRequest);
        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while submitting application.",e);
        }
        return response;
    }

    public ApplicationSubmissionContext getAppSubmissionContext(ApplicationId appId, String appName,
                                                                RunContext runContext ,
                                                                String appMasterClassName, NodeCapacity capacity) throws Exception {
        ApplicationSubmissionContext context = null;
        try {
            Path applicationStagingPath = runContext.getApplicationBasePath();
            context = Records.newRecord(ApplicationSubmissionContext.class);
            context.setApplicationId(appId);
            context.setApplicationName(appName);
            context.setQueue(runContext.getQueueName());

            ContainerLaunchContext container = Records.newRecord(ContainerLaunchContext.class);
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            FileSystem fileSystem = FileSystem.get(configuration);

            addResource(new Path(applicationStagingPath,"application.jar"),fileSystem,localResources);
            addResource(new Path(applicationStagingPath,"config.out"),fileSystem,localResources);

            container.setLocalResources(localResources);

            Map<String, String> env = new HashMap<String, String>();
            env.putAll(System.getenv());

            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./application.jar");
            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./config.out");

            for (String classpath : configuration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), classpath);
            }
            container.setEnvironment(env);
            container.setCommands(ImmutableList.of(getCommand(appMasterClassName)));

            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(capacity.getMemory());
            container.setResource(capability);

            context.setAMContainerSpec(container);

            System.out.println("Context  created is " + context);

        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while creating application context for application master.",e);
        }
        return context;
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

    private String getCommand(String appMasterClassName) {
        if(appMasterClassName == null) {
            throw new IllegalArgumentException("Application master classname cannot be null");
        }
        ImmutableList<String> commandParts
                = new ImmutableList.Builder<String>().add("${JAVA_HOME}/bin/java").add(appMasterClassName)
                                                     .add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
                                                     .add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
                                                     .build();
        Joiner joiner = Joiner.on(' ');
        String command = joiner.join(commandParts);
        System.out.println("Command = " + command );
        return command;
    }

    private void uploadToStagingDirectory(Class jarOfClass ,FileSystem fileSystem ,Map<String, LocalResource> localResources ,Path remoteStagingDir , String nameOfResource) {
        try {
            Path jarPath = FileUtils.getCurrentJar(jarOfClass);
            FileStatus status = fileSystem.getFileStatus(jarPath);
            LocalResource jarSource = Records.newRecord(LocalResource.class);
            jarSource.setType(LocalResourceType.FILE);
            jarSource.setVisibility(LocalResourceVisibility.APPLICATION);

            URL url = ConverterUtils.getYarnUrlFromPath(jarPath);

            jarSource.setResource(url);

            long length = status.getLen();

            jarSource.setTimestamp(status.getModificationTime());
            jarSource.setSize(length);

            System.out.println("Adding resource url " + url + " of size " + length);
            localResources.put(nameOfResource, jarSource);
        }catch (Exception e){
            throw new SingularException("Exception while uploading to remote path " + remoteStagingDir ,e);
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

                System.out.println("Adding resource url " + url + " of size " + length);

                localResources.put(namePair.getResourceName(), jarSource);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while adding system runtime resources to staging.",e);
        }
    }

    public GetApplicationReportResponse getApplicationReport(ApplicationId applicationId) {
        GetApplicationReportResponse reportResponse = null;
        try{
            GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
            reportRequest.setApplicationId(applicationId);
            reportResponse = resourceManager.getApplicationReport(reportRequest);
        }catch (Exception e){
            throw new SingularException("Exception while getting application report.",e);
        }
        return reportResponse;
    }

    public boolean killApplication(ApplicationId applicationId) throws Exception {
        KillApplicationResponse response = null;
        try{
            KillApplicationRequest killApplicationRequest = Records.newRecord(KillApplicationRequest.class);
            killApplicationRequest.setApplicationId(applicationId);
            response = resourceManager.forceKillApplication(killApplicationRequest);
        } catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while killing the application " + applicationId , e);
        }
        return response != null;
    }
}
