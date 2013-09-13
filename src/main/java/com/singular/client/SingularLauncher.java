package com.singular.client;

import com.singular.SingularConstants;
import com.singular.SingularException;
import com.singular.api.JobSpecification;
import com.singular.core.ClientResourceManagerHelper;
import com.singular.core.NodeCapacity;
import com.singular.core.PathNamePair;
import com.singular.core.RunContext;
import com.singular.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Singular launcher , entry point for clients to this api to configure and launch singular runnable.
 *
 * @author Rahul Bhattacharjee
 */
public class SingularLauncher {

    private final static String DEFAULT_APP_MASTER_CLASS = "com.singular.core.master.DefaultApplicationMaster";
    private final static String APP_JAR = "application.jar";

    private JobSpecPair job;
    private ClientResourceManagerHelper clientResourceManagerHelper = null;
    private ApplicationId applicationId = null;
    private Configuration configuration = null;

    public SingularLauncher() {
        clientResourceManagerHelper = new ClientResourceManagerHelper();
    }

    public void addJob(Class<? extends Runnable> task , JobSpecification config) {
        job = new JobSpecPair(task,config);
    }

    public void submit() {
        try {
            this.configuration = new Configuration();

            System.out.println("HDFS base location => " + FileUtils.getHdfsBaseLocation(configuration));

            ClientResourceManagerHelper clientResourceManagerHelper = new ClientResourceManagerHelper(configuration);
            applicationId = clientResourceManagerHelper.getNewApplicationId();

            Path remotePath = FileUtils.getPathForSystem(FileUtils.getHdfsBaseLocation(configuration), applicationId.toString());
            Path localPath = FileUtils.getCurrentJar(this.getClass());

            Path completeRemotePath = FileUtils.transferToSystem(configuration,localPath,remotePath,APP_JAR);
            Properties config = new Properties();

            prepareConfiguration(config,remotePath);
            Path completeRemoteConfigPath = FileUtils.persistConfigurationInHDFS(configuration, config , remotePath , SingularConstants.CONFIG_FILE_NAME);

            System.out.println("Remote path " + remotePath);

            RunContext context = new RunContext().setQueueName("default").setApplicationBasePath(remotePath);

            ApplicationSubmissionContext submissionContext = clientResourceManagerHelper.getAppSubmissionContext(applicationId,job.getSpecification().getApplicationName(),
                    context,DEFAULT_APP_MASTER_CLASS,new NodeCapacity());
            SubmitApplicationResponse response = clientResourceManagerHelper.submitApplication(submissionContext);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while launching application with id " + applicationId,e);
        }
    }

    private void prepareConfiguration(Properties config, Path remotePath) {
        config.put(SingularConstants.KEY_NO_OF_CONTAINERS ,job.getSpecification().getNoOfContainers()+"");
        config.put(SingularConstants.KEY_NO_OF_CORES,job.getSpecification().getCores()+"");
        config.put(SingularConstants.KEY_MEM, job.getSpecification().getMemory()+"");
        config.put(SingularConstants.KEY_CONTAINER_CLASS,job.getRun().getCanonicalName()+"");
        config.put(SingularConstants.KEY_APPLICATION_STAGING_PATH,remotePath.toString());
    }

    public void submitAndWaitForCompletion(long pollInternal , TimeUnit unit) {
        this.submit();
        while(!this.isComplete()){
            try {
                unit.sleep(pollInternal);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isComplete() {
        JobStatus status = getStatus();
        if(status == JobStatus.COMPLETED || status == JobStatus.FAILED || status == JobStatus.KILLED)
            return true;
        else
            return false;
    }

    public JobStatus getStatus() {
        if(clientResourceManagerHelper == null){
           clientResourceManagerHelper = new ClientResourceManagerHelper();
        }
        YarnApplicationState applicationState =  clientResourceManagerHelper.getApplicationReport(applicationId).
                getApplicationReport().getYarnApplicationState();

        JobStatus status = null;
        switch (applicationState) {
            case FAILED: status = JobStatus.FAILED;
                break;
            case FINISHED: status = JobStatus.COMPLETED;
                break;
            case SUBMITTED:status = JobStatus.RUNNING;
                break;
            case KILLED:status = JobStatus.KILLED;
                break;
            case ACCEPTED:status = JobStatus.UNDEFINED;
                break;
            default:status = JobStatus.UNDEFINED;
        }
        return status;
    }

    /**
     * More of a bean class for a job.
     */
    private static class JobSpecPair {
        private Class run;
        private JobSpecification specification;

        private JobSpecPair(Class run, JobSpecification specification) {
            this.run = run;
            this.specification = specification;
        }

        private Class getRun() {
            return run;
        }

        private JobSpecification getSpecification() {
            return specification;
        }
    }
}
