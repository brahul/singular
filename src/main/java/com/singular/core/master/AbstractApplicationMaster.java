package com.singular.core.master;

import com.singular.SingularException;
import com.singular.core.AMResourceManagerHelper;
import com.singular.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.Map;
import java.util.Properties;

/**
 * Base abstract application master for other abstract master implementations.
 *
 * @author Rahul Bhattacharjee
 */
public abstract class AbstractApplicationMaster implements Runnable {

    private Configuration configuration;
    private AMResourceManagerHelper amResourceManagerHelper;
    private ApplicationAttemptId applicationAttemptId;

    public AbstractApplicationMaster()  {
        this(new Configuration());
    }

    public AbstractApplicationMaster(Configuration configuration) {
        this.configuration = configuration;
        this.amResourceManagerHelper = new AMResourceManagerHelper(configuration);
        Map<String, String> envs = System.getenv();
        String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
        if (containerIdString == null) {
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        applicationAttemptId = containerId.getApplicationAttemptId();
    }

    public void register() {
        try {
            this.amResourceManagerHelper.registerApplicationMaster(this.applicationAttemptId);
        } catch (Exception e) {
            throw new SingularException("Exception while registering application.",e);
        }
    }

    public void deRegister(boolean isSuccess)  {
        try {
            this.amResourceManagerHelper.finishApplicationRequest(applicationAttemptId,isSuccess);
        }catch (Exception e) {
            throw new SingularException("Exception while de registering.",e);
        }
    }

    public abstract void launchContainers() throws Exception;

    @Override
    public void run() {
        boolean isSuccess = true;
        this.register();
        try {
            this.launchContainers();
        } catch (Exception e) {
            e.printStackTrace();
            isSuccess = false;
            System.out.print("Exception while launching application master.");
        } finally {
            this.deRegister(isSuccess);
        }
    }

    protected AMResourceManagerHelper getAMResourceHelper() {
        return this.amResourceManagerHelper;
    }

    protected ApplicationAttemptId getApplicationAttemptId() {
        return this.applicationAttemptId;
    }

    protected Properties getJobConfiguration() {
        String base = FileUtils.getHdfsBaseLocation(getConfiguration());
        String applicationId = applicationAttemptId.getApplicationId().toString();
        Path configPath = new Path(base+applicationId);
        return FileUtils.retrieveConfigurationFromHDFS(this.configuration, configPath , "config.out");
    }

    protected Configuration getConfiguration(){
        return this.configuration;
    }
}
