package com.singular.core;

import com.singular.SingularException;
import com.singular.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Application master related helper.
 *
 * @author Rahul Bhattacharjee
 */
public class AMResourceManagerHelper {

    private final Configuration configuration;
    private AMRMProtocol resourceManager;

    private final Lock lock = new ReentrantLock();

    public AMResourceManagerHelper(Configuration configuration) {
        this.configuration = configuration;
        this.resourceManager = getResourceManager(configuration);
    }

    private AMRMProtocol getResourceManager(Configuration conf) {
        AMRMProtocol resourceManager = null;
        try{
            YarnConfiguration yarnConf = new YarnConfiguration(conf);
            InetSocketAddress rmAddress =
                    NetUtils.createSocketAddr(yarnConf.get(
                            YarnConfiguration.RM_SCHEDULER_ADDRESS,
                            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));

            YarnRPC rpc = YarnRPC.create(yarnConf);
            resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while creating Resource manager.",e);
        }
        return resourceManager;
    }

    public AllocateResponse allocate(AllocateRequest allocateRequest) {
        lock.lock();
        try {
            return resourceManager.allocate(allocateRequest);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while allocating containers." ,e);
        } finally {
            lock.unlock();
        }
    }

    public void registerApplicationMaster(ApplicationAttemptId attemptId) throws IOException{
        final int DEFAULT_RPC = -9999;
        final String DEFAULT_SITE = "www.singular.com";

        try{
            RegisterApplicationMasterRequest applicationMasterRequest =
                    Records.newRecord(RegisterApplicationMasterRequest.class);
            applicationMasterRequest.setApplicationAttemptId(attemptId);
            applicationMasterRequest.setHost(FileUtils.getCurrentHost());
            applicationMasterRequest.setRpcPort(DEFAULT_RPC);
            applicationMasterRequest.setTrackingUrl(DEFAULT_SITE);

            RegisterApplicationMasterResponse applicationMasterResponse = null;

            lock.lock();
            try {
                resourceManager.registerApplicationMaster(applicationMasterRequest);
            }finally {
                lock.unlock();
            }
            System.out.println("AM registration response " + applicationMasterResponse);
        }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while registering with RM.",e);
        }
    }

    public void finishApplicationRequest(ApplicationAttemptId attemptId, boolean isSuccess)  throws IOException{
        try {
            FinishApplicationMasterRequest request = Records.newRecord(FinishApplicationMasterRequest.class);
            request.setAppAttemptId(attemptId);
            if(isSuccess) {
                request.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
            } else {
                request.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
            }
            lock.lock();
            try {
                resourceManager.finishApplicationMaster(request);
            }finally {
                lock.unlock();
            }
         }catch (Exception e){
            e.printStackTrace();
            throw new SingularException("Exception while finishing application.",e);
        }
    }
}

