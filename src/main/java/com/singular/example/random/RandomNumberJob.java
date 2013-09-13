package com.singular.example.random;

import com.singular.api.JobSpecification;
import com.singular.client.SingularLauncher;

import java.util.concurrent.TimeUnit;

/**
 * It would generate random numbers .
 *
 * @author Rahul Bhattacharjee
 */
public class  RandomNumberJob {

    /**
     * Example for running multiple jobs with different specification using singular launcher.
     *
     * @param argv
     */
    public static void main(String [] argv) {

        SingularLauncher launcher = new SingularLauncher();

        JobSpecification specificationOne = new JobSpecification().setMemory(100).setCores(1)
                .setNumberOfContainers(5).setApplicationName("test_app");

        launcher.addJob(RandomNumberTask.class,specificationOne);

        JobSpecification specificationTwo = new JobSpecification().setMemory(100).setCores(1).setNumberOfContainers(2);
        launcher.addJob(RandomNumberTask.class, specificationTwo);

        launcher.submitAndWaitForCompletion(2 , TimeUnit.SECONDS);

        System.out.println("Submitted and completed the job.");

    }
}
