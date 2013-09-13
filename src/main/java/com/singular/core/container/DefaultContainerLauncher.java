package com.singular.core.container;

import com.singular.SingularException;

/**
 * @author Rahul Bhattacharjee
 */
public class DefaultContainerLauncher {

    private String className;

    public DefaultContainerLauncher(){}

    public DefaultContainerLauncher(String className){
        this.className = className;
    }

    public static void main(String [] argv) {
        DefaultContainerLauncher defaultContainerLauncher = new DefaultContainerLauncher(argv[0]);
        defaultContainerLauncher.launchContainer();
    }

    public void launchContainer() {
        try {
            Class classInstance = Class.forName(className);
            Object obj = classInstance.newInstance();

            if(!(obj instanceof Runnable)) {
                throw new SingularException(className +" is not instance of Runnable.");
            }

            Runnable runnable = (Runnable) obj;
            runnable.run();

        } catch (Exception e) {
            System.out.println("Exception while launching container.");
            throw new SingularException("Exception while launching container." ,e);
        }
    }
}
