package com.singular.example.random;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Will generate random numbers in sequence with some delay.
 *
 * @author Rahul Bhattacharjee
 */
public class RandomNumberTask implements Runnable {

    private Random RANDOM = new Random(new Object().hashCode());

    @Override
    public void run() {
        System.out.println("Printing random numbers with delay.");
        for(int i = 0 ; i < 10 ; i++) {
            System.out.println(i+ " value = " + RANDOM.nextInt(100));
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
