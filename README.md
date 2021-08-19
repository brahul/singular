IMPORTANT PR UPDATE

singular
========

Singular is a weekend effort to make a high level library on top of Apache YARN which will enable writing distributed
applications easier. The way Java has made multi threaded programming much easier. Similarly it would be nice to 
write distributed programs with the same ease.


Just define - Job specification with number of containers you would like to run your application on and define the 
requirement of your container.Like memory and number of cores that you wish for.

The example explains it all -

https://github.com/brahul/singular/blob/master/src/main/java/com/singular/example/random/RandomNumberJob.java

Run : hadoop jar singular-1.0-SNAPSHOT.jar com.singular.example.random.RandomNumberJob

Note:
1. Currently it runs only in non secure yarn clusters.Auth is not integrated.
2. Based on Hadoop - 2.0.6

Wish list -
+ Logging in hdfs.
+ Integrate with a embedded web container in the application master for serving the logs for that application.
+ Mechanish through which various containers can work together and sync.
+ Way to pass configuration from the launcher to all the way to the containers.
+ Refacator the code.
+ Unit test
+ KERBEROS authentication.
+ Elasticity policy so that the containers can expand or shrink based on some elasticity policy.
