package com.singular.utils;

import com.singular.SingularConstants;
import com.singular.SingularException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Fs related utility.
 *
 * @author Rahul Bhattacharjee
 */
public class FileUtils {

    public static Path getPathForSystem(String systemDir, String appId) {
        return new Path(systemDir + "/" + appId);
    }

    public static Path getCurrentJar(Class applicationClass) {
        String jarUrl = findContainingJar(applicationClass);
        if(jarUrl == null)
            throw new NullPointerException(applicationClass + " non existing in the classpath.");
        return new Path(jarUrl);
    }

    public static String getCurrentHost() {
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostname;
    }

    public static Path transferToSystem(Configuration configuration, Path localPath, Path remoteDir,String remoteFileName){
        Path finalRemotePath = null;
        InputStream in = null;
        OutputStream out = null;

        System.out.println("Transferring local " + localPath + " ,remoteStaging " + remoteDir + " ,fileNam=" + remoteFileName);

        try {
            FileSystem localFs = FileSystem.getLocal(configuration);
            FileSystem remoteFs = FileSystem.get(configuration);

            in = localFs.open(localPath);
            finalRemotePath = new Path(remoteDir,remoteFileName);
            out = remoteFs.create(finalRemotePath);
            copyStream( in ,out);
        }catch (Exception e){
            throw new IllegalArgumentException("Exception while transferring file." ,e);
        }finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return finalRemotePath;
    }

    public static Path persistConfigurationInHDFS(Configuration configuration, Properties conf , Path remoteBasePath , String fileName) {
        OutputStream out = null;
        Path remotePath = null;
        try {
            remotePath = new Path(remoteBasePath,fileName);
            FileSystem remoteFs = FileSystem.get(configuration);
            out = remoteFs.create(remotePath);
            conf.store(out,null);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while persisting configuration.",e);
        }finally{
            try {
                if(out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return remotePath;
    }

    public static Properties retrieveConfigurationFromHDFS(Configuration configuration, Path remoteBasePath , String fileName) {
        Properties conf = new Properties();
        InputStream in = null;
        try{
            Path remoteConfigPath = new Path(remoteBasePath,fileName);
            FileSystem remoteFs = FileSystem.get(configuration);
            in = remoteFs.open(remoteConfigPath);
            conf.load(in);
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while retrieving singular configuration from hdfs.",e);
        }finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return conf;
    }

    public static void copyStream(InputStream in , OutputStream out) {
        try {
            IOUtils.copy(in,out);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while copying stream.",e);
        }
    }

    /**
     * Taken from apache hadoop project.
     * Apache 2.0 license.
     *
     * @param className
     * @return
     */
    public static String findContainingJar(Class className) {
        ClassLoader loader = className.getClassLoader();
        String classFile = className.getName().replaceAll("\\.", "/") + ".class";

        try {
            for(Enumeration itr = loader.getResources(classFile);
                itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();

                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = toReturn.replaceAll("\\+", "%2B");
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static String stripFilename(String path) {
        int last = path.lastIndexOf('/');
        String name = path.substring(last+1);
        return name;
    }

    public static OutputStream getPathForStoringConfiguration(Path path,Configuration configuration) throws Exception{
        FileSystem remoteFs = FileSystem.get(configuration);
        return remoteFs.create(path);
    }

    public static String getHdfsBaseLocation(Configuration configuration) {
        String hdfsBase = configuration.get("fs.default.name");
        return hdfsBase + SingularConstants.KEY_STAGING_DIR + "/";
    }

    public static DataInputStream getJobConfiguration(Configuration configuration,String path) {
        try{
            FileSystem fs = FileSystem.get(configuration);
            return fs.open(new Path(path));
        }catch (Exception e) {
            e.printStackTrace();
            throw new SingularException("Exception while deser-ing job configuration.",e);
        }
    }
}

