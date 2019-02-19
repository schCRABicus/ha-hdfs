package com.epam.bigdata.training.client.am;

import com.epam.bigdata.training.client.Constants;
import com.epam.bigdata.training.client.LaunchConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Application master container launch context factory.
 */
public class AMContainerContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AMContainerContextFactory.class);

    public static ContainerLaunchContext build(ApplicationId appId, YarnConfiguration yarnConfiguration, LaunchConfiguration conf) throws IOException {
        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(yarnConfiguration);
        addToLocalResources(fs, conf.getAppMasterJarPath(), Constants.AM_JAR_NAME, appId.getId(), conf.getAppName(),
                localResources, null);
        addToLocalResources(fs, conf.getAppJarPath(), Constants.APP_JAR_NAME, appId.getId(), conf.getAppName(),
                localResources, null);

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Setup the environment for the application master");
        Map<String, String> env = getAMEnvironment(localResources, fs, yarnConfiguration, conf);

        // Set the necessary command to execute the application master
        final String command = AMLaunchCommandFactory.build(conf);

        LOG.info("Completed setting up app master command " + command);
        List<String> commands = Collections.singletonList(command);

        // Set up the container launch context for the application master
        return ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

//        // Service data is a binary blob that can be passed to the application
//        // Not needed in this scenario
//        // amContainer.setServiceData(serviceData);
//
//        // Setup security tokens
//        if (UserGroupInformation.isSecurityEnabled()) {
//            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
//            Credentials credentials = new Credentials();
//            String tokenRenewer = yarnConfiguration.get(YarnConfiguration.RM_PRINCIPAL);
//            if (tokenRenewer == null || tokenRenewer.length() == 0) {
//                throw new IOException(
//                        "Can't get Master Kerberos principal for the RM to use as renewer");
//            }
//
//            // For now, only getting tokens for the default file-system.
//            final Token<?> tokens[] =
//                    fs.addDelegationTokens(tokenRenewer, credentials);
//            if (tokens != null) {
//                for (Token<?> token : tokens) {
//                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
//                }
//            }
//            DataOutputBuffer dob = new DataOutputBuffer();
//            credentials.writeTokenStorageToStream(dob);
//            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
//            amContainer.setTokens(fsTokens);
//        }
//
//        // Set up the container launch context for the application master
//        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
//                localResources, env, commands, null, null, null);
    }

    private static void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, int appId, String appName, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(/*fs.getHomeDirectory()*/"/", suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    private static Map<String, String> getAMEnvironment(
            Map<String, LocalResource> localResources, FileSystem fs, YarnConfiguration yarnConfiguration, LaunchConfiguration conf) throws IOException{

        Map<String, String> env = new HashMap<>();

        // Set ApplicationMaster jar file
        LocalResource appMasterJarResource = localResources.get(Constants.AM_JAR_NAME);
        Path hdfsAppMasterJarPath = new Path(fs.getHomeDirectory(), appMasterJarResource.getResource().getFile());
        FileStatus hdfsAppMasterJarStatus = fs.getFileStatus(hdfsAppMasterJarPath);
        long hdfsAppMasterJarLength = hdfsAppMasterJarStatus.getLen();
        long hdfsAppMasterJarTimestamp = hdfsAppMasterJarStatus.getModificationTime();

        env.put(Constants.AM_JAR_PATH, hdfsAppMasterJarPath.toString());
        env.put(Constants.AM_JAR_TIMESTAMP, Long.toString(hdfsAppMasterJarTimestamp));
        env.put(Constants.AM_JAR_LENGTH, Long.toString(hdfsAppMasterJarLength));

        // Set Application logic jar file
        LocalResource appJarResource = localResources.get(Constants.APP_JAR_NAME);
        Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
        FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
        long hdfsAppJarLength = hdfsAppJarStatus.getLen();
        long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

        env.put(Constants.APP_JAR_PATH, hdfsAppJarPath.toString());
        env.put(Constants.APP_JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
        env.put(Constants.APP_JAR_LENGTH, Long.toString(hdfsAppJarLength));

        env.put(Constants.APP_NAME, conf.getAppName());

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : yarnConfiguration.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());

        return env;
    }
}
