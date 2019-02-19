package com.epam.bigdata.training.appmaster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LaunchConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchConfiguration.class);

    // Application Attempt Id ( combination of attemptId and fail count )
    private ApplicationAttemptId appAttemptID;

    /**
     * Default file system url.
     */
    private String defaultFs;

    /**
     * Resource manager address.
     */
    private String rmAddress;

    // No. of containers to run shell command on
    private int numTotalContainers = 1;

    // Memory to request for the container
    private int containerMemory = 10;

    // VirtualCores to request for the container
    private int containerVirtualCores = 1;

    // Priority of the request
    private int requestPriority;

    // Location of jar file ( obtained from info set in env )
    // App jar file path in fs
    private String appJarPath = "";

    // Timestamp needed for creating a local resource
    private long appJarTimestamp = 0;

    // File length needed for local resource
    private long appJarPathLen = 0;

    private String appName;

    /**
     * Application main class to call in container.
     */
    private String appMainClass = "";

    /**
     * Path to application input.
     */
    private String appInputPath;

    /**
     * Path to application output.
     */
    private String appOutputPath;

    private Options opts;

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws org.apache.commons.cli.ParseException
     * @throws java.io.IOException
     */
    public boolean init(String[] args) throws Exception {
        opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("default_fs", true, "Default file system. Mandatory.");
        opts.addOption("rm_address", true, "Resource manager address. Mandatory.");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the application jar");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the application jar");
        opts.addOption("num_containers", true,
                "Number of containers on which the application jar needs to be executed");
        opts.addOption("app_jar_path", true,
                "Application jar path");
        opts.addOption("app_main_class", true,
                "Application main class");
        opts.addOption("app_input_path", true, "Path to application input");
        opts.addOption("app_output_path", true, "Path to application output");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("help", false, "Print usage");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ApplicationAttemptId.fromString(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Neither Container Id nor Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ContainerId.fromString(envs
                    .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name() + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name() + " not set in the environment");
        }

        if (envs.containsKey(Constants.APP_JAR_PATH)) {
            appJarPath = envs.get(Constants.APP_JAR_PATH);
        }

        if (envs.containsKey(Constants.APP_JAR_TIMESTAMP)) {
            appJarTimestamp = Long.valueOf(envs.get(Constants.APP_JAR_TIMESTAMP));
        }

        if (envs.containsKey(Constants.APP_JAR_LENGTH)) {
            appJarPathLen = Long.valueOf(envs.get(Constants.APP_JAR_LENGTH));
        }

        appName = envs.get(Constants.APP_NAME);
        if (appJarPath.isEmpty() || appJarTimestamp <= 0 || appJarPathLen <= 0) {
            LOG.error("Illegal values in env for application jar path" + ", path="
                    + appJarPath + ", len=" + appJarPathLen + ", timestamp="+ appJarTimestamp);
            throw new IllegalArgumentException(
                    "Illegal values in env for application jar path");
        }

        appMainClass = cliParser.getOptionValue("app_main_class");
        if (StringUtils.isEmpty(appMainClass)) {
            throw new IllegalArgumentException("Cannot run Application without main class specified");
        }

        defaultFs = cliParser.getOptionValue("default_fs");
        rmAddress = cliParser.getOptionValue("rm_address");

        if (StringUtils.isEmpty(defaultFs)) {
            throw new IllegalArgumentException("Default fs must be specified");
        }
        if (StringUtils.isEmpty(rmAddress)) {
            throw new IllegalArgumentException("Default fs must be specified");
        }

        if (!cliParser.hasOption("app_input_path")) {
            throw new IllegalArgumentException("No app input specified");
        }

        if (!cliParser.hasOption("app_output_path")) {
            throw new IllegalArgumentException("No app output specified");
        }
        appInputPath = cliParser.getOptionValue("app_input_path");
        appOutputPath = cliParser.getOptionValue("app_output_path");

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clusterTimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run Application with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        return true;
    }

    /**
     * Helper function to print out usage
     */
    public void printUsage() {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    public ApplicationAttemptId getAppAttemptID() {
        return appAttemptID;
    }

    public int getNumTotalContainers() {
        return numTotalContainers;
    }

    public int getContainerMemory() {
        return containerMemory;
    }

    public int getContainerVirtualCores() {
        return containerVirtualCores;
    }

    public int getRequestPriority() {
        return requestPriority;
    }

    public String getAppJarPath() {
        return appJarPath;
    }

    public String getAppMainClass() {
        return appMainClass;
    }

    public long getAppJarTimestamp() {
        return appJarTimestamp;
    }

    public long getAppJarPathLen() {
        return appJarPathLen;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppAttemptID(ApplicationAttemptId appAttemptID) {
        this.appAttemptID = appAttemptID;
    }

    public void setNumTotalContainers(int numTotalContainers) {
        this.numTotalContainers = numTotalContainers;
    }

    public void setContainerMemory(int containerMemory) {
        this.containerMemory = containerMemory;
    }

    public void setContainerVirtualCores(int containerVirtualCores) {
        this.containerVirtualCores = containerVirtualCores;
    }

    public void setRequestPriority(int requestPriority) {
        this.requestPriority = requestPriority;
    }

    public void setAppJarPath(String appJarPath) {
        this.appJarPath = appJarPath;
    }

    public void setAppJarTimestamp(long appJarTimestamp) {
        this.appJarTimestamp = appJarTimestamp;
    }

    public void setAppJarPathLen(long appJarPathLen) {
        this.appJarPathLen = appJarPathLen;
    }

    public String getDefaultFs() {
        return defaultFs;
    }

    public String getRmAddress() {
        return rmAddress;
    }

    public String getAppInputPath() {
        return appInputPath;
    }

    public String getAppOutputPath() {
        return appOutputPath;
    }

    public void setDefaultFs(String defaultFs) {
        this.defaultFs = defaultFs;
    }

    public void setRmAddress(String rmAddress) {
        this.rmAddress = rmAddress;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setAppMainClass(String appMainClass) {
        this.appMainClass = appMainClass;
    }

    public void setAppInputPath(String appInputPath) {
        this.appInputPath = appInputPath;
    }

    public void setAppOutputPath(String appOutputPath) {
        this.appOutputPath = appOutputPath;
    }
}
