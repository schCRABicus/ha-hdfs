package com.epam.bigdata.training.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Launch configuration defining basic settings required to submit the application for the application master.
 */
public class LaunchConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchConfiguration.class);

    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";

    /**
     * Default file system url.
     */
    private String defaultFs;

    /**
     * Resource manager address.
     */
    private String rmAddress;

    // App master priority
    private int amPriority = 0;

    // Queue for App master
    private String amQueue = "";

    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 10;

    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;

    // ApplicationMaster jar file
    private String appMasterJarPath = "";

    // ApplicationMaster main class
    private String appMasterMainClass = "";

    // Application logic jar file
    private String appJarPath = "";

    // Application main class
    private String appMainClass = "";

    /**
     * Path to application input.
     */
    private String appInputPath;

    /**
     * Path to application output.
     */
    private String appOutputPath;

    // Container priority
    private int requestPriority = 0;

    // Amount of memory to request for container in which the HelloYarn will be executed
    private int containerMemory = 10;

    // Amount of virtual cores to request for container in which the HelloYarn will be executed
    private int containerVirtualCores = 1;

    // Number of containers in which the HelloYarn needs to be executed
    private int numContainers = 1;

    // Timeout threshold for client. Kill app after time interval expires.
    private long clientTimeout = 600_000;

    // Command line options
    private Options opts;

    /**
     * Parse command line options
     * @param args Parsed command line options
     * @return Whether the init was successful to nitialize launch configuration with all mandatory options.
     * @throws org.apache.commons.cli.ParseException if arguments are invalid.
     */
    public boolean init(String[] args) throws ParseException {
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - HotelsYarnApplication");
        opts.addOption("default_fs", true, "Default file system. Mandatory.");
        opts.addOption("rm_address", true, "Resource manager address. Mandatory.");
        opts.addOption("appname", true, "Application Name. Default value - HelloYarn");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("main_class", true, "Jar file main class for the application master");
        opts.addOption("app_jar", true, "Jar file containing the application logic to launch in the container");
        opts.addOption("app_main_class", true, "Application jar file main class for the application");
        opts.addOption("app_input_path", true, "Path to application input");
        opts.addOption("app_output_path", true, "Path to application output");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the HotelsYarnApplication");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the HotelsYarnApplication");
        opts.addOption("num_containers", true, "No. of containers on which the HotelsYarnApplication needs to be executed");
        opts.addOption("help", false, "Print usage");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage();
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        appName = cliParser.getOptionValue("appname", "Yarn Hotel Application");
        defaultFs = cliParser.getOptionValue("default_fs");
        rmAddress = cliParser.getOptionValue("rm_address");

        if (StringUtils.isEmpty(defaultFs)) {
            throw new IllegalArgumentException("Default fs must be specified");
        }
        if (StringUtils.isEmpty(rmAddress)) {
            throw new IllegalArgumentException("Default fs must be specified");
        }

        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        if (!cliParser.hasOption("main_class")) {
            throw new IllegalArgumentException("No main class specified for application master");
        }

        if (!cliParser.hasOption("app_jar")) {
            throw new IllegalArgumentException("No App jar file specified for application master");
        }

        if (!cliParser.hasOption("app_main_class")) {
            throw new IllegalArgumentException("No app main class specified for application master");
        }

        if (!cliParser.hasOption("app_input_path")) {
            throw new IllegalArgumentException("No app input specified");
        }

        if (!cliParser.hasOption("app_output_path")) {
            throw new IllegalArgumentException("No app output specified");
        }

        appMasterJarPath = cliParser.getOptionValue("jar");
        appMasterMainClass = cliParser.getOptionValue("main_class");

        appJarPath = cliParser.getOptionValue("app_jar");
        appMainClass = cliParser.getOptionValue("app_main_class");

        appInputPath = cliParser.getOptionValue("app_input_path");
        appOutputPath = cliParser.getOptionValue("app_output_path");

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

        if (StringUtils.isEmpty(appJarPath) || StringUtils.isEmpty(appMainClass)) {
            throw new IllegalArgumentException("Either app jar or app main class has not been specified");
        }

        clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

        return true;
    }

    /**
     * Helper function to print out usage
     */
    public void printUsage() {
        new HelpFormatter().printHelp("ClientLauncher", opts);
    }

    /**
     * Adjust settings to reflect available resources.
     * @param appResponse   Resource Manager response.
     */
    public void adjustToAvailableResources(GetNewApplicationResponse appResponse) {
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capability of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }
    }

    public String getAppName() {
        return appName;
    }

    public int getAmPriority() {
        return amPriority;
    }

    public String getAmQueue() {
        return amQueue;
    }

    public int getAmMemory() {
        return amMemory;
    }

    public int getAmVCores() {
        return amVCores;
    }

    public String getAppMasterJarPath() {
        return appMasterJarPath;
    }

    public String getAppMasterMainClass() {
        return appMasterMainClass;
    }

    public int getRequestPriority() {
        return requestPriority;
    }

    public int getContainerMemory() {
        return containerMemory;
    }

    public int getContainerVirtualCores() {
        return containerVirtualCores;
    }

    public int getNumContainers() {
        return numContainers;
    }

    public long getClientTimeout() {
        return clientTimeout;
    }

    public String getAppJarPath() {
        return appJarPath;
    }

    public String getAppMainClass() {
        return appMainClass;
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
}
