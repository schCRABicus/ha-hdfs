package com.epam.bigdata.training.app;

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

    /**
     * Default file system url.
     */
    private String defaultFs;

    /**
     * Path to application input.
     */
    private String appInputPath;

    /**
     * Path to application output.
     */
    private String appOutputPath;

    /**
     * Containers amount.
     */
    private int numContainers = 1;

    /**
     * Start offset of input stream.
     */
    private long inputStartOffset;

    /**
     * End offset of input stream.
     */
    private long inputEndOffset;

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
        opts.addOption("default_fs", true, "Default file system. Mandatory.");
        opts.addOption("app_input_path", true, "Path to application input");
        opts.addOption("app_output_path", true, "Path to application output");
        opts.addOption("num_containers", true, "No. of containers on which the HotelsYarnApplication needs to be executed.Defaults to 1");
        opts.addOption("input_start_offset", true, "Input start offset");
        opts.addOption("input_end_offset", true, "Input end offset");
        opts.addOption("help", false, "Print usage");

        CommandLine cliParser = new GnuParser().parse(opts, args);


        defaultFs = cliParser.getOptionValue("default_fs");

        if (StringUtils.isEmpty(defaultFs)) {
            throw new IllegalArgumentException("Default fs must be specified");
        }

        if (!cliParser.hasOption("app_input_path")) {
            throw new IllegalArgumentException("No app input specified");
        }

        if (!cliParser.hasOption("app_output_path")) {
            throw new IllegalArgumentException("No app output specified");
        }

        if (!cliParser.hasOption("input_start_offset")) {
            throw new IllegalArgumentException("No input start offset specified");
        }

        if (!cliParser.hasOption("input_end_offset")) {
            throw new IllegalArgumentException("No input end offset specified");
        }
        appInputPath = cliParser.getOptionValue("app_input_path");
        appOutputPath = cliParser.getOptionValue("app_output_path");
        inputStartOffset = Long.valueOf(cliParser.getOptionValue("input_start_offset"));
        inputEndOffset = Long.valueOf(cliParser.getOptionValue("input_end_offset"));

        numContainers = Integer.valueOf(cliParser.getOptionValue("num_containers", "1"));

        return true;
    }

    /**
     * Helper function to print out usage
     */
    public void printUsage() {
        new HelpFormatter().printHelp("Hotels Application", opts);
    }

    public String getDefaultFs() {
        return defaultFs;
    }

    public String getAppInputPath() {
        return appInputPath;
    }

    public String getAppOutputPath() {
        return appOutputPath;
    }

    public int getNumContainers() {
        return numContainers;
    }

    public long getInputStartOffset() {
        return inputStartOffset;
    }

    public long getInputEndOffset() {
        return inputEndOffset;
    }
}
