package com.epam.bigdata.training.client.am;

import com.epam.bigdata.training.client.LaunchConfiguration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AMLaunchCommandFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AMLaunchCommandFactory.class);

    public static String build(LaunchConfiguration conf) {
        LOG.info("Setting up app master command");

        return Stream.of(
                ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",
//                "java",
//                "-cp " + conf.getAppMasterJarPath(),

                // Set Xmx based on am memory size
                "-Xmx" + conf.getAmMemory() + "m",

                "-XX:+PrintGCDetails",
                "-XX:+PrintGCDateStamps",
                "-XX:MetaspaceSize=256M",
                "-Xloggc:" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMasterGCLogs.gcout",

                // Set main class name
                conf.getAppMasterMainClass(),

                // Set params for Application Master
                "--container_memory " + String.valueOf(conf.getContainerMemory()),
                "--container_vcores " + String.valueOf(conf.getContainerVirtualCores()),
                "--num_containers " + String.valueOf(conf.getNumContainers()),
                "--priority " + String.valueOf(conf.getRequestPriority()),
                "--app_jar_path " + conf.getAppJarPath(),
                "--app_main_class " + conf.getAppMainClass(),
                "--default_fs " + conf.getDefaultFs(),
                "--rm_address " + conf.getRmAddress(),
                "--app_input_path " + conf.getAppInputPath(),
                "--app_output_path " + conf.getAppOutputPath(),

                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout",
                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr"
        ).collect(Collectors.joining(" "));
//        return "pwd";
    }
}
