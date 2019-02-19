package com.epam.bigdata.training.client;

import com.epam.bigdata.training.client.components.YarnApplicationMonitor;
import com.epam.bigdata.training.client.components.YarnApplicationSubmitter;
import com.epam.bigdata.training.commons.tracer.HTracerUtils;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.impl.ZipkinSpanReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Yarn Client.
 *
 * <p />
 * After YarnClient is started, the client can then set up application context,
 * prepare the very first container of the application that contains the ApplicationMaster (AM),
 * and then submit the application.
 *
 * <p />
 * This is the <pre>Client<-->ResourceManager</pre> interface.
 */
public class ClientLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ClientLauncher.class);

    public static void main(String[] args) throws Exception {
        // Initialize launch configuration
        final LaunchConfiguration conf = initLaunchConfiguration(args);

        // Configure yarn settings
        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.address", conf.getRmAddress());
        yarnConfiguration.set("fs.defaultFS", conf.getDefaultFs());
        yarnConfiguration.set("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        yarnConfiguration.set("sampler.classes", "AlwaysSampler");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.hostname", "zipkin");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.port", "9410");

        // The first step that a client needs to do is to initialize and start a YarnClient.
        final YarnClient yarnClient = YarnClient.createYarnClient();
        HTracerUtils.trace("Yarn Client", "YARN client initialization", yarnConfiguration, () -> {
            yarnClient.init(yarnConfiguration);
            yarnClient.start();
        });

        // Once a client is set up, the client needs to create an application, and get its application id.
        final YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        conf.adjustToAvailableResources(appResponse);

        // Setup the ApplicationSubmissionContext which defines all the information needed by the RM to launch the AM.
        ApplicationSubmissionContext context = ApplicationSubmissionContextFactory.build(app, yarnConfiguration, conf);

        // After the setup process is complete, the client is ready to submit the application with specified priority and queue.
        YarnApplicationSubmitter.submit(yarnClient, context, conf);

        /*
            At this point, the RM will have accepted the application and in the background,
            will go through the process of allocating a container with the required specifications and
            then eventually setting up and launching the AM on the allocated container.
         */
        boolean result = YarnApplicationMonitor.monitor(appResponse.getApplicationId(), yarnClient);

        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    private static LaunchConfiguration initLaunchConfiguration(String[] args) {
        final LaunchConfiguration conf = new LaunchConfiguration();
        try {
            boolean initialized = conf.init(args);

            if (!initialized) {
                System.exit(0);
            }
        } catch (ParseException e) {
            LOG.error("Failed to initialized configuration", e);
            conf.printUsage();
            System.exit(1);
        }
        return conf;
    }
}
