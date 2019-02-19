package com.epam.bigdata.training.client;

import com.epam.bigdata.training.client.am.AMContainerContextFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@code ApplicationSubmissionContext} represents all of the
 * information needed by the {@code ResourceManager} to launch
 * the {@code ApplicationMaster} for an application.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link org.apache.hadoop.yarn.api.records.ApplicationId} of the application.</li>
 *   <li>Application user.</li>
 *   <li>Application name.</li>
 *   <li>{@link org.apache.hadoop.yarn.api.records.Priority} of the application.</li>
 *   <li>
 *     {@link org.apache.hadoop.yarn.api.records.ContainerLaunchContext} of the container in which the
 *     <code>ApplicationMaster</code> is executed.
 *   </li>
 *   <li>
 *     maxAppAttempts. The maximum number of application attempts.
 *     It should be no larger than the global number of max attempts in the
 *     Yarn configuration.
 *   </li>
 *   <li>
 *     attemptFailuresValidityInterval. The default value is -1.
 *     when attemptFailuresValidityInterval in milliseconds is set to
 *     {@literal >} 0, the failure number will no take failures which happen
 *     out of the validityInterval into failure count. If failure count
 *     reaches to maxAppAttempts, the application will be failed.
 *   </li>
 *   <li>Optional, application-specific {@link org.apache.hadoop.yarn.api.records.LogAggregationContext}</li>
 * </ul>
 *
 * This class is responsible for context construction.
 */
public class ApplicationSubmissionContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSubmissionContextFactory.class);

    public static ApplicationSubmissionContext build(YarnClientApplication app, YarnConfiguration yarnConfiguration, LaunchConfiguration conf) throws IOException {
        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        // set the application name
        appContext.setApplicationName(conf.getAppName());

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and vcores requirements
        Resource capability = Resource.newInstance(conf.getAmMemory(), conf.getAmVCores());
        appContext.setResource(capability);

        // Set the priority for the application master
        Priority pri = Priority.newInstance(conf.getAmPriority());
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(conf.getAmQueue());

        // Set the ContainerLaunchContext to describe the Container in which the ApplicationMaster is launched.
        appContext.setAMContainerSpec(AMContainerContextFactory.build(appId, yarnConfiguration, conf));

        return appContext;
    }
}
