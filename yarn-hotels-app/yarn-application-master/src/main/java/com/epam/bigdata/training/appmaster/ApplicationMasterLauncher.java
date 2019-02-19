package com.epam.bigdata.training.appmaster;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The AM is the actual owner of the job.
 * It will be launched by the RM and via the client will be provided all the necessary information
 * and resources about the job that it has been tasked with to oversee and complete.
 *
 * <p />
 * As the AM is launched within a container that may (likely will) be sharing a physical host with other containers,
 * given the multi-tenancy nature, amongst other issues,
 * it cannot make any assumptions of things like pre-configured ports that it can listen on.
 *
 * <p />
 * When the AM starts up, several parameters are made available to it via the environment.
 * These include the ContainerId for the AM container,
 * the application submission time and details about the NM (NodeManager) host running the ApplicationMaster.
 * Ref ApplicationConstants for parameter names.
 *
 * <p />
 * All interactions with the RM require an ApplicationAttemptId
 * (there can be multiple attempts per application in case of failures).
 * The ApplicationAttemptId can be obtained from the AM’s container id.
 * There are helper APIs to convert the value obtained from the environment into objects.
 */
public class ApplicationMasterLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterLauncher.class);

    /**
     * heartbeat interval in milliseconds between AM and RM
     */
    private static final int AM_RM_HEARTBEAT_INTERVAL = 1_000;

    public static void main(String[] args) throws IOException, YarnException {
        final LaunchConfiguration conf = initLaunchConfiguration(args);

        // First, obtain the application attempt id and configure launch settings
        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.address", conf.getRmAddress());
        yarnConfiguration.set("fs.defaultFS", conf.getDefaultFs());
        yarnConfiguration.set("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        yarnConfiguration.set("sampler.classes", "AlwaysSampler");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.hostname", "zipkin");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.port", "9410");

        /*
            After an AM has initialized itself completely, we can start the two clients:
            one to ResourceManager, and one to NodeManagers.
         */
        final AtomicInteger numCompletedContainers = new AtomicInteger();
        final AtomicInteger numFailedContainers = new AtomicInteger();

        // Create AM - RM client
        ResourceManagerCallbackHandler rmCallbackHandler = new ResourceManagerCallbackHandler(numCompletedContainers, numFailedContainers,yarnConfiguration, conf);
        AMRMClientAsync<AMRMClient.ContainerRequest> amRMClientAsync =
                AMRMClientAsync.createAMRMClientAsync(AM_RM_HEARTBEAT_INTERVAL, rmCallbackHandler);
        amRMClientAsync.init(yarnConfiguration);
        amRMClientAsync.start();

        // Create AM - NM Client
        NodeManagerCallbackHandler nmCallbackHandler = new NodeManagerCallbackHandler(numCompletedContainers, numFailedContainers);
        NMClientAsyncImpl nmClientAsync = new NMClientAsyncImpl(nmCallbackHandler);
        nmClientAsync.init(yarnConfiguration);
        nmClientAsync.start();

        rmCallbackHandler.setNmClientAsync(nmClientAsync);
        rmCallbackHandler.setAmrmClientAsync(amRMClientAsync);

        try {
            /*
                The AM has to emit heartbeats to the RM to keep it informed that the AM is alive and still running.
                The timeout expiry interval at the RM is defined by a config setting accessible via
                YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS with the default being defined by
                YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS.
                The ApplicationMaster needs to register itself with the ResourceManager to start heartbeating.
             */
            // Register self with ResourceManager. This will start heartbeating to the RM
            //final String appMasterHostname = NetUtils.getHostname();
            RegisterApplicationMasterResponse response = amRMClientAsync.registerApplicationMaster("", 0, "");

            // In the response of the registration, maximum resource capability if included.
            // You may want to use this to check the application’s request.
            // Dump out information about cluster capability as seen by the resource manager
            int maxMem = response.getMaximumResourceCapability().getMemory();
            LOG.info("Max mem capability of resources in this cluster " + maxMem);

            int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
            LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

            // A resource ask cannot exceed the max.
            if (conf.getContainerMemory() > maxMem) {
                LOG.info("Container memory specified above max threshold of cluster."
                        + " Using max value." + ", specified=" + conf.getContainerMemory() + ", max="
                        + maxMem);
                conf.setContainerMemory(maxMem);
            }

            if (conf.getContainerVirtualCores() > maxVCores) {
                LOG.info("Container virtual cores specified above max threshold of  cluster."
                        + " Using max value." + ", specified=" + conf.getContainerVirtualCores() + ", max="
                        + maxVCores);
                conf.setContainerVirtualCores(maxVCores);
            }
            List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
            LOG.info("Received " + previousAMRunningContainers.size()
                    + " previous AM's running containers on AM registration.");

            // Based on the task requirements, the AM can ask for a set of containers to run its tasks on.
            // We can now calculate how many containers we need, and request those many containers.
            int numTotalContainersToRequest = conf.getNumTotalContainers() - previousAMRunningContainers.size();

            // Setup ask for containers from RM
            // Send request for containers to RM
            // Until we get our fully allocated quota, we keep on polling RM for
            // containers
            // Keep looping until all the containers are launched and shell script
            // executed on them ( regardless of success/failure).
            for (int i = 0; i < numTotalContainersToRequest; ++i) {
                AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(conf);
                amRMClientAsync.addContainerRequest(containerAsk);
            }

            LOG.info("numCompletedContainers = " + numCompletedContainers.get());
            LOG.info("conf.getNumTotalContainers() = " + conf.getNumTotalContainers());
            // After container allocation requests have been sent by the application manager,
            // containers will be launched asynchronously, by the event handler of the AMRMClientAsync client.
            while (numCompletedContainers.get() < conf.getNumTotalContainers() && !Thread.interrupted()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.exit(1);
                }
            }

        } finally {
            // wait for application to complete
            try {
                LOG.info("unregistering application master");
                amRMClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finished Successfully", "");
            } catch (YarnException | IOException e) {
                LOG.error("Failed to unregister application", e);
            }

            LOG.info("About to stop the AM - RM client");
            amRMClientAsync.stop();
        }

        ResultsAggregator.aggregateAndWrite(yarnConfiguration, conf);

        System.exit(0);
    }

    private static LaunchConfiguration initLaunchConfiguration(String[] args) {
        final LaunchConfiguration conf = new LaunchConfiguration();
        try {
            boolean initialized = conf.init(args);

            if (!initialized) {
                System.exit(0);
            }
        } catch (Exception e) {
            LOG.error("Failed to initialized configuration", e);
            conf.printUsage();
            System.exit(1);
        }
        return conf;
    }

    /**
     * <pre>
     * In setupContainerAskForRM(), the follow two things need some set up:
     *     Resource capability:
     *          Currently, YARN supports memory based resource requirements so the request should define
     *          how much memory is needed. The value is defined in MB and has to less than the max capability
     *          of the cluster and an exact multiple of the min capability.
     *          Memory resources correspond to physical memory limits imposed on the task containers.
     *          It will also support computation based resource (vCore), as shown in the code.
     *
     *     Priority: When asking for sets of containers, an AM may define different priorities to each set.
     *          For example, the Map-Reduce AM may assign a higher priority to containers
     *          needed for the Map tasks and a lower priority for the Reduce tasks’ containers.
     * </pre>
     * @return Container request
     */
    static AMRMClient.ContainerRequest setupContainerAskForRM(LaunchConfiguration conf) {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Priority.newInstance(conf.getRequestPriority());

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(conf.getContainerMemory(), conf.getContainerVirtualCores());

        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }
}
