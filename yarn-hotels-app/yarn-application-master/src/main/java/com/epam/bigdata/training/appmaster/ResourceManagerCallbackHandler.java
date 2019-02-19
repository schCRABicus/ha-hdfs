package com.epam.bigdata.training.appmaster;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Callback handler that processes the responses from <code>Resource Manager</code>.
 */
public class ResourceManagerCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerCallbackHandler.class);

    // Counter for completed containers ( complete denotes successful or failed )
    private final AtomicInteger numCompletedContainers;

    // Count of failed containers
    private final AtomicInteger numFailedContainers;

    private NMClientAsync nmClientAsync;

    private AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;

    private final YarnConfiguration yarnConfiguration;

    private final LaunchConfiguration conf;

    // Allocated container count so that we know how many containers has the RM
    // allocated to us]
    private AtomicInteger numAllocatedContainers = new AtomicInteger();

    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.\
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private AtomicInteger numIgnore = new AtomicInteger();

    private AtomicInteger totalRetries = new AtomicInteger(10);

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();

    protected final Set<ContainerId> launchedContainers =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    private BoundedAppender diagnostics = new BoundedAppender(64 * 1024);

    private final ApplicationAttemptId appAttemptID;

    private final Map<String, LocalResource> containerLocalResources;

    private volatile boolean done = true;

    public ResourceManagerCallbackHandler(
            AtomicInteger numCompletedContainers, AtomicInteger numFailedContainers,
            YarnConfiguration yarnConfiguration, LaunchConfiguration conf
    ) throws IOException {
        this.appAttemptID = conf.getAppAttemptID();
        this.numCompletedContainers = numCompletedContainers;
        this.numFailedContainers = numFailedContainers;

        this.yarnConfiguration = yarnConfiguration;
        this.conf = conf;

        this.containerLocalResources = prepareContainerLocalResources(yarnConfiguration, conf);
    }

    public void setNmClientAsync(NMClientAsync nmClientAsync) {
        this.nmClientAsync = nmClientAsync;
    }

    public void setAmrmClientAsync(AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync) {
        this.amrmClientAsync = amrmClientAsync;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
        LOG.info("Got response from RM for container ask, completedCnt="
                + completedContainers.size());
        for (ContainerStatus containerStatus : completedContainers) {
            String message = appAttemptID + " got container status for containerID="
                    + containerStatus.getContainerId() + ", state="
                    + containerStatus.getState() + ", exitStatus="
                    + containerStatus.getExitStatus() + ", diagnostics="
                    + containerStatus.getDiagnostics();
            if (containerStatus.getExitStatus() != 0) {
                LOG.error(message);
                diagnostics.append(containerStatus.getDiagnostics());
            } else {
                LOG.info(message);
            }

            // non complete containers should not be here
            assert (containerStatus.getState() == ContainerState.COMPLETE);
            // ignore containers we know nothing about - probably from a previous
            // attempt
            if (!launchedContainers.contains(containerStatus.getContainerId())) {
                LOG.info("Ignoring completed status of "
                        + containerStatus.getContainerId()
                        + "; unknown container(probably launched by previous attempt)");
                continue;
            }

            // increment counters for completed/failed containers
            int exitStatus = containerStatus.getExitStatus();
            if (0 != exitStatus) {
                // container failed
                if (ContainerExitStatus.ABORTED != exitStatus) {
                    // shell script failed
                    // counts as completed
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
                } else {
                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    numAllocatedContainers.decrementAndGet();
                    numRequestedContainers.decrementAndGet();
                    // we do not need to release the container as it would be done
                    // by the RM
                }
            } else {
                // nothing to do
                // container completed successfully
                numCompletedContainers.incrementAndGet();
                LOG.info("Container completed successfully." + ", containerId="
                        + containerStatus.getContainerId());
            }
        }

        // ask for more containers if any failed
        int askCount = conf.getNumTotalContainers() - numRequestedContainers.get();
        numRequestedContainers.addAndGet(askCount);

        if (askCount > 0) {
            for (int i = 0; i < askCount; ++i) {
                AMRMClient.ContainerRequest containerAsk = ApplicationMasterLauncher.setupContainerAskForRM(conf);
                amrmClientAsync.addContainerRequest(containerAsk);
            }
        }

        if (numCompletedContainers.get() + numIgnore.get() >= conf.getNumTotalContainers()) {
            done = true;
        }
    }

    /**
     * When there are containers allocated, the handler sets up a thread that runs the code to launch containers.
     * @param allocatedContainers Allocated containers
     */
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
        LOG.info("Got response from RM for container ask, allocatedCnt="
                + allocatedContainers.size());
        numAllocatedContainers.addAndGet(allocatedContainers.size());
        int idx = 0;
        for (Container allocatedContainer : allocatedContainers) {
            ContainerLaunchContext containerLaunchContext = createContainerLaunchContext(
                    containerLocalResources, conf, idx++
            );
            LaunchContainerRunnable runnableLaunchContainer =
                    new LaunchContainerRunnable(allocatedContainer, nmClientAsync, containerLaunchContext);
            Thread launchThread = new Thread(runnableLaunchContainer);

            // launch and start the container on a separate thread to keep
            // the main thread unblocked
            // as all containers may not be allocated at one go.
            launchThreads.add(launchThread);
            launchedContainers.add(allocatedContainer.getId());
            launchThread.start();
        }
    }

    @Override
    public void onContainersUpdated(
            List<UpdatedContainer> containers) {
        for (UpdatedContainer container : containers) {
            LOG.info("Container {} updated, updateType={}, resource={}, "
                            + "execType={}",
                    container.getContainer().getId(),
                    container.getUpdateType().toString(),
                    container.getContainer().getResource().toString(),
                    container.getContainer().getExecutionType());

            nmClientAsync.updateContainerResourceAsync(container.getContainer());
        }
    }

    @Override
    public void onRequestsRejected(List<RejectedSchedulingRequest> rejReqs) {
        List<SchedulingRequest> reqsToRetry = new ArrayList<>();
        for (RejectedSchedulingRequest rejReq : rejReqs) {
            LOG.info("Scheduling Request {} has been rejected. Reason {}",
                    rejReq.getRequest(), rejReq.getReason());
            reqsToRetry.add(rejReq.getRequest());
        }
        totalRetries.addAndGet(-1 * reqsToRetry.size());
        if (totalRetries.get() <= 0) {
            LOG.info("Exiting, since retries are exhausted !!");
            done = true;
        } else {
            amrmClientAsync.addSchedulingRequests(reqsToRetry);
        }
    }

    @Override public void onShutdownRequest() {
        LOG.info("Shutdown request received. Processing since "
                + "keep_containers_across_application_attempts is disabled");
        done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
        // set progress to deliver to RM on next heartbeat
        return (float) numCompletedContainers.get() / conf.getNumTotalContainers();
    }

    @Override
    public void onError(Throwable e) {
        LOG.error("Error in RMCallbackHandler: ", e);
        done = true;
    }

    /**
     * Thread to connect to the {@link org.apache.hadoop.yarn.api.ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        private final Container container;

        private final NMClientAsync nmClientAsync;

        private final ContainerLaunchContext containerLaunchContext;

        /**
         * @param container Allocated container
         * @param nmClientAsync NodeManager async client.
         */
        public LaunchContainerRunnable(
                Container container, NMClientAsync nmClientAsync, ContainerLaunchContext containerLaunchContext) {
            this.container = container;
            this.nmClientAsync = nmClientAsync;
            this.containerLaunchContext = containerLaunchContext;
        }

        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        @Override
        public void run() {
            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            ((NodeManagerCallbackHandler) nmClientAsync.getCallbackHandler()).addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, containerLaunchContext);
        }
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

    /**
     * Launch container by create ContainerLaunchContext
     *
     * @param conf          Launch configuration.
     * @param idx           Container index.
     * @return
     */
    private ContainerLaunchContext createContainerLaunchContext(Map<String, LocalResource> localResources, LaunchConfiguration conf, int idx) {


        // todo: [m.lushchytski] provide boundaries here
        long start;
        long end;
        try {
            FileSystem fs = FileSystem.get(yarnConfiguration);
            FileStatus fileStatus = fs.getFileStatus(new Path(conf.getAppInputPath()));

            long partitionLength = Math.round(fileStatus.getLen() / conf.getNumTotalContainers());
            start = idx * partitionLength;
            end = Math.min(start + partitionLength, fileStatus.getLen());
        } catch (IOException e) {
            throw new RuntimeException("Failed to calculate split for the container");
        }

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Setup the environment for the application");
        Map<String, String> env = getEnvironment(yarnConfiguration);

        List<String> commands = Collections.singletonList(
                Stream.of(
                        ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",

                        // Set Xmx based on am memory size
                        "-Xmx512m", // todo: [m.lushchytski] make generic

                        "-XX:+PrintGCDetails",
                        "-XX:+PrintGCDateStamps",
                        "-XX:MetaspaceSize=256M",
                        "-Xloggc:" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/ContainerGCLogs.gcout",

                        // Set main class name
                        conf.getAppMainClass(),

                        // Launch configuration options
                        "--default_fs " + conf.getDefaultFs(),
                        "--app_input_path " + conf.getAppInputPath(),
                        "--app_output_path " + conf.getAppOutputPath() + "_" + idx,
                        "--num_containers " + String.valueOf(conf.getNumTotalContainers()),
                        "--input_start_offset " + start,
                        "--input_end_offset " + end,

                        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/HotelsApp.stdout",
                        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/HotelsApp.stderr"
                ).collect(Collectors.joining(" "))
        );

        // Set up the container launch context for the application
        return ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
    }

    private static Map<String, LocalResource> prepareContainerLocalResources(Configuration yarnConfiguration, LaunchConfiguration conf) throws IOException {
        Map<String, LocalResource> localResources = new HashMap<>();

        LOG.info("Copy Application jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(yarnConfiguration);
        addToLocalResources(fs, Constants.APP_JAR_NAME, Constants.APP_JAR_IN_CONTAINER_NAME, conf.getAppAttemptID().getApplicationId().getId(), conf.getAppName(),
                localResources, null);

        return localResources;
    }

    private static Map<String, String> getEnvironment(YarnConfiguration yarnConfiguration) {

        Map<String, String> env = new HashMap<>();

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
