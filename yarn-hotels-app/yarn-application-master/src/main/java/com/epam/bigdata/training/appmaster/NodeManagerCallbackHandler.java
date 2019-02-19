package com.epam.bigdata.training.appmaster;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeManagerCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodeManagerCallbackHandler.class);

    private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();

    private final AtomicInteger numCompletedContainers;
    private final AtomicInteger numFailedContainers;

    public NodeManagerCallbackHandler(AtomicInteger numCompletedContainers, AtomicInteger numFailedContainers) {
        this.numCompletedContainers = numCompletedContainers;
        this.numFailedContainers = numFailedContainers;
    }

    public void addContainer(ContainerId containerId, Container container) {
        containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to stop Container " + containerId);
        }
        containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
                                          ContainerStatus containerStatus) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Container Status: id=" + containerId + ", status=" +
                    containerStatus);
        }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
                                   Map<String, ByteBuffer> allServiceResponse) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to start Container " + containerId);
        }
        Container container = containers.get(containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to start Container {}", containerId, t);
        containers.remove(containerId);

        numCompletedContainers.incrementAndGet();
        numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(
            ContainerId containerId, Throwable t) {
        LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to stop Container " + containerId);
        containers.remove(containerId);
    }

    @Deprecated
    @Override
    public void onIncreaseContainerResourceError(
            ContainerId containerId, Throwable t) {}

    @Deprecated
    @Override
    public void onContainerResourceIncreased(
            ContainerId containerId, Resource resource) {}

    @Override
    public void onUpdateContainerResourceError(
            ContainerId containerId, Throwable t) {
    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId,
                                           Resource resource) {
    }
}
