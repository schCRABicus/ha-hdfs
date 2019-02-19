package com.epam.bigdata.training.client.components;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A dedicated service which constantly communicates with the RM
 * and requests for a report of the application via the getApplicationReport() method of YarnClient.
 *
 * <p />
 * The ApplicationReport received from the RM consists of the following:
 *
 * <ul>
 *     <li>
 *         General application information:
 *         Application id, queue to which the application was submitted,
 *         user who submitted the application and the start time for the application.
 *     </li>
 *     <li>
 *         ApplicationMaster details:
 *         the host on which the AM is running,
 *         the rpc port (if any) on which it is listening for requests from clients
 *         and a token that the client needs to communicate with the AM.
 *     </li>
 *     <li>
 *         Application tracking information:
 *         If the application supports some form of progress tracking,
 *         it can set a tracking url which is available via ApplicationReport’s getTrackingUrl() method
 *         that a client can look at to monitor progress.
 *     </li>
 *     <li>
 *         Application status:
 *         The state of the application as seen by the ResourceManager is available via
 *         ApplicationReport#getYarnApplicationState.
 *         If the YarnApplicationState is set to FINISHED, the client should refer to
 *         ApplicationReport#getFinalApplicationStatus to check for the actual success/failure of the
 *         application task itself.
 *         In case of failures, ApplicationReport#getDiagnostics may be useful to shed
 *         some more light on the the failure.
 *     </li>
 * </ul>
 */
public class YarnApplicationMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMonitor.class);

    /**
     * Status check interval.
     */
    private static final int STATUS_CHECK_INTERVAL = 1_000;

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     * @throws java.io.IOException
     */
    public static boolean monitor(ApplicationId appId, YarnClient yarnClient)
            throws YarnException, IOException {

        while (!Thread.interrupted()) {
            // Check app status every 1 second.
            try {
                Thread.sleep(STATUS_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                LOG.error("Thread sleep in monitoring loop interrupted, shutting down monitor");
                return false;
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. "
                            + " Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return true;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                return false;
            }
        }

        return false;
    }
}
