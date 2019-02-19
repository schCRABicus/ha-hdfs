package com.epam.bigdata.training.client.components;

import com.epam.bigdata.training.client.LaunchConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Submitter of the very first container of the application that contains the ApplicationMaster.
 */
public class YarnApplicationSubmitter {

    /**
     * Submit the prepared very first container of the application that contains the ApplicationMaster.
     * @param yarnClient    Yarn Client
     * @param context       Application submission context with all the metadata provided
     *                      required to start the app container.
     * @param conf          Launch configuration.
     * @throws IOException
     * @throws YarnException
     */
    public static void submit(YarnClient yarnClient, ApplicationSubmissionContext context, LaunchConfiguration conf) throws IOException, YarnException {
        // Set the priority for the application master
        Priority pri = Priority.newInstance(conf.getAmPriority());
        context.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        context.setQueue(conf.getAmQueue());

        // Submit the application
        yarnClient.submitApplication(context);
    }
}
