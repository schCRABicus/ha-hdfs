package com.epam.bigdata.training.app;

import com.epam.bigdata.training.app.hotels.HotelsDataAnalyzer;
import com.epam.bigdata.training.commons.fs.FsUtils;
import com.epam.bigdata.training.commons.hotel.CompositeHotelId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ApplicationLauncher {

    private static final Logger log = LoggerFactory.getLogger(ApplicationLauncher.class);

    public static void main(String[] args) {
        log.info("Going to analyze hotels data");

        // Initialize launch configuration
        final LaunchConfiguration conf = initLaunchConfiguration(args);

        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("fs.defaultFS", conf.getDefaultFs());
        yarnConfiguration.set("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        yarnConfiguration.set("sampler.classes", "AlwaysSampler");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.hostname", "zipkin");
        yarnConfiguration.set("hadoop.htrace.zipkin.scribe.port", "9410");

        // First, finding top 3 most popular hotels between couples
        Map<CompositeHotelId, Long> results = HotelsDataAnalyzer.findTop3MostPopularBetweenCouples(yarnConfiguration, conf);

        // Now, preparing the desired output
        final List<String> output = new ArrayList<>(results.size() + 1);
        output.add("Hotel Country,Hotel Market,Popularity"); // header
        results.forEach((key, value) -> output.add(
                key.getCountry() + "," + key.getMarket() + "," + value
        ));

        // Finally, outputting the results
        FsUtils.write(yarnConfiguration, conf.getAppOutputPath(), output);
        log.info("Successfully written the results to {}", conf.getAppOutputPath());

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
            log.error("Failed to initialized configuration", e);
            conf.printUsage();
            System.exit(1);
        }
        return conf;
    }
}
