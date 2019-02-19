package com.epam.bigdata.training.appmaster;

import com.epam.bigdata.training.commons.fs.FsUtils;
import com.epam.bigdata.training.commons.hotel.CompositeHotelId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated class to aggregate container outputs into the single file.
 */
public class ResultsAggregator {

    private static final Logger log = LoggerFactory.getLogger(ResultsAggregator.class);

    /**
     * Parts are written into the prefixed _idx csv files so the goal of this
     * task is to iterate over each file, parse and combine into a single one.
     * @param conf Launch configuration containing target output path and number
     *             of containers (and thus number of result splits)
     */
    public static void aggregateAndWrite(Configuration yarnConfiguration, LaunchConfiguration conf) throws IOException {
        log.info("Going to aggregate the output of {} containers into the single {} file",
                conf.getNumTotalContainers(), conf.getAppOutputPath());

        final Map<CompositeHotelId, Long> result = new HashMap<>();
        for (int i = 0; i < conf.getNumTotalContainers(); i++) {
            Path path = new Path(conf.getAppOutputPath() + "_" + i);

            try {
                final AtomicBoolean header = new AtomicBoolean(true);
                FsUtils.readLineByLine(yarnConfiguration, conf.getAppOutputPath() + "_" + i, line -> {
                    if (!header.getAndSet(false)) {
                        String[] values = line.split(",");
                        CompositeHotelId id = new CompositeHotelId(values[0], values[1]);
                        Long count = Long.valueOf(values[2]);

                        result.merge(id, count, Long::sum);
                    }
                });
            } catch (Exception e) {
                log.warn("Failed to read the contents of part {}", path, e);
            }
        }

        log.info("Aggregated the output of {} containers", conf.getNumTotalContainers());

        log.info("Going to write aggregated result into {}", conf.getAppOutputPath());
        final List<String> output = new ArrayList<>(result.size() + 1);
        output.add("Hotel Country, Hotel Market, Popularity");
        result.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .forEachOrdered(entry -> output.add(
                        entry.getKey().getCountry() + "," + entry.getKey().getMarket() + "," + entry.getValue()
        ));

        FsUtils.write(yarnConfiguration, conf.getAppOutputPath(), output);
    }
}
