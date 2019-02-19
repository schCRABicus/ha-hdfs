package com.epam.bigdata.training.app.hotels;

import com.epam.bigdata.training.app.LaunchConfiguration;
import com.epam.bigdata.training.commons.fs.FsUtils;
import com.epam.bigdata.training.commons.hotel.CompositeHotelId;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class responsible for analysing the hotels data.
 */
public class HotelsDataAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(HotelsDataAnalyzer.class);

    private static final String HOTEL_COUNTRY_COLUMN = "hotel_country";
    private static final String HOTEL_MARKET_COLUMN = "hotel_market";
    private static final String ADULTS_COUNT_COLUMN = "srch_adults_cnt";

    /**
     * Finds top 3 most popular hotels between couples. (Treat hotel as composite key of continent country and market).
     * @param configuration File system configuration.
     * @param conf          Launch configuration.
     * @return Map of 3 most poplar hotels.
     * @throws HotelsAnalyzingException
     */
    public static Map<CompositeHotelId, Long> findTop3MostPopularBetweenCouples(Configuration configuration, LaunchConfiguration conf) throws HotelsAnalyzingException {
        log.info("Going to find top 3 most popular hotels between couples in source {}", conf.getAppInputPath());
        final Map<String, Long> counts = new HashMap<>();

        final HeaderData headerData = new HeaderData();
        FsUtils.readLineByLineWithHeaderAndOffset(configuration, conf.getAppInputPath(), conf.getInputStartOffset(), conf.getInputEndOffset(), line -> {
            if (!headerData.isRead()) {
                final String[] headers = extractHeaders(line);

                if (headers.length <= 0) {
                    log.warn("Failed to read headers, can't proceed further. Aborting operation.");
                    throw new HotelsAnalyzingException("Failed to read header values");
                }

                headerData.setRead(true);
                headerData.setHotelCountryColumnIdx(findIndex(headers, HOTEL_COUNTRY_COLUMN));
                headerData.setHotelMarketColumnIdx(findIndex(headers, HOTEL_MARKET_COLUMN));
                headerData.setAdultsCountIdx(findIndex(headers, ADULTS_COUNT_COLUMN));

                assert headerData.getHotelCountryColumnIdx() > 0 : "No hotel country column available in source file";
                assert headerData.getHotelMarketColumnIdx() > 0 : "No hotel market column available in source file";
                assert headerData.getAdultsCountIdx() > 0 : "No adults count column available in source file";

                return;
            }

            final String[] tokens = line.split(",");

            String country = getSafely(tokens, headerData.getHotelCountryColumnIdx());
            String market = getSafely(tokens, headerData.getHotelMarketColumnIdx());
            String adults = getSafely(tokens, headerData.getAdultsCountIdx());

            if (anyNull(country, market, adults)) {
                log.info("Skipping line with a nullable key or search adults count: country = {}, market = {}, adults = {}",
                        country, market, adults);
                return;
            }

            String key = buildHotelCompositeKey(country, market);
            int cnt = Integer.valueOf(adults);

            // if couple is searching for a hotel, then increase its popularity
            if (cnt == 2) {
                counts.merge(key, 1L, Long::sum);

                log.debug("Added 1 popularity point to hotel {}", key);
            }
        });

        //LinkedHashMap preserve the ordering of elements in which they are inserted
        Map<CompositeHotelId, Long> results = new LinkedHashMap<>();

        // now, find top 3 most popular
        counts.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(entry -> {
                    String[] tokens = entry.getKey().split("@");
                    results.put(new CompositeHotelId(tokens[0], tokens[1]), entry.getValue());
                });

        return results;
    }

    @VisibleForTesting
    static String[] extractHeaders(@Nullable String line) {
        if (StringUtils.isEmpty(line)) {
            return new String[0];
        }

        return line.split(",");
    }

    @VisibleForTesting
    static int findIndex(String[] source, String target) {
        for (int i = 0; i < source.length; i++) {
            if (source[i].equals(target)) {
                return i;
            }
        }

        return -1;
    }

    static String buildHotelCompositeKey(String country, String market) {
        return country + "@" + market;
    }

    @VisibleForTesting
    @Nullable
    static String getSafely(String[] source, int index) {
        if (index >= source.length) {
            return null;
        }

        return source[index];
    }

    /**
     * Verifies whether there is a null value in the list of the provided strings.
     * @param values
     * @return true if any null value and false otherwise.
     */
    @VisibleForTesting
    static boolean anyNull(String... values) {
        for (String value : values) {
            if (value == null) {
                return true;
            }
        }

        return false;
    }

    private static class HeaderData {

        boolean read = false;
        int hotelCountryColumnIdx;
        int hotelMarketColumnIdx;
        int adultsCountIdx;

        public int getHotelCountryColumnIdx() {
            return hotelCountryColumnIdx;
        }

        public void setHotelCountryColumnIdx(int hotelCountryColumnIdx) {
            this.hotelCountryColumnIdx = hotelCountryColumnIdx;
        }

        public int getHotelMarketColumnIdx() {
            return hotelMarketColumnIdx;
        }

        public void setHotelMarketColumnIdx(int hotelMarketColumnIdx) {
            this.hotelMarketColumnIdx = hotelMarketColumnIdx;
        }

        public int getAdultsCountIdx() {
            return adultsCountIdx;
        }

        public void setAdultsCountIdx(int adultsCountIdx) {
            this.adultsCountIdx = adultsCountIdx;
        }

        public boolean isRead() {
            return read;
        }

        public void setRead(boolean read) {
            this.read = read;
        }
    }
}
