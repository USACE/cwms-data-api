package cwms.cda.data.dao;

import cwms.cda.api.errors.InvalidItemException;
import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Strategy for resolving multiple time-series records in a single write request that resolve to
 * the same effective storage timestamp. CWMS stores values at minute precision, so records are
 * grouped by their date-time truncated to the minute. See ADR 0018.
 */
public enum DuplicateDateTimeResolution {
    /**
     * Reject the request if any storage timestamp has more than one record.
     */
    ERROR,
    /**
     * Keep the first record supplied for each storage timestamp.
     */
    FIRST,
    /**
     * Keep the last record supplied for each storage timestamp.
     */
    LAST,
    /**
     * Store the mean of the non-missing values supplied for each storage timestamp.
     */
    AVERAGE;

    static final int UNSCREENED_QUALITY = 0;
    static final int MISSING_QUALITY = 5;
    private static final long MINUTE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final int MAX_REPORTED_DUPLICATES = 10;

    /**
     * Parse a duplicate-date-time-resolution query parameter value, ignoring case.
     *
     * @param input parameter value, e.g. "first"
     * @return matching strategy, or null if input is null
     * @throws IllegalArgumentException if the value is not a known strategy
     */
    public static DuplicateDateTimeResolution getDuplicateDateTimeResolution(String input) {
        DuplicateDateTimeResolution retval = null;
        if (input != null) {
            retval = DuplicateDateTimeResolution.valueOf(input.trim().toUpperCase(Locale.ROOT));
        }
        return retval;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Resolve records that share a storage timestamp according to this strategy.
     *
     * @param values records in request payload order
     * @return records with at most one record per storage timestamp. If there are no
     *         duplicates the input list is returned unchanged.
     * @throws InvalidItemException if this strategy is {@link #ERROR} and duplicates exist
     */
    public List<TimeSeries.Record> resolve(List<TimeSeries.Record> values) {
        if (values == null || values.size() < 2 || !hasDuplicates(values)) {
            return values;
        }

        Map<Long, List<TimeSeries.Record>> groups = new LinkedHashMap<>();
        for (TimeSeries.Record value : values) {
            groups.computeIfAbsent(storageMinute(value), k -> new ArrayList<>()).add(value);
        }

        if (this == ERROR) {
            throw duplicateError(groups);
        }

        List<TimeSeries.Record> retval = new ArrayList<>(groups.size());
        for (Map.Entry<Long, List<TimeSeries.Record>> entry : groups.entrySet()) {
            List<TimeSeries.Record> group = entry.getValue();
            if (this == FIRST) {
                retval.add(group.get(0));
            } else if (this == LAST) {
                retval.add(group.get(group.size() - 1));
            } else {
                retval.add(average(entry.getKey(), group));
            }
        }
        return retval;
    }

    private static boolean hasDuplicates(List<TimeSeries.Record> values) {
        Set<Long> seen = new HashSet<>();
        for (TimeSeries.Record value : values) {
            if (!seen.add(storageMinute(value))) {
                return true;
            }
        }
        return false;
    }

    private static long storageMinute(TimeSeries.Record value) {
        if (value.getDateTime() == null) {
            throw new InvalidItemException("Time-series records must include a date-time.", null);
        }
        return Math.floorDiv(value.getDateTime().getTime(), MINUTE_MILLIS);
    }

    private static InvalidItemException duplicateError(Map<Long, List<TimeSeries.Record>> groups) {
        List<Long> duplicateMinutes = groups.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        String reported = duplicateMinutes.stream()
                .limit(MAX_REPORTED_DUPLICATES)
                .map(minute -> Instant.ofEpochMilli(minute * MINUTE_MILLIS).toString())
                .collect(Collectors.joining(", "));
        if (duplicateMinutes.size() > MAX_REPORTED_DUPLICATES) {
            reported += String.format(", ... (%d more)", duplicateMinutes.size() - MAX_REPORTED_DUPLICATES);
        }
        return new InvalidItemException(String.format("Multiple values were supplied for the same minute "
                + "at %d date-time(s): %s. Use the duplicate-date-time-resolution parameter (first, last, average) "
                + "to resolve them.", duplicateMinutes.size(), reported), null);
    }

    /**
     * Average the non-missing values in the group. The quality code is the most common quality
     * among the averaged records (or among all records if every value is missing). If there is a
     * tie for most common, the quality cannot be determined and the value is stored as unscreened,
     * or as missing when the averaged value is missing.
     */
    private static TimeSeries.Record average(long minute, List<TimeSeries.Record> group) {
        List<TimeSeries.Record> nonMissing = group.stream()
                .filter(r -> !isMissing(r.getValue()))
                .collect(Collectors.toList());

        Double value = null;
        int quality;
        if (nonMissing.isEmpty()) {
            quality = mostCommonQuality(group, MISSING_QUALITY);
        } else {
            value = nonMissing.stream().mapToDouble(TimeSeries.Record::getValue).average().getAsDouble();
            quality = mostCommonQuality(nonMissing, UNSCREENED_QUALITY);
        }
        return new TimeSeries.Record(new Timestamp(minute * MINUTE_MILLIS), value, quality);
    }

    private static boolean isMissing(Double value) {
        return value == null || value.isNaN() || value == -Float.MAX_VALUE;
    }

    private static int mostCommonQuality(List<TimeSeries.Record> records, int tieQuality) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (TimeSeries.Record r : records) {
            counts.merge(r.getQualityCode(), 1, Integer::sum);
        }
        int max = counts.values().stream().max(Integer::compare).orElse(0);
        List<Integer> mostCommon = counts.entrySet().stream()
                .filter(e -> e.getValue() == max)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        return mostCommon.size() == 1 ? mostCommon.get(0) : tieQuality;
    }
}
