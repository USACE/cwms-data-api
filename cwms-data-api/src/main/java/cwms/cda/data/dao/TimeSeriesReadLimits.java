package cwms.cda.data.dao;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

/** Request limits checked before retaining unbounded result data. */
public final class TimeSeriesReadLimits {
    private final int maximumValues;
    private final int maximumWindowRows;
    private final int maximumLegacyCharacters;

    /** Uses the operator's limits, with conservative defaults for a small API task. */
    public TimeSeriesReadLimits() {
        this(Integer.getInteger("cwms.cda.timeseries.maxResponseValues", 100_000),
                Integer.getInteger("cwms.cda.timeseries.maxWindowRows", 1_000_000),
                Integer.getInteger("cwms.cda.timeseries.maxLegacyCharacters", 8_000_000));
    }

    TimeSeriesReadLimits(int maximumValues, int maximumWindowRows, int maximumLegacyCharacters) {
        if (maximumValues < 1 || maximumValues > 1_000_000
                || maximumWindowRows < maximumValues || maximumWindowRows > 10_000_000
                || maximumLegacyCharacters < 1 || maximumLegacyCharacters > 32_000_000) {
            throw new IllegalArgumentException("Invalid time-series read limits");
        }
        this.maximumValues = maximumValues;
        this.maximumWindowRows = maximumWindowRows;
        this.maximumLegacyCharacters = maximumLegacyCharacters;
    }

    /**
     * Validates explicit page sizes, including sizes supplied inside cursor tokens.
     * @throws IllegalArgumentException if the page size is less than -1
     * @throws Exceeded if the page size exceeds the response limit
     */
    public void checkPageSize(int pageSize) {
        if (pageSize < -1) {
            throw new IllegalArgumentException("page-size must be -1 or greater");
        }
        if (pageSize > maximumValues) {
            throw new Exceeded("Requested page exceeds the time-series response limit; use a smaller page-size.");
        }
    }

    void checkWindowRows(long rows) {
        if (rows > maximumWindowRows) {
            throw new Exceeded("Requested window exceeds the time-series read limit; use a narrower date range.");
        }
    }

    int maximumWindowRows() {
        return maximumWindowRows;
    }

    void checkResponseValues(long values) {
        if (values > maximumValues) {
            throw new Exceeded("Requested result exceeds the time-series response limit; use pagination.");
        }
    }

    void checkLegacyCharacters(long characters) {
        if (characters > maximumLegacyCharacters) {
            throw new Exceeded("Requested result exceeds the time-series response limit; use a narrower date range.");
        }
    }

    void checkRegularWindow(Instant begin, Instant end, long intervalMinutes) {
        if (intervalMinutes > 0 && !end.isBefore(begin)) {
            long intervals = Duration.between(begin, end).toMinutes() / intervalMinutes;
            checkWindowRows(intervals + 1);
        }
    }

    String readLegacy(Clob clob, Runnable checkDeadline) throws SQLException, IOException {
        if (clob == null) {
            return null;
        }
        checkDeadline.run();
        checkLegacyCharacters(clob.length());
        StringBuilder result = new StringBuilder();
        char[] buffer = new char[8192];
        try (Reader reader = clob.getCharacterStream()) {
            int length;
            while ((length = reader.read(buffer)) != -1) {
                checkDeadline.run();
                checkLegacyCharacters((long) result.length() + length);
                result.append(buffer, 0, length);
            }
        }
        return result.toString();
    }

    /** Signals an explicit rejection; a result must never be silently truncated to fit a limit. */
    public static final class Exceeded extends RuntimeException {
        Exceeded(String message) {
            super(message);
        }
    }
}
