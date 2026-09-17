package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/** Merges observed rows and expected gaps while retaining only the requested page and its next cursor row. */
final class TimeSeriesPageReader {
    private final List<TimeSeries.Record> values = new ArrayList<>();
    private int total;

    private TimeSeriesPageReader() {
    }

    static TimeSeriesPageReader read(TimeSeries.Record first, Supplier<TimeSeries.Record> nextRow,
                                    Iterator<Timestamp> expectedTimes, boolean trim,
                                    Timestamp cursor, int pageSize) {
        return read(first, nextRow, expectedTimes, trim, cursor, pageSize, () -> { });
    }

    static TimeSeriesPageReader read(TimeSeries.Record first, Supplier<TimeSeries.Record> nextRow,
                                    Iterator<Timestamp> expectedTimes, boolean trim,
                                    Timestamp cursor, int pageSize, Runnable checkDeadline) {
        return read(first, nextRow, expectedTimes, trim, cursor, pageSize, checkDeadline, null);
    }

    static TimeSeriesPageReader read(TimeSeries.Record first, Supplier<TimeSeries.Record> nextRow,
                                    Iterator<Timestamp> expectedTimes, boolean trim,
                                    Timestamp cursor, int pageSize, Runnable checkDeadline, TimeSeriesReadLimits limits) {
        return readInternal(first, nextRow, expectedTimes, trim, cursor, pageSize, checkDeadline, limits, null);
    }

    /** Merges a database-counted page whose expected timestamps already respect trimming and the cursor. */
    static TimeSeriesPageReader readPage(TimeSeries.Record first, Supplier<TimeSeries.Record> nextRow,
                                        Iterator<Timestamp> expectedTimes, Timestamp cursor, int pageSize,
                                        int total, Runnable checkDeadline) {
        if (pageSize < 0 || total < 0) {
            throw new IllegalArgumentException("A database-counted page requires a nonnegative size and total");
        }
        return readInternal(first, nextRow, expectedTimes, false, cursor, pageSize, checkDeadline, null, total);
    }

    private static TimeSeriesPageReader readInternal(TimeSeries.Record first, Supplier<TimeSeries.Record> nextRow,
                                                     Iterator<Timestamp> expectedTimes, boolean trim,
                                                     Timestamp cursor, int pageSize, Runnable checkDeadline,
                                                     TimeSeriesReadLimits limits, Integer knownTotal) {
        if (limits != null) {
            limits.checkPageSize(pageSize);
        }
        TimeSeriesPageReader page = new TimeSeriesPageReader();
        page.total = knownTotal == null ? 0 : knownTotal;
        TimeSeries.Record raw = first;
        Timestamp expected = expectedTimes.hasNext() ? expectedTimes.next() : null;
        long retainedLimit = pageSize == 0 ? 0 : pageSize < 0 ? Long.MAX_VALUE : (long) pageSize + 1;
        while ((raw != null || (!trim && expected != null))
                && (knownTotal == null || page.values.size() < retainedLimit)) {
            checkDeadline.run();
            if (Thread.currentThread().isInterrupted()) {
                throw new java.util.concurrent.CancellationException("Time-series read cancelled");
            }
            TimeSeries.Record candidate;
            if (expected != null && (raw == null || expected.getTime() < raw.getDateTime().getTime())) {
                candidate = new TimeSeries.Record(expected, null, 0);
                expected = expectedTimes.hasNext() ? expectedTimes.next() : null;
            } else {
                candidate = raw;
                if (expected != null && expected.getTime() == raw.getDateTime().getTime()) {
                    expected = expectedTimes.hasNext() ? expectedTimes.next() : null;
                }
                raw = nextRow.get();
            }
            if (knownTotal == null) {
                page.total = Math.incrementExact(page.total);
                if (limits != null) {
                    limits.checkWindowRows(page.total);
                }
            }
            if (page.values.size() < retainedLimit
                    && (cursor == null || candidate.getDateTime().getTime() >= cursor.getTime())) {
                if (limits != null && pageSize == -1) {
                    limits.checkResponseValues((long) page.values.size() + 1);
                }
                page.values.add(candidate);
            }
        }
        return page;
    }

    int getTotal() {
        return total;
    }

    List<TimeSeries.Record> getValues() {
        return values;
    }
}
