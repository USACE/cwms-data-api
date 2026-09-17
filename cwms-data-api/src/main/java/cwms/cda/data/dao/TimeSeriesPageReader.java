package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/** Counts the complete merged window while retaining only the requested page and its next cursor row. */
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
        TimeSeriesPageReader page = new TimeSeriesPageReader();
        TimeSeries.Record raw = first;
        Timestamp expected = expectedTimes.hasNext() ? expectedTimes.next() : null;
        long retainedLimit = pageSize == 0 ? 0 : pageSize < 0 ? Long.MAX_VALUE : (long) pageSize + 1;
        while (raw != null || (!trim && expected != null)) {
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
            page.total = Math.incrementExact(page.total);
            if (page.values.size() < retainedLimit
                    && (cursor == null || candidate.getDateTime().getTime() >= cursor.getTime())) {
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
