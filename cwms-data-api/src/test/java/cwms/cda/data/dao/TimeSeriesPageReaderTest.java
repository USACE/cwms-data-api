package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class TimeSeriesPageReaderTest {
    @Test
    void retainsOnlyPageAndLookaheadWhileCountingMillionRows() {
        Iterator<TimeSeries.Record> rows = IntStream.range(0, 1_000_000)
                .mapToObj(i -> row(i * 60_000L)).iterator();
        TimeSeriesPageReader page = TimeSeriesPageReader.read(rows.next(),
                () -> rows.hasNext() ? rows.next() : null,
                LongStream.range(0, 1_000_000).mapToObj(i -> new Timestamp(i * 60_000L)).iterator(),
                true, null, 500);
        assertEquals(1_000_000, page.getTotal());
        assertEquals(501, page.getValues().size());
        assertEquals(30_000_000L, page.getValues().get(500).getDateTime().getTime());
    }

    @Test
    void countsGapsAndOffGridRowsBeforeCursor() {
        Iterator<TimeSeries.Record> rows = Arrays.asList(row(30_000), row(180_000)).iterator();
        TimeSeriesPageReader page = TimeSeriesPageReader.read(rows.next(),
                () -> rows.hasNext() ? rows.next() : null,
                LongStream.rangeClosed(0, 4).mapToObj(i -> new Timestamp(i * 60_000)).iterator(),
                false, new Timestamp(90_000), 1);
        assertEquals(6, page.getTotal());
        assertEquals(2, page.getValues().size());
        assertEquals(120_000, page.getValues().get(0).getDateTime().getTime());
        assertEquals(180_000, page.getValues().get(1).getDateTime().getTime());
    }

    @Test
    void trimStopsAtLastObservedRow() {
        Iterator<TimeSeries.Record> rows = Arrays.asList(row(30_000), row(180_000)).iterator();
        TimeSeriesPageReader page = TimeSeriesPageReader.read(rows.next(),
                () -> rows.hasNext() ? rows.next() : null,
                LongStream.rangeClosed(1, 4).mapToObj(i -> new Timestamp(i * 60_000)).iterator(),
                true, null, -1);
        assertEquals(4, page.getTotal());
        assertEquals(4, page.getValues().size());
        assertEquals(180_000, page.getValues().get(3).getDateTime().getTime());
    }

    @Test
    void metadataOnlyCountsWithoutRetainingValues() {
        TimeSeriesPageReader page = TimeSeriesPageReader.read(row(0), () -> null,
                Collections.emptyIterator(), false, null, 0);
        assertEquals(1, page.getTotal());
        assertEquals(0, page.getValues().size());
    }

    @Test
    void emptyTrimmedWindowHasNoSyntheticRows() {
        TimeSeriesPageReader page = TimeSeriesPageReader.read(null, () -> null,
                Collections.singletonList(new Timestamp(0)).iterator(), true, null, 500);
        assertEquals(0, page.getTotal());
        assertEquals(0, page.getValues().size());
    }

    private static TimeSeries.Record row(long millis) {
        return new TimeSeries.Record(new Timestamp(millis), 1.0, 0);
    }
}
