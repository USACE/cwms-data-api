package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void fixedGridArithmeticAndCursorAlignmentMatchIntervalLibraryAcrossCalendarBoundaries() throws Exception {
        int checked = 0;
        for (String name : new String[]{"1Minute", "5Minutes", "15Minutes", "1Hour", "6Hours", "12Hours",
                "1Day", "2Days", "3Days", "1Week"}) {
            java.util.Optional<mil.army.usace.hec.metadata.Interval> found =
                    mil.army.usace.hec.metadata.IntervalFactory.findAny(
                            mil.army.usace.hec.metadata.IntervalFactory.equalsName(name));
            if (!found.isPresent()) {
                continue; // Unsupported library intervals cannot enter the database page path.
            }
            checked++;
            mil.army.usace.hec.metadata.Interval interval = found.get();
            long seconds = interval.getSeconds();
            for (long offsetSeconds : new long[]{0, seconds / 120 * 60, seconds - 60}) {
                mil.army.usace.hec.metadata.IntervalOffset offset =
                        mil.army.usace.hec.metadata.IntervalOffset.fromSeconds(Math.toIntExact(offsetSeconds));
                for (String origin : new String[]{"2023-12-31T23:00:00Z", "2024-01-31T23:00:00Z",
                        "2024-02-28T23:00:00Z"}) {
                    java.time.Instant first = interval.getTimeOnNextOrCurrentInterval(
                            java.time.Instant.parse(origin), offset, java.time.ZoneOffset.UTC);
                    java.time.Instant next = first;
                    for (int index = 0; index < 200; index++) {
                        String example = name + " offset=" + offsetSeconds + " origin=" + origin + " step=" + index;
                        assertEquals(first.plusSeconds(index * seconds), next, example);
                        assertEquals(next, interval.getTimeOnNextOrCurrentInterval(next, offset,
                                java.time.ZoneOffset.UTC), example);
                        java.time.Instant cursor = next.plusMillis(500);
                        java.time.Instant restarted = interval.getTimeOnNextOrCurrentInterval(cursor, offset,
                                java.time.ZoneOffset.UTC);
                        if (restarted.isBefore(cursor)) {
                            restarted = interval.getNextIntervalTime(restarted, java.time.ZoneOffset.UTC);
                        }
                        assertEquals(next.plusSeconds(seconds), restarted, example);
                        next = interval.getNextIntervalTime(next, java.time.ZoneOffset.UTC);
                    }
                }
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(checked >= 4, "Expected common fixed intervals to be available");
    }

    @Test
    void knownTotalStopsAfterPageAndLookahead() {
        java.util.concurrent.atomic.AtomicInteger fetched = new java.util.concurrent.atomic.AtomicInteger();
        TimeSeriesPageReader page = TimeSeriesPageReader.readPage(row(0),
                () -> row(fetched.incrementAndGet() * 60_000L),
                LongStream.range(0, 1_000_000).mapToObj(i -> new Timestamp(i * 60_000L)).iterator(),
                null, 500, 1_000_000, () -> { });
        assertEquals(1_000_000, page.getTotal());
        assertEquals(501, page.getValues().size());
        assertEquals(501, fetched.get());
    }

    @Test
    void boundedPagesMatchCompleteMergeForSparseAndOffGridWindows() {
        java.util.Random random = new java.util.Random(20260917);
        for (int example = 0; example < 500; example++) {
            boolean trim = random.nextBoolean();
            boolean regular = random.nextBoolean();
            int size = random.nextInt(12);
            Timestamp cursor = new Timestamp(random.nextInt(120) * 1000L + random.nextInt(2) * 250L);
            java.util.List<TimeSeries.Record> raw = new java.util.ArrayList<>();
            int count = random.nextInt(40);
            for (int index = 0; index < count; index++) {
                raw.add(row(random.nextInt(100) * 1000L + random.nextInt(2) * 500L));
            }
            raw.sort(java.util.Comparator.comparing(TimeSeries.Record::getDateTime));
            long first = raw.isEmpty() ? 0 : raw.get(0).getDateTime().getTime();
            long last = raw.isEmpty() ? 0 : raw.get(raw.size() - 1).getDateTime().getTime();
            java.util.List<Timestamp> grid = regular ? LongStream.rangeClosed(0, 100)
                    .mapToObj(i -> new Timestamp(i * 1000))
                    .filter(time -> !trim || time.getTime() >= first)
                    .collect(java.util.stream.Collectors.toList()) : Collections.emptyList();
            Iterator<TimeSeries.Record> all = raw.iterator();
            TimeSeriesPageReader reference = TimeSeriesPageReader.read(all.hasNext() ? all.next() : null,
                    () -> all.hasNext() ? all.next() : null, grid.iterator(), trim, cursor, size);
            Iterator<TimeSeries.Record> selected = raw.stream()
                    .filter(value -> value.getDateTime().getTime() >= cursor.getTime())
                    .limit(size == 0 ? 0 : size + 1L).iterator();
            Iterator<Timestamp> pageGrid = grid.stream()
                    .filter(time -> time.getTime() >= cursor.getTime())
                    .filter(time -> !trim || (!raw.isEmpty() && time.getTime() <= last)).iterator();
            TimeSeriesPageReader bounded = TimeSeriesPageReader.readPage(selected.hasNext() ? selected.next() : null,
                    () -> selected.hasNext() ? selected.next() : null,
                    pageGrid, cursor, size, reference.getTotal(), () -> { });
            assertEquals(reference.getValues(), bounded.getValues(), "Example " + example);
            assertEquals(reference.getTotal(), bounded.getTotal());
        }
    }

    @Test
    void unlimitedPageStopsBeforeRetainingAnOversizedResult() {
        java.util.concurrent.atomic.AtomicInteger fetched = new java.util.concurrent.atomic.AtomicInteger();
        TimeSeriesReadLimits limits = new TimeSeriesReadLimits(10, 1000, 8000);
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> TimeSeriesPageReader.read(row(0),
                () -> row(fetched.incrementAndGet()), Collections.emptyIterator(), true, null, -1, () -> { }, limits));
        assertEquals(11, fetched.get());
    }

    @Test
    void smallPageCannotScanAnUnlimitedWindow() {
        java.util.concurrent.atomic.AtomicInteger fetched = new java.util.concurrent.atomic.AtomicInteger();
        TimeSeriesReadLimits limits = new TimeSeriesReadLimits(10, 100, 8000);
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> TimeSeriesPageReader.read(row(0),
                () -> row(fetched.incrementAndGet()), Collections.emptyIterator(), true, null, 1, () -> { }, limits));
        assertEquals(101, fetched.get());
    }

    @Test
    void deadlineInterruptsGapGenerationWithoutFetchingAnotherRow() {
        java.util.concurrent.atomic.AtomicInteger checked = new java.util.concurrent.atomic.AtomicInteger();
        assertThrows(cwms.cda.datasource.ReadDeadline.Expired.class, () -> TimeSeriesPageReader.read(
                null, () -> {
                    throw new AssertionError("Empty series must not fetch another row");
                },
                LongStream.range(0, 1_000_000).mapToObj(Timestamp::new).iterator(), false, null, 500,
                () -> {
                    if (checked.incrementAndGet() == 10) {
                        throw new cwms.cda.datasource.ReadDeadline.Expired();
                    }
                }));
        assertEquals(10, checked.get());
    }

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
