package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.errors.InvalidItemException;
import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class DuplicateDateTimeResolutionTest {

    private static final Instant NOON = Instant.parse("2024-01-01T12:00:00Z");

    private static TimeSeries.Record rec(long secondsAfterNoon, Double value, int quality) {
        return new TimeSeries.Record(Timestamp.from(NOON.plusSeconds(secondsAfterNoon)), value, quality);
    }

    @ParameterizedTest
    @ValueSource(strings = {"error", "FIRST", "Last", " average "})
    void testParseIsCaseInsensitive(String input) {
        assertEquals(input.trim().toLowerCase(),
                DuplicateDateTimeResolution.getDuplicateDateTimeResolution(input).toString());
    }

    @Test
    void testParseInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> DuplicateDateTimeResolution.getDuplicateDateTimeResolution("median"));
        assertNull(DuplicateDateTimeResolution.getDuplicateDateTimeResolution(null));
    }

    @ParameterizedTest
    @EnumSource(DuplicateDateTimeResolution.class)
    void testNoDuplicatesReturnsInput(DuplicateDateTimeResolution duplicateResolution) {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, 1.0, 0), rec(60, 2.0, 0), rec(3600, 3.0, 0));
        assertSame(values, duplicateResolution.resolve(values));
    }

    @Test
    void testSameHourDifferentMinuteNotGrouped() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, 1.0, 0), rec(59 * 60, 2.0, 0));
        assertEquals(2, DuplicateDateTimeResolution.ERROR.resolve(values).size());
    }

    @Test
    void testErrorOnIdenticalTimestamps() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, 1.0, 0), rec(0, 2.0, 0));
        InvalidItemException ex = assertThrows(InvalidItemException.class,
                () -> DuplicateDateTimeResolution.ERROR.resolve(values));
        assertTrue(ex.getMessage().contains("2024-01-01T12:00:00Z"), ex.getMessage());
    }

    @Test
    void testErrorOnSubMinuteTimestamps() {
        List<TimeSeries.Record> values = Arrays.asList(rec(10, 1.0, 0), rec(40, 2.0, 0));
        assertThrows(InvalidItemException.class, () -> DuplicateDateTimeResolution.ERROR.resolve(values));
    }

    @Test
    void testErrorMessageLimitsReportedTimes() {
        TimeSeries.Record[] values = new TimeSeries.Record[30];
        for (int i = 0; i < 15; i++) {
            values[2 * i] = rec(i * 60L, 1.0, 0);
            values[2 * i + 1] = rec(i * 60L + 1, 2.0, 0);
        }
        InvalidItemException ex = assertThrows(InvalidItemException.class,
                () -> DuplicateDateTimeResolution.ERROR.resolve(Arrays.asList(values)));
        assertTrue(ex.getMessage().contains("15 date-time(s)"), ex.getMessage());
        assertTrue(ex.getMessage().contains("(5 more)"), ex.getMessage());
    }

    @Test
    void testFirstKeepsPayloadOrder() {
        // Later timestamp within the minute is supplied first; order must not be re-sorted.
        TimeSeries.Record first = rec(40, 1.0, 3);
        List<TimeSeries.Record> values = Arrays.asList(first, rec(10, 2.0, 5), rec(60, 9.0, 0));
        List<TimeSeries.Record> resolved = DuplicateDateTimeResolution.FIRST.resolve(values);
        assertEquals(Arrays.asList(first, values.get(2)), resolved);
    }

    @Test
    void testLastKeepsValueAndQualityTogether() {
        TimeSeries.Record last = rec(10, 2.0, 5);
        List<TimeSeries.Record> values = Arrays.asList(rec(120, 9.0, 0), rec(40, 1.0, 3), last);
        List<TimeSeries.Record> resolved = DuplicateDateTimeResolution.LAST.resolve(values);
        assertEquals(Arrays.asList(values.get(0), last), resolved);
        assertEquals(2.0, resolved.get(1).getValue());
        assertEquals(5, resolved.get(1).getQualityCode());
    }

    @Test
    void testAverageUsesMostCommonQuality() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, 1.0, 3), rec(20, 2.0, 3), rec(40, 6.0, 5));
        List<TimeSeries.Record> resolved = DuplicateDateTimeResolution.AVERAGE.resolve(values);
        assertEquals(1, resolved.size());
        assertEquals(3.0, resolved.get(0).getValue(), 1e-9);
        assertEquals(3, resolved.get(0).getQualityCode());
        assertEquals(Timestamp.from(NOON), resolved.get(0).getDateTime());
    }

    @Test
    void testAverageQualityTieIsUnscreened() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, 1.0, 3), rec(20, 2.0, 5));
        TimeSeries.Record resolved = DuplicateDateTimeResolution.AVERAGE.resolve(values).get(0);
        assertEquals(1.5, resolved.getValue(), 1e-9);
        assertEquals(DuplicateDateTimeResolution.UNSCREENED_QUALITY, resolved.getQualityCode());
    }

    @Test
    void testAverageIgnoresMissingValues() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, null, 5), rec(10, (double) -Float.MAX_VALUE, 5),
                rec(20, 4.0, 3), rec(30, 8.0, 3));
        TimeSeries.Record resolved = DuplicateDateTimeResolution.AVERAGE.resolve(values).get(0);
        assertEquals(6.0, resolved.getValue(), 1e-9);
        assertEquals(3, resolved.getQualityCode());
    }

    @Test
    void testAverageAllMissing() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, null, 5), rec(10, null, 5));
        TimeSeries.Record resolved = DuplicateDateTimeResolution.AVERAGE.resolve(values).get(0);
        assertNull(resolved.getValue());
        assertEquals(5, resolved.getQualityCode());
    }

    @Test
    void testAverageAllMissingQualityTieIsMissing() {
        List<TimeSeries.Record> values = Arrays.asList(rec(0, null, 0), rec(10, null, 5));
        TimeSeries.Record resolved = DuplicateDateTimeResolution.AVERAGE.resolve(values).get(0);
        assertNull(resolved.getValue());
        assertEquals(DuplicateDateTimeResolution.MISSING_QUALITY, resolved.getQualityCode());
    }
}
