package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.sql.Clob;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TimeSeriesReadLimitsTest {
    private final TimeSeriesReadLimits limits = new TimeSeriesReadLimits(100, 1000, 8000);

    @Test
    void largeWindowAcquiresBulkCapacityBeforeItsDataCursorAndHardCapTakesPrecedence() {
        AtomicInteger acquisitions = new AtomicInteger();
        TimeSeriesReadLimits configured = new TimeSeriesReadLimits(100000, 1000000, 8000000);
        TimeSeriesReadLimits request = configured.withBulkAdmission(acquisitions::incrementAndGet);
        Instant begin = Instant.parse("2020-01-01T00:00:00Z");
        request.checkRegularWindow(begin, begin.plusSeconds(99999L * 60), 1);
        assertEquals(0, acquisitions.get());
        request.checkRegularWindow(begin, begin.plusSeconds(100000L * 60), 1);
        assertEquals(1, acquisitions.get());
        assertThrows(TimeSeriesReadLimits.Exceeded.class,
                () -> request.checkRegularWindow(begin, begin.plusSeconds(1000000L * 60), 1));
        assertEquals(1, acquisitions.get());
        configured.checkWindowRows(100001);
        assertEquals(1, acquisitions.get(), "Request admission must not leak into shared configuration");
    }

    @Test
    void oversizedLegacyClobIsRejectedBeforeReadingItsContent() throws Exception {
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(Long.MAX_VALUE);
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.readLegacy(clob, () -> { }));
        verify(clob, never()).getCharacterStream();
    }

    @Test
    void legacyReaderChecksActualContentAndPreservesSmallResponse() throws Exception {
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(2L);
        when(clob.getCharacterStream()).thenReturn(new StringReader("{}"));
        assertEquals("{}", limits.readLegacy(clob, () -> { }));
        when(clob.getCharacterStream()).thenReturn(new StringReader("x".repeat(8001)));
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.readLegacy(clob, () -> { }));
    }

    @Test
    void inclusiveLimitsRejectRatherThanTruncate() {
        assertDoesNotThrow(() -> limits.checkPageSize(-1));
        assertDoesNotThrow(() -> limits.checkPageSize(100));
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.checkPageSize(101));
        assertDoesNotThrow(() -> limits.checkResponseValues(100));
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.checkResponseValues(101));
        assertDoesNotThrow(() -> limits.checkWindowRows(1000));
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.checkWindowRows(1001));
        assertDoesNotThrow(() -> limits.checkLegacyCharacters(8000));
        assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> limits.checkLegacyCharacters(8001));
    }

    @Test
    void regularWindowCanBeRejectedBeforeOpeningDataCursor() {
        Instant begin = Instant.parse("2000-01-01T00:00:00Z");
        assertDoesNotThrow(() -> limits.checkRegularWindow(begin, begin.plusSeconds(999 * 60), 1));
        assertThrows(TimeSeriesReadLimits.Exceeded.class,
                () -> limits.checkRegularWindow(begin, begin.plusSeconds(1000 * 60), 1));
        assertDoesNotThrow(() -> limits.checkRegularWindow(begin, begin.plusSeconds(1000 * 60), 0));
    }
}
