package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.api.DataApiTestIT;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.udt.records.DATE_RANGE_T;

@Tag("integration")
class TimeSeriesReadLimitsTestIT extends DataApiTestIT {
    @Test
    void oracleGapCollectionIsReadAsBoundedRows() throws Exception {
        connectionAsWebUser(connection -> {
            TimeSeriesDaoImpl dao = new TimeSeriesDaoImpl(JooqDao.getDslContext(connection, "SPK"),
                    new MetricRegistry());
            Timestamp begin = Timestamp.from(Instant.parse("2024-01-01T00:00:00Z"));
            Timestamp end = Timestamp.from(Instant.parse("2024-01-01T00:05:00Z"));
            DATE_RANGE_T range = new DATE_RANGE_T(begin, end, "UTC", "T", "T", null);
            List<Timestamp> times = dao.fetchBoundedExpectedTimes(range, "1Minute", 0, "UTC",
                    new TimeSeriesReadLimits(10, 1000, 8000));
            assertEquals(6, times.size());
            assertEquals(begin, times.get(0));
            assertEquals(end, times.get(5));
            assertThrows(TimeSeriesReadLimits.Exceeded.class, () -> dao.fetchBoundedExpectedTimes(
                    range, "1Minute", 0, "UTC", new TimeSeriesReadLimits(2, 1000, 8000)));
        });
    }
}
