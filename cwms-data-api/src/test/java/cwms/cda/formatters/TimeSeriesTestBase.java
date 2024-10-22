package cwms.cda.formatters;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;

import cwms.cda.data.dto.TimeSeries;

public abstract class TimeSeriesTestBase {
    public abstract OutputFormatter getOutputFormatter();

    protected TimeSeries getTimeSeries() {
        TimeSeries ts = new TimeSeries(null, -1, 0, "Test.Test.Elev.0.0.RAW", "SPK", ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]"), ZonedDateTime.parse("2021-06-22T08:00:00-07:00[PST8PDT]"), null, Duration.ZERO);
        ts.addValue(Timestamp.from(ts.getBegin().toInstant()), 30.0, 0, null);
        return ts;
    }

    public void singleTimeseriesFormat() {
        TimeSeries ts = getTimeSeries();
        OutputFormatter v2 = getOutputFormatter();
		String result = v2.format(ts);
		assertNotNull(result);
        assertTrue(result.contains("Test.Test.Elev.0.0.RAW"));
		assertTrue(result.contains("values"));
    }

}
