package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import mil.army.usace.hec.metadata.DataSetIllegalArgumentException;
import mil.army.usace.hec.metadata.OfficeId;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifier;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifierFactory;
import org.junit.jupiter.api.Test;

class DurationTest {

    @Test
    void testBuilder() throws DataSetIllegalArgumentException {

        OfficeId swt = new OfficeId("SWT");
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        TimeSeriesIdentifier timeSeriesidentifier = TimeSeriesIdentifierFactory.from(swt, "BASE-SUB.Area.Inst.1Hour.0.TEST", zoneId);
        mil.army.usace.hec.metadata.Duration dur = timeSeriesidentifier.getDuration();

        Duration.Builder builder = new Duration.Builder();
        builder.withDuration(dur);

        Duration dto = builder.build();

        assertNotNull(dto);
        assertEquals(dur.getMinutes(), dto.getMinutes());
        assertEquals(dur.getTimeOfRecord().name(), dto.getTimeOfRecord().name());
    }

}