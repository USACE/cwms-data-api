package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import cwms.radar.formatters.json.JsonV2;
import java.time.ZoneId;
import mil.army.usace.hec.metadata.DataSetIllegalArgumentException;
import mil.army.usace.hec.metadata.OfficeId;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifier;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifierFactory;
import org.junit.jupiter.api.Test;

class TimeSeriesIdentifierTest {
    @Test
    void test_serialize() throws JsonProcessingException, DataSetIllegalArgumentException {
        cwms.radar.data.dto.TimeSeriesIdentifier tsId = buildTSId();

        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsId);
        assertNotNull(result);
        System.out.println(result);
    }

    private static cwms.radar.data.dto.TimeSeriesIdentifier buildTSId() throws DataSetIllegalArgumentException {
        OfficeId swt = new OfficeId("SWT");
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        TimeSeriesIdentifier timeSeriesidentifier = TimeSeriesIdentifierFactory.from(swt, "BASE"
                + "-SUB.Area.Inst.1Hour.0.TEST", zoneId);

        cwms.radar.data.dto.TimeSeriesIdentifier.Builder builder =
                new cwms.radar.data.dto.TimeSeriesIdentifier.Builder();
        cwms.radar.data.dto.TimeSeriesIdentifier tsId =
                builder.withTimeSeriesIdentifier(timeSeriesidentifier).build();
        return tsId;
    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException, DataSetIllegalArgumentException {
        cwms.radar.data.dto.TimeSeriesIdentifier tsId = buildTSId();

        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsId);

        cwms.radar.data.dto.TimeSeriesIdentifier tsId2 = om.readValue(result,
                cwms.radar.data.dto.TimeSeriesIdentifier.class);

        assertNotNull(tsId2);

        assertEquals(tsId.getLocationId().getLocation(), tsId2.getLocationId().getLocation());
        assertEquals(tsId.getParameter().getParameter(), tsId2.getParameter().getParameter());
        assertEquals(tsId.getParameterType().getType(), tsId2.getParameterType().getType());
        assertEquals(tsId.getDuration().getMinutes(), tsId2.getDuration().getMinutes());
        assertEquals(tsId.getInterval(), tsId2.getInterval());
        assertEquals(tsId.getVersion(), tsId2.getVersion());

    }


    @Test
    void testToString() throws JsonProcessingException, DataSetIllegalArgumentException {
        cwms.radar.data.dto.TimeSeriesIdentifier tsId = buildTSId();

        String result = tsId.toString();
        assertNotNull(result);
        System.out.println(result);

        assertEquals("BASE-SUB.Area.Inst.1Hour.0.TEST", result);
    }
}
