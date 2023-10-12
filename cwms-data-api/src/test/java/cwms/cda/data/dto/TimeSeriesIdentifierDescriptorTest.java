package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class TimeSeriesIdentifierDescriptorTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsID);
        assertNotNull(result);

    }

    private TimeSeriesIdentifierDescriptor buildTsId() {
        TimeSeriesIdentifierDescriptor.Builder builder =
                new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withOfficeId("SWT");
        builder = builder.withTimeSeriesId("BASE-SUB.Area.Inst.1Hour.0.TEST");
        builder = builder.withZoneId(ZoneId.of("America/Los_Angeles"));
        builder = builder.withIntervalOffsetMinutes(0L);
        builder = builder.withActive(true);

        return builder.build();
    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        jsonRoundtrip(tsID);

        // See if we can pass null for intervalOffsetMinutes
        TimeSeriesIdentifierDescriptor.Builder builder =
                new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withIntervalOffsetMinutes(null);
        jsonRoundtrip(builder.build());

        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withActive(false);
        jsonRoundtrip(builder.build());

        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withZoneId(null);
        jsonRoundtrip(builder.build());

        // Not sure if I should allow officeId to be null...
        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withOfficeId(null);
        jsonRoundtrip(builder.build());

    }

    private static void jsonRoundtrip(TimeSeriesIdentifierDescriptor tsID) throws JsonProcessingException {
        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsID);

        TimeSeriesIdentifierDescriptor tsID2 = om.readValue(result,
                TimeSeriesIdentifierDescriptor.class);

        assertNotNull(tsID2);

        assertEquals(tsID.getTimeSeriesId(), tsID2.getTimeSeriesId());
        assertEquals(tsID.getOfficeId(), tsID2.getOfficeId());
        assertEquals(tsID.getTimezoneName(), tsID2.getTimezoneName());
        assertEquals(tsID.getIntervalOffsetMinutes(), tsID2.getIntervalOffsetMinutes());
        assertEquals(tsID.isActive(), tsID2.isActive());
    }


    @Test
    void test_serialization_with_formats()
    {
        // This test verifies that the TimeSeriesIdentifierDescriptor can be serialized by the Formats class.
        // It will fail like:
        //  No Format for this content-type and data-type : (application/json;version=2, cwms.cda.data.dto.TimeSeriesIdentifierDescriptor)
        // If JsonV2 does not contain TimeSeriesIdentifierDescriptor in its list of classes
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        ContentType contentType = Formats.parseHeader(Formats.JSONV2);
        String jsonStr = Formats.format(contentType, tsID);
        assertNotNull(jsonStr);
    }

}