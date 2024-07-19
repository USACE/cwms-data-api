package cwms.cda.data.dto.timeseriesprofile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final public class TimeSeriesProfileTest {
    @Test
    void testTimeSeriesProfileSerializationRoundTrip() {
        TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeriesProfile.class);
        String serialized = Formats.format(contentType, timeSeriesProfile);
        TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfile.class), serialized, TimeSeriesProfile.class);
        testAssertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testTimeSeriesProfileSerializationRoundTripFromFile() throws Exception {
        TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofile.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfile.class), serialized, TimeSeriesProfile.class);
        testAssertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization from file failed");
    }

     static TimeSeriesProfile buildTestTimeSeriesProfile() {
        CwmsId locationId = new CwmsId.Builder()
                .withName("location")
                .withOfficeId("office")
                .build();
        CwmsId referenceTsId = new CwmsId.Builder()
                .withName("location.Elev.Inst.0.0.REV")
                .withOfficeId("office")
                .build();
        return new TimeSeriesProfile.Builder()
                .withKeyParameter("Depth")
                .withReferenceTsId(referenceTsId)
                .withLocationId(locationId)
                .withDescription("Description")
                .withParameterList(Arrays.asList("Temperature", "Depth"))
                .build();
    }

    public static void testAssertEquals(TimeSeriesProfile expected, TimeSeriesProfile actual, String message) {
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getDescription(), actual.getDescription(), message);
        assertEquals(expected.getParameterList(), actual.getParameterList(), message);
        assertEquals(expected.getKeyParameter(), actual.getKeyParameter(), message);
        if(expected.getReferenceTsId()!=null &&  actual.getReferenceTsId()!=null) {
            assertEquals(expected.getReferenceTsId().getName(), actual.getReferenceTsId().getName(), message);
            assertEquals(expected.getReferenceTsId().getOfficeId(), actual.getReferenceTsId().getOfficeId(), message);
        }
        else {
            assertEquals(expected.getReferenceTsId(), actual.getReferenceTsId(),message);
        }
    }
}
