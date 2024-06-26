package cwms.cda.data.dto.timeseriesprofile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class TimeSeriesProfileTest {
    @Test
    void testTimeSeriesProfileSerializationRoundTrip() {
        TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2);
        String serialized = Formats.format(contentType, timeSeriesProfile);
        TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2), serialized, TimeSeriesProfile.class);
        testAssertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testTimeSeriesProfileSerializationRoundTripFromFile() throws Exception {
        TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofile.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2), serialized, TimeSeriesProfile.class);
        testAssertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization from file failed");
    }

    private TimeSeriesProfile buildTestTimeSeriesProfile() {
        return new TimeSeriesProfile.Builder()
                .withOfficeId("Office")
                .withKeyParameter("Depth")
                .withRefTsId("TimeSeries")
                .withLocationId("Location")
                .withDescription("Description")
                .withParameterList(Arrays.asList("Temperature", "Depth"))
                .build();
    }

    private void testAssertEquals(TimeSeriesProfile expected, TimeSeriesProfile actual, String message) {
        assertEquals(expected.getLocationId(), actual.getLocationId(), message);
        assertEquals(expected.getDescription(), actual.getDescription(), message);
        assertEquals(expected.getParameterList(), actual.getParameterList(), message);
        assertEquals(expected.getKeyParameter(), actual.getKeyParameter(), message);
        assertEquals(expected.getRefTsId(), actual.getRefTsId(), message);
    }
}
