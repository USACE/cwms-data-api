package cwms.cda.data.dto.timeseriesprofile;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimeValuePairTest {
    @Test
    void testTimeValuePairRoundTrip() {
        TimeValuePair timeValuePair = buildTestTimeValuePair();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeValuePair.class);
        String serialized = Formats.format(contentType, timeValuePair);
        TimeValuePair deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeValuePair.class), serialized, TimeValuePair.class);
        testAssertEquals(timeValuePair, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testTimeValuePairSerializationRoundTripFromFile() throws Exception {
        TimeValuePair timeValuePair = buildTestTimeValuePair();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timevaluepair.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeValuePair deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeValuePair.class), serialized, TimeValuePair.class);
        testAssertEquals(timeValuePair, deserialized, "Roundtrip serialization from file failed");
    }

    static TimeValuePair buildTestTimeValuePair() {
        return new TimeValuePair.Builder()
                .withValue(1.0)
                .withQuality(0)
                .withDateTime(Instant.parse("2025-07-09T12:00:00.00Z"))
                .build();
    }

    public static void testAssertEquals(TimeValuePair expected, TimeValuePair actual, String message) {
        assertEquals(expected.getValue(), actual.getValue(), message);
        assertEquals(expected.getDateTime(), actual.getDateTime(), message);
        assertEquals(expected.getQuality(), actual.getQuality(), message);
    }

}
