package cwms.cda.data.dto.timeseriesprofile;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParameterInfoTest {
    @Test
    void testParameterInfoColumnarRoundTrip() {
        ParameterInfo parameterInfo = buildParameterInfoColumnar();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, ParameterInfoColumnar.class);
        String serialized = Formats.format(contentType, parameterInfo);
        ParameterInfoColumnar deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, ParameterInfoColumnar.class), serialized, ParameterInfoColumnar.class);
        testAssertEquals((ParameterInfoColumnar)parameterInfo, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testParameterInfoIndexedRoundTrip() {
        ParameterInfo parameterInfo = buildParameterInfoIndexed();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, ParameterInfoIndexed.class);
        String serialized = Formats.format(contentType, parameterInfo);
        ParameterInfoIndexed deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, ParameterInfoIndexed.class), serialized, ParameterInfoIndexed.class);
        testAssertEquals((ParameterInfoIndexed) parameterInfo, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testParameterInfoColumnarRoundTripFromFile() throws Exception {
        ParameterInfo parameterInfo = buildParameterInfoColumnar();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/parameterinfocolumnar.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ParameterInfoColumnar deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, ParameterInfoColumnar.class), serialized, ParameterInfoColumnar.class);
        testAssertEquals((ParameterInfoColumnar)parameterInfo, deserialized, "Roundtrip serialization from file failed");
    }
    @Test
    void testParameterInfoIndexedRoundTripFromFile() throws Exception {
        ParameterInfo parameterInfo = buildParameterInfoIndexed();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/parameterinfoindexed.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ParameterInfoIndexed deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, ParameterInfoIndexed.class), serialized, ParameterInfoIndexed.class);
        testAssertEquals((ParameterInfoIndexed)parameterInfo, deserialized, "Roundtrip serialization from file failed");
    }

    static ParameterInfo buildParameterInfoColumnar() {
        return new ParameterInfoColumnar.Builder()
                .withEndColumn(20)
                .withStartColumn(10)
                .withParameter("Depth")
                .withUnit("m")
                .build();
    }
    static ParameterInfo buildParameterInfoIndexed() {
        return new ParameterInfoIndexed.Builder()
                .withIndex(1)
                .withParameter("Depth")
                .withUnit("m")
                .build();
    }

    static void testAssertEquals(ParameterInfoColumnar expected, ParameterInfoColumnar actual, String message) {
        assertEquals(expected.getParameter(), actual.getParameter(), message);
        assertEquals(expected.getParameterInfoString(), actual.getParameterInfoString(), message);
        assertEquals(expected.getStartColumn(), actual.getStartColumn(), message);
        assertEquals(expected.getEndColumn(), actual.getEndColumn(), message);
        assertEquals(expected.getUnit(), actual.getUnit(), message);
    }
    static void testAssertEquals(ParameterInfoIndexed expected, ParameterInfoIndexed actual, String message) {
        assertEquals(expected.getParameter(), actual.getParameter(), message);
        assertEquals(expected.getParameterInfoString(), actual.getParameterInfoString(), message);
        assertEquals(expected.getIndex(), actual.getIndex(), message);
        assertEquals(expected.getUnit(), actual.getUnit(), message);
    }

}
