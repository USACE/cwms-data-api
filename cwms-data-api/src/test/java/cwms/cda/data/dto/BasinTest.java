package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.data.dto.basinconnectivity.Stream;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


final class BasinTest {
    @Test
    void testBasinSerializationRoundTrip() {
        Basin basin = buildTestBasin("TEST_BASIN1", "TEST_OFFICE1");
        String serialized = Formats.format(Formats.parseHeader(Formats.JSON), basin);
        Basin deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSON), serialized, Basin.class);
        assertEquals(basin, deserialized, "Roundtrip serialization failed");
        assertEquals(basin.hashCode(), deserialized.hashCode(), "Roundtrip serialization failed");
    }

    @Test
    void testBasinSerializationRoundTripFromFile() throws Exception {
        Basin basin = buildTestBasin("TEST_BASIN", "TEST_OFFICE");
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/basin/basin.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Basin deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSON), serialized, Basin.class);
        assertEquals(basin, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testValidate() {
        String basinId = "TEST_BASIN2";
        String officeId = "SPK";

        assertAll(() -> {
            Basin basin = new Basin.Builder().build();
            assertThrows(FieldException.class, basin::validate,
                    "Expected validate() to throw FieldException because Office Id field can't be null, but it didn't");
        }, () -> {
            Basin basin = new Basin.Builder().withBasinName(basinId).build();
            assertThrows(FieldException.class, basin::validate,
                    "Expected validate() to throw FieldException because Basin Name field can't be null, but it didn't");
        }, () -> {
            Basin basin = new Basin.Builder().withBasinName(basinId).withOfficeId(officeId).build();
            assertDoesNotThrow(basin::validate,
                    "Expected validate()");
        });
    }

    private Basin buildTestBasin(String basinName, String officeId) {
        return new Basin.Builder()
                .withBasinName(basinName)
                .withOfficeId(officeId)
                .withPrimaryStream(buildTestPrimaryStream())
                .withSortOrder(1.0)
                .withBasinArea(1.0)
                .withContributingArea(1.0)
                .withParentBasinId("TEST_PARENT2")
                .withStreams(buildTestStreams())
                .withSubBasin(buildTestSubBasins())
                .build();
    }

    private Collection<Stream> buildTestStreams() {

        Collection<Stream> streams = new ArrayList<>();
        streams.add(new Stream.Builder("TEST_STREAM5", true, 150.0, "TEST_OFFICE5")
                .build());
        streams.add(new Stream.Builder("TEST_STREAM6", false, 250.0, "TEST_OFFICE6")
                .build());
        streams.add(new Stream.Builder("TEST_STREAM7", true, 350.0, "TEST_OFFICE7")
                .build());

        return streams;
    }

    private Stream buildTestPrimaryStream() {
        return new Stream.Builder("TEST_STREAM", true, 150.0, "TEST_OFFICE4")
                .build();
    }

    private Collection<Basin> buildTestSubBasins() {
        Collection<Basin> subBasins = new ArrayList<>();
        subBasins.add(new Basin.Builder()
                .withBasinName("TEST_BASIN8")
                .withOfficeId("TEST_OFFICE8")
                .withPrimaryStream(buildTestPrimaryStream())
                .withSortOrder(2.0)
                .withBasinArea(195.0)
                .withContributingArea(180.0)
                .withParentBasinId("TEST_PARENT8")
                .withStreams(buildTestStreams())
                .build());
        subBasins.add(new Basin.Builder()
                .withBasinName("TEST_BASIN9")
                .withOfficeId("TEST_OFFICE9")
                .withPrimaryStream(buildTestPrimaryStream())
                .withSortOrder(3.0)
                .withBasinArea(295.0)
                .withParentBasinId("TEST_PARENT9")
                .withStreams(buildTestStreams())
                .build());

        return subBasins;
    }
}
