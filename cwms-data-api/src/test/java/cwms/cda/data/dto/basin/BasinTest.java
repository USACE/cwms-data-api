package cwms.cda.data.dto.basin;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


final class BasinTest {
    @Test
    void testBasinSerializationRoundTrip() {
        Basin basin = buildTestBasin(new LocationIdentifier.Builder()
                .withName("TEST_LOCATION1")
                .withOfficeId("NVE")
                .build(), false);
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, Basin.class), basin);
        Basin deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, Basin.class), serialized, Basin.class);
        assertEquals(basin, deserialized, "Roundtrip serialization failed");
        assertEquals(basin.hashCode(), deserialized.hashCode(), "Roundtrip serialization failed");
    }

    @Test
    void testBasinSerializationRoundTripFromFile() throws Exception {
        Basin basin = buildTestBasin(new LocationIdentifier.Builder()
                .withName("TEST_LOCATION2")
                .withOfficeId("MVR")
                .build(), false);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/basin/basin.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Basin deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, Basin.class), serialized, Basin.class);
        assertEquals(basin, deserialized, "Roundtrip serialization failed");

        Basin basin1 = buildTestBasin(new LocationIdentifier.Builder()
                .withName("TEST_LOCATION2")
                .withOfficeId("MVR")
                .build(), true);
        InputStream resource1 = this.getClass().getResourceAsStream("/cwms/cda/data/dto/basin/basin1.json");
        assertNotNull(resource1);
        String serialized1 = IOUtils.toString(resource1, StandardCharsets.UTF_8);
        Basin deserialized1 = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, Basin.class), serialized1, Basin.class);
        assertEquals(basin1, deserialized1, "Roundtrip serialization failed");
    }

    @Test
    void testValidate() {
        LocationIdentifier basinId = new LocationIdentifier.Builder()
                .withName("NAB")
                .withOfficeId("TEST_OFFICE3")
                .build();

        assertAll(() -> {
            Basin basin = new Basin.Builder().build();
            assertThrows(FieldException.class, basin::validate,
                    "Expected validate() to throw FieldException because Office Id field can't be null, but it didn't");
        }, () -> {
            Basin basin = new Basin.Builder().withBasinId(basinId).build();
            assertDoesNotThrow(basin::validate,
                    "Expected validate()");
        });
    }

    private Basin buildTestBasin(LocationIdentifier basinId, boolean parentBasinIdNull) {
        if (parentBasinIdNull) {
            return new Basin.Builder()
                    .withBasinId(basinId)
                    .withPrimaryStreamId(new LocationIdentifier.Builder()
                            .withName("TEST_LOCATION4")
                            .withOfficeId("MVP")
                            .build())
                    .withSortOrder(1.0)
                    .withBasinArea(1005.0)
                    .withContributingArea(1050.0)
                    .withParentBasinId(new LocationIdentifier.Builder().build())
                    .withAreaUnit("SI")
                    .build();
        } else {
            return new Basin.Builder()
                    .withBasinId(basinId)
                    .withPrimaryStreamId(new LocationIdentifier.Builder()
                            .withName("TEST_LOCATION4")
                            .withOfficeId("MVP")
                            .build())
                    .withSortOrder(1.0)
                    .withBasinArea(1005.0)
                    .withContributingArea(1050.0)
                    .withParentBasinId(new LocationIdentifier.Builder()
                            .withName("TEST_LOCATION5")
                            .withOfficeId("NAE")
                            .build())
                    .withAreaUnit("SI")
                    .build();
        }
    }
}
