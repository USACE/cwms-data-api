package cwms.cda.data.dto.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;

import org.junit.jupiter.api.Test;

public class ForecastLocationTest {

    @Test
    void testRoundTripJson() {
        ForecastLocation l1 = new ForecastLocation.Builder()
                .withLocationId("location")
                .withSortOrder(-1)
                .withIsPrimary(true)
                .build();
        ContentType contentType = Formats.parseHeader(Formats.JSON, ForecastLocation.class);

        String jsonString = Formats.format(contentType, l1);
        assertNotNull(jsonString);

        ForecastLocation l2 = Formats.parseContent(contentType, jsonString, ForecastLocation.class);
        assertNotNull(l2);

        DTOMatch.assertMatch(l1, l2);
    }

    @Test
    void testMissingRequired() {
        assertThrows(FieldException.class, () -> new ForecastLocation.Builder()
                .build().validate());
        assertThrows(FieldException.class, () -> new ForecastLocation.Builder()
                .withSortOrder(-1)
                .build().validate());
        assertThrows(FieldException.class, () -> new ForecastLocation.Builder()
                .withLocationId("loc")
                .build().validate());
    }

    @Test
    void testInvalidPrimary() {
        assertThrows(IllegalArgumentException.class, () -> new ForecastLocation.Builder()
                .withLocationId("loc")
                .withSortOrder(-1)
                .withIsPrimary(false)
                .build());

        assertThrows(IllegalArgumentException.class, () -> new ForecastLocation.Builder()
                .withLocationId("loc")
                .withSortOrder(1)
                .withIsPrimary(true)
                .build());

        assertThrows(IllegalArgumentException.class, () -> new ForecastLocation.Builder()
                .withLocationId("loc")
                .withIsPrimary(true)
                .withSortOrder(1)
                .build());

        assertThrows(IllegalArgumentException.class, () -> new ForecastLocation.Builder()
                .withLocationId("loc")
                .withIsPrimary(false)
                .withSortOrder(-1)
                .build());
    }

    @Test
    void testGetters() {
        ForecastLocation location = new ForecastLocation.Builder()
                .withLocationId("location")
                .withSortOrder(-1)
                .withIsPrimary(true)
                .build();

        assertEquals("location", location.getLocationId());
        assertEquals(-1, location.getSortOrder());
        assertEquals(Boolean.TRUE, location.isPrimary());

        location = new ForecastLocation.Builder()
                .withLocationId("location")
                .withIsPrimary(true)
                .build();

        assertEquals("location", location.getLocationId());
        assertEquals(-1, location.getSortOrder());
        assertEquals(Boolean.TRUE, location.isPrimary());

        location = new ForecastLocation.Builder()
                .withLocationId("location")
                .withSortOrder(-1)
                .build();

        assertEquals("location", location.getLocationId());
        assertEquals(-1, location.getSortOrder());
        assertEquals(Boolean.TRUE, location.isPrimary());

        location = new ForecastLocation.Builder()
                .withLocationId("location")
                .withSortOrder(1)
                .build();

        assertEquals("location", location.getLocationId());
        assertEquals(1, location.getSortOrder());
        assertEquals(Boolean.FALSE, location.isPrimary());

    }

}
