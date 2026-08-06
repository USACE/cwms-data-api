package cwms.cda.data.dto.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.errors.FieldException;
import cwms.cda.helpers.DTOMatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class ForecastLocationTest {

    @Test
    void testRoundTripJson() throws JsonProcessingException {
        ForecastLocation l1 = new ForecastLocation.Builder()
                .withLocationId("location")
                .withSortOrder(-1)
                .withIsPrimary(true)
                .build();

        ObjectMapper om = buildObjectMapper();

        String jsonString = om.writeValueAsString(l1);
        assertNotNull(jsonString);

        ForecastLocation l2 = om.readValue(jsonString, ForecastLocation.class);
        assertNotNull(l2);

        assertForecastLocationEquals(l1, l2);
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

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        return JsonV2.buildObjectMapper();
    }

    void assertForecastLocationEquals(ForecastLocation l1, ForecastLocation l2) throws JsonProcessingException {
        DTOMatch.assertMatch(l1, l2);
    }

}
