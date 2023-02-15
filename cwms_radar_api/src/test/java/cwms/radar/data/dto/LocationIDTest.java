package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.json.JsonV2;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class LocationIDTest {

    @Test
    void testBuilderBaseAndSub(){
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        LocationID.Builder builder = new LocationID.Builder();
        builder.withBaseLocation("BASE");
        builder.withSubLocation("SUB");
        builder.withZoneId(zoneId);
        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());
        assertEquals("SUB", locationID.getSubLocation());
        assertEquals("BASE-SUB", locationID.getLocation());
        assertEquals("America/Los_Angeles", locationID.getZoneId().getId());
    }

    @Test
    void testBuilderBaseAndNullSub(){

        LocationID.Builder builder = new LocationID.Builder();
        builder.withBaseLocation("BASE");
        builder.withSubLocation(null);

        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());
        assertEquals("", locationID.getSubLocation());
        assertEquals("BASE", locationID.getLocation());

    }

    @Test
    void testBuilderJustBase(){

        LocationID.Builder builder = new LocationID.Builder();
        builder.withBaseLocation("BASE");

        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());

        assertNull(locationID.getZoneId());

    }

    @Test
    void testBuilderMultiBase(){

        LocationID.Builder builder = new LocationID.Builder();
        builder.withBaseLocation("first");
        builder.withBaseLocation("second");
        builder.withBaseLocation("third");
        builder.withBaseLocation("BASE");

        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());

        assertNull(locationID.getZoneId());
    }


    @Test
    void testBuilderDashedLocation(){
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        LocationID.Builder builder = new LocationID.Builder();
        builder.withLocation("BASE-SUB");
        builder.withZoneId(zoneId);
        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());
        assertEquals("SUB", locationID.getSubLocation());
        assertEquals("BASE-SUB", locationID.getLocation());
        assertEquals("America/Los_Angeles", locationID.getZoneId().getId());
    }

    @Test
    void testBuilderDashedLocationNoZoneId(){
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        LocationID.Builder builder = new LocationID.Builder();
        builder.withLocation("BASE-SUB");

        LocationID locationID = builder.build();
        assertNotNull(locationID);
        assertEquals("BASE", locationID.getBaseLocation());
        assertEquals("SUB", locationID.getSubLocation());
        assertEquals("BASE-SUB", locationID.getLocation());
        assertNull(locationID.getZoneId());
    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException {
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        LocationID.Builder builder = new LocationID.Builder();
        builder.withLocation("BASE-SUB");
        LocationID locationID = builder.build();


        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedLocationId = om.writeValueAsString(locationID);
        assertNotNull(serializedLocationId);
//        System.out.println(serializedLocationId);

        LocationID locationID2 = om.readValue(serializedLocationId, LocationID.class);
        assertNotNull(locationID2);

        assertEquals(locationID.getLocation(), locationID2.getLocation());

    }

}