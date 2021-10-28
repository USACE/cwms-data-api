package cwms.radar.api;

import cwms.radar.api.enums.Nation;
import cwms.radar.data.dto.Location;
import cwms.radar.formatters.Formats;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LocationControllerTest extends ControllerTest
{
    private static String OFFICE_ID = "LRL";

    @Test
    public void testDeserializeLocationXml() throws IOException
    {
        String xml = loadResourceAsString("cwms/radar/api/location_create.xml");
        xml.replaceAll("\\s+","");
        assertNotNull(xml);
        Location location = LocationController.deserializeLocation(xml, Formats.XML, OFFICE_ID);
        assertNotNull(location);
        assertEquals("LOC_TEST", location.getName());
        assertEquals("LRL", location.getOfficeId());
        assertEquals("NGVD-29", location.getHorizontalDatum());
        assertEquals("UTC", location.getTimezoneName());
        assertEquals(Nation.US, location.getNation());
    }

    @Test
    public void testDeserializeLocationJSON() throws IOException
    {
        String json = loadResourceAsString("cwms/radar/api/location_create.json");
        assertNotNull(json);
        Location location = LocationController.deserializeLocation(json, Formats.JSON, OFFICE_ID);
        assertNotNull(location);
        assertEquals("LOC_TEST", location.getName());
        assertEquals("LRL", location.getOfficeId());
        assertEquals("NGVD-29", location.getHorizontalDatum());
        assertEquals("UTC", location.getTimezoneName());
        assertEquals(Nation.US, location.getNation());
    }
}
