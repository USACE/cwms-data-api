package cwms.radar.data.dao;

import cwms.radar.api.enums.Nation;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.Location;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class LocationsDaoTest extends DaoTest
{
    private static final String OFFICE_ID = "LRL";

    @Test
    void testStoreLocation() throws Exception
    {
        try
        {
            Location location = buildTestLocation();
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
            locationsDao.storeLocation(location);
            Location retrievedLocation = locationsDao.getLocation(location.getName(), UnitSystem.EN.getValue(), OFFICE_ID);
            assertEquals(location, retrievedLocation);
        }
        finally
        {
            cleanUpRoutine();
        }
    }

    @Test
    void testDeleteLocation() throws Exception
    {
        Location location = buildTestLocation();
        LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        locationsDao.storeLocation(location);
        locationsDao.deleteLocation(location.getName(), location.getOfficeId());
        assertThrows(IOException.class, () -> locationsDao.getLocation(location.getName(), UnitSystem.EN.getValue(), OFFICE_ID));
    }

    @Test
    void testRenameLocation() throws Exception
    {
        Location location = buildTestLocation();
        Location renamedLocation = new Location.Builder(location).withName("RENAMED_TEST_LOCATION2").build();
        LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        try
        {
            locationsDao.storeLocation(location);
            locationsDao.renameLocation(location.getName(), renamedLocation);
            assertThrows(IOException.class, () -> locationsDao.getLocation(location.getName(), UnitSystem.EN.getValue(), OFFICE_ID));
            assertNotNull(locationsDao.getLocation(renamedLocation.getName(), UnitSystem.EN.getValue(), OFFICE_ID));
        }
        finally
        {
            locationsDao.deleteLocation(renamedLocation.getName(), location.getOfficeId());
            cleanUpRoutine();
        }
    }

    private void cleanUpRoutine() throws Exception
    {
        Location location = buildTestLocation();
        LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        try
        {
            locationsDao.deleteLocation(location.getName(), location.getOfficeId());
        }
        catch(IOException ex)
        {
            System.out.println("Location already successfully deleted. Clean-up complete");
        }
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "SITE", ZoneId.of("UTC"), 50.0, 50.0, "NVGD29", "LRL")
                .withElevation(10.0)
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withBoundingOfficeId("LRL")
                .withLongName("TEST_LOCATION")
                .withPublishedLatitude(50.0)
                .withPublishedLongitude(50.0)
                .withDescription("for testing")
                .build();
    }
}
