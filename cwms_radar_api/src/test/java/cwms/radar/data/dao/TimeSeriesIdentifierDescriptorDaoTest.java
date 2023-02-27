package cwms.radar.data.dao;

import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.flogger.FluentLogger;
import cwms.radar.api.enums.Nation;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.TimeSeriesIdentifierDescriptor;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled   // Disabled b/c this was testing against a real database.
class TimeSeriesIdentifierDescriptorDaoTest {
    FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE = "SWT";
    public static final String locationId = "TEST_LOCATION2";

    @BeforeEach
    void setUp() throws SQLException, IOException {

        Location location = buildTestLocation();

        try {
            deleteLocation(location);
        } catch (Exception e) {
            logger.atFine().log("Exception deleting test Location.");
        }
        createLocation(location);
    }

    @AfterEach
    void tearDown()  {
        Location location = buildTestLocation();
        try {
            deleteLocation(location);
        } catch (Exception e) {
            logger.atFine().log("Exception deleting test Location.");
        }
    }

    @Test
    void testCreate() throws SQLException, IOException {
        Location location = buildTestLocation();

        int random = (int) (Math.random() * 1000);

        try (DSLContext dsl = getDslContext(OFFICE)) {
            assertNotNull(dsl);
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            int i = 0;
            String tsId = String.format(location.getName()
                    + ".Precip-Cumulative.Inst.15Minutes.0.TEST%d_%d", random, i);

            // Make sure it doesn't exist
            Optional<TimeSeriesIdentifierDescriptor> found =
                    dao.getTimeSeriesIdentifier(OFFICE, tsId);
            assertFalse(found.isPresent());

            try {
                TimeSeriesIdentifierDescriptor ts = new TimeSeriesIdentifierDescriptor.Builder()
                        .withOfficeId(OFFICE)
                        .withTimeSeriesId(tsId)
                        .build();

                boolean vers = false;

                // Create it
                dao.create(ts, vers, null, null, false);

                // Make sure it exists
                found = dao.getTimeSeriesIdentifier(OFFICE, tsId);
                assertTrue(found.isPresent());
            } finally {
                dao.deleteAll(OFFICE, tsId);
            }
        }

    }


    private Location buildTestLocation() {

        return new Location.Builder(locationId, "SITE", ZoneId.of("UTC"),
                50.0, 50.0, "NVGD29", OFFICE)
                .withElevation(10.0)
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withBoundingOfficeId(OFFICE)
                .withLongName("TEST_LOCATION")
                .withPublishedLatitude(50.0)
                .withPublishedLongitude(50.0)
                .withDescription("for testing")
                .build();
    }

    public void createLocation(Location location) throws SQLException, IOException {

        try (DSLContext dsl = getDslContext(OFFICE)) {

            LocationsDaoImpl locationsDao = new LocationsDaoImpl(dsl);
            locationsDao.storeLocation(location);
        }
    }

    private void deleteLocation(Location location) throws Exception {
        try (DSLContext dsl = getDslContext(OFFICE)) {
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(dsl);
            locationsDao.deleteLocation(location.getName(), location.getOfficeId());
        } catch (IOException ex) {
            System.out.println("Location already successfully deleted. Clean-up complete");
        }
    }


}