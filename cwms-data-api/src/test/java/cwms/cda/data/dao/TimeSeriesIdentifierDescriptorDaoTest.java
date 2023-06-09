/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;

import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

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
        }
    }


}