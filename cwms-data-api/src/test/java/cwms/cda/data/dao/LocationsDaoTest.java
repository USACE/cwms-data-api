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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.json.JsonV2;

import org.geojson.FeatureCollection;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
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
        locationsDao.deleteLocation(location.getName(), location.getOfficeId());
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

	@Test
	void getLocationsGeoJson() throws SQLException, JsonProcessingException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationsDao dao = new LocationsDaoImpl(lrl);
			final String names = null;

			final String units = "EN";
			final String datum = null;
			final String office = "LRL";
			FeatureCollection fc = dao.buildFeatureCollection(names, units, office);
			assertNotNull(fc);
			ObjectMapper mapper = JsonV2.buildObjectMapper();
			String json =  mapper.writeValueAsString(fc);
			assertNotNull(json);
		}

	}

	@Test
	void getLocationsGeoJsonWithNames() throws SQLException, JsonProcessingException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationsDao dao = new LocationsDaoImpl(lrl);
			final String names = "Highbridge|Brookville";
			final String format = "geojson";
			final String units = "EN";
			final String datum = null;
			final String office = "LRL";

			FeatureCollection fc = dao.buildFeatureCollection(names, units, office);
			assertNotNull(fc);
			ObjectMapper mapper = JsonV2.buildObjectMapper();
			String json =  mapper.writeValueAsString(fc);
			assertNotNull(json);
		}
	}

}
