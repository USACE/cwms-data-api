package cwms.radar.data.dao;

import java.sql.SQLException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.json.JsonV2;
import org.geojson.FeatureCollection;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;

// These tests look for specific data from an LRL pluggable database.
@Disabled
public class LocationsDaoTest
{

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