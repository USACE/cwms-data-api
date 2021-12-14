package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.LocationCategory;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class LocationCategoryDaoTest
{

	@Test
	void getLocationCategories() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationCategoryDao dao = new LocationCategoryDao(lrl);
			List<LocationCategory> cats = dao.getLocationCategories();
			assertNotNull(cats);
			assertFalse(cats.isEmpty());
		}

	}

	@Test
	void getLocationCategory() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationCategoryDao dao = new LocationCategoryDao(lrl);
			Optional<LocationCategory> optCat = dao.getLocationCategory("LRL",
					"CWMS Mobile Location Listings");
			assertTrue(optCat.isPresent());
			LocationCategory cat = optCat.get();
			assertNotNull(cat);

		}

	}

	@Test
	void getLocationCategory2() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationCategoryDao dao = new LocationCategoryDao(lrl);
			Optional<LocationCategory> optCat = dao.getLocationCategory("CWMS", "Agency Aliases");
			assertTrue(optCat.isPresent());
			LocationCategory cat = optCat.get();
			assertNotNull(cat);

		}

	}

}
