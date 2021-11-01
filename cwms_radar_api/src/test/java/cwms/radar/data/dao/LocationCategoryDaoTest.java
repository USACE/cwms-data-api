package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.List;

import cwms.radar.data.dto.LocationCategory;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class LocationCategoryDaoTest
{

	@Test
	void getLocationCat() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			LocationCategoryDao dao = new LocationCategoryDao(lrl);
			List<LocationCategory> cats = dao.getLocationCategories();
			assertNotNull(cats);
			assertFalse(cats.isEmpty());
		}

	}

}
