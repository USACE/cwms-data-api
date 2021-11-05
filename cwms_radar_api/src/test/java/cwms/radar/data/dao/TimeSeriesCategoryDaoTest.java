package cwms.radar.data.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import cwms.radar.data.dto.TimeSeriesCategory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
class TimeSeriesCategoryDaoTest
{

	@Test
	void test_getTimeSeriesCategories() throws SQLException
	{
		String office = "SWT";
		try(Connection connection = getConnection(); DSLContext dsl = getDslContext(connection, office))
		{
			TimeSeriesCategoryDao dao = new TimeSeriesCategoryDao(dsl);

			List<TimeSeriesCategory> cats = dao.getTimeSeriesCategories(office);
			assertNotNull(cats);
			assertFalse(cats.isEmpty());

			List<TimeSeriesCategory> cats2 = dao.getTimeSeriesCategories();
			assertNotNull(cats2);
			assertFalse(cats2.isEmpty());
		}
	}

	@Test
	void test_getTimeSeriesCategory() throws SQLException
	{
		String office = "SWT";
		try(Connection connection = getConnection(); DSLContext dsl = getDslContext(connection, office))
		{
			TimeSeriesCategoryDao dao = new TimeSeriesCategoryDao(dsl);

			TimeSeriesCategory cat = dao.getTimeSeriesCategory(office, "Lakes").get();
			assertNotNull(cat);
		}
	}
}