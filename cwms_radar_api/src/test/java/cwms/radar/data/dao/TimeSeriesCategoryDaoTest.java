package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.List;

import cwms.radar.data.dto.TimeSeriesCategory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class TimeSeriesCategoryDaoTest
{

	@Test
	void getTimeSeriesCategories() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			TimeSeriesCategoryDao dao = new TimeSeriesCategoryDao(lrl);
			List<TimeSeriesCategory> cats = dao.getTimeSeriesCategories();
			assertNotNull(cats);
			assertFalse(cats.isEmpty());

			// We don't have any at LRL in my test db
			//			List<TimeSeriesCategory> cats2 = cdm.getTimeSeriesCategories("LRL");
			//			assertNotNull(cats2);
			//			assertFalse(cats2.isEmpty());
		}

	}

}
