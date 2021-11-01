package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.List;

import cwms.radar.data.dto.TimeSeriesGroup;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class TimeSeriesGroupDaoTest
{

	@Test
	void getTimeSeriesGroups() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			TimeSeriesGroupDao dao = new TimeSeriesGroupDao(lrl);
			List<TimeSeriesGroup> groups = dao.getTimeSeriesGroups();
			assertNotNull(groups);
			assertFalse(groups.isEmpty());
		}

	}
}
