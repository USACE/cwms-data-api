package cwms.cda.data.dao;

import java.sql.SQLException;
import java.util.List;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.TimeSeriesGroup;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class TimeSeriesGroupDaoTest
{

	@Test
	void getTimeSeriesGroups() throws SQLException {
		DSLContext lrl = getDslContext(getConnection(), "LRL");
		TimeSeriesGroupDao dao = new TimeSeriesGroupDao(lrl);
		List<TimeSeriesGroup> groups = dao.getTimeSeriesGroups();
		assertNotNull(groups);
		assertFalse(groups.isEmpty());


	}
}
