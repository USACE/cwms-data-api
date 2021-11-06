package cwms.radar.data.dao;

import java.sql.Connection;
import java.sql.SQLException;

import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.json.PgJsonFormatter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import usace.cwms.db.jooq.dao.CwmsDbBasinJooq;
import usace.cwms.db.jooq.dao.CwmsDbLocJooq;
import usace.cwms.db.jooq.dao.CwmsDbStreamJooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class BasinDaoTest extends DaoTest
{
	private static final String OFFICE_ID = "LRL";

	@Test
	public void testGetBasin() throws Exception
	{
		try
		{
			String basinName = "TEST_BASIN";
			BasinDao basinDao = new BasinDao(getDslContext(getConnection(), OFFICE_ID));
			basinDao.getAllBasins("EN", OFFICE_ID);
			Basin basin = basinDao.getBasin(basinName, UnitSystem.EN.getValue(), OFFICE_ID);
			assertNotNull(basin);
			assertEquals(basinName, basin.getBasinName());
			assertEquals("Dummy_R", basin.getPrimaryStream().getStreamName());
			PgJsonFormatter pgJsonFormatter = new PgJsonFormatter();
			String pgJson = pgJsonFormatter.format(basin);
			assertNotNull(pgJson);
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		}
	}

	@BeforeAll
	private static void setUpTestBasin() throws Exception
	{
		CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
		CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();
		CwmsDbLocJooq locationJooq = new CwmsDbLocJooq();
		Connection c = getConnection();
		String basinId = "TEST_BASIN7";
		String primaryStreamID = "TEST_PRIM_STREAM7";
		String stationUnits = "km";
		String areaUnits = "km2";
		locationJooq.store(getConnection(), OFFICE_ID, "TEST_PRIM_STREAM7", "CA", null, "UTC", "STREAM", 1.0, 1.0, 0.0, "ft", "NVGD29", "NVGD29", "TEST_PRIMARY_STREAM", "TEST_PRIMARY_STREAM", "", true, true);
		locationJooq.store(getConnection(), OFFICE_ID, "TEST_BASIN7", "CA", null, "UTC", "BASIN", 1.0, 1.0, 0.0, "ft", "NVGD29", "NVGD29", "TEST_PRIMARY_STREAM", "TEST_PRIMARY_STREAM", "", true, true);
		streamJooq.storeStream(c, primaryStreamID, true, true, stationUnits, false, null, null, null, null, null, null, 100.0, 1.0, "", OFFICE_ID);
		basinJooq.storeBasin(c, basinId, true, true, null, 0.0, "TEST_PRIM_STREAM7", 10.0, 8.0, areaUnits, OFFICE_ID);
	}

	@AfterAll
	private static void clearTestBasin() throws Exception
	{
		CwmsDbStreamJooq streamJooq = new CwmsDbStreamJooq();
		CwmsDbBasinJooq basinJooq = new CwmsDbBasinJooq();
		basinJooq.deleteBasin(getConnection(), "TEST_BASIN7", "DELETE ALL", OFFICE_ID);
		streamJooq.deleteStream(getConnection(), "TEST_PRIM_STREAM7", "DELETE ALL", OFFICE_ID);
	}
}
