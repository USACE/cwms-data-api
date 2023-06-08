package cwms.radar.data.dao;

import java.sql.SQLException;

import cwms.radar.data.dto.Office;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class OfficeDaoTest
{

	@Test
	void getOfficeById() throws SQLException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			OfficeDao dao = new OfficeDao(lrl);
			Office office = dao.getOfficeById("LRL").get();
			assertNotNull(office);
		}

	}

}
