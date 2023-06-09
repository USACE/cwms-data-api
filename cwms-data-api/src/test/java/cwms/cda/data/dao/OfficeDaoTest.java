package cwms.cda.data.dao;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.Office;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
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
