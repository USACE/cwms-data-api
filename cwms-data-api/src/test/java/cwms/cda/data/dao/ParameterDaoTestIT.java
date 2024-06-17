package cwms.cda.data.dao;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

class ParameterDaoTestIT extends DaoTest
{

	@Test
	void test_getParameters() throws Exception
	{
		try(Connection conn = getConnection())
		{
			DSLContext ctx = dslContext(conn);
			ParameterDao dao = new ParameterDao(ctx);
			String parameters = dao.getParameters();
			assertNotNull(parameters);
		}
	}
}