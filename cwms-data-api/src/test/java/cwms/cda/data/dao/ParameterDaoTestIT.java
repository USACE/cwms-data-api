package cwms.cda.data.dao;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.Parameters;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class ParameterDaoTestIT extends DataApiTestIT
{

	@Test
	void test_getParametersV2() throws Exception
	{
		CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
		db.connection(c -> {
			DSLContext ctx = dslContext(c);
			ParameterDao dao = new ParameterDao(ctx);
			Parameters parameters = dao.getParametersV2("SPK");
			assertNotNull(parameters);
		});
	}
}