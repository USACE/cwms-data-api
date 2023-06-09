package cwms.cda.data.dao;

import java.sql.SQLException;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.data.dto.Pool;
import cwms.cda.data.dto.Pools;
import cwms.cda.formatters.json.JsonV2;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class PoolDaoTest
{

	@Test
	public void testCatalog() throws SQLException, JsonProcessingException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			PoolDao dao = new PoolDao(lrl);

			String ANY_MASK = "*";

			String idMask = ANY_MASK;
			String nameMask = ANY_MASK;
			String bottomMask = ANY_MASK;
			String topMask = ANY_MASK;

			boolean isExplicit;
			boolean isImplicit;

			isExplicit = false;
			isImplicit = true;
			List<Pool> impPools = dao.catalogPools(idMask, nameMask, bottomMask, topMask, isExplicit, isImplicit,
					"LRL");
			assertNotNull(impPools);
			assertFalse(impPools.isEmpty(), "Expected some implicit pools to be found");

			isExplicit = true;
			isImplicit = false;
			List<Pool> expPools = dao.catalogPools(idMask, nameMask, bottomMask, topMask, isExplicit, isImplicit,
					"LRL");
			assertNotNull(expPools);
			// Looks like I don't have any explicit pools in db to test against.
			//			assertFalse(expPools.isEmpty());

			ObjectMapper mapper = JsonV2.buildObjectMapper();
			String json = mapper.writeValueAsString(impPools);
			assertNotNull(json);
		}
	}


	@Test
	public void testRetrievePools() throws SQLException, JsonProcessingException
	{
		try(DSLContext lrl = getDslContext(getConnection(), "LRL"))
		{
			PoolDao dao = new PoolDao(lrl);

			String ANY_MASK = "*";

			String idMask = ANY_MASK;
			String nameMask = ANY_MASK;
			String bottomMask = ANY_MASK;
			String topMask = ANY_MASK;


			boolean isExplicit = false;
			boolean isImplicit = true;
			Pools pools = dao.retrievePools(null, 5, idMask, nameMask, bottomMask, topMask, isExplicit, isImplicit,
					"LRL");

			assertNotNull(pools);

			String page = pools.getPage();
			String nextPage = pools.getNextPage();

			Pools pools2 = dao.retrievePools(nextPage, 5, idMask, nameMask, bottomMask, topMask, isExplicit, isImplicit,
					"LRL");
			Pools pools3 = dao.retrievePools(pools2.getNextPage(), 5, idMask, nameMask, bottomMask, topMask, isExplicit,
					isImplicit, "LRL");

			assertNotNull(page);

		}
	}

}
