package cwms.radar.data.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import cwms.radar.data.dto.Pool;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.TooManyRowsException;

import usace.cwms.db.dao.ifc.pool.PoolType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_POOL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_pool.RETRIEVE_POOL;
import usace.cwms.db.jooq.dao.util.PoolTypeUtil;

public class PoolDao extends JooqDao<PoolType>
{

	public PoolDao(DSLContext dsl)
	{
		super(dsl);
	}

	public List<Pool> catalogPools(String projectIdMask, String poolNameMask,
									   String bottomLevelMask, String topLevelMask, boolean includeExplicit,
									   boolean includeImplicit, String officeIdMask)
	{
		List<Pool> output = new ArrayList<>();

		if (includeExplicit)
		{
			output.addAll(catalogPoolsInternal( true, false, projectIdMask, poolNameMask, bottomLevelMask, topLevelMask, officeIdMask));
		}

		//CWMS_POOL does not differentiate between implicit or explicit.
		if (includeImplicit)
		{
			output.addAll(catalogPoolsInternal( false, true, projectIdMask, poolNameMask, bottomLevelMask, topLevelMask, officeIdMask));
		}

		return output;
	}

	private List<Pool> catalogPoolsInternal( boolean includeExplicit, boolean includeImplicit,
												String projectIdMask, String poolNameMask, String bottomLevelMask,
												String topLevelMask, String officeIdMask)
	{
		String includeExplicitStr = OracleTypeMap.formatBool(includeExplicit);
		String includeImplicitStr = OracleTypeMap.formatBool(includeImplicit);
		Result<Record> records = CWMS_POOL_PACKAGE.call_CAT_POOLS(dsl.configuration(), projectIdMask,
				poolNameMask, bottomLevelMask, topLevelMask, includeExplicitStr, includeImplicitStr,
				officeIdMask);
		return PoolTypeUtil.toPoolType(records, includeImplicit)
				.stream().map(Pool::new).collect(Collectors.toList());
	}

	public Pool retrievePool( String projectId, String poolName, String officeId)
	{
		Pool retval = null;
		RETRIEVE_POOL pool = CWMS_POOL_PACKAGE.call_RETRIEVE_POOL(dsl.configuration(), projectId, poolName,	officeId);

		PoolType poolType = PoolTypeUtil.toPoolType(pool, projectId, poolName, officeId, false);
		if(poolType != null){
			retval = new Pool(poolType);
		}
		return retval;
	}

	public Pool retrievePoolFromCatalog(String projectId, String poolName,
										String bottomMask, String topMask, boolean includeExplicit,
										boolean includeImplicit, String officeIdMask)
	{
		Pool pool = null;

		List<Pool> pools = catalogPools(projectId, poolName, bottomMask, topMask, includeExplicit, includeImplicit, officeIdMask);
		if(pools != null)
		{
			if(pools.size() == 1)
			{
				pool = pools.get(0);
			} else {
				throw new TooManyRowsException(String.format(
						"PoolController.getOne is expected to be called with arguments that return a single unique pool. " +
								"Arguments:[projectId:%s poolId:%s, bottomMask:%s, topMask:%s, implicit:%b, " +
								"explicit:%b, office:%s] returned the following %d pools:%s",
						projectId, poolName, bottomMask, topMask, includeImplicit, includeExplicit, officeIdMask, pools.size(), pools.toString()));
			}
		}
		return pool;
	}
}
