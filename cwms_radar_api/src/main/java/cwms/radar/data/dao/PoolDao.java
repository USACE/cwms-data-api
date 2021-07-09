package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

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

	public List<PoolType> catalogPools(String projectIdMask, String poolNameMask,
									   String bottomLevelMask, String topLevelMask, boolean includeExplicit,
									   boolean includeImplicit, String officeIdMask) throws SQLException
	{
		List<PoolType> output = new ArrayList<>();

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

	private List<PoolType> catalogPoolsInternal( boolean includeExplicit, boolean includeImplicit,
												String projectIdMask, String poolNameMask, String bottomLevelMask,
												String topLevelMask, String officeIdMask) throws SQLException
	{
		String includeExplicitStr = OracleTypeMap.formatBool(includeExplicit);
		String includeImplicitStr = OracleTypeMap.formatBool(includeImplicit);
		Result<Record> records = CWMS_POOL_PACKAGE.call_CAT_POOLS(dsl.configuration(), projectIdMask,
				poolNameMask, bottomLevelMask, topLevelMask, includeExplicitStr, includeImplicitStr,
				officeIdMask);
		return PoolTypeUtil.toPoolType(records, includeImplicit);
	}

	public PoolType retrievePool( String projectId, String poolName, String officeId)
			throws SQLException
	{
		RETRIEVE_POOL pool = CWMS_POOL_PACKAGE.call_RETRIEVE_POOL(dsl.configuration(), projectId, poolName,	officeId);
		return PoolTypeUtil.toPoolType(pool, projectId, poolName, officeId, false);
	}

}
