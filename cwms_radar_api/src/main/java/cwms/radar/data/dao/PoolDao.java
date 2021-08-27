package cwms.radar.data.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.Pool;
import cwms.radar.data.dto.Pools;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.TooManyRowsException;
import org.jooq.impl.DSL;

import usace.cwms.db.dao.ifc.pool.PoolNameType;
import usace.cwms.db.dao.ifc.pool.PoolType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_POOL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_pool.RETRIEVE_POOL;
import usace.cwms.db.jooq.codegen.tables.AV_POOL;

import static java.util.stream.Collectors.toList;

public class PoolDao extends JooqDao<PoolType>
{
	private static Logger logger = Logger.getLogger(PoolDao.class.getName());

	public PoolDao(DSLContext dsl)
	{
		super(dsl);
	}


	public List<Pool> catalogPools(String projectIdMask, String poolNameMask,
								   String bottomLevelMask, String topLevelMask, boolean includeExplicit,
								   boolean includeImplicit, String officeIdMask)
	{
		AV_POOL view = AV_POOL.AV_POOL;

		List<String> types = getTypes(includeExplicit, includeImplicit);
		Condition condition = getCondition(projectIdMask, poolNameMask, bottomLevelMask, topLevelMask, officeIdMask, types);

		return dsl.select(DSL.asterisk()).from(view)
				.where(condition)
				.orderBy(view.DEFINITION_TYPE,
					view.OFFICE_ID.upper(), view.PROJECT_ID.upper(), view.ATTRIBUTE, view.POOL_NAME.upper())
				.stream()
				.map(r -> toPool(r, true))
				.collect(toList());
	}

	@NotNull
	private List<String> getTypes(boolean includeExplicit, boolean includeImplicit)
	{
		List<String> types = new ArrayList<>();
		if (includeExplicit)
		{
			types.add("EXPLICIT");
		}

		if(includeImplicit)
		{
			types.add("IMPLICIT");
		}
		return types;
	}



	private Condition getCondition(String projectIdMask, String poolNameMask, String bottomLevelMask,
								   String topLevelMask, String officeIdMask, List<String> types)
	{
		AV_POOL view = AV_POOL.AV_POOL;
		Condition condition = view.DEFINITION_TYPE.in(types);

		if(projectIdMask != null){
			condition = condition.and(view.PROJECT_ID.likeRegex(projectIdMask));
		}

		if(poolNameMask != null){
			condition = condition.and(view.POOL_NAME.likeRegex(poolNameMask));
		}

		if(bottomLevelMask != null){
			condition = condition.and(view.BOTTOM_LEVEL.likeRegex(bottomLevelMask));
		}

		if(topLevelMask != null){
			condition = condition.and(view.TOP_LEVEL.likeRegex(topLevelMask));
		}

		if(officeIdMask != null){
			condition = condition.and(view.OFFICE_ID.likeRegex(officeIdMask));
		}
		return condition;
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

		return records.stream().map(r->toPool(r, includeImplicit))
				.collect(Collectors.toList());
	}


	@NotNull
	private static Pool toPool(Record catRecord, boolean isImplicit)
	{
		String poolId = catRecord.get("POOL_NAME", String.class);
		String projectId = catRecord.get("PROJECT_ID", String.class);
		String officeId = catRecord.get("OFFICE_ID", String.class);
		String bottomLevelId = catRecord.get("BOTTOM_LEVEL", String.class);
		String topLevelId = catRecord.get("TOP_LEVEL", String.class);

		// These fields weren't in PoolType, they appear to be
		// hard-wired to null in the pl/sql for implicit pools, not sure about explicit.
		Number attribute = catRecord.get("ATTRIBUTE", Number.class);
		String description = catRecord.get("DESCRIPTION", String.class);

		String clobText = catRecord.get("CLOB_TEXT", String.class);

		PoolNameType name = new PoolNameType(poolId, officeId);
		Pool.Builder b = Pool.Builder.newInstance();
		b.withPoolName(name);
		b.withProjectId(projectId);
		b.withBottomLevelId(bottomLevelId);
		b.withTopLevelId(topLevelId);
		b.withImplicit(isImplicit);
		b.withAttribute(attribute);
		b.withDescription( description);
		b.withClobText(clobText);
		return b.build();
	}


	public Pool retrievePool( String projectId, String poolName, String officeId)
	{
		RETRIEVE_POOL pool = CWMS_POOL_PACKAGE.call_RETRIEVE_POOL(dsl.configuration(), projectId, poolName,	officeId);

		Pool.Builder b = Pool.Builder.newInstance();
		b.withPoolName(new PoolNameType(poolName, officeId));
		b.withProjectId(projectId);
		b.withBottomLevelId(pool.getP_BOTTOM_LEVEL_ID());
		b.withTopLevelId(pool.getP_TOP_LEVEL_ID());
		b.withImplicit(false);

		return b.build();
	}

	public Pool retrievePoolFromCatalog(String projectId, String poolName,
										String bottomMask, String topMask, boolean includeExplicit,
										boolean includeImplicit, String officeIdMask)
	{
		Pool pool = null;

		List<Pool> pools = catalogPools(projectId, poolName, bottomMask, topMask, includeExplicit, includeImplicit, officeIdMask);
		if(pools != null && !pools.isEmpty())
		{
			if(pools.size() == 1)
			{
				pool = pools.get(0);
			} else {
				throw new TooManyRowsException(String.format(
						"PoolController.getOne is expected to be called with arguments that return a single unique pool. " +
								"Arguments:[projectId:%s poolId:%s, bottomMask:%s, topMask:%s, implicit:%b, " +
								"explicit:%b, office:%s] returned the following %d pools:%s",
						projectId, poolName, bottomMask, topMask, includeImplicit, includeExplicit, officeIdMask, pools.size(), pools));
			}
		}
		return pool;
	}

	public Pools retrievePools(String cursor, int pageSize,
							   String projectIdMask, String poolNameMask,
							   String bottomLevelMask, String topLevelMask, boolean includeExplicit,
							   boolean includeImplicit, String officeIdMask){
		Integer total = null;
		int offset = 0;

		AV_POOL view = AV_POOL.AV_POOL;

		if(cursor != null && !cursor.isEmpty())
		{
			String[] parts = Pools.decodeCursor(cursor);

			logger.info("decoded cursor: " + Arrays.toString(parts));

			if(parts.length > 2) {
				offset = Integer.parseInt(parts[0]);
				if(!"null".equals(parts[1])){
					try {
						total = Integer.valueOf(parts[1]);
					} catch(NumberFormatException e){
						logger.log(Level.INFO, "Could not parse " + parts[1]);
					}
				}
				pageSize = Integer.parseInt(parts[2]); // Why are we taking pageSize as an arg and also pulling it from cursor?
			}
		}

		List<String> types = getTypes(includeExplicit, includeImplicit);
		Condition condition = getCondition(projectIdMask, poolNameMask, bottomLevelMask, topLevelMask, officeIdMask, types);

		List<Pool> pools = dsl.select(DSL.asterisk()).from(view)
				.where(condition)
				.orderBy(view.DEFINITION_TYPE,
						view.OFFICE_ID.upper(), view.PROJECT_ID.upper(), view.ATTRIBUTE, view.POOL_NAME.upper())
				.offset(offset)
				.limit(pageSize)
				.stream().map(r -> toPool(r, true)).collect(toList());

		Pools.Builder builder = new Pools.Builder(offset, pageSize, total);
		builder.addAll(pools);
		return builder.build();
	}

}
