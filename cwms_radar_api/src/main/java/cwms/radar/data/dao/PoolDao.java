package cwms.radar.data.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import cwms.radar.data.dto.Pool;
import cwms.radar.data.dto.Pools;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.TooManyRowsException;

import usace.cwms.db.dao.ifc.pool.PoolNameType;
import usace.cwms.db.dao.ifc.pool.PoolType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_POOL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_pool.RETRIEVE_POOL;
import usace.cwms.db.jooq.dao.util.PoolTypeUtil;

import static java.util.Comparator.nullsLast;
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
		Pool pool = new Pool(name, projectId, bottomLevelId, topLevelId, isImplicit);
		pool.setAttribute(attribute);
		pool.setDescription(description);

		pool.setClobText(clobText);
		return pool;
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

	public Pools retrievePools(String cursor, int pageSize,
							   String projectIdMask, String poolNameMask,
							   String bottomLevelMask, String topLevelMask, boolean includeExplicit,
							   boolean includeImplicit, String officeIdMask){
		int total = 0;

		final Pool lastItem;

		List<Pool> pools = catalogPools(projectIdMask, poolNameMask, bottomLevelMask, topLevelMask, includeExplicit, includeImplicit, officeIdMask);

		if( cursor == null || cursor.isEmpty() ){
			total = pools.size();
			lastItem = null;
		} else {
			String[] parts = Pools.decodeCursor(cursor);

			logger.info("decoded cursor: " + Arrays.toString(parts));

			if(parts.length > 2) {
				String itemPortion = parts[0];
				lastItem = Pools.decodeItem(itemPortion);
				total = Integer.parseInt(parts[1]);
				pageSize = Integer.parseInt(parts[2]);
			} else {
				lastItem = null;
			}
		}

		// Apart from the initial isImplicit, the rest are the way the pl/sql already sorts.
		//
		Comparator<Pool> comparator = Comparator.comparing((Function<Pool, Boolean>) PoolType::isImplicit)
				.thenComparing(p -> p.getPoolName().getOfficeId(),  String.CASE_INSENSITIVE_ORDER)
				.thenComparing(Pool::getProjectId,  String.CASE_INSENSITIVE_ORDER)
				.thenComparing(p-> p.getAttribute()==null?null:p.getAttribute().doubleValue(),
						nullsLast(Comparator.naturalOrder()))
				.thenComparing((Pool p) -> p.getPoolName().getPoolName(), String.CASE_INSENSITIVE_ORDER);

		Stream<Pool> stream = pools.stream();
		if(lastItem != null)
		{
			// filter first so that there is hopefully less to sort.
			stream = stream.filter(p -> comparator.compare(lastItem, p) < 0);
		}

		List<Pool> collected = stream.sorted(comparator)
				.limit(pageSize)
				.collect(toList());

		Pools.Builder builder = new Pools.Builder(lastItem, pageSize, total);
		builder.addAll(collected);
		return builder.build();
	}

}
