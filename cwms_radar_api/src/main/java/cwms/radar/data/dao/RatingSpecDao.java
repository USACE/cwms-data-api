package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.RatingSpecs;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectLimitStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.util.oracle.OracleDSL;

import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

import static cwms.radar.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;

public class RatingSpecDao extends JooqDao<RatingSpec>
{
	private static final Logger logger = Logger.getLogger(RatingSpecDao.class.getName());
	public RatingSpecDao(DSLContext dsl)
	{
		super(dsl);
	}


	public Collection<RatingSpec> retrieveRatingSpecs(String office, String specIdMask)
	{
		Set<RatingSpec> retval;

		Condition condition = AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull()
			.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_GROUP.isNull())
			.and(AV_RATING_SPEC.AV_RATING_SPEC.ALIASED_ITEM.isNull())
			.and(AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull())
			.and(AV_RATING.AV_RATING.LOC_ALIAS_GROUP.isNull())
			.and(AV_RATING.AV_RATING.ALIASED_ITEM.isNull());

		if( office != null ) {
			condition = condition.and(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID.eq(office));
		}

		if( specIdMask != null ){
			condition = condition.and(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.likeRegex(specIdMask));
		}

		ResultQuery< ? extends Record> query = dsl.select(
				AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.LOCATION_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.VERSION,
				AV_RATING_SPEC.AV_RATING_SPEC.SOURCE_AGENCY,
				AV_RATING_SPEC.AV_RATING_SPEC.ACTIVE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_UPDATE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_ACTIVATE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_MIGRATE_EXT_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.IND_ROUNDING_SPECS,
				AV_RATING_SPEC.AV_RATING_SPEC.DEP_ROUNDING_SPEC,
				AV_RATING_SPEC.AV_RATING_SPEC.DATE_METHODS,
				AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION,
				AV_RATING.AV_RATING.EFFECTIVE_DATE
			)
			.from(AV_RATING_SPEC.AV_RATING_SPEC)
			.leftOuterJoin(AV_RATING.AV_RATING)
			.on(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(AV_RATING.AV_RATING.RATING_ID))
			.where(condition)
			.fetchSize(1000)
		;

		//	logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
		query.fetchStream().forEach(rec -> {
			RatingSpec template = buildRatingSpec(rec);

			Timestamp effectiveDate = rec.get(AV_RATING.AV_RATING.EFFECTIVE_DATE);
			ZonedDateTime effective = toZdt(effectiveDate);

			List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
			if(effective != null)
			{
				list.add(effective);
			}
		});

		retval = map.entrySet().stream()
			.map( entry -> new RatingSpec.Builder()
				.fromRatingSpec(entry.getKey())
				.withEffectiveDates(entry.getValue())
				.build())
			.collect(Collectors.toCollection(LinkedHashSet::new));

		return retval;
	}


	public RatingSpecs retrieveRatingSpecs(String cursor, int pageSize,
			String office, String specIdMask)
	{
		Integer total = null;
		int offset = 0;

		if(cursor != null && !cursor.isEmpty())
		{
			String[] parts = CwmsDTOPaginated.decodeCursor(cursor);

			if(parts.length > 2) {
				offset = Integer.parseInt(parts[0]);
				if(!"null".equals(parts[1])){
					try {
						total = Integer.valueOf(parts[1]);
					} catch(NumberFormatException e){
						logger.log(Level.INFO, "Could not parse " + parts[1]);
					}
				}
				pageSize = Integer.parseInt(parts[2]);
			}
		}

		Set<RatingSpec> retval = getRatingSpecs(office, specIdMask, offset, offset + pageSize);

		RatingSpecs.Builder builder = new RatingSpecs.Builder(offset, pageSize, total);
		builder.specs(new ArrayList<>(retval));
		return builder.build();
	}

	@NotNull
	private Set<RatingSpec> getRatingSpecs(String office, String specIdMask, int firstRow, int lastRow)
	{
		Set<RatingSpec> retval;

		Condition condition = AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull()
			.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_GROUP.isNull())
			.and(AV_RATING_SPEC.AV_RATING_SPEC.ALIASED_ITEM.isNull())
			;

		if( office != null ) {
			condition = condition.and(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID.eq(office));
		}

		if( specIdMask != null ){
			condition = condition.and(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.likeRegex(specIdMask));
		}

		Condition ratingAliasNullCond = AV_RATING.AV_RATING.ALIASED_ITEM.isNull().and(
			AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull()).and(AV_RATING.AV_RATING.LOC_ALIAS_GROUP.isNull());

		SelectLimitStep<? extends  Record> innerSelect = dsl.select(
			OracleDSL.rownum().as("rnum"), AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID,
			AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID, AV_RATING_SPEC.AV_RATING_SPEC.DATE_METHODS,
			AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID, AV_RATING_SPEC.AV_RATING_SPEC.LOCATION_ID,
			AV_RATING_SPEC.AV_RATING_SPEC.VERSION, AV_RATING_SPEC.AV_RATING_SPEC.SOURCE_AGENCY,
			AV_RATING_SPEC.AV_RATING_SPEC.ACTIVE_FLAG, AV_RATING_SPEC.AV_RATING_SPEC.AUTO_UPDATE_FLAG,
			AV_RATING_SPEC.AV_RATING_SPEC.AUTO_ACTIVATE_FLAG, AV_RATING_SPEC.AV_RATING_SPEC.AUTO_MIGRATE_EXT_FLAG,
			AV_RATING_SPEC.AV_RATING_SPEC.IND_ROUNDING_SPECS, AV_RATING_SPEC.AV_RATING_SPEC.DEP_ROUNDING_SPEC,
			AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION, AV_RATING_SPEC.AV_RATING_SPEC.ALIASED_ITEM).from(
			AV_RATING_SPEC.AV_RATING_SPEC).where(condition)
			.orderBy(AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID);

		ResultQuery< ? extends Record> query = dsl.select(
				DSL.field(DSL.quotedName("rnum"), Integer.class),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.DATE_METHODS),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.LOCATION_ID),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.VERSION),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.SOURCE_AGENCY),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.ACTIVE_FLAG),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_UPDATE_FLAG),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_ACTIVATE_FLAG),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_MIGRATE_EXT_FLAG),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.IND_ROUNDING_SPECS),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.DEP_ROUNDING_SPEC),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION),
				innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.ALIASED_ITEM),
				AV_RATING.AV_RATING.EFFECTIVE_DATE)
			.from(innerSelect)
			.leftOuterJoin(AV_RATING.AV_RATING)
			.on(innerSelect.field(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID).eq(AV_RATING.AV_RATING.RATING_ID))
			.where(ratingAliasNullCond
				// This is the limit condition - the whole reason for the weird query....rnum starts at 1...
				.and(DSL.field(DSL.quotedName("rnum")).greaterThan(firstRow))
						.and(DSL.field(DSL.quotedName("rnum")).lessOrEqual(lastRow))
				)
			.orderBy(DSL.field(DSL.quotedName("rnum")),
				AV_RATING.AV_RATING.EFFECTIVE_DATE.asc());

		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
		query.fetchStream().forEach(rec -> {
			RatingSpec template = buildRatingSpec(rec);

			Timestamp effectiveDate = rec.get(AV_RATING.AV_RATING.EFFECTIVE_DATE);
			ZonedDateTime effective = toZdt(effectiveDate);

			List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
			if(effective != null)
			{
				list.add(effective);
			}
		});

		retval = map.entrySet().stream()
			.map( entry -> new RatingSpec.Builder()
				.fromRatingSpec(entry.getKey())
				.withEffectiveDates(entry.getValue())
				.build())
			.collect(Collectors.toCollection(LinkedHashSet::new));
		return retval;
	}


	public Optional<RatingSpec> retrieveRatingSpec(String office, String specId)
	{
		Set<RatingSpec> retval;

		Condition condition = AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull()
			.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_GROUP.isNull())
			.and(AV_RATING_SPEC.AV_RATING_SPEC.ALIASED_ITEM.isNull())
			.and(AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull())
			.and(AV_RATING.AV_RATING.LOC_ALIAS_GROUP.isNull())
			.and(AV_RATING.AV_RATING.ALIASED_ITEM.isNull())
			.and(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(specId))
			;

		if( office != null ) {
			condition = condition.and(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID.eq(office));
		}

		ResultQuery< ? extends Record> query = dsl.select(
				AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.LOCATION_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.VERSION,
				AV_RATING_SPEC.AV_RATING_SPEC.SOURCE_AGENCY,
				AV_RATING_SPEC.AV_RATING_SPEC.ACTIVE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_UPDATE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_ACTIVATE_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.AUTO_MIGRATE_EXT_FLAG,
				AV_RATING_SPEC.AV_RATING_SPEC.IND_ROUNDING_SPECS,
				AV_RATING_SPEC.AV_RATING_SPEC.DEP_ROUNDING_SPEC,
				AV_RATING_SPEC.AV_RATING_SPEC.DATE_METHODS,
				AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION,
				AV_RATING.AV_RATING.EFFECTIVE_DATE
			)
			.from(AV_RATING_SPEC.AV_RATING_SPEC)
			.leftOuterJoin(AV_RATING.AV_RATING)
			.on(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(AV_RATING.AV_RATING.RATING_ID))
			.where(condition)
			.fetchSize(1000);

		//		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
		query.fetchStream().forEach(rec -> {
			RatingSpec template = buildRatingSpec(rec);

			Timestamp effectiveDate = rec.get(AV_RATING.AV_RATING.EFFECTIVE_DATE);
			ZonedDateTime effective = toZdt(effectiveDate);

			List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
			if(effective != null)
			{
				list.add(effective);
			}
		});

		retval = map.entrySet().stream()
			.map( entry -> new RatingSpec.Builder()
				.fromRatingSpec(entry.getKey())
				.withEffectiveDates(entry.getValue())
				.build())
			.collect(Collectors.toCollection(LinkedHashSet::new));

		// There should only be one key in the map
		if(retval.size()!= 1){
			throw new IllegalStateException("More than one rating spec found for id: " + specId);
		}

		return retval.stream().findFirst();
	}

	private static ZonedDateTime toZdt(final Timestamp time ){
		if( time != null ) {
			return ZonedDateTime.ofInstant(time.toInstant(), ZoneId.of("UTC"));
		} else {
			return null;
		}
	}

	private RatingSpec buildRatingSpec(Record rec)
	{
		RatingSpec retval = null;

		if(rec != null)
		{
			String officeId = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID);
			String ratingId = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID);
			String templateId = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID);
			String locId = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.LOCATION_ID);
			String version = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.VERSION);
			String agency = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.SOURCE_AGENCY);
			String active = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.ACTIVE_FLAG);
			boolean activeFlag = active != null && active.equals("T");
			String autoUp = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_UPDATE_FLAG);
			boolean autoUpdateFlag = autoUp != null && autoUp.equals("T");
			String autoAct = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_ACTIVATE_FLAG);
			boolean autoActivateFlag = autoAct != null && autoAct.equals("T");
			String autoMig = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.AUTO_MIGRATE_EXT_FLAG);
			boolean autoMigrateExtFlag = autoMig != null && autoMig.equals("T");
			String indRndSpecs = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.IND_ROUNDING_SPECS);

			String depRndSpecs = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.DEP_ROUNDING_SPEC);
			String desc = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION);

			String dateMethods = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.DATE_METHODS);

			retval = new RatingSpec.Builder()
				.withOfficeId(officeId)
				.withRatingId(ratingId)
				.withTemplateId(templateId)
				.withLocationId(locId)
				.withVersion(version)
				.withSourceAgency(agency)
				.withActive(activeFlag)
				.withAutoUpdate(autoUpdateFlag)
				.withAutoActivate(autoActivateFlag)
				.withAutoMigrateExtension(autoMigrateExtFlag)
				.withIndependentRoundingSpecs(buildIndependentRoundingSpecs(indRndSpecs))
				.withDependentRoundingSpec(depRndSpecs)
				.withDescription(desc)
				.withDateMethods(dateMethods)
				.build();
		}

		return retval;
	}


}
