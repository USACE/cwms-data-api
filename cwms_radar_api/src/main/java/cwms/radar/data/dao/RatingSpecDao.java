package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.rating.RatingSpec;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;

import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

public class RatingSpecDao extends JooqDao<RatingSpec>
{
	private static final Logger logger = Logger.getLogger(RatingSpecDao.class.getName());
	public RatingSpecDao(DSLContext dsl)
	{
		super(dsl);
	}


	public Set<RatingSpec> retrieveRatingSpecs(String office, String specIdMask)
	{
		Set<RatingSpec> retval;

		Condition condition = AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull()
				.and(AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull());

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
				.on(
						AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(AV_RATING.AV_RATING.RATING_ID))
				.where(condition)
				.fetchSize(1000)
				;

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
						.effectiveDates(entry.getValue())
						.build())
				.collect(Collectors.toCollection(LinkedHashSet::new));

		return retval;
	}

	public Optional<RatingSpec> retrieveRatingSpec(String office, String specId)
	{
		Set<RatingSpec> retval;

		Condition condition = AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull()
				.and(AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull())
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
				.on(
						AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(AV_RATING.AV_RATING.RATING_ID))
				.where(condition)
				.fetchSize(1000)
				;

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
						.effectiveDates(entry.getValue())
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

			retval = new RatingSpec.Builder().officeId(officeId).ratingId(ratingId)
					.templateId(templateId).locationId(locId).version(version).sourceAgency(agency)
					.active(activeFlag).autoUpdate(autoUpdateFlag).autoActivate(autoActivateFlag)
					.autoMigrateExtension(autoMigrateExtFlag).indRoundingSpecs(indRndSpecs)
					.depRoundingSpec(depRndSpecs).description(desc)
					.dateMethods(dateMethods)
					.build();
		}

		return retval;
	}


}
