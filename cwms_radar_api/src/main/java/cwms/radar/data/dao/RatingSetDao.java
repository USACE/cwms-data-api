package cwms.radar.data.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.rating.ParameterSpec;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.RatingTemplate;
import kotlin.Pair;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.ResultQuery;
import org.jooq.exception.DataAccessException;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import usace.cwms.db.dao.ifc.rating.CwmsDbRating;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_TEMPLATE;

public class RatingSetDao extends JooqDao<RatingSet> implements RatingDao
{
	private static final Logger logger = Logger.getLogger(RatingSetDao.class.getName());
	public RatingSetDao(DSLContext dsl)
	{
		super(dsl);
	}

	@Override
	public void create(RatingSet ratingSet) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c -> {
				// can't exist if we are creating, if it exists use store
				boolean overwriteExisting = false;
				ratingSet.storeToDatabase(c, overwriteExisting);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException)
			{
				throw (RatingException) cause;
			}
			throw new IOException("Failed to create Rating", ex);
		}
	}

	@Override
	public RatingSet retrieve(String officeId, String specificationId) throws IOException, RatingException
	{
		final RatingSet[] retval = new RatingSet[1];
		try
		{
			dsl.connection(c -> retval[0] = RatingSet.fromDatabase(RatingSet.DatabaseLoadMethod.EAGER, c, officeId,
					specificationId));
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException)
			{
				if(cause.getMessage().contains("contains no rating templates"))
				{
					return null;
				}

				throw (RatingException) cause;
			}
			throw new IOException("Failed to retrieve Rating", ex);
		}
		return retval[0];
	}

	// store/update
	@Override
	public void store(RatingSet ratingSet) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c -> {
				boolean overwriteExisting = true;
				ratingSet.storeToDatabase(c, overwriteExisting);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException)
			{
				throw (RatingException) cause;
			}
			throw new IOException("Failed to store Rating", ex);
		}
	}

	@Override
	public void delete(String officeId, String ratingSpecId) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c -> {
				//				deleteWithRatingSet(c, officeId, ratingSpecId);
				delete(c, officeId, ratingSpecId);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException)
			{
				throw (RatingException) cause;
			}
			throw new IOException("Failed to delete Rating", ex);
		}

	}

	public void delete(Connection c, String officeId, String ratingSpecId) throws SQLException, RatingException
	{
		delete(c, DeleteRule.DELETE_ALL, ratingSpecId, officeId);
	}

	public void delete(Connection c, DeleteRule deleteRule, String ratingSpecId, String officeId) throws SQLException
	{
		CwmsDbRating cwmsDbRating = CwmsDbServiceLookup.buildCwmsDb(CwmsDbRating.class, c);
		cwmsDbRating.deleteSpecs(c, ratingSpecId, deleteRule.getRule(), officeId);
	}

	// This doesn't seem to work.
	private void deleteWithRatingSet(Connection c, String officeId, String ratingSpecId) throws RatingException
	{
		RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, ratingSpecId);
		ratingSet.removeAllRatings();
		final boolean overwriteExisting = true;
		ratingSet.storeToDatabase(c, overwriteExisting);  // Does this actually delete?
	}

	public void delete(String officeId, String specificationId, long[] effectiveDates)
			throws IOException, RatingException
	{

		try
		{
			dsl.connection(c -> {
				deleteWithRatingSet(c, officeId, specificationId, effectiveDates); // This doesn't seem to work.
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException)
			{
				throw (RatingException) cause;
			}
			throw new IOException("Failed to delete Rating", ex);
		}
	}


	private void deleteWithRatingSet(Connection c, String officeId, String specificationId, long[] effectiveDates)
			throws RatingException
	{
		RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, specificationId);
		for(final long effectiveDate : effectiveDates)
		{
			ratingSet.removeRating(effectiveDate);
		}

		final boolean overwriteExisting = true;
		ratingSet.storeToDatabase(c, overwriteExisting);
	}


	@Override
	public String retrieveRatings(String format, String names, String unit, String datum, String office, String start,
	                              String end, String timezone)
	{
		return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(), names, format, unit, datum, start, end,
				timezone, office);
	}

	public Set<RatingTemplate> retrieveRatingTemplates(String office, String templateIdMask)
	{
		Set<RatingTemplate> retval;

		final RecordMapper<Record, Pair<RatingTemplate, String>> mapper = queryRecord -> {
			RatingTemplate template = buildRatingTemplate(queryRecord);

			String specID = queryRecord.get(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID);
			return new Pair<>(template, specID);
		};

		ResultQuery< ? extends Record> query = dsl.select(
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.INDEPENDENT_PARAMETERS,
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DEPENDENT_PARAMETER,
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DESCRIPTION, AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.VERSION,
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID, AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID,
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.RATING_METHODS, AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID,
				AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID).from(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE)
				.leftOuterJoin(
				AV_RATING_SPEC.AV_RATING_SPEC).on(
				AV_RATING_SPEC.AV_RATING_SPEC.TEMPLATE_ID.eq(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID))
				.where(
				AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID.eq(office)
						.and(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID.likeRegex(templateIdMask))
						.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull())
				)
				.fetchSize(1000);

//		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingTemplate, List<String>> map = new LinkedHashMap<>();

		query.fetchStream().forEach(rec -> {
			Pair<RatingTemplate, String> pair = mapper.map(rec);
			List<String> list = map.computeIfAbsent(pair.component1(), k -> new ArrayList<>());
			String specID = pair.component2();
			if(specID != null)
			{
				list.add(specID);
			}
		});

		retval = map.entrySet().stream()
				.map( entry -> new RatingTemplate.Builder()
						.fromRatingTemplate(entry.getKey())
						.withSpecs(entry.getValue())
						.build())
				.collect(Collectors.toCollection(LinkedHashSet::new));

		return retval;
	}

	private RatingTemplate buildRatingTemplate(Record queryRecord)
	{
		String indParameters = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.INDEPENDENT_PARAMETERS);
		String depParameter = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DEPENDENT_PARAMETER);
		String description = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DESCRIPTION);
		String version = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.VERSION);
		String templateId = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID);
		String office = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID);
		String ratingMethods = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.RATING_METHODS);

		List<ParameterSpec> independentParameters = buildParameterSpecs(indParameters, ratingMethods);

		return new RatingTemplate.Builder()
				.withOfficeId(office)
				.withId(templateId)
				.withVersion(version)
				.withDescription(description)
				.withDependentParameter(depParameter)
				.withIndependentParameters(independentParameters)
				.build();
	}

	private List<ParameterSpec> buildParameterSpecs(String indParameters, String ratingMethods)
	{
		List<ParameterSpec> retval = new ArrayList<>();
		String[] indParams = indParameters.split(",");
		String[] methodsForParam = ratingMethods.split("/");

		if( indParams.length != methodsForParam.length )
		{
			throw new IllegalStateException("Number of independent parameters does not match number of rating methods. indParams: " + indParameters + " methodsForParam: " + ratingMethods);
		}

		for(int i = 0; i < indParams.length; i++)
		{
				String[] methods = methodsForParam[i].split(",");
				retval.add(new ParameterSpec(indParams[i], methods[0], methods[1], methods[2]));
		}

		return retval;
	}

	public Set<RatingSpec> retrieveRatingSpecs(String office, String specIdMask)
	{
		Set<RatingSpec> retval;

		final RecordMapper<Record, Pair<RatingSpec, ZonedDateTime>> mapper = queryRecord -> {
			RatingSpec template = buildRatingSpec(queryRecord);

			Timestamp effectiveDate = queryRecord.get(AV_RATING.AV_RATING.EFFECTIVE_DATE);
			return new Pair<>(template, toZdt(effectiveDate));
		};

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
				AV_RATING_SPEC.AV_RATING_SPEC.DESCRIPTION,
				AV_RATING.AV_RATING.EFFECTIVE_DATE
						)
				.from(AV_RATING_SPEC.AV_RATING_SPEC)
				.leftOuterJoin(AV_RATING.AV_RATING)
				.on(
						AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.eq(AV_RATING.AV_RATING.RATING_ID))
				.where(AV_RATING_SPEC.AV_RATING_SPEC.OFFICE_ID.eq(office)
						.and(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID.likeRegex(specIdMask))
						.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull())
						.and(AV_RATING.AV_RATING.LOC_ALIAS_CATEGORY.isNull())
				)
				.fetchSize(1000)
				;

//		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
		query.fetchStream().forEach(rec -> {
			Pair<RatingSpec, ZonedDateTime> pair = mapper.map(rec);
			List<ZonedDateTime> list = map.computeIfAbsent(pair.component1(), k -> new ArrayList<>());
			ZonedDateTime effective = pair.component2();
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

			retval = new RatingSpec.Builder().officeId(officeId)
					.templateId(templateId).locationId(locId).version(version).sourceAgency(agency)
					.active(activeFlag).autoUpdate(autoUpdateFlag).autoActivate(autoActivateFlag)
					.autoMigrateExtension(autoMigrateExtFlag).indRoundingSpecs(indRndSpecs)
					.depRoundingSpec(depRndSpecs).description(							desc)
					.build();
		}

		return retval;

	}
}
