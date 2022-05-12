package cwms.radar.data.dao;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.rating.ParameterSpec;
import cwms.radar.data.dto.rating.RatingTemplate;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;

import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_TEMPLATE;

public class RatingTemplateDao extends JooqDao<RatingTemplate>
{
	private static final Logger logger = Logger.getLogger(RatingTemplateDao.class.getName());
	public RatingTemplateDao(DSLContext dsl)
	{
		super(dsl);
	}

	public Set<RatingTemplate> retrieveRatingTemplates(String office, String templateIdMask)
	{
		Set<RatingTemplate> retval;

		Condition condition = 	AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull();

		if(office != null){
			condition = condition.and(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID.eq(office));
		}

		if(templateIdMask != null){
			condition = condition.and(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID.likeRegex(templateIdMask));
		}

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
				.where(condition)
				.fetchSize(1000);

		//		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingTemplate, List<String>> map = new LinkedHashMap<>();

		query.fetchStream().forEach(rec -> {
			RatingTemplate template = buildRatingTemplate(rec);
			String specID = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID);

			List<String> list = map.computeIfAbsent(template, k -> new ArrayList<>());
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

	public Optional<RatingTemplate> retrieveRatingTemplate(String office, String templateId)
	{
		Set<RatingTemplate> retval;

		Condition condition = AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID.eq(templateId)
				.and(AV_RATING_SPEC.AV_RATING_SPEC.LOC_ALIAS_CATEGORY.isNull());

		if(office != null){
			condition = condition.and(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID.eq(office));
		}

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
				.where(condition)
				.fetchSize(1000);

		//		logger.info(() -> query.getSQL(ParamType.INLINED));

		Map<RatingTemplate, List<String>> map = new LinkedHashMap<>();

		query.fetchStream().forEach(rec -> {
			RatingTemplate template = buildRatingTemplate(rec);
			String specID = rec.get(AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID);

			List<String> list = map.computeIfAbsent(template, k -> new ArrayList<>());
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

		// There should only be one key in the map

		if(retval.size()!= 1){
			throw new IllegalStateException("More than one template found for templateId: " + templateId);
		}

		return retval.stream().findFirst();
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


}
