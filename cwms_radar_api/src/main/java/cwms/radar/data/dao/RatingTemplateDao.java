package cwms.radar.data.dao;

import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.ParameterSpec;
import cwms.radar.data.dto.rating.RatingTemplate;
import cwms.radar.data.dto.rating.RatingTemplates;
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
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectForUpdateStep;
import org.jooq.TableField;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_TEMPLATE;

public class RatingTemplateDao extends JooqDao<RatingTemplate> {
    private static final Logger logger = Logger.getLogger(RatingTemplateDao.class.getName());

    public RatingTemplateDao(DSLContext dsl) {
        super(dsl);
    }

    public Set<RatingTemplate> retrieveRatingTemplates(String office, String templateIdMask) {
        Set<RatingTemplate> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        Condition condition = specView.ALIASED_ITEM.isNull();

        AV_RATING_TEMPLATE tempView = AV_RATING_TEMPLATE.AV_RATING_TEMPLATE;

        if (office != null) {
            condition = condition.and(tempView.OFFICE_ID.eq(office));
        }

        if (templateIdMask != null) {
            Condition regex = JooqDao.caseInsensitiveLikeRegex(tempView.TEMPLATE_ID, templateIdMask);
            condition = condition.and(regex);
        }

        ResultQuery<? extends Record> query = dsl.select(
                        tempView.OFFICE_ID, tempView.TEMPLATE_ID,
                        tempView.INDEPENDENT_PARAMETERS, tempView.DEPENDENT_PARAMETER,
                        tempView.DESCRIPTION, tempView.VERSION,
                        tempView.RATING_METHODS, tempView.TEMPLATE_CODE,
                        specView.ALIASED_ITEM, specView.TEMPLATE_CODE, specView.RATING_ID
                )
                .from(tempView)
                .leftOuterJoin(specView)
                .on(specView.TEMPLATE_CODE.eq(tempView.TEMPLATE_CODE))
                .where(condition)
                .fetchSize(1000);

        //		logger.info(() -> query.getSQL(ParamType.INLINED));

        return buildRatingTemplateSet(query);
    }

    @NotNull
    private Set<RatingTemplate> buildRatingTemplateSet(ResultQuery<? extends Record> query) {

        TableField<usace.cwms.db.jooq.codegen.tables.records.AV_RATING_SPEC, String> idField =
                AV_RATING_SPEC.AV_RATING_SPEC.RATING_ID;

        Map<RatingTemplate, List<String>> map = new LinkedHashMap<>();

        try (Stream<? extends Record> stream = query.fetchStream()) {
            stream.forEach(rec -> {
                RatingTemplate template = buildRatingTemplate(rec);
                String specID = rec.get(idField);

                List<String> list = map.computeIfAbsent(template, k -> new ArrayList<>());
                if (specID != null) {
                    list.add(specID);
                }
            });
        }

        return map.entrySet().stream()
                .map(entry -> new RatingTemplate.Builder()
                        .fromRatingTemplate(entry.getKey())
                        .withRatingIds(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public Optional<RatingTemplate> retrieveRatingTemplate(String office, String templateId) {
        Set<RatingTemplate> retval;

        AV_RATING_TEMPLATE tempView = AV_RATING_TEMPLATE.AV_RATING_TEMPLATE;
        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;

        Condition condition = tempView.TEMPLATE_ID.eq(templateId)
                .and(specView.ALIASED_ITEM.isNull());

        if (office != null) {
            condition = condition.and(tempView.OFFICE_ID.eq(office));
        }

        ResultQuery<? extends Record> query = dsl.select(
                        tempView.TEMPLATE_CODE, tempView.OFFICE_ID, tempView.TEMPLATE_ID,
                        tempView.INDEPENDENT_PARAMETERS, tempView.DEPENDENT_PARAMETER,
                        tempView.DESCRIPTION, tempView.VERSION, tempView.RATING_METHODS,
                        specView.ALIASED_ITEM, specView.TEMPLATE_CODE, specView.RATING_ID
                ).from(tempView)
                .leftOuterJoin(specView).on(
                        specView.TEMPLATE_CODE.eq(tempView.TEMPLATE_CODE))
                .where(condition)
                .fetchSize(1000);

        //		logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingTemplate, List<String>> map = new LinkedHashMap<>();

        try (Stream<? extends Record> stream = query.fetchStream()) {
            stream.forEach(rec -> {
                RatingTemplate template = buildRatingTemplate(rec);
                String specID = rec.get(specView.RATING_ID);

                List<String> list = map.computeIfAbsent(template, k -> new ArrayList<>());
                if (specID != null) {
                    list.add(specID);
                }
            });
        }

        retval = map.entrySet().stream()
                .map(entry -> new RatingTemplate.Builder()
                        .fromRatingTemplate(entry.getKey())
                        .withRatingIds(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // There should only be one key in the map
        if (retval.size() != 1) {
            throw new IllegalStateException("More than one template found for templateId: " + templateId);
        }

        return retval.stream().findFirst();
    }


    private RatingTemplate buildRatingTemplate(Record queryRecord) {
        String indParameters =
                queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.INDEPENDENT_PARAMETERS);
        String depParameter =
                queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DEPENDENT_PARAMETER);
        String description = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.DESCRIPTION);
        String version = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.VERSION);
        String templateId = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.TEMPLATE_ID);
        String office = queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.OFFICE_ID);
        String ratingMethods =
                queryRecord.get(AV_RATING_TEMPLATE.AV_RATING_TEMPLATE.RATING_METHODS);

        List<ParameterSpec> independentParameters = buildParameterSpecs(indParameters,
                ratingMethods);

        return new RatingTemplate.Builder()
                .withOfficeId(office)
                .withId(templateId)
                .withVersion(version)
                .withDescription(description)
                .withDependentParameter(depParameter)
                .withIndependentParameterSpecs(independentParameters)
                .build();
    }

    private List<ParameterSpec> buildParameterSpecs(String indParameters, String ratingMethods) {
        List<ParameterSpec> retval = new ArrayList<>();
        String[] indParams = indParameters.split(",");
        String[] methodsForParam = ratingMethods.split("/");

        if (indParams.length != methodsForParam.length) {
            throw new IllegalStateException("Number of independent parameters does not match "
                    + "number of rating methods. indParams: " + indParameters + " methodsForParam:"
                    + " " + ratingMethods);
        }

        for (int i = 0; i < indParams.length; i++) {
            String[] methods = methodsForParam[i].split(",");
            retval.add(new ParameterSpec(indParams[i], methods[1], methods[0], methods[2]));
        }

        return retval;
    }


    public RatingTemplates retrieveRatingTemplates(String cursor, int pageSize, String office,
                                                   String templateIdMask) {
        Integer total = null;
        int offset = 0;

        if (cursor != null && !cursor.isEmpty()) {
            String[] parts = CwmsDTOPaginated.decodeCursor(cursor);

            if (parts.length > 2) {
                offset = Integer.parseInt(parts[0]);
                if (!"null".equals(parts[1])) {
                    try {
                        total = Integer.valueOf(parts[1]);
                    } catch (NumberFormatException e) {
                        logger.log(Level.INFO, "Could not parse " + parts[1]);
                    }
                }
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        Collection<RatingTemplate> templates = getRatingTemplates(office, templateIdMask, offset, pageSize);

        RatingTemplates.Builder builder = new RatingTemplates.Builder(offset, pageSize, total);
        builder.templates(new ArrayList<>(templates));
        return builder.build();
    }

    @NotNull
    private Set<RatingTemplate> getRatingTemplates(String office, String templateIdMask,
                                                   int firstRow, int pageSize) {
        AV_RATING_TEMPLATE tempView = AV_RATING_TEMPLATE.AV_RATING_TEMPLATE;
        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;

        Condition condition = specView.ALIASED_ITEM.isNull();

        if (office != null) {
            condition = condition.and(tempView.OFFICE_ID.eq(office));
        }

        if (templateIdMask != null) {
            Condition maskRegex = JooqDao.caseInsensitiveLikeRegex(tempView.TEMPLATE_ID,
                    templateIdMask);
            condition = condition.and(maskRegex);
        }

        SelectForUpdateStep<? extends Record> query = dsl.select(
                        tempView.TEMPLATE_CODE, tempView.OFFICE_ID, tempView.TEMPLATE_ID,
                        tempView.INDEPENDENT_PARAMETERS, tempView.DEPENDENT_PARAMETER,
                        tempView.DESCRIPTION, tempView.VERSION, tempView.RATING_METHODS,
                        specView.TEMPLATE_CODE, specView.RATING_ID, specView.ALIASED_ITEM)
                .from(tempView)
                .leftOuterJoin(specView).on(tempView.TEMPLATE_CODE.eq(specView.TEMPLATE_CODE))
                .where(condition)
                .orderBy(tempView.OFFICE_ID, tempView.TEMPLATE_ID, specView.RATING_ID)
                .limit(pageSize)
                .offset(firstRow);

//				logger.info(() -> query.getSQL(ParamType.INLINED));

        return buildRatingTemplateSet(query);
    }

}
