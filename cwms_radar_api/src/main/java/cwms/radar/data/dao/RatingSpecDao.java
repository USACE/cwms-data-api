package cwms.radar.data.dao;

import static cwms.radar.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;

import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.RatingSpecs;
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

public class RatingSpecDao extends JooqDao<RatingSpec> {
    private static final Logger logger = Logger.getLogger(RatingSpecDao.class.getName());

    public RatingSpecDao(DSLContext dsl) {
        super(dsl);
    }


    public Collection<RatingSpec> retrieveRatingSpecs(String office, String specIdMask) {

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        Condition condition =
                specView.LOC_ALIAS_CATEGORY.isNull()
                        .and(specView.LOC_ALIAS_GROUP.isNull())
                        .and(specView.ALIASED_ITEM.isNull())
                        .and(ratView.LOC_ALIAS_CATEGORY.isNull())
                        .and(ratView.LOC_ALIAS_GROUP.isNull())
                        .and(ratView.ALIASED_ITEM.isNull());

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null) {
            condition =
                    condition.and(JooqDao.caseInsensitiveLikeRegex(
                            specView.RATING_ID, specIdMask));
        }

        ResultQuery<? extends Record> query = dsl.select(specView.OFFICE_ID,
                specView.RATING_ID,
                specView.TEMPLATE_ID,
                specView.LOCATION_ID,
                specView.VERSION,
                specView.SOURCE_AGENCY,
                specView.ACTIVE_FLAG,
                specView.AUTO_UPDATE_FLAG,
                specView.AUTO_ACTIVATE_FLAG,
                specView.AUTO_MIGRATE_EXT_FLAG,
                specView.IND_ROUNDING_SPECS,
                specView.DEP_ROUNDING_SPEC,
                specView.DATE_METHODS,
                specView.DESCRIPTION,
                ratView.EFFECTIVE_DATE)
                .from(specView)
                .leftOuterJoin(ratView)
                .on(specView.RATING_ID.eq(ratView.RATING_ID))
                .where(condition)
                .fetchSize(1000);

        //	logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec template = buildRatingSpec(rec);

            Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
            ZonedDateTime effective = toZdt(effectiveDate);

            List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
            if (effective != null) {
                list.add(effective);
            }
        });

        return map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }


    public RatingSpecs retrieveRatingSpecs(String cursor, int pageSize, String office,
                                           String specIdMask) {
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

        Set<RatingSpec> retval = getRatingSpecs(office, specIdMask, offset, offset + pageSize);

        RatingSpecs.Builder builder = new RatingSpecs.Builder(offset, pageSize, total);
        builder.specs(new ArrayList<>(retval));
        return builder.build();
    }

    @NotNull
    public Set<RatingSpec> getRatingSpecs(String office, String specIdMask, int firstRow,
                                          int lastRow) {
        Set<RatingSpec> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        Condition condition = specView.LOC_ALIAS_CATEGORY.isNull()
                .and(specView.LOC_ALIAS_GROUP.isNull())
                .and(specView.ALIASED_ITEM.isNull());

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null) {
            condition = condition.and(JooqDao.caseInsensitiveLikeRegex(
                            specView.RATING_ID, specIdMask));
        }

        Condition ratingAliasNullCond = ratView.ALIASED_ITEM.isNull()
                .and(ratView.LOC_ALIAS_CATEGORY.isNull())
                .and(ratView.LOC_ALIAS_GROUP.isNull());

        SelectLimitStep<? extends Record> innerSelect = dsl.select(
                        OracleDSL.rownum().as("rnum"), specView.OFFICE_ID,
                        specView.RATING_ID,
                        specView.DATE_METHODS,
                        specView.TEMPLATE_ID,
                        specView.LOCATION_ID,
                        specView.VERSION,
                        specView.SOURCE_AGENCY,
                        specView.ACTIVE_FLAG,
                        specView.AUTO_UPDATE_FLAG,
                        specView.AUTO_ACTIVATE_FLAG,
                        specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.IND_ROUNDING_SPECS,
                        specView.DEP_ROUNDING_SPEC,
                        specView.DESCRIPTION,
                        specView.ALIASED_ITEM)
                .from(specView)
                .where(condition)
                .orderBy(specView.TEMPLATE_ID);

        ResultQuery<? extends Record> query = dsl.select(
                        DSL.field(DSL.quotedName("rnum"), Integer.class),
                        innerSelect.field(specView.OFFICE_ID),
                        innerSelect.field(specView.RATING_ID),
                        innerSelect.field(specView.DATE_METHODS),
                        innerSelect.field(specView.TEMPLATE_ID),
                        innerSelect.field(specView.LOCATION_ID),
                        innerSelect.field(specView.VERSION),
                        innerSelect.field(specView.SOURCE_AGENCY),
                        innerSelect.field(specView.ACTIVE_FLAG),
                        innerSelect.field(specView.AUTO_UPDATE_FLAG),
                        innerSelect.field(specView.AUTO_ACTIVATE_FLAG),
                        innerSelect.field(specView.AUTO_MIGRATE_EXT_FLAG),
                        innerSelect.field(specView.IND_ROUNDING_SPECS),
                        innerSelect.field(specView.DEP_ROUNDING_SPEC),
                        innerSelect.field(specView.DESCRIPTION),
                        innerSelect.field(specView.ALIASED_ITEM),
                        ratView.EFFECTIVE_DATE)
                .from(innerSelect)
                .leftOuterJoin(ratView)
                .on(innerSelect.field(specView.RATING_ID).eq(ratView.RATING_ID))
                .where(ratingAliasNullCond
                        // This is the limit condition - the whole reason for the weird query...
                        // .rnum starts at 1...
                        .and(DSL.field(DSL.quotedName("rnum")).greaterThan(firstRow))
                        .and(DSL.field(DSL.quotedName("rnum")).lessOrEqual(lastRow))
                )
                .orderBy(DSL.field(DSL.quotedName("rnum")),
                        ratView.EFFECTIVE_DATE.asc());

        logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec template = buildRatingSpec(rec);

            Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
            ZonedDateTime effective = toZdt(effectiveDate);

            List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
            if (effective != null) {
                list.add(effective);
            }
        });

        retval = map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return retval;
    }


    public Optional<RatingSpec> retrieveRatingSpec(String office, String specId) {
        Set<RatingSpec> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        Condition condition = specView.LOC_ALIAS_CATEGORY.isNull()
                .and(specView.LOC_ALIAS_GROUP.isNull())
                .and(specView.ALIASED_ITEM.isNull())
                .and(ratView.LOC_ALIAS_CATEGORY.isNull())
                .and(ratView.LOC_ALIAS_GROUP.isNull())
                .and(ratView.ALIASED_ITEM.isNull())
                .and(specView.RATING_ID.eq(specId));

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        ResultQuery<? extends Record> query = dsl.select(
                        specView.OFFICE_ID,
                        specView.RATING_ID,
                        specView.TEMPLATE_ID,
                        specView.LOCATION_ID,
                        specView.VERSION,
                        specView.SOURCE_AGENCY,
                        specView.ACTIVE_FLAG,
                        specView.AUTO_UPDATE_FLAG,
                        specView.AUTO_ACTIVATE_FLAG,
                        specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.IND_ROUNDING_SPECS,
                        specView.DEP_ROUNDING_SPEC,
                        specView.DATE_METHODS,
                        specView.DESCRIPTION,
                        ratView.EFFECTIVE_DATE
                )
                .from(specView)
                .leftOuterJoin(ratView)
                .on(specView.RATING_ID.eq(ratView.RATING_ID))
                .where(condition)
                .fetchSize(1000);

        //		logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec template = buildRatingSpec(rec);

            Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
            ZonedDateTime effective = toZdt(effectiveDate);

            List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
            if (effective != null) {
                list.add(effective);
            }
        });

        retval = map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // There should only be one key in the map
        if (retval.size() != 1) {
            throw new IllegalStateException("More than one rating spec found for id: " + specId);
        }

        return retval.stream().findFirst();
    }

    public static ZonedDateTime toZdt(final Timestamp time) {
        if (time != null) {
            return ZonedDateTime.ofInstant(time.toInstant(), ZoneId.of("UTC"));
        } else {
            return null;
        }
    }

    public static RatingSpec buildRatingSpec(Record rec) {
        RatingSpec retval = null;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;

        if (rec != null) {
            String officeId = rec.get(specView.OFFICE_ID);
            String ratingId = rec.get(specView.RATING_ID);
            String templateId = rec.get(specView.TEMPLATE_ID);
            String locId = rec.get(specView.LOCATION_ID);
            String version = rec.get(specView.VERSION);
            String agency = rec.get(specView.SOURCE_AGENCY);
            String active = rec.get(specView.ACTIVE_FLAG);
            boolean activeFlag = active != null && active.equals("T");
            String autoUp = rec.get(specView.AUTO_UPDATE_FLAG);
            boolean autoUpdateFlag = autoUp != null && autoUp.equals("T");
            String autoAct = rec.get(specView.AUTO_ACTIVATE_FLAG);
            boolean autoActivateFlag = autoAct != null && autoAct.equals("T");
            String autoMig = rec.get(specView.AUTO_MIGRATE_EXT_FLAG);
            boolean autoMigrateExtFlag = autoMig != null && autoMig.equals("T");
            String indRndSpecs = rec.get(specView.IND_ROUNDING_SPECS);

            String depRndSpecs = rec.get(specView.DEP_ROUNDING_SPEC);
            String desc = rec.get(specView.DESCRIPTION);

            String dateMethods = rec.get(specView.DATE_METHODS);

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
