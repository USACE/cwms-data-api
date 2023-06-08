/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.conf.ParamType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
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

        // We don't want to also check AV_RATING_SPEC.ALIASED_ITEM b/c we
        // don't care whether the specs returned are an alias or not.
        // We do want to exclude the aliased ratings b/c we only want one
        // copy of each matching rating.

        Condition condition = ratView.ALIASED_ITEM.isNull();

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null) {
            Condition likeRegex = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID, specIdMask);
            condition = condition.and(likeRegex);
        }

        ResultQuery<? extends Record> query = dsl.select(specView.RATING_SPEC_CODE,
                        specView.OFFICE_ID, specView.RATING_ID, specView.TEMPLATE_ID,
                        specView.LOCATION_ID, specView.VERSION, specView.SOURCE_AGENCY,
                        specView.ACTIVE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.AUTO_ACTIVATE_FLAG,
                        specView.AUTO_MIGRATE_EXT_FLAG, specView.IND_ROUNDING_SPECS,
                        specView.DEP_ROUNDING_SPEC, specView.DATE_METHODS, specView.DESCRIPTION,
                        ratView.RATING_SPEC_CODE, ratView.EFFECTIVE_DATE)
                .from(specView)
                .leftOuterJoin(ratView)
                .on(specView.RATING_SPEC_CODE.eq(ratView.RATING_SPEC_CODE))
                .where(condition)
                .fetchSize(1000);

        //	logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        try (Stream<? extends Record> stream = query.fetchStream()) {
            stream.forEach(rec -> {
                RatingSpec template = buildRatingSpec(rec);

                Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
                ZonedDateTime effective = toZdt(effectiveDate);

                List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
                if (effective != null) {
                    list.add(effective);
                }
            });
        }

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

        Set<RatingSpec> retval = getRatingSpecs(office, specIdMask, offset, pageSize);

        RatingSpecs.Builder builder = new RatingSpecs.Builder(offset, pageSize, total);
        builder.specs(new ArrayList<>(retval));
        return builder.build();
    }

    @NotNull
    public Set<RatingSpec> getRatingSpecs(String office, String specIdMask, int firstRow,
                                          int pageSize) {
        Set<RatingSpec> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        // We don't want to also check AV_RATING_SPEC.ALIASED_ITEM b/c we
        // don't care whether the specs returned are an alias or not.
        // We do want to exclude the aliased ratings b/c we only want one
        // copy of each matching rating.
        Condition condition = ratView.ALIASED_ITEM.isNull();

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null) {
            Condition maskRegex = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID, specIdMask);
            condition = condition.and(maskRegex);
        }

        ResultQuery<? extends Record> query = dsl.select(specView.RATING_SPEC_CODE,
                        specView.OFFICE_ID, specView.RATING_ID, specView.DATE_METHODS,
                        specView.TEMPLATE_ID, specView.LOCATION_ID, specView.VERSION,
                        specView.SOURCE_AGENCY, specView.ACTIVE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.IND_ROUNDING_SPECS, specView.DEP_ROUNDING_SPEC,
                        specView.DESCRIPTION, specView.ALIASED_ITEM,
                        ratView.RATING_SPEC_CODE, ratView.EFFECTIVE_DATE)
                .from(specView)
                .leftOuterJoin(ratView)
                .on(specView.RATING_SPEC_CODE.eq(ratView.RATING_SPEC_CODE))
                .where(condition)
                .orderBy(specView.OFFICE_ID, specView.TEMPLATE_ID, ratView.RATING_ID,
                        ratView.EFFECTIVE_DATE)
                .limit(pageSize)
                .offset(firstRow);

        logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        try (Stream<? extends Record> stream = query.fetchStream()) {
            stream.forEach(rec -> {
                RatingSpec template = buildRatingSpec(rec);

                Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
                ZonedDateTime effective = toZdt(effectiveDate);

                List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
                if (effective != null) {
                    list.add(effective);
                }
            });
        }

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

        Condition condition = ratView.ALIASED_ITEM.isNull();

        if (specId != null) {
            condition = condition.and(specView.RATING_ID.eq(specId));
        }

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        ResultQuery<? extends Record> query = dsl.select(
                        specView.RATING_SPEC_CODE,
                        specView.OFFICE_ID, specView.RATING_ID, specView.TEMPLATE_ID,
                        specView.LOCATION_ID, specView.VERSION, specView.SOURCE_AGENCY,
                        specView.ACTIVE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.IND_ROUNDING_SPECS, specView.DEP_ROUNDING_SPEC,
                        specView.DATE_METHODS, specView.DESCRIPTION,
                        ratView.RATING_SPEC_CODE, ratView.EFFECTIVE_DATE
                )
                .from(specView)
                .leftOuterJoin(ratView)
                .on(specView.RATING_SPEC_CODE.eq(ratView.RATING_SPEC_CODE))
                .where(condition)
                .orderBy(specView.OFFICE_ID, specView.RATING_ID, ratView.EFFECTIVE_DATE)
                .fetchSize(1000);

        //		logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<ZonedDateTime>> map = new LinkedHashMap<>();
        try (Stream<? extends Record> stream = query.fetchStream()) {
            stream.forEach(rec -> {
                RatingSpec template = buildRatingSpec(rec);

                Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
                ZonedDateTime effective = toZdt(effectiveDate);

                List<ZonedDateTime> list = map.computeIfAbsent(template, k -> new ArrayList<>());
                if (effective != null) {
                    list.add(effective);
                }
            });
        }

        retval = map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // There should only be one key in the map
        if (retval.size() > 1) {
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


    public void delete(String office, DeleteMethod deleteMethod, String ratingSpecId) {
        String deleteAction;
        switch(deleteMethod) {
            case DELETE_ALL:
                deleteAction = DeleteRule.DELETE_ALL.getRule();
                break;
            case DELETE_DATA:
                deleteAction = DeleteRule.DELETE_DATA.getRule();
                break;
            case DELETE_KEY:
                deleteAction = DeleteRule.DELETE_KEY.getRule();
                break;
            default:
                throw new IllegalArgumentException("Delete Method provided does not match accepted rule constants: "
                    + deleteMethod);
        }
        dsl.connection(c ->
            CWMS_RATING_PACKAGE.call_DELETE_SPECS(
                getDslContext(c,office).configuration(),
                ratingSpecId,
                deleteAction,
                office)
        );
        
    }

    public void create(String xml, boolean failIfExists) {
        final String office = RatingDao.extractOfficeFromXml(xml);
        dsl.connection(c -> 
            CWMS_RATING_PACKAGE.call_STORE_SPECS__3(
                getDslContext(c,office).configuration(),
                xml,
                OracleTypeMap.formatBool(failIfExists))
        );
        
    }
}
