package cwms.radar.data.dao;

import static org.jooq.impl.DSL.field;

import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.AbstractRatingMetadata;
import cwms.radar.data.dto.rating.ExpressionRating;
import cwms.radar.data.dto.rating.RatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadataList;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.SimpleRating;
import cwms.radar.data.dto.rating.TransitionalRating;
import cwms.radar.data.dto.rating.UsgsStreamRating;
import cwms.radar.data.dto.rating.VirtualRating;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectSeekStepN;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_TRANSITIONAL_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_USGS_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_RATING;

public class RatingMetadataDao extends JooqDao<RatingSpec> {

    public RatingMetadataDao(DSLContext dsl) {
        super(dsl);
    }

    public RatingMetadataList retrieve(String cursor, int pageSize, String office,
                                       String specIdMask) {

        Object[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        RatingMetadataList.KeySet keySet = RatingMetadataList.KeySet.build(parts);

        pageSize = RatingMetadataList.KeySet.parsePageSize(parts, pageSize);

        Object[] seekValues = null;
        if (keySet != null) {
            seekValues = keySet.getSeekValues();
        }
        List<RatingMetadata> ratingMetadata = getRatingMetadata(office, specIdMask,
                RatingMetadataList.KeySet.getSeekFieldNames(), seekValues,
                pageSize);

        RatingMetadataList.Builder builder = new RatingMetadataList.Builder(pageSize);
        builder.withKeySet(keySet);
        builder.withMetadata(ratingMetadata);

        return builder.build();
    }

    /**
     * Retrieve a list of RatingMetadata objects.
     * @param office The office to search for ratings.
     *               If null, all offices are searched.
     * @param specIdMask    The spec id mask to search for ratings.
     *                      If null, all spec ids are searched.
     * @param seekFieldNames   The field names to use for keyset pagination.
     * @param seekValues    The values to use for keyset pagination.
     *                      Size and order must match seekFieldNames except for first page.
     *                      On first page seekValues will be null.
     * @param pageSize  The number of records to return.
     * @return A list of RatingMetadata objects.
     */
    @NotNull
    private List<RatingMetadata> getRatingMetadata(String office, String specIdMask,
                                                  @NotNull String[] seekFieldNames,
                                                  @Nullable Object[] seekValues,
                                                  int pageSize) {
        List<RatingMetadata> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;
        AV_USGS_RATING usgsView = AV_USGS_RATING.AV_USGS_RATING;

        // Does it matter for speed or other reasons if we check
        // LOC_ALIAS_CATEGORY or LOC_ALIAS_GROUP too?
        // I know that specView ALIASED_ITEM can be null without ratView ALIASED_ITEM being null
        // but is ALIASED_ITEM ever null when the LOC_ALIAS_* fields aren't?
        Condition condition = specView.ALIASED_ITEM.isNull()
                .and(ratView.ALIASED_ITEM.isNull());

        if (office != null && !office.isEmpty()) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null && !specIdMask.isEmpty()) {
            condition = condition.and(JooqDao.caseInsensitiveLikeRegex(
                    specView.RATING_ID, specIdMask));
        }

        Collection<OrderField<?>> seekFields = getOrderFields(seekFieldNames);

        SelectSeekStepN<Record> records = dsl.select(specView.OFFICE_ID, specView.TEMPLATE_ID,
                        specView.RATING_SPEC_CODE,
                        specView.RATING_ID, specView.LOCATION_ID, specView.DESCRIPTION,
                        specView.VERSION, specView.DEP_ROUNDING_SPEC, specView.IND_ROUNDING_SPECS,
                        specView.DATE_METHODS, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.ACTIVE_FLAG, specView.SOURCE_AGENCY, specView.ALIASED_ITEM,
                        ratView.OFFICE_ID, ratView.RATING_ID, ratView.EFFECTIVE_DATE,
                        ratView.RATING_SPEC_CODE,
                        ratView.CREATE_DATE, ratView.TRANSITION_DATE,
                        ratView.VERSION, ratView.DESCRIPTION,
                        ratView.DATABASE_UNITS, ratView.NATIVE_UNITS,
                        ratView.ACTIVE_FLAG,
                        ratView.FORMULA, ratView.ALIASED_ITEM,
                        usgsView.OFFICE_ID, usgsView.RATING_SPEC, usgsView.LATEST_EFFECITVE,
                        usgsView.LATEST_CREATE, usgsView.USGS_SITE, usgsView.AUTO_UPDATE_FLAG,
                        usgsView.AUTO_ACTIVATE_FLAG, usgsView.AUTO_MIGRATE_EXT_FLAG,
                        usgsView.RATING_METHOD_ID
                )
                .from(specView)
                .leftJoin(ratView)
                .on(specView.RATING_SPEC_CODE.eq(ratView.RATING_SPEC_CODE))
                .leftOuterJoin(usgsView).on(specView.RATING_SPEC_CODE.eq(usgsView.RATING_SPEC_CODE)
                        .and(ratView.EFFECTIVE_DATE.eq(usgsView.LATEST_EFFECITVE))
                )
                .where(condition)
                .orderBy(seekFields);

        ResultQuery<Record> query;

        if (seekValues != null && seekValues.length > 0) {
            query = records.seek(seekValues)
                    .limit(pageSize);
        } else {
            query = records.limit(pageSize);
        }

        // logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<AbstractRatingMetadata>> metadata = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec spec = RatingSpecDao.buildRatingSpec(rec);

            AbstractRatingMetadata metadataRecord = buildTableRatingMetadata(rec);
            if (metadataRecord != null) {
                List<AbstractRatingMetadata> list = metadata.computeIfAbsent(spec,
                        k -> new ArrayList<>());
                list.add(metadataRecord);
            }
        });

        retval = metadata.entrySet().stream()
                .map(entry -> new RatingMetadata.Builder()
                        .withRatingSpec(entry.getKey())
                        .withRatings(entry.getValue())
                        .build())
                .collect(Collectors.toList());
        return retval;
    }


    @NotNull
    public List<RatingMetadata> getTransitionalRatingMetadata(String office, String specIdMask,
                                                   @NotNull String[] seekFieldNames,
                                                   @Nullable Object[] seekValues,
                                                   int pageSize) {
        List<RatingMetadata> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_TRANSITIONAL_RATING transView = AV_TRANSITIONAL_RATING.AV_TRANSITIONAL_RATING;


        Condition condition = DSL.trueCondition();

        if (office != null && !office.isEmpty()) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null && !specIdMask.isEmpty()) {
            condition = condition.and(JooqDao.caseInsensitiveLikeRegex(
                    specView.RATING_ID, specIdMask));
        }

        // "AV_RATING_SPEC.OFFICE_ID",
        // "AV_RATING_SPEC.TEMPLATE_ID",
        // "AV_RATING_SPEC.RATING_ID",
        // "AV_RATING.EFFECTIVE_DATE"

        Collection<OrderField<?>> seekFields = getOrderFields(seekFieldNames);

        seekFields = seekFields.stream().map(field -> {
            if (field.equals(AV_RATING.AV_RATING.EFFECTIVE_DATE)) {
                return transView.EFFECTIVE_DATE;
            } else {
                return field;
            }
        }).collect(Collectors.toList());


//        Field<Long> transCode = (field( "CWMS_20.AT_TRANSITIONAL_RATING.TRANSITIONAL_RATING_CODE", Long.class));
//        Field<String> transDesc = field( "CWMS_20.AT_TRANSITIONAL_RATING.DESCRIPTION", String.class);
//        Field<String> transActive = field( "CWMS_20.AT_TRANSITIONAL_RATING.ACTIVE_FLAG", String.class);

        SelectSeekStepN<? extends Record> records = dsl.select(specView.OFFICE_ID,
                        specView.TEMPLATE_ID,
                        specView.RATING_ID, specView.LOCATION_ID, specView.DESCRIPTION,
                        specView.VERSION, specView.DEP_ROUNDING_SPEC, specView.IND_ROUNDING_SPECS,
                        specView.DATE_METHODS, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.ACTIVE_FLAG, specView.SOURCE_AGENCY, specView.ALIASED_ITEM,
                        transView.OFFICE_ID, transView.EFFECTIVE_DATE,
                        transView.CREATE_DATE, transView.TRANSITION_DATE, transView.NATIVE_UNITS
//                        ,transCode, transDesc, transActive

                )
                .from(specView)
                .leftJoin(transView)
                .on(specView.OFFICE_ID.eq(transView.OFFICE_ID)
                        .and(specView.RATING_SPEC_CODE.eq(transView.RATING_SPEC_CODE))
                )
//                .leftJoin("CWMS_20.AT_TRANSITIONAL_RATING")
//                .on(transView.TRANSITIONAL_RATING_CODE.eq(transCode))
                .where(condition)
                .orderBy(seekFields);

        ResultQuery<? extends Record> query;

        if (seekValues != null && seekValues.length > 0) {
            query = records.seek(seekValues)
                    .limit(pageSize);
        } else {
            query = records.limit(pageSize);
        }

        // logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<AbstractRatingMetadata>> metadata = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec spec = RatingSpecDao.buildRatingSpec(rec);

            TransitionalRating metadataRecord = buildTransRatingMetadata(rec);
            if (metadataRecord != null) {
                List<AbstractRatingMetadata> list = metadata.computeIfAbsent(spec,
                        k -> new ArrayList<>());
                list.add(metadataRecord);
            }
        });

        retval = metadata.entrySet().stream()
                .map(entry -> new RatingMetadata.Builder()
                        .withRatingSpec(entry.getKey())
                        .withRatings(entry.getValue())
                        .build())
                .collect(Collectors.toList());
        return retval;
    }

    private TransitionalRating buildTransRatingMetadata(Record rec) {
        TransitionalRating retval = null;

        AV_TRANSITIONAL_RATING ratView = AV_TRANSITIONAL_RATING.AV_TRANSITIONAL_RATING;

//        // Can only do this if the user for the database connection has
//        // table access permission.
//        Field<Long> transCode = (field( "AT_TRANSITIONAL_RATING.TRANSITIONAL_RATING_CODE", Long.class));
//        Field<String> transDesc = field( "AT_TRANSITIONAL_RATING.DESCRIPTION", String.class);
//        Field<String> transActive = field( "AT_TRANSITIONAL_RATING.ACTIVE_FLAG", String.class);

        if (rec != null) {
            String officeId = rec.get(ratView.OFFICE_ID);
            String ratingSpecId = rec.get(ratView.RATING_SPEC);
            String description = null; //rec.get(transDesc);

            // which units to use?
            // String databaseUnits = rec.get(AV_RATING.AV_RATING.DATABASE_UNITS);
            String nativeUnits = rec.get(ratView.NATIVE_UNITS);
            String active = null;  //rec.get(transActive);
            boolean activeFlag = active != null && active.equals("T");
            Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
            ZonedDateTime effective = RatingSpecDao.toZdt(effectiveDate);
            Timestamp createDate = rec.get(ratView.CREATE_DATE);
            ZonedDateTime create = RatingSpecDao.toZdt(createDate);
            Timestamp transitionDate = rec.get(ratView.TRANSITION_DATE);
            ZonedDateTime transition = RatingSpecDao.toZdt(transitionDate);

            TransitionalRating.Builder builder = new TransitionalRating.Builder();

            AbstractRatingMetadata.Builder absBuilder = builder;

            // Set the AbstractRatingMetadata fields
            absBuilder = absBuilder
                    .withOfficeId(officeId)
                    .withRatingSpecId(ratingSpecId)
                    .withDescription(description)
                    .withUnitsId(nativeUnits)
                    .withActive(activeFlag)
                    .withEffectiveDate(effective)
                    .withCreateDate(create)
                    .withTransitionDate(transition)
            ;

            retval = builder.build();
        }

        return retval;
    }



    @NotNull
    private static Collection<OrderField<?>> getOrderFields(String[] fieldNames) {
        Collection<OrderField<?>> seekFields = new ArrayList<>();
        for (String fieldName : fieldNames) {
            seekFields.add(fieldFromName(fieldName));
        }

        return seekFields;
    }

    private static Field<?> fieldFromName(String fieldName) {
        Field<?> retval;

        String[] parts = fieldName.split("\\.");

        if (parts.length == 2) {
            TableImpl<?> table = getTable(parts[0]);
            String column = parts[1];
            retval = table.field(column);
        } else {
            throw new IllegalArgumentException("Unknown field name: " + fieldName);
        }
        return retval;
    }

    @NotNull
    private static TableImpl<?> getTable(String view) {
        TableImpl<?> table;
        if (AV_RATING_SPEC.AV_RATING_SPEC.getName().equals(view)) {
            table = AV_RATING_SPEC.AV_RATING_SPEC;
        } else if (AV_RATING.AV_RATING.getName().equals(view)) {
            table = AV_RATING.AV_RATING;
        } else {
            throw new IllegalArgumentException("Unknown table: " + view);
        }
        return table;
    }

    private AbstractRatingMetadata buildTableRatingMetadata(Record rec) {
        AbstractRatingMetadata retval = null;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;
        AV_USGS_RATING usgsView = AV_USGS_RATING.AV_USGS_RATING;

        if (rec != null) {
            String officeId = rec.get(ratView.OFFICE_ID);
            String ratingId = rec.get(ratView.RATING_ID);
            String description = rec.get(ratView.DESCRIPTION);

            // which units to use?
            // String databaseUnits = rec.get(AV_RATING.AV_RATING.DATABASE_UNITS);
            String nativeUnits = rec.get(ratView.NATIVE_UNITS);
            String active = rec.get(ratView.ACTIVE_FLAG);
            boolean activeFlag = active != null && active.equals("T");
            Timestamp effectiveDate = rec.get(ratView.EFFECTIVE_DATE);
            ZonedDateTime effective = RatingSpecDao.toZdt(effectiveDate);
            Timestamp createDate = rec.get(ratView.CREATE_DATE);
            ZonedDateTime create = RatingSpecDao.toZdt(createDate);
            Timestamp transitionDate = rec.get(ratView.TRANSITION_DATE);
            ZonedDateTime transition = RatingSpecDao.toZdt(transitionDate);

            AbstractRatingMetadata.Builder builder;

            String formula = rec.get(ratView.FORMULA);
            String usgsSite = rec.get(usgsView.USGS_SITE);

            // Build the right builder and set concrete type fields.
            if (formula != null) {
                builder = new ExpressionRating.Builder()
                        .withExpression(formula)

                ;
            } else if (usgsSite != null) {
                builder = new UsgsStreamRating.Builder()
                        .withUsgsSite(usgsSite);
            } else {
                builder = new SimpleRating.Builder()
                ;
            }

            // Set the AbstractRatingMetadata fields
            builder = builder
                    .withOfficeId(officeId)
                    .withRatingSpecId(ratingId)
                    .withDescription(description)
                    .withUnitsId(nativeUnits)
                    .withActive(activeFlag)
                    .withEffectiveDate(effective)
                    .withCreateDate(create)
                    .withTransitionDate(transition)
            ;

            retval = builder.build();
        }

        return retval;
    }


    @NotNull
    public List<RatingMetadata> getVirtualRatingMetadata(String office, String specIdMask,
                                                              @NotNull String[] seekFieldNames,
                                                              @Nullable Object[] seekValues,
                                                              int pageSize) {
        List<RatingMetadata> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_VIRTUAL_RATING virtView = AV_VIRTUAL_RATING.AV_VIRTUAL_RATING;


        Condition condition = DSL.trueCondition();

        if (office != null && !office.isEmpty()) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null && !specIdMask.isEmpty()) {
            condition = condition.and(JooqDao.caseInsensitiveLikeRegex(
                    specView.RATING_ID, specIdMask));
        }

        // "AV_RATING_SPEC.OFFICE_ID",
        // "AV_RATING_SPEC.TEMPLATE_ID",
        // "AV_RATING_SPEC.RATING_ID",
        // "AV_RATING.EFFECTIVE_DATE"

        Collection<OrderField<?>> seekFields = getOrderFields(seekFieldNames);

        seekFields = seekFields.stream().map(field -> {
            if (field.equals(AV_RATING.AV_RATING.EFFECTIVE_DATE)) {
                return virtView.EFFECTIVE_DATE;
            } else {
                return field;
            }
        }).collect(Collectors.toList());



        SelectSeekStepN<? extends Record> records = dsl.select(specView.OFFICE_ID,
                        specView.TEMPLATE_ID,
                        specView.RATING_ID, specView.LOCATION_ID, specView.DESCRIPTION,
                        specView.VERSION, specView.DEP_ROUNDING_SPEC, specView.IND_ROUNDING_SPECS,
                        specView.DATE_METHODS, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.ACTIVE_FLAG, specView.SOURCE_AGENCY, specView.ALIASED_ITEM,
                        virtView.OFFICE_ID, virtView.RATING_SPEC_CODE,
                        virtView.EFFECTIVE_DATE,
                        virtView.CREATE_DATE, virtView.TRANSITION_DATE, virtView.UNITS
//                        ,transCode, transDesc, transActive

                )
                .from(specView)
                .leftJoin(virtView)
                .on(specView.OFFICE_ID.eq(virtView.OFFICE_ID)
                        .and(specView.RATING_SPEC_CODE.eq(virtView.RATING_SPEC_CODE))
                )
//                .leftJoin("CWMS_20.AT_TRANSITIONAL_RATING")
//                .on(transView.TRANSITIONAL_RATING_CODE.eq(transCode))
                .where(condition)
                .orderBy(seekFields);

        ResultQuery<? extends Record> query;

        if (seekValues != null && seekValues.length > 0) {
            query = records.seek(seekValues)
                    .limit(pageSize);
        } else {
            query = records.limit(pageSize);
        }

        // logger.info(() -> query.getSQL(ParamType.INLINED));

        Map<RatingSpec, List<AbstractRatingMetadata>> metadata = new LinkedHashMap<>();
        query.fetchStream().forEach(rec -> {
            RatingSpec spec = RatingSpecDao.buildRatingSpec(rec);

            VirtualRating metadataRecord = buildVirtualRatingMetadata(rec);
            if (metadataRecord != null) {
                List<AbstractRatingMetadata> list = metadata.computeIfAbsent(spec,
                        k -> new ArrayList<>());
                list.add(metadataRecord);
            }
        });

        retval = metadata.entrySet().stream()
                .map(entry -> new RatingMetadata.Builder()
                        .withRatingSpec(entry.getKey())
                        .withRatings(entry.getValue())
                        .build())
                .collect(Collectors.toList());
        return retval;
    }

    private VirtualRating buildVirtualRatingMetadata(Record rec) {
        VirtualRating retval = null;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_VIRTUAL_RATING virtView = AV_VIRTUAL_RATING.AV_VIRTUAL_RATING;


        if (rec != null) {
            String officeId = rec.get(virtView.OFFICE_ID);
            String ratingSpecId = rec.get(virtView.RATING_SPEC);
            String description = null; //rec.get(transDesc);

            // which units to use?
            // String databaseUnits = rec.get(AV_RATING.AV_RATING.DATABASE_UNITS);
            String nativeUnits = rec.get(virtView.UNITS);
            String active = null;  //rec.get(transActive);
            boolean activeFlag = active != null && active.equals("T");
            Timestamp effectiveDate = rec.get(virtView.EFFECTIVE_DATE);
            ZonedDateTime effective = RatingSpecDao.toZdt(effectiveDate);
            Timestamp createDate = rec.get(virtView.CREATE_DATE);
            ZonedDateTime create = RatingSpecDao.toZdt(createDate);
            Timestamp transitionDate = rec.get(virtView.TRANSITION_DATE);
            ZonedDateTime transition = RatingSpecDao.toZdt(transitionDate);

            VirtualRating.Builder builder = new VirtualRating.Builder();

            AbstractRatingMetadata.Builder absBuilder = builder;

            // Set the AbstractRatingMetadata fields
            absBuilder = absBuilder
                    .withOfficeId(officeId)
                    .withRatingSpecId(ratingSpecId)
                    .withDescription(description)
                    .withUnitsId(nativeUnits)
                    .withActive(activeFlag)
                    .withEffectiveDate(effective)
                    .withCreateDate(create)
                    .withTransitionDate(transition)
            ;

            retval = builder.build();
        }

        return retval;
    }


}
