package cwms.radar.data.dao;

import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.AbstractRatingMetadata;
import cwms.radar.data.dto.rating.ExpressionRatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadataList;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.TableRatingMetadata;
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
import org.jooq.impl.TableImpl;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

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
                        specView.RATING_ID, specView.LOCATION_ID, specView.DESCRIPTION,
                        specView.VERSION, specView.DEP_ROUNDING_SPEC, specView.IND_ROUNDING_SPECS,
                        specView.DATE_METHODS, specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.AUTO_ACTIVATE_FLAG, specView.AUTO_UPDATE_FLAG,
                        specView.ACTIVE_FLAG, specView.SOURCE_AGENCY, specView.ALIASED_ITEM,
                        ratView.OFFICE_ID, ratView.RATING_ID, ratView.EFFECTIVE_DATE,
                        ratView.CREATE_DATE, ratView.TRANSITION_DATE,
                        ratView.VERSION, ratView.DESCRIPTION,
                        ratView.DATABASE_UNITS, ratView.NATIVE_UNITS,
                        ratView.ACTIVE_FLAG,
                        ratView.FORMULA, ratView.ALIASED_ITEM
                )
                .from(specView)
                .leftJoin(ratView)
                .on(specView.OFFICE_ID.eq(ratView.OFFICE_ID)
                        .and(specView.RATING_ID.eq(ratView.RATING_ID))
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

        if (rec != null) {
            String officeId = rec.get(AV_RATING.AV_RATING.OFFICE_ID);
            String ratingId = rec.get(AV_RATING.AV_RATING.RATING_ID);
            String description = rec.get(AV_RATING.AV_RATING.DESCRIPTION);

            // which units to use?
            // String databaseUnits = rec.get(AV_RATING.AV_RATING.DATABASE_UNITS);
            String nativeUnits = rec.get(AV_RATING.AV_RATING.NATIVE_UNITS);
            String active = rec.get(AV_RATING.AV_RATING.ACTIVE_FLAG);
            boolean activeFlag = active != null && active.equals("T");
            Timestamp effectiveDate = rec.get(AV_RATING.AV_RATING.EFFECTIVE_DATE);
            ZonedDateTime effective = RatingSpecDao.toZdt(effectiveDate);
            Timestamp createDate = rec.get(AV_RATING.AV_RATING.CREATE_DATE);
            ZonedDateTime create = RatingSpecDao.toZdt(createDate);
            Timestamp transitionDate = rec.get(AV_RATING.AV_RATING.TRANSITION_DATE);
            ZonedDateTime transition = RatingSpecDao.toZdt(transitionDate);

            AbstractRatingMetadata.Builder builder;

            String formula = rec.get(AV_RATING.AV_RATING.FORMULA);

            // Build the right builder and set concrete type fields.
            if (formula != null) {
                builder = new ExpressionRatingMetadata.Builder()
                        .withExpression(formula)
                ;
            } else {
                builder = new TableRatingMetadata.Builder()
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


}
