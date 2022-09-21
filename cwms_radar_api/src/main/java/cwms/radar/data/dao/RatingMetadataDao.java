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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectSeekStepN;
import org.jooq.conf.ParamType;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

public class RatingMetadataDao extends JooqDao<RatingSpec> {
    private static final Logger logger = Logger.getLogger(RatingMetadataDao.class.getName());

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
        List<RatingMetadata> ratingMetadata = getRatingMetadata(office, specIdMask, seekValues,
                pageSize);

        RatingMetadataList.Builder builder = new RatingMetadataList.Builder(pageSize);
        builder.withKeySet(keySet);
        builder.withMetadata(ratingMetadata);

        return builder.build();
    }

    @NotNull
    public List<RatingMetadata> getRatingMetadata(String office, String specIdMask,
                                                  Object[] seekValues, int pageSize) {
        List<RatingMetadata> retval;

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        Condition condition = specView.LOC_ALIAS_CATEGORY.isNull()
                .and(ratView.LOC_ALIAS_CATEGORY.isNull());

        if (office != null) {
            condition = condition.and(specView.OFFICE_ID.eq(office));
        }

        if (specIdMask != null) {
            condition = condition.and(JooqDao.caseInsensitiveLikeRegex(
                    specView.RATING_ID, specIdMask));
        }


        Collection<OrderField<?>> fields = new ArrayList<>();
        fields.add(specView.OFFICE_ID);
        fields.add(specView.TEMPLATE_ID);
        fields.add(specView.RATING_ID);
        fields.add(ratView.OFFICE_ID);
        fields.add(ratView.RATING_ID);
        fields.add(ratView.EFFECTIVE_DATE.desc());


        SelectSeekStepN<Record> records = dsl.select(specView.RATING_SPEC_CODE,
                        specView.RATING_ID,
                        specView.TEMPLATE_CODE,
                        specView.TEMPLATE_ID,
                        specView.OFFICE_ID,
                        specView.LOCATION_ID,
                        specView.DESCRIPTION,
                        specView.VERSION,
                        specView.DEP_ROUNDING_SPEC,
                        specView.IND_ROUNDING_SPECS,
                        specView.DATE_METHODS,
                        specView.AUTO_MIGRATE_EXT_FLAG,
                        specView.AUTO_ACTIVATE_FLAG,
                        specView.AUTO_UPDATE_FLAG,
                        specView.ACTIVE_FLAG,
                        specView.SOURCE_AGENCY,
                        specView.LOC_ALIAS_GROUP,
                        specView.LOC_ALIAS_CATEGORY,
                        specView.ALIASED_ITEM,
                        ratView.OFFICE_ID,
                        ratView.RATING_CODE,
                        ratView.RATING_ID,
                        ratView.TEMPLATE_ID,
                        ratView.OFFICE_ID,
                        ratView.LOCATION_ID,
                        ratView.RATING_SPEC_CODE,
                        ratView.TEMPLATE_CODE,
                        ratView.PARENT_RATING_CODE,
                        ratView.EFFECTIVE_DATE,
                        ratView.VERSION,
                        ratView.DESCRIPTION,
                        ratView.DATABASE_UNITS,
                        ratView.FORMULA,
                        ratView.ACTIVE_FLAG,
                        ratView.CREATE_DATE,
                        ratView.TRANSITION_DATE,
                        ratView.NATIVE_UNITS,
                        ratView.ALIASED_ITEM,
                        ratView.LOC_ALIAS_CATEGORY,
                        ratView.LOC_ALIAS_GROUP)
                .from(specView)
                .leftJoin(ratView)
                .on(specView.RATING_ID.eq(ratView.RATING_ID))
                .where(condition)
                .orderBy(fields);

        ResultQuery<Record> query;

        if (seekValues != null && seekValues.length > 0) {
            query = records.seek(seekValues)
                    .limit(pageSize);
        } else {
            query = records.limit(pageSize);
        }

        logger.info(() -> query.getSQL(ParamType.INLINED));

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

    private AbstractRatingMetadata buildTableRatingMetadata(Record rec) {
        AbstractRatingMetadata retval = null;

        if (rec != null) {

            String officeId = rec.get(AV_RATING.AV_RATING.OFFICE_ID);
            String ratingId = rec.get(AV_RATING.AV_RATING.RATING_ID);
            String description = rec.get(AV_RATING.AV_RATING.DESCRIPTION);

            // which units to use?
//            String databaseUnits = rec.get(AV_RATING.AV_RATING.DATABASE_UNITS);
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
