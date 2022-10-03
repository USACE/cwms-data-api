package cwms.radar.data.dao;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.RatingAdapter.toDTO;
import static org.jooq.impl.DSL.field;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.Controllers;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.rating.AbstractRatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadataList;
import cwms.radar.data.dto.rating.RatingSpec;
import hec.data.RatingException;
import hec.data.cwmsRating.AbstractRating;
import hec.data.cwmsRating.RatingSet;
import hec.data.cwmsRating.io.RatingXmlCompatUtil;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SelectConditionStep;
import org.jooq.SelectForUpdateStep;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_TRANSITIONAL_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_RATING;

public class RatingMetadataDao extends JooqDao<RatingSpec> {
    private static final Logger logger = Logger.getLogger(RatingMetadataDao.class.getName());

    public static final String EMPTY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ratings xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
            + "xsi:noNamespaceSchemaLocation=\"https://www.hec.usace.army"
            + ".mil/xmlSchema/cwms/Ratings.xsd\"/>";

    private final MetricRegistry metrics;

    public RatingMetadataDao(DSLContext dsl, MetricRegistry metrics) {
        super(dsl);
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public RatingMetadataList retrieve(String cursor, int pageSize, String office,
                                       String specIdMask, ZonedDateTime start,
                                       ZonedDateTime end) {
        int offset = 0;
        if (cursor != null && !cursor.isEmpty()) {
            String[] parts = CwmsDTOPaginated.decodeCursor(cursor);

            if (parts.length >= 2) {
                offset = Integer.parseInt(parts[0]);
                pageSize = Integer.parseInt(parts[parts.length - 1]);
            }
        }
        int finalOffset = offset;
        int finalPageSize = pageSize;

        return retrieve(office, specIdMask, start, end, finalOffset, finalPageSize);
    }

    private RatingMetadataList retrieve(String office, String specIdMask, ZonedDateTime start,
                                        ZonedDateTime end, final int offset, final int pageSize) {
        metrics.histogram(name(RatingMetadataDao.class, "retrieve", "pageSize"))
                .update(pageSize);
        metrics.histogram(name(RatingMetadataDao.class, "retrieve", "offset"))
                .update(offset);
        try (final Timer.Context ignored = markAndTime("retrieve")) {
            Set<String> ratingIds = getRatingIds(office, specIdMask, offset, pageSize);

            Map<RatingSpec, Set<AbstractRatingMetadata>> map =
                    getRatingsForIds(office, ratingIds, start, end);

            RatingMetadataList.Builder builder = new RatingMetadataList.Builder(pageSize);
            boolean isLastPage = ratingIds.size() < pageSize; // Not entirely certain about this...

            List<RatingMetadata> metadata = map.entrySet().stream()
                    .map(entry -> {
                        RatingMetadata.Builder rmBuilder = new RatingMetadata.Builder();
                        rmBuilder.withRatingSpec(entry.getKey());
                        rmBuilder.withRatings(entry.getValue());
                        return rmBuilder.build();
                    })
                    .collect(Collectors.toList());
            map.forEach((spec, ratings) -> {
                builder.withMetadata(metadata);
                builder.withOffset(offset);
                builder.withIsLastPage(isLastPage);
            });

            return builder.build();
        }
    }


    @NotNull
    public Set<String> getRatingIds(String office, String templateIdMask, int offset, int limit) {
        AV_RATING ratView = AV_RATING.AV_RATING;
        AV_VIRTUAL_RATING virtView = AV_VIRTUAL_RATING.AV_VIRTUAL_RATING;
        AV_TRANSITIONAL_RATING transView = AV_TRANSITIONAL_RATING.AV_TRANSITIONAL_RATING;

        try (final Timer.Context ignored = markAndTime("getRatingIds")) {
            Condition condition = DSL.trueCondition();
            Condition virtCondition = DSL.trueCondition();
            Condition transCondition = DSL.trueCondition();

            if (office != null) {
                condition = condition.and(ratView.OFFICE_ID.eq(office));
                virtCondition = virtCondition.and(virtView.OFFICE_ID.eq(office));
                transCondition = transCondition.and(transView.OFFICE_ID.eq(office));
            }

            if (templateIdMask != null) {
                condition =
                        condition.and(JooqDao.caseInsensitiveLikeRegex(ratView.RATING_ID,
                                templateIdMask));
                virtCondition =
                        virtCondition.and(JooqDao.caseInsensitiveLikeRegex(virtView.RATING_SPEC,
                                templateIdMask));
                transCondition =
                        transCondition.and(JooqDao.caseInsensitiveLikeRegex(transView.RATING_SPEC,
                                templateIdMask));
            }

            Field<String> idField = field("RATING_ID", String.class);

            SelectConditionStep<Record2<String, String>> ratingStep = dsl.select(
                            ratView.OFFICE_ID,
                            ratView.RATING_ID.as(idField))
                    .from(ratView)
                    .where(condition);

            SelectConditionStep<Record2<String, String>> virtStep = dsl.select(
                    virtView.OFFICE_ID,
                            virtView.RATING_SPEC.as(idField))
                    .from(virtView)
                    .where(virtCondition);

            SelectConditionStep<Record2<String, String>> transStep = dsl.select(
                    transView.OFFICE_ID,
                            transView.RATING_SPEC.as(idField))
                    .from(transView)
                    .where(transCondition);

            SelectForUpdateStep<Record1<String>> query = dsl.selectDistinct(idField)
                    .from(ratingStep.union(virtStep).union(transStep))
                    .orderBy(idField.asc())
                    .limit(limit)
                    .offset(offset);

//        logger.info(() -> query.getSQL(ParamType.INLINED));

            return new LinkedHashSet<>(query.fetch(idField));
        }
    }

    @NotNull
    public Map<RatingSpec, Set<AbstractRatingMetadata>> getRatingsForIds(
            String office, Set<String> ratingIds, ZonedDateTime start, ZonedDateTime end) {
        try (final Timer.Context ignored = markAndTime("getRatingsForIds")) {
            Map<hec.data.cwmsRating.RatingSpec, Set<AbstractRating>> retval;

            boolean useParallel = true;
            Stream<Map<hec.data.cwmsRating.RatingSpec, Set<AbstractRating>>> mapStream;
            if (useParallel) {
                mapStream = ratingIds.stream()
                        .map(ratingId -> CompletableFuture.supplyAsync(() ->
                                retrieveRatings(office, ratingId, start, end)))
                        .collect(Collectors.toList())
                        .stream()
                        .map(CompletableFuture::join);
            } else {
                // for 100 rating ids this took 39 sec. for 50 it took 22s.
                // Each ratingId fetch took 300-400 ms
                // constructing the objects from the fetched xml took 0-100ms each but usually 1ms
                mapStream = ratingIds.stream()
                        .map(ratingId -> retrieveRatings(office, ratingId, start, end));
            }

            // merge stream of maps into one map
            retval = mapStream
                    .reduce(new LinkedHashMap<>(), (map1, map2) -> {
                        map2.forEach((key, value) -> map1.merge(key, value, (set1, set2) -> {
                            set1.addAll(set2);
                            return set1;
                        }));
                        return map1;
                    });


            return  toDTO(retval);
        }
    }

    public Map<hec.data.cwmsRating.RatingSpec, Set<AbstractRating>> retrieveRatings(
            String office, String templateIdMask, ZonedDateTime start, ZonedDateTime end) {
        try (final Timer.Context ignored = markAndTime("retrieveRatings")) {
            Configuration configuration = dsl.configuration();
            String effectiveTw = "F";
            String specIdMask = templateIdMask;
            Timestamp startDate = null;
            if(start != null) {
                startDate = Timestamp.from(start.toInstant());
            }

            Timestamp endDate = null;
            if(end != null) {
                endDate = Timestamp.from(end.toInstant());
            }
            String timeZone = "UTC";
            // We dont want the templates or the specs but if we don't retrieve them with the
            // ratings then RatingSet.fromXml won't parse the output.
            Boolean retrieveTemplates = true;
            Boolean retrieveSpecs = true;
            Boolean retrieveRatings = true;
            Boolean recurse = true;
            // Each rating could potentially be megabytes in size - Don't include the points.
            String includePoints = "F";

            String xmlText = CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_XML_DATA(configuration,
                    effectiveTw, specIdMask, startDate, endDate, timeZone,
                    retrieveTemplates, retrieveSpecs, retrieveRatings,
                    recurse, includePoints, office);

            // remember that sometime the xmlText comes back as an empty xml doc like EMPTY
            // when that happens we can't build a spec to use as a key.
            return getRatingsFromXml(xmlText);
        }
    }


    @NotNull
    public Map<hec.data.cwmsRating.RatingSpec, Set<AbstractRating>> getRatingsFromXml(String xmlText) {
        try (final Timer.Context ignored = markAndTime("getRatingsFromXml")) {
            Map<hec.data.cwmsRating.RatingSpec, Set<AbstractRating>> retval = new LinkedHashMap<>();

            if (xmlText != null) {
                xmlText = xmlText.trim();
                if (xmlText.length() >= 200 || !EMPTY.equals(xmlText)) {
                    try {
                        RatingSet ratingSet = RatingXmlCompatUtil.getInstance().createRatingSet(xmlText);
                        if (ratingSet != null) {
                            hec.data.cwmsRating.RatingSpec spec = ratingSet.getRatingSpec();
                            if (spec != null) {
                                // even if there are no ratings, we still want to return the spec
                                Set<AbstractRating> ratingsForSpec = retval.computeIfAbsent(spec,
                                            k -> new LinkedHashSet<>());
                                AbstractRating[] ratings = ratingSet.getRatings();
                                if (ratings != null && ratings.length > 0) {
                                    for (AbstractRating rating : ratings) {
                                        if (rating != null) {
                                            ratingsForSpec.add(rating);
                                        }
                                    }
                                }
                            } else {
                                logger.warning("RatingSpec was null for xmlText:" + xmlText);
                            }
                        }
                    } catch (RatingException e) {
                        logger.log(Level.WARNING, "Could not parse xml: " + xmlText, e);
                    }
                }
            }
            return retval;
        }
    }


}
