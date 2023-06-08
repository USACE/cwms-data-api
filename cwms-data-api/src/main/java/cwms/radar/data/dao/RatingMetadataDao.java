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
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.*;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import static org.jooq.impl.DSL.field;

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
        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;

        try (final Timer.Context ignored = markAndTime("getRatingIds")) {
            Condition condition = DSL.trueCondition();
            Condition virtCondition = DSL.trueCondition();
            Condition transCondition = DSL.trueCondition();

            if (office != null) {
                condition = condition.and(specView.OFFICE_ID.eq(office));
            }

            if (templateIdMask != null) {
                Condition ratingIdLike = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID,
                        templateIdMask);
                condition = condition.and(ratingIdLike);
            }

            Field<String> idField = field("RATING_ID", String.class);

            SelectConditionStep<Record2<String, String>> ratingStep = dsl.select(
                            specView.OFFICE_ID,
                            specView.RATING_ID.as(idField))
                    .from(specView)
                    .where(condition);

            SelectForUpdateStep<Record1<String>> query = dsl.selectDistinct(idField)
                    .from(ratingStep)
                    .orderBy(idField.asc())
                    .limit(limit)
                    .offset(offset);

            return new LinkedHashSet<>(query.fetch(idField));
        }
    }

    @NotNull
    public Map<RatingSpec, Set<AbstractRatingMetadata>> getRatingsForIds(
            String office, Set<String> ratingIds, ZonedDateTime start, ZonedDateTime end) {
        try (final Timer.Context ignored = markAndTime("getRatingsForIds")) {

            boolean useParallel = true;
            Stream<Map<RatingSpec, Set<AbstractRatingMetadata>>> mapStream;
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

            Map<RatingSpec, Set<AbstractRatingMetadata>> retval = new LinkedHashMap<>();

            mapStream.forEach(map -> map.forEach((spec, ratings) -> {
                Set<AbstractRatingMetadata> setForSpec = retval.computeIfAbsent(spec, s -> new LinkedHashSet<>());
                if(ratings != null) {
                    setForSpec.addAll(ratings);
                }
            }));

            return retval;
        }
    }

    @NotNull
    public Map<RatingSpec, Set<AbstractRatingMetadata>> retrieveRatings(
            String office, String templateIdMask, ZonedDateTime start, ZonedDateTime end) {
        Map<RatingSpec,Set<AbstractRatingMetadata>> retval = new LinkedHashMap<>();

        try (final Timer.Context ignored = markAndTime("retrieveRatings")) {
            RatingSpecDao ratingSpecDao = new RatingSpecDao(dsl);
            Optional<RatingSpec> spec = ratingSpecDao.retrieveRatingSpec(office, templateIdMask);

            if (spec.isPresent()) {
                RatingSet ratingSet = getRatingSet(office, templateIdMask, start, end);
                Set<AbstractRating> ratings = getAbstractRatings(ratingSet);

                retval.put(spec.get(), RatingAdapter.toDTO(ratings));
            }
            return retval;
        }
    }

    @Nullable
    private RatingSet getRatingSet(String office, String templateIdMask, ZonedDateTime start,
                                   ZonedDateTime end) {
        RatingSet retval;
        try (final Timer.Context ignored = markAndTime("getRatingSet")) {
            Configuration configuration = dsl.configuration();
            String effectiveTw = "F";
            String specIdMask = templateIdMask;
            Timestamp startDate = null;
            if (start != null) {
                startDate = Timestamp.from(start.toInstant());
            }

            Timestamp endDate = null;
            if (end != null) {
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

            // Sometimes the xmlText comes back as an empty xml doc like EMPTY
            retval = getRatingSetFromXml(xmlText);
        }
        return retval;
    }

    @Nullable
    private static Set<AbstractRating> getAbstractRatings(RatingSet ratingSet) {
        Set<AbstractRating> ratings = null;
        if (ratingSet != null) {
            AbstractRating[] abstractRatings = ratingSet.getRatings();
            if (abstractRatings != null) {
                ratings = new LinkedHashSet<>();
                for (AbstractRating rating : abstractRatings) {
                    if (rating != null) {
                        ratings.add(rating);
                    }
                }
            }
        }
        return ratings;
    }

    @Nullable
    public RatingSet getRatingSetFromXml(String xmlText) {
        RatingSet retval = null;
        try (final Timer.Context ignored = markAndTime("getRatingSetFromXml")) {
            if (xmlText != null) {
                xmlText = xmlText.trim();
                if (xmlText.length() >= 200 || !EMPTY.equals(xmlText)) {
                    try {
                        retval = RatingXmlFactory.ratingSet(xmlText);
                    } catch (RatingException e) {
                        logger.log(Level.WARNING, "Could not parse xml: " + xmlText, e);
                    }
                }
            }
            return retval;
        }
    }

}
