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

package cwms.cda.data.dao;

import static com.google.common.flogger.LazyArgs.lazy;

import static cwms.cda.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.rating.RatingEffectiveDatesMap;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.data.dto.rating.RatingSpecEffectiveDates;
import cwms.cda.data.dto.rating.RatingSpecs;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.flogger.FluentLogger;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import cwms.cda.formatters.FormattingException;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

public class RatingSpecDao extends JooqDao<RatingSpec> {
    public static final Calendar GMT_CALENDAR = getGmtCalendar();
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String OFFICE_ID = "OFFICE_ID";
    public static final String SPECIFICATION_ID = "SPECIFICATION_ID";
    public static final String LOCATION_ID = "LOCATION_ID";
    public static final String VERSION = "VERSION";
    public static final String EFFECTIVE_DATE = "EFFECTIVE_DATE";
    public static final String CREATE_DATE = "CREATE_DATE";

    private static final List<String> RATINGS_COLUMN_LIST = sortedUpperList(
            OFFICE_ID,
            SPECIFICATION_ID,
            EFFECTIVE_DATE,
            CREATE_DATE
    );

    private static List<String> sortedUpperList(String... items) {
        return Arrays.stream(items)
                .sorted()
                .collect(toList());
    }

    private static Calendar getGmtCalendar() {
        Calendar retVal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        retVal.set(Calendar.MILLISECOND, 0); //this is what the OracleTypeMap does, without this we get back some millisecond offset that is incorrect
        return retVal;
    }

    public RatingSpecDao(DSLContext dsl) {
        super(dsl);
    }

    @NotNull
    public RatingSpecs retrieveRatingSpecs(String cursor, int pageSize, String office, String specIdMask) {
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
                        logger.atInfo().log("Could not parse %s", parts[1]);
                    }
                }
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
        AV_RATING ratView = AV_RATING.AV_RATING;

        // Conditions that define WHICH SPECS match
        Condition specCondition = specView.TEMPLATE_ID.notLike("%Stage-Offset%")
            .and(specView.TEMPLATE_ID.notLike("%Stage-Shift%"))
            .and(specView.ALIASED_ITEM.isNull());

        if (office != null) {
            specCondition = specCondition.and(specView.OFFICE_ID.eq(office.toUpperCase()));
        }

        if (specIdMask != null) {
            Condition maskRegex = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID, specIdMask);
            specCondition = specCondition.and(maskRegex);
        }

        if (total == null) {
            total = dsl.fetchCount(specView, specCondition);
        }

        var specPage = dsl
            .select(
                specView.RATING_SPEC_CODE,
                specView.OFFICE_ID, specView.RATING_ID, specView.DATE_METHODS,
                specView.TEMPLATE_ID, specView.LOCATION_ID, specView.VERSION,
                specView.SOURCE_AGENCY, specView.ACTIVE_FLAG, specView.AUTO_UPDATE_FLAG,
                specView.AUTO_ACTIVATE_FLAG, specView.AUTO_MIGRATE_EXT_FLAG,
                specView.IND_ROUNDING_SPECS, specView.DEP_ROUNDING_SPEC,
                specView.DESCRIPTION, specView.ALIASED_ITEM
            )
            .from(specView)
            .where(specCondition)
            .orderBy(specView.OFFICE_ID, specView.RATING_ID)
            .limit(pageSize)
            .offset(offset)
            .asTable("spec_page");

        Field<Long> spSpecCode = specPage.field(specView.RATING_SPEC_CODE);
        Field<String> spOfficeId = specPage.field(specView.OFFICE_ID);
        Field<String> spRatingId = specPage.field(specView.RATING_ID);

        Field<List<ZonedDateTime>> effectiveDates =
            DSL.multiset(
                    dsl.select(ratView.EFFECTIVE_DATE)
                       .from(ratView)
                       .where(ratView.RATING_SPEC_CODE.eq(spSpecCode))
                       .and(ratView.ALIASED_ITEM.isNull())
                       .orderBy(ratView.EFFECTIVE_DATE)
                )
               .convertFrom(r ->
                   r.getValues(ratView.EFFECTIVE_DATE).stream()
                    .map(RatingSpecDao::toZdt)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList())
               )
               .as("effective_dates");

        ResultQuery<? extends Record> query = dsl.select(
                spSpecCode,
                spOfficeId,
                spRatingId,
                specPage.field(specView.DATE_METHODS),
                specPage.field(specView.TEMPLATE_ID),
                specPage.field(specView.LOCATION_ID),
                specPage.field(specView.VERSION),
                specPage.field(specView.SOURCE_AGENCY),
                specPage.field(specView.ACTIVE_FLAG),
                specPage.field(specView.AUTO_UPDATE_FLAG),
                specPage.field(specView.AUTO_ACTIVATE_FLAG),
                specPage.field(specView.AUTO_MIGRATE_EXT_FLAG),
                specPage.field(specView.IND_ROUNDING_SPECS),
                specPage.field(specView.DEP_ROUNDING_SPEC),
                specPage.field(specView.DESCRIPTION),
                specPage.field(specView.ALIASED_ITEM),
                effectiveDates
            )
            .from(specPage)
            .orderBy(spOfficeId, spRatingId)
            .fetchSize(DEFAULT_FETCH_SIZE);

        logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

        List<RatingSpec> specs = query.fetch()
            .stream()
            .map(rec -> {
                RatingSpec template = buildRatingSpec(rec);
                List<ZonedDateTime> dates = rec.get(effectiveDates);
                return new RatingSpec.Builder()
                    .fromRatingSpec(template)
                    .withEffectiveDates(dates == null ? List.of() : dates)
                    .build();
            })
            .collect(toList());

        RatingSpecs.Builder builder = new RatingSpecs.Builder(offset, pageSize, total);
        builder.withSpecs(specs);
        return builder.build();
    }


        public Optional<RatingSpec> retrieveRatingSpec(String office, String specId) {
            RatingSpecs ratingSpecs = retrieveRatingSpecs(null, 1, office, specId);
            List<RatingSpec> specs = ratingSpecs.getSpecs();
            if(specs.size() > 1) {
                throw new IllegalStateException("More than one rating spec found for specId: " + specId);
            } else if(specs.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(specs.get(0));
            }
        }

    public static ZonedDateTime toZdt(final Timestamp time) {
        if (time != null) {
            return ZonedDateTime.ofInstant(time.toInstant(), ZoneId.of("UTC"));
        } else {
            return null;
        }
    }

    public static RatingSpec buildRatingSpec(Record rec) {
        RatingSpec retVal = null;

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

            retVal = new RatingSpec.Builder()
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

        return retVal;
    }


    public void delete(String office, DeleteMethod deleteMethod, String ratingSpecId) {
        String deleteAction;
        switch (deleteMethod) {
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
                        getDslContext(c, office).configuration(),
                        ratingSpecId,
                        deleteAction,
                        office)
        );
    }

    public void create(RatingSpec spec, boolean failIfExists) {
        String xml = null;
        try {
            xml = RatingSpecXmlUtils.toPlSqlXml(spec);
            create(xml, failIfExists);
        } catch (JsonProcessingException ex) {
            String msg = spec != null ?
                    "Error rendering '" + spec + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.atWarning().withCause(ex).log(msg);
            throw new FormattingException(msg, ex);
        }
    }

    // In my tests this method wouldn't fail if the input was
    // mostly right, it just wouldn't create anything.
    public void create(String xml, boolean failIfExists) {
        final String office = RatingDao.extractOfficeFromXml(xml);
        dsl.connection(c ->
                CWMS_RATING_PACKAGE.call_STORE_SPECS__3(
                        getDslContext(c, office).configuration(),
                        xml,
                        formatBool(failIfExists))
        );
    }

    /**
     * Retrieve effective dates for specs matching the specIdMask and officeIdMask within the given date range.
     * NOTE: This makes a separate query to get the list of non-aliased spec ids for the officeIdMask, so that aliased specs can be skipped.
     */
    public RatingEffectiveDatesMap retrieveSpecEffectiveDates(String officeIdMask, String specIdMask, Instant begin, Instant end) {
        //set of non-alias spec ids used to filter out aliased specs
        Map<String, List<String>> officeToRatingIdsNoAliasesMap = getRatingIds(officeIdMask, "*", false);
        return connectionResult(dsl, conn -> {
            //office->spec->dates
            NavigableMap<String, NavigableMap<String, NavigableSet<Instant>>> specDateMap = new TreeMap<>();
            //instantiate empty Instant list for each office/spec combination so that specs with no effective dates are included in the final result
            for (Map.Entry<String, List<String>> entry : officeToRatingIdsNoAliasesMap.entrySet()) {
                String officeId = entry.getKey();
                List<String> specIds = entry.getValue();
                NavigableMap<String, NavigableSet<Instant>> specMap = specDateMap.computeIfAbsent(officeId, k -> new TreeMap<>());
                for (String specId : specIds) {
                    specMap.put(specId, new TreeSet<>());
                }
            }
            try (ResultSet rs = catRatings(conn, officeIdMask, specIdMask, begin, end)) {
                checkMetaData(rs.getMetaData(), RATINGS_COLUMN_LIST, "Ratings");
                while (rs.next()) {
                    String officeId = rs.getString(OFFICE_ID);
                    String specId = rs.getString(SPECIFICATION_ID);
                    List<String> ratingIdsNoAliases = officeToRatingIdsNoAliasesMap.get(officeId);
                    if (ratingIdsNoAliases != null && !ratingIdsNoAliases.contains(specId)) { // skip aliased specs based on queried list of rating ids not including aliases
                        continue;
                    }
                    Timestamp timestamp = rs.getTimestamp(EFFECTIVE_DATE, GMT_CALENDAR);
                    Instant date = timestamp.toInstant();
                    NavigableSet<Instant> dateList = specDateMap.computeIfAbsent(officeId, k -> new TreeMap<>())
                            .computeIfAbsent(specId, k -> new TreeSet<>());
                    dateList.add(date);
                }
                return buildRatingEffectiveDatesMap(specDateMap);
            }
        });
    }

    //package scoped for unit testing
    static RatingEffectiveDatesMap buildRatingEffectiveDatesMap(NavigableMap<String, NavigableMap<String, NavigableSet<Instant>>> specDateMap) {
        Map<String, List<RatingSpecEffectiveDates>> officeToSpecDatesMap = new LinkedHashMap<>(specDateMap.size());
        for (Map.Entry<String, NavigableMap<String, NavigableSet<Instant>>> entry : specDateMap.entrySet()) {
            String officeId = entry.getKey();
            List<RatingSpecEffectiveDates> specEffectiveDatesForOffice = new ArrayList<>();
            NavigableMap<String, NavigableSet<Instant>> specMap = entry.getValue();
            for (Map.Entry<String, NavigableSet<Instant>> specEntry : specMap.entrySet()) {
                String specId = specEntry.getKey();
                NavigableSet<Instant> dateList = specEntry.getValue();
                if (dateList.isEmpty()) {
                    continue; // skip empty specs
                }
                RatingSpecEffectiveDates datesForSpec = new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId(specId)
                        .withEffectiveDates(dateList)
                        .build();
                specEffectiveDatesForOffice.add(datesForSpec);
            }
            officeToSpecDatesMap.put(officeId, specEffectiveDatesForOffice);
        }
        return new RatingEffectiveDatesMap.Builder()
                .withOfficeToSpecDates(officeToSpecDatesMap)
                .build();
    }

    private ResultSet catRatings(Connection conn, String officeIdMask, String specIdMask, Instant begin, Instant end) throws SQLException {

        Timestamp pEffectiveDateStart = begin == null ? null : Timestamp.from(begin);
        Timestamp pEffectiveDateEnd = end == null ? null : Timestamp.from(end);

        // This object does not need to be closed, it eagerly fetches the data from the statement.
        CachedRowSet output = RowSetProvider.newFactory()
                .createCachedRowSet();

        try (CallableStatement statement = conn.prepareCall("{CALL CWMS_20.CWMS_RATING.CAT_RATINGS(?, ?, ?, ?, ?, ?)}")) {
            statement.registerOutParameter(1, Types.REF_CURSOR);
            statement.setString(2, specIdMask);
            statement.setTimestamp(3, pEffectiveDateStart, GMT_CALENDAR);
            statement.setTimestamp(4, pEffectiveDateEnd, GMT_CALENDAR);
            statement.setString(5, "UTC");
            statement.setString(6, officeIdMask);
            statement.execute();

            //The result set in the statement is closed when the statement is closed
            output.populate(statement.getObject(1, ResultSet.class));
        }

        return output;
    }

    private Map<String, List<String>> getRatingIds(String office, String templateIdMask, boolean includeAliases) {
        return connectionResult(dsl, conn -> {
            AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
            Condition condition = DSL.noCondition();

            if (office != null && !office.isEmpty() && !office.equals("*")) {
                condition = condition.and(specView.OFFICE_ID.eq(office));
            }

            if (templateIdMask != null) {
                Condition ratingIdLike = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID,
                        templateIdMask);
                condition = condition.and(ratingIdLike);
            }

            if (!includeAliases) {
                condition = condition.and(specView.ALIASED_ITEM.isNull());
            }

            Field<String> officeField = specView.OFFICE_ID;
            Field<String> idField = specView.RATING_ID;

            return DSL.using(conn)
                    .selectDistinct(officeField, idField)
                    .from(specView)
                    .where(condition)
                    .orderBy(officeField, idField)
                    .fetchGroups(officeField, idField);
        });
    }
}
