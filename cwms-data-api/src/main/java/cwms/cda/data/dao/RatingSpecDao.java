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

import cwms.cda.data.dto.rating.RatingEffectiveDatesMap;
import static cwms.cda.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;

import cwms.cda.data.dto.CwmsDTOPaginated;
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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import java.util.stream.Stream;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectForUpdateStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import static org.jooq.impl.DSL.field;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_RATING;
import usace.cwms.db.jooq.codegen.tables.AV_RATING_SPEC;

public class RatingSpecDao extends JooqDao<RatingSpec> {
    public static final Calendar GMT_CALENDAR = getGmtCalendar();
    private static final Logger logger = Logger.getLogger(RatingSpecDao.class.getName());
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
                .fetchSize(DEFAULT_FETCH_SIZE);

        logger.fine(() -> query.getSQL(ParamType.INLINED));

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
        Set<RatingSpec> retVal;

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

        logger.fine(() -> query.getSQL(ParamType.INLINED));

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

        retVal = map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return retVal;
    }


    public Optional<RatingSpec> retrieveRatingSpec(String office, String specId) {
        Set<RatingSpec> retVal;

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
                .fetchSize(DEFAULT_FETCH_SIZE);

        logger.fine(() -> query.getSQL(ParamType.INLINED));

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

        retVal = map.entrySet().stream()
                .map(entry -> new RatingSpec.Builder()
                        .fromRatingSpec(entry.getKey())
                        .withEffectiveDates(entry.getValue())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // There should only be one key in the map
        if (retVal.size() > 1) {
            throw new IllegalStateException("More than one rating spec found for id: " + specId);
        }

        return retVal.stream().findFirst();
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
                formatBool(failIfExists))
        );
    }

    /**
     * Retrieve effective dates for specs matching the specIdMask and officeIdMask within the given date range.
     * NOTE: This makes a separate query to get the list of non-aliased spec ids for the officeIdMask, so that aliased specs can be skipped.
     */
    public RatingEffectiveDatesMap retrieveSpecEffectiveDates(String officeIdMask, String specIdMask, Instant begin, Instant end) {
        //set of non-alias spec ids used to filter out aliased specs
        Set<String> ratingIdsNoAliases = getRatingIds(officeIdMask, "*", false);
        return connectionResult(dsl, conn -> {
            //office->spec->dates
            NavigableMap<String, NavigableMap<String, NavigableSet<Instant>>> specDateMap = new TreeMap<>();
            ResultSet rs = catRatings(conn, officeIdMask, specIdMask, begin, end);
            OracleTypeMap.checkMetaData(rs.getMetaData(), RATINGS_COLUMN_LIST, "Ratings");
            while(rs.next()) {
                String officeId = rs.getString(OFFICE_ID);
                String specId = rs.getString(SPECIFICATION_ID);
                if(!ratingIdsNoAliases.contains(specId)) { // skip aliased specs based on queried list of rating ids not including aliases
                    continue;
                }
                Timestamp timestamp = rs.getTimestamp(EFFECTIVE_DATE, GMT_CALENDAR);
                Instant date = timestamp.toInstant();
                NavigableSet<Instant> dateList = specDateMap.computeIfAbsent(officeId, k -> new TreeMap<>())
                        .computeIfAbsent(specId, k -> new TreeSet<>());
                dateList.add(date);
            }
            return buildRatingEffectiveDatesMap(specDateMap);
        });
    }

    //package scoped for unit testing
    static RatingEffectiveDatesMap buildRatingEffectiveDatesMap(NavigableMap<String, NavigableMap<String, NavigableSet<Instant>>> specDateMap) {
        Map<String, List<RatingSpecEffectiveDates>> officeToSpecDatesMap = new LinkedHashMap<>(specDateMap.size());
        for(Map.Entry<String, NavigableMap<String, NavigableSet<Instant>>> entry : specDateMap.entrySet()) {
            String officeId = entry.getKey();
            List<RatingSpecEffectiveDates> specEffectiveDatesForOffice = new ArrayList<>();
            NavigableMap<String, NavigableSet<Instant>> specMap = entry.getValue();
            for(Map.Entry<String, NavigableSet<Instant>> specEntry : specMap.entrySet()) {
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

        try (CallableStatement statement = conn.prepareCall("{CALL CWMS_20.CWMS_RATING.CAT_RATINGS(?, ?, ?, ?, ?, ?)}"))
        {
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

    private Set<String> getRatingIds(String office, String templateIdMask, boolean includeAliases) {
        return connectionResult(dsl, conn ->
        {
            AV_RATING_SPEC specView = AV_RATING_SPEC.AV_RATING_SPEC;
            Condition condition = DSL.noCondition();

            if (office != null) {
                condition = condition.and(specView.OFFICE_ID.eq(office));
            }

            if (templateIdMask != null) {
                Condition ratingIdLike = JooqDao.caseInsensitiveLikeRegex(specView.RATING_ID,
                        templateIdMask);
                condition = condition.and(ratingIdLike);
            }

            if(!includeAliases) {
                condition = condition.and(specView.ALIASED_ITEM.isNull());
            }

            Field<String> idField = field("RATING_ID", String.class);

            SelectConditionStep<Record2<String, String>> ratingStep = DSL.using(conn).select(
                            specView.OFFICE_ID,
                            specView.RATING_ID.as(idField))
                    .from(specView)
                    .where(condition);

            SelectForUpdateStep<Record1<String>> query = DSL.using(conn).selectDistinct(idField)
                    .from(ratingStep)
                    .orderBy(idField.asc());
            return new LinkedHashSet<>(query.fetch(idField));
        });

    }
}
