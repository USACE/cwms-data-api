/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LocationToPublishedData;
import cwms.cda.data.dto.LocationToPublishedDataList;
import cwms.cda.data.dto.PublishedTimeSeriesData;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static java.util.stream.Collectors.toList;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.upper;
import static usace.cwms.db.jooq.codegen.tables.AV_A2W_TS_CODES_BY_LOC.AV_A2W_TS_CODES_BY_LOC;
import static usace.cwms.db.jooq.codegen.tables.AV_A2W_TS_CODES_BY_LOC2.AV_A2W_TS_CODES_BY_LOC2;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID.AV_CWMS_TS_ID;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC2.AV_LOC2;

public final class PublishedTimeSeriesDao extends JooqDao<TimeSeriesProfile> {

    public PublishedTimeSeriesDao(DSLContext dsl) {
        super(dsl);
    }

    public LocationToPublishedDataList retrievePublishedTimeSeriesIds(PublishedRetrievalParameters retrievalParameters, String cursor, int pageSize)
    {
        int total = 0;
        String cursorOffice = null;
        String cursorLocId = null;

        Condition officeCondition = caseInsensitiveLikeRegexNullTrue(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID, retrievalParameters.getOfficeId().orElse("*"));
        Condition locationCondition = caseInsensitiveLikeRegexNullTrue(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID, retrievalParameters.getLocationId().orElse("*"));

        Condition whereClause = locationCondition.and(officeCondition);

        if (cursor == null || cursor.isEmpty()) {
            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                    .from(AV_A2W_TS_CODES_BY_LOC2)
                    .leftOuterJoin(AV_CWMS_TS_ID)
                    .on(AV_A2W_TS_CODES_BY_LOC2.TS_CODE.eq(AV_CWMS_TS_ID.TS_CODE.coerce(AV_A2W_TS_CODES_BY_LOC2.TS_CODE)))
                    .where(whereClause);// Ensures rows matching cwmsIds are returned
            Record1<Integer> rec = count.fetchOne();
            if(rec != null) {
                total = rec.value1();
            }
        } else {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            if (parts.length > 1) {
                cursorOffice = LocationToPublishedDataList.getOffice(cursor);
                cursorLocId = LocationToPublishedDataList.getId(cursor);
                total = Integer.parseInt(parts[1]);
                pageSize = Integer.parseInt(parts[2]);
            }
        }
        int finalizedPageSize = pageSize;
        int finalizedTotal = total;

        Condition moreInSameOffice = cursorLocId == null || cursorOffice == null ? noCondition() :
                AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID.eq(cursorOffice.toUpperCase())
                        .and(upper(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID).greaterThan(cursorLocId.toUpperCase()));
        Condition nextOffices = cursorOffice == null ? noCondition():
                upper(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID).greaterThan(cursorOffice.toUpperCase());
        Condition pagingCondition = moreInSameOffice.or(nextOffices);

        Map<InsensitiveCwmsId, Map<String, PublishedTimeSeriesData>> locationToTsParameterMap = new LinkedHashMap<>();
        Map<InsensitiveCwmsId, String> locationKindMap = new LinkedHashMap<>();
        Map<InsensitiveCwmsId, String> locationBoundingOfficeMap = new LinkedHashMap<>();

        return connectionResult(dsl, conn -> {
            DSLContext ctx = getDslContext(conn, null);
            ctx.select(AV_A2W_TS_CODES_BY_LOC2.CWMS_TS_ID, AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID,
                            AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID, AV_CWMS_TS_ID.TIME_ZONE_ID,
                            AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET, AV_A2W_TS_CODES_BY_LOC2.TS_TYPE,
                            AV_LOC2.LOCATION_KIND_ID, AV_LOC2.BOUNDING_OFFICE_ID,
                            AV_A2W_TS_CODES_BY_LOC.DATE_REFRESHED, AV_A2W_TS_CODES_BY_LOC.NOTES,
                            AV_LOC2.ACTIVE_FLAG)
                    .from(AV_A2W_TS_CODES_BY_LOC2)
                    .leftOuterJoin(AV_CWMS_TS_ID)
                    .on(AV_A2W_TS_CODES_BY_LOC2.TS_CODE.eq(AV_CWMS_TS_ID.TS_CODE.coerce(AV_A2W_TS_CODES_BY_LOC2.TS_CODE)))
                    .leftOuterJoin(AV_LOC2)
                    .on(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID.eq(AV_LOC2.LOCATION_ID)
                            .and(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID.eq(AV_LOC2.DB_OFFICE_ID))
                            .and(AV_LOC2.UNIT_SYSTEM.eq("EN")))
                    .leftOuterJoin(AV_A2W_TS_CODES_BY_LOC)
                    .on(AV_A2W_TS_CODES_BY_LOC2.LOCATION_CODE.eq(AV_A2W_TS_CODES_BY_LOC.LOCATION_CODE.coerce(AV_A2W_TS_CODES_BY_LOC2.LOCATION_CODE)))
                    .where(whereClause)
                    .and(pagingCondition)
                    .orderBy(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID, AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID)
                    .limit(finalizedPageSize)
                    .fetch()
                    .forEach(row -> {
                        String locationId = row.get(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID);
                        String tsParameterFromRow = row.get(AV_A2W_TS_CODES_BY_LOC2.TS_TYPE);
                        String officeId = row.get(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID);
                        String kind = row.get(AV_LOC2.LOCATION_KIND_ID);
                        String boundingOfficeId = row.get(AV_LOC2.BOUNDING_OFFICE_ID);
                        Timestamp refreshDate = row.get(AV_A2W_TS_CODES_BY_LOC.DATE_REFRESHED);
                        String notes = row.get(AV_A2W_TS_CODES_BY_LOC.NOTES);

                        PublishedTimeSeriesData tsId = buildTsId(row, refreshDate, notes);
                        InsensitiveCwmsId key = new InsensitiveCwmsId(new CwmsId.Builder()
                                .withOfficeId(officeId)
                                .withName(locationId)
                                .build());
                        locationToTsParameterMap.computeIfAbsent(key, k -> new LinkedHashMap<>()).put(tsParameterFromRow, tsId);
                        locationKindMap.putIfAbsent(key, kind);
                        locationBoundingOfficeMap.putIfAbsent(key, boundingOfficeId);
                    });

            List<LocationToPublishedData> identifiersList = locationToTsParameterMap.entrySet().stream()
                    .map(entry -> new LocationToPublishedData.Builder()
                            .withLocationId(entry.getKey().getCwmsId())
                            .withPublishedTimesSeries(entry.getValue())
                            .withKind(locationKindMap.get(entry.getKey()))
                            .withBoundingOfficeId(locationBoundingOfficeMap.get(entry.getKey()))
                            .build())
                    .collect(toList());

            return new LocationToPublishedDataList.Builder()
                    .withCursor(cursor)
                    .withTotal(finalizedTotal)
                    .withPageSize(finalizedPageSize)
                    .withLocationToPublishedData(identifiersList)
                    .build();
        });

    }

    private PublishedTimeSeriesData buildTsId(Record row, Timestamp refreshDate, String notes)
    {
        String timeSeriesId = row.get(AV_A2W_TS_CODES_BY_LOC2.CWMS_TS_ID);
        String officeId = row.get(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID);
        BigDecimal intervalUtcMinuteOffset = row.get(AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET);
        String timeZoneId = row.get(AV_CWMS_TS_ID.TIME_ZONE_ID);
        boolean active = parseBool(row.get(AV_LOC2.ACTIVE_FLAG));

        return new PublishedTimeSeriesData.Builder()
                .withTimeSeriesId(new CwmsId.Builder()
                        .withOfficeId(officeId)
                        .withName(timeSeriesId)
                        .build())
                .withTimezoneName(timeZoneId)
                .withIntervalOffsetMinutes(intervalUtcMinuteOffset == null ? null : intervalUtcMinuteOffset.intValue())
                .withActive(active)
                .withDateRefreshed(refreshDate == null ? null : refreshDate.toInstant())
                .withNotes(notes)
                .build();
    }

    //building inner class that wraps CwmsId but implements equals hashcode called InsensitiveCwmsId
    private static class InsensitiveCwmsId {
        private final CwmsId cwmsId;

        public InsensitiveCwmsId(CwmsId cwmsId) {
            this.cwmsId = cwmsId;
        }

        private CwmsId getCwmsId() {
            return cwmsId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InsensitiveCwmsId that = (InsensitiveCwmsId) o;
            return cwmsId.getName().equalsIgnoreCase(that.cwmsId.getName()) &&
                    cwmsId.getOfficeId().equalsIgnoreCase(that.cwmsId.getOfficeId());
        }

        @Override
        public int hashCode() {
            return cwmsId.getName().toUpperCase().hashCode() + cwmsId.getOfficeId().toUpperCase().hashCode();
        }
    }

}
