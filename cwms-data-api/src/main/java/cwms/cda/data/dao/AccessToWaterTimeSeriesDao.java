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
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.data.dto.TimeSeriesIdentifiersByType;
import cwms.cda.data.dto.TimeSeriesIdentifiersByTypeList;
import cwms.cda.data.dto.TimeSeriesMetaData;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static java.util.stream.Collectors.toList;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.SelectConditionStep;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.upper;
import static usace.cwms.db.jooq.codegen.tables.AV_A2W_TS_CODES_BY_LOC2.AV_A2W_TS_CODES_BY_LOC2;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID.AV_CWMS_TS_ID;

public final class AccessToWaterTimeSeriesDao extends JooqDao<TimeSeriesProfile> {

    public AccessToWaterTimeSeriesDao(DSLContext dsl) {
        super(dsl);
    }

    public TimeSeriesIdentifiersByTypeList retrieveAccessToWaterTimeSeriesIds(AccessToWaterRetrievalParameters retrievalParameters, String cursor, int pageSize)
    {
        int total = 0;
        String cursorOffice = null;
        String cursorLocId = null;

        Condition officeCondition = retrievalParameters.getOfficeId()
                .map(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID::eq)
                .orElse(noCondition());

        Condition locationCondition = retrievalParameters.getLocationId()
                .map(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID::eq)
                .orElse(noCondition());

        Condition whereClause = locationCondition.and(officeCondition);

        if (cursor == null || cursor.isEmpty()) {
            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                    .from(AV_A2W_TS_CODES_BY_LOC2)
                    .leftOuterJoin(AV_CWMS_TS_ID)
                    .on(AV_A2W_TS_CODES_BY_LOC2.TS_CODE.equal(AV_CWMS_TS_ID.TS_CODE))
                    .where(whereClause);// Ensures rows matching cwmsIds are returned
            Record1<Integer> rec = count.fetchOne();
            if(rec != null) {
                total = rec.value1();
            }
        } else {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            if (parts.length > 1) {
                cursorOffice = TimeSeriesIdentifiersByTypeList.getOffice(cursor);
                cursorLocId = TimeSeriesIdentifiersByTypeList.getId(cursor);
                total = Integer.parseInt(parts[1]);
                pageSize = Integer.parseInt(parts[2]);
            }
        }
        int finalizedPageSize = pageSize;

        Condition moreInSameOffice = cursorLocId == null || cursorOffice == null ? noCondition() :
                AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID.eq(cursorOffice.toUpperCase())
                        .and(upper(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID).greaterThan(cursorLocId.toUpperCase()));
        Condition nextOffices = cursorOffice == null ? noCondition():
                upper(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID).greaterThan(cursorOffice.toUpperCase());
        Condition pagingCondition = moreInSameOffice.or(nextOffices);

        Map<InsensitiveCwmsId, Map<String, TimeSeriesMetaData>> locationToTsTypeMap = new LinkedHashMap<>();

        connection(dsl, conn ->
                dsl.select(AV_A2W_TS_CODES_BY_LOC2.CWMS_TS_ID, AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID,
                                AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID, AV_CWMS_TS_ID.TIME_ZONE_ID,
                                AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET)
                        .from(AV_A2W_TS_CODES_BY_LOC2)
                        .leftOuterJoin(AV_CWMS_TS_ID)
                        .on(AV_A2W_TS_CODES_BY_LOC2.TS_CODE.equal(AV_CWMS_TS_ID.TS_CODE))
                        .where(whereClause)
                        .and(pagingCondition)
                        .orderBy(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID, AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID)
                        .limit(finalizedPageSize)
                        .fetchStream()
                        .forEach(row -> {
                            String locationId = row.get(AV_A2W_TS_CODES_BY_LOC2.LOCATION_ID);
                            String tsTypeFromRow = row.get(AV_A2W_TS_CODES_BY_LOC2.TS_TYPE);
                            String officeId = row.get(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID);

                            TimeSeriesMetaData tsId = buildTsId(row);
                            locationToTsTypeMap.computeIfAbsent(new InsensitiveCwmsId(CwmsId.buildCwmsId(locationId, officeId)),
                                            k -> new LinkedHashMap<>()).put(tsTypeFromRow, tsId);
                        })
        );

        List<TimeSeriesIdentifiersByType> identifiersList = locationToTsTypeMap.entrySet().stream()
                .map(entry -> new TimeSeriesIdentifiersByType.Builder()
                        .withLocationId(entry.getKey().getCwmsId())
                        .withTimeSeriesIds(entry.getValue())
                        .build())
                .collect(toList());

        return new TimeSeriesIdentifiersByTypeList.Builder()
                .withCursor(cursor)
                .withTotal(total)
                .withPageSize(pageSize)
                .withTimeSeriesIdsForLocations(identifiersList)
                .build();

    }

    private TimeSeriesMetaData buildTsId(Record5<String, String, String, String, BigDecimal> row)
    {
        String timeSeriesId = row.get(AV_A2W_TS_CODES_BY_LOC2.CWMS_TS_ID);
        String officeId = row.get(AV_A2W_TS_CODES_BY_LOC2.DB_OFFICE_ID);
        BigDecimal intervalUtcMinuteOffset = row.get(AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET);
        String timeZoneId = row.get(AV_CWMS_TS_ID.TIME_ZONE_ID);
        return new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withOfficeId(officeId)
                        .withTimezoneName(timeZoneId)
                        .withTimeSeriesId(timeSeriesId)
                        .withIntervalOffsetMinutes(intervalUtcMinuteOffset.longValue())
                        .build())
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
