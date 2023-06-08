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

import static usace.cwms.db.dao.util.OracleTypeMap.parseBool;
import static usace.cwms.db.dao.util.OracleTypeMap.toZoneId;

import com.google.common.flogger.FluentLogger;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.radar.data.dto.TimeSeriesIdentifierDescriptors;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2;

public class TimeSeriesIdentifierDescriptorDao extends JooqDao<TimeSeriesIdentifierDescriptor> {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();


    public TimeSeriesIdentifierDescriptorDao(DSLContext dsl) {
        super(dsl);
    }


    public void create(TimeSeriesIdentifierDescriptor tsid, boolean versionedFlag,
                       Number intervalForward, Number intervalBackward, boolean failIfExists
    ) {
        dsl.connection(c -> {
            BigDecimal tsCode = CWMS_TS_PACKAGE.call_CREATE_TS_CODE(
                getDslContext(c,tsid.getOfficeId()).configuration(),
                tsid.getTimeSeriesId(),
                tsid.getIntervalOffsetMinutes(), intervalForward, intervalBackward,
                OracleTypeMap.formatBool(versionedFlag),
                OracleTypeMap.formatBool(tsid.isActive()),
                OracleTypeMap.formatBool(failIfExists), tsid.getOfficeId());
            logger.atFine().log("Created tsCode: %s for %s", tsCode, tsid.getTimeSeriesId());
        });
        
        
    }

    public TimeSeriesIdentifierDescriptors getTimeSeriesIdentifiers(String cursor, int pageSize, String office,
                                                                    String idRegex) {
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
                        logger.at(Level.INFO).log("Could not parse %s", parts[1]);
                    }
                }
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        Collection<TimeSeriesIdentifierDescriptor> retval = getTimeSeriesIdentifiers(office, idRegex, offset, pageSize);

        TimeSeriesIdentifierDescriptors.Builder builder = new TimeSeriesIdentifierDescriptors.Builder(offset, pageSize, total);
        builder.withDescriptors(retval);
        return builder.build();
    }


    public Collection<TimeSeriesIdentifierDescriptor> getTimeSeriesIdentifiers(String office, String idRegex, int firstRow,
                                                                               int pageSize) {

        Condition whereCondition = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID.equalIgnoreCase(office);
        if (idRegex != null && !idRegex.isEmpty()) {
            whereCondition = whereCondition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID, idRegex));
        }

        return dsl
                .selectDistinct(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID)
                .from(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2)
                .where(whereCondition)
                .orderBy(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID, AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID)
                .limit(pageSize)
                .offset(firstRow)
                .stream()
                .map(this::toDescriptor)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private TimeSeriesIdentifierDescriptor toDescriptor(org.jooq.Record5<String, String, BigDecimal, String, String> r) {
        String officeId = r.get(r.field1());
        String tsId = r.get(r.field2());
        BigDecimal utcOffset = r.get(r.field3());
        String activeFlag = r.get(r.field4());
        String zoneId = r.get(r.field5());

        String locationId = null;
        if( tsId != null && tsId.contains(".")){
            locationId = tsId.substring(0, tsId.indexOf('.'));
        }

        return new TimeSeriesIdentifierDescriptor.Builder()
                .withOfficeId(officeId)
                .withTimeSeriesId(tsId)
                .withZoneId(toZoneId(zoneId, locationId))
                .withIntervalOffsetMinutes(utcOffset.longValueExact())
                .withActive(parseBool(activeFlag))
                .build();
    }


    public Optional<TimeSeriesIdentifierDescriptor> getTimeSeriesIdentifier(String office, String timeseriesId) {
        return connectionResult(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            Optional<usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor> tsIdDesc = tsDao.retrieveTSIdentifierWithAliasSupport(connection, timeseriesId, office);

            Optional<TimeSeriesIdentifierDescriptor> retval = Optional.empty();
            if (tsIdDesc.isPresent()) {
                retval = Optional.of(toDto(tsIdDesc.get()));
            }

            return retval;
        });
    }

    public static TimeSeriesIdentifierDescriptor toDto(usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor tsId) {
        return new TimeSeriesIdentifierDescriptor.Builder()
                .withOfficeId(tsId.getOfficeId())
                .withTimeSeriesId(tsId.getTimeSeriesId())
                .withZoneId(tsId.getZoneId())
                .withIntervalOffsetMinutes((long) tsId.getIntervalOffsetMinutes())
                .withActive(tsId.isActive())
                .build();
    }

    public void update(String office, String timeseriesId, Number utcOffsetMinutes, Number intervalForward,
                       Number intervalBackward, boolean activeFlag) {
        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.updateTsId(connection, office, timeseriesId, utcOffsetMinutes, intervalForward, intervalBackward, activeFlag);
        });

    }

    public void rename(String officeId, String origId, String newId, Long utcOffset) {

        if (utcOffset == null) {
            CWMS_TS_PACKAGE.call_RENAME_TS(dsl.configuration(), officeId, origId, newId);
        } else {
            CWMS_TS_PACKAGE.call_RENAME_TS__2(dsl.configuration(), origId, newId, utcOffset,
                    officeId);
        }
    }

    public void delete(String office, String timeseriesId, DeleteMethod method) {
        switch (method) {
            case DELETE_KEY:
                deleteKey(office, timeseriesId);
                break;
            case DELETE_DATA:
                deleteData(office, timeseriesId);
                break;
            case DELETE_ALL:
                deleteAll(office, timeseriesId);
                break;
            default:
                throw new IllegalArgumentException("Unknown delete method: " + method);
        }
    }


    public void deleteAll(String officeId, String tsId) {
        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteAll(connection, officeId, tsId);
        });
    }

    public void deleteData(String officeId, String tsId) {
        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteData(connection, officeId, tsId);
        });
    }

    public void deleteKey(String officeId, String tsId) {
        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteKey(connection, officeId, tsId);
        });
    }


}
