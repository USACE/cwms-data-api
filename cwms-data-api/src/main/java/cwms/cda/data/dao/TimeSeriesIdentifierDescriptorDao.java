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


import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptors;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record5;
import org.jooq.Record6;
import org.jooq.Table;
import org.jooq.impl.DSL;
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
        connection(dsl, c -> {
            BigDecimal tsCode = CWMS_TS_PACKAGE.call_CREATE_TS_CODE(
                getDslContext(c,tsid.getOfficeId()).configuration(),
                tsid.getTimeSeriesId(),
                tsid.getIntervalOffsetMinutes(), intervalForward, intervalBackward,
                formatBool(versionedFlag),
                formatBool(tsid.isActive()),
                formatBool(failIfExists), tsid.getOfficeId());
            logger.atFine().log("Created tsCode: %s for %s", tsCode, tsid.getTimeSeriesId());
        });
    }

    public TimeSeriesIdentifierDescriptors getTimeSeriesIdentifiers(String cursor, int pageSize, String office,
                                                                    String idRegex, boolean includeAliases) {
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

        Condition whereCondition = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.ALIASED_ITEM.isNull();
        if (office != null && !office.isEmpty()) {
            whereCondition = whereCondition.and(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID.equalIgnoreCase(office));
        }
        if (idRegex != null && !idRegex.isEmpty()) {
            whereCondition = whereCondition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID, idRegex));
        }

        Collection<TimeSeriesIdentifierDescriptor> retval;

        if (!includeAliases) {
            retval = dsl
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
                    .offset(offset)
                    .stream()
                    .map(this::toDescriptor)
                    .collect(Collectors.toList());
        } else {
            Table<?> innerTable = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.as("tab1");
            Field<String> tsId = innerTable.field("CWMS_TS_ID", String.class);
            Field<BigDecimal> innerTsCode = innerTable.field("TS_CODE", BigDecimal.class);
            Field<String> aliasedItem = innerTable.field("ALIASED_ITEM", String.class);
            retval = dsl
                .select(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID,
                    DSL.multiset(
                        dsl.selectDistinct(
                            tsId
                        ).from(innerTable)
                            .where(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_CODE.eq(innerTsCode))
                            .and(aliasedItem.isNotNull())
                    ).convertFrom(rs -> rs.map(r -> r.get(tsId, String.class)))
                )
                .from(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2)
                .where(whereCondition)
                .orderBy(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID, AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID)
                .limit(pageSize)
                .offset(offset)
                .stream()
                .map(this::toDescriptorWithAliases)
                .collect(Collectors.toList());
        }

        if (!retval.isEmpty() && total == null) {
            total = dsl.selectCount().from(dsl
                .selectDistinct(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                    AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID)
                .from(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2)
                .where(whereCondition))
                .fetchOne(0, Integer.class);
        } else if (total == null) {
            total = 0;
        }

        TimeSeriesIdentifierDescriptors.Builder builder = new TimeSeriesIdentifierDescriptors
                    .Builder(offset, pageSize, total);
        builder.withDescriptors(retval);
        return builder.build();
    }

    private TimeSeriesIdentifierDescriptor toDescriptor(Record5<String, String, BigDecimal, String, String> r) {
        String officeId = r.get(r.field1());
        String tsId = r.get(r.field2());
        BigDecimal utcOffset = r.get(r.field3());
        String activeFlag = r.get(r.field4());
        String zoneId = r.get(r.field5());

        String locationId = null;
        if (tsId != null && tsId.contains(".")) {
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

    private TimeSeriesIdentifierDescriptor toDescriptorWithAliases(Record6<String, String, BigDecimal,
                String, String, List<String>> r) {

        String officeId = r.get(r.field1());
        String tsId = r.get(r.field2());
        BigDecimal utcOffset = r.get(r.field3());
        String activeFlag = r.get(r.field4());
        String zoneId = r.get(r.field5());
        List<String> aliases = r.get(r.field6());

        String locationId = null;
        if (tsId != null && tsId.contains(".")) {
            locationId = tsId.substring(0, tsId.indexOf('.'));
        }

        return new TimeSeriesIdentifierDescriptor.Builder()
            .withOfficeId(officeId)
            .withTimeSeriesId(tsId)
            .withZoneId(toZoneId(zoneId, locationId))
            .withIntervalOffsetMinutes(utcOffset.longValueExact())
            .withActive(parseBool(activeFlag))
            .withAliases(aliases)
            .build();
    }


    public Optional<TimeSeriesIdentifierDescriptor> getTimeSeriesIdentifier(String office, String timeseriesId) {
        AV_CWMS_TS_ID2 view = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
        return connectionResult(dsl, connection -> {
            Record5<String, String, Long, String, String>
                result = dsl.select(view.CWMS_TS_ID, view.DB_OFFICE_ID, view.INTERVAL,
                                    view.TIME_ZONE_ID, view.TS_ACTIVE_FLAG)
                .from(view)
                .where(view.CWMS_TS_ID.eq(timeseriesId).and(view.DB_OFFICE_ID.eq(office))).fetchOne();
            Optional<TimeSeriesIdentifierDescriptor> retval = Optional.empty();
            if (result != null) {
                retval = Optional.of(toDto(result));
            }

            return retval;
        });
    }

    public static TimeSeriesIdentifierDescriptor toDto(Record5<String, String, Long, String, String> rec) {
        AV_CWMS_TS_ID2 view = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
        return new TimeSeriesIdentifierDescriptor.Builder()
                .withOfficeId(rec.get(view.DB_OFFICE_ID))
                .withTimeSeriesId(rec.get(view.CWMS_TS_ID))
                .withZoneId(ZoneId.of(rec.get(view.TIME_ZONE_ID)))
                .withIntervalOffsetMinutes(rec.get(view.INTERVAL))
                .withActive(parseBool(rec.get(view.TS_ACTIVE_FLAG)))
                .build();
    }

    public void update(String office, String timeseriesId, Number utcOffsetMinutes, Number intervalForward,
                       Number intervalBackward, boolean activeFlag) {
        connection(dsl, connection -> {
            setOffice(connection,office);
            CWMS_TS_PACKAGE.call_UPDATE_TS_ID__2(getDslContext(connection, office).configuration(), timeseriesId,
                utcOffsetMinutes, intervalForward, intervalBackward, "UTC", formatBool(activeFlag), office);
        });

    }

    public void rename(String officeId, String origId, String newId, Long utcOffset) {
        dsl.connection(c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            if (utcOffset == null) {
                CWMS_TS_PACKAGE.call_RENAME_TS(configuration, officeId, origId, newId);
            } else {
                CWMS_TS_PACKAGE.call_RENAME_TS__2(configuration, origId, newId, utcOffset,
                        officeId);
            }
        });
        
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
            setOffice(connection,officeId);
            CWMS_TS_PACKAGE.call_DELETE_TS(getDslContext(connection, officeId).configuration(),
                tsId, DeleteRule.DELETE_ALL.toString(), officeId);
        });
    }

    public void deleteData(String officeId, String tsId) {
        connection(dsl, connection -> {
            setOffice(connection,officeId);
            CWMS_TS_PACKAGE.call_DELETE_TS(getDslContext(connection, officeId).configuration(),
                tsId, DeleteRule.DELETE_DATA.toString(), officeId);
        });
    }

    public void deleteKey(String officeId, String tsId) {
        connection(dsl, connection -> {
            setOffice(connection,officeId);
            CWMS_TS_PACKAGE.call_DELETE_TS(getDslContext(connection, officeId).configuration(),
                tsId, DeleteRule.DELETE_KEY.toString(), officeId);
        });
    }
}
