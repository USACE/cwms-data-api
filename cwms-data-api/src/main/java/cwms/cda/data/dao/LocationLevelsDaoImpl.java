/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import static java.util.stream.Collectors.toList;
import static mil.army.usace.hec.metadata.IntervalFactory.equalsName;
import static mil.army.usace.hec.metadata.IntervalFactory.isRegular;
import static usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL.AV_LOCATION_LEVEL;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.SeasonalValueBean;
import cwms.cda.data.dto.TimeSeries;
import hec.data.Duration;
import hec.data.Parameter;
import hec.data.ParameterType;
import hec.data.level.IAttributeParameterTypedValue;
import hec.data.level.ILocationLevelRef;
import hec.data.level.ISpecifiedLevel;
import hec.data.level.JDomLocationLevelRef;
import hec.data.level.JDomSeasonalIntervalImpl;
import hec.data.location.LocationTemplate;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import mil.army.usace.hec.metadata.constants.NumericalConstants;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectLimitPercentAfterOffsetStep;
import org.jooq.TableField;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_level.RETRIEVE_LOCATION_LEVEL3;
import usace.cwms.db.jooq.codegen.udt.records.SEASONAL_VALUE_T;
import usace.cwms.db.jooq.codegen.udt.records.SEASONAL_VALUE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_ARRAY;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_TYPE;

public class LocationLevelsDaoImpl extends JooqDao<LocationLevel> implements LocationLevelsDao {
    private static final Logger logger = Logger.getLogger(LocationLevelsDaoImpl.class.getName());

    private static final String ATTRIBUTE_ID_PARSING_REGEXP = "(.*)\\.(.*)\\.(.*)";
    public static final Pattern attributeIdParsingPattern =
            Pattern.compile(ATTRIBUTE_ID_PARSING_REGEXP);

    public LocationLevelsDaoImpl(DSLContext dsl) {
        super(dsl);
    }

    // This is the legacy method that is used by the old API.
    @Override
    public String getLocationLevels(String format, String names, String office, String unit,
                                    String datum, String begin,
                                    String end, String timezone) {
        return CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOCATION_LEVELS_F(dsl.configuration(),
                names, format, unit, datum, begin, end, timezone, office);
    }

    @Override
    public LocationLevels getLocationLevels(String cursor, int pageSize,
                                            String levelIdMask, String office, @NotNull String unit,
                                            String datum, ZonedDateTime beginZdt, ZonedDateTime endZdt) {
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
                        logger.log(Level.INFO, "Could not parse {0}", parts[1]);
                    }
                }
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL view = AV_LOCATION_LEVEL;

        Condition whereCondition = DSL.upper(view.UNIT_SYSTEM).eq(unit.toUpperCase());

        if (office != null && !office.isEmpty()) {
            whereCondition = whereCondition.and(DSL.upper(view.OFFICE_ID).eq(office.toUpperCase()));
        }

        if (levelIdMask != null && !levelIdMask.isEmpty()) {
            whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
                    view.LOCATION_LEVEL_ID, levelIdMask));
        }

        if (beginZdt != null) {
            whereCondition = whereCondition.and(view.LEVEL_DATE.greaterOrEqual(
                    Timestamp.from(beginZdt.toInstant())));
        }
        if (endZdt != null) {
            whereCondition = whereCondition.and(view.LEVEL_DATE.lessThan(
                    Timestamp.from(endZdt.toInstant())));
        }

        Map<LevelLookup, LocationLevel.Builder> builderMap = new LinkedHashMap<>();

        SelectLimitPercentAfterOffsetStep<Record> query = dsl.selectDistinct(getAddSeasonalValueFields())
                .from(view)
                .where(whereCondition)
                .orderBy(DSL.upper(view.OFFICE_ID), DSL.upper(view.LOCATION_LEVEL_ID),
                        view.LEVEL_DATE, view.CALENDAR_OFFSET
                )
                .offset(offset)
                .limit(pageSize);

        logger.info(() -> "getLocationLevels query: " + query.getSQL(ParamType.INLINED));

        query.stream().forEach(r -> addSeasonalValue(r, builderMap));

        List<LocationLevel> levels = new java.util.ArrayList<>();
        for (LocationLevel.Builder builder : builderMap.values()) {
            levels.add(builder.build());
        }

        LocationLevels.Builder builder = new LocationLevels.Builder(offset, pageSize, total);
        builder.addAll(levels);
        return builder.build();
    }

    private static class LevelLookup {
        private final JDomLocationLevelRef locationLevelRef;
        private final Date effectiveDate;

        public LevelLookup(String officeId, String locLevelId, String attributeId, String attributeValue, String attributeUnits, Date effectiveDate) {
            this(new JDomLocationLevelRef(officeId, locLevelId, attributeId, attributeValue, attributeUnits), effectiveDate);
        }

        public LevelLookup(JDomLocationLevelRef locationLevelRef, Date effectiveDate) {
            this.locationLevelRef = locationLevelRef;
            this.effectiveDate = effectiveDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LevelLookup that = (LevelLookup) o;
            return Objects.equals(locationLevelRef, that.locationLevelRef) && Objects.equals(effectiveDate, that.effectiveDate);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(locationLevelRef);
            result = 31 * result + Objects.hashCode(effectiveDate);
            return result;
        }
    }

    @Override
    public void storeLocationLevel(LocationLevel locationLevel) {
        BigInteger months = locationLevel.getIntervalMonths() == null ? null :
            BigInteger.valueOf(locationLevel.getIntervalMonths());
        BigInteger minutes = locationLevel.getIntervalMinutes() == null ? null :
            BigInteger.valueOf(locationLevel.getIntervalMinutes());
        Timestamp date = Timestamp.from(locationLevel.getLevelDate().toInstant());
        Timestamp intervalOrigin = locationLevel.getIntervalOrigin() == null ? null :
            Timestamp.from(locationLevel.getIntervalOrigin().toInstant());
        SEASONAL_VALUE_TAB_T seasonalValues = getSeasonalValues(locationLevel);
        connection(dsl, c -> {
            String officeId = locationLevel.getOfficeId();
            setOffice(c, officeId);
            CWMS_LEVEL_PACKAGE.call_STORE_LOCATION_LEVEL3(DSL.using(c).configuration(),
                locationLevel.getLocationLevelId(), locationLevel.getConstantValue(), locationLevel.getLevelUnitsId(),
                locationLevel.getLevelComment(),
                date, "UTC", locationLevel.getAttributeValue(), locationLevel.getAttributeUnitsId(),
                locationLevel.getAttributeDurationId(), locationLevel.getAttributeComment(), intervalOrigin, months,
                minutes, locationLevel.getInterpolateString(), locationLevel.getSeasonalTimeSeriesId(), seasonalValues,
                "F",
                officeId);
        });
    }

    private static SEASONAL_VALUE_TAB_T getSeasonalValues(LocationLevel locationLevel) {
        List<SeasonalValueBean> seasonalValues = locationLevel.getSeasonalValues();

        SEASONAL_VALUE_TAB_T pSeasonalValues = null;
        if (seasonalValues != null && !seasonalValues.isEmpty()) {
            pSeasonalValues = new SEASONAL_VALUE_TAB_T();
            for (SeasonalValueBean seasonalValue : seasonalValues) {
                SEASONAL_VALUE_T seasonalValueT = new SEASONAL_VALUE_T();
                seasonalValueT.setOFFSET_MINUTES(toBigDecimal(seasonalValue.getOffsetMinutes()));
                if (seasonalValue.getOffsetMonths() != null) {
                    seasonalValueT.setOFFSET_MONTHS(seasonalValue.getOffsetMonths().byteValue());
                }
                seasonalValueT.setVALUE(toBigDecimal(seasonalValue.getValue()));
                pSeasonalValues.add(seasonalValueT);
            }
        }
        return pSeasonalValues;
    }

    @NotNull
    private List<SeasonalValueBean> buildSeasonalValues(RETRIEVE_LOCATION_LEVEL3 level) {
        List<SeasonalValueBean> seasonalValues = Collections.emptyList();
        SEASONAL_VALUE_TAB_T values = level.getP_SEASONAL_VALUES();
        if (values != null) {
            seasonalValues = values.stream()
                    .filter(Objects::nonNull)
                    .map(LocationLevelsDaoImpl::buildSeasonalValue)
                    .collect(toList());
        }
        return seasonalValues;
    }

    public static SeasonalValueBean buildSeasonalValue(SEASONAL_VALUE_T fromBean) {
        return new SeasonalValueBean.Builder(fromBean.getVALUE().doubleValue())
                .withOffsetMonths(fromBean.getOFFSET_MONTHS())
                .withOffsetMinutes(Optional.ofNullable(fromBean.getOFFSET_MINUTES())
                    .map(BigDecimal::toBigInteger).orElse(null))
                .build();
    }

    @Override
    public void deleteLocationLevel(String locationLevelName, ZonedDateTime zonedDateTime,
                                    String officeId, Boolean cascadeDelete) {
        try {
            Timestamp date;
            if (zonedDateTime != null) {
                date = Timestamp.from(zonedDateTime.toInstant());
            } else {
                date = null;
            }
            if (date != null) {
                connection(dsl, c -> {
                    String cascade = "F";
                    if (cascadeDelete != null && cascadeDelete) {
                        cascade = "T";
                    }
                    CWMS_LEVEL_PACKAGE.call_DELETE_LOCATION_LEVEL(getDslContext(c, officeId).configuration(),
                            locationLevelName, date, "UTC", null,
                            null, null, cascade, officeId, "VN");
                });
            } else {
                Record1<Long> levelCode = dsl.selectDistinct(AV_LOCATION_LEVEL.LOCATION_LEVEL_CODE)

                        .from(AV_LOCATION_LEVEL)
                        .where(AV_LOCATION_LEVEL.LOCATION_LEVEL_ID.eq(locationLevelName))
                        .and(AV_LOCATION_LEVEL.OFFICE_ID.eq(officeId)
                        )
                        .fetchOne();
                CWMS_LEVEL_PACKAGE.call_DELETE_LOCATION_LEVEL__2(dsl.configuration(),
                        BigInteger.valueOf(levelCode.value1()), cascadeDelete ? "T" : "F");
            }

        } catch (DataAccessException ex) {
            throw new RuntimeException("Failed to delete Location Level ", ex);
        }
    }

    @Override
    public void renameLocationLevel(String oldLocationLevelName, String newLocationLevelName,
                                    String officeId) {
        CWMS_LEVEL_PACKAGE.call_RENAME_LOCATION_LEVEL(dsl.configuration(),
                oldLocationLevelName, newLocationLevelName, officeId);
    }

    @Override
    public LocationLevel retrieveLocationLevel(String locationLevelName, String pUnits,
                                               ZonedDateTime effectiveDate, String officeId, ZonedDateTime start, ZonedDateTime end) {
        Timestamp date = Timestamp.from(effectiveDate.toInstant());
        Timestamp startDate = start == null ? null : Timestamp.from(start.toInstant());
        Timestamp endDate = end == null ? null : Timestamp.from(end.toInstant());
        return connectionResult(dsl, c -> {
            String units = pUnits;
            Configuration configuration = getDslContext(c, officeId).configuration();

            if (start != null && end != null) {

                ZoneId tz = start.getZone();
                if (units == null) {
                    String parameter = locationLevelName.split("\\.")[1];
                    logger.info("Getting default units for " + parameter);
                    String defaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                            configuration, parameter, UnitSystem.SI.getValue());
                    logger.info("Default units are " + defaultUnits);
                    units = defaultUnits;
                }

                Result<?> result = dsl.select(AV_LOCATION_LEVEL.LOCATION_LEVEL_ID,
                                AV_LOCATION_LEVEL.OFFICE_ID, AV_LOCATION_LEVEL.LEVEL_DATE,
                                AV_LOCATION_LEVEL.CONSTANT_LEVEL, AV_LOCATION_LEVEL.LEVEL_UNIT)
                    .from(AV_LOCATION_LEVEL)
                    .where(AV_LOCATION_LEVEL.LOCATION_LEVEL_ID.eq(locationLevelName))
                        .and(AV_LOCATION_LEVEL.OFFICE_ID.eq(officeId))
                        .and(AV_LOCATION_LEVEL.LEVEL_DATE.ge(startDate))
                        .and(AV_LOCATION_LEVEL.LEVEL_DATE.le(endDate))
                        .and(AV_LOCATION_LEVEL.LEVEL_UNIT.eq(units))
                    .fetch();

                if (result.isEmpty()) {
                    throw new NotFoundException("No location level found for " + locationLevelName + " at " + date);
                }

                boolean parentData = false;
                String locLevelId = null;
                Timestamp recentDate = null;
                Double constantValue = null;
                List<LocationLevel.ConstantValue> constantList = new ArrayList<>();
                for (Record r : result) {
                    if (!parentData)
                    {
                        locLevelId = r.get(AV_LOCATION_LEVEL.LOCATION_LEVEL_ID);
                    }

                    if (recentDate == null || recentDate.before(r.get(AV_LOCATION_LEVEL.LEVEL_DATE))) {
                            recentDate = r.get(AV_LOCATION_LEVEL.LEVEL_DATE);
                            constantValue = r.get(AV_LOCATION_LEVEL.CONSTANT_LEVEL);
                    }
                    constantList.add(new LocationLevel.ConstantValue.Builder()
                            .withConstantValue(r.get(AV_LOCATION_LEVEL.CONSTANT_LEVEL))
                            .withLevelDate(ZonedDateTime.of(r.get(AV_LOCATION_LEVEL.LEVEL_DATE).toLocalDateTime(),
                                    tz))
                            .build());
                }

                return new LocationLevel.Builder(locLevelId,
                        ZonedDateTime.of(recentDate.toLocalDateTime(), tz))
                        .withConstantValueList(constantList).withConstantValue(constantValue)
                        .withOfficeId(officeId).withAttributeUnitsId(units)
                        .build();

            } else {
                RETRIEVE_LOCATION_LEVEL3 level = CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOCATION_LEVEL3(
                        configuration, locationLevelName, units, date,
                        "UTC", null, null, units,
                        "F", officeId);
                List<SeasonalValueBean> seasonalValues = buildSeasonalValues(level);
                if (units == null) {
                    String parameter = locationLevelName.split("\\.")[1];
                    logger.info("Getting default units for " + parameter);
                    String defaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                            configuration, parameter, UnitSystem.SI.getValue());
                    logger.info("Default units are " + defaultUnits);
                    units = defaultUnits;
                }
                return new LocationLevel.Builder(locationLevelName, effectiveDate)
                        .withLevelUnitsId(units)
                        .withAttributeUnitsId(units)
                        .withInterpolateString(level.getP_INTERPOLATE())
                        .withIntervalMinutes(Optional.ofNullable(level.getP_INTERVAL_MINUTES())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalMonths(Optional.ofNullable(level.getP_INTERVAL_MONTHS())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalOrigin(level.getP_INTERVAL_ORIGIN(), effectiveDate)
                        .withLevelComment(level.getP_LEVEL_COMMENT())
                        .withOfficeId(officeId)
                        .withAttributeParameterId(level.get(RETRIEVE_LOCATION_LEVEL3.P_ATTRIBUTE_ID))
                        .withSeasonalTimeSeriesId(level.get(RETRIEVE_LOCATION_LEVEL3.P_TSID))
                        .withSeasonalValues(seasonalValues)
                        .withConstantValue(Optional.ofNullable(level.getP_LEVEL_VALUE())
                                .map(BigDecimal::doubleValue).orElse(null))
                        .build();
            }
        });
    }

    // These are all the fields that we need to pull out of jOOQ record for addSeasonalValue
    private Collection<TableField<?,?>> getAddSeasonalValueFields() {
        Set<TableField<?,?>> retval = new LinkedHashSet<>();

        retval.add(AV_LOCATION_LEVEL.OFFICE_ID);
        retval.add(AV_LOCATION_LEVEL.LOCATION_LEVEL_ID);
        retval.add(AV_LOCATION_LEVEL.LEVEL_DATE);
        retval.add(AV_LOCATION_LEVEL.TSID);
        retval.add(AV_LOCATION_LEVEL.CONSTANT_LEVEL);
        retval.add(AV_LOCATION_LEVEL.INTERVAL_ORIGIN);
        retval.add(AV_LOCATION_LEVEL.INTERPOLATE);
        retval.add(AV_LOCATION_LEVEL.ATTRIBUTE_ID);
        retval.add(AV_LOCATION_LEVEL.ATTRIBUTE_VALUE);
        retval.add(AV_LOCATION_LEVEL.ATTRIBUTE_UNIT);
        retval.add(AV_LOCATION_LEVEL.ATTRIBUTE_COMMENT);
        retval.add(AV_LOCATION_LEVEL.LEVEL_UNIT);
        retval.add(AV_LOCATION_LEVEL.LEVEL_COMMENT);

        retval.addAll(getParseSeasonalValuesFields());

        return retval;
    }


    private void addSeasonalValue(Record r,
                                  Map<LevelLookup, LocationLevel.Builder> builderMap) {
        usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL view = AV_LOCATION_LEVEL;

        Timestamp levelDateTimestamp = r.get(view.LEVEL_DATE);
        String attrId = r.get(view.ATTRIBUTE_ID);
        Double oattrVal = r.get(view.ATTRIBUTE_VALUE);
        String locLevelId = r.get(view.LOCATION_LEVEL_ID);
        String officeId = r.get(view.OFFICE_ID);
        String levelUnit = r.get(view.LEVEL_UNIT);
        String attrUnit = r.get(AV_LOCATION_LEVEL.ATTRIBUTE_UNIT);

        Date levelDate = null;
        if (levelDateTimestamp != null) {
            levelDate = new Date(levelDateTimestamp.getTime());
        }

        String attrStr = null;
        if (oattrVal != null) {
            attrStr = oattrVal.toString(); // this is weird. allow it for now but maybe this should be doing some rounding?
        }

        JDomLocationLevelRef locationLevelRef = new JDomLocationLevelRef(officeId, locLevelId, attrId, attrStr, attrUnit);
        LevelLookup levelLookup = new LevelLookup(locationLevelRef, levelDate);

        LocationLevel.Builder builder;
        if (builderMap.containsKey(levelLookup)) {
            builder = builderMap.get(levelLookup);
        } else {
            ZonedDateTime levelZdt = null;
            if (levelDate != null) {
                levelZdt = ZonedDateTime.ofInstant(levelDate.toInstant(), ZoneId.of("UTC"));
            }
            builder = new LocationLevel.Builder(locLevelId, levelZdt);
            builder = withLocationLevelRef(builder, locationLevelRef);

            builder.withAttributeParameterId(attrId);
            builder.withAttributeUnitsId(attrUnit);
            builder.withLevelUnitsId(levelUnit);

            if (oattrVal != null) {
                builder.withAttributeValue(BigDecimal.valueOf(oattrVal));
            }
            builder.withLevelComment(r.get(view.LEVEL_COMMENT));
            builder.withAttributeComment(r.get(view.ATTRIBUTE_COMMENT));
            builder.withConstantValue(r.get(view.CONSTANT_LEVEL));
            builder.withSeasonalTimeSeriesId(r.get(view.TSID));

            builderMap.put(levelLookup, builder);
        }


        String interp = r.get(view.INTERPOLATE);
        builder.withInterpolateString(interp);

        Double seasonalLevel = r.get(view.SEASONAL_LEVEL);

        if (seasonalLevel != null) {
//            JDomSeasonalValuesImpl  seasonalValuesImpl = new JDomSeasonalValuesImpl();
//
//            Timestamp intervalOriginDateTimeStamp = r.get(view.INTERVAL_ORIGIN);
//            // seasonal stuff
//            Date intervalOriginDate = null;
//            if (intervalOriginDateTimeStamp != null) {
//                intervalOriginDate = new Date(intervalOriginDateTimeStamp.getTime());
//            }
//
//            seasonalValuesImpl.setOrigin(intervalOriginDate);
//
//            String calInterval = r.get(view.CALENDAR_INTERVAL);
//            DayToSecond dayToSecond = r.get(view.TIME_INTERVAL);
//            JDomSeasonalIntervalImpl offset = new JDomSeasonalIntervalImpl();
//            offset.setYearMonthString(calInterval);
//            if (dayToSecond != null) {
//                offset.setDaysHoursMinutesString(dayToSecond.toString());
//            }
//            seasonalValuesImpl.setOffset(offset);
           // TODO: LocationLevel is missing seasonal origin and offset.

            String calOffset = r.get(view.CALENDAR_OFFSET);
            String timeOffset = r.get(view.TIME_OFFSET);
            JDomSeasonalIntervalImpl newSeasonalOffset = buildSeasonalOffset(calOffset, timeOffset);
            SeasonalValueBean seasonalValue = buildSeasonalValueBean(seasonalLevel, newSeasonalOffset) ;
            builder.withSeasonalValue(seasonalValue);
        }
    }

    private LocationLevel.Builder withLocationLevelRef(LocationLevel.Builder builder, JDomLocationLevelRef locationLevelRef) {
        ISpecifiedLevel specifiedLevel = locationLevelRef.getSpecifiedLevel();
        if (specifiedLevel != null) {
            builder = builder.withSpecifiedLevelId(specifiedLevel.getId());
        }

        Parameter parameter = locationLevelRef.getParameter();
        if (parameter != null) {
            builder = builder.withParameterId(parameter.toString());
        }

        ParameterType parameterType = locationLevelRef.getParameterType();
        if (parameterType != null) {
            builder = builder.withParameterTypeId(parameterType.toString());
        }

        Duration duration = locationLevelRef.getDuration();
        if (duration != null) {
            builder = builder.withDurationId(duration.toString());
        }


        return builder
                .withOfficeId(locationLevelRef.getOfficeId())
                ;
    }

    private SeasonalValueBean buildSeasonalValueBean(Double seasonalLevel,
                                                     JDomSeasonalIntervalImpl offset) {
        // Avoiding JDomSeasonalValueImpl b/c it does units conversion to SI.
        return new SeasonalValueBean.Builder(seasonalLevel)
                .withOffsetMinutes(BigInteger.valueOf(offset.getTotalMinutes()))
                .withOffsetMonths(offset.getTotalMonths())
                .build();
    }

    // These are all the fields that we need to pull out of jOOQ record for parseSeasonalValues
    private Collection<TableField<?,?>> getParseSeasonalValuesFields() {
        Set<TableField<?,?>> retval = new LinkedHashSet<>();

        retval.add(AV_LOCATION_LEVEL.SEASONAL_LEVEL);
        retval.add(AV_LOCATION_LEVEL.CALENDAR_INTERVAL);
        retval.add(AV_LOCATION_LEVEL.TIME_INTERVAL);
        retval.add(AV_LOCATION_LEVEL.CALENDAR_OFFSET);
        retval.add(AV_LOCATION_LEVEL.TIME_OFFSET);

        return retval;
    }


    @NotNull
    private static JDomSeasonalIntervalImpl buildSeasonalOffset(String calOffset,
                                                                String timeOffset) {
        JDomSeasonalIntervalImpl newSeasonalOffset = new JDomSeasonalIntervalImpl();
        newSeasonalOffset.setYearMonthString(calOffset);
        newSeasonalOffset.setDaysHoursMinutesString(timeOffset);
        newSeasonalOffset.setDaysHoursMinutesString(timeOffset);
        return newSeasonalOffset;
    }

    @Override
    public TimeSeries retrieveLocationLevelAsTimeSeries(ILocationLevelRef levelRef,
                                                        Instant start, Instant end,
                                                        Interval interval, String units) {
        String officeId = levelRef.getOfficeId();
        String locationLevelId = levelRef.getLocationLevelId();
        String attributeId = null;
        Number attributeValue = null;
        String attributeUnits = null;
        IAttributeParameterTypedValue attribute = levelRef.getAttribute();
        if (attribute != null) {
            attributeId = attribute.getAttributeId();
            attributeValue = attribute.getValueBigDecimal();
            attributeUnits = attribute.getUnits();
        }
        ZoneId locationZoneId = getLocationZoneId(levelRef.getLocationRef());
        ZTSV_ARRAY specifiedTimes = buildTsvArray(start, end, interval, locationZoneId);
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);

        ZTSV_ARRAY locLvlValues = call_RETRIEVE_LOC_LVL_VALUES3(dsl.configuration(),
                specifiedTimes, locationLevelId, units, attributeId, attributeValue,
                attributeUnits, "UTC", officeId);

        if (locLvlValues.isEmpty()) {
            throw new NotFoundException(String.format(
                    "No time series found for: %s between start time: %s and end time: %s",
                    levelRef, start, end));
        }
        return buildTimeSeries(levelRef, interval, locLvlValues, locationZoneId);
    }

    public static ZTSV_ARRAY call_RETRIEVE_LOC_LVL_VALUES3(Configuration configuration,
                                                           ZTSV_ARRAY specifiedTimes,
                                                           String locationLevelId,
                                                           String levelUnits,
                                                           String attributeId,
                                                           Number attributeValue,
                                                           String attributeUnits, String timezoneId,
                                                           String officeId) {
        /*
            Here are the options for the P_LEVEL_PRECEDENCE parameter taken from
            https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_database/browse/schema/src/cwms
            /cwms_level_pkg.sql#1507,1770,1775,1786,1825,1830,1841
            N specifies results from non-virtual (normal) location levels only
            V specifies results from virtual location levels only
            NV specifies results from non-virtual (normal) location levels where they exist,
                with virtual location levels allowed where non-virtual levels don't exist
            VN (default) specifies results from virtual location levels where they exist,
                with non-virtual location levels allowed where virtual levels don't exist
         */
        String levelPrecedence = "VN";
        return CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOC_LVL_VALUES3(configuration,
                specifiedTimes, locationLevelId, levelUnits, attributeId, attributeValue,
                attributeUnits, timezoneId, officeId, levelPrecedence);
    }

    private ZoneId getLocationZoneId(LocationTemplate locationRef) {
        String timeZone = CWMS_LOC_PACKAGE.call_GET_LOCAL_TIMEZONE__2(dsl.configuration(),
                locationRef.getLocationId(), locationRef.getOfficeId());
        return toZoneId(timeZone, locationRef.getLocationId());
    }

    private static TimeSeries buildTimeSeries(ILocationLevelRef levelRef, Interval interval,
                                              ZTSV_ARRAY locLvlValues, ZoneId locationTimeZone) {
        String timeSeriesId = String.format("%s.%s.%s.%s.%s.%s", levelRef.getLocationRef().getLocationId(),
                levelRef.getParameter().getParameter(), levelRef.getParameterType().getParameterType(),
                interval.getInterval(), levelRef.getDuration().toString(), levelRef.getSpecifiedLevel().getId());
        int size = locLvlValues.size();
        String levelUnits = levelRef.getParameter().getUnitsString();
        String officeId = levelRef.getOfficeId();
        Instant start = locLvlValues.get(0).getDATE_TIME().toInstant();
        Instant end = locLvlValues.get(size - 1).getDATE_TIME().toInstant();
        ZonedDateTime firstValueTime = ZonedDateTime.ofInstant(start, NumericalConstants.UTC_ZONEID);
        ZonedDateTime lastValueTime = ZonedDateTime.ofInstant(end, NumericalConstants.UTC_ZONEID);
        TimeSeries timeSeries = new TimeSeries(null, size, size, timeSeriesId,
                officeId, firstValueTime, lastValueTime, levelUnits,
                java.time.Duration.ofSeconds(interval.getSeconds()),
                null, null, locationTimeZone.getId(), null, VersionType.UNVERSIONED);
        for (ZTSV_TYPE tsv : locLvlValues) {
            Timestamp dateTime = tsv.getDATE_TIME();
            Double value = tsv.getVALUE();
            if (value == null) {
                value = NumericalConstants.HEC_UNDEFINED_DOUBLE;
            }
            BigDecimal qualityCode = tsv.getQUALITY_CODE();
            int quality = 0;
            if (qualityCode != null) {
                quality = qualityCode.intValue();
            }
            timeSeries.addValue(dateTime, value, quality);
        }
        return timeSeries;
    }

    private ZTSV_ARRAY buildTsvArray(Instant start, Instant end, Interval interval,
                                     ZoneId locationTimeZone) {
        ZTSV_ARRAY retVal = new ZTSV_ARRAY();
        Interval iterateInterval = interval;
        if (interval.isIrregular()) {
            iterateInterval = IntervalFactory.findAny(isRegular()
                            .and(equalsName(interval.getInterval())))
                    .orElse(IntervalFactory.regular1Day());
        }
        try {
            Instant time = start;
            while (time.isBefore(end) || time.equals(end)) {
                retVal.add(new ZTSV_TYPE(Timestamp.from(time), null, null));
                time = iterateInterval.getNextIntervalTime(time, locationTimeZone);
            }
        } catch (mil.army.usace.hec.metadata.DataSetIllegalArgumentException ex) {
            throw new IllegalArgumentException("Error building time series intervals "
                    + "for interval id: " + interval, ex);
        }
        return retVal;
    }
}
