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
import static org.jooq.impl.DSL.asterisk;
import static usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL.AV_LOCATION_LEVEL;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.SeasonalValueBean;
import cwms.cda.data.dto.TimeSeries;
import hec.data.DataSetException;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Parameter;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.data.level.IAttributeParameterTypedValue;
import hec.data.level.ILocationLevelRef;
import hec.data.level.ISeasonalValue;
import hec.data.level.JDomLocationLevelImpl;
import hec.data.level.JDomSeasonalIntervalImpl;
import hec.data.level.JDomSeasonalValueImpl;
import hec.data.level.JDomSeasonalValuesImpl;
import hec.data.location.LocationTemplate;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import mil.army.usace.hec.metadata.constants.NumericalConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectLimitPercentAfterOffsetStep;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;
import usace.cwms.db.dao.ifc.level.CwmsDbLevel;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
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
    public void storeLocationLevel(LocationLevel locationLevel, ZoneId zoneId) {

        try {
            BigInteger months = locationLevel.getIntervalMonths() == null ? null :
                    BigInteger.valueOf(locationLevel.getIntervalMonths());
            BigInteger minutes = locationLevel.getIntervalMinutes() == null ? null :
                    BigInteger.valueOf(locationLevel.getIntervalMinutes());
            Date date =
                    Date.from(locationLevel.getLevelDate().toLocalDateTime().atZone(zoneId).toInstant());
            Date intervalOrigin = locationLevel.getIntervalOrigin() == null ? null :
                    Date.from(locationLevel.getIntervalOrigin().toLocalDateTime().atZone(zoneId).toInstant());
            List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> seasonalValues =
                    getSeasonalValues(locationLevel);
            connection(dsl, c -> {
                setOffice(c, locationLevel.getOfficeId());
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.storeLocationLevel(c, locationLevel.getLocationLevelId(),
                        locationLevel.getConstantValue(), locationLevel.getLevelUnitsId(),
                        locationLevel.getLevelComment(), date,
                        TimeZone.getTimeZone(zoneId), locationLevel.getAttributeValue(),
                        locationLevel.getAttributeUnitsId(), locationLevel.getAttributeDurationId(),
                        locationLevel.getAttributeComment(), intervalOrigin, months,
                        minutes, Boolean.parseBoolean(locationLevel.getInterpolateString()),
                        locationLevel.getSeasonalTimeSeriesId(),
                        seasonalValues, false, locationLevel.getOfficeId());
            });
        } catch (DataAccessException ex) {
            throw new RuntimeException("Failed to store Location Level", ex);
        }
    }

    private static List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> getSeasonalValues(LocationLevel locationLevel) {
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> retVal = Collections.emptyList();

        if (locationLevel != null) {
            retVal = buildSeasonalValues(locationLevel.getSeasonalValues());
        }

        return retVal;
    }

    @Nullable
    private static List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> buildSeasonalValues(List<SeasonalValueBean> levelSeasonalValues) {
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> seasonalValues = null;
        if (levelSeasonalValues != null) {
            seasonalValues = levelSeasonalValues.stream()
                    .map(LocationLevelsDaoImpl::buildSeasonalValue)
                    .collect(toList());
        }
        return seasonalValues;
    }

    @NotNull
    private List<SeasonalValueBean> buildSeasonalValues(LocationLevelPojo fromPojo) {
        List<SeasonalValueBean> seasonalValues = Collections.emptyList();
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> fromValues
                = fromPojo.getSeasonalValues();
        if (fromValues != null) {
            seasonalValues = fromValues.stream()
                    .filter(Objects::nonNull)
                    .map(LocationLevelsDaoImpl::buildSeasonalValue)
                    .collect(toList());
        }
        return seasonalValues;
    }

    @NotNull
    public static usace.cwms.db.dao.ifc.level.SeasonalValueBean buildSeasonalValue(SeasonalValueBean bean) {
        usace.cwms.db.dao.ifc.level.SeasonalValueBean storeBean =
                new usace.cwms.db.dao.ifc.level.SeasonalValueBean();
        storeBean.setValue(bean.getValue());
        Integer offsetMonths = bean.getOffsetMonths();
        if (offsetMonths != null) {
            storeBean.setOffsetMonths(offsetMonths.byteValue());
        }

        BigInteger offsetMinutes = bean.getOffsetMinutes();
        if (offsetMinutes != null) {
            storeBean.setOffsetMinutes(offsetMinutes);
        }
        return storeBean;
    }

    public static SeasonalValueBean buildSeasonalValue(usace.cwms.db.dao.ifc.level.SeasonalValueBean fromBean) {
        return new SeasonalValueBean.Builder(fromBean.getValue())
                .withOffsetMonths(fromBean.getOffsetMonths())
                .withOffsetMinutes(fromBean.getOffsetMinutes())
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
    public void renameLocationLevel(String oldLocationLevelName, String newLocationLevelName, String officeId) {
        CWMS_LEVEL_PACKAGE.call_RENAME_LOCATION_LEVEL(dsl.configuration(), oldLocationLevelName, newLocationLevelName,
            officeId);
    }

    @Override
    public LocationLevel retrieveLocationLevel(String locationLevelName, String units,
                                               ZonedDateTime effectiveDate, String officeId) {
        TimeZone timezone = TimeZone.getTimeZone(effectiveDate.getZone());
        Date date =
                Date.from(effectiveDate.toLocalDateTime().atZone(ZoneId.systemDefault()).toInstant());
        AtomicReference<LocationLevel> locationLevelRef = new AtomicReference<>();

        connection(dsl, c -> {
            CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
            LocationLevelPojo levelPojo = levelJooq.retrieveLocationLevel(c,
                    locationLevelName, units, date, timezone, null, null,
                    units, false, officeId);
            if (units == null && levelPojo.getLevelUnitsId() == null) {
                final String parameter = locationLevelName.split("\\.")[1];
                Configuration configuration = getDslContext(c, officeId).configuration();
                logger.info("Getting default units for " + parameter);
                final String defaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                    configuration,
                    parameter,
                    UnitSystem.SI.getValue()
                    );
                logger.info("Default units are " + defaultUnits);
                levelPojo.setLevelUnitsId(defaultUnits);
            }
            LocationLevel level = getLevelFromPojo(levelPojo, effectiveDate);
            locationLevelRef.set(level);
        });

        return locationLevelRef.get();
    }

    private LocationLevel getLevelFromPojo(LocationLevelPojo copyFromPojo,
                                           ZonedDateTime effectiveDate) {
        List<SeasonalValueBean> seasonalValues = buildSeasonalValues(copyFromPojo);
        return new LocationLevel.Builder(copyFromPojo.getLocationId(), effectiveDate)
                .withAttributeComment(copyFromPojo.getAttributeComment())
                .withAttributeDurationId(copyFromPojo.getAttributeDurationId())
                .withAttributeParameterId(copyFromPojo.getAttributeParameterId())
                .withLocationLevelId(copyFromPojo.getLocationId())
                .withAttributeValue(copyFromPojo.getAttributeValue())
                .withAttributeParameterTypeId(copyFromPojo.getAttributeParameterTypeId())
                .withAttributeUnitsId(copyFromPojo.getAttributeUnitsId())
                .withDurationId(copyFromPojo.getDurationId())
                .withInterpolateString(copyFromPojo.getInterpolateString())
                .withIntervalMinutes(copyFromPojo.getIntervalMinutes())
                .withIntervalMonths(copyFromPojo.getIntervalMonths())
                .withIntervalOrigin(copyFromPojo.getIntervalOrigin(), effectiveDate)
                .withLevelComment(copyFromPojo.getLevelComment())
                .withLevelUnitsId(copyFromPojo.getLevelUnitsId())
                .withOfficeId(copyFromPojo.getOfficeId())
                .withParameterId(copyFromPojo.getParameterId())
                .withParameterTypeId(copyFromPojo.getParameterTypeId())
                .withSeasonalTimeSeriesId(copyFromPojo.getSeasonalTimeSeriesId())
                .withSeasonalValues(seasonalValues)
                .withConstantValue(copyFromPojo.getSiParameterUnitsConstantValue())
                .withSpecifiedLevelId(copyFromPojo.getSpecifiedLevelId())
                .build();
    }

    @Override
    public LocationLevels getLocationLevels(String cursor, int pageSize,
                                            String levelIdMask, String office, String unit,
                                            String datum, ZonedDateTime beginZdt,
                                            ZonedDateTime endZdt) {
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

        Condition whereCondition = view.TSID.isNotNull();

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

        if(unit != null && !unit.isEmpty()){
            whereCondition = whereCondition.and(DSL.upper(view.UNIT_SYSTEM).eq(unit.toUpperCase()));
        }

        Condition siAndTsIdNull = view.UNIT_SYSTEM.eq("SI").and(view.TSID.isNull());
        whereCondition = whereCondition.or(siAndTsIdNull);

        Map<JDomLocationLevelImpl, JDomLocationLevelImpl> levelMap = new LinkedHashMap<>();

        SelectLimitPercentAfterOffsetStep<Record> query = dsl.selectDistinct(asterisk())
                .from(view)
                .where(whereCondition)
                .orderBy(DSL.upper(view.OFFICE_ID), DSL.upper(view.LOCATION_LEVEL_ID),
                        view.LEVEL_DATE
                )
                .offset(offset)
                .limit(pageSize);

//        logger.fine(() -> "getLocationLevels query: " + query.getSQL(ParamType.INLINED));

        query.stream().forEach(r -> addSeasonalValue(r, levelMap));

        List<LocationLevel> levels = new java.util.ArrayList<>();
        for (JDomLocationLevelImpl levelImpl : levelMap.values()) {
            LocationLevel level = new LocationLevel.Builder(levelImpl).build();
            levels.add(level);
        }

        LocationLevels.Builder builder = new LocationLevels.Builder(offset, pageSize, total);
        builder.addAll(levels);
        return builder.build();
    }

    private void addSeasonalValue(Record r, Map<JDomLocationLevelImpl,
            JDomLocationLevelImpl> levelMap) {
        usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL view = AV_LOCATION_LEVEL;
        Timestamp levelDateTimestamp = r.get(view.LEVEL_DATE);

        Date levelDate = null;
        if (levelDateTimestamp != null) {
            levelDate = new Date(levelDateTimestamp.getTime());
        }

        // always SI parameter units
        Double dConstLevel = r.get(view.CONSTANT_LEVEL);


        // seasonal stuff
        Timestamp intervalOriginDateTimeStamp = r.get(view.INTERVAL_ORIGIN);
        Date intervalOriginDate = null;
        if (intervalOriginDateTimeStamp != null) {
            intervalOriginDate = new Date(intervalOriginDateTimeStamp.getTime());
        }

        String interp = r.get(view.INTERPOLATE);

        Boolean boolInterp = null;
        if (interp != null) {
            boolInterp = OracleTypeMap.parseBool(interp);
        }

        String attrId = r.get(view.ATTRIBUTE_ID);
        Double oattrVal = r.get(view.ATTRIBUTE_VALUE);

        StringValueUnits attrVal = parseAttributeValue(r, attrId, oattrVal);

        String locLevelId = r.get(view.LOCATION_LEVEL_ID);
        String attrComment = r.get(view.ATTRIBUTE_COMMENT);
        String officeId = r.get(view.OFFICE_ID);
        String levelSiUnit = r.get(view.LEVEL_UNIT);

        // built to compare the loc level ref and eff date.
        JDomLocationLevelImpl locationLevelImpl = new JDomLocationLevelImpl(officeId, locLevelId,
                levelDate,
                levelSiUnit, attrId, attrVal._value, attrVal._units);

        // set interpolated value
        locationLevelImpl.setInterpolateSeasonal(boolInterp);

        String levelComment = r.get(view.LEVEL_COMMENT);
        if (levelMap.containsKey(locationLevelImpl)) {
            locationLevelImpl = levelMap.get(locationLevelImpl);
        } else {
            levelMap.put(locationLevelImpl, locationLevelImpl);

            setLevelData(r.get(view.TSID), dConstLevel, attrComment, locationLevelImpl, levelSiUnit,
                    levelComment);
        }
        parseSeasonalValues(r, intervalOriginDate, locationLevelImpl, levelSiUnit);
    }

    private void setLevelData(String tsid, Double dConstLevel, String attrComment,
                              JDomLocationLevelImpl locationLevelImpl, String levelSiUnit,
                              String levelComment) throws UnitsConversionException {
        locationLevelImpl.setLevelComment(levelComment);

        if (locationLevelImpl.getLocationLevelRef().getAttribute() != null) {
            locationLevelImpl.getLocationLevelRef().getAttribute().setComment(attrComment);
        }
        // set the level value
        if (dConstLevel != null) {
            // make sure that it is in the correct units.
            String parameterUnits = locationLevelImpl.getParameter().getUnitsString();
            if (Units.canConvertBetweenUnits(levelSiUnit, parameterUnits)) {
                dConstLevel = Units.convertUnits(dConstLevel, levelSiUnit, parameterUnits);
                // constant value
                locationLevelImpl.setSiParameterUnitsConstantValue(dConstLevel);
                locationLevelImpl.setUnits(parameterUnits);
            } else {
                locationLevelImpl.setSiParameterUnitsConstantValue(dConstLevel);
            }
        }
        // seasonal time series

        if (tsid != null) {
            locationLevelImpl.setSeasonalTimeSeriesId(tsid);
        }
    }

    private void parseSeasonalValues(Record rs, Date intervalOriginDate,
                                     JDomLocationLevelImpl locationLevelImpl, String levelSiUnit)
            throws DataSetException {
        usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL view = AV_LOCATION_LEVEL;
        // seasonal val
        Double dSeasLevel = rs.get(view.SEASONAL_LEVEL);

        if (dSeasLevel != null) {
            // retrieve existing seasonal value stuff
            JDomSeasonalValuesImpl seasonalValuesImpl = locationLevelImpl.getSeasonalValuesObject();
            if (seasonalValuesImpl == null) {
                seasonalValuesImpl = new JDomSeasonalValuesImpl();
                seasonalValuesImpl.setOrigin(intervalOriginDate);
                JDomSeasonalIntervalImpl offset = new JDomSeasonalIntervalImpl();
                String calInterval = rs.get(view.CALENDAR_INTERVAL);
                offset.setYearMonthString(calInterval);
                DayToSecond dayToSecond = rs.get(view.TIME_INTERVAL);
                if (dayToSecond != null) {
                    offset.setDaysHoursMinutesString(dayToSecond.toString());
                }
                seasonalValuesImpl.setOffset(offset);
                locationLevelImpl.setSeasonalValuesObject(seasonalValuesImpl);
            }

            // retrieve list of existing seasonal values
            List<ISeasonalValue> seasonalValues = seasonalValuesImpl.getSeasonalValues();

            // create new seasonal value with current record information
            JDomSeasonalValueImpl newSeasonalValue = new JDomSeasonalValueImpl();
            JDomSeasonalIntervalImpl newSeasonalOffset = new JDomSeasonalIntervalImpl();
            String calOffset = rs.get(view.CALENDAR_OFFSET);
            newSeasonalOffset.setYearMonthString(calOffset);
            String timeOffset = rs.get(view.TIME_OFFSET);
            newSeasonalOffset.setDaysHoursMinutesString(timeOffset);
            newSeasonalOffset.setDaysHoursMinutesString(timeOffset);
            newSeasonalValue.setOffset(newSeasonalOffset);
            newSeasonalValue.setPrototypeParameterType(locationLevelImpl.getPrototypeLevel());

            // make sure that it is in the correct units.
            String parameterUnits = locationLevelImpl.getParameter().getUnitsString();
            if (Units.canConvertBetweenUnits(levelSiUnit, parameterUnits)) {
                dSeasLevel = Units.convertUnits(dSeasLevel, levelSiUnit, parameterUnits);
                // constant value
                newSeasonalValue.setSiParameterUnitsValue(dSeasLevel);
                locationLevelImpl.setUnits(parameterUnits);
            } else {
                newSeasonalValue.setSiParameterUnitsValue(dSeasLevel);
            }
            // add new seasonal value to existing seasonal values
            seasonalValues.add(newSeasonalValue);
            seasonalValuesImpl.setSeasonalValues(seasonalValues);
        }
    }

    private static class StringValueUnits {
        String _value;
        String _units;
    }

    private StringValueUnits parseAttributeValue(Record rs, String attrId, Double oattrVal) {
        usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL view = AV_LOCATION_LEVEL;
        // query pulls SI parameter units
        String attrSiUnit = rs.get(view.ATTRIBUTE_UNIT);
        StringValueUnits attrVal = new StringValueUnits();
        if (attrId != null) {
            // we want are attributes in en parameter units.
            // this should be done via an oracle procedure call.
            Matcher matcher = attributeIdParsingPattern.matcher(attrId);
            if (!matcher.matches() || matcher.groupCount() != 3) {
                throw new DataSetException("Illegal location level attribute identifier: " + attrId);
            }
            // Flow.Max.6Hours
            String sattrParam = matcher.group(1);
            getEnAttributeValue(attrVal, oattrVal, attrSiUnit, sattrParam);
        }
        return attrVal;
    }

    private void getEnAttributeValue(StringValueUnits stringValueUnits, Double oattrVal,
                                     String attrSiUnit,
                                     String parameterId) throws DataSetIllegalArgumentException,
            UnitsConversionException {
        String attrEnUnit = null;
        String attrVal = null;
        if (oattrVal != null && attrSiUnit != null && parameterId != null) {
            Parameter parameter = new Parameter(parameterId);
            attrEnUnit = parameter.getUnitsStringForSystem(Units.ENGLISH_ID);
            double siAttrVal = ((Number) oattrVal).doubleValue();
            if (Units.canConvertBetweenUnits(attrSiUnit, attrEnUnit)) {
                double enSiVal = Units.convertUnits(siAttrVal, attrSiUnit, attrEnUnit);
                BigDecimal bd = BigDecimal.valueOf(enSiVal);
                BigDecimal setScale = bd.setScale(9, RoundingMode.HALF_UP);
                BigDecimal round = setScale.round(new MathContext(9, RoundingMode.HALF_UP));
                attrVal = round.toPlainString();
            }
        }
        stringValueUnits._value = attrVal;
        stringValueUnits._units = attrEnUnit;
    }

    @Override
    public TimeSeries retrieveLocationLevelAsTimeSeries(ILocationLevelRef levelRef, Instant start, Instant end,
                                                        Interval interval, String units) {
        String officeId = levelRef.getOfficeId();
        String locationLevelId = levelRef.getLocationLevelId();
        String levelUnits = units;
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
                specifiedTimes, locationLevelId, levelUnits, attributeId, attributeValue, attributeUnits,
                "UTC", officeId );

        if (locLvlValues.isEmpty()) {
            throw new NotFoundException("No time series found for: " + levelRef + " between start time: " + start + " and end time: " + end);
        }
        return buildTimeSeries(levelRef, interval, locLvlValues, locationZoneId);
    }

    public static ZTSV_ARRAY call_RETRIEVE_LOC_LVL_VALUES3(Configuration configuration, ZTSV_ARRAY specifiedTimes,
                                                           String locationLevelId, String levelUnits,
                                                           String attributeId, Number attributeValue,
                                                           String attributeUnits, String timezoneId,
                                                           String officeId) {
        /*
            Here are the options for the P_LEVEL_PRECEDENCE parameter taken from
            https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_database/browse/schema/src/cwms/cwms_level_pkg.sql#1507,1770,1775,1786,1825,1830,1841
            N specifies results from non-virtual (normal) location levels only
            V specifies results from virtual location levels only
            NV specifies results from non-virtual (normal) location levels where they exist, with virtual location levels allowed where non-virtual levels don't exist
            VN (default) specifies results from virtual location levels where they exist, with non-virtual location levels allowed where virtual levels don't exist
         */
        String levelPrecedence = "VN";
        return CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOC_LVL_VALUES3(configuration,
                specifiedTimes, locationLevelId, levelUnits, attributeId, attributeValue, attributeUnits,
                timezoneId, officeId, levelPrecedence);
    }

    private ZoneId getLocationZoneId(LocationTemplate locationRef) {
        String timeZone = CWMS_LOC_PACKAGE.call_GET_LOCAL_TIMEZONE__2(dsl.configuration(),
                locationRef.getLocationId(), locationRef.getOfficeId());
        return OracleTypeMap.toZoneId(timeZone, locationRef.getLocationId());
    }

    private static TimeSeries buildTimeSeries(ILocationLevelRef levelRef, Interval interval,
                                              ZTSV_ARRAY locLvlValues, ZoneId locationTimeZone) {
        String timeSeriesId = levelRef.getLocationRef().getLocationId() + "." + levelRef.getParameter().getParameter()
                + "." + levelRef.getParameterType().getParameterType() + "." + interval.getInterval() + "."
                + levelRef.getDuration().toString() + "." + levelRef.getSpecifiedLevel().getId();
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

    private ZTSV_ARRAY buildTsvArray(Instant start, Instant end, Interval interval, ZoneId locationTimeZone) {
        ZTSV_ARRAY retVal = new ZTSV_ARRAY();
        Interval iterateInterval = interval;
        if (interval.isIrregular()) {
            iterateInterval = IntervalFactory.findAny(isRegular().and(equalsName(interval.getInterval())))
                    .orElse(IntervalFactory.regular1Day());
        }
        try {

            Instant time = start;
            while (time.isBefore(end) || time.equals(end)) {
                retVal.add(new ZTSV_TYPE(Timestamp.from(time), null, null));
                time = iterateInterval.getNextIntervalTime(time, locationTimeZone);
            }
        } catch (mil.army.usace.hec.metadata.DataSetIllegalArgumentException ex) {
            throw new IllegalArgumentException("Error building time series intervals for interval id: " + interval, ex);
        }
        return retVal;
    }
}
