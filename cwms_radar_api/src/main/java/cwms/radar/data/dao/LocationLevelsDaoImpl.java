package cwms.radar.data.dao;

import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.asterisk;
import static usace.cwms.db.jooq.codegen.tables.AV_LOCATION_LEVEL.AV_LOCATION_LEVEL;

import cwms.radar.api.enums.Unit;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.LocationLevel;
import cwms.radar.data.dto.LocationLevels;
import cwms.radar.data.dto.SeasonalValueBean;
import hec.data.DataSetException;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Parameter;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.data.level.ISeasonalValue;
import hec.data.level.JDomLocationLevelImpl;
import hec.data.level.JDomSeasonalIntervalImpl;
import hec.data.level.JDomSeasonalValueImpl;
import hec.data.level.JDomSeasonalValuesImpl;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectLimitPercentAfterOffsetStep;
import org.jooq.exception.DataAccessException;
import org.jooq.types.DayToSecond;
import usace.cwms.db.dao.ifc.level.CwmsDbLevel;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;

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
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> retval = Collections.emptyList();

        if (locationLevel != null) {
            retval = buildSeasonalValues(locationLevel.getSeasonalValues());
        }

        return retval;
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
            Date date;
            if (zonedDateTime != null) {
                date = Date.from(zonedDateTime.toLocalDateTime().atZone(zonedDateTime.getZone()).toInstant());
            } else {
                date = null;
            }
            if (date != null) {
                connection(dsl, c -> {
                    CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                    levelJooq.deleteLocationLevel(c, locationLevelName, date, null,
                            null, null, cascadeDelete, officeId);
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
            //logger.log(Level.SEVERE, "Failed to delete Location Level", ex);
            throw new RuntimeException("Failed to delete Location Level ", ex);
        }
    }

    @Override
    public void renameLocationLevel(String oldLocationLevelName,
                                    LocationLevel renamedLocationLevel) {
        // no need to validate the level here we are just using the name and office field
        try {
            connection(dsl, c -> {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.renameLocationLevel(c, oldLocationLevelName,
                        renamedLocationLevel.getLocationLevelId(),
                        renamedLocationLevel.getOfficeId());
            });
        } catch (DataAccessException ex) {
            //logger.log(Level.SEVERE, "Failed to rename Location Level", ex);
            throw new RuntimeException("Failed to rename Location Level", ex);
        }
    }

    @Override
    public LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem,
                                               ZonedDateTime effectiveDate, String officeId) {
        TimeZone timezone = TimeZone.getTimeZone(effectiveDate.getZone());
        Date date =
                Date.from(effectiveDate.toLocalDateTime().atZone(ZoneId.systemDefault()).toInstant());
        String unitIn = UnitSystem.EN.value().equals(unitSystem) ? Unit.FEET.getValue() :
                Unit.METER.getValue();
        AtomicReference<LocationLevel> locationLevelRef = new AtomicReference<>();

        connection(dsl, c -> {
            CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
            LocationLevelPojo levelPojo = levelJooq.retrieveLocationLevel(c,
                    locationLevelName, unitIn, date, timezone, null, null,
                    unitIn, false, officeId);
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

        Condition siAndTsIdNull = view.UNIT_SYSTEM.eq("SI").and(view.TSID.isNull());
        Condition tsIdNotNull = view.TSID.isNotNull();

        Condition whereCondition = siAndTsIdNull.or(tsIdNotNull);

        if (office != null && !office.isEmpty()) {
            whereCondition = whereCondition.and(view.OFFICE_ID.upper().eq(office.toUpperCase()));
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

        Map<JDomLocationLevelImpl, JDomLocationLevelImpl> levelMap = new HashMap<>();

        SelectLimitPercentAfterOffsetStep<Record> query = dsl.selectDistinct(asterisk())
                .from(view)
                .where(whereCondition)
                .orderBy(view.OFFICE_ID.upper(), view.LOCATION_LEVEL_ID.upper(),
                        view.LEVEL_DATE
                )
                .offset(offset)
                .limit(pageSize);

        //logger.log(Level.INFO, "getLocationLevels query: " + query.getSQL(ParamType.INLINED));

        List<LocationLevel> levels = query
                .stream()
                .map(r -> toLocationLevel(r, levelMap))
                .collect(toList());

        LocationLevels.Builder builder = new LocationLevels.Builder(offset, pageSize, total);
        builder.addAll(levels);
        return builder.build();
    }

    private LocationLevel toLocationLevel(Record r, Map<JDomLocationLevelImpl,
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

        return new LocationLevel.Builder(locationLevelImpl).build();
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


}
