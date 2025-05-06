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
import static usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_LOCATION_LEVEL.AV_VIRTUAL_LOCATION_LEVEL;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevels;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalValueBean;
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

import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import cwms.cda.data.dto.locationlevel.VirtualLocationLevel;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import mil.army.usace.hec.metadata.constants.NumericalConstants;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectLimitPercentAfterOffsetStep;
import org.jooq.TableField;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_level.RETRIEVE_LOCATION_LEVEL3;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_LEVEL_T;
import usace.cwms.db.jooq.codegen.udt.records.SEASONAL_VALUE_T;
import usace.cwms.db.jooq.codegen.udt.records.SEASONAL_VALUE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_ARRAY;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_TYPE;

public class LocationLevelsDaoImpl extends JooqDao<LocationLevel> implements LocationLevelsDao {
    private static final Logger logger = Logger.getLogger(LocationLevelsDaoImpl.class.getName());

    private static final String ATTRIBUTE_ID_PARSING_REGEXP = "(.*)\\.(.*)\\.(.*)";
    public static final Pattern attributeIdParsingPattern =
            Pattern.compile(ATTRIBUTE_ID_PARSING_REGEXP);
    private static final String LOCATION_LEVEL_ID_PARSING_REGEXP = "\\.";
    public static final Pattern locationLevelIdParsingPattern =
            Pattern.compile(LOCATION_LEVEL_ID_PARSING_REGEXP);

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

        final SelectLimitPercentAfterOffsetStep<Record> queryFinal = query;

        logger.info(() -> "getLocationLevels query: " + queryFinal.getSQL(ParamType.INLINED));

        query.stream().forEach(r -> parseLevels(r, builderMap));

        // Virtual Levels
        usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_LOCATION_LEVEL virtView = AV_VIRTUAL_LOCATION_LEVEL;

        whereCondition = DSL.noCondition();

        if (office != null && !office.isEmpty()) {
            whereCondition = whereCondition.and(DSL.upper(virtView.OFFICE_ID).eq(office.toUpperCase()));
        }

        if (levelIdMask != null && !levelIdMask.isEmpty()) {
            whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
                    virtView.LOCATION_LEVEL_ID, levelIdMask));
        }

        if (beginZdt != null) {
            whereCondition = whereCondition.and(virtView.EFFECTIVE_DATE_UTC.greaterOrEqual(
                    Timestamp.from(beginZdt.toInstant())));
        }
        if (endZdt != null) {
            whereCondition = whereCondition.and(virtView.EFFECTIVE_DATE_UTC.lessThan(
                    Timestamp.from(endZdt.toInstant())));
        }

        query = dsl.selectDistinct(getAddVirtualFields())
                .from(virtView)
                .where(whereCondition)
                .orderBy(DSL.upper(virtView.OFFICE_ID), DSL.upper(virtView.LOCATION_LEVEL_ID),
                        virtView.EFFECTIVE_DATE_UTC
                )
                .offset(offset)
                .limit(pageSize);

        final SelectLimitPercentAfterOffsetStep<Record> virtQueryFinal = query;

        logger.info(() -> "getLocationLevels query: " + virtQueryFinal.getSQL(ParamType.INLINED));

        query.stream().forEach(r -> parseVirtualLevels(r, builderMap, unit));

        List<LocationLevel> levels = new java.util.ArrayList<>();
        for (LocationLevel.Builder builder : builderMap.values()) {
            if (builder instanceof TimeSeriesLocationLevel.Builder) {
                levels.add(((TimeSeriesLocationLevel.Builder) builder).build());
            } else if (builder instanceof SeasonalLocationLevel.Builder) {
                levels.add(((SeasonalLocationLevel.Builder) builder).build());
            } else if (builder instanceof ConstantLocationLevel.Builder) {
                levels.add(((ConstantLocationLevel.Builder) builder).build());
            } else if (builder instanceof VirtualLocationLevel.Builder) {
                levels.add(((VirtualLocationLevel.Builder) builder).build());
            } else {
                throw new IllegalArgumentException("Unknown builder type: " + builder.getClass().getName());
            }
        }

        LocationLevels.Builder builder = new LocationLevels.Builder(offset, pageSize, total);
        builder.addAll(levels);
        return builder.build();
    }

    private static class LevelLookup {
        private final JDomLocationLevelRef locationLevelRef;
        private final Date effectiveDate;

        public LevelLookup(String officeId, String locLevelId, String attributeId, String attributeValue,
                String attributeUnits, Date effectiveDate) {
            this(new JDomLocationLevelRef(officeId, locLevelId, attributeId, attributeValue, attributeUnits),
                    effectiveDate);
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
            return Objects.equals(locationLevelRef, that.locationLevelRef)
                    && Objects.equals(effectiveDate, that.effectiveDate);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(locationLevelRef);
            result = 31 * result + Objects.hashCode(effectiveDate);
            return result;
        }
    }

    @Override
    public void storeLocationLevel(LocationLevel locationLevel)
    {
        if (locationLevel instanceof VirtualLocationLevel) {
            VirtualLocationLevel virtualLocationLevel = (VirtualLocationLevel) locationLevel;
            Timestamp date = Timestamp.from(locationLevel.getLevelDate().toInstant());
            Timestamp expirationDate = Timestamp.from(virtualLocationLevel.getExpirationDate().toInstant());
            STR_TAB_TAB_T constituentTab = new STR_TAB_TAB_T();
            for (VirtualLocationLevel.Constituent constituent : virtualLocationLevel.getConstituents()) {
                constituentTab.add(new STR_TAB_T(constituent.getConstituentList()));
            }
            connection(dsl, c -> {
                String officeId = locationLevel.getOfficeId();
                setOffice(c, officeId);
                CWMS_LEVEL_PACKAGE.call_STORE_VIRTUAL_LOCATION_LEVEL(DSL.using(c).configuration(),
                        locationLevel.getLocationLevelId(), constituentTab, virtualLocationLevel.getConstituentConnections(),
                        locationLevel.getLevelComment(), locationLevel.getAttributeDurationId(),
                        locationLevel.getAttributeValue(), locationLevel.getAttributeUnitsId(),
                        locationLevel.getAttributeComment(), date, expirationDate,
                        "UTC", "F", "F",
                        officeId);
            });
        } else {
            BigInteger months = null;
            BigInteger minutes = null;
            Timestamp intervalOrigin = null;
            Timestamp date = Timestamp.from(locationLevel.getLevelDate().toInstant());
            SEASONAL_VALUE_TAB_T seasonalValues = null;
            Number constantValue = null;
            String seasonalTimeSeriesId = null;
            String interpolateString = null;

            if(locationLevel instanceof SeasonalLocationLevel)
            {
                SeasonalLocationLevel seasonalLocationLevel = (SeasonalLocationLevel) locationLevel;
                months = seasonalLocationLevel.getIntervalMonths() == null ? null :
                        BigInteger.valueOf(seasonalLocationLevel.getIntervalMonths());
                minutes = seasonalLocationLevel.getIntervalMinutes() == null ? null :
                        BigInteger.valueOf(seasonalLocationLevel.getIntervalMinutes());
                intervalOrigin = seasonalLocationLevel.getIntervalOrigin() == null ? null :
                        Timestamp.from(seasonalLocationLevel.getIntervalOrigin().toInstant());
                interpolateString = seasonalLocationLevel.getInterpolateString();

                seasonalValues = getSeasonalValues(seasonalLocationLevel);
            }
            else if(locationLevel instanceof ConstantLocationLevel)
            {
                constantValue = ((ConstantLocationLevel) locationLevel).getConstantValue();
            }
            else if(locationLevel instanceof TimeSeriesLocationLevel)
            {
                seasonalTimeSeriesId = ((TimeSeriesLocationLevel) locationLevel).getSeasonalTimeSeriesId();
            }

            final Number constantValueFinal = constantValue;
            final Timestamp dateFinal = date;
            final Timestamp intervalOriginFinal = intervalOrigin;
            final BigInteger monthsFinal = months;
            final BigInteger minutesFinal = minutes;
            final SEASONAL_VALUE_TAB_T seasonalValuesFinal = seasonalValues;
            final String seasonalTimeSeriesIdFinal = seasonalTimeSeriesId;
            final String interpolateStringFinal = interpolateString;

            connection(dsl, c ->
            {
                String officeId = locationLevel.getOfficeId();
                setOffice(c, officeId);
                CWMS_LEVEL_PACKAGE.call_STORE_LOCATION_LEVEL3(DSL.using(c).configuration(),
                        locationLevel.getLocationLevelId(), constantValueFinal, locationLevel.getLevelUnitsId(),
                        locationLevel.getLevelComment(),
                        dateFinal, "UTC", locationLevel.getAttributeValue(), locationLevel.getAttributeUnitsId(),
                        locationLevel.getAttributeDurationId(), locationLevel.getAttributeComment(), intervalOriginFinal, monthsFinal,
                        minutesFinal, interpolateStringFinal, seasonalTimeSeriesIdFinal, seasonalValuesFinal,
                        "F",
                        officeId);
            });
        }
    }

    private static SEASONAL_VALUE_TAB_T getSeasonalValues(SeasonalLocationLevel locationLevel) {
        List<SeasonalValueBean> seasonalValues = locationLevel.getSeasonalValues();

        SEASONAL_VALUE_TAB_T pSeasonalValues = null;
        if (seasonalValues != null && !seasonalValues.isEmpty()) {
            pSeasonalValues = new SEASONAL_VALUE_TAB_T();
            for(SeasonalValueBean seasonalValue : seasonalValues) {
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

    @NotNull
    private List<SeasonalValueBean> buildSeasonalValues(LOCATION_LEVEL_T level) {
        List<SeasonalValueBean> seasonalValues = Collections.emptyList();
        SEASONAL_VALUE_TAB_T values = level.getSEASONAL_VALUES();
        if (values != null) {
            seasonalValues = values.stream()
                    .filter(Objects::nonNull)
                    .map(LocationLevelsDaoImpl::buildSeasonalValue)
                    .collect(toList());
        }
        return seasonalValues;
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
                connection(dsl, c ->
                    CWMS_LEVEL_PACKAGE.call_DELETE_LOCATION_LEVEL3(getDslContext(c, officeId).configuration(), locationLevelName,
                            date, "UTC", null, null,
                            null, cascadeDelete ? "T" : "F", "F", "F",
                            officeId, "VN", "F", "T", "T")
                );
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
                                               ZonedDateTime effectiveDate, String officeId) {
        Timestamp date = Timestamp.from(effectiveDate.toInstant());
        String[] levelIdParts = locationLevelIdParsingPattern.split(locationLevelName);
        if (levelIdParts.length <= 2) {
            throw new IllegalArgumentException("Location level name is in an invalid format, must be separated by '.'");
        }
        String parameter = levelIdParts[1];
        return connectionResult(dsl, c -> {

            String units = pUnits;
            Configuration configuration = getDslContext(c, officeId).configuration();
            if (units != null && (units.equalsIgnoreCase("SI")
                    || units.equalsIgnoreCase("EN"))) {
                units = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(configuration,
                        parameter, units);
            } else if (units == null) {
                logger.info("Getting default units for " + parameter);
                String defaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                        configuration, parameter, UnitSystem.SI.getValue());
                logger.info("Default units are " + defaultUnits);
                units = defaultUnits;
            }
            LOCATION_LEVEL_T level = CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOCATION_LEVEL__2(
                    configuration, locationLevelName, units, date,
                    "UTC", null, null,
                    null, "T", officeId, "VN");
            if (level == null) {
                throw new NotFoundException("Location level not found: " + locationLevelName);
            }
            Timestamp pEffectiveDate = level.getLEVEL_DATE();
            ZonedDateTime realEffectiveDate = ZonedDateTime.ofInstant(pEffectiveDate.toInstant(), effectiveDate.getZone());
            List<VirtualLocationLevel.Constituent> constituents = new ArrayList<>();
            STR_TAB_TAB_T constituentTab = level.getCONSTITUENTS();
            Double constantValue = Optional.ofNullable(level.getLEVEL_VALUE())
                    .map(BigDecimal::doubleValue).orElse(null);
            List<SeasonalValueBean> seasonalValues = buildSeasonalValues(level);
            String seasonalTimeSeriesId = level.getTSID();
            if (constituentTab != null) {
                ZonedDateTime expirationDate = ZonedDateTime.ofInstant(level.getEXPIRATION_DATE().toInstant(), effectiveDate.getZone());

                constituentTab.forEach(constituent -> {
                    VirtualLocationLevel.Constituent.Builder constituentBuilder = new VirtualLocationLevel.Constituent.Builder(constituent.get(0), constituent.get(1), constituent.get(2));
                    if (constituent.size() > 3 && constituent.get(3) != null) {
                        constituentBuilder.withAttributeId(constituent.get(3));
                    }
                    if (constituent.size() > 4 && constituent.get(4) != null) {
                        constituentBuilder.withAttributeValue(Double.valueOf(constituent.get(4)));
                    }
                    if (constituent.size() > 5 && constituent.get(5) != null) {
                        constituentBuilder.withAttributeUnits(constituent.get(5));
                    }

                    constituents.add(constituentBuilder.build());
                });

                return new VirtualLocationLevel.Builder(locationLevelName, realEffectiveDate)
                    .withConstituents(constituents)
                    .withConstituentConnections(level.getCONNECTIONS())
                    .withExpirationDate(expirationDate)
                    .withLevelUnitsId(units)
                    .withAttributeUnitsId(units)
                    .withLevelComment(level.getLEVEL_COMMENT())
                    .withOfficeId(officeId)
                    .withAttributeParameterId(level.getATTRIBUTE_PARAMETER_ID())
                    .withInterpolateString(level.getINTERPOLATE())
                    .build();
            }
            else if (!seasonalValues.isEmpty()) {
                return new SeasonalLocationLevel.Builder(locationLevelName, realEffectiveDate)
                        .withLevelUnitsId(units)
                        .withAttributeUnitsId(units)
                        .withIntervalMinutes(Optional.ofNullable(level.getINTERVAL_MINUTES())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalMonths(Optional.ofNullable(level.getINTERVAL_MONTHS())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalOrigin(level.getINTERVAL_ORIGIN(), effectiveDate)
                        .withLevelComment(level.getLEVEL_COMMENT())
                        .withOfficeId(officeId)
                        .withIntervalMinutes(Optional.ofNullable(level.getINTERVAL_MINUTES())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalMonths(Optional.ofNullable(level.getINTERVAL_MONTHS())
                                .map(BigInteger::intValue).orElse(null))
                        .withIntervalOrigin(level.getINTERVAL_ORIGIN(), effectiveDate)
                        .withAttributeParameterId(level.getATTRIBUTE_PARAMETER_ID())
                        .withSeasonalValues(seasonalValues)
                        .withInterpolateString(level.getINTERPOLATE())
                        .build();
            } else if (constantValue != null) {
                return new ConstantLocationLevel.Builder(locationLevelName, realEffectiveDate)
                        .withLevelUnitsId(units)
                        .withAttributeUnitsId(units)
                        .withLevelComment(level.getLEVEL_COMMENT())
                        .withOfficeId(officeId)
                        .withAttributeParameterId(level.getATTRIBUTE_PARAMETER_ID())
                        .withConstantValue(constantValue)
                        .withInterpolateString(level.getINTERPOLATE())
                        .build();
            } else if (seasonalTimeSeriesId != null) {
                return new TimeSeriesLocationLevel.Builder(locationLevelName, realEffectiveDate)
                        .withLevelUnitsId(units)
                        .withAttributeUnitsId(units)
                        .withLevelComment(level.getLEVEL_COMMENT())
                        .withOfficeId(officeId)
                        .withAttributeParameterId(level.getATTRIBUTE_PARAMETER_ID())
                        .withSeasonalTimeSeriesId(level.getTSID())
                        .withInterpolateString(level.getINTERPOLATE())
                        .build();
            } else {
                throw new IllegalArgumentException("Location level does not match expected level type: " + locationLevelName);
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

    // These are all the fields that we need to pull out of jOOQ record for addVirtualValue
    private Collection<TableField<?,?>> getAddVirtualFields() {
        Set<TableField<?,?>> retval = new LinkedHashSet<>();
        usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_LOCATION_LEVEL view = AV_VIRTUAL_LOCATION_LEVEL;

        retval.add(view.OFFICE_ID);
        retval.add(view.LOCATION_LEVEL_ID);
        retval.add(view.ATTRIBUTE_ID);
        retval.add(view.DURATION_CODE);
        retval.add(view.EFFECTIVE_DATE_UTC);
        retval.add(view.CONNECTIONS);
        retval.add(view.DURATION_ID);
        retval.add(view.EXPIRATION_DATE_UTC);
        retval.add(view.ATTR_UNIT_EN);
        retval.add(view.ATTR_VALUE_EN);
        retval.add(view.ATTR_UNIT_SI);
        retval.add(view.ATTR_VALUE_SI);

        return retval;
    }


    private void parseLevels(Record r,
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

        ZonedDateTime levelZdt = null;
        if (levelDate != null) {
            levelZdt = ZonedDateTime.ofInstant(levelDate.toInstant(), ZoneId.of("UTC"));
        }

        Double seasonalLevel = r.get(view.SEASONAL_LEVEL);
        Double constantLevel = r.get(view.CONSTANT_LEVEL);
        String tsId = r.get(view.TSID);
        String interp = r.get(view.INTERPOLATE);
        String calOffset = r.get(view.CALENDAR_OFFSET);
        String timeOffset = r.get(view.TIME_OFFSET);
        String levelComment = r.get(view.LEVEL_COMMENT);
        String attributeComment = r.get(view.ATTRIBUTE_COMMENT);
        DayToSecond timeInterval = r.get(view.TIME_INTERVAL);
        String calendarInterval = r.get(view.CALENDAR_INTERVAL);
        Timestamp intervalOrigin = r.get(view.INTERVAL_ORIGIN);

        if (constantLevel != null) {
            ConstantLocationLevel.Builder constantBuilder = new ConstantLocationLevel.Builder(locLevelId, levelZdt);
            constantBuilder.withConstantValue(constantLevel);
            constantBuilder = withLocationLevelRef(constantBuilder, locationLevelRef);

            constantBuilder.withAttributeParameterId(attrId);
            constantBuilder.withAttributeUnitsId(attrUnit);
            constantBuilder.withLevelUnitsId(levelUnit);

            if (oattrVal != null) {
                constantBuilder.withAttributeValue(BigDecimal.valueOf(oattrVal));
            }
            constantBuilder.withLevelComment(levelComment);
            constantBuilder.withAttributeComment(attributeComment);
            builderMap.put(levelLookup, constantBuilder);
        } else if (seasonalLevel != null) {

            JDomSeasonalIntervalImpl newSeasonalOffset = buildSeasonalOffset(calOffset, timeOffset);
            SeasonalValueBean seasonalValue = buildSeasonalValueBean(seasonalLevel, newSeasonalOffset);
            SeasonalLocationLevel.Builder seasonalBuilder = new SeasonalLocationLevel.Builder(locLevelId, levelZdt);
            seasonalBuilder.withSeasonalValue(seasonalValue);
            seasonalBuilder.withInterpolateString(interp);
            seasonalBuilder.withIntervalMinutes(timeInterval.getMinutes());
            seasonalBuilder.withAttributeParameterId(attrId);
            seasonalBuilder.withAttributeUnitsId(attrUnit);
            seasonalBuilder.withLevelUnitsId(levelUnit);
            seasonalBuilder.withLevelComment(levelComment);
            seasonalBuilder.withAttributeComment(attributeComment);
            seasonalBuilder = withLocationLevelRef(seasonalBuilder, locationLevelRef);
            JDomSeasonalIntervalImpl offset = new JDomSeasonalIntervalImpl();
            offset.setYearMonthString(calendarInterval);
            seasonalBuilder.withIntervalMonths(offset.getMonths());
            seasonalBuilder.withIntervalOrigin(intervalOrigin, levelZdt);

            builderMap.put(levelLookup, seasonalBuilder);
        } else if (tsId != null) {
            TimeSeriesLocationLevel.Builder timeSeriesBuilder = new TimeSeriesLocationLevel.Builder(locLevelId, levelZdt);
            timeSeriesBuilder.withSeasonalTimeSeriesId(tsId);
            timeSeriesBuilder.withAttributeParameterId(attrId);
            timeSeriesBuilder.withAttributeUnitsId(attrUnit);
            timeSeriesBuilder.withLevelUnitsId(levelUnit);
            timeSeriesBuilder.withLevelComment(levelComment);
            timeSeriesBuilder.withAttributeComment(attributeComment);
            timeSeriesBuilder = withLocationLevelRef(timeSeriesBuilder, locationLevelRef);
            builderMap.put(levelLookup, timeSeriesBuilder);
        } else {
            LocationLevel.Builder builder = new LocationLevel.Builder(locLevelId, levelZdt);
            builder.withAttributeParameterId(attrId);
            builder.withAttributeUnitsId(attrUnit);
            builder.withLevelUnitsId(levelUnit);
            builder.withLevelComment(levelComment);
            builder.withAttributeComment(attributeComment);
            builder = withLocationLevelRef(builder, locationLevelRef);
            builderMap.put(levelLookup, builder);
        }
    }

    private void parseVirtualLevels(Record r,
            Map<LevelLookup, LocationLevel.Builder> builderMap, String unit) {
        usace.cwms.db.jooq.codegen.tables.AV_VIRTUAL_LOCATION_LEVEL view = AV_VIRTUAL_LOCATION_LEVEL;

        Timestamp levelDateTimestamp = r.get(view.EFFECTIVE_DATE_UTC);
        String attrId = r.get(view.ATTRIBUTE_ID);
        String locLevelId = r.get(view.LOCATION_LEVEL_ID);
        String officeId = r.get(view.OFFICE_ID);

        String attrUnit;
        Double oattrVal = null;
        if (unit.equalsIgnoreCase(UnitSystem.EN.value())) {
            attrUnit = r.get(view.ATTR_UNIT_EN);
            if (r.get(view.ATTR_VALUE_EN) != null) {
                oattrVal = r.get(view.ATTR_VALUE_EN).doubleValue();
            }

        } else {
            attrUnit = r.get(view.ATTR_UNIT_SI);
            if (r.get(view.ATTR_VALUE_EN) != null) {
                oattrVal = r.get(view.ATTR_VALUE_SI).doubleValue();
            }
        }

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

        VirtualLocationLevel.Builder builder;
        if (!builderMap.containsKey(levelLookup)) {
            ZonedDateTime levelZdt = null;
            if (levelDate != null) {
                levelZdt = ZonedDateTime.ofInstant(levelDate.toInstant(), ZoneId.of("UTC"));
            }
            builder = new VirtualLocationLevel.Builder(locLevelId, levelZdt);
            builder = withVirtualLocationLevelRef(builder, locationLevelRef);

            builder.withAttributeParameterId(attrId);
            builder.withAttributeUnitsId(attrUnit);

            if (oattrVal != null) {
                builder.withAttributeValue(BigDecimal.valueOf(oattrVal));
            }

            builderMap.put(levelLookup, builder);
        }
    }

    private <T extends LocationLevel.Builder> T withLocationLevelRef(T builder, JDomLocationLevelRef locationLevelRef) {
        ISpecifiedLevel specifiedLevel = locationLevelRef.getSpecifiedLevel();
        if (specifiedLevel != null) {
            builder = (T) builder.withSpecifiedLevelId(specifiedLevel.getId());
        }

        Parameter parameter = locationLevelRef.getParameter();
        if (parameter != null) {
            builder = (T) builder.withParameterId(parameter.toString());
        }

        ParameterType parameterType = locationLevelRef.getParameterType();
        if (parameterType != null) {
            builder = (T) builder.withParameterTypeId(parameterType.toString());
        }

        Duration duration = locationLevelRef.getDuration();
        if (duration != null) {
            builder = (T) builder.withDurationId(duration.toString());
        }


        return (T) builder
                .withOfficeId(locationLevelRef.getOfficeId())
                ;
    }

    private VirtualLocationLevel.Builder withVirtualLocationLevelRef(VirtualLocationLevel.Builder builder, JDomLocationLevelRef locationLevelRef) {
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
