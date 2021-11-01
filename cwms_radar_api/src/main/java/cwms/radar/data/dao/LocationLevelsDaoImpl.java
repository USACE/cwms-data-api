package cwms.radar.data.dao;

import cwms.radar.api.enums.Unit;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.LocationLevel;
import cwms.radar.data.dto.SeasonalValueBean;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.level.CwmsDbLevel;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocationLevelsDaoImpl extends JooqDao<LocationLevel> implements LocationLevelsDao
{
    private static final Logger logger = Logger.getLogger(LocationLevelsDaoImpl.class.getName());

    public LocationLevelsDaoImpl(DSLContext dsl)
    {
        super(dsl);
    }

    @Override
    public void storeLocationLevel(LocationLevel locationLevel, ZoneId zoneId) throws IOException
    {
        try
        {
            BigInteger months = locationLevel.getIntervalMonths() == null ? null : BigInteger.valueOf(locationLevel.getIntervalMonths());
            BigInteger minutes = locationLevel.getIntervalMinutes() == null? null : BigInteger.valueOf(locationLevel.getIntervalMinutes());
            Date date = Date.from(locationLevel.getLevelDate().toLocalDateTime().atZone(zoneId).toInstant());
            Date intervalOrigin = locationLevel.getIntervalOrigin() == null ? null :  Date.from(locationLevel.getIntervalOrigin().toLocalDateTime().atZone(zoneId).toInstant());
            List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> seasonalValues = getSeasonalValues(locationLevel);
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.storeLocationLevel(c, locationLevel.getLocationId(), locationLevel.getSiParameterUnitsConstantValue(), locationLevel.getLevelUnitsId(), locationLevel.getLevelComment(), date,
                        TimeZone.getTimeZone(zoneId), locationLevel.getAttributeValue(), locationLevel.getAttributeUnitsId(), locationLevel.getAttributeDurationId(),
                        locationLevel.getAttributeComment(), intervalOrigin, months,
                        minutes, Boolean.parseBoolean(locationLevel.getInterpolateString()), locationLevel.getSeasonalTimeSeriesId(),
                        seasonalValues, false, locationLevel.getOfficeId());
            });
        }
        catch(DataAccessException ex)
        {
            logger.log(Level.SEVERE, "Failed to store Location Level", ex);
            throw new IOException("Failed to store Location Level");
        }
    }

    private List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> getSeasonalValues(LocationLevel locationLevel)
    {
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> seasonalValues = null;
        if(locationLevel.getSeasonalValues() != null) {
            seasonalValues = new ArrayList<>();
            for (SeasonalValueBean bean : locationLevel.getSeasonalValues()) {
                usace.cwms.db.dao.ifc.level.SeasonalValueBean storeBean = new usace.cwms.db.dao.ifc.level.SeasonalValueBean();
                storeBean.setValue(bean.getValue());
                storeBean.setOffsetMonths(bean.getOffsetMonths().byteValue());
                storeBean.setOffsetMinutes(bean.getOffsetMinutes());
                seasonalValues.add(storeBean);
            }
        }
        return seasonalValues;
    }

    @Override
    public void deleteLocationLevel(String locationLevelName, ZonedDateTime zonedDateTime, String officeId, Boolean cascadeDelete) throws IOException
    {
        try
        {
            Date date = Date.from(zonedDateTime.toLocalDateTime().atZone(zonedDateTime.getZone()).toInstant());
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.deleteLocationLevel(c, locationLevelName, date, null, null, null, cascadeDelete, officeId);
            });
        }
        catch(DataAccessException ex)
        {
            logger.log(Level.SEVERE, "Failed to delete Location Level", ex);
            throw new IOException("Failed to delete Location Level ");
        }
    }

    @Override
    public void renameLocationLevel(String oldLocationLevelName, LocationLevel renamedLocationLevel) throws IOException
    {
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.renameLocationLevel(c, oldLocationLevelName, renamedLocationLevel.getLocationId(), renamedLocationLevel.getOfficeId());
            });
        }
        catch(DataAccessException ex)
        {
            logger.log(Level.SEVERE, "Failed to rename Location Level", ex);
            throw new IOException("Failed to rename Location Level");
        }
    }

    @Override
    public LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem, ZonedDateTime effectiveDate, String officeId) throws IOException
    {
        TimeZone timezone = TimeZone.getTimeZone(effectiveDate.getZone());
        Date date = Date.from(effectiveDate.toLocalDateTime().atZone(ZoneId.systemDefault()).toInstant());
        String unitIn = UnitSystem.EN.value().equals(unitSystem) ? Unit.FEET.getValue() : Unit.METER.getValue();
        AtomicReference<LocationLevel> locationLevelRef = new AtomicReference<>();
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                LocationLevelPojo levelPojo = levelJooq.retrieveLocationLevel(c, locationLevelName, unitIn, date, timezone, null, null, unitIn, false, officeId);
                LocationLevel level = getLevelFromPojo(levelPojo, effectiveDate);
                locationLevelRef.set(level);
            });
        }
        catch(DataAccessException ex)
        {
            logger.log(Level.SEVERE, "Failed to retrieve Location Level", ex);
            throw new IOException("Failed to retrieve Location Level");
        }
        return locationLevelRef.get();
    }

    private LocationLevel getLevelFromPojo(LocationLevelPojo copyFromPojo, ZonedDateTime effectiveDate)
    {
        List<usace.cwms.db.dao.ifc.level.SeasonalValueBean> copyFromSeasonalValues = copyFromPojo.getSeasonalValues();
        List<SeasonalValueBean> seasonalValues = new ArrayList<>();
        for(usace.cwms.db.dao.ifc.level.SeasonalValueBean copyFromBean : copyFromSeasonalValues)
        {
            seasonalValues.add(new SeasonalValueBean.Builder(copyFromBean.getValue())
                .withOffsetMonths(copyFromBean.getOffsetMonths().intValue())
                .withOffsetMinutes(copyFromBean.getOffsetMinutes())
                .build());
        }
        return new LocationLevel.Builder(copyFromPojo.getLocationId(), effectiveDate)
            .withAttributeComment(copyFromPojo.getAttributeComment())
            .withAttributeDurationId(copyFromPojo.getAttributeDurationId())
            .withAttributeParameterId(copyFromPojo.getAttributeParameterId())
            .withLocationId(copyFromPojo.getLocationId())
            .withAttributeValue(copyFromPojo.getAttributeValue())
            .withAttributeParameterTypeId(copyFromPojo.getAttributeParameterTypeId())
            .withAttributeUnitsId(copyFromPojo.getAttributeUnitsId())
            .withDurationId(copyFromPojo.getDurationId())
            .withInterpolateString(copyFromPojo.getInterpolateString())
            .withIntervalMinutes(copyFromPojo.getIntervalMinutes())
            .withIntervalMonths(copyFromPojo.getIntervalMonths())
            .withIntervalOrigin(ZonedDateTime.ofInstant(copyFromPojo.getIntervalOrigin().toInstant(), effectiveDate.getZone()))
            .withLevelComment(copyFromPojo.getLevelComment())
            .withLevelUnitsId(copyFromPojo.getLevelUnitsId())
            .withOfficeId(copyFromPojo.getOfficeId())
            .withParameterId(copyFromPojo.getParameterId())
            .withParameterTypeId(copyFromPojo.getParameterTypeId())
            .withSeasonalTimeSeriesId(copyFromPojo.getSeasonalTimeSeriesId())
            .withSeasonalValues(seasonalValues)
            .withSiParameterUnitsConstantValue(copyFromPojo.getSiParameterUnitsConstantValue())
            .withSpecifiedLevelId(copyFromPojo.getSpecifiedLevelId())
            .build();
    }
}
