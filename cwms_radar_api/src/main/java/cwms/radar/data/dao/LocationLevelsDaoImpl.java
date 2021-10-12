package cwms.radar.data.dao;

import cwms.radar.api.enums.Unit;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.LocationLevel;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.level.CwmsDbLevel;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;
import usace.cwms.db.dao.ifc.loc.CwmsDbLoc;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

public class LocationLevelsDaoImpl extends JooqDao<LocationLevelPojo> implements LocationLevelsDao
{

    public LocationLevelsDaoImpl(DSLContext dsl)
    {
        super(dsl);
    }



    @Override
    public void storeLocationLevel(LocationLevel locationLevel) throws IOException
    {
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                levelJooq.storeLocationLevel(c, locationLevel.getLocationId(), locationLevel.getSiParameterUnitsConstantValue(), locationLevel.getLevelUnitsId(), locationLevel.getLevelComment(), locationLevel.getLevelDate(),
                        TimeZone.getTimeZone(locationLevel.getTimeZoneId()), locationLevel.getAttributeValue(), locationLevel.getAttributeUnitsId(), locationLevel.getAttributeDurationId(),
                        locationLevel.getAttributeComment(), locationLevel.getIntervalOrigin(), BigInteger.valueOf(locationLevel.getIntervalMonths()),
                        BigInteger.valueOf(locationLevel.getIntervalMinutes()), Boolean.parseBoolean(locationLevel.getInterpolateString()), locationLevel.getSeasonalTimeSeriesId(),
                        locationLevel.getSeasonalValues(), true, locationLevel.getOfficeId());
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to store Location Level");
        }
    }

    @Override
    public void deleteLocationLevel(String locationLevelName, String officeId) throws IOException
    {
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                String elevationUnits = Unit.METER.getValue();
                levelJooq.deleteLocationLevel(c, locationLevelName, null, null, null, null, false, officeId);
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to rename Location Level");
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
                String elevationUnits = Unit.METER.getValue();
                levelJooq.renameLocationLevel(c, oldLocationLevelName, renamedLocationLevel.getLocationId(), renamedLocationLevel.getOfficeId());
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to rename Location Level");
        }
    }

    @Override
    public LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem, ZoneId timeZoneId, String officeId) throws IOException
    {
        TimeZone timezone = TimeZone.getTimeZone(timeZoneId);
        String unitIn = UnitSystem.EN.value().equals(unitSystem) ? Unit.FEET.getValue() : Unit.METER.getValue();
        AtomicReference<LocationLevel> locationLevelRef = new AtomicReference<>();
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLevel levelJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLevel.class, c);
                LocationLevelPojo level = levelJooq.retrieveLocationLevel(c, locationLevelName, unitIn, null, timezone, null, null, unitIn, false, officeId);
                locationLevelRef.set(new LocationLevel(level));
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to store Location Level");
        }
        return locationLevelRef.get();
    }
}
