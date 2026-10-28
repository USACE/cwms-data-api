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

package cwms.cda.data.dao.location.kind;

import static cwms.cda.data.dao.JooqDao.buildDouble;
import static cwms.cda.data.dao.JooqDao.parseBool;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationRef;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLookupType;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import cwms.cda.helpers.ZoneIdHelper;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOCK_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AT_LOCK_GATE_TYPE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOCK;
import usace.cwms.db.jooq.codegen.tables.CWMS_OFFICE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCK_OBJ_T;

public final class LockDao extends JooqDao<Lock> {

    public LockDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Lock> retrieveLockCatalog(CwmsId projectId) {
        return retrieveLockCatalog(projectId, UnitSystem.SI);
    }

    public List<Lock> retrieveLockCatalog(CwmsId projectId, UnitSystem unitSystem) {
        AV_LOCK lock = AV_LOCK.AV_LOCK;
        AV_LOC loc = AV_LOC.AV_LOC;
        AT_LOCK_GATE_TYPE chamber = AT_LOCK_GATE_TYPE.AT_LOCK_GATE_TYPE;
        CWMS_OFFICE chamberOffice = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.select(lock.fields())
              .select(loc.LOCATION_ID, loc.LOCATION_KIND_ID, loc.TIME_ZONE_NAME, loc.LATITUDE, loc.LONGITUDE,
                    loc.HORIZONTAL_DATUM, loc.VERTICAL_DATUM, loc.ELEVATION, loc.UNIT_ID, loc.COUNTY_NAME,
                    loc.STATE_INITIAL, loc.PUBLIC_NAME, loc.LONG_NAME, loc.DESCRIPTION, loc.ACTIVE_FLAG,
                    loc.LOCATION_TYPE, loc.MAP_LABEL, loc.PUBLISHED_LATITUDE, loc.PUBLISHED_LONGITUDE,
                    loc.BOUNDING_OFFICE_ID, loc.NATION_ID, loc.NEAREST_CITY)
              .select(chamber.CHAMBER_TYPE_DISPLAY_VALUE, chamber.CHAMBER_TYPE_TOOLTIP,
                    chamber.CHAMBER_TYPE_ACTIVE, chamberOffice.OFFICE_ID)
              .from(lock)
              .join(loc)
              .on(loc.LOCATION_CODE.eq(lock.LOCK_LOCATION_CODE)
                    .and(loc.UNIT_SYSTEM.eq(lock.UNIT_SYSTEM)))
              .leftJoin(chamber)
              .on(chamber.CHAMBER_TYPE_CODE.eq(lock.CHAMBER_LOCATION_DESCRIPTION_CODE))
              .leftJoin(chamberOffice)
              .on(chamberOffice.OFFICE_CODE.eq(chamber.DB_OFFICE_CODE.cast(Long.class)))
              .where(lock.DB_OFFICE_ID.eq(projectId.getOfficeId())
                    .and(lock.PROJECT_ID.eq(projectId.getName()))
                    .and(lock.UNIT_SYSTEM.eq(unitSystem.value())))
              .orderBy(lock.LOCK_ID)
              .fetch(r -> catMap(r, projectId));
    }

    public Lock retrieveLock(CwmsId lockId, UnitSystem unitSystem) {
        return connectionResult(dsl, c -> {
            var configuration = getDslContext(c, lockId.getOfficeId()).configuration();
            LOCATION_REF_T locationRef = getLocationRef(CwmsId.buildCwmsId(lockId.getOfficeId(),
                    lockId.getName()));
            Lock retVal;
            if (unitSystem.equals(UnitSystem.EN)) {
                retVal = unitConvertToEN(map(CWMS_LOCK_PACKAGE.call_RETRIEVE_LOCK(configuration, locationRef)));
            } else {
                retVal = map(CWMS_LOCK_PACKAGE.call_RETRIEVE_LOCK(configuration, locationRef));
            }
            if (retVal == null) {
                throw new NotFoundException("Lock not found: " + lockId);
            }
            return retVal;
        });
    }

    public void storeLock(Lock lock, boolean failIfExists) {
        connection(dsl, c -> {
            setOffice(c, lock.getLocation().getOfficeId());
            CWMS_LOCK_PACKAGE.call_STORE_LOCK(DSL.using(c).configuration(), map(lock), formatBool(failIfExists));
        });
    }

    public void deleteLock(CwmsId lockId, DeleteRule deleteRule) {
        connection(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            CWMS_LOCK_PACKAGE.call_DELETE_LOCK(DSL.using(c).configuration(), lockId.getName(), deleteRule.getRule(),
                    lockId.getOfficeId());
        });
    }

    public void renameLock(CwmsId lockId, String newName) {
        connection(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            CWMS_LOCK_PACKAGE.call_RENAME_LOCK(DSL.using(c).configuration(), lockId.getName(), newName,
                    lockId.getOfficeId());
        });
    }

    static Lock catMap(Record r, CwmsId projectId) {
        AV_LOCK lock = AV_LOCK.AV_LOCK;
        AV_LOC loc = AV_LOC.AV_LOC;
        AT_LOCK_GATE_TYPE chamber = AT_LOCK_GATE_TYPE.AT_LOCK_GATE_TYPE;
        String officeId = r.get(lock.DB_OFFICE_ID);
        String lockId = r.get(loc.LOCATION_ID);
        Location lockLoc = new Location.Builder(lockId, r.get(loc.LOCATION_KIND_ID),
                ZoneIdHelper.parseZoneIdWithAliases(r.get(loc.TIME_ZONE_NAME)),
                buildDouble(r.get(loc.LATITUDE)), buildDouble(r.get(loc.LONGITUDE)),
                r.get(loc.HORIZONTAL_DATUM), officeId)
                .withActive(parseBool(r.get(loc.ACTIVE_FLAG)))
                .withDescription(r.get(loc.DESCRIPTION))
                .withElevation(r.get(loc.ELEVATION))
                .withElevationUnits(r.get(loc.UNIT_ID))
                .withCountyName(r.get(loc.COUNTY_NAME))
                .withBoundingOfficeId(r.get(loc.BOUNDING_OFFICE_ID))
                .withNation(Nation.nationForName(r.get(loc.NATION_ID)))
                .withMapLabel(r.get(loc.MAP_LABEL))
                .withPublicName(r.get(loc.PUBLIC_NAME))
                .withPublishedLatitude(buildDouble(r.get(loc.PUBLISHED_LATITUDE)))
                .withPublishedLongitude(buildDouble(r.get(loc.PUBLISHED_LONGITUDE)))
                .withVerticalDatum(r.get(loc.VERTICAL_DATUM))
                .withLongName(r.get(loc.LONG_NAME))
                .withStateInitial(r.get(loc.STATE_INITIAL))
                .withLocationType(r.get(loc.LOCATION_TYPE))
                .withNearestCity(r.get(loc.NEAREST_CITY))
                .build();
        String baseLocationId = lockId.contains("-") ? lockId.substring(0, lockId.indexOf('-')) : lockId;
        String boundingOffice = r.get(loc.BOUNDING_OFFICE_ID);
        String levelOffice = boundingOffice != null ? boundingOffice : officeId;
        LookupType chamberType = null;
        if (r.get(chamber.CHAMBER_TYPE_DISPLAY_VALUE) != null) {
            chamberType = new LookupType.Builder()
                    .withOfficeId(r.get(CWMS_OFFICE.CWMS_OFFICE.OFFICE_ID))
                    .withDisplayValue(r.get(chamber.CHAMBER_TYPE_DISPLAY_VALUE))
                    .withTooltip(r.get(chamber.CHAMBER_TYPE_TOOLTIP))
                    .withActive(parseBool(r.get(chamber.CHAMBER_TYPE_ACTIVE)))
                    .build();
        }
        Double maxLift = r.get(lock.MAXIMUM_LOCK_LIFT);
        return new Lock.Builder()
                .withLocation(lockLoc)
                .withProjectId(projectId)
                .withChamberType(chamberType)
                .withLockLength(nullToZero(r.get(lock.LOCK_LENGTH)))
                .withLockWidth(nullToZero(r.get(lock.LOCK_WIDTH)))
                .withNormalLockLift(nullToZero(r.get(lock.NORMAL_LOCK_LIFT)))
                .withVolumePerLockage(nullToZero(r.get(lock.VOLUME_PER_LOCKAGE)))
                .withMinimumDraft(nullToZero(r.get(lock.MINIMUM_DRAFT)))
                .withMaximumLockLift(maxLift == null ? -9999 : maxLift)
                .withLengthUnits(r.get(lock.LENGTH_UNIT_ID))
                .withVolumeUnits(r.get(lock.VOLUME_UNIT_ID))
                .withElevationUnits(r.get(lock.ELEV_UNIT_ID))
                .withHighWaterUpperPoolWarningLevel(r.get(lock.ELEV_CLOSURE_HIGH_WATER_UPPER_POOL_WARNING))
                .withHighWaterLowerPoolWarningLevel(r.get(lock.ELEV_CLOSURE_HIGH_WATER_LOWER_POOL_WARNING))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(levelOffice, baseLocationId + ".Elev-Closure.Inst.0.High Water Upper Pool"),
                        r.get(lock.ELEV_CLOSURE_HIGH_WATER_UPPER_POOL)))
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(levelOffice, baseLocationId + ".Elev-Closure.Inst.0.High Water Lower Pool"),
                        r.get(lock.ELEV_CLOSURE_HIGH_WATER_LOWER_POOL)))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(levelOffice, baseLocationId + ".Elev-Closure.Inst.0.Low Water Upper Pool"),
                        r.get(lock.ELEV_CLOSURE_LOW_WATER_UPPER_POOL)))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(levelOffice, baseLocationId + ".Elev-Closure.Inst.0.Low Water Lower Pool"),
                        r.get(lock.ELEV_CLOSURE_LOW_WATER_LOWER_POOL)))
                .build();
    }

    private static double nullToZero(Double value) {
        return value == null ? 0.0 : value;
    }

    static CwmsId map(Record r) {
        String officeId = r.getValue("DB_OFFICE_ID", String.class);
        String baseLocationId = r.getValue("PROJECT_ID", String.class);
        String subLocationId = r.getValue("LOCK_ID", String.class);
        return CwmsId.buildCwmsId(officeId, baseLocationId + "-" + subLocationId);
    }

    static LOCK_OBJ_T map(Lock lock) {
        LOCK_OBJ_T retval = new LOCK_OBJ_T();
        retval.setLOCK_LOCATION(LocationUtil.getLocation(lock.getLocation()));
        retval.setPROJECT_LOCATION_REF(getLocationRef(lock.getProjectId()));
        retval.setLOCK_WIDTH(lock.getLockWidth());
        retval.setLOCK_LENGTH(lock.getLockLength());
        retval.setNORMAL_LOCK_LIFT(lock.getNormalLockLift());
        retval.setVOLUME_PER_LOCKAGE(lock.getVolumePerLockage());
        retval.setMINIMUM_DRAFT(lock.getMinimumDraft());
        retval.setUNITS_ID(lock.getLengthUnits());
        retval.setVOLUME_UNITS_ID(lock.getVolumeUnits());
        retval.setELEV_UNITS_ID(lock.getElevationUnits());
        retval.setCHAMBER_LOCATION_DESCRIPTION(getLookupType(lock.getChamberType()));
        retval.setELEV_CLOSURE_HIGH_WATER_LOWER_POOL(lock.getHighWaterLowerPoolLocationLevel() == null
                ? null : lock.getHighWaterLowerPoolLocationLevel().getLevelValue());
        retval.setELEV_CLOSURE_HIGH_WATER_UPPER_POOL(lock.getHighWaterUpperPoolLocationLevel() == null
                ? null : lock.getHighWaterUpperPoolLocationLevel().getLevelValue());
        retval.setELEV_CLOSURE_LOW_WATER_LOWER_POOL(lock.getLowWaterLowerPoolLocationLevel() == null
                ? null : lock.getLowWaterLowerPoolLocationLevel().getLevelValue());
        retval.setELEV_CLOSURE_LOW_WATER_UPPER_POOL(lock.getLowWaterUpperPoolLocationLevel() == null
                ? null : lock.getLowWaterUpperPoolLocationLevel().getLevelValue());
        retval.setMAXIMUM_LOCK_LIFT(lock.getMaximumLockLift());
        retval.setELEV_CLOSURE_HIGH_WATER_LOWER_POOL_WARNING(lock.getHighWaterLowerPoolWarningLevel());
        retval.setELEV_CLOSURE_HIGH_WATER_UPPER_POOL_WARNING(lock.getHighWaterUpperPoolWarningLevel());
        return retval;
    }

    static Lock map(LOCK_OBJ_T lock) {
        if (lock == null) {
            return null;
        }
        return new Lock.Builder()
                .withLocation(LocationUtil.getLocation(lock.getLOCK_LOCATION()))
                .withProjectId(LocationUtil.getLocationIdentifier(lock.getPROJECT_LOCATION_REF()))
                .withLockLength(lock.getLOCK_LENGTH())
                .withLockWidth(lock.getLOCK_WIDTH())
                .withNormalLockLift(lock.getNORMAL_LOCK_LIFT())
                .withVolumePerLockage(lock.getVOLUME_PER_LOCKAGE())
                .withMinimumDraft(lock.getMINIMUM_DRAFT())
                .withLengthUnits(lock.getUNITS_ID())
                .withVolumeUnits(lock.getVOLUME_UNITS_ID())
                .withElevationUnits(lock.getELEV_UNITS_ID())
                .withHighWaterLowerPoolWarningLevel(lock.getELEV_CLOSURE_HIGH_WATER_LOWER_POOL_WARNING())
                .withHighWaterUpperPoolWarningLevel(lock.getELEV_CLOSURE_HIGH_WATER_UPPER_POOL_WARNING())
                .withChamberType(getLookupType(lock.getCHAMBER_LOCATION_DESCRIPTION()))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                String.format("%s.Elev-Closure.Inst.0.High Water Upper Pool",
                                        lock.getLOCK_LOCATION().getLOCATION_REF().getBASE_LOCATION_ID())),
                        lock.getELEV_CLOSURE_HIGH_WATER_UPPER_POOL()))
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                String.format("%s.Elev-Closure.Inst.0.High Water Lower Pool",
                                        lock.getLOCK_LOCATION().getLOCATION_REF().getBASE_LOCATION_ID())),
                        lock.getELEV_CLOSURE_HIGH_WATER_LOWER_POOL()))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                String.format("%s.Elev-Closure.Inst.0.Low Water Lower Pool",
                                        lock.getLOCK_LOCATION().getLOCATION_REF().getBASE_LOCATION_ID())),
                        lock.getELEV_CLOSURE_LOW_WATER_LOWER_POOL()))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                String.format("%s.Elev-Closure.Inst.0.Low Water Upper Pool",
                                        lock.getLOCK_LOCATION().getLOCATION_REF().getBASE_LOCATION_ID())),
                        lock.getELEV_CLOSURE_LOW_WATER_UPPER_POOL()))
                .withMaximumLockLift(lock.getMAXIMUM_LOCK_LIFT() == null ? -9999 : lock.getMAXIMUM_LOCK_LIFT())
                .build();
    }

    static String mapToLockRef(String office, String locationName) {
        return String.format("/locks/%s?office=%s", locationName, office);
    }

    Lock unitConvertToEN(Lock lock) {
        return connectionResult(dsl, c -> {
            var configuration = getDslContext(c, lock.getLocation().getOfficeId()).configuration();
            String length = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(configuration, "Length", "EN");
            String volume = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(configuration, "Volume", "EN");
            return new Lock.Builder()
                .withElevationUnits(length)
                .withVolumeUnits(volume)
                .withLengthUnits(length)
                .withLocation(lock.getLocation())
                .withProjectId(lock.getProjectId())
                .withLockWidth(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getLockWidth(),
                        lock.getLengthUnits(), length))
                .withLockLength(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getLockLength(),
                        lock.getLengthUnits(), length))
                .withNormalLockLift(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getNormalLockLift(),
                        lock.getLengthUnits(), length))
                .withVolumePerLockage(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration,
                        lock.getVolumePerLockage(), lock.getVolumeUnits(), volume))
                .withMinimumDraft(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getMinimumDraft(),
                        lock.getElevationUnits(), length))
                .withHighWaterLowerPoolWarningLevel(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration,
                        lock.getHighWaterLowerPoolWarningLevel(), lock.getElevationUnits(), length))
                .withHighWaterUpperPoolWarningLevel(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration,
                        lock.getHighWaterUpperPoolWarningLevel(), lock.getElevationUnits(), length))
                .withChamberType(lock.getChamberType())
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(lock.getHighWaterLowerPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getHighWaterLowerPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(lock.getHighWaterUpperPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getHighWaterUpperPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(lock.getLowWaterLowerPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getLowWaterLowerPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(lock.getLowWaterUpperPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getLowWaterUpperPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withMaximumLockLift(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(configuration, lock.getMaximumLockLift(),
                        lock.getElevationUnits(), length))
                .build();
        });
    }
}
