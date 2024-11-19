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

import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationRef;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLookupType;
import static java.util.stream.Collectors.toList;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import java.time.ZoneId;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOCK_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCK_OBJ_T;

public final class LockDao extends JooqDao<Lock> {

    public LockDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Lock> retrieveLockCatalog(CwmsId projectId) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectId.getOfficeId());
            Result<Record> catalogResults = CWMS_LOCK_PACKAGE.call_CAT_LOCK(dsl.configuration(),
                    projectId.getName(), projectId.getOfficeId());
            return catalogResults.stream().map(LockDao::catMap).collect(toList());
        });
    }

    public Lock retrieveLock(CwmsId lockId, UnitSystem unitSystem) {
        return connectionResult(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            LOCATION_REF_T locationRef = getLocationRef(CwmsId.buildCwmsId(lockId.getOfficeId(),
                    lockId.getName()));
            Lock retVal;
            if (unitSystem.equals(UnitSystem.EN)) {
                retVal = unitConvertToEN(map(CWMS_LOCK_PACKAGE.call_RETRIEVE_LOCK(dsl.configuration(), locationRef)));
            } else {
                retVal = map(CWMS_LOCK_PACKAGE.call_RETRIEVE_LOCK(dsl.configuration(), locationRef));
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

    static Lock catMap(Record r) {
        String officeId = r.getValue("DB_OFFICE_ID", String.class);
        String baseLocationId = r.getValue("BASE_LOCATION_ID", String.class);
        String subLocationId = r.getValue("SUB_LOCATION_ID", String.class);
        String projectOfficeId = r.getValue("PROJECT_OFFICE_ID", String.class);
        String projectLocationId = r.getValue("PROJECT_LOCATION_ID", String.class);
        String timeZone = r.getValue("TIME_ZONE_NAME", String.class);
        double latitude = r.getValue("LATITUDE", Double.class);
        double longitude = r.getValue("LONGITUDE", Double.class);
        String horizDatum = r.getValue("HORIZONTAL_DATUM", String.class);
        double elevation = r.getValue("ELEVATION", Double.class);
        String elevUnit = r.getValue("ELEVATION_UNIT_ID", String.class);
        String verticalDatum = r.getValue("VERTICAL_DATUM", String.class);
        String publicName = r.getValue("PUBLIC_NAME", String.class);
        String longName = r.getValue("LONG_NAME", String.class);
        String description = r.getValue("DESCRIPTION", String.class);
        boolean active = r.getValue("ACTIVE_FLAG", Boolean.class);
        String lockId;
        if (subLocationId == null) {
            lockId = baseLocationId;
        } else {
            lockId = baseLocationId + "-" + subLocationId;
        }
        Location lockLoc = new Location.Builder(officeId, lockId)
                .withLocationKind("LOCK")
                .withTimeZoneName(ZoneId.of(timeZone))
                .withLatitude(latitude)
                .withLongitude(longitude)
                .withLongName(longName)
                .withDescription(description)
                .withActive(active)
                .withElevation(elevation)
                .withElevationUnits(elevUnit)
                .withHorizontalDatum(horizDatum)
                .withVerticalDatum(verticalDatum)
                .withPublicName(publicName)
                .build();
        return new Lock.Builder()
                .withLocation(lockLoc)
                .withProjectId(CwmsId.buildCwmsId(projectOfficeId, projectLocationId))
                .build();
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
            setOffice(c, lock.getLocation().getOfficeId());

            String length = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(dsl.configuration(), "Length", "EN");
            String volume = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(dsl.configuration(), "Volume", "EN");
            return new Lock.Builder()
                .withElevationUnits(length)
                .withVolumeUnits(volume)
                .withLengthUnits(length)
                .withLocation(lock.getLocation())
                .withProjectId(lock.getProjectId())
                .withLockWidth(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getLockWidth(),
                        lock.getLengthUnits(), length))
                .withLockLength(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getLockLength(),
                        lock.getLengthUnits(), length))
                .withNormalLockLift(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getNormalLockLift(),
                        lock.getLengthUnits(), length))
                .withVolumePerLockage(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(),
                        lock.getVolumePerLockage(), lock.getVolumeUnits(), volume))
                .withMinimumDraft(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getMinimumDraft(),
                        lock.getElevationUnits(), length))
                .withHighWaterLowerPoolWarningLevel(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(),
                        lock.getHighWaterLowerPoolWarningLevel(), lock.getElevationUnits(), length))
                .withHighWaterUpperPoolWarningLevel(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(),
                        lock.getHighWaterUpperPoolWarningLevel(), lock.getElevationUnits(), length))
                .withChamberType(lock.getChamberType())
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(lock.getHighWaterLowerPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getHighWaterLowerPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(lock.getHighWaterUpperPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getHighWaterUpperPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(lock.getLowWaterLowerPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getLowWaterLowerPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(lock.getLowWaterUpperPoolLocationLevel().getLevelLink(),
                        CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getLowWaterUpperPoolLocationLevel().getLevelValue(),
                                lock.getElevationUnits(), length)))
                .withMaximumLockLift(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getMaximumLockLift(),
                        lock.getElevationUnits(), length))
                .build();
        });
    }
}
