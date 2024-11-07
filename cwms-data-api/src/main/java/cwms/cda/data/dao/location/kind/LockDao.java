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
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.val;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOCK_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOCK;
import usace.cwms.db.jooq.codegen.udt.records.LOCK_OBJ_T;

public final class LockDao extends JooqDao<Lock> {
    private static final AV_LOCK view = AV_LOCK.AV_LOCK;

    public LockDao(DSLContext dsl) {
        super(dsl);
    }

    public List<CwmsId> retrieveLockIds(CwmsId projectId) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectId.getOfficeId());
            Result<Record> catalogResults = CWMS_LOCK_PACKAGE.call_CAT_LOCK(dsl.configuration(),
                    projectId.getName(), projectId.getOfficeId());
            return catalogResults.stream().map(LockDao::catMap).collect(toList());
        });
    }

    public Lock retrieveLock(CwmsId lockId, UnitSystem units) {
        if (units == null) {
            units = UnitSystem.SI;
        }
        UnitSystem unitSystemFinal = units;
        return connectionResult(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            LocationsDao locationsDao = new LocationsDaoImpl(dsl);
            Record dbRecord = DSL.using(c)
                .select(asterisk())
                .from(view)
                .where(view.LOCK_ID.eq(lockId.getName()).and(view.DB_OFFICE_ID.eq(lockId.getOfficeId()))
                        .and(view.UNIT_SYSTEM.equalIgnoreCase(unitSystemFinal.name())))
                .fetchOne();
            if (dbRecord == null) {
                throw new NotFoundException("Lock not found: " + lockId);
            }

            Long chamberTypeCode = dbRecord.get(view.CHAMBER_LOCATION_DESCRIPTION_CODE);
            Table<?> chamberTable = table("CWMS_20.AT_LOCK_GATE_TYPE");
            Table<?> officeTable = table("CWMS_20.CWMS_OFFICE");
            SelectConditionStep<Record> chamberQuery = DSL.using(c).select(asterisk())
                    .from(chamberTable
                            .join(officeTable)
                            .on(field(name(chamberTable.getQualifiedName().unquotedName().toString(),
                                        "DB_OFFICE_CODE").unquotedName())
                                    .eq(field(name(officeTable.getQualifiedName().unquotedName().toString(),
                                            "OFFICE_CODE").unquotedName()))))
                    .where(field(name(chamberTable.getQualifiedName().unquotedName().toString(),
                                "CHAMBER_TYPE_CODE").unquotedName())
                            .eq(val(chamberTypeCode)));
            Record chamberResult = chamberQuery.fetchOne();
            Location lockLocation = locationsDao.getLocation(dbRecord.get(view.LOCK_ID),
                    "SI", dbRecord.get(view.DB_OFFICE_ID));
            if (chamberResult == null) {
                return map(dbRecord, lockLocation, null);
            }
            LookupType chamber = new LookupType.Builder()
                    .withOfficeId(chamberResult.get("OFFICE_ID", String.class))
                    .withActive(chamberResult.get("CHAMBER_TYPE_ACTIVE", Boolean.class))
                    .withTooltip(chamberResult.get("CHAMBER_TYPE_TOOLTIP", String.class))
                    .withDisplayValue(chamberResult.get("CHAMBER_TYPE_DISPLAY_VALUE", String.class))
                    .build();

            Lock retVal =  map(dbRecord, lockLocation, chamber);
            return unitConvert(retVal, unitSystemFinal);
        });
    }

    private Lock unitConvert(Lock lock, UnitSystem unitSystem) {
        // Converts minimum draft value to expected units, all other units come back as expected
        return connectionResult(dsl, c -> {
            setOffice(c, lock.getLocation().getOfficeId());
            String lengthDefaultUnit = unitSystem.getValue().equalsIgnoreCase("EN") ? "in" : "mm";
            String targetLengthUnits = lock.getLengthUnits();
            return new Lock.Builder()
                .withLocation(lock.getLocation())
                .withLengthUnits(targetLengthUnits)
                .withVolumePerLockage(lock.getVolumePerLockage())
                .withElevationUnits(lock.getElevationUnits())
                .withVolumeUnits(lock.getVolumeUnits())
                .withNormalLockLift(lock.getNormalLockLift())
                .withMaximumLockLift(lock.getMaximumLockLift())
                .withProjectId(lock.getProjectId())
                .withChamberType(lock.getChamberType())
                .withMinimumDraft(CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), lock.getMinimumDraft(),
                    lengthDefaultUnit, targetLengthUnits))
                .withLockLength(lock.getLockLength())
                .withHighWaterUpperPoolWarningLevel(lock.getHighWaterUpperPoolWarningLevel())
                .withHighWaterLowerPoolWarningLevel(lock.getHighWaterLowerPoolWarningLevel())
                .withHighWaterUpperPoolLocationLevel(lock.getHighWaterUpperPoolLocationLevel())
                .withHighWaterLowerPoolLocationLevel(lock.getHighWaterLowerPoolLocationLevel())
                .withLowWaterUpperPoolLocationLevel(lock.getLowWaterUpperPoolLocationLevel())
                .withLowWaterLowerPoolLocationLevel(lock.getLowWaterLowerPoolLocationLevel())
                .withLockWidth(lock.getLockWidth())
                .build();
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

    static CwmsId catMap(Record r) {
        String officeId = r.getValue("DB_OFFICE_ID", String.class);
        String baseLocationId = r.getValue("BASE_LOCATION_ID", String.class);
        String subLocationId = r.getValue("SUB_LOCATION_ID", String.class);
        if (subLocationId == null) {
            return CwmsId.buildCwmsId(officeId, baseLocationId);
        } else {
            return CwmsId.buildCwmsId(officeId, baseLocationId + "-" + subLocationId);
        }
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
        retval.setELEV_INOPERABLE_HIGH_WATER_LOWER_POOL(lock.getHighWaterLowerPoolLocationLevel() == null
                ? null : lock.getHighWaterLowerPoolLocationLevel().getLevelValue());
        retval.setELEV_INOPERABLE_HIGH_WATER_UPPER_POOL(lock.getHighWaterUpperPoolLocationLevel() == null
                ? null : lock.getHighWaterUpperPoolLocationLevel().getLevelValue());
        retval.setELEV_INOPERABLE_LOW_WATER_LOWER_POOL(lock.getLowWaterLowerPoolLocationLevel() == null
                ? null : lock.getLowWaterLowerPoolLocationLevel().getLevelValue());
        retval.setELEV_INOPERABLE_LOW_WATER_UPPER_POOL(lock.getLowWaterUpperPoolLocationLevel() == null
                ? null : lock.getLowWaterUpperPoolLocationLevel().getLevelValue());
        retval.setMAXIMUM_LOCK_LIFT(lock.getMaximumLockLift());
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
                // TODO : ADD Warning buffers once implemented into the DB
//                .withHighWaterLowerPoolWarningLevel()
//                .withHighWaterUpperPoolWarningLevel()
                .withChamberType(getLookupType(lock.getCHAMBER_LOCATION_DESCRIPTION()))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                makeLevelID(lock.getPROJECT_LOCATION_REF().getBASE_LOCATION_ID(), true, true)),
                        lock.getELEV_INOPERABLE_HIGH_WATER_UPPER_POOL()))
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                makeLevelID(lock.getPROJECT_LOCATION_REF().getBASE_LOCATION_ID(), true, false)),
                        lock.getELEV_INOPERABLE_HIGH_WATER_LOWER_POOL()))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                makeLevelID(lock.getPROJECT_LOCATION_REF().getBASE_LOCATION_ID(), false, false)),
                        lock.getELEV_INOPERABLE_LOW_WATER_LOWER_POOL()))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(
                        mapToLockRef(lock.getLOCK_LOCATION().getBOUNDING_OFFICE_ID(),
                                makeLevelID(lock.getPROJECT_LOCATION_REF().getBASE_LOCATION_ID(), false, true)),
                        lock.getELEV_INOPERABLE_LOW_WATER_UPPER_POOL()))
                .withMaximumLockLift(lock.getMAXIMUM_LOCK_LIFT() == null ? -9999 : lock.getMAXIMUM_LOCK_LIFT())
                .build();
    }

    static Lock map(Record result, Location lockLocation, LookupType chamberType) {
        CwmsId projectId = CwmsId.buildCwmsId(result.get(view.DB_OFFICE_ID), result.get(view.PROJECT_ID));
        return new Lock.Builder()
                .withLocation(lockLocation)
                .withProjectId(projectId)
                .withLockLength(result.get(view.LOCK_LENGTH, Double.class))
                .withLockWidth(result.get(view.LOCK_WIDTH, Double.class))
                .withNormalLockLift(result.get(view.NORMAL_LOCK_LIFT, Double.class))
                .withVolumePerLockage(result.get(view.VOLUME_PER_LOCKAGE, Double.class))
                .withMinimumDraft(result.get(view.MINIMUM_DRAFT, Double.class))
                .withLengthUnits(result.get(view.LENGTH_UNIT_ID))
                .withVolumeUnits(result.get(view.VOLUME_UNIT_ID))
                .withHighWaterLowerPoolWarningLevel(result.get(view.ELEV_INOPERABLE_HIGH_WATER_LOWER_POOL_WARNING,
                        Double.class))
                .withHighWaterUpperPoolWarningLevel(result.get(view.ELEV_INOPERABLE_HIGH_WATER_UPPER_POOL_WARNING,
                        Double.class))
                .withChamberType(chamberType)
                .withElevationUnits(result.get(view.ELEV_UNIT_ID))
                .withVolumeUnits(result.get(view.VOLUME_UNIT_ID))
                .withMaximumLockLift(result.get(view.MAXIMUM_LOCK_LIFT, Double.class))
                .withLowWaterUpperPoolLocationLevel(new LockLocationLevelRef(mapToLockRef(lockLocation.getOfficeId(),
                        makeLevelID(lockLocation.getName(), false, true)),
                    result.get(view.ELEV_INOPERABLE_LOW_WATER_UPPER_POOL, Double.class)))
                .withLowWaterLowerPoolLocationLevel(new LockLocationLevelRef(mapToLockRef(lockLocation.getOfficeId(),
                        makeLevelID(lockLocation.getName(), false, false)),
                    result.get(view.ELEV_INOPERABLE_LOW_WATER_LOWER_POOL, Double.class)))
                .withHighWaterLowerPoolLocationLevel(new LockLocationLevelRef(mapToLockRef(lockLocation.getOfficeId(),
                        makeLevelID(lockLocation.getName(), true, false)),
                    result.get(view.ELEV_INOPERABLE_HIGH_WATER_LOWER_POOL, Double.class)))
                .withHighWaterUpperPoolLocationLevel(new LockLocationLevelRef(mapToLockRef(lockLocation.getOfficeId(),
                        makeLevelID(lockLocation.getName(), true, true)),
                    result.get(view.ELEV_INOPERABLE_HIGH_WATER_UPPER_POOL, Double.class)))
                .build();
    }

    private static String makeLevelID(String location, boolean high, boolean upper) {
        if (high) {
            if (upper) {
                return String.format("%s.Elev-Inoperable.Inst.0.High Water Upper Pool", location);
            } else {
                return String.format("%s.Elev-Inoperable.Inst.0.High Water Lower Pool", location);
            }
        } else {
            if (upper) {
                return String.format("%s.Elev-Inoperable.Inst.0.Low Water Upper Pool", location);
            } else {
                return String.format("%s.Elev-Inoperable.Inst.0.Low Water Lower Pool", location);
            }
        }
    }

    static String mapToLockRef(String office, String locationName) {
        return String.format("/locks/%s?office=%s", locationName, office);
    }
}
