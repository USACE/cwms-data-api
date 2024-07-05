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


import static cwms.cda.data.dao.location.kind.LocationUtil.getLocation;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationIdentifier;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationRef;
import static java.util.stream.Collectors.toList;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.PhysicalStructureChange;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineSetting;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TURBINE_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_STRUCTURE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.TURBINE_CHANGE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.TURBINE_CHANGE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.TURBINE_SETTING_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.TURBINE_SETTING_TAB_T;

public class TurbineDao extends JooqDao<Turbine> {

    public TurbineDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Turbine> retrieveTurbines(String projectLocationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(projectLocationId, officeId);
            return CWMS_TURBINE_PACKAGE.call_RETRIEVE_TURBINES(DSL.using(conn).configuration(), locationRefT)
                .stream()
                .map(TurbineDao::map)
                .collect(toList());
        });
    }

    public Turbine retrieveTurbine(String locationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(locationId, officeId);
            Configuration configuration = DSL.using(conn).configuration();
            PROJECT_STRUCTURE_OBJ_T turbineObjT =
                CWMS_TURBINE_PACKAGE.call_RETRIEVE_TURBINE(configuration, locationRefT);
            if (turbineObjT == null) {
                throw new NotFoundException("Turbine: " + officeId + "." + locationId + " not found");
            }
            return map(turbineObjT);
        });
    }

    static Turbine map(PROJECT_STRUCTURE_OBJ_T turbine) {
        return new Turbine.Builder()
            .withProjectId(getLocationIdentifier(turbine.getPROJECT_LOCATION_REF()))
            .withLocation(getLocation(turbine.getSTRUCTURE_LOCATION()))
            .build();
    }

    static PROJECT_STRUCTURE_OBJ_T map(Turbine turbine) {
        PROJECT_STRUCTURE_OBJ_T retval = new PROJECT_STRUCTURE_OBJ_T();
        retval.setPROJECT_LOCATION_REF(getLocationRef(turbine.getProjectId()));
        retval.setSTRUCTURE_LOCATION(getLocation(turbine.getLocation()));
        return retval;
    }

    public void storeTurbine(Turbine turbine, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, turbine.getLocation().getOfficeId());
            CWMS_TURBINE_PACKAGE.call_STORE_TURBINE(DSL.using(conn).configuration(), map(turbine),
                OracleTypeMap.formatBool(failIfExists));
        });
    }

    public void deleteTurbine(String locationId, String officeId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TURBINE_PACKAGE.call_DELETE_TURBINE(DSL.using(conn).configuration(), locationId,
                deleteRule.getRule(), officeId);
        });
    }

    public void renameTurbine(String officeId, String oldId, String newId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TURBINE_PACKAGE.call_RENAME_TURBINE(DSL.using(conn).configuration(), oldId,
                newId, officeId);
        });
    }

    public List<PhysicalStructureChange<TurbineSetting>> retrieveOperationalChanges(CwmsId projectId, Instant startTime,
        Instant endTime, boolean startInclusive, boolean endInclusive, String unitSystem, long rowLimit) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, projectId.getOfficeId());
            LOCATION_REF_T locationRef = getLocationRef(projectId);
            Timestamp startTimestamp = Timestamp.from(startTime);
            Timestamp endTimestamp = Timestamp.from(endTime);
            BigInteger rowLimitBig = BigInteger.valueOf(rowLimit);
            TURBINE_CHANGE_TAB_T turbineChanges = CWMS_TURBINE_PACKAGE.call_RETRIEVE_TURBINE_CHANGES(
                DSL.using(conn).configuration(), locationRef, startTimestamp, endTimestamp,
                "UTC", unitSystem, OracleTypeMap.formatBool(startInclusive), OracleTypeMap.formatBool(endInclusive),
                rowLimitBig);
            List<PhysicalStructureChange<TurbineSetting>> retval = new ArrayList<>();
            if (turbineChanges != null) {
                turbineChanges.stream()
                    .map(this::map)
                    .forEach(retval::add);
            }
            return retval;
        });
    }

    public void storeOperationalChanges(List<PhysicalStructureChange<TurbineSetting>> physicalStructureChange,
        boolean overrideProtection) {
        if (physicalStructureChange.isEmpty()) {
            return;
        }
        connection(dsl, conn -> {
            setOffice(conn, physicalStructureChange.get(0).getOfficeId());
            TURBINE_CHANGE_TAB_T changes = new TURBINE_CHANGE_TAB_T();
            physicalStructureChange.stream()
                .map(this::map)
                .forEach(changes::add);
            CWMS_TURBINE_PACKAGE.call_STORE_TURBINE_CHANGES(dsl.configuration(), changes, null, null,
                "UTC", "T", "T",
                OracleTypeMap.formatBool(overrideProtection));
        });
    }

    public void deleteOperationalChanges(CwmsId projectId, Instant startTime,
        Instant endTime, boolean overrideProtection) {
        connection(dsl, conn -> {
            setOffice(conn, projectId.getOfficeId());
            String startInclusive = "T";
            String endInclusive = "T";
            LOCATION_REF_T locationRef = getLocationRef(projectId);
            Timestamp startTimestamp = Timestamp.from(startTime);
            Timestamp endTimestamp = Timestamp.from(endTime);
            CWMS_TURBINE_PACKAGE.call_DELETE_TURBINE_CHANGES(DSL.using(conn).configuration(), locationRef,
                startTimestamp, endTimestamp, "UTC", startInclusive, endInclusive,
                OracleTypeMap.formatBool(overrideProtection));
        });
    }

    private PhysicalStructureChange<TurbineSetting> map(TURBINE_CHANGE_OBJ_T change) {
        Set<TurbineSetting> settings = new HashSet<>();
        if (change.getSETTINGS() != null) {
            change.getSETTINGS().stream()
                .map(this::map)
                .forEach(settings::add);
        }
        return new PhysicalStructureChange.Builder<TurbineSetting>()
            .withTailwaterElevation(change.getELEV_TAILWATER())
            .withPoolElevation(change.getELEV_POOL())
            .withElevationUnits(change.getELEV_UNITS())
            .withChangeDate(change.getCHANGE_DATE().toInstant())
            .withNotes(change.getCHANGE_NOTES())
            .withDischargeUnits(change.getDISCHARGE_UNITS())
            .withNewTotalDischargeOverride(change.getNEW_TOTAL_DISCHARGE_OVERRIDE())
            .withOldTotalDischargeOverride(change.getOLD_TOTAL_DISCHARGE_OVERRIDE())
            .withProtected(OracleTypeMap.parseBool(change.getPROTECTED()))
            .withProjectId(LocationUtil.getLocationIdentifier(change.getPROJECT_LOCATION_REF()))
            .withDischargeComputationType(LocationUtil.getLookupType(change.getDISCHARGE_COMPUTATION()))
            .withReasonType(LocationUtil.getLookupType(change.getSETTING_REASON()))
            .withSettings(settings)
            .build();
    }

    private TURBINE_CHANGE_OBJ_T map(PhysicalStructureChange<TurbineSetting> change) {
        TURBINE_SETTING_TAB_T settings = new TURBINE_SETTING_TAB_T();
        change.getSettings().stream()
            .map(this::map)
            .forEach(settings::add);
        TURBINE_CHANGE_OBJ_T retval = new TURBINE_CHANGE_OBJ_T();
        retval.setELEV_TAILWATER(change.getTailwaterElevation());
        retval.setELEV_POOL(change.getPoolElevation());
        retval.setELEV_UNITS(change.getElevationUnits());
        retval.setCHANGE_DATE(Timestamp.from(change.getChangeDate()));
        retval.setCHANGE_NOTES(change.getNotes());
        retval.setDISCHARGE_UNITS(change.getDischargeUnits());
        retval.setNEW_TOTAL_DISCHARGE_OVERRIDE(change.getNewTotalDischargeOverride());
        retval.setOLD_TOTAL_DISCHARGE_OVERRIDE(change.getOldTotalDischargeOverride());
        retval.setPROTECTED(OracleTypeMap.formatBool(change.isProtected()));
        retval.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(change.getProjectId()));
        retval.setDISCHARGE_COMPUTATION(LocationUtil.getLookupType(change.getDischargeComputationType()));
        retval.setSETTING_REASON(LocationUtil.getLookupType(change.getReasonType()));
        retval.setSETTINGS(settings);
        return retval;
    }

    private TurbineSetting map(TURBINE_SETTING_OBJ_T setting) {
        return new TurbineSetting.Builder()
            .withLocationId(LocationUtil.getLocationIdentifier(setting.getTURBINE_LOCATION_REF()))
            .withGenerationUnits(setting.getGENERATION_UNITS())
            .withRealPower(setting.getREAL_POWER())
            .withScheduledLoad(setting.getSCHEDULED_LOAD())
            .withDischargeUnits(setting.getDISCHARGE_UNITS())
            .withOldDischarge(setting.getOLD_DISCHARGE())
            .withNewDischarge(setting.getNEW_DISCHARGE())
            .build();
    }

    private TURBINE_SETTING_OBJ_T map(TurbineSetting setting) {
        TURBINE_SETTING_OBJ_T retval = new TURBINE_SETTING_OBJ_T();
        retval.setTURBINE_LOCATION_REF(LocationUtil.getLocationRef(setting.getLocationId()));
        retval.setGENERATION_UNITS(setting.getGenerationUnits());
        retval.setREAL_POWER(setting.getRealPower());
        retval.setSCHEDULED_LOAD(setting.getScheduledLoad());
        retval.setDISCHARGE_UNITS(setting.getDischargeUnits());
        retval.setOLD_DISCHARGE(setting.getOldDischarge());
        retval.setNEW_DISCHARGE(setting.getNewDischarge());
        return retval;
    }
}
