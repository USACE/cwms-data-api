/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.GateSetting;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.data.dto.location.kind.ProjectStructure;
import cwms.cda.data.dto.location.kind.VirtualOutlet;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_OUTLET_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.GATE_CHANGE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.GATE_CHANGE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.GATE_SETTING_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.GATE_SETTING_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_STRUCTURE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_TAB_T;
import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationRef;

public class OutletDao extends JooqDao<Outlet> {

    public OutletDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Outlet> retrieveOutletsForProject(String officeId, String projectId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            Configuration config = DSL.using(conn).configuration();
            LOCATION_REF_T locRef = LocationUtil.getLocationRef(projectId, officeId);

            LocationGroupDao locGroupDao = new LocationGroupDao(dsl);
            List<LocationGroup> groups = locGroupDao.getLocationGroups(officeId,
                    Outlet.RATING_LOC_GROUP_CATEGORY, projectId);

            return CWMS_OUTLET_PACKAGE.call_RETRIEVE_OUTLETS(config, locRef)
                                      .stream()
                                      .map(struct -> mapToOutlet(struct, groups))
                                      .collect(Collectors.toList());
        });
    }

    public Outlet retrieveOutlet(String officeId, String locationId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locRef = LocationUtil.getLocationRef(locationId, officeId);
            Configuration config = DSL.using(conn).configuration();
            PROJECT_STRUCTURE_OBJ_T outletStruct = CWMS_OUTLET_PACKAGE.call_RETRIEVE_OUTLET(config, locRef);
            
            LocationGroupDao locGroupDao = new LocationGroupDao(dsl);
            List<LocationGroup> groups = locGroupDao.getLocationGroups(officeId, Outlet.RATING_LOC_GROUP_CATEGORY);

            return mapToOutlet(outletStruct, groups);
        });
    }

    private Optional<LocationGroup> getRatingGroup(String locationId, List<LocationGroup> groups) {
        return groups.stream()
                     .filter(group -> group.getAssignedLocations()
                                           .stream()
                                           .map(AssignedLocation::getLocationId)
                                           .anyMatch(id -> id.equalsIgnoreCase(locationId)))
                     .findFirst();
    }

    public void storeOutlet(Outlet outlet, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, outlet.getProjectId().getOfficeId());
            PROJECT_STRUCTURE_OBJ_T structure = mapToProjectStructure(outlet);
            CWMS_OUTLET_PACKAGE.call_STORE_OUTLET(DSL.using(conn).configuration(), structure, 
                    outlet.getRatingGroupId().getName(), formatBool(failIfExists));
        });
    }

    public void deleteOutlet(String officeId, String locationId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_OUTLET_PACKAGE.call_DELETE_OUTLET(DSL.using(conn).configuration(), locationId, deleteRule.getRule(),
                                                   officeId);
        });
    }

    public List<VirtualOutlet> retrieveVirtualOutletsForProject(String officeId, String projectId) {
        return connectionResult(dsl, conn -> {
            Configuration config = DSL.using(conn).configuration();
            List<VirtualOutlet> output = new ArrayList<>();
            //projectId and officeId are used as a mask in RETRIEVE_COMPOUND_OUTLETS,
            // however this usage expects a specific id, since retrieveOutlets does not use a mask.
            STR_TAB_TAB_T tabs = CWMS_OUTLET_PACKAGE.call_RETRIEVE_COMPOUND_OUTLETS(config, projectId, officeId);

            if (!tabs.isEmpty()) {
                STR_TAB_T tab = tabs.get(0);
                //This table has a minimum of two columns:  office, and project location
                //Everything else after that is a virtual outlet id.  This requires an additional retrieval to determine
                //the actual virtual outlet records for each outlet.
                for (int i = 2; i < tab.size(); i++) {
                    String virtualOutletId = tab.get(i);
                    VirtualOutlet virtualOutlet = retrieveVirtualOutlet(config, virtualOutletId, projectId,
                                                                        officeId);
                    output.add(virtualOutlet);
                }
            }

            return output;
        });
    }

    public VirtualOutlet retrieveVirtualOutlet(String officeId, String projectId, String virtualOutletId) {
        return connectionResult(dsl, conn -> {
            Configuration config = DSL.using(conn).configuration();
            return retrieveVirtualOutlet(config, virtualOutletId, projectId, officeId);
        });
    }

    private VirtualOutlet retrieveVirtualOutlet(Configuration config, String virtualOutletId, String projectId,
                                                String officeId) {
        List<VirtualOutletRecord> outletRecords = CWMS_OUTLET_PACKAGE
                .call_RETRIEVE_COMPOUND_OUTLET(config, virtualOutletId, projectId, officeId)
                                                               .stream()
                                                               .map(ArrayList::new)
                                                               .map(table -> mapToVirtualRecord(table, officeId))
                                                               .collect(Collectors.toList());
        CwmsId.Builder builder = new CwmsId.Builder().withOfficeId(officeId);

        return new VirtualOutlet.Builder().withVirtualOutletId(builder.withName(virtualOutletId).build())
                                          .withProjectId(builder.withName(projectId).build())
                                          .withVirtualRecords(outletRecords)
                                          .build();
    }

    public void storeVirtualOutlet(VirtualOutlet outlet, boolean failIfExists) {
        List<List<String>> virtualOutlets = outlet.getVirtualRecords()
                                                  .stream()
                                                  .map(this::mapVirtualRecords)
                                                  .collect(Collectors.toList());

        STR_TAB_TAB_T outlets = new STR_TAB_TAB_T(
                virtualOutlets.stream().map(STR_TAB_T::new).collect(Collectors.toList()));

        CwmsId projectId = outlet.getProjectId();
        CwmsId outletId = outlet.getVirtualOutletId();

        connection(dsl, conn -> {
            setOffice(conn, outlet.getProjectId().getOfficeId());
            CWMS_OUTLET_PACKAGE.call_STORE_COMPOUND_OUTLET(DSL.using(conn).configuration(), projectId.getName(),
                                                           outletId.getName(), outlets,
                                                           formatBool(failIfExists),
                                                           projectId.getOfficeId());
        });
    }

    public void deleteVirtualOutlet(String officeId, String projectId, String virtualOutletId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_OUTLET_PACKAGE.call_DELETE_COMPOUND_OUTLET(DSL.using(conn).configuration(), projectId,
                                                            virtualOutletId, deleteRule.getRule(), officeId);
        });
    }

    private Outlet mapToOutlet(PROJECT_STRUCTURE_OBJ_T outlet,
                               List<LocationGroup> groups) {
        Location location = LocationUtil.getLocation(outlet.getSTRUCTURE_LOCATION());
        Optional<LocationGroup> locGroupReturn = getRatingGroup(location.getName(), groups);
        CwmsId ratingGroupId = locGroupReturn.map(id -> new CwmsId.Builder().withName(id.getId())
                                                                         .withOfficeId(id.getOfficeId())
                                                                         .build())
                                             .orElse(null);
        String sharedLocAliasId = locGroupReturn.map(LocationGroup::getSharedLocAliasId)
                                                .orElse(null);
        CwmsId projectId = LocationUtil.getLocationIdentifier(outlet.getPROJECT_LOCATION_REF());

        return new Outlet.Builder().withLocation(location)
                                   .withProjectId(projectId)
                                   .withRatingGroupId(ratingGroupId)
                                   .withRatingSpecId(sharedLocAliasId)
                                   .build();
    }

    private PROJECT_STRUCTURE_OBJ_T mapToProjectStructure(ProjectStructure outlet) {
        PROJECT_STRUCTURE_OBJ_T output = new PROJECT_STRUCTURE_OBJ_T();
        output.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(outlet.getProjectId()));
        output.setSTRUCTURE_LOCATION(LocationUtil.getLocation(outlet.getLocation()));
        return output;
    }

    private VirtualOutletRecord mapToVirtualRecord(List<String> table, String officeId) {
        List<CwmsId> downstreamOutlets = new ArrayList<>();
        String outlet = null;
        for (int i = 1; i < table.size(); i++) {
            String downstreamOutlet = table.get(i);
            if (downstreamOutlet != null) {
                downstreamOutlets.add(new CwmsId.Builder().withName(downstreamOutlet).withOfficeId(officeId).build());
            }
        }
        if (!table.isEmpty()) {
            outlet = table.get(0);
        }
        return new VirtualOutletRecord.Builder().withOutletId(new CwmsId.Builder().withName(outlet)
                                                                 .withOfficeId(officeId).build())
                                                .withDownstreamOutletIds(downstreamOutlets)
                                                .build();
    }

    private List<String> mapVirtualRecords(VirtualOutletRecord outletRecord) {
        List<String> output = outletRecord.getDownstreamOutletIds()
                                          .stream()
                                          .map(CwmsId::getName)
                                          .collect(Collectors.toList());
        output.add(0, outletRecord.getOutletId().getName());
        return output;
    }

    public void renameOutlet(String officeId, String oldOutletId, String newOutletId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_OUTLET_PACKAGE.call_RENAME_OUTLET(DSL.using(conn).configuration(), oldOutletId, newOutletId, officeId);
        });
    }

    public List<GateChange> retrieveOperationalChanges(CwmsId projectId, Instant startTime, Instant endTime,
                                                       boolean startInclusive, boolean endInclusive, UnitSystem unitSystem,
                                                       long rowLimit) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, projectId.getOfficeId());

            LOCATION_REF_T locationRef = getLocationRef(projectId);
            Timestamp startTimestamp = Timestamp.from(startTime);
            Timestamp endTimestamp = Timestamp.from(endTime);
            BigInteger rowLimitBig = BigInteger.valueOf(rowLimit);
            GATE_CHANGE_TAB_T changeTab = CWMS_OUTLET_PACKAGE.call_RETRIEVE_GATE_CHANGES(
                    DSL.using(conn).configuration(), locationRef, startTimestamp, endTimestamp, "UTC", unitSystem.getValue(),
                    formatBool(startInclusive), formatBool(endInclusive), rowLimitBig);

            List<GateChange> output = new ArrayList<>();
            if (changeTab == null) {
                throw new NotFoundException("No operational changes found at " + projectId + " for time " + startTime +
                                                    " to " + endTime + ".\n" +
                                                    "Start inclusive: " + startInclusive + "\n" +
                                                    "End inclusive: " + endInclusive + "\n" +
                                                    "Unit system: " + unitSystem + "\n" +
                                                    "Row limit: " + rowLimit);
            }
            changeTab.stream().map(OutletDao::map).forEach(output::add);
            return output;
        });
    }

    public void storeOperationalChanges(List<GateChange> physicalStructureChange, boolean overrideProtection) {
        if (physicalStructureChange.isEmpty()) {
            return;
        }
        connection(dsl, conn -> {
            setOffice(conn, physicalStructureChange.get(0).getProjectId().getOfficeId());
            GATE_CHANGE_TAB_T changes = new GATE_CHANGE_TAB_T();
            physicalStructureChange.stream().map(OutletDao::map).forEach(changes::add);
            CWMS_OUTLET_PACKAGE.call_STORE_GATE_CHANGES(DSL.using(conn).configuration(), changes, null, null, "UTC",
                                                        "T", "T", formatBool(overrideProtection));
        });
    }

    public void deleteOperationalChanges(CwmsId projectId, Instant startTime, Instant endTime,
                                         boolean overrideProtection) {
        connection(dsl, conn -> {
            setOffice(conn, projectId.getOfficeId());
            String startInclusive = "T";
            String endInclusive = "T";
            LOCATION_REF_T locationRef = getLocationRef(projectId);
            Timestamp startTimestamp = Timestamp.from(startTime);
            Timestamp endTimestamp = Timestamp.from(endTime);
            CWMS_OUTLET_PACKAGE.call_DELETE_GATE_CHANGES(DSL.using(conn).configuration(), locationRef, startTimestamp,
                                                         endTimestamp, "UTC", startInclusive, endInclusive,
                                                         formatBool(overrideProtection));
        });
    }

    private static GATE_SETTING_OBJ_T map(GateSetting setting) {
        GATE_SETTING_OBJ_T output = new GATE_SETTING_OBJ_T();
        output.setOPENING(setting.getOpening());
        output.setINVERT_ELEV(setting.getInvertElevation());
        output.setOPENING_UNITS(setting.getOpeningUnits());
        output.setOPENING_PARAMETER(setting.getOpeningParameter());
        output.setOUTLET_LOCATION_REF(LocationUtil.getLocationRef(setting.getLocationId()));
        return output;
    }

    private static GateSetting map(GATE_SETTING_OBJ_T setting) {
        CwmsId locationId = LocationUtil.getLocationIdentifier(setting.getOUTLET_LOCATION_REF());
        return new GateSetting.Builder().withLocationId(locationId)
                                        .withOpening(setting.getOPENING())
                                        .withOpeningParameter(setting.getOPENING_PARAMETER())
                                        .withOpeningUnits(setting.getOPENING_UNITS())
                                        .withInvertElevation(setting.getINVERT_ELEV())
                                        .build();
    }

    private static GateChange map(GATE_CHANGE_OBJ_T change) {
        List<GateSetting> settings = change.getSETTINGS().stream().map(OutletDao::map).collect(Collectors.toList());
        CwmsId projectId = LocationUtil.getLocationIdentifier(change.getPROJECT_LOCATION_REF());
        LookupType compType = LocationUtil.getLookupType(change.getDISCHARGE_COMPUTATION());
        return new GateChange.Builder().withProjectId(projectId)
                                       .withDischargeComputationType(compType)
                                       .withReasonType(LocationUtil.getLookupType(change.getRELEASE_REASON()))
                                       .withProtected(parseBool(change.getPROTECTED()))
                                       .withNewTotalDischargeOverride(change.getNEW_TOTAL_DISCHARGE_OVERRIDE())
                                       .withOldTotalDischargeOverride(change.getOLD_TOTAL_DISCHARGE_OVERRIDE())
                                       .withDischargeUnits(change.getDISCHARGE_UNITS())
                                       .withPoolElevation(change.getELEV_POOL())
                                       .withTailwaterElevation(change.getELEV_TAILWATER())
                                       .withElevationUnits(change.getELEV_UNITS())
                                       .withNotes(change.getCHANGE_NOTES())
                                       .withChangeDate(change.getCHANGE_DATE().toInstant())
                                       .withSettings(settings)
                                       .build();
    }

    private static GATE_CHANGE_OBJ_T map(GateChange change) {
        GATE_CHANGE_OBJ_T output = new GATE_CHANGE_OBJ_T();
        GATE_SETTING_TAB_T settings = new GATE_SETTING_TAB_T();
        change.getSettings().stream().map(OutletDao::map).forEach(settings::add);

        output.setELEV_TAILWATER(change.getTailwaterElevation());
        output.setELEV_POOL(change.getPoolElevation());
        output.setELEV_UNITS(change.getElevationUnits());
        output.setCHANGE_DATE(Timestamp.from(change.getChangeDate()));
        output.setCHANGE_NOTES(change.getNotes());
        output.setDISCHARGE_UNITS(change.getDischargeUnits());
        output.setNEW_TOTAL_DISCHARGE_OVERRIDE(change.getNewTotalDischargeOverride());
        output.setOLD_TOTAL_DISCHARGE_OVERRIDE(change.getOldTotalDischargeOverride());
        output.setPROTECTED(formatBool(change.isProtected()));
        output.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(change.getProjectId()));
        output.setDISCHARGE_COMPUTATION(LocationUtil.getLookupType(change.getDischargeComputationType()));
        output.setRELEASE_REASON(LocationUtil.getLookupType(change.getReasonType()));
        output.setREFERENCE_ELEV(change.getReferenceElevation());
        output.setSETTINGS(settings);
        return output;
    }
}
