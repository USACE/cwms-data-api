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

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.data.dto.location.kind.ProjectStructure;
import cwms.cda.data.dto.location.kind.VirtualOutlet;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_OUTLET_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_STRUCTURE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_TAB_T;

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
            List<LocationGroup> groups = locGroupDao.getLocationGroups(officeId, Outlet.RATING_LOC_GROUP_CATEGORY);

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

    private CwmsId getRatingGroupId(String locationId, List<LocationGroup> groups) {
        return groups.stream()
                     .filter(group -> group.getAssignedLocations()
                                           .stream()
                                           .anyMatch(loc -> loc.getLocationId().equalsIgnoreCase(locationId)))
                     .findFirst()
                     .map(locGroup -> new CwmsId.Builder().withName(locGroup.getId())
                                                          .withOfficeId(locGroup.getOfficeId())
                                                          .build())
                     .orElse(null);
    }

    public void storeOutlet(ProjectStructure outlet, String ratingGroup, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, outlet.getProjectId().getOfficeId());
            PROJECT_STRUCTURE_OBJ_T structure = mapToProjectStructure(outlet);
            CWMS_OUTLET_PACKAGE.call_STORE_OUTLET(DSL.using(conn).configuration(), structure, ratingGroup,
                                                  OracleTypeMap.formatBool(failIfExists));
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
            //projectId and officeId are used as a mask in RETRIEVE_COMPOUND_OUTLETS, however this usage expects a specific
            //id, since retrieveOutlets does not use a mask.
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
        List<VirtualOutletRecord> outletRecords = CWMS_OUTLET_PACKAGE.call_RETRIEVE_COMPOUND_OUTLET(config, virtualOutletId,
                                                                                              projectId, officeId)
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

    public void storeVirtualOutlet(String officeId, String projectId, String virtualOutletId,
                                   List<VirtualOutletRecord> records, boolean failIfExists) {
        List<List<String>> virtualOutlets = records.stream()
                                                    .map(this::mapVirtualRecords)
                                                    .collect(Collectors.toList());

        STR_TAB_TAB_T outlets = new STR_TAB_TAB_T(
                virtualOutlets.stream().map(STR_TAB_T::new).collect(Collectors.toList()));
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_OUTLET_PACKAGE.call_STORE_COMPOUND_OUTLET(DSL.using(conn).configuration(), projectId, virtualOutletId,
                                                           outlets, OracleTypeMap.formatBool(failIfExists), officeId);
        });
    }

    public void deleteVirtualOutlet(String officeId, String projectId, String virtualOutletId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_OUTLET_PACKAGE.call_DELETE_COMPOUND_OUTLET(DSL.using(conn).configuration(), projectId,
                                                            virtualOutletId, deleteRule.getRule(), officeId);
        });
    }

    private Outlet mapToOutlet(PROJECT_STRUCTURE_OBJ_T projectStructure,
                               List<LocationGroup> groups) {
        Location location = LocationUtil.getLocation(projectStructure.getSTRUCTURE_LOCATION());
        CwmsId ratingGroupId = getRatingGroupId(location.getName(), groups);
        CwmsId projectId = LocationUtil.getLocationIdentifier(projectStructure.getPROJECT_LOCATION_REF());

        return new Outlet.Builder().withLocation(location)
                                   .withProjectId(projectId)
                                   .withRatingGroupId(ratingGroupId)
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
        return new VirtualOutletRecord.Builder().withOutletId(
                                                         new CwmsId.Builder().withName(outlet).withOfficeId(officeId).build())
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
}
