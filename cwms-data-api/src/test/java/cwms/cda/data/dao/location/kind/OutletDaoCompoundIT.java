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
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

class OutletDaoCompoundIT extends ProjectStructureDaoIT {
    private static final String OUTLET_KIND = "OUTLET";
    private static final String COMPOUND_OUTLET_RATING_GROUP = "Rating-" + PROJECT_LOC2.getName() + "-CompoundOutlet";

    private static final Location COMPOUND_OUTLET_1_LOC = buildProjectStructureLocation("CompoundOutlet1", OUTLET_KIND);
    private static final Location COMPOUND_OUTLET_2_LOC = buildProjectStructureLocation(
            PROJECT_LOC2.getName() + "-CompoundOutlet2", OUTLET_KIND);
    private static final Location COMPOUND_OUTLET_3_LOC = buildProjectStructureLocation("CompoundOutlet3", OUTLET_KIND);

    private static final Location CO1_I53 = buildProjectStructureLocation("I53", OUTLET_KIND);
    private static final Location CO1_I25 = buildProjectStructureLocation("I25", OUTLET_KIND);
    private static final Location CO1_LOW_FLOW = buildProjectStructureLocation("Low Flow Gate", OUTLET_KIND);

    private static final Location CO2_INTAKE = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-Intake",
                                                                             OUTLET_KIND);
    private static final Location CO2_WEIR = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-Weir",
                                                                           OUTLET_KIND);
    private static final Location CO2_CONDUIT = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-CO2Conduit",
                                                                              OUTLET_KIND);

    private static final Location CO3_I1 = buildProjectStructureLocation("I1", OUTLET_KIND);
    private static final Location CO3_I2 = buildProjectStructureLocation("I2", OUTLET_KIND);
    private static final Location CO3_I3 = buildProjectStructureLocation("I3", OUTLET_KIND);
    private static final Location CO3_CONDUIT = buildProjectStructureLocation("Conduit", OUTLET_KIND);

    private static final List<VirtualOutletRecord> EXISTING_COMPOUND_OUTLET = Arrays.asList(
            buildCompoundOutletRecord(CO1_I25, CO1_LOW_FLOW), buildCompoundOutletRecord(CO1_I53, CO1_LOW_FLOW),
            buildCompoundOutletRecord(CO1_LOW_FLOW));

    private static final List<VirtualOutletRecord> BASE_AND_SUB_LOC_COMPOUND_OUTLET = Arrays.asList(
            buildCompoundOutletRecord(CO2_INTAKE, CO2_WEIR, CO2_CONDUIT), buildCompoundOutletRecord(CO2_WEIR),
            buildCompoundOutletRecord(CO2_CONDUIT));

    private static final List<VirtualOutletRecord> BASE_LOC_ONLY_COMPOUND_OUTLET = Arrays.asList(
            buildCompoundOutletRecord(CO3_I1, CO3_CONDUIT), buildCompoundOutletRecord(CO3_I2, CO3_CONDUIT),
            buildCompoundOutletRecord(CO3_I3, CO3_CONDUIT), buildCompoundOutletRecord(CO3_CONDUIT));
    private static final Outlet CO1_I25_OUTLET = buildTestOutlet(CO1_I25, PROJECT_LOC2);
    private static final Outlet CO1_I53_OUTLET = buildTestOutlet(CO1_I53, PROJECT_LOC2);
    private static final Outlet CO1_LOW_FLOW_OUTLET = buildTestOutlet(CO1_LOW_FLOW, PROJECT_LOC2);
    private static final Outlet CO2_CONDUIT_OUTLET = buildTestOutlet(CO2_CONDUIT, PROJECT_LOC2);
    private static final Outlet CO2_INTAKE_OUTLET = buildTestOutlet(CO2_INTAKE, PROJECT_LOC2);
    private static final Outlet CO2_WEIR_OUTLET = buildTestOutlet(CO2_WEIR, PROJECT_LOC2);
    private static final Outlet CO3_I1_OUTLET = buildTestOutlet(CO3_I1, PROJECT_LOC2);
    private static final Outlet CO3_I2_OUTLET = buildTestOutlet(CO3_I2, PROJECT_LOC2);
    private static final Outlet CO3_I3_OUTLET = buildTestOutlet(CO3_I3, PROJECT_LOC2);
    private static final Outlet CO3_CONDUIT_OUTLET = buildTestOutlet(CO3_CONDUIT, PROJECT_LOC2);

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);
            try {
                locationsDao.storeLocation(COMPOUND_OUTLET_1_LOC);
                locationsDao.storeLocation(COMPOUND_OUTLET_2_LOC);
                locationsDao.storeLocation(COMPOUND_OUTLET_3_LOC);
                locationsDao.storeLocation(CO1_I25);
                locationsDao.storeLocation(CO1_I53);
                locationsDao.storeLocation(CO1_LOW_FLOW);
                locationsDao.storeLocation(CO2_CONDUIT);
                locationsDao.storeLocation(CO2_INTAKE);
                locationsDao.storeLocation(CO2_WEIR);
                locationsDao.storeLocation(CO3_I1);
                locationsDao.storeLocation(CO3_I2);
                locationsDao.storeLocation(CO3_I3);
                locationsDao.storeLocation(CO3_CONDUIT);

                outletDao.storeOutlet(CO1_I25_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO1_I53_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO1_LOW_FLOW_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO2_CONDUIT_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO2_INTAKE_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO2_WEIR_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO3_I1_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO3_I2_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO3_I3_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);
                outletDao.storeOutlet(CO3_CONDUIT_OUTLET, COMPOUND_OUTLET_RATING_GROUP, false);

                //Should always have outlet 1 as a compound outlet
                outletDao.storeCompoundOutlet(PROJECT_LOC2.getName(), COMPOUND_OUTLET_1_LOC.getName(),
                                              COMPOUND_OUTLET_1_LOC.getOfficeId(),
                                              EXISTING_COMPOUND_OUTLET, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void teardown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);

            //Compound outlets
            outletDao.deleteCompoundOutlet(PROJECT_LOC2.getName(), COMPOUND_OUTLET_1_LOC.getName(),
                                           PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);

            outletDao.deleteOutlet(CO1_I25.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO1_I53.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO1_LOW_FLOW.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_CONDUIT.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_INTAKE.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_WEIR.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I1.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I2.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I3.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_CONDUIT.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(COMPOUND_OUTLET_1_LOC.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(COMPOUND_OUTLET_2_LOC.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(COMPOUND_OUTLET_3_LOC.getName(), PROJECT_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);

            locationsDao.deleteLocation(COMPOUND_OUTLET_1_LOC.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(COMPOUND_OUTLET_2_LOC.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(COMPOUND_OUTLET_3_LOC.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO1_I25.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO1_I53.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO1_LOW_FLOW.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO2_CONDUIT.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO2_INTAKE.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO2_WEIR.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO3_I1.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO3_I2.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO3_I3.getName(), PROJECT_LOC.getOfficeId(), true);
            locationsDao.deleteLocation(CO3_CONDUIT.getName(), PROJECT_LOC.getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_get_all_outlets_with_compound_outlets() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            List<Outlet> outlets = dao.retrieveOutletsForProject(PROJECT_LOC2.getName(), PROJECT_LOC.getOfficeId());

            Outlet compoundOutlet = outlets.stream()
                                           .filter(outlet -> outlet.getLocation()
                                                                   .getName()
                                                                   .equalsIgnoreCase(
                                                                           EXISTING_COMPOUND_OUTLET.getLocation()
                                                                                                   .getName()))
                                           .findFirst()
                                           .orElse(null);
            assertNotNull(compoundOutlet);
            List<VirtualOutletRecord> virtualOutletRecords = compoundOutlet.getCompoundOutletRecords();
            assertEquals(EXISTING_COMPOUND_OUTLET.size(), virtualOutletRecords.size());
            assertAll(EXISTING_COMPOUND_OUTLET.stream()
                                              .map(compoundOutletRecord -> () -> compareOutletRecords(
                                                      compoundOutletRecord, virtualOutletRecords)));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_compound_outlet_only_base_loc() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            dao.storeCompoundOutlet(BASE_LOC_ONLY_COMPOUND_OUTLET.getProjectId().getName(),
                                    BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getName(),
                                    BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getOfficeId(),
                                    BASE_LOC_ONLY_COMPOUND_OUTLET.getCompoundOutletRecords(), false);

            List<VirtualOutletRecord> virtualOutletRecords = dao.retrieveOutlet(
                    BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getName(),
                    BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getOfficeId()).getCompoundOutletRecords();
            dao.deleteCompoundOutlet(BASE_LOC_ONLY_COMPOUND_OUTLET.getProjectId().getName(),
                                     BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getName(),
                                     BASE_LOC_ONLY_COMPOUND_OUTLET.getLocation().getOfficeId(), DeleteRule.DELETE_ALL);

            assertEquals(BASE_LOC_ONLY_COMPOUND_OUTLET.getCompoundOutletRecords().size(), virtualOutletRecords.size());
            assertAll(BASE_LOC_ONLY_COMPOUND_OUTLET.getCompoundOutletRecords()
                                                   .stream()
                                                   .map(compoundOutletRecord -> () -> compareOutletRecords(
                                                           compoundOutletRecord, virtualOutletRecords)));

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Disabled("Currently there's a bug with compound outlets and sub-locations - this is coming back without a hyphen")
    @Test
    void test_compound_outlet_crud_with_sub_loc() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            dao.storeCompoundOutlet(BASE_AND_SUB_LOC_COMPOUND_OUTLET.getProjectId().getName(),
                                    BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getName(),
                                    BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getOfficeId(),
                                    BASE_AND_SUB_LOC_COMPOUND_OUTLET.getCompoundOutletRecords(), false);

            List<VirtualOutletRecord> virtualOutletRecords = dao.retrieveOutlet(
                    BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getName(),
                    BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getOfficeId()).getCompoundOutletRecords();
            dao.deleteCompoundOutlet(BASE_AND_SUB_LOC_COMPOUND_OUTLET.getProjectId().getName(),
                                     BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getName(),
                                     BASE_AND_SUB_LOC_COMPOUND_OUTLET.getLocation().getOfficeId(),
                                     DeleteRule.DELETE_ALL);

            assertEquals(BASE_AND_SUB_LOC_COMPOUND_OUTLET.getCompoundOutletRecords().size(),
                         virtualOutletRecords.size());
            assertAll(BASE_AND_SUB_LOC_COMPOUND_OUTLET.getCompoundOutletRecords()
                                                      .stream()
                                                      .map(compoundOutletRecord -> () -> compareOutletRecords(
                                                              compoundOutletRecord, virtualOutletRecords)));

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static void compareOutletRecords(VirtualOutletRecord expectedRecord,
                                             List<VirtualOutletRecord> receivedRecords) {
        //the received record outlet ids are unique, so we should assert that others don't exist in there.
        VirtualOutletRecord receivedRecord = null;
        List<String> errors = new ArrayList<>();
        for (VirtualOutletRecord outletRecord : receivedRecords) {
            if (outletRecord.getOutletId().getName().equalsIgnoreCase(expectedRecord.getOutletId().getName())
                    && outletRecord.getOutletId().getOfficeId().equalsIgnoreCase(expectedRecord.getOutletId().getOfficeId())) {
                if (receivedRecord != null) {
                    errors.add("Duplicate record for: " + outletRecord.getOutletId());
                } else {
                    receivedRecord = outletRecord;
                }
            }
        }

        if (receivedRecord != null) {
            DTOMatch.assertMatch(expectedRecord, receivedRecord);
        } else {
            errors.add("No record found for: " + expectedRecord.getOutletId());
        }

        assertTrue(errors.isEmpty(), String.join("\n", errors));
    }


    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc,
                                          VirtualOutletRecord... virtualOutletRecords) {
        Outlet.Builder builder = new Outlet.Builder().withProjectId(
                                                             new CwmsId.Builder().withName(projectLoc.getName()).withOfficeId(projectLoc.getOfficeId()).build())
                                                     .withLocation(outletLoc);
        if (virtualOutletRecords != null) {
            builder.withCompoundOutletRecords(Arrays.asList(virtualOutletRecords));
        }
        return builder.build();
    }

    private static VirtualOutletRecord buildCompoundOutletRecord(Location upstream, Location... downstream) {
        if (downstream == null || downstream.length == 0) {
            return new VirtualOutletRecord.Builder().withOutletId(
                    new CwmsId.Builder().withName(upstream.getName()).withOfficeId(upstream.getOfficeId()).build()).build();
        }

        List<CwmsId> downstreamOutletIds = Arrays.stream(downstream)
                                                 .map(loc -> new CwmsId.Builder().withName(loc.getName())
                                                                                 .withOfficeId(loc.getOfficeId())
                                                                                 .build())
                                                 .collect(Collectors.toList());
        return new VirtualOutletRecord.Builder().withOutletId(
                                                         new CwmsId.Builder().withName(upstream.getName()).withOfficeId(upstream.getOfficeId()).build())
                                                .withDownstreamOutletIds(downstreamOutletIds)
                                                .build();
    }
}
