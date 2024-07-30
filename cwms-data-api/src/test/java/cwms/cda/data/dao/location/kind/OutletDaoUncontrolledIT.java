/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class OutletDaoUncontrolledIT extends ProjectStructureIT {
    private static final String OUTLET_KIND = "OUTLET";
    private static final CwmsId SPILLWAY_LOW_FLOW_RATING_GROUP = new CwmsId.Builder()
            .withName("Rating-" + PROJECT_1_ID.getName() + "-LowFlow").withOfficeId(PROJECT_1_ID.getOfficeId()).build();
    private static final CwmsId SPILLWAY_LOW_FLOW_RATING_GROUP_MODIFIED = new CwmsId.Builder()
            .withName("Rating-" + PROJECT_1_ID.getName() + "-LowFlow-Modified")
            .withOfficeId(PROJECT_1_ID.getOfficeId()).build();
    private static final Location SPILLWAY_LOW_FLOW_1_LOC
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-LF1", OUTLET_KIND);
    private static final Location SPILLWAY_LOW_FLOW_2_LOC
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-LF2", OUTLET_KIND);
    private static final Location SPILLWAY_LOW_FLOW_3_LOC
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-LF3", OUTLET_KIND);
    private static final Outlet SPILLWAY_LOW_FLOW_1_OUTLET = buildTestOutlet(SPILLWAY_LOW_FLOW_1_LOC,
            PROJECT_LOC, SPILLWAY_LOW_FLOW_RATING_GROUP);
    private static final Outlet SPILLWAY_LOW_FLOW_2_OUTLET = buildTestOutlet(SPILLWAY_LOW_FLOW_2_LOC,
            PROJECT_LOC, SPILLWAY_LOW_FLOW_RATING_GROUP);

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            try {
                storeLocation(context, SPILLWAY_LOW_FLOW_1_LOC);
                storeLocation(context, SPILLWAY_LOW_FLOW_2_LOC);
                outletDao.storeOutlet(SPILLWAY_LOW_FLOW_1_OUTLET, false);
                outletDao.storeOutlet(SPILLWAY_LOW_FLOW_2_OUTLET, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void cleanup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            outletDao.deleteOutlet(SPILLWAY_LOW_FLOW_1_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_1_LOC.getName(),
                    DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(SPILLWAY_LOW_FLOW_2_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_2_LOC.getName(),
                    DeleteRule.DELETE_ALL);
            deleteLocation(context, SPILLWAY_LOW_FLOW_1_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_1_LOC.getName());
            deleteLocation(context, SPILLWAY_LOW_FLOW_2_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_2_LOC.getName());
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_outlet_with_base_loc_only() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            Outlet outlet = dao.retrieveOutlet(SPILLWAY_LOW_FLOW_1_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_1_LOC.getName());
            DTOMatch.assertMatch(SPILLWAY_LOW_FLOW_1_OUTLET, outlet);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_outlet_crud() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            //Delete
            dao.deleteOutlet(SPILLWAY_LOW_FLOW_2_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_2_LOC.getName(), DeleteRule.DELETE_KEY);

            //Retrieve for project
            List<Outlet> retrievedOutlets = dao.retrieveOutletsForProject(PROJECT_1_ID.getOfficeId(),
                    PROJECT_1_ID.getName());
            doesNotContainOutlet(retrievedOutlets, SPILLWAY_LOW_FLOW_2_OUTLET);

            //Create
            Outlet modifiedOutlet = new Outlet.Builder(SPILLWAY_LOW_FLOW_2_OUTLET).withRatingGroupId(
                    SPILLWAY_LOW_FLOW_RATING_GROUP_MODIFIED).build();
            dao.storeOutlet(modifiedOutlet, true);

            //Single retrieve
            Outlet retrievedModifiedOutlet = dao.retrieveOutlet(SPILLWAY_LOW_FLOW_2_LOC.getOfficeId(),
                    SPILLWAY_LOW_FLOW_2_LOC.getName());
            DTOMatch.assertMatch(modifiedOutlet, retrievedModifiedOutlet);

            //Update (back to original)
            dao.storeOutlet(SPILLWAY_LOW_FLOW_2_OUTLET, false);

            List<Outlet> finalOutlets = dao.retrieveOutletsForProject(PROJECT_1_ID.getOfficeId(),
                    PROJECT_1_ID.getName());
            containsOutlet(finalOutlets, SPILLWAY_LOW_FLOW_2_OUTLET);

            assertThrows(NotFoundException.class, () -> dao.retrieveOutlet(SPILLWAY_LOW_FLOW_3_LOC.getOfficeId(),
                    SPILLWAY_LOW_FLOW_3_LOC.getName()));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_retrieve_outlets_for_project() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            List<Outlet> outlets = dao.retrieveOutletsForProject(PROJECT_1_ID.getOfficeId(),
                    PROJECT_1_ID.getName());
            containsOutlet(outlets, SPILLWAY_LOW_FLOW_1_OUTLET);
            containsOutlet(outlets, SPILLWAY_LOW_FLOW_1_OUTLET);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_retrieve_outlets_loc_group() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            Outlet retrivedOutlet = dao.retrieveOutlet(SPILLWAY_LOW_FLOW_1_LOC.getOfficeId(), SPILLWAY_LOW_FLOW_1_LOC.getName());
            DTOMatch.assertMatch(SPILLWAY_LOW_FLOW_1_OUTLET, retrivedOutlet);
            DTOMatch.assertMatch(retrivedOutlet.getRatingGroupId(), SPILLWAY_LOW_FLOW_RATING_GROUP);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void containsOutlet(List<Outlet> outlets, Outlet expectedOutlet) {
        String name = expectedOutlet.getLocation().getName();
        Outlet receivedOutlet = outlets.stream()
                .filter(outlet -> outlet.getLocation()
                        .getName()
                        .equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
        assertNotNull(receivedOutlet);
        DTOMatch.assertMatch(expectedOutlet, receivedOutlet);
    }

    private void doesNotContainOutlet(List<Outlet> outlets, Outlet expectedOutlet) {
        String name = expectedOutlet.getLocation().getName();
        Outlet receivedOutlet = outlets.stream()
                .filter(outlet -> outlet.getLocation()
                        .getName()
                        .equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
        assertNull(receivedOutlet);
    }


    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId) {
        return new Outlet.Builder()
                .withLocation(outletLoc)
                .withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                        .withOfficeId(projectLoc.getOfficeId()).build())
                .withRatingGroupId(ratingId)
                .build();
    }
}
