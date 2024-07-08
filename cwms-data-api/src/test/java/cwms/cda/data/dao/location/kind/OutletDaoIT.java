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
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class OutletDaoIT extends ProjectStructureDaoIT {
    private static final String TAINTER_GATE_RATING_GROUP = "Rating-" + PROJECT_LOC.getName() + "-TainterGate";
    private static final String BOX_CULVERT_RATING_GROUP = "Rating-" + PROJECT_LOC2.getName() + "-BoxCulvert";
    private static final String TAINTER_GATE_RATING_GROUP_MODIFIED = "Rating-" + PROJECT_LOC.getName() + "-TainterGate Modified";
    private static final String OUTLET_KIND = "OUTLET";
    private static final Location TAINTER_GATE_1_LOC = buildProjectStructureLocation(PROJECT_LOC.getName() + "-TG1",
                                                                                     OUTLET_KIND);
    private static final Location TAINTER_GATE_2_LOC = buildProjectStructureLocation(PROJECT_LOC.getName() + "-TG2",
                                                                                     OUTLET_KIND);
    private static final Location TAINTER_GATE_3_LOC = buildProjectStructureLocation(PROJECT_LOC.getName() + "-TG3",
                                                                                     OUTLET_KIND);
    private static final Location BOX_CULVERT_1_LOC = buildProjectStructureLocation("BC1", OUTLET_KIND);
    private static final Outlet TAINTER_GATE_1_OUTLET = buildTestOutlet(TAINTER_GATE_1_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet TAINTER_GATE_2_OUTLET = buildTestOutlet(TAINTER_GATE_2_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet TAINTER_GATE_3_OUTLET = buildTestOutlet(TAINTER_GATE_3_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet BOX_CULVERT_1_OUTLET = buildTestOutlet(BOX_CULVERT_1_LOC, PROJECT_LOC2, BOX_CULVERT_RATING_GROUP);

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);
            try {
                locationsDao.storeLocation(TAINTER_GATE_1_LOC);
                locationsDao.storeLocation(TAINTER_GATE_2_LOC);
                locationsDao.storeLocation(TAINTER_GATE_3_LOC);
                locationsDao.storeLocation(BOX_CULVERT_1_LOC);
                outletDao.storeOutlet(TAINTER_GATE_1_OUTLET, TAINTER_GATE_1_OUTLET.getRatingGroupId(), false);
                outletDao.storeOutlet(TAINTER_GATE_2_OUTLET, TAINTER_GATE_2_OUTLET.getRatingGroupId(), false);
                outletDao.storeOutlet(BOX_CULVERT_1_OUTLET, BOX_CULVERT_1_OUTLET.getRatingGroupId(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);
            outletDao.deleteOutlet(TAINTER_GATE_1_LOC.getName(), TAINTER_GATE_1_LOC.getOfficeId(),
                                   DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(TAINTER_GATE_2_LOC.getName(), TAINTER_GATE_2_LOC.getOfficeId(),
                                   DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(BOX_CULVERT_1_LOC.getName(), BOX_CULVERT_1_LOC.getOfficeId(), DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(TAINTER_GATE_1_LOC.getName(), TAINTER_GATE_1_LOC.getBoundingOfficeId(), true);
            locationsDao.deleteLocation(TAINTER_GATE_2_LOC.getName(), TAINTER_GATE_2_LOC.getBoundingOfficeId(), true);
            locationsDao.deleteLocation(TAINTER_GATE_3_LOC.getName(), TAINTER_GATE_3_LOC.getBoundingOfficeId(), true);
            locationsDao.deleteLocation(BOX_CULVERT_1_LOC.getName(), BOX_CULVERT_1_LOC.getBoundingOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_outlet_with_base_loc_only() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            Outlet outlet = dao.retrieveOutlet(BOX_CULVERT_1_LOC.getName(), BOX_CULVERT_1_LOC.getOfficeId());
            DTOMatch.assertMatch(BOX_CULVERT_1_OUTLET, outlet);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_outlet_crud() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            List<Outlet> initialOutlets = dao.retrieveOutletsForProject(PROJECT_LOC.getName(),
                                                                        PROJECT_LOC.getBoundingOfficeId());

            //This shouldn't exist in the DB yet.
            dao.storeOutlet(TAINTER_GATE_3_OUTLET, TAINTER_GATE_3_OUTLET.getRatingGroupId(), true);

            List<Outlet> retrievedOutlets = dao.retrieveOutletsForProject(PROJECT_LOC.getName(),
                                                                          PROJECT_LOC.getBoundingOfficeId());
            assertNotEquals(initialOutlets.size(), retrievedOutlets.size());
            DTOMatch.assertMatch(TAINTER_GATE_3_OUTLET, retrievedOutlets.get(2));

            Outlet newOutlet = new Outlet.Builder(TAINTER_GATE_3_OUTLET)
                    .withRatingGroupId(TAINTER_GATE_RATING_GROUP_MODIFIED)
                    .build();
            dao.storeOutlet(newOutlet, TAINTER_GATE_RATING_GROUP_MODIFIED, false);
            Outlet updatedOutlet = dao.retrieveOutlet(TAINTER_GATE_3_LOC.getName(),
                                                      TAINTER_GATE_3_LOC.getBoundingOfficeId());
            
            //DELETE_KEY will just remove the AT_OUTLET key, and since we don't have any changes this will be fine.
            dao.deleteOutlet(TAINTER_GATE_3_LOC.getName(), TAINTER_GATE_3_LOC.getBoundingOfficeId(),
                             DeleteRule.DELETE_KEY);

            DTOMatch.assertMatch(newOutlet, updatedOutlet);

            List<Outlet> finalOutlets = dao.retrieveOutletsForProject(PROJECT_LOC.getName(),
                                                                      PROJECT_LOC.getBoundingOfficeId());
            assertEquals(initialOutlets.size(), finalOutlets.size());

            assertThrows(NotFoundException.class, () -> dao.retrieveOutlet(TAINTER_GATE_3_LOC.getName(),
                                                                           TAINTER_GATE_3_LOC.getBoundingOfficeId()));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_retrieve_outlets_for_project() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            List<Outlet> outlets = dao.retrieveOutletsForProject(PROJECT_LOC.getName(),
                                                                 PROJECT_LOC.getBoundingOfficeId());

            assertEquals(2, outlets.size());
            DTOMatch.assertMatch(TAINTER_GATE_1_OUTLET, outlets.get(0));
            DTOMatch.assertMatch(TAINTER_GATE_2_OUTLET, outlets.get(1));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, String ratingId) {
        return new Outlet.Builder().withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                                                                      .withOfficeId(projectLoc.getBoundingOfficeId())
                                                                      .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .build();
    }
}