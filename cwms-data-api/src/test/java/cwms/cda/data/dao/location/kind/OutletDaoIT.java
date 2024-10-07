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

import com.google.common.flogger.FluentLogger;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
class OutletDaoIT extends BaseOutletDaoIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final CwmsId TAINTER_GATE_RATING_GROUP = new CwmsId.Builder().withName("Rating-" + PROJECT_1_ID.getName() + "-TainterGate").withOfficeId(OFFICE_ID).build();
    private static final CwmsId BOX_CULVERT_RATING_GROUP = new CwmsId.Builder().withName("Rating-" + PROJECT_2_ID.getName() + "-BoxCulvert").withOfficeId(OFFICE_ID).build();
    private static final CwmsId TAINTER_GATE_RATING_GROUP_MODIFIED = new CwmsId.Builder().withName("Rating-" + PROJECT_1_ID.getName() + "-TainterGate Modified").withOfficeId(OFFICE_ID).build();
    private static final String OUTLET_KIND = "OUTLET";
    private static final Location TAINTER_GATE_1_LOC = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-TG1",
                                                                                     OUTLET_KIND);
    private static final Location TAINTER_GATE_2_LOC = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-TG2",
                                                                                     OUTLET_KIND);
    private static final Location TAINTER_GATE_3_LOC = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-TG3",
                                                                                     OUTLET_KIND);
    private static final Location BOX_CULVERT_1_LOC = buildProjectStructureLocation("BC1", OUTLET_KIND);
    private static final Outlet TAINTER_GATE_1_OUTLET = buildTestOutlet(TAINTER_GATE_1_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet TAINTER_GATE_2_OUTLET = buildTestOutlet(TAINTER_GATE_2_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet TAINTER_GATE_3_OUTLET = buildTestOutlet(TAINTER_GATE_3_LOC, PROJECT_LOC, TAINTER_GATE_RATING_GROUP);
    private static final Outlet BOX_CULVERT_1_OUTLET = buildTestOutlet(BOX_CULVERT_1_LOC, PROJECT_LOC2, BOX_CULVERT_RATING_GROUP);
    private static final CwmsId TG_LOC4_ID = new CwmsId.Builder().withOfficeId(OFFICE_ID)
                                                                 .withName(PROJECT_2_ID.getName() + "-TG4")
                                                                 .build();

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            deleteLocation(context, TG_LOC4_ID.getOfficeId(), TG_LOC4_ID.getName());
            try {
                storeLocation(context, TAINTER_GATE_1_LOC);
                storeLocation(context, TAINTER_GATE_2_LOC);
                storeLocation(context, TAINTER_GATE_3_LOC);
                storeLocation(context, BOX_CULVERT_1_LOC);
                storeOutlet(context, TAINTER_GATE_1_OUTLET);
                storeOutlet(context, TAINTER_GATE_2_OUTLET);
                storeOutlet(context, BOX_CULVERT_1_OUTLET);
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
            deleteLocationGroup(context, TAINTER_GATE_1_OUTLET);
            deleteLocationGroup(context, TAINTER_GATE_2_OUTLET);
            deleteLocationGroup(context, BOX_CULVERT_1_OUTLET);
            deleteOutlet(context, TAINTER_GATE_1_OUTLET);
            deleteOutlet(context, TAINTER_GATE_2_OUTLET);
            deleteOutlet(context, BOX_CULVERT_1_OUTLET);
            deleteLocation(context, TAINTER_GATE_1_LOC.getOfficeId(), TAINTER_GATE_1_LOC.getName());
            deleteLocation(context, TAINTER_GATE_2_LOC.getOfficeId(), TAINTER_GATE_2_LOC.getName());
            deleteLocation(context, TAINTER_GATE_3_LOC.getOfficeId(), TAINTER_GATE_3_LOC.getName());
            deleteLocation(context, BOX_CULVERT_1_LOC.getOfficeId(), BOX_CULVERT_1_LOC.getName());
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_outlet_with_base_loc_only() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            Outlet outlet = dao.retrieveOutlet(BOX_CULVERT_1_LOC.getOfficeId(), BOX_CULVERT_1_LOC.getName());
            DTOMatch.assertMatch(BOX_CULVERT_1_OUTLET, outlet);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_outlet_crud() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            //Delete
            dao.deleteOutlet(TAINTER_GATE_2_LOC.getOfficeId(), TAINTER_GATE_2_LOC.getName(), DeleteRule.DELETE_KEY);

            //Retrieve for project
            List<Outlet> retrievedOutlets = dao.retrieveOutletsForProject(PROJECT_1_ID.getOfficeId(),
                                                                          PROJECT_1_ID.getName());
            DTOMatch.assertDoesNotContainDto(retrievedOutlets, TAINTER_GATE_2_OUTLET, this::isOutletSimilar, "Found " + TAINTER_GATE_2_OUTLET.getLocation().getName() + " when it should not be present.");

            //Create
            Outlet modifiedOutlet = new Outlet.Builder(TAINTER_GATE_2_OUTLET).withRatingGroupId(
                    TAINTER_GATE_RATING_GROUP_MODIFIED).build();
            dao.storeOutlet(modifiedOutlet, true);

            //Single retrieve
            Outlet retrievedModifiedOutlet = dao.retrieveOutlet(TAINTER_GATE_2_LOC.getOfficeId(),
                                                      TAINTER_GATE_2_LOC.getName());
            DTOMatch.assertMatch(modifiedOutlet, retrievedModifiedOutlet);

            deleteLocationGroup(context, modifiedOutlet);

            //Update (back to original)
            dao.storeOutlet(TAINTER_GATE_2_OUTLET, false);

            List<Outlet> finalOutlets = dao.retrieveOutletsForProject(PROJECT_1_ID.getOfficeId(),
                                                                      PROJECT_1_ID.getName());
            DTOMatch.assertContainsDto(finalOutlets, TAINTER_GATE_2_OUTLET, this::isOutletSimilar, DTOMatch::assertMatch, "Unable to find " + TAINTER_GATE_2_OUTLET.getLocation().getName() + " when it should exist.");

            assertThrows(NotFoundException.class, () -> dao.retrieveOutlet(TAINTER_GATE_3_LOC.getOfficeId(),
                                                                           TAINTER_GATE_3_LOC.getName()));
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
            DTOMatch.assertContainsDto(outlets, TAINTER_GATE_1_OUTLET, this::isOutletSimilar, DTOMatch::assertMatch, "Unable to find " + TAINTER_GATE_1_OUTLET.getLocation().getName() + " when it should exist.");
            DTOMatch.assertContainsDto(outlets, TAINTER_GATE_2_OUTLET, this::isOutletSimilar, DTOMatch::assertMatch, "Unable to find " + TAINTER_GATE_2_OUTLET.getLocation().getName() + " when it should exist.");
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Disabled("Disabled due to a DB issue.  See https://jira.hecdev.net/browse/CWDB-296")
    @Test
    void test_rename_outlets() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);

            //Shouldn't exist in the db.
            dao.storeOutlet(TAINTER_GATE_3_OUTLET, true);
            dao.renameOutlet(OFFICE_ID, TAINTER_GATE_3_LOC.getName(), TG_LOC4_ID.getName());
            Outlet outlet = dao.retrieveOutlet(TG_LOC4_ID.getOfficeId(), TG_LOC4_ID.getName());
            assertThrows(NotFoundException.class, () -> dao.retrieveOutlet(OFFICE_ID, TAINTER_GATE_3_LOC.getName()));
            assertNotNull(outlet);
            dao.deleteOutlet(TG_LOC4_ID.getOfficeId(), TG_LOC4_ID.getName(), DeleteRule.DELETE_KEY);

            //Location gets renamed, so let's delete the new location, then store the old one.
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            locationsDao.deleteLocation(TG_LOC4_ID.getName(), TG_LOC4_ID.getOfficeId(), true);
            try {
                locationsDao.storeLocation(TAINTER_GATE_3_LOC);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private boolean isOutletSimilar(Outlet left, Outlet right) {
        boolean output = false;
        if (left != null && right != null) {
            output = left.getLocation().getName().equalsIgnoreCase(right.getLocation().getName())
                    && left.getLocation().getOfficeId().equalsIgnoreCase(right.getLocation().getOfficeId());
        }
        return output;
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId) {
        return new Outlet.Builder().withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                                                                      .withOfficeId(projectLoc.getOfficeId())
                                                                      .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .build();
    }
}