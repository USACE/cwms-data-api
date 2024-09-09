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

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.location.kind.Outlet;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class OutletDaoRatingIT extends BaseOutletDaoIT {
    private static final String LOW_FLOW_CURVE_ID = "opening-low flow,elev;flow";
    private static final String SPILLWAY_CURVE_ID = "elev;flow curve";
    private static final String OUTLET_KIND = "OUTLET";
    private static final CwmsId PROJECT_LOW_FLOW_RATING_GROUP = new CwmsId.Builder()
            .withName("Rating-" + PROJECT_1_ID.getName() + "-LowFlow").withOfficeId(PROJECT_1_ID.getOfficeId()).build();
    private static final CwmsId PROJECT_SPILLWAY_RATING_GROUP = new CwmsId.Builder()
            .withName("Rating-" + PROJECT_1_ID.getName() + "-Spillway")
            .withOfficeId(PROJECT_1_ID.getOfficeId()).build();
    private static final Location PROJECT_LOW_FLOW_LOC
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-LF", OUTLET_KIND);
    private static final Location PROJECT_SPILLWAY_LOC
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-Spillway", OUTLET_KIND);
    private static final Outlet PROJECT_LOW_FLOW_OUTLET = buildTestOutlet(PROJECT_LOW_FLOW_LOC,
            PROJECT_LOC, PROJECT_LOW_FLOW_RATING_GROUP);
    private static final Outlet PROJECT_SPILLWAY_OUTLET = buildTestOutlet(PROJECT_SPILLWAY_LOC,
            PROJECT_LOC, PROJECT_SPILLWAY_RATING_GROUP);

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
    }

    @AfterAll
    static void cleanup() throws Exception {
        tearDownProject();
    }

    @Test
    void test_uncontrolled_outlet() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            outletDao.storeOutlet(PROJECT_SPILLWAY_OUTLET, false);
            LocationGroupDao locationGroupDao = new LocationGroupDao(context);
            List<LocationGroup> groups = locationGroupDao.getLocationGroups();
            assertNotNull(groups);
            LocationGroup group = retrieveFromGroup(groups, PROJECT_SPILLWAY_RATING_GROUP);
            assertNotNull(group);
            LocationGroup modifiedGroup = new LocationGroup(group.getLocationCategory(), group.getOfficeId(),
                    group.getId(), group.getDescription(), SPILLWAY_CURVE_ID, group.getSharedRefLocationId(),
                    group.getLocGroupAttribute());
            LocationGroup newGroup = new LocationGroup(modifiedGroup, group.getAssignedLocations());
            locationGroupDao.unassignAllLocs(group);
            locationGroupDao.assignLocs(newGroup);
            Outlet retrievedOutlet = outletDao.retrieveOutlet(PROJECT_SPILLWAY_LOC.getOfficeId(),
                    PROJECT_SPILLWAY_LOC.getName());
            assertNotNull(retrievedOutlet);
            assertEquals(retrievedOutlet.getRatingGroupId().getName(), PROJECT_SPILLWAY_RATING_GROUP.getName());
            assertEquals(retrievedOutlet.getRatingGroupId().getOfficeId(), PROJECT_SPILLWAY_RATING_GROUP.getOfficeId());
            outletDao.deleteOutlet(PROJECT_SPILLWAY_LOC.getOfficeId(), PROJECT_SPILLWAY_LOC.getName(), DeleteRule.DELETE_ALL);
            locationGroupDao.delete(newGroup.getLocationCategory().getId(), newGroup.getId(),
                    true, newGroup.getOfficeId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_controlled_outlet() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            outletDao.storeOutlet(PROJECT_LOW_FLOW_OUTLET, false);
            LocationGroupDao locationGroupDao = new LocationGroupDao(context);
            List<LocationGroup> groups = locationGroupDao.getLocationGroups();
            assertNotNull(groups);
            LocationGroup group = retrieveFromGroup(groups, PROJECT_LOW_FLOW_RATING_GROUP);
            assertNotNull(group);
            LocationGroup modifiedGroup = new LocationGroup(group.getLocationCategory(), group.getOfficeId(),
                    group.getId(), group.getDescription(), LOW_FLOW_CURVE_ID, group.getSharedRefLocationId(),
                    group.getLocGroupAttribute());
            LocationGroup newGroup = new LocationGroup(modifiedGroup, group.getAssignedLocations());
            locationGroupDao.unassignAllLocs(group);
            locationGroupDao.assignLocs(newGroup);
            Outlet retrievedOutlet = outletDao.retrieveOutlet(PROJECT_LOW_FLOW_LOC.getOfficeId(),
                    PROJECT_LOW_FLOW_LOC.getName());
            assertNotNull(retrievedOutlet);
            assertEquals(retrievedOutlet.getRatingGroupId().getName(), PROJECT_LOW_FLOW_RATING_GROUP.getName());
            assertEquals(retrievedOutlet.getRatingGroupId().getOfficeId(), PROJECT_LOW_FLOW_RATING_GROUP.getOfficeId());
            outletDao.deleteOutlet(PROJECT_LOW_FLOW_LOC.getOfficeId(), PROJECT_LOW_FLOW_LOC.getName(), DeleteRule.DELETE_ALL);
            locationGroupDao.delete(newGroup.getLocationCategory().getId(), newGroup.getId(),
                    true, newGroup.getOfficeId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static LocationGroup retrieveFromGroup(List<LocationGroup> groups, CwmsId targetGroup) {
        for (LocationGroup group : groups) {
            if (group.getId().equals(targetGroup.getName())
                    && group.getOfficeId().equals(targetGroup.getOfficeId())) {
                return group;
            }
        }
        return null;
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
