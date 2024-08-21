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
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.GateSetting;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class OutletDaoChangeIT extends ProjectStructureIT {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String TAINTER_GATE_RATING_SPEC_ID = PROJECT_1_ID.getName() + ".Opening-TainterGate,Elev;Flow-TainterGate.Standard.Production";
    private static final CwmsId TAINTER_GATE_RATING_GROUP = new CwmsId.Builder().withName(
            "Rating-" + PROJECT_1_ID.getName() + "-TainterGate").withOfficeId(OFFICE_ID).build();
    private static final String OUTLET_KIND = "OUTLET";
    private static final CwmsId TAINTER_GATE_10_ID = new CwmsId.Builder().withName(PROJECT_1_ID.getName() + "-TG10")
                                                                         .withOfficeId(OFFICE_ID)
                                                                         .build();
    private static final CwmsId TAINTER_GATE_20_ID = new CwmsId.Builder().withName("TG20")
                                                                         .withOfficeId(OFFICE_ID)
                                                                         .build();
    private static final Location TAINTER_GATE_10_LOC = buildProjectStructureLocation(TAINTER_GATE_10_ID.getName(),
                                                                                      OUTLET_KIND);
    private static final Location TAINTER_GATE_20_LOC = buildProjectStructureLocation(TAINTER_GATE_20_ID.getName(),
                                                                                      OUTLET_KIND);
    private static final Outlet TAINTER_GATE_10_OUTLET = buildTestOutlet(TAINTER_GATE_10_LOC, PROJECT_LOC,
                                                                         TAINTER_GATE_RATING_GROUP);
    private static final Outlet TAINTER_GATE_20_OUTLET = buildTestOutlet(TAINTER_GATE_20_LOC, PROJECT_LOC,
                                                                         TAINTER_GATE_RATING_GROUP);
    private static final Instant JAN_FIRST = ZonedDateTime.now()
                                                          .withYear(2024)
                                                          .withMonth(1)
                                                          .withDayOfMonth(1)
                                                          .withHour(0)
                                                          .withMinute(0)
                                                          .withSecond(0)
                                                          .withNano(0)
                                                          .toInstant();
    private static final Instant JAN_SECOND = ZonedDateTime.now()
                                                           .withYear(2024)
                                                           .withMonth(1)
                                                           .withDayOfMonth(2)
                                                           .withHour(0)
                                                           .withMinute(0)
                                                           .withSecond(0)
                                                           .withNano(0)
                                                           .toInstant();
    private static final GateChange CHANGE_1 = buildTestGateChange(PROJECT_1_ID, JAN_FIRST,
                                                                   buildTestGateSetting(TAINTER_GATE_10_ID, 10, 20));
    private static final GateChange CHANGE_2 = buildTestGateChange(PROJECT_1_ID, JAN_SECOND,
                                                                   buildTestGateSetting(TAINTER_GATE_10_ID, 1, 2),
                                                                   buildTestGateSetting(TAINTER_GATE_20_ID, 3, 4));

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            LocationGroupDao locGroupDao = new LocationGroupDao(context);
            try {
                storeLocation(context, TAINTER_GATE_10_LOC);
                storeLocation(context, TAINTER_GATE_20_LOC);
                deleteLocGroup(locGroupDao);
                outletDao.storeOutlet(TAINTER_GATE_10_OUTLET, false);
                outletDao.storeOutlet(TAINTER_GATE_20_OUTLET, false);

                Optional<LocationGroup> ratingGroup = locGroupDao.getLocationGroup(
                        TAINTER_GATE_10_OUTLET.getRatingCategoryId().getOfficeId(),
                        TAINTER_GATE_10_OUTLET.getRatingCategoryId().getName(),
                        TAINTER_GATE_10_OUTLET.getRatingGroupId().getName());

                if (ratingGroup.isPresent()) {
                    LocationGroup realGroup = ratingGroup.get();
                    List<AssignedLocation> assLocs = realGroup.getAssignedLocations();
                    realGroup = new LocationGroup(realGroup.getLocationCategory(), realGroup.getOfficeId(),
                                                  realGroup.getId(), realGroup.getDescription(),
                                                  TAINTER_GATE_RATING_SPEC_ID, realGroup.getSharedRefLocationId(),
                                                  realGroup.getLocGroupAttribute());
                    realGroup = new LocationGroup(realGroup, assLocs);
                    locGroupDao.delete(realGroup.getLocationCategory().getId(), realGroup.getId(), true,
                                       realGroup.getOfficeId());
                    locGroupDao.create(realGroup);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static void deleteLocGroup(LocationGroupDao locGroupDao) {
        try {
            locGroupDao.delete(TAINTER_GATE_10_OUTLET.getRatingCategoryId().getName(),
                               TAINTER_GATE_10_OUTLET.getRatingGroupId().getName(), true,
                               TAINTER_GATE_10_OUTLET.getRatingGroupId().getOfficeId());
        } catch (RuntimeException e) {
            LOGGER.atFinest().withCause(e).log("Failed to delete location group");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            LocationGroupDao locGroupDao = new LocationGroupDao(context);
            outletDao.deleteOutlet(TAINTER_GATE_10_LOC.getOfficeId(), TAINTER_GATE_10_LOC.getName(),
                                   DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(TAINTER_GATE_20_LOC.getOfficeId(), TAINTER_GATE_20_LOC.getName(),
                                   DeleteRule.DELETE_ALL);
            deleteLocGroup(locGroupDao);
            deleteLocation(context, TAINTER_GATE_10_LOC.getOfficeId(), TAINTER_GATE_10_LOC.getName());
            deleteLocation(context, TAINTER_GATE_20_LOC.getOfficeId(), TAINTER_GATE_20_LOC.getName());
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void testCrud() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao dao = new OutletDao(context);
            dao.storeOperationalChanges(Arrays.asList(CHANGE_1, CHANGE_2), true);
            List<GateChange> changes = dao.retrieveOperationalChanges(PROJECT_1_ID, JAN_FIRST, JAN_SECOND, true, true,
                                                                      "EN", 3);
            assertContainsAll(changes, CHANGE_1, CHANGE_2);

            GateChange modifiedChange = new GateChange.Builder(CHANGE_1).withTailwaterElevation(50.0).build();

            dao.storeOperationalChanges(Collections.singletonList(modifiedChange), true);
            changes = dao.retrieveOperationalChanges(PROJECT_1_ID, JAN_FIRST, JAN_SECOND, true, true, "EN", 3);

            assertContainsAll(changes, modifiedChange, CHANGE_2);

            dao.deleteOperationalChanges(PROJECT_1_ID, JAN_FIRST, JAN_SECOND, true);
            changes = dao.retrieveOperationalChanges(PROJECT_1_ID, JAN_FIRST, JAN_SECOND, true, true, "EN", 3);

            assertTrue(changes.isEmpty());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void assertContainsAll(List<GateChange> retrievedChanges, GateChange ... changes) {
        assertAll(Arrays.stream(changes)
                       .map(change -> () -> assertContains(retrievedChanges, change)));
    }

    private void assertContains(List<GateChange> retrievedChanges, GateChange change) {
        GateChange retrievedChange = retrievedChanges.stream()
                                                     .filter(c -> c.getChangeDate().equals(change.getChangeDate()))
                                                     .findFirst()
                                                     .orElse(null);
        DTOMatch.assertMatch(change, retrievedChange);
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId) {
        return new Outlet.Builder().withProjectId(
                                           new CwmsId.Builder().withName(projectLoc.getName()).withOfficeId(projectLoc.getOfficeId()).build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .build();
    }

    static GateChange buildTestGateChange(CwmsId projectId, Instant changeDate, GateSetting... settingVargs) {
        LookupType dischargeComputationType = new LookupType.Builder().withActive(true)
                                                                      .withDisplayValue("A")
                                                                      .withTooltip("Adjusted by an automated method")
                                                                      .withOfficeId(OFFICE_ID)
                                                                      .build();
        LookupType reasonType = new LookupType.Builder().withActive(true)
                                                        .withDisplayValue("O")
                                                        .withTooltip("Other release")
                                                        .withOfficeId(OFFICE_ID)
                                                        .build();
        boolean isProtected = true;
        Double newTotalDischargeOverride = 1.0;
        Double oldTotalDischargeOverride = 2.0;
        String dischargeUnits = "cfs";
        Double poolElevation = 3.0;
        Double tailwaterElevation = 4.0;
        String elevationUnits = "ft";
        String notes = "Test notes";
        List<GateSetting> settings = Arrays.asList(settingVargs);

        return new GateChange.Builder().withProjectId(projectId)
                                       .withDischargeComputationType(dischargeComputationType)
                                       .withReasonType(reasonType)
                                       .withProtected(isProtected)
                                       .withNewTotalDischargeOverride(newTotalDischargeOverride)
                                       .withOldTotalDischargeOverride(oldTotalDischargeOverride)
                                       .withDischargeUnits(dischargeUnits)
                                       .withPoolElevation(poolElevation)
                                       .withTailwaterElevation(tailwaterElevation)
                                       .withElevationUnits(elevationUnits)
                                       .withNotes(notes)
                                       .withChangeDate(changeDate)
                                       .withSettings(settings)
                                       .build();
    }

    static GateSetting buildTestGateSetting(CwmsId locationId, double opening, double invertElev) {
        return new GateSetting.Builder().withLocationId(locationId)
                                        .withOpening(opening)
                                        .withOpeningParameter("Opening")
                                        .withOpeningUnits("ft")
                                        .withInvertElevation(invertElev)
                                        .build();
    }
}
