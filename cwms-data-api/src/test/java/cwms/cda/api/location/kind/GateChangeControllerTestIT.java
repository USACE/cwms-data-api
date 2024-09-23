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

package cwms.cda.api.location.kind;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.location.kind.BaseOutletDaoIT;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.GateSetting;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

class GateChangeControllerTestIT extends BaseOutletDaoIT {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String OUTLET_KIND = "OUTLET";
    private static final String CONDUIT_GATE_RATING_SPEC_ID = PROJECT_1_ID.getName() + ".Opening-ConduitGate,Elev;Flow-ConduitGate.Standard.Production";
    private static final CwmsId CONDUIT_GATE_RATING_GROUP = new CwmsId.Builder().withName(
            "Rating-" + PROJECT_1_ID.getName() + "-ConduitGate").withOfficeId(OFFICE_ID).build();
    private static final CwmsId CONDUIT_GATE_1_ID = new CwmsId.Builder().withName(PROJECT_1_ID.getName() + "-CG100")
                                                                        .withOfficeId(OFFICE_ID)
                                                                        .build();
    private static final CwmsId CONDUIT_GATE_2_ID = new CwmsId.Builder().withName("CG200")
                                                                        .withOfficeId(OFFICE_ID)
                                                                        .build();
    private static final Location CONDUIT_GATE_1 = buildProjectStructureLocation(CONDUIT_GATE_1_ID.getName(),
                                                                                 OUTLET_KIND);
    private static final Location CONDUIT_GATE_2 = buildProjectStructureLocation(CONDUIT_GATE_2_ID.getName(),
                                                                                 OUTLET_KIND);
    private static final Outlet CONDUIT_GATE_1_OUTLET = buildTestOutlet(CONDUIT_GATE_1, PROJECT_LOC,
                                                                        CONDUIT_GATE_RATING_GROUP);
    private static final Outlet CONDUIT_GATE_2_OUTLET = buildTestOutlet(CONDUIT_GATE_2, PROJECT_LOC,
                                                                        CONDUIT_GATE_RATING_GROUP);

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
                                                                   buildTestGateSetting(CONDUIT_GATE_1_ID, 10, 20));
    private static final GateChange CHANGE_2 = buildTestGateChange(PROJECT_1_ID, JAN_SECOND,
                                                                   buildTestGateSetting(CONDUIT_GATE_1_ID, 1, 2),
                                                                   buildTestGateSetting(CONDUIT_GATE_2_ID, 3, 4));

    @BeforeAll
    public static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            try {
                deleteLocationGroup(context, CONDUIT_GATE_1_OUTLET);
                storeLocation(context, CONDUIT_GATE_1);
                storeLocation(context, CONDUIT_GATE_2);
                outletDao.storeOutlet(CONDUIT_GATE_1_OUTLET, false);
                outletDao.storeOutlet(CONDUIT_GATE_2_OUTLET, false);
                createRatingSpecForOutlet(context, CONDUIT_GATE_1_OUTLET, CONDUIT_GATE_RATING_SPEC_ID);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            outletDao.deleteOutlet(CONDUIT_GATE_1.getOfficeId(), CONDUIT_GATE_1.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CONDUIT_GATE_2.getOfficeId(), CONDUIT_GATE_2.getName(), DeleteRule.DELETE_ALL);
            try {
                outletDao.deleteOperationalChanges(PROJECT_1_ID, null, null, true);
            } catch (RuntimeException ex) {
                LOGGER.atFinest().withCause(ex).log("We don't care about this...");
            }
            deleteLocation(context, CONDUIT_GATE_1.getOfficeId(), CONDUIT_GATE_1.getName());
            deleteLocation(context, CONDUIT_GATE_2.getOfficeId(), CONDUIT_GATE_2.getName());
            deleteLocationGroup(context, CONDUIT_GATE_1_OUTLET);
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_changes_crud() {
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, GateChange.class), Arrays.asList(CHANGE_1, CHANGE_2), GateChange.class);

        //Create the gate changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/gate-changes")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        String allJson = given().log()
                                    .ifValidationFails(LogDetail.ALL, true)
                                    .contentType(Formats.JSONV1)
                                .when()
                                    .redirects()
                                    .follow(true)
                                    .redirects()
                                    .max(3)
                                    .queryParam(BEGIN, JAN_FIRST.toString())
                                    .queryParam(END, JAN_SECOND.toString())
                                    .queryParam(START_TIME_INCLUSIVE, true)
                                    .queryParam(END_TIME_INCLUSIVE, true)
                                    .get("projects/" + OFFICE_ID + "/" + PROJECT_1_ID.getName() + "/gate-changes")
                                .then()
                                    .log()
                                    .ifValidationFails(LogDetail.ALL, true)
                                    .assertThat()
                                    .statusCode(is(HttpServletResponse.SC_OK))
                                    .extract()
                                    .body()
                                    .asString();
        List<GateChange> changes = Formats.parseContentList(Formats.parseHeader(Formats.JSONV1, GateChange.class),
                                                            allJson, GateChange.class);

        DTOMatch.assertContainsDto(changes, CHANGE_1, this::isSimilar, DTOMatch::assertMatch, "Does not contain modified change for " + CHANGE_1.getChangeDate());
        DTOMatch.assertContainsDto(changes, CHANGE_2, this::isSimilar, DTOMatch::assertMatch, "Does not contain modified change for " + CHANGE_2.getChangeDate());

        GateSetting modifiedSetting = buildTestGateSetting(CONDUIT_GATE_2_ID, 30, 40);
        GateChange modifiedChange = new GateChange.Builder(CHANGE_1).withSettings(Collections.singletonList(modifiedSetting))
                                                                    .build();

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, GateChange.class), Collections.singletonList(modifiedChange), GateChange.class);
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/gate-changes")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        allJson = given().log()
                             .ifValidationFails(LogDetail.ALL, true)
                             .contentType(Formats.JSONV1)
                         .when()
                             .redirects()
                             .follow(true)
                             .redirects()
                             .max(3)
                             .queryParam(BEGIN, JAN_FIRST.toString())
                             .queryParam(END, JAN_SECOND.toString())
                             .queryParam(START_TIME_INCLUSIVE, true)
                             .queryParam(END_TIME_INCLUSIVE, true)
                             .get("projects/" + OFFICE_ID + "/" + PROJECT_1_ID.getName() + "/gate-changes")
                         .then()
                             .log()
                             .ifValidationFails(LogDetail.ALL, true)
                             .assertThat()
                             .statusCode(is(HttpServletResponse.SC_OK))
                             .extract()
                             .body()
                             .asString();

        changes = Formats.parseContentList(Formats.parseHeader(Formats.JSONV1, GateChange.class),
                                           allJson, GateChange.class);

        DTOMatch.assertContainsDto(changes, modifiedChange, this::isSimilar, DTOMatch::assertMatch, "Does not contain modified change for " + modifiedChange.getChangeDate());

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(BEGIN, JAN_FIRST.toString())
            .queryParam(END, JAN_SECOND.toString())
            .queryParam(OVERRIDE_PROTECTION, "true")
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + PROJECT_1_ID.getOfficeId() + "/" + PROJECT_1_ID.getName() + "/gate-changes")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        given()
            .log()
            .ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
        .when()
            .redirects()
            .follow(true)
            .redirects()
            .max(3)
            .queryParam(BEGIN, JAN_FIRST.toString())
            .queryParam(END, JAN_SECOND.toString())
            .queryParam(START_TIME_INCLUSIVE, true)
            .queryParam(END_TIME_INCLUSIVE, true)
            .get("projects/" + OFFICE_ID + "/" + PROJECT_1_ID.getName() + "/gate-changes")
        .then()
            .log()
            .ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    private boolean isSimilar(GateChange left, GateChange right) {
        boolean output = false;

        if (left != null && right != null) {
            output = left.getChangeDate().equals(right.getChangeDate());
        }

        return output;
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId, String ratingSpecId) {
        return new Outlet.Builder().withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                                                                      .withOfficeId(projectLoc.getOfficeId())
                                                                      .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .withRatingSpecId(ratingSpecId)
                                   .build();
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId) {
        return buildTestOutlet(outletLoc, projectLoc, ratingId, null);
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