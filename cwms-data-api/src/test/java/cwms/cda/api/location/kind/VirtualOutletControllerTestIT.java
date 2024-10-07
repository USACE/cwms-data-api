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

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dao.location.kind.ProjectStructureIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.data.dto.location.kind.VirtualOutlet;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@Tag("integration")
class VirtualOutletControllerTestIT  extends ProjectStructureIT {
    private static final String OUTLET_KIND = "OUTLET";
    private static final CwmsId VIRTUAL_OUTLET_RATING_GROUP = new CwmsId.Builder().withName("Rating-" + PROJECT_LOC2.getName() + "-VirtualOutlet")
                                                                                  .withOfficeId(OFFICE_ID)
                                                                                  .build();

    private static final CwmsId EXISTING_VIRTUAL_OUTLET_ID = new CwmsId.Builder().withName("Virtual Outlet 10")
                                                                                 .withOfficeId(OFFICE_ID)
                                                                                 .build();
    private static final CwmsId MISSING_VIRTUAL_OUTLET_ID = new CwmsId.Builder().withName("Virtual Outlet 30")
                                                                                .withOfficeId(OFFICE_ID)
                                                                                .build();

    private static final Location CO1_I53 = buildProjectStructureLocation("I530", OUTLET_KIND);
    private static final Location CO1_I25 = buildProjectStructureLocation("I250", OUTLET_KIND);
    private static final Location CO1_LOW_FLOW = buildProjectStructureLocation("Low Flow Gate 0", OUTLET_KIND);

    private static final Location CO2_INTAKE = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-Intake1",
                                                                             OUTLET_KIND);
    private static final Location CO2_WEIR = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-Weir1",
                                                                           OUTLET_KIND);
    private static final Location CO2_CONDUIT = buildProjectStructureLocation(PROJECT_LOC2.getName() + "-CO2Conduit1",
                                                                              OUTLET_KIND);

    private static final Location CO3_I1 = buildProjectStructureLocation("I10", OUTLET_KIND);
    private static final Location CO3_I2 = buildProjectStructureLocation("I20", OUTLET_KIND);
    private static final Location CO3_I3 = buildProjectStructureLocation("I30", OUTLET_KIND);
    private static final Location CO3_CONDUIT = buildProjectStructureLocation("Conduit1", OUTLET_KIND);

    private static final List<VirtualOutletRecord> EXISTING_VIRTUAL_OUTLET_RECORDS = Arrays.asList(
            buildVirtualOutletRecord(CO1_I25, CO1_LOW_FLOW), buildVirtualOutletRecord(CO1_I53, CO1_LOW_FLOW),
            buildVirtualOutletRecord(CO1_LOW_FLOW));

    private static final List<VirtualOutletRecord> MULTI_DS_RECORDS = Arrays.asList(
            buildVirtualOutletRecord(CO2_INTAKE, CO2_WEIR, CO2_CONDUIT), buildVirtualOutletRecord(CO2_WEIR),
            buildVirtualOutletRecord(CO2_CONDUIT));

    private static final List<VirtualOutletRecord> MISSING_VIRTUAL_OUTLET_RECORDS = Arrays.asList(
            buildVirtualOutletRecord(CO3_I1, CO3_CONDUIT), buildVirtualOutletRecord(CO3_I2, CO3_CONDUIT),
            buildVirtualOutletRecord(CO3_I3, CO3_CONDUIT), buildVirtualOutletRecord(CO3_CONDUIT));
    private static final Outlet CO1_I25_OUTLET = buildTestOutlet(CO1_I25);
    private static final Outlet CO1_I53_OUTLET = buildTestOutlet(CO1_I53);
    private static final Outlet CO1_LOW_FLOW_OUTLET = buildTestOutlet(CO1_LOW_FLOW);
    private static final Outlet CO2_CONDUIT_OUTLET = buildTestOutlet(CO2_CONDUIT);
    private static final Outlet CO2_INTAKE_OUTLET = buildTestOutlet(CO2_INTAKE);
    private static final Outlet CO2_WEIR_OUTLET = buildTestOutlet(CO2_WEIR);
    private static final Outlet CO3_I1_OUTLET = buildTestOutlet(CO3_I1);
    private static final Outlet CO3_I2_OUTLET = buildTestOutlet(CO3_I2);
    private static final Outlet CO3_I3_OUTLET = buildTestOutlet(CO3_I3);
    private static final Outlet CO3_CONDUIT_OUTLET = buildTestOutlet(CO3_CONDUIT);

    private static final VirtualOutlet EXISTING_VIRTUAL_OUTLET = new VirtualOutlet.Builder().withProjectId(PROJECT_2_ID)
                                                                                            .withVirtualOutletId(
                                                                                                    EXISTING_VIRTUAL_OUTLET_ID)
                                                                                            .withVirtualRecords(
                                                                                                    EXISTING_VIRTUAL_OUTLET_RECORDS)
                                                                                            .build();
    private static final VirtualOutlet MISSING_VIRTUAL_OUTLET = new VirtualOutlet.Builder().withProjectId(PROJECT_2_ID)
                                                                                           .withVirtualOutletId(
                                                                                                   MISSING_VIRTUAL_OUTLET_ID)
                                                                                           .withVirtualRecords(
                                                                                                   MISSING_VIRTUAL_OUTLET_RECORDS)
                                                                                           .build();

    @BeforeAll
    static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            try {
                storeLocation(context, CO1_I25);
                storeLocation(context, CO1_I53);
                storeLocation(context, CO1_LOW_FLOW);
                storeLocation(context, CO2_CONDUIT);
                storeLocation(context, CO2_INTAKE);
                storeLocation(context, CO2_WEIR);
                storeLocation(context, CO3_I1);
                storeLocation(context, CO3_I2);
                storeLocation(context, CO3_I3);
                storeLocation(context, CO3_CONDUIT);

                outletDao.storeOutlet(CO1_I25_OUTLET, false);
                outletDao.storeOutlet(CO1_I53_OUTLET, false);
                outletDao.storeOutlet(CO1_LOW_FLOW_OUTLET, false);
                outletDao.storeOutlet(CO2_CONDUIT_OUTLET, false);
                outletDao.storeOutlet(CO2_INTAKE_OUTLET, false);
                outletDao.storeOutlet(CO2_WEIR_OUTLET, false);
                outletDao.storeOutlet(CO3_I1_OUTLET, false);
                outletDao.storeOutlet(CO3_I2_OUTLET, false);
                outletDao.storeOutlet(CO3_I3_OUTLET, false);
                outletDao.storeOutlet(CO3_CONDUIT_OUTLET, false);

                outletDao.storeVirtualOutlet(EXISTING_VIRTUAL_OUTLET, false);
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
            OutletDao outletDao = new OutletDao(context);

            outletDao.deleteVirtualOutlet(PROJECT_LOC2.getOfficeId(), PROJECT_LOC2.getName(),
                                          EXISTING_VIRTUAL_OUTLET_ID.getName(), DeleteRule.DELETE_ALL);

            outletDao.deleteOutlet(CO1_I25.getOfficeId(), CO1_I25.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO1_I53.getOfficeId(), CO1_I53.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO1_LOW_FLOW.getOfficeId(), CO1_LOW_FLOW.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_CONDUIT.getOfficeId(), CO2_CONDUIT.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_INTAKE.getOfficeId(), CO2_INTAKE.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO2_WEIR.getOfficeId(), CO2_WEIR.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I1.getOfficeId(), CO3_I1.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I2.getOfficeId(), CO3_I2.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_I3.getOfficeId(), CO3_I3.getName(), DeleteRule.DELETE_ALL);
            outletDao.deleteOutlet(CO3_CONDUIT.getOfficeId(), CO3_CONDUIT.getName(), DeleteRule.DELETE_ALL);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_get_all() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + PROJECT_2_ID.getOfficeId() + "/" + PROJECT_2_ID.getName() + "/virtual-outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("virtual-outlet-id.name", contains(EXISTING_VIRTUAL_OUTLET.getVirtualOutletId().getName()))
            .body("virtual-outlet-id.office-id", contains(EXISTING_VIRTUAL_OUTLET.getVirtualOutletId().getOfficeId()))
            .body("project-id.name", contains(EXISTING_VIRTUAL_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", contains(EXISTING_VIRTUAL_OUTLET.getProjectId().getOfficeId()))
        ;
    }

    @Test
    void test_crud() {
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, VirtualOutlet.class), MISSING_VIRTUAL_OUTLET);

        //Create the virtual outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/virtual-outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));


        //Read the virtual outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + PROJECT_2_ID.getOfficeId() + "/" + PROJECT_2_ID.getName() + "/virtual-outlets/" + MISSING_VIRTUAL_OUTLET_ID.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("virtual-outlet-id.name", equalTo(MISSING_VIRTUAL_OUTLET.getVirtualOutletId().getName()))
            .body("virtual-outlet-id.office-id", equalTo(MISSING_VIRTUAL_OUTLET.getVirtualOutletId().getOfficeId()))
            .body("project-id.name", equalTo(MISSING_VIRTUAL_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", equalTo(MISSING_VIRTUAL_OUTLET.getProjectId().getOfficeId()))
        ;

        //Update the virtual outlet
        //Use a totally different set of virtual records.
        VirtualOutlet temp = new VirtualOutlet.Builder().withVirtualOutletId(MISSING_VIRTUAL_OUTLET.getVirtualOutletId())
                                                        .withVirtualRecords(MULTI_DS_RECORDS)
                                                        .withProjectId(MISSING_VIRTUAL_OUTLET.getProjectId())
                                                        .build();
        json = Formats.format(Formats.parseHeader(Formats.JSONV1, VirtualOutlet.class), temp);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/virtual-outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Delete the virtual outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .queryParam(METHOD, DeleteRule.DELETE_ALL.toString())
            .header(AUTH_HEADER, USER.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + PROJECT_2_ID.getOfficeId() + "/" + PROJECT_2_ID.getName() + "/virtual-outlets/" + MISSING_VIRTUAL_OUTLET_ID.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Confirm deletion of virtual outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + PROJECT_2_ID.getOfficeId() + "/" + PROJECT_2_ID.getName() + "/virtual-outlets/" + MISSING_VIRTUAL_OUTLET_ID.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    private static Outlet buildTestOutlet(Location outletLoc) {
        return new Outlet.Builder().withProjectId(
                                           new CwmsId.Builder().withName(ProjectStructureIT.PROJECT_LOC2.getName())
                                                               .withOfficeId(ProjectStructureIT.PROJECT_LOC2.getOfficeId())
                                                               .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(VIRTUAL_OUTLET_RATING_GROUP)
                                   .build();
    }

    private static VirtualOutletRecord buildVirtualOutletRecord(Location upstream, Location... downstream) {
        if (downstream == null || downstream.length == 0) {
            return new VirtualOutletRecord.Builder().withOutletId(
                                                            new CwmsId.Builder().withName(upstream.getName()).withOfficeId(upstream.getOfficeId()).build())
                                                    .build();
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
