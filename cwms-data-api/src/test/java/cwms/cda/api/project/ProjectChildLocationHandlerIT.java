/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.project;


import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.PROJECT_LIKE;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.core.Is.is;

import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.EmbankmentDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.data.dto.project.Project;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ProjectChildLocationHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    @Test
    void test_get_embankment() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);

            ProjectDao prjDao = new ProjectDao(dsl);

            String projectName = "projChild";
            CwmsId projectId = new CwmsId.Builder()
                    .withName(projectName)
                    .withOfficeId(OFFICE)
                    .build();

            Project testProject = buildTestProject(OFFICE, projectName);
            prjDao.create(testProject);

            String locName1 = "TEST_LOCATION1";
            String locName2 = "TEST_LOCATION2";
            try {
                createLocation(locName1, true, OFFICE);
                createLocation(locName2, true, OFFICE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            LocationsDaoImpl locationsDao = new LocationsDaoImpl(dsl);
            Location location1 = buildTestLocation(OFFICE, locName1);
            try {
                locationsDao.storeLocation(location1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Location location2 = buildTestLocation(OFFICE, locName2);
            try {
                locationsDao.storeLocation(location2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


            Embankment embank = buildTestEmbankment(location1, projectId);
            EmbankmentDao embankmentDao = new EmbankmentDao(dsl);
            embankmentDao.storeEmbankment(embank, false);

            Embankment embank2 = buildTestEmbankment(location2, projectId);
            embankmentDao.storeEmbankment(embank2, false);
            try {

                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSON)
                    .queryParam(Controllers.OFFICE, OFFICE)
                    .queryParam(PROJECT_LIKE, projectName)
                    .queryParam(LOCATION_KIND_LIKE, "(EMBANKMENT|TURBINE)")
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .get("/projects/child-locations/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK))
                    .body("size()", is(1))
                    .body("[0].project.office-id", equalToIgnoringCase(OFFICE))
                    .body("[0].project.name", equalToIgnoringCase(projectName))
                    .body("[0].embankments.size()", is(2))
                    .body("[0].embankments[0].office-id", equalToIgnoringCase(OFFICE))
                    .body("[0].embankments[0].name", equalToIgnoringCase(locName1))
                    .body("[0].embankments[1].office-id", equalToIgnoringCase(OFFICE))
                    .body("[0].embankments[1].name", equalToIgnoringCase(locName2))
                ;
            } finally {
                embankmentDao.deleteEmbankment(embank.getLocation().getName(), OFFICE, DeleteRule.DELETE_ALL );
                embankmentDao.deleteEmbankment(embank2.getLocation().getName(), OFFICE, DeleteRule.DELETE_ALL);
                prjDao.delete(projectId.getOfficeId(), projectId.getName(), DeleteRule.DELETE_ALL);
            }
        });

    }

    private Embankment buildTestEmbankment(Location location,  CwmsId projId) {
        return new Embankment.Builder()
                .withLocation(location)
                .withMaxHeight(5.0)
                .withProjectId(projId)
                .withStructureLength(10.0)
                .withStructureType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Rolled Earth-Filled")
                        .withTooltip("An embankment formed by compacted earth")
                        .withActive(true)
                        .build())
                .withDownstreamProtectionType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Arch Facing")
                        .withTooltip("Protected by the faces of the concrete arches")
                        .withActive(true)
                        .build())
                .withUpstreamProtectionType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Blanket")
                        .withTooltip("Protected by blanket of concrete")
                        .withActive(true)
                        .build())
                .withUpstreamSideSlope(15.0)
                .withLengthUnits("ft")
                .withTopWidth(20.0)
                .withStructureLength(25.0)
                .withDownstreamSideSlope(90.0)
                .build();
    }

    private Location buildTestLocation(String office, String name) {
        return new Location.Builder(name, "EMBANKMENT", ZoneId.of("UTC"),
                50.0, 50.0, "NVGD29", office)
                .withElevation(10.0)
                .withElevationUnits("ft")
                .withLocationType("SITE")
                .withActive(true)
                .withNation(Nation.US)
                .withStateInitial("CA")
                .withCountyName("Yolo")
                .withBoundingOfficeId(office)
                .withPublishedLatitude(38.54)
                .withPublishedLongitude(-121.74)
                .withDescription("for testing")
                .withNearestCity("Davis")
                .build();
    }

    public static Project buildTestProject(String office, String prjId) {
        Location pbLoc = new Location.Builder(office,prjId + "-PB")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();
        Location ngLoc = new Location.Builder(office,prjId + "-NG")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Location prjLoc = new Location.Builder(office, prjId)
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        return new Project.Builder()
                .withLocation(prjLoc)
                .withProjectOwner("Project Owner")
                .withAuthorizingLaw("Authorizing Law")
                .withFederalCost(BigDecimal.valueOf(100.0))
                .withNonFederalCost(BigDecimal.valueOf(50.0))
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
                .withCostYear(Instant.now())
                .withCostUnit("$")
                .withYieldTimeFrameEnd(Instant.now())
                .withYieldTimeFrameStart(Instant.now())
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
                .withProjectRemarks("Remarks")
                .withPumpBackLocation(pbLoc)
                .withNearGageLocation(ngLoc)
                .withBankFullCapacityDesc("Bank Full Capacity Description")
                .withDownstreamUrbanDesc("Downstream Urban Description")
                .withHydropowerDesc("Hydropower Description")
                .withSedimentationDesc("Sedimentation Description")
                .build();

    }

}
