package cwms.cda.api;

import static cwms.cda.api.Controllers.BOUNDING_OFFICE_LIKE;
import static cwms.cda.api.Controllers.EXCLUDE_EMPTY;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.Duration;

import java.time.ZoneId;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.formatters.Formats;

import static io.restassured.RestAssured.*;

import io.restassured.filter.log.LogDetail;
import io.restassured.response.Response;

import static org.hamcrest.Matchers.*;

@Tag("integration")
public class CatalogControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    //// These have to match the groups in ts_catalog_setup.sql
    public static final String A_TO_M = "A to M";
    public static final String N_TO_Z = "N to Z";
    public static final String EVENS = "Evens";
    public static final String TEST_CATEGORY = "Test Category";
    ////

    @BeforeAll
    public static void setup_data() throws Exception {
        // Create some locations and create some ts.
        createLocation("Alder Springs",true, OFFICE);
        createLocation("Wet Meadows",true, OFFICE);
        createLocation("Pine Flat-Outflow",true, OFFICE);
        createLocation("Flat Lake",true, OFFICE);

        createProject("Flat Project", OFFICE);
        createTimeseries(OFFICE,"Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.one");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.two");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.three");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.four");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.one");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.two");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.three");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.four");

        // Complicated
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_setup.sql");


    }

    private static void createProject(String id, String office) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            ProjectDao projectDao = new ProjectDao(dsl);
            Project project = new Project.Builder()
                    .withLocation(new Location.Builder(id,
                            "PROJECT",
                            ZoneId.of("UTC"),
                            0.0,
                            0.0,
                            "WGS84",
                            office)
                            .build())
                    .build();
        projectDao.create(project, true);
        });
    }

    private static void deleteProject(String id, String office) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            ProjectDao projectDao = new ProjectDao(dsl);

            projectDao.delete(office, id, DeleteRule.DELETE_KEY);
        });
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_cleanup.sql");
        deleteProject("Flat Project", OFFICE);
    }

    @Test
    void test_no_aliased_results_returned() {
        given().accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY,false)
            .queryParam(LIKE,".*-cda$")
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$",hasKey("total"))
            .body("total",is(4))
            .body("entries.size()",is(4));
    }


    @Test
    void test_queries_are_case_insensitive() {
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY,false)
            .queryParam(LIKE,"alder spRINgs.*-CDA$")
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$",hasKey("total"))
            .body("total",is(2))
            .body("entries.size()",is(2));
    }

    @Test
    void test_all_office_pagination_works() {
        assertTimeout(Duration.ofMinutes(5), () -> {
            final int pageSize = 50;
            Response initialResponse =
                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                    .queryParam("page-size",pageSize)
                    .queryParam(EXCLUDE_EMPTY,false)
                .when()
                    .get("/catalog/TIMESERIES")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                    .statusCode(is(200))
                    .body("$",hasKey("total"))
                    .body("$",hasKey("next-page"))
                    .body("page-size",is(pageSize))
                    .body("entries.size()",is(pageSize))
                    .extract()
                .response();
            String nextPage = initialResponse.path("next-page");

            final int total = initialResponse.path("total");
            int totalRetrieved = initialResponse.path("entries.size()");

            String lastRowPreviousPage = initialResponse.path("entries.last().name");

            do {
                Response pageN =
                    given()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .accept(Formats.JSONV2)
                        .queryParam("page",nextPage)
                        .queryParam(EXCLUDE_EMPTY,false)
                    .when()
                        .get("/catalog/TIMESERIES")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .assertThat()
                        .statusCode(is(200))
                        .body("$",hasKey("total"))
                        .body("page-size",is(pageSize))
                        .body("page",equalTo(nextPage))
                        .body("entries[0].name",not(equalTo(lastRowPreviousPage)))
                        .extract()
                    .response();
                nextPage = pageN.path("next-page");
                int pageTotal = pageN.path("entries.size()");
                if (pageTotal > 0) {
                    lastRowPreviousPage = pageN.path("entries.last().name");
                } else {
                    lastRowPreviousPage = "No data in this response.";
                }
                totalRetrieved += pageTotal;
            } while( nextPage != null );
            assertEquals(total, totalRetrieved, "Expected amount of results not returned.");
        }, "Catalog retrieval got stuck; possibly in endless loop");
    }


    @Test
    void test_loc_group_with_ts_group() {


        // filter by loc group and ts group should find the intersection
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, N_TO_Z)
            .queryParam(TIMESERIES_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(TIMESERIES_GROUP_LIKE, EVENS)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(4))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(4))
            .body("entries[0].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.four"))
            .body("entries[1].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.two"))
            .body("entries[2].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.four"))
            .body("entries[3].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.two"))
            ;
    }

    @Test
    void test_loc_group() {


        // filter by just loc group
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, A_TO_M)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(2))
            .body("entries[0].name",equalTo("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda"))
            .body("entries[1].name",equalTo("Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda"))
        ;



        // filter by just loc group
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, N_TO_Z)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(10))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(10))
            .body("entries[0].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.four"))
            .body("entries[1].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.one"))
            .body("entries[2].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda"))
            .body("entries[3].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.three"))
            .body("entries[4].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.two"))
            .body("entries[5].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.four"))
            .body("entries[6].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.one"))
            .body("entries[7].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda"))
            .body("entries[8].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.three"))
            .body("entries[9].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.two"))
        ;

    }


    @Test
    void test_ts_with_bounding() {

        // we create Wet Meadows with a bounding office of SPK
        given()
            .accept("application/json;version=2")
            .queryParam(BOUNDING_OFFICE_LIKE, OFFICE)
            .queryParam(LIKE, "^Wet Meadows.*")
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", greaterThan(3))
            .body("$", hasKey("entries"))
            .body("entries.size()",greaterThan(3))
            .body("entries.name",everyItem(startsWith("Wet Meadows")))
        ;
    }

    @Test
    void test_loc_kind() {

        String pattern = "^Flat";

        // First with just the regex.  This should match Flat Lake and Flat Project
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(2))
        ;

        // Now add the LOCATION_KIND filter
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
            .queryParam(LOCATION_KIND_LIKE, "PROJECT")  // just Flat Project
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(1))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(1))
            .body("entries[0].name", equalTo("Flat Project"))
        ;
    }

}
