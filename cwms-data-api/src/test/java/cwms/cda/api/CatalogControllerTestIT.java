package cwms.cda.api;

import static cwms.cda.api.Controllers.BOUNDING_OFFICE_LIKE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

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

    @BeforeAll
    public static void setup_data() throws Exception {
        // Create some locations and create some ts.
        createLocation("Alder Springs",true,"SPK");
        createLocation("Wet Meadows",true,"SPK");
        createLocation("Pine Flat-Outflow",true,"SPK");
        createTimeseries("SPK","Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda");
        createTimeseries("SPK","Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.one");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.two");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.three");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.four");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.one");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.two");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.three");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.four");

        // Complicated
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_setup.sql");


    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_cleanup.sql");
    }

    @Test
    void test_no_aliased_results_returned() {
        given().accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(OFFICE, "SPK")
            .queryParam("like",".*-cda$")
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
            .queryParam("office", "SPK")
            .queryParam("like","alder spRINgs.*-CDA$")
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

                lastRowPreviousPage = pageN.path("entries.last().name");
                int pageTotal = pageN.path("entries.size()");
                totalRetrieved += pageTotal;
                /*if( nextPage == null && totalRetrieved < total) {
                    fail("Pagination not complete, system returned 'last page' before all values retrieved.");
                }*/
            } while( nextPage != null );

            assertEquals(total, totalRetrieved, "Initial count and retrieval do not match");
        }, "Catalog retrieval got stuck; possibly in endless loop");
    }


    @Test
    void test_loc_group_with_ts_group() {


        String nToZ = "N to Z";
        String evens = "Evens";

        // filter by loc group and ts group should find the intersection
        given()
                .accept("application/json;version=2")
                .queryParam(OFFICE, "SPK")
                .queryParam(LOCATION_CATEGORY_LIKE,"Test Category")
                .queryParam(LOCATION_GROUP_LIKE, nToZ)
                .queryParam(TIMESERIES_CATEGORY_LIKE,"Test Category")
                .queryParam(TIMESERIES_GROUP_LIKE, evens)
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

        String aToM = "A to M";

        // filter by just loc group
        given()
                .accept("application/json;version=2")
                .queryParam(OFFICE, "SPK")
                .queryParam(LOCATION_CATEGORY_LIKE,"Test Category")
                .queryParam(LOCATION_GROUP_LIKE, aToM)
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

        String nToZ = "N to Z";

        // filter by just loc group
        given()
                .accept("application/json;version=2")
                .queryParam(OFFICE, "SPK")
                .queryParam(LOCATION_CATEGORY_LIKE,"Test Category")
                .queryParam(LOCATION_GROUP_LIKE, nToZ)
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
                .queryParam(BOUNDING_OFFICE_LIKE, "SPK")
                .queryParam(LIKE, "^Wet Meadows.*")
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


}
