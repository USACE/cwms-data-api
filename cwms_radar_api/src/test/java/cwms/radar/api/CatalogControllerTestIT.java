package cwms.radar.api;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.isNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import cwms.radar.formatters.Formats;
import fixtures.RadarApiSetupCallback;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;

import static io.restassured.RestAssured.*;
import io.restassured.matcher.RestAssuredMatchers.*;
import io.restassured.response.Response;

import static org.hamcrest.Matchers.*;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class CatalogControllerTestIT {


    @BeforeAll
    public static void before_all(){

    }

    @Test
    public void test_no_aliased_results_returned(){
        given().accept(Formats.JSONV2)
        .queryParam("office", "SPK")
        .get("/catalog/TIMESERIES").then().assertThat()
        .statusCode(is(200))
        .body("$",hasKey("total"))
        .body("total",is(4))
        .body("entries.size()",is(4));
    }


    @Test
    public void test_queries_are_case_insensitive(){
        given().accept("application/json;version=2")
        .queryParam("office", "SPK")
        .queryParam("like","ALDER.*")
        .get("/catalog/TIMESERIES").then().assertThat()
        .statusCode(is(200))
        .body("$",hasKey("total"))
        .body("total",is(2))
        .body("entries.size()",is(2));
    }

    @Test
    public void test_all_office_pagination_works() {
        final int pageSize = 2;
        Response initialResponse = 
            given().accept(Formats.JSONV2)
            .queryParam("page-size",pageSize)
            .get("/catalog/TIMESERIES")
            .then()
                .assertThat()
                .statusCode(is(200))
                .body("$",hasKey("total"))
                .body("$",hasKey("next-page"))
                .body("page-size",is(pageSize))
                .body("entries.size()",is(pageSize))
                .extract()
                    .response();
        String nextPage = initialResponse.path("next-page");
        
        String firstRowFirstPage = initialResponse.path("entries[0].name");

        given().accept(Formats.JSONV2)
            .queryParam("page",nextPage)
            .get("/catalog/TIMESERIES")
            .then()
                .assertThat()
                .statusCode(is(200))
                .body("$",hasKey("total"))
                .body("$",hasKey("next-page"))
                .body("page",equalTo(nextPage))
                .body("page-size",is(pageSize))
                .body("next-page",not(equalTo(nextPage)))
                .body("entries[0].name",not(equalTo(firstRowFirstPage)));
                ;
    }
}
