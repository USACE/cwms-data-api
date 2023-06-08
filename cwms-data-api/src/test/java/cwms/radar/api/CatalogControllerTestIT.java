package cwms.radar.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


import cwms.radar.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;

import static io.restassured.RestAssured.*;
import io.restassured.response.Response;

import static org.hamcrest.Matchers.*;

@Tag("integration")
@ExtendWith(CwmsDataApiSetupCallback.class)
public class CatalogControllerTestIT extends DataApiTestIT {

    

    @BeforeAll
    public static void setup_data() throws Exception {
        createLocation("Alder Springs",true,"SPK");
        createLocation("Wet Meadows",true,"SPK");
        createLocation("Pine Flat-Outflow",true,"SPK");
        createTimeseries("SPK","Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar");
        createTimeseries("SPK","Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-radar");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-radar");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-radar");
    }

    @Test
    public void test_no_aliased_results_returned(){
        given().accept(Formats.JSONV2)
            .queryParam("office", "SPK")
            .queryParam("like",".*-radar$")
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
        .queryParam("like","alder spRINgs.*-RADAR$")
        .get("/catalog/TIMESERIES").then().assertThat()
        .statusCode(is(200))
        .body("$",hasKey("total"))
        .body("total",is(2))
        .body("entries.size()",is(2));
    }

    @Test
    public void test_all_office_pagination_works() {
        
        assertTimeout(Duration.ofMinutes(5), () -> {
            final int pageSize = 500;
            Response initialResponse = 
                given()
                    .accept(Formats.JSONV2)
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
            
            final int total = initialResponse.path("total");
            int totalRetrieved = initialResponse.path("entries.size()");

            String lastRowPreviousPage = initialResponse.path("entries.last().name");
            do {
                Response pageN = given().accept(Formats.JSONV2)
                .queryParam("page",nextPage)
                .get("/catalog/TIMESERIES")
                .then()
                    .assertThat()
                    .statusCode(is(200))
                    .body("$",hasKey("total"))
                    //.body("$",hasKey("next-page"))                    
                    .body("page-size",is(pageSize))
                    .body("page",equalTo(nextPage))
                    //.body("next-page",not(equalTo(nextPage)))
                    .body("entries[0].name",not(equalTo(lastRowPreviousPage)))
                    .extract().response();
                    ;

                nextPage = pageN.path("next-page");
                
                lastRowPreviousPage = pageN.path("entries.last().name");
                int pageTotal = pageN.path("entries.size()");
                totalRetrieved += pageTotal;
                /*if( nextPage == null && totalRetrieved < total) {
                    fail("Pagination not complete, system returned 'last page' before all values retrieved.");
                }*/
            } while( nextPage != null );
            assertEquals(total,totalRetrieved, "Initial count and retrieval do not match");
        }, "Catalog retrieval got stuck; possibly in endless loop");
        
        
    }
}
