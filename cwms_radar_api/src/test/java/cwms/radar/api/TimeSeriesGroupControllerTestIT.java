package cwms.radar.api;

import java.util.List;

import fixtures.RadarApiSetupCallback;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
class TimeSeriesGroupControllerTestIT
{

    public static final String CWMS_OFFICE = "CWMS";

    @Test
    void test_group_SPK(){

        Response response = given()
                .accept("application/json")
                .queryParam("office", "SPK")
                .get("/timeseries/group");

        response.then().assertThat()
        .statusCode(is(200))
        .body(
                "$.size()", is(1),
                "[0].time-series-category.office-id", is("SPK"),
                "[0].office-id", is("SPK")
        );

        JsonPath jsonPathEval = response.jsonPath();
        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.contains(testGroupId));
    }

    @Test
    void test_group_CWMS(){

        Response response = given()
                .accept("application/json")
                .queryParam("office", CWMS_OFFICE)
                .get("/timeseries/group");

        JsonPath jsonPathEval = response.jsonPath();
        response.then().assertThat()
                .statusCode(is(200))
                .body("$.size()",greaterThan(0))
        ;

        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group2";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.hasItem(testGroupId));

        int itemIndex = ids.indexOf(testGroupId);

        assertThat(jsonPathEval.get("[" + itemIndex + "].time-series-category.office-id"), Matchers.is("CWMS"));

        List<String> tsIds = jsonPathEval.get("[" + itemIndex + "].assigned-time-series.timeseries-id");
        assertNotNull(tsIds);
        assertFalse(tsIds.isEmpty());

        String[] lookFor = {"Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-radar",
                "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar"};

        for(final String tsId : lookFor)
        {
            assertThat("Response did not contain expected item", tsIds, Matchers.hasItem(tsId));
        }
    }
}
