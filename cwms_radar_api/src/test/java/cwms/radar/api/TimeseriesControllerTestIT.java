package cwms.radar.api;

import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.Formats;
import fixtures.TestAccounts;
import fixtures.TestAccounts.KeyUser;
import io.restassured.RestAssured;
import io.restassured.path.json.config.JsonPathConfig;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@Tag("integration")
public class TimeseriesControllerTestIT extends DataApiTestIT {

    @Test
    public void test_lrl_timeseries_psuedo_reg1hour() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        String tsData = IOUtils.toString(
            this.getClass()
                .getResourceAsStream("/cwms/radar/api/lrl/pseudo_reg_1hour.json"),"UTF-8"
            );

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location,true,officeId);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
            
            // get it back
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
                .queryParam("units","cfs")
                .queryParam("name",ts.get("name").asText())
                .queryParam("begin","2023-01-11T12:00:00-00:00")
                .queryParam("end","2023-01-11T13:00:00-00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[1][1]",closeTo(600.0,0.0001))
                .body("values[0][1]",closeTo(500.0,0.0001))
                
                ;
        } catch( SQLException ex) {
            throw new RuntimeException("Unable to create location for TS",ex);
        }
    }

    @Test
    public void test_lrl_1day() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/radar/api/lrl/1day_offset.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        
        try {
            createLocation(location,true,officeId);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
            
            // get it back
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().everything(true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
                .queryParam("units","F")
                .queryParam("name",ts.get("name").asText())
                .queryParam("begin","2023-02-02T06:00:00-05:00")
                .queryParam("end","2023-02-02T06:00:00-05:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().body().log().everything(true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]",closeTo(35,0.0001))
                
                
                ;
        } catch( SQLException ex) {
            throw new RuntimeException("Unable to create location for TS",ex);
        }
    }

    @Test
    public void test_delete_ts() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/radar/api/lrl/1day_offset.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location,true,officeId);

        KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().everything(true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().body().log().everything(true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().everything(true)
            .accept(Formats.JSONV2)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
            .queryParam("begin","2023-02-02T11:00:00+00:00")
            .queryParam("end","2023-02-02T11:00:00+00:00")
            .queryParam("start-time-inclusive","true")
            .queryParam("end-time-inclusive","true")
            .queryParam("override-protection","true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + ts.get("name").asText())
        .then()
            .log().body().log().everything(true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get it back
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().everything(true)
            .accept(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
            .queryParam("units","F")
            .queryParam("name",ts.get("name").asText())
            .queryParam("begin","2023-02-02T11:00:00+00:00")
            .queryParam("end","2023-02-03T11:00:00+00:00")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().body().log().everything(true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values[0][1]",nullValue());
    }
}
