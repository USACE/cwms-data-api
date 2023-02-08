package cwms.radar.api;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.closeTo;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.radar.formatters.Formats;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import fixtures.TestAccounts.KeyUser;
import io.restassured.RestAssured;
import io.restassured.path.json.config.JsonPathConfig;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class TimeseriesControllerTestIT {
    
    public ArrayList<String> locationsCreated = new ArrayList<>();

    public static String createLocationQuery = null;
    public static String deleteLocationQuery = null;

    @BeforeAll
    public static void queries() throws Exception {
        createLocationQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/radar/data/sql_templates/create_location.sql"),"UTF-8"
                            );
        deleteLocationQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/radar/data/sql_templates/delete_location.sql"),"UTF-8"
                            );
    }

    @AfterEach
    public void delete_timeseries() throws SQLException {
        locationsCreated.forEach(location -> {
            try {
                CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
                db.connection((c)-> {            
                    try(PreparedStatement stmt = c.prepareStatement(deleteLocationQuery);) {
                        stmt.setString(1,location);                
                        stmt.setString(2,"SPK");
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to delete location",ex);
                    }
                });    
            } catch(SQLException ex) {
                throw new RuntimeException("Unable to delete location",ex);
            }
        
        });
    }

    private void createLocation(String location, boolean active, String office) throws SQLException {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        System.out.println(db.getPdUser());
        System.out.println(db.getPassword());
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createLocationQuery);) {
                stmt.setString(1,location);
                stmt.setString(2,active ? "T" : "F");
                stmt.setString(3,office);
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create location",ex);
            }
        },db.getPdUser());
        
    }
    

    @Test
    public void test_lrl_timeseries_psuedo_reg1hour() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/radar/api/lrl/pseudo_reg_1hour.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        locationsCreated.add(location);

        String officeID = "SPK";
        try {
            createLocation(location,true,officeID);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeID)
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
                .queryParam("office",officeID)
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
        locationsCreated.add(location);

        String officeID = "SPK";
        try {
            createLocation(location,true,officeID);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().everything(true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeID)
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
                .queryParam("office",officeID)
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
}
