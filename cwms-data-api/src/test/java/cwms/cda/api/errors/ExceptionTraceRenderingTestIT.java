package cwms.cda.api.errors;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.Formats;
import fixtures.StackTraceFeatureExtension;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
class ExceptionTraceRenderingTestIT extends DataApiTestIT {
    private static final String JSON_FILE = "/cwms/cda/api/lrl/1hour.json";

    @Test
    @ExtendWith(StackTraceFeatureExtension.class)
    void returnsStackTraceLinesForTraceRoleWhenRequestFails() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL2;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .cookie("JSESSIONIDSSO", user.getJSessionId())
            .queryParam(Controllers.OFFICE, officeId)
            .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
            .queryParam(Controllers.BEGIN, "not-a-date")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/filtered/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("incidentIdentifier", notNullValue())
            .body("details.stackTraceLines.size()", greaterThan(0))
            .body("details.stackTraceLines[0]", containsString("Exception"));
    }
}
