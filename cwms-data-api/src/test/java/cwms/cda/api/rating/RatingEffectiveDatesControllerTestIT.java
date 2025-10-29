package cwms.cda.api.rating;

import cwms.cda.api.Controllers;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STORE_TEMPLATE;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class RatingEffectiveDatesControllerTestIT extends DataApiTestIT {
    private static final String EXISTING_LOC = "RatingsDatesTestIT";
    private static final String EXISTING_SPEC = EXISTING_LOC + ".Stage;Flow.USGS-BASE.Production";
    private static final String TEMPLATE = "Stage;Flow.USGS-BASE";
    private static final String SPK = "SPK";

    @BeforeAll
    static void beforeAll() throws Exception {
        store(false);
    }

    @AfterAll
    static void cleanUp()
    {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Delete Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/ratings/template/" + TEMPLATE)
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    static void store(boolean storeTemplate) throws Exception
    {
        //Make sure we always have something.
        createLocation(EXISTING_LOC, true, SPK);

        String ratingXml = readResourceFile("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production_2.xml");
        ratingXml = ratingXml.replaceAll("STJ-St_Joseph-Missouri", EXISTING_LOC);
        String ratingXml2 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2016-06-06T00:00:00Z");
        String ratingXml3 = ratingXml.replaceAll("2002-04-09T13:53:01Z", "2085-06-06T00:00:00Z");
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml);
        RatingSetContainer container2 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
        RatingSetContainer container3 = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml3);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
        String setXml2 = RatingContainerXmlFactory.toXml(container2, "", 0, true, false);
        String setXml3 = RatingContainerXmlFactory.toXml(container3, "", 0, true, false);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(templateXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/template")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create Spec
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(specXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/spec")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, storeTemplate)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the second set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, storeTemplate)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Create the third set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml3)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, storeTemplate)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @Test
    void getEffectiveDatesForSpecId() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE_MASK, SPK)
            .queryParam(Controllers.RATING_ID_MASK, EXISTING_SPEC)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/effective-dates")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-to-spec-dates." + SPK + ".size()", is(1))
            .body("office-to-spec-dates." + SPK + "[0].rating-spec-id", is(EXISTING_SPEC))
            .body("office-to-spec-dates." + SPK + "[0].effective-dates.size()", is(2));
    }

    @Test
    void getEffectiveDatesGetAll() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE_MASK, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/effective-dates")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-to-spec-dates." + SPK, not(empty()));

    }
}
