/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletResponse;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.logging.Level;
import java.util.logging.Logger;


@Tag("integration")
class WaterContractTypeControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SWT";
    private static final LookupType CONTRACT_TYPE;
    public static final Logger logger =
            Logger.getLogger(WaterContractTypeControllerTestIT.class.getName());
    static {
        CONTRACT_TYPE = new LookupType.Builder().withActive(true).withOfficeId(OFFICE_ID)
                .withDisplayValue("TEST Contract Type").withTooltip("TEST LOOKUP").build();
    }

    @AfterEach
    void cleanup() throws SQLException {
        cleanupType();
    }

    @Test
    void test_create_get_WaterContractType() throws Exception {
        // Test Structure
        // 1) Create a WaterContractType
        // 2) Get the WaterContractType, assert it exists

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT_TYPE);

        // create water contract type
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .queryParam("fail-if-exists", false)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/contract-types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get water contract type and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/projects/" + OFFICE_ID + "/contract-types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", equalTo(OFFICE_ID))
            .body("[0].display-value", equalTo(CONTRACT_TYPE.getDisplayValue()))
            .body("[0].tooltip", equalTo(CONTRACT_TYPE.getTooltip()))
            .body("[0].active", equalTo(CONTRACT_TYPE.getActive()))
        ;
    }

    @Test
    void test_store_delete_WaterContractType() throws Exception {
        // Test Structure
        // 1) Create a WaterContractType
        // 2) Get the WaterContractType, assert it exists
        // 3) Delete the WaterContractType
        // 4) Get the WaterContractType, assert it does not exist

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT_TYPE);

        // create water contract type
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .queryParam("fail-if-exists", false)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/contract-types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get water contract type and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/contract-types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", equalTo(OFFICE_ID))
            .body("[0].display-value", equalTo(CONTRACT_TYPE.getDisplayValue()))
            .body("[0].tooltip", equalTo(CONTRACT_TYPE.getTooltip()))
            .body("[0].active", equalTo(CONTRACT_TYPE.getActive()))
        ;

        // delete water contract type
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/contract-types/" + CONTRACT_TYPE.getDisplayValue())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // get water contract type and assert that it does not exist
        List<LookupType> results = Arrays.asList(given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/contract-types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract().body().as(LookupType[].class))
        ;
        BiPredicate<LookupType, LookupType> match = (i, s) -> i.getDisplayValue().equals(s.getDisplayValue());
        DTOMatch.assertDoesNotContainDto(results, CONTRACT_TYPE,
               match, "Contract Type not deleted");
    }

    private void cleanupType() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            try {

                lookupTypeDao.deleteLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE",
                        CONTRACT_TYPE.getOfficeId(), CONTRACT_TYPE.getDisplayValue());
            } catch (NotFoundException e) {
                logger.log(Level.INFO, format("Cleanup failed to delete lookup type: %s", e.getMessage()));
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }
}
