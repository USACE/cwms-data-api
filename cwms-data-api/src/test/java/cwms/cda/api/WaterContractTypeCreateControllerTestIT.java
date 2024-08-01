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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.cda.api.watersupply.WaterContractCreateController;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;


@Tag("integration")
class WaterContractTypeCreateControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "HQ";
    private static final WaterUserContract CONTRACT;
    private static final LookupType CONTRACT_TYPE;
    static {
        try (InputStream contractStream = WaterContractCreateController.class
                .getResourceAsStream("/cwms/cda/api/waterusercontract.json")){
            assert contractStream != null;
            String contractJson = IOUtils.toString(contractStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
            CONTRACT_TYPE = new LookupType.Builder().withActive(true).withOfficeId(OFFICE_ID)
                    .withDisplayValue("TEST Contract Type").withTooltip("TEST LOOKUP").build();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void test_create_get_WaterContractType() throws Exception {
        // Test Structure
        // 1) Create a WaterContractType
        // 2) Get the WaterContractType, assert it exists
        // 3) Cleanup

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
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName() + "/types")
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
                .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                        + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                        + CONTRACT.getContractId().getName() + "/types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", equalTo(OFFICE_ID))
            .body("[0].display-value", equalTo(CONTRACT_TYPE.getDisplayValue()))
            .body("[0].tooltip", equalTo(CONTRACT_TYPE.getTooltip()))
            .body("[0].active", equalTo(CONTRACT_TYPE.getActive()))
        ;

        // cleanup
        cleanupType();
    }

    private void cleanupType() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            lookupTypeDao.deleteLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE",
                    CONTRACT_TYPE.getOfficeId(), CONTRACT_TYPE.getDisplayValue());
        }, CwmsDataApiSetupCallback.getWebUser());
    }
}
