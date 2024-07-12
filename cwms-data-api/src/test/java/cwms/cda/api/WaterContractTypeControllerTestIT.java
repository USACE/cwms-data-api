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

import cwms.cda.api.watersupply.WaterContractController;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class WaterContractTypeControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final WaterUserContract CONTRACT;
    static {
        try (InputStream contractStream = WaterContractController.class.getResourceAsStream("/cwms/cda/api/waterusercontract.json")){
            assert contractStream != null;
            String contractJson = IOUtils.toString(contractStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void test_create_get_WaterContractType() {
        // Test Structure
        // 1) Create a WaterContractType
        // 2) Get the WaterContractType, assert it exists

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // create water contract type
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                        + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                        + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        // get water contract type and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .pathParam(OFFICE, OFFICE_ID)
                .pathParam(PROJECT_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
                .pathParam(WATER_USER, CONTRACT.getWaterUser().getEntityName())
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getParentLocationRef().getName()
                        + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                        + CONTRACT.getContractId().getName() + "/types")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(OFFICE_ID))
            .body("display-value", equalTo(CONTRACT.getWaterContract().getDisplayValue()))
            .body("tooltip", equalTo(CONTRACT.getWaterContract().getTooltip()))
            .body("active", equalTo(CONTRACT.getWaterContract().getActive()))
        ;

    }
}
