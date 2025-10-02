/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.api.openapi;

import static io.restassured.RestAssured.given;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.JsonToYamlConverter;
import io.restassured.filter.log.LogDetail;
import java.io.File;
import java.io.FileWriter;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
@Tag("openapi")
final class OpenApiYamlConversionIT extends DataApiTestIT {

    @Test
    void saveSpecAndConvertToYaml() throws Exception {
        String json = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/swagger-docs")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .contentType(Formats.JSON)
            .extract().asString();

        JsonToYamlConverter converter = new JsonToYamlConverter();

        String jsonFilePath = "build/openapi.json";

        try (FileWriter jsonFw = new FileWriter(jsonFilePath)) {
            jsonFw.write(json);
        }

        String yaml = converter.asYaml(json);

        String filePath = "build/openapi.yaml";

        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write(yaml);
        }

        File file = new File(filePath);
        assertTrue(file.exists());

        File jsonFile = new File(jsonFilePath);
        assertTrue(jsonFile.exists());
    }
}
