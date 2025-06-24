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

package cwms.cda.api;

import static cwms.cda.formatters.Formats.JSON;
import static cwms.cda.formatters.Formats.JSONV2;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class RateLimitingTestIT extends DataApiTestIT {

    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

    @Test
    void testRateLimitingAcrossEndpoints() {
        List<RateLimitTest> rateLimitTests = new ArrayList<>();
        AtomicBoolean rateLimitReachedAtomic = new AtomicBoolean(false);

        for (int i = 0; i <= Runtime.getRuntime().availableProcessors(); i++) {
            RateLimitTest
                rateLimitTest = new RateLimitTest(rateLimitReachedAtomic);
            rateLimitTest.start();
            rateLimitTests.add(rateLimitTest);
        }
        for (RateLimitTest rateLimitTest : rateLimitTests) {
            try {
                rateLimitTest.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Rate limit test interrupted", e);
            }
        }
        assertTrue(rateLimitReachedAtomic.get());
    }

    private static final class RateLimitTest extends Thread {
        private final AtomicBoolean rateLimitReached;

        RateLimitTest(AtomicBoolean rateLimitReached) {
            this.rateLimitReached = rateLimitReached;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i <= 25; i++) {
                    Response response = given()
                        .accept(JSONV2)
                        .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                    .when()
                        .get("/levels/")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
//                        .statusCode(isOneOf(HttpServletResponse.SC_OK, 429))
                        .extract().response();
                    if (response.statusCode() == 429) {
                        rateLimitReached.set(true);
                    }

                    response = given()
                        .accept(JSONV2)
                    .when()
                        .get("/offices/")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                        .statusCode(isOneOf(HttpServletResponse.SC_OK, 429))
                        .extract().response();
                    if (response.statusCode() == 429) {
                        rateLimitReached.set(true);
                    }

                    response = given()
                        .accept(JSON)
                        .queryParam(Controllers.OFFICE, user.getOperatingOffice())
                    .when()
                        .get("/projects/")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                        .statusCode(isOneOf(HttpServletResponse.SC_OK, 429))
                        .extract().response();
                    if (response.statusCode() == 429) {
                        rateLimitReached.set(true);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Error during rate limit test", e);
            }
        }
    }
}



