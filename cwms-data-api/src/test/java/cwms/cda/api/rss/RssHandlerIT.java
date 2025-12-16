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

package cwms.cda.api.rss;

import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.xml.XmlPath;
import io.restassured.path.xml.config.XmlPathConfig;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.math.BigInteger;
import java.net.URI;
import java.sql.Timestamp;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jooq.Configuration;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_MSG_PACKAGE;

@Tag("integration")
final class RssHandlerIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SWT";
    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

    @BeforeEach
    void setup() throws Exception {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            Configuration configuration = DSL.using(c).configuration();
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(configuration, OFFICE_ID);
            String text = "<cwms_message type=\"Status\">\n" +
                "  <property name=\"operation\" type=\"String\">%s</property>\n" +
                "</cwms_message>";
            for (int i = 0; i < 12; i++) {
                CWMS_MSG_PACKAGE.call_LOG_MESSAGE(configuration, "CDA IT", "TEST_RUNNER",
                    "test.github.com", BigInteger.ZERO, new Timestamp(System.currentTimeMillis()),
                    format(text, i), BigInteger.ONE, true, true);
            }
        });
    }

    @Test
    void test_rss_feed_with_pagination() {
        // Page 1: verify core RSS elements + 5 items + next link exists
        ExtractableResponse<Response> page1 = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.RSS)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 5)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/rss/" + OFFICE_ID + "/status")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        String xmlBody = page1.asString();
        XmlPath xml = rssXml(xmlBody);

        assertNotNull(xml.getString("rss.@version"));
        assertNotNull(xml.getString("rss.channel.title"));
        assertNotNull(xml.getString("rss.channel.description"));
        List<?> items = xml.getList("rss.channel.item");
        assertNotNull(items);
        assertEquals(5, items.size(), "Expected 5 <item> elements on first page");
        assertNotNull(xml.getString("rss.channel.item[0].description"));
        assertNotNull(xml.getString("rss.channel.item[0].pubDate"));
        assertNotNull(xml.getString("rss.channel.item[0].guid"));

        String nextHref = nextLinkHref(xml);
        assertNotNull(nextHref, "Expected an atom:link rel=\"next\" on the first page");

        // Walk pages via nextLine
        int pagesVisited = 1;
        int maxPages = 500;
        while (nextHref != null && pagesVisited < maxPages) {
            String nextPath = toPathAndQuery(nextHref);

            ExtractableResponse<Response> nextPage = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.RSS)
                .header(AUTH_HEADER, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(nextPath)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .extract();

            XmlPath nextXml = rssXml(nextPage.asString());

            // Still a valid RSS document with items
            assertNotNull(nextXml.getString("rss.channel.title"));
            assertNotNull(nextXml.getList("rss.channel.item"));

            pagesVisited++;
            nextHref = nextLinkHref(nextXml);
        }

        assertTrue(pagesVisited > 1, "Expected to visit more than one page");
        assertTrue(pagesVisited < maxPages, "Hit maxPages guard before finding last page");
        assertNull(nextHref, "Expected last page to have no atom:link rel=\"next\"");
    }

    @Test
    void test_rss_feed_unknown_queue() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.RSS)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 5)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/rss/" + OFFICE_ID + "/answering-machine")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
            .extract();
    }

    private static XmlPath rssXml(String xmlBody) {
        return new XmlPath(xmlBody)
            .using(XmlPathConfig.xmlPathConfig()
                .declaredNamespace("atom", "http://www.w3.org/2005/Atom"));
    }

    private static String nextLinkHref(XmlPath xml) {
        return xml.getString("rss.channel.'atom:link'.find { it.@rel == 'next' }.@href");
    }

    private static String toPathAndQuery(String href) {
        URI uri = URI.create(href);
        if (uri.getScheme() == null) {
            return href;
        }
        String path = uri.getRawPath().replace("/cwms-data", "");
        String query = uri.getRawQuery();
        return query == null ? path : path + "?" + query;
    }
}
