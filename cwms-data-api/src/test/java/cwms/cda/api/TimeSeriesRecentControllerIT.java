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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesCategoryDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import javax.servlet.http.HttpServletResponse;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
class TimeSeriesRecentControllerIT extends DataApiTestIT {
    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private static final Logger LOGGER = Logger.getLogger(TimeSeriesRecentControllerIT.class.getName());
    private static final String OFFICE_ID = "SPK";
    private static final String LOCATION = "ZACK1";
    private static final String TS_ID = LOCATION + ".Depth.Inst.15Minutes.0.TS_TEST";
    private static final String CATEGORY_ID = "TEST";
    private static final String GROUP_ID = "TEST_GROUP";
    private static final ZonedDateTime START = ZonedDateTime.parse("2024-09-21T08:00:00-07:00[PST8PDT]");
    private static final ZonedDateTime END = ZonedDateTime.parse("2024-09-21T09:00:00-07:00[PST8PDT]");
    private static final ZonedDateTime VERSION_DATE = ZonedDateTime.parse("2024-09-21T09:00:00-07:00[PST8PDT]");
    private static TimeSeriesGroup group;

    @BeforeAll
    static void setup() throws Exception {
        createLocation(LOCATION, true, OFFICE_ID);
    }

    @AfterAll
    static void cleanup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            TimeSeriesDaoImpl tsDao = new TimeSeriesDaoImpl(ctx);
            TimeSeriesCategoryDao tsCategoryDao = new TimeSeriesCategoryDao(ctx);
            TimeSeriesGroupDao tsGroupDao = new TimeSeriesGroupDao(ctx);
            try {
                tsDao.delete(OFFICE_ID, TS_ID, new TimeSeriesDaoImpl.DeleteOptions.Builder()
                        .withVersionDate(Date.from(VERSION_DATE.toInstant())).withMaxVersion(false)
                        .withOverrideProtection("F").withEndTimeInclusive(true).withStartTimeInclusive(true).build());
            } catch (NotFoundException e) {
                LOGGER.log(Level.CONFIG, "TimeSeries not found");
            }
            try {
                tsGroupDao.unassignAllTs(group);
                tsGroupDao.delete(CATEGORY_ID, GROUP_ID, OFFICE_ID);
            } catch (NotFoundException e) {
                LOGGER.log(Level.CONFIG, "Group not found");
            }
            try {
                tsCategoryDao.delete(CATEGORY_ID, true, OFFICE_ID);
            } catch (NotFoundException e) {
                LOGGER.log(Level.CONFIG, "Category not found");
            }
        });
    }

    @Test
    void test_retrieving_recent_ts_data() throws Exception {
        TimeSeries ts = buildTimeSeries(OFFICE_ID, TS_ID);
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        String json = Formats.format(contentType, ts);

        // create timeseries
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV2)
            .accept(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(json)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        TimeSeriesCategory category = new TimeSeriesCategory(OFFICE_ID, CATEGORY_ID, "Test Category");
        json = JsonV1.buildObjectMapper().writeValueAsString(category);

        // add timeseries to category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .body(json)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        group = new TimeSeriesGroup(category, OFFICE_ID, GROUP_ID, "Test Group", null, TS_ID);
        List<AssignedTimeSeries> tsList = Collections
                .singletonList(new AssignedTimeSeries(OFFICE_ID, TS_ID, null, null, TS_ID, 0));
        group = new TimeSeriesGroup(group, tsList);
        json = JsonV1.buildObjectMapper().writeValueAsString(group);

        // add timeseries to group
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .body(json)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get recent data using category and group
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.CATEGORY_ID, CATEGORY_ID)
            .queryParam(Controllers.GROUP_ID, GROUP_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/recent/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("size()", is(1))
        ;

        // get recent data using timeseries id
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(TS_IDS, TS_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/recent/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("size()", is(1))
        ;
    }

    @NotNull
    private TimeSeries buildTimeSeries(String officeId, String tsId) {
        long diff = END.toEpochSecond() - START.toEpochSecond();
        assertEquals(3600, diff); // just to make sure I've got the date parsing thing right.

        int minutes = 15;
        int count = 60/15 ; // do I need a +1?  ie should this be 12 or 13?
        // Also, should end be the last point or the next interval?

        TimeSeries ts = new TimeSeries(null,
                -1,
                0,
                tsId,
                officeId,
                START,
                END,
                "m",
                Duration.ofMinutes(minutes),
                null,
                VERSION_DATE,
                VersionType.SINGLE_VERSION);

        ZonedDateTime next = START;
        for(int i = 0; i < count; i++) {
            Timestamp dateTime = Timestamp.from(next.toInstant());
            ts.addValue(dateTime, (double) i, 0);
            next = next.plusMinutes(minutes);
        }
        return ts;
    }
}
