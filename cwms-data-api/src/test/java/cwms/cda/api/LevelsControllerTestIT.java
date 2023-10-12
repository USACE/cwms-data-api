/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.sql.Connection;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class LevelsControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    @Test
    void test_location_level() throws Exception {
        createLocation("level_as_single_value", true, OFFICE);
        String levelId = "level_as_single_value.Stor.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        LocationLevel level = new LocationLevel.Builder(levelId, time)
                .withOfficeId(OFFICE)
                .withConstantValue(1.0)
                .withLevelUnitsId("ac-ft")
                .build();
            CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
                DSLContext dsl = dslContext((Connection) c, OFFICE);
                LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
                dao.storeLocationLevel(level, level.getLevelDate().getZone());
            });

        //Read level without unit
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("m3"))
            // I think we need to create a custom matcher.
            // This really shouldn't use equals but due to a quirk in
            // RestAssured it appears to be necessary.
            .body("constant-value",equalTo(1233.4818f)); // 1 ac-ft to m3

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
            .queryParam(UNIT, "ac-ft")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("ac-ft"))
            .body("constant-value",equalTo(1.0F));
    }



    @Test
    void test_level_as_timeseries() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        createLocation("level_as_timeseries", true, OFFICE);
        String levelId = "level_as_timeseries.Flow.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        int effectiveDateCount = 10;
        NavigableMap<Instant, LocationLevel> levels = new TreeMap<>();
        for (int i = 0; i < effectiveDateCount; i++) {
            LocationLevel level = new LocationLevel.Builder(levelId, time.plusDays(i))
                    .withOfficeId(OFFICE)
                    .withConstantValue((double) i)
                    .withLevelUnitsId("cfs")
                    .build();
            levels.put(level.getLevelDate().toInstant(), level);
            CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
                DSLContext dsl = dslContext((Connection) c, OFFICE);
                LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
                dao.storeLocationLevel(level, level.getLevelDate().getZone());
            });
        }

        //Read level timeseries
        TimeSeries timeSeries =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", OFFICE)
                .queryParam(BEGIN, time.toInstant().toString())
                .queryParam(END, time.plusDays(effectiveDateCount).toInstant().toString())
                .queryParam(INTERVAL, "1Hour")
                .queryParam(UNIT, "cfs")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/" + levelId + "/timeseries/")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
            .extract()
                .response()
                .as(TimeSeries.class);
        assertEquals("level_as_timeseries.Flow.Ave.1Hour.1Day.Regulating", timeSeries.getName());
        assertEquals(OFFICE, timeSeries.getOfficeId());
        assertEquals(time.toInstant(), timeSeries.getBegin().toInstant());
        assertEquals(time.plusDays(effectiveDateCount).toInstant(), timeSeries.getEnd().toInstant());
        assertEquals(24 * effectiveDateCount + 1, timeSeries.getTotal());
        List<TimeSeries.Record> values = timeSeries.getValues();
        for (int i = 0; i < values.size(); i++) {
            TimeSeries.Record record = values.get(i);
            assertEquals(time.plusHours(i).toInstant(), record.getDateTime().toInstant(), "Time check failed at iteration: " + i);
            assertEquals(0, record.getQualityCode(), "Quality check failed at iteration: " + i);
            Double constantValue = levels.floorEntry(record.getDateTime().toInstant())
                    .getValue()
                    .getConstantValue();
            assertEquals(constantValue, record.getValue(), 0.0001, "Value check failed at iteration: " + i);
        }
    }
}
