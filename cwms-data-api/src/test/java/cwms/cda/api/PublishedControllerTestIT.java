/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.data.dao.DaoTest.getDslContext;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;

import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import hec.heclib.dss.DSSPathname;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID.AV_CWMS_TS_ID;

@Tag("integration")
public final class PublishedControllerTestIT extends DataApiTestIT {

    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private static final String OFFICE_ID = "SPK";
    private static final String OFFICE_ID_2 = "SWT";
    private static final String OFFICE_ID_3 = "NWD";
    private static final ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
    private static final ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");
    private static final Map<String, String> LOCATION_TO_OFFICE = new LinkedHashMap<>();
    private static final Map<String, Map<String, String>> LOCATION_TO_TS_ID = new LinkedHashMap<>();
    private static final String STAGE = "STAGE";
    private static final String INFLOW = "INFLOW";
    private static final String PRECIP = "PRECIP";
    private static final Map<String, Integer> TS_CODE_MAP = new LinkedHashMap<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        Map<String, String> aarkParamToTsIdMap = new LinkedHashMap<>();
        aarkParamToTsIdMap.put(STAGE, "AARK.Stage.Inst.15Minutes.0.Ccp-Rev");
        aarkParamToTsIdMap.put(INFLOW,  "AARK.Flow.Inst.1Hour.0.Ccp-Rev");
        aarkParamToTsIdMap.put(PRECIP, "AARK.Precip.Inst.15Minutes.0.Ccp-Rev");

        Map<String, String> addiParamToTsIdMap = new LinkedHashMap<>();
        addiParamToTsIdMap.put(STAGE, "ADDI.Stage.Inst.15Minutes.0.Ccp-Rev");
        addiParamToTsIdMap.put(INFLOW,  "ADDI.Flow.Inst.15Minutes.0.Ccp-Rev");
        addiParamToTsIdMap.put(PRECIP, "ADDI.Precip.Inst.15Minutes.0.Ccp-Rev");

        Map<String, String> bbnkParamToTsIdMap = new LinkedHashMap<>();
        bbnkParamToTsIdMap.put(STAGE, "BBNK.Stage.Inst.15Minutes.0.Ccp-Rev");
        bbnkParamToTsIdMap.put(INFLOW,  "BBNK.Flow.Inst.15Minutes.0.Ccp-Rev");
        bbnkParamToTsIdMap.put(PRECIP, "BBNK.Precip.Inst.15Minutes.0.Ccp-Rev");

        LOCATION_TO_TS_ID.put("AARK", aarkParamToTsIdMap);
        LOCATION_TO_OFFICE.put("AARK", OFFICE_ID);
        LOCATION_TO_TS_ID.put("ADDI", addiParamToTsIdMap);
        LOCATION_TO_OFFICE.put("ADDI", OFFICE_ID_2);
        LOCATION_TO_TS_ID.put("BBNK", bbnkParamToTsIdMap);
        LOCATION_TO_OFFICE.put("BBNK", OFFICE_ID_3);

        List<TimeSeries> tsToCreate = new ArrayList<>();
        for (String locationId : LOCATION_TO_TS_ID.keySet()) {
            String officeId = LOCATION_TO_OFFICE.get(locationId);
            try {
                createLocation(locationId, true, officeId, "SITE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            Map<String, String> tsIds = LOCATION_TO_TS_ID.get(locationId);
            for (String tsId : tsIds.values()) {
                DSSPathname path = new DSSPathname(tsId);
                int minutes = "1Hour".equals(path.getDPart()) ? 60 : 15;
                int count = 60 / minutes;

                String unit = tsId.contains("Flow") ? "cms" : "m";

                TimeSeries ts = new TimeSeries(
                        null, -1, 0, tsId, officeId, start, end, unit, Duration.ofMinutes(minutes));

                ZonedDateTime next = start;
                for (int i = 0; i < count; i++) {
                    Timestamp dateTime = Timestamp.valueOf(next.toLocalDateTime());
                    ts.addValue(dateTime, (double) i, 0);
                    next = next.plusMinutes(minutes);
                }
                tsToCreate.add(ts);
            }
        }


        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON CWMS_20.AT_A2W_TS_CODES_BY_LOC TO " + CwmsDataApiSetupCallback.getWebUser());
                stmt.execute("GRANT SELECT ON CWMS_20.AV_LOC TO " + CwmsDataApiSetupCallback.getWebUser());
                stmt.execute("begin "
                        + "cwms_sec.add_user_to_group('" + CwmsDataApiSetupCallback.getWebUser() + "','CWMS Users', 'NWD'); "
                        + "cwms_sec.add_user_to_group('" + CwmsDataApiSetupCallback.getWebUser() + "','TS ID Creator', 'NWD'); "
                        + "end;");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "CWMS_20"); // <-- important
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context, new MetricRegistry());
            for (TimeSeries ts : tsToCreate) {
                timeSeriesDao.create(ts);
            }

            // Retrieve ts_code for each TS ID
            for (String locationId : LOCATION_TO_TS_ID.keySet()) {
                Map<String, String> tsIds = LOCATION_TO_TS_ID.get(locationId);
                for (String tsId : tsIds.values()) {
                    Integer tsCode = context.select(AV_CWMS_TS_ID.TS_CODE)
                            .from(AV_CWMS_TS_ID)
                            .where(AV_CWMS_TS_ID.CWMS_TS_ID.eq(tsId))
                            .fetchOneInto(Integer.class);

                    if (tsCode != null) {
                        TS_CODE_MAP.put(tsId, tsCode);
                    } else {
                        throw new RuntimeException("TS Code not found for " + tsId);
                    }
                }
            }

            // SQL-based replacement for p_load_a2w_by_location
            for (String locationId : LOCATION_TO_TS_ID.keySet()) {
                String officeId = LOCATION_TO_OFFICE.get(locationId);
                Map<String, String> tsIds = LOCATION_TO_TS_ID.get(locationId);

                Integer stageCode = tsIds.get(STAGE) == null ? null : TS_CODE_MAP.get(tsIds.get(STAGE));
                Integer flowCode  = tsIds.get(INFLOW)  == null ? null : TS_CODE_MAP.get(tsIds.get(INFLOW));
                Integer precipCode = tsIds.get(PRECIP) == null ? null : TS_CODE_MAP.get(tsIds.get(PRECIP));
                int numTsCodes = tsIds.values().size();

                context.connection(conn -> {
                    try {
                        Long locationCode;
                        try (java.sql.PreparedStatement ps = conn.prepareStatement(
                                "select location_code from cwms_20.av_loc " +
                                        "where unit_system = 'EN' and location_id = ? and db_office_id = ?")) {
                            ps.setString(1, locationId);
                            ps.setString(2, officeId);

                            try (java.sql.ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) {
                                    locationCode = rs.getLong(1);
                                } else {
                                    throw new SQLException("Location code not found for " + locationId);
                                }
                            }
                        }

                        String mergeSql =
                                "merge into cwms_20.at_a2w_ts_codes_by_loc t " +
                                        "using (select ? db_office_id, ? location_code from dual) s " +
                                        "on (t.db_office_id = s.db_office_id and t.location_code = s.location_code) " +
                                        "when matched then update set " +
                                        "  t.date_refreshed = sysdate, " +
                                        "  t.display_flag = ?, " +
                                        "  t.notes = ?, " +
                                        "  t.num_ts_codes = ?, " +
                                        "  t.ts_code_stage = ?, " +
                                        "  t.ts_code_inflow = ?, " +
                                        "  t.ts_code_precip = ? " +
                                        "when not matched then insert (" +
                                        "  db_office_id, location_code, date_refreshed, display_flag, notes, num_ts_codes, ts_code_stage, ts_code_inflow, ts_code_precip" +
                                        ") values (" +
                                        "  s.db_office_id, s.location_code, sysdate, ?, ?, ?, ?, ?, ?" +
                                        ")";

                        try (java.sql.PreparedStatement ps = conn.prepareStatement(mergeSql)) {
                            ps.setString(1, officeId);
                            ps.setLong(2, locationCode);

                            ps.setString(3, "T");
                            ps.setString(4, "Generated data");
                            ps.setInt(5, numTsCodes);
                            ps.setObject(6, stageCode);
                            ps.setObject(7, flowCode);
                            ps.setObject(8, precipCode);

                            ps.setString(9, "T");
                            ps.setString(10, "Generated data");
                            ps.setInt(11, numTsCodes);
                            ps.setObject(12, stageCode);
                            ps.setObject(13, flowCode);
                            ps.setObject(14, precipCode);

                            ps.executeUpdate();
                        }

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());

            for (Integer tsCode : TS_CODE_MAP.values()) {
                context.connection(conn -> {
                    String[] columns = {
                            "ts_code_elev", "ts_code_stage", "ts_code_precip", "ts_code_inflow",
                            "ts_code_outflow", "ts_code_sur_release", "ts_code_stor_drought",
                            "ts_code_stor_flood", "ts_code_elev_tw", "ts_code_stage_tw",
                            "ts_code_rule_curve_elev", "ts_code_power_gen", "ts_code_temp_air",
                            "ts_code_temp_water", "ts_code_do", "ts_code_ph", "ts_code_cond",
                            "ts_code_wind_dir", "ts_code_wind_speed", "ts_code_volt",
                            "ts_code_pct_flood", "ts_code_pct_con", "ts_code_irrad", "ts_code_evap"
                    };

                    for (String column : columns) {
                        try (java.sql.PreparedStatement ps = conn.prepareStatement(
                                "update cwms_20.at_a2w_ts_codes_by_loc set " + column + " = null where " + column + " = ?")) {
                            ps.setObject(1, tsCode);
                            ps.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        }, CwmsDataApiSetupCallback.getWebUser());

        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesDeleteOptions options = new TimeSeriesDaoImpl.DeleteOptions.Builder()
                    .withStartTime(Date.from(start.toInstant()))
                    .withEndTime(Date.from(end.toInstant()))
                    .withEndTimeInclusive(true)
                    .withStartTimeInclusive(true)
                    .withMaxVersion(true)
                    .withVersionDate(Date.from(start.toInstant()))
                    .withOverrideProtection("T")
                    .build();

            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context, new MetricRegistry());
            LOCATION_TO_TS_ID.forEach((locationId, tsIds) -> {
                String officeId = LOCATION_TO_OFFICE.get(locationId);
                tsIds.values().forEach(tsId ->
                        timeSeriesDao.delete(officeId, tsId, options));
            });
        }, CwmsDataApiSetupCallback.getWebUser());

        databaseLink.connection(c -> {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("REVOKE SELECT, INSERT, UPDATE, DELETE ON CWMS_20.AT_A2W_TS_CODES_BY_LOC FROM " + CwmsDataApiSetupCallback.getWebUser());
                stmt.execute("REVOKE SELECT ON CWMS_20.AV_LOC FROM " + CwmsDataApiSetupCallback.getWebUser());
                stmt.execute("begin "
                        + "cwms_sec.remove_user_from_group('" + CwmsDataApiSetupCallback.getWebUser() + "','CWMS Users', 'NWD'); "
                        + "cwms_sec.remove_user_from_group('" + CwmsDataApiSetupCallback.getWebUser() + "','TS ID Creator', 'NWD'); "
                        + "end;");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, "CWMS_20");
    }

    @Test
    void testGetAllNotPaginated()
    {
        // Create test parameters (mock the filters for officeId, locationId, and tsType)
        String officeMask = "SPK";  // Example office ID
        String locationMask = "AARK|ADDI";  // Example location IDs

        // Send a request to the endpoint to retrieve all time series profiles without pagination
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam("office-mask", officeMask)
                .queryParam("location-mask", locationMask)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/published/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("'location-to-published-data'.size()", greaterThan(0))
                .body("'location-to-published-data'.'location-id'.name", hasItems("AARK"))
                .body("'location-to-published-data'[0].'published-times-series'.STAGE", notNullValue())
                .body("'location-to-published-data'[0].'published-times-series'.INFLOW", notNullValue());

    }

    @Test
    void testGetAllPaginated()
    {
        String locationMask = "AARK|ADDI";

        // --- Non-paginated request ---
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam("office-mask", OFFICE_ID)
            .queryParam("location-mask", locationMask)
        .when()
            .redirects().follow(true)
            .get("/published/")
        .then()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("'location-to-published-data'.size()", greaterThan(0))
            .body("'location-to-published-data'.'location-id'.name", hasItems("AARK"))
            .body("'location-to-published-data'[0].'published-times-series'.STAGE", notNullValue());

        // --- Page 1 ---
        ExtractableResponse<Response> response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 1)
        .when()
            .get("/published/")
        .then()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("'location-to-published-data'.size()", is(1))
            .body("page", notNullValue())
            .extract();

        String nextPageCursor = response.path("page");
        assert nextPageCursor != null;

        // --- Page 2 ---
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 1)
            .queryParam(PAGE, nextPageCursor)
        .when()
            .get("/published/")
        .then()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("'location-to-published-data'.size()", is(1));
    }

    @Test
    void test_published_getall_with_spk_swt_nwd()
    {
        // 3 dams from different districts (SPK, SWT, NWD)
        // at least 3 different elements of data for each (STAGE, FLOW, PRECIP)
        String locationMask = "AARK|ADDI|BBNK";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam("location-mask", locationMask)
        .when()
            .redirects().follow(true)
            .get("/published/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("'location-to-published-data'.size()", is(3))
            .body("'location-to-published-data'.'location-id'.name", hasItems("AARK", "ADDI", "BBNK"))
            .body("'location-to-published-data'.find { it.'location-id'.name == 'AARK' }.'published-times-series'.STAGE", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'AARK' }.'published-times-series'.INFLOW", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'AARK' }.'published-times-series'.PRECIP", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'ADDI' }.'published-times-series'.STAGE", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'ADDI' }.'published-times-series'.INFLOW", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'ADDI' }.'published-times-series'.PRECIP", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'BBNK' }.'published-times-series'.STAGE", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'BBNK' }.'published-times-series'.INFLOW", notNullValue())
            .body("'location-to-published-data'.find { it.'location-id'.name == 'BBNK' }.'published-times-series'.PRECIP", notNullValue());
    }
}
