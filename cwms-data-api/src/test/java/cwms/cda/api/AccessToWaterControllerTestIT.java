package cwms.cda.api;

import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import hec.heclib.dss.DSSPathname;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID.AV_CWMS_TS_ID;

@Tag("integration")
@Disabled
public final class AccessToWaterControllerTestIT extends DataApiTestIT {

    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private static final String OFFICE_ID = "SPK";
    private static final ZonedDateTime start = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
    private static final ZonedDateTime end = ZonedDateTime.parse("2021-06-21T09:00:00-07:00[PST8PDT]");
    private static final Map<String, Map<String, String>> LOCATION_TO_TS_ID = new LinkedHashMap<>();
    private static final String STAGE = "STAGE";
    private static final String FLOW = "FLOW";
    private static final Map<String, Integer> TS_CODE_MAP = new LinkedHashMap<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        Map<String, String> aarkParamToTsIdMap = new LinkedHashMap<>();
        aarkParamToTsIdMap.put(STAGE, "AARK.Stage.Inst.15Minutes.0.Ccp-Rev");
        aarkParamToTsIdMap.put(FLOW, "AARK.Flow.Inst.1Hour.0.Ccp-Rev");
        Map<String, String> addiParamToTsIdMap = new LinkedHashMap<>();
        addiParamToTsIdMap.put(STAGE, "ADDI.Stage.Inst.15Minutes.0.Ccp-Rev");
        LOCATION_TO_TS_ID.put("AARK", aarkParamToTsIdMap);
        LOCATION_TO_TS_ID.put("ADDI", addiParamToTsIdMap);
        List<TimeSeries> tsToCreate = new ArrayList<>();
        for(String locationId : LOCATION_TO_TS_ID.keySet())
        {
            try {
                createLocation(locationId, true, OFFICE_ID, "SITE");
                createLocation(locationId, true, OFFICE_ID, "SITE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Map<String, String> tsIds = LOCATION_TO_TS_ID.get(locationId);
            for(String tsId : tsIds.values())
            {
                DSSPathname path = new DSSPathname(tsId);
                int minutes = 15;
                if("1Hour".equals(path.getDPart()))
                {
                    minutes = 60;
                }
                int count = 60 / minutes;
                TimeSeries ts = new TimeSeries(null, -1, 0, tsId, OFFICE_ID, start, end, "m", Duration.ofMinutes(minutes));

                ZonedDateTime next = start;
                for(int i = 0; i < count; i++)
                {
                    Timestamp dateTime = Timestamp.valueOf(next.toLocalDateTime());
                    ts.addValue(dateTime, (double) i, 0);
                    next = next.plusMinutes(minutes);
                }
                tsToCreate.add(ts);
            }
        }

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context);
            for(TimeSeries ts : tsToCreate)
            {
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

            // Call a2w store procedure
            for (String locationId : LOCATION_TO_TS_ID.keySet()) {
                Map<String, String> tsIds = LOCATION_TO_TS_ID.get(locationId);
                context.connection(conn -> {
                    try (CallableStatement cs = conn.prepareCall("{ call cwms_cma_pkg.p_load_a2w_by_location(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) }")) {
                        cs.setString(1, OFFICE_ID);
                        cs.setString(2, locationId);
                        cs.setString(3, "Y");  // Example display flag
                        cs.setString(4, "Generated data"); // Example notes
                        cs.setInt(5, tsIds.values().size());
                        cs.setObject(6, tsIds.get(STAGE) == null ? null : TS_CODE_MAP.get(tsIds.get(STAGE)));
                        cs.setObject(7, tsIds.get(FLOW) == null ? null : TS_CODE_MAP.get(tsIds.get(FLOW)));
                        cs.setObject(8, null);
                        cs.setObject(9, null);
                        cs.setObject(10, null);
                        cs.setObject(11, null);
                        cs.setObject(12, null);
                        cs.setObject(13, null);
                        cs.setObject(14, null);
                        cs.setObject(15, null);
                        cs.setObject(16, null);
                        cs.setObject(17, null);
                        cs.setObject(18, null);
                        cs.setObject(19, null);
                        cs.setObject(20, null);
                        cs.setObject(21, null);
                        cs.setObject(22, null);
                        cs.setObject(23, null);
                        cs.setObject(24, null);
                        cs.setObject(25, null);
                        cs.setObject(26, null);
                        cs.setObject(27, null);
                        cs.setObject(28, null);
                        cs.setObject(29, null);
                        cs.setObject(30, null);
                        cs.setObject(31, null);
                        cs.setObject(32, null);
                        cs.setObject(33, null);
                        cs.registerOutParameter(34, Types.VARCHAR); // p_error_msg as OUT parameter
                        cs.execute();
                    }
                });
            }
        }, "CWMS_20");
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            for (Object tsCode : TS_CODE_MAP.values()) {
                context.connection(conn -> {
                    try (CallableStatement cs = conn.prepareCall("{ call cwms_cma_pkg.p_clear_a2w_ts_code(?) }")) {
                        cs.setObject(1, tsCode);
                        cs.execute();
                    }
                });
            }
            TimeSeriesDeleteOptions options = new TimeSeriesDaoImpl.DeleteOptions.Builder()
                    .withStartTime(Date.from(start.toInstant())).withEndTime(Date.from(end.toInstant()))
                    .withEndTimeInclusive(true).withStartTimeInclusive(true).withMaxVersion(true)
                    .withVersionDate(Date.from(start.toInstant())).withOverrideProtection("T").build();
            TimeSeriesDao timeSeriesDao = new TimeSeriesDaoImpl(context);
            LOCATION_TO_TS_ID.values().forEach(tsIds ->
                    tsIds.values().forEach(tsId ->
                            timeSeriesDao.delete(OFFICE_ID, tsId, options)));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testGetAllNotPaginated()
    {
        // Create test parameters (mock the filters for officeId, locationId, and tsType)
        String officeMask = "SPK";  // Example office ID
        String locationMask = "AARK||ADDI";  // Example location IDs
        String tsType = "Stage||Flow";  // Example time series types

        // Send a request to the endpoint to retrieve all time series profiles without pagination
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam("office-mask", officeMask)
                .queryParam("location-mask", locationMask)
                .queryParam("ts-type", tsType)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/access-to-water/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("result.size()", greaterThan(0)) // Ensure there is some data returned
                .body("result[0].tsId", notNullValue()) // Check that each time series has a tsId
                .body("result[0].locationId", equalTo("AARK")) // Example check for locationId
                .body("result[0].tsType", equalTo("Stage")); // Example check for time series type
    }

    @Test
    void testGetAllPaginated()
    {
        // Create test parameters (mock the filters for officeId, locationId, and tsType)
        String locationMask = "AARK||ADDI";  // Example location IDs
        String tsType = "Stage||Flow";  // Example time series types

        // Send a request to the endpoint to retrieve all time series profiles without pagination
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam("office-mask", OFFICE_ID)
                .queryParam("location-mask", locationMask)
                .queryParam("ts-type", tsType)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/access-to-water/") // Endpoint for time series retrieval
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("result.size()", greaterThan(0)) // Ensure there is some data returned
                .body("result[0].tsId", notNullValue()) // Check that each time series has a tsId
                .body("result[0].locationId", equalTo("AARK")) // Example check for locationId
                .body("result[0].tsType", equalTo("Stage")); // Example check for time series type

        // Retrieve with pagination, page 1
        ExtractableResponse<Response> response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(PAGE_SIZE, 1)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/access-to-water/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("typed-time-series-identifiers.size()", is(1))
                .body("next-page", is(notNullValue())).extract();

        String nextPageCursor = response.path("next-page");
        assert nextPageCursor != null;

        // Retrieve with pagination, page 2
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .contentType(Formats.JSONV1)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(PAGE_SIZE, 1)
                .queryParam(PAGE, nextPageCursor)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/access-to-water/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("typed-time-series-identifiers.size()", is(1));
    }
}
