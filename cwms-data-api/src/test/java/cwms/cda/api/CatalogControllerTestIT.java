package cwms.cda.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static cwms.cda.api.Controllers.*;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.data.dto.catalog.TimeSeriesAlias;
import cwms.cda.data.dto.catalog.TimeseriesCatalogEntry;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.Duration;

import java.time.ZoneId;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.formatters.Formats;

import static io.restassured.RestAssured.*;

import io.restassured.filter.log.LogDetail;
import io.restassured.response.Response;

import static org.hamcrest.Matchers.*;

@Tag("integration")
public class CatalogControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    //// These have to match the groups in ts_catalog_setup.sql
    public static final String A_TO_M = "A to M";
    public static final String N_TO_Z = "N to Z";
    public static final String EVENS = "Evens";
    public static final String TEST_CATEGORY = "Test Category";
    ////

    @BeforeAll
    static void setup_data() throws Exception {
        // Create some locations and create some ts.
        createLocation("Alder Springs",true, OFFICE);
        createLocation("Wet Meadows",true, OFFICE);
        createLocation("Pine Flat-Outflow",true, OFFICE);
        createLocation("Flat Lake",true, OFFICE);

        createProject("Flat Project", OFFICE);
        createTimeseries(OFFICE,"Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.one");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.two");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.three");
        createTimeseries(OFFICE,"Pine Flat-Outflow.Stage.Inst.15Minutes.0.four");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.one");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.two");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.three");
        createTimeseries(OFFICE,"Wet Meadows.Depth-SWE.Inst.15Minutes.0.four");

        // Complicated
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_setup.sql");

        loadSqlDataFromResource("cwms/cda/data/sql/location_catalog_setup.sql");
    }

    private static void createProject(String id, String office) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            ProjectDao projectDao = new ProjectDao(dsl);
            Project project = new Project.Builder()
                    .withLocation(new Location.Builder(id,
                            "PROJECT",
                            ZoneId.of("UTC"),
                            0.0,
                            0.0,
                            "WGS84",
                            office)
                            .build())
                    .build();
        projectDao.create(project, true);
        });
    }

    private static void deleteProject(String id, String office) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            ProjectDao projectDao = new ProjectDao(dsl);

            projectDao.delete(office, id, DeleteRule.DELETE_KEY);
        });
    }

    @AfterAll
    static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/ts_catalog_cleanup.sql");
        deleteProject("Flat Project", OFFICE);
        cleanupBasins();
        cleanupStreams();
    }

    @Test
    void test_no_aliased_results_returned() {
        given().accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY,false)
            .queryParam(LIKE,".*-cda$")
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$",hasKey("total"))
            .body("total",is(4))
            .body("entries.size()",is(4));
    }

    @Test
    void test_no_aliases_returned() {
        Integer numAliases = given().accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY, false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .extract()
            .jsonPath()
            .getObject("entries.aliases.aliases.size()", Integer.class);
        assertEquals(0, (int) numAliases, "Expected no aliases, but found some.");
    }

    @Test
    void test_aliases_returned() {
        Integer numAliases = given().accept(Formats.JSONV2)
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY,false)
            .queryParam(INCLUDE_ALIASES,true)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .extract()
            .jsonPath()
            .getObject("entries.aliases.aliases.size()", Integer.class);
        assertTrue(numAliases > 0, "Expected aliases, but found none.");
    }

    @Test
    void test_alias_is_correct() throws JsonProcessingException {
        Response response = given().accept(Formats.JSONV2)
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(EXCLUDE_EMPTY, false)
                .queryParam(INCLUDE_ALIASES, true)
        .when()
                .get("/catalog/TIMESERIES");
        String json = response.body().asPrettyString();
        ObjectMapper om = JsonV2.buildObjectMapper();
        JsonNode root = om.readTree(json);
        JsonNode entriesNode = root.get("entries");
        String entriesJson = om.writeValueAsString(entriesNode);
        List<TimeseriesCatalogEntry> entries = om.readValue(entriesJson, new TypeReference<List<TimeseriesCatalogEntry>>() {});
        assertNotNull(entries);
        TimeseriesCatalogEntry alias = entries
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()).stream()
                .filter(e -> e.getName().equals("Pine Flat-Outflow.Stage.Inst.15Minutes.0.one"))
                .findFirst()
                .orElse(null);
        assertNotNull(alias);
        assertTrue(alias.getAliases().contains(new TimeSeriesAlias.Builder()
                .withName("Test Category-LessThan3")
                .withValue("test alias 1")
                .build()));
        //make sure no entries exist with name "test alias 1"
        List<TimeseriesCatalogEntry> aliasesAsAnEntry = entries
                .stream()
                .filter(Objects::nonNull)
                .filter(e -> e.getName().equals("test alias 1"))
                .collect(Collectors.toList());
        assertTrue(aliasesAsAnEntry.isEmpty(), "Found entries with name 'test alias 1', which should not exist.");
    }


    @Test
    void test_queries_are_case_insensitive() {
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EXCLUDE_EMPTY,false)
            .queryParam(LIKE,"alder spRINgs.*-CDA$")
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$",hasKey("total"))
            .body("total",is(2))
            .body("entries.size()",is(2));
    }

    @Test
    void test_all_office_pagination_works() {
        assertTimeout(Duration.ofMinutes(5), () -> {
            final int pageSize = 50;
            Response initialResponse =
                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                    .queryParam("page-size",pageSize)
                    .queryParam(EXCLUDE_EMPTY,false)
                .when()
                    .get("/catalog/TIMESERIES")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                    .statusCode(is(200))
                    .body("$",hasKey("total"))
                    .body("$",hasKey("next-page"))
                    .body("page-size",is(pageSize))
                    .body("entries.size()",is(pageSize))
                    .extract()
                .response();
            String nextPage = initialResponse.path("next-page");

            final int total = initialResponse.path("total");
            int totalRetrieved = initialResponse.path("entries.size()");

            String lastRowPreviousPage = initialResponse.path("entries.last().name");

            do {
                Response pageN =
                    given()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .accept(Formats.JSONV2)
                        .queryParam("page",nextPage)
                        .queryParam(EXCLUDE_EMPTY,false)
                    .when()
                        .get("/catalog/TIMESERIES")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .assertThat()
                        .statusCode(is(200))
                        .body("$",hasKey("total"))
                        .body("page-size",is(pageSize))
                        .body("page",equalTo(nextPage))
                        .body("entries[0].name",not(equalTo(lastRowPreviousPage)))
                        .extract()
                    .response();
                nextPage = pageN.path("next-page");
                int pageTotal = pageN.path("entries.size()");
                if (pageTotal > 0) {
                    lastRowPreviousPage = pageN.path("entries.last().name");
                } else {
                    lastRowPreviousPage = "No data in this response.";
                }
                totalRetrieved += pageTotal;
            } while( nextPage != null );
            assertEquals(total, totalRetrieved, "Expected amount of results not returned.");
        }, "Catalog retrieval got stuck; possibly in endless loop");
    }


    @Test
    void test_loc_group_with_ts_group() {


        // filter by loc group and ts group should find the intersection
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, N_TO_Z)
            .queryParam(TIMESERIES_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(TIMESERIES_GROUP_LIKE, EVENS)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(4))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(4))
            .body("entries[0].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.four"))
            .body("entries[1].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.two"))
            .body("entries[2].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.four"))
            .body("entries[3].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.two"))
            ;
    }

    @Test
    void test_loc_group() {


        // filter by just loc group
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, A_TO_M)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(2))
            .body("entries[0].name",equalTo("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda"))
            .body("entries[1].name",equalTo("Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda"))
        ;



        // filter by just loc group
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LOCATION_CATEGORY_LIKE, TEST_CATEGORY)
            .queryParam(LOCATION_GROUP_LIKE, N_TO_Z)
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(10))
            .body("$", hasKey("entries"))
            .body("entries.size()",is(10))
            .body("entries[0].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.four"))
            .body("entries[1].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.one"))
            .body("entries[2].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda"))
            .body("entries[3].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.three"))
            .body("entries[4].name",equalTo("Pine Flat-Outflow.Stage.Inst.15Minutes.0.two"))
            .body("entries[5].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.four"))
            .body("entries[6].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.one"))
            .body("entries[7].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda"))
            .body("entries[8].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.three"))
            .body("entries[9].name",equalTo("Wet Meadows.Depth-SWE.Inst.15Minutes.0.two"))
        ;

    }


    @Test
    void test_ts_with_bounding() {

        // we create Wet Meadows with a bounding office of SPK
        given()
            .accept("application/json;version=2")
            .queryParam(BOUNDING_OFFICE_LIKE, OFFICE)
            .queryParam(LIKE, "^Wet Meadows.*")
            .queryParam(EXCLUDE_EMPTY,false)
        .when()
            .get("/catalog/TIMESERIES")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", greaterThan(3))
            .body("$", hasKey("entries"))
            .body("entries.size()",greaterThan(3))
            .body("entries.name",everyItem(startsWith("Wet Meadows")))
        ;
    }

    @Test
    void test_loc_kind() {

        String pattern = "^Flat";

        // First with just the regex.  This should match Flat Lake and Flat Project
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(2))
        ;

        // Now add the LOCATION_KIND filter
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
            .queryParam(LOCATION_KIND_LIKE, "PROJECT")  // just Flat Project
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(1))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(1))
            .body("entries[0].name", equalTo("Flat Project"))
        ;
    }

    @Test
    void test_loc_aliases() {

        String pattern = "*Streamflow";

        // Retrieve without aliases
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
            .queryParam(INCLUDE_ALIASES, false)
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(2))
            .body("entries[0].aliases.size()", is(0))
        ;

        // retrieve with aliases
        given()
            .accept("application/json;version=2")
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LIKE, pattern)
            .queryParam(INCLUDE_ALIASES, true)
        .when()
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(200))
            .body("$", hasKey("total"))
            .body("total", is(2))
            .body("$", hasKey("entries"))
            .body("entries.size()", is(2))
            .body("entries[0].name", isOneOf("Alder Springs Streamflow", "Pine Flat-Outflow Streamflow"))
            .body("entries[0].aliases.size()", isOneOf(1, 2))
            .body("entries[0].aliases[0].value",
                isOneOf("Alder Stream Alias Loc", "Alder Stream Alias Loc 2", "Pine Stream Alias Loc"))
        ;
    }

    @Test
    void testFilterLocations() throws Exception{
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String officeId = "SPK";

        String json = loadResourceAsString("cwms/cda/api/filter_locations.json");
        ContentType contentType = new ContentType(Formats.JSON);
        List<Location> locations = Formats.parseContentList(contentType, json, Location.class);

        String baseLocationName = locations.get(0).getName();

        // create base location
        createLocation(baseLocationName, true, officeId, locations.get(0).getLocationKind());

        String subLocationName = locations.get(1).getName();

        // create sub-location
        createLocation(subLocationName, true, officeId, locations.get(1).getLocationKind());

        String downStreamJson1 = loadResourceAsString("cwms/cda/api/loc_filter_stream1.json");
        Stream stream1 = Formats.parseContent(contentType, downStreamJson1, Stream.class);
        createStream(stream1);

        String subLocation2Name = locations.get(2).getName();

        // create second sub-location
        createLocation(subLocation2Name, true, officeId, locations.get(2).getLocationKind());

        String basinJson = loadResourceAsString("cwms/cda/api/loc_filter_basin.json");
        Basin basin = Formats.parseContent(contentType, basinJson, Basin.class);
        createBasin(basin);

        String subLocation3Name = locations.get(3).getName();

        // create second sub-location
        createLocation(subLocation3Name, true, officeId, locations.get(3).getLocationKind());

        String downStreamJson2 = loadResourceAsString("cwms/cda/api/loc_filter_stream2.json");
        Stream stream2 = Formats.parseContent(contentType, downStreamJson2, Stream.class);
        createStream(stream2);

        String stringToMatch = String.format("%s-.*", baseLocationName);

        // get all valid locations
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, String.format("%s*", baseLocationName))
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(4))
            .body("entries[0].name", isOneOf(baseLocationName, subLocationName, subLocation2Name, subLocation3Name))
            .body("entries[1].name", isOneOf(baseLocationName, subLocationName, subLocation2Name, subLocation3Name))
            .body("entries[2].name", isOneOf(baseLocationName, subLocationName, subLocation2Name, subLocation3Name))
            .body("entries[3].name", isOneOf(baseLocationName, subLocationName, subLocation2Name, subLocation3Name));

        // get all valid locations filtering out base location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(3))
            .body("entries[0].name", isOneOf(subLocationName, subLocation2Name, subLocation3Name))
            .body("entries[1].name", isOneOf(subLocationName, subLocation2Name, subLocation3Name))
            .body("entries[2].name", isOneOf(subLocationName, subLocation2Name, subLocation3Name));

        // get valid locations using base location, filtering out OUTLET kind
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(LOCATION_KIND_LIKE, "^(BASIN)$")
            .queryParam(NEGATE_LOCATION_KIND_LIKE, true)
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(2))
            .body("entries[0].name", isOneOf(subLocationName, subLocation3Name))
            .body("entries[1].name", isOneOf(subLocationName, subLocation3Name));

        // get valid locations using base location, filtering out STREAM kind
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(LOCATION_KIND_LIKE, "^(STREAM)*$")
            .queryParam(NEGATE_LOCATION_KIND_LIKE, true)
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(1))
            .body("entries[0].name", is(subLocation2Name));

        // get valid locations using base location, filtering out STREAM kind using NOT operator
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(LOCATION_KIND_LIKE, "NOT:^(STREAM)*$")
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(1))
            .body("entries[0].name", is(subLocation2Name));

        // get valid locations using base location, filtering out STREAM kind using NOT operator and negation parameter
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(LOCATION_KIND_LIKE, "NOT:^(STREAM)*$")
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
            .queryParam(NEGATE_LOCATION_KIND_LIKE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(1))
            .body("entries[0].name", is(subLocation2Name));

        // get valid locations using base location, filtering out expected kinds. Should return 0 locations
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(LIKE, stringToMatch)
            .queryParam(UNIT_SYSTEM, UnitSystem.SI.getValue())
            .queryParam(LOCATION_KIND_LIKE, "^(BASIN|STREAM)*$")
            .queryParam(NEGATE_LOCATION_KIND_LIKE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/catalog/LOCATIONS")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entries.size()", is(0));
    }
}
