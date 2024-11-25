/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dao.location.kind.LockDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

@Tag("integration")
final class LockControllerIT extends DataApiTestIT {
    private static final Logger LOGGER = Logger.getLogger(LockControllerIT.class.getName());
    private static final Location PROJECT_LOC;
    private static final Location PROJECT_LOC2;
    private static final Location LOCK_LOC;
    private static final Location LOCK_LOC2;
    private static final Lock LOCK;
    private static final Lock STORABLE_LOCK;
    private static List<LocationLevel> locationLevelsToCleanup = new ArrayList<>();
    private static Lock lockToCleanup;

    static {
        try (
            InputStream projectStream = LockControllerIT.class.getResourceAsStream(
                "/cwms/cda/api/project_location_lock.json");
            InputStream projectStream2 = LockControllerIT.class.getResourceAsStream(
                "/cwms/cda/api/project_location_lock2.json");
            InputStream lockStream = LockControllerIT.class.getResourceAsStream("/cwms/cda/api/lock.json")) {
			String projectLocJson = IOUtils.toString(projectStream, StandardCharsets.UTF_8);
            String projectLocJson2 = IOUtils.toString(projectStream2, StandardCharsets.UTF_8);
            ContentType contentType = new ContentType(Formats.JSONV1);
            PROJECT_LOC = Formats.parseContent(contentType, projectLocJson, Location.class);
            PROJECT_LOC2 =Formats.parseContent(contentType, projectLocJson2, Location.class);
			String lockJson = IOUtils.toString(lockStream, StandardCharsets.UTF_8);
            LOCK = Formats.parseContent(contentType, lockJson, Lock.class);
            LOCK_LOC = LOCK.getLocation();
            LOCK_LOC2 = new Location.Builder("SPK", "TEST_LOCATION3")
                    .withName("TEST_LOCATION3")
                    .withLocationKind("LOCK")
                    .withDescription("Test Lock")
                    .withHorizontalDatum("NVGD29")
                    .withTimeZoneName(ZoneId.of("UTC"))
                    .withOfficeId("SPK")
                    .withActive(true)
                    .withNation(Nation.US)
                    .withLatitude(38.6)
                    .withLongitude(-121.5)
                    .withNearestCity("Sacramento")
                    .withLocationType("LOCK")
                    .build();
            STORABLE_LOCK = new Lock.Builder()
                    .withLockLength(LOCK.getLockLength())
                    .withLockWidth(LOCK.getLockWidth())
                    .withMaximumLockLift(LOCK.getMaximumLockLift())
                    .withElevationUnits(LOCK.getElevationUnits())
                    .withChamberType(LOCK.getChamberType())
                    .withLocation(LOCK_LOC)
                    .withLengthUnits(LOCK.getLengthUnits())
                    .withVolumePerLockage(LOCK.getVolumePerLockage())
                    .withVolumeUnits(LOCK.getVolumeUnits())
                    .withProjectId(LOCK.getProjectId())
                    .withMinimumDraft(LOCK.getMinimumDraft())
                    .build();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            try {
                DSLContext context = getDslContext(c, LOCK_LOC.getOfficeId());
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                locationsDao.storeLocation(PROJECT_LOC);
                locationsDao.storeLocation(PROJECT_LOC2);
                PROJECT_OBJ_T projectObjT = buildProject(PROJECT_LOC);
                PROJECT_OBJ_T projectObjT2 = buildProject(PROJECT_LOC2);
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT2, "T");
                locationsDao.storeLocation(LOCK_LOC);
                locationsDao.storeLocation(LOCK_LOC2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, LOCK_LOC.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                locationsDao.deleteLocation(LOCK_LOC.getName(), LOCK_LOC.getOfficeId(), true);

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
                LOGGER.log(Level.CONFIG, String.format("Unable to delete Lock location: %s", ex.getMessage()));
            }
            try {
                locationsDao.deleteLocation(LOCK_LOC2.getName(), LOCK_LOC2.getOfficeId(), true);

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
                LOGGER.log(Level.CONFIG, String.format("Unable to delete Lock location: %s", ex.getMessage()));
            }
            try {
                CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
                    DeleteRule.DELETE_ALL.getRule(), PROJECT_LOC.getOfficeId());

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
                LOGGER.log(Level.CONFIG, String.format("Unable to delete project: %s", ex.getMessage()));
            }
            try {
                locationsDao.deleteLocation(PROJECT_LOC.getName(), PROJECT_LOC.getOfficeId(), true);
            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
                LOGGER.log(Level.CONFIG, String.format("Unable to delete project location: %s", ex.getMessage()));
            }
            try {
                locationsDao.deleteLocation(PROJECT_LOC2.getName(), PROJECT_LOC2.getOfficeId(), true);
            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
                LOGGER.log(Level.CONFIG, String.format("Unable to delete project location: %s", ex.getMessage()));
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    void cleanupBetween() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, LOCK_LOC.getOfficeId());
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(context);
            LockDao lockDao = new LockDao(context);
            if (lockToCleanup != null) {
                try {
                    lockDao.deleteLock(CwmsId.buildCwmsId(lockToCleanup.getLocation().getOfficeId(), lockToCleanup.getLocation().getName()), DeleteRule.DELETE_ALL);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, String.format("Unable to delete lock: %s", ex.getMessage()));
                }
            }
            for (LocationLevel level : createLocationLevelList(LOCK)) {
                try {
                    levelsDao.deleteLocationLevel(level.getLocationLevelId(), level.getLevelDate(), level.getOfficeId(), true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, String.format("Unable to delete location level: %s", ex.getMessage()));
                }
            }
            for (LocationLevel level : locationLevelsToCleanup) {
                try {
                    levelsDao.deleteLocationLevel(level.getLocationLevelId(), level.getLevelDate(), level.getOfficeId(), true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, String.format("Unable to delete location level: %s", ex.getMessage()));
                }
            }
            locationLevelsToCleanup.clear();
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_get_create_delete_EN() {

        // Structure of test:
        // 1)Create the Lock
        // 2)Retrieve the Lock and assert that it exists
        // 3)Delete the Lock
        // 4)Retrieve the Lock and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), STORABLE_LOCK);
        List<LocationLevel> levelList = createLocationLevelList(LOCK);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }

        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = LOCK.getLocation().getOfficeId();
        // Retrieve the Lock and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(UNIT, "EN")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location", not(nullValue()))
            .body("project-id.name", equalTo(LOCK.getProjectId().getName()))
            .body("project-id.office-id", equalTo(LOCK.getProjectId().getOfficeId()))
            .body("maximum-lock-lift", equalTo((float) LOCK.getMaximumLockLift()))
            .body("elevation-units", equalTo(LOCK.getElevationUnits()))
            .body("chamber-type.display-value", equalTo(LOCK.getChamberType().getDisplayValue()))
            .body("chamber-type.tooltip", equalTo(LOCK.getChamberType().getTooltip()))
            .body("volume-per-lockage", equalTo((float) LOCK.getVolumePerLockage()))
            .body("volume-units", equalTo(LOCK.getVolumeUnits()))
            .body("lock-width", equalTo((float) LOCK.getLockWidth()))
            .body("lock-length", equalTo((float) LOCK.getLockLength()))
            .body("length-units", equalTo(LOCK.getLengthUnits()))
            .body("minimum-draft", equalTo((float) LOCK.getMinimumDraft()))
            .body("high-water-upper-pool-location-level.level-value",
                    equalTo(LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue().floatValue()))
            .body("high-water-lower-pool-location-level.level-value",
                    equalTo(LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue().floatValue()))
            .body("low-water-upper-pool-location-level.level-value",
                    equalTo(LOCK.getLowWaterUpperPoolLocationLevel().getLevelValue().floatValue()))
            .body("low-water-lower-pool-location-level.level-value",
                    equalTo(LOCK.getLowWaterLowerPoolLocationLevel().getLevelValue().floatValue()))
            .body("high-water-upper-pool-warning-level",
                    equalTo((float) (LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue() - LOCK.getHighWaterUpperPoolWarningLevel())))
            .body("high-water-lower-pool-warning-level",
                    equalTo((float) (LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue() - LOCK.getHighWaterLowerPoolWarningLevel())))
        ;

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Lock and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_create_delete_SI() throws Exception {

        // Structure of test:
        // 1)Create the Lock
        // 2)Retrieve the Lock and assert that it exists
        // 3)Delete the Lock
        // 4)Retrieve the Lock and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        Lock metricLock = new Lock.Builder()
                .withLockLength(STORABLE_LOCK.getLockLength())
                .withLockWidth(STORABLE_LOCK.getLockWidth())
                .withProjectId(STORABLE_LOCK.getProjectId())
                .withElevationUnits("m")
                .withChamberType(STORABLE_LOCK.getChamberType())
                .withMinimumDraft(STORABLE_LOCK.getMinimumDraft())
                .withMaximumLockLift(STORABLE_LOCK.getMaximumLockLift())
                .withVolumeUnits("m3")
                .withLocation(STORABLE_LOCK.getLocation())
                .withNormalLockLift(STORABLE_LOCK.getNormalLockLift())
                .withVolumePerLockage(STORABLE_LOCK.getVolumePerLockage())
                .withLengthUnits("m")
                .withHighWaterLowerPoolWarningLevel(LOCK.getHighWaterLowerPoolWarningLevel())
                .withHighWaterUpperPoolWarningLevel(LOCK.getHighWaterUpperPoolWarningLevel())
                .build();
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), metricLock);
        List<LocationLevel> levelList = createLocationLevelList(LOCK);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }

        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = metricLock.getLocation().getOfficeId();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, office);
            double metricWarningLevelUpper = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue() - LOCK.getHighWaterUpperPoolWarningLevel(), LOCK.getElevationUnits(), "m");
            double metricWarningLevelLower = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue() - LOCK.getHighWaterLowerPoolWarningLevel(), LOCK.getElevationUnits(), "m");
            double metricHighWaterLowerPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricHighWaterUpperPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricLowWaterLowerPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getLowWaterLowerPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricLowWaterUpperPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getLowWaterUpperPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");

            // Retrieve the Lock and assert that it exists
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .queryParam(Controllers.OFFICE, office)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/projects/locks/" + metricLock.getLocation().getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("location", not(nullValue()))
                .body("project-id.name", equalTo(metricLock.getProjectId().getName()))
                .body("project-id.office-id", equalTo(metricLock.getProjectId().getOfficeId()))
                .body("maximum-lock-lift", equalTo((float) metricLock.getMaximumLockLift()))
                .body("elevation-units", equalTo(metricLock.getElevationUnits()))
                .body("chamber-type.display-value", equalTo(metricLock.getChamberType().getDisplayValue()))
                .body("chamber-type.tooltip", equalTo(metricLock.getChamberType().getTooltip()))
                .body("volume-per-lockage", equalTo((float) metricLock.getVolumePerLockage()))
                .body("volume-units", equalTo(metricLock.getVolumeUnits()))
                .body("lock-width", equalTo((float) metricLock.getLockWidth()))
                .body("lock-length", equalTo((float) metricLock.getLockLength()))
                .body("length-units", equalTo(metricLock.getLengthUnits()))
                .body("minimum-draft", equalTo((float) metricLock.getMinimumDraft()))
                .body("high-water-upper-pool-location-level.level-value", equalTo((float) metricHighWaterUpperPoolValue))
                .body("high-water-lower-pool-location-level.level-value", equalTo((float) metricHighWaterLowerPoolValue))
                .body("low-water-upper-pool-location-level.level-value", equalTo((float) metricLowWaterUpperPoolValue))
                .body("low-water-lower-pool-location-level.level-value", equalTo((float) metricLowWaterLowerPoolValue))
                .body("high-water-upper-pool-warning-level", equalTo((float) metricWarningLevelUpper))
                .body("high-water-lower-pool-warning-level", equalTo((float) metricWarningLevelLower))
            ;
        }, CwmsDataApiSetupCallback.getWebUser());

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Lock and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_update_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .queryParam(Controllers.NAME, "NewBogus")
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/projects/locks/bogus")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void storeRetrieveSameLockNameDifferentProject() throws Exception {
        // Structure of test:
        // 1)Create the two Locks
        // 2)Retrieve the Locks and assert that they exist
        // 3)Delete the Locks
        // 4)Retrieve the Locks and assert that they do not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        Lock metricLock = new Lock.Builder()
            .withLockLength(120.5)
            .withLockWidth(56.4)
            .withProjectId(CwmsId.buildCwmsId(PROJECT_LOC2.getOfficeId(), PROJECT_LOC2.getName()))
            .withElevationUnits("m")
            .withChamberType(STORABLE_LOCK.getChamberType())
            .withMinimumDraft(15.3)
            .withMaximumLockLift(60.2)
            .withVolumeUnits("m3")
            .withLocation(LOCK_LOC2)
            .withNormalLockLift(45.1)
            .withVolumePerLockage(87.8)
            .withLengthUnits("m")
            .withHighWaterLowerPoolWarningLevel(12.0)
            .withHighWaterUpperPoolWarningLevel(12.0)
            .build();
        Lock metricLockWithLevels = new Lock.Builder()
            .withLockLength(120.5)
            .withLockWidth(56.4)
            .withProjectId(CwmsId.buildCwmsId(PROJECT_LOC2.getOfficeId(), PROJECT_LOC2.getName()))
            .withElevationUnits("m")
            .withChamberType(STORABLE_LOCK.getChamberType())
            .withMinimumDraft(15.3)
            .withMaximumLockLift(60.2)
            .withVolumeUnits("m3")
            .withLocation(LOCK_LOC2)
            .withNormalLockLift(45.1)
            .withVolumePerLockage(87.8)
            .withLengthUnits("m")
            .withHighWaterLowerPoolWarningLevel(12.0)
            .withHighWaterUpperPoolWarningLevel(12.0)
            .withLowWaterUpperPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.Low Water Upper Pool?office=%s",
                        LOCK_LOC2.getName(), LOCK_LOC2.getOfficeId()), 25.0))
            .withLowWaterLowerPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.Low Water Lower Pool?office=%s",
                        LOCK_LOC2.getName(), LOCK_LOC2.getOfficeId()), 50.0))
            .withHighWaterUpperPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.High Water Upper Pool?office=%s",
                        LOCK_LOC2.getName(), LOCK_LOC2.getOfficeId()), 30.0))
            .withHighWaterLowerPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.High Water Lower Pool?office=%s",
                        LOCK_LOC2.getName(), LOCK_LOC2.getOfficeId()), 60.0))
            .build();
        final Lock metricLockProj2 = metricLock;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), metricLock);

        // location level storage assumes SI units
        List<LocationLevel> levelList = createLocationLevelList(LOCK);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }

        levelList = createLocationLevelList(metricLockWithLevels);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }
        locationLevelsToCleanup.addAll(levelList);

        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        lockToCleanup = metricLockProj2;

        metricLock = new Lock.Builder()
            .withLockLength(STORABLE_LOCK.getLockLength())
            .withLockWidth(STORABLE_LOCK.getLockWidth())
            .withProjectId(STORABLE_LOCK.getProjectId())
            .withElevationUnits("m")
            .withChamberType(STORABLE_LOCK.getChamberType())
            .withMinimumDraft(STORABLE_LOCK.getMinimumDraft())
            .withMaximumLockLift(STORABLE_LOCK.getMaximumLockLift())
            .withVolumeUnits("m3")
            .withLocation(STORABLE_LOCK.getLocation())
            .withNormalLockLift(STORABLE_LOCK.getNormalLockLift())
            .withVolumePerLockage(STORABLE_LOCK.getVolumePerLockage())
            .withLengthUnits("m")
            .withHighWaterLowerPoolWarningLevel(LOCK.getHighWaterLowerPoolWarningLevel())
            .withHighWaterUpperPoolWarningLevel(LOCK.getHighWaterUpperPoolWarningLevel())
            .build();
        final Lock metricLockProj1 = metricLock;
        json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), metricLock);
        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        String office = metricLock.getLocation().getOfficeId();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, office);
            double metricWarningLevelUpper = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue() - LOCK.getHighWaterUpperPoolWarningLevel(), LOCK.getElevationUnits(), "m");
            double metricWarningLevelLower = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue() - LOCK.getHighWaterLowerPoolWarningLevel(), LOCK.getElevationUnits(), "m");
            double metricHighWaterLowerPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterLowerPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricHighWaterUpperPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getHighWaterUpperPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricLowWaterLowerPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getLowWaterLowerPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");
            double metricLowWaterUpperPoolValue = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(context.configuration(),
                    LOCK.getLowWaterUpperPoolLocationLevel().getLevelValue(), LOCK.getElevationUnits(), "m");

            // Retrieve the Lock and assert that it exists
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(UNIT, "SI")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/projects/locks/" + metricLockProj1.getLocation().getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("location", not(nullValue()))
                .body("project-id.name", equalTo(metricLockProj1.getProjectId().getName()))
                .body("project-id.office-id", equalTo(metricLockProj1.getProjectId().getOfficeId()))
                .body("maximum-lock-lift", equalTo((float) metricLockProj1.getMaximumLockLift()))
                .body("elevation-units", equalTo(metricLockProj1.getElevationUnits()))
                .body("chamber-type.display-value", equalTo(metricLockProj1.getChamberType().getDisplayValue()))
                .body("chamber-type.tooltip", equalTo(metricLockProj1.getChamberType().getTooltip()))
                .body("volume-per-lockage", equalTo((float) metricLockProj1.getVolumePerLockage()))
                .body("volume-units", equalTo(metricLockProj1.getVolumeUnits()))
                .body("lock-width", equalTo((float) metricLockProj1.getLockWidth()))
                .body("lock-length", equalTo((float) metricLockProj1.getLockLength()))
                .body("length-units", equalTo(metricLockProj1.getLengthUnits()))
                .body("minimum-draft", equalTo((float) metricLockProj1.getMinimumDraft()))
                .body("high-water-upper-pool-location-level.level-value", equalTo((float) metricHighWaterUpperPoolValue))
                .body("high-water-lower-pool-location-level.level-value", equalTo((float) metricHighWaterLowerPoolValue))
                .body("low-water-upper-pool-location-level.level-value", equalTo((float) metricLowWaterUpperPoolValue))
                .body("low-water-lower-pool-location-level.level-value", equalTo((float) metricLowWaterLowerPoolValue))
                .body("high-water-upper-pool-warning-level", equalTo((float) (metricWarningLevelUpper)))
                .body("high-water-lower-pool-warning-level", equalTo((float) (metricWarningLevelLower)))
            ;

            // Retrieve the Lock and assert that it exists
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV1)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(UNIT, "SI")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/projects/locks/" + metricLockProj2.getLocation().getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("location", not(nullValue()))
                .body("project-id.name", equalTo(metricLockProj2.getProjectId().getName()))
                .body("project-id.office-id", equalTo(metricLockProj2.getProjectId().getOfficeId()))
                .body("maximum-lock-lift", equalTo((float) metricLockProj2.getMaximumLockLift()))
                .body("elevation-units", equalTo(metricLockProj2.getElevationUnits()))
                .body("chamber-type.display-value", equalTo(metricLockProj2.getChamberType().getDisplayValue()))
                .body("chamber-type.tooltip", equalTo(metricLockProj2.getChamberType().getTooltip()))
                .body("volume-per-lockage", equalTo((float) metricLockProj2.getVolumePerLockage()))
                .body("volume-units", equalTo(metricLockProj2.getVolumeUnits()))
                .body("lock-width", equalTo((float) metricLockProj2.getLockWidth()))
                .body("lock-length", equalTo((float) metricLockProj2.getLockLength()))
                .body("length-units", equalTo(metricLockProj2.getLengthUnits()))
                .body("minimum-draft", equalTo((float) metricLockProj2.getMinimumDraft()))
                .body("high-water-upper-pool-location-level.level-value",
                        equalTo(metricLockWithLevels.getHighWaterUpperPoolLocationLevel().getLevelValue().floatValue()))
                .body("high-water-lower-pool-location-level.level-value",
                        equalTo(metricLockWithLevels.getHighWaterLowerPoolLocationLevel().getLevelValue().floatValue()))
                .body("low-water-upper-pool-location-level.level-value",
                        equalTo(metricLockWithLevels.getLowWaterUpperPoolLocationLevel().getLevelValue().floatValue()))
                .body("low-water-lower-pool-location-level.level-value",
                        equalTo(metricLockWithLevels.getLowWaterLowerPoolLocationLevel().getLevelValue().floatValue()))
                .body("high-water-upper-pool-warning-level",
                        equalTo((float) (metricLockWithLevels.getHighWaterUpperPoolLocationLevel().getLevelValue()
                                - metricLockWithLevels.getHighWaterUpperPoolWarningLevel())))
                .body("high-water-lower-pool-warning-level",
                        equalTo((float) (metricLockWithLevels.getHighWaterLowerPoolLocationLevel().getLevelValue()
                                - metricLockWithLevels.getHighWaterLowerPoolWarningLevel())))
            ;
        }, CwmsDataApiSetupCallback.getWebUser());

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Lock and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(PROJECT_ID, metricLockProj1.getProjectId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + metricLockProj2.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Lock and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + metricLockProj2.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        lockToCleanup = null;
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + Instant.now().toEpochMilli())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_save_lock_with_level_values() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), LOCK);
        List<LocationLevel> levelList = createLocationLevelList(LOCK);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }

        //Create the Lock with specified level values
        // expect an error to be thrown
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;
    }

    @Test
    void test_get_all() {

        // Structure of test:
        // 1)Create the Lock
        // 2)Retrieve the Lock with getAll and assert that it exists
        // 3)Delete the Lock
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), STORABLE_LOCK);
        List<LocationLevel> levelList = createLocationLevelList(LOCK);
        // store location levels
        for (LocationLevel level : levelList) {
            String levelJson = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), level);
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.JSONV1)
                .body(levelJson)
                .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/levels/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
            ;
        }
        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = LOCK.getLocation().getOfficeId();
        // Retrieve the Lock and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.PROJECT_ID, LOCK.getProjectId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].location.name", equalTo(LOCK.getLocation().getName()))
            .body("[0].location.office-id", equalTo(LOCK.getLocation().getOfficeId()))
        ;

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    private static PROJECT_OBJ_T buildProject(Location projectLocation) {
        PROJECT_OBJ_T retval = new PROJECT_OBJ_T();
        retval.setPROJECT_LOCATION(LocationUtil.getLocation(projectLocation));
        retval.setPUMP_BACK_LOCATION(null);
        retval.setNEAR_GAGE_LOCATION(null);
        retval.setAUTHORIZING_LAW(null);
        retval.setCOST_YEAR(Timestamp.from(Instant.now()));
        retval.setFEDERAL_COST(BigDecimal.ONE);
        retval.setNONFEDERAL_COST(BigDecimal.TEN);
        retval.setFEDERAL_OM_COST(BigDecimal.ZERO);
        retval.setNONFEDERAL_OM_COST(BigDecimal.valueOf(15.0));
        retval.setCOST_UNITS_ID("$");
        retval.setREMARKS("TEST RESERVOIR PROJECT");
        retval.setPROJECT_OWNER("CDA");
        retval.setHYDROPOWER_DESCRIPTION("HYDRO DESCRIPTION");
        retval.setSEDIMENTATION_DESCRIPTION("SEDIMENTATION DESCRIPTION");
        retval.setDOWNSTREAM_URBAN_DESCRIPTION("DOWNSTREAM URBAN DESCRIPTION");
        retval.setBANK_FULL_CAPACITY_DESCRIPTION("BANK FULL CAPACITY DESCRIPTION");
        retval.setYIELD_TIME_FRAME_START(Timestamp.from(Instant.now()));
        retval.setYIELD_TIME_FRAME_END(Timestamp.from(Instant.now()));
        return retval;
    }

    private List<LocationLevel> createLocationLevelList(Lock lock) {
        List<LocationLevel> retVal = new ArrayList<>();
        LocationLevel lowLowerLevel = new LocationLevel.Builder(lock.getLowWaterLowerPoolLocationLevel().getLevelId(), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getLowWaterLowerPoolLocationLevel().getLevelValue())
                .withOfficeId(lock.getLowWaterLowerPoolLocationLevel().getOfficeId())
                .withSpecifiedLevelId(lock.getLowWaterLowerPoolLocationLevel().getSpecifiedLevelId())
                .build();
        retVal.add(lowLowerLevel);
        LocationLevel lowUpperLevel = new LocationLevel.Builder(lock.getLowWaterUpperPoolLocationLevel().getLevelId(), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getLowWaterUpperPoolLocationLevel().getLevelValue())
                .withOfficeId(lock.getLowWaterUpperPoolLocationLevel().getOfficeId())
                .withSpecifiedLevelId(lock.getLowWaterUpperPoolLocationLevel().getSpecifiedLevelId())
                .build();
        retVal.add(lowUpperLevel);
        LocationLevel highLowerLevel = new LocationLevel.Builder(lock.getHighWaterLowerPoolLocationLevel().getLevelId(), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getHighWaterLowerPoolLocationLevel().getLevelValue())
                .withOfficeId(lock.getHighWaterLowerPoolLocationLevel().getOfficeId())
                .withSpecifiedLevelId(lock.getHighWaterLowerPoolLocationLevel().getSpecifiedLevelId())
                .build();
        retVal.add(highLowerLevel);
        LocationLevel highUpperLevel = new LocationLevel.Builder(lock.getHighWaterUpperPoolLocationLevel().getLevelId(), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getHighWaterUpperPoolLocationLevel().getLevelValue())
                .withOfficeId(lock.getHighWaterUpperPoolLocationLevel().getOfficeId())
                .withSpecifiedLevelId(lock.getHighWaterUpperPoolLocationLevel().getSpecifiedLevelId())
                .build();
        retVal.add(highUpperLevel);
        LocationLevel warningBuffer = new LocationLevel.Builder(String.format("%s.Elev-Closure.Inst.0.Warning Buffer", lock.getLocation().getName()), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getHighWaterLowerPoolWarningLevel())
                .withOfficeId(lock.getLocation().getOfficeId())
                .withSpecifiedLevelId("Warning Buffer")
                .build();
        retVal.add(warningBuffer);
        return retVal;
    }
}