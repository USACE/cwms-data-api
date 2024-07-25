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

package cwms.cda.data.dao.location.kind;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

@Tag("integration")
final class TurbineDaoIT extends DataApiTestIT {
    private static final String OFFICE = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final Location PROJECT_LOC = buildProjectLocation("PROJECT1dao2");
    private static final Location PROJECT_LOC2 = buildProjectLocation("PROJECT2dao2");
    private static final Location TURBINE_LOC1 = buildTurbineLocation("PROJECT1dao2-TURB_LOC1dao2");
    private static final Location TURBINE_LOC2 = buildTurbineLocation("TURB_LOC2dao2");
    private static final Location TURBINE_LOC3 = buildTurbineLocation("TURB_LOC3dao2");

    @BeforeAll
    public static void setup() throws Exception {
        tearDown();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE);
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                try {
                    CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC), "T");
                    CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC2), "T");
                    locationsDao.storeLocation(TURBINE_LOC1);
                    locationsDao.storeLocation(TURBINE_LOC2);
                    locationsDao.storeLocation(TURBINE_LOC3);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE);
                cleanTurbine(context, TURBINE_LOC1);
                cleanTurbine(context, TURBINE_LOC2);
                cleanTurbine(context, TURBINE_LOC3);
                cleanProject(context, PROJECT_LOC);
                cleanProject(context, PROJECT_LOC2);
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    private static void cleanTurbine(DSLContext context, Location turbine) {
        try {
            new TurbineDao(context).deleteTurbine(turbine.getName(), OFFICE, DeleteRule.DELETE_ALL);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }

        try {
            new LocationsDaoImpl(context).deleteLocation(turbine.getName(), OFFICE, true);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }
    }

    private static void cleanProject(DSLContext context, Location project) {
        try {
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), project.getName(),
                DeleteRule.DELETE_ALL.getRule(), OFFICE);
        } catch (DataAccessException ex) {
            if (!JooqDao.isNotFound(ex)) {
                throw ex;
            }
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }

        try {
            new LocationsDaoImpl(context).deleteLocation(project.getName(), OFFICE, true);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE);
                TurbineDao turbineDao = new TurbineDao(context);
                Turbine turbine = buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName());
                turbineDao.storeTurbine(turbine, false);
                String turbineId = turbine.getLocation().getName();
                String turbineOfficeId = turbine.getLocation().getOfficeId();
                Turbine retrievedTurbine = turbineDao.retrieveTurbine(turbineId, turbineOfficeId);
                DTOMatch.assertMatch(turbine, retrievedTurbine);
                turbineDao.deleteTurbine(turbineId, turbineOfficeId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> turbineDao.retrieveTurbine(turbineId,
                    turbineOfficeId));
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE);
                TurbineDao turbineDao = new TurbineDao(context);
                Turbine turbine1 = buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName());
                turbineDao.storeTurbine(turbine1, false);
                Turbine turbine2 = buildTestTurbine(TURBINE_LOC2, PROJECT_LOC.getName());
                turbineDao.storeTurbine(turbine2, false);
                Turbine turbine3 = buildTestTurbine(TURBINE_LOC3, PROJECT_LOC2.getName());
                turbineDao.storeTurbine(turbine3, false);
                turbineDao.storeTurbine(turbine2, false);
                String turbineId = turbine2.getLocation().getName();
                String turbineOfficeId = turbine2.getLocation().getOfficeId();
                List<Turbine> retrievedTurbine = turbineDao.retrieveTurbines(turbine1.getProjectId().getName(),
                    turbine1.getProjectId().getOfficeId());
                assertEquals(2, retrievedTurbine.size());
                assertTrue(retrievedTurbine.stream()
                    .anyMatch(t -> t.getLocation().getName().equals(turbine1.getLocation().getName())));
                assertTrue(retrievedTurbine.stream()
                    .anyMatch(t -> t.getLocation().getName().equals(turbine2.getLocation().getName())));
                assertFalse(retrievedTurbine.stream()
                    .anyMatch(t -> t.getLocation().getName().equals(turbine3.getLocation().getName())));
                turbineDao.deleteTurbine(turbineId, turbineOfficeId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> turbineDao.retrieveTurbine(turbineId,
                    turbineOfficeId));
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE);
            TurbineDao turbineDao = new TurbineDao(context);
            Turbine turbine = buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName());
            turbineDao.storeTurbine(turbine, false);
            String originalId = turbine.getLocation().getName();
            String office = turbine.getLocation().getOfficeId();
            String newId = turbine.getLocation().getName() + "New";
            turbineDao.renameTurbine(office, originalId, newId);
            assertThrows(NotFoundException.class, () -> turbineDao.retrieveTurbine(originalId, office));
            Turbine retrievedTurbine = turbineDao.retrieveTurbine(newId, office);
            assertEquals(newId, retrievedTurbine.getLocation().getName());
            turbineDao.deleteTurbine(newId, office, DeleteRule.DELETE_ALL);
        });
    }

    @Test
    void testTurbineChangesRoundTrip() throws Exception {
        InputStream resource = this.getClass()
            .getResourceAsStream("/cwms/cda/data/dao/location/kind/turbine-settings.json");
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, TurbineChange.class);
        List<TurbineChange> settingsFromDisk = Formats.parseContentList(contentType, serialized, TurbineChange.class);
        CwmsId projectId = new CwmsId.Builder()
            .withOfficeId(PROJECT_LOC.getOfficeId()).withName(PROJECT_LOC.getName()).build();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE);
            TurbineDao turbineDao = new TurbineDao(context);
            turbineDao.storeTurbine(buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName()), false);
            turbineDao.storeTurbine(buildTestTurbine(TURBINE_LOC2, PROJECT_LOC.getName()), false);
            turbineDao.storeTurbine(buildTestTurbine(TURBINE_LOC3, PROJECT_LOC.getName()), false);
            Instant begin = ZonedDateTime.of(2024, 3, 4, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")).toInstant();
            Instant end = ZonedDateTime.of(2024, 3, 4, 2, 0, 0, 0, ZoneId.of("America/Los_Angeles")).toInstant();
            turbineDao.storeOperationalChanges(settingsFromDisk, true);
            List<TurbineChange> settings =
                turbineDao.retrieveOperationalChanges(projectId, begin, end, true, true, "SI", 3);
            assertEquals(settingsFromDisk.size(), settings.size());
            for (int i = 0; i < settingsFromDisk.size(); i++) {
                DTOMatch.assertMatch(settingsFromDisk.get(i), settings.get(i));
            }
            turbineDao.deleteOperationalChanges(projectId, begin, end, true);
            settings = turbineDao.retrieveOperationalChanges(projectId, begin, end, true, true, "SI", 3);
            assertTrue(settings.isEmpty());
            turbineDao.deleteTurbine(TURBINE_LOC1.getName(), TURBINE_LOC1.getOfficeId(), DeleteRule.DELETE_ALL);
            turbineDao.deleteTurbine(TURBINE_LOC2.getName(), TURBINE_LOC2.getOfficeId(), DeleteRule.DELETE_ALL);
            turbineDao.deleteTurbine(TURBINE_LOC3.getName(), TURBINE_LOC3.getOfficeId(), DeleteRule.DELETE_ALL);
        });

    }

    private static Location buildProjectLocation(String projectId) {
        return new Location.Builder(projectId, "PROJECT", ZoneId.of("UTC"),
            38.5613824, -121.7298432, "NVGD29", OFFICE)
            .withElevation(10.0)
            .withElevationUnits("m")
            .withLocationType("SITE")
            .withCountyName("Sacramento")
            .withNation(Nation.US)
            .withActive(true)
            .withStateInitial("CA")
            .withPublishedLatitude(38.5613824)
            .withPublishedLongitude(-121.7298432)
            .withBoundingOfficeId(OFFICE)
            .withNation(Nation.US)
            .withLongName(projectId + ".long name")
            .withDescription("for testing")
            .withNearestCity("Davis")
            .build();
    }

    private static Turbine buildTestTurbine(Location location, String projectId) {
        return new Turbine.Builder()
            .withLocation(location)
            .withProjectId(new CwmsId.Builder()
                .withName(projectId)
                .withOfficeId(PROJECT_LOC.getOfficeId())
                .build())
            .build();
    }

    private static Location buildTurbineLocation(String locationId) {
        return new Location.Builder(locationId, "TURBINE", ZoneId.of("UTC"),
            38.5613824, -121.7298432, "NVGD29", OFFICE)
            .withElevation(10.0)
            .withElevationUnits("m")
            .withLocationType("SITE")
            .withCountyName("Sacramento")
            .withNation(Nation.US)
            .withActive(true)
            .withStateInitial("CA")
            .withBoundingOfficeId(OFFICE)
            .withPublishedLatitude(38.5613824)
            .withPublishedLongitude(-121.7298432)
            .withNation(Nation.US)
            .withLongName(locationId + ".long name")
            .withDescription("for testing")
            .withNearestCity("Davis")
            .build();
    }

    private static PROJECT_OBJ_T buildProject(Location location) {
        PROJECT_OBJ_T retval = new PROJECT_OBJ_T();
        retval.setPROJECT_LOCATION(LocationUtil.getLocation(location));
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
}
