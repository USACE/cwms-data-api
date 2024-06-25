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

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.Turbine;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
final class TurbineDaoIT extends DataApiTestIT {

    private static final Location PROJECT_LOC = buildProjectLocation("PROJECT1");
    private static final Location PROJECT_LOC2 = buildProjectLocation("PROJECT2");
    private static final Location TURBINE_LOC1 = buildTurbineLocation("PROJECT-TURB_LOC1");
    private static final Location TURBINE_LOC2 = buildTurbineLocation("TURB_LOC2");
    private static final Location TURBINE_LOC3 = buildTurbineLocation("TURB_LOC3");

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
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
        });
    }

    @AfterAll
    public static void tearDown() throws Exception {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            locationsDao.deleteLocation(TURBINE_LOC1.getName(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(TURBINE_LOC2.getName(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(TURBINE_LOC3.getName(), databaseLink.getOfficeId());
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
                    DeleteRule.DELETE_ALL.getRule(), databaseLink.getOfficeId());
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC2.getName(),
                    DeleteRule.DELETE_ALL.getRule(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(PROJECT_LOC.getName(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(PROJECT_LOC2.getName(), databaseLink.getOfficeId());
        });
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TurbineDao turbineDao = new TurbineDao(context);
            Turbine turbine = buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName());
            turbineDao.storeTurbine(turbine, false);
            String turbineId = turbine.getLocation().getName();
            String turbineOfficeId = turbine.getLocation().getOfficeId();
            Turbine retrievedTurbine = turbineDao.retrieveTurbine(turbineId,
                    turbineOfficeId);
            assertEquals(turbine, retrievedTurbine);
            turbineDao.deleteTurbine(turbineId, turbineOfficeId, DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> turbineDao.retrieveTurbine(turbineId,
                    turbineOfficeId));
        });
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            TurbineDao turbineDao = new TurbineDao(context);
            Turbine turbine1 = buildTestTurbine(TURBINE_LOC1, PROJECT_LOC.getName());
            turbineDao.storeTurbine(turbine1, false);
            Turbine turbine2 = buildTestTurbine(TURBINE_LOC2, PROJECT_LOC.getName());
            turbineDao.storeTurbine(turbine2, false);
            Turbine turbine3 = buildTestTurbine(TURBINE_LOC3, PROJECT_LOC2.getName());
            turbineDao.storeTurbine(turbine2, false);
            String turbineId = turbine2.getLocation().getName();
            String turbineOfficeId = turbine2.getLocation().getOfficeId();
            List<Turbine> retrievedTurbine = turbineDao.retrieveTurbines(turbine1.getProjectId().getName(),
                    turbine1.getProjectId().getOfficeId());
            assertEquals(2, retrievedTurbine.size());
            assertTrue(retrievedTurbine.contains(turbine1));
            assertTrue(retrievedTurbine.contains(turbine2));
            assertFalse(retrievedTurbine.contains(turbine3));
            turbineDao.deleteTurbine(turbineId, turbineOfficeId, DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> turbineDao.retrieveTurbine(turbineId,
                    turbineOfficeId));
        });
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
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

    private static Location buildProjectLocation(String projectId) {
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder(projectId, "PROJECT", ZoneId.of("UTC"),
                38.5613824, -121.7298432, "NVGD29", officeId)
                .withElevation(10.0)
                .withElevationUnits("m")
                .withLocationType("SITE")
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withPublishedLatitude(38.5613824)
                .withPublishedLongitude(-121.7298432)
                .withBoundingOfficeId(officeId)
                .withLongName("UNITED STATES")
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
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder(locationId, "TURBINE", ZoneId.of("UTC"),
                38.5613824, -121.7298432, "NVGD29", officeId)
                .withElevation(10.0)
                .withElevationUnits("m")
                .withLocationType("SITE")
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withBoundingOfficeId(officeId)
                .withPublishedLatitude(38.5613824)
                .withPublishedLongitude(-121.7298432)
                .withLongName("UNITED STATES")
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
