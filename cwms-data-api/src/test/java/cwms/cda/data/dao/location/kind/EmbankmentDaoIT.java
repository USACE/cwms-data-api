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
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.data.dto.location.kind.EmbankmentTest;
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
final class EmbankmentDaoIT extends DataApiTestIT {

    private static final Location PROJECT_LOC = buildProjectLocation("PROJECT1");
    private static final Location PROJECT_LOC2 = buildProjectLocation("PROJECT2");
    private static final Location EMBANK_LOC1 = buildEmbankmentLocation("PROJECT-EMBANK_LOC1");
    private static final Location EMBANK_LOC2 = buildEmbankmentLocation("EMBANK_LOC2");
    private static final Location EMBANK_LOC3 = buildEmbankmentLocation("EMBANK_LOC3");

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC), "T");
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC2), "T");
                locationsDao.storeLocation(EMBANK_LOC1);
                locationsDao.storeLocation(EMBANK_LOC2);
                locationsDao.storeLocation(EMBANK_LOC3);
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
            locationsDao.deleteLocation(EMBANK_LOC1.getName(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(EMBANK_LOC2.getName(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(EMBANK_LOC3.getName(), databaseLink.getOfficeId());
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
            EmbankmentDao embankmentDao = new EmbankmentDao(context);
            Embankment embankment = buildTestEmbankment(EMBANK_LOC1, PROJECT_LOC.getName());
            embankmentDao.storeEmbankment(embankment, false);
            String embankmentId = embankment.getLocation().getName();
            String embankmentOfficeId = embankment.getLocation().getOfficeId();
            Embankment retrievedEmbankment = embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId);
            EmbankmentTest.assertSame(embankment, retrievedEmbankment);
            embankmentDao.deleteEmbankment(embankmentId, embankmentOfficeId, DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId));
        });
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            EmbankmentDao embankmentDao = new EmbankmentDao(context);
            Embankment embankment1 = buildTestEmbankment(EMBANK_LOC1, PROJECT_LOC.getName());
            embankmentDao.storeEmbankment(embankment1, false);
            Embankment embankment2 = buildTestEmbankment(EMBANK_LOC2, PROJECT_LOC.getName());
            embankmentDao.storeEmbankment(embankment2, false);
            Embankment embankment3 = buildTestEmbankment(EMBANK_LOC3, PROJECT_LOC2.getName());
            embankmentDao.storeEmbankment(embankment2, false);
            String embankmentId = embankment2.getLocation().getName();
            String embankmentOfficeId = embankment2.getLocation().getOfficeId();
            List<Embankment> retrievedEmbankment = embankmentDao.retrieveEmbankments(embankment1.getProjectId().getName(),
                    embankment1.getProjectId().getOfficeId());
            assertEquals(2, retrievedEmbankment.size());
            assertTrue(retrievedEmbankment.stream().anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(embankment1.getLocation().getName())));
            assertTrue(retrievedEmbankment.stream().anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(embankment2.getLocation().getName())));
            assertFalse(retrievedEmbankment.stream().anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(embankment3.getLocation().getName())));
            assertFalse(retrievedEmbankment.contains(embankment3));
            embankmentDao.deleteEmbankment(embankmentId, embankmentOfficeId, DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId));
        });
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            EmbankmentDao embankmentDao = new EmbankmentDao(context);
            Embankment embankment = buildTestEmbankment(EMBANK_LOC1, PROJECT_LOC.getName());
            embankmentDao.storeEmbankment(embankment, false);
            String originalId = embankment.getLocation().getName();
            String office = embankment.getLocation().getOfficeId();
            String newId = embankment.getLocation().getName() + "New";
            embankmentDao.renameEmbankment(office, originalId, newId);
            assertThrows(NotFoundException.class, () -> embankmentDao.retrieveEmbankment(originalId, office));
            Embankment retrievedEmbankment = embankmentDao.retrieveEmbankment(newId, office);
            assertEquals(newId, retrievedEmbankment.getLocation().getName());
            embankmentDao.deleteEmbankment(newId, office, DeleteRule.DELETE_ALL);
        });
    }

    private static Embankment buildTestEmbankment(Location location, String projectId) {
        return new Embankment.Builder()
                .withLocation(location)
                .withMaxHeight(5.0)
                .withProjectId(new CwmsId.Builder()
                        .withName(projectId)
                        .withOfficeId(PROJECT_LOC.getOfficeId())
                        .build())
                .withStructureLength(10.0)
                .withStructureType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Rolled Earth-Filled")
                        .withTooltip("An embankment formed by compacted earth")
                        .withActive(true)
                        .build())
                .withDownstreamProtectionType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Arch Facing")
                        .withTooltip("Protected by the faces of the concrete arches")
                        .withActive(true)
                        .build())
                .withUpstreamProtectionType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Blanket")
                        .withTooltip("Protected by blanket of concrete")
                        .withActive(true)
                        .build())
                .withUpstreamSideSlope(15.0)
                .withLengthUnits("m")
                .withTopWidth(20.0)
                .withStructureLength(25.0)
                .withDownstreamSideSlope(90.0)
                .build();
    }

    private static Location buildProjectLocation(String locationId) {
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder(locationId, "PROJECT", ZoneId.of("UTC"),
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

    private static Location buildEmbankmentLocation(String locationId) {
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder(locationId, "EMBANKMENT", ZoneId.of("UTC"),
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
