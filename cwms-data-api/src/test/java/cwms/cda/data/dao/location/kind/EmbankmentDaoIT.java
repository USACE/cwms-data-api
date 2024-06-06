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
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
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

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
final class EmbankmentDaoIT extends DataApiTestIT {

    private static final Location PROJECT_LOC = buildProjectLocation();
    private static final Location EMBANKMENT_LOC = buildEmbankmentLocation();

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                PROJECT_OBJ_T projectObjT = buildProject();
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
                locationsDao.storeLocation(EMBANKMENT_LOC);
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
            locationsDao.deleteLocation(EMBANKMENT_LOC.getName(), databaseLink.getOfficeId());
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
                    DeleteRule.DELETE_ALL.getRule(), databaseLink.getOfficeId());
            locationsDao.deleteLocation(PROJECT_LOC.getName(), databaseLink.getOfficeId());
        });
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            EmbankmentDao embankmentDao = new EmbankmentDao(context);
            Embankment embankment = buildTestEmbankment();
            embankmentDao.storeEmbankment(embankment, false);
            String embankmentId = embankment.getLocation().getName();
            String embankmentOfficeId = embankment.getLocation().getOfficeId();
            Embankment retrievedEmbankment = embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId);
            assertEquals(embankment, retrievedEmbankment);
            retrievedEmbankment = embankmentDao.retrieveEmbankments(embankment.getProjectId(),
                    embankment.getProjectOfficeId()).get(0);
            assertEquals(embankment, retrievedEmbankment);
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
            Embankment embankment = buildTestEmbankment();
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

    private static Embankment buildTestEmbankment() {
        return new Embankment.Builder()
                .withLocation(EMBANKMENT_LOC)
                .withHeightMax(5.0)
                .withProjectId(PROJECT_LOC.getName())
                .withProjectOfficeId(PROJECT_LOC.getOfficeId())
                .withStructureLength(10.0)
                .withStructureType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Rolled Earth-Filled")
                        .withTooltip("An embankment formed by compacted earth")
                        .withActive(true)
                        .build())
                .withDownstreamProtType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Arch Facing")
                        .withTooltip("Protected by the faces of the concrete arches")
                        .withActive(true)
                        .build())
                .withUpstreamProtType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("Concrete Blanket")
                        .withTooltip("Protected by blanket of concrete")
                        .withActive(true)
                        .build())
                .withUpstreamSideSlope(15.0)
                .withUnitsId("m")
                .withTopWidth(20.0)
                .withStructureLength(25.0)
                .withDownstreamSideSlope(90.0)
                .build();
    }

    private static Location buildProjectLocation() {
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder("PROJECT", "PROJECT", ZoneId.of("UTC"),
                38.5613824, -121.7298432, "NVGD29", officeId)
                .withElevation(10.0)
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
                .build();
    }

    private static Location buildEmbankmentLocation() {
        String officeId = CwmsDataApiSetupCallback.getDatabaseLink().getOfficeId();
        return new Location.Builder("PROJECT-EMBANKMENT_LOC", "EMBANKMENT", ZoneId.of("UTC"),
                38.5613824, -121.7298432, "NVGD29", officeId)
                .withElevation(10.0)
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
                .build();
    }

    private static PROJECT_OBJ_T buildProject() {
        PROJECT_OBJ_T retval = new PROJECT_OBJ_T();
        retval.setPROJECT_LOCATION(LocationUtil.getLocation(PROJECT_LOC));
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
