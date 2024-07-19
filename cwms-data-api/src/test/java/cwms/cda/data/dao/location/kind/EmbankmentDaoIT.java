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

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
import fixtures.CwmsDataApiSetupCallback;
import cwms.cda.helpers.DTOMatch;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

@Tag("integration")
@TestInstance(Lifecycle.PER_CLASS)
final class EmbankmentDaoIT extends ProjectStructureDaoIT {
    private static final String EMBANKMENT_KIND = "EMBANKMENT";
    private static final Location EMBANK_LOC1 = buildProjectStructureLocation("PROJECT-EMBANK_LOC1_IT", EMBANKMENT_KIND);
    private static final Location EMBANK_LOC2 = buildProjectStructureLocation("EMBANK_LOC2_IT", EMBANKMENT_KIND);
    private static final Location EMBANK_LOC3 = buildProjectStructureLocation("EMBANK_LOC3_IT", EMBANKMENT_KIND);

    @BeforeAll
    public void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                locationsDao.storeLocation(EMBANK_LOC1);
                locationsDao.storeLocation(EMBANK_LOC2);
                locationsDao.storeLocation(EMBANK_LOC3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public void tearDown() throws Exception {
        tearDownProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                locationsDao.deleteLocation(EMBANK_LOC1.getName(), OFFICE_ID, true);
                locationsDao.deleteLocation(EMBANK_LOC2.getName(), OFFICE_ID, true);
                locationsDao.deleteLocation(EMBANK_LOC3.getName(), OFFICE_ID, true);
            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EmbankmentDao embankmentDao = new EmbankmentDao(context);
            Embankment embankment = buildTestEmbankment(EMBANK_LOC1, PROJECT_LOC.getName());
            embankmentDao.storeEmbankment(embankment, false);
            String embankmentId = embankment.getLocation().getName();
            String embankmentOfficeId = embankment.getLocation().getOfficeId();
            Embankment retrievedEmbankment = embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId);
            DTOMatch.assertMatch(embankment, retrievedEmbankment);
            embankmentDao.deleteEmbankment(embankmentId, embankmentOfficeId, DeleteRule.DELETE_ALL);
            assertThrows(NotFoundException.class, () -> embankmentDao.retrieveEmbankment(embankmentId,
                    embankmentOfficeId));
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
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
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
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
        },
        CwmsDataApiSetupCallback.getWebUser());
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
}
