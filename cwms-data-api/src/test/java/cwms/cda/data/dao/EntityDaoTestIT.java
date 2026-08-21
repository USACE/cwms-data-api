package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Entity;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
public final class EntityDaoTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
    private static final String ENTITY_ID = "TE";
    private static final String PARENT_ID = "USACE";

    private static Entity buildTestEntity(String longName, String categoryId) {
        return new Entity.Builder()
                .withId(new CwmsId.Builder()
                        .withName(ENTITY_ID)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withCategoryId(categoryId)
                .withParentEntityId(PARENT_ID)
                .withLongName(longName)
                .build();
    }

    @AfterEach
    void teardown() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EntityDao dao = new EntityDao(context);
            CwmsId id = new CwmsId.Builder()
                    .withName(ENTITY_ID)
                    .withOfficeId(OFFICE_ID)
                    .build();
            try {
                dao.deleteEntity(id, true);
            } catch (Exception ignore) {
                FluentLogger.forEnclosingClass().atFine().log("No entity to delete in teardown.");
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreRetrieveDeleteAll() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EntityDao dao = new EntityDao(context);
            Entity entity = buildTestEntity("Test Entity", "GOV");
            dao.createEntity(entity);
            Entity childEntity1 = new Entity.Builder()
                    .withId(new CwmsId.Builder()
                            .withName("TE_CHILD1")
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withCategoryId("GOV")
                    .withParentEntityId(ENTITY_ID)
                    .withLongName("Test Entity Child 1")
                    .build();
            Entity childEntity2 = new Entity.Builder()
                    .withId(new CwmsId.Builder()
                            .withName("TE_CHILD2")
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withCategoryId("GOV")
                    .withParentEntityId(ENTITY_ID)
                    .withLongName("Test Entity Child 2")
                    .build();
            dao.createEntity(childEntity1);
            dao.createEntity(childEntity2);
            List<Entity> retrieved = dao.retrieveEntities(OFFICE_ID, null, ENTITY_ID, true, null, null);
            assertEquals(2, retrieved.size());

            dao.deleteEntity(entity.getId(), true);
            //verify deletion
            List<Entity> retrievedAfterDelete = dao.retrieveEntities(OFFICE_ID, null, ENTITY_ID, true, null, null);
            assertEquals(0, retrievedAfterDelete.size());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieve() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EntityDao dao = new EntityDao(context);
            Entity entity = buildTestEntity("Test Entity", "GOV");
            CwmsId id = entity.getId();
            dao.createEntity(entity);
            Entity retrieved = dao.retrieveEntity(id);
            DTOMatch.assertMatch(id, retrieved.getId());
            assertEquals(entity.getLongName(), retrieved.getLongName());
            assertEquals(entity.getCategoryId(), retrieved.getCategoryId());
            assertEquals(entity.getParentEntityId(), retrieved.getParentEntityId());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testUpdate() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EntityDao dao = new EntityDao(context);
            Entity original = buildTestEntity("Test Entity", "GOV");
            CwmsId id = original.getId();
            dao.createEntity(original);
            Entity updated = buildTestEntity("Test Ent", null);
            dao.updateEntity(updated);
            Entity retrieved = dao.retrieveEntity(id);
            DTOMatch.assertMatch(updated, retrieved);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDelete() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            EntityDao dao = new EntityDao(context);
            Entity entity = buildTestEntity("Test Entity", "GOV");
            CwmsId id = entity.getId();
            dao.createEntity(entity);
            //verify created
            Entity retrieved = dao.retrieveEntity(id);
            DTOMatch.assertMatch(id, retrieved.getId());
            // delete
            dao.deleteEntity(id, true);
            // verify deleted
            assertThrows(NotFoundException.class, () -> dao.retrieveEntity(id));
        }, CwmsDataApiSetupCallback.getWebUser());
    }
}
