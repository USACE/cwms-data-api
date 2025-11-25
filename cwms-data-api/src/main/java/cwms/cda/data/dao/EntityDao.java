package cwms.cda.data.dao;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Entity;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENTITY_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_entity.RETRIEVE_ENTITY;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class EntityDao extends JooqDao<Entity> {

    private static final String OFFICE_ID = "OFFICE_ID";
    private static final String ENTITY_ID = "ENTITY_ID";
    private static final String PARENT_ENTITY_ID = "PARENT_ENTITY_ID";
    private static final String CATEGORY_ID = "CATEGORY_ID";
    private static final String ENTITY_NAME = "ENTITY_NAME";

    public EntityDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Entity> retrieveEntities(String officeIdMask, String entityIdMask, String parentIdMask, boolean matchNullParents, String categoryIdMask, String entityNameMask) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeIdMask);
            Result<Record> records = CWMS_ENTITY_PACKAGE.call_CAT_ENTITIES(DSL.using(conn).configuration(), entityIdMask, parentIdMask, formatBool(matchNullParents), categoryIdMask, entityNameMask, officeIdMask);
            return records.stream().map(this::fromJooqEntityRecord)
                    .collect(toList());
        });
    }

    public Entity retrieveEntity(CwmsId entityId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, entityId.getOfficeId());
            RETRIEVE_ENTITY entity = CWMS_ENTITY_PACKAGE.call_RETRIEVE_ENTITY(DSL.using(conn).configuration(), null,
                    null, null, null, null, entityId.getName(), entityId.getOfficeId());
            return fromJooqEntity(entity, entityId);
        });
    }

    public void createEntity(Entity entity) {
        storeEntity(entity, true, true);
    }

    public void updateEntity(Entity entity) {
        storeEntity(entity, false, false);
    }

    public void deleteEntity(CwmsId entityId, boolean deleteChildren) {
        connection(dsl, conn -> {
            setOffice(conn, entityId.getOfficeId());
            CWMS_ENTITY_PACKAGE.call_DELETE_ENTITY__2(DSL.using(conn).configuration(),
                    entityId.getName(), formatBool(deleteChildren), entityId.getOfficeId());
        });
    }

    private void storeEntity(Entity entity, boolean failIfExists, boolean ignoreNulls) {
        connection(dsl, conn -> {
            setOffice(conn, entity.getId().getOfficeId());
            CWMS_ENTITY_PACKAGE.call_STORE_ENTITY(DSL.using(conn).configuration(), entity.getId().getName(), entity.getLongName(),
                    entity.getParentEntityId(), entity.getCategoryId(), formatBool(failIfExists), formatBool(ignoreNulls),
                    entity.getId().getOfficeId());
        });
    }

    static Entity fromJooqEntity(RETRIEVE_ENTITY entity, CwmsId entityId) {
        return new Entity.Builder()
                .withId(entityId)
                .withParentEntityId(entity.getP_PARENT_ENTITY_ID())
                .withCategoryId(entity.getP_CATEGORY_ID())
                .withLongName(entity.getP_ENTITY_NAME())
                .build();
    }

    private Entity fromJooqEntityRecord(Record r) {
        return new Entity.Builder()
                .withId(new CwmsId.Builder()
                        .withOfficeId(r.get(OFFICE_ID, String.class))
                        .withName(r.get(ENTITY_ID, String.class))
                        .build())
                .withParentEntityId(r.get(PARENT_ENTITY_ID, String.class))
                .withCategoryId(r.get(CATEGORY_ID, String.class))
                .withLongName(r.get(ENTITY_NAME, String.class))
                .build();
    }
}
