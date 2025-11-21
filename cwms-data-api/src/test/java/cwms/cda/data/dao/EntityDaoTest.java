package cwms.cda.data.dao;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.cwms_entity.RETRIEVE_ENTITY;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EntityDaoTest {

    @Test
    void testFromJooqEntity() {
        CwmsId id = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("NWS")
                .build();
        RETRIEVE_ENTITY jooqEntity = mock(RETRIEVE_ENTITY.class);
        when(jooqEntity.getP_ENTITY_NAME()).thenReturn("National Weather Service");
        when(jooqEntity.getP_PARENT_ENTITY_ID()).thenReturn("NOAA");
        when(jooqEntity.getP_CATEGORY_ID()).thenReturn("GOV");
        var entity = EntityDao.fromJooqEntity(jooqEntity, id);
        DTOMatch.assertMatch(id, entity.getId());
        assertEquals("National Weather Service", entity.getLongName());
        assertEquals("NOAA", entity.getParentEntityId());
        assertEquals("GOV", entity.getCategoryId());
    }
}
