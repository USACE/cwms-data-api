package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class EntityTest {

    @Test
    void createEntityAllFieldsProvidedSuccess() {
        CwmsId id = new CwmsId.Builder()
                .withName("NWS")
                .withOfficeId("SPK")
                .build();

        Entity entity = new Entity.Builder()
                .withId(id)
                .withParentEntityId("NOAA")
                .withCategoryId("GOV")
                .withLongName("National Weather Service")
                .build();

        assertAll(
                () -> assertEquals("NWS", entity.getId().getName(), "Entity name"),
                () -> assertEquals("SPK", entity.getId().getOfficeId(), "Entity office id"),
                () -> assertEquals("NOAA", entity.getParentEntityId(), "Parent entity id"),
                () -> assertEquals("GOV", entity.getCategoryId(), "Category id"),
                () -> assertEquals("National Weather Service", entity.getLongName(), "Long name")
        );
    }

    @Test
    void createEntitySerializeRoundTrip() {
        CwmsId id = new CwmsId.Builder()
                .withName("NWS")
                .withOfficeId("SPK")
                .build();

        Entity original = new Entity.Builder()
                .withId(id)
                .withParentEntityId("NOAA")
                .withCategoryId("GOV")
                .withLongName("National Weather Service")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, original);
        Entity deserialized = Formats.parseContent(contentType, json, Entity.class);

        DTOMatch.assertMatch(original, deserialized);
    }

    @Test
    void createEntityMissingRequiredField() {
        //missing required id
        assertThrows(FieldException.class, () -> {
            Entity entity = new Entity.Builder()
                    .withParentEntityId("NOAA")
                    .withCategoryId("GOV")
                    .withLongName("National Weather Service")
                    .build();
            entity.validate();
        });
    }
}
