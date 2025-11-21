package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DTOMatch;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void testIOSerializationRoundTrip() throws Exception {
        Entity expectedEntity= new Entity.Builder()
                .withId(new CwmsId.Builder()
                        .withName("NWS")
                        .withOfficeId("SPK")
                        .build())
                .withParentEntityId("NOAA")
                .withCategoryId("GOV")
                .withLongName("National Weather Service")
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/entity.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSONV2);
        Entity deserialized = Formats.parseContent(contentType, json, Entity.class);

        DTOMatch.assertMatch(expectedEntity, deserialized);

        String serialized = JsonV2.buildObjectMapper().writeValueAsString(deserialized);
        assertEquals(json.replaceAll("\\s+", ""), serialized.replaceAll("\\s+", ""));

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
