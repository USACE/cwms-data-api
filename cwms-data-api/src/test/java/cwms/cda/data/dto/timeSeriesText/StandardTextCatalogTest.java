package cwms.cda.data.dto.timeSeriesText;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

class StandardTextCatalogTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        builder.withStandardTextValue(buildSTV("theId", "theOffice", "theText"));
        builder.withStandardTextValue(buildSTV("anotherId", "theOffice", "moreText"));

        StandardTextCatalog catalog = builder.build();
        assertNotNull(catalog);

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalog);
        assertNotNull(json);
//        System.out.println(json);

        assertTrue(json.contains("theOffice"));
        assertTrue(json.contains("theId"));
        assertTrue(json.contains("anotherId"));
        assertTrue(json.contains("theText"));
        assertTrue(json.contains("moreText"));
    }

    private static StandardTextValue buildSTV(String theId, String theOffice, String theText) {
        return new StandardTextValue.Builder()
                .withId(buildID(theId, theOffice))
                .withStandardText(theText).build();
    }

    private static StandardTextId buildID(String theId, String theOffice) {
        return new StandardTextId.Builder()
                .withId(theId)
                .withOfficeId(theOffice).build();
    }

    @Test
    void testRoundtrip() throws JsonProcessingException {
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        builder.withStandardTextValue(buildSTV("theId", "theOffice", "theText"));
        builder.withStandardTextValue(buildSTV("anotherId", "theOffice", "moreText"));

        StandardTextCatalog catalog = builder.build();
        assertNotNull(catalog);

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalog);
        assertNotNull(json);

        StandardTextCatalog catalog2 = mapper.readValue(json, StandardTextCatalog.class);

        assertNotNull(catalog2);
        assertCatalogEquals(catalog, catalog2);

    }

    private static void assertCatalogEquals(StandardTextCatalog catalog, StandardTextCatalog catalog2) {
        assertEquals(catalog.getOfficeId(), catalog2.getOfficeId());

        assertEquals(catalog.getValues().size(), catalog2.getValues().size());
        for (StandardTextValue value : catalog.getValues()) {
            assertTrue(catalog2.getValues().contains(value));
        }

        for (StandardTextValue value : catalog2.getValues()) {
            assertTrue(catalog.getValues().contains(value));
        }
    }

}