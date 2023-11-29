package cwms.cda.data.dto.timeSeriesText;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

class StandardTextValueTest {

    @Test
    void testSerialization() throws JsonProcessingException
    {

        StandardTextValue standardTextValue = new StandardTextValue.Builder()
                .withId(new StandardTextId.Builder()
                        .withId("theId")
                        .withOfficeId("theOffice").build())
                .withStandardText("textValue").build();

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(standardTextValue);
        assertNotNull(json);
        System.out.println(json);
    }

    @Test
    void testDeserialization() throws JsonProcessingException
    {
        String input = "{\"id\":{\"office-id\":\"theOffice\",\"id\":\"theId\"},\"standard-text\":\"textValue\"}";

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        StandardTextValue standardTextValue = objectMapper.readValue(input, StandardTextValue.class);
        assertNotNull(standardTextValue);
        StandardTextId id = standardTextValue.getId();
        assertNotNull(id);
        assertEquals("theOffice", id.getOfficeId());
        assertEquals("theId", id.getId());
        assertEquals("textValue", standardTextValue.getStandardText());


    }

}