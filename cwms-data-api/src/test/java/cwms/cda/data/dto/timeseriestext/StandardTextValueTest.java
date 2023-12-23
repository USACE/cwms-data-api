package cwms.cda.data.dto.timeseriestext;

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
                        .withId("E")
                        .withOfficeId("CWMS").build())
                .withStandardText("ESTIMATED").build();

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(standardTextValue);
        assertNotNull(json);

    }

    @Test
    void testDeserialization() throws JsonProcessingException
    {
        String input = "{\"id\":{\"office-id\":\"CWMS\",\"id\":\"E\"},\"standard-text\":\"ESTIMATED\"}";

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        StandardTextValue standardTextValue = objectMapper.readValue(input, StandardTextValue.class);
        assertNotNull(standardTextValue);
        StandardTextId id = standardTextValue.getId();
        assertNotNull(id);
        assertEquals("CWMS", id.getOfficeId());
        assertEquals("E", id.getId());
        assertEquals("ESTIMATED", standardTextValue.getStandardText());


    }



}