package cwms.cda.data.dto.timeseriestext;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

class StandardTextIdTest {


    @Test
    void testSerialization() throws JsonProcessingException
    {

        StandardTextId id = new StandardTextId.Builder()
                .withId("theId")
                .withOfficeId("theOffice").build();


        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(id);
        assertNotNull(json);
//        System.out.println(json);
    }

    @Test
    void testDeserialization() throws JsonProcessingException
    {
        String input = "{\"office-id\":\"theOffice\",\"id\":\"theId\"}";

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        StandardTextId id = objectMapper.readValue(input, StandardTextId.class);
        assertNotNull(id);
        assertEquals("theOffice", id.getOfficeId());
        assertEquals("theId", id.getId());
    }

}