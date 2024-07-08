package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TimeSeriesCategoryTest {

    @Test
    void test_serialize_json() throws JsonProcessingException {
        TimeSeriesCategory category = new TimeSeriesCategory("SPK", "cat_for_serialize_test", "UnitTesting");

        ObjectMapper om = JsonV1.buildObjectMapper();

        String result = om.writeValueAsString(category);
        assertNotNull(result);

        JsonNode jsonNode = om.readTree(result);
        assertEquals("SPK", jsonNode.get("office-id").asText());
        assertEquals("cat_for_serialize_test", jsonNode.get("id").asText());
        assertEquals("UnitTesting", jsonNode.get("description").asText());

        TimeSeriesCategory cat2 = om.readValue(result, TimeSeriesCategory.class);

        assertEquals(category, cat2);
    }

    @Test
    void test_serialize_list() throws JsonProcessingException {
        List<TimeSeriesCategory> cats = new ArrayList<>();
        TimeSeriesCategory category1 = new TimeSeriesCategory("SPK", "cat_for_serialize_test1", "UnitTesting");
        TimeSeriesCategory category2 = new TimeSeriesCategory("SPK", "cat_for_serialize_test2", "UnitTesting");
        cats.add(category1);
        cats.add(category2);

        ContentType contentType = new ContentType(Formats.JSON);
        String result = Formats.format(contentType, cats, TimeSeriesCategory.class);

        assertNotNull(result);

        ObjectMapper om = JsonV1.buildObjectMapper();
        JsonNode jsonNode = om.readTree(result);
        assertEquals("SPK", jsonNode.get(0).get("office-id").asText());
        assertEquals("cat_for_serialize_test1", jsonNode.get(0).get("id").asText());
    }

    @Test
    void test_serialize_empty_list() throws JsonProcessingException {
        List<TimeSeriesCategory> cats = new ArrayList<>();

        ContentType contentType = new ContentType(Formats.JSON);
        String result = Formats.format(contentType, cats, TimeSeriesCategory.class);

        assertNotNull(result);

        ObjectMapper om = JsonV1.buildObjectMapper();
        JsonNode jsonNode = om.readTree(result);
        assertEquals(0, jsonNode.size());
    }

}