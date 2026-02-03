package cwms.cda.data.dto.rating;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class IndependentRoundingSpecTest {

    @Test
    void testDeserializeJSON() throws IOException {
        InputStream resource = getClass().getResourceAsStream("/cwms/cda/data/dto/rating/independent_rounding_spec.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        IndependentRoundingSpec spec = om.readValue(json, IndependentRoundingSpec.class);

        assertNotNull(spec);
        assertEquals(1, spec.getPosition());
        assertEquals("12345", spec.getValue());
    }

    @Test
    void testRoundtripJSON() throws JsonProcessingException {
        IndependentRoundingSpec spec = new IndependentRoundingSpec(2, "54321");

        ObjectMapper om = JsonV2.buildObjectMapper();
        String json = om.writeValueAsString(spec);
        assertNotNull(json);

        IndependentRoundingSpec spec2 = om.readValue(json, IndependentRoundingSpec.class);
        assertNotNull(spec2);
        assertEquals(spec.getPosition(), spec2.getPosition());
        assertEquals(spec.getValue(), spec2.getValue());
        assertEquals(spec, spec2);
    }

    @Test
    void testDeserializeXML() throws IOException {
        InputStream resource = getClass().getResourceAsStream("/cwms/cda/data/dto/rating/independent_rounding_spec.xml");
        assertNotNull(resource);
        String xml = IOUtils.toString(resource, StandardCharsets.UTF_8);

        XMLv2 xmlv2 = new XMLv2();
        IndependentRoundingSpec spec = xmlv2.parseContent(xml, IndependentRoundingSpec.class);

        assertNotNull(spec);
        assertEquals(1, spec.getPosition());
        assertEquals("12345", spec.getValue());
    }

    @Test
    void testRoundtripXML() {
        IndependentRoundingSpec spec = new IndependentRoundingSpec(3, "98765");

        XMLv2 xmlv2 = new XMLv2();
        String xml = xmlv2.format(spec);
        assertNotNull(xml);

        IndependentRoundingSpec spec2 = xmlv2.parseContent(xml, IndependentRoundingSpec.class);
        assertNotNull(spec2);
        assertEquals(spec.getPosition(), spec2.getPosition());
        assertEquals(spec.getValue(), spec2.getValue());
        assertEquals(spec, spec2);
    }

    @Test
    void testRoundtripXML_noPosition() {
        IndependentRoundingSpec spec = new IndependentRoundingSpec("121212");

        XMLv2 xmlv2 = new XMLv2();
        String xml = xmlv2.format(spec);
        assertNotNull(xml);

        IndependentRoundingSpec spec2 = xmlv2.parseContent(xml, IndependentRoundingSpec.class);
        assertNotNull(spec2);
        assertEquals(spec.getPosition(), spec2.getPosition());
        assertEquals(spec.getValue(), spec2.getValue());
        assertEquals(spec, spec2);
    }


}
