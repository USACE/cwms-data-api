package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

class ParameterTest {

    @Test
    void testBuilder() {
        Parameter.Builder builder = new Parameter.Builder();
        builder = builder.withParameterId(-65535);
        builder = builder.withParameter("Area");
        builder = builder.withUnitsString("m2");
        builder = builder.withBaseParameter("Area");
        builder = builder.withSubParameter(null);

        Parameter parameter = builder.build();
        assertNotNull(parameter);
        assertEquals(-65535, parameter.getParameterId());
        assertEquals("Area", parameter.getParameter());
        assertEquals("m2", parameter.getUnitsString());
        assertEquals("Area", parameter.getBaseParameter());
        assertNull(parameter.getSubParameter());
    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException {
        Parameter.Builder builder = new Parameter.Builder();
        builder = builder.withParameterId(-65535);
        builder = builder.withParameter("Area");
        builder = builder.withUnitsString("m2");
        builder = builder.withBaseParameter("Area");
        builder = builder.withSubParameter(null);

        Parameter parameter = builder.build();

        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedLocationId = om.writeValueAsString(parameter);
        assertNotNull(serializedLocationId);
//        System.out.println(serializedLocationId);

        Parameter parameter2 = om.readValue(serializedLocationId, Parameter.class);
        assertNotNull(parameter2);

        assertEquals(parameter.getParameterId(), parameter2.getParameterId());
        assertEquals(parameter.getParameter(), parameter2.getParameter());
        assertEquals(parameter.getUnitsString(), parameter2.getUnitsString());
        assertEquals(parameter.getBaseParameter(), parameter2.getBaseParameter());
        assertEquals(parameter.getSubParameter(), parameter2.getSubParameter());

    }

}