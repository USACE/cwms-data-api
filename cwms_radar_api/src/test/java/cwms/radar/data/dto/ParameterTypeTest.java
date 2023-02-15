package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ParameterTypeTest {

    @Test
    void testBuilder(){
        ParameterType.Builder builder = new ParameterType.Builder();
        builder = builder.withType("Inst");
        ParameterType pt = builder.build();

        assertNotNull(pt);
        assertEquals("Inst", pt.getType());

    }

}