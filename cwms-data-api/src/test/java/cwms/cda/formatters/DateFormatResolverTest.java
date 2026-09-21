package cwms.cda.formatters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cwms.cda.api.Controllers;
import io.javalin.http.Context;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DateFormatResolverTest {

    @Test
    void testResolve_Default() {
        assertEquals(DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN), DateFormatResolver.resolve(null, null));
    }

    @Test
    void testResolve_Enum() {
        assertEquals(DateFormat.epochMillis(), DateFormatResolver.resolve(DateFormatParameter.EPOCH_MILLIS.getValue(), null));

        assertEquals(DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN), DateFormatResolver.resolve(DateFormatParameter.ISO_INSTANT.getValue(), null));

        assertEquals(DateFormat.pattern(DateFormatResolver.ISO_OFFSET_PATTERN), DateFormatResolver.resolve(DateFormatParameter.ISO_OFFSET.getValue(), null));
        
        assertEquals(DateFormat.pattern(DateFormatResolver.ISO_LOCAL_PATTERN), DateFormatResolver.resolve(DateFormatParameter.ISO_LOCAL.getValue(), null));

        assertEquals(DateFormat.pattern(DateFormatResolver.DATE_ONLY_PATTERN), DateFormatResolver.resolve(DateFormatParameter.DATE_ONLY.getValue(), null));
    }

    @Test
    void testResolve_CustomWithPattern() {
        assertEquals(DateFormat.pattern("yyyyMMdd"), DateFormatResolver.resolve("custom", "yyyyMMdd"));
    }

    @Test
    void testResolve_CustomMissingPattern() {
        assertThrows(IllegalArgumentException.class, () -> DateFormatResolver.resolve("custom", null));
    }

    @Test
    void testResolve_Unsupported() {
        assertThrows(UnsupportedOperationException.class, () -> DateFormatResolver.resolve("unsupported-format", null));
    }

}
