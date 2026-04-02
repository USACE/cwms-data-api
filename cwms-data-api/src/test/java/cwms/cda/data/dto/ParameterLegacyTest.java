package cwms.cda.data.dto;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static cwms.cda.helpers.DTOMatch.assertMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ParameterLegacyTest {

    @Test
    void test_serialization() {
        ParameterLegacy expected = new ParameterLegacy.Builder()
                .withAbstractParam("Area")
                .withName("Area")
                .withOffice("All")
                .withDefaultEnglishUnit("ft2")
                .withDefaultSiUnit("m2")
                .withLongName("Surface Area")
                .withDescription("Area of a surface")
                .build();
        ContentType contentType = new ContentType(Formats.JSON_LEGACY);
        String json = Formats.format(contentType, expected);
        ParameterLegacy parsedParam = Formats.parseContent(contentType, json, ParameterLegacy.class);
        assertMatch(expected, parsedParam);
    }

    @Test
    void test_from_resource_file() throws Exception {
        InputStream resource = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/parameter_legacy.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON_LEGACY);
        ParameterLegacy receivedTzs = Formats.parseContent(contentType, json, ParameterLegacy.class);
        assertNotNull(receivedTzs);
    }
}
