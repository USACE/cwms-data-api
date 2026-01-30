package cwms.cda.data.dto.rating;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.jooq.XML;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class RatingSpecsTest {

    @ParameterizedTest
    @CsvSource({
            "json, " + Formats.JSONV2,
            "xml, " + Formats.XMLV2
    })
    void testDeserialize(String ext, String format) throws IOException {
        InputStream resource = getClass().getResourceAsStream("/cwms/cda/data/dto/rating/rating_specs." + ext);
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ContentType type = Formats.parseHeader(format, RatingSpecs.class);
        RatingSpecs specs = Formats.parseContent(type, json, RatingSpecs.class);

        assertNotNull(specs);
        assertEquals(2, specs.getSpecs().size());
    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.XMLV2, Formats.DEFAULT})
    void testRoundtrip(String format) {
        RatingSpecs specs = buildRatingSpecs();

        ContentType type = Formats.parseHeader(format, RatingSpecs.class);
        String xml = Formats.format(type, specs);
        assertNotNull(xml);

        RatingSpecs specs2 = Formats.parseContent(type, xml, RatingSpecs.class);
        assertNotNull(specs2);
        assertEquals(specs, specs2);
    }

    private RatingSpecs buildRatingSpecs() {
        List<RatingSpec> specList = new ArrayList<>();
        specList.add(RatingSpecTest.buildRatingSpec("SWT", "ARBU.Elev;Stor.Linear.Production"));
        specList.add(RatingSpecTest.buildRatingSpec("SWT", "OBRK.Elev;Stor.Linear.Production"));

        RatingSpecs.Builder builder = new RatingSpecs.Builder(0, 10, 2);
        builder.withSpecs(specList);
        return builder.build();
    }
}
