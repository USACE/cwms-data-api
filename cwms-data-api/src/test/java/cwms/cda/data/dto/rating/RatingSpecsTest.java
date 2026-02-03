package cwms.cda.data.dto.rating;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasXPath;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

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
    
    @Test
    void testXmlSerialize() throws ParserConfigurationException, SAXException, IOException {
        RatingSpecs specs = buildRatingSpecs();
        String xml = Formats.format(Formats.parseHeader(Formats.XMLV2, RatingSpecs.class), specs);
        assertNotNull(xml);
        System.out.println("[DEBUG_LOG] xml: " + xml);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new InputSource(new StringReader(xml)));

        assertThat(doc, hasXPath("/rating-specs/page-size", is("10")));
        assertThat(doc, hasXPath("/rating-specs/total", is("2")));

        assertThat(doc, hasXPath("/rating-specs/specs/rating-spec[1]/office-id", is("SWT")));
        assertThat(doc, hasXPath("/rating-specs/specs/rating-spec[1]/rating-id", is("ARBU.Elev;Stor.Linear.Production")));

        assertThat(doc, hasXPath("/rating-specs/specs/rating-spec[2]/office-id", is("SWT")));
        assertThat(doc, hasXPath("/rating-specs/specs/rating-spec[2]/rating-id", is("OBRK.Elev;Stor.Linear.Production")));
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
