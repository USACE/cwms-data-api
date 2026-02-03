package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.data.dto.rating.RatingSpecTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RatingSpecXmlUtilsTest {

    @Test
    void testToPlSqlXml() throws JsonProcessingException {
        RatingSpec spec = RatingSpecTest.buildRatingSpec("SWT", "ARBU.Elev;Stor.Linear.Production");
        String xml = RatingSpecXmlUtils.toPlSqlXml(spec);
//        System.out.println("Debug:" + xml);

        assertTrue(xml.contains("version='1.0'") || xml.contains("version=\"1.0\""));
        assertTrue(xml.contains("encoding='UTF-8'") || xml.contains("encoding=\"UTF-8\""));
        assertTrue(xml.contains("<ratings"), "Should contain <ratings");
        assertTrue(xml.contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""), "Should contain xsi namespace");
        assertTrue(xml.contains("xsi:noNamespaceSchemaLocation=\"http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd\""), "Should contain schema location");
        
        // The office-id should be on rating-spec, NOT on ratings
        assertFalse(xml.contains("<ratings office-id="), "office-id should NOT be an attribute of <ratings>");
        assertTrue(xml.contains("<rating-spec office-id=\"SWT\">"), "office-id should be an attribute of <rating-spec>");

        assertTrue(xml.contains("<rating-spec-id>ARBU.Elev;Stor.Linear.Production</rating-spec-id>"));
        assertTrue(xml.contains("<ind-rounding-specs>"));
        assertTrue(xml.contains("<ind-rounding-spec"));
        assertTrue(xml.contains("<dep-rounding-spec>"));
        
        assertFalse(xml.contains("rating-id>"));
        assertFalse(xml.contains("independent-rounding-spec"));
        assertFalse(xml.contains("dependent-rounding-spec"));
    }
}
